"""
591 房屋交易網 爬蟲。

爬取策略：
- URL 加 order=posttime_desc（按刊登時間新→舊排序）
- 每頁 30 筆，逐頁檢查 source_id 是否已在 DB
- 遇到已存在的 source_id → 停止（表示已窮舉完新物件）
- 若為首次執行（DB 空）→ 每個 district/type 最多 100 筆
- 建物類型直接在 URL 過濾（type=2/5/7），不再撈全部再過濾
"""
import re
import logging
import random
from datetime import datetime
from typing import Optional, Callable

from playwright.sync_api import Page, BrowserContext

from config import (
    TARGET_REGIONS,
    BUILDING_TYPES,
    MAX_FLOOR_FOR_APARTMENT,
)
from scraper.browser_manager import get_browser_context, human_delay

logger = logging.getLogger(__name__)

BASE_URL = "https://sale.591.com.tw"
DEFAULT_LIMIT = 30        # 預設每次抓取最多幾筆新物件（總數）
MAX_EMPTY_PAGES = 3       # 連續 N 頁都是舊物件就停（表示已窮舉）


# ── 工具函式 ──────────────────────────────────────────────────────────────────

def _parse_price(text: str) -> Optional[float]:
    """嚴格解析「N,NNN萬」格式，避免幻覺。找不到就回 None，不亂猜。"""
    text = text.replace(",", "").strip()
    m = re.search(r"([\d.]+)\s*萬", text)
    if m:
        try:
            return float(m.group(1)) * 10000
        except ValueError:
            return None
    return None


def _parse_area(text: str) -> Optional[float]:
    m = re.search(r"([\d.]+)\s*坪", text)
    return float(m.group(1)) if m else None


def _parse_age(text: str) -> Optional[int]:
    current_year = datetime.now().year
    m = re.search(r"屋齡\s*(\d+)\s*年", text)
    if m:
        return int(m.group(1))
    m = re.search(r"民國\s*(\d+)\s*年", text)
    if m:
        return current_year - (int(m.group(1)) + 1911)
    m = re.search(r"(19\d{2}|20[01]\d)\s*年", text)
    if m:
        return current_year - int(m.group(1))
    m = re.search(r"(\d{1,2})\s*年", text)
    if m:
        val = int(m.group(1))
        if 1 <= val <= 80:
            return val
    return None


def _parse_floors(text: str) -> tuple[Optional[str], Optional[int]]:
    """591 卡片樓層常見格式：'3F/5F'、'整棟/2F'、'1-4F/4F'"""
    text = text.strip()
    # 「整棟/2F」→ floor=整棟, total=2
    m = re.search(r"整棟[/／](\d+)\s*[Ff層]", text)
    if m:
        return "整棟", int(m.group(1))
    # 「3F/5F」或「1-4F/4F」→ floor=前段, total=後段
    m = re.search(r"([\d\-]+)\s*[Ff層]\s*[/／]\s*(\d+)\s*[Ff層]", text)
    if m:
        return m.group(1), int(m.group(2))
    # 單一「5F」
    m = re.search(r"(\d+)\s*[Ff層]", text)
    if m:
        return m.group(1), int(m.group(1))
    return None, None


def _detect_building_type(text: str) -> Optional[str]:
    if "透天" in text:
        return "透天厝"
    if "店面" in text or "店舖" in text:
        return "店面"
    if "公寓" in text:
        return "公寓"
    if "華廈" in text:
        return "華廈"
    if "大樓" in text:
        return "大樓"
    return None


def _card_road_name(addr: str) -> str:
    """從地址抓路名（砍城市/區前綴避免貪婪）。空字串代表 addr 無路名。"""
    if not addr:
        return ""
    inner = re.sub(r"^(台北市|臺北市|新北市|桃園市|基隆市|新竹市|新竹縣|宜蘭縣)", "", addr)
    inner = re.sub(r"^[一-龥]{1,3}區", "", inner)
    m = re.search(r"([一-龥]{1,5}(?:路|街|大道)(?:[一二三四五六七八九十]段)?)", inner)
    return m.group(1) if m else ""


def _card_fields_changed(item: dict, existing: dict) -> str:
    """
    比對 591 listing card 四欄 vs DB 舊 doc。任一有明顯變動 → 回變動原因字串；完全相同回 ""。
    被任一條件觸發 → 應強制重抓 pipeline（card 資訊不足以分辨「資料補齊」vs「ID 重用」）。

    門檻（吸收 OCR/API 雜訊，但抓得到真實變動）：
      - 建坪差 > 0.5 坪
      - 總樓層不等（exact）
      - 所在樓層不等（exact，字串比）
      - 價格差 > max(10 萬, 舊值 1%)
      - card 地址路名變更（雙邊都有值且不等；單邊空不算）
    """
    reasons = []

    new_ar = item.get("building_area_ping")
    old_ar = existing.get("building_area_ping")
    if new_ar and old_ar and abs(float(new_ar) - float(old_ar)) > 0.5:
        reasons.append(f"建坪 {old_ar:.2f}→{new_ar:.2f}")

    new_tf = item.get("total_floors")
    old_tf = existing.get("total_floors")
    if new_tf and old_tf and int(new_tf) != int(old_tf):
        reasons.append(f"總樓 {old_tf}F→{new_tf}F")

    new_f = item.get("floor")
    old_f = existing.get("floor")
    if new_f and old_f and str(new_f) != str(old_f):
        reasons.append(f"樓 {old_f}→{new_f}")

    new_p = item.get("price_ntd")
    old_p = existing.get("price_ntd")
    if new_p and old_p and abs(new_p - old_p) > max(100000, old_p * 0.01):
        reasons.append(f"價 {int(old_p/10000)}→{int(new_p/10000)}萬")

    new_road = _card_road_name(item.get("address"))
    old_road = _card_road_name(existing.get("address"))
    if new_road and old_road and new_road != old_road:
        reasons.append(f"路名 {old_road}→{new_road}")

    return "; ".join(reasons)


def _is_target_type(item: dict) -> tuple[bool, str]:
    """
    URL 已用 shape=1,3 過濾為公寓+透天，信任這個過濾。
    只需排除 5 層以上的公寓（5F 以上不是都更主力）。
    回傳 (是否符合, 原因字串)。不符合時 reason 非空。
    """
    btype = (item.get("building_type") or "").strip()
    floors = item.get("total_floors")
    if btype == "公寓" and floors and floors > MAX_FLOOR_FOR_APARTMENT:
        return False, f"總樓層 {floors}F（>{MAX_FLOOR_FOR_APARTMENT}，實為華廈）"
    return True, ""


# ── 主要爬取函式 ──────────────────────────────────────────────────────────────

def scrape_591(
    headless: bool = True,
    progress_callback=None,
    max_districts: int = 0,
    district_filter: str = "",   # 舊單一過濾（向下相容）
    city_filter: str = "",       # 舊（向下相容）
    districts_filter: list = None,  # 新：明確指定的行政區列表
    check_exists: Optional[Callable] = None,
    limit: int = DEFAULT_LIMIT,
) -> dict:
    """
    爬取 591 目標區域的物件。
    回傳 {"new": [...新物件...], "price_updates": [...價格變動...]}

    check_exists(source_id) -> None | dict
        回傳 None 表示 DB 中沒有，回傳 dict 表示已存在。
    """
    from database.time_utils import now_tw
    all_new = []
    all_price_updates = []
    scrape_session_at = now_tw()

    # 以城市為單位分組，每個城市一次請求合併所有目標區（section=x,y,z）
    df_set = set(districts_filter or [])
    city_groups = []
    for city, info in TARGET_REGIONS.items():
        if city_filter and city_filter != city:
            continue
        districts_for_city = []
        for district, section_id in info["districts"].items():
            if df_set:
                if district not in df_set:
                    continue
            elif district_filter and district_filter not in district:
                continue
            districts_for_city.append((district, section_id))
        if districts_for_city:
            city_groups.append((city, info["region_id"], districts_for_city))

    type_codes = ",".join(BUILDING_TYPES.values())
    total = len(city_groups)

    with get_browser_context(headless=headless) as ctx:
        for idx, (city, region_id, districts_for_city) in enumerate(city_groups, 1):
            if len(all_new) >= limit:
                break

            section_ids = ",".join(s for _, s in districts_for_city)
            target_districts = set(d for d, _ in districts_for_city)

            remaining = limit - len(all_new)
            msg = f"[{idx}/{total}] {city}（{len(target_districts)} 區，剩 {remaining} 配額）"
            logger.info(msg)
            if progress_callback:
                progress_callback(msg)
            try:
                result = _scrape_district(
                    ctx,
                    city=city,
                    district="",
                    region_id=region_id,
                    section_id=section_ids,
                    target_districts=target_districts,
                    type_code=type_codes,
                    check_exists=check_exists,
                    limit=remaining,
                    progress_cb=progress_callback,
                )
                all_new.extend(result["new"])
                all_price_updates.extend(result["price_updates"])
                human_delay(1.5, 3.0)
            except Exception as e:
                logger.error(f"Failed {city}: {e}")

    # 公寓額外過濾樓層（URL type=2 已限定公寓，但仍需排除 5 層以上）
    before = len(all_new)
    kept = []
    for r in all_new:
        ok, reason = _is_target_type(r)
        if ok:
            kept.append(r)
        elif progress_callback:
            progress_callback(
                f"  ⏭ 跳過 {r.get('source_id')}：{reason} — {(r.get('title') or '')[:20]}"
            )
    all_new = kept

    # 標記 591 上的位置（0 = 最新，在 591 上排最前面）
    session_iso = scrape_session_at.isoformat()
    for rank, item in enumerate(all_new):
        item["list_rank"] = rank
        item["scrape_session_at"] = session_iso

    logger.info(f"591 爬取完成：{before} 筆原始 → {len(all_new)} 筆符合條件，{len(all_price_updates)} 筆價格變動")

    return {"new": all_new, "price_updates": all_price_updates}


LAST_FETCH_ERROR: Optional[str] = None   # module-level 狀態，外層可讀


def _fetch_listing_page_api(region_id: str, section_id: str, shape: str, first_row: int,
                             city: str, target_districts: Optional[set] = None,
                             progress_cb: Optional[Callable] = None) -> list[dict]:
    """直接呼叫 591 BFF API 取得 listing（跳過 Playwright + JS render）。

    API: https://bff-house.591.com.tw/v1/web/sale/list
    shape: "1"=公寓, "3"=透天（本專案主用公寓）
    Filters out: is_pro_advertisement=True / is_newhouse=True
    Returns: list of item dicts, mapped to scraper 的 item 格式。

    失敗原因會寫進 module-level LAST_FETCH_ERROR，也會推到 progress_cb 讓 UI log 看得到。
    """
    global LAST_FETCH_ERROR
    import httpx as _httpx
    import time as _time
    url = "https://bff-house.591.com.tw/v1/web/sale/list"
    params = {
        "timestamp": int(_time.time() * 1000),
        "type": 2, "category": 1,
        "regionid": region_id,
        "section": section_id,
        "shape": shape,
        "firstRow": first_row,
        "shType": "list",
        "order": "posttime_desc",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://sale.591.com.tw/",
    }
    last_err = None
    for attempt in range(3):
        try:
            r = _httpx.get(url, params=params, headers=headers, timeout=20, verify=False)
            if r.status_code == 429:
                last_err = f"429 被限流（嘗試 {attempt+1}/3）"
                if progress_cb:
                    progress_cb(f"  ⚠ 591 API 被限流，等 {5*(attempt+1)}s 後重試…")
                _time.sleep(5 * (attempt + 1))
                continue
            r.raise_for_status()
            house_list = r.json().get("data", {}).get("house_list") or []
            LAST_FETCH_ERROR = None
            return _parse_api_items(house_list, city, target_districts)
        except _httpx.TimeoutException as e:
            last_err = f"逾時（{type(e).__name__}，嘗試 {attempt+1}/3）"
            if progress_cb:
                progress_cb(f"  ⚠ 591 API 連線逾時，重試…")
            _time.sleep(2)
            continue
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            break
    LAST_FETCH_ERROR = last_err or "未知錯誤"
    logger.warning(f"591 API listing 失敗 (firstRow={first_row}): {LAST_FETCH_ERROR}")
    if progress_cb:
        progress_cb(f"  ⚠ 591 API 失敗：{LAST_FETCH_ERROR}")
    return []


def _parse_api_items(house_list: list, city: str, target_districts: Optional[set]) -> list[dict]:

    items = []
    for it in house_list:
        if it.get("is_pro_advertisement") or it.get("is_newhouse"):
            continue
        # 跨區檢查
        sect_name = it.get("section_name") or ""
        if target_districts and sect_name and sect_name not in target_districts:
            continue
        houseid = it.get("houseid")
        if not houseid:
            continue
        source_id = f"591_{houseid}"
        # 591 URL
        detail_url = f"{BASE_URL}/home/house/detail/2/{houseid}.html"
        # floor "4F/5F" → floor=4, total_floors=5
        floor_raw = str(it.get("floor") or "")
        floor_str = floor_raw
        total_floors = None
        fm = re.match(r"(\d+)F?\s*[/／]\s*(\d+)F?", floor_raw)
        if fm:
            floor_str = fm.group(1)
            try: total_floors = int(fm.group(2))
            except Exception: pass
        # 坪數：area = 權狀坪數, mainarea = 主建物坪數。用 area (近似 591 DOM 顯示的「權狀坪數」)
        area_ping = it.get("area") or it.get("mainarea")
        try: area_ping = float(area_ping) if area_ping is not None else None
        except Exception: area_ping = None
        # 價格：API 的 price 單位是「萬」
        price_wan = it.get("price")
        try: price_ntd = int(price_wan) * 10000 if price_wan else None
        except Exception: price_ntd = None
        try: age = int(it.get("houseage")) if it.get("houseage") is not None else None
        except Exception: age = None
        # address
        street = it.get("address") or it.get("street_name") or ""
        full_address = f"{city}{sect_name}{street}".strip() or f"{city}{sect_name}"
        items.append({
            "source": "591",
            "source_id": source_id,
            "url": detail_url,
            "title": it.get("title") or "",
            "city": city,
            "district": sect_name or None,
            "address": full_address,
            "building_type": it.get("shape_name") or "公寓",
            "total_floors": total_floors,
            "floor": floor_str,
            "building_age": age,
            "building_age_completed_year": __import__("database.models", fromlist=["age_to_completed_year"]).age_to_completed_year(age),
            "building_age_source": "591_card" if age else None,
            "building_area_ping": area_ping,
            "price_ntd": price_ntd,
            "price_per_ping": (price_ntd / area_ping) if price_ntd and area_ping else None,
            "image_url": it.get("photo_url") or None,
            "_published_text": it.get("refreshtime") or "",
            "_raw_text": "",  # API 模式沒 raw text
        })
    return items


def _scrape_district(
    ctx: BrowserContext,
    city: str,
    district: str,
    region_id: str,
    section_id: str,
    building_type: str = "",
    type_code: str = "",
    target_districts: Optional[set] = None,
    check_exists: Optional[Callable] = None,
    limit: int = DEFAULT_LIMIT,
    progress_cb: Optional[Callable] = None,
) -> dict:
    """
    爬取單一 city × 多個 district 的物件。
    使用 591 BFF JSON API 取代 Playwright browser 翻頁（browser 在這已失效，回傳都是第一頁）。
    - 按刊登時間新→舊排序
    - 遇到已存在 source_id 就停止
    - 首次執行上限 MAX_NEW_FIRST_RUN 筆
    """
    new_items = []
    price_updates = []
    page_size = 30
    stop = False
    consecutive_complete = 0  # 連續多少筆是完整舊資料

    try:
        for first_row in range(0, page_size * 82, page_size):  # 591 最多 82 頁
            items = _fetch_listing_page_api(region_id, section_id, type_code, first_row, city, target_districts, progress_cb=progress_cb)
            if not items:
                logger.debug(f"  {district}/{building_type} firstRow={first_row}: 0 筆，停止")
                break

            for item in items:
                src_id = item.get("source_id")
                if not src_id:
                    continue

                existing = check_exists(src_id) if check_exists else None

                if existing is not None:
                    # 已存在 → 三種子狀態
                    from database.models import is_record_complete

                    # 1. 價格變動偵測
                    new_price = item.get("price_ntd")
                    old_price = existing.get("price_ntd")
                    if new_price and old_price and abs(new_price - old_price) > 10000:
                        price_updates.append({
                            "source_id": src_id,
                            "old_price": old_price,
                            "new_price": new_price,
                            "new_price_per_ping": item.get("price_per_ping"),
                            "title": item.get("title", ""),
                            "district": district,
                        })
                        logger.info(
                            f"  價格變動：{src_id} "
                            f"{int(old_price//10000)}萬→{int(new_price//10000)}萬"
                        )

                    # ★ card 變動偵測：建坪 / 樓層 / 價格 / 路名 任一不同 → 強制重抓 pipeline
                    # 這擺在 consecutive_complete 判斷前，完整舊資料也會被重抓（因為 card 變了就可能 ID 重用）
                    change_reason = _card_fields_changed(item, existing)
                    if change_reason:
                        item["_force_reanalyze"] = True
                        item["_existing_doc"] = existing
                        item["_change_reason"] = change_reason
                        new_items.append(item)
                        consecutive_complete = 0
                        if progress_cb:
                            progress_cb(
                                f"  🔄 {district} card 變動重抓（{change_reason}）：{(item.get('title') or '')[:25]}"
                            )
                        continue

                    # 「已分析過且在 3 天內」OR「最近 enrich 過且在 3 天內」視為已處理：
                    # 即使 is_record_complete=False（591 card 本來就沒地坪，怎麼 enrich 都補不到），
                    # 也算累進 consecutive_complete，讓 batch 正常停在已追上進度的位置。
                    # 同時避免「永遠缺欄位」物件每次 batch 都觸發 enrich → 13 筆無限重複的 bug。
                    _recently_analyzed = False
                    from datetime import datetime as _dt, timezone as _tz
                    for _ts_field in ("analysis_completed_at", "last_enrich_attempt_at"):
                        _ts_iso = existing.get(_ts_field)
                        if not _ts_iso:
                            continue
                        try:
                            _ts = _dt.fromisoformat(_ts_iso.replace("Z", "+00:00"))
                            if _ts.tzinfo is None:
                                _ts = _ts.replace(tzinfo=_tz.utc)
                            _age_hr = (_dt.now(_tz.utc) - _ts).total_seconds() / 3600
                            if _age_hr < 72:
                                _recently_analyzed = True
                                break
                        except Exception:
                            pass

                    # 2. 資料完整 或 最近分析過 → 累積「連續完整舊資料」計數
                    if is_record_complete(existing) or _recently_analyzed:
                        consecutive_complete += 1
                        if consecutive_complete >= 5:
                            # 連續 5 筆都完整/近分析 → 表示已追上最新進度
                            stop = True
                            break
                        continue

                    # 3. 資料不完整 → 標記為要做 enrichment
                    item["_enrich_existing"] = True
                    item["_existing_doc"] = existing
                    new_items.append(item)
                    consecutive_complete = 0
                    if progress_cb:
                        progress_cb(f"  ↻ {district} 補資料：{(item.get('title') or '')[:25]}")
                    continue

                # 全新物件
                consecutive_complete = 0
                new_items.append(item)
                if progress_cb:
                    btype_shown = item.get("building_type") or "?"
                    progress_cb(
                        f"  ✓ {district} [{btype_shown}] 第 {len(new_items)} 筆："
                        f"{(item.get('title') or '')[:25]}"
                    )

                if len(new_items) >= limit:
                    stop = True
                    break

            logger.debug(
                f"  {district}/{building_type} firstRow={first_row}: "
                f"{len(items)} 筆，累積新增 {len(new_items)} 筆"
            )

            if stop:
                break

    except Exception as e:
        logger.error(f"_scrape_district error ({city}{district}/{building_type}): {e}")

    return {"new": new_items, "price_updates": price_updates}


# ── 頁面解析 ──────────────────────────────────────────────────────────────────

def _parse_listing_page(page: Page, city: str, district: str, building_type: str, target_districts: Optional[set] = None) -> list[dict]:
    """解析搜尋結果頁面上的所有物件卡片。
    只抓「主列表」卡片，排除：
      - 側邊「猜你喜歡 guess-you-like」推薦區
      - 置頂/特殊「ware-item--visible」標記卡
      - 其他廣告推薦區塊
    """
    items = []

    try:
        page.wait_for_selector("a[href*='/house/']", timeout=15000)
    except Exception:
        logger.debug(f"No /house/ links found after 15s ({city}{district})")
        try:
            page.screenshot(path=f"debug_empty_{city}_{district}.png")
        except Exception:
            pass
        return items

    # 精準抓主列表：.ware-item（排除 ware-item--visible 變體 + 排除 guess-you-like 祖先）
    # DOM 結構：.ware-item > .ware-item__content > .ware-item__header > <a>
    # 用 evaluate 帶 closest() 檢查，Playwright CSS selector 無法表達「祖先非 X」
    card_handles = page.evaluate_handle(r"""() => {
        const all = Array.from(document.querySelectorAll('.ware-item a[href*="/house/"]'));
        return all.filter(a => {
            // 排除 guess-you-like 推薦區
            if (a.closest('.guess-you-like, .recommend-right-item-wrap')) return false;
            // 排除特殊標記卡（ware-item--visible 通常是置頂/廣告）
            const card = a.closest('.ware-item');
            if (card && card.classList.contains('ware-item--visible')) return false;
            return true;
        });
    }""")
    try:
        props = card_handles.get_properties()
        cards = [h.as_element() for _, h in props.items() if h.as_element()]
    except Exception:
        cards = []

    if not cards:
        # fallback（少見，保底抓法）
        logger.warning(f"主列表 .ware-item selector 抓不到，fallback 到寬 selector ({city}{district})")
        cards = page.query_selector_all("a[href*='/house/']")

    # 去重（同一張卡片內多個 <a> 指向同一 URL）
    seen_hrefs = set()
    unique_cards = []
    for card in cards:
        try:
            href = card.get_attribute("href") or ""
        except Exception:
            continue
        if href and href not in seen_hrefs:
            seen_hrefs.add(href)
            unique_cards.append(card)

    if not unique_cards:
        logger.warning(f"No listing cards found: {city} {district}")
        return items

    for card in unique_cards:
        try:
            item = _parse_card(card, page, city, district, building_type, target_districts)
            if item:
                items.append(item)
        except Exception as e:
            logger.debug(f"Card parse error: {e}")

    return items


def _parse_card(card, page: Page, city: str, district: str, building_type: str, target_districts: Optional[set] = None) -> Optional[dict]:
    """
    解析單一物件卡片。
    card 是 <a href="/house/..."> 元素，
    用 evaluate() 取得父層 <li> 的完整 innerText。
    """
    try:
        href = card.get_attribute("href") or ""
        if not href.startswith("http"):
            href = BASE_URL + href

        source_id_match = re.search(r"/(\d{6,})", href)
        if not source_id_match:
            return None
        source_id = source_id_match.group(1)

        title = (card.inner_text() or "").strip()[:100]
        if not title:
            title = card.get_attribute("title") or ""

        # 往上找合適的「卡片容器」取完整文字
        # 策略：找容器同時滿足 (a) 含「萬」或「坪」(b) 不太大 (<1000字)
        info_text = card.evaluate("""el => {
            // 優先精準 .ware-item，否則找 li/article 等容器
            const selectors = ['.ware-item', '[class~="ware-item"]',
                               'li', 'article', '[class*="house-list"]',
                               '[class*="listing-item"]', '[class*="list-item"]'];
            for (const sel of selectors) {
                const node = el.closest(sel);
                if (node && node.innerText && node.innerText.length < 1000
                    && /萬|坪/.test(node.innerText)) {
                    return node.innerText;
                }
            }
            // 往上爬 6 層找含「萬」或「坪」的合理大小容器
            let node = el.parentElement;
            for (let i = 0; i < 6 && node; i++) {
                const t = node.innerText || '';
                if (t.length > 50 && t.length < 1000 && /萬|坪/.test(t)) {
                    return t;
                }
                node = node.parentElement;
            }
            return el.innerText || '';
        }""") or ""
        info_text = info_text.strip()

        # Debug: 印出前幾張的 info_text 以便 verify（stripped 後 < 20 字表示抓得太少）
        if len(info_text) < 30:
            logger.warning(
                f"info_text 過短！source={source_id}, 長度={len(info_text)}, 內容={info_text!r}"
            )

        detected_type = _detect_building_type(info_text)

        area_ping = None
        m = re.search(r"權狀\s*([\d.]+)\s*坪", info_text)
        if m:
            area_ping = float(m.group(1))
        else:
            area_ping = _parse_area(info_text)

        floor_str, total_floors = _parse_floors(info_text)
        age = _parse_age(info_text)

        # 售價：嚴格只從 591 標準 .ware-item__price-value class 抓
        # 不從 info_text 抓避免抓到貸款預估、頭期款、廣告金額等
        price = None
        try:
            price_text = card.evaluate(r"""el => {
                // 用精準 class 名找 .ware-item（不能用 [class*="ware-item"]
                // 因為會誤中 .ware-item__media 之類的子元素）
                const card = el.closest('.ware-item') ||
                             el.closest('[class~="ware-item"]') ||
                             el.closest('article') ||
                             el.closest('li');
                if (!card) return '';
                const priceEl = card.querySelector('.ware-item__price-value, [class*="price-value"]');
                if (!priceEl || !priceEl.innerText) return '';
                const t = priceEl.innerText.trim();
                if (/^[\d,]+(\.\d+)?$/.test(t)) return t + '萬';
                return '';
            }""") or ""
            if price_text:
                price = _parse_price(price_text)
        except Exception:
            pass

        # 理性檢查：台北/新北房價最低也百萬等級，< 100 萬一定抓錯
        if price is not None and price < 1_000_000:
            logger.debug(f"售價 {price} 不合理，捨棄 ({source_id})")
            price = None

        # 首圖：從卡片容器內抓第一張 <img>，優先 data-src / src
        image_url = None
        try:
            image_url = card.evaluate(r"""el => {
                const card = el.closest('.ware-item') ||
                             el.closest('[class~="ware-item"]') ||
                             el.closest('article') ||
                             el.closest('li');
                if (!card) return '';
                const imgs = card.querySelectorAll('img');
                for (const img of imgs) {
                    const src = img.getAttribute('data-src') ||
                                img.getAttribute('data-original') ||
                                img.getAttribute('src') || '';
                    if (!src || src.startsWith('data:')) continue;
                    if (/\/build\/static\/|\/header\/|\/icon/i.test(src)) continue;
                    return src.startsWith('//') ? 'https:' + src : src;
                }
                return '';
            }""") or None
        except Exception:
            image_url = None

        # 辨識卡片實際所屬行政區（從文字前段抓）
        actual_district = None
        for pat in [
            r"([\u4e00-\u9fa5]{1,3}區)[-－]",   # 「XX區-街名」
            r"([\u4e00-\u9fa5]{1,3}區)\s*\n",   # 「XX區\n」
        ]:
            m = re.search(pat, info_text)
            if m:
                actual_district = m.group(1)
                break

        # 若有目標區清單，且辨識到的區不在清單內，跳過
        if target_districts and actual_district and actual_district not in target_districts:
            logger.debug(f"跳過跨區：實際{actual_district} 不在目標 {target_districts} ({source_id})")
            return None

        # 嘗試抓街道（格式「XX區-街名」或「XX區\n街名」）
        address_text = ""
        addr_m = re.search(
            r"區[-－]\s*\n?([\w\d巷弄路街號段一二三四五六七八九十]+)",
            info_text,
        )
        if addr_m:
            address_text = addr_m.group(1)

        published_text = ""
        pub_m = re.search(
            r"(\d+天前|今天|昨天|剛剛|\d+小時前|\d+分鐘前|\d{4}[/-]\d{1,2}[/-]\d{1,2})",
            info_text,
        )
        if pub_m:
            published_text = pub_m.group(1)

        # 優先用辨識出的實際行政區（若有）
        card_district = actual_district or district
        full_address = f"{city}{card_district}{address_text}" if address_text else f"{city}{card_district}"

        return {
            "source": "591",
            "source_id": f"591_{source_id}",
            "url": href,
            "title": title,
            "city": city,
            "district": card_district,
            "address": full_address,
            "building_type": detected_type or building_type,
            "total_floors": total_floors,
            "floor": floor_str,
            "building_age": age,
            "building_age_completed_year": __import__("database.models", fromlist=["age_to_completed_year"]).age_to_completed_year(age),
            "building_age_source": "591_card" if age else None,
            "building_area_ping": area_ping,
            "price_ntd": price,
            "price_per_ping": (price / area_ping) if price and area_ping else None,
            "image_url": image_url,
            "_published_text": published_text,
            "_raw_text": info_text[:500],
        }

    except Exception as e:
        logger.debug(f"_parse_card error: {e}")
        return None


# ── 詳情頁補充資料 ─────────────────────────────────────────────────────────────

def screenshot_detail_page(ctx: BrowserContext, url: str, source_id: str):
    """
    開詳情頁截圖 + 讀社區地址 + 座標 + 刊登/更新時間。
    回傳 dict：{path, community_addr, page_coords, published_text, updated_text}
    （保留 tuple 相容：callers 可 unpack 頭三個）
    """
    from config import SCREENSHOTS_DIR
    SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    page = ctx.new_page()
    try:
        # 設寬 viewport 確保數字欄位完整渲染
        page.set_viewport_size({"width": 1920, "height": 1080})
        # 重試邏輯
        loaded = False
        for attempt in range(1, 4):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=25000)
                loaded = True
                break
            except Exception:
                logger.warning(f"  詳情頁連線失敗 (第{attempt}次) {source_id}")
                human_delay(3.0, 5.0)
        if not loaded:
            return None
        human_delay(2.0, 3.5)  # 多等一下確保 CSS 渲染完
        _dismiss_login_popup(page)

        # 591 錯誤頁偵測：物件下架/刪除時回 "對不起，您訪問的頁面不存在"
        # 這在 listing 列表還在快取顯示時常發生（卡片抓得到、點進去卻是 404 頁）
        try:
            _dtitle = (page.title() or "").strip()
            _body_head = page.evaluate("() => (document.body && document.body.innerText || '').slice(0, 300)") or ""
        except Exception:
            _dtitle = ""; _body_head = ""
        if ("不存在" in _dtitle) or ("您查詢的物件不存在" in _body_head) or ("已關閉或者被刪除" in _body_head) or ("您訪問的頁面不存在" in _body_head):
            logger.warning(f"591 物件已下架/刪除 ({source_id}): title={_dtitle!r}")
            class _DelistedResult(tuple):
                def __new__(cls):
                    inst = super().__new__(cls, (None, "__DELISTED__", None))
                    inst.delisted = True
                    inst.published_text = None
                    inst.updated_text = None
                    inst.addr_path = None
                    inst.house_path = None
                    return inst
            return _DelistedResult()

        path = SCREENSHOTS_DIR / f"{source_id.replace('/', '_')}_detail.png"

        # 策略：用 full_page=True 抓整頁，再裁掉底部廣告/相關推薦。
        # 591 詳情頁的重要欄位分佈（CSS px）：
        #   0~1080：標題/相簿/價格
        #   1080~2400：物件基本資料 table（屋齡、樓層、總樓、坪數、地址）
        #   2400~3800：房屋介紹 table（土地坪數、使用分區、管理費）  ← 缺這塊就讀不到土地
        # 注意：page.screenshot(clip=...) 不會自動延伸 viewport，需 full_page 才能截到下方內容。
        page.evaluate("window.scrollTo(0, 0)")
        human_delay(0.3, 0.6)
        # 先全頁滾一次觸發 lazy 渲染（591 詳情頁有些區塊在進入視口才載入）
        page.evaluate(r"""async () => {
            const total = document.body.scrollHeight;
            for (let y = 0; y < Math.min(total, 4500); y += 800) {
                window.scrollTo(0, y);
                await new Promise(r => setTimeout(r, 200));
            }
            window.scrollTo(0, 0);
        }""")
        # 等到房屋介紹 table 的關鍵欄位確實渲染出來（lazy）——否則截到空白 table
        try:
            page.wait_for_function(
                r"""() => {
                    const t = document.body.innerText || '';
                    return t.includes('土地坪數') || t.includes('使用分區') || t.includes('管理費');
                }""",
                timeout=10000
            )
        except Exception:
            logger.debug(f"  房屋介紹 lazy render timeout ({source_id}) — 繼續")
        human_delay(0.7, 1.2)
        full_path = SCREENSHOTS_DIR / f"{source_id.replace('/', '_')}_detail_full.png"
        page.screenshot(path=str(full_path), full_page=True)

        # 用 DOM 動態定位「物件基本資料」與「房屋介紹」兩個 section 的 y 座標（CSS px）。
        # 591 listing 因照片數量不同，table 起始 y 會浮動 → 固定切片會失準。
        # 同時抓中央內容區的 x 範圍（591 在 1920 viewport 下，主內容欄約 400-1400，左右兩側留白）。
        section_ys = page.evaluate(r"""() => {
            function findByText(...keywords) {
                const all = document.querySelectorAll('h2, h3, h4, .info-title, [class*="title"], [class*="info"] dt, [class*="info"] [class*="head"]');
                for (const el of all) {
                    const t = (el.innerText || '').trim();
                    if (keywords.some(k => t.includes(k))) {
                        // 找包住 title 的有意義寬度容器（往上走找 width > 600）
                        let p = el, bestR = el.getBoundingClientRect();
                        for (let i = 0; i < 6 && p; i++) {
                            const r = p.getBoundingClientRect();
                            if (r.width > 600 && r.width < 1400) {
                                bestR = r;
                                break;
                            }
                            p = p.parentElement;
                        }
                        return {
                            y: bestR.top + window.scrollY,
                            h: bestR.height,
                            x: bestR.left + window.scrollX,
                            w: bestR.width,
                        };
                    }
                }
                return null;
            }
            return {
                basic: findByText('物件基本資料', '基本資料'),
                house: findByText('房屋介紹', '房屋資訊', '房屋'),
            };
        }""")

        addr_path = SCREENSHOTS_DIR / f"{source_id.replace('/', '_')}_addr.png"
        house_path = SCREENSHOTS_DIR / f"{source_id.replace('/', '_')}_house.png"
        try:
            from PIL import Image
            im = Image.open(full_path)
            DPR = 2
            page_h_actual = im.height

            basic = section_ys.get("basic") if isinstance(section_ys, dict) else None
            house = section_ys.get("house") if isinstance(section_ys, dict) else None

            # 房屋介紹切片（含土地坪數/使用分區）：以 DOM y 為起點，向下取 1500 CSS px
            # 縮小切片高度（1800→1500）以避免 PNG 超過 Claude Vision 5MB base64 限制
            if house and house.get("y") is not None:
                h_top = max(0, int(house["y"] - 30) * DPR)
                h_bot = min(page_h_actual, int(house["y"] + 1500) * DPR)
                if h_bot > h_top:
                    im.crop((0, h_top, im.width, h_bot)).save(house_path)

            # 物件基本資料切片（含地址）：
            # 優先用 basic DOM y；若 basic 找不到但 house 找到了，「基本資料」必在 house 上方 →
            # 從 (house_y - 1500 CSS) 到 house_y 切一段（通常剛好涵蓋地址/屋齡/樓層/坪數表格）
            if basic and basic.get("y") is not None:
                a_top = max(0, int(basic["y"] - 30) * DPR)
                a_bot = min(page_h_actual, int(basic["y"] + 1400) * DPR)
                if a_bot > a_top:
                    im.crop((0, a_top, im.width, a_bot)).save(addr_path)
            elif house and house.get("y") is not None and house["y"] > 400:
                # fallback：基本資料必在房屋介紹上方
                a_top = max(0, int(house["y"] - 1500) * DPR)
                a_bot = int(house["y"]) * DPR
                if a_bot > a_top:
                    im.crop((0, a_top, im.width, a_bot)).save(addr_path)
            else:
                # 最 fallback：section DOM 完全找不到 → 給全頁 y=500~2500 CSS px 區間
                # （通常基本資料表格在這個範圍內，避開最上面 header + gallery）
                a_top = 500 * DPR
                a_bot = min(page_h_actual, 2500 * DPR)
                if a_bot > a_top:
                    im.crop((0, a_top, im.width, a_bot)).save(addr_path)

            # 合併「基本資料 + 房屋介紹」成 shot_path（供 tile OCR 用）：
            # - 垂直：basic.y-30 (或 house.y-30 若無 basic) 到 house.y+1000
            #   新版 591 很多頁面只有「房屋介紹」section，所有結構化欄位都在裡面
            # - 水平：以內容區 x/w 為中央帶（1920 viewport 兩側各 ~400px 留白可切掉）
            # x 範圍：取 basic、house 的最左 / 最右，多加 20px padding
            sections = [s for s in (basic, house) if s and s.get("x") is not None]
            x_lefts = [s.get("x") for s in sections]
            x_rights = [s.get("x", 0) + s.get("w", 0) for s in sections]
            # top 優先用 basic；沒有 basic 時需往上涵蓋價格/屋況卡（屋齡、樓層、建坪、價格都在這裡）
            # 依經驗價格卡大約在 house.y 上方 800-900 CSS，取 max(200, house.y - 900) 避開最上頭 header
            bot = (house or {}).get("y")
            if basic:
                top = basic.get("y")
            elif house:
                top = max(200, (house.get("y") or 0) - 900)
            else:
                top = None
            if top is not None and bot is not None:
                t = max(0, int(top - 30) * DPR)
                b = min(page_h_actual, int(bot + 1000) * DPR)
                if x_lefts and x_rights:
                    cx_left = max(0, int(min(x_lefts) - 20) * DPR)
                    cx_right = min(im.width, int(max(x_rights) + 20) * DPR)
                else:
                    cx_left, cx_right = 0, im.width
                im.crop((cx_left, t, cx_right, b)).save(path)
                logger.info(f"  shot_path crop x=[{cx_left}-{cx_right}] y=[{t}-{b}] basic={basic is not None} ({source_id})")
            else:
                # 完全找不到 section → 退回固定 y 截至 4500
                im.crop((0, 0, im.width, min(page_h_actual, 4500 * DPR))).save(path)

            full_path.unlink(missing_ok=True)
            logger.info(
                f"  截圖切片 sections (CSS y): basic={(basic or {}).get('y')}, house={(house or {}).get('y')}"
            )
        except Exception as _e:
            logger.warning(f"  截圖裁切失敗，沿用原圖: {_e}")
            full_path.replace(path)
        # 從 DOM 盡可能多抓地址候選（純文字，不受 CSS 防爬影響；比 OCR 可靠很多）
        detail_data = page.evaluate(r"""() => {
            const results = { addr_candidates: [], lat: null, lng: null };

            // 規則：地址只從「社區」或「地址」這兩個明確 label 右邊的值抓。
            // 其他地方（實價登錄 banner、熱門社區推薦、仲介介紹、title h1 等）都視為不可信。
            // 搜尋頁面上所有短文字節點，若內文精確等於「社區」或「地址」→ 取它「結構上的兄弟值」。
            const LABEL_TOKENS = ['社區', '地址', '物件地址', '門牌'];
            const candidates = document.querySelectorAll(
                'dt, dd, span, div, td, li, p, label, [class*="label"], [class*="item"]'
            );
            candidates.forEach(lbl => {
                const text = (lbl.innerText || '').trim();
                if (!LABEL_TOKENS.includes(text)) return;
                // 取 value 的幾個策略（依序嘗試）：
                //   a. nextElementSibling
                //   b. 父元素內最後一個非 label 的 child
                //   c. 父元素的 nextElementSibling（table row 的下一 td）
                let val = '';
                if (lbl.nextElementSibling) {
                    val = (lbl.nextElementSibling.innerText || '').trim();
                }
                if (!val && lbl.parentElement) {
                    const siblings = Array.from(lbl.parentElement.children)
                        .filter(c => c !== lbl && (c.innerText || '').trim());
                    if (siblings.length) val = (siblings[siblings.length - 1].innerText || '').trim();
                }
                if (!val && lbl.parentElement && lbl.parentElement.nextElementSibling) {
                    val = (lbl.parentElement.nextElementSibling.innerText || '').trim();
                }
                // 最少要 5 字 + 含路/街/號/巷 才算地址（擋掉像空字串、短短幾字的誤值）
                // 擋掉雜訊：仲介名/瀏覽數/更新時間 被串進來的情況
                const NOISE = /(仲介|代理|經紀|瀏覽|更新|刊登|小時前|分鐘前|天前|人看過|\d+人)/;
                if (val && val.length >= 5 && val.length <= 40
                    && /[路街道巷弄號段]/.test(val)
                    && !NOISE.test(val)) {
                    results.addr_candidates.push({ src: `label-${text}`, text: val });
                }
            });

            // (6) 591 原生座標
            document.querySelectorAll('script').forEach(s => {
                const m = (s.textContent || '').match(/rsMapIframe\?lat=([\d.]+)&lng=([\d.]+)/);
                if (m && !results.lat) { results.lat = parseFloat(m[1]); results.lng = parseFloat(m[2]); }
            });

            // (7) 刊登時間 / 最後更新
            //   591 詳情頁常出現「刊登時間」「刊登日期」「更新時間」「最後更新」等標籤
            //   我們抓整頁 label=內容 pair，再用 regex 找
            results.published_text = null;
            results.updated_text = null;
            const bodyText = document.body.innerText || '';
            // 刊登
            let m = bodyText.match(/(?:刊登(?:時間|日期)|上架時間)[\s:：]*([^\n]{1,30})/);
            if (m) results.published_text = m[1].trim();
            // 更新
            m = bodyText.match(/(?:最後更新|更新(?:時間|日期))[\s:：]*([^\n]{1,30})/);
            if (m) results.updated_text = m[1].trim();
            // 也嘗試 JSON-LD 的 datePosted / dateModified
            document.querySelectorAll('script[type="application/ld+json"]').forEach(s => {
                try {
                    const j = JSON.parse(s.textContent || '{}');
                    const root = j['@graph'] ? j['@graph'][0] : j;
                    if (!results.published_text && root.datePosted) results.published_text = root.datePosted;
                    if (!results.updated_text && root.dateModified) results.updated_text = root.dateModified;
                } catch (e) {}
            });
            return results;
        }""")
        addr_candidates = detail_data.get("addr_candidates") or []
        # 優先序：社區 label（通常含完整號之 X）> 地址 label（可能只到巷）
        # 同時要求含「號」最優，無則含「巷/弄」
        def _pick(src_filter, need_num=True):
            for c in addr_candidates:
                if not c.get("text"): continue
                if src_filter and not any(s in c.get("src", "") for s in src_filter): continue
                if need_num and "號" not in c["text"]: continue
                return c["text"]
            return None
        community_addr = (
            _pick(["社區"], need_num=True)          # 社區 + 含號
            or _pick(["社區"], need_num=False)      # 社區 + 僅到巷
            or _pick(["地址", "門牌"], need_num=True)   # 地址 + 含號
            or _pick(["地址", "門牌"], need_num=False)  # 地址 + 僅到巷
        )

        page_coords = None
        if detail_data.get("lat") and detail_data.get("lng"):
            page_coords = (detail_data["lat"], detail_data["lng"])
        if addr_candidates:
            logger.info(f"  DOM 地址候選 ({source_id})：{[(c.get('src'), (c.get('text') or '')[:40]) for c in addr_candidates]}")
        published_text = detail_data.get("published_text")
        updated_text = detail_data.get("updated_text")
        if published_text or updated_text:
            logger.info(f"  DOM 日期 ({source_id}): 刊登={published_text!r} 更新={updated_text!r}")
        # Backward-compatible tuple return + extra attrs（含新切的 addr_path / house_path）
        class _DetailResult(tuple):
            def __new__(cls, path, addr, coords, pub, upd, addr_p, house_p):
                inst = super().__new__(cls, (path, addr, coords))
                inst.published_text = pub
                inst.updated_text = upd
                inst.addr_path = addr_p
                inst.house_path = house_p
                return inst
        return _DetailResult(
            str(path), community_addr, page_coords,
            published_text, updated_text,
            str(addr_path) if addr_path.exists() else None,
            str(house_path) if house_path.exists() else None,
        )
    except Exception as e:
        logger.debug(f"Screenshot detail page failed ({url}): {e}")
        return None, None, None
    finally:
        page.close()


def fetch_listing_detail(ctx: BrowserContext, url: str) -> dict:
    """
    進入物件詳情頁抓取卡片沒有的資料。
    嚴格規則：只從確定的 DOM 元素抓取，不用自由文字 regex 避免誤抓其他物件的地址。
    抓不到就回傳空，絕不猜測。
    """
    page = ctx.new_page()
    extra = {}
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        human_delay(1.5, 2.5)
        _dismiss_login_popup(page)

        # 地址：591 詳情頁通常有專用的 address 區塊
        # 不抓 body 全文避免誤抓相關推薦等其他物件的地址
        for sel in [
            ".info-addr-content", ".address", "[class*='houseAddr']",
            "[class*='info-addr']", ".detail-info-row .address",
        ]:
            try:
                el = page.query_selector(sel)
                if el:
                    addr = (el.inner_text() or "").strip()
                    # 驗證格式看起來像台灣地址
                    if addr and len(addr) >= 5 and "區" in addr:
                        extra["address"] = addr
                        break
            except Exception:
                pass

        # 從詳情頁「表格結構」裡抓欄位，不從全文 regex
        # 591 詳情頁通常有 dt/dd 或 label/value 對應結構
        info_rows = page.query_selector_all(
            ".detail-info-row, .info-item, [class*='info'] dt, [class*='info'] dd"
        )
        info_text_limited = ""
        for row in info_rows[:50]:  # 最多讀 50 個資訊列，不讀整頁
            try:
                t = (row.inner_text() or "").strip()
                if t:
                    info_text_limited += t + "\n"
            except Exception:
                pass

        # 土地坪數（從限定範圍內抓）
        land_m = re.search(r"土地[^\n]{0,10}?([\d.]+)\s*坪", info_text_limited)
        if land_m:
            try:
                extra["land_area_ping"] = float(land_m.group(1))
            except ValueError:
                pass

        # 屋齡（從限定範圍內抓）
        age_m = re.search(r"屋齡[：:\s]*(\d+)\s*年", info_text_limited)
        if age_m:
            from database.models import age_to_completed_year as _atc
            _age = int(age_m.group(1))
            extra["building_age"] = _age
            extra["building_age_completed_year"] = _atc(_age)
            extra["building_age_source"] = "591_detail"

    except Exception as e:
        logger.debug(f"Detail fetch error for {url}: {e}")
    finally:
        page.close()
    return extra


# ── 工具 ──────────────────────────────────────────────────────────────────────

def _dismiss_login_popup(page: Page) -> None:
    for sel in [".close", ".btn-close", "[class*='close']",
                "[class*='modal'] button", ".popup-close"]:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.click()
                human_delay(0.5, 1.0)
                return
        except Exception:
            pass
