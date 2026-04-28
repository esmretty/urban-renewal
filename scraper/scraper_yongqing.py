"""永慶房屋爬蟲。

跟 591 不同：
- 列表頁純 HTTP（BeautifulSoup parse SSR HTML）
- 詳情頁需要 Playwright（座標在 JS render 完才出現的 google maps 連結裡）
- 詳情頁有完整土地坪、使用分區、建物分項、社區名（591 都沒有或要 OCR）
- 但地址只到路段（沒巷弄門牌）

公開介面：
    scrape_yongqing(headless, progress_callback, districts_filter, check_exists, limit)
        → {"new": [...新物件 item dict...], "price_updates": [...]}

每個 item dict 標準格式跟 591 一致（make_property_doc 能直接吃）。
"""
from __future__ import annotations

import logging
import re
import time
import urllib.parse
from typing import Callable, Optional

import requests
import urllib3
from bs4 import BeautifulSoup

# 永慶證書在某些 Python 版本下會驗證失敗（Missing Subject Key Identifier）。
# 我們只讀公開資料、不傳憑證，可接受不驗證。關掉 InsecureRequestWarning 避免 log 噪音。
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config import SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX
from database.time_utils import now_tw_iso

logger = logging.getLogger(__name__)

# 永慶置頂廣告位（每個列表頁第一筆都是這個 ID）
SKIP_LISTING_FIRST_IDS = {"4308114"}

DEFAULT_LIMIT = 30
DEFAULT_PAGE_SIZE = 25

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "zh-TW,zh;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

# 永慶屋型 → 我們系統用的 building_type
TYPE_MAP = {
    "無電梯公寓": "公寓",
    "電梯大廈": "電梯大樓",
    "華廈": "華廈",
    "透天": "透天厝",
    "店面": "店面",
}

LAST_FETCH_ERROR: Optional[str] = None


def _human_sleep():
    import random
    time.sleep(random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX))


# ── 列表頁 ──────────────────────────────────────────────────────────────────

def _build_list_url(districts: list[str], building_types: list[str], page: int = 1) -> str:
    """組永慶列表頁 URL。
    districts 例：['台北市-大安區', '台北市-信義區']
    building_types 例：['無電梯公寓']  (永慶用語)
    """
    dist_csv = ",".join(districts)
    type_csv = ",".join(building_types)
    return (
        "https://buy.yungching.com.tw/list/"
        f"{urllib.parse.quote(dist_csv)}_c/"
        f"{urllib.parse.quote(type_csv)}_type"
        f"?od=80&pg={page}"   # od=80 = 最新刊登
    )


_LAST_403_AT = 0.0   # 上次遭 403 的時間 (epoch sec)，用於 cooldown


def _fetch(url: str, retries: int = 3) -> Optional[str]:
    """打永慶 HTTP，回 HTML text 或 None。
    永慶會 rate-limit (403/429)，遇到時 backoff 久一點。"""
    global LAST_FETCH_ERROR, _LAST_403_AT
    # 若不久前才被 403，先 cooldown
    since_403 = time.time() - _LAST_403_AT
    if _LAST_403_AT and since_403 < 30:
        wait = 30 - since_403
        logger.info(f"永慶 cooldown {wait:.1f}s（剛被 403，先休息再打）")
        time.sleep(wait)
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=DEFAULT_HEADERS, timeout=20, verify=False)
            if r.status_code == 200:
                return r.text
            if r.status_code in (403, 429):
                _LAST_403_AT = time.time()
                wait = 15 + attempt * 15   # 15s, 30s, 45s
                logger.warning(f"永慶 {r.status_code} rate limit，等 {wait}s 後重試 {url[:80]}")
                time.sleep(wait)
                continue
            LAST_FETCH_ERROR = f"HTTP {r.status_code}"
            logger.warning(f"永慶 fetch fail {r.status_code} {url[:80]}")
            return None
        except Exception as e:
            LAST_FETCH_ERROR = str(e)[:200]
            logger.warning(f"永慶 fetch exception {e} attempt={attempt+1}/{retries}")
            time.sleep(3)
    return None


def _parse_listing_card_OLD_UNUSED(card_a, listing_first_seen: bool = False) -> Optional[dict]:
    """從一個列表卡片的 <a href="/house/XXXX"> 元素解析出 item dict。
    回傳 None 表示這張卡片不要（廣告位、解析失敗等）。"""
    href = card_a.get("href") or ""
    m = re.search(r"/house/(\d{6,8})", href)
    if not m:
        return None
    house_id = m.group(1)
    if listing_first_seen and house_id in SKIP_LISTING_FIRST_IDS:
        return None   # 永慶置頂廣告位

    # 從卡片內文字抓出價格、坪數、屋齡、樓層、地址
    text = card_a.get_text(" ", strip=True)
    if not text:
        return None

    # 價格：例「2,500萬」or「12,800 萬」
    price_m = re.search(r"([\d,]+)\s*萬", text)
    price_ntd = None
    if price_m:
        try:
            price_ntd = int(price_m.group(1).replace(",", "")) * 10000
        except ValueError:
            pass

    # 建坪
    area_m = re.search(r"建坪[\s:：]*([\d.]+)", text)
    if not area_m:
        area_m = re.search(r"([\d.]+)\s*坪", text)   # fallback
    building_area_ping = float(area_m.group(1)) if area_m else None

    # 屋齡：「15.2年」「30年」「未滿1年」
    age_m = re.search(r"(?:屋齡[\s:：]*)?([\d.]+)\s*年", text)
    building_age = float(age_m.group(1)) if age_m else None

    # 樓層：「5/14樓」「1/4樓」
    floor_m = re.search(r"(\d+)\s*/\s*(\d+)\s*樓", text)
    floor = floor_m.group(1) if floor_m else None
    total_floors = int(floor_m.group(2)) if floor_m else None

    # 地址：列表頁卡片通常會顯示「台北市大安區安和路二段」
    addr_m = re.search(r"((?:台北市|新北市)[^\d\s]+(?:路|街|大道)[^\d\s]*(?:[一二三四五六七八九十]段)?)", text)
    address = addr_m.group(1) if addr_m else None

    # 屋型：列表頁卡片可能有「住宅大樓」「公寓」字樣（不一定）
    building_type = None
    for kw in ["無電梯公寓", "電梯大廈", "電梯大樓", "華廈", "公寓", "透天", "店面"]:
        if kw in text:
            building_type = TYPE_MAP.get(kw, kw)
            break

    # 封面圖（卡片內有 <img>）
    img = card_a.find("img")
    image_url = img.get("src") if img else None

    # 城市/區從地址抽出
    city = None
    district = None
    if address:
        city_m = re.match(r"^(台北市|新北市)", address)
        if city_m:
            city = city_m.group(1)
        dist_m = re.search(r"(?:台北市|新北市)([^\d]+?區)", address)
        if dist_m:
            district = dist_m.group(1)

    if not (price_ntd and address):
        return None   # 缺核心欄位

    src_id = f"yongqing_{house_id}"
    detail_url = f"https://buy.yungching.com.tw/house/{house_id}"

    return {
        "source": "永慶",
        "source_id": src_id,
        "url": detail_url,
        "title": (text[:80]),
        "city": city,
        "district": district,
        "address": address,
        "building_type": building_type,
        "total_floors": total_floors,
        "floor": floor,
        "building_age": building_age,
        "building_age_completed_year": (
            __import__("database.models", fromlist=["age_to_completed_year"]).age_to_completed_year(building_age)
        ),
        "building_age_source": "yongqing_card" if building_age else None,
        "building_area_ping": building_area_ping,
        "price_ntd": price_ntd,
        "price_per_ping": (price_ntd / building_area_ping) if (price_ntd and building_area_ping) else None,
        "image_url": image_url,
        "_yongqing_house_id": house_id,
    }


def _parse_listing_page(html: str) -> list[dict]:
    """從列表頁的 JSON-LD ItemList 拿物件 ID 列表。
    永慶用 Angular SPA，卡片詳細欄位（價格/坪數）不在 SSR HTML 裡（Angular custom element 是空的），
    所以列表頁只能拿到 ID + name + image，其他靠詳情頁 enrich。"""
    import json as _json
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict] = []
    seen: set = set()
    for sc in soup.find_all("script", type="application/ld+json"):
        try:
            data = _json.loads(sc.string or "")
        except Exception:
            continue
        if not (isinstance(data, dict) and data.get("@type") == "ItemList"):
            continue
        for el in data.get("itemListElement", []):
            url = el.get("url") or ""
            m = re.search(r"/house/(\d{6,8})", url)
            if not m:
                continue
            house_id = m.group(1)
            if house_id in SKIP_LISTING_FIRST_IDS:
                continue
            if house_id in seen:
                continue
            seen.add(house_id)
            items.append({
                "source": "永慶",
                "source_id": f"yongqing_{house_id}",
                "url": f"https://buy.yungching.com.tw/house/{house_id}",
                "title": el.get("name") or "",
                "image_url": el.get("image"),
                "building_type": "公寓",   # 第一階段只抓無電梯公寓 → 對應公寓
                "_yongqing_house_id": house_id,
                "_listing_position": el.get("position"),
            })
    return items


# ── 詳情頁（用 Playwright 拿座標 + BeautifulSoup parse 詳細欄位）────────────

def _parse_detail_html(item: dict, detail_html: str) -> None:
    """解析詳情頁 HTML（無論 HTTP 拿到的還是 Playwright render 過的）。
    從 SSR HTML 中 grep 全部欄位（不含座標 — 座標需 Playwright render JS）。
    結果 in-place 寫入 item。"""
    if not detail_html:
        return
    soup = BeautifulSoup(detail_html, "html.parser")
    text = soup.get_text(" ", strip=True)

    # === 從 ld+json (schema.org Product) 抓基本欄位 ===
    import json as _json
    for sc in soup.find_all("script", type="application/ld+json"):
        try:
            data = _json.loads(sc.string or "")
        except Exception:
            continue
        if isinstance(data, dict) and data.get("@type") == "Product":
            if not item.get("title"):
                item["title"] = data.get("name")
            img = data.get("image")
            if img and not item.get("image_url"):
                item["image_url"] = img if isinstance(img, str) else (img[0] if img else None)
            offers = data.get("offers") or {}
            if offers.get("price"):
                try:
                    item["price_ntd"] = int(offers["price"]) * 10000   # 永慶 price 單位是萬
                except (ValueError, TypeError):
                    pass
            yc_id = data.get("productID")
            if yc_id:
                item["_yongqing_yc_id"] = yc_id
            break

    # === 從文字 grep 詳細欄位 ===

    # 地址（含路段）— 收緊 regex 不讓貪婪吃到「整層住家」這類後綴
    # 格式：城市 + 區 + 路名 + 可選的「X段」結尾
    addr_m = re.search(
        r"((?:台北市|新北市|桃園市|台中市|高雄市)[一-龥]{2,4}區[一-龥]{1,8}(?:路|街|大道)(?:[一二三四五六七八九十]段)?)",
        text,
    )
    if addr_m:
        item["address"] = addr_m.group(1)
        cm = re.match(r"^(台北市|新北市|桃園市|台中市|高雄市)", addr_m.group(1))
        if cm:
            item["city"] = cm.group(1)
        dm = re.search(r"(?:台北市|新北市|桃園市|台中市|高雄市)([一-龥]{2,4}區)", addr_m.group(1))
        if dm:
            item["district"] = dm.group(1)

    # 樓層：「5/14樓」「1/4樓」
    floor_m = re.search(r"(\d+)\s*/\s*(\d+)\s*樓", text)
    if floor_m:
        item["floor"] = floor_m.group(1)
        item["total_floors"] = int(floor_m.group(2))

    # 屋齡
    age_m = re.search(r"屋齡[\s:：]*([\d.]+)\s*年", text)
    if age_m:
        try:
            from database.models import age_to_completed_year as _atc
            _age = float(age_m.group(1))
            item["building_age"] = _age
            item["building_age_completed_year"] = _atc(_age)
            item["building_age_source"] = "yongqing_detail"
        except ValueError:
            pass

    # 建物坪數：對應 591 的「權狀坪」= 永慶的「建物坪數」(含車位)
    # 永慶詳情頁實際呈現：「建物坪數 387.76坪 (含車位 114.22 坪)」
    # 然後分項：主建物 / 共同使用 / 附屬建物
    # 591 卡片 area 欄位 = 建物權狀坪(含車位) → 對齊永慶「建物坪數」
    total_m = re.search(r"建物坪數[\s:：]*([\d.]+)", text)
    if not total_m:
        # 緊湊版「建物387.76坪」也接受
        total_m = re.search(r"建物\s*([\d.]+)\s*坪", text)
    if total_m:
        try:
            item["building_area_ping"] = float(total_m.group(1))
        except ValueError:
            pass

    # 額外存「主建物」當參考欄位（永慶獨家資訊，不取代 building_area_ping）
    main_m = re.search(r"主建物[\s:：]*([\d.]+)", text)
    if main_m:
        try:
            item["building_area_main_ping"] = float(main_m.group(1))
        except ValueError:
            pass

    # 若主流程仍抓不到「建物坪數」，最後 fallback 才用主建物
    if not item.get("building_area_ping") and item.get("building_area_main_ping"):
        item["building_area_ping"] = item["building_area_main_ping"]
        logger.warning(f"yongqing {item.get('source_id')} 抓不到「建物坪數」，用主建物 fallback")

    # 土地坪數
    m = re.search(r"土地坪數[\s:：]*([\d.]+)", text)
    if not m:
        m = re.search(r"土地[\s:：]?\s*([\d.]+)\s*坪", text)
    if m:
        try:
            item["land_area_ping"] = float(m.group(1))
        except ValueError:
            pass

    # 使用分區（過濾含糊回答）
    zoning_m = re.search(
        r"使用分區[\s:：]*([^，,。\s]+(?:住宅區|商業區|工業區|農業區|文教區|風景區|保護區))",
        text,
    )
    if zoning_m:
        zoning_raw = zoning_m.group(1)
        if not any(bad in zoning_raw for bad in ["謄本", "複雜", "不明", "未知"]):
            item["zoning_original"] = zoning_raw

    # 型態（建物類型）：從「型態 X」字樣抓
    # 永慶 type 詞彙：公寓 / 無電梯公寓 / 電梯大廈 / 華廈 / 透天 / 店面 / 套房 等
    type_m = re.search(r"型態\s+([^\s，,。]{1,8})", text)
    if type_m:
        yc_type = type_m.group(1)
        # 對應到我們系統的 building_type 詞彙
        type_map = {
            "公寓": "公寓",
            "無電梯公寓": "公寓",
            "電梯大廈": "電梯大樓",
            "電梯大樓": "電梯大樓",
            "華廈": "華廈",
            "透天": "透天厝",
            "透天厝": "透天厝",
            "店面": "店面",
            "套房": "套房",
        }
        item["building_type"] = type_map.get(yc_type, yc_type)
        item["_yongqing_type_raw"] = yc_type   # 保留原文 debug 用

    # 社區名稱 — 從 BreadcrumbList JSON-LD 拿（不抓自由文字避免誤抓 UI label）
    for sc in soup.find_all("script", type="application/ld+json"):
        try:
            bdata = _json.loads(sc.string or "")
        except Exception:
            continue
        if isinstance(bdata, dict) and bdata.get("@type") == "BreadcrumbList":
            breadcrumbs = bdata.get("itemListElement", [])
            # 社區名稱通常在 breadcrumb 最後一個 (position 5 之後)
            # 例：買屋 / 台北市 / 大安區 / 安和路二段 / 安和名園
            # 取最後一個非「路/街/大道」結尾的就是社區名
            for el in reversed(breadcrumbs):
                name = el.get("name") or ""
                if name and not re.search(r"(?:路|街|大道|區|市)$", name):
                    if name not in (item.get("city") or "", item.get("district") or ""):
                        item["community_name"] = name
                        break
            break

    # 計算單價
    if item.get("price_ntd") and item.get("building_area_ping"):
        item["price_per_ping"] = item["price_ntd"] / item["building_area_ping"]


# ── 兩階段 enrich ─────────────────────────────────────────────────────────────
# Stage 1（快）：HTTP 抓 SSR HTML + parse 全部非座標欄位（~1 秒/筆）
# Stage 2（慢）：Playwright 開頁面 + 滾地圖 + 抓座標（~7-10 秒/筆）
# 主流程先 Stage 1 快速過濾掉不要的（樓高 > 5、enrich fail），
# 通過的物件才跑 Stage 2 拿座標
def _enrich_basic_via_http(item: dict, max_retries: int = 2) -> bool:
    """Stage 1：HTTP 拿 SSR HTML + parse 全部欄位（除座標）。
    回傳 True 表示拿到核心欄位（type / address / price），False 表示失敗。
    失敗時不寫 item。"""
    house_id = item.get("_yongqing_house_id")
    url = item["url"]
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            html = _fetch(url, retries=1)   # _fetch 內部已有自己 retry，外面這層用 max_retries 包
            if html and len(html) > 5000 and '"@type":"Product"' in html:
                _parse_detail_html(item, html)
                if _enrich_did_succeed(item):
                    if attempt > 1:
                        logger.info(f"yongqing HTTP-enrich {house_id} 第 {attempt} 次成功")
                    return True
                last_err = (
                    f"missing core fields after parse: price={item.get('price_ntd')}, "
                    f"addr={item.get('address')}, type_raw={item.get('_yongqing_type_raw')}"
                )
            else:
                last_err = f"HTML 不完整（{len(html or '')} chars）"
            logger.warning(f"yongqing HTTP-enrich {house_id} attempt {attempt}: {last_err}")
        except Exception as e:
            last_err = str(e)[:120]
            logger.warning(f"yongqing HTTP-enrich {house_id} attempt {attempt} exception: {last_err}")
        if attempt < max_retries:
            time.sleep(2.0)
    logger.warning(f"yongqing HTTP-enrich FAILED {house_id} ({max_retries} 次)：{last_err}")
    return False


def _enrich_coords_via_playwright_inner(url: str, headless: bool = True, timeout_sec: int = 18) -> Optional[tuple]:
    """實際跑 Playwright — 給 watchdog 包起來呼叫。失敗回 None。"""
    from playwright.sync_api import sync_playwright
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            try:
                ctx = browser.new_context(
                    user_agent=USER_AGENT,
                    locale="zh-TW",
                    viewport={"width": 1280, "height": 900},
                )
                page = ctx.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_sec * 1000)
                # 滾到地圖區強制 lazy-load
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.5)")
                    page.wait_for_timeout(1500)
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.7)")
                    page.wait_for_timeout(1500)
                except Exception:
                    pass
                gmap_href = page.evaluate(
                    "() => document.querySelector('a[href*=\"google.com/maps\"]')?.href"
                )
                if gmap_href:
                    m = re.search(r"q=(-?\d+\.\d+),(-?\d+\.\d+)", gmap_href)
                    if m:
                        return (float(m.group(1)), float(m.group(2)))
            finally:
                try: browser.close()
                except Exception: pass
    except Exception as e:
        logger.warning(f"yongqing Playwright inner 失敗: {e}")
    return None


def _enrich_coords_via_playwright(item: dict, headless: bool = True, timeout_sec: int = 25) -> bool:
    """Stage 2：Playwright 開頁面拿座標。
    用 ThreadPoolExecutor 加 hard timeout，超時直接放棄、loop 繼續下一筆。
    座標拿不到仍可繼續 ingest（pipeline 會 fallback geocoding）。"""
    import concurrent.futures
    house_id = item.get("_yongqing_house_id")
    url = item["url"]
    HARD_TIMEOUT = timeout_sec + 10   # 內部 18s timeout + 10s buffer = 28s outer

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_enrich_coords_via_playwright_inner, url, headless, timeout_sec)
            try:
                coords = fut.result(timeout=HARD_TIMEOUT)
            except concurrent.futures.TimeoutError:
                logger.warning(
                    f"yongqing Playwright HARD-TIMEOUT {HARD_TIMEOUT}s for {house_id}，"
                    f"放棄座標繼續 ingest（內部 thread 會自然消亡）"
                )
                return False
        if coords:
            item["latitude"] = coords[0]
            item["longitude"] = coords[1]
            item["source_latitude"] = coords[0]
            item["source_longitude"] = coords[1]
            return True
    except Exception as e:
        logger.warning(f"yongqing Playwright outer 失敗 {house_id}: {e}")
    return False


def _enrich_did_succeed(item: dict) -> bool:
    """判定 enrich 是否成功 — 看核心欄位有沒有抓到。"""
    return bool(
        item.get("price_ntd")
        and item.get("address")
        and item.get("_yongqing_type_raw")    # 抓到型態才算詳情頁完整解析
    )


def _enrich_from_detail(item: dict, headless: bool = True, max_retries: int = 2) -> bool:
    """[向後相容] 完整 enrich (HTTP + Playwright)。新主流程已分兩階段，這個給 single-URL 用。
    回傳 True/False 表示是否成功（核心欄位都拿到）。"""
    if not _enrich_basic_via_http(item, max_retries=max_retries):
        return False
    # 座標失敗不算整體失敗（pipeline 會自己 fallback geocoding）
    _enrich_coords_via_playwright(item, headless=headless)
    return True


# ── 公開主流程 ──────────────────────────────────────────────────────────────

def scrape_yongqing(
    headless: bool = True,
    progress_callback: Optional[Callable] = None,
    districts_filter: Optional[list] = None,
    check_exists: Optional[Callable] = None,
    limit: int = DEFAULT_LIMIT,
) -> dict:
    """主入口：跟 scraper_591.scrape_591 同樣的回傳格式。

    districts_filter 例：['大安區', '信義區', '中山區', '中正區', '文山區']
    （不含「台北市」前綴；本函式自動補）

    回傳 {"new": [...], "price_updates": [...]}
    """
    if not progress_callback:
        progress_callback = lambda msg, pct=None, **kw: logger.info(msg)

    if not districts_filter:
        from config import SCHEDULED_SCRAPE_DISTRICTS
        districts_filter = SCHEDULED_SCRAPE_DISTRICTS

    # 永慶 URL 要「{city}-{district}」格式（如「台北市-大安區」、「新北市-板橋區」）
    # 用 TARGET_REGIONS 反查 district → city
    from config import TARGET_REGIONS
    _dist_to_city = {}
    for _city, _v in TARGET_REGIONS.items():
        for _dn in (_v.get("districts") or {}).keys():
            _dist_to_city[_dn] = _city
    def _prefix_city(d: str) -> str:
        # 已含「市-」前綴的不動
        if "-" in d and d.startswith(("台北市", "新北市")):
            return d
        city = _dist_to_city.get(d, "台北市")   # 找不到預設台北
        return f"{city}-{d}"
    full_districts = [_prefix_city(d) for d in districts_filter]
    building_types = ["無電梯公寓"]   # 第一階段只抓公寓

    new_items: list[dict] = []
    price_updates: list[dict] = []
    consecutive_complete = 0
    session_at = now_tw_iso()

    progress_callback(f"開始爬永慶（{len(districts_filter)} 區公寓，最多 {limit} 筆）", 0)

    page_no = 1
    stop = False
    while not stop and len(new_items) < limit and page_no <= 30:
        url = _build_list_url(full_districts, building_types, page=page_no)
        progress_callback(f"  永慶列表 第 {page_no} 頁", min(45.0, page_no * 5))
        html = _fetch(url)
        if not html:
            break

        page_items = _parse_listing_page(html)
        if not page_items:
            break

        for item in page_items:
            item["scrape_session_at"] = session_at
            item["list_rank"] = len(new_items)
            src_id = item["source_id"]
            existing = check_exists(src_id) if check_exists else None

            if existing is not None:
                # 已存在 → 看價格變動
                old_price = existing.get("price_ntd")
                new_price = item.get("price_ntd")
                if old_price and new_price and abs(new_price - old_price) > 10000:
                    price_updates.append({
                        "source_id": src_id,
                        "old_price": old_price,
                        "new_price": new_price,
                        "title": item.get("title", ""),
                        "district": item.get("district"),
                    })
                    progress_callback(
                        f"  💰 永慶價格變動 {src_id} {int(old_price//10000)}萬→{int(new_price//10000)}萬"
                    )
                consecutive_complete += 1
                if consecutive_complete >= 5:
                    stop = True
                    progress_callback("  ↻ 永慶連續 5 筆已存在，停止")
                    break
                continue

            # 全新物件 → 兩階段 enrich
            # 注意：先別 reset consecutive_complete=0，等下面確認 enrich 成功 + 真要當新物件再 reset。
            # 避免「夾在 existing 之間的 enrich-fail / 非公寓物件」打斷連續判定，導致 batch 跑到底。

            # 每筆 HTTP 之前 sleep（避免被永慶 anti-bot 偵測）
            time.sleep(2.5)

            progress_callback(f"  → 永慶 HTTP 抓詳情: {item.get('title','')[:30]}")

            # Stage 1：HTTP 拿全部欄位（除座標）— 快（~1 秒/筆，有 rate-limit cooldown）
            ok_basic = _enrich_basic_via_http(item, max_retries=2)
            if not ok_basic:
                # 即使 enrich 失敗，_parse_detail_html 也可能抓到 type_raw（華廈/大樓/別墅 etc）
                # → 確認非公寓 → 不入 retry queue，直接 skip（這 src_id 永遠不會分析）
                _type_raw = item.get("_yongqing_type_raw") or ""
                _NON_APT = ("華廈", "電梯大廈", "電梯大樓", "住宅大樓", "別墅", "店面", "套房")
                if _type_raw and any(t in _type_raw for t in _NON_APT):
                    progress_callback(
                        f"  ⏭ 永慶 跳過 {item.get('_yongqing_house_id')}：型態={_type_raw}（非公寓），不入重試佇列"
                    )
                    try:
                        from database.retry_queue import dequeue_by_source_id
                        dequeue_by_source_id(item.get("source_id"))
                    except Exception: pass
                else:
                    # 真的是 enrich 失敗（HTML 不完整 / parse fail）→ 進 retry queue
                    try:
                        from database.retry_queue import enqueue as _retry_enqueue
                        _retry_enqueue(
                            source_id=item["source_id"],
                            source="永慶",
                            url=item["url"],
                            error="HTTP enrich failed: SSR HTML 不完整或 parse 失敗",
                            extra_context={"district": item.get("district"), "title": item.get("title")},
                        )
                    except Exception as _eq:
                        logger.warning(f"retry_queue enqueue 失敗 {item.get('source_id')}: {_eq}")
                    progress_callback(
                        f"  ⏭ 永慶 跳過 {item.get('_yongqing_house_id')}：HTTP enrich 失敗，已加入重試佇列"
                    )
                # enrich-fail 物件不會入 DB，跟「已存在」效果一樣 → 累 consecutive 才能正常停
                consecutive_complete += 1
                if consecutive_complete >= 5:
                    stop = True
                    progress_callback("  ↻ 永慶連續 5 筆無新物件（含 enrich-fail / 非公寓），停止")
                    break
                continue

            # 過濾：只看樓高 — 5 樓含以下分析
            tf = item.get("total_floors")
            if tf and int(tf) > 5:
                progress_callback(
                    f"  ⏭ 永慶 跳過 {item.get('_yongqing_house_id')}：總樓層 {tf} 樓 > 5（型態={item.get('_yongqing_type_raw','-')}）"
                )
                # 非公寓 → 把這 src_id 從 retry queue 清掉
                try:
                    from database.retry_queue import dequeue_by_source_id
                    dequeue_by_source_id(item.get("source_id"))
                except Exception as _de:
                    logger.warning(f"dequeue_by_source_id 失敗 {item.get('source_id')}: {_de}")
                # 永遠不會入 DB → 算 existing 一樣 ++
                consecutive_complete += 1
                if consecutive_complete >= 5:
                    stop = True
                    progress_callback("  ↻ 永慶連續 5 筆無新物件（含非公寓），停止")
                    break
                continue

            # 跨來源 dup 早偵測：item 已有 address+area+price → 查 591/信義 既有 doc
            # 命中就算 consecutive_complete，避免「永慶 src_id 從沒入過 DB → 永遠當新 → 跑到底」
            from database.db import find_cross_source_duplicate
            cross_hit = find_cross_source_duplicate(item)
            if cross_hit:
                progress_callback(f"  ↻ 永慶 {item.get('_yongqing_house_id')} 跨來源命中既有 doc {cross_hit[:8]}（{item.get('address','')[:20]}），算已存在")
                consecutive_complete += 1
                # 仍 push 進 new_items 讓下游 dup_merge 寫 source_ids，下次能直接命中 check_exists
                new_items.append(item)
                if consecutive_complete >= 5:
                    stop = True
                    progress_callback("  ↻ 永慶連續 5 筆已存在（含跨來源），停止")
                    break
                if len(new_items) >= limit:
                    stop = True
                    break
                _human_sleep()
                continue

            # 真的是「新物件」要入 DB → reset consecutive
            consecutive_complete = 0

            # Stage 2：通過樓高 filter 的才開 Playwright 拿座標（~7-10 秒/筆）
            progress_callback(f"  → 永慶 Playwright 拿座標: {item.get('address','')[:20]}")
            got_coords = _enrich_coords_via_playwright(item, headless=headless, timeout_sec=20)
            if not got_coords:
                # 座標拿不到不算整體失敗（pipeline 會自己 fallback geocoding）
                logger.info(f"yongqing {item.get('_yongqing_house_id')} 座標拿不到，繼續 ingest（pipeline 會 fallback）")

            new_items.append(item)
            progress_callback(f"  ✓ 已加入：第 {len(new_items)} 筆 {item.get('address','')[:25]}")
            if len(new_items) >= limit:
                stop = True
                break

            _human_sleep()

        page_no += 1
        _human_sleep()

    progress_callback(
        f"永慶完成：新 {len(new_items)} 筆 / 改價 {len(price_updates)} 筆", 50
    )
    return {"new": new_items, "price_updates": price_updates}


def scrape_yongqing_single(url: str) -> Optional[dict]:
    """給 /api/scrape_url 用的：抓單一永慶 URL 回 item dict。"""
    m = re.search(r"buy\.yungching\.com\.tw/house/(\d{6,8})", url)
    if not m:
        return None
    house_id = m.group(1)

    # 從詳情頁直接抓所有資料（不走列表頁）
    detail_url = f"https://buy.yungching.com.tw/house/{house_id}"
    item = {
        "source": "永慶",
        "source_id": f"yongqing_{house_id}",
        "url": detail_url,
        "_yongqing_house_id": house_id,
        "scrape_session_at": now_tw_iso(),
        "list_rank": 0,
    }
    # 用 HTTP 拿基本 SSR HTML 抓初步欄位
    html = _fetch(detail_url)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)

        # 從 ld+json 拿基本資訊（永慶 schema.org Product 區塊）
        import json as _json
        for sc in soup.find_all("script", type="application/ld+json"):
            try:
                data = _json.loads(sc.string or "")
                if isinstance(data, dict) and data.get("@type") == "Product":
                    item["title"] = data.get("name")
                    img = data.get("image")
                    if img:
                        item["image_url"] = img if isinstance(img, str) else img[0]
                    offers = data.get("offers") or {}
                    if offers.get("price"):
                        item["price_ntd"] = int(offers["price"]) * 10000   # 永慶 price 單位是萬
                    yc_id = data.get("productID")
                    if yc_id:
                        item["_yongqing_yc_id"] = yc_id
                    break
            except Exception:
                continue

        # 地址、城市、區
        addr_m = re.search(r"((?:台北市|新北市)[^\d\s]+(?:路|街|大道)[^\d\s]*(?:[一二三四五六七八九十]段)?)", text)
        if addr_m:
            item["address"] = addr_m.group(1)
            cm = re.match(r"^(台北市|新北市)", addr_m.group(1))
            if cm:
                item["city"] = cm.group(1)
            dm = re.search(r"(?:台北市|新北市)([^\d]+?區)", addr_m.group(1))
            if dm:
                item["district"] = dm.group(1)

        # 樓層
        floor_m = re.search(r"(\d+)\s*/\s*(\d+)\s*樓", text)
        if floor_m:
            item["floor"] = floor_m.group(1)
            item["total_floors"] = int(floor_m.group(2))

        # 屋齡
        age_m = re.search(r"屋齡[\s:：]*([\d.]+)\s*年", text)
        if age_m:
            from database.models import age_to_completed_year as _atc
            _age = float(age_m.group(1))
            item["building_age"] = _age
            item["building_age_completed_year"] = _atc(_age)
            item["building_age_source"] = "yongqing_single"

        item["building_type"] = "公寓"   # 暫預設，可由詳情頁 enrich 修正

    # 用 Playwright 補座標 + 結構化詳情
    try:
        _enrich_from_detail(item, headless=True)
    except Exception as e:
        logger.warning(f"yongqing single enrich fail {house_id}: {e}")

    return item if item.get("price_ntd") and item.get("address") else None
