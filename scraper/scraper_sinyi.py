"""信義房屋爬蟲 — 第三來源（591 + 永慶 + 信義）。

設計重點（與永慶比較）：
- 列表頁 SSR Next.js __NEXT_DATA__ 已含完整資料（含 lat/lng/price/area），
  → 不需要 Playwright Stage 2 拿座標
- 詳情頁的 main detail data 是 client-side JS 才抓，SSR 只塞 minimal info
  → **完全靠列表頁**抓，速度快（~0.5 秒/筆）
- 地址只到路段（如「台北市信義區松山路」），需後段 LVR triangulate / reverse geocode
  補完整門牌（同永慶 path）

URL 模式（用戶提供）:
  https://www.sinyi.com.tw/buy/list/{屋型}-type/Taipei-city/{zipcodes}-zip/publish-desc/{page}
  例：https://www.sinyi.com.tw/buy/list/apartment-type/Taipei-city/100-104-106-110-116-zip/publish-desc/1
  - apartment-type = 公寓
  - 100=中正 104=中山 106=大安 110=信義 116=文山
  - publish-desc = 最新刊登排序
  - page 從 1 開始

Anti-bot：信義列表 SSR 看似無 reCAPTCHA challenge（reCAPTCHA 只在表單提交時觸發），
但保守加 cooldown 機制（同永慶 _LAST_403_AT pattern）。
"""
from __future__ import annotations
import json
import logging
import re
import time
from typing import Optional

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

LAST_FETCH_ERROR: Optional[str] = None
_LAST_403_AT = 0.0   # cooldown timestamp

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9",
}

# 行政區 → 信義 zipcode（台北市）
DISTRICT_TO_ZIP = {
    "中正區": "100", "大同區": "103", "中山區": "104",
    "松山區": "105", "大安區": "106", "萬華區": "108",
    "信義區": "110", "士林區": "111", "北投區": "112",
    "內湖區": "114", "南港區": "115", "文山區": "116",
}

# 我們系統用的「公寓」對應信義 URL slug
TYPE_TO_SLUG = {
    "公寓": "apartment",
    "華廈": "mansion",
    "電梯大樓": "tower",
    "透天": "townhouse",
}

# houselandtype code → 我們系統用的 building_type
# A = 公寓, E = 套房（同樣 5F 以下，但獨立計算），其他暫推測
HOUSELANDTYPE_MAP = {
    "A": "公寓",
    "B": "華廈",
    "C": "電梯大樓",
    "D": "透天",
    "E": "套房",
    "F": "店面",
    "L": "預售屋",
}


def _fetch(url: str, retries: int = 3) -> Optional[str]:
    """打信義 HTTP，回 HTML text 或 None。
    遇 403/429 backoff 久一點 + 設 _LAST_403_AT cooldown。"""
    global LAST_FETCH_ERROR, _LAST_403_AT
    since_403 = time.time() - _LAST_403_AT
    if _LAST_403_AT and since_403 < 30:
        wait = 30 - since_403
        logger.info(f"信義 cooldown {wait:.1f}s（剛被 403/429）")
        time.sleep(wait)
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=DEFAULT_HEADERS, timeout=20, verify=False)
            if r.status_code == 200:
                return r.text
            if r.status_code in (403, 429):
                _LAST_403_AT = time.time()
                wait = 15 + attempt * 15
                logger.warning(f"信義 {r.status_code} rate limit，等 {wait}s 重試 {url[:80]}")
                time.sleep(wait)
                continue
            LAST_FETCH_ERROR = f"HTTP {r.status_code}"
            logger.warning(f"信義 fetch fail {r.status_code} {url[:80]}")
            return None
        except Exception as e:
            LAST_FETCH_ERROR = str(e)[:200]
            logger.warning(f"信義 fetch exception {e} attempt={attempt+1}/{retries}")
            time.sleep(3)
    return None


def _build_list_url(districts: list[str], building_type: str = "公寓", page: int = 1) -> str:
    """組信義列表 URL。districts 例：['大安區','信義區']；building_type 我們系統用語。"""
    type_slug = TYPE_TO_SLUG.get(building_type, "apartment")
    zips = "-".join(DISTRICT_TO_ZIP[d] for d in districts if d in DISTRICT_TO_ZIP)
    if not zips:
        zips = "100-104-106-110-116"   # 預設 5 區
    return (
        f"https://www.sinyi.com.tw/buy/list/{type_slug}-type/Taipei-city/{zips}-zip"
        f"/publish-desc/{page}"
    )


def _parse_next_data(html: str) -> Optional[dict]:
    """從 SSR HTML 抽 __NEXT_DATA__ JSON island。"""
    if not html:
        return None
    m = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
        html, re.S,
    )
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception as e:
        logger.warning(f"信義 __NEXT_DATA__ parse 失敗: {e}")
        return None


def _item_from_listing(it: dict) -> dict:
    """把信義列表的單筆 dict 轉成我們系統的 item 格式。"""
    from database.models import age_to_completed_year
    house_no = it.get("houseNo") or ""
    addr = it.get("address") or ""
    # 從 address 抽 city + district
    city = ""
    district = ""
    cm = re.match(r"^(台北市|新北市)", addr)
    if cm:
        city = cm.group(1)
    dm = re.search(r"(?:台北市|新北市)([^\d]+?區)", addr)
    if dm:
        district = dm.group(1)
    # age 可能是「57.0年」、「預售」等 → 抽數字
    age_raw = it.get("age") or ""
    age_num: Optional[float] = None
    am = re.search(r"([\d.]+)", str(age_raw))
    if am:
        try: age_num = float(am.group(1))
        except ValueError: pass
    # 屋型
    hl_codes = it.get("houselandtype") or []
    bld_type = None
    if hl_codes:
        bld_type = HOUSELANDTYPE_MAP.get(hl_codes[0]) or "公寓"
    # 樓層 "4-5" 取低樓 (主樓)
    floor_raw = it.get("floor") or ""
    floor_first = re.match(r"(\d+)", str(floor_raw))
    floor_str = floor_first.group(1) if floor_first else None
    total_floors = None
    try:
        total_floors = int(it.get("totalfloor"))
    except (TypeError, ValueError):
        pass
    # price (信義單位「萬」)
    price_wan = it.get("totalPrice")
    price_ntd = int(price_wan) * 10000 if price_wan else None
    # 建坪/地坪
    area_b = it.get("areaBuilding")
    if area_b is not None and area_b <= 0:
        area_b = None
    area_l = it.get("areaLand")
    if area_l is not None and area_l <= 0:
        area_l = None

    detail_url = f"https://www.sinyi.com.tw/buy/house/{house_no}"
    item = {
        "source": "信義",
        "source_id": f"sinyi_{house_no}",
        "url": detail_url,
        "title": it.get("name") or "",
        "city": city,
        "district": district,
        "address": addr,
        "building_type": bld_type,
        "total_floors": total_floors,
        "floor": floor_str,
        "building_age": age_num,
        "building_age_completed_year": age_to_completed_year(age_num) if age_num else None,
        "building_age_source": "sinyi_card" if age_num else None,
        "building_area_ping": area_b,
        "land_area_ping": area_l,
        "price_ntd": price_ntd,
        "price_per_ping": (price_ntd / area_b) if (price_ntd and area_b) else None,
        "image_url": (it.get("image") or [None])[0] or it.get("largeImage"),
        "latitude": it.get("latitude"),
        "longitude": it.get("longitude"),
        "source_latitude": it.get("latitude"),
        "source_longitude": it.get("longitude"),
        "community_name": it.get("commName") or it.get("name") or None,
        "_sinyi_house_no": house_no,
        "_sinyi_houselandtype": hl_codes,
    }
    return item


def scrape_sinyi(
    headless: bool,
    progress_callback,
    districts_filter: Optional[list[str]] = None,
    check_exists=None,
    limit: int = 30,
    building_type: str = "公寓",
) -> dict:
    """主流程。回 {"new": [...items...], "price_updates": [...]}.

    headless 參數信義不需要 Playwright，僅為簽名相容。"""
    global LAST_FETCH_ERROR
    LAST_FETCH_ERROR = None
    new_items: list[dict] = []
    price_updates: list[dict] = []
    consecutive_complete = 0
    page = 1
    max_pages = 20  # 安全上限

    districts_filter = districts_filter or []
    use_districts = [d for d in districts_filter if d in DISTRICT_TO_ZIP]
    if not use_districts:
        use_districts = ["中正區", "中山區", "大安區", "信義區", "文山區"]

    progress_callback(f"信義 開始抓 {','.join(use_districts)} {building_type}")
    while page <= max_pages and len(new_items) < limit:
        url = _build_list_url(use_districts, building_type=building_type, page=page)
        progress_callback(f"信義 列表頁 {page}: {url}")
        html = _fetch(url)
        if not html:
            progress_callback(f"信義 列表頁 {page} 抓失敗，停止")
            break
        nd = _parse_next_data(html)
        if not nd:
            progress_callback("信義 列表頁無 __NEXT_DATA__，停止")
            break
        try:
            buy = nd["props"]["initialReduxState"]["buyReducer"]
        except KeyError:
            progress_callback("信義 NEXT_DATA 結構未預期")
            break
        items = buy.get("list") or []
        total_cnt = buy.get("totalCnt", 0)
        if not items:
            progress_callback(f"信義 列表頁 {page} 0 筆，已到底")
            break

        for raw in items:
            try:
                item = _item_from_listing(raw)
            except Exception as e:
                logger.warning(f"信義 item parse 失敗: {e}")
                continue
            if not item.get("price_ntd") or not item.get("address"):
                continue
            # 過濾樓高 > 5（同永慶邏輯，順手清 retry queue）
            tf = item.get("total_floors")
            if tf and int(tf) > 5:
                progress_callback(
                    f"  ⏭ 信義 跳過 {item['_sinyi_house_no']}：總樓層 {tf} 樓 > 5"
                )
                try:
                    from database.retry_queue import dequeue_by_source_id
                    dequeue_by_source_id(item["source_id"])
                except Exception: pass
                continue

            src_id = item["source_id"]
            if check_exists:
                existing = check_exists(src_id)
                if existing:
                    # 既有：簡單 price update 偵測（不做 enrich path，跟永慶相同）
                    if existing.get("price_ntd") and item.get("price_ntd") \
                            and existing["price_ntd"] != item["price_ntd"]:
                        price_updates.append({
                            "source_id": src_id,
                            "old_price": existing["price_ntd"],
                            "new_price": item["price_ntd"],
                        })
                        progress_callback(
                            f"  💰 信義 改價 {src_id}: {existing['price_ntd']/10000:.0f} → {item['price_ntd']/10000:.0f} 萬"
                        )
                    consecutive_complete += 1
                    if consecutive_complete >= 5:
                        progress_callback("  ↻ 信義 連續 5 筆已存在，停止")
                        break
                    continue
            consecutive_complete = 0
            new_items.append(item)
            progress_callback(
                f"  ✓ 信義 第 {len(new_items)} 筆: {item['address'][:25]} {item.get('totalfloor','?')}F"
            )
            if len(new_items) >= limit:
                break
            time.sleep(0.5)   # 列表內不打 detail，slight throttle 即可

        if consecutive_complete >= 5 or len(new_items) >= limit:
            break
        if page * 20 >= total_cnt:
            progress_callback(f"信義 已抓完 totalCnt={total_cnt}")
            break
        page += 1
        time.sleep(2.0)   # 換頁間 sleep 較久

    progress_callback(
        f"信義 完成：新 {len(new_items)} 筆、改價 {len(price_updates)} 筆"
    )
    return {"new": new_items, "price_updates": price_updates}


def scrape_sinyi_single(url: str) -> Optional[dict]:
    """給 /api/scrape_url 用：單一信義 URL 回 item dict。
    策略：找 houseNo → 從列表頁能找到該物件就用、否則 fallback 嘗試 detail HTML（minimal）。"""
    m = re.search(r"/buy/house/([A-Z0-9]{4,8})", url, re.IGNORECASE)
    if not m:
        return None
    house_no = m.group(1).upper()
    detail_url = f"https://www.sinyi.com.tw/buy/house/{house_no}"

    # 直接打 detail 頁拿基本資料（lat/lng/houseNo + 列表 cache 可能也在 SSR）
    html = _fetch(detail_url)
    if not html:
        return None
    nd = _parse_next_data(html)
    if not nd:
        return None
    try:
        dd = nd["props"]["initialReduxState"]["buyReducer"]["detailData"]
    except KeyError:
        dd = {}

    # detailData 沒帶 price/address — fallback 從相關物件清單找同 houseNo
    similar_list = []
    try:
        rs = nd["props"]["initialReduxState"]
        for v in rs.values():
            if not isinstance(v, dict): continue
            for vv in v.values():
                if isinstance(vv, list):
                    similar_list.extend(vv)
                elif isinstance(vv, dict):
                    for vvv in vv.values():
                        if isinstance(vvv, list):
                            similar_list.extend(vvv)
    except Exception:
        pass
    matched = next((it for it in similar_list
                    if isinstance(it, dict) and it.get("houseNo") == house_no), None)
    if matched:
        item = _item_from_listing(matched)
        return item
    # detailData fallback：只有 lat/lng
    return {
        "source": "信義",
        "source_id": f"sinyi_{house_no}",
        "url": detail_url,
        "_sinyi_house_no": house_no,
        "latitude": dd.get("latitude"),
        "longitude": dd.get("longitude"),
        "source_latitude": dd.get("latitude"),
        "source_longitude": dd.get("longitude"),
    }
