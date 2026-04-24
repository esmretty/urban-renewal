"""
地址轉座標，並計算最近捷運站距離。
優先使用 Google Maps；無 API Key 則 fallback 至 Nominatim (免費)。
"""
import math
import time
import logging
import httpx
from typing import Optional

from config import GOOGLE_MAPS_API_KEY, MRT_STATIONS

logger = logging.getLogger(__name__)


def geocode_address(address: str) -> Optional[tuple[float, float]]:
    """
    將地址字串轉成 (lat, lng)。
    回傳 None 表示找不到。
    """
    if GOOGLE_MAPS_API_KEY:
        return _geocode_google(address)
    return _geocode_nominatim(address)


def geocode_with_district(address: str) -> list[dict]:
    """
    呼叫 Google Geocoding 並解出每個候選的 city / district。
    回傳 list of {lat, lng, city, district, formatted_address}，按 Google 回傳順序。
    找不到回空 list。
    """
    if not GOOGLE_MAPS_API_KEY:
        return []
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    try:
        resp = httpx.get(
            url,
            params={"address": address, "key": GOOGLE_MAPS_API_KEY, "language": "zh-TW", "region": "tw"},
            timeout=10,
        )
        data = resp.json()
        status = data.get("status")
        if status != "OK":
            # ZERO_RESULTS 是正常（地址查無），只在 debug 級別記
            # 其他（REQUEST_DENIED / OVER_QUERY_LIMIT / INVALID_REQUEST）是環境問題，要 WARN
            if status == "ZERO_RESULTS":
                logger.debug(f"geocode ZERO_RESULTS for {address!r}")
            else:
                logger.warning(
                    f"Google Geocoding API 失敗 status={status} "
                    f"message={data.get('error_message', '(none)')} address={address!r}"
                )
            return []
        out = []
        for r in data.get("results", []):
            loc = r["geometry"]["location"]
            city = district = None
            for c in r.get("address_components", []):
                types = c.get("types", [])
                long_name = c.get("long_name", "")
                if "administrative_area_level_1" in types:
                    city = long_name.replace("臺北", "台北")
                elif "administrative_area_level_2" in types or "administrative_area_level_3" in types:
                    # Taiwan 區常出現在 level_2 或 level_3
                    if long_name.endswith("區") and district is None:
                        district = long_name
            out.append({
                "lat": loc["lat"],
                "lng": loc["lng"],
                "city": city,
                "district": district,
                "formatted_address": r.get("formatted_address", "").replace("臺北", "台北"),
                "partial_match": bool(r.get("partial_match")),
            })
        return out
    except Exception as e:
        logger.warning(f"geocode_with_district failed: {e}")
        return []


def verify_and_fix_road(addr_with_region: str, city: str, district: str) -> dict:
    """驗證地址的路/街名是否真實存在；不存在時嘗試用 Claude 做 fuzzy 修正（諧音/錯字）。

    回傳：
      {"status": "ok", "address": 原地址}
          → 路名存在（至少一筆 Google 結果 formatted 含原路名，或有非 partial_match 的結果）
      {"status": "fixed", "address": 修正地址, "original_road": 原路名, "fixed_road": 新路名}
          → Claude 修正成功 + 新地址驗證通過
      {"status": "invalid", "reason": "..."}
          → 路不存在且無法修正
    """
    import re as _re
    # 先把 city/district 前綴拿掉再抽路名，避免把「台北市中正區紹安街」整段抓進去
    inner = _re.sub(r"^(台北市|臺北市|新北市|桃園市|台中市|臺中市|高雄市|台南市|臺南市|基隆市|新竹市|新竹縣|宜蘭縣|苗栗縣|彰化縣|南投縣|雲林縣|嘉義市|嘉義縣|屏東縣|花蓮縣|台東縣|臺東縣|澎湖縣|金門縣|連江縣)", "", addr_with_region)
    inner = _re.sub(r"^[\u4e00-\u9fa5]{1,3}區", "", inner)
    road_m = _re.match(r"^([\u4e00-\u9fa5]+(?:路|街|大道))", inner)
    if not road_m:
        return {"status": "ok", "address": addr_with_region}
    original_road = road_m.group(1)

    def _in_target_area(c: dict) -> bool:
        """結果必須落在目標 city+district，避免拿其他縣市同名路誤判為存在。"""
        return (c.get("city") == city) and (c.get("district") == district)

    cands = geocode_with_district(addr_with_region)
    # Google API 整個失敗（非 OK 狀態）回 [] → 無法判斷真實性，視為 ok 跳過驗證，
    # 不要把每個地址都標可疑。真實錯誤已在 geocode_with_district 層 WARN 出來。
    if not cands:
        return {"status": "ok", "address": addr_with_region}
    # 只計算「同 city+district」的結果；跨縣市結果不算數
    in_area = [c for c in cands if _in_target_area(c)]
    # 目標區內有結果的 formatted 含原路名 → OK
    if any(original_road in (c.get("formatted_address") or "") for c in in_area):
        return {"status": "ok", "address": addr_with_region}
    # 目標區內有非 partial_match 的結果 → 路存在
    if any(not c.get("partial_match") for c in in_area):
        return {"status": "ok", "address": addr_with_region}

    # 路在目標區內不存在 → 嘗試 Claude fuzzy 修正
    claude_road = _claude_fix_road(addr_with_region, city, district, cands, original_road)
    if claude_road and claude_road != original_road:
        fixed_addr = addr_with_region.replace(original_road, claude_road, 1)
        new_cands = geocode_with_district(fixed_addr)
        new_in_area = [c for c in new_cands if _in_target_area(c)]
        road_found = any(claude_road in (c.get("formatted_address") or "") for c in new_in_area)
        any_exact = any(not c.get("partial_match") for c in new_in_area)
        if road_found and any_exact:
            logger.info(f"地址路名 Claude 修正（在 {city}{district} 內）：{original_road} → {claude_road}")
            return {"status": "fixed", "address": fixed_addr,
                    "original_road": original_road, "fixed_road": claude_road}
    return {"status": "invalid", "reason": f"路名「{original_road}」在 {city}{district} 內不存在且無法修正"}


def _claude_fix_road(addr_with_region: str, city: str, district: str, cands: list, original_road: Optional[str] = None) -> Optional[str]:
    """問 Claude：給了原地址 + 該行政區 LVR 真實路名清單 + Google 近似結果，挑最接近的路名。
    回傳路名字串 或 None。

    關鍵：原路名（通常是 OCR 誤讀，如「簡沂街」）常非真實路名；
    Google partial match 會回「同門牌其他路」引導 Claude 亂猜（例如「濟南路」）。
    因此改讓 Claude 從 LVR 清單挑「字形最近」的真實路名（例如「臨沂街」）。
    """
    try:
        from analysis.claude_analyzer import client, MODEL_TEXT
    except Exception:
        return None
    # LVR 路名清單：從該行政區實價登錄抽出真實路名，給 Claude 當選項
    try:
        from analysis.lvr_index import list_roads_in_district
        lvr_roads_raw = list_roads_in_district(city or "", district or "")
    except Exception:
        lvr_roads_raw = []
    # 排除原路名：LVR 可能含歷史誤植（如「紹安街」），若留著 Claude 會選回原名不改
    lvr_roads = [r for r in lvr_roads_raw if r != original_road] if original_road else lvr_roads_raw
    lvr_roads_str = "（LVR 無樣本，請從 Google 結果推測）" if not lvr_roads \
        else "、".join(lvr_roads[:150])   # 上限 150 條控 token
    suggestions = "\n".join(f"  - {c.get('formatted_address')}" for c in cands[:5]) or "  (無)"
    prompt = (
        f"以下地址的「路/街」名可能是 OCR 誤讀（591 詳情頁常用 CSS 位移把字拆錯）：\n\n"
        f"原地址：{addr_with_region}\n"
        f"Google 只回 partial match（都是附近同門牌的其他路，不可靠）：\n{suggestions}\n\n"
        f"{city}{district} 的**真實**路名清單（從實價登錄抽出，原路名「{original_road or ''}」已排除）：\n{lvr_roads_str}\n\n"
        f"請從上述真實路名清單中挑選**字形最相近**原路名的那一條。\n"
        f"常見誤讀：\n"
        f"  - 「臨」被讀成「簡」（兩字右半相近）\n"
        f"  - 「詔」被讀成「紹」\n"
        f"  - 「福」和「興」在某些字體混淆\n"
        f"若清單中**沒有**跟原路名字形夠像的，回 UNKNOWN（寧可留錯 OCR 也不要胡亂改成不相關的路）。\n\n"
        f"只回路名（例如「臨沂街」、「詔安街」），不要解釋。"
    )
    try:
        import re as _re
        response = client.messages.create(
            model=MODEL_TEXT,
            max_tokens=30,
            messages=[{"role": "user", "content": prompt}],
        )
        text = (response.content[0].text or "").strip().strip("「」\"'").strip()
        if not text or text == "UNKNOWN" or len(text) > 12:
            return None
        if not _re.fullmatch(r"[\u4e00-\u9fa5]+(?:路|街|大道)", text):
            return None
        # 二次把關：Claude 回的路名必須真的在 LVR 清單裡
        if lvr_roads and text not in lvr_roads:
            logger.warning(f"_claude_fix_road 回傳 '{text}' 不在 LVR 清單，捨棄")
            return None
        return text
    except Exception as e:
        logger.warning(f"_claude_fix_road failed: {e}")
    return None


def _geocode_google(address: str) -> Optional[tuple[float, float]]:
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    try:
        resp = httpx.get(url, params={"address": address, "key": GOOGLE_MAPS_API_KEY}, timeout=10)
        data = resp.json()
        if data.get("status") == "OK":
            loc = data["results"][0]["geometry"]["location"]
            return (loc["lat"], loc["lng"])
    except Exception as e:
        logger.warning(f"Google geocoding failed: {e}")
    return None


def _geocode_nominatim(address: str) -> Optional[tuple[float, float]]:
    """
    使用 OpenStreetMap Nominatim 免費服務。
    注意：有 rate limit（1 req/sec），請勿快速連打。
    """
    url = "https://nominatim.openstreetmap.org/search"
    headers = {"User-Agent": "UrbanRenewalResearch/1.0"}
    # 在地址前加台灣，提高精確度
    query = address if "台灣" in address or "Taiwan" in address else f"台灣 {address}"
    try:
        resp = httpx.get(
            url,
            params={"q": query, "format": "json", "limit": 1, "countrycodes": "tw"},
            headers=headers,
            timeout=10,
        )
        results = resp.json()
        if results:
            return (float(results[0]["lat"]), float(results[0]["lon"]))
    except Exception as e:
        logger.warning(f"Nominatim geocoding failed for '{address}': {e}")
    time.sleep(1)  # Nominatim rate limit
    return None


def haversine_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """計算兩點間距離（公尺）。"""
    R = 6371000  # 地球半徑（公尺）
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


_MRT_EXITS_CACHE = None
def _load_mrt_exits():
    global _MRT_EXITS_CACHE
    if _MRT_EXITS_CACHE is not None:
        return _MRT_EXITS_CACHE
    try:
        import json
        from pathlib import Path
        p = Path(__file__).resolve().parent.parent / "data" / "mrt_exits.json"
        _MRT_EXITS_CACHE = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        _MRT_EXITS_CACHE = {}
    return _MRT_EXITS_CACHE


def get_nearest_mrt(lat: float, lng: float) -> tuple[Optional[str], Optional[float]]:
    """
    回傳 (最近捷運站名稱, 走路直線距離公尺)。
    相容舊 callers — 如需出口編號請用 get_nearest_mrt_exit。
    """
    station, _exit_num, dist = get_nearest_mrt_exit(lat, lng)
    return station, dist


def get_nearest_mrt_exit(lat: float, lng: float) -> tuple[Optional[str], Optional[str], Optional[float]]:
    """
    回傳 (最近捷運站名稱, 出口編號, 直線距離公尺)。
    使用台北市開放資料「捷運車站出入口座標」計算「到最近出口」的距離，比只用站體中心更精確。
    Fallback：若出口資料缺失，用 MRT_STATIONS 站中心座標（出口編號回 None）。
    """
    exits = _load_mrt_exits()
    nearest_station = None
    nearest_exit = None
    nearest_dist = float("inf")
    if exits:
        for station, exit_list in exits.items():
            for ex in exit_list:
                d = haversine_distance(lat, lng, ex["lat"], ex["lng"])
                if d < nearest_dist:
                    nearest_dist = d
                    nearest_station = station
                    nearest_exit = ex.get("exit")
    if nearest_station is None:
        for name, (slat, slng) in MRT_STATIONS.items():
            d = haversine_distance(lat, lng, slat, slng)
            if d < nearest_dist:
                nearest_dist = d
                nearest_station = name
                nearest_exit = None
    if nearest_dist == float("inf"):
        return None, None, None
    # 該站只有 1 個出口 → 不標出口編號（寫「1 號出口」對用戶沒意義）
    if nearest_station and exits:
        station_exits = exits.get(nearest_station) or []
        if len(station_exits) <= 1:
            nearest_exit = None
    return nearest_station, nearest_exit, round(nearest_dist, 1)


def get_nearby_mrt_stations(lat: float, lng: float, max_dist_m: float = 1500, top_n: int = 3) -> list:
    """回最近 N 個捷運站（以站中心最近出口的距離為準），超過 max_dist_m 的不收。
    回 list of {"name", "dist_m"}。"""
    exits = _load_mrt_exits()
    if not exits:
        return []
    # 每站取最近出口距離
    per_station = []
    for station, exit_list in exits.items():
        min_d = float("inf")
        for ex in exit_list:
            d = haversine_distance(lat, lng, ex["lat"], ex["lng"])
            if d < min_d:
                min_d = d
        if min_d <= max_dist_m:
            per_station.append({"name": station, "dist_m": round(min_d, 1)})
    per_station.sort(key=lambda x: x["dist_m"])
    return per_station[:top_n]
