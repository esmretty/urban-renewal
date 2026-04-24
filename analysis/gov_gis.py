"""
政府公開 GIS API 整合：座標 → 土地使用分區。

目前覆蓋：
  - 台北市 zonegeo.udd.gov.taipei GeoServer WFS（typename: Taipei:ublock97-TWD97）
  - NLSC TownVillagePointQuery 取段地號（輔助）

未覆蓋（暫時手動輸入）：
  - 新北市
"""
import math
import logging
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ── 座標投影 ─────────────────────────────────────────────────────────────────

def wgs84_to_twd97(lat: float, lng: float) -> tuple[float, float]:
    """
    WGS84 (degrees) → TWD97 TM2 (meters).
    台灣本島：central meridian 121°, false_easting 250000, k0=0.9999, GRS80.
    """
    a = 6378137.0
    f = 1 / 298.257222101
    e2 = f * (2 - f)
    ep2 = e2 / (1 - e2)
    k0 = 0.9999
    lon0 = math.radians(121.0)
    fe = 250000.0
    fn = 0.0

    phi = math.radians(lat)
    lam = math.radians(lng)
    n = a / math.sqrt(1 - e2 * math.sin(phi) ** 2)
    t = math.tan(phi) ** 2
    c = ep2 * math.cos(phi) ** 2
    A = math.cos(phi) * (lam - lon0)

    M = a * (
        (1 - e2 / 4 - 3 * e2**2 / 64 - 5 * e2**3 / 256) * phi
        - (3 * e2 / 8 + 3 * e2**2 / 32 + 45 * e2**3 / 1024) * math.sin(2 * phi)
        + (15 * e2**2 / 256 + 45 * e2**3 / 1024) * math.sin(4 * phi)
        - (35 * e2**3 / 3072) * math.sin(6 * phi)
    )
    x = fe + k0 * n * (
        A
        + (1 - t + c) * A**3 / 6
        + (5 - 18 * t + t**2 + 72 * c - 58 * ep2) * A**5 / 120
    )
    y = fn + k0 * (
        M
        + n * math.tan(phi) * (
            A**2 / 2
            + (5 - t + 9 * c + 4 * c**2) * A**4 / 24
            + (61 - 58 * t + t**2 + 600 * c - 330 * ep2) * A**6 / 720
        )
    )
    return x, y


# ── 分區名稱判定 ─────────────────────────────────────────────────────────────

# 我們關心的「實質都市計畫分區」（建商可改建的類別）
_REAL_ZONE_PREFIX = ("住", "商", "工", "農", "保護", "風景", "文教", "倉儲", "行政")
# 公共設施／道路用地等（地號上有，但對都更試算意義不同）
_NON_RESIDENTIAL = (
    "高速公路", "道路", "公園", "國小", "國中", "高中", "大學", "幼", "醫院", "市場",
    "停車", "變電", "電信", "機關", "宗教", "綠地", "兒童遊樂", "廣場",
)


def is_real_zone(usenam: str) -> bool:
    """是否為「實質都市計畫分區」（住/商/工 等可建蔽的類別）。"""
    if not usenam:
        return False
    if any(usenam.startswith(p) for p in _REAL_ZONE_PREFIX):
        # 排除如「住宅區（保留地）」之類例外（極少）
        return True
    return False


def normalize_zone_name(usemem: Optional[str], usenam: Optional[str] = None) -> Optional[str]:
    """
    將「住3 / 第三種住宅區」之類 normalize 為標準名「第三種住宅區」。
    優先用 usemem (全名)，fallback 用 usenam (簡稱)。
    """
    if usemem and usemem.strip():
        return usemem.strip()
    if usenam:
        # 「住3」→「第三種住宅區」
        m = re.match(r"^(住|商|工)([1-6一二三四五六])(?:[-之]([1-6一二三四五六]))?", usenam.strip())
        if m:
            kind = {"住": "住宅", "商": "商業", "工": "工業"}[m.group(1)]
            n_map = {"1": "一", "2": "二", "3": "三", "4": "四", "5": "五", "6": "六"}
            num = n_map.get(m.group(2), m.group(2))
            sub = m.group(3)
            sub_str = ("之" + (n_map.get(sub, sub) if sub else "")) if sub else ""
            return f"第{num}{sub_str}種{kind}區"
        return usenam
    return None


# ── 台北市 GeoServer WFS：使用分區 ───────────────────────────────────────────

TAIPEI_WFS_URL = "https://zonegeo.udd.gov.taipei/geoserver/Taipei/ows"
TAIPEI_TYPENAME = "Taipei:ublock97-TWD97"
TAIPEI_SRS = "EPSG:3826"
TAIPEI_PORTAL_URL = "https://zonemap.udd.gov.taipei/ZoneMapOP/"


def _wfs_query_taipei(x: float, y: float, half_size: float) -> list:
    """以 (x, y) 為中心、邊長 2*half_size 的 BBOX 查詢台北分區 layer。"""
    bbox = f"{x - half_size},{y - half_size},{x + half_size},{y + half_size},{TAIPEI_SRS}"
    try:
        r = httpx.get(
            TAIPEI_WFS_URL,
            params={
                "service": "WFS", "request": "GetFeature", "version": "1.0.0",
                "outputFormat": "json", "typename": TAIPEI_TYPENAME,
                "bbox": bbox, "maxFeatures": 20,
            },
            timeout=15, verify=False,   # 政府站 SSL 不規範
        )
        return (r.json() or {}).get("features", [])
    except Exception as e:
        logger.warning(f"_wfs_query_taipei error: {e}")
        return []


def query_zoning_taipei(lat: float, lng: float) -> Optional[dict]:
    """
    座標 → 台北市使用分區。
    策略：以增大的 bbox 試查，取第一個「實質分區」。
    回傳 {"zone_code", "zone_label", "zone_name"} 或 None。
    """
    x, y = wgs84_to_twd97(lat, lng)
    seen_labels = set()
    real_zones = []
    non_real = []
    for half in (5, 15, 30, 60, 120):
        feats = _wfs_query_taipei(x, y, half)
        for f in feats:
            p = f.get("properties", {}) or {}
            usenam = p.get("usenam")
            if not usenam:
                continue
            usetxt = p.get("usetxt") or ""
            # 用 usetxt 做唯一識別（同 usenam 但不同原始分區要分開）
            key = usetxt or usenam
            if key in seen_labels:
                continue
            seen_labels.add(key)
            zone_name = normalize_zone_name(p.get("usemem"), usenam)
            original_zone = None
            # 規則 A：usenam 帶 (特)/(遷)/(核)/(抄) 等「狀態標記」後綴 → 原始分區 = 剝掉標記
            # 例：「第三種住宅區(特)(遷)」→ 原始分區「第三種住宅區」
            stripped = re.sub(r"\((?:特|遷|核|抄|鄰|公)\)", "", usenam)
            if stripped and stripped != usenam:
                original_zone = normalize_zone_name(None, stripped)
            else:
                # 規則 B：usetxt 裡明確寫 (住3)/(商2) 等原分區代碼
                m_orig = re.search(r"\(([^)]+)\)", usetxt)
                if m_orig and "特" in usenam:
                    original_zone = normalize_zone_name(None, m_orig.group(1))
            entry = {
                "zone_code": p.get("usecod"),
                "zone_label": usenam,
                "zone_name": zone_name,
                "original_zone": original_zone,
            }
            if is_real_zone(usenam):
                real_zones.append(entry)
            else:
                non_real.append(entry)
        if real_zones:
            break  # 找到實質分區就停止擴大
    if real_zones:
        # shallow copy 第一筆當 result，避免 result["zone_list"]=real_zones 製造自我引用循環
        # （real_zones[0] 本來就是 result → 塞回自己的 key 會產生 cycle，Firestore 序列化爆掉）
        result = dict(real_zones[0])
        if len(real_zones) > 1:
            result["zone_list"] = real_zones
        return result
    if non_real:
        return non_real[0]
    return None


# ── 台北市 GeoServer WFS：臨路寬度 ───────────────────────────────────────────

TAIPEI_ROADSIZE_TYPENAME = "Taipei:roadsize-TWD97"


def _point_to_line_dist(px, py, coords):
    """點 (px,py) 到折線 coords 的最短距離（TWD97 公尺）。"""
    min_d = float("inf")
    for i in range(len(coords) - 1):
        ax, ay = coords[i]
        bx, by = coords[i + 1]
        dx, dy = bx - ax, by - ay
        if dx == 0 and dy == 0:
            d = math.hypot(px - ax, py - ay)
        else:
            t = max(0, min(1, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
            d = math.hypot(px - (ax + t * dx), py - (ay + t * dy))
        if d < min_d:
            min_d = d
    return min_d


def query_road_width_taipei(lat: float, lng: float, address_hint: str = "") -> Optional[dict]:
    """
    座標 → 最近道路的路寬。
    address_hint: 物件地址，用來優先選同路名的道路。
    回傳 {"road_name": str, "road_width_m": float, "all_roads": [...]} 或 None。
    """
    x, y = wgs84_to_twd97(lat, lng)
    for half in (80, 150):
        bbox = f"{x - half},{y - half},{x + half},{y + half},{TAIPEI_SRS}"
        try:
            r = httpx.get(
                TAIPEI_WFS_URL,
                params={
                    "service": "WFS", "request": "GetFeature", "version": "1.0.0",
                    "outputFormat": "json", "typename": TAIPEI_ROADSIZE_TYPENAME,
                    "bbox": bbox, "maxFeatures": 20,
                },
                timeout=15, verify=False,
            )
            feats = (r.json() or {}).get("features", [])
        except Exception as e:
            logger.warning(f"query_road_width_taipei error: {e}")
            feats = []
        if feats:
            seen = set()
            all_roads = []
            for f in feats:
                p = f.get("properties", {})
                name = p.get("road_name1", "")
                width_str = (p.get("road_width") or "").replace("M", "").replace("m", "").strip()
                try:
                    width = float(width_str)
                except Exception:
                    continue
                key = f"{name}_{width}"
                if key in seen:
                    continue
                seen.add(key)
                dist = 9999.0
                geom = f.get("geometry", {})
                coords_list = geom.get("coordinates", [])
                if geom.get("type") == "MultiLineString":
                    for seg in coords_list:
                        dist = min(dist, _point_to_line_dist(x, y, seg))
                elif geom.get("type") == "LineString":
                    dist = _point_to_line_dist(x, y, coords_list)
                all_roads.append({"road_name": name, "road_width_m": width, "distance_m": round(dist, 1)})
            if all_roads:
                picked = all_roads[0]
                if address_hint:
                    stripped = re.sub(r"^.*區", "", address_hint)
                    addr_lane = re.search(r"(\d+巷)", stripped)
                    addr_alley = re.search(r"(\d+弄)", stripped)
                    addr_road_base = re.sub(r"\d+巷.*$", "", stripped)
                    lane_str = addr_lane.group(1) if addr_lane else ""
                    alley_str = addr_alley.group(1) if addr_alley else ""

                    all_roads.sort(key=lambda r: r.get("distance_m", 9999))
                    min_dist = all_roads[0]["distance_m"] if all_roads else 9999

                    def _priority(r):
                        name = r.get("road_name", "")
                        d = r.get("distance_m", 9999)
                        same_road = addr_road_base and addr_road_base in name
                        has_lane = lane_str and lane_str in name
                        has_alley = alley_str and alley_str in name
                        name_has_alley = "弄" in name
                        # 地址沒弄時：候選路名也不能有「弄」（否則是子巷，不是真正面對的路）
                        # 地址有弄時：候選路名必須含同弄
                        alley_match = has_alley if alley_str else (not name_has_alley)
                        # 0. 精確同路名（含段、巷，無額外弄層）→ 最優
                        if same_road and has_lane and alley_match:
                            return (0, d)
                        # 1. 不管哪路，較所有其他選擇都少 20m 以上
                        if d + 20 < min_dist or (d == min_dist and all(
                            r2.get("distance_m", 9999) >= d + 20
                            for r2 in all_roads if r2 is not r
                        )):
                            return (1, d)
                        # 2. 本路+巷一樣但含多餘弄層（e.g. 地址無弄但候選有弄）→ 次選
                        if same_road and has_lane:
                            return (2, d)
                        # 3. 距離最近
                        return (3, d)

                    all_roads.sort(key=_priority)
                    picked = all_roads[0]

                # Fallback：bbox 完全沒找到「同路+同巷」的候選，但 address_hint 有寫明巷名
                # → 用 CQL filter 直接查該「路+巷」的路寬（全台北該路段都查）
                if address_hint:
                    stripped2 = re.sub(r"^.*區", "", address_hint)
                    target_m = re.match(
                        r"([\u4e00-\u9fa5]+(?:路|街|大道)(?:[一二三四五六七八九十]段)?\d+巷)",
                        stripped2,
                    )
                    if target_m:
                        target_road = target_m.group(1)
                        already_found = any(r["road_name"] == target_road for r in all_roads)
                        if not already_found:
                            try:
                                fr = httpx.get(
                                    TAIPEI_WFS_URL,
                                    params={
                                        "service": "WFS", "request": "GetFeature", "version": "1.0.0",
                                        "outputFormat": "json", "typename": TAIPEI_ROADSIZE_TYPENAME,
                                        "CQL_FILTER": f"road_name1='{target_road}'",
                                        "maxFeatures": 5,
                                    },
                                    timeout=15, verify=False,
                                )
                                for f2 in (fr.json() or {}).get("features", []):
                                    p2 = f2.get("properties", {})
                                    w2 = (p2.get("road_width") or "").replace("M", "").replace("m", "").strip()
                                    try:
                                        w2f = float(w2)
                                    except Exception:
                                        continue
                                    entry = {
                                        "road_name": target_road, "road_width_m": w2f,
                                        "distance_m": 9999.0,
                                    }
                                    all_roads.append(entry)
                                    picked = entry   # 用 CQL 查到的同路巷覆蓋 bbox 選的
                                    break
                            except Exception as e:
                                logger.debug(f"CQL fallback 失敗: {e}")
                return {
                    "road_name": picked["road_name"],
                    "road_width_m": picked["road_width_m"],
                    "all_roads": all_roads,
                }
    return None


# ── 新北市（待實作） ─────────────────────────────────────────────────────────

def query_zoning_newtaipei(lat: float, lng: float) -> Optional[dict]:
    """新北市目前無已知公開 ArcGIS/GeoServer endpoint，回 None。"""
    return None


# ── NLSC：座標 → 段地號（輔助欄位，Phase 2 可用） ───────────────────────────

def query_section_parcel(lat: float, lng: float) -> Optional[dict]:
    """NLSC TownVillagePointQuery → 鄉鎮市區/段名/村里。"""
    try:
        r = httpx.get(
            f"https://api.nlsc.gov.tw/other/TownVillagePointQuery/{lng}/{lat}",
            timeout=10, verify=False,
        )
        # 回傳是 XML，簡單 regex 抽
        text = r.text
        def _x(tag):
            m = re.search(f"<{tag}>([^<]+)</{tag}>", text)
            return m.group(1).strip() if m else None
        if "<ctyName>" not in text:
            return None
        return {
            "city": _x("ctyName"),
            "district": _x("townName"),
            "section": _x("sectName"),
            "village": _x("villageName"),
        }
    except Exception as e:
        logger.warning(f"query_section_parcel error: {e}")
        return None


# ── 高層 API ─────────────────────────────────────────────────────────────────

def lookup_zoning_by_coord(lat: float, lng: float, city: str) -> dict:
    """
    給座標 + 城市，回傳 zoning 結果（與 lookup_zoning 介面對齊）。

    Returns:
        {
          "zoning": str | None,        # 標準名「第三種住宅區」
          "zoning_source": str,         # arcgis_taipei / arcgis_newtaipei / no_coord / unsupported_city / not_found
          "zoning_source_url": str | None,
          "zone_label": str | None,    # 原始簡稱「住3」
          "zone_code": str | None,     # 代碼「R3」
          "error": str | None,
        }
    """
    if not lat or not lng:
        return {"zoning": None, "zoning_source": "no_coord",
                "zoning_source_url": None, "zone_label": None, "zone_code": None,
                "error": "缺座標"}
    if city == "台北市":
        z = query_zoning_taipei(lat, lng)
        if z:
            return {
                "zoning": z["zone_name"],
                "zoning_source": "arcgis_taipei",
                "zoning_source_url": TAIPEI_PORTAL_URL,
                "zone_label": z["zone_label"],
                "zone_code": z["zone_code"],
                "original_zone": z.get("original_zone"),
                "zone_list": z.get("zone_list"),
                "error": None,
            }
        return {"zoning": None, "zoning_source": "not_found",
                "zoning_source_url": TAIPEI_PORTAL_URL, "zone_label": None,
                "zone_code": None, "error": "WFS 查詢無結果（座標可能在邊界）"}
    if city == "新北市":
        return {"zoning": None, "zoning_source": "unsupported_city",
                "zoning_source_url": None, "zone_label": None, "zone_code": None,
                "error": "新北市暫無公開 GIS endpoint，請手動輸入"}
    return {"zoning": None, "zoning_source": "unsupported_city",
            "zoning_source_url": None, "zone_label": None, "zone_code": None,
            "error": f"未支援的城市：{city}"}
