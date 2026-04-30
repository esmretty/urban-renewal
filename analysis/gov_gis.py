"""
政府公開 GIS API 整合：座標 → 土地使用分區。

目前覆蓋：
  - 台北市 zonegeo.udd.gov.taipei GeoServer WFS（typename: Taipei:ublock97-TWD97）
  - NLSC TownVillagePointQuery 取段地號（輔助）

未覆蓋（暫時手動輸入）：
  - 新北市
"""
import json
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
_REAL_ZONE_PREFIX = ("住", "商", "工", "農", "文教", "倉儲", "行政")
# 「保護區」「風景區」雖也是合法分區，但都更/危老不可建 → 不算實質可建分區
import re as _re
_REAL_ZONE_FULL_RE = _re.compile(r"^第[一二三四五]種(住宅|商業|工業|農業)區")
# 公共設施／道路用地等（地號上有，但對都更試算意義不同）
_NON_RESIDENTIAL = (
    "高速公路", "道路", "公園", "國小", "國中", "高中", "大學", "幼", "醫院", "市場",
    "停車", "變電", "電信", "機關", "宗教", "綠地", "兒童遊樂", "廣場",
)


def is_real_zone(usenam: str) -> bool:
    """是否為「實質都市計畫分區」（住/商/工 等可建蔽的類別）。
    含「住宅區」「住3」「第三種住宅區」「商業區」「第二種商業區」「工業區」等。
    排除：道路用地、公園用地、機關用地、保護區、風景區（不可建或非住）"""
    if not usenam:
        return False
    if any(usenam.startswith(p) for p in _REAL_ZONE_PREFIX):
        return True
    if _REAL_ZONE_FULL_RE.match(usenam):
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


# ── 新北市 ArcGIS（urban.planning.ntpc.gov.tw 政府服務） ──────────────────────
#
# Endpoint：https://arcgis.planning.ntpc.gov.tw/server/rest/services/NTPC_Urban
# Token：從 https://urban.planning.ntpc.gov.tw/NtpcUrbArcgisToken/urban_planning_ntpc_gov_tw.js
#       取得（網站 long-lived token）。如果失效需重抓。
NTPC_ARCGIS_BASE = "https://arcgis.planning.ntpc.gov.tw/server/rest/services/NTPC_Urban"
NTPC_TOKEN_URL = "https://urban.planning.ntpc.gov.tw/NtpcUrbArcgisToken/urban_planning_ntpc_gov_tw.js"
_NTPC_TOKEN_CACHE = {"token": None, "fetched_at": 0}


def _get_ntpc_token() -> str:
    """取 NTPC ArcGIS token（cache 1 小時）。失敗回空字串。"""
    import time as _t
    if _NTPC_TOKEN_CACHE["token"] and (_t.time() - _NTPC_TOKEN_CACHE["fetched_at"]) < 3600:
        return _NTPC_TOKEN_CACHE["token"]
    try:
        r = httpx.get(NTPC_TOKEN_URL, timeout=10, verify=False)
        m = re.search(r'NtpcUrbToken="([^"]+)"', r.text)
        if m:
            _NTPC_TOKEN_CACHE["token"] = m.group(1)
            _NTPC_TOKEN_CACHE["fetched_at"] = _t.time()
            return m.group(1)
    except Exception as e:
        logger.warning(f"_get_ntpc_token error: {e}")
    return ""


_NUMERAL_LZ4 = {"一", "二", "三", "四", "五"}


def _build_ntpc_zone_entry(a: dict) -> Optional[dict]:
    """把 NTPC LandUse_WMS 一筆 feature attributes 轉成標準 zone dict。
    回 None 表 LZ3 為空無法判斷。

    LZ4 解析規則：
      - 「一/二/三/四/五」→ 第N種住宅區 / 第N種商業區（中英文數字 LZ4 才合成）
      - 「附2」→ 新店都市計畫管制注記，依 LZ7=300% 視為第四種住宅區
      - 其他（如「住」「住(再)」「機廿九」「公(三十三)」）→ LZ4 是冗餘標籤，zone_name 直接用 LZ3
    """
    lz3 = (a.get("LZ3") or "").strip()
    lz4 = (a.get("LZ4") or "").strip()
    if not lz3:
        return None
    far_rate = a.get("LZ7")
    if lz3 == "住宅區" and lz4 in _NUMERAL_LZ4:
        zone_name = f"第{lz4}種住宅區"
        zone_label = f"住{lz4}"
        zone_code = {"一": "R1", "二": "R2", "三": "R3", "四": "R4", "五": "R5"}.get(lz4)
    elif lz3 == "住宅區" and lz4 == "附2":
        # 新店都市計畫「住宅區附2」實質容積 300% = 視為第四種住宅區
        zone_name = "第四種住宅區"
        zone_label = "住四(附2)"
        zone_code = "R4"
    elif lz3 == "商業區" and lz4 in _NUMERAL_LZ4:
        zone_name = f"第{lz4}種商業區"
        zone_label = f"商{lz4}"
        zone_code = {"一": "C1", "二": "C2", "三": "C3", "四": "C4"}.get(lz4)
    else:
        # 其他情況（含 lz3=住宅區 但 lz4=「住」/「住(再)」等冗餘標籤、保護區、機關用地、商業區無細分等）
        zone_name = lz3
        zone_label = lz3
        zone_code = None
    return {
        "zone_name": zone_name,
        "zone_label": zone_label,
        "zone_code": zone_code,
        "original_zone": f"{lz3}{lz4}" if lz4 else lz3,
        "zone_list": [zone_name] if zone_name else [],
        "plan_name": a.get("LZ1"),
        "coverage_rate": a.get("LZ5") or a.get("LZ6"),
        "far_rate": far_rate,
    }


def query_zoning_newtaipei(lat: float, lng: float) -> Optional[dict]:
    """新北市座標 → 都市計畫使用分區。
    回 {"zone_name", "zone_label", "zone_code", "original_zone", "zone_list"} 或 None。

    LandUse_WMS layer 0 欄位：
      LZ3 = 分區（住宅區/商業區...） — 主要欄位
      LZ4 = 細分（一/二/三...，如有）
      LZ5/LZ6 = 建蔽率%, LZ7 = 容積率%
      LZ1 = 都市計畫名稱（背景資訊）

    策略：座標可能落在「道路 / 公園 / 公設」等非可建分區（geocode 不準，591 給的點偏到馬路上）。
    用 envelope 多半徑試查（10m → 30m → 60m），優先回實質分區（住/商/工/...）。
    全找不到實質分區才回非實質（道路/公設）。
    """
    token = _get_ntpc_token()
    if not token:
        return None
    # WGS84 約略換算：1° 緯度 ~111000m；經度需 cos(lat) 修正
    deg_per_m_lat = 1 / 111000
    import math as _m
    deg_per_m_lng = 1 / (111000 * max(_m.cos(_m.radians(lat)), 0.1))
    real_zones = []
    non_real = []
    seen_keys = set()
    for half_m in (10, 30, 60):
        dlat = half_m * deg_per_m_lat
        dlng = half_m * deg_per_m_lng
        envelope = {
            "xmin": lng - dlng, "ymin": lat - dlat,
            "xmax": lng + dlng, "ymax": lat + dlat,
            "spatialReference": {"wkid": 4326},
        }
        try:
            r = httpx.get(
                f"{NTPC_ARCGIS_BASE}/LandUse_WMS/MapServer/0/query",
                params={
                    "geometry": json.dumps(envelope),
                    "geometryType": "esriGeometryEnvelope",
                    "inSR": 4326,
                    "spatialRel": "esriSpatialRelIntersects",
                    "outFields": "LZ1,LZ3,LZ4,LZ5,LZ6,LZ7",
                    "returnGeometry": "false",
                    "f": "json",
                    "token": token,
                },
                timeout=15, verify=False,
            )
            data = r.json() or {}
            feats = data.get("features") or []
        except Exception as e:
            logger.warning(f"query_zoning_newtaipei error (half_m={half_m}): {e}")
            continue
        for f in feats:
            a = f.get("attributes") or {}
            entry = _build_ntpc_zone_entry(a)
            if not entry:
                continue
            # 去重 — 同 zone_label 不重複加
            key = entry.get("original_zone") or entry.get("zone_label")
            if key in seen_keys:
                continue
            seen_keys.add(key)
            if is_real_zone(entry["zone_label"]):
                real_zones.append(entry)
            else:
                non_real.append(entry)
        if real_zones:
            break  # 找到實質分區即停止擴大
    if real_zones:
        # 多塊實質分區命中：優先取容積率最高（300% > 280% > 50%）
        # 容積率高 = 真正可開發地塊；低容積率多半是套圖殘餘或邊角
        def _far(z):
            try:
                return float(z.get("far_rate") or 0)
            except (TypeError, ValueError):
                return 0
        real_zones.sort(key=_far, reverse=True)
        # zone_list dedup by zone_name（保留順序）：同名 polygon 不重複列
        # ArcGIS 同基地常切多塊 polygon 但 zone_name 一樣（細部計畫拼圖殘餘）
        seen = set()
        unique_zones = []
        for z in real_zones:
            zn = z.get("zone_name")
            if zn and zn not in seen:
                seen.add(zn)
                unique_zones.append(z)
        result = dict(unique_zones[0])
        if len(unique_zones) > 1:
            result["zone_list"] = [z["zone_name"] for z in unique_zones]
        return result
    if non_real:
        return non_real[0]
    return None


def query_road_width_newtaipei(lat: float, lng: float, address_hint: str = "") -> Optional[dict]:
    """新北市座標 → 最近道路的路寬。
    回 {"road_name", "road_width_m", "all_roads"} 或 None。

    NtpcRoadWidth layer 0 (NtpcCenterLine) 欄位：
      RoadName, RoadWidth (公尺), RoadKind, RoadNum
    """
    token = _get_ntpc_token()
    if not token:
        return None
    try:
        # ~80m envelope 找鄰近路 (lat 0.0007 ≈ 78m, lng 0.0008 ≈ 80m at TW lat)
        delta = 0.0008
        r = httpx.get(
            f"{NTPC_ARCGIS_BASE}/NtpcRoadWidth/MapServer/0/query",
            params={
                "where": "1=1",
                "geometry": json.dumps({
                    "xmin": lng - delta, "ymin": lat - delta,
                    "xmax": lng + delta, "ymax": lat + delta,
                }),
                "geometryType": "esriGeometryEnvelope",
                "inSR": 4326,
                "outSR": 4326,
                "outFields": "RoadName,RoadWidth,RoadKind,RoadNum",
                "returnGeometry": "true",
                "f": "json",
                "token": token,
            },
            timeout=15, verify=False,
        )
        data = r.json() or {}
        feats = data.get("features") or []
        if not feats:
            return None
        # 計算每條路距離：geometry 是 polyline (paths)
        # 簡化：用 envelope/bbox center 跟我們的 lat/lng 差距估
        all_roads = []
        seen = set()
        for f in feats:
            attrs = f.get("attributes") or {}
            name = (attrs.get("RoadName") or "").strip()
            width = attrs.get("RoadWidth")
            if not name or width is None:
                continue
            try: width = float(width)
            except Exception: continue
            key = (name, width)
            if key in seen:
                continue
            seen.add(key)
            # 距離估計（取 polyline 第一段中點）
            geom = f.get("geometry") or {}
            paths = geom.get("paths") or []
            if paths and paths[0]:
                pts = paths[0]
                mid = pts[len(pts) // 2]
                dx = (mid[0] - lng) * 101000   # ~1 lng° ≈ 101km at TW lat
                dy = (mid[1] - lat) * 111000
                dist = (dx * dx + dy * dy) ** 0.5
            else:
                dist = 9999.0
            all_roads.append({
                "road_name": name,
                "road_width_m": width,
                "distance_m": round(dist, 1),
            })
        if not all_roads:
            return None
        # 排序 + 套用 address_hint 優先（同台北邏輯）
        all_roads.sort(key=lambda r: r.get("distance_m", 9999))
        picked = all_roads[0]
        if address_hint:
            stripped = re.sub(r"^.*區", "", address_hint)
            for r in all_roads:
                rn = r.get("road_name") or ""
                # 路+巷精確 match 優先
                if rn and rn in stripped:
                    picked = r
                    break
        return {
            "road_name": picked["road_name"],
            "road_width_m": picked["road_width_m"],
            "all_roads": all_roads,
        }
    except Exception as e:
        logger.warning(f"query_road_width_newtaipei error: {e}")
        return None


# ── NLSC：座標 → 段地號（輔助欄位，Phase 2 可用） ───────────────────────────

def _wgs84_to_3857(lat: float, lng: float) -> tuple:
    """WGS84 (EPSG:4326) → Web Mercator (EPSG:3857) 公尺座標。"""
    x = lng * 20037508.34 / 180.0
    y = math.log(math.tan(math.radians(90 + lat) / 2)) * 6378137.0
    return x, y


def fetch_zoning_map_image_newtaipei(
    lat: float, lng: float, output_path: str, half_radius_m: int = 150,
) -> bool:
    """新北市：取得**4 層合成圖** PNG。

    層次（由下往上）：
    1. NLSC EMAP WMS 底圖（含路名 + 街道格網）
    2. NTPC ArcGIS LandUse_WMS export（透明 + 隱藏標籤）alpha 0.5 半透明色塊
    3. NTPC ArcGIS NtpcRoadWidth query → PIL 畫每條路「路名 寬度m」白底黑字標籤
    4. PIL 中心紅點（物件位置，含黑色描邊）

    Args:
        lat, lng: WGS84 座標
        output_path: 存圖檔絕對路徑
        half_radius_m: bbox 半徑（預設 150m）

    Returns:
        True = 成功（至少底圖寫入）；False = 全失敗
    """
    import io as _io
    from pathlib import Path
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        logger.warning("PIL 未安裝，無法合成新北地籍圖")
        return False

    token = _get_ntpc_token()
    if not token:
        logger.warning("NTPC token 取得失敗，無法 export 地圖")
        return False
    cx, cy = _wgs84_to_3857(lat, lng)
    img_size = 800
    xmin, ymin = cx - half_radius_m, cy - half_radius_m
    xmax, ymax = cx + half_radius_m, cy + half_radius_m

    def to_px(mx, my):
        return (
            (mx - xmin) / (2 * half_radius_m) * img_size,
            (ymax - my) / (2 * half_radius_m) * img_size,
        )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # ── 層 1：NLSC EMAP 底圖 ─────────────────────────────────────────────
    try:
        r = httpx.get(
            "https://wms.nlsc.gov.tw/wms",
            params={
                "REQUEST": "GetMap", "VERSION": "1.1.1",
                "LAYERS": "EMAP", "STYLES": "",
                "FORMAT": "image/png", "SRS": "EPSG:3857",
                "BBOX": f"{xmin},{ymin},{xmax},{ymax}",
                "WIDTH": img_size, "HEIGHT": img_size,
            },
            timeout=20, verify=False,
        )
        if r.status_code != 200 or len(r.content) < 1000:
            logger.warning(f"NLSC EMAP 底圖失敗 http={r.status_code} size={len(r.content)}")
            return False
        base = Image.open(_io.BytesIO(r.content)).convert("RGBA")
    except Exception as e:
        logger.warning(f"NLSC EMAP 底圖例外: {e}")
        return False

    # ── 層 2：NTPC LandUse 透明 + 無 label ──────────────────────────────
    landuse_img = None
    try:
        dyn = json.dumps([{
            "id": 0,
            "source": {"type": "mapLayer", "mapLayerId": 0},
            "drawingInfo": {"showLabels": False},
        }])
        r = httpx.get(
            f"{NTPC_ARCGIS_BASE}/LandUse_WMS/MapServer/export",
            params={
                "bbox": f"{xmin},{ymin},{xmax},{ymax}",
                "bboxSR": 3857, "imageSR": 3857,
                "size": f"{img_size},{img_size}", "format": "png", "dpi": 96,
                "transparent": "true", "dynamicLayers": dyn,
                "f": "image", "token": token,
            },
            timeout=15, verify=False,
        )
        if r.status_code == 200 and len(r.content) > 500:
            ld = Image.open(_io.BytesIO(r.content)).convert("RGBA")
            ld_a = ld.split()[3].point(lambda v: int(v * 0.5))
            ld.putalpha(ld_a)
            landuse_img = ld
    except Exception as e:
        logger.warning(f"NTPC LandUse export 例外（不影響底圖）: {e}")

    composed = (
        Image.alpha_composite(base, landuse_img) if landuse_img else base
    )

    # ── 層 3：路名+路寬標籤 ─────────────────────────────────────────────
    roads = []
    try:
        from math import atan, exp, pi
        lat_min = (atan(exp(ymin / 6378137.0)) * 360 / pi) - 90
        lat_max = (atan(exp(ymax / 6378137.0)) * 360 / pi) - 90
        lng_min = xmin / 20037508.34 * 180
        lng_max = xmax / 20037508.34 * 180
        envelope = json.dumps({
            "xmin": lng_min, "ymin": lat_min,
            "xmax": lng_max, "ymax": lat_max,
        })
        rr = httpx.get(
            f"{NTPC_ARCGIS_BASE}/NtpcRoadWidth/MapServer/0/query",
            params={
                "where": "1=1",
                "geometry": envelope,
                "geometryType": "esriGeometryEnvelope",
                "inSR": 4326, "outSR": 3857,
                "outFields": "RoadName,RoadWidth",
                "returnGeometry": "true",
                "f": "json", "token": token,
            },
            timeout=15, verify=False,
        )
        for f in (rr.json() or {}).get("features", []):
            attrs = f.get("attributes") or {}
            paths = (f.get("geometry") or {}).get("paths") or []
            if not paths:
                continue
            roads.append({
                "name": (attrs.get("RoadName") or "").strip(),
                "width": attrs.get("RoadWidth"),
                "paths": paths,
            })
    except Exception as e:
        logger.warning(f"NTPC RoadWidth query 例外: {e}")

    draw = ImageDraw.Draw(composed)
    font = None
    for fp in (
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "C:/Windows/Fonts/msjh.ttc",
    ):
        try:
            font = ImageFont.truetype(fp, 14)
            break
        except (OSError, IOError):
            continue
    if font is None:
        font = ImageFont.load_default()

    seen_label = set()
    for road in roads:
        if not road["name"] or road["width"] is None:
            continue
        label = f"{road['name']} {road['width']}m"
        for path_pts in road["paths"]:
            if len(path_pts) < 2:
                continue
            mid_idx = len(path_pts) // 2
            mx, my = path_pts[mid_idx]
            px, py = to_px(mx, my)
            if not (0 <= px < img_size and 0 <= py < img_size):
                continue
            key = (label, int(px // 80), int(py // 80))
            if key in seen_label:
                continue
            seen_label.add(key)
            bbox_t = draw.textbbox((px, py), label, font=font)
            draw.rectangle(
                (bbox_t[0] - 3, bbox_t[1] - 2, bbox_t[2] + 3, bbox_t[3] + 2),
                fill=(255, 255, 255, 235),
            )
            draw.text((px, py), label, fill=(20, 20, 20, 255), font=font)

    # ── 層 4：中心紅點 ─────────────────────────────────────────────────
    cpx, cpy = img_size / 2, img_size / 2
    for r_pin, color in ((14, (0, 0, 0, 255)), (10, (220, 30, 30, 255))):
        draw.ellipse(
            (cpx - r_pin, cpy - r_pin, cpx + r_pin, cpy + r_pin),
            fill=color,
        )

    try:
        composed.convert("RGB").save(output_path, format="PNG")
        return True
    except Exception as e:
        logger.warning(f"composed 寫檔失敗: {e}")
        return False


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
        z = query_zoning_newtaipei(lat, lng)
        if z:
            return {
                "zoning": z["zone_name"],
                "zoning_source": "arcgis_newtaipei",
                "zoning_source_url": "https://urban.planning.ntpc.gov.tw/NtpcURInfo/",
                "zone_label": z["zone_label"],
                "zone_code": z["zone_code"],
                "original_zone": z.get("original_zone"),
                "zone_list": z.get("zone_list"),
                "error": None,
            }
        return {"zoning": None, "zoning_source": "not_found",
                "zoning_source_url": "https://urban.planning.ntpc.gov.tw/NtpcURInfo/",
                "zone_label": None, "zone_code": None,
                "error": "新北 ArcGIS 查詢無結果（可能 token 失效或座標在邊界）"}
    return {"zoning": None, "zoning_source": "unsupported_city",
            "zoning_source_url": None, "zone_label": None, "zone_code": None,
            "error": f"未支援的城市：{city}"}
