"""
台北市建物屬性查詢：樓高 / 結構。

資料源：台北市都發局 ArcGIS GISDB 服務 layer 19 (建物_Build)
  https://www.historygis.udd.gov.taipei/arcgis/rest/services/Urban/GISDB/MapServer/19

欄位：
  Build_NO  (smallint) — 樓層數（含地上）
  Build_STR (str, 1-2) — 結構類別，例如 "R"=鋼筋混凝土、"T"=鐵皮/臨時、"C"=磚造
  TerrainID (str)      — 分區代碼（參考）

用法：
  from analysis.building_info import query_building_floors
  r = query_building_floors(24.9944539, 121.5439555)
  # r = {"floors": 4, "structure": "R", "label": "4R", "candidates": [...]}
"""
import logging
import math
from typing import Optional, List, Dict

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

GISDB_BLDG_URL = (
    "https://www.historygis.udd.gov.taipei/arcgis/rest/services/"
    "Urban/GISDB/MapServer/19/query"
)
# layer 25 = 道路(面)_Road — 作為「這個地號是不是路」的依據
GISDB_ROAD_URL = (
    "https://www.historygis.udd.gov.taipei/arcgis/rest/services/"
    "Urban/GISDB/MapServer/25/query"
)
# Urban/Land layer 5 = 地籍(<=5000) — 含 LANDCODE 地號 polygon
PARCEL_URL = (
    "https://www.historygis.udd.gov.taipei/arcgis/rest/services/"
    "Urban/Land/MapServer/5/query"
)

STRUCTURE_LABELS = {
    "R": "鋼筋混凝土",
    "C": "加強磚造 / 磚造",
    "T": "鐵皮 / 臨時結構",
    "S": "鋼骨",
    "W": "木造",
}


def _wgs84_to_webmercator(lat: float, lng: float):
    x = lng * 20037508.34 / 180.0
    y = math.log(math.tan((90 + lat) * math.pi / 360.0)) * 20037508.34 / math.pi
    return x, y


def query_building_floors(
    lat: float, lng: float, *, buffer_m: float = 3.0, timeout: float = 6.0
) -> Optional[Dict]:
    """
    以 (lat, lng) 為中心、buffer_m 公尺為半邊長的方框查詢建物屬性。

    回傳：
      {
        "floors": int,           # 主要建物樓數（過濾 T 鐵皮後取最大 Build_NO）
        "structure": str,        # 結構代碼 "R"/"C"/...
        "structure_label": str,  # 中文標籤
        "label": str,            # e.g. "4R"
        "candidates": [          # 所有命中的建物（含周邊 buffer 內的）
            {"floors": int, "structure": str, "label": str, "terrain_id": str},
            ...
        ]
      }
    若查不到任何建物，回傳 None。
    """
    x, y = _wgs84_to_webmercator(lat, lng)
    env = f"{x - buffer_m},{y - buffer_m},{x + buffer_m},{y + buffer_m}"
    params = {
        "geometry": env,
        "geometryType": "esriGeometryEnvelope",
        "inSR": "102100",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "Build_NO,Build_STR,TerrainID",
        "returnGeometry": "false",
        "f": "json",
    }
    try:
        resp = requests.get(GISDB_BLDG_URL, params=params, timeout=timeout, verify=False)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("query_building_floors error: %s", e)
        return None

    features = data.get("features") or []
    if not features:
        return None

    cands: List[Dict] = []
    for f in features:
        a = f.get("attributes") or {}
        floors = a.get("Build_NO")
        struct = a.get("Build_STR") or ""
        if floors is None:
            continue
        cands.append({
            "floors": int(floors),
            "structure": struct,
            "label": f"{int(floors)}{struct}",
            "terrain_id": a.get("TerrainID"),
        })
    if not cands:
        return None

    # 主棟：先排除 T（鐵皮/臨時），再取 floors 最大者；若全是 T 則全部納入
    non_t = [c for c in cands if c["structure"] != "T"]
    main_pool = non_t or cands
    main = max(main_pool, key=lambda c: c["floors"])

    return {
        "floors": main["floors"],
        "structure": main["structure"],
        "structure_label": STRUCTURE_LABELS.get(main["structure"], main["structure"]),
        "label": main["label"],
        "candidates": cands,
    }


def _webmerc_to_wgs84(x: float, y: float):
    lng = x / 20037508.34 * 180.0
    lat = y / 20037508.34 * 180.0
    lat = 180.0 / math.pi * (2 * math.atan(math.exp(lat * math.pi / 180.0)) - math.pi / 2)
    return lat, lng


def _query_buildings_with_geom(
    lat: float, lng: float, *, radius_m: float = 200.0, timeout: float = 20.0
):
    """查詢 (lat,lng) 周圍 radius_m 內所有建物（含幾何）。"""
    x, y = _wgs84_to_webmercator(lat, lng)
    env = f"{x - radius_m},{y - radius_m},{x + radius_m},{y + radius_m}"
    params = {
        "geometry": env,
        "geometryType": "esriGeometryEnvelope",
        "inSR": "102100",
        "outSR": "102100",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "Build_NO,Build_STR,TerrainID,OBJECTID",
        "returnGeometry": "true",
        "f": "json",
    }
    resp = requests.get(GISDB_BLDG_URL, params=params, timeout=timeout, verify=False)
    resp.raise_for_status()
    return resp.json().get("features") or []


def _query_roads_with_geom(lat: float, lng: float, radius_m: float = 250.0, timeout: float = 20.0):
    """查道路(面) polygon，作為「這個地號是不是路」的依據。"""
    x, y = _wgs84_to_webmercator(lat, lng)
    env = f"{x - radius_m},{y - radius_m},{x + radius_m},{y + radius_m}"
    params = {
        "geometry": env, "geometryType": "esriGeometryEnvelope",
        "inSR": "102100", "outSR": "102100",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "OBJECTID", "returnGeometry": "true", "f": "json",
    }
    try:
        resp = requests.get(GISDB_ROAD_URL, params=params, timeout=timeout, verify=False)
        resp.raise_for_status()
        return resp.json().get("features") or []
    except Exception as e:
        logger.warning("query_roads error: %s", e)
        return []


def _query_parcels_with_geom(lat: float, lng: float, radius_m: float = 250.0, timeout: float = 25.0):
    """查地號 polygon (Urban/Land/5 地籍 <=5000)。"""
    x, y = _wgs84_to_webmercator(lat, lng)
    env = f"{x - radius_m},{y - radius_m},{x + radius_m},{y + radius_m}"
    params = {
        "geometry": env, "geometryType": "esriGeometryEnvelope",
        "inSR": "102100", "outSR": "102100",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "LANDCODE,SECTION1,SECTION2,OBJECTID,TYPE",
        "returnGeometry": "true", "f": "json",
    }
    try:
        resp = requests.get(PARCEL_URL, params=params, timeout=timeout, verify=False)
        resp.raise_for_status()
        return resp.json().get("features") or []
    except Exception as e:
        logger.warning("query_parcels error: %s", e)
        return []


def find_redevelopment_block(
    lat: float, lng: float, *,
    max_floors: int = 4,
    radius_m: float = 250.0,
    adjacency_m: float = 0.5,     # 地號 polygon 邊界相接（一般共用邊距離=0，給 0.5m 容錯）
    min_building_area_sqm: float = 30.0,
    exclude_structures=("T",),    # 計算 max_floors 時排除鐵皮/臨時
    road_coverage_threshold: float = 0.4,  # 地號 40% 以上被道路面覆蓋 → 視為路
) -> Optional[Dict]:
    """
    從 (lat,lng) 所在地號 flood-fill「潛在重建基地」：

    基本單位：地號（而非建築物）
    納入規則：
      - seed = 目標點所在地號
      - 相鄰地號 (adjacency_m 內) 的鄰居，若符合下面條件就納入：
          a) 不是道路（road polygon 覆蓋比例 < road_coverage_threshold）
          b) 地號上最高的非 T 建物樓數 ≤ max_floors（空地或防火巷納入；5F+ 擋下）
    """
    try:
        from shapely.geometry import Polygon, Point
        from shapely.ops import unary_union
    except ImportError:
        logger.error("shapely not installed")
        return None

    # 1) 取資料
    parcel_feats = _query_parcels_with_geom(lat, lng, radius_m=radius_m)
    bldg_feats = _query_buildings_with_geom(lat, lng, radius_m=radius_m)
    road_feats = _query_roads_with_geom(lat, lng, radius_m=radius_m)

    if not parcel_feats:
        logger.warning("no parcels returned")
        return None

    # 2) 建物 → shapely polygons（保留 Build_NO / Build_STR）
    buildings = []
    for f in bldg_feats:
        a = f.get("attributes") or {}
        floors = a.get("Build_NO")
        struct = a.get("Build_STR") or ""
        rings = (f.get("geometry") or {}).get("rings") or []
        if floors is None or not rings:
            continue
        try:
            poly = Polygon(rings[0], holes=rings[1:])
            if not poly.is_valid:
                poly = poly.buffer(0)
            if poly.is_empty or poly.area < min_building_area_sqm:
                continue
        except Exception:
            continue
        buildings.append({"floors": int(floors), "structure": struct, "poly": poly})

    # 3) 道路面 → union
    road_polys = []
    for f in road_feats:
        rings = (f.get("geometry") or {}).get("rings") or []
        if not rings:
            continue
        try:
            poly = Polygon(rings[0], holes=rings[1:])
            if not poly.is_valid:
                poly = poly.buffer(0)
            if not poly.is_empty:
                road_polys.append(poly)
        except Exception:
            continue
    road_union = unary_union(road_polys) if road_polys else None

    # 4) 地號 → shapely polygons + 附加屬性
    parcels = []
    for f in parcel_feats:
        a = f.get("attributes") or {}
        landcode = a.get("LANDCODE") or ""
        section = (a.get("SECTION2") or a.get("SECTION1") or "").strip()
        rings = (f.get("geometry") or {}).get("rings") or []
        if not rings:
            continue
        try:
            poly = Polygon(rings[0], holes=rings[1:])
            if not poly.is_valid:
                poly = poly.buffer(0)
            if poly.is_empty:
                continue
        except Exception:
            continue

        # 此地號被建物覆蓋的最高樓層
        # 連棟建物 polygon 常橫跨多個地號，所以只要地號內被建物覆蓋「有意義的比例」就記入
        # (>= 10m² 或 >= 15% 地號面積) — 兩條件任一成立即算
        parcel_max_floors = 0
        for b in buildings:
            if b["structure"] in exclude_structures:
                continue
            if not poly.intersects(b["poly"]):
                continue
            inter_area = poly.intersection(b["poly"]).area
            if inter_area == 0:
                continue
            if inter_area >= 10.0 or (poly.area > 0 and inter_area / poly.area >= 0.15):
                parcel_max_floors = max(parcel_max_floors, b["floors"])

        # 道路覆蓋率
        road_cover = 0.0
        if road_union is not None and road_union.intersects(poly):
            road_cover = road_union.intersection(poly).area / poly.area if poly.area > 0 else 0.0
        is_road = road_cover >= road_coverage_threshold

        parcels.append({
            "landcode": landcode,
            "section": section,
            "poly": poly,
            "rings": rings,
            "max_floors": parcel_max_floors,
            "road_cover": round(road_cover, 3),
            "is_road": is_road,
            "area_sqm": poly.area,
        })

    if not parcels:
        return None

    # 5) seed = 點所在地號；若點剛好在兩塊邊界上，取最近的非 road
    seed_x, seed_y = _wgs84_to_webmercator(lat, lng)
    seed_pt = Point(seed_x, seed_y)
    seed_idx = None
    for i, p in enumerate(parcels):
        if p["poly"].contains(seed_pt) and not p["is_road"]:
            seed_idx = i
            break
    if seed_idx is None:
        non_road = [(i, p) for i, p in enumerate(parcels) if not p["is_road"]]
        if not non_road:
            return None
        seed_idx, _ = min(non_road, key=lambda kv: kv[1]["poly"].distance(seed_pt))

    # 6) BFS — 地號邊界相接為鄰，符合條件才納入
    buf_polys = [p["poly"].buffer(adjacency_m) for p in parcels]
    visited = set()
    queue = [seed_idx]
    included: List[int] = []
    while queue:
        i = queue.pop(0)
        if i in visited:
            continue
        visited.add(i)
        p = parcels[i]
        is_seed = (i == seed_idx)
        if not is_seed:
            if p["is_road"]:
                continue
            if p["max_floors"] > max_floors:
                continue
        included.append(i)
        # 擴展相鄰地號
        for j, bj in enumerate(buf_polys):
            if j in visited:
                continue
            if buf_polys[i].intersects(bj):
                queue.append(j)

    # 7) 匯整結果
    polys = [parcels[i]["poly"] for i in included]
    union = unary_union(polys)
    total_area = union.area

    rings_wgs84 = []
    rings_webmerc = []
    parcel_info = []
    max_floors_hist = {}
    for i in included:
        p = parcels[i]
        rings_webmerc.append(p["rings"])
        rings_wgs84.append([
            [list(_webmerc_to_wgs84(x, y))[::-1] for (x, y) in ring]
            for ring in p["rings"]
        ])
        parcel_info.append({
            "landcode": p["landcode"],
            "section": p["section"],
            "max_floors": p["max_floors"],
            "area_sqm": round(p["area_sqm"], 1),
            "road_cover": p["road_cover"],
        })
        mf = p["max_floors"]
        key = f"{mf}F" if mf > 0 else "空地/防火巷"
        max_floors_hist[key] = max_floors_hist.get(key, 0) + 1

    seed = parcels[seed_idx]
    return {
        "seed_landcode": seed["landcode"],
        "seed_section": seed["section"],
        "parcel_count": len(included),
        "total_area_sqm": round(total_area, 1),
        "total_area_ping": round(total_area / 3.30579, 1),
        "rings_wgs84": rings_wgs84,
        "rings_webmerc": rings_webmerc,
        "parcels": parcel_info,
        "max_floors_hist": max_floors_hist,
        "max_floors_threshold": max_floors,
    }


if __name__ == "__main__":
    import json, sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    mode = sys.argv[1] if len(sys.argv) > 1 else "floor"
    if mode == "block":
        lat = float(sys.argv[2]); lng = float(sys.argv[3])
        max_f = int(sys.argv[4]) if len(sys.argv) > 4 else 4
        r = find_redevelopment_block(lat, lng, max_floors=max_f)
        if r:
            r.pop("rings_wgs84", None); r.pop("rings_webmerc", None)
        try:
            print(json.dumps(r, ensure_ascii=False, indent=2))
        except UnicodeEncodeError:
            sys.stdout.buffer.write(json.dumps(r, ensure_ascii=False, indent=2).encode("utf-8"))
    else:
        if len(sys.argv) >= 3:
            lat = float(sys.argv[1]); lng = float(sys.argv[2])
        else:
            lat, lng = 24.9944539, 121.5439555
        print(json.dumps(query_building_floors(lat, lng), ensure_ascii=False, indent=2))
