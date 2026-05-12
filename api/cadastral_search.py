"""地塊搜尋 router — 地圖模式「輸入區+段+地號 → 飛到該地塊」功能。

隔離原則（同 gis_overlay.py 設計）：
  - 整個 module 自包含，所有地塊查詢邏輯都在這個 file
  - app.py 只用 1 行 `app.include_router(cadastral_search.router)` 掛載
  - revert 時刪本 file + 拿掉那 1 行 include 即可，其他 file 完全不動

支援範圍：
  - 台北 8 區：透過 臺北地政局 ArcGIS REST (maps.land.gov.taipei) 拿
    (Layer 1 段界 → 段名清單；Layer 2 地號 → 含 AA10 公告面積 m² 的官方資料)
    早期版本用 zonegeo.udd.gov.taipei GeoServer 但無面積欄位，2026-05 換 land.gov.taipei
  - 新北 4 區（板橋/新店/中和/永和）：透過 NTPC ArcGIS query 配 NtpcURInfo session token
    打 NTPC_Urban/Land/MapServer/0/query；段名清單從 _ntpc_cadastral_data 拿
    (一次性 enumerate 出來，hardcode 進去；段名年單位才會變)

API:
  - GET /api/cadastral_search/segments?city=&district= → 回該區段名清單
  - GET /api/cadastral_search/lookup?city=&district=&segment=&landno=
    → 回 {ok, center, bbox, polygon, segment, landno, district, area_sqm, area_ping}
    → 找不到回 {ok: false, reason: '...'}
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import get_current_user
from api._ntpc_cadastral_data import NTPC_DISTRICT_BBOX_TWD97, NTPC_SEGMENTS

# disk cache：台北段名 dump 到 data/cache/tpe_segments/<district>.json
# 段名年單位才變，避免每次 server restart 第一個 user 等 3-15 秒 ArcGIS cold fetch
_TPE_SEGMENTS_CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "cache" / "tpe_segments"

logger = logging.getLogger(__name__)
router = APIRouter()

# 臺北地政局自家 ArcGIS（公開，無 token；地政雲背後就是這個）
# Layer 0 LAND (group), 1 段界, 2 地號 (含 AA10 公告面積), 3 地段範圍
_TPE_LAND_ARCGIS_URL = "https://maps.land.gov.taipei/server/rest/services/Tiled3857/Landtest/MapServer"

# 台北 12 區代碼 (從地政雲 dropdown 抄)：AA46 欄位值
_TPE_DISTRICT_CODE: dict[str, str] = {
    "松山區": "01",
    "大安區": "02",
    "中正區": "03",
    "萬華區": "05",
    "大同區": "09",
    "中山區": "10",
    "文山區": "11",
    "南港區": "13",
    "內湖區": "14",
    "士林區": "15",
    "北投區": "16",
    "信義區": "17",
}
_TPE_DISTRICTS = set(_TPE_DISTRICT_CODE.keys())
_TPE_DISTRICT_CODE_TO_NAME = {v: k for k, v in _TPE_DISTRICT_CODE.items()}

import re as _re
# 地號合法格式：純數字 or 數字-數字 (e.g., "1", "1-1", "123-45")
_LANDNO_RE = _re.compile(r"^\d{1,5}(-\d{1,5})?$")

_NTPC_LAND_FIELD_LANDNO = "NTPCUPGIS_SDE.NTPCGIS2.%Land.LANDNO"
_NTPC_LAND_FIELD_SECTNAME = "NTPCUPGIS_SDE.NTPCGIS2.%Land.SECTNAME"
_NTPC_LAND_FIELD_AREA = "NTPCUPGIS_SDE.NTPCGIS2.%Land.AA10"
_NTPC_LAND_FIELD_SCNO = "NTPCUPGIS_SDE.NTPCGIS2.%Land.SCNO"
_NTPC_LAND_FIELD_PLAND = "NTPCUPGIS_SDE.NTPCGIS2.%Land.PLAND"          # 公有 / 私有
_NTPC_LAND_FIELD_PLAND_NAME = "NTPCUPGIS_SDE.NTPCGIS2.%Land.PLANDNAME"  # 管理機關 (公有時)
_NTPC_LAND_FIELD_PLAND_OWNER = "NTPCUPGIS_SDE.NTPCGIS2.%Land.PLANDLNAME"  # 所有人 (公有時)
# 註：NTPC URInfo 地籍 schema 列出 AA05/AA16/AA17 但實際 query 全 null，不取

# 段名 → (AA46, AA48) memory cache：避免每筆 lookup 都重打 Layer 1
# {(district_name, segment_name): (AA46, AA48)}
_TPE_SEGMENT_CODE_CACHE: dict[tuple[str, str], tuple[str, str]] = {}

# 段名清單 memory cache：{district: sorted list of KCNT}
_SEGMENTS_CACHE: dict[str, list[str]] = {}


def _pad_landno(landno: str) -> str:
    """User 輸入「1」「1-1」「123-45」→ ArcGIS AA49 8 字元格式「00010000」「00010001」「01230045」。"""
    landno = (landno or "").strip()
    if not landno:
        return ""
    if "-" in landno:
        main, sub = landno.split("-", 1)
    else:
        main, sub = landno, "0"
    try:
        return f"{int(main):04d}{int(sub):04d}"
    except ValueError:
        return ""


def _list_tpe_segments(district: str) -> list[str]:
    """打臺北地政 ArcGIS Layer 1 (段界) 拿該區所有段名。
    Cache 順序：memory → disk → cold fetch ArcGIS + 寫雙層 cache
    每區 ~40-200 段（<1000 ArcGIS query limit），response ~5-30KB"""
    cached = _SEGMENTS_CACHE.get(district)
    if cached is not None:
        return cached
    # disk cache check
    disk_path = _TPE_SEGMENTS_CACHE_DIR / f"{district}.json"
    if disk_path.exists():
        try:
            with open(disk_path, encoding="utf-8") as f:
                data = json.load(f)
            segs = data.get("segments") or []
            if segs and isinstance(data.get("segment_codes"), dict):
                _SEGMENTS_CACHE[district] = segs
                # 順便把段名→代碼填回 memory cache
                for kcnt, aa48 in data["segment_codes"].items():
                    _TPE_SEGMENT_CODE_CACHE[(district, kcnt)] = (_TPE_DISTRICT_CODE[district], aa48)
                logger.info(f"_list_tpe_segments {district} → disk cache hit ({len(segs)} 段)")
                return segs
        except Exception as e:
            logger.debug(f"disk cache 讀取失敗 {disk_path}: {e}")
    # ArcGIS Layer 1 段界 query
    aa46 = _TPE_DISTRICT_CODE[district]
    try:
        r = httpx.get(
            _TPE_LAND_ARCGIS_URL + "/1/query",
            params={
                "f": "json",
                "where": f"AA46='{aa46}'",
                "outFields": "AA48,KCNT",
                "returnGeometry": "false",
            },
            timeout=30,
            verify=False,
        )
    except Exception as e:
        logger.warning(f"_list_tpe_segments {district} ArcGIS 失敗: {e}")
        raise HTTPException(502, "上游臺北地政 ArcGIS 連線失敗")
    if r.status_code != 200 or "json" not in (r.headers.get("content-type") or ""):
        logger.warning(f"_list_tpe_segments {district} ArcGIS http={r.status_code}")
        raise HTTPException(502, f"上游臺北地政 ArcGIS 回 {r.status_code}")
    try:
        data = r.json()
    except Exception:
        raise HTTPException(502, "上游臺北地政 ArcGIS 回非 JSON")
    if data.get("error"):
        raise HTTPException(502, f"上游臺北地政 ArcGIS 拒絕: {data['error'].get('message','')}")
    # 解析 features：每個是一個 段 record (AA46, AA48, KCNT)
    segment_codes: dict[str, str] = {}
    for feat in data.get("features") or []:
        attrs = feat.get("attributes") or {}
        kcnt = attrs.get("KCNT")
        aa48 = attrs.get("AA48")
        if kcnt and aa48:
            segment_codes[kcnt] = aa48
            _TPE_SEGMENT_CODE_CACHE[(district, kcnt)] = (aa46, aa48)
    segs = sorted(segment_codes.keys())
    _SEGMENTS_CACHE[district] = segs
    # 寫 disk cache (年單位才會變，restart 後直接 hit disk)
    try:
        _TPE_SEGMENTS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(disk_path, "w", encoding="utf-8") as f:
            json.dump({
                "district": district,
                "segments": segs,
                "segment_codes": segment_codes,  # 段名→AA48 mapping
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"_list_tpe_segments 寫 disk cache 失敗: {e}")
    logger.info(f"_list_tpe_segments {district} → 從 ArcGIS cold fetch ({len(segs)} 段)，已寫 disk")
    return segs


def _get_tpe_segment_code(district: str, segment: str) -> Optional[str]:
    """段名 → AA48 (4 字元代碼)。需要時 trigger _list_tpe_segments 來載入該區 mapping。"""
    key = (district, segment)
    if key in _TPE_SEGMENT_CODE_CACHE:
        return _TPE_SEGMENT_CODE_CACHE[key][1]
    # 觸發載入該區 mapping
    _list_tpe_segments(district)
    if key in _TPE_SEGMENT_CODE_CACHE:
        return _TPE_SEGMENT_CODE_CACHE[key][1]
    return None


def _query_tpe_plot(district: str, segment: str, landno: str) -> Optional[dict]:
    """打臺北地政 ArcGIS Layer 2 (地號) 查單一地塊。
    用 AA46 (區代碼) + AA48 (段代碼) + AA49 (8 字元 padded 地號) 精確查詢。
    回傳含 AA10 官方公告面積 m²。
    """
    aa46 = _TPE_DISTRICT_CODE.get(district)
    if not aa46:
        return None
    aa48 = _get_tpe_segment_code(district, segment)
    if not aa48:
        return None
    aa49 = _pad_landno(landno)
    if not aa49:
        return None
    where = f"AA46='{aa46}' AND AA48='{aa48}' AND AA49='{aa49}'"
    try:
        r = httpx.get(
            _TPE_LAND_ARCGIS_URL + "/2/query",
            params={
                "f": "json",
                "where": where,
                "outFields": "AA05,AA10,AA16,AA17,KCNT,LandNo,AA46,AA48,AA49,TWD97E,TWD97N",
                "returnGeometry": "true",
                "outSR": "4326",   # WGS84 lng/lat
            },
            timeout=20,
            verify=False,
        )
    except Exception as e:
        logger.warning(f"_query_tpe_plot 失敗: {e}")
        raise HTTPException(502, "上游臺北地政 ArcGIS 連線失敗")
    if r.status_code != 200 or "json" not in (r.headers.get("content-type") or ""):
        raise HTTPException(502, f"上游臺北地政 ArcGIS 回 {r.status_code}")
    try:
        data = r.json()
    except Exception:
        raise HTTPException(502, "上游臺北地政 ArcGIS 回非 JSON")
    if data.get("error"):
        raise HTTPException(502, f"上游臺北地政 ArcGIS 拒絕: {data['error'].get('message','')}")
    feats = data.get("features") or []
    if not feats:
        return None
    feat = feats[0]
    attrs = feat.get("attributes") or {}
    esri_geom = feat.get("geometry") or {}
    rings = esri_geom.get("rings") or []
    if not rings:
        return None
    geojson_geom = {"type": "Polygon", "coordinates": rings}
    lngs, lats = [], []
    for ring in rings:
        for lng, lat in ring:
            lngs.append(lng)
            lats.append(lat)
    if not lngs:
        return None
    center = [sum(lngs) / len(lngs), sum(lats) / len(lats)]
    plot_bbox = [min(lngs), min(lats), max(lngs), max(lats)]
    area_sqm = attrs.get("AA10")     # 官方公告面積 m²
    area_ping = round(area_sqm / 3.305785, 2) if area_sqm else None  # m² → 坪
    return {
        "center": center,
        "bbox": plot_bbox,
        "polygon": geojson_geom,
        "district": district,
        "segment": segment,
        "landno": landno,
        "area_sqm": area_sqm,                                       # 公告面積 m²
        "area_ping": area_ping,                                     # 公告面積 坪
        "land_value_per_sqm": attrs.get("AA16"),                    # 公告現值 元/m²
        "land_price_per_sqm": attrs.get("AA17"),                    # 公告地價 元/m²
        "announce_date_roc": attrs.get("AA05"),                     # 公告日期 民國年月日
    }


def _query_tpe_plot_at_point(lat: float, lng: float) -> Optional[dict]:
    """按地圖點查台北地塊 (使用 ArcGIS spatialRel=Intersects POINT)。"""
    try:
        r = httpx.get(
            _TPE_LAND_ARCGIS_URL + "/2/query",
            params={
                "f": "json",
                "geometry": f"{lng},{lat}",
                "geometryType": "esriGeometryPoint",
                "inSR": "4326",
                "spatialRel": "esriSpatialRelIntersects",
                "outFields": "AA05,AA10,AA16,AA17,KCNT,LandNo,AA46,AA48,AA49",
                "returnGeometry": "true",
                "outSR": "4326",
            },
            timeout=10,
            verify=False,
        )
    except Exception as e:
        logger.debug(f"_query_tpe_plot_at_point 失敗: {e}")
        return None
    if r.status_code != 200 or "json" not in (r.headers.get("content-type") or ""):
        return None
    try:
        data = r.json()
    except Exception:
        return None
    if data.get("error"):
        return None
    feats = data.get("features") or []
    if not feats:
        return None
    feat = feats[0]
    attrs = feat.get("attributes") or {}
    esri_geom = feat.get("geometry") or {}
    rings = esri_geom.get("rings") or []
    if not rings:
        return None
    geojson_geom = {"type": "Polygon", "coordinates": rings}
    lngs, lats = [], []
    for ring in rings:
        for lng_, lat_ in ring:
            lngs.append(lng_); lats.append(lat_)
    if not lngs:
        return None
    center = [sum(lngs) / len(lngs), sum(lats) / len(lats)]
    plot_bbox = [min(lngs), min(lats), max(lngs), max(lats)]
    area_sqm = attrs.get("AA10")
    area_ping = round(area_sqm / 3.305785, 2) if area_sqm else None
    district = _TPE_DISTRICT_CODE_TO_NAME.get(attrs.get("AA46"), "?")
    return {
        "center": center,
        "bbox": plot_bbox,
        "polygon": geojson_geom,
        "district": district,
        "segment": attrs.get("KCNT") or "?",
        "landno": attrs.get("LandNo") or "?",
        "area_sqm": area_sqm,
        "area_ping": area_ping,
        "land_value_per_sqm": attrs.get("AA16"),
        "land_price_per_sqm": attrs.get("AA17"),
        "announce_date_roc": attrs.get("AA05"),
    }


def _query_ntpc_plot_at_point(lat: float, lng: float) -> Optional[dict]:
    """按地圖點查新北地塊 (使用 NtpcURInfo session + ArcGIS POINT query)。"""
    from api.gis_overlay import _get_ntpcurinfo_layer_meta
    meta = _get_ntpcurinfo_layer_meta("地籍圖")
    if not meta or not meta.get("agstoken") or not meta.get("mapsrvurl"):
        return None
    try:
        r = httpx.get(
            meta["mapsrvurl"] + "/0/query",
            params={
                "f": "json",
                "token": meta["agstoken"],
                "geometry": f"{lng},{lat}",
                "geometryType": "esriGeometryPoint",
                "inSR": "4326",
                "spatialRel": "esriSpatialRelIntersects",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "4326",
            },
            timeout=15,
            verify=False,
        )
    except Exception as e:
        logger.debug(f"_query_ntpc_plot_at_point 失敗: {e}")
        return None
    if r.status_code != 200 or "json" not in (r.headers.get("content-type") or ""):
        return None
    try:
        data = r.json()
    except Exception:
        return None
    if data.get("error"):
        return None
    feats = data.get("features") or []
    if not feats:
        return None
    feat = feats[0]
    attrs = feat.get("attributes") or {}
    esri_geom = feat.get("geometry") or {}
    rings = esri_geom.get("rings") or []
    if not rings:
        return None
    geojson_geom = {"type": "Polygon", "coordinates": rings}
    lngs, lats = [], []
    for ring in rings:
        for lng_, lat_ in ring:
            lngs.append(lng_); lats.append(lat_)
    if not lngs:
        return None
    center = [sum(lngs) / len(lngs), sum(lats) / len(lats)]
    plot_bbox = [min(lngs), min(lats), max(lngs), max(lats)]
    # district from bbox lookup (lat/lng → 新北哪區)
    district = _guess_ntpc_district(lat, lng)
    area_sqm = attrs.get(_NTPC_LAND_FIELD_AREA)
    area_ping = round(area_sqm / 3.305785, 2) if area_sqm else None
    return {
        "center": center,
        "bbox": plot_bbox,
        "polygon": geojson_geom,
        "district": district or "新北市",
        "segment": attrs.get(_NTPC_LAND_FIELD_SECTNAME) or "?",
        "landno": attrs.get(_NTPC_LAND_FIELD_LANDNO) or "?",
        "area_sqm": area_sqm,
        "area_ping": area_ping,
        "ownership": attrs.get(_NTPC_LAND_FIELD_PLAND),
        "ownership_manager": attrs.get(_NTPC_LAND_FIELD_PLAND_NAME),
        "ownership_owner": attrs.get(_NTPC_LAND_FIELD_PLAND_OWNER),
    }


def _guess_ntpc_district(lat: float, lng: float) -> Optional[str]:
    """WGS84 lat/lng → TWD97 → 哪個新北行政區 (bbox 命中)。"""
    try:
        from analysis.gov_gis import wgs84_to_twd97
        x, y = wgs84_to_twd97(lat, lng)
    except Exception:
        return None
    for dist, (xmin, ymin, xmax, ymax) in NTPC_DISTRICT_BBOX_TWD97.items():
        if xmin <= x <= xmax and ymin <= y <= ymax:
            return dist
    return None


def _iter_coords(coords):
    """flatten GeoJSON coordinates 到 (lng, lat) tuples。"""
    if not coords:
        return
    if isinstance(coords[0], (int, float)):
        yield coords[0], coords[1]
        return
    for c in coords:
        yield from _iter_coords(c)


def _enrich_with_zoning(result: dict, city: str) -> dict:
    """為 plot result 加上 zoning 欄位 (使用 plot center)。台北 / 新北 各自的 zoning service。"""
    if not result or not result.get("center"):
        return result
    try:
        from analysis.gov_gis import wgs84_to_twd97
        lng, lat = result["center"][0], result["center"][1]
        x, y = wgs84_to_twd97(lat, lng)
    except Exception:
        return result
    z = _query_tpe_zoning_at(x, y) if city == "台北市" else _query_ntpc_zoning_at(x, y)
    if z:
        result.update(z)
    return result


def _query_tpe_zoning_at(twd97_x: float, twd97_y: float) -> Optional[dict]:
    """台北市使用分區 lookup (ublock97-TWD97 layer)。
    Adaptive bbox：先 ±15m (剛好涵蓋多數地塊但不沾到隔壁地塊)，沒命中就放寬到 ±50m。
    這是因為 plot 中心可能正好落在道路 / 分區間隙。
    多分區命中 (e.g., 一半商三一半住三) 暫只取 first；之後可改 shapely intersection 算比例。
    """
    for radius in (15, 50):
        bbox = f"{twd97_x - radius},{twd97_y - radius},{twd97_x + radius},{twd97_y + radius},EPSG:3826"
        try:
            r = httpx.get(
                "https://zonegeo.udd.gov.taipei/geoserver/Taipei/wfs",
                params={
                    "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                    "typeNames": "Taipei:ublock97-TWD97",
                    "outputFormat": "application/json",
                    "bbox": bbox,
                    "count": "10",
                },
                timeout=8,
                verify=False,
            )
        except Exception as e:
            logger.debug(f"_query_tpe_zoning_at radius={radius} 失敗: {e}")
            continue
        if r.status_code != 200:
            continue
        try:
            d = r.json()
        except Exception:
            continue
        feats = d.get("features") or []
        if not feats:
            continue
        # 抓所有命中的 zoning 多邊形，去重
        matched = []
        seen_codes = set()
        for f in feats:
            props = f.get("properties") or {}
            code = props.get("usecod")
            if code and code not in seen_codes:
                seen_codes.add(code)
                matched.append({
                    "code": code,
                    "name": props.get("usenam"),
                    "text": props.get("usetxt"),
                    "category": props.get("usekin"),
                    "memo": props.get("usemem"),
                })
        if not matched:
            continue
        primary = matched[0]
        return {
            "zoning_code": primary["code"],
            "zoning_name": primary["name"],
            "zoning_text": primary["text"],
            "zoning_category": primary["category"],
            "zoning_memo": primary["memo"],
            "zoning_all": matched if len(matched) > 1 else None,
            "zoning_approx": radius >= 30,  # 標示「鄰近最近 zoning」如果 bbox 大
        }
    return None


def _query_ntpc_zoning_at(twd97_x: float, twd97_y: float) -> Optional[dict]:
    """新北市使用分區 lookup (NTPC ArcGIS LandUse_WMS layer 0)。
    Schema 重點欄位：
      LZ1 = 都市計畫案名稱 (e.g., 「擬定永和都市計畫細部計畫案」)
      LZ3 = 使用分區名稱 (e.g., 「商業區」「住宅區」「公園兼廣場用地」)
      LZ8 = 都市計畫類別
    需 NTPC token；point query 取 first feature。
    """
    try:
        from analysis.gov_gis import _get_ntpc_token
        token = _get_ntpc_token()
        if not token:
            return None
        r = httpx.get(
            "https://arcgis.planning.ntpc.gov.tw/server/rest/services/NTPC_Urban/LandUse_WMS/MapServer/0/query",
            params={
                "f": "json",
                "token": token,
                "geometry": f"{twd97_x},{twd97_y}",
                "geometryType": "esriGeometryPoint",
                "inSR": "3826",
                "spatialRel": "esriSpatialRelIntersects",
                "outFields": "LZ1,LZ3,LZ8",
                "returnGeometry": "false",
            },
            timeout=10,
            verify=False,
        )
    except Exception as e:
        logger.debug(f"_query_ntpc_zoning_at 失敗: {e}")
        return None
    if r.status_code != 200 or "json" not in (r.headers.get("content-type") or ""):
        return None
    try:
        d = r.json()
    except Exception:
        return None
    feats = d.get("features") or []
    if not feats:
        return None
    attrs = feats[0].get("attributes") or {}
    name = attrs.get("LZ3")             # 使用分區名稱
    plan = attrs.get("LZ1")             # 都市計畫案名稱
    category = attrs.get("LZ8")         # 都市計畫類別
    if not name:
        return None
    return {
        "zoning_code": None,
        "zoning_name": name,            # 商業區 / 住宅區 / 公園兼廣場用地 / 道路用地 ...
        "zoning_text": name,
        "zoning_category": category,    # 都市計畫類別
        "zoning_memo": plan,            # 都市計畫案
    }


def _query_ntpc_plot(district: str, segment: str, landno: str) -> Optional[dict]:
    """打 NTPC ArcGIS Land/MapServer/0/query 查單一地塊。
    NTPC ArcGIS WAF 擋 multi-field WHERE，所以策略：
      WHERE LANDNO=N + district bbox 過濾 → Python 端再篩 SECTNAME 一致的那筆
    回傳：找到 → {center, bbox, polygon, area_sqm, ...}；找不到 → None
    """
    bbox = NTPC_DISTRICT_BBOX_TWD97.get(district)
    if not bbox:
        return None
    # 拿 NtpcURInfo session 的 ArcGIS token + URL (跟地籍圖 tile 共用同一個 session)
    from api.gis_overlay import _get_ntpcurinfo_layer_meta
    meta = _get_ntpcurinfo_layer_meta("地籍圖")
    if not meta or not meta.get("agstoken") or not meta.get("mapsrvurl"):
        raise HTTPException(502, "上游 NTPC ArcGIS session 暫時不可用，請稍後再試")
    xmin, ymin, xmax, ymax = bbox
    # WHERE 只能放一個 quoted field name，AND 多個會被 WAF 擋（含 % 字元觸發 SQL injection 偵測）
    where = f'"{_NTPC_LAND_FIELD_LANDNO}"=\'{landno}\''
    try:
        r = httpx.get(
            meta["mapsrvurl"] + "/0/query",
            params={
                "f": "json",
                "token": meta["agstoken"],
                "where": where,
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "4326",   # 回 WGS84 lng/lat 省得換算
                "geometry": f"{xmin},{ymin},{xmax},{ymax}",
                "geometryType": "esriGeometryEnvelope",
                "inSR": "3826",
                "spatialRel": "esriSpatialRelIntersects",
            },
            timeout=20,
            verify=False,
        )
    except Exception as e:
        logger.warning(f"NTPC ArcGIS query 失敗: {e}")
        raise HTTPException(502, "上游 NTPC ArcGIS 連線失敗")
    if r.status_code != 200 or "json" not in (r.headers.get("content-type") or ""):
        logger.warning(f"NTPC ArcGIS http={r.status_code} ct={r.headers.get('content-type')}")
        raise HTTPException(502, f"上游 NTPC ArcGIS 異常 ({r.status_code})")
    try:
        data = r.json()
    except Exception:
        raise HTTPException(502, "上游 NTPC ArcGIS 回非 JSON")
    if data.get("error"):
        logger.warning(f"NTPC ArcGIS error: {data['error']}")
        raise HTTPException(502, f"上游 NTPC ArcGIS 拒絕: {data['error'].get('message','')}")
    feats = data.get("features") or []
    # Python 端篩 SECTNAME 一致的那筆 (ArcGIS feature attributes 用 . 分隔的 full key)
    match = None
    for feat in feats:
        attrs = feat.get("attributes") or {}
        if attrs.get(_NTPC_LAND_FIELD_SECTNAME) == segment:
            match = feat
            break
    if not match:
        return None
    esri_geom = match.get("geometry") or {}
    rings = esri_geom.get("rings") or []
    if not rings:
        return None
    geojson_geom = {"type": "Polygon", "coordinates": rings}
    lngs, lats = [], []
    for ring in rings:
        for lng, lat in ring:
            lngs.append(lng)
            lats.append(lat)
    if not lngs:
        return None
    center = [sum(lngs) / len(lngs), sum(lats) / len(lats)]
    plot_bbox = [min(lngs), min(lats), max(lngs), max(lats)]
    attrs = match.get("attributes") or {}
    area_sqm = attrs.get(_NTPC_LAND_FIELD_AREA)
    area_ping = round(area_sqm / 3.305785, 2) if area_sqm else None
    return {
        "center": center,
        "bbox": plot_bbox,
        "polygon": geojson_geom,
        "district": district,
        "segment": segment,
        "landno": landno,
        "area_sqm": area_sqm,         # 公告面積 m²
        "area_ping": area_ping,
        "ownership": attrs.get(_NTPC_LAND_FIELD_PLAND),               # 公有 / 私有
        "ownership_manager": attrs.get(_NTPC_LAND_FIELD_PLAND_NAME),   # 管理機關 (公有時)
        "ownership_owner": attrs.get(_NTPC_LAND_FIELD_PLAND_OWNER),    # 所有人 (公有時)
        "scno": attrs.get(_NTPC_LAND_FIELD_SCNO),
    }


@router.get("/api/cadastral_search/segments")
async def cadastral_search_segments(
    city: str = Query(..., description="台北市 / 新北市"),
    district: str = Query(..., description="區名 e.g. 大安區"),
    _user: dict = Depends(get_current_user),
):
    """回該區所有 distinct 段名 (給前端下拉選單用)。"""
    city = (city or "").strip()
    district = (district or "").strip()
    if not city or not district:
        raise HTTPException(400, "city/district 不可空")
    if any(c in district for c in ("'", '"', '\\', '%', '<', '>', ';')):
        raise HTTPException(400, "district 含不合法字元")
    if city == "新北市":
        # 新北從 hardcoded 清單拿 (一次性 enumerate，段名年單位才會變)
        if district not in NTPC_SEGMENTS:
            raise HTTPException(400, f"district 不在新北段清單支援範圍 (目前支援: {list(NTPC_SEGMENTS.keys())})")
        segments = NTPC_SEGMENTS[district]
        return {"city": city, "district": district, "segments": segments, "count": len(segments)}
    if city != "台北市":
        raise HTTPException(400, "city 必須是 台北市 或 新北市")
    if district not in _TPE_DISTRICTS:
        raise HTTPException(400, "district 不屬於台北市行政區")
    segments = _list_tpe_segments(district)
    return {"city": city, "district": district, "segments": segments, "count": len(segments)}


@router.get("/api/cadastral_search/lookup")
async def cadastral_search_lookup(
    city: str = Query(..., description="台北市 / 新北市"),
    district: str = Query(..., description="區名 e.g. 大安區"),
    segment: str = Query(..., description="段名 e.g. 龍泉段一小段"),
    landno: str = Query(..., description="地號 e.g. 2-1 或 123"),
    _user: dict = Depends(get_current_user),
):
    """查詢單一地塊位置 + 公告面積。"""
    city = (city or "").strip()
    district = (district or "").strip()
    segment = (segment or "").strip()
    landno = (landno or "").strip()
    if not (city and district and segment and landno):
        raise HTTPException(400, "city/district/segment/landno 都不可空")
    # 防 injection：擋掉特殊字元
    for s, name in [(district, "district"), (segment, "segment"), (landno, "landno")]:
        if any(c in s for c in ("'", '"', '\\', '%', '<', '>', ';')):
            raise HTTPException(400, f"{name} 含不合法字元")
    # 地號格式 fast-fail：必須是純數字 or 「數字-數字」(避免無效 query 浪費上游時間)
    if not _LANDNO_RE.match(landno):
        return {"ok": False, "reason": f"地號格式不對 (允許「123」或「123-45」這種，得到「{landno}」)"}
    if city == "新北市":
        if district not in NTPC_DISTRICT_BBOX_TWD97:
            raise HTTPException(400, f"district 不在新北支援清單 (目前支援: {list(NTPC_DISTRICT_BBOX_TWD97.keys())})")
        result = _query_ntpc_plot(district, segment, landno)
        if not result:
            return {"ok": False, "reason": "找不到該地塊 — 請確認區/段/地號拼寫"}
        _enrich_with_zoning(result, "新北市")
        return {"ok": True, **result}
    if city != "台北市":
        raise HTTPException(400, "city 必須是 台北市 或 新北市")
    if district not in _TPE_DISTRICTS:
        raise HTTPException(400, "district 不屬於台北市行政區")
    result = _query_tpe_plot(district, segment, landno)
    if not result:
        return {"ok": False, "reason": "找不到該地塊 — 請確認區/段/地號拼寫"}
    _enrich_with_zoning(result, "台北市")
    return {"ok": True, **result}


@router.get("/api/cadastral_search/at_point")
async def cadastral_search_at_point(
    lat: float = Query(..., description="WGS84 緯度"),
    lng: float = Query(..., description="WGS84 經度"),
    _user: dict = Depends(get_current_user),
):
    """按地圖座標反查該位置地塊資料。

    台北市優先短路：先平行打 Taipei (plot + zoning)，命中即回。台北物件不會碰 NTPC
    URInfo session，避免第一次點擊被 NTPC session token (10-20s) 拖住。
    Taipei miss 才退而求其次打新北（這時 NTPC session 冷啟動的成本由 click 那筆吸收）。
    """
    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        raise HTTPException(400, "lat/lng 不在合法範圍")
    from concurrent.futures import ThreadPoolExecutor
    try:
        from analysis.gov_gis import wgs84_to_twd97
        twd97_x, twd97_y = wgs84_to_twd97(lat, lng)
    except Exception:
        twd97_x = twd97_y = None
    with ThreadPoolExecutor(max_workers=4) as ex:
        # ── Phase 1: Taipei plot + zoning（fast，無 token）─────────────
        tpe_plot_fut = ex.submit(_query_tpe_plot_at_point, lat, lng)
        tpe_zoning_fut = ex.submit(_query_tpe_zoning_at, twd97_x, twd97_y) if twd97_x is not None else None
        tpe_plot = tpe_plot_fut.result()
        if tpe_plot:
            tpe_zoning = tpe_zoning_fut.result() if tpe_zoning_fut else None
            if tpe_zoning:
                tpe_plot.update(tpe_zoning)
            return {"ok": True, **tpe_plot}
        # Taipei miss → 取消 zoning future (還沒跑完就讓它跑完丟掉；ArcGIS 不可中斷)
        if tpe_zoning_fut:
            try: tpe_zoning_fut.result(timeout=0.001)
            except Exception: pass
        # ── Phase 2: 新北 plot + zoning（need NTPC URInfo session）────
        ntpc_plot_fut = ex.submit(_query_ntpc_plot_at_point, lat, lng)
        ntpc_zoning_fut = ex.submit(_query_ntpc_zoning_at, twd97_x, twd97_y) if twd97_x is not None else None
        ntpc_plot = ntpc_plot_fut.result()
        ntpc_zoning = ntpc_zoning_fut.result() if ntpc_zoning_fut else None
    if ntpc_plot:
        if ntpc_zoning:
            ntpc_plot.update(ntpc_zoning)
        return {"ok": True, **ntpc_plot}
    return {"ok": False, "reason": "此位置無地籍資料（可能為公園/水面/道路）"}
