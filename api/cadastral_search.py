"""地塊搜尋 router — 地圖模式「輸入區+段+地號 → 飛到該地塊」功能。

隔離原則（同 gis_overlay.py 設計）：
  - 整個 module 自包含，所有地塊查詢邏輯都在這個 file
  - app.py 只用 1 行 `app.include_router(cadastral_search.router)` 掛載
  - revert 時刪本 file + 拿掉那 1 行 include 即可，其他 file 完全不動

支援範圍：
  - 台北 8 區：透過 GeoServer WFS GetFeature 查 Taipei:LAND-ALL-TWD97
    (attribute schema: dist_name + sect_name + land_no)
  - 新北 4 區（板橋/新店/中和/永和）：透過 NTPC ArcGIS query 配 NtpcURInfo session token
    打 NTPC_Urban/Land/MapServer/0/query；段名清單從 _ntpc_cadastral_data 拿
    (一次性 enumerate 出來，hardcode 進去；段名年單位才會變)

API:
  - GET /api/cadastral_search/segments?city=&district= → 回該區段名清單
  - GET /api/cadastral_search/lookup?city=&district=&segment=&landno=
    → 回 {ok, center:[lng,lat], bbox:[w,s,e,n], polygon: GeoJSON, segment, landno, district}
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
# 段名年單位才變，避免每次 server restart 第一個 user 等 3-15 秒 GeoServer cold fetch
_TPE_SEGMENTS_CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "cache" / "tpe_segments"

logger = logging.getLogger(__name__)
router = APIRouter()

_TPE_WFS_URL = "https://zonegeo.udd.gov.taipei/geoserver/Taipei/wfs"
_TPE_DISTRICTS = {"大安區", "信義區", "中山區", "中正區", "文山區",
                   "松山區", "萬華區", "大同區", "內湖區", "南港區",
                   "士林區", "北投區"}
_NTPC_LAND_FIELD_LANDNO = "NTPCUPGIS_SDE.NTPCGIS2.%Land.LANDNO"
_NTPC_LAND_FIELD_SECTNAME = "NTPCUPGIS_SDE.NTPCGIS2.%Land.SECTNAME"

# 段名 cache：{district: sorted list of sect_name}
# WFS query 一次撈該區所有 plot 的 sect_name 去重 → 永久 cache 在 module 記憶體
# 段名不會頻繁變動 (年單位)，restart 才重抓
_SEGMENTS_CACHE: dict[str, list[str]] = {}
_NTPC_DISTRICTS = {"板橋區", "新店區", "中和區", "永和區", "新莊區",
                    "三重區", "蘆洲區", "土城區", "樹林區", "汐止區",
                    "淡水區", "林口區", "三峽區", "鶯歌區", "泰山區",
                    "五股區", "八里區", "深坑區", "石碇區", "坪林區",
                    "烏來區", "瑞芳區", "貢寮區", "雙溪區", "平溪區",
                    "金山區", "萬里區", "三芝區", "石門區"}


def _list_tpe_segments(district: str) -> list[str]:
    """打 WFS 拿該區所有 plot 的 sect_name 去重 + 排序。
    Cache 順序：memory → disk → cold fetch GeoServer + 寫雙層 cache
    首次 cold ~5-20 秒；disk hit ~10ms；memory hit 0ms"""
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
            if segs:
                _SEGMENTS_CACHE[district] = segs
                logger.info(f"_list_tpe_segments {district} → disk cache hit ({len(segs)} 段)")
                return segs
        except Exception as e:
            logger.debug(f"disk cache 讀取失敗 {disk_path}: {e}")
    try:
        r = httpx.get(_TPE_WFS_URL, params={
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": "Taipei:LAND-ALL-TWD97",
            "outputFormat": "application/json",
            "propertyName": "sect_name",   # 只取段名 attribute，省頻寬
            "cql_filter": f"dist_name='{district}'",
        }, timeout=60, verify=False)
    except Exception as e:
        logger.warning(f"_list_tpe_segments {district} WFS 失敗: {e}")
        raise HTTPException(502, "上游 GeoServer 連線失敗")
    if r.status_code != 200:
        logger.warning(f"_list_tpe_segments {district} WFS http={r.status_code}")
        raise HTTPException(502, f"上游 GeoServer 回 {r.status_code}")
    try:
        data = r.json()
    except Exception:
        raise HTTPException(502, "上游 GeoServer 回非 JSON")
    seen = set()
    for feat in data.get("features") or []:
        sect = (feat.get("properties") or {}).get("sect_name")
        if sect:
            seen.add(sect)
    result = sorted(seen)
    _SEGMENTS_CACHE[district] = result
    # 寫 disk cache (年單位才會變，restart 後直接 hit disk)
    try:
        _TPE_SEGMENTS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(disk_path, "w", encoding="utf-8") as f:
            json.dump({"district": district, "segments": result}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"_list_tpe_segments 寫 disk cache 失敗: {e}")
    logger.info(f"_list_tpe_segments {district} → 從 GeoServer cold fetch ({len(result)} 段)，已寫 disk")
    return result


def _query_tpe_plot(district: str, segment: str, landno: str) -> Optional[dict]:
    """打 GeoServer WFS GetFeature 查單一地塊。
    回傳：找到 → {center, bbox, polygon, ...}；找不到 → None"""
    # CQL filter：dist_name + sect_name + land_no 三者完全相等
    cql = (
        f"dist_name='{district}' AND "
        f"sect_name='{segment}' AND "
        f"land_no='{landno}'"
    )
    try:
        r = httpx.get(_TPE_WFS_URL, params={
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": "Taipei:LAND-ALL-TWD97",
            "outputFormat": "application/json",
            "srsName": "EPSG:4326",   # 直接回 WGS84，省得我們本機換算
            "cql_filter": cql,
            "count": "1",
        }, timeout=15, verify=False)
    except Exception as e:
        logger.warning(f"GeoServer WFS 查詢失敗: {e}")
        raise HTTPException(502, "上游 GeoServer 連線失敗")
    if r.status_code != 200:
        logger.warning(f"GeoServer WFS 回 {r.status_code}: {r.text[:200]}")
        raise HTTPException(502, f"上游 GeoServer 回 {r.status_code}")
    try:
        data = r.json()
    except Exception as e:
        logger.warning(f"GeoServer 回非 JSON: {e}")
        raise HTTPException(502, "上游 GeoServer 回非 JSON")
    feats = data.get("features") or []
    if not feats:
        return None
    feat = feats[0]
    geom = feat.get("geometry") or {}
    props = feat.get("properties") or {}
    # 算 polygon centroid + bbox（取所有 coord 平均當中心；bbox 取 min/max）
    coords_iter = _iter_coords(geom.get("coordinates"))
    lngs, lats = [], []
    for lng, lat in coords_iter:
        lngs.append(lng)
        lats.append(lat)
    if not lngs:
        return None
    center = [sum(lngs) / len(lngs), sum(lats) / len(lats)]
    bbox = [min(lngs), min(lats), max(lngs), max(lats)]
    return {
        "center": center,           # [lng, lat] for Leaflet flyTo
        "bbox": bbox,               # [w, s, e, n]
        "polygon": geom,            # GeoJSON geometry (MultiPolygon / Polygon)
        "district": district,
        "segment": segment,
        "landno": landno,
        "sect_id": props.get("sect_id"),
    }


def _iter_coords(coords):
    """flatten GeoJSON coordinates 到 (lng, lat) tuples。"""
    if not coords:
        return
    if isinstance(coords[0], (int, float)):
        yield coords[0], coords[1]
        return
    for c in coords:
        yield from _iter_coords(c)


def _query_ntpc_plot(district: str, segment: str, landno: str) -> Optional[dict]:
    """打 NTPC ArcGIS Land/MapServer/0/query 查單一地塊。
    NTPC ArcGIS WAF 擋 multi-field WHERE，所以策略：
      WHERE LANDNO=N + district bbox 過濾 → Python 端再篩 SECTNAME 一致的那筆
    回傳：找到 → {center, bbox, polygon, ...}；找不到 → None
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
    # ArcGIS f=json 回 esri geometry (rings)，要轉成 GeoJSON polygon 給前端 L.geoJSON 用
    esri_geom = match.get("geometry") or {}
    rings = esri_geom.get("rings") or []
    if not rings:
        return None
    # esri rings → GeoJSON Polygon (outer + inner rings 同層級；
    # 簡化處理：假定第一 ring 是 outer，其他都 inner)
    geojson_geom = {
        "type": "Polygon",
        "coordinates": rings,
    }
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
    return {
        "center": center,
        "bbox": plot_bbox,
        "polygon": geojson_geom,
        "district": district,
        "segment": segment,
        "landno": landno,
        "scno": attrs.get("NTPCUPGIS_SDE.NTPCGIS2.%Land.SCNO"),
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
    """查詢單一地塊位置 (台北市)。"""
    city = (city or "").strip()
    district = (district or "").strip()
    segment = (segment or "").strip()
    landno = (landno or "").strip()
    if not (city and district and segment and landno):
        raise HTTPException(400, "city/district/segment/landno 都不可空")
    # 防 SQL injection / CQL injection：擋掉特殊字元
    for s, name in [(district, "district"), (segment, "segment"), (landno, "landno")]:
        if any(c in s for c in ("'", '"', '\\', '%', '<', '>', ';')):
            raise HTTPException(400, f"{name} 含不合法字元")
    if city == "新北市":
        if district not in NTPC_DISTRICT_BBOX_TWD97:
            raise HTTPException(400, f"district 不在新北支援清單 (目前支援: {list(NTPC_DISTRICT_BBOX_TWD97.keys())})")
        result = _query_ntpc_plot(district, segment, landno)
        if not result:
            return {"ok": False, "reason": "找不到該地塊 — 請確認區/段/地號拼寫"}
        return {"ok": True, **result}
    if city != "台北市":
        raise HTTPException(400, "city 必須是 台北市 或 新北市")
    if district not in _TPE_DISTRICTS:
        raise HTTPException(400, "district 不屬於台北市行政區")
    result = _query_tpe_plot(district, segment, landno)
    if not result:
        return {"ok": False, "reason": "找不到該地塊 — 請確認區/段/地號拼寫"}
    return {"ok": True, **result}
