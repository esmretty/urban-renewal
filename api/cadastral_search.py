"""地塊搜尋 router — 地圖模式「輸入區+段+地號 → 飛到該地塊」功能。

隔離原則（同 gis_overlay.py 設計）：
  - 整個 module 自包含，所有地塊查詢邏輯都在這個 file
  - app.py 只用 1 行 `app.include_router(cadastral_search.router)` 掛載
  - revert 時刪本 file + 拿掉那 1 行 include 即可，其他 file 完全不動

支援範圍（MVP）：
  - 台北 8 區：透過 GeoServer WFS GetFeature 查 Taipei:LAND-ALL-TWD97
    (attribute schema: dist_name + sect_name + land_no/land_pnum/land_snum)
  - 新北 4 區：暫不支援（NtpcURInfo session-based proxy，技術上可行但要包 token chain）
    若 city='新北市' 直接回 HTTP 501 提示「目前只支援台北市」

API:
  - GET /api/cadastral_search/lookup?city=&district=&segment=&landno=
    → 回 {ok, center:[lng,lat], bbox:[w,s,e,n], polygon: GeoJSON, segment, landno, district}
    → 找不到回 {ok: false, reason: '...'}
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()

_TPE_WFS_URL = "https://zonegeo.udd.gov.taipei/geoserver/Taipei/wfs"
_TPE_DISTRICTS = {"大安區", "信義區", "中山區", "中正區", "文山區",
                   "松山區", "萬華區", "大同區", "內湖區", "南港區",
                   "士林區", "北投區"}
_NTPC_DISTRICTS = {"板橋區", "新店區", "中和區", "永和區", "新莊區",
                    "三重區", "蘆洲區", "土城區", "樹林區", "汐止區",
                    "淡水區", "林口區", "三峽區", "鶯歌區", "泰山區",
                    "五股區", "八里區", "深坑區", "石碇區", "坪林區",
                    "烏來區", "瑞芳區", "貢寮區", "雙溪區", "平溪區",
                    "金山區", "萬里區", "三芝區", "石門區"}


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
        if district not in _NTPC_DISTRICTS:
            raise HTTPException(400, "district 不屬於新北市行政區")
        raise HTTPException(501, "目前地塊搜尋只支援台北市（新北市資料源待後續實作）")
    if city != "台北市":
        raise HTTPException(400, "city 必須是 台北市 或 新北市")
    if district not in _TPE_DISTRICTS:
        raise HTTPException(400, "district 不屬於台北市行政區")
    result = _query_tpe_plot(district, segment, landno)
    if not result:
        return {"ok": False, "reason": "找不到該地塊 — 請確認區/段/地號拼寫"}
    return {"ok": True, **result}
