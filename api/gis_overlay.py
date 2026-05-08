"""GIS overlay proxy — 前端地圖模式 optional 圖層用。

設計目標：
  前端 Leaflet `L.tileLayer.wms('/api/gis_overlay/{layer}', ...)` 把這個 endpoint
  當成 WMS GetMap server。本 module 把 client 帶來的 WMS params 轉發到對應的
  政府 GIS server（台北 GeoServer WMS / 新北 ArcGIS REST export），統一回 PNG。

支援的 layer：
  - zoning_tpe       台北 使用分區 (WMS Taipei:ublock97-TWD97 + -text)
  - cadastral_tpe    台北 地籍   (WMS Taipei:LAND-ALL-TWD97)
  - zoning_ntpc      新北 使用分區 (ArcGIS LandUse_WMS/MapServer)
  - cadastral_ntpc   新北 地籍   (TODO Phase A.5: 找對應 ArcGIS service)

隔離原則（plan 約定）：
  - 整個 module 自包含，所有 GIS forward 邏輯都在這
  - app.py 只用 1 行 `app.include_router(gis_overlay.router)` 掛載
  - revert 時刪本 file + 拿掉那 1 行 import/include 即可
  - 不改既有 [analysis/gov_gis.py](analysis/gov_gis.py)（重用其常數但 deepcopy 不污染）
"""
from __future__ import annotations

import base64
import logging
import time
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Request, Response

logger = logging.getLogger(__name__)

router = APIRouter()

# ── 1×1 透明 PNG (政府 server 失敗時回給前端，避免 Leaflet 拿到非 image 報錯) ──
_TRANSPARENT_1X1_PNG = base64.b64decode(
    b"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="
)

# ── 圖層 → 上游 GIS server 對應 ──────────────────────────────────────────────
# WMS layer name 直接列在這（重用 analysis/gov_gis.py 既有常數的概念但不 import 避免耦合）
_TPE_WMS_URL = "https://zonegeo.udd.gov.taipei/geoserver/Taipei/wms"

_LAYER_DEFS: dict[str, dict] = {
    # ── 台北市（GeoServer WMS forward） ─────────────────────────────────────
    "zoning_tpe": {
        "kind": "wms",
        "upstream": _TPE_WMS_URL,
        # 主圖 + 文字（住三/商二）兩層 — text layer 不一定每個 zoom 都有，但 GeoServer 會自動處理
        "layers": "Taipei:ublock97-TWD97,Taipei:ublock97-TWD97-text",
    },
    # 地籍拆兩個 backend：線任 zoom 顯示，地號文字只 z=18 或 19 才顯示 (前端 minZoom 控制)
    "cadastral_lines_tpe": {
        "kind": "wms",
        "upstream": _TPE_WMS_URL,
        "layers": "Taipei:LAND-ALL-TWD97",
    },
    "cadastral_numbers_tpe": {
        "kind": "wms",
        "upstream": _TPE_WMS_URL,
        "layers": "Taipei:LAND-ALL-TWD97-TEXT",
    },
    # 台北建物樓層 — 都發局 GISDB layer 19 (建物_Build polygon)
    # 此 service 公開無 token，但 layer 19 minScale=5000，zoom < ~17 不會顯示 polygon (政府 server scale-dependent)
    # 純色塊 polygon，沒含「4R 5R T」label (那是 Build_NO/Build_STR attribute，要另用 query 拿，下階段補)
    "building_floors_tpe": {
        "kind": "arcgis_export",
        "upstream": "https://www.historygis.udd.gov.taipei/arcgis/rest/services/Urban/GISDB/MapServer/export",
        "layer_show": "19",   # 用 layers=show:N 模式，不用 dynamicLayers (此 service 不支援 dynamicLayers)
    },
    # 台北市「已劃定」都市更新地區範圍 — 都發局 PlanTheme layer 0 (332 個 polygon)
    # attributes: PROJNUM (案號)、PLANDES (公告說明)、PLANDATE (公告日期)、PLANLEV
    # 這是「政府已公告劃定」的都更地區；「審議/申請中個案」在 uro.gov.taipei 要另外 scrape
    "renewal_planned_tpe": {
        "kind": "arcgis_export",
        "upstream": "https://www.historygis.udd.gov.taipei/arcgis/rest/services/UrbanPlan2/PlanTheme/MapServer/export",
        "layer_show": "0",
    },
    # ── 新北市（ArcGIS REST export，需要 token） ────────────────────────────
    "zoning_ntpc": {
        "kind": "arcgis_export",
        "upstream": "https://arcgis.planning.ntpc.gov.tw/server/rest/services/NTPC_Urban/LandUse_WMS/MapServer/export",
        # ArcGIS dynamicLayers payload — 隱藏 labels 避免低 zoom 字塊
        "dynamic_layers": '[{"id":0,"source":{"type":"mapLayer","mapLayerId":0},"drawingInfo":{"showLabels":false}}]',
        "needs_token": True,   # NTPC 要 token，台北 historygis 不用
    },
    # cadastral_ntpc 待 Phase A.5 確認新北 ArcGIS 有無對應地籍 layer
}

# ── 簡易 in-memory cache（10 min TTL，bbox+layer 為 key） ──
_CACHE: dict[tuple, tuple[bytes, float]] = {}
_CACHE_TTL = 600.0
_CACHE_MAX = 200


def _cache_get(key: tuple) -> Optional[bytes]:
    item = _CACHE.get(key)
    if not item:
        return None
    content, expires_at = item
    if time.time() > expires_at:
        _CACHE.pop(key, None)
        return None
    return content


def _cache_set(key: tuple, content: bytes) -> None:
    if len(_CACHE) >= _CACHE_MAX:
        # 簡單 evict：清掉最舊的 1/3
        sorted_items = sorted(_CACHE.items(), key=lambda kv: kv[1][1])
        for k, _ in sorted_items[: _CACHE_MAX // 3]:
            _CACHE.pop(k, None)
    _CACHE[key] = (content, time.time() + _CACHE_TTL)


# ── 新北 ArcGIS token（重用 analysis/gov_gis.py 既有 cache，避免兩處重抓） ──
def _get_ntpc_token() -> str:
    try:
        from analysis.gov_gis import _get_ntpc_token as _gov_token
        return _gov_token()
    except Exception as e:
        logger.warning(f"取 NTPC token 失敗: {e}")
        return ""


# ── 上游 dispatch ──────────────────────────────────────────────────────────
def _fetch_wms(upstream: str, layer_names: str, bbox: str, width: int, height: int, srs: str) -> Optional[bytes]:
    """直接 forward WMS GetMap 到 GeoServer。"""
    try:
        r = httpx.get(
            upstream,
            params={
                "service": "WMS",
                "version": "1.1.1",
                "request": "GetMap",
                "layers": layer_names,
                "bbox": bbox,
                "width": str(width),
                "height": str(height),
                "srs": srs,
                "format": "image/png",
                "transparent": "true",
                "styles": "",
            },
            timeout=12,
            verify=False,
        )
        if r.status_code != 200:
            logger.warning(f"WMS upstream http={r.status_code} {upstream} {layer_names}")
            return None
        if "image" not in r.headers.get("content-type", ""):
            logger.warning(
                f"WMS upstream non-image ct={r.headers.get('content-type')} body[:200]={r.text[:200]!r}"
            )
            return None
        if len(r.content) < 100:
            return None
        return r.content
    except Exception as e:
        logger.warning(f"WMS upstream 例外: {e}")
        return None


def _fetch_arcgis_export(cfg: dict, bbox: str, width: int, height: int, srs: str) -> Optional[bytes]:
    """ArcGIS MapServer/export — Leaflet 給的 WMS-style bbox 轉成 ArcGIS 格式。
    bbox 格式相同（W,S,E,N comma-separated），SRS 數字部分 ('EPSG:3857' → 3857) 給 bboxSR/imageSR。

    cfg 支援兩種 layer 指定模式：
      - cfg['layer_show'] = '19'        → params['layers'] = 'show:19' (台北 historygis 走這個)
      - cfg['dynamic_layers'] = '[...]' → params['dynamicLayers'] = json (新北 NTPC 走這個)
    cfg['needs_token'] = True 才帶 NTPC token (historygis 是公開不需要)。
    """
    sr_num = srs.split(":")[-1] if ":" in srs else srs   # "EPSG:3857" → "3857"
    upstream = cfg["upstream"]
    params = {
        "bbox": bbox,
        "bboxSR": sr_num,
        "imageSR": sr_num,
        "size": f"{width},{height}",
        "format": "png",
        "dpi": "96",
        "transparent": "true",
        "f": "image",
    }
    if cfg.get("layer_show"):
        params["layers"] = "show:" + cfg["layer_show"]
    if cfg.get("dynamic_layers"):
        params["dynamicLayers"] = cfg["dynamic_layers"]
    if cfg.get("needs_token"):
        params["token"] = _get_ntpc_token()
    try:
        r = httpx.get(
            upstream,
            params=params,
            timeout=12,
            verify=False,
        )
        if r.status_code != 200:
            logger.warning(f"ArcGIS upstream http={r.status_code} {upstream}")
            return None
        if "image" not in r.headers.get("content-type", ""):
            logger.warning(
                f"ArcGIS upstream non-image ct={r.headers.get('content-type')} body[:200]={r.text[:200]!r}"
            )
            return None
        if len(r.content) < 100:
            return None
        return r.content
    except Exception as e:
        logger.warning(f"ArcGIS upstream 例外: {e}")
        return None


# ── Endpoint ───────────────────────────────────────────────────────────────
@router.get("/api/gis_overlay/{layer}")
async def gis_overlay(layer: str, request: Request) -> Response:
    """前端 Leaflet `L.tileLayer.wms('/api/gis_overlay/zoning_tpe', { layers: 'zoning_tpe', ... })` 用。

    Required query params (都從 Leaflet 自動帶過來)：
      bbox=W,S,E,N    範圍 (4 個 number)
      width, height   tile size
      srs / SRS       投影 (例如 EPSG:3857)

    回傳：image/png（10 min memory cache 命中省 round trip）；上游失敗 → 透明 1×1 png + 504。
    """
    if layer not in _LAYER_DEFS:
        raise HTTPException(404, f"unknown layer: {layer}")

    qp = request.query_params
    bbox = qp.get("bbox") or qp.get("BBOX") or ""
    try:
        width = int(qp.get("width") or qp.get("WIDTH") or "256")
        height = int(qp.get("height") or qp.get("HEIGHT") or "256")
    except ValueError:
        raise HTTPException(400, "width/height 不是整數")
    srs = qp.get("srs") or qp.get("SRS") or qp.get("crs") or qp.get("CRS") or "EPSG:3857"

    if not bbox or len(bbox.split(",")) != 4:
        raise HTTPException(400, "bbox 必須是 W,S,E,N (4 個 number)")

    # cache key — bbox 浮點數差幾 m 視為同 key 太麻煩，直接用原字串做 key
    cache_key = (layer, bbox, width, height, srs)
    cached = _cache_get(cache_key)
    if cached:
        return Response(content=cached, media_type="image/png", headers={"X-Cache": "HIT"})

    cfg = _LAYER_DEFS[layer]
    if cfg["kind"] == "wms":
        content = _fetch_wms(cfg["upstream"], cfg["layers"], bbox, width, height, srs)
    elif cfg["kind"] == "arcgis_export":
        content = _fetch_arcgis_export(cfg, bbox, width, height, srs)
    else:
        raise HTTPException(500, f"unknown kind: {cfg['kind']}")

    if not content:
        # 上游失敗 → 回透明 1×1，前端 layer 不會 break；status 504 給前端可選擇靜默或顯示警告
        return Response(content=_TRANSPARENT_1X1_PNG, media_type="image/png", status_code=504, headers={"X-Cache": "MISS-FAIL"})

    _cache_set(cache_key, content)
    return Response(
        content=content,
        media_type="image/png",
        headers={"Cache-Control": "max-age=600", "X-Cache": "MISS"},
    )
