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
    # 留著但 frontend 已不接 (用更完整的 redevelop_uro_tpe 取代，含審議中個案 + 整建住宅等 8 子類型)
    "renewal_planned_tpe": {
        "kind": "arcgis_export",
        "upstream": "https://www.historygis.udd.gov.taipei/arcgis/rest/services/UrbanPlan2/PlanTheme/MapServer/export",
        "layer_show": "0",
    },
    # 台北市都更審議 — zonegeo.udd.gov.taipei GeoServer Taipei:uro-redevelop-ALL-5
    # 對齊 https://bim.udd.gov.taipei/UDDPlanMap/「都市更新審議」面板，含 8 個子類型疊圖：
    #   layer=10 公劃更新地區(依都更條例)         紅色
    #   layer=17 廢止89.91年公劃更新地區         深紅
    #   layer=20 公告自劃單元(自劃事業權變計畫案件) 藍
    #   layer=30 核准自劃單元                    橘
    #   layer=40 都市計畫劃定更新地區
    #   layer=42 (停止適用)107年公劃更新地區     黃
    #   layer=44 高氯離子混凝土建築地區          褐
    #   layer=48 迅行劃定更新地區                粉
    # 點擊每個區塊 ID 在 https://gis.uro.taipei/showproj_uro.html?case_id=${ID} 看詳情
    # 不加 cql_filter → GeoServer render 全部 8 子類型疊一起 (跟 UDDPlanMap 全勾的視覺一致)
    "redevelop_uro_tpe": {
        "kind": "wms",
        "upstream": _TPE_WMS_URL,
        "layers": "Taipei:uro-redevelop-ALL-5",
    },
    # NLSC 全國地籍段邊界 (LANDSECT) — 補新北地籍
    # 走 WMS (maps.nlsc.gov.tw/S_Maps/wms) 不是 WMTS：兩者是並存 OGC 標準不是升級關係。
    # WMS 可給任意 bbox + size 較有彈性 (前端日後可改 nonTiledLayer 抓視窗整圖)，
    # 但對「detail」沒幫助 — LANDSECT 本身只有「地段外圍圖(段籍圖)」= 段邊界 (一段
    # 含數百地號)，**沒個別地塊**。要拿個別地塊+地號，NLSC 限政府機關/學術單位申請
    # 「地籍圖 API/WFS」；民間付費走內政部地政司「地政電子資料流通服務網」每筆約 1 元。
    "cadastral_ntpc": {
        "kind": "nlsc_wms",
        "layer_id": "LANDSECT",
    },
    # NLSC 公有土地地籍圖 (LAND_OPENDATA) — 補新北「公有地」polygon (私有地不含)
    # 此 layer 只在 WMTS catalog (GetCapabilities 219 layer)，不在 WMS (66 layer) → 用 WMTS
    "cadastral_public_ntpc": {
        "kind": "nlsc_wmts",
        "layer_id": "LAND_OPENDATA",
    },
    # 591 maptiles DMAPS forward proxy — 個別地塊 + 地號 polygon (政府 NLSC 授權)
    # 詳見 _fetch_591_dmaps docstring：純 forward 不 cache 不重製；用戶 (個人投資者)
    # 已明示確認 591 ToS 灰色地帶範圍。591 改設定立即 fallback (前端 tileerror handler)。
    "cadastral_591": {
        "kind": "591_dmaps_proxy",
        "layer_id": "DMAPS",
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


def _fetch_591_dmaps(cfg: dict, bbox: str, width: int, height: int, srs: str) -> Optional[bytes]:
    """591 maptiles forward proxy — DMAPS (政府 NLSC 授權的個別地塊 + 地號 polygon 圖)。

    背景：NLSC DMAPS layer 限政府/學術機關申請，不對民間公開 (公開 GetCapabilities
    沒列、直接打回 0 bytes)。591 跟 NLSC 有商業授權，把 DMAPS tile mirror 在
    AWS CloudFront CDN (maptiles.591.com.tw)，用戶看 land.591.com.tw/map 的「地籍
    查詢」即用此 source。CORS Access-Control-Allow-Origin: *、無 referer 檢查、無
    token —— **技術上**對外開放。

    使用界限（**用戶為個人投資者使用，已明示確認風險範圍**）：
      - 純 forward proxy，**不 cache 不重製**——每次 user 請求才即時走 591，
        從 591 server 端流量分佈跟「user 自己開瀏覽器 browse 591 land.map」幾乎相同
      - 不 bulk download / pre-warm cache（這條線堅持不做，違反著作權重製）
      - 591 改設定（加 referer check / token / IP block）→ 立即 fallback 到 NLSC
        LANDSECT（前端 tileerror handler 自動降級），不長期 depend on
      - 流量小：個人投資 + 少數白名單用戶，不會在 591 server 觸發異常偵測

    替代方案（合法但 cost / 範圍受限）：
      a) NLSC 申請：限政府機關/學術單位/國營機構，個人投資者申不到
      b) 內政部地政司「地政電子資料流通服務網」每筆 ~1 NTD，自己付費 + cache
      c) 維持 NLSC LANDSECT 段邊界 + 公有地（無個別地塊+地號，新北體驗差）
    """
    try:
        import math
        parts = [float(v) for v in bbox.split(",")]
        if len(parts) != 4:
            return None
        xmin, ymin, xmax, ymax = parts
        EARTH_HALF = 20037508.342789244
        tile_span = xmax - xmin
        if tile_span <= 0:
            return None
        z = round(math.log2(2 * EARTH_HALF / tile_span))
        if z < 0 or z > 22:
            return None
        n = 2 ** z
        full_span = 2 * EARTH_HALF / n
        x = round((xmin + EARTH_HALF) / full_span)
        y = round((EARTH_HALF - ymax) / full_span)
        if x < 0 or x >= n or y < 0 or y >= n:
            return None

        layer_id = cfg.get("layer_id", "DMAPS")
        url = f"https://maptiles.591.com.tw/S_Maps/wmts/{layer_id}/default/GoogleMapsCompatible/{z}/{y}/{x}"
        # 帶誠實的 referer 表明流量來源 (不偽裝成 591 內部，591 想抓就抓得到)
        r = httpx.get(url, timeout=10, verify=False, headers={
            "Referer": "https://taipei.retty-ai.com/",
            "User-Agent": "Mozilla/5.0 (urban-renewal personal research)",
        })
        if r.status_code != 200:
            return None
        ct = r.headers.get("content-type", "")
        if "image" not in ct or "gif" in ct:
            # 591 拒絕時通常回 image/gif size=0
            return None
        if len(r.content) < 100:
            return None
        return r.content
    except Exception as e:
        logger.debug(f"591 DMAPS proxy 例外: {e}")
        return None


def _fetch_nlsc_wms(cfg: dict, bbox: str, width: int, height: int, srs: str) -> Optional[bytes]:
    """NLSC WMS (maps.nlsc.gov.tw/S_Maps/wms → wms.nlsc.gov.tw/wms) — dynamic GetMap。

    NLSC 同時提供 WMS 跟 WMTS 兩個獨立的 OGC 標準 (WMS 2013/06、WMTS 2013/08，
    並存多年；不是升級關係)：
      WMTS — 預切 tile，固定 size (256×256)、速度快，解析度由 zoom level 決定
      WMS  — dynamic render，可指定任意 bbox + 任意 size

    對 LANDSECT 這種低密度 layer，給政府 server 較大 size (e.g. 1024×1024) 時
    image bytes 變大但「政府 render 出來的 polygon 資料密度」一樣 — image bytes
    多是因為 raster pixel 數變多 + 線條較平滑，不代表「detail 多」。NLSC LANDSECT
    本身就是「段外圍圖」，無論 zoom 多大都只有段邊界 (一段含數百地號)，沒個別地塊。

    無 token、無 referer 限制 (官方 doc 列為免費 OGC 標準介接)。
    """
    try:
        r = httpx.get(
            "https://maps.nlsc.gov.tw/S_Maps/wms",
            params={
                "service": "WMS",
                "version": "1.1.1",
                "request": "GetMap",
                "layers": cfg["layer_id"],
                "bbox": bbox,
                "srs": srs,
                "width": str(width),
                "height": str(height),
                "format": "image/png",
                "transparent": "true",
                "styles": "",
            },
            timeout=12,
            verify=False,
            follow_redirects=True,
        )
        if r.status_code != 200:
            logger.debug(f"NLSC WMS http={r.status_code} layer={cfg['layer_id']}")
            return None
        if "image" not in r.headers.get("content-type", ""):
            return None
        if len(r.content) < 100:
            return None
        return r.content
    except Exception as e:
        logger.warning(f"NLSC WMS 例外: {e}")
        return None


def _fetch_nlsc_wmts(cfg: dict, bbox: str, width: int, height: int, srs: str) -> Optional[bytes]:
    """NLSC WMTS — 把 Leaflet WMS-style request 轉成 WMTS GetTile (z/y/x)。

    NLSC WMTS 是 tile-based service (https://wmts.nlsc.gov.tw/wmts/{layer}/default/EPSG:3857/{z}/{y}/{x})。
    Leaflet `L.tileLayer.wms` 每個 tile request 的 bbox 剛好對應一個 web mercator tile (256×256)，
    所以 bbox→(z,x,y) 是 1:1 mapping，不會 lossy。
    """
    import math
    try:
        parts = [float(v) for v in bbox.split(",")]
        if len(parts) != 4:
            return None
        xmin, ymin, xmax, ymax = parts
    except ValueError:
        return None

    # 從 bbox 反算 web mercator tile 座標
    EARTH_HALF = 20037508.342789244
    tile_span = xmax - xmin   # 每 tile 寬 (web mercator meters)
    if tile_span <= 0:
        return None
    z = round(math.log2(2 * EARTH_HALF / tile_span))
    if z < 0 or z > 22:
        return None
    n = 2 ** z
    full_span = 2 * EARTH_HALF / n
    x = round((xmin + EARTH_HALF) / full_span)
    y = round((EARTH_HALF - ymax) / full_span)
    if x < 0 or x >= n or y < 0 or y >= n:
        # 跨日界線或極區 — 直接 fail，前端拿透明
        return None

    layer_id = cfg["layer_id"]
    url = f"https://wmts.nlsc.gov.tw/wmts/{layer_id}/default/EPSG:3857/{z}/{y}/{x}"
    try:
        r = httpx.get(url, timeout=10, verify=False)
        if r.status_code != 200:
            logger.debug(f"NLSC WMTS http={r.status_code} {url}")
            return None
        if "image" not in r.headers.get("content-type", ""):
            return None
        if len(r.content) < 100:
            return None
        return r.content
    except Exception as e:
        logger.warning(f"NLSC WMTS 例外: {e}")
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
    # 591 forward proxy 故意不 cache：純 forward (非重製)，每次 user 請求才走 591
    skip_cache = _LAYER_DEFS[layer]["kind"] == "591_dmaps_proxy"
    cache_key = (layer, bbox, width, height, srs)
    if not skip_cache:
        cached = _cache_get(cache_key)
        if cached:
            return Response(content=cached, media_type="image/png", headers={"X-Cache": "HIT"})

    cfg = _LAYER_DEFS[layer]
    if cfg["kind"] == "wms":
        content = _fetch_wms(cfg["upstream"], cfg["layers"], bbox, width, height, srs)
    elif cfg["kind"] == "arcgis_export":
        content = _fetch_arcgis_export(cfg, bbox, width, height, srs)
    elif cfg["kind"] == "nlsc_wms":
        content = _fetch_nlsc_wms(cfg, bbox, width, height, srs)
    elif cfg["kind"] == "nlsc_wmts":
        content = _fetch_nlsc_wmts(cfg, bbox, width, height, srs)
    elif cfg["kind"] == "591_dmaps_proxy":
        content = _fetch_591_dmaps(cfg, bbox, width, height, srs)
    else:
        raise HTTPException(500, f"unknown kind: {cfg['kind']}")

    if not content:
        # 上游失敗 → 回透明 1×1，前端 layer 不會 break；status 504 給前端可選擇靜默或顯示警告
        return Response(content=_TRANSPARENT_1X1_PNG, media_type="image/png", status_code=504, headers={"X-Cache": "MISS-FAIL"})

    if not skip_cache:
        _cache_set(cache_key, content)
    return Response(
        content=content,
        media_type="image/png",
        headers={"Cache-Control": "no-store" if skip_cache else "max-age=600", "X-Cache": "BYPASS" if skip_cache else "MISS"},
    )
