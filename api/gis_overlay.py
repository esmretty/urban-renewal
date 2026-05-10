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
        "kind": "wms", "upstream": _TPE_WMS_URL,
        "layers": "Taipei:ublock97-TWD97,Taipei:ublock97-TWD97-text",
        "disk_cache": True,
        "display_name": "台北市 土地分區",
    },
    # 地籍拆兩個 backend：線任 zoom 顯示，地號文字只 z=18 或 19 才顯示 (前端 minZoom 控制)
    # 用 format_options=dpi:60 (default 90) 同時縮線寬 + 縮字體 → 跟新北 NtpcURInfo 視覺接近
    # 注意：Taipei GeoServer 對 SLD_BODY / TextSymbolizer 全吞掉，只有 dpi 是有效的 styling 開關
    "cadastral_lines_tpe": {
        "kind": "wms", "upstream": _TPE_WMS_URL,
        "layers": "Taipei:LAND-ALL-TWD97",
        "format_options": "dpi:45",   # default 90 → 45 線變細一半
        "disk_cache": True,
        "display_name": "台北市 地籍線",
    },
    "cadastral_numbers_tpe": {
        "kind": "wms", "upstream": _TPE_WMS_URL,
        "layers": "Taipei:LAND-ALL-TWD97-TEXT",
        "format_options": "dpi:50",   # default 90 → 50 字體縮小但比 dpi:45 略大
        "disk_cache": True,
        "display_name": "台北市 地號文字",
    },
    # 台北建物樓層 — 都發局 GISDB layer 19 (建物_Build polygon, 含 4R/5R/T label)
    "building_floors_tpe": {
        "kind": "arcgis_export",
        "upstream": "https://www.historygis.udd.gov.taipei/arcgis/rest/services/Urban/GISDB/MapServer/export",
        "layer_show": "19",
        "disk_cache": True,
        "display_name": "台北市 建物套繪圖",
    },
    # 台北市「已劃定」都市更新地區範圍 — 都發局 PlanTheme layer 0 (332 個 polygon)
    # attributes: PROJNUM (案號)、PLANDES (公告說明)、PLANDATE (公告日期)、PLANLEV
    # 留著但 frontend 已不接 (用更完整的 redevelop_uro_tpe 取代，含審議中個案 + 整建住宅等 8 子類型)
    "renewal_planned_tpe": {
        "kind": "arcgis_export",
        "upstream": "https://www.historygis.udd.gov.taipei/arcgis/rest/services/UrbanPlan2/PlanTheme/MapServer/export",
        "layer_show": "0",
    },
    # 台北市都更審議子類型 — 對齊 https://bim.udd.gov.taipei/UDDPlanMap/Layer_Redevelop.json
    # 主 GeoServer layer Taipei:uro-redevelop-ALL-5 配 cql_filter (layer=N) 篩子類型；
    # 兩個獨立 layer (Taipei:115PublicPlanREArea-5, Taipei:63yAgoBud) 無 cql_filter
    # fill_color 必填：GeoServer 預設 SLD 是 grayscale，必須用 SLD_BODY override 對齊
    # UDDPlanMap fillcolor。
    # 用戶 2026-05-09 移除：redev_revoked (廢止89.91年)、redev_107expired (107年停用)、
    # redev_taipei_view (臺北好好看 II) → 13 個降到 10 個
    # 都更案隨時變動 (新申請/核准/廢止) — disk cache 永不過期，靠 admin scheduler 或手動清
    "redev_pub_renew":     {"kind": "wms", "upstream": _TPE_WMS_URL, "layers": "Taipei:uro-redevelop-ALL-5", "cql_filter": "layer=10", "fill_color": "#FF0000", "disk_cache": True, "display_name": "台北 都更/公劃更新地區"},
    "redev_chloride":      {"kind": "wms", "upstream": _TPE_WMS_URL, "layers": "Taipei:uro-redevelop-ALL-5", "cql_filter": "layer=44", "fill_color": "#D0B17A", "disk_cache": True, "display_name": "台北 都更/高氯離子混凝土"},
    "redev_urgent":        {"kind": "wms", "upstream": _TPE_WMS_URL, "layers": "Taipei:uro-redevelop-ALL-5", "cql_filter": "layer=48", "fill_color": "#FFD0FF", "disk_cache": True, "display_name": "台北 都更/迅行劃定"},
    "redev_self_announce": {"kind": "wms", "upstream": _TPE_WMS_URL, "layers": "Taipei:uro-redevelop-ALL-5", "cql_filter": "layer=20", "fill_color": "#0000FF", "disk_cache": True, "display_name": "台北 都更/公告自劃"},
    "redev_self_approved": {"kind": "wms", "upstream": _TPE_WMS_URL, "layers": "Taipei:uro-redevelop-ALL-5", "cql_filter": "layer=30", "fill_color": "#FF7F00", "disk_cache": True, "display_name": "台北 都更/核准自劃"},
    "redev_planned":       {"kind": "wms", "upstream": _TPE_WMS_URL, "layers": "Taipei:uro-redevelop-ALL-5", "cql_filter": "layer=40", "fill_color": "#FF00FF", "disk_cache": True, "display_name": "台北 都更/都計劃定"},
    "redev_invalid":       {"kind": "wms", "upstream": _TPE_WMS_URL, "layers": "Taipei:uro-redevelop-ALL-5", "cql_filter": "layer=50", "fill_color": "#00FFFF", "disk_cache": True, "display_name": "台北 都更/已失效或廢止"},
    "redev_pub_business":  {"kind": "wms", "upstream": _TPE_WMS_URL, "layers": "Taipei:uro-redevelop-ALL-5", "cql_filter": "layer=12", "fill_color": "#6495ED", "disk_cache": True, "display_name": "台北 都更/公劃內事業"},
    "redev_115_revised":   {"kind": "wms", "upstream": _TPE_WMS_URL, "layers": "Taipei:115PublicPlanREArea-5", "fill_color": "#FF9966", "disk_cache": True, "display_name": "台北 都更/115年修訂公劃"},
    "redev_63y_building":  {"kind": "wms", "upstream": _TPE_WMS_URL, "layers": "Taipei:63yAgoBud", "fill_color": "#1F4E79", "disk_cache": True, "display_name": "台北 都更/63年以前建築物"},
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
    # 新北市完整地籍圖 (個別地塊邊界 + 地號) — 透過 NtpcURInfo session 動態拿 token 後打 NTPC
    # 自家 ArcGIS server (arcgis2.planning.ntpc.gov.tw NTPC_Urban/Land/MapServer)
    "cadastral_full_ntpc": {
        "kind": "ntpcurinfo_layer",
        "ntpc_layer_name": "地籍圖",
        "disk_cache": True,
        "display_name": "新北市 地籍圖（個別地塊+地號）",
    },
    # 新北市都更圖層 — 全部走 NtpcURInfo NTPC_MyGis/NTPC_SI/MapServer，
    # 各 sub-layer 同 service 不同 layer ID (LAYERIDS)。
    # 都更案隨時變動 (新申請/核准/廢止) — disk cache 永不過期，靠 admin scheduler 或手動清。
    "redev_ntpc_ama": {
        "kind": "ntpcurinfo_layer", "ntpc_layer_name": "都市更新事業計畫案",
        "fill_color": "#FF7F00", "disk_cache": True,
        "display_name": "新北 都更/都市更新事業計畫案",
    },
    "redev_ntpc_easy": {
        "kind": "ntpcurinfo_layer", "ntpc_layer_name": "簡易都更",
        "fill_color": "#0066CC", "disk_cache": True,
        "display_name": "新北 都更/簡易都更",
    },
    "redev_ntpc_danger": {
        "kind": "ntpcurinfo_layer", "ntpc_layer_name": "危老重建",
        "fill_color": "#FF0000", "disk_cache": True,
        "display_name": "新北 都更/危老重建",
    },
    "redev_ntpc_amdm": {
        "kind": "ntpcurinfo_layer", "ntpc_layer_name": "防災案件",
        "fill_color": "#9933CC", "disk_cache": True,
        "display_name": "新北 都更/防災案件",
    },
    "redev_ntpc_rzoning": {
        "kind": "ntpcurinfo_layer", "ntpc_layer_name": "劃定更新地區",
        "fill_color": "#008080", "disk_cache": True,
        "display_name": "新北 都更/劃定更新地區",
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
        "dynamic_layers": '[{"id":0,"source":{"type":"mapLayer","mapLayerId":0},"drawingInfo":{"showLabels":false}}]',
        "needs_token": True,
        "disk_cache": True,
        "display_name": "新北市 土地分區",
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
def _fetch_wms(upstream: str, layer_names: str, bbox: str, width: int, height: int, srs: str,
               cql_filter: Optional[str] = None, sld_body: Optional[str] = None,
               format_options: Optional[str] = None) -> Optional[bytes]:
    """直接 forward WMS GetMap 到 GeoServer。
    cql_filter: GeoServer CQL filter（用來在同一個 layer 下篩 sub-set，例如 'layer=10'）
    sld_body: 自訂 SLD XML 字串 (override server-side default styling)。

    注意：zonegeo.udd.gov.taipei GeoServer 對 SLD_BODY 有限制 — 只認 stroke override，
    fill / TextSymbolizer 都被吞掉 (實測 v6/v7 fail)。redev layer 因此走前端 SVG filter
    colorize；地籍線改用 SLD_BODY 把預設粗灰線換成 0.4px 黑細線對齊新北視覺。
    """
    params = {
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
    }
    if cql_filter:
        params["cql_filter"] = cql_filter
    if sld_body:
        params["SLD_BODY"] = sld_body
    if format_options:
        params["format_options"] = format_options
    try:
        r = httpx.get(
            upstream,
            params=params,
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


# ── NtpcURInfo session chain ───────────────────────────────────────────
# 新北市 NtpcURInfo 系統的多個 dynamic layer 都走同一條 chain，但每個 layer 有自己的
# AGSTOKEN + MAPSRVURL + LAYERIDS。
#
# Chain：
#   1. fetch https://urban.planning.ntpc.gov.tw/NtpcURInfo/ → 從 HTML hidden #hdToken 拿 session token
#   2. POST ajax/datahandler.ashx { keyword: GetLayerList, timeStamp: hdToken, timeName: btoa("MapToken") }
#      → 一次拿全部 layer config (含 LAYERNAME / MAPSRVURL / AGSTOKEN / LAYERIDS)
#   3. 用 AGSTOKEN 打 MAPSRVURL/export?layers=show:LAYERIDS 拿 PNG image
#
# 整個 layer list 一起 refresh + cache (一次 datahandler call 換到所有 layer 的 token)。
# Token cache 約 50 min (NtpcURInfo session 通常活 30min-1hr，保守設 50 min refresh)。
# cookies 必須持續：UrbanRenewalQuery.ashx 用 hdToken 配合 ASP.NET_SessionId / TS010f2491
# session cookie 一起檢驗，缺 cookie → MSGCODE 300「參數檢驗失敗」
_NTPCURINFO_CACHE: dict = {"by_name": {}, "fetched_at": 0.0, "hdtoken": None, "cookies": {}}
_NTPCURINFO_TTL = 50 * 60  # 50 min


def _refresh_ntpcurinfo_layers() -> bool:
    """重新打 NtpcURInfo / datahandler.ashx GetLayerList，把全部 layer config (含
    AGSTOKEN/MAPSRVURL/LAYERIDS) 寫進 _NTPCURINFO_CACHE['by_name']。回 True/False。"""
    import base64
    import re as _re
    import time as _t
    try:
        with httpx.Client(verify=False, timeout=15, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://urban.planning.ntpc.gov.tw/NtpcURInfo/",
        }) as client:
            r = client.get("https://urban.planning.ntpc.gov.tw/NtpcURInfo/")
            m = _re.search(r'id="hdToken"[^>]*value="([^"]+)"', r.text)
            if not m:
                logger.warning("NtpcURInfo: 抓不到 #hdToken")
                return False
            hdtoken = m.group(1)
            r2 = client.post(
                "https://urban.planning.ntpc.gov.tw/NtpcURInfo/ajax/datahandler.ashx",
                data={
                    "keyword": "GetLayerList",
                    "timeStamp": hdtoken,
                    "timeName": base64.b64encode(b"MapToken").decode(),
                },
            )
            j = r2.json()
            if j.get("MSGCODE") != 200:
                logger.warning(f"NtpcURInfo GetLayerList MSGCODE={j.get('MSGCODE')}")
                return False
            by_name: dict[str, dict] = {}
            for it in j.get("DATA", []) or []:
                name = it.get("LAYERNAME") or ""
                if not name:
                    continue
                by_name[name] = {
                    "agstoken": it.get("AGSTOKEN") or "",
                    "mapsrvurl": it.get("MAPSRVURL") or "",
                    "layerids": (it.get("LAYERIDS") or "").strip() or "0",
                }
            if not by_name:
                logger.warning("NtpcURInfo GetLayerList: 0 layers")
                return False
            _NTPCURINFO_CACHE["by_name"] = by_name
            _NTPCURINFO_CACHE["hdtoken"] = hdtoken
            _NTPCURINFO_CACHE["fetched_at"] = _t.time()
            # 把 client 的 session cookies 保留下來給 UrbanRenewalQuery.ashx 用
            # (server 用 ASP.NET_SessionId + TS01xxxx 驗證 hdToken；缺 cookie → MSGCODE 300)
            try:
                _NTPCURINFO_CACHE["cookies"] = dict(client.cookies)
            except Exception:
                _NTPCURINFO_CACHE["cookies"] = {}
            return True
    except Exception as e:
        logger.warning(f"NtpcURInfo refresh layer list 失敗: {e}")
        return False


def _get_ntpcurinfo_layer_meta(layer_name: str) -> Optional[dict]:
    """拿指定 LAYERNAME (中文，例如「地籍圖」「都市更新事業計畫案」) 的 dict
    {agstoken, mapsrvurl, layerids}；過期就 refresh。"""
    import time as _t
    cache = _NTPCURINFO_CACHE
    fresh = cache["by_name"] and (_t.time() - cache["fetched_at"]) < _NTPCURINFO_TTL
    if not fresh:
        if not _refresh_ntpcurinfo_layers():
            return None
    return cache["by_name"].get(layer_name)


def _get_ntpcurinfo_hdtoken() -> Optional[str]:
    """拿 NtpcURInfo session 的 hdToken (給 UrbanRenewalQuery / InterfaceNtpcService 用)。
    跟 layer list 共用同一條 refresh，所以呼叫 _get_ntpcurinfo_layer_meta 任一 layer
    的 side effect 會把 hdtoken 也帶回來；這 fn 只是顯式接口。"""
    import time as _t
    cache = _NTPCURINFO_CACHE
    if cache["hdtoken"] and (_t.time() - cache["fetched_at"]) < _NTPCURINFO_TTL:
        return cache["hdtoken"]
    if _refresh_ntpcurinfo_layers():
        return cache["hdtoken"]
    return None


def _invalidate_ntpcurinfo_token() -> None:
    """token 被 server 拒絕時 (export 回 401/json) 強制下次 refresh。"""
    _NTPCURINFO_CACHE["fetched_at"] = 0.0
    _NTPCURINFO_CACHE["hdtoken"] = None
    _NTPCURINFO_CACHE["by_name"] = {}
    _NTPCURINFO_CACHE["cookies"] = {}


def _get_ntpcurinfo_cookies() -> dict:
    """拿 cached session cookies (給 UrbanRenewalQuery.ashx 用)；過期就 refresh。"""
    import time as _t
    cache = _NTPCURINFO_CACHE
    if cache["cookies"] and (_t.time() - cache["fetched_at"]) < _NTPCURINFO_TTL:
        return cache["cookies"]
    if _refresh_ntpcurinfo_layers():
        return cache["cookies"] or {}
    return {}


def _bbox_to_tile_xyz(bbox: str) -> Optional[tuple[int, int, int]]:
    """從 web mercator bbox 反算 tile (z, y, x)。Leaflet 256x256 tile 跟 web mercator
    tile 1:1 對應；只在 Leaflet 預設 tileSize=256 + WMS standard tile bbox 才精確。"""
    import math
    try:
        parts = [float(v) for v in bbox.split(",")]
        if len(parts) != 4: return None
        xmin, ymin, xmax, ymax = parts
    except ValueError:
        return None
    EARTH = 20037508.342789244
    span = xmax - xmin
    if span <= 0: return None
    z = round(math.log2(2 * EARTH / span))
    if z < 0 or z > 22: return None
    n = 2 ** z
    full = 2 * EARTH / n
    x = round((xmin + EARTH) / full)
    y = round((EARTH - ymax) / full)
    if x < 0 or x >= n or y < 0 or y >= n:
        return None
    return (z, y, x)


# Disk cache 通用機制 — Plan B (lazy populate)
# user 滑到哪 cache 哪，不主動 batch download，避免上游 server 偵測 burst pattern。
# 1 month 後熱區自然接近 100% cached，平日上游流量 = 平常 user browse 流量。
# 啟用方式：layer config 加 "disk_cache": True
import os as _os
from pathlib import Path as _Path
_DISK_CACHE_BASE = _Path(__file__).resolve().parent.parent / "data" / "cache"


def _disk_cache_variant(cfg: dict) -> str:
    """layer config 變動 (SLD / cql_filter / format_options) 自動產生新 cache path 後綴，
    避免換 style 後舊 tile 還在 serve。'' = 沒任何 variant 影響 → 用 base path 跟之前完全相容。"""
    import hashlib
    sld = cfg.get("sld_body") or ""
    cql = cfg.get("cql_filter") or ""
    fo = cfg.get("format_options") or ""
    if not sld and not cql and not fo:
        return ""
    h = hashlib.md5((sld + "|" + cql + "|" + fo).encode("utf-8")).hexdigest()[:8]
    return f"v_{h}"


def _disk_cache_path(layer: str, cfg: dict, z: int, y: int, x: int):
    variant = _disk_cache_variant(cfg)
    base = _DISK_CACHE_BASE / layer
    if variant:
        base = base / variant
    return base / f"{z}" / f"{y}" / f"{x}.png"


def _disk_cache_get(layer: str, cfg: dict, z: int, y: int, x: int) -> Optional[bytes]:
    """讀 disk cache file。**沒有 TTL 過期** — file 永遠 valid 直到被手動或 scheduler 清掉。
    cfg-driven variant：SLD/cql_filter 變動 → 自動切到新 cache path，舊的 orphan 留著。"""
    p = _disk_cache_path(layer, cfg, z, y, x)
    if not p.exists():
        return None
    try:
        return p.read_bytes()
    except Exception as e:
        logger.debug(f"disk cache read fail {p}: {e}")
        return None


def _disk_cache_set(layer: str, cfg: dict, z: int, y: int, x: int, content: bytes) -> None:
    p = _disk_cache_path(layer, cfg, z, y, x)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)
    except Exception as e:
        logger.debug(f"disk cache write fail {p}: {e}")


def _disk_cache_stats(layer: str) -> dict:
    """回 layer 的 cache file 數 + total size (bytes) + oldest mtime。"""
    base = _DISK_CACHE_BASE / layer
    if not base.exists():
        return {"file_count": 0, "total_bytes": 0, "oldest_mtime": None}
    count, total, oldest = 0, 0, None
    try:
        for p in base.rglob("*.png"):
            try:
                st = p.stat()
                count += 1
                total += st.st_size
                if oldest is None or st.st_mtime < oldest:
                    oldest = st.st_mtime
            except Exception:
                continue
    except Exception as e:
        logger.debug(f"_disk_cache_stats fail: {e}")
    return {"file_count": count, "total_bytes": total, "oldest_mtime": oldest}


def _disk_cache_clear(layer: str) -> int:
    """rmtree layer disk cache dir，回刪除 file 數量。"""
    import shutil
    base = _DISK_CACHE_BASE / layer
    if not base.exists():
        return 0
    count = sum(1 for _ in base.rglob("*.png"))
    try:
        shutil.rmtree(base)
    except Exception as e:
        logger.warning(f"disk cache clear {layer} fail: {e}")
        return 0
    return count


# 註：gis_overlay_refresh scheduler 已整合進既有 settings/scheduler 系統 (跟更新預售屋單價同
# 格式)，cmd type='gis_overlay_refresh'，由 api/app.py 的 _scheduled_scrape_loop 統一驅動。
# 本 file 只保留 _disk_cache_clear helper 給 scheduler runner / 手動 admin endpoint 共用。


def _fetch_ntpcurinfo_layer(cfg: dict, bbox: str, width: int, height: int, srs: str) -> Optional[bytes]:
    """打任一 NtpcURInfo dynamic layer 的 export endpoint (cfg['ntpc_layer_name'] = 中文 LAYERNAME)。
    Disk cache 由 endpoint 統一處理 (此 fn 純 fetch)。"""
    layer_name = cfg.get("ntpc_layer_name") or ""
    meta = _get_ntpcurinfo_layer_meta(layer_name)
    if not meta or not meta.get("agstoken") or not meta.get("mapsrvurl"):
        logger.debug(f"NtpcURInfo layer meta 抓不到: {layer_name}")
        return None
    sr_num = srs.split(":")[-1] if ":" in srs else srs
    try:
        r = httpx.get(
            meta["mapsrvurl"] + "/export",
            params={
                "token": meta["agstoken"],
                "bbox": bbox,
                "bboxSR": sr_num,
                "imageSR": sr_num,
                "size": f"{width},{height}",
                "format": "png",
                "transparent": "true",
                "layers": "show:" + meta["layerids"],
                "f": "image",
            },
            timeout=10,
            verify=False,
        )
        if r.status_code != 200:
            return None
        ct = r.headers.get("content-type", "")
        if "image" not in ct:
            if "json" in ct:
                logger.debug(f"NtpcURInfo {layer_name} 拒絕 (可能 token 過期): {r.text[:200]}")
                _invalidate_ntpcurinfo_token()
            return None
        if len(r.content) < 100:
            return None
        return r.content
    except Exception as e:
        logger.warning(f"NtpcURInfo {layer_name} fetch 例外: {e}")
        return None


# ── 新北 都更案件 by 座標 (auto-enrich 用) ────────────────────────────
# NtpcURInfo /ajax/UrbanRenewalQuery.ashx 提供 4 種 GetXxxCaseByXY，給定 TWD97 座標
# 回傳「該位置上所有套疊到的都更案件清單」(ID + ApplyPeople + shp)。
# 我們在物件分析 pipeline 裡呼叫一次，把 4 種類型結果合併存進 doc_data["redev_cases"]。
# 每筆 case 拿到 ID 後再打 GetXxxCaseDetail 取詳細欄位，server-side 串成 summary 字串。
# 重要：sub_type 的 ByXY keyword 必須對齊 map tile 底層 ArcGIS table，否則 filter / map
# 看到的是兩套不同資料：
#   layer 20 都市更新事業計畫案 → UNITSGEOMETRY → GetUnitsCaseByXY (不是 GetAMACaseByXY 整建維護諮詢)
#   layer 27 防災案件 → Self_Governance_Articles_Geometry → GetSGACaseByXY (不是 GetAMDMCaseByXY 整維案)
# detail_keyword 為空字串 → ByXY 已含足夠資料 (Units 走這條，欄位 UN01/UN02/Schedule 都在 ByXY 回應內)
_RENEWAL_QUERY_TYPES = [
    # (sub_type_id, by_xy_keyword, detail_keyword, display_label)
    ("ama",     "GetUnitsCaseByXY",         "",                       "都市更新事業計畫案"),
    ("easy",    "GetEasyUrbanCaseByXY",     "GetEasyUrbanCaseDetail", "簡易都更"),
    ("danger",  "GetDangerCaseByXY",        "GetDangerCaseDetail",    "危老重建"),
    ("amdm",    "GetSGACaseByXY",           "GetSGACaseDetail",       "防災案件"),
    ("rzoning", "GetRZoningUAreaCaseByXY",  "",                       "劃定更新地區"),
]


def _build_ntpc_case_summary(sub_type: str, detail: dict) -> str:
    """server-side 把 detail dict (or Units ByXY item) 串成 user-friendly summary。
    各 sub_type 欄位不同 — 挑出最有用的幾個：案名 / 狀態 / 段地號 / 改建規模。"""
    if not detail:
        return ""
    def _g(k):
        v = detail.get(k)
        if v is None or str(v).strip().lower() in ("none", ""):
            return ""
        return str(v).strip()
    if sub_type == "ama":
        # Units (UNITSGEOMETRY)：ByXY 直接回 UN02 (案名) + Schedule (狀態) + UN320 (階段)
        name = _g("UN02")
        schedule = _g("Schedule")
        stage = _g("UN320")
        parts = []
        if name: parts.append(name)
        sub = []
        if schedule: sub.append(schedule)
        if stage: sub.append(f"階段 {stage}")
        if sub: parts.append(" / ".join(sub))
        return " | ".join(parts)
    if sub_type == "danger":
        # 危老 detail：LandNo + 改建前/後規模
        landno = _g("LandNo")
        before = " ".join(filter(None, [_g("BeforeFloor") and _g("BeforeFloor") + "F",
                                        _g("BeforeConstruction"),
                                        _g("BeforeHousehold") and _g("BeforeHousehold") + "戶"]))
        after = " ".join(filter(None, [_g("AfterFloor") and _g("AfterFloor") + "F",
                                       _g("AfterConstruction"),
                                       _g("AfterHousehold") and _g("AfterHousehold") + "戶"]))
        parts = []
        if landno: parts.append(landno)
        if before and after: parts.append(f"原 {before} → 改建 {after}")
        elif before: parts.append(f"原 {before}")
        elif after: parts.append(f"改建 {after}")
        return " | ".join(parts)
    if sub_type == "easy":
        # 簡易都更 detail：基地面積 + 容積獎勵 + 設計廠商
        area = _g("Area")
        vr = _g("VolumeReward")
        vendor = _g("DesignVendor")
        parts = []
        if area: parts.append(f"基地 {area} ㎡")
        if vr: parts.append(f"容積獎勵 {vr}%")
        if vendor: parts.append(vendor)
        return " | ".join(parts)
    if sub_type == "amdm":
        # SGA detail (防災)：ProcessingStage 是合併「段地號 + 案類型」的長字串，當 case_name
        # 用；改建/原規模欄位 (NBuildingNo/OBuildingNo 等) 多為 null，只列有值的
        stage_name = _g("ProcessingStage")
        parts = []
        if stage_name: parts.append(stage_name)
        return " | ".join(parts)
    if sub_type == "rzoning":
        # 劃定更新地區：ByXY/ByDist 直接回 CaseName (例「劃定新北市板橋區公館段1986地號1筆土地更新地區」)
        return _g("CaseName")
    return ""


def _fetch_ntpc_case_detail(client: httpx.Client, hdtoken: str, time_name: str,
                            detail_kw: str, case_id) -> Optional[dict]:
    """打 GetXxxCaseDetail 拿單一案件詳細資料 dict (or None)。"""
    try:
        r = client.post(
            "https://urban.planning.ntpc.gov.tw/NtpcURInfo/ajax/UrbanRenewalQuery.ashx",
            data={
                "keyword": detail_kw,
                "caseID": case_id,
                "timeStamp": hdtoken,
                "timeName": time_name,
            },
        )
        if r.status_code != 200:
            return None
        j = r.json()
        if j.get("MSGCODE") != 200:
            return None
        md = j.get("MAINDATA")
        if isinstance(md, list) and md:
            return md[0]
        if isinstance(md, dict):
            return md
        return None
    except Exception as e:
        logger.debug(f"_fetch_ntpc_case_detail {detail_kw}/{case_id} 失敗: {e}")
        return None


# ── 台北 都更案件 by 座標 (auto-enrich 用) ────────────────────────────
# 走 zonegeo.udd.gov.taipei GeoServer WFS GetFeature + INTERSECTS(the_geom, POINT(X Y))，
# 同 endpoint 一次拿所有 sub-type (用 properties.layer 分流)。
# layer 對應 sub_type：10=pub_renew / 12=pub_business / 20=self_announce / 30=self_approved
#                       40=planned / 44=chloride / 48=urgent / 50=invalid
# 另兩個獨立 WFS layer：Taipei:115PublicPlanREArea-5 / Taipei:63yAgoBud
_TPE_REDEV_LAYER_MAP = {
    "10": ("pub_renew",     "公劃更新地區"),
    "12": ("pub_business",  "公劃內事業"),
    "20": ("self_announce", "公告自劃"),
    "30": ("self_approved", "核准自劃"),
    "40": ("planned",       "都計劃定"),
    "44": ("chloride",      "高氯離子混凝土"),
    "48": ("urgent",        "迅行劃定"),
    "50": ("invalid",       "已失效或廢止"),
}
_TPE_WFS_URL = "https://zonegeo.udd.gov.taipei/geoserver/Taipei/wfs"


def query_tpe_renewal_cases(lat: float, lng: float) -> list[dict]:
    """給定 WGS84 lat/lng → 回台北市 都更案件 (套疊到該位置的) 清單。
    走 GeoServer WFS GetFeature INTERSECTS。3 個 typeName 都查 (uro-redevelop-ALL-5 +
    115PublicPlanREArea-5 + 63yAgoBud)。失敗 / 非台北 → []。"""
    if lat is None or lng is None:
        return []
    try:
        from analysis.gov_gis import wgs84_to_twd97 as _to_twd97
        x, y = _to_twd97(float(lat), float(lng))
    except Exception as e:
        logger.debug(f"query_tpe_renewal_cases coord 換算失敗: {e}")
        return []
    # TWD97 台北範圍粗略 bbox
    if not (295000 <= x <= 320000 and 2762000 <= y <= 2790000):
        return []
    cases: list[dict] = []
    point_filter = f"INTERSECTS(the_geom, POINT({round(x)} {round(y)}))"
    queries = [
        ("Taipei:uro-redevelop-ALL-5", "ALL_5"),
        ("Taipei:115PublicPlanREArea-5", "115_revised"),
        ("Taipei:63yAgoBud", "63y_building"),
    ]
    try:
        for type_name, kind in queries:
            try:
                r = httpx.get(_TPE_WFS_URL, params={
                    "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                    "typeNames": type_name,
                    "outputFormat": "application/json",
                    "cql_filter": point_filter,
                }, timeout=8, verify=False)
                if r.status_code != 200:
                    continue
                j = r.json()
                for feat in j.get("features", []) or []:
                    props = feat.get("properties") or {}
                    if kind == "ALL_5":
                        layer_id = str(props.get("layer") or "")
                        sub = _TPE_REDEV_LAYER_MAP.get(layer_id)
                        if not sub:
                            continue
                        sub_id, sub_label = sub
                    elif kind == "115_revised":
                        sub_id, sub_label = "115_revised", "115年修訂公劃"
                    else:
                        sub_id, sub_label = "63y_building", "63年以前建築物"
                    cases.append({
                        "sub_type": sub_id,
                        "sub_type_label": sub_label,
                        "case_id": props.get("ID") or props.get("NO") or "",
                        "applicant": "",
                        "summary": "",  # 台北 GeoServer 沒對應 detail API；保留欄位 shape 一致
                    })
            except Exception as e:
                logger.debug(f"query_tpe_renewal_cases {type_name} 失敗: {e}")
                continue
    except Exception as e:
        logger.warning(f"query_tpe_renewal_cases httpx 失敗: {e}")
        return []
    return cases


def query_ntpc_renewal_cases(lat: float, lng: float, with_detail: bool = True,
                              client: Optional[httpx.Client] = None) -> list[dict]:
    """給定 WGS84 lat/lng → 回新北市 4 種都更案件 (套疊到該位置的) 清單。
    只 query 新北市範圍內座標；非新北 / lat/lng 缺 → 回 []。

    with_detail=True (default) → 每筆 case 額外打 GetXxxCaseDetail 取詳細欄位，
    寫進 case['summary'] (server-side 串好的 user-friendly 摘要)。
    client (optional) → 外部傳入的 long-lived httpx.Client (給 backfill 用)；
    沒傳就在這 fn 內部開 with-block (NTPC 第一次 connection setup ~3s，後續 ~0.03s — backfill
    跑 700+ 筆就要重用 client)。
    每筆 case 結構：{sub_type, sub_type_label, case_id, applicant, summary}"""
    if lat is None or lng is None:
        return []
    try:
        from analysis.gov_gis import wgs84_to_twd97 as _to_twd97
        x, y = _to_twd97(float(lat), float(lng))
    except Exception as e:
        logger.debug(f"query_ntpc_renewal_cases coord 換算失敗: {e}")
        return []
    if not (260000 <= x <= 360000 and 2730000 <= y <= 2810000):
        return []
    import base64
    hdtoken = _get_ntpcurinfo_hdtoken()
    if not hdtoken:
        return []
    cookies = _get_ntpcurinfo_cookies()
    timeName = base64.b64encode(b"MapToken").decode()
    base = "https://urban.planning.ntpc.gov.tw/NtpcURInfo/ajax/UrbanRenewalQuery.ashx"

    from concurrent.futures import ThreadPoolExecutor

    def _by_xy_one(cli, kw):
        try:
            r = cli.post(base, data={
                "keyword": kw, "X": str(round(x)), "Y": str(round(y)),
                "timeStamp": hdtoken, "timeName": timeName,
            })
            if r.status_code != 200:
                return None
            j = r.json()
            if j.get("MSGCODE") != 200:
                return None
            md = j.get("MAINDATA")
            return md if isinstance(md, list) else None
        except Exception as e:
            logger.debug(f"query_ntpc_renewal_cases {kw} 失敗: {e}")
            return None

    def _do(cli):
        cases: list[dict] = []
        with ThreadPoolExecutor(max_workers=4) as exe:
            futures = [
                (sub_id, by_xy_kw, detail_kw, label,
                 exe.submit(_by_xy_one, cli, by_xy_kw))
                for sub_id, by_xy_kw, detail_kw, label in _RENEWAL_QUERY_TYPES
            ]
            for sub_id, by_xy_kw, detail_kw, label, fut in futures:
                md = fut.result()
                if not md:
                    continue
                for item in md:
                    # Units (ama) case_id 在 UN01 欄位、無 ApplyPeople；其他 sub_type 用 ID + ApplyPeople
                    case_id = item.get("UN01") if sub_id == "ama" else item.get("ID")
                    case = {
                        "sub_type": sub_id,
                        "sub_type_label": label,
                        "case_id": case_id,
                        "applicant": item.get("ApplyPeople") or "",
                        "summary": "",
                    }
                    # detail_kw 空字串代表 ByXY 已含足夠資料 (Units)；直接用 item 當 detail 算 summary
                    if not detail_kw:
                        case["summary"] = _build_ntpc_case_summary(sub_id, item)
                    elif with_detail and case_id:
                        detail = _fetch_ntpc_case_detail(cli, hdtoken, timeName, detail_kw, case_id)
                        if detail:
                            case["summary"] = _build_ntpc_case_summary(sub_id, detail)
                    cases.append(case)
        return cases

    try:
        if client is not None:
            return _do(client)
        with httpx.Client(verify=False, timeout=8, cookies=cookies, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://urban.planning.ntpc.gov.tw/NtpcURInfo/",
        }) as cli:
            return _do(cli)
    except Exception as e:
        logger.warning(f"query_ntpc_renewal_cases httpx client 失敗: {e}")
        return []


def make_ntpc_query_client() -> httpx.Client:
    """給 backfill 用 — 開一個長期 httpx.Client 帶好 cookies + headers，可重複呼叫
    query_ntpc_renewal_cases(lat, lng, client=...) 共用。"""
    cookies = _get_ntpcurinfo_cookies()
    return httpx.Client(verify=False, timeout=12, cookies=cookies, headers={
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://urban.planning.ntpc.gov.tw/NtpcURInfo/",
    })


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


def _browser_cache_ttl(layer: str, cfg: dict) -> int:
    """瀏覽器 / CDN cache 期限 (seconds)。0 = no-store。
    對齊 admin scheduler 設定的 refresh 週期 (settings/scheduler，預設 15-180 天)：
    server disk cache 已經是永不過期 + 由 scheduler/admin 手動清，瀏覽器 cache 跟著拉長。
      stable layers (zoning / cadastral / building 多年不變): 90 天
      redev layers (case polygon 偶爾異動，scheduler 預設 15-30 天清):      30 天
      591 / skip_cache:                                                     0 = no-store
    user 開 v2 地圖第一次 fetch tile 之後，幾乎所有 panning / 重訪 都直接從瀏覽器 cache
    秒拿，不再 round-trip server (50-150ms × N tiles)。
    admin 手動清 cache 後，user 端要等 browser cache 自然 expire 才會看到新版 tile —
    這個 trade-off 接受 (跟 scheduler 週期同數量級)。"""
    if cfg["kind"] == "591_dmaps_proxy" or cfg.get("skip_cache"):
        return 0
    if layer.startswith("redev_"):
        return 30 * 86400      # 30 days
    return 90 * 86400          # 90 days


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

    cfg = _LAYER_DEFS[layer]
    # skip_cache：都更圖層 (動態變動) + 591 forward proxy (不 cache 重製) 都不該 cache
    skip_cache = cfg["kind"] == "591_dmaps_proxy" or bool(cfg.get("skip_cache"))
    # cache_key 含 SLD/cql variant — config 變動時 key 自動跟著換，避免吃舊 cache
    cache_key = (layer, _disk_cache_variant(cfg), bbox, width, height, srs)
    # 瀏覽器 / CDN cache 期限 — 跟 server-side disk cache (永不過期，靠 admin/scheduler 清) 不同概念
    # 控制 user 第 N 次造訪同 tile 時瀏覽器自己 cache 不再 round-trip 到 server
    browser_ttl = _browser_cache_ttl(layer, cfg)
    cc_header = "no-store" if browser_ttl <= 0 else f"public, max-age={browser_ttl}"

    # 1. memory cache (10 min TTL，所有 layer 共用)
    if not skip_cache:
        cached = _cache_get(cache_key)
        if cached:
            return Response(content=cached, media_type="image/png", headers={"X-Cache": "HIT", "Cache-Control": cc_header})

    # 2. disk cache — 永不過期。只有 admin 手動清 cache 或 scheduler 時間到才 rmtree
    # 只對 Leaflet 預設 256×256 標準 tile request 才 cache
    disk_cache_on = bool(cfg.get("disk_cache")) and width == 256 and height == 256
    tile_xyz = _bbox_to_tile_xyz(bbox) if disk_cache_on else None
    if disk_cache_on and tile_xyz:
        z, y, x = tile_xyz
        disk_content = _disk_cache_get(layer, cfg, z, y, x)
        if disk_content:
            # 順便回填 memory cache (下個 user 同 tile 也 hit memory)
            if not skip_cache:
                _cache_set(cache_key, disk_content)
            return Response(content=disk_content, media_type="image/png", headers={"X-Cache": "DISK-HIT", "Cache-Control": cc_header})
    if cfg["kind"] == "wms":
        content = _fetch_wms(cfg["upstream"], cfg["layers"], bbox, width, height, srs,
                             cfg.get("cql_filter"), cfg.get("sld_body"), cfg.get("format_options"))
    elif cfg["kind"] == "arcgis_export":
        content = _fetch_arcgis_export(cfg, bbox, width, height, srs)
    elif cfg["kind"] == "nlsc_wms":
        content = _fetch_nlsc_wms(cfg, bbox, width, height, srs)
    elif cfg["kind"] == "nlsc_wmts":
        content = _fetch_nlsc_wmts(cfg, bbox, width, height, srs)
    elif cfg["kind"] == "591_dmaps_proxy":
        content = _fetch_591_dmaps(cfg, bbox, width, height, srs)
    elif cfg["kind"] == "ntpcurinfo_layer":
        content = _fetch_ntpcurinfo_layer(cfg, bbox, width, height, srs)
    else:
        raise HTTPException(500, f"unknown kind: {cfg['kind']}")

    if not content:
        # 上游失敗 → 回透明 1×1，前端 layer 不會 break；status 504 給前端可選擇靜默或顯示警告
        # no-store：fail 可能是短暫 server 異常，不該被 browser/CDN cache 起來導致長期看不到 tile
        return Response(content=_TRANSPARENT_1X1_PNG, media_type="image/png", status_code=504,
                        headers={"X-Cache": "MISS-FAIL", "Cache-Control": "no-store"})

    if not skip_cache:
        _cache_set(cache_key, content)
    # 寫 disk cache (Plan B lazy populate)
    if disk_cache_on and tile_xyz:
        z, y, x = tile_xyz
        _disk_cache_set(layer, cfg, z, y, x, content)
    return Response(
        content=content,
        media_type="image/png",
        headers={"Cache-Control": cc_header, "X-Cache": "BYPASS" if skip_cache else "MISS"},
    )


# ── Admin endpoints (僅 admin 可存取) ─────────────────────────────────
# 注意：此 file 沒 require_admin import，要從 api.auth 拿；放在 endpoint 上方
def _disk_cache_layers() -> list[str]:
    """列出所有有 disk_cache: True 設定的 layer name。"""
    return [name for name, cfg in _LAYER_DEFS.items() if cfg.get("disk_cache")]


def _layer_data_source(name: str, cfg: dict) -> str:
    """回傳 layer 的資料來源 (中文) 給 admin UI 顯示。"""
    upstream = cfg.get("upstream", "") or ""
    kind = cfg.get("kind", "")
    if "zonegeo.udd.gov.taipei" in upstream:
        return "台北市都發局 GeoServer"
    if "historygis.udd.gov.taipei" in upstream:
        return "台北市都發局 GISDB"
    if "arcgis.planning.ntpc.gov.tw" in upstream:
        return "新北市城鄉發展局 ArcGIS"
    if kind == "ntpcurinfo_layer":
        return "新北市城鄉發展局 NtpcURInfo"
    if kind == "nlsc_wms":
        return "國土測繪中心 WMS"
    if kind == "nlsc_wmts":
        return "國土測繪中心 WMTS"
    if kind == "591_dmaps_proxy":
        return "591 地圖 (forward proxy)"
    return "—"


def _layer_group(name: str, cfg: dict) -> tuple[str, str]:
    """回傳 (group_id, group_label)；無 group 回 ('', '')。"""
    if name.startswith("redev_ntpc_"):
        return ("ntpc_renewal", "新北 都更圖層")
    if name.startswith("redev_"):
        return ("taipei_renewal", "台北 都更圖層")
    return ("", "")


def _layer_admin_meta(name: str) -> dict:
    """admin UI 用：layer name + display_name + data_source + group。"""
    cfg = _LAYER_DEFS.get(name) or {}
    g_id, g_label = _layer_group(name, cfg)
    return {
        "layer": name,
        "display_name": cfg.get("display_name", name),
        "data_source": _layer_data_source(name, cfg),
        "group_id": g_id,
        "group_label": g_label,
    }


from fastapi import Depends as _Depends
from api.auth import require_admin as _require_admin


@router.get("/admin/gis_overlay/cache_stats")
async def admin_cache_stats(admin: dict = _Depends(_require_admin)):
    """每 layer 的 disk cache 統計 (中文名 / file 數 / total bytes / 最舊 cache mtime)。
    scheduler 部分由既有 /admin/scheduler/status 提供 (cmd type='gis_overlay_refresh')。"""
    out = []
    for name in _disk_cache_layers():
        meta = _layer_admin_meta(name)
        stats = _disk_cache_stats(name)
        out.append({
            **meta,
            "file_count": stats["file_count"],
            "total_bytes": stats["total_bytes"],
            "oldest_mtime": stats["oldest_mtime"],
        })
    return {"layers": out}


from pydantic import BaseModel as _BaseModel
from typing import List as _List


class _RefreshReq(_BaseModel):
    layers: _List[str]


@router.post("/admin/gis_overlay/refresh")
async def admin_refresh_layers(body: _RefreshReq, admin: dict = _Depends(_require_admin)):
    """手動清掉指定 layer 的 disk cache。下次 user 看時自動重抓 (Plan B 邏輯)。
    透過 _run_gis_overlay_refresh 走同一條 log path → 出現在 admin 執行紀錄。"""
    valid = set(_disk_cache_layers())
    invalid = [n for n in body.layers if n not in valid]
    if invalid:
        raise HTTPException(400, f"不存在的 layer: {invalid}")
    from api.app import _run_gis_overlay_refresh
    msg = await _run_gis_overlay_refresh(body.layers, trigger_label="gis_overlay_refresh_manual")
    return {"status": "ok", "message": msg}


# 註：獨立 /admin/gis_overlay/scheduler endpoint 已移除。
# scheduler 設定改透過既有 /admin/scheduler/config 統一處理 (cmd type='gis_overlay_refresh')。
