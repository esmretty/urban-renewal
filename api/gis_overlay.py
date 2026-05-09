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
    "cadastral_lines_tpe": {
        "kind": "wms", "upstream": _TPE_WMS_URL,
        "layers": "Taipei:LAND-ALL-TWD97",
        "disk_cache": True,
        "display_name": "台北市 地籍線",
    },
    "cadastral_numbers_tpe": {
        "kind": "wms", "upstream": _TPE_WMS_URL,
        "layers": "Taipei:LAND-ALL-TWD97-TEXT",
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
        "kind": "ntpcurinfo_cadastral",
        "disk_cache": True,
        "display_name": "新北市 地籍圖（個別地塊+地號）",
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
def _fetch_wms(upstream: str, layer_names: str, bbox: str, width: int, height: int, srs: str, cql_filter: Optional[str] = None) -> Optional[bytes]:
    """直接 forward WMS GetMap 到 GeoServer。
    cql_filter: GeoServer CQL filter（用來在同一個 layer 下篩 sub-set，例如 'layer=10'）

    注意：zonegeo.udd.gov.taipei GeoServer **不接受 SLD_BODY 或 user-supplied STYLES**，
    layer 預設 SLD render 是 grayscale (170,170,170 + 0,0,0 outline)。要彩色對齊
    UDDPlanMap 必須在前端用 SVG filter colorize tile image (見 map_overlays.css)。
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


# ── NtpcURInfo cadastral chain ─────────────────────────────────────────
# 新北市城鄉發展局 NtpcURInfo 系統的「地籍圖」layer (含個別地塊+地號)。
# Service 在 arcgis2.planning.ntpc.gov.tw/server/rest/services/NTPC_Urban/Land/MapServer
# 用 NTPC 自家 ArcGIS portal token 認證；token 從 NtpcURInfo session 動態拿，1-2hr expire。
#
# Chain：
#   1. fetch https://urban.planning.ntpc.gov.tw/NtpcURInfo/ → 從 HTML hidden #hdToken 拿 session token
#   2. POST ajax/datahandler.ashx { keyword: GetLayerList, timeStamp: hdToken, timeName: btoa("MapToken") }
#      → 拿 layer config 含「地籍圖」item 的 MAPSRVURL + AGSTOKEN
#   3. 用 AGSTOKEN 打 MAPSRVURL/export 拿 PNG image
#
# Token cache 約 1 小時 (NtpcURInfo session 通常活 30min-1hr，保守設 50 min refresh)
_NTPCURINFO_CACHE: dict = {"agstoken": None, "fetched_at": 0.0, "mapsrvurl": None}
_NTPCURINFO_TTL = 50 * 60  # 50 min


def _refresh_ntpcurinfo_token() -> Optional[tuple[str, str]]:
    """fetch NtpcURInfo 拿 fresh AGSTOKEN + MAPSRVURL，回 (token, url) 或 None。"""
    import base64
    import re as _re
    try:
        with httpx.Client(verify=False, timeout=15, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://urban.planning.ntpc.gov.tw/NtpcURInfo/",
        }) as client:
            r = client.get("https://urban.planning.ntpc.gov.tw/NtpcURInfo/")
            m = _re.search(r'id="hdToken"[^>]*value="([^"]+)"', r.text)
            if not m:
                logger.warning("NtpcURInfo: 抓不到 #hdToken")
                return None
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
                return None
            land = next((it for it in j.get("DATA", []) if it.get("LAYERNAME") == "地籍圖"), None)
            if not land:
                logger.warning("NtpcURInfo 沒找到「地籍圖」layer item")
                return None
            return land.get("AGSTOKEN", ""), land.get("MAPSRVURL", "")
    except Exception as e:
        logger.warning(f"NtpcURInfo refresh token 失敗: {e}")
        return None


def _get_ntpcurinfo_token() -> tuple[Optional[str], Optional[str]]:
    """拿 cached AGSTOKEN + MAPSRVURL，過期就 refresh。"""
    import time as _t
    if _NTPCURINFO_CACHE["agstoken"] and (_t.time() - _NTPCURINFO_CACHE["fetched_at"]) < _NTPCURINFO_TTL:
        return _NTPCURINFO_CACHE["agstoken"], _NTPCURINFO_CACHE["mapsrvurl"]
    res = _refresh_ntpcurinfo_token()
    if res:
        _NTPCURINFO_CACHE["agstoken"] = res[0]
        _NTPCURINFO_CACHE["mapsrvurl"] = res[1]
        _NTPCURINFO_CACHE["fetched_at"] = _t.time()
        return res
    return None, None


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


def _disk_cache_get(layer: str, z: int, y: int, x: int) -> Optional[bytes]:
    """讀 disk cache file。**沒有 TTL 過期** — file 永遠 valid 直到被手動或 scheduler 清掉。"""
    p = _DISK_CACHE_BASE / layer / f"{z}" / f"{y}" / f"{x}.png"
    if not p.exists():
        return None
    try:
        return p.read_bytes()
    except Exception as e:
        logger.debug(f"disk cache read fail {p}: {e}")
        return None


def _disk_cache_set(layer: str, z: int, y: int, x: int, content: bytes) -> None:
    p = _DISK_CACHE_BASE / layer / f"{z}" / f"{y}" / f"{x}.png"
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


# ── Scheduler config (settings/gis_overlay_scheduler) ─────────────────
# Active 模式：cache file 永不過期；只有 admin 手動點「清除 cache」或 scheduler 時間到
# 才執行 _disk_cache_clear (rmtree)，下次 user 看才重抓上游
def _load_overlay_scheduler() -> dict:
    """讀 Firestore settings/gis_overlay_scheduler；不存在回預設 (停用)。"""
    default = {
        "enabled": False,
        "interval_days": 30,            # 15 / 30 / 60 / 180
        "layers": [],                   # list of layer name (空 = 不影響任何 layer)
        "last_run_at": None,            # ISO timestamp，scheduler 上次清 cache 完成時間
    }
    try:
        from database.db import get_firestore
        doc = get_firestore().collection("settings").document("gis_overlay_scheduler").get()
        if doc.exists:
            d = doc.to_dict() or {}
            for k, v in default.items():
                if k not in d: d[k] = v
            return d
    except Exception as e:
        logger.debug(f"load gis_overlay_scheduler fail: {e}")
    return default


def _save_overlay_scheduler_field(field: str, value) -> None:
    """更新 settings/gis_overlay_scheduler 單一欄位 (merge)。"""
    try:
        from database.db import get_firestore
        get_firestore().collection("settings").document("gis_overlay_scheduler").set(
            {field: value}, merge=True,
        )
    except Exception as e:
        logger.warning(f"update gis_overlay_scheduler.{field} fail: {e}")


async def _gis_overlay_scheduler_loop():
    """Background runner — 每 1 hr check scheduler 設定，到時間了就 rmtree 指定 layer cache。
    用戶選 active 模式：cache 永不 lazy expire，只有 scheduler 或手動才會清。"""
    import asyncio as _asyncio
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    while True:
        try:
            await _asyncio.sleep(3600)   # 每 1 小時 check 一次 (interval_days 級別不需要更頻繁)
        except _asyncio.CancelledError:
            logger.info("[gis_overlay_scheduler] loop cancelled")
            break
        try:
            cfg = _load_overlay_scheduler()
            if not cfg.get("enabled"):
                continue
            layers = cfg.get("layers") or []
            if not layers:
                continue
            interval_days = int(cfg.get("interval_days") or 30)
            last_run = cfg.get("last_run_at")
            now_utc = _dt.now(_tz.utc)
            if last_run:
                try:
                    last_dt = _dt.fromisoformat(last_run.replace("Z", "+00:00"))
                    if (now_utc - last_dt) < _td(days=interval_days):
                        continue   # 還沒到下次 refresh 時間
                except Exception as e:
                    logger.debug(f"parse last_run_at fail (treat as never run): {e}")
            # 執行 refresh
            total_deleted = 0
            for name in layers:
                if name in _LAYER_DEFS:
                    total_deleted += _disk_cache_clear(name)
            _save_overlay_scheduler_field("last_run_at", now_utc.isoformat())
            logger.info(f"[gis_overlay_scheduler] refresh 完成: {len(layers)} layers, 共刪除 {total_deleted} 個 file")
        except Exception as e:
            logger.exception(f"[gis_overlay_scheduler] loop error: {e}")


def _fetch_ntpcurinfo_cadastral(cfg: dict, bbox: str, width: int, height: int, srs: str) -> Optional[bytes]:
    """打 NtpcURInfo 地籍圖 export — 個別地塊邊界 + 地號 polygon (新北市)。
    Disk cache 由 endpoint 統一處理 (此 fn 純 fetch)。"""
    token, mapsrv = _get_ntpcurinfo_token()
    if not token or not mapsrv:
        return None
    sr_num = srs.split(":")[-1] if ":" in srs else srs
    try:
        r = httpx.get(
            mapsrv + "/export",
            params={
                "token": token,
                "bbox": bbox,
                "bboxSR": sr_num,
                "imageSR": sr_num,
                "size": f"{width},{height}",
                "format": "png",
                "transparent": "true",
                "layers": "show:0",
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
                logger.debug(f"NtpcURInfo cadastral 拒絕 (可能 token 過期): {r.text[:200]}")
                _NTPCURINFO_CACHE["agstoken"] = None   # invalidate token, 下次 refresh
            return None
        if len(r.content) < 100:
            return None
        return r.content
    except Exception as e:
        logger.warning(f"NtpcURInfo cadastral fetch 例外: {e}")
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

    cfg = _LAYER_DEFS[layer]
    # skip_cache：都更圖層 (動態變動) + 591 forward proxy (不 cache 重製) 都不該 cache
    skip_cache = cfg["kind"] == "591_dmaps_proxy" or bool(cfg.get("skip_cache"))
    cache_key = (layer, bbox, width, height, srs)

    # 1. memory cache (10 min TTL，所有 layer 共用)
    if not skip_cache:
        cached = _cache_get(cache_key)
        if cached:
            return Response(content=cached, media_type="image/png", headers={"X-Cache": "HIT"})

    # 2. disk cache — 永不過期。只有 admin 手動清 cache 或 scheduler 時間到才 rmtree
    # 只對 Leaflet 預設 256×256 標準 tile request 才 cache
    disk_cache_on = bool(cfg.get("disk_cache")) and width == 256 and height == 256
    tile_xyz = _bbox_to_tile_xyz(bbox) if disk_cache_on else None
    if disk_cache_on and tile_xyz:
        z, y, x = tile_xyz
        disk_content = _disk_cache_get(layer, z, y, x)
        if disk_content:
            # 順便回填 memory cache (下個 user 同 tile 也 hit memory)
            if not skip_cache:
                _cache_set(cache_key, disk_content)
            return Response(content=disk_content, media_type="image/png", headers={"X-Cache": "DISK-HIT", "Cache-Control": "max-age=600"})
    if cfg["kind"] == "wms":
        content = _fetch_wms(cfg["upstream"], cfg["layers"], bbox, width, height, srs, cfg.get("cql_filter"))
    elif cfg["kind"] == "arcgis_export":
        content = _fetch_arcgis_export(cfg, bbox, width, height, srs)
    elif cfg["kind"] == "nlsc_wms":
        content = _fetch_nlsc_wms(cfg, bbox, width, height, srs)
    elif cfg["kind"] == "nlsc_wmts":
        content = _fetch_nlsc_wmts(cfg, bbox, width, height, srs)
    elif cfg["kind"] == "591_dmaps_proxy":
        content = _fetch_591_dmaps(cfg, bbox, width, height, srs)
    elif cfg["kind"] == "ntpcurinfo_cadastral":
        content = _fetch_ntpcurinfo_cadastral(cfg, bbox, width, height, srs)
    else:
        raise HTTPException(500, f"unknown kind: {cfg['kind']}")

    if not content:
        # 上游失敗 → 回透明 1×1，前端 layer 不會 break；status 504 給前端可選擇靜默或顯示警告
        return Response(content=_TRANSPARENT_1X1_PNG, media_type="image/png", status_code=504, headers={"X-Cache": "MISS-FAIL"})

    if not skip_cache:
        _cache_set(cache_key, content)
    # 寫 disk cache (Plan B lazy populate)
    if disk_cache_on and tile_xyz:
        z, y, x = tile_xyz
        _disk_cache_set(layer, z, y, x, content)
    return Response(
        content=content,
        media_type="image/png",
        headers={"Cache-Control": "no-store" if skip_cache else "max-age=600", "X-Cache": "BYPASS" if skip_cache else "MISS"},
    )


# ── Admin endpoints (僅 admin 可存取) ─────────────────────────────────
# 注意：此 file 沒 require_admin import，要從 api.auth 拿；放在 endpoint 上方
def _disk_cache_layers() -> list[str]:
    """列出所有有 disk_cache: True 設定的 layer name。"""
    return [name for name, cfg in _LAYER_DEFS.items() if cfg.get("disk_cache")]


from fastapi import Depends as _Depends
from api.auth import require_admin as _require_admin


@router.get("/admin/gis_overlay/cache_stats")
async def admin_cache_stats(admin: dict = _Depends(_require_admin)):
    """每 layer 的 disk cache 統計 (中文名 / file 數 / total bytes / 最舊 cache mtime)。"""
    out = []
    for name in _disk_cache_layers():
        cfg = _LAYER_DEFS[name]
        stats = _disk_cache_stats(name)
        out.append({
            "layer": name,
            "display_name": cfg.get("display_name", name),
            "file_count": stats["file_count"],
            "total_bytes": stats["total_bytes"],
            "oldest_mtime": stats["oldest_mtime"],
        })
    sched = _load_overlay_scheduler()
    # 計算下次 scheduler 預期執行時間
    next_run_at = None
    if sched.get("enabled") and sched.get("layers"):
        if sched.get("last_run_at"):
            try:
                from datetime import datetime as _dt, timezone as _tz, timedelta as _td
                last_dt = _dt.fromisoformat(sched["last_run_at"].replace("Z", "+00:00"))
                next_run_at = (last_dt + _td(days=int(sched["interval_days"]))).isoformat()
            except Exception:
                pass
        else:
            next_run_at = "下個小時 (尚未執行過)"
    return {"layers": out, "scheduler": sched, "next_run_at": next_run_at}


from pydantic import BaseModel as _BaseModel
from typing import List as _List


class _RefreshReq(_BaseModel):
    layers: _List[str]


@router.post("/admin/gis_overlay/refresh")
async def admin_refresh_layers(body: _RefreshReq, admin: dict = _Depends(_require_admin)):
    """手動清掉指定 layer 的 disk cache。下次 user 看時自動重抓 (Plan B 邏輯)。"""
    valid = set(_disk_cache_layers())
    out = []
    for name in body.layers:
        if name not in valid:
            out.append({"layer": name, "deleted": 0, "error": "unknown layer"})
            continue
        deleted = _disk_cache_clear(name)
        out.append({"layer": name, "deleted": deleted})
    return {"results": out}


class _SchedulerReq(_BaseModel):
    enabled: bool
    interval_days: int
    layers: _List[str]


_ALLOWED_INTERVAL_DAYS = {15, 30, 60, 180}


@router.post("/admin/gis_overlay/scheduler")
async def admin_set_scheduler(body: _SchedulerReq, admin: dict = _Depends(_require_admin)):
    """更新 settings/gis_overlay_scheduler config。
    啟用 + interval_days 立即影響所有 disk cache 的 TTL (lazy expiry，不需 background runner)。"""
    if body.interval_days not in _ALLOWED_INTERVAL_DAYS:
        raise HTTPException(400, f"interval_days 必須是 {sorted(_ALLOWED_INTERVAL_DAYS)} 之一")
    valid = set(_disk_cache_layers())
    invalid = [name for name in body.layers if name not in valid]
    if invalid:
        raise HTTPException(400, f"不存在的 layer: {invalid}")
    try:
        from database.db import get_firestore
        # merge=True 保留既有 last_run_at；如果 admin 改設定 layers/interval 想立刻重算下次時間
        # 可由 admin 額外觸發 manual refresh，那會更新 last_run_at
        get_firestore().collection("settings").document("gis_overlay_scheduler").set({
            "enabled": bool(body.enabled),
            "interval_days": int(body.interval_days),
            "layers": list(body.layers),
        }, merge=True)
    except Exception as e:
        raise HTTPException(500, f"寫 settings 失敗: {e}")
    return {"status": "ok", "config": _load_overlay_scheduler()}
