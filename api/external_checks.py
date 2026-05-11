"""外部連結測試 router — 一鍵測試所有對外服務是否正常。

隔離原則：
  - 整個 module 自包含，所有 probe 邏輯都在這個 file
  - app.py 只用 1 行 `app.include_router(external_checks.router)` 掛載
  - revert 時刪本 file + 拿掉那 1 行 include 即可

設計：
  - 每個對外 dependency 一個 ProbeSpec：name / category / url / probe_fn / failure_impact
  - 平行 fire 所有 probe (max_workers=16)，timeout 15s/each
  - 結果 30 秒 in-memory cache，避免 admin 連按重啟 hammer 上游
  - 統一回 {ok, status_code, time_ms, note} 格式

API:
  - GET /admin/external_checks/run?force=0  → 跑所有 probe (cache hit 不重打)
  - GET /admin/external_checks/run?force=1  → 強制重打
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Optional

import httpx
from fastapi import APIRouter, Depends, Query

from api.auth import require_admin

logger = logging.getLogger(__name__)
router = APIRouter()


# ─── ProbeSpec ────────────────────────────────────────────────────────────
@dataclass
class ProbeSpec:
    name: str              # 顯示名稱
    category: str          # 分組顯示 (房源 scraper / 地圖 GIS / Google / AI / 其他)
    host: str              # 顯示用 (網域)
    failure_impact: str    # 用戶角度的失敗影響（給 admin 看的）
    probe: Callable[[], "ProbeResult"]


@dataclass
class ProbeResult:
    ok: bool
    status_code: Optional[int]
    time_ms: int
    note: str = ""         # 額外資訊或錯誤訊息


# ─── Probe helpers ────────────────────────────────────────────────────────
def _http_probe(url: str, *, timeout: float = 15.0, method: str = "HEAD",
                expect_status: tuple = (200,), follow_redirects: bool = True,
                headers: Optional[dict] = None) -> ProbeResult:
    """通用 HTTP probe — HEAD/GET 看 status code。"""
    t0 = time.time()
    try:
        with httpx.Client(timeout=timeout, verify=False, follow_redirects=follow_redirects) as c:
            if method == "HEAD":
                r = c.head(url, headers=headers or {})
            else:
                r = c.get(url, headers=headers or {})
        dt = int((time.time() - t0) * 1000)
        ok = r.status_code in expect_status
        return ProbeResult(ok=ok, status_code=r.status_code, time_ms=dt,
                           note="" if ok else f"預期 {expect_status} 得到 {r.status_code}")
    except httpx.TimeoutException:
        return ProbeResult(ok=False, status_code=None, time_ms=int((time.time() - t0) * 1000),
                           note=f"timeout (>{timeout}s)")
    except Exception as e:
        return ProbeResult(ok=False, status_code=None, time_ms=int((time.time() - t0) * 1000),
                           note=f"{type(e).__name__}: {str(e)[:80]}")


def _probe_firestore() -> ProbeResult:
    """Firestore 連線測：limit(1) 一個系統 collection"""
    t0 = time.time()
    try:
        from database.db import get_firestore
        db = get_firestore()
        list(db.collection("properties").limit(1).stream())
        return ProbeResult(ok=True, status_code=200, time_ms=int((time.time() - t0) * 1000))
    except Exception as e:
        return ProbeResult(ok=False, status_code=None, time_ms=int((time.time() - t0) * 1000),
                           note=f"{type(e).__name__}: {str(e)[:80]}")


def _probe_google_geocode() -> ProbeResult:
    """Google Geocoding API 測：固定地址查一筆"""
    import os
    api_key = os.getenv("GOOGLE_MAPS_API_KEY", "")
    if not api_key:
        return ProbeResult(ok=False, status_code=None, time_ms=0,
                           note="GOOGLE_MAPS_API_KEY 未設定")
    t0 = time.time()
    try:
        r = httpx.get("https://maps.googleapis.com/maps/api/geocode/json", params={
            "address": "台北101",
            "key": api_key,
            "language": "zh-TW",
        }, timeout=10, verify=False)
        dt = int((time.time() - t0) * 1000)
        ok = r.status_code == 200 and r.json().get("status") in ("OK", "ZERO_RESULTS")
        gstatus = r.json().get("status") if r.status_code == 200 else ""
        return ProbeResult(ok=ok, status_code=r.status_code, time_ms=dt,
                           note=f"Google status={gstatus}" if not ok else "")
    except Exception as e:
        return ProbeResult(ok=False, status_code=None, time_ms=int((time.time() - t0) * 1000),
                           note=f"{type(e).__name__}: {str(e)[:80]}")


def _probe_anthropic() -> ProbeResult:
    """Anthropic API key 測：list models endpoint"""
    import os
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return ProbeResult(ok=False, status_code=None, time_ms=0,
                           note="ANTHROPIC_API_KEY 未設定")
    t0 = time.time()
    try:
        r = httpx.get("https://api.anthropic.com/v1/models", headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        }, timeout=10, verify=False)
        dt = int((time.time() - t0) * 1000)
        return ProbeResult(ok=r.status_code == 200, status_code=r.status_code, time_ms=dt)
    except Exception as e:
        return ProbeResult(ok=False, status_code=None, time_ms=int((time.time() - t0) * 1000),
                           note=f"{type(e).__name__}: {str(e)[:80]}")


def _probe_line() -> ProbeResult:
    """LINE Messaging API：bot info endpoint 驗證 token"""
    import os
    token = os.getenv("LINE_CHANNEL_TOKEN", "").strip()
    if not token:
        return ProbeResult(ok=False, status_code=None, time_ms=0,
                           note="LINE_CHANNEL_TOKEN 未設定")
    t0 = time.time()
    try:
        r = httpx.get("https://api.line.me/v2/bot/info", headers={
            "Authorization": f"Bearer {token}",
        }, timeout=10, verify=False)
        dt = int((time.time() - t0) * 1000)
        return ProbeResult(ok=r.status_code == 200, status_code=r.status_code, time_ms=dt)
    except Exception as e:
        return ProbeResult(ok=False, status_code=None, time_ms=int((time.time() - t0) * 1000),
                           note=f"{type(e).__name__}: {str(e)[:80]}")


def _probe_taipei_geoserver() -> ProbeResult:
    """Taipei GeoServer WMS GetCapabilities"""
    return _http_probe(
        "https://zonegeo.udd.gov.taipei/geoserver/Taipei/wms?service=WMS&request=GetCapabilities",
        method="GET", timeout=15,
    )


def _probe_taipei_historygis() -> ProbeResult:
    """Taipei historygis ArcGIS — services root"""
    return _http_probe(
        "https://www.historygis.udd.gov.taipei/arcgis/rest/services?f=json",
        method="GET", timeout=15,
    )


def _probe_taipei_land_arcgis() -> ProbeResult:
    """Taipei 地政 ArcGIS - Land MapServer info"""
    return _http_probe(
        "https://maps.land.gov.taipei/server/rest/services/Tiled3857/Landtest/MapServer?f=json",
        method="GET", timeout=15,
    )


def _probe_ntpc_arcgis() -> ProbeResult:
    """NTPC ArcGIS — 用 token 真實 query 一個 layer info"""
    t0 = time.time()
    try:
        from analysis.gov_gis import _get_ntpc_token
        token = _get_ntpc_token()
        if not token:
            return ProbeResult(ok=False, status_code=None, time_ms=int((time.time() - t0) * 1000),
                               note="無法取得 NTPC token")
        r = httpx.get(
            "https://arcgis.planning.ntpc.gov.tw/server/rest/services/NTPC_Urban/LandUse_WMS/MapServer",
            params={"f": "json", "token": token},
            timeout=15, verify=False,
        )
        dt = int((time.time() - t0) * 1000)
        return ProbeResult(ok=r.status_code == 200, status_code=r.status_code, time_ms=dt,
                           note="" if r.status_code == 200 else f"http={r.status_code}")
    except Exception as e:
        return ProbeResult(ok=False, status_code=None, time_ms=int((time.time() - t0) * 1000),
                           note=f"{type(e).__name__}: {str(e)[:80]}")


def _probe_ntpc_urinfo_session() -> ProbeResult:
    """NtpcURInfo session — 試拿 cadastral 地籍圖 token (整個 session chain)"""
    t0 = time.time()
    try:
        from api.gis_overlay import _get_ntpcurinfo_layer_meta
        meta = _get_ntpcurinfo_layer_meta("地籍圖")
        dt = int((time.time() - t0) * 1000)
        ok = bool(meta and meta.get("agstoken") and meta.get("mapsrvurl"))
        return ProbeResult(ok=ok, status_code=200 if ok else None, time_ms=dt,
                           note="" if ok else "session/token 取不到")
    except Exception as e:
        return ProbeResult(ok=False, status_code=None, time_ms=int((time.time() - t0) * 1000),
                           note=f"{type(e).__name__}: {str(e)[:80]}")


def _probe_ntpc_urinfo_arcgis() -> ProbeResult:
    """NTPC URInfo 後端 ArcGIS — 直接打 server root"""
    return _http_probe(
        "https://arcgis2.planning.ntpc.gov.tw/server/rest/services?f=json",
        method="GET", timeout=15,
    )


def _probe_nlsc_wms() -> ProbeResult:
    """NLSC WMS GetCapabilities"""
    return _http_probe(
        "https://maps.nlsc.gov.tw/S_Maps/wms?service=WMS&request=GetCapabilities",
        method="GET", timeout=15,
    )


def _probe_nlsc_wmts() -> ProbeResult:
    """NLSC WMTS 抓一個固定 tile"""
    return _http_probe(
        "https://wmts.nlsc.gov.tw/wmts/LANDSECT/default/EPSG:3857/14/6852/13720",
        method="GET", timeout=15,
    )


def _probe_nlsc_townvillage() -> ProbeResult:
    """NLSC TownVillage 地址 reverse API"""
    return _http_probe(
        "https://api.nlsc.gov.tw/other/TownVillagePointQuery/121.5/25.05",
        method="GET", timeout=15,
    )


def _probe_591_dmaps() -> ProbeResult:
    """591 maptiles DMAPS 試抓一個 tile (需要 Referer)"""
    return _http_probe(
        "https://maptiles.591.com.tw/S_Maps/wmts/DMAPS/default/GoogleMapsCompatible/14/6852/13720",
        method="GET", timeout=15,
        headers={"Referer": "https://taipei.retty-ai.com/"},
    )


def _probe_plvr() -> ProbeResult:
    """內政部實價登錄首頁"""
    return _http_probe("https://plvr.land.moi.gov.tw/", method="GET", timeout=15)


def _probe_uddplanmap() -> ProbeResult:
    """UDDPlanMap 都更案視覺風格參考"""
    return _http_probe("https://bim.udd.gov.taipei/UDDPlanMap/", method="GET", timeout=15)


def _probe_591() -> ProbeResult:
    """591 列表頁 (HEAD avoid heavy DOM)"""
    return _http_probe("https://sale.591.com.tw/", method="GET", timeout=15,
                        headers={"User-Agent": "Mozilla/5.0"})


def _probe_yungching() -> ProbeResult:
    return _http_probe("https://buy.yungching.com.tw/", method="GET", timeout=15,
                        headers={"User-Agent": "Mozilla/5.0"})


def _probe_sinyi() -> ProbeResult:
    return _http_probe("https://www.sinyi.com.tw/", method="GET", timeout=15,
                        headers={"User-Agent": "Mozilla/5.0"})


def _probe_nominatim() -> ProbeResult:
    return _http_probe(
        "https://nominatim.openstreetmap.org/search?q=台北101&format=json&limit=1",
        method="GET", timeout=15,
        headers={"User-Agent": "Taipei-RealEstate-Retty/1.0"},
    )


def _probe_osm_tiles() -> ProbeResult:
    """OpenStreetMap base tile sample"""
    return _http_probe(
        "https://tile.openstreetmap.org/14/13720/6852.png",
        method="HEAD", timeout=15,
        headers={"User-Agent": "Mozilla/5.0"},
    )


# ─── ProbeSpec list ───────────────────────────────────────────────────────
PROBES: list[ProbeSpec] = [
    # ─── 房源 scraper ─────────────────────────────────────
    ProbeSpec(
        name="591 房屋", category="房源 scraper", host="sale.591.com.tw",
        failure_impact="新物件無法抓進來；既有物件查看不受影響",
        probe=_probe_591,
    ),
    ProbeSpec(
        name="永慶房屋", category="房源 scraper", host="buy.yungching.com.tw",
        failure_impact="永慶 cross-source 新物件無法抓；不影響 591 物件",
        probe=_probe_yungching,
    ),
    ProbeSpec(
        name="信義房屋", category="房源 scraper", host="www.sinyi.com.tw",
        failure_impact="信義 cross-source 新物件無法抓；不影響其他來源",
        probe=_probe_sinyi,
    ),
    # ─── GIS / 地圖 ─────────────────────────────────────
    ProbeSpec(
        name="台北 GeoServer", category="地圖 GIS", host="zonegeo.udd.gov.taipei",
        failure_impact="地圖模式台北土地分區/地籍/都更圖層全部失效；用戶看不到色塊",
        probe=_probe_taipei_geoserver,
    ),
    ProbeSpec(
        name="台北 historygis ArcGIS", category="地圖 GIS", host="www.historygis.udd.gov.taipei",
        failure_impact="台北建物樓層 4R/5R/T 標記消失；台北「已劃定都更」polygon 失效",
        probe=_probe_taipei_historygis,
    ),
    ProbeSpec(
        name="台北地政 ArcGIS", category="地圖 GIS", host="maps.land.gov.taipei",
        failure_impact="台北地塊搜尋 + 面積查詢失效（新發現的 endpoint）",
        probe=_probe_taipei_land_arcgis,
    ),
    ProbeSpec(
        name="新北 ArcGIS (zoning)", category="地圖 GIS", host="arcgis.planning.ntpc.gov.tw",
        failure_impact="新北土地分區圖層失效；新北物件分析無法判斷分區",
        probe=_probe_ntpc_arcgis,
    ),
    ProbeSpec(
        name="新北 URInfo (session)", category="地圖 GIS", host="urban.planning.ntpc.gov.tw",
        failure_impact="新北地籍圖 + 5 種都更圖層 + 地塊搜尋全失效；新北物件無法判斷套疊都更案",
        probe=_probe_ntpc_urinfo_session,
    ),
    ProbeSpec(
        name="新北 URInfo ArcGIS", category="地圖 GIS", host="arcgis2.planning.ntpc.gov.tw",
        failure_impact="URInfo 後端 tile 服務（同上影響範圍）",
        probe=_probe_ntpc_urinfo_arcgis,
    ),
    ProbeSpec(
        name="NLSC WMS", category="地圖 GIS", host="maps.nlsc.gov.tw",
        failure_impact="新北「段」分界線消失；對台北沒影響",
        probe=_probe_nlsc_wms,
    ),
    ProbeSpec(
        name="NLSC WMTS", category="地圖 GIS", host="wmts.nlsc.gov.tw",
        failure_impact="新北公有地圖層 (LAND_OPENDATA) 失效",
        probe=_probe_nlsc_wmts,
    ),
    ProbeSpec(
        name="NLSC TownVillage API", category="地圖 GIS", host="api.nlsc.gov.tw",
        failure_impact="地址→行政區/里 推斷失效；學區判定受影響",
        probe=_probe_nlsc_townvillage,
    ),
    ProbeSpec(
        name="591 dmaps tile", category="地圖 GIS", host="maptiles.591.com.tw",
        failure_impact="新北個別地塊精細顯示失效（fallback 到段邊界，影響小）",
        probe=_probe_591_dmaps,
    ),
    ProbeSpec(
        name="OpenStreetMap tile", category="地圖 GIS", host="tile.openstreetmap.org",
        failure_impact="Leaflet 底圖灰掉（用戶看到地圖空白）",
        probe=_probe_osm_tiles,
    ),
    # ─── 內政部 ─────────────────────────────────────
    ProbeSpec(
        name="實價登錄 (內政部)", category="政府資料", host="plvr.land.moi.gov.tw",
        failure_impact="新一季 LVR 資料無法更新；既有 LVR triangulate 不影響",
        probe=_probe_plvr,
    ),
    ProbeSpec(
        name="UDDPlanMap (都更案視覺)", category="政府資料", host="bim.udd.gov.taipei",
        failure_impact="都更 layer fillcolor 同步參考來源失效（影響小）",
        probe=_probe_uddplanmap,
    ),
    # ─── Google / Firebase ─────────────────────────────────────
    ProbeSpec(
        name="Firestore + Auth", category="Google", host="firestore.googleapis.com",
        failure_impact="整站爆炸：登不進、看不到 DB、不能改",
        probe=_probe_firestore,
    ),
    ProbeSpec(
        name="Google Geocoding", category="Google", host="maps.googleapis.com",
        failure_impact="新地址無座標；reverse_geo 失效；新物件無法上地圖",
        probe=_probe_google_geocode,
    ),
    ProbeSpec(
        name="OSM Nominatim", category="Google", host="nominatim.openstreetmap.org",
        failure_impact="Google geocode 失敗時 fallback 失效；影響小",
        probe=_probe_nominatim,
    ),
    # ─── AI / Notification ─────────────────────────────────────
    ProbeSpec(
        name="Anthropic Claude API", category="AI/通知", host="api.anthropic.com",
        failure_impact="所有新物件 AI 分析停擺；既有分析查看不影響",
        probe=_probe_anthropic,
    ),
    ProbeSpec(
        name="LINE Messaging API", category="AI/通知", host="api.line.me",
        failure_impact="新物件 LINE 通知無法送出；其他不影響",
        probe=_probe_line,
    ),
]


# ─── 30s in-memory result cache ───────────────────────────────────────────
_RESULT_CACHE: dict = {"ts": 0.0, "results": []}
_RESULT_CACHE_TTL = 30.0


def _run_all_probes() -> list[dict]:
    """平行 fire 所有 probe (max_workers=16)，回 list of dict。"""
    out: list[dict] = []

    def _wrap(spec: ProbeSpec):
        try:
            res = spec.probe()
        except Exception as e:
            res = ProbeResult(ok=False, status_code=None, time_ms=0,
                               note=f"probe exception: {type(e).__name__}: {str(e)[:80]}")
        return {
            "name": spec.name,
            "category": spec.category,
            "host": spec.host,
            "failure_impact": spec.failure_impact,
            "ok": res.ok,
            "status_code": res.status_code,
            "time_ms": res.time_ms,
            "note": res.note,
        }

    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = [ex.submit(_wrap, spec) for spec in PROBES]
        for fut in as_completed(futures):
            out.append(fut.result())
    # 保留 PROBES 原本順序 (admin UI 看起來有序)
    name_to_pos = {spec.name: i for i, spec in enumerate(PROBES)}
    out.sort(key=lambda x: name_to_pos.get(x["name"], 999))
    return out


@router.get("/admin/external_checks/run")
async def run_external_checks(
    force: int = Query(0, description="1 = 強制重打，0 = 用 30s cache"),
    admin: dict = Depends(require_admin),
):
    """跑所有 21 個對外 service 的健康檢查。"""
    now = time.time()
    if not force and (now - _RESULT_CACHE["ts"]) < _RESULT_CACHE_TTL and _RESULT_CACHE["results"]:
        return {
            "cached": True,
            "age_s": int(now - _RESULT_CACHE["ts"]),
            "total": len(_RESULT_CACHE["results"]),
            "ok_count": sum(1 for r in _RESULT_CACHE["results"] if r["ok"]),
            "results": _RESULT_CACHE["results"],
        }
    results = _run_all_probes()
    _RESULT_CACHE["ts"] = now
    _RESULT_CACHE["results"] = results
    return {
        "cached": False,
        "age_s": 0,
        "total": len(results),
        "ok_count": sum(1 for r in results if r["ok"]),
        "results": results,
    }
