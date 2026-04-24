"""
UDDPlanMap (台北市都市開發審議地圖) 截圖：
打開都更相關圖層、置中到指定 (lat,lng)、加紅色 marker、存截圖。
用來給每個物件一張「都更利多地圖」讓使用者直接看。

CLI:
    python -m analysis.uddplanmap_screenshot <lat> <lng> [out.png] [--zoom 18]
"""
import argparse
import logging
from pathlib import Path
from typing import Optional

from playwright.sync_api import sync_playwright, BrowserContext

logger = logging.getLogger(__name__)


def _wait_tiles(page, timeout_ms=30000, stable_ms=1500, label=""):
    """
    等 ArcGIS map 所有 layer tile 讀完：
      1. 先等至少有一次 update-end 被觸發（確保開始載）
      2. 再等 pending==0 並維持 stable_ms 內沒新 pending 才返回
    """
    # 1) 等第一次 update-end 事件
    try:
        page.wait_for_function(
            "() => window.__tileHook && window.__tileHook.everLoaded",
            timeout=timeout_ms,
        )
    except Exception:
        logger.warning("wait_tiles[%s]: never loaded within %dms", label, timeout_ms)
        return
    # 2) pending 要 stable_ms 內保持 0
    deadline = timeout_ms
    elapsed = 0
    stable_for = 0
    step = 300
    while elapsed < deadline:
        page.wait_for_timeout(step)
        elapsed += step
        pending = page.evaluate("() => window.__tileHook ? window.__tileHook.pending : 0")
        if pending == 0:
            stable_for += step
            if stable_for >= stable_ms:
                logger.info("wait_tiles[%s]: stable for %dms (total %dms)", label, stable_for, elapsed)
                return
        else:
            stable_for = 0
    logger.warning("wait_tiles[%s]: gave up after %dms, pending=%s", label, elapsed, pending)

URL = "https://bim.udd.gov.taipei/UDDPlanMap/"

# 要打開的圖層 — 以 (layerid, opacity) 為準；opacity=None 則不覆寫預設
#  - 基本圖層：土地使用分區圖 30%, 地籍圖 50%（使用者要求永遠打開）
#  - 都市更新審議：公劃/迅行/自劃/老屋等
DEFAULT_LAYERS = [
    ("TAIPEI_MAP", None),            # 台北底圖
    ("UrbanPlan", 0.3),              # 土地使用分區圖 30%
    ("Urban_Land", 0.5),             # 地籍圖 50%
    # ── 都市更新審議 section ──
    ("segment10", None),             # 公劃更新地區(依都更條例)
    ("segment48", None),             # 迅行劃定更新地區
    ("segment40", None),             # 都市計畫劃定更新地區
    ("segment60", None),             # 107年公劃更新地區
    ("segment12", None),             # 公劃地區內事業(權變案件)
    ("segment20", None),             # 公告自劃單元
    ("segment30", None),             # 核准自劃單元
    ("buildmore50y", 0.5),           # 63年以前建築物 50%
]

_ENABLE_LAYERS_JS = """
(targets) => {
    const inputs = Array.from(document.querySelectorAll('input.Layer-Item[layerid]'));
    const wantIds = new Set(targets.map(t => t.id));
    const out = [];
    // 1) 先把不在 target 裡、但目前是 on 的 layer 關掉（跨 shot reset）
    for (const el of inputs) {
        const lid = el.getAttribute('layerid');
        if (!wantIds.has(lid) && el.checked) {
            const toggle = el.closest('.toggle');
            if (toggle) toggle.click(); else el.click();
        }
    }
    // 2) 打開 target layers
    for (const t of targets) {
        const el = inputs.find(x => x.getAttribute('layerid') === t.id);
        if (!el) { out.push({id: t.id, ok: false, reason: 'not_found'}); continue; }
        if (!el.checked) {
            const toggle = el.closest('.toggle');
            if (toggle) toggle.click(); else el.click();
        }
        out.push({id: t.id, ok: true, checked: el.checked});
    }
    return out;
}
"""

# 應用 opacity 到 map 上已存在的 layer（要等 layer 被加入 map 之後才能成功）
_APPLY_OPACITY_JS = """
(targets) => {
    const out = [];
    const m = window.map;
    const ids = m.layerIds || [];
    for (const t of targets) {
        if (t.opacity == null) continue;
        // 嘗試多種可能的 id
        const candidates = [t.id, t.id + "_0", t.id + "_1"];
        let matched = null;
        for (const cid of candidates) {
            const l = m.getLayer(cid);
            if (l && typeof l.setOpacity === 'function') { matched = cid; l.setOpacity(t.opacity); break; }
        }
        // 再 fallback: 找 layerIds 裡含 t.id 字串的
        if (!matched) {
            for (const lid of ids) {
                if (lid && lid.indexOf(t.id) >= 0) {
                    const l = m.getLayer(lid);
                    if (l && typeof l.setOpacity === 'function') { l.setOpacity(t.opacity); matched = lid; break; }
                }
            }
        }
        out.push({id: t.id, opacity: t.opacity, applied_as: matched, all_ids: ids.slice(0, 50)});
    }
    return out;
}
"""

_CENTER_AND_MARK_JS = """
([lng, lat, z, polygons]) => new Promise((resolve) => {
    try {
        require([
            "esri/geometry/Point", "esri/geometry/Polygon",
            "esri/SpatialReference",
            "esri/geometry/webMercatorUtils",
            "esri/graphic", "esri/symbols/SimpleMarkerSymbol",
            "esri/symbols/SimpleLineSymbol", "esri/symbols/SimpleFillSymbol",
            "esri/Color", "esri/layers/GraphicsLayer"
        ], function(Point, PolygonG, SR, wm, Graphic, SMS, SLS, SFS, Color, GL) {
            const wgs = new Point(lng, lat, new SR({wkid: 4326}));
            const proj = wm.geographicToWebMercator(wgs);
            window.map.centerAndZoom(proj, z);
            let g = window.__probeGL;
            if (!g) { g = new GL(); window.map.addLayer(g); window.__probeGL = g; }
            g.clear();

            // 先畫 polygon overlay（紅框紅底半透明）
            if (polygons && polygons.length) {
                const fill = new SFS(
                    SFS.STYLE_SOLID,
                    new SLS(SLS.STYLE_SOLID, new Color([230, 30, 30, 0.95]), 3),
                    new Color([230, 30, 30, 0.25])
                );
                polygons.forEach(rings => {
                    const pg = new PolygonG({rings: rings, spatialReference: {wkid: 102100}});
                    g.add(new Graphic(pg, fill));
                });
            }

            // 再畫中心紅點（確保在 polygon 上層）
            const sym = new SMS(SMS.STYLE_CIRCLE, 16,
                new SLS(SLS.STYLE_SOLID, new Color([0, 80, 255, 1]), 3),
                new Color([0, 80, 255, 0.6]));
            g.add(new Graphic(proj, sym));
            resolve({ok: true, x: proj.x, y: proj.y});
        });
    } catch (e) { resolve({ok: false, err: e.message}); }
})
"""


def capture(
    lat: float,
    lng: float,
    out_path: str,
    zoom: int = 18,
    layers=None,
    ctx: Optional[BrowserContext] = None,
    extra_shots=None,
    polygons_webmerc=None,   # [[[x,y],...], ...] SR=102100，會畫紅框半透明紅底
) -> str:
    """
    開 UDDPlanMap，打開圖層、置中到 (lat,lng)、加 marker、截圖到 out_path。
    回傳 out_path。

    ctx: 傳入現有 BrowserContext 避免重複開瀏覽器。若為 None 自開自關。
    extra_shots: [(zoom, out_path, extra_layers)] — 在同一個 page 上改 zoom/圖層再截圖，
                 避免重跑整支流程。extra_layers 為 None 代表沿用。
    """
    layers = layers or DEFAULT_LAYERS

    def _run(context: BrowserContext):
        page = context.new_page()
        try:
            page.goto(URL, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(3000)
            page.wait_for_function(
                "() => window.map && window.map.loaded === true",
                timeout=20000,
            )

            # 掛監聽：追蹤所有 layer 的 update-start / update-end 事件
            page.evaluate("""
            () => {
                if (window.__tileHook) return;
                window.__tileHook = { pending: 0, everLoaded: false };
                const hook = (layer) => {
                    if (!layer || !layer.on) return;
                    layer.on('update-start', () => { window.__tileHook.pending++; });
                    layer.on('update-end',   () => {
                        window.__tileHook.pending = Math.max(0, window.__tileHook.pending - 1);
                        window.__tileHook.everLoaded = true;
                    });
                };
                (window.map.layerIds || []).forEach(id => hook(window.map.getLayer(id)));
                (window.map.graphicsLayerIds || []).forEach(id => hook(window.map.getLayer(id)));
                // 之後新增的 layer 也要 hook
                window.map.on('layer-add-result', e => hook(e.layer));
            }
            """)
            try:
                page.keyboard.press("Escape")
                page.wait_for_timeout(300)
            except Exception:
                pass

            def _apply_layers(layer_list):
                targets = [{"id": lid, "opacity": op} for (lid, op) in layer_list]
                r = page.evaluate(_ENABLE_LAYERS_JS, targets)
                logger.info("uddplanmap layer toggle: %s", [(x["id"], x.get("checked")) for x in r])
                # 等 layer 被加入 map 完成（layer-add-result 事件）
                _wait_tiles(page, label="after layer toggle", stable_ms=1000, timeout_ms=15000)
                r2 = page.evaluate(_APPLY_OPACITY_JS, targets)
                logger.info("uddplanmap opacity apply: %s", [(x["id"], x.get("applied_as")) for x in r2])

            def _center_and_shot(zm, out):
                result = page.evaluate(_CENTER_AND_MARK_JS, [lng, lat, zm, polygons_webmerc or []])
                logger.info("uddplanmap center: z=%s result=%s", zm, result)
                # 等所有 tile layer 真的讀完
                _wait_tiles(page, label=f"shot z={zm}")
                Path(out).parent.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=out, full_page=False)
                logger.info("uddplanmap screenshot saved: %s (z=%s)", out, zm)

            _apply_layers(layers)
            page.wait_for_timeout(2000)
            _center_and_shot(zoom, out_path)

            for ez, eout, elayers in (extra_shots or []):
                if elayers is not None:
                    _apply_layers(elayers)
                    page.wait_for_timeout(1500)
                _center_and_shot(ez, eout)
        finally:
            page.close()

    if ctx is not None:
        _run(ctx)
    else:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx2 = browser.new_context(
                viewport={"width": 1920, "height": 1200},
                device_scale_factor=2,   # 2x 提高截圖解析度
            )
            try:
                _run(ctx2)
            finally:
                browser.close()
    return out_path


def _cli():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("lat", type=float)
    ap.add_argument("lng", type=float)
    ap.add_argument("out", nargs="?", default="uddplanmap.png")
    ap.add_argument("--zoom", type=int, default=18)
    args = ap.parse_args()
    capture(args.lat, args.lng, args.out, zoom=args.zoom)


if __name__ == "__main__":
    _cli()
