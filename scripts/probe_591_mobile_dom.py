"""勘察 591 mobile 網頁 (m.591.com.tw) 的 DOM 結構，確認所有欄位都拿得到。

目標：取代 desktop 詳情頁 + Vision OCR 流程，用 mobile DOM 直接 querySelector 拿。

需要 feature parity 的欄位（desktop 現在抓的）：
  building_area_ping (建坪)
  land_area_ping (土地坪數)
  building_age (屋齡)
  floor / total_floors (樓層)
  price (價格)
  lat / lng (座標)
  community_name (社區名)
  community_address (建案地址)
  address (完整門牌)
  published_text / updated_text (上架/更新時間)
  bodyText (法拍偵測)
  title (法拍偵測)
  shape / building_type (公寓/透天)

執行：python scripts/probe_591_mobile_dom.py
產出：data/screenshots/_probe_mobile_dom.html (raw HTML) + 終端印 selector 命中
"""
from __future__ import annotations
import sys, os, time, json
sys.path.insert(0, ".")
os.makedirs("data/screenshots", exist_ok=True)

from playwright.sync_api import sync_playwright

URL = "https://m.591.com.tw/v2/sale/19344152"
OUT_HTML = "data/screenshots/_probe_mobile_dom.html"


def probe():
    with sync_playwright() as p:
        # mobile UA + viewport
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 414, "height": 896},  # iPhone 11 sizing
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        )
        page = ctx.new_page()
        t0 = time.time()
        page.goto(URL, wait_until="networkidle", timeout=30000)
        dt_load = time.time() - t0
        print(f"頁面載入: {dt_load:.2f}s")

        # 先存整個 HTML 給離線分析
        html = page.content()
        with open(OUT_HTML, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"HTML 寫入: {OUT_HTML} ({len(html)} bytes)")

        # 試各種 selector 看會抓到什麼
        # 1) title
        title = page.evaluate("() => document.title || ''")
        print(f"\n[title] {title!r}")

        # 2) 整個 body 純文字（看法拍偵測會抓到啥）
        body_text = page.evaluate("() => document.body.innerText")
        print(f"\n[body innerText 長度] {len(body_text)} chars")
        print(f"[前 600 字] {body_text[:600]!r}")

        # 3) 找關鍵字位置：建坪 / 土地 / 屋齡 / 樓層 / 上架 / 更新 / 建案
        for kw in ["建坪", "登記", "土地", "屋齡", "樓層", "上架", "更新", "建案", "地址", "社區", "經紀人"]:
            idx = body_text.find(kw)
            if idx >= 0:
                snippet = body_text[max(0, idx-30):idx+80]
                print(f"  '{kw}' @{idx}: {snippet!r}")

        # 4) 試 OG meta 抓 lat/lng / community
        meta = page.evaluate(r"""() => {
            const out = {};
            document.querySelectorAll('meta[property], meta[name]').forEach(m => {
                const k = m.getAttribute('property') || m.getAttribute('name');
                if (k) out[k] = m.getAttribute('content');
            });
            return out;
        }""")
        print(f"\n[meta keys] {list(meta.keys())}")
        for k in ("og:title", "og:description", "og:image", "og:url",
                  "lat", "lng", "geo.position", "ICBM", "description"):
            if k in meta:
                print(f"  {k}: {meta[k]!r}")

        # 5) 看是否有 __INITIAL_STATE__ / __NUXT__ / window.* 含 raw data
        scripts = page.evaluate(r"""() => {
            const s = document.querySelectorAll('script');
            const candidates = [];
            for (const x of s) {
                const t = x.textContent || '';
                if (t.includes('__NUXT__') || t.includes('__INITIAL_STATE__') ||
                    t.includes('window.HOUSE') || t.includes('houseData') ||
                    t.includes('lat') && t.includes('lng') && t.length > 200 && t.length < 500000) {
                    candidates.push(t.slice(0, 300));
                }
            }
            return candidates;
        }""")
        print(f"\n[script candidates] count={len(scripts)}")
        for i, s in enumerate(scripts[:3]):
            print(f"  [{i}]: {s!r}")

        # 6) 全頁 HTML 找含 lat / lng 的字串位置
        for kw in ['"lat":', '"lng":', '"latitude"', '"longitude"', 'lat=', 'lng=']:
            idx = html.find(kw)
            if idx >= 0:
                print(f"\n  HTML '{kw}' @{idx}: {html[max(0,idx-30):idx+150]!r}")

        # 7) 找 published / posttime / update_time
        for kw in ['posttime', 'publish', 'update_time', '上架', '更新時間']:
            idx = html.find(kw)
            if idx >= 0:
                print(f"  HTML '{kw}' @{idx}: {html[max(0,idx-30):idx+200]!r}")

        browser.close()


if __name__ == "__main__":
    probe()
