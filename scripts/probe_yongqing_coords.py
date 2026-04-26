"""一次性 probe：用 Playwright 抓永慶物件詳情頁的座標。
試多種策略：leaflet marker / iframe src / data-attr / network sniffing。"""
import json
import re
import sys
from playwright.sync_api import sync_playwright

URL = "https://buy.yungching.com.tw/house/4308114"
TIMEOUT_MS = 30_000


def main():
    captured_api = []
    found_coords = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
            locale="zh-TW",
            viewport={"width": 1280, "height": 900},
        )
        page = ctx.new_page()

        # 攔截所有回應，找含座標的 JSON
        def on_response(resp):
            url = resp.url
            ct = (resp.headers.get("content-type") or "").lower()
            if "json" in ct or "/api/" in url:
                try:
                    body = resp.text()
                    captured_api.append({"url": url, "len": len(body), "body_preview": body[:400]})
                    # grep 座標
                    for m in re.finditer(r'"(?:lat|latitude|lng|longitude|y|x)"\s*:\s*(-?\d+\.\d+)', body):
                        found_coords.append({"src": "api", "url": url, "match": m.group(0)})
                    for m in re.finditer(r'(2[45]\.\d{3,7})\D{1,30}(12[01]\.\d{3,7})', body):
                        found_coords.append({"src": "api_pair", "url": url, "lat": m.group(1), "lng": m.group(2)})
                except Exception:
                    pass

        page.on("response", on_response)

        print(f"[1] 開啟 {URL}")
        page.goto(URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
        print("[2] 等待 networkidle (讓 lazy load 完成)")
        try:
            page.wait_for_load_state("networkidle", timeout=TIMEOUT_MS)
        except Exception as e:
            print(f"   networkidle timeout: {e}")

        # 滾到地圖區強制觸發 lazy render
        print("[3] 滾動到「位置」區段觸發地圖")
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.5)")
            page.wait_for_timeout(2000)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.7)")
            page.wait_for_timeout(2000)
        except Exception as e:
            print(f"   scroll fail: {e}")

        # === 策略 A: 從 DOM 找 leaflet marker / google maps 元素 ===
        print("[4] 策略 A: DOM 搜尋 leaflet / google maps")
        dom_coords = page.evaluate("""() => {
            const results = [];
            // leaflet marker (有 _latlng 屬性)
            document.querySelectorAll('.leaflet-marker-icon').forEach(el => {
                if (el._latlng) results.push({type: 'leaflet_marker', lat: el._latlng.lat, lng: el._latlng.lng});
            });
            // 用 leaflet global 物件
            if (typeof L !== 'undefined' && L.marker) {
                results.push({type: 'leaflet_loaded', has_L: true});
            }
            // google maps iframe src
            document.querySelectorAll('iframe').forEach(f => {
                const src = f.src || '';
                const m1 = src.match(/[?&!]q=(-?\\d+\\.\\d+)[%2C,](-?\\d+\\.\\d+)/);
                const m2 = src.match(/!2d(-?\\d+\\.\\d+)!3d(-?\\d+\\.\\d+)/);
                const m3 = src.match(/center=(-?\\d+\\.\\d+)[%2C,](-?\\d+\\.\\d+)/);
                if (m1) results.push({type: 'iframe_q', lat: m1[1], lng: m1[2], src: src.slice(0,200)});
                if (m2) results.push({type: 'iframe_2d3d', lng: m2[1], lat: m2[2], src: src.slice(0,200)});
                if (m3) results.push({type: 'iframe_center', lat: m3[1], lng: m3[2], src: src.slice(0,200)});
                if (src.includes('google.com/maps') || src.includes('maps.googleapis')) {
                    results.push({type: 'iframe_url', src: src.slice(0,300)});
                }
            });
            // data-lat / data-lng
            document.querySelectorAll('[data-lat], [data-lng], [data-latitude], [data-longitude]').forEach(el => {
                const lat = el.getAttribute('data-lat') || el.getAttribute('data-latitude');
                const lng = el.getAttribute('data-lng') || el.getAttribute('data-longitude');
                if (lat || lng) results.push({type: 'data_attr', lat, lng, tag: el.tagName, cls: el.className.slice(0,80)});
            });
            // 找包含 google maps 連結的 a 標籤
            document.querySelectorAll('a[href*="google.com/maps"]').forEach(a => {
                results.push({type: 'a_gmap_link', href: a.href.slice(0,300)});
            });
            return results;
        }""")
        print(f"   DOM 結果: {json.dumps(dom_coords, ensure_ascii=False, indent=2)}")

        # === 策略 B: 全頁文本 + 屬性 grep 座標 pattern ===
        print("[5] 策略 B: 整頁 outerHTML grep")
        full_html = page.content()
        pairs = re.findall(r'(2[45]\.\d{3,7})[^\d]{1,30}(12[01]\.\d{3,7})', full_html)
        if pairs:
            print(f"   找到 {len(pairs)} 組可能座標 pair (lat~25, lng~121)：{pairs[:5]}")
        else:
            print("   無座標 pair")

        single_lats = re.findall(r'"lat[itude]*"\s*:\s*"?(-?\d+\.\d+)', full_html)[:5]
        single_lngs = re.findall(r'"lng[itude]*"\s*:\s*"?(-?\d+\.\d+)|"longitude"\s*:\s*"?(-?\d+\.\d+)', full_html)[:5]
        print(f"   single lat hits: {single_lats}")
        print(f"   single lng hits: {single_lngs}")

        browser.close()

    # === 結果摘要 ===
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"URL: {URL}")
    print(f"DOM 策略找到的座標: {dom_coords}")
    print(f"網路攔截到的 API 數: {len(captured_api)}")
    print(f"從 API 抓到的座標 hit: {len(found_coords)}")
    for fc in found_coords[:10]:
        print(f"  - {fc}")
    print()
    if captured_api:
        print("前 10 個 API 呼叫：")
        for c in captured_api[:10]:
            print(f"  {c['url']}  (len={c['len']})")


if __name__ == "__main__":
    main()
