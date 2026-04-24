"""偵察全國土地使用分區查詢系統的後端 ArcGIS REST endpoint。"""
from playwright.sync_api import sync_playwright

URLS = ["https://zonemap.udd.gov.taipei/ZoneMapOP/"]

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context()
    page = ctx.new_page()
    interesting = []
    # 抓所有 XHR / fetch 請求（不只 arcgis）
    def hook(r):
        u = r.url
        if r.resource_type in ("xhr", "fetch") or "rest/services" in u or "Service" in u or ".aspx" in u or ".ashx" in u:
            interesting.append(("REQ", u, r.method))
    page.on("request", hook)

    for url in URLS:
        print(f"\n=== {url} ===")
        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(5000)
        except Exception as e:
            print(f"  goto error: {e}")
        seen_hosts = set()
        for kind, u, method in interesting:
            from urllib.parse import urlparse
            p = urlparse(u)
            key = f"{method} {p.netloc}{p.path}"
            if key in seen_hosts: continue
            seen_hosts.add(key)
            print(f"  [{kind}] {key}")
        # 模擬搜尋一個地址後再看看
        print("\n--- 模擬填入地址 + 搜尋 ---")
        try:
            page.evaluate("""() => {
                const sel = document.getElementById('OtherQMemu');
                if (sel) for (const o of sel.options) {
                    if (o.text.trim() === '門牌號碼') { sel.value=o.value; sel.dispatchEvent(new Event('change',{bubbles:true})); break; }
                }
            }""")
            page.wait_for_timeout(1500)
            page.evaluate("""() => {
                const panel = document.querySelector('#tqM6');
                if (!panel) return;
                const selects = panel.querySelectorAll('select');
                for (const o of selects[0].options) if (o.text.trim()==='大安區') {
                    selects[0].value=o.value; selects[0].dispatchEvent(new Event('change',{bubbles:true})); break;
                }
            }""")
            page.wait_for_timeout(1500)
            interesting2 = []
            page.on("request", lambda r: interesting2.append((r.method, r.url)) if (
                r.resource_type in ('xhr','fetch') or 'rest/services' in r.url
            ) else None)
            page.evaluate("""() => {
                const panel = document.querySelector('#tqM6');
                const selects = panel.querySelectorAll('select');
                const allInputs = Array.from(panel.querySelectorAll('input[type="text"]'));
                const inputs = allInputs.filter(i => i.value !== selects[0].value);
                inputs[0].value='辛亥路三段'; inputs[0].dispatchEvent(new Event('input',{bubbles:true}));
                inputs[1].value='157'; inputs[1].dispatchEvent(new Event('input',{bubbles:true}));
                inputs[2].value='12'; inputs[2].dispatchEvent(new Event('input',{bubbles:true}));
                inputs[3].value='4'; inputs[3].dispatchEvent(new Event('input',{bubbles:true}));
                const btn = Array.from(panel.querySelectorAll('button')).find(b=>(b.innerText||'').trim()==='搜尋');
                if (btn) btn.click();
            }""")
            page.wait_for_timeout(4000)
            for method, u in interesting2[:30]:
                from urllib.parse import urlparse as _up
                p = _up(u)
                print(f"  [SEARCH] {method} {p.netloc}{p.path}?{p.query[:120]}")
        except Exception as e:
            print(f"  err: {e}")
    ctx.close()
    browser.close()
