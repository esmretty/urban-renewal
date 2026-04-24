"""找出 LVR CSV 的實際下載 URL（模擬點擊本期下載按鈕）。"""
from playwright.sync_api import sync_playwright

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=False, slow_mo=300)
    ctx = browser.new_context(accept_downloads=True)
    page = ctx.new_page()
    captured = []
    page.on("request", lambda r: (
        captured.append((r.method, r.url))
        if ("Download" in r.url or ".zip" in r.url) else None
    ))
    page.on("download", lambda d: captured.append(("DOWNLOAD", d.url)))
    page.goto("https://plvr.land.moi.gov.tw/DownloadOpenData", wait_until="networkidle", timeout=30000)
    page.wait_for_timeout(3000)
    # 點「本期下載」tab
    page.evaluate("""() => {
        const a = Array.from(document.querySelectorAll('a')).find(x => (x.innerText||'').trim() === '本期下載');
        if (a) a.click();
    }""")
    page.wait_for_timeout(3000)
    # 列出目前可見的 checkbox 和 table
    info = page.evaluate("""() => {
        const cbs = Array.from(document.querySelectorAll('input[type="checkbox"]')).slice(0, 40).map((cb, i) => ({
            i, id: cb.id, name: cb.name, value: cb.value,
            label: cb.closest('tr')?.innerText?.slice(0,60) || cb.parentElement?.innerText?.slice(0,60) || ''
        }));
        return cbs;
    }""")
    print(f"checkboxes: {len(info)}")
    for c in info[:30]:
        print(f"  {c}")
    # 嘗試勾台北 + 新北 不動產買賣 CSV，點下載
    page.evaluate("""() => {
        // 找含台北市的 checkbox
        const cbs = document.querySelectorAll('input[type="checkbox"]');
        for (const cb of cbs) {
            const t = cb.closest('tr')?.innerText || '';
            if (/台北市|臺北市|新北市/.test(t) && /A\\.csv|買賣|成屋/.test(t) && !cb.checked) {
                cb.click();
            }
        }
    }""")
    page.wait_for_timeout(1000)
    # 點下載
    page.evaluate("""() => {
        const btns = Array.from(document.querySelectorAll('a, button')).find(b => (b.innerText||'').trim() === '下載');
        if (btns) btns.click();
    }""")
    page.wait_for_timeout(5000)
    print("\n抓到的 network 請求:")
    for m, u in captured[:40]:
        print(f"  [{m}] {u[:220]}")
    ctx.close()
    browser.close()
