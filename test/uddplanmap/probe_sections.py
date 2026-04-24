"""Probe UDDPlanMap sidebar by dumping structured HTML."""
from playwright.sync_api import sync_playwright
import re

URL = "https://bim.udd.gov.taipei/UDDPlanMap/"


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1600, "height": 1000})
        page = ctx.new_page()
        page.goto(URL, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(3000)

        # 展開 sidebar
        try:
            page.click(".sidebar-toggle", timeout=3000)
            page.wait_for_timeout(600)
        except Exception:
            pass

        # 展開每一個分類
        page.evaluate("""
        () => {
            document.querySelectorAll('.panel-heading, .accordion-toggle, [data-toggle="collapse"], .accordion-header').forEach(e => {
                try { e.click(); } catch (_) {}
            });
        }
        """)
        page.wait_for_timeout(800)

        # 找 sidebar 區塊，dump innerText
        sidebar_text = page.evaluate("""
        () => {
            const sb = document.querySelector('#sidebar, .sidebar, #leftPanel, .left-panel, aside');
            if (sb) return sb.innerText;
            // fallback: 找含 Layer-Item 的共同祖先
            const first = document.querySelector('input.Layer-Item');
            if (!first) return '';
            let p = first;
            for (let i = 0; i < 10; i++) {
                if (p.parentElement && p.parentElement.querySelectorAll('input.Layer-Item').length > 20) p = p.parentElement;
                else break;
            }
            return p.innerText;
        }
        """)
        print("=== SIDEBAR TEXT ===")
        print(sidebar_text[:3000])
        print()

        # 對每個 Layer-Item，找最近的有文字的 <li> / row 的兄弟節點
        result = page.evaluate("""
        () => {
            const out = [];
            document.querySelectorAll('input.Layer-Item[layerid]').forEach(el => {
                // 找祖先裡第一個 <li>
                let li = el.closest('li');
                let label = "";
                if (li) {
                    const clone = li.cloneNode(true);
                    clone.querySelectorAll('.toggle, .toggle-group, script, style, input').forEach(x => x.remove());
                    label = (clone.innerText || "").replace(/\\s+/g, " ").trim();
                }
                // 找所在的 panel / collapse 區塊的 header
                let section = "";
                let p = el.parentElement;
                while (p) {
                    if (p.classList.contains('panel') || p.classList.contains('accordion-item') || (p.id && p.id.startsWith('collapse'))) {
                        // 找前一個姊妹節點的 header
                        const head = p.querySelector('.panel-heading, .accordion-header, .panel-title') ||
                                     (p.previousElementSibling && p.previousElementSibling.querySelector('.panel-heading, .accordion-header'));
                        if (head) { section = head.innerText.trim(); break; }
                    }
                    p = p.parentElement;
                }
                out.push({layerid: el.getAttribute('layerid'), section, label, opacity: el.getAttribute('opacity')});
            });
            return out;
        }
        """)
        import json
        with open("sections_dump.txt", "w", encoding="utf-8") as f:
            for r in result:
                f.write(f"{r['layerid']:25s} | op={r['opacity']} | {r['label']}\n")
            f.write("\n=== sidebar text ===\n")
            f.write(sidebar_text)
        print("wrote sections_dump.txt")

        browser.close()


if __name__ == "__main__":
    main()
