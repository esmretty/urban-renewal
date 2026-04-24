"""
偵察 TCD 地籍套繪圖的「路寬」圖層 switch。
打開後地圖上會標出每條街的路寬數字。
"""
import re
import logging
from pathlib import Path
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

OUT = Path("data/probes/tcd_road")
OUT.mkdir(parents=True, exist_ok=True)


def dump(page, name):
    try:
        page.screenshot(path=str(OUT / f"{name}.png"), full_page=False, timeout=8000)
    except Exception as e:
        logger.debug(f"screenshot {name} failed: {e}")
    try:
        (OUT / f"{name}.html").write_text(page.content(), encoding="utf-8")
    except Exception:
        pass
    logger.info(f"  saved {name}")


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=300)
        ctx = browser.new_context(
            viewport={"width": 1600, "height": 900}, locale="zh-TW",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
        )
        page = ctx.new_page()

        logger.info("① 開地籍套繪圖")
        page.goto("https://zonemap.udd.gov.taipei/ZoneMapOP/", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(4000)
        dump(page, "01_home")

        # ─ 列出左上角所有可能的 switch / 圖層控制 ─
        logger.info("② 枚舉所有 checkbox / switch / 圖層 toggle 元素")
        toggles = page.evaluate("""() => {
            const cbs = document.querySelectorAll('input[type="checkbox"]');
            return Array.from(cbs).map((cb, i) => {
                // 找最近的 label / 旁邊文字
                let labelText = '';
                if (cb.id) {
                    const lab = document.querySelector(`label[for="${cb.id}"]`);
                    if (lab) labelText = lab.innerText;
                }
                if (!labelText) {
                    const parent = cb.parentElement;
                    if (parent) labelText = parent.innerText.slice(0, 40);
                }
                const rect = cb.getBoundingClientRect();
                return {
                    idx: i,
                    id: cb.id || '',
                    name: cb.name || '',
                    checked: cb.checked,
                    label: labelText.trim().slice(0, 30),
                    visible: rect.width > 0 && rect.height > 0,
                    x: Math.round(rect.x), y: Math.round(rect.y),
                };
            }).filter(c => c.visible);
        }""")
        logger.info(f"  共 {len(toggles)} 個可見 checkbox：")
        for t in toggles:
            logger.info(f"    [{t['idx']}] {t}")

        # 找含「路寬」「道路」「巷道」的 toggle
        logger.info("③ 找「路寬」相關 toggle")
        candidates = [t for t in toggles if any(k in (t['label'] or '') for k in ('路寬','道路','巷道','寬'))]
        logger.info(f"  匹配 {len(candidates)} 個：{candidates}")

        if candidates:
            target = candidates[0]
            logger.info(f"④ 點擊 idx={target['idx']} ({target['label']!r})")
            page.evaluate(f"""(idx) => {{
                const cb = document.querySelectorAll('input[type=\"checkbox\"]')[idx];
                if (cb && !cb.checked) cb.click();
            }}""", target['idx'])
            page.wait_for_timeout(2500)
            dump(page, "02_after_toggle")

        logger.info("結束，停 25 秒給你看")
        page.wait_for_timeout(25000)
        ctx.close()
        browser.close()


if __name__ == "__main__":
    main()
