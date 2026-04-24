"""
精確操作 地籍套繪圖查詢 zonemap.udd.gov.taipei 的「門牌號碼」查詢。
樣本：大安區 辛亥路三段 157 巷 12 弄 4 號
"""
import re
import logging
from pathlib import Path
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

OUT = Path("data/probes/tcd")
OUT.mkdir(parents=True, exist_ok=True)

# 樣本
DISTRICT = "大安區"
ROAD = "嘉興街"
LANE = ""
ALLEY = ""
NUM = "399"


def dump(page, name):
    try:
        page.screenshot(path=str(OUT / f"{name}.png"), full_page=False, timeout=8000)
    except Exception: pass
    try:
        (OUT / f"{name}.html").write_text(page.content(), encoding="utf-8")
    except Exception: pass
    logger.info(f"  saved {name}")


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=250)
        ctx = browser.new_context(
            viewport={"width": 1600, "height": 900},
            locale="zh-TW",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
        )
        page = ctx.new_page()

        logger.info("① 開地籍套繪圖查詢")
        page.goto("https://zonemap.udd.gov.taipei/ZoneMapOP/", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(4000)

        # ─ 切到「門牌號碼」模式 ─
        logger.info("② 把 OtherQMemu select 改為「門牌號碼」")
        page.evaluate("""() => {
            const sel = document.getElementById('OtherQMemu');
            if (!sel) return 'no select';
            for (const opt of sel.options) {
                if (opt.text.trim() === '門牌號碼') {
                    sel.value = opt.value;
                    sel.dispatchEvent(new Event('change', {bubbles: true}));
                    return `switched to value=${opt.value}`;
                }
            }
            return 'option not found';
        }""")
        page.wait_for_timeout(1500)
        dump(page, "10_menpai_mode")

        # ─ 找到門牌模式下的所有 select / input，分別記錄 ─
        panel_info = page.evaluate("""() => {
            // tqM6 應該是門牌模式的 panel
            const panel = document.querySelector('#tqM6') || document.querySelector('[id*="tqM6"]')?.parentElement;
            if (!panel) return { error: 'no tqM6 panel' };
            const inputs = Array.from(panel.querySelectorAll('input, select')).map((e, idx) => ({
                idx, tag: e.tagName, type: e.type || '', id: e.id, placeholder: e.placeholder || '', value: e.value || '',
                label: (e.previousElementSibling?.innerText || '').slice(0,20),
            }));
            return { inputs };
        }""")
        logger.info(f"  門牌 panel 內容：{panel_info}")

        # ─ 定位 form 元素，精確填資料 ─
        # 已知 panel 結構：
        #   selects[0] = 行政區 (大安區...), selects[1] = tqM6_land 道路(可能不必填)
        #   inputs[0] = 路段（如「辛亥路三段」）
        #   inputs[1] = 巷, inputs[2] = 弄, inputs[3] = 號, inputs[4] = 之號
        logger.info(f"③ 填入 {DISTRICT} {ROAD} {LANE}巷 {ALLEY}弄 {NUM}號")
        fill_result = page.evaluate(f"""() => {{
            const panel = document.querySelector('#tqM6');
            if (!panel) return {{error:'no #tqM6'}};
            const selects = panel.querySelectorAll('select');
            // 只取非唯讀 (placeholder 非空 或 label 非 "*readonly display") 的 input
            const allInputs = Array.from(panel.querySelectorAll('input[type="text"]'));
            // 排除第一個顯示行政區名稱的（唯讀 duplicate）
            const inputs = allInputs.filter(i => i.value !== selects[0]?.value);

            // 選行政區
            for (const opt of selects[0].options) {{
                if (opt.text.trim() === '{DISTRICT}') {{
                    selects[0].value = opt.value;
                    selects[0].dispatchEvent(new Event('change', {{bubbles:true}}));
                    break;
                }}
            }}

            // 等 tqM6_land 道路下拉載入後再 dump 看選項
            window._loadedRoads = () => {{
                const r = panel.querySelector('#tqM6_land');
                return r ? Array.from(r.options).map(o => o.text) : [];
            }};

            const vals = ['{ROAD}', '{LANE}', '{ALLEY}', '{NUM}'];
            const setInput = (inp, v) => {{
                inp.focus();
                inp.value = v;
                inp.dispatchEvent(new Event('input', {{bubbles:true}}));
                inp.dispatchEvent(new Event('change', {{bubbles:true}}));
            }};
            const filled = [];
            for (let i = 0; i < 4 && i < inputs.length; i++) {{
                setInput(inputs[i], vals[i]);
                filled.push({{i, value: vals[i], place: inputs[i].placeholder}});
            }}
            return {{ filled, numInputs: inputs.length }};
        }}""")
        logger.info(f"  填入結果：{fill_result}")
        page.wait_for_timeout(2000)
        # 看 tqM6_land 道路下拉現在有哪些選項
        roads = page.evaluate("() => window._loadedRoads ? window._loadedRoads() : []")
        logger.info(f"  tqM6_land 道路下拉 ({len(roads)} 個選項，前 10):")
        for r0 in roads[:10]:
            logger.info(f"    • {r0!r}")
        # 試找含「嘉興」的選項
        match = next((r0 for r0 in roads if "嘉興" in r0), None)
        if match:
            logger.info(f"  ↪ 找到道路選項 {match!r}，選之")
            page.evaluate(f"""() => {{
                const sel = document.getElementById('tqM6_land');
                for (const opt of sel.options) if (opt.text.trim() === '{match}') {{
                    sel.value = opt.value;
                    sel.dispatchEvent(new Event('change', {{bubbles:true}}));
                    break;
                }}
            }}""")
            page.wait_for_timeout(1500)
        dump(page, "11_filled")

        # ─ 找「搜尋」按鈕 ─
        logger.info("④ 點搜尋")
        page.evaluate("""() => {
            const panel = document.querySelector('#tqM6');
            if (!panel) return;
            const btn = Array.from(panel.querySelectorAll('button')).find(
                b => (b.innerText || '').trim() === '搜尋'
            );
            if (btn) btn.click();
        }""")
        page.wait_for_timeout(4000)
        dump(page, "12_after_search")

        # 選 qListCoor 的結果（觸發定位 + 資訊面板）
        logger.info("⑤ 選 qListCoor 第 1 筆結果（觸發跳轉/popup）")
        page.evaluate("""() => {
            const sel = document.getElementById('qListCoor');
            if (!sel || sel.options.length < 2) return;
            sel.selectedIndex = 1;
            sel.dispatchEvent(new Event('change', {bubbles:true}));
        }""")
        page.wait_for_timeout(3000)
        dump(page, "13_selected_result")

        # 全文找分區字樣
        text = page.evaluate("() => document.body.innerText")
        hits = re.findall(r".{0,30}(?:住[一二三四五六]|住宅區|商[一二三四五六]|商業區|工業|保護|農業).{0,30}", text)
        logger.info(f"  找到分區關鍵字片段 {len(hits)} 個：")
        for h in hits[:15]:
            logger.info(f"    • {h.strip()!r}")

        # 找專門的結果面板 DOM
        panels = page.evaluate("""() => {
            const out = [];
            for (const sel of ['[class*="result"]', '[class*="info"]', '[class*="popup"]', '[id*="result"]', '[id*="info"]', '[id*="zone"]']) {
                document.querySelectorAll(sel).forEach(el => {
                    const t = (el.innerText || '').trim();
                    if (t && t.length < 500 && /住|商|工|分區|地號/.test(t)) {
                        out.push({ sel, id: el.id, cls: el.className.slice(0,60), text: t.slice(0, 200) });
                    }
                });
            }
            return out;
        }""")
        logger.info(f"  疑似資訊面板 {len(panels)} 個：")
        for p in panels[:10]:
            logger.info(f"    • {p}")

        logger.info("結束，停 20 秒給你看畫面")
        page.wait_for_timeout(20000)
        ctx.close()
        browser.close()


if __name__ == "__main__":
    main()
