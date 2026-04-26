"""探查永慶列表頁的排序選項：找出「最新刊登」對應的 ?od= 值。"""
import json
import re
from playwright.sync_api import sync_playwright

URL = "https://buy.yungching.com.tw/region/%E4%BD%8F%E5%AE%85_p/%E5%8F%B0%E5%8C%97%E5%B8%82-%E5%A4%A7%E5%AE%89%E5%8D%80_c/%E5%85%AC%E5%AF%93_type"

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            locale="zh-TW",
            viewport={"width": 1280, "height": 900},
        )
        page = ctx.new_page()
        page.goto(URL, wait_until="domcontentloaded", timeout=30_000)
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        page.wait_for_timeout(2000)

        # === 找頁面上所有排序相關的元素 ===
        sort_data = page.evaluate("""() => {
            const out = {selects: [], dropdowns: [], links: [], buttons: []};
            // 1. <select> 下拉
            document.querySelectorAll('select').forEach(s => {
                const opts = Array.from(s.options).map(o => ({value: o.value, text: o.textContent.trim(), selected: o.selected}));
                out.selects.push({name: s.name, id: s.id, cls: s.className, options: opts});
            });
            // 2. 自訂 dropdown：找含「排序 / 最新 / 刊登 / 屋齡」的元素
            document.querySelectorAll('*').forEach(el => {
                const text = (el.textContent || '').trim();
                if (text.length > 40 || text.length < 2) return;
                if (/(排序|最新|刊登|屋齡|價格|坪數|依時間|新上)/.test(text)) {
                    const tag = el.tagName.toLowerCase();
                    if (['li', 'span', 'div', 'a', 'button', 'option'].includes(tag)) {
                        const data_attrs = {};
                        for (const attr of el.attributes) {
                            if (attr.name.startsWith('data-') || ['value', 'href'].includes(attr.name)) {
                                data_attrs[attr.name] = attr.value;
                            }
                        }
                        if (Object.keys(data_attrs).length > 0) {
                            out.dropdowns.push({tag, text, attrs: data_attrs, cls: el.className.slice(0,80)});
                        }
                    }
                }
            });
            // 3. 找 a[href*="od="]
            document.querySelectorAll('a[href*="od="]').forEach(a => {
                out.links.push({text: a.textContent.trim().slice(0, 30), href: a.href});
            });
            return out;
        }""")

        print("=== <select> elements ===")
        for s in sort_data["selects"]:
            print(json.dumps(s, ensure_ascii=False, indent=2))

        print("\n=== Dropdown-like elements (帶排序字眼) ===")
        for d in sort_data["dropdowns"][:30]:
            print(json.dumps(d, ensure_ascii=False))

        print("\n=== a[href*='od='] links ===")
        for l in sort_data["links"][:20]:
            print(json.dumps(l, ensure_ascii=False))

        # === 點開排序下拉看選項 ===
        print("\n=== 嘗試點開排序選單 ===")
        try:
            # 永慶常見：找含「排序」或「降序」字樣的按鈕
            triggered = False
            for sel in ['button:has-text("排序")', 'div:has-text("排序"):not(:has(div))', '[class*="sort"]', '[class*="order"]']:
                try:
                    btn = page.locator(sel).first
                    if btn.count() > 0:
                        btn.click(timeout=3000)
                        page.wait_for_timeout(1500)
                        triggered = True
                        print(f"   點到了：{sel}")
                        break
                except Exception:
                    continue
            if triggered:
                opened = page.evaluate("""() => {
                    const items = [];
                    document.querySelectorAll('li, div, button, a').forEach(el => {
                        const t = (el.textContent || '').trim();
                        if (t.length < 2 || t.length > 20) return;
                        if (/(最新|刊登|時間|新到舊|降價|屋齡|低到高|高到低|大到小|小到大|預設|綜合)/.test(t)) {
                            const attrs = {};
                            for (const a of el.attributes) {
                                if (a.name.startsWith('data-') || a.name === 'href' || a.name === 'value') attrs[a.name] = a.value;
                            }
                            items.push({tag: el.tagName.toLowerCase(), text: t, attrs, parent_attrs: el.parentElement ? Object.fromEntries(Array.from(el.parentElement.attributes).filter(a => a.name.startsWith('data-')).map(a => [a.name, a.value])) : {}});
                        }
                    });
                    // 去重
                    const seen = new Set();
                    return items.filter(i => { const k = i.text + JSON.stringify(i.attrs); if (seen.has(k)) return false; seen.add(k); return true; });
                }""")
                print(f"   選單內找到 {len(opened)} 個排序選項：")
                for o in opened[:30]:
                    print(f"     {json.dumps(o, ensure_ascii=False)}")
            else:
                print("   找不到排序按鈕")
        except Exception as e:
            print(f"   點選單失敗: {e}")

        # === 比對不同 od 值的第一筆物件 + 看物件詳情頁有沒有刊登時間 ===
        print("\n=== 比對 od=1~80 的第 1, 2 筆物件 ID ===")
        first_ids = {}
        for od in [1, 2, 3, 4, 5, 10, 11, 12, 20, 30, 40, 50, 60, 70, 80]:
            try:
                page.goto(f"{URL}?od={od}", wait_until="domcontentloaded", timeout=15_000)
                try:
                    page.wait_for_load_state("networkidle", timeout=8_000)
                except Exception:
                    pass
                ids = page.evaluate("""() => {
                    const arr = [];
                    document.querySelectorAll('a[href*="/house/"]').forEach(a => {
                        const m = a.href.match(/\\/house\\/(\\d{6,8})/);
                        if (m && !arr.includes(m[1])) arr.push(m[1]);
                    });
                    return arr.slice(0, 3);
                }""")
                first_ids[od] = ids
                print(f"   od={od}: {ids}")
            except Exception as e:
                print(f"   od={od}: ERROR {e}")

        browser.close()

        print("\n=== 結論 ===")
        print("尋找前 3 筆物件不包含 4308114（從前面測試知 4308114 是「精選/置頂」）的 od 值，")
        print("其前 1 筆很可能是真實的「按該 od 排序」第一名")

if __name__ == "__main__":
    main()
