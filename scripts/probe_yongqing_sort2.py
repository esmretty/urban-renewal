"""探查永慶：找排序按鈕 + 物件詳情頁是否有刊登時間。"""
import json
import re
import sys
from playwright.sync_api import sync_playwright

LIST_URL = "https://buy.yungching.com.tw/region/%E4%BD%8F%E5%AE%85_p/%E5%8F%B0%E5%8C%97%E5%B8%82-%E5%A4%A7%E5%AE%89%E5%8D%80_c/%E5%85%AC%E5%AF%93_type"

def main():
    sys.stdout.reconfigure(encoding='utf-8')
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            locale="zh-TW",
            viewport={"width": 1280, "height": 1200},
        )
        page = ctx.new_page()
        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=30_000)
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        page.wait_for_timeout(3000)

        # === 1. 截圖列表頁，存檔讓 user 看 ===
        page.screenshot(path="d:/Coding/urban-renewal/scripts/yongqing_list.png", full_page=False)
        print("[1] 列表頁截圖存到 scripts/yongqing_list.png")

        # === 2. 找列表頁所有可疑「排序」相關文字 ===
        sort_clues = page.evaluate("""() => {
            const results = [];
            // 全頁掃描，找含「排序」「最新」「依」字樣的小元素
            document.querySelectorAll('button, a, div, span, li').forEach(el => {
                const text = (el.textContent || '').trim();
                if (text.length < 2 || text.length > 25) return;
                if (!/(排序|最新刊登|依.*排序|依時間|預設排序|依價格|依屋齡|綜合排序)/.test(text)) return;
                // 不要太多子元素的 (避免 container)
                if (el.children.length > 3) return;
                const rect = el.getBoundingClientRect();
                if (rect.width === 0 || rect.height === 0) return;
                results.push({
                    tag: el.tagName.toLowerCase(),
                    text,
                    visible: rect.top > 0 && rect.top < 1200,
                    cls: el.className ? String(el.className).slice(0, 100) : '',
                    rect: {x: Math.round(rect.left), y: Math.round(rect.top), w: Math.round(rect.width)}
                });
            });
            return results;
        }""")
        print(f"\n[2] 找到 {len(sort_clues)} 個可疑排序元素：")
        for s in sort_clues[:15]:
            print(f"   {json.dumps(s, ensure_ascii=False)}")

        # === 3. 嘗試 hover/click 排序按鈕 ===
        print("\n[3] 嘗試點擊排序按鈕")
        for clue in sort_clues:
            if not clue.get("visible"):
                continue
            text = clue["text"]
            tag = clue["tag"]
            if "排序" not in text and "依" not in text:
                continue
            try:
                # 用文字選 element
                btn = page.get_by_text(text, exact=True).first
                if btn.count() > 0:
                    print(f"   點擊「{text}」({tag})")
                    btn.click(timeout=3000)
                    page.wait_for_timeout(1500)
                    # 抓彈出的選項
                    options = page.evaluate("""() => {
                        const items = [];
                        document.querySelectorAll('li, a, button, div').forEach(el => {
                            const t = (el.textContent || '').trim();
                            if (t.length < 3 || t.length > 15) return;
                            if (/(最新刊登|刊登時間|降價|屋齡|低到高|高到低|大到小|小到大|預設|綜合|新到舊|舊到新)/.test(t)) {
                                if (el.children.length > 0) return;  // 只要葉節點
                                const rect = el.getBoundingClientRect();
                                if (rect.width === 0) return;
                                const attrs = {};
                                for (const a of el.attributes) {
                                    if (a.name.startsWith('data-') || ['value','href','onclick'].includes(a.name)) attrs[a.name] = a.value;
                                }
                                items.push({tag: el.tagName.toLowerCase(), text: t, attrs});
                            }
                        });
                        const seen = new Set();
                        return items.filter(i => { const k = i.text; if (seen.has(k)) return false; seen.add(k); return true; });
                    }""")
                    print(f"   彈出 {len(options)} 個排序選項：")
                    for o in options:
                        print(f"     {json.dumps(o, ensure_ascii=False)}")
                    # 截圖看打開的選單
                    page.screenshot(path="d:/Coding/urban-renewal/scripts/yongqing_sort_open.png", full_page=False)
                    print("   選單截圖存到 scripts/yongqing_sort_open.png")
                    break
            except Exception as e:
                print(f"   click 失敗 {text}: {e}")

        # === 4. 點「最新刊登」看 URL 變化 ===
        print("\n[4] 嘗試點「最新刊登」看 URL 變化")
        url_before = page.url
        try:
            target = page.get_by_text("最新刊登", exact=True).first
            if target.count() > 0:
                target.click(timeout=3000)
                page.wait_for_timeout(2000)
                url_after = page.url
                print(f"   URL before: {url_before}")
                print(f"   URL after:  {url_after}")
            else:
                print("   找不到「最新刊登」按鈕")
        except Exception as e:
            print(f"   點擊失敗: {e}")

        # === 5. 抓物件詳情頁，看有沒有「上架日 / 刊登時間」 ===
        print("\n[5] 抓物件 4308114 詳情頁，找時間欄位")
        page.goto("https://buy.yungching.com.tw/house/4308114", wait_until="domcontentloaded", timeout=30_000)
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        page.wait_for_timeout(2000)
        time_clues = page.evaluate("""() => {
            const results = [];
            document.querySelectorAll('*').forEach(el => {
                if (el.children.length > 0) return;
                const t = (el.textContent || '').trim();
                if (t.length < 3 || t.length > 50) return;
                if (/(刊登|上架|更新|前刊|今日|昨日|\\d+天前|\\d{4}[/.\\-]\\d{1,2}[/.\\-]\\d{1,2}|\\d{1,2}\\/\\d{1,2})/.test(t)) {
                    results.push({tag: el.tagName.toLowerCase(), text: t, cls: String(el.className || '').slice(0, 60)});
                }
            });
            return results.slice(0, 30);
        }""")
        print(f"   找到 {len(time_clues)} 個含時間字樣元素：")
        for t in time_clues[:20]:
            print(f"     {json.dumps(t, ensure_ascii=False)}")

        browser.close()

if __name__ == "__main__":
    main()
