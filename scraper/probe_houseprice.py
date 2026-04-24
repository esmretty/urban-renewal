"""
偵察 5168 實價登錄（price.houseprice.tw）正式查詢流程：
  1. 選縣市下拉 = 台北市
  2. 選行政區下拉 = 大安區
  3. 關鍵字 = 辛亥路三段157（不打巷）
  4. 展開「更多」，建物坪數 min = max = 30.73
  5. 送出 → 結果應該唯一或數筆
  6. 點 .sub_tr_btn → 讀 .land-detail li

樣本：591_20039030 台北市大安區辛亥路三段157巷 30.73 坪 公寓
"""
import re
import logging
from pathlib import Path
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

SAMPLE_CITY = "台北市"
SAMPLE_DISTRICT = "大安區"
SAMPLE_ROAD_KW = "辛亥路三段157"
SAMPLE_AREA_PING = 30.73
SAMPLE_LAND_PING = 10.81

OUT = Path("data/probes/houseprice")
OUT.mkdir(parents=True, exist_ok=True)


def _pick_area_band(ping: float) -> str:
    if ping < 20: return "-20"
    if ping < 30: return "20-30"
    if ping < 40: return "30-40"
    if ping < 60: return "40-60"
    return "60-"


def dump(page, name: str):
    try:
        page.screenshot(path=str(OUT / f"{name}.png"), full_page=False, timeout=5000)
    except Exception as e:
        logger.debug(f"  screenshot {name} failed: {e}")
    try:
        (OUT / f"{name}.html").write_text(page.content(), encoding="utf-8")
    except Exception as e:
        logger.debug(f"  html {name} failed: {e}")
    logger.info(f"  saved {name}")


def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=250)
        ctx = browser.new_context(
            viewport={"width": 1400, "height": 900},
            locale="zh-TW",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = ctx.new_page()

        logger.info("① 開實價登錄首頁")
        page.goto("https://price.houseprice.tw/", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)
        dump(page, "10_home")

        # ─ 分析頁面中的下拉選單 ─
        logger.info("② 枚舉所有 select + 可點的「選縣市/選行政區」按鈕")
        selectors_info = page.evaluate("""() => {
            const out = {selects: [], buttons: []};
            document.querySelectorAll('select').forEach(s => {
                const opts = Array.from(s.options || []).slice(0, 8).map(o => o.text);
                out.selects.push({ name: s.name, id: s.id, options: opts });
            });
            // 5168 通常用自訂下拉（div button），label 可能是「選縣市」「選行政區」
            document.querySelectorAll('button, .el-select, [class*="dropdown"]').forEach(b => {
                const t = (b.innerText || '').trim().slice(0, 20);
                if (t && /縣市|行政區|地區|更多|類別|類型/.test(t)) {
                    out.buttons.push({
                        tag: b.tagName, cls: b.className.slice(0, 60), text: t,
                    });
                }
            });
            return out;
        }""")
        logger.info(f"  <select> tags: {selectors_info['selects']}")
        logger.info(f"  類下拉按鈕：")
        for b in selectors_info["buttons"][:20]:
            logger.info(f"    • {b}")

        # ─ 嘗試點「選縣市」→ 選「台北市」 ─
        logger.info("③ 嘗試開縣市下拉並選台北市")
        # 通用策略：找文字含「縣市」或「城市」的按鈕點一下
        for txt in ["選縣市", "縣市", "臺北市", "台北市"]:
            try:
                btn = page.get_by_text(txt, exact=False).first
                if btn and btn.is_visible():
                    btn.click()
                    page.wait_for_timeout(800)
                    logger.info(f"  點了含「{txt}」的按鈕")
                    break
            except Exception as e:
                logger.debug(f"  {txt} 失敗：{e}")
        dump(page, "11_city_dropdown_open")

        # 選單裡找「台北市」
        try:
            tpe = page.get_by_text("台北市", exact=True).first
            if tpe.is_visible():
                tpe.click()
                page.wait_for_timeout(800)
                logger.info("  已選 台北市")
        except Exception as e:
            try:
                tpe = page.get_by_text("臺北市", exact=True).first
                tpe.click()
                page.wait_for_timeout(800)
                logger.info("  已選 臺北市")
            except Exception as e2:
                logger.warning(f"  選不到台北市：{e2}")

        dump(page, "12_city_selected")

        # ─ 選行政區 = 大安區 ─
        logger.info("④ 開行政區下拉並選大安區")
        for txt in ["選行政區", "行政區", "選區域"]:
            try:
                btn = page.get_by_text(txt, exact=False).first
                if btn.is_visible():
                    btn.click()
                    page.wait_for_timeout(800)
                    logger.info(f"  點了含「{txt}」的按鈕")
                    break
            except Exception:
                pass
        try:
            da = page.get_by_text(SAMPLE_DISTRICT, exact=True).first
            if da.is_visible():
                da.click()
                page.wait_for_timeout(800)
                logger.info(f"  已選 {SAMPLE_DISTRICT}")
        except Exception as e:
            logger.warning(f"  選不到大安區：{e}")
        dump(page, "13_district_selected")

        # ─ 關鍵字 ─
        logger.info(f"⑤ 輸入關鍵字「{SAMPLE_ROAD_KW}」")
        kw_input = page.query_selector('input[placeholder*="關鍵字"]')
        if kw_input:
            kw_input.click()
            kw_input.fill(SAMPLE_ROAD_KW)
            page.wait_for_timeout(500)
            logger.info("  已輸入關鍵字")
        dump(page, "14_kw_typed")

        # ─ 點「更多」看坪數篩選 ─（文字是「更多(0)」含括號）
        logger.info("⑥ 點「更多」展開進階篩選")
        clicked_more = page.evaluate("""() => {
            const els = document.querySelectorAll('span, button, a, div');
            for (const el of els) {
                const t = (el.innerText || '').trim();
                if (/^更多(\\(\\d+\\))?$/.test(t) && el.children.length < 5) {
                    el.click();
                    return t;
                }
            }
            return null;
        }""")
        logger.info(f"  點到的「更多」按鈕文字：{clicked_more!r}")
        page.wait_for_timeout(1500)
        dump(page, "15_more_opened")

        # 枚舉進階面板裡的 input
        more_inputs = page.evaluate("""() => {
            const inputs = document.querySelectorAll('input[type="number"], input[type="text"], input[placeholder]');
            return Array.from(inputs).slice(0, 30).map(i => ({
                name: i.name, id: i.id, placeholder: i.placeholder || '',
                value: i.value, label: (i.closest('label')?.innerText || '').slice(0, 30),
            }));
        }""")
        logger.info(f"  頁面上的 input {len(more_inputs)} 個：")
        for i in more_inputs:
            logger.info(f"    • {i}")

        # ─ 確保「建物」radio 被選（預設就是建物，但以防萬一）─
        logger.info("⑦a 確保 建物/土地 toggle = 建物")
        page.evaluate("""() => {
            const radios = document.querySelectorAll('input[type="radio"]');
            for (const r of radios) {
                const lab = (r.closest('label')?.innerText || '').trim();
                if (lab === '建物' && !r.checked) r.click();
            }
        }""")
        page.wait_for_timeout(300)

        # ─ 建物坪數 min/max：第 3 組 min/max（index=2），精確值 ─
        logger.info(f"⑦b 填建物坪數 min = max = {SAMPLE_AREA_PING}（pair index 2）")
        area_filled = page.evaluate(f"""() => {{
            const mins = document.querySelectorAll('input[placeholder="最低"]');
            const maxs = document.querySelectorAll('input[placeholder="最高"]');
            // pair 0=總價, 1=單價, 2=坪數, 3=屋齡, 4=樓層
            if (mins.length < 3 || maxs.length < 3) return false;
            const set = (inp, val) => {{
                inp.focus();
                inp.value = String(val);
                inp.dispatchEvent(new Event('input', {{ bubbles: true }}));
                inp.dispatchEvent(new Event('change', {{ bubbles: true }}));
            }};
            set(mins[2], {SAMPLE_AREA_PING});
            set(maxs[2], {SAMPLE_AREA_PING});
            return true;
        }}""")
        logger.info(f"  坪數填入結果：{area_filled}")
        page.wait_for_timeout(500)
        dump(page, "16_filters_filled")

        # ─ 送出查詢 ─
        logger.info("⑧ 送出查詢")
        for txt in ["查詢", "搜尋", "送出"]:
            try:
                btn = page.get_by_text(txt, exact=True).first
                if btn.is_visible():
                    btn.click()
                    page.wait_for_timeout(4000)
                    logger.info(f"  點了 {txt}")
                    break
            except Exception:
                pass
        dump(page, "17_results")
        logger.info(f"  搜尋後 url = {page.url}")

        # ─ 看結果列 ─
        rows_info = page.evaluate("""() => {
            const rows = document.querySelectorAll('tr.group, tr[class*="group"]');
            return Array.from(rows).map((tr, idx) => {
                const cells = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim());
                return { idx, sample: cells.slice(0, 3).join(' | ').slice(0, 120) };
            });
        }""")
        logger.info(f"  結果列數：{len(rows_info)}")
        for r in rows_info[:10]:
            logger.info(f"    [{r['idx']}] {r['sample']!r}")

        # ─ 依序點擊每一列，收集 modal 內的 zoning + 完整地址 ─
        results = []
        for i in range(min(len(rows_info), 10)):
            logger.info(f"⑨ 點第 {i} 列的 .sub_tr_btn")
            try:
                page.evaluate(f"""(idx) => {{
                    const rows = document.querySelectorAll('tr.group, tr[class*="group"]');
                    const btn = rows[idx]?.querySelector('.sub_tr_btn') || rows[idx]?.querySelector('td.cursor-pointer');
                    if (btn) {{ btn.scrollIntoView(); btn.click(); }}
                }}""", i)
                page.wait_for_timeout(2500)
                detail = page.evaluate("""() => {
                    // modal 標題通常含完整地址，例如「大安區辛亥路三段157巷12弄4號3樓 成交明細」
                    const title = document.querySelector('.modal .modal-title, [class*="modal"] h1, [class*="modal"] h3, [class*="modal"] [class*="title"]');
                    const titleText = title ? title.innerText.trim() : '';
                    const zones = Array.from(document.querySelectorAll('.land-detail li'))
                        .map(e => e.innerText.trim());
                    return { titleText, zones };
                }""")
                logger.info(f"  row[{i}] title={detail['titleText']!r} zoning={detail['zones']}")
                results.append({"idx": i, **detail})
                # 關 modal：點 .modal-close
                page.evaluate("""() => {
                    const close = document.querySelector('.modal-close');
                    if (close) close.click();
                }""")
                page.wait_for_timeout(600)
            except Exception as e:
                logger.error(f"  row[{i}] 失敗：{e}")

        logger.info("⑩ 彙整：")
        all_zones = [z for r in results for z in r["zones"]]
        all_titles = [r["titleText"] for r in results if r["titleText"]]
        for z in sorted(set(all_zones)):
            logger.info(f"  zoning {z!r}  × {all_zones.count(z)}")
        for t in sorted(set(all_titles)):
            logger.info(f"  address {t!r}  × {all_titles.count(t)}")

        # 挑最完整的 zoning（含「第.種住宅區」 > 「住.」 > 「住」）
        def zone_completeness(s):
            if "第" in s and "種" in s: return 3
            if any(c in s for c in "一二三四五六七") and "住" in s: return 2
            return 1
        best = max(all_zones, key=zone_completeness) if all_zones else None
        logger.info(f"⑪ 最終選用分區：{best!r}")

        logger.info("結束，停 10 秒給你看畫面")
        page.wait_for_timeout(10000)
        ctx.close()
        browser.close()


if __name__ == "__main__":
    main()
