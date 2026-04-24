"""
[DEPRECATED] 已被 analysis/gov_gis.py（GeoServer WFS 直查）取代。
保留檔案參考用（Playwright + Vision OCR pipeline 的實作範例）。
不再被 scraper/zoning_lookup.py 引用。

舊版流程：
  1. 切到「門牌號碼」模式
  2. 填入行政區 + 路段 + 巷 + 弄 + 號
  3. 點搜尋
  4. 選 #qListCoor 第 1 筆
  5. 截圖地圖 → Claude Sonnet Vision 讀綠點位置的分區標籤

分區標籤格式（地圖上）：
  住1, 住2, 住3, 住4, 商1, 商2, 商3, 商4, 工1, 工2, 農業, 保護, 八 (公園) 等
"""
import re
import base64
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime

import anthropic
from playwright.sync_api import BrowserContext

from config import ANTHROPIC_API_KEY, SCREENSHOTS_DIR

logger = logging.getLogger(__name__)

TCD_URL = "https://zonemap.udd.gov.taipei/ZoneMapOP/"
MODEL_VISION = "claude-sonnet-4-6"

_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


# ── 地圖 標籤 ↔ 正式分區名 ────────────────────────────────────────────────────

ZONE_LABEL_TO_NAME = {
    # 住宅區
    "住1": "第一種住宅區", "住2": "第二種住宅區", "住3": "第三種住宅區",
    "住4": "第四種住宅區", "住2-1": "第二之一種住宅區", "住2-2": "第二之二種住宅區",
    "住3-1": "第三之一種住宅區", "住3-2": "第三之二種住宅區",
    "住4-1": "第四之一種住宅區",
    # 商業區
    "商1": "第一種商業區", "商2": "第二種商業區",
    "商3": "第三種商業區", "商4": "第四種商業區",
    # 工業區
    "工1": "第一種工業區", "工2": "第二種工業區", "工3": "第三種工業區",
    # 特殊區
    "農業": "農業區", "保護": "保護區",
    "行政": "行政區", "文教": "文教區",
    "倉儲": "倉儲區", "風景": "風景區",
    "公": "公園用地",
}


def normalize_zone_label(label: str) -> Optional[str]:
    """地圖標籤 → 正式分區名。找不到就回原值。"""
    if not label:
        return None
    label = label.strip()
    if label in ZONE_LABEL_TO_NAME:
        return ZONE_LABEL_TO_NAME[label]
    # 「八」「一」「二」 通常是公園編號 (e.g. 八=兒八公園)
    if re.match(r"^[一二三四五六七八九十百]+$", label):
        return f"公園用地({label})"
    return label


# ── 主要 API ─────────────────────────────────────────────────────────────────

def lookup_tcd(
    ctx: BrowserContext,
    *,
    district: str,
    road: str,
    lane: str = "",
    alley: str = "",
    num: str = "",
) -> dict:
    """
    在台北市地籍套繪圖查分區。
    輸入：district=行政區（例「大安區」），road=路段名（例「辛亥路三段」），lane/alley/num
    回傳：
        {
            "zone_label": "住3",          # 地圖上原始標籤
            "zone_name": "第三種住宅區",  # 正式名稱
            "screenshot_path": "...",
            "source_url": "...",          # 可給使用者點開驗證的 URL
            "error": None | str,
        }
    """
    out = {
        "zone_label": None,
        "zone_name": None,
        "screenshot_path": None,
        "source_url": TCD_URL,
        "error": None,
    }
    page = ctx.new_page()
    try:
        page.goto(TCD_URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(4000)

        # 切「門牌號碼」模式
        page.evaluate("""() => {
            const sel = document.getElementById('OtherQMemu');
            if (!sel) return;
            for (const opt of sel.options) {
                if (opt.text.trim() === '門牌號碼') {
                    sel.value = opt.value;
                    sel.dispatchEvent(new Event('change', {bubbles:true}));
                    break;
                }
            }
        }""")
        page.wait_for_timeout(1500)

        # 填表
        filled = page.evaluate(f"""() => {{
            const panel = document.querySelector('#tqM6');
            if (!panel) return false;
            const selects = panel.querySelectorAll('select');
            const allInputs = Array.from(panel.querySelectorAll('input[type="text"]'));
            const inputs = allInputs.filter(i => i.value !== selects[0]?.value);

            // 行政區
            for (const opt of selects[0].options) {{
                if (opt.text.trim() === '{district}') {{
                    selects[0].value = opt.value;
                    selects[0].dispatchEvent(new Event('change', {{bubbles:true}}));
                    break;
                }}
            }}

            const vals = ['{road}', '{lane}', '{alley}', '{num}'];
            const setInput = (inp, v) => {{
                inp.focus();
                inp.value = v;
                inp.dispatchEvent(new Event('input', {{bubbles:true}}));
                inp.dispatchEvent(new Event('change', {{bubbles:true}}));
            }};
            for (let i = 0; i < 4 && i < inputs.length; i++) setInput(inputs[i], vals[i]);
            return true;
        }}""")
        if not filled:
            out["error"] = "tcd_form_not_found"
            return out

        # 搜尋
        page.evaluate("""() => {
            const panel = document.querySelector('#tqM6');
            const btn = Array.from(panel.querySelectorAll('button')).find(b => (b.innerText || '').trim() === '搜尋');
            if (btn) btn.click();
        }""")
        page.wait_for_timeout(3500)

        # 檢查結果
        qlist = page.evaluate("""() => {
            const sel = document.getElementById('qListCoor');
            if (!sel) return [];
            return Array.from(sel.options).map(o => o.text);
        }""")
        if not qlist or len(qlist) < 2 or qlist[0] == "共0筆":
            out["error"] = "tcd_no_result"
            return out

        # 選第 1 筆結果，觸發地圖定位
        page.evaluate("""() => {
            const sel = document.getElementById('qListCoor');
            sel.selectedIndex = 1;
            sel.dispatchEvent(new Event('change', {bubbles:true}));
        }""")
        page.wait_for_timeout(3500)

        # 截圖
        safe_id = re.sub(r"[^\w\-]", "_", f"{district}_{road}_{lane}_{alley}_{num}")
        Path(SCREENSHOTS_DIR, "tcd").mkdir(parents=True, exist_ok=True)
        shot_path = Path(SCREENSHOTS_DIR, "tcd", f"{safe_id}.png")
        page.screenshot(path=str(shot_path), full_page=False)
        out["screenshot_path"] = str(shot_path)

        # Vision OCR
        label = _vision_read_zone_label(str(shot_path))
        out["zone_label"] = label
        out["zone_name"] = normalize_zone_label(label) if label else None
        return out

    except Exception as e:
        logger.error(f"lookup_tcd error: {e}", exc_info=True)
        out["error"] = f"tcd_exception: {e}"
        return out
    finally:
        page.close()


VISION_PROMPT = """這是台北市地籍套繪圖查詢系統的截圖。
地圖中央有一個綠色圓點標示查詢到的門牌位置。

請告訴我：**綠色圓點所在的那塊地籍**的「土地使用分區」標籤是什麼？

可能值範例：「住1」「住2」「住3」「住4」「住2-1」「商1」「商2」「商3」「商4」「工1」「工2」「農業」「保護」「公」等。

嚴格規則：
- 只看綠色圓點**正下方或最接近**的那塊地籍上的文字，不要看旁邊其他地籍的
- 看不清楚就填 null，絕不猜測

只回傳 JSON，不要任何其他文字：
{"zone_label": "<文字>"}
"""


def _vision_read_zone_label(screenshot_path: str) -> Optional[str]:
    try:
        with open(screenshot_path, "rb") as f:
            img_b64 = base64.standard_b64encode(f.read()).decode("utf-8")
        resp = _client.messages.create(
            model=MODEL_VISION,
            max_tokens=200,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": img_b64}},
                    {"type": "text", "text": VISION_PROMPT},
                ],
            }],
        )
        text = resp.content[0].text.strip()
        import json
        m = re.search(r"\{.*?\}", text, re.DOTALL)
        if m:
            data = json.loads(m.group(0))
            return data.get("zone_label")
    except Exception as e:
        logger.error(f"Vision OCR failed: {e}")
    return None


# ── 從 591 地址拆出 district/road/lane/alley/num ──────────────────────────────

def parse_taipei_address(full_address: str) -> Optional[dict]:
    """
    「台北市大安區辛亥路三段157巷12弄4號」
      → {'district':'大安區', 'road':'辛亥路三段', 'lane':'157', 'alley':'12', 'num':'4'}
    至少要有 district + road 才回傳非空，否則 None。
    """
    if not full_address:
        return None
    s = full_address.strip()
    s = re.sub(r"^(?:台北市|臺北市)", "", s)

    m_dist = re.match(r"([\u4e00-\u9fa5]{1,3}區)", s)
    if not m_dist:
        return None
    district = m_dist.group(1)
    s = s[len(district):]

    # 路段：...路[段]
    m_road = re.match(r"(.+?(?:路|街|大道)(?:[一二三四五六七八九十]段)?)", s)
    if not m_road:
        return None
    road = m_road.group(1)
    s = s[len(road):]

    lane = alley = num = ""
    m_lane = re.search(r"(\d+)巷", s)
    if m_lane: lane = m_lane.group(1)
    m_alley = re.search(r"(\d+)弄", s)
    if m_alley: alley = m_alley.group(1)
    # 支援複合門牌：「8-1號」「8之1號」「85-3號」→ 統一轉成 TCD zonemap 能接受的「8之1」格式
    # （TCD 號欄位要的是「之」不是連字號，直接用 hyphen 會查無）
    m_num = re.search(r"(\d+(?:[-之]\d+)?)號", s)
    if m_num:
        num = m_num.group(1).replace("-", "之")

    return {"district": district, "road": road, "lane": lane, "alley": alley, "num": num}
