"""
Claude API 呼叫模組。

Phase 1：純文字分析（用 Haiku，快且便宜）
Phase 2：截圖視覺分析（用 Sonnet，有 Vision 能力）
"""
import base64
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import anthropic

from config import ANTHROPIC_API_KEY
from analysis.scorer import get_recommendation

logger = logging.getLogger(__name__)

# 初始化 Anthropic client（啟用 prompt caching）
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

MODEL_TEXT = "claude-haiku-4-5-20251001"   # 初步文字分析
MODEL_VISION = "claude-sonnet-4-6"          # 截圖視覺分析


# ── Phase 1：文字分析 ─────────────────────────────────────────────────────────

SYSTEM_PROMPT_TEXT = """你是台灣都市更新專家，專門分析台北市/新北市的房地產物件是否具備都更或危老重建潛力。

【禁止幻覺 - 絕對規則】
- 只能根據提供的資料進行分析，絕不可捏造數字、地址、屋齡、坪數、價格、使用分區等任何資料
- 提供的資料如果是「未知」「null」「？」，你的分析必須明確指出「資料缺失無法判斷」，不准用常識、推測、或類似物件猜測填補
- 不准編造不存在的地標、捷運站、都更案名、建物名稱、社區名
- 推估建議（advice）可以，但絕不可包裝成「已知事實」
- 若資訊嚴重不足，`summary` 直接回覆「資訊不足，無法評估」即可

分析時要考慮（僅在有對應資料時）：
1. 屋齡（台北市防災型都更門檻：屋齡≥30年）
2. 樓層結構（4-5層公寓無電梯最有重建需求）
3. 建物類型（透天、公寓、店面的都更路徑不同）
4. 地理位置（TOD、商業地段、特定都更優先區）
5. 價格是否合理（跟都更換回價值比較）

【法規邊界】
- 防災型都更只適用「台北市」，新北市請用一般都更（容積獎勵率較低）
- 危老重建全國皆適用，門檻是屋齡≥30 年且符合耐震標準

請以 JSON 格式回覆，不要有其他文字。"""

TEXT_ANALYSIS_SCHEMA = {
    "key_strengths": "list of strings - 只根據提供資料列出（最多3點）；若無明確優勢就空陣列",
    "key_risks": "list of strings - 只根據提供資料或資料缺失列出（最多3點）；若無明確風險就空陣列",
    "renewal_path": "string - 建議都更路徑：都更/危老/防災都更/不建議/資料不足無法判斷",
    "summary": "string - 50字以內的綜合評估；資料不足時直接寫「資訊不足，無法評估」",
}


def analyze_property_text(property_data: dict) -> dict:
    """
    Phase 1：純文字分析。
    輸入 property_data（dict），回傳分析結果 dict。
    """
    prompt = _build_text_prompt(property_data)
    try:
        response = client.messages.create(
            model=MODEL_TEXT,
            max_tokens=800,
            system=[
                {
                    "type": "text",
                    "text": SYSTEM_PROMPT_TEXT,
                    "cache_control": {"type": "ephemeral"},  # 快取系統提示
                }
            ],
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        # 去掉可能的 markdown code block
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        import json
        return json.loads(text)
    except Exception as e:
        logger.error(f"Claude text analysis failed: {e}")
        return {
            "key_strengths": [],
            "key_risks": ["分析失敗"],
            "estimated_zoning": None,
            "estimated_road_width_m": None,
            "estimated_land_area_sqm": None,
            "renewal_path": "未知",
            "summary": "分析失敗，請稍後重試。",
        }


def _build_text_prompt(p: dict) -> str:
    age_str = f"{p.get('building_age')} 年" if p.get("building_age") else "未知"
    price_wan = f"{p.get('price_ntd', 0) / 10000:,.0f} 萬" if p.get("price_ntd") else "未知"
    mrt_str = (
        f"{p.get('nearest_mrt')} ({p.get('nearest_mrt_dist_m'):.0f}m)"
        if p.get("nearest_mrt")
        else "未知"
    )

    return f"""請分析以下物件的都更潛力：

物件資訊：
- 地點：{p.get('city', '')} {p.get('district', '')} {p.get('address', '（地址未知）')}
- 類型：{p.get('building_type', '未知')}，{p.get('total_floors', '?')} 層樓
- 屋齡：{age_str}
- 建物坪數：{p.get('building_area_ping', '?')} 坪
- 土地坪數：{p.get('land_area_ping', '未知')} 坪
- 售價：{price_wan}
- 最近捷運：{mrt_str}
- 標題/描述：{p.get('title', '')}

請依照要求的 JSON schema 回覆：
{TEXT_ANALYSIS_SCHEMA}"""


# ── 詳情頁 Vision OCR（Phase 1，補卡片缺的欄位） ──────────────────────────────

ADDRESS_OCR_PROMPT_TEMPLATE = """從這張 591 物件詳情頁截圖，**只抓「地址」一個欄位**。

頁面上會有「地址」標籤（在「物件基本資料」表格中），其右側即完整門牌（含巷弄號樓）。

【已知前提】
- 該物件位於：**{city}{district}**（這是卡片已抓到的可信資料，不會錯）
- 該行政區實際存在過的路/街/大道名稱（來自實價登錄）：
{road_list}
- 上面清單**僅供參考**：你看到的路名若不在清單，先嘗試判斷是不是相似字誤讀（如 羅↔新、東↔栗、南↔雨、景↔暑、北↔比、興↔與、龍↔瀧）；若視覺真的對應到清單外的合法路名（新建路、小巷），照實回。
- **嚴禁猜測**：視覺模糊到無法辨識就回 null，**不准**從清單硬挑一個。

【只回 JSON，一行】格式：
{{"address":"<完整門牌或 null>"}}"""


DETAIL_FULL_OCR_PROMPT = """Extract fields from this 591 Taiwan real estate detail page screenshot.

**CRITICAL FORMAT RULE**: Your ENTIRE response must be pure JSON. No introduction, no explanation, no markdown fences, no commentary — start with `{` and end with `}`.

Fields (use null when unknown, never omit keys):
- building_area_ping (number): 權狀坪數 / 建物坪數
- land_area_ping (number): 土地坪數
- building_age (number): 屋齡（年）
- total_floors (number): 總樓層
- floor (string): 物件樓層
- price_wan (number): 售價（萬）
- zoning (string): 使用分區（「住三」「商二」等）

注意：address 和 building_type 兩個欄位**不在這次任務內**（address 另有專屬 OCR，building_type 一律視為「公寓」由源頭 591 filter 保證）。

回覆範本（僅此 JSON 一行）：
{"building_area_ping":null,"land_area_ping":null,"building_age":null,"total_floors":null,"floor":null,"price_wan":null,"zoning":null}"""


DETAIL_OCR_PROMPT = """請仔細看這張 591 房屋詳情頁截圖，找出以下兩個欄位。

要找的欄位（591 詳情頁的標籤寫法很多種）：

1. **土地坪數** (land_area_ping)
   - 任何標籤包含「土地」兩字，後面接坪數的都算
   - 可能寫法：「土地坪數」「土地面積」「土地(持分)坪數」「土地持分」「基地坪數」「土地」等等
   - 格式都是「XX坪」結尾的數字
   - 範例：「土地坪數: 22.76坪」→ 22.76；「土地(持分)坪數: 8.5坪」→ 8.5
   - **絕對不要**把「建物坪數」「權狀坪數」「主建坪數」「附屬建物」「公共設施」當成土地

2. **使用分區** (zoning)
   - 標籤可能寫：「使用分區」「都市計畫分區」「土地使用分區」
   - 值範例：「住三」「商二」「住宅區」「商業區」

【絕對規則】
- 仔細掃過整張圖，只要看到「土地」兩字後面接數字+坪，就抓那個數字
- 真的找不到才填 null，不准猜測

請以 JSON 格式回覆，不要任何其他文字：
{
  "land_area_ping": <土地相關坪數的純數字；找不到填 null>,
  "zoning": "<使用分區的值；找不到填 null>"
}"""


def _normalize_address_format(addr: str) -> str:
    """統一複合門牌格式為「N之M號」。
    - 「10-1號」→「10之1號」
    - 「10號之1」→「10之1號」
    - 「10之1號」→「10之1號」（保持不變）"""
    if not addr:
        return addr
    # 1) 「N號之M」→「N之M號」（號在中間的非標準格式調整為號在末尾）
    addr = re.sub(r"(\d+)號之(\d+)", r"\1之\2號", addr)
    # 2) 「N-M號」→「N之M號」（hyphen 統一為「之」）
    addr = re.sub(r"(\d+)-(\d+)號", r"\1之\2號", addr)
    return addr


def _clean_address_garbage(addr: str) -> str:
    """去除「數字」與「巷/弄/號」之間的錯字/綴字 + 複合門牌格式標準化。
    案例：「恆光街3時巷」→「3巷」；「85X號」→「85號」；「10-1號」→「10之1號」。
    數字本身可含 '-' 或 '之'（複合門牌），其他中文字/英文都視為雜訊。"""
    if not addr:
        return addr
    addr = re.sub(r"(\d+(?:[-之]\d+)?)([^\d\-之巷弄號]+?)(巷|弄|號)", r"\1\3", addr)
    addr = _normalize_address_format(addr)
    return addr


def _ocr_address_once(crop_path: str, city: str, district: str, road_list: list) -> Optional[str]:
    """單次跑 address-only OCR；失敗回 None。"""
    img_b64, media_type = _encode_image(crop_path)
    if not img_b64:
        return None
    road_list_str = "(資料庫中該區尚無路名樣本，請憑視覺辨識)" if not road_list \
        else "  - " + "\n  - ".join(road_list[:120])   # 上限 120 條，控 token
    prompt = ADDRESS_OCR_PROMPT_TEMPLATE.format(
        city=city or "", district=district or "", road_list=road_list_str
    )
    try:
        response = client.messages.create(
            model=MODEL_VISION,
            max_tokens=200,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": img_b64}},
                    {"type": "text", "text": prompt},
                ],
            }],
        )
        text = response.content[0].text.strip()
        import json
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if not m:
            return None
        data = json.loads(m.group(0))
        addr = data.get("address")
        if isinstance(addr, str):
            addr = addr.strip()
            return addr if addr else None
    except Exception as e:
        logger.warning(f"address OCR pass failed: {e}")
    return None


def extract_address_consensus(crop_path: str, city: str, district: str) -> Optional[str]:
    """
    地址 OCR：切 2×2 四塊，取 r0c1 / r1c0 / r1c1 三塊各跑一次 → consensus voting。
    跳過 r0c0（左上通常是標題/首圖），地址欄通常在右上（r0c1）或下半（r1c*）。
    三塊獨立 OCR 比「整張一次」抗誤讀（字形相近的「臨 vs 簡」「詔 vs 紹」等），
    投票採「出現次數最多」的路名對應結果；平手取第一個。

    搭配 LVR whitelist 弱驗證 — 路名不在該區 LVR 清單只 log，不否決。
    """
    if not crop_path:
        return None
    try:
        from analysis.lvr_index import list_roads_in_district
        road_list = list_roads_in_district(city or "", district or "")
    except Exception:
        road_list = []

    # 地址 crop 本來就是 scraper 裁切好的窄區（通常 1200×500 左右），
    # 切片會把窄條再切碎反而丟失文字上下文。直接整張餵 Vision 最穩。
    res = _ocr_address_once(crop_path, city, district, road_list)
    candidates = [res.strip()] if res else []
    logger.info(f"地址 OCR (整張) 候選: {candidates}")
    if not candidates:
        return None

    # 投票：出現次數最多的勝出
    from collections import Counter
    tally = Counter(candidates)
    final = tally.most_common(1)[0][0]
    if len(tally) > 1:
        logger.info(f"地址 OCR 投票：{dict(tally)} → 採用 {final!r}")
    # 去除 591 刊登人員打錯的綴字（如「3時巷」→「3巷」、「85A號」→「85號」）
    cleaned = _clean_address_garbage(final)
    if cleaned != final:
        logger.info(f"  地址去綴字: {final!r} → {cleaned!r}")
        final = cleaned
    # LVR 弱驗證（只記 log，不否決）：先剝 city/district 前綴再抽路名，
    # 避免 greedy regex 把「中山區合江」吃進去當「路名」
    try:
        inner = re.sub(r"^(台北市|臺北市|新北市|桃園市|台中市|臺中市|高雄市|台南市|臺南市|基隆市|新竹市)", "", final)
        inner = re.sub(r"^[一-龥]{1,3}區", "", inner)
        m = re.match(r"^([一-龥]{1,5}(?:路|街|大道))", inner)
        if m and road_list and m.group(1) not in road_list:
            logger.warning(
                f"  ⚠ OCR 路名 '{m.group(1)}' 不在 {city}{district} LVR 路名清單中（{len(road_list)} 條）；"
                f"可能是冷門路或 OCR 誤讀。地址：{final!r}"
            )
    except Exception:
        pass
    return final


def _split_image_into_tiles(screenshot_path: str, cols: int = 2, rows: int = 4,
                             fixed_tile_w: int = None, fixed_tile_h: int = None) -> list:
    """把原圖切成 cols×rows 張 tile。
    - 預設模式：按原圖尺寸等分（舊行為，tile 大小隨原圖浮動）
    - fixed_tile_w/h 給值時：**固定 tile 尺寸**，從原圖左上角開始切 cols×rows 塊各 w×h
      （超出原圖邊界的 tile 以原圖邊界截斷，可能較小；關鍵區域在左上/上方時不受影響）
    回傳 tile path list（r0c0, r0c1, r1c0, r1c1, ...）。
    Vision 需要文字大小穩定，fixed 模式避免超長頁面稀釋字體密度。"""
    from PIL import Image
    from pathlib import Path
    # PIL 安全限制放寬（591 長頁 93M pixels 會超過預設 89M）
    Image.MAX_IMAGE_PIXELS = 200_000_000
    # 注意：PIL `with Image.open()` 不會釋放 pixel buffer（只關 fp）→ 必須顯式 close()
    im = Image.open(screenshot_path)
    try:
        w, h = im.size
        if fixed_tile_w and fixed_tile_h:
            tile_w, tile_h = fixed_tile_w, fixed_tile_h
        else:
            tile_w = w // cols
            tile_h = h // rows
        stem = Path(screenshot_path).with_suffix("")
        tiles = []
        for r in range(rows):
            for c in range(cols):
                left = c * tile_w
                top = r * tile_h
                right = min(left + tile_w, w)
                bottom = min(top + tile_h, h)
                if right <= left or bottom <= top:
                    continue   # tile 完全落在原圖外 → 略過
                tile_path = f"{stem}_tile_r{r}c{c}.png"
                cropped = im.crop((left, top, right, bottom))
                try: cropped.save(tile_path)
                finally: cropped.close()
                tiles.append(tile_path)
    finally:
        im.close()
    return tiles


def _ocr_one_tile(tile_path: str) -> dict:
    """對單一 tile 跑 Full detail OCR，回 dict（失敗回空 dict）。"""
    img_b64, media_type = _encode_image(tile_path)
    if not img_b64:
        return {}
    try:
        response = client.messages.create(
            model=MODEL_VISION,
            max_tokens=500,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": img_b64}},
                    {"type": "text", "text": DETAIL_FULL_OCR_PROMPT},
                ],
            }],
        )
        text = response.content[0].text.strip()
        import json
        m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
        if m:
            data = json.loads(m.group(1))
        else:
            m = re.search(r"\{.*\}", text, re.DOTALL)
            data = json.loads(m.group(0)) if m else {}
        # floor 欄位：OCR 有時會抓到 "2F" / "2樓" / "2/5F" 等尾綴 → 只保留第一段數字
        if data.get("floor") is not None:
            import re as _re
            s = str(data["floor"]).split("/")[0]
            m2 = _re.search(r"\d+", s)
            data["floor"] = int(m2.group(0)) if m2 else None
        return {k: v for k, v in data.items() if v is not None and v != ""}
    except Exception as e:
        logger.warning(f"tile OCR 失敗 ({tile_path}): {e}")
        return {}


def extract_full_detail_from_screenshot(screenshot_path: str) -> dict:
    """切 2×2 共 4 片，取 r0c1（右上）/ r1c0（左下）/ r1c1（右下）三片跑 OCR 合併。
    跳過 r0c0（左上通常是標題/首圖，沒欄位資料）。
    每片是原圖 1/4 大小 → Vision 看到的文字像素密度翻倍，大幅降低字太小誤判。
    三片用 ThreadPoolExecutor 平行呼叫 Claude API → 總耗時 ≈ max(單片)，不是 sum。"""
    # 固定 tile 尺寸 1200×1500，不隨原圖長度浮動 → Vision 每次看到的字體密度一致
    # 591 詳情頁基本資料區塊通常在前 3000px 內，2×2 固定切可覆蓋
    try:
        all_tiles = _split_image_into_tiles(
            screenshot_path, cols=2, rows=2,
            fixed_tile_w=1200, fixed_tile_h=1500,
        )
    except Exception as e:
        logger.warning(f"tile 切片失敗 fallback 單張: {e}")
        return _ocr_one_tile(screenshot_path)

    # _split_image_into_tiles 回傳順序：r0c0, r0c1, r1c0, r1c1 → 跳過 [0]，取 [1:]
    target_tiles = all_tiles[1:] if len(all_tiles) >= 4 else all_tiles

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=len(target_tiles)) as executor:
        per_tile_results = list(executor.map(_ocr_one_tile, target_tiles))

    merged = {}
    for result in per_tile_results:
        for k, v in result.items():
            if v not in (None, "") and merged.get(k) in (None, ""):
                merged[k] = v
    logger.info(
        f"Vision 3-tile OCR (parallel, r0c1/r1c0/r1c1) merged={merged} "
        f"per_tile_keys={[list(r.keys()) for r in per_tile_results]}"
    )
    return merged


def extract_detail_from_screenshot(screenshot_path: str) -> dict:
    """用 Sonnet Vision 從詳情頁截圖抓土地坪數、使用分區。
    其他欄位（地址、屋齡、樓層）由列表頁卡片抓取，比 OCR 準確。"""
    img_b64, media_type = _encode_image(screenshot_path)
    if not img_b64:
        logger.warning(f"無法讀取截圖 {screenshot_path}")
        return {}
    try:
        response = client.messages.create(
            model=MODEL_VISION,  # Sonnet，Chinese OCR 更準
            max_tokens=500,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": img_b64,
                        },
                    },
                    {"type": "text", "text": DETAIL_OCR_PROMPT},
                ],
            }],
        )
        text = response.content[0].text.strip()
        logger.info(f"Vision OCR 原始回應 ({screenshot_path}):\n{text[:500]}")

        # 從 Sonnet 多話的回應中擷取 JSON block
        import json
        json_str = None
        # 優先抓 ```json ... ``` 區塊
        m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
        if m:
            json_str = m.group(1)
        else:
            # fallback: 抓任何 {...} 大括號區塊
            m = re.search(r"\{.*?\}", text, re.DOTALL)
            if m:
                json_str = m.group(0)
        if not json_str:
            logger.warning(f"Vision 回應找不到 JSON：{text[:200]}")
            return {}

        result = json.loads(json_str)
        cleaned = {k: v for k, v in result.items() if v is not None and v != ""}
        logger.info(f"Vision OCR 解析結果：{cleaned}")
        return cleaned
    except Exception as e:
        logger.error(f"Vision OCR failed for {screenshot_path}: {e}", exc_info=True)
        return {}


# ── Phase 2：截圖視覺分析 ─────────────────────────────────────────────────────

SYSTEM_PROMPT_VISION = """你是台灣都市更新與地政專家。
你會看到政府地圖截圖（地籍圖、容積分區圖、都更地圖），請從中擷取關鍵資訊。
請以 JSON 格式回覆，不要有其他文字。"""


def analyze_maps(
    property_data: dict,
    screenshot_cadastral: Optional[str] = None,
    screenshot_zoning: Optional[str] = None,
    screenshot_renewal: Optional[str] = None,
) -> dict:
    """
    Phase 2：截圖視覺分析。
    傳入截圖檔案路徑，回傳分析結果 dict。
    """
    content = []

    # 加入截圖
    screenshots = {
        "地籍圖（parcel map）": screenshot_cadastral,
        "都市計畫容積分區圖（zoning map）": screenshot_zoning,
        "都更地圖（urban renewal map）": screenshot_renewal,
    }

    for label, path in screenshots.items():
        if path and Path(path).exists():
            img_b64, media_type = _encode_image(path)
            if img_b64:
                content.append({
                    "type": "text",
                    "text": f"\n【{label}】",
                })
                content.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": img_b64,
                    },
                })

    if not content:
        logger.warning("No valid screenshots for vision analysis")
        return {}

    content.append({"type": "text", "text": _build_vision_prompt(property_data)})

    try:
        response = client.messages.create(
            model=MODEL_VISION,
            max_tokens=1500,
            system=[
                {
                    "type": "text",
                    "text": SYSTEM_PROMPT_VISION,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": content}],
        )
        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        import json
        return json.loads(text)
    except Exception as e:
        logger.error(f"Claude vision analysis failed: {e}")
        return {}


def _build_vision_prompt(p: dict) -> str:
    return f"""請分析以上地圖截圖，針對以下物件回答：
地點：{p.get('city', '')} {p.get('district', '')} {p.get('address', '（地址未知）')}

請回覆以下 JSON（無法判斷的欄位填 null）：
{{
  "cadastral": {{
    "parcel_area_sqm": <地號面積，單位 m²>,
    "parcel_shape": "<方正/不規則/長條>",
    "adjacent_empty_land": <是否有鄰接空地，true/false>,
    "estimated_consolidation_area_sqm": <周邊可整合基地估計面積（m²）>,
    "road_frontage_m": <臨路寬度（公尺）>,
    "notes": "<地籍圖觀察備註>"
  }},
  "zoning": {{
    "zone_type": "<使用分區，例：住三乙>",
    "legal_far": <法定容積率，例：2.25>,
    "legal_bcr": <法定建蔽率，例：0.45>,
    "special_zone": "<特定專用區或無>",
    "notes": "<分區圖觀察備註>"
  }},
  "renewal": {{
    "in_renewal_zone": <是否在既有都更或防災都更範圍，true/false>,
    "nearby_projects": "<鄰近都更案名或無>",
    "notes": "<都更地圖觀察備註>"
  }},
  "overall_assessment": "<整體評估：200字以內，說明都更可行性、優缺點、建議路徑>"
}}"""


def _encode_image(path: str):
    """讀檔 → 必要時 downscale 到 ≤5MB 符合 Claude Vision API 限制。
    回傳 (base64_str, media_type) tuple；舊呼叫端只用 [0] 仍相容（但 media_type 會錯）。"""
    try:
        from PIL import Image
        import io
        with open(path, "rb") as f:
            raw = f.read()
        # base64 會把大小放大 1.33x；Claude 上限 5MB 指 base64 後大小 → raw 要 <3.75MB 才安全
        if len(raw) <= 3_700_000:
            return base64.standard_b64encode(raw).decode("utf-8"), "image/png"
        # PIL Image.open 必須顯式 close() 才釋放 pixel buffer
        im = Image.open(io.BytesIO(raw))
        try:
            max_dim = 2000
            if max(im.size) > max_dim:
                scale = max_dim / max(im.size)
                resized = im.resize((int(im.width * scale), int(im.height * scale)), Image.LANCZOS)
                im.close()
                im = resized
            if im.mode != "RGB":
                converted = im.convert("RGB")
                im.close()
                im = converted
            buf = io.BytesIO()
            im.save(buf, format="JPEG", quality=85, optimize=True)
            out = buf.getvalue()
            logger.info(f"  截圖過大 ({len(raw)//1024}KB) → 縮圖 JPEG ({len(out)//1024}KB)")
            return base64.standard_b64encode(out).decode("utf-8"), "image/jpeg"
        finally:
            try: im.close()
            except Exception: pass
    except Exception as e:
        logger.error(f"Failed to encode image {path}: {e}")
        return None, "image/png"


# ── 最終綜合建議 ──────────────────────────────────────────────────────────────

def generate_final_recommendation(
    property_data: dict,
    score: dict,
    renewal_calc: dict,
    text_analysis: dict,
    vision_analysis: Optional[dict] = None,
) -> dict:
    """
    產生結構化五段建議（不依賴分數）。
    """
    p = property_data
    floors = p.get("total_floors")
    age = p.get("building_age")
    city = p.get("city", "")
    zoning = p.get("zoning") or ""
    mrt = p.get("nearest_mrt")
    mrt_dist = p.get("nearest_mrt_dist_m")

    sections = []
    is_taipei = "台北" in city or "臺北" in city

    # § 1 樓高
    if floors:
        if floors >= 5:
            sections.append(f"【樓高】{floors}層，住戶數多協調困難，建商整合意願低")
        elif floors == 4:
            sections.append(f"【樓高】{floors}層，住戶少易協調，建商整合意願高")
        else:
            sections.append(f"【樓高】{floors}層，住戶極少，建商整合意願高")
    else:
        sections.append("【樓高】資料缺失")

    # § 2 屋齡：用 ✓ / ✗ 條列資格
    if age:
        build_year = datetime.now().year - age
        fz_ok = build_year <= 1974 and is_taipei
        dg_ok = age >= 20
        wl_ok = age >= 30
        def _chk(ok, label, note=""):
            if ok:
                return f"<chk-y>{label}{note}</chk-y>"
            else:
                return f"<chk-n>{label}</chk-n>"
        fz_note = "（需耐震評估）" if fz_ok else ""
        sections.append(f"【屋齡】{age}年　{_chk(fz_ok, '防災型都更', fz_note)}　{_chk(dg_ok, '一般都更')}　{_chk(wl_ok, '危老')}")
    else:
        sections.append("【屋齡】資料缺失")

    # § 3 整合難度
    sections.append("【整合難度】待評估")

    # § 4 分回價值 + 出價建議
    v2 = renewal_calc.get("v2") or {}
    price_ntd = p.get("price_ntd")
    price_wan = price_ntd / 10000 if price_ntd else None
    scenarios = v2.get("scenarios") or {}
    dugen = scenarios.get("都更") or {}
    weishau = scenarios.get("危老") or {}
    d_val = dugen.get("total_value_wan")
    w_val = weishau.get("total_value_wan")
    if d_val and price_wan and price_wan > 0:
        d_mult = d_val / price_wan
        w_mult = w_val / price_wan if w_val else None
        w_max = w_val / 3.2 if w_val else 0
        d_max = d_val / 3.2
        line1 = f"危老 {w_val:,.0f}萬（{w_mult:.2f}倍）" if w_val else "危老 —"
        line2 = f"都更 {d_val:,.0f}萬（{d_mult:.2f}倍）"
        sections.append(
            f"【分回價值】{line1}　{line2}\n"
            f"<bid_selector w_val=\"{w_val or 0}\" d_val=\"{d_val}\" w_max=\"{w_max:.0f}\" d_max=\"{d_max:.0f}\">"
        )
    else:
        missing = []
        if not p.get("land_area_ping"):
            missing.append("土地坪數")
        if not zoning:
            missing.append("使用分區")
        if not price_wan:
            missing.append("售價")
        sections.append(f"【分回價值】缺{'/'.join(missing or ['資料'])}，無法計算")

    # § 5 其他：地段分析 + TOD
    extras = []
    district_name = p.get("district") or ""
    if district_name:
        extras.append(f"• {city}{district_name}地段")
    if mrt and mrt_dist:
        if mrt_dist <= 200:
            extras.append(f"• ☑TOD {mrt}站{int(mrt_dist)}m（200m內最高+30%容積）")
        elif mrt_dist <= 500:
            extras.append(f"• ☑TOD {mrt}站{int(mrt_dist)}m（500m內最高+20%容積）")
        else:
            extras.append(f"• 最近捷運{mrt}站{int(mrt_dist)}m，不在TOD範圍")
    if extras:
        sections.append("【其他】" + "\n".join(extras))

    reason = "\n\n".join(sections)

    # 簡化建議標籤
    rec = "待評估"
    if v2:
        scenarios = (v2.get("scenarios") or {})
        d_val = (scenarios.get("都更") or {}).get("return_value_wan")
        if d_val and price_wan and price_wan > 0:
            mult = d_val / price_wan
            if mult >= 3.2:
                rec = "值得考慮"
            elif mult >= 2.5:
                rec = "一般"
            else:
                rec = "不建議"

    return {
        "recommendation": rec,
        "reason": reason,
        "summary": text_analysis.get("summary", ""),
    }
