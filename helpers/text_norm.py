"""
集中所有地址 / 文字 normalize 邏輯的單一來源。

之前散在 analysis/geocoder.py, analysis/lvr_index.py, analysis/claude_analyzer.py,
database/models.py, api/manual_analyze.py — 每改一條規則要去多檔 grep。集中後：
  - 改一處到處生效
  - 新加 normalize step 不會漏到某條 path（CLAUDE.md policy 12 那種繁簡漏網雷的根治）

原檔保留薄 re-export alias 維持 backward compat，所以 callers 不用改 import。
新 code 建議直接 `from helpers.text_norm import X`。
"""
import re


# ── 全形 / 半形 ─────────────────────────────────────────────────────
def to_halfwidth(s: str) -> str:
    """全形 → 半形（數字 / 英文 / 符號）。全形空白變一般空白。"""
    if not s:
        return s
    out = []
    for ch in s:
        code = ord(ch)
        if 0xFF01 <= code <= 0xFF5E:
            out.append(chr(code - 0xFEE0))
        elif code == 0x3000:    # 全形空白
            out.append(" ")
        else:
            out.append(ch)
    return "".join(out)


# ── 繁簡 / 異體字 ───────────────────────────────────────────────────
def zhtw_normalize(s: str) -> str:
    """把 Google reverse geocode 偶爾回的簡體 / 異體字一次轉成台灣繁體。
    至少涵蓋 reverse_geocode 已踩到的字 (区→區，板桥→板橋，縣字、臺/台異體)；
    不做的：庄、湾 等繁簡都是有效台灣字、誤改風險高。"""
    if not s:
        return s
    return (s.replace("区", "區")
             .replace("桥", "橋")
             .replace("县", "縣")
             .replace("臺", "台"))


# ── 樓層 ────────────────────────────────────────────────────────────
# Google geocoder 對「N號X樓」處理不穩 — 帶樓層常解不出 ROOFTOP 精度、退到上層
# 巷弄/路段 centroid。例：「永吉路278巷58弄5號2樓」回 58弄 centroid (差 20m)，
# 但「永吉路278巷58弄5號」回精確 ROOFTOP。同棟所有樓層 lat/lng 本該一樣，
# 樓層資訊對地理座標無意義 → forward geocode 前一律 strip。
# 樣式：
#   "5號2樓" / "5號十二樓" / "5號B1樓" / "5號頂樓" / "5號地下1樓"
FLOOR_TAIL_RE = re.compile(
    r"\s*(?:B\d+樓|地下\d*樓?|頂樓|[\d一二三四五六七八九十]+樓)\s*$"
)


def strip_floor(address: str) -> str:
    """從地址尾端剝掉樓層 token 再 forward geocode。樓層不影響經緯度，
    保留會讓 Google 降級到 centroid。"""
    if not address:
        return address
    return FLOOR_TAIL_RE.sub("", address).strip()


# ── 區 / 市前綴 ─────────────────────────────────────────────────────
def strip_region_prefix(addr: str, city: str = "", district: str = "") -> str:
    """從地址字串去除所有 city / district 開頭前綴（處理舊資料重複前綴）。
    e.g. 「台北市中正區中正區羅斯福路...」→「羅斯福路...」
    注意繁簡體：傳入的 city 可能是「台北市」，但 LVR 資料用「臺北市」→ 兩者都要剝。
    """
    if not addr:
        return addr
    # city 前綴：一律比對「台北市|臺北市|新北市」（而且可能重複多次）
    addr = re.sub(r"^(台北市|臺北市|新北市)+", "", addr)
    # district 前綴：若有傳入具體 district 先剝，再 fallback 任何「X區」
    if district:
        addr = re.sub(f"^({re.escape(district)})+", "", addr)
    addr = re.sub(r"^([一-龥]{1,3}區)+", "", addr)
    return addr.strip()


# ── 里名 ────────────────────────────────────────────────────────────
# Google reverse-geocode 偶爾在「區」跟路名之間塞「華興里」之類；LVR / 591 都不存
# 里名，比對前必須剝除否則 ±0.01 三角驗證 fail
_LI_PREFIX_RE = re.compile(r"^[一-龥]{1,4}里")


def strip_li_name(addr: str) -> str:
    """剝掉地址開頭的「X里」token（限 1-4 個中文字 + 里）。
    必須在 strip_region_prefix 之後呼叫（先剝市區、剝完才看里）。"""
    if not addr:
        return addr
    return _LI_PREFIX_RE.sub("", addr)


# ── 段（阿拉伯 ↔ 漢字）─────────────────────────────────────────────
_DIGIT_TO_CN = {"1": "一", "2": "二", "3": "三", "4": "四", "5": "五",
                "6": "六", "7": "七", "8": "八", "9": "九", "10": "十"}


def digit_section_to_cn(addr: str) -> str:
    """阿拉伯數字段 → 漢字段（1段→一段 ... 10段→十段；其餘保持）。
    LVR 存法用漢字段，比對前要轉一致。"""
    if not addr:
        return addr
    return re.sub(r"(\d+)段",
                  lambda m: _DIGIT_TO_CN.get(m.group(1), m.group(1)) + "段",
                  addr)


# ── 複合門牌（N之M號 / N-M號）統一格式 ──────────────────────────────
def normalize_address_format(addr: str) -> str:
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


# ── 雜字 / 錯字 garbage 清除 ────────────────────────────────────────
def clean_address_garbage(addr: str) -> str:
    """去除「數字」與「巷/弄/號」之間的錯字/綴字 + 複合門牌格式標準化。
    案例：「恆光街3時巷」→「3巷」；「85X號」→「85號」；「10-1號」→「10之1號」。
    數字本身可含 '-' 或 '之'（複合門牌），其他中文字/英文都視為雜訊。"""
    if not addr:
        return addr
    addr = re.sub(r"(\d+(?:[-之]\d+)?)([^\d\-之巷弄號]+?)(巷|弄|號)", r"\1\3", addr)
    addr = normalize_address_format(addr)
    return addr
