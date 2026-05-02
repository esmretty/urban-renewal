"""
Firestore 物件資料輔助函式。
不再使用 SQLAlchemy ORM，改用 dict + Firestore。
"""
import re
from datetime import datetime, timedelta
from typing import Optional
from database.time_utils import now_tw, now_tw_iso


# 判定一筆物件是否「資料完整」的關鍵欄位
REQUIRED_FIELDS = [
    "price_ntd", "building_area_ping", "land_area_ping",
    "building_age", "address",
]


# ── sources[] 是來源唯一真相 ─────────────────────────────────────────────────────
# 每筆物件 doc 的「來源」資訊存在 sources[] 陣列：
#   sources = [{name, source_id, url, added_at, alive}]
# 同物件多來源（591 重發 / 跨來源 dedup）→ append 一筆
# verify_alive 不再 archive 整 doc，只 toggle sources[i].alive
# source_keys[] 是平面索引（Firestore array_contains 用，從 sources 衍生）
# 不再有 url / source_id / source / url_alt 主欄位

# 規範化映射：sources[].name 可以是中文（"永慶"/"信義"）給前端 badge 用，
# 但 source_keys[] 平面索引一律用英文 canonical name（"yongqing"/"sinyi"/"591"），
# 避免相同物件用兩種 key 存在 DB 內查不到。
_NAME_CANONICAL = {
    "591": "591",
    "永慶": "yongqing",
    "yongqing": "yongqing",
    "信義": "sinyi",
    "sinyi": "sinyi",
    "manual": "manual",
}
_KNOWN_PREFIXES = ("591_", "yongqing_", "sinyi_", "manual_")


def canonical_source_name(name: str) -> str:
    """name → canonical English (永慶→yongqing, 信義→sinyi)"""
    n = (name or "").strip()
    return _NAME_CANONICAL.get(n, n)


def strip_known_prefix(sid: str) -> str:
    """剝掉跨平台 prefix (yongqing_/sinyi_/591_/manual_)，避免 source_id 帶 prefix 造成 key 不一致"""
    s = (sid or "").strip()
    for p in _KNOWN_PREFIXES:
        if s.startswith(p):
            return s[len(p):]
    return s


def compute_source_keys(sources: list) -> list:
    """從 sources[] 衍生平面索引 ["591:20114614", "yongqing:8893"]（canonical 英文 name + 純 site_id）。
    用於 Firestore array_contains query（Firestore 不支援巢狀 array 欄位查詢）。"""
    out = []
    for s in (sources or []):
        name = canonical_source_name(s.get("name"))
        sid = strip_known_prefix(s.get("source_id"))
        # 也剝掉 name_ 前綴（若 sid 是 "{name}_X" 形式，例如 raw scrapper 設的）
        if sid.startswith(f"{name}_"):
            sid = sid.split("_", 1)[1]
        if name and sid:
            key = f"{name}:{sid}"
            if key not in out:
                out.append(key)
    return out


def make_source_key(name: str, site_id: str) -> str:
    """組 source_key (canonical_name:site_id)。
    name 自動轉 canonical 英文（永慶→yongqing, 信義→sinyi）；site_id 自動剝跨平台 prefix。"""
    n = canonical_source_name(name)
    sid = strip_known_prefix(site_id)
    if sid.startswith(f"{n}_"):
        sid = sid.split("_", 1)[1]
    return f"{n}:{sid}"


def primary_source(doc: dict) -> Optional[dict]:
    """取「主要顯示用」的 source：alive==true 中最早 added_at；無 alive 用最早 added_at。
    所有「主 url 給 admin/LINE 用」的 caller 走這個 helper，避免直接讀已廢的 doc.url。"""
    sources = doc.get("sources") or []
    if not sources:
        return None
    alive = [s for s in sources if s.get("alive") is not False]
    pool = alive or sources
    return min(pool, key=lambda s: s.get("added_at") or "")


def primary_url(doc: dict) -> Optional[str]:
    """取主要顯示用 url（從 primary_source）"""
    s = primary_source(doc)
    return s.get("url") if s else None


def primary_source_id(doc: dict) -> Optional[str]:
    """取主要顯示用 source_id（含 prefix，如 591_20114614）"""
    s = primary_source(doc)
    if not s:
        return None
    name = s.get("name") or ""
    sid = s.get("source_id") or ""
    if sid and not sid.startswith(f"{name}_"):
        return f"{name}_{sid}"
    return sid or None


def add_source_to_doc(doc: dict, name: str, site_id: str, url: str, added_at: Optional[str] = None) -> bool:
    """append 一筆 source 到 doc.sources[]（若 source_key 已存在則不重複）。
    回傳：True = 真的加了；False = 已存在 / 無效 input 跳過。
    呼叫端負責再寫回 Firestore。"""
    # Guard：空 url / 空 site_id 視為無效，不寫進 sources（避免 garbage entries
    # 像 {url: None, alive: True} 出現在 sources 列表裡 — 之前 audit 發現 44 筆混雜）
    if not url or not site_id:
        return False
    sources = list(doc.get("sources") or [])
    key = make_source_key(name, site_id)
    existing_keys = {make_source_key(s.get("name"), s.get("source_id")) for s in sources}
    if key in existing_keys:
        return False
    sources.append({
        "name": name,
        "source_id": site_id,
        "url": url,
        "added_at": added_at or now_tw_iso(),
        "alive": True,
    })
    doc["sources"] = sources
    doc["source_keys"] = compute_source_keys(sources)
    return True


def remove_source_from_doc(doc: dict, name: str, site_id: str) -> bool:
    """從 doc.sources[] 移除指定 source（換物件流程）。
    回傳：True = 有移除；False = 沒找到。"""
    sources = list(doc.get("sources") or [])
    key = make_source_key(name, site_id)
    new_sources = [s for s in sources if make_source_key(s.get("name"), s.get("source_id")) != key]
    if len(new_sources) == len(sources):
        return False
    doc["sources"] = new_sources
    doc["source_keys"] = compute_source_keys(new_sources)
    return True


def set_source_alive(doc: dict, name: str, site_id: str, alive: bool) -> bool:
    """更新 doc.sources[] 中某 source 的 alive flag（verify_alive 用）。
    回傳：True = 找到並更新；False = 沒找到。"""
    sources = list(doc.get("sources") or [])
    key = make_source_key(name, site_id)
    found = False
    for s in sources:
        if make_source_key(s.get("name"), s.get("source_id")) == key:
            s["alive"] = bool(alive)
            found = True
    if found:
        doc["sources"] = sources
    return found


def all_sources_dead(doc: dict) -> bool:
    """所有 sources alive=False → 整個物件可標 archived。"""
    sources = doc.get("sources") or []
    if not sources:
        return False
    return all(s.get("alive") is False for s in sources)


def age_to_completed_year(age) -> Optional[int]:
    """把「屋齡 N 年」回推成完工年 = 當前年 - N。
    抓爬蟲那刻轉換 → 之後 UI 顯示時當下重算（屋齡會跟著年份走）。"""
    try:
        if age is None: return None
        a = int(round(float(age)))
        if a < 0 or a > 200: return None
        return now_tw().year - a
    except Exception:
        return None


def extract_district(s: str) -> str:
    """從字串抽出台灣行政區名，忽略「市」字前綴（避免誤抓「市大安區」這種跨字 match）。"""
    if not s:
        return ""
    # 先剝 city 前綴再找
    s2 = re.sub(r"^(台北市|臺北市|新北市)", "", s)
    m = re.search(r"(?:市)?([\u4e00-\u9fa5]{2,3}區)", s2)
    return m.group(1) if m else ""


# 真實門牌地址必須含的「路名 token」（路/街/大道/巷/弄）
# 用於擋掉 591 詳情頁 community-name 屋主亂填的廣告詞（如「近後山埤1號出口」含「號」但無路名）
_ADDRESS_ROAD_TOKEN_RE = re.compile(r"(?:路|街|大道|巷|弄)")


def looks_like_real_address(text: str, *, require_number: bool = False) -> bool:
    """是否為「真實門牌地址」— 必須含「路/街/大道/巷/弄」之一。

    背景：591 詳情頁有 <community-name> 自訂欄位，是屋主自由填寫的「社區名」。
    很多屋主拿來填廣告詞（例：「近後山埤1號出口」、「捷運站旁」），這類字串常含「號」字
    但其實不是門牌地址。如果只用「含號」當條件採用，會把廣告詞當地址存進 DB。

    Args:
        text: 待檢驗字串
        require_number: True → 也必須含「號」字（要求門牌完整到號）

    Returns:
        True = 看起來是真實地址；False = 應拒收（空字串、廣告詞、站名、過於模糊）

    範例：
        looks_like_real_address("松江路313巷")              → True
        looks_like_real_address("景新街418巷11弄16號")     → True
        looks_like_real_address("近後山埤1號出口")          → False（無路名 token）
        looks_like_real_address("捷運站旁")                → False
        looks_like_real_address("松江路313巷", require_number=True) → False（無「號」）
    """
    if not text:
        return False
    if require_number and "號" not in text:
        return False
    return bool(_ADDRESS_ROAD_TOKEN_RE.search(text))


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
    addr = re.sub(r"^([\u4e00-\u9fa5]{1,3}區)+", "", addr)
    return addr.strip()


def compose_full_address(doc: dict, prefer_inferred: bool = True) -> str:
    """拼 city + district + address 回完整地址（讀取端用）。
    容錯：即使 address 仍含舊 prefix 也能正確處理。"""
    base = (doc.get("address_inferred") if prefer_inferred else None) or doc.get("address") or ""
    if not base:
        return ""
    city = doc.get("city") or ""
    district = doc.get("district") or ""
    # 如果 address 已有 city 前綴（舊資料殘留）→ 直接用
    if city and base.startswith(city):
        return base
    if base.startswith("台北市") or base.startswith("臺北市") or base.startswith("新北市"):
        return base
    return f"{city}{district}{base}"


def is_record_complete(doc: dict) -> bool:
    """檢查 Firestore 記錄是否所有關鍵欄位都有值。"""
    return all(doc.get(f) not in (None, "", 0) for f in REQUIRED_FIELDS)


def sanitize_for_firestore(doc: dict, *, max_depth: int = 20) -> dict:
    """
    清理 doc 讓 Firestore 能吞：
      - 偵測循環參考 → 對到過的 id 用 None 取代（並 log 欄位路徑）
      - 深度超過 max_depth → 截斷為 None（並 log）
      - 其他型別（set / tuple / datetime 等）保持原樣或轉 list
    回傳淺拷貝後的新 dict（原 doc 不動）。
    """
    import logging
    _log = logging.getLogger(__name__)
    seen: set = set()
    problems: list = []

    def _walk(v, depth: int, path: str):
        if depth > max_depth:
            problems.append(f"depth>{max_depth} at {path}")
            return None
        if isinstance(v, (str, int, float, bool)) or v is None:
            return v
        # 循環偵測只對 dict / list / tuple 做（這些才會被嵌套）
        if isinstance(v, (dict, list, tuple)):
            oid = id(v)
            if oid in seen:
                problems.append(f"cycle at {path}")
                return None
            seen.add(oid)
            try:
                if isinstance(v, dict):
                    return {k: _walk(vv, depth + 1, f"{path}.{k}") for k, vv in v.items()}
                return [_walk(vv, depth + 1, f"{path}[{i}]") for i, vv in enumerate(v)]
            finally:
                seen.discard(oid)
        # 其他型別：交給 Firestore 處理（datetime / GeoPoint / bytes 等）
        return v

    clean = _walk(doc, 0, "<root>")
    if problems:
        _log.warning("sanitize_for_firestore: problems detected -> %s", problems[:10])
    return clean


def get_missing_fields(doc: dict) -> list[str]:
    """回傳缺哪些關鍵欄位。"""
    return [f for f in REQUIRED_FIELDS if doc.get(f) in (None, "", 0)]


def is_basement_floor(floor_str) -> bool:
    """偵測 591 floor 字串是否為「純地下室」物件（CLAUDE.md「抗性物件 — 地下室」）。
    591 listing API 對純地下室會給 'B1/5F'（地下 1 樓 / 共 5 樓），舊 parse_floor_range
    把 B1 解析成 1F → 嚴重誤判。

    重要：只有「純地下室」才算抗性。1F+地下室（如 B1F~1F/5F、1F+B1/5F）視為 1F 物件，
    地下室僅是附屬空間，不算抗性。判定邏輯：拆 / 前段，再用 ~/-/+ 拆 token，
    **每個 token 都以 B 開頭** → 純地下室；任一 token 是非 B 數字 → 1F+地下室不算抗性。

    True 例：'B1', 'B1F', 'B1/5F', 'B1~B2/5F', 'B1F-B2F/5F', '地下1樓'
    False 例：'1F/5F', '4F~5F/5F', 'B1F~1F/5F', '1F+B1/5F', None, '', '5F'
    """
    if not floor_str:
        return False
    import re as _re
    s = str(floor_str).strip().upper()
    if not s:
        return False
    # 拆 / 前段（591 floor「物件樓層 / 總樓層」格式）
    main = s.split("/", 1)[0].strip()
    if not main:
        return False
    # 中文「地下」直接視為純地下室
    if "地下" in main:
        return True
    # 拆範圍 token（~ - － +），每個非空 token 都必須以 'B' 開頭才算純地下室
    tokens = [t.strip() for t in _re.split(r"[~\-－+]", main) if t.strip()]
    if not tokens:
        return False
    return all(_re.match(r"^B\d", t) for t in tokens)


def parse_floor_range(floor_str, total_floors=None):
    """解析 floor 字串成 (min, max, total) 三元組 — 支援樓中樓格式。

    格式：
      "3"    / "3F"   / "3樓"           → (3, 3, total)
      "1~2"  / "1F~2F" / "1樓-2樓"      → (1, 2, total)
      "1F~2F/4F"                          → (1, 2, 4)   ← 樓中樓 (1-2樓物件、整棟 4 樓)
      "1+2"  / "1F+2F"                   → (1, 2, total)
      "B1/5F" / "B1F"                     → (None, None, 5)  ← 地下室；caller 改看 is_basement_floor
      "—" / "" / None                    → (None, None, total)

    回 (floor_range_min, floor_range_max, total_floors)。
    地下室 case (含 'B' 前綴) 一律回 floor_range_min/max=None — 別讓 B1 被當 1F；
    caller 需要另外用 is_basement_floor() 標 is_basement flag。
    """
    import re as _re
    if not floor_str:
        return (None, None, total_floors)
    s = str(floor_str).strip()
    if not s or s == "—":
        return (None, None, total_floors)
    parsed_total = total_floors
    # 1) 拆 / 後的總樓層部分
    if "/" in s:
        parts = s.split("/", 1)
        s = parts[0].strip()
        try:
            t_clean = _re.sub(r"[Ff樓\s]", "", parts[1])
            if t_clean:
                parsed_total = int(t_clean)
        except (ValueError, TypeError):
            pass
    # 1.5) 地下室（B1 / B2 / 地下 X 樓）→ floor_range 全部回 None，total 仍保留
    if is_basement_floor(floor_str):
        return (None, None, parsed_total)
    # 2) 拆範圍 (~ - － +) 抽出每段的數字
    nums = []
    for tok in _re.split(r"[~\-－+]", s):
        m = _re.search(r"\d+", tok)
        if m:
            try:
                n = int(m.group(0))
                if 0 < n < 200:   # 合理樓層範圍
                    nums.append(n)
            except ValueError:
                pass
    if not nums:
        return (None, None, parsed_total)
    return (min(nums), max(nums), parsed_total)


def make_property_doc(
    item: dict,
    scores: dict,
    renewal: dict,
    text_analysis: dict,
    final: dict,
    lat=None,
    lng=None,
    nearest_mrt=None,
    mrt_dist=None,
    mrt_exit=None,
    land_sqm=None,
    doc_id: Optional[str] = None,
) -> dict:
    """組裝要存入 Firestore 的物件 document。
    doc_id：物件唯一 ID（YYYYMMDD-XXXXXX 格式），caller 必須傳。
            若 None 表示不知道，會先生成一個（但 caller 用 col.document() 寫時請用回傳的 id 欄位）。"""
    if doc_id is None:
        from database.db import gen_dated_id
        doc_id = gen_dated_id(item.get("scrape_session_at"))

    source_name = item.get("source", "591")
    source_id_raw = item.get("source_id") or ""
    site_id = source_id_raw.split("_", 1)[1] if source_id_raw.startswith(f"{source_name}_") else source_id_raw
    sources_arr = [{
        "name": source_name,
        "source_id": site_id,
        "url": item.get("url"),
        "added_at": now_tw_iso(),
        "alive": True,
    }]
    return {
        "id": doc_id,
        # sources[] 是來源唯一真相；source_keys[] 是平面索引給 Firestore array_contains 用
        "sources": sources_arr,
        "source_keys": compute_source_keys(sources_arr),
        "archived": False,        # 新建/重抓物件一律不是 archived
        "image_url": item.get("image_url"),
        "scraped_at": now_tw_iso(),
        "published_at": _parse_published_at(item.get("_published_text"))
                        or _proxy_published_at(item),
        "updated_at": _parse_published_at(item.get("_updated_text")),    # 591 詳情頁「最後更新」
        "list_rank": item.get("list_rank"),
        "scrape_session_at": item.get("scrape_session_at"),
        "title": item.get("title"),
        "city": item.get("city"),
        "district": item.get("district"),
        "address": item.get("address"),
        "building_type": item.get("building_type"),
        "total_floors": item.get("total_floors"),
        "floor": item.get("floor"),
        "building_age": item.get("building_age"),
        "building_age_source": item.get("building_age_source"),
        "building_age_completed_year": item.get("building_age_completed_year"),
        "building_area_ping": item.get("building_area_ping"),
        "land_area_ping": item.get("land_area_ping"),
        "land_area_sqm": land_sqm,
        "price_ntd": item.get("price_ntd"),
        "price_per_ping": item.get("price_per_ping"),
        "latitude": lat,
        "longitude": lng,
        # 591 原生座標（API/詳情頁給的），OCR 之前就拿到；
        # 跟最終 latitude/longitude 分開存，用來抓「地址被錯誤修正」的 regression
        "source_latitude": item.get("source_latitude"),
        "source_longitude": item.get("source_longitude"),
        "nearest_mrt": nearest_mrt,
        "nearest_mrt_dist_m": mrt_dist,
        "score_total": scores.get("total"),
        "score_age": scores.get("age"),
        "score_far": scores.get("far"),
        "score_land": scores.get("land"),
        "score_tod": scores.get("tod"),
        "score_road": scores.get("road"),
        "score_consolidation": scores.get("consolidation"),
        "renewal_type": renewal.get("renewal_type"),
        "renewal_bonus_rate": renewal.get("bonus_rate"),
        "renewal_new_area_ping": renewal.get("estimated_return_ping"),
        "renewal_value_ntd": renewal.get("estimated_return_value"),
        "renewal_profit_ntd": renewal.get("renewal_profit"),
        # ── v2 計算結果不存 DB（CLAUDE.md 規則 8）：multiple/分回坪等動態結果由前端 + LINE hook 即時算
        "new_house_price_wan_override": None,                   # 用戶手動覆寫
        "ai_analysis": text_analysis.get("summary"),
        "ai_recommendation": final.get("recommendation"),
        "ai_reason": final.get("reason"),
        "analysis_status": "done",         # done / pending / skipped
        "analysis_completed_at": None,     # ISO8601 UTC，pipeline 完成時寫入
        "skip_reason": None,                # e.g. "5F_apartment" / "land_too_small"
        "is_foreclosure": False,            # 法拍屋
        "foreclosure_reasons": None,        # ["標題含#", "代理人刊登"] etc
        "is_remote_area": False,            # 偏遠地段（新北市天險隔開的區，如板橋浮洲/大漢溪以西）
        "unsuitable_for_renewal": False,    # 新北 4 區土地分區非住宅用地 → 不適合都更
        "unsuitable_reason": None,          # 給用戶看的中性說明
        "deep_analysis_done": False,
        # 價格歷史（重複出現時追蹤）
        "is_price_changed": False,
        "price_history": [],
        # Phase 2 欄位
        "zoning": None,
        "zoning_candidates": None,        # [{address, zoning, distance_m, is_most_likely}]
        "zoning_source": None,            # "5168" | "tcd_via_5168" | "tcd_via_reverse_geo" | "coord_mismatch" | ...
        "zoning_source_url": None,        # 查詢來源的 URL（前端可點）
        "zoning_lookup_at": None,
        "zoning_error": None,             # 錯誤訊息（若查不到）
        "address_probable": None,
        "address_inferred": None,             # LVR 反推的完整門牌
        "address_inferred_confidence": None,  # unique / multi / none
        "address_inferred_candidates": None,  # 多筆候選 address list
        "lvr_records": None,                  # LVR 同坪數成交紀錄
        "road_width_name": None,              # 臨路名稱
        "road_width_all": None,               # 附近所有道路路寬
        "legal_far": None,
        "road_width_m": None,
        "in_renewal_zone": None,
        "screenshot_cadastral": None,
        "screenshot_zoning": None,
        "screenshot_renewal": None,
    }


DEFAULT_SKIP_THRESHOLDS = {
    "max_floors": 5,                         # 樓層 >= 此值先不分析（5 表 5F 以上皆跳）
    "max_total_price_wan": 5000,             # 總價 > 5000 萬不分析
    "max_price_per_building_ping_wan": 130,  # 建物單價 > 130 萬/坪不分析
    "max_price_per_land_ping_wan": 300,      # 土地單價 > 300 萬/坪不分析
    "min_land_ping": 7,                      # 土地 < 7 坪不分析（太小難整合）
}


def detect_foreclosure(item: dict, detail_text: str = "") -> tuple[bool, list]:
    """
    偵測法拍屋。
    規則：
      1. 標題含 "#" 或全形 "＃" 且 刊登者含 "代理人" → 法拍。
         591 代理人標記常用全形 ＃（U+FF03），也有半形 # — 兩種都要抓。
      2. 591「社區」欄位 RAW value（item["_community_raw"]）含「【」廣告詞 → 法拍。
         仲介在「社區」label 寫「【店長推薦】XX」這種廣告字串通常是法拍特徵。
    回 (是否法拍, [reason 列表])
    """
    title = item.get("title") or ""
    raw = item.get("_raw_text") or detail_text or ""
    has_hash = "#" in title or "＃" in title
    if has_hash and "代理人" in raw:
        return True, ["標題含 # 或 ＃ + 代理人"]
    community_raw = item.get("_community_raw") or ""
    if "【" in community_raw:
        return True, [f"591「社區」欄位含「【」廣告詞：{community_raw[:50]}"]
    return False, []


def should_skip_analysis(item: dict, thresholds: dict = None) -> tuple[bool, str]:
    """
    回傳 (是否跳過, 原因碼)。原因碼：
      over_max_floors | price_too_high | building_ping_too_high |
      land_ping_too_high | land_too_small
    """
    t = {**DEFAULT_SKIP_THRESHOLDS, **(thresholds or {})}
    floors = item.get("total_floors")
    if floors and floors >= t["max_floors"]:
        return True, "over_max_floors"

    price_wan = (item.get("price_ntd") or 0) / 10000
    if price_wan and price_wan > t["max_total_price_wan"]:
        return True, "price_too_high"

    bld = item.get("building_area_ping")
    if price_wan and bld and (price_wan / bld) > t["max_price_per_building_ping_wan"]:
        return True, "building_ping_too_high"

    land = item.get("land_area_ping")
    if price_wan and land and (price_wan / land) > t["max_price_per_land_ping_wan"]:
        return True, "land_ping_too_high"

    if land is not None and 0 < land < t["min_land_ping"]:
        return True, "land_too_small"

    return False, ""


# ── 欄位分類（merge 時用） ────────────────────────────────────────────────────
# 每人自己的設定：不存中央 properties，改存 users/{uid}/watchlist/{source_id}
USER_OVERRIDE_FIELDS = {
    "road_width_m_override", "new_house_price_wan_override", "desired_price_wan",
    "bonus_weishau", "bonus_dugen", "rebuild_coeff",
    "floor_premium", "zoning_ratios",
    "deleted", "note", "tags",
    "added_at", "last_viewed_at",
}
PREFER_NEW_FIELDS = {
    "score_total", "score_age", "score_far", "score_land", "score_tod",
    "score_road", "score_consolidation",
    "ai_analysis", "ai_recommendation", "ai_reason",
    "renewal_type", "renewal_bonus_rate", "renewal_new_area_ping",
    "renewal_value_ntd", "renewal_profit_ntd",
    "zoning_lookup_at", "zoning_source", "zoning_source_url",
    "zoning_candidates", "zoning_error",
    "address", "address_inferred", "address_inferred_confidence",
    "address_inferred_candidates",
    "image_url", "list_rank",
    # 注意：scraped_at / scrape_session_at 不放這裡 — 它們是「物件第一次入庫」timestamp，
    # frontend 排序用 _added_at = scraped_at，reanalyze 時刷新會讓既有 doc 跑到列表最前面像新物件。
    # 移到 default 走 "preserve old"。
    "published_at", "updated_at", "title", "deep_analysis_done",
    "screenshot_cadastral", "screenshot_zoning", "screenshot_renewal",
    "analysis_status", "analysis_completed_at",
    "is_remote_area",
    "unsuitable_for_renewal", "unsuitable_reason",
    "is_basement",
}
CONFLICT_TRACK_FIELDS = {
    "building_age", "building_area_ping", "land_area_ping",
    "total_floors", "floor", "zoning",
}


def merge_watchlist_with_central(central: dict, watch: dict) -> dict:
    """
    讀取 endpoint 用：把中央 properties 的共用事實 + 使用者 watchlist overrides 合併。
    watchlist 任一欄位有值（非 None）就蓋掉 central 對應欄位；其他保留 central。
    """
    if not watch:
        return dict(central)
    out = dict(central)
    for k, v in watch.items():
        if v is not None:
            out[k] = v
    return out


def merge_property_doc(old: dict, new: dict) -> tuple[dict, list]:
    """
    依欄位類型合併新舊 doc，回傳 (merged_doc, newly_added_conflicts_list)。
    分類規則：
      USER_OVERRIDE → 永遠保留舊（舊空才補）
      price_ntd → 新值蓋過 + 舊值寫進 price_history
      CONFLICT_TRACK → 舊空才補；已有且與新不同 → 記到 field_conflicts，保留舊
      PREFER_NEW → 取新
      其他 → 舊空才補
    """
    merged = dict(old)
    conflicts_log = dict(old.get("field_conflicts") or {})
    newly_added = []
    now_iso = now_tw_iso()

    # 這幾個欄位是「reset-on-reanalyze」類型 — 新分析沒給值代表「本次判定無結果」，
    # 必須清掉舊錯誤值（例如之前 LVR 誤反推的 address_inferred 或 arcgis 誤查的 zoning_original）。
    RESET_ON_REANALYZE = {
        "address", "address_inferred",   # 位址若新抓不到、就清掉，不保留舊錯值
        "address_inferred_confidence", "address_inferred_candidates",
        "address_inferred_candidates_detail",   # 帶地坪的候選清單
        "address_road_fixed",            # 路名修正紀錄（Claude fuzzy），重抓若不再修正就要清掉
        "address_suspicious", "address_suspicious_reason",   # 路名可疑標記
        "land_area_inconsistent",   # LVR 地坪分散警告；重抓若改判一致就要清掉
        "zoning_original",   # 只在特定專用區才有值；歷史錯誤查詢會殘留
        "road_width_unknown",   # 路寬不明標記；重抓若能對上 GeoServer 就要清掉
        "regeocode_failed", "regeocode_failed_addr",   # re-geocode 失敗標記；重抓成功就要清掉
    }

    for k, v_new in new.items():
        v_old = old.get(k)
        # sources / source_keys 由 caller (api/app.py) 明確處理（append/remove），merge 不動
        if k in ("sources", "source_keys"):
            continue
        # reset 類：允許 None 覆寫舊值
        if k in RESET_ON_REANALYZE:
            merged[k] = v_new
            continue
        # 其他欄位：新值 None/空 就跳過（保留舊值，如 scrape_session_at/list_rank）
        if v_new in (None, "", [], {}):
            continue

        if k in USER_OVERRIDE_FIELDS:
            if v_old in (None, "", [], {}):
                merged[k] = v_new
        elif k == "price_ntd":
            if v_old and abs((v_new or 0) - (v_old or 0)) > 1:
                hist = list(old.get("price_history") or [])
                hist.append({"price": v_old, "scraped_at": old.get("scraped_at")})
                merged["price_history"] = hist
                merged["is_price_changed"] = True
            merged[k] = v_new
        elif k in CONFLICT_TRACK_FIELDS:
            if v_old in (None, "", [], {}):
                merged[k] = v_new
            elif v_old != v_new:
                conflicts_log[k] = {"old": v_old, "new": v_new, "at": now_iso}
                newly_added.append(k)
        elif k in PREFER_NEW_FIELDS:
            merged[k] = v_new
        else:
            if v_old in (None, "", [], {}):
                merged[k] = v_new

    if newly_added:
        merged["field_conflicts"] = conflicts_log
    return merged, newly_added


def doc_richness(doc: dict) -> int:
    """資料豐富度分數（非 None / 非 0 / 非 '' 的關鍵欄位數量）。"""
    keys = [
        "land_area_ping", "building_age", "address",
        "latitude", "longitude", "nearest_mrt",
        "zoning", "address_probable", "image_url",
    ]
    return sum(1 for k in keys if doc.get(k) not in (None, "", 0))


def make_minimal_doc(
    item: dict,
    lat=None,
    lng=None,
    nearest_mrt=None,
    mrt_dist=None,
    mrt_exit=None,
    land_sqm=None,
    skip_reason: str = "",
    doc_id: Optional[str] = None,
) -> dict:
    """
    跳過 AI 分析的物件：只存基本資料（5168 + TCD + Claude 都不跑）。
    使用者在前端按「分析」按鈕後會升級為 full doc。
    """
    if doc_id is None:
        from database.db import gen_dated_id
        doc_id = gen_dated_id(item.get("scrape_session_at"))
    source_name = item.get("source", "591")
    source_id_raw = item.get("source_id") or ""
    site_id = source_id_raw.split("_", 1)[1] if source_id_raw.startswith(f"{source_name}_") else source_id_raw
    sources_arr = [{
        "name": source_name,
        "source_id": site_id,
        "url": item.get("url"),
        "added_at": now_tw_iso(),
        "alive": True,
    }]
    return {
        "id": doc_id,
        # sources[] 是來源唯一真相；source_keys[] 是平面索引給 Firestore array_contains 用
        "sources": sources_arr,
        "source_keys": compute_source_keys(sources_arr),
        "archived": False,        # 新建/重抓物件一律不是 archived
        "image_url": item.get("image_url"),
        "scraped_at": now_tw_iso(),
        "published_at": _parse_published_at(item.get("_published_text"))
                        or _proxy_published_at(item),
        "updated_at": _parse_published_at(item.get("_updated_text")),    # 591 詳情頁「最後更新」
        "list_rank": item.get("list_rank"),
        "scrape_session_at": item.get("scrape_session_at"),
        "title": item.get("title"),
        "city": item.get("city"),
        "district": item.get("district"),
        "address": item.get("address"),
        "building_type": item.get("building_type"),
        "total_floors": item.get("total_floors"),
        "floor": item.get("floor"),
        "building_age": item.get("building_age"),
        "building_age_source": item.get("building_age_source"),
        "building_age_completed_year": item.get("building_age_completed_year"),
        "building_area_ping": item.get("building_area_ping"),
        "land_area_ping": item.get("land_area_ping"),
        "land_area_sqm": land_sqm,
        "price_ntd": item.get("price_ntd"),
        "price_per_ping": item.get("price_per_ping"),
        "latitude": lat,
        "longitude": lng,
        "source_latitude": item.get("source_latitude"),
        "source_longitude": item.get("source_longitude"),
        "nearest_mrt": nearest_mrt,
        "nearest_mrt_dist_m": mrt_dist,
        "nearest_mrt_exit": mrt_exit,   # 出口編號（e.g. "2"）；單出口站為 None
        # 分析相關欄位全 null，跳過的記錄不計算
        "score_total": None,
        "renewal_type": None,
        "new_house_price_wan_override": None,
        "ai_analysis": None,
        "ai_recommendation": None,
        "ai_reason": None,
        "is_foreclosure": False,
        "foreclosure_reasons": None,
        "is_remote_area": False,
        "analysis_status": "pending",
        "skip_reason": skip_reason,
        "deep_analysis_done": False,
        "is_price_changed": False,
        "price_history": [],
        "zoning": None,
        "zoning_candidates": None,
        "zoning_source": None,
        "zoning_source_url": None,
        "zoning_lookup_at": None,
        "zoning_error": None,
        "address_probable": None,
        "address_inferred": None,
        "address_inferred_confidence": None,
        "address_inferred_candidates": None,
        "lvr_records": None,
        "road_width_name": None,
        "road_width_all": None,
    }


def _proxy_published_at(item: dict):
    """
    當 591 卡片沒有明確的「N天前」文字時，用爬蟲時的 591 排序位置做代理：
    scrape_session_at - list_rank 分鐘。位置越前（rank 越小）→ 時間戳越新。
    保證同批內排序與 591 一致，且不同批之間新批永遠壓過舊批。
    """
    session_at = item.get("scrape_session_at")
    rank = item.get("list_rank")
    if not session_at or rank is None:
        return None
    try:
        ts = datetime.fromisoformat(session_at) - timedelta(minutes=int(rank))
        return ts.isoformat()
    except Exception:
        return None


def _parse_published_at(text: str):
    if not text:
        return None
    now = now_tw().replace(tzinfo=None)   # 591 "3 天前" 相對敘述以台北日期為基準
    if any(k in text for k in ("今天", "剛剛", "小時前", "分鐘前")):
        return now.isoformat()
    if "昨天" in text:
        return (now - timedelta(days=1)).isoformat()
    m = re.search(r"(\d+)\s*天前", text)
    if m:
        return (now - timedelta(days=int(m.group(1)))).isoformat()
    m = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", text)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}T00:00:00"
    return None
