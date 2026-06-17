"""Unified property dedup rule — single source of truth for `is_same_property`.

3 個現有 dup wrapper (find_cross_source_duplicate / find_duplicate / _dup_key)
保留各自的 lookup 機制 (Firestore query / 線性 scan / hash bucket) 為效能優化，
但「最終判定兩個物件是不是同戶」一律呼叫本檔的 `is_same_property`。

Rule spec (5 conditions, all required):
  1. district 完全相同
  2. price_ntd exact integer ==
  3. building_area_ping diff <= 0.01
  4. road_base (含段不含巷) ==; lane 兩邊都有就比，任一邊空略過
  5. floor 兩邊都有值且相等 (嚴格版：任一邊空 → 不同戶)

LVR triangulate (analysis/lvr_index.py) 是另一條 path，本檔不影響它。
"""
from __future__ import annotations
import re
from typing import Optional

# 1. address normalize: strip 市 + 區 prefix 再抽 road
_CITY_PREFIX_RE = re.compile(
    r"^(台北市|臺北市|新北市|桃園市|台中市|臺中市|高雄市|台南市|臺南市|基隆市|新竹市)"
)
_DISTRICT_PREFIX_RE = re.compile(r"^[一-龥]{1,3}區")

# 2. road_base: 路/街/大道 + optional 段 (但不含巷)
_ROAD_BASE_RE = re.compile(
    r"[一-龥]{1,5}(?:路|街|大道)(?:[一二三四五六七八九十]段)?"
)
# 3. lane: 緊接在 road_base 之後的 N巷
_LANE_RE = re.compile(r"\d+巷")


def _strip_addr_prefixes(addr: str) -> str:
    """剝掉「台北市」「大安區」等前綴，留下純路名段巷號樓部分。"""
    if not addr:
        return ""
    a = _CITY_PREFIX_RE.sub("", addr.strip())
    a = _DISTRICT_PREFIX_RE.sub("", a)
    return a


def extract_road_parts(addr: str) -> tuple[Optional[str], Optional[str]]:
    """從 address 抽出 (road_base, lane)。

    road_base: 「路+街+大道」+ optional「N段」 (不含巷)
    lane: 緊接 road_base 之後的「N巷」 (optional)

    抽不出來 road_base → 回 (None, None)。
    """
    if not addr:
        return (None, None)
    a = _strip_addr_prefixes(addr)
    m = _ROAD_BASE_RE.search(a)
    if not m:
        return (None, None)
    road_base = m.group(0)
    after = a[m.end():]
    lm = _LANE_RE.search(after)
    lane = lm.group(0) if lm else None
    return (road_base, lane)


def _normalize_floor(f) -> Optional[str]:
    """Floor 可能是 int / str / None / 空字串。回 normalized str 或 None。"""
    if f is None:
        return None
    s = str(f).strip()
    return s if s else None


_MAX_SAME_PROPERTY_DISTANCE_M = 300.0


def _haversine_m(la1, ln1, la2, ln2) -> float:
    """兩 (lat,lng) 距離（公尺）— 標準 haversine 公式。
    任一輸入 None / 非 number → return inf (caller 用 inf 判定為「無法距離防線」)。"""
    import math
    try:
        la1, ln1, la2, ln2 = float(la1), float(ln1), float(la2), float(ln2)
    except (TypeError, ValueError):
        return float("inf")
    R = 6371000.0
    p1, p2 = math.radians(la1), math.radians(la2)
    dlat = math.radians(la2 - la1)
    dlon = math.radians(ln2 - ln1)
    aa = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(aa), math.sqrt(1 - aa))


def is_same_property(a: dict, b: dict) -> bool:
    """兩個物件 (a, b dicts with 標準欄位) 是否視為同一個物理單位？

    用於 listing-vs-listing dup detection。**不適用** listing-vs-LVR (LVR
    比對在 analysis/lvr_index.py，不比 price)。

    任一條件不通過 → 返 False (寧可漏 dup 也不誤 merge)。

    rule 6 (距離防線)：兩 doc 都有 lat/lng 且 haversine 距離 > 300m → False。
    解 lane optional 規則 (rule 4 「lane 一邊空略過」) 對「路門牌號 vs 同路巷內地址」
    的盲點，例：松江路100巷15號 vs 松江路221號 兩座標差 837m 但 5 條規則全過。
    300m 上限是保守值：同棟 cross-source 座標差通常 < 30m (591 偏移 100-300m vs
    永慶/信義 精準座標)，300m 仍能 catch 不同棟。任一邊無座標 → 規則跳過。"""
    # 1. district 必比 (任一邊空 → 不同)
    da, db = a.get("district"), b.get("district")
    if not da or not db or da != db:
        return False

    # 2. price exact (任一邊空 → 不同)
    pa, pb = a.get("price_ntd"), b.get("price_ntd")
    if pa is None or pb is None:
        return False
    try:
        if int(pa) != int(pb):
            return False
    except (TypeError, ValueError):
        return False

    # 3. building_area_ping diff <= 0.01 (任一邊空 → 不同)
    # 加 1e-6 epsilon 容忍浮點精度誤差 (例 abs(30.00-30.01) 實際 0.01000...0009)
    ba, bb = a.get("building_area_ping"), b.get("building_area_ping")
    if ba is None or bb is None:
        return False
    try:
        if abs(float(ba) - float(bb)) > 0.01 + 1e-6:
            return False
    except (TypeError, ValueError):
        return False

    # 4. road_base == 必比；lane 兩邊都有就比、任一邊空略過
    road_a, lane_a = extract_road_parts(a.get("address") or "")
    road_b, lane_b = extract_road_parts(b.get("address") or "")
    if not road_a or not road_b:
        return False
    if road_a != road_b:
        return False
    if lane_a and lane_b and lane_a != lane_b:
        return False

    # 5. floor 比對：
    #   - 兩邊都有 → 嚴格相等（虎林街教訓：3F vs 2F 不誤合）
    #   - 一邊有一邊空 → 不同（避免「3F」vs「整棟」誤認）
    #   - 兩邊都空 → 過（591 整棟透天「1~4F」物件沒推單一 floor，但同棟物件其他欄位已嚴格比對）
    fa = _normalize_floor(a.get("floor"))
    fb = _normalize_floor(b.get("floor"))
    if fa and fb:
        if fa != fb:
            return False
    elif fa or fb:
        return False

    # 6. 距離防線：兩邊都有座標時 > 300m → 不同物理單位（rule 4 lane optional 的補強）
    #   優先用 source_latitude (來源原生精準座標)；fallback 才用 latitude
    #   (pipeline 算過的 latitude 可能是 LVR triangulate 推錯地址再 geocode 的錯位置)
    #   任一邊缺座標 → 跳過此條，仍信任 1-5 結果
    la1 = a.get("source_latitude") or a.get("latitude")
    ln1 = a.get("source_longitude") or a.get("longitude")
    la2 = b.get("source_latitude") or b.get("latitude")
    ln2 = b.get("source_longitude") or b.get("longitude")
    if la1 and ln1 and la2 and ln2:
        if _haversine_m(la1, ln1, la2, ln2) > _MAX_SAME_PROPERTY_DISTANCE_M:
            return False

    return True


def dup_bucket_key(d: dict) -> tuple:
    """Coarse hash bucket key — 給 batch enrich 階段 O(1) 查 dup 候選用。

    同 bucket 的 item 是 dup **候選** — 呼叫端必須再用 is_same_property 做 final 確認。
    bucket precision 比 is_same_property 寬鬆 (0.1 坪 + 1 萬 + road_base) 避免每張卡
    都自成一 bucket 導致 hash lookup 失去意義；最終判定仍由 is_same_property 把關。
    """
    addr = d.get("address") or ""
    a = _strip_addr_prefixes(addr)
    m = _ROAD_BASE_RE.search(a)
    road_base = m.group(0) if m else ""
    bld = round((d.get("building_area_ping") or 0) * 10) / 10   # 0.1 坪精度
    price_wan = round((d.get("price_ntd") or 0) / 10000)        # 1 萬精度
    floor = _normalize_floor(d.get("floor")) or ""
    return (d.get("district") or "", road_base, bld, price_wan, floor)
