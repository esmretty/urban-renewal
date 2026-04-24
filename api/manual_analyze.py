"""
手動輸入地址送出分析 — 驗證邏輯與 LVR 比對。

流程（對照使用者規格）：
  Step 1: 地址有到「號」且存在（Geocode ROOFTOP 或 LVR 有紀錄）
          → 整理地址 → LVR 查 → 有就比對建坪/地坪差 > 0.01 坪 則回報 mismatch
  Step 2: 地址有到「號」但不存在 → 提供最接近的地址建議；找不到就回報錯誤
  Step 3: 地址沒到「號」但有建坪/地坪 → 用地址（到路/巷）+ 坪數 去 LVR 交叉比對
  Step 4: 地址沒到「號」且建坪/地坪都沒填 → 錯誤
"""
import hashlib
import logging
import re
import sqlite3
from datetime import datetime, timezone
from difflib import get_close_matches
from typing import Optional

from analysis.geocoder import geocode_address, geocode_with_district
from analysis.lvr_index import init_db
from analysis.building_info import query_building_floors

logger = logging.getLogger(__name__)

AREA_TOLERANCE_PING = 0.01   # 使用者規格：誤差超過 0.01 坪即視為不同物件


# ─────────────────────────────────────────────────────────
# 地址 normalize
# ─────────────────────────────────────────────────────────

def _to_halfwidth(s: str) -> str:
    """全形 → 半形（數字 / 英文 / 符號）。"""
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


_CN_DIGIT = {"一": 1, "二": 2, "兩": 2, "三": 3, "四": 4, "五": 5,
             "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}


def _cn_num_to_int(s: str) -> Optional[int]:
    """支援 1~99：一、十、十一、二十、二十三…"""
    if not s:
        return None
    if s == "十":
        return 10
    if len(s) == 1:
        return _CN_DIGIT.get(s)
    if s[0] == "十":              # 十一、十二…十九
        ones = _CN_DIGIT.get(s[1:], 0)
        return 10 + ones if ones else None
    if "十" in s:                  # 二十、二十三、九十九
        parts = s.split("十")
        tens = _CN_DIGIT.get(parts[0])
        if tens is None:
            return None
        ones = _CN_DIGIT.get(parts[1], 0) if len(parts) > 1 and parts[1] else 0
        return tens * 10 + ones
    # 純漢字組合（不含十）→ 不合理，當單字處理
    return _CN_DIGIT.get(s)


_DIGIT_TO_CN = {"1": "一", "2": "二", "3": "三", "4": "四", "5": "五",
                "6": "六", "7": "七", "8": "八", "9": "九", "10": "十"}


def normalize_address(s: str) -> str:
    """
    全形→半形、去空白、剝除樓層、段格式統一（LVR 存法用漢字「一段」「二段」...）。
    支援樓層：4F、4樓、五樓、五F、十二樓、二十F、4F-1、4樓之1…
    支援段：1段、2段、３段 → 一段、二段、三段
    """
    if not s:
        return ""
    s = _to_halfwidth(s)
    s = s.replace(" ", "").replace("　", "").strip()
    s = s.replace("臺北", "台北")
    # 去開頭的「縣市+區」前綴（LVR 有時會含，使用者輸入通常沒有）
    s = re.sub(r"^(台北市|新北市|桃園市|基隆市)", "", s)
    s = re.sub(r"^[\u4e00-\u9fa5]{1,4}區", "", s)
    # 段：阿拉伯 → 漢字（1段→一段 ... 10段→十段；其餘保持）
    s = re.sub(r"(\d+)段",
               lambda m: _DIGIT_TO_CN.get(m.group(1), m.group(1)) + "段",
               s)
    # 剝除尾端樓層（阿拉伯或漢字 + 可選「之X」「-X」）
    s = re.sub(
        r"(?:\d+|[一二兩三四五六七八九十]+)[樓FfＦｆ](?:[之\-]?[\d一二兩三四五六七八九十]+)?$",
        "",
        s,
    )
    # 去掉尾端的「之X」或「-X」分號（LVR 有「XX號之三」這種）
    s = re.sub(r"[之\-][\d一二兩三四五六七八九十]+$", "", s)
    return s.strip()


_FLOOR_RE = re.compile(
    r"(\d+|[一二兩三四五六七八九十]+)[樓FfＦｆ](?:[之\-]?[\d一二兩三四五六七八九十]+)?$"
)


def extract_floor(addr: str) -> Optional[int]:
    """抽地址尾端的樓層數字（1F/1樓/一樓/十二樓 → 1, 1, 1, 12）。無樓層回 None。"""
    if not addr:
        return None
    s = _to_halfwidth(addr).replace(" ", "").replace("　", "")
    m = _FLOOR_RE.search(s)
    if not m:
        return None
    raw = m.group(1)
    if raw.isdigit():
        return int(raw)
    return _cn_num_to_int(raw)


def has_number(addr: str) -> bool:
    """地址是否有到「號」層級。"""
    return "號" in (addr or "")


def extract_road_seg(addr: str) -> Optional[str]:
    """從地址抽出 road_seg（到「段」為止）；若無段就到「路/街/大道」。
    例如：景興路一段 → '景興路一段'；景興路 → '景興路'。
    """
    if not addr:
        return None
    m = re.search(r"([\u4e00-\u9fa5]+(?:路|街|大道))([一二三四五六七八九十]段)?", addr)
    if not m:
        return None
    return (m.group(1) + (m.group(2) or ""))


# ─────────────────────────────────────────────────────────
# LVR 查詢
# ─────────────────────────────────────────────────────────

def _strip_section(s: str) -> str:
    """去掉路名後的「X段」以做 loose 比對（景興路一段96巷 ↔ 景興路96巷）。"""
    return re.sub(r"((?:路|街|大道))[一二兩三四五六七八九十]段", r"\1", s)


def lvr_lookup_exact(city: str, district: str, address: str) -> list[dict]:
    """
    用完整地址在 LVR 裡做比對（normalize 後）。
    兩階段：
      1) 嚴格：normalize 後完全相等（景興路一段96巷10號 = 景興路一段96巷10號）
      2) Loose：若嚴格找不到 → 去掉「X段」再比對
         （使用者「景興路96巷10號」能對上 LVR「景興路一段96巷10號」）
    回傳所有符合的交易紀錄。
    """
    conn = init_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT address, area_ping, land_ping, year_completed, txn_date,
               price_total, zone_urban, building_type, total_floors, floor
        FROM lvr
        WHERE city = ? AND district = ?
        """,
        (city, district),
    )
    rows = cur.fetchall()
    conn.close()

    target_strict = normalize_address(address)
    target_loose = _strip_section(target_strict)

    def _mk(r):
        return {
            "address": r[0], "area_ping": r[1], "land_ping": r[2],
            "year_completed": r[3], "txn_date": r[4], "price_total": r[5],
            "zone_urban": r[6], "building_type": r[7],
            "total_floors": r[8], "floor": r[9],
        }

    strict = [_mk(r) for r in rows if normalize_address(r[0]) == target_strict]
    if strict:
        return strict
    # fallback：loose (ignore 段)
    loose = [_mk(r) for r in rows
             if _strip_section(normalize_address(r[0])) == target_loose]
    return loose


def lvr_fuzzy_suggestions(city: str, district: str, address: str, limit: int = 5) -> list[str]:
    """
    Step 2 用：地址不存在時，從 LVR 找出最接近的地址建議。
    先以 district + 路/段 縮範圍，再用 difflib 比對整串。
    """
    road_seg = extract_road_seg(address)
    if not road_seg:
        return []
    conn = init_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT address
        FROM lvr
        WHERE city = ? AND district = ? AND road_seg LIKE ?
        """,
        (city, district, f"%{road_seg}%"),
    )
    addrs = [r[0] for r in cur.fetchall() if r[0]]
    conn.close()
    if not addrs:
        return []
    target = normalize_address(address)
    normalized = {normalize_address(a): a for a in addrs}
    matches = get_close_matches(target, list(normalized.keys()), n=limit, cutoff=0.55)
    return [normalized[m] for m in matches]


def lvr_lookup_by_area(
    city: str,
    district: str,
    address: str,
    building_area_ping: Optional[float] = None,
    land_area_ping: Optional[float] = None,
) -> list[dict]:
    """
    Step 3 用：地址未到「號」，用 road_seg + 建坪/地坪 做交叉比對。
    差 > 0.01 坪視為不同物件。
    """
    road_seg = extract_road_seg(address)
    if not road_seg:
        return []
    conn = init_db()
    cur = conn.cursor()
    sql = """
        SELECT address, area_ping, land_ping, year_completed, txn_date,
               price_total, zone_urban, building_type, total_floors, floor
        FROM lvr
        WHERE city = ? AND district = ? AND road_seg LIKE ?
    """
    params = [city, district, f"%{road_seg}%"]
    if building_area_ping is not None:
        sql += " AND area_ping BETWEEN ? AND ?"
        params += [building_area_ping - AREA_TOLERANCE_PING,
                   building_area_ping + AREA_TOLERANCE_PING]
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    records = []
    for r in rows:
        rec = {
            "address": r[0], "area_ping": r[1], "land_ping": r[2],
            "year_completed": r[3], "txn_date": r[4], "price_total": r[5],
            "zone_urban": r[6], "building_type": r[7],
            "total_floors": r[8], "floor": r[9],
        }
        if land_area_ping is not None and rec["land_ping"] is not None:
            if abs(rec["land_ping"] - land_area_ping) > AREA_TOLERANCE_PING:
                continue
        records.append(rec)
    return records


# ─────────────────────────────────────────────────────────
# 差距比對
# ─────────────────────────────────────────────────────────

def area_mismatch(user_val: Optional[float], lvr_val: Optional[float]) -> bool:
    """使用者輸入與 LVR 值差 > 0.01 坪 → True。任一為 None 不算 mismatch。"""
    if user_val is None or lvr_val is None:
        return False
    return abs(user_val - lvr_val) > AREA_TOLERANCE_PING


# ─────────────────────────────────────────────────────────
# source_id 生成
# ─────────────────────────────────────────────────────────

def infer_building_type_from_lvr(lvr_type: Optional[str]) -> Optional[str]:
    """LVR 的 building_type 欄通常是「公寓(5樓含以下無電梯)」「住宅大樓(11層含以上有電梯)」之類，萃出我們用的類別。"""
    if not lvr_type:
        return None
    t = lvr_type
    if "大樓" in t or "華廈" in t:
        return "大樓" if "大樓" in t else "華廈"
    if "公寓" in t:
        return "公寓"
    if "透天" in t:
        return "透天厝"
    if "店面" in t:
        return "店面"
    return None


def infer_building_type_from_floors(floors: Optional[int], structure: Optional[str]) -> str:
    """沒有 LVR 時用 GISDB 樓數粗估：≥11→大樓、6-10→華廈、≤5→公寓。鐵皮/磚造→公寓兜底。"""
    if floors is None:
        return "公寓"
    if floors >= 11:
        return "大樓"
    if floors >= 6:
        return "華廈"
    return "公寓"


def make_manual_source_id(city: str, district: str, address: str) -> str:
    """
    地址 hash 固定（同地址 id 一致）→ 方便覆寫舊紀錄；不含時間戳避免碎片化。
    若要強制新增，由 endpoint 用 `force=True` 或加後綴處理。
    """
    norm = f"{city}|{district}|{normalize_address(address)}"
    h = hashlib.sha1(norm.encode("utf-8")).hexdigest()[:10]
    return f"manual_{h}"


# ─────────────────────────────────────────────────────────
# 主驗證函式（給 endpoint 用）
# ─────────────────────────────────────────────────────────

def validate_manual_input(
    *,
    city: str,
    district: str,
    address: str,
    building_area_ping: Optional[float] = None,
    land_area_ping: Optional[float] = None,
    price_wan: Optional[float] = None,
    use_source: str = "auto",   # auto / user / lvr
) -> dict:
    """
    驗證流程：回傳 dict，其中 `status` 欄為下列之一：
      - "ok"          : 可以進分析；附 normalized item
      - "error"       : 輸入不合格；附 error 說明
      - "not_found"   : 地址不存在；附 suggestions
      - "lvr_mismatch": LVR 存在但建/地坪對不上；附 lvr_record, user_input
    """
    # 先從原輸入抽樓層（normalize 會剝除，在之前抓）
    floor_num = extract_floor(address)
    addr = normalize_address(address)
    if not addr:
        return {"status": "error", "error": "請輸入地址"}

    road_seg = extract_road_seg(addr)
    if not road_seg:
        return {"status": "error", "error": "地址至少要到「路」（例如「景興路」或「景興路一段」）"}

    if not city or not district:
        return {"status": "error", "error": "請選擇縣市與行政區"}

    # 限定在設定的目標區域內
    from config import is_target_region, TARGET_REGIONS
    if not is_target_region(city, district):
        allowed = ", ".join(
            f"{c}（{'/'.join(v['districts'].keys())}）"
            for c, v in TARGET_REGIONS.items()
        )
        return {
            "status": "error",
            "error": f"{city}{district} 不在分析範圍內。目前僅支援：{allowed}",
        }

    # 把樓層加回地址（pipeline / 顯示用）；內部比對/查詢仍用 normalize 後的 addr
    display_addr = addr + (f"{floor_num}樓" if floor_num else "")
    # DB 一律存純地址（不含 city/district 前綴）
    from database.models import strip_region_prefix
    display_addr = strip_region_prefix(display_addr, city, district)

    normalized_item = {
        "city": city,
        "district": district,
        "address": display_addr,      # 純地址（DB 欄位約定：不含 city/district）
        "floor": floor_num,           # 關鍵：1F 會觸發 20% 樓層加成
        "building_area_ping": building_area_ping,
        "land_area_ping": land_area_ping,
        "price_ntd": int(price_wan * 10000) if price_wan else None,
    }

    if has_number(addr):
        # Step 1 + Step 2：先 geocode 驗地址存在 + district 正確，之後才 LVR
        geo_candidates = geocode_with_district(addr)

        if not geo_candidates:
            # geocode 也找不到 → fallback 查 LVR 有沒有類似 address（fuzzy），否則叫用戶重輸
            suggestions = lvr_fuzzy_suggestions(city, district, addr)
            if not suggestions:
                return {
                    "status": "error",
                    "error": f"地址「{addr}」找不到對應房屋，請重新輸入（含正確路名與門牌）。",
                }
            return {
                "status": "not_found",
                "error": f"地址「{addr}」找不到，請從下列建議選擇或修正後重送：",
                "suggestions": suggestions,
            }

        # 過濾出 city+district 都對得上的候選
        matched = [g for g in geo_candidates if g.get("city") == city and g.get("district") == district]
        if not matched:
            # 若所有候選都「非台北市」→ 目前不支援，直接拒絕（不列選單）
            all_cities = {g.get("city") for g in geo_candidates if g.get("city")}
            if all_cities and "台北市" not in all_cities:
                _city_txt = "、".join(sorted(all_cities))
                return {
                    "status": "error",
                    "error": f"「{addr}」位於 {_city_txt}，目前僅支援台北市分析。",
                }
            # district 對不上 → 提供修正建議（含正確的 city/district/address）
            # 用原始輸入地址（含樓層）給前端，用戶選修正後重送時樓層才不會被丟掉
            addr_with_floor = addr + (f"{floor_num}樓" if floor_num else "")
            # 只保留台北市候選（新北市候選濾掉，不給用戶選）
            structured = [
                {
                    "city": g.get("city") or "",
                    "district": g.get("district") or "",
                    "address": addr_with_floor,
                    "formatted": f"{g.get('city') or ''}{g.get('district') or ''}{addr_with_floor}",
                }
                for g in geo_candidates
                if g.get("city") == "台北市" and g.get("district")
            ]
            if not structured:
                return {
                    "status": "error",
                    "error": f"「{addr}」地址可定位，但不在台北市範圍內（目前僅支援台北市），請檢查地址或重新輸入。",
                }
            return {
                "status": "district_mismatch",
                "error": f"您選的是「{city}{district}」，但「{addr}」實際位於下列區（請選擇正確的區或取消）：",
                "candidates": structured,
            }

        # district 正確 → 用此座標；LVR 用修正後的 city/district 查
        coord = (matched[0]["lat"], matched[0]["lng"])
        lvr_rows = lvr_lookup_exact(city, district, addr)

        # ── 推 building_type + total_floors ──
        # 1) 優先 LVR 的 building_type
        inferred_type = None
        if lvr_rows:
            inferred_type = infer_building_type_from_lvr(lvr_rows[0].get("building_type"))
            if lvr_rows[0].get("total_floors"):
                normalized_item.setdefault("total_floors", lvr_rows[0]["total_floors"])
        # 2) fallback：GISDB 建物層數
        if coord:
            try:
                bldg = query_building_floors(coord[0], coord[1])
                if bldg:
                    normalized_item.setdefault("total_floors", bldg["floors"])
                    if not inferred_type:
                        inferred_type = infer_building_type_from_floors(bldg["floors"], bldg["structure"])
            except Exception as e:
                logger.warning("GISDB query failed: %s", e)
        if inferred_type:
            normalized_item["building_type"] = inferred_type

        # ── 大樓 fallback 檢測 ──
        # 只靠「個別物件的強信號」，不做同路段擴散（會誤殺附近的老公寓）
        is_highrise = inferred_type == "大樓"
        if floor_num and floor_num >= 11:
            is_highrise = True           # 樓層 ≥11 必是大樓（住宅大樓法定門檻）
        if "大樓" in address:
            is_highrise = True           # 地址字串明示大樓

        if is_highrise:
            return {
                "status": "error",
                "error": "此地址為「大樓」類型（樓層高、整合困難），不在分析對象內。",
            }

        # 存在 — 若 LVR 有紀錄則比對坪數
        if lvr_rows:
            lvr = lvr_rows[0]   # 取最近一筆（lvr_lookup_exact 未排序，但同地址紀錄一致）
            bld_mis = area_mismatch(building_area_ping, lvr["area_ping"])
            land_mis = area_mismatch(land_area_ping, lvr["land_ping"])
            if (bld_mis or land_mis) and use_source == "auto":
                return {
                    "status": "lvr_mismatch",
                    "error": "您輸入的坪數與 LVR 登錄不符（差距 > 0.01 坪）。請選擇要用哪一組資料。",
                    "lvr_record": lvr,
                    "user_input": {
                        "building_area_ping": building_area_ping,
                        "land_area_ping": land_area_ping,
                    },
                    "mismatch_fields": [
                        *(["building_area_ping"] if bld_mis else []),
                        *(["land_area_ping"] if land_mis else []),
                    ],
                }
            # 選用哪個
            source_rec = lvr if use_source == "lvr" else None
            if source_rec:
                normalized_item["building_area_ping"] = source_rec["area_ping"]
                normalized_item["land_area_ping"] = source_rec["land_ping"]
                if source_rec.get("total_floors"):
                    normalized_item["total_floors"] = source_rec["total_floors"]
                if source_rec.get("floor"):
                    normalized_item["floor"] = source_rec["floor"]
                if source_rec.get("year_completed"):
                    normalized_item["building_age"] = datetime.now().year - source_rec["year_completed"]
                normalized_item["_lvr_matched"] = True
            else:
                # 用 user 值或 LVR 沒差距，可順便把 LVR 的額外欄位補上
                if not normalized_item.get("building_area_ping") and lvr.get("area_ping"):
                    normalized_item["building_area_ping"] = lvr["area_ping"]
                if not normalized_item.get("land_area_ping") and lvr.get("land_ping"):
                    normalized_item["land_area_ping"] = lvr["land_ping"]
                normalized_item["_lvr_matched"] = True

        return {"status": "ok", "item": normalized_item}

    # Step 3 + Step 4：地址沒到「號」
    if building_area_ping is None and land_area_ping is None:
        return {
            "status": "error",
            "error": "地址未填到「號」，則必須至少填寫建坪或地坪其中之一。",
        }

    matches = lvr_lookup_by_area(city, district, addr, building_area_ping, land_area_ping)
    if len(matches) == 1:
        m = matches[0]
        # LVR address 通常含「臺北市XX區」前綴，統一 strip 存純地址
        normalized_item["address"] = strip_region_prefix(m["address"], city, district)
        if not normalized_item.get("building_area_ping"):
            normalized_item["building_area_ping"] = m["area_ping"]
        if not normalized_item.get("land_area_ping"):
            normalized_item["land_area_ping"] = m["land_ping"]
        if m.get("year_completed"):
            normalized_item["building_age"] = datetime.now().year - m["year_completed"]
        if m.get("total_floors"):
            normalized_item["total_floors"] = m["total_floors"]
        bt = infer_building_type_from_lvr(m.get("building_type"))
        if bt:
            normalized_item["building_type"] = bt
        if bt == "大樓":
            return {
                "status": "error",
                "error": "此地址在 LVR 的類型為「大樓」，不在分析對象內。",
            }
        normalized_item["_lvr_matched"] = True
        return {"status": "ok", "item": normalized_item}
    elif len(matches) > 1:
        return {
            "status": "not_found",
            "error": f"路名 + 坪數在 LVR 有 {len(matches)} 筆符合，請從中選一個確切地址：",
            "suggestions": sorted({m["address"] for m in matches}),
        }
    else:
        # 沒 LVR 符合 → 照使用者輸入跑（路名+坪數也是合法輸入）
        return {"status": "ok", "item": normalized_item}
