"""
將下載的 LVR CSV 匯入 sqlite 作為查詢索引；提供 triangulate_address()。

索引策略：
  - 每筆 row 的 strong feature: (district, road_pattern, total_floors, area_ping_2dec, age_year)
  - 查詢時：先比對 district + road（LIKE prefix） + total_floors，
    再用建物坪數（權狀面積，精確到 0.01 坪）float match < 0.15 坪，
    再用屋齡 ±1 年微調
  - 剩 1 筆 → unique_match（高信心）
  - 剩 2-3 筆 → 用座標距離選最近那筆（中信心）
  - 0 或 >3 筆 → no_match
"""
import csv
import sqlite3
import re
import math
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
LVR_DIR = BASE_DIR / "data" / "lvr"
DB_PATH = BASE_DIR / "data" / "lvr_index.db"

# 1 m² = 0.3025 坪
M2_PER_PING = 3.30578
PING_PER_M2 = 1 / M2_PER_PING


# ── Sqlite schema ────────────────────────────────────────────────────────────

def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
    CREATE TABLE IF NOT EXISTS lvr (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        district TEXT,
        road TEXT,
        road_seg TEXT,
        address TEXT,
        total_floors INTEGER,
        floor INTEGER,
        area_m2 REAL,
        area_ping REAL,
        land_m2 REAL,
        land_ping REAL,
        year_completed INTEGER,
        building_type TEXT,
        txn_date TEXT,
        price_total INTEGER,
        zone_urban TEXT,
        source TEXT,
        note TEXT                    -- 備註（含特殊交易原因）
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS ix_lvr_lookup ON lvr(district, road_seg, total_floors)")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_lvr_area ON lvr(area_ping)")
    return conn


# ── CSV 解析 ─────────────────────────────────────────────────────────────────

# CSV 欄位英文鍵（LVR 實際 header 用中文，row 2 才是英文 key）
FIELD_MAP = {
    "鄉鎮市區": "district",
    "土地位置建物門牌": "address",
    "土地位置或建物門牌": "address",  # 預售屋版本
    "移轉層次": "floor_str",
    "總樓層數": "total_floors_str",
    "建物型態": "building_type",
    "建築完成年月": "year_completed_str",
    "建物移轉總面積平方公尺": "area_m2_str",
    "交易年月日": "txn_date",
    "總價元": "price_total_str",
    "都市土地使用分區": "zone_urban",
}


def _parse_floor_chinese(s: str) -> Optional[int]:
    """「三層」→ 3；「1F」→ 1。取第一個可辨識的整數。"""
    if not s:
        return None
    s = s.strip()
    digit_m = re.search(r"(\d+)", s)
    if digit_m:
        return int(digit_m.group(1))
    cn_map = {"一":1,"二":2,"三":3,"四":4,"五":5,"六":6,"七":7,"八":8,"九":9,"十":10,
              "十一":11,"十二":12,"十三":13,"十四":14,"十五":15,"十六":16,"十七":17,"十八":18,"十九":19,"二十":20,
              "廿":20,"卅":30}
    for k in sorted(cn_map, key=len, reverse=True):
        if k in s:
            return cn_map[k]
    return None


def _parse_roc_date(s: str) -> Optional[int]:
    """「0890303」→ 2000；只回西元年份。"""
    if not s:
        return None
    s = s.strip()
    # 通常 6-7 位數字，民國年占前 2-3 位
    digits = re.sub(r"\D", "", s)
    if len(digits) >= 5:
        # 前 len(digits)-4 位是年
        year_digits = digits[:len(digits) - 4]
        try:
            roc_y = int(year_digits)
            return 1911 + roc_y
        except Exception:
            return None
    return None


def _parse_total_price(s: str) -> Optional[int]:
    if not s:
        return None
    digits = re.sub(r"[^\d]", "", s)
    return int(digits) if digits else None


_ROADS_CACHE = {}   # (city, district) -> set[str]
def list_roads_in_district(city: str, district: str) -> list[str]:
    """回傳該行政區所有出現過的「路/街/大道」名稱（去重 + 排序）。
    用作 OCR 地址校正的閉集合 whitelist（給 Vision prompt 當錨）。"""
    if not city or not district:
        return []
    key = (city.strip(), district.strip())
    if key in _ROADS_CACHE:
        return _ROADS_CACHE[key]
    try:
        conn = init_db()
        rows = conn.execute(
            "SELECT DISTINCT road FROM lvr WHERE city LIKE ? AND district = ? AND road IS NOT NULL AND road != ''",
            (f"%{key[0]}%", key[1])
        ).fetchall()
        roads = sorted({r[0] for r in rows if r[0]})
    except Exception:
        roads = []
    _ROADS_CACHE[key] = roads
    return roads


def _extract_road_seg(address: str) -> tuple[str, str, Optional[str]]:
    """
    從完整門牌抽「路/街主幹」+「路/街+段」+「巷」。
    例：「臺北市大安區辛亥路三段157巷12弄4號」→ ("辛亥路", "辛亥路三段", "157巷")
    例：「臺北市中山區南京東路三段100號」→ ("南京東路", "南京東路三段", None)
    """
    if not address:
        return "", "", None
    s = re.sub(r"^(臺北市|台北市|新北市|台中市|臺中市|高雄市|桃園市)", "", address)
    s = re.sub(r"^[\u4e00-\u9fa5]{1,3}區", "", s)
    m = re.match(r"([\u4e00-\u9fa5]{1,5}(?:路|街|大道))([一二三四五六七八九十]段)?", s)
    if not m:
        return "", "", None
    road = m.group(1)
    road_seg = road + (m.group(2) or "")
    lane = None
    lane_m = re.search(r"(\d+巷)", s)
    if lane_m:
        lane = lane_m.group(1)
    return road, road_seg, lane


def import_csv(csv_path: Path, conn: sqlite3.Connection):
    """把一個 LVR CSV 檔匯入 sqlite。回傳 inserted count。"""
    if not csv_path.exists():
        return 0
    name = csv_path.name.lower()
    city = "台北市" if name.startswith("a_") else "新北市" if name.startswith("f_") else None
    source = "預售" if "_b.csv" in name else "買賣"
    if not city:
        return 0

    csv.field_size_limit(1024 * 1024)  # 有些備註欄超大
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        # LVR CSV 第一 row 是中文 header（DictReader 會當 keys），第二 row 是英文 key → 要跳過
        rows = list(reader)
    if not rows:
        return 0
    # 檢查：DictReader 的 fieldnames 應該已是第一 row 的中文欄；然後 rows[0] 可能是英文 header
    # Simpler: 依中文鍵讀 row，若第一筆值看起來是「The villages and towns urban district」這種英文
    # 就 skip 第一筆
    first = rows[0]
    def looks_english(r): return bool(re.match(r"^[A-Za-z\s]+$", r.get("鄉鎮市區", "") or ""))
    if looks_english(first):
        rows = rows[1:]

    def _fullwidth_to_half(s: str) -> str:
        return s.translate(str.maketrans("０１２３４５６７８９", "0123456789"))

    inserted = 0
    for r in rows:
        district = (r.get("鄉鎮市區") or "").strip()
        address = _fullwidth_to_half((r.get("土地位置建物門牌") or r.get("土地位置或建物門牌") or "").strip())
        if not address or not district:
            continue
        floor = _parse_floor_chinese(r.get("移轉層次", ""))
        total_floors = _parse_floor_chinese(r.get("總樓層數", ""))
        area_m2 = None
        try:
            area_m2 = float((r.get("建物移轉總面積平方公尺") or "").strip() or "0")
        except Exception:
            pass
        if not area_m2 or area_m2 < 5:
            continue
        area_ping = round(area_m2 * PING_PER_M2, 2)
        year_completed = _parse_roc_date(r.get("建築完成年月", ""))
        building_type = (r.get("建物型態") or "").strip()
        txn_date = (r.get("交易年月日") or "").strip()
        price_total = _parse_total_price(r.get("總價元", ""))
        zone_urban = (r.get("都市土地使用分區") or "").strip()
        road, road_seg, _ = _extract_road_seg(address)

        # 土地面積
        land_m2 = None
        try:
            land_m2 = float((r.get("土地移轉總面積平方公尺") or "").strip() or "0")
        except Exception:
            pass
        land_ping = round(land_m2 * PING_PER_M2, 2) if land_m2 else None

        # 備註（特殊交易）
        note = (r.get("備註") or "").strip()

        conn.execute("""
            INSERT INTO lvr (city, district, road, road_seg, address,
                             total_floors, floor, area_m2, area_ping,
                             land_m2, land_ping,
                             year_completed, building_type, txn_date, price_total,
                             zone_urban, source, note)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (city, district, road, road_seg, address,
              total_floors, floor, area_m2, area_ping,
              land_m2, land_ping,
              year_completed, building_type, txn_date, price_total,
              zone_urban, source, note))
        inserted += 1
    conn.commit()
    return inserted


def ensure_fresh():
    """確保 LVR 索引為最新。每 7 天自動重新下載 + rebuild。"""
    marker = LVR_DIR / ".last_refresh"
    if marker.exists():
        from datetime import datetime
        mtime = datetime.fromtimestamp(marker.stat().st_mtime)
        if (datetime.now() - mtime).total_seconds() < 604800:  # 7 天
            return  # 已刷新，跳過
    # 如果 sqlite 已存在且有資料，只下載本期增量
    if DB_PATH.exists():
        conn = init_db()
        cnt = conn.execute("SELECT COUNT(*) FROM lvr").fetchone()[0]
        conn.close()
        if cnt > 50000:
            # 已有大量資料 → 只下載本期（~2 秒）
            try:
                from scraper.download_lvr import download_season
                download_season(None)  # 只下本期
                # 增量匯入本期
                conn = init_db()
                for csv_path in sorted((LVR_DIR / "current").glob("*.csv")):
                    import_csv(csv_path, conn)
                conn.commit()
                conn.close()
            except Exception as e:
                logger.warning(f"LVR 增量更新失敗：{e}")
            marker.touch()
            return
    # 首次或資料太少 → 完整下載
    try:
        from scraper.download_lvr import download_all
        download_all()  # 全量（101S3 ~ 最新，實價登錄開辦至今）
    except Exception as e:
        logger.warning(f"LVR 下載失敗：{e}")
    build_index(reset=True)
    marker.touch()


def build_index(reset: bool = False) -> dict:
    """掃 data/lvr/**/*.csv → sqlite。"""
    if reset and DB_PATH.exists():
        DB_PATH.unlink()
    conn = init_db()
    conn.execute("DELETE FROM lvr")
    total = 0
    for csv_path in sorted(LVR_DIR.rglob("*.csv")):
        n = import_csv(csv_path, conn)
        logger.info(f"  {csv_path.relative_to(BASE_DIR)} → {n} rows")
        total += n
    conn.commit()
    conn.close()
    return {"total": total}


# ── 查詢 ─────────────────────────────────────────────────────────────────────

def _haversine(lat1, lng1, lat2, lng2):
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _pick_closest_by_address(candidates: list[str], ref_addr: str) -> str:
    """從 candidates 中挑跟 ref_addr 巷弄最吻合的。"""
    # 抽出 ref_addr 的巷+弄
    ref_lane = re.search(r"(\d+巷)", ref_addr)
    ref_alley = re.search(r"(\d+弄)", ref_addr)
    ref_l = ref_lane.group(1) if ref_lane else ""
    ref_a = ref_alley.group(1) if ref_alley else ""

    def score(addr):
        s = 0
        if ref_l and ref_l in addr: s += 2
        if ref_a and ref_a in addr: s += 3  # 弄 match 更有鑑別力
        return s

    ranked = sorted(candidates, key=score, reverse=True)
    return ranked[0]


def _reverse_geocode_lane(lat: float, lng: float, road_hint: str, lane_hint: str = "") -> Optional[str]:
    """
    Google reverse geocode fallback：
    嚴格要求「同路 + 同巷（若有 lane_hint）」才回值，否則 None（避免 591 座標偏到鄰巷誤推門牌）。
    """
    from config import GOOGLE_MAPS_API_KEY
    if not GOOGLE_MAPS_API_KEY:
        return None
    try:
        import httpx
        r = httpx.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"latlng": f"{lat},{lng}", "key": GOOGLE_MAPS_API_KEY, "language": "zh-TW"},
            timeout=10,
        )
        data = r.json()
        if data.get("status") != "OK":
            return None

        road_short = re.sub(r"[一二三四五六七八九十]段$", "", road_hint)

        candidates = []
        for res in data.get("results", []):
            types = res.get("types", [])
            if not any(t in types for t in ("street_address", "premise", "subpremise")):
                continue
            addr = res.get("formatted_address", "")
            if road_short not in addr or not re.search(r"\d+號", addr):
                continue
            if lane_hint and lane_hint not in addr:
                continue
            loc_type = res.get("geometry", {}).get("location_type", "")
            if loc_type == "RANGE_INTERPOLATED":
                continue
            loc = res.get("geometry", {}).get("location", {})
            dist = _haversine(lat, lng, loc["lat"], loc["lng"]) if loc else 9999
            is_rooftop = 1 if loc_type == "ROOFTOP" else 0
            # 從 Google 的 formatted_address 抽出「到號為止」的標準地址，
            # 切掉後面的 subpremise / 亂碼（e.g. "9號四樓號"、"7 樓2號" 的垃圾）。
            canon_m = re.search(
                r"((?:[\u4e00-\u9fa5]+(?:市|縣))?(?:[\u4e00-\u9fa5]{1,3}區)?"
                r"[\u4e00-\u9fa5]+(?:路|街|大道)"
                r"(?:[一二三四五六七八九十]段)?"
                r"(?:\d+巷)?(?:\d+弄)?"
                r"\d+(?:[-之]\d+)?號)",
                addr,
            )
            if not canon_m:
                continue
            canon = canon_m.group(1)
            # 去掉「台灣 / 臺灣」前綴（regex 的市/縣 prefix 可能貪婪吃進去）
            canon = re.sub(r"^(台灣|臺灣)", "", canon).strip()
            # 排除畸形：正則應該已保證，但再保險一次
            if re.search(r"樓.*號", canon):
                continue
            if road_short not in canon:
                continue
            if lane_hint and lane_hint not in canon:
                continue
            candidates.append((-is_rooftop, dist, canon))

        if candidates:
            candidates.sort()
            return candidates[0][2]
    except Exception as e:
        logger.debug(f"_reverse_geocode_lane error: {e}")
    return None


def _normalize_addr_for_dedup(addr: str) -> str:
    """字元標準化：臺↔台、之↔-、全形/半形數字等，用於 dedup 比對。
    不改動顯示用的原字串，只作為 dedup key。"""
    if not addr:
        return ""
    s = addr
    s = s.replace("臺", "台")
    s = s.replace("-", "之")
    # 全形數字 → 半形
    s = s.translate(str.maketrans("0123456789", "0123456789"))
    s = s.replace(" ", "")
    return s


def _build_candidates_detail(rows: list) -> list:
    """把 rows 轉成 [{address, land_ping}]。
    dedup 規則：以地址「號之前的前綴」為 key（同一棟建物不同樓層視為同一候選），
    每組取第一筆有 land_ping 的紀錄，address 也取到「號」為止（不帶樓）。
    比對前先做字元標準化（臺/台、之/-、全形數字）避免同地址重複顯示。"""
    def _prefix(addr: str) -> str:
        m = re.search(r"^(.+?\d+(?:[-之]\d+)?號)", addr or "")
        return m.group(1) if m else (addr or "")
    by_key = {}
    for r in rows:
        addr = r[0]
        land = r[2]
        if not addr or land is None or land <= 0:
            continue
        prefix = _prefix(addr)
        key = _normalize_addr_for_dedup(prefix)
        if key not in by_key:
            by_key[key] = {"address": prefix, "land_ping": land}
    return list(by_key.values())


def _stat_consistent(rows: list) -> bool:
    """無 lane_hint 時使用：rows 統計上是否一致（同批建案訊號）。
    N ≥ 2 + 地坪 std ≤ 0.1 + 完工年份全一致。"""
    valid = [r for r in rows if r[2] and r[2] > 0]
    if len(valid) < 2:
        return False
    lands = [r[2] for r in valid]
    years = [r[3] for r in valid if r[3]]
    mean_land = sum(lands) / len(lands)
    std_land = (sum((x - mean_land) ** 2 for x in lands) / len(lands)) ** 0.5
    if std_land > 0.1:
        return False
    if years and len(set(years)) > 1:
        return False
    return True


def triangulate_address(
    *,
    city: str,
    district: str,
    road_seg: Optional[str],
    total_floors: Optional[int],
    building_area_ping: Optional[float],
    building_age: Optional[int] = None,
    coord: Optional[tuple[float, float]] = None,
    floor: Optional[str] = None,
    area_tolerance_ping: float = 0.01,
    lane_hint: Optional[str] = None,
    alley_hint: Optional[str] = None,
    land_area_ping: Optional[float] = None,
) -> dict:
    """
    用 LVR 歷史資料反推真實地址。
    lane_hint: 591 地址中的巷（例如 "790巷"），用來縮小搜尋範圍。
    """
    out = {"confidence": "none", "address": None, "candidates": [], "rows_matched": 0,
           "lvr_records": [], "lvr_land_ping": None}
    if not (city and district and road_seg):
        return out

    rows = []
    if building_area_ping:
        conn = init_db()
        cur = conn.cursor()
        sql = """
            SELECT address, area_ping, land_ping, year_completed, txn_date,
                   price_total, zone_urban, building_type, note
            FROM lvr
            WHERE city = ? AND district = ? AND road_seg = ?
              AND area_ping BETWEEN ? AND ?
        """
        params = [city, district, road_seg,
                  building_area_ping - area_tolerance_ping,
                  building_area_ping + area_tolerance_ping]
        if total_floors:
            sql += " AND total_floors = ?"
            params.append(total_floors)
        if lane_hint:
            sql += " AND address LIKE ?"
            params.append(f"%{lane_hint}%")
        sql += " ORDER BY txn_date DESC"
        cur.execute(sql, params)
        rows = cur.fetchall()
        conn.close()

    # 不用屋齡篩選（屋齡每年成長，且 591/LVR 計算方式可能不同，會誤殺正確匹配）

    # 土地坪數嚴格驗證：完全相同的房子建坪+地坪都必須 ±0.01。
    # 規則：
    #   (A) OCR 有 land_area_ping → LVR land_ping 必須 ±0.01 才算同棟；差太多整組作廢
    #   (B) OCR 沒 land_area_ping → 無法驗證是否同棟 → 一律不用 LVR 反推地址
    #        （LVR records 保留給前端參考，但不寫 inferred / 不覆蓋 land）
    if rows:
        if land_area_ping is None:
            # 情境 B：591 沒地坪 → 用 LVR 推測「就是這間」（unique）或列候選下拉（multi）
            # - 有 lane_hint：rows 已經由 SQL 限制在同巷內，可直接用
            # - 無 lane_hint (只到路)：必須通過統計一致性檢查，否則放棄（免亂配）
            out["lvr_records"] = [
                {
                    "address": r[0], "area_ping": r[1], "land_ping": r[2],
                    "year_completed": r[3], "txn_date": r[4], "price_total": r[5],
                    "zone_urban": r[6], "building_type": r[7],
                    "note": r[8] or "", "is_special": bool(r[8] and r[8].strip()),
                } for r in rows
            ]
            out["rows_matched"] = len(rows)

            # 分三種情境：
            # (i) 有 lane_hint：SQL 已限制同巷 → rows 都算同批候選
            # (ii) 無 lane_hint + LVR 統計一致（地坪 std≤0.1 + 同年）→ 視同同批候選
            # (iii) 無 lane_hint + LVR 分散 → 仍列所有候選讓用戶挑，但標 land_area_inconsistent，
            #        預設選項改成「座標反查」（Google），地坪不填（用戶挑 LVR 時再填該筆地坪）
            consistent = bool(lane_hint) or _stat_consistent(rows)
            cands = _build_candidates_detail(rows)

            if consistent and cands:
                default = cands[0]
                out["address"] = default["address"]
                out["lvr_land_ping"] = default["land_ping"]
                out["candidates_detail"] = cands
                out["candidates"] = [c["address"] for c in cands]
                out["confidence"] = "unique" if len(cands) == 1 else "multi"
                out["land_area_source"] = "lvr"
                _append_floor(out, floor)
                return out

            if not consistent and cands:
                # 分散 case：加座標反查結果當首選 + 列所有 LVR 候選供用戶挑
                out["land_area_inconsistent"] = True
                rev_addr = None
                if coord and road_seg:
                    rev_addr = _reverse_geocode_lane(coord[0], coord[1], road_seg, lane_hint or "")
                # cands 加上反查選項（若有）為第一筆；預設就選它（地坪=None）
                full_cands = []
                if rev_addr:
                    full_cands.append({"address": rev_addr, "land_ping": None, "is_reverse_geo": True})
                full_cands.extend(cands)
                out["candidates_detail"] = full_cands
                out["candidates"] = [c["address"] for c in full_cands]
                default = full_cands[0]
                out["address"] = default["address"]
                # 若預設是 reverse_geo → 不填地坪（None）；若 LVR candidate → 填其 land_ping
                if default.get("land_ping") is not None:
                    out["lvr_land_ping"] = default["land_ping"]
                    out["land_area_source"] = "lvr"
                out["confidence"] = "reverse_geo" if default.get("is_reverse_geo") else "multi"
                _append_floor(out, floor)
                return out

            # LVR 無法推測 → 座標反查 Google 當備援
            if coord and road_seg:
                rev_addr = _reverse_geocode_lane(coord[0], coord[1], road_seg, lane_hint or "")
                if rev_addr:
                    out["confidence"] = "reverse_geo"
                    out["address"] = rev_addr
            _append_floor(out, floor)
            return out
        # 情境 A：OCR 有地坪 → 嚴格 ±0.01 篩
        strict = [r for r in rows if r[2] is not None and abs(r[2] - land_area_ping) <= 0.01]
        if strict:
            rows = strict
        else:
            # ±0.01 嚴格沒中 → 試 loose ±0.1（視為同物件+警告；caller 看 addr_has_number 決定用不用）
            loose = [r for r in rows if r[2] is not None and abs(r[2] - land_area_ping) <= 0.1]
            if loose:
                rows = loose
                out["land_area_mismatch_warning"] = True
            else:
                # 完全比不上 → LVR 視為不相關（records 保留作參考），
                # 但仍嘗試座標反查 Google 給用戶 address_inferred（不靠 LVR、單純地址補完）
                out["lvr_records"] = [
                    {
                        "address": r[0], "area_ping": r[1], "land_ping": r[2],
                        "year_completed": r[3], "txn_date": r[4], "price_total": r[5],
                        "zone_urban": r[6], "building_type": r[7],
                        "note": r[8] or "", "is_special": bool(r[8] and r[8].strip()),
                    } for r in rows
                ]
                out["rows_matched"] = len(rows)
                if coord and road_seg:
                    rev_addr = _reverse_geocode_lane(coord[0], coord[1], road_seg, lane_hint or "")
                    if rev_addr:
                        out["confidence"] = "reverse_geo"
                        out["address"] = rev_addr
                _append_floor(out, floor)
                return out

    # rows 完全空白（LVR SQL 層就沒中）→ 至少跑一次 reverse_geo 給 address_inferred
    # 原本 reverse_geo fallback 被包在 if rows: 裡 → rows=[] 時永遠不會跑
    if not rows and coord and road_seg:
        rev_addr = _reverse_geocode_lane(coord[0], coord[1], road_seg, lane_hint or "")
        if rev_addr:
            out["confidence"] = "reverse_geo"
            out["address"] = rev_addr
        _append_floor(out, floor)
        return out

    # 取 distinct address
    addr_set = {r[0] for r in rows}
    out["rows_matched"] = len(rows)
    out["candidates"] = sorted(addr_set)
    out["candidates_detail"] = _build_candidates_detail(rows)

    # 所有 match 的 LVR 紀錄（給前端顯示）
    out["lvr_records"] = [
        {
            "address": r[0],
            "area_ping": r[1],
            "land_ping": r[2],
            "year_completed": r[3],
            "txn_date": r[4],
            "price_total": r[5],
            "zone_urban": r[6],
            "building_type": r[7],
            "note": r[8] or "",
            "is_special": bool(r[8] and r[8].strip()),
        }
        for r in rows
    ]

    # 取 LVR 的土地坪數（第一筆有值的）
    for rec in out["lvr_records"]:
        if rec.get("land_ping") and rec["land_ping"] > 0:
            out["lvr_land_ping"] = rec["land_ping"]
            break

    # 如果同坪數 match 沒有土地坪 → 查同棟其他樓層（只用 address 去掉樓層比對）
    if not out["lvr_land_ping"] and out["candidates"]:
        try:
            # 取第一個候選地址，去掉樓層部分查 LVR
            base_addr = re.sub(r"[一二三四五六七八九十\d]+樓.*$", "", out["candidates"][0])
            if base_addr and conn:
                pass  # conn 已 close，需重開
            conn2 = init_db()
            rows2 = conn2.execute(
                "SELECT land_ping FROM lvr WHERE address LIKE ? AND land_ping > 0 LIMIT 1",
                (f"%{base_addr}%",)
            ).fetchall()
            conn2.close()
            if rows2:
                out["lvr_land_ping"] = rows2[0][0]
        except Exception:
            pass

    # 用 dedupe 後的 candidates_detail 判斷 unique vs multi（同棟不同樓視為同一候選）
    dedup_addrs = [c["address"] for c in (out.get("candidates_detail") or [])]
    if len(dedup_addrs) == 1:
        out["confidence"] = "unique"
        out["address"] = dedup_addrs[0]
    elif len(dedup_addrs) > 1:
        out["confidence"] = "multi"
        # 多筆：用座標定位的地址選最接近的
        if coord and road_seg:
            rev_addr = _reverse_geocode_lane(coord[0], coord[1], road_seg, lane_hint or "")
            if rev_addr:
                best = _pick_closest_by_address(dedup_addrs, rev_addr)
                out["address"] = best
            else:
                out["address"] = sorted(dedup_addrs)[0]
        else:
            out["address"] = sorted(dedup_addrs)[0]
    elif len(addr_set) >= 1:
        # dedup 後是空（例如 rows 的 land_ping 都 None）→ fallback 用舊邏輯
        if len(addr_set) == 1:
            out["confidence"] = "unique"
            out["address"] = sorted(addr_set)[0]
        else:
            out["confidence"] = "multi"
            out["address"] = sorted(addr_set)[0]

    # LVR 沒 match → 用 Google reverse geocode 找同巷弄的最近門牌
    if out["confidence"] == "none" and coord and road_seg:
        rev_addr = _reverse_geocode_lane(coord[0], coord[1], road_seg, lane_hint or "")
        if rev_addr:
            out["confidence"] = "reverse_geo"
            out["address"] = rev_addr

    _append_floor(out, floor)
    return out


def _append_floor(out: dict, floor):
    """把 591 的樓層 append 到 out['address'] 尾端（取代 LVR 地址中的 X樓/X層/XF）。"""
    if not (out.get("address") and floor):
        return
    addr = out["address"]
    addr = re.sub(r"[一二三四五六七八九十\d]+樓\S*$", "", addr)
    addr = re.sub(r"[一二三四五六七八九十\d]+層\S*$", "", addr)
    addr = re.sub(r"\d+F\S*$", "", addr)
    addr = addr.rstrip()
    f_main = str(floor).split("/")[0]
    f_m = re.search(r"\d+", f_main)
    f_num = f_m.group(0) if f_m else ""
    out["address"] = f"{addr}{f_num}樓" if f_num else addr
