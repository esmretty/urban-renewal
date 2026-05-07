"""從 4 個 CSV (台北/新北 國小/國中) 解析成統一 row-based lookup JSON。

Output: data/school_districts/lookup.json
Schema:
  {
    "_meta": {"version": "2", "generated_at": ISO, "source_files": [...]},
    "rows": [
      {
        "city": "新北市" | "台北市",
        "district": "板橋區",          // 物件所在區（lookup key）
        "school_district": "板橋區",   // 學校所在區（跨區共同學區時不同）
        "kind": "elementary" | "junior_high",
        "school": "板橋國小",          // 校名（含「(國中部)」、「板橋區XX國小」跨區前綴）
        "zone_type": "基本" | "自由" | "共同",
        "village": "留侯里",           // 純里名（補「里」suffix；用於 lookup match）
        "village_raw": "留侯里",       // 原始「里／鄰」欄位字串（detail page 顯示）
        "neighborhoods_raw": null,     // 「1-12鄰」/「(8-25鄰)」/「(7、9鄰)」等切割描述（無切割=null）
        "source_page": 2,
        "extra": {
          "source_table": "主表" | "附件" | null,  // ntpc_jh
          "school_address": str | null,            // taipei_jh
          "school_phone": str | null,              // taipei_jh
          "school_code": str | null,               // taipei_jh
        }
      },
      ...
    ]
  }

設計：
- row-based 而非 nested — detail page 可以「如實」顯示原始 CSV 欄位
- village 純里名（lookup key）vs village_raw（顯示）分離
- 鄰級切割保留在 neighborhoods_raw（未來接 TGOS 鄰 API 後可做精準匹配）
- zone_type 統一三類（基本/自由/共同），CSV 原值「基本學區」「自由學區」「共同學區」剝「學區」後綴
"""
import csv
import io
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "school_districts"


# ── 「里／鄰」欄位 parse ──
# 把 raw cell 拆成 (village, neighborhoods_raw)
# 範例：
#   "留侯里"                → ("留侯里", None)
#   "文湖里1–12鄰"          → ("文湖里", "1–12鄰")
#   "福德里(8-25鄰)"        → ("福德里", "(8-25鄰)")
#   "重慶里5～13、17～19、21～25、30～31、33～35鄰" → ("重慶里", "5～13、17～19、21～25、30～31、33～35鄰")
#   "介壽（1～4鄰）"        → ("介壽里", "（1～4鄰）")  [台北 CSV 里名常省略「里」字]
#   "介壽"                  → ("介壽里", None)
#   "中正（正義街）"        → ("中正里", "（正義街）")
#   "聯翠里1鄰除外"         → ("聯翠里", "1鄰除外")
#   "全區轄區里"            → None（filter 掉非里名）
SERIAL_TOKENS = {"一", "二", "三", "四", "五", "六", "七", "八", "九", "十"}
NON_VILLAGE_NAMES = {"全區", "轄區", "全區轄區", "全區里", "轄區里", "全區轄區里"}
# 直轄市/縣市名 — 用於從「里／鄰」前綴抓「跨區」prefix（例「中和區民生里」、「板橋區後埔里」）
REGION_RE = re.compile(r"^([一-龥]{1,4}區)\s*(.+)$")


def parse_village_field(raw: str) -> tuple[str | None, str | None, str, str | None]:
    """從 CSV「里／鄰」cell 解析。
    回 (village, neighborhoods_raw, village_raw_normalized, target_district_prefix)
      - village: 純里名（lookup key）
      - neighborhoods_raw: 鄰級切割描述（如「8-25鄰」），無=None
      - village_raw_normalized: CSV 原值（去除末尾序號 noise）
      - target_district_prefix: 若 cell 含「X區」前綴 → X區（跨區共同學區），無=None

    解不出回 (None, None, raw, None)
    """
    if not raw:
        return None, None, raw, None
    s = raw.strip()
    if not s:
        return None, None, s, None

    # ── 末尾序號 strip ── 「後埔里1–13鄰二」「(7、9鄰)三」這種 noise (整理 PDF 時 emit 進 cell)
    s_normalized = re.sub(r"\s*[一二三四五六七八九十]\s*$", "", s).strip()

    # ── X區 prefix 抓（跨區共同學區）──
    target_dist = None
    s_after_region = s_normalized
    m_region = REGION_RE.match(s_normalized)
    if m_region:
        target_dist = m_region.group(1)
        s_after_region = m_region.group(2).strip()

    # 排除非里名描述詞
    if s_after_region in NON_VILLAGE_NAMES:
        return None, None, s_normalized, None

    # 找「里」的位置 — 拆 village（「里」前 + 「里」字）跟 neighborhoods（「里」後）
    m_li = re.search(r"^(.+?里)(.*)$", s_after_region)
    if m_li:
        village = m_li.group(1).strip()
        rest = m_li.group(2).strip()
        # 若 village 是純序號 + 里 (例「四里」「等4里」) → 視為 noise
        if re.fullmatch(r"[一二三四五六七八九十百0-9]+\s*里", village):
            return None, None, s_normalized, None
        if village in NON_VILLAGE_NAMES:
            return None, None, s_normalized, None
        return village, (rest if rest else None), s_normalized, target_dist

    # 沒「里」字 — 找鄰級 marker 拆（台北 CSV 「介壽（1～4鄰）」這種）
    m_paren = re.search(r"^(.+?)([（(].*[)）].*|\d.*鄰.*|[除\d].*)$", s_after_region)
    if m_paren:
        name = m_paren.group(1).strip()
        rest = m_paren.group(2).strip()
        if name in NON_VILLAGE_NAMES:
            return None, None, s_normalized, None
        village = name + "里" if not name.endswith("里") else name
        return village, (rest if rest else None), s_normalized, target_dist

    # 沒鄰級切割也沒「里」— 純里名（台北慣例）
    if s_after_region in NON_VILLAGE_NAMES:
        return None, None, s_normalized, None
    village = s_after_region if s_after_region.endswith("里") else (s_after_region + "里")
    return village, None, s_normalized, target_dist


def split_school_names(school: str) -> list[str]:
    """同 cell 兩校黏字 split — 例「永和國中福和國中」→ ["永和國中","福和國中"]
    pattern：「國[小中]」緊接「[一-龥]{1,2}國[小中]」 = 第二校開始。
    含「(國中部)」「(高中國中部)」之類 trailing 標註保留給最後一校。
    """
    if not school:
        return []
    # 處理 「(國中部)」「(高中國中部)」trailing — 暫時 mask 不影響 split
    # 例「永平高中(國中部)」是單校；「板橋國中信義國中」是雙校
    # split pattern：在「國小」或「國中」之後若立即跟另一個漢字校名前綴 → split point
    splits = re.split(r"(?<=國[小中])(?=[一-龥]{2,4}(?:國[小中]|高中))", school)
    return [p.strip() for p in splits if p.strip()]


# ── 學校類型 normalize ──
def norm_zone_type(s: str) -> str:
    """「基本」「基本學區」→「基本」；「自由」「自由學區」→「自由」；「共同學區」→「共同」"""
    if not s:
        return ""
    t = s.strip()
    # 剝末尾「學區」
    if t.endswith("學區"):
        t = t[:-2].strip()
    return t


# ── 各 CSV 解析 ──
def parse_ntpc_es(path: Path) -> list[dict]:
    """新北國小：行政區,學校,類型,里／鄰,來源頁"""
    out = []
    with open(path, encoding="utf-8-sig") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            district = (row.get("行政區") or "").strip()
            school = (row.get("學校") or "").strip()
            zone_type = norm_zone_type(row.get("類型") or "")
            village_field = (row.get("里／鄰") or "").strip()
            page = (row.get("來源頁") or "").strip()
            if not (district and school and village_field):
                continue
            village, nb_raw, raw_norm, target_dist = parse_village_field(village_field)
            if not village:
                logger.info(f"[ntpc_es] skip non-village: {village_field!r}")
                continue
            for sch_one in split_school_names(school):
                sch_d, sch_clean = _extract_school_region(sch_one, default_region=district)
                out.append({
                    "city": "新北市",
                    "district": (target_dist or district),  # 跨區 prefix override
                    "school_district": sch_d,
                    "kind": "elementary",
                    "school": sch_clean,
                    "zone_type": zone_type,
                    "village": village,
                    "village_raw": raw_norm,
                    "neighborhoods_raw": nb_raw,
                    "source_page": _try_int(page),
                    "extra": {},
                })
    return out


def parse_ntpc_jh(path: Path) -> list[dict]:
    """新北國中：資料來源,行政區,學校,類型,里／鄰,頁碼"""
    out = []
    with open(path, encoding="utf-8-sig") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            source_table = (row.get("資料來源") or "").strip()
            district = (row.get("行政區") or "").strip()
            school = (row.get("學校") or "").strip()
            zone_type = norm_zone_type(row.get("類型") or "")
            village_field = (row.get("里／鄰") or "").strip()
            page = (row.get("頁碼") or "").strip()
            if not (district and school and village_field):
                continue
            village, nb_raw, raw_norm, target_dist = parse_village_field(village_field)
            if not village:
                logger.info(f"[ntpc_jh] skip non-village: {village_field!r}")
                continue
            for sch_one in split_school_names(school):
                sch_d, sch_clean = _extract_school_region(sch_one, default_region=district)
                out.append({
                    "city": "新北市",
                    "district": (target_dist or district),
                    "school_district": sch_d,
                    "kind": "junior_high",
                    "school": sch_clean,
                    "zone_type": zone_type,
                    "village": village,
                    "village_raw": raw_norm,
                    "neighborhoods_raw": nb_raw,
                    "source_page": _try_int(page),
                    "extra": {"source_table": source_table or None},
                })
    return out


def parse_taipei_es(path: Path) -> list[dict]:
    """台北國小：行政區,學校名稱,學區所在區,類型,里／鄰,來源頁"""
    out = []
    with open(path, encoding="utf-8-sig") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            district = (row.get("行政區") or "").strip()
            school = (row.get("學校名稱") or "").strip()
            sch_d_csv = (row.get("學區所在區") or "").strip() or district
            zone_type = norm_zone_type(row.get("類型") or "")
            village_field = (row.get("里／鄰") or "").strip()
            page = (row.get("來源頁") or "").strip()
            if not (district and school and village_field):
                continue
            village, nb_raw, raw_norm, target_dist = parse_village_field(village_field)
            if not village:
                logger.info(f"[taipei_es] skip non-village: {village_field!r}")
                continue
            # 台北 CSV「行政區」=學校所在區、「學區所在區」=該里所屬區（跨區時 ≠ 行政區）
            obj_district = sch_d_csv  # 物件實際所在區（lookup key）
            school_position_district = district
            for sch_one in split_school_names(school):
                if not sch_one.endswith("國小"):
                    sch_one = sch_one + "國小"
                out.append({
                    "city": "台北市",
                    "district": (target_dist or obj_district),
                    "school_district": school_position_district,
                    "kind": "elementary",
                    "school": sch_one,
                    "zone_type": zone_type,
                    "village": village,
                    "village_raw": raw_norm,
                    "neighborhoods_raw": nb_raw,
                    "source_page": _try_int(page),
                    "extra": {},
                })
    return out


def parse_taipei_jh(path: Path) -> list[dict]:
    """台北國中：行政區,學校名稱,學校代碼,類型,區分,里／鄰,原始項目,校址,電話及傳真"""
    out = []
    with open(path, encoding="utf-8-sig") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            district = (row.get("行政區") or "").strip()
            school = (row.get("學校名稱") or "").strip()
            school_code = (row.get("學校代碼") or "").strip()
            zone_type = norm_zone_type(row.get("類型") or "")
            obj_district_csv = (row.get("區分") or "").strip() or district  # 該里所屬區（lookup key）
            village_field = (row.get("里／鄰") or "").strip()
            address = (row.get("校址") or "").strip()
            phone = (row.get("電話及傳真") or "").strip()
            if not (district and school and village_field):
                continue
            village, nb_raw, raw_norm, target_dist = parse_village_field(village_field)
            if not village:
                logger.info(f"[taipei_jh] skip non-village: {village_field!r}")
                continue
            for sch_one in split_school_names(school):
                if not (sch_one.endswith("國中") or "國中部" in sch_one):
                    sch_one = sch_one + "國中"
                out.append({
                    "city": "台北市",
                    "district": (target_dist or obj_district_csv),
                    "school_district": district,  # 學校位置區=「行政區」column
                    "kind": "junior_high",
                    "school": sch_one,
                    "zone_type": zone_type,
                    "village": village,
                    "village_raw": raw_norm,
                    "neighborhoods_raw": nb_raw,
                    "source_page": None,
                    "extra": {
                        "school_code": school_code or None,
                        "school_address": address or None,
                        "school_phone": phone or None,
                    },
                })
    return out


def _extract_school_region(school: str, default_region: str) -> tuple[str, str]:
    """從學校名抽前綴「X區」當 school_district，剩下當乾淨校名。
    例：「板橋區信義國小」→ ("板橋區", "信義國小")
        「板橋國中」      → (default_region, "板橋國中")
    """
    m = re.match(r"^([一-龥]{1,4}區)\s*(.+)$", school)
    if m:
        return m.group(1), m.group(2).strip()
    return default_region, school


def _try_int(s: str) -> int | None:
    if not s:
        return None
    s = s.strip().split(",")[0]   # 處理「10,11」這種跨頁標示
    try:
        return int(s)
    except ValueError:
        return None


# ── Main build ──
def main():
    sources = [
        ("ntpc_elementary.csv", parse_ntpc_es),
        ("ntpc_junior_high.csv", parse_ntpc_jh),
        ("taipei_elementary.csv", parse_taipei_es),
        ("taipei_junior_high.csv", parse_taipei_jh),
    ]
    all_rows = []
    for fname, fn in sources:
        path = DATA / fname
        if not path.exists():
            logger.warning(f"[skip] {fname} 不存在")
            continue
        rows = fn(path)
        logger.info(f"[{fname}] parsed {len(rows)} rows")
        all_rows.extend(rows)

    logger.info(f"\n總 rows: {len(all_rows)}")

    # 統計：每 city x kind 多少校 / 多少里
    stats = {}
    for r in all_rows:
        key = (r["city"], r["kind"])
        s = stats.setdefault(key, {"schools": set(), "villages": set(), "districts": set()})
        s["schools"].add(r["school"])
        s["villages"].add((r["district"], r["village"]))
        s["districts"].add(r["district"])
    for (city, kind), s in sorted(stats.items()):
        logger.info(f"  {city}/{kind}: {len(s['districts'])} 區, {len(s['schools'])} 校, {len(s['villages'])} (區,里) pairs")

    out_obj = {
        "_meta": {
            "version": "2",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source_files": [s[0] for s in sources],
            "total_rows": len(all_rows),
        },
        "rows": all_rows,
    }
    out_path = DATA / "lookup.json"
    with open(out_path, "w", encoding="utf-8") as fp:
        json.dump(out_obj, fp, ensure_ascii=False, indent=2)
    logger.info(f"\nWROTE {out_path} ({out_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
