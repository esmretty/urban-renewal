"""一次性 script：解析台北 CSV + 新北 PDF，輸出統一的學區 lookup JSON。

Output: data/school_districts/lookup.json
Schema:
  {
    "台北市": {
      "信義區": {
        "華興里": {"elementary": ["興雅國小"], "junior_high": ["興雅國中"]}
      }
    },
    "新北市": {
      "板橋區": {
        "湳興里": {"elementary": ["板橋國小", "後埔國小"], "junior_high": ["板橋國中"]}
      }
    }
  }

設計：
- 一個里可能對應多個學校（自由學區、共同學區、鄰級切割）
  因為我們從座標只能拿到「里」不能拿到「鄰」，里級多校情況聚合所有可能學校
- 學校名統一加上後綴（國小/國中），台北 CSV 沒後綴會自動補
- 「(國中部)」「(高中國中部)」標註保留
"""
import os, sys, csv, io, json, re
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "school_districts"

# 結構：{city: {district: {village: {"elementary": set, "junior_high": set}}}}
out = {"台北市": {}, "新北市": {}}


def add(city, district, village, kind, school):
    """kind: 'elementary' / 'junior_high'"""
    if not (city and district and village and school):
        return
    out.setdefault(city, {}).setdefault(district, {}).setdefault(
        village, {"elementary": set(), "junior_high": set()}
    )
    out[city][district][village][kind].add(school)


# ── 台北市 CSV ──────────────────────────────────────────
def parse_taipei():
    for kind, fname, suffix in [
        ("elementary", "taipei_elementary.csv", "國小"),
        ("junior_high", "taipei_junior_high.csv", "國中"),
    ]:
        with open(DATA / fname, encoding="utf-8-sig") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                district = (row.get("行政區") or "").strip()
                village = (row.get("里") or "").strip()
                schools_raw = (row.get("學校學區") or "").strip()
                if not (district and village and schools_raw):
                    continue
                # 處理「、」「及」「，」分隔符 + 「共同學區」「國小」字樣
                # e.g. "三民、民權共同學區" → ["三民", "民權"]
                #      "民權及健康國小共同學區" → ["民權", "健康"]
                cleaned = re.sub(r"國小|共同學區|及", "、", schools_raw)
                schools = [s.strip() for s in re.split(r"[、,，]", cleaned) if s.strip()]
                for s in schools:
                    if s and s != "區":   # 偶爾有殘字
                        # 統一加後綴
                        full = s if s.endswith(suffix) else (s + suffix)
                        add("台北市", district, village, kind, full)


# ── 新北市 PDF ──────────────────────────────────────────
def parse_ntpc(pdf_path, kind, suffix):
    """每頁的 table 含「學校名稱 / 基本學區 / 自由學區 / 備註」。
    用學校名稱 + 基本學區 + 自由學區 反推「該里學生可去哪些學校」。
    PDF 文字裡夾雜頁眉(區別)，從每頁前 600 字找 'XX區' 拿 district context。
    """
    import pdfplumber

    pdf = pdfplumber.open(pdf_path)
    cur_district = None
    DISTRICT_RE = re.compile(r"([一-龥]{2,3}區)\s*\d{2,3}\s*學年度")
    # 國小 PDF 每頁有「板橋區 115 學年度國小學區一覽表」這種 header
    # 國中 PDF 是「{N}.{區名}……P.{頁碼}」目錄式，但每頁本身沒明顯 district header
    # → 國中 PDF 改用目錄頁解析建立 page→district 映射

    page_to_district = {}
    if "_jh" in str(pdf_path):
        # 國中 PDF：解析第一頁目錄
        toc_text = pdf.pages[0].extract_text() or ""
        toc_lines = re.findall(
            r"(\d{2})\.([一-龥]{1,4})…+P\s*(\d{1,3})", toc_text
        )
        # toc_lines: [('01', '板橋', '02'), ('02', '土城', '05'), ...]
        items = []
        for _idx, name, pg in toc_lines:
            items.append((name + "區", int(pg) - 1))   # PDF page → 0-indexed
        items.sort(key=lambda x: x[1])
        for i, (district_name, start_pg) in enumerate(items):
            end_pg = items[i + 1][1] if i + 1 < len(items) else len(pdf.pages)
            # 若多個區共用同 start_pg (e.g. 新店+深坑都在 P14)，至少給第一個區 1 頁
            end_pg = max(end_pg, start_pg + 1)
            for p in range(start_pg, end_pg):
                # setdefault：同 page 多區時，第一個（toc 順序在前）的區獲得歸屬
                page_to_district.setdefault(p, district_name)

    def parse_villages(text):
        """從「基本學區」cell 抽取里名 list。
        e.g. "留侯里、流芳里、赤松里、黃石里" → ["留侯里", "流芳里", "赤松里", "黃石里"]
        忽略括號內鄰級切割註記 (e.g. "華東里(1-6、19-21 鄰)" → "華東里")
        """
        if not text:
            return []
        # 移除換行
        t = text.replace("\n", "")
        # 移除括號內容（中文+英文括號）
        t = re.sub(r"\([^)]*\)", "", t)
        t = re.sub(r"（[^）]*）", "", t)
        # 移除「等里」尾標
        t = re.sub(r"等里$", "", t)
        # 用「、」「，」分隔
        parts = re.split(r"[、,，]", t)
        villages = []
        for p in parts:
            p = p.strip()
            if not p:
                continue
            # 補「里」字（有些「板橋國小」row 是 "留侯里、流芳里、赤松、黃石、挹秀..." 中後段省略「里」字）
            if not p.endswith("里"):
                p = p + "里"
            # 過濾明顯不是里的字串
            if 1 < len(p) <= 6 and "里" in p:
                villages.append(p)
        return villages

    target_districts = {"板橋區", "新莊區", "新店區", "中和區", "永和區"}
    for pg_i, page in enumerate(pdf.pages):
        # 國小 PDF：從 page 前文字找 district header
        if not page_to_district:
            text_head = (page.extract_text() or "")[:200]
            m = DISTRICT_RE.search(text_head)
            if m:
                cur_district = m.group(1)
            page_district = cur_district
        else:
            page_district = page_to_district.get(pg_i)

        if not page_district or page_district not in target_districts:
            continue

        for tab in page.extract_tables() or []:
            for row in tab:
                if not row or len(row) < 2:
                    continue
                school = (row[0] or "").replace("\n", "").strip()
                base_district_str = (row[1] or "").strip()
                if not school or not base_district_str:
                    continue
                # 跳過 header row
                if school in ("學校名稱", "學校"):
                    continue
                # 學校名 normalize（移除 "(國中部)" "(高中國中部)" 並加後綴）
                school_clean = re.sub(r"\([^)]*\)", "", school).strip()
                if not school_clean.endswith(suffix):
                    school_clean = school_clean + suffix
                # 合併基本 + 自由學區（自由學區在 row[2]）
                free_district_str = (row[2] or "").strip() if len(row) > 2 else ""
                villages = parse_villages(base_district_str) + parse_villages(free_district_str)
                for v in villages:
                    add("新北市", page_district, v, kind, school_clean)


def main():
    parse_taipei()
    print(f"台北 done. districts={list(out['台北市'].keys())}")

    es_pdf = DATA / "_ntpc_es_115.pdf"
    if es_pdf.exists():
        parse_ntpc(es_pdf, "elementary", "國小")
        print(f"新北國小 done.")
    jh_pdf = DATA / "_ntpc_jh_115.pdf"
    if jh_pdf.exists():
        parse_ntpc(jh_pdf, "junior_high", "國中")
        print(f"新北國中 done.")

    # set → list 才能 JSON serialize
    serializable = {}
    for city, dists in out.items():
        serializable[city] = {}
        for d, vills in dists.items():
            serializable[city][d] = {}
            for v, kinds in vills.items():
                serializable[city][d][v] = {
                    "elementary": sorted(kinds["elementary"]),
                    "junior_high": sorted(kinds["junior_high"]),
                }

    out_path = DATA / "lookup.json"
    with open(out_path, "w", encoding="utf-8") as fp:
        json.dump(serializable, fp, ensure_ascii=False, indent=2)
    print(f"\nWROTE {out_path}")
    # 統計
    for city in serializable:
        n_v = sum(len(d) for d in serializable[city].values())
        print(f"  {city}: {len(serializable[city])} 區, {n_v} 里")
        for d, vills in serializable[city].items():
            print(f"    {d}: {len(vills)} 里")


if __name__ == "__main__":
    main()
