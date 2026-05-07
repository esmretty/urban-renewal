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
    # 國小 PDF 頁首：「中和區 115 學年度國小學區一覽表」(只有每區第一頁有，後續同區頁面沒)
    # 過去寫 r"([一-龥]{2,3}區)\s*\d{2,3}\s*學年度" 太寬鬆，會被表格欄標題列「自由學區 114學年度前備註」
    # 誤當行政區 header → cur_district 被覆寫成「自由學區」(不在白名單) → 後續同區整批頁面被跳過。
    # 修法：要求 「學年度國[小中]學區一覽表」整段標題出現才算 header；只在 page 首字段內找 (避開
    # 內文偶爾出現相似字樣)。其他無 header 頁保留 cur_district sticky 沿用上一頁的區。
    DISTRICT_RE = re.compile(r"([一-龥]{2,3}區)\s*\d{2,3}\s*學年度國[小中]學區一覽表")
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

    def parse_villages_with_region(text, default_region):
        """從「基本學區」/「自由學區」cell 抽取 (region, village) tuple list。

        PDF 實際：跨區共同學區 cell 內會混雜本區跟外區里名：
          「永和區前溪里、保平里、保安里」← 第一個帶「永和區」前綴，後續沿用 sticky region
          「福祥里、永和區雙和里」← 前段沒前綴 (= default_region)，後段帶「永和區」
        所以解析時要按順序 scan，遇到 X區 prefix 就 update sticky region，後續無前綴里沿用。

        過濾「等N里」「N里」尾巴 (PDF 描述「復興等4里」會被誤拆) → 整個 token 丟。
        """
        if not text:
            return []
        # 移除換行
        t = text.replace("\n", "")
        # 移除括號內容（中文+英文括號）
        t = re.sub(r"\([^)]*\)", "", t)
        t = re.sub(r"（[^）]*）", "", t)
        # 用「、」「，」分隔
        parts = re.split(r"[、,，]", t)
        out = []
        cur_region = default_region
        # PDF 「自由學區」cell 內常用「一、二、三、四、五」中文編號分段 (e.g.
        # 「一、中原里(1-6 鄰) 二、景平里...」)，移括號後 split 「、」會把序號獨立成 token，
        # 補「里」suffix 變假村里「一里」「四里」「五里」 — 直接濾掉純序號 token
        SERIAL_TOKENS = {"一","二","三","四","五","六","七","八","九","十","1","2","3","4","5","6","7","8","9","10"}
        for p in parts:
            p = p.strip()
            if not p:
                continue
            if p in SERIAL_TOKENS:
                continue
            # 偵測 X區 prefix → update sticky region + 拆出後段當村里名
            m = re.match(r"^([一-龥]{2,3}區)\s*(.+)$", p)
            if m:
                cur_region = m.group(1)
                village = m.group(2).strip()
            else:
                village = p
            # 過濾 PDF 描述性詞（非真實里名）：
            #   「等N里」「等里」(例：「復興等4里」「景安等里」)
            #   「各里」「其餘里」「全里」
            if "等" in village:
                continue
            if village in ("各里", "其餘里", "全里"):
                continue
            # 尾巴為「N+里」(純數字+里) — 來自 PDF 描述「四里」「等4里」之類
            if re.fullmatch(r"[一二三四五六七八九十百0-9]+\s*里", village):
                continue
            # 「{真名里}{序號}」黏字串：PDF 序號「N、」緊接前 row 末括號+換行造成 (例：
            # 「六、留侯里(7、9鄰)\n七、雙玉里」移括號 + 移換行後變「留侯里七、雙玉里」，
            # split 「、」 後「留侯里七」獨立 → 應 strip 尾序號 →「留侯里」)
            m_serial = re.match(r"^(.+里)([一二三四五六七八九十0-9]+)$", village)
            if m_serial:
                village = m_serial.group(1)
            # 補「里」字（PDF 中後段省略「里」字 — e.g. "留侯里、流芳里、赤松、黃石"）
            if not village.endswith("里"):
                village = village + "里"
            # 過濾明顯不是里的字串
            if 1 < len(village) <= 6 and "里" in village:
                out.append((cur_region, village))
        return out

    # 舊 API 兼容：parse_villages(text) 仍可用，回 list of village 名 (用 default_region 拋掉)
    def parse_villages(text):
        return [v for _, v in parse_villages_with_region(text, "")]

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
                school_raw = (row[0] or "").strip()
                base_district_str = (row[1] or "").strip()
                if not school_raw or not base_district_str:
                    continue
                # 跨區共同學區 row 校名 cell 是「X區\n校名」(換行分區別 + 校名) — 黏回單字串
                # 例：「永和區\n永平國小」 → 「永和區永平國小」
                school_raw = re.sub(r"^([一-龥]{2,3}區)\s*\n\s*", r"\1", school_raw)
                # 多校共用同 row case：cell 內用換行/頓號分校 (例：永和區國中只有永和+福和兩校
                # 共用同 row → "永和國中\n福和國中")。要拆。
                school_names = [s.strip() for s in re.split(r"[\n、,，]", school_raw) if s.strip()]
                # 跳過 header row
                if any(s in ("學校名稱", "學校") for s in school_names):
                    continue
                # 合併基本 + 自由學區（自由學區在 row[2]）— 多校共用同 villages
                free_district_str = (row[2] or "").strip() if len(row) > 2 else ""

                # 解析每校 region + clean name
                # 校名前綴若帶 X區 (例：「永和區永平國小」) → 該校屬該 X區；否則屬本頁主區 (sticky)
                schools_with_region = []
                for s in school_names:
                    s_clean = re.sub(r"\([^)]*\)", "", s).strip()
                    if not s_clean:
                        continue
                    region_m = re.match(r"^([一-龥]{2,3}區)\s*(.+)$", s_clean)
                    if region_m:
                        s_region = region_m.group(1)
                        s_clean = region_m.group(2).strip()
                    else:
                        s_region = page_district
                    # 高中附設國中部 normalize：「錦和高中」(校名) + 「國中」(suffix) → 「錦和高中國中」
                    # 真實校名通常稱「錦和國中」 → 拿掉中間「高中」
                    if kind == "junior_high" and "高中" in s_clean and not s_clean.endswith("國中"):
                        s_clean = s_clean.replace("高中", "")
                    if not s_clean.endswith(suffix):
                        s_clean = s_clean + suffix
                    schools_with_region.append((s_region, s_clean))

                # 解析里名 list — 每個里抓自己的 region (cell 內前綴 sticky scan)
                # default region 用「該校所屬 region」(校名前綴 X區 + page_district fallback) 而非 page sticky
                # 避免：page 25 sticky=中和區但實際 row 是「永和區永平國小」(該校的村里應歸永和區)
                default_v_region = schools_with_region[0][0] if schools_with_region else page_district
                villages_with_region = (
                    parse_villages_with_region(base_district_str, default_v_region) +
                    parse_villages_with_region(free_district_str, default_v_region)
                )

                # 注意：lookup 只 by (新北市, village_region, village_name) 索引；school 名 normalize
                # 已不帶 X區 前綴。所以跨區共同學區的學校會出現在「他區的 lookup」下且校名不帶區別前綴。
                # 例：中和區福祥里 → 「中和國小、永平國小」(永平本來屬永和區，跨區可去)。
                # 用戶看到時可能不知「永平國小」是永和區的 — 接受 trade-off (跟既有 schema 一致)。
                for s_region, school_clean in schools_with_region:
                    for v_region, village in villages_with_region:
                        add("新北市", v_region, village, kind, school_clean)


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
