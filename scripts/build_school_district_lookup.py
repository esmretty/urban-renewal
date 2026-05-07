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


# (city, village) → set of valid district names — 用 NLSC TownVillagePoint 數據建反查
# 用途：PDF 跨區共同學區 cell 沒寫前綴 X區 時，page_district sticky 會把里 emit 給錯區
# (例：中和區「錦和國小」cell 寫「錦和里」沒寫「永和區」前綴 → 我們 emit 給中和區)
# 但「錦和里」實際只存在中和區 — 行政區劃 ground truth 來校正 page_district
_VILLAGE_REGION_MAP = None


def _load_village_region_map():
    """載 _raw_twvillage.json (NLSC TownVillagePoint dump)，build (city, village) → {district set}"""
    global _VILLAGE_REGION_MAP
    if _VILLAGE_REGION_MAP is not None:
        return _VILLAGE_REGION_MAP
    src = DATA / "_raw_twvillage.json"
    mp = {}
    if src.exists():
        with open(src, encoding="utf-8") as fp:
            d = json.load(fp)
        for f in d.get("features", []):
            p = f.get("properties") or {}
            cnty = p.get("county")
            town = p.get("town")
            vill = p.get("village")
            if cnty and town and vill:
                mp.setdefault((cnty, vill), set()).add(town)
    _VILLAGE_REGION_MAP = mp
    return mp


def add(city, district, village, kind, school):
    """kind: 'elementary' / 'junior_high'

    region 校正：若 (city, village) 真實只屬於某些 district，且當前 district 不在那 set 內，
    強制 override 成 mapping 的 unique district (mapping set 大小=1 時才 override，避免重名歧義)
    """
    if not (city and district and village and school):
        return
    # NLSC ground truth 校正：里只在某區存在 / 但被 emit 到別區 → 修正成正確區
    mp = _load_village_region_map()
    valid = mp.get((city, village))
    if valid:
        if district not in valid:
            # PDF 給的 district 不在該里的真實所屬區 set 內
            if len(valid) == 1:
                # 該里只在一區存在 → 直接 override
                district = next(iter(valid))
            else:
                # 多區重名 — 無法判斷，跳過 (避免 emit 到錯區造成假資料)
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
                village = re.sub(r"\s+", "", (row.get("里") or ""))
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
        # 移除「【...】」自由學區 cross-school 註解
        # PDF 自由學區 cell 慣例：「湳興里【大觀、板橋、後埔國小自由學區】」
        # 表示湳興里被多校共享為自由學區。註解內容是學校 list，不是村里名。
        # 不 strip 會讓 split 「、」後「湳興里【大觀」黏字串，被 「【」filter 殺掉 →
        # 整個湳興里漏掉。先 strip 整段 →「湳興里」乾淨 token。
        t = re.sub(r"【[^】]*】", "", t)
        # 「。」當分隔符 — PDF 內偶爾用句號斷句 (例: 「龍福里。\n二、富民里」→ 移
        # 換行後變「龍福里。二、富民里」→ split 「、」後「龍福里。二」會被補後綴成
        # 假村里「龍福里。二里」)。把「。」替成「、」讓 split 自然處理。
        t = t.replace("。", "、")
        # 中文/阿拉伯序號 marker (例:「一、二、三、四」) 在「、」前剝掉 — 自由學區
        # cell 慣例「一、XX 二、YY」段落，移括號後變「XX 二、YY」 → 「二」黏前後
        # token 形成假村里 (例：「留侯四里」、「龍福里。二里」)。在 split 前先剝光，
        # 「、」分隔符保留 → split 後 token 乾淨。
        t = re.sub(r"[一二三四五六七八九十0-9]+(?=[、，,])", "", t)
        # 用「、」「，」分隔
        parts = re.split(r"[、,，]", t)
        out = []
        cur_region = default_region
        # PDF 「自由學區」cell 內常用「一、二、三、四、五」中文編號分段 (e.g.
        # 「一、中原里(1-6 鄰) 二、景平里...」)，移括號後 split 「、」會把序號獨立成 token，
        # 補「里」suffix 變假村里「一里」「四里」「五里」 — 直接濾掉純序號 token
        SERIAL_TOKENS = {"一","二","三","四","五","六","七","八","九","十","1","2","3","4","5","6","7","8","9","10"}
        for p in parts:
            # strip all whitespace (含內部) — PDF 表格 rendering 會在中文間插空白
            # (例：「宏 翠 里」「塗 潭 里」「敦厚 里」)。內部空白移除前比 strip()
            # 完整 — 不然會留下 "宏 翠 里" 這種假 key。
            p = re.sub(r"\s+", "", p)
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
            # 過濾 PDF 描述性詞 / parser noise：
            #   「等N里」「等里」(例：「復興等4里」「景安等里」)
            #   「各里」「其餘里」「全里」
            #   含括號殘字「）」「(」「【」「】」(PDF 跨頁 cell 切斷造成的 orphan paren — 例「17 鄰）里」)
            #   含「鄰」字 (鄰級切割描述詞，e.g. 「N 鄰」 之類，括號處理失敗 leftover)
            if "等" in village:
                continue
            if village in ("各里", "其餘里", "全里"):
                continue
            # 非里名描述詞 (PDF 偶爾出現「全區」「轄區」「全區轄區」之類描述
            # 不是具體村里 — e.g. 「坪林國小 全區」表示坪林國小服務全區無切割)
            # 注意：此時 village 尚未補「里」suffix → 比對含 / 不含「里」 兩形式
            if village in ("全區", "轄區", "全區轄區",
                           "全區里", "轄區里", "全區轄區里"):
                continue
            if any(c in village for c in "（）()【】鄰"):
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
    # 跨頁 row 處理：PDF 一個學校 row 經常跨 2 頁切，cell 內容被切斷 (例：「後埔里(14-」
    # 在 page 6 結尾、「17 鄰)」在 page 7 開頭)。如果獨立 parse 每頁 cell，半截括號 +
    # noise leftover 會讓 villages 漏掉 (後埔里會被「(」filter 殺掉)。
    # 修法：用 pending row buffer，遇到新校名 row 就 flush 前一個 row + 開新 buffer；
    # 遇到延續 row (校名空) 就把 cell 內容**字串拼接**到 pending 後面，等真正 flush 時
    # 才 parse — 這樣括號自然配對、文字完整。
    pending_row = None  # {'schools_with_region': [...], 'base_text': '', 'free_text': '', 'page_district': '...'}

    def flush_pending():
        nonlocal pending_row
        if not pending_row or not pending_row['schools_with_region']:
            pending_row = None
            return
        # parse 完整拼接後的 base + free
        vlist = (
            parse_villages_with_region(pending_row['base_text'], pending_row['page_district']) +
            parse_villages_with_region(pending_row['free_text'], pending_row['page_district'])
        )
        for s_region, school_clean in pending_row['schools_with_region']:
            for v_region, village in vlist:
                add("新北市", v_region, village, kind, school_clean)
        pending_row = None

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
                free_district_str = (row[2] or "").strip() if len(row) > 2 else ""
                # 整 row 都空 → skip (但不清 pending)
                if not school_raw and not base_district_str and not free_district_str:
                    continue

                if school_raw:
                    # 新校名 row → 先 flush 上個 pending、解析新校名、開新 pending buffer
                    # 跨區共同學區 row 校名 cell 是「X區\n校名」(換行分區別 + 校名) — 黏回單字串
                    school_raw_n = re.sub(r"^([一-龥]{2,3}區)\s*\n\s*", r"\1", school_raw)
                    # 多校共用同 row case
                    school_names = [s.strip() for s in re.split(r"[\n、,，]", school_raw_n) if s.strip()]
                    # 跳過 header row
                    if any(s in ("學校名稱", "學校") for s in school_names):
                        continue
                    # 解析每校 region + clean name
                    new_schools_with_region = []
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
                        # 高中附設國中部 normalize
                        if kind == "junior_high" and "高中" in s_clean and not s_clean.endswith("國中"):
                            s_clean = s_clean.replace("高中", "")
                        if not s_clean.endswith(suffix):
                            s_clean = s_clean + suffix
                        new_schools_with_region.append((s_region, s_clean))
                    if not new_schools_with_region:
                        continue
                    # flush 前一個 pending (跨頁 buffer 就是在這裡完成 parse)
                    flush_pending()
                    # 開新 pending buffer (page_district 用該 row 開始的頁面 sticky 區)
                    pending_row = {
                        'schools_with_region': new_schools_with_region,
                        'base_text': base_district_str,
                        'free_text': free_district_str,
                        'page_district': page_district,
                    }
                else:
                    # 校名空 + base/free 有 → 跨頁延續：把這 row cell **字串拼接**到 pending 後
                    # 不獨立 parse。等下一個新校名 row 出現時才 flush + parse 完整拼接內容。
                    # 這樣 PDF 切爛的「後埔里(14-」+「17 鄰)」在 flush 時自然連成「後埔里(14-17 鄰)」
                    # → 括號正常配對 → 後埔里被正確 emit
                    if pending_row:
                        pending_row['base_text'] += base_district_str
                        pending_row['free_text'] += free_district_str
            # 表內 loop 結束 — 不 flush，跨頁延續可能在下一頁

    # 全部 page 跑完 → flush 最後一個 pending
    flush_pending()


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
