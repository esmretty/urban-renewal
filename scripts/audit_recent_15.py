"""撈最近 15 筆物件，逐筆檢查所有欄位的問題（所有 source 一起看）。
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import init_db, get_col


def looks_address_garbage(addr: str) -> bool:
    """是否疑似廣告詞 / 站名 / 過於模糊（不是真實門牌）"""
    if not addr:
        return False
    import re
    if not re.search(r"(?:路|街|大道|巷|弄)", addr):
        return True
    return False


def main():
    init_db()
    col = get_col()
    # 撈最近 50 筆按 _added_at desc 拍掉 archived/deleted 取 15
    docs = []
    for d in col.where("archived", "==", False).get():
        x = d.to_dict() or {}
        if x.get("deleted"):
            continue
        added = x.get("scrape_session_at") or x.get("scraped_at") or ""
        docs.append((d.id, added, x))
    docs.sort(key=lambda t: t[1], reverse=True)
    docs = docs[:15]

    print(f"=== 最近 15 筆物件 audit ===\n")
    issues_by_doc = {}
    for doc_id, added, x in docs:
        issues = []
        sources = [s.get("name") for s in (x.get("sources") or [])] or [x.get("source")]
        addr = x.get("address") or ""
        addr_inf = x.get("address_inferred") or ""

        # 1. 地址問題
        if looks_address_garbage(addr):
            issues.append(f"address 看似廣告詞/站名: {addr!r}")
        if looks_address_garbage(addr_inf):
            issues.append(f"address_inferred 看似廣告詞/站名: {addr_inf!r}")

        # 2. 缺關鍵欄位
        for k, label in [
            ("price_ntd", "售價"),
            ("building_area_ping", "建坪"),
            ("building_age", "屋齡"),
            ("address", "地址"),
            ("city", "城市"),
            ("district", "行政區"),
            ("latitude", "緯度"),
            ("zoning", "分區"),
        ]:
            if x.get(k) in (None, "", 0):
                issues.append(f"缺 {label} ({k})")

        # 3. 路寬未抓到（關鍵 — 都更試算需要）
        if x.get("road_width_m") is None and not x.get("road_width_unknown"):
            issues.append(f"路寬完全沒值且沒標 unknown")
        elif x.get("road_width_unknown"):
            issues.append(f"路寬未收錄（road_width_unknown=True）— 可能是新北小巷弄圖資未涵蓋")

        # 4. 地籍圖（新北市重大 bug）
        if x.get("city") == "新北市":
            if x.get("screenshot_roadwidth"):
                issues.append("⚠️ 新北市但有 screenshot_roadwidth — 截的是台北 zonemap 結果，可能無效圖")
            else:
                issues.append("新北市無 screenshot_roadwidth（前端不會顯示「地籍圖」按鈕）")

        # 5. 地坪
        if x.get("land_area_ping") in (None, 0):
            issues.append("缺地坪 (LVR 沒對到 OR DB 沒匯入)")

        # 6. zoning 異常值
        z = x.get("zoning") or ""
        if z and not any(t in z for t in ("住宅", "商業", "工業", "農業", "保護", "風景")):
            issues.append(f"zoning 異常: {z!r}（可能落在非實質分區）")

        # 7. 推址 confidence
        if addr_inf and not x.get("address_inferred_confidence"):
            issues.append("address_inferred 沒 confidence 標籤")

        issues_by_doc[doc_id] = (sources, x, issues)

    # 印出 audit 結果
    for doc_id, (sources, x, issues) in issues_by_doc.items():
        addr_full = (x.get("address_inferred") or x.get("address") or "")
        title = (x.get("title") or "")[:30]
        print(f"[{doc_id}] {x.get('source_id')} src={sources}")
        print(f"  {x.get('city')}{x.get('district')} | {addr_full[:40]} | 屋齡{x.get('building_age')} | "
              f"{x.get('floor')}/{x.get('total_floors')}F | 建{x.get('building_area_ping')}坪 | "
              f"{x.get('price_ntd', 0) // 10000 if x.get('price_ntd') else '?'}萬")
        print(f"  title: {title}")
        if issues:
            for i in issues:
                print(f"    ⚠ {i}")
        else:
            print(f"    ✓ 無明顯問題")
        print()

    # 統計
    total = len(issues_by_doc)
    issue_counter = {}
    for sources, x, issues in issues_by_doc.values():
        for i in issues:
            key = i.split("(")[0].split("[")[0].split(":")[0].strip()
            issue_counter[key] = issue_counter.get(key, 0) + 1
    print("=" * 60)
    print(f"統計：{total} 筆物件中問題分布")
    for k, c in sorted(issue_counter.items(), key=lambda t: -t[1]):
        print(f"  {c:2d} 筆 — {k}")


if __name__ == "__main__":
    main()
