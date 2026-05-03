"""量測 properties doc 各欄位的 size 占比 — 釐清「卡片需要 vs 不需要」。"""
import sys, json
sys.path.insert(0, ".")

from database.db import init_db, get_col
init_db()
col = get_col()

docs = list(col.where("district", "in", ["大安區", "信義區", "文山區"]).get())
print(f"總筆數: {len(docs)}")

field_sizes = {}
for d in docs:
    data = d.to_dict() or {}
    for k, v in data.items():
        sz = len(json.dumps(v, default=str, ensure_ascii=False))
        field_sizes.setdefault(k, []).append(sz)

agg = []
for k, sizes in field_sizes.items():
    agg.append({
        "field": k,
        "n_present": len(sizes),
        "avg_bytes": sum(sizes) / len(sizes),
        "total_kb": sum(sizes) / 1024,
    })
agg.sort(key=lambda x: -x["total_kb"])

total = sum(x["total_kb"] for x in agg)
print(f"全 docs 加總 JSON: {total:.0f} KB\n")

# 卡片真正用到的欄位
CARD_FIELDS = {
    "source_id", "id", "address", "address_inferred", "district", "city",
    "building_type", "price_ntd",
    "building_area_ping", "land_area_ping", "building_age",
    "total_floors", "floor", "floor_range_min", "floor_range_max",
    "zoning", "base_far_pct", "effective_far_pct", "zoning_list", "zoning_ratios",
    "road_width_m",
    "is_basement", "is_remote_area", "unsuitable_for_renewal", "is_foreclosure",
    "image_url", "sources", "archived",
    "new_house_price_wan_override", "floor_premium", "rebuild_coeff",
    "bonus_weishau", "bonus_dugen", "desired_price_wan",
    "_in_watchlist", "user_url", "added_at_user",
    "list_rank", "scrape_session_at", "published_at", "title",
    "deleted", "analysis_status", "analysis_error", "analysis_in_progress",
    "land_area_suspicious",
}

print(f"{'rank':<5}{'field':<40}{'avg(B)':>10}{'total(KB)':>12}{'%':>8}{'card?':>8}")
print("-" * 86)
card_total = 0
non_card_total = 0
for i, x in enumerate(agg[:40]):
    pct = x["total_kb"] / total * 100
    in_card = x["field"] in CARD_FIELDS
    if in_card: card_total += x["total_kb"]
    else: non_card_total += x["total_kb"]
    mark = "Y" if in_card else "."
    print(f"{i+1:<5}{x['field']:<40}{x['avg_bytes']:>10.0f}{x['total_kb']:>12.1f}{pct:>7.1f}%{mark:>8}")

# 全部加總（不只 top 40）
card_total = sum(x["total_kb"] for x in agg if x["field"] in CARD_FIELDS)
non_card_total = total - card_total
print(f"\n=== 卡片需要 vs 不需要 ===")
print(f"  卡片要 ({len([x for x in agg if x['field'] in CARD_FIELDS])} 欄位): {card_total:.0f} KB ({card_total/total*100:.0f}%)")
print(f"  其他   ({len([x for x in agg if x['field'] not in CARD_FIELDS])} 欄位): {non_card_total:.0f} KB ({non_card_total/total*100:.0f}%)")
print(f"\n如果只回 card 欄位：response 從 {total:.0f} KB → {card_total:.0f} KB（節省 {non_card_total/total*100:.0f}%）")
