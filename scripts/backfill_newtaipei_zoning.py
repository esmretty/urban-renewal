"""Backfill 新北市物件的 zoning + 都更可行性閘門。

對每筆 district ∈ {板橋區, 新店區, 中和區, 永和區} 的 doc：
1. 重新跑 query_zoning_newtaipei(lat, lng) 拿正確分區（修 _build_ntpc_zone_entry / is_real_zone bug）
2. 比對 SUITABLE_ZONING_FOR_RENEWAL_NEW_TAIPEI 判定是否適合都更
3. 不適合 → 標 unsuitable_for_renewal=True + 清都更/評分/AI 欄位

執行：
  python scripts/backfill_newtaipei_zoning.py            # dry-run
  python scripts/backfill_newtaipei_zoning.py --apply
"""
import sys
import argparse
sys.path.insert(0, r'd:\Coding\urban-renewal')

from database.db import init_db, get_col
from analysis.gov_gis import query_zoning_newtaipei
from analysis.scorer import is_zoning_suitable_for_renewal


CLEAR_FIELDS_ON_UNSUITABLE = (
    "score_total", "score_age", "score_far", "score_land",
    "score_tod", "score_road", "score_consolidation",
    "renewal_type", "renewal_bonus_rate",
    "renewal_new_area_ping", "renewal_value_ntd", "renewal_profit_ntd",
    "ai_analysis", "ai_recommendation", "ai_reason",
)


def main(apply: bool):
    init_db()
    col = get_col()
    nt_districts = {"板橋區", "新店區", "中和區", "永和區"}
    docs = list(col.stream())
    print(f"total docs: {len(docs)}")

    nt_docs = [(d, d.to_dict() or {}) for d in docs
               if (d.to_dict() or {}).get("district") in nt_districts]
    print(f"新北 4 區 docs: {len(nt_docs)}")

    plan_all = []   # 所有需要 update 的 entries
    plan_zoning_changed = []
    plan_to_unsuitable = []
    plan_back_to_suitable = []
    no_coord = []
    query_failed = []

    for d, x in nt_docs:
        lat = x.get("latitude")
        lng = x.get("longitude")
        district = x.get("district")
        old_zoning = x.get("zoning")
        old_zone_list = x.get("zoning_list")
        old_unsuitable = bool(x.get("unsuitable_for_renewal"))

        if not (lat and lng):
            no_coord.append((d.id, district, x.get("address")))
            continue

        try:
            z = query_zoning_newtaipei(lat, lng)
        except Exception as e:
            query_failed.append((d.id, district, str(e)[:60]))
            continue

        if not z:
            query_failed.append((d.id, district, "no result"))
            continue

        new_zone_list = z.get("zone_list") or ([z.get("zone_name")] if z.get("zone_name") else [])
        # zoning 顯示：多分區用「、」連接全列出
        if len(new_zone_list) > 1:
            new_zoning = "、".join(new_zone_list)
        else:
            new_zoning = new_zone_list[0] if new_zone_list else None
        new_orig = z.get("original_zone")
        # suitability：傳 list，任一在 SUITABLE 都算 suitable
        suitable, reason = is_zoning_suitable_for_renewal(district, new_zone_list)
        new_unsuitable = not suitable

        zoning_diff = (old_zoning != new_zoning)
        zone_list_diff = (list(old_zone_list or []) != new_zone_list)
        unsuitable_diff = (old_unsuitable != new_unsuitable)

        if zoning_diff or zone_list_diff or unsuitable_diff:
            entry = {
                "doc_id": d.id, "district": district,
                "address": x.get("address") or "",
                "old_zoning": old_zoning, "new_zoning": new_zoning,
                "new_zone_list": new_zone_list,
                "new_orig": new_orig,
                "zoning_diff": zoning_diff,
                "zone_list_diff": zone_list_diff,
                "old_unsuitable": old_unsuitable, "new_unsuitable": new_unsuitable,
                "reason": reason,
            }
            plan_all.append(entry)
            if zoning_diff:
                plan_zoning_changed.append(entry)
            if new_unsuitable and not old_unsuitable:
                plan_to_unsuitable.append(entry)
            elif not new_unsuitable and old_unsuitable:
                plan_back_to_suitable.append(entry)

    print(f"\n=== 統計 ===")
    print(f"需要 update 的 doc: {len(plan_all)}")
    print(f"  分區值有變化: {len(plan_zoning_changed)}")
    print(f"  新標為 unsuitable: {len(plan_to_unsuitable)}")
    print(f"  從 unsuitable 變回 suitable: {len(plan_back_to_suitable)}")
    print(f"無座標跳過: {len(no_coord)}")
    print(f"查詢失敗: {len(query_failed)}")

    print(f"\n=== 詳情 ===")
    for e in plan_all:
        marks = []
        if e["zoning_diff"]:
            marks.append(f"分區 {e['old_zoning']!r}→{e['new_zoning']!r}")
        if e["new_unsuitable"] and not e["old_unsuitable"]:
            marks.append("→不適合都更")
        elif e["old_unsuitable"] and not e["new_unsuitable"]:
            marks.append("→變回適合")
        print(f"  [{e['doc_id']}] {e['district']} {e['address'][:18]:18}: {' / '.join(marks)}")

    if not apply:
        print(f"\n[dry-run] 加 --apply 才會真寫")
        return

    print(f"\n=== APPLYING ===")
    n_updated = 0
    for e in plan_all:
        updates = {}
        if e["zoning_diff"] or e.get("zone_list_diff"):
            updates["zoning"] = e["new_zoning"]
            updates["zoning_original"] = e["new_orig"]
            updates["zoning_list"] = e["new_zone_list"]
        if e["new_unsuitable"]:
            updates["unsuitable_for_renewal"] = True
            updates["unsuitable_reason"] = e["reason"]
            updates["analysis_status"] = "skipped"
            updates["skip_reason"] = "unsuitable_zoning"
            for f in CLEAR_FIELDS_ON_UNSUITABLE:
                updates[f] = None
        elif e["old_unsuitable"]:
            updates["unsuitable_for_renewal"] = False
            updates["unsuitable_reason"] = None
        if not updates:
            continue
        try:
            col.document(e["doc_id"]).update(updates)
            n_updated += 1
        except Exception as err:
            print(f"  ✗ {e['doc_id']}: {err}")
    print(f"\ndone: updated {n_updated} docs")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    main(apply=args.apply)
