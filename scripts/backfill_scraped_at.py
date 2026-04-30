"""Backfill scraped_at = min(scraped_at, all sources[].added_at)
修「reanalyze 把 scraped_at 刷新成 now」造成 doc 在前端 _added_at 排序跑到最前面的 bug。

執行：
  python scripts/backfill_scraped_at.py            # dry-run
  python scripts/backfill_scraped_at.py --apply
"""
import sys, argparse
sys.path.insert(0, r'd:\Coding\urban-renewal')
from database.db import init_db, get_col


def main(apply: bool):
    init_db()
    col = get_col()
    docs = list(col.stream())
    print(f"total docs: {len(docs)}")

    plan = []
    for d in docs:
        x = d.to_dict() or {}
        sources = x.get("sources") or []
        added_ats = [s.get("added_at") for s in sources if s.get("added_at")]
        if not added_ats:
            continue
        earliest_source_added = min(added_ats)
        scraped_at = x.get("scraped_at")
        # 如果 scraped_at 比最早 source.added_at 更晚 → 修正
        if scraped_at and earliest_source_added and scraped_at > earliest_source_added:
            plan.append({
                "doc_id": d.id,
                "old_scraped_at": scraped_at,
                "new_scraped_at": earliest_source_added,
                "address": x.get("address") or x.get("address_inferred") or "",
            })

    print(f"\n=== 需要 backfill 的 doc: {len(plan)} ===")
    for p in plan[:30]:
        print(f"  {p['doc_id']} {p['address'][:18]:20} {p['old_scraped_at'][:19]} → {p['new_scraped_at'][:19]}")
    if len(plan) > 30:
        print(f"  ... ({len(plan) - 30} more)")

    if not apply:
        print("\n[dry-run] 加 --apply 才會真寫")
        return

    print("\n=== APPLYING ===")
    n = 0
    for p in plan:
        col.document(p["doc_id"]).update({"scraped_at": p["new_scraped_at"]})
        n += 1
    print(f"done: updated {n} docs")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    main(apply=args.apply)
