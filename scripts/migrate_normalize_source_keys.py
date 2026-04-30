"""一次性 migration：把 source_keys[] 重建為 canonical 英文 name + 純 site_id。

問題：
- sources[].name 有時用中文 ("永慶"/"信義")、有時用英文 ("yongqing"/"sinyi")
- sources[].source_id 有時帶 prefix ("yongqing_X")、有時不帶 ("X")
- 結果 source_keys 同一物件存兩種格式 ("永慶:yongqing_X" 跟 "yongqing:X")
- check_exists 透過 source_keys array_contains query，兩種 key 不互通 → 漏掉既有物件

修法：
- 用 compute_source_keys() (新版含 canonical mapping + prefix strip) 重建所有 doc 的 source_keys
- sources[] 不動（保留中文 name 給前端 badge 用）
- source_keys[] 重建為純英文 canonical key

執行：
  python scripts/migrate_normalize_source_keys.py            # dry-run
  python scripts/migrate_normalize_source_keys.py --apply
"""
import sys
import argparse
sys.path.insert(0, r'd:\Coding\urban-renewal')

from database.db import init_db, get_col
from database.models import compute_source_keys


def main(apply: bool):
    init_db()
    col = get_col()
    docs = list(col.stream())
    print(f"total docs: {len(docs)}")

    plan = []
    for d in docs:
        x = d.to_dict() or {}
        old_keys = list(x.get("source_keys") or [])
        sources = x.get("sources") or []
        new_keys = compute_source_keys(sources)
        if old_keys != new_keys:
            plan.append((d.id, old_keys, new_keys))

    print(f"\n=== 需要 update 的 doc: {len(plan)} ===")
    for did, old, new in plan[:30]:
        print(f"  {did}")
        print(f"    舊: {old}")
        print(f"    新: {new}")
    if len(plan) > 30:
        print(f"  ... ({len(plan) - 30} more)")

    if not apply:
        print("\n[dry-run] 加 --apply 才會真寫")
        return

    print("\n=== APPLYING ===")
    n = 0
    for did, _, new_keys in plan:
        col.document(did).update({"source_keys": new_keys})
        n += 1
    print(f"done: updated {n} docs")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    main(apply=args.apply)
