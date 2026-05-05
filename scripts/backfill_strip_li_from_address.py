"""一次性 backfill：把 DB 既有 doc 的 address / address_inferred / title 中
被 Google reverse-geocode 污染的「X里」前綴 / 中綴剝掉。

執行：python scripts/backfill_strip_li_from_address.py [--apply]
不加 --apply 是 dry-run（只印影響，不寫 DB）。
"""
import os, sys, re, argparse

os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "firebase-credentials.json")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from database.db import init_db, get_col

ADDR_FIELDS = ("address", "address_inferred", "title")
LI_LEAD = re.compile(r"^[一-龥]{1,4}里")
LI_AFTER_DIST = re.compile(r"(區)[一-龥]{1,4}里")


def clean(s: str) -> str:
    if not s:
        return s
    s2 = LI_AFTER_DIST.sub(r"\1", s)
    s2 = LI_LEAD.sub("", s2)
    return s2


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="真寫 DB；省略則只 dry-run")
    args = p.parse_args()

    init_db()
    col = get_col()
    docs = list(col.get())

    affected = []
    for d in docs:
        data = d.to_dict() or {}
        updates = {}
        for f in ADDR_FIELDS:
            v = data.get(f) or ""
            if not v:
                continue
            v2 = clean(v)
            if v != v2:
                updates[f] = (v, v2)
        if updates:
            affected.append((d.id, updates))

    print(f"掃 {len(docs)} 筆，需要更新 {len(affected)} 筆")
    for doc_id, ups in affected:
        print(f"  [{doc_id}]")
        for f, (before, after) in ups.items():
            print(f"    {f}: {before!r}\n      → {after!r}")

    if not affected:
        print("沒有 doc 需要更新")
        return

    if not args.apply:
        print("\n(dry-run；加 --apply 才真寫 DB)")
        return

    for doc_id, ups in affected:
        new_vals = {f: after for f, (_b, after) in ups.items()}
        col.document(doc_id).update(new_vals)
    print(f"\n已 update {len(affected)} 筆 doc")


if __name__ == "__main__":
    main()
