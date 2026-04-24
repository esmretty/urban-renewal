"""一次性 migration：把中央 DB 所有 address / address_inferred 從「含 city/district 前綴」
轉為「純地址」。城市/行政區另存 city / district 欄位。

用法：
    python scripts/migrate_strip_address_prefix.py --dry-run   # 先看會改哪些
    python scripts/migrate_strip_address_prefix.py             # 實際寫入
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database.db import get_col
from database.models import strip_region_prefix


def main(dry_run: bool):
    col = get_col()
    total = 0
    changed = 0
    samples = []
    for d in col.get():
        total += 1
        doc = d.to_dict() or {}
        city = doc.get("city") or ""
        district = doc.get("district") or ""
        updates = {}
        for field in ("address", "address_inferred"):
            old = doc.get(field)
            if not old:
                continue
            new = strip_region_prefix(old, city, district)
            if new != old:
                updates[field] = new
        if updates:
            changed += 1
            if len(samples) < 20:
                samples.append((d.id, updates, {"city": city, "district": district}))
            if not dry_run:
                col.document(d.id).update(updates)

    print(f"\n總 docs: {total}")
    print(f"需要更新: {changed}")
    print("\n前 20 筆樣本：")
    for sid, upd, loc in samples:
        print(f"  [{sid}] city={loc['city']} district={loc['district']}")
        for k, v in upd.items():
            print(f"    {k}: → {v!r}")
    if dry_run:
        print("\n(dry-run, 實際未寫入)")
    else:
        print(f"\n✅ 已更新 {changed} 筆")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只掃不寫")
    args = ap.parse_args()
    main(args.dry_run)
