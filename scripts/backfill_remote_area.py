"""Backfill is_remote_area 給既有 properties。

對每筆 doc：
  - district 不在 REMOTE_POLYGONS_NEW_TAIPEI 鍵裡 → 設 False（若沒設過）
  - 沒 lat/lng → 設 False（無法判定）
  - 有 lat/lng 且 district 有 polygon → 跑 polygon test
    True/False 跟 doc 既有不同 → 列入計畫

執行：
  python scripts/backfill_remote_area.py            # dry-run
  python scripts/backfill_remote_area.py --apply    # 實際寫
"""
import sys
import argparse
sys.path.insert(0, r'd:\Coding\urban-renewal')

from database.db import init_db, get_col
from analysis.geocoder import is_remote_area_new_taipei
from config import REMOTE_POLYGONS_NEW_TAIPEI


def main(apply: bool):
    init_db()
    col = get_col()
    docs = list(col.stream())
    print(f"total docs: {len(docs)}")

    target_districts = set(REMOTE_POLYGONS_NEW_TAIPEI.keys())
    print(f"目標區（含 polygon）: {sorted(target_districts)}")

    to_set_true = []     # district 有 polygon + 在 polygon 內 + 原本不是 True
    to_fix_false = []    # district 有 polygon + 不在 polygon 內 + 原本誤標 True (修正)
    to_set_init = []     # 從未設過 is_remote_area，落定 False
    skipped_no_coord = 0
    skipped_already_correct = 0

    for d in docs:
        x = d.to_dict() or {}
        district = x.get("district")
        lat = x.get("latitude") or x.get("lat")
        lng = x.get("longitude") or x.get("lng")
        old = x.get("is_remote_area")

        if district in target_districts:
            if not lat or not lng:
                skipped_no_coord += 1
                continue
            new = is_remote_area_new_taipei(lat, lng, district)
            if new == old:
                skipped_already_correct += 1
                continue
            if new and old is not True:
                to_set_true.append((d.id, district, lat, lng, x.get("address") or x.get("address_inferred") or "(無)"))
            elif (not new) and old is True:
                to_fix_false.append((d.id, district, lat, lng, x.get("address") or x.get("address_inferred") or "(無)"))
            elif old is None:
                to_set_init.append(d.id)
        else:
            # 非目標區 — 確保有 is_remote_area=False（schema 一致性）
            if old is None:
                to_set_init.append(d.id)

    print(f"\n=== Plan ===")
    print(f"標 True (新偏遠): {len(to_set_true)} 筆")
    for did, dist, lat, lng, addr in to_set_true[:30]:
        print(f"  {did} [{dist}] ({lat:.4f},{lng:.4f}) {addr[:40]}")
    if len(to_set_true) > 30:
        print(f"  ... ({len(to_set_true) - 30} more)")

    print(f"\n修誤標 (本不偏遠 但 DB 標 True): {len(to_fix_false)} 筆")
    for did, dist, lat, lng, addr in to_fix_false[:10]:
        print(f"  {did} [{dist}] ({lat:.4f},{lng:.4f}) {addr[:40]}")

    print(f"\n初始化為 False (DB 沒此欄位 → 落定): {len(to_set_init)} 筆")
    print(f"無座標跳過: {skipped_no_coord}")
    print(f"已正確跳過: {skipped_already_correct}")

    if not apply:
        print("\n[dry-run] 加 --apply 才會真寫")
        return

    print("\n=== APPLYING ===")
    n = 0
    for did, _dist, _lat, _lng, _addr in to_set_true:
        col.document(did).update({"is_remote_area": True})
        n += 1
    for did, _dist, _lat, _lng, _addr in to_fix_false:
        col.document(did).update({"is_remote_area": False})
        n += 1
    for did in to_set_init:
        col.document(did).update({"is_remote_area": False})
        n += 1
    print(f"done: updated {n} docs")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    main(apply=args.apply)
