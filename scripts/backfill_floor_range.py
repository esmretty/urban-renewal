"""Backfill 既有 doc 的 floor_range_min / floor_range_max。

對每筆 doc parse_floor_range(doc.floor, doc.total_floors)，補寫 floor_range_min / max
（也順便把樓中樓字串「1F~2F/4F」內含的總樓層補回 doc.total_floors，若原本沒值）。

中央 properties + 各用戶 manual / bookmarks 都跑。
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def backfill_collection(col, label):
    from database.models import parse_floor_range
    docs = list(col.stream())
    n_total = len(docs)
    n_updated = 0
    n_total_filled = 0
    n_floor_normalized = 0
    for d in docs:
        dd = d.to_dict() or {}
        floor = dd.get("floor")
        total = dd.get("total_floors")
        if not floor and not total:
            continue
        fmin, fmax, ftot = parse_floor_range(floor, total)
        updates = {}
        # 1) floor_range_min/max
        if fmin is not None and dd.get("floor_range_min") != fmin:
            updates["floor_range_min"] = fmin
        if fmax is not None and dd.get("floor_range_max") != fmax:
            updates["floor_range_max"] = fmax
        # 2) total_floors (樓中樓常缺值，從 / 後抽)
        if ftot is not None and not dd.get("total_floors") and ftot != total:
            updates["total_floors"] = ftot
            n_total_filled += 1
        # 3) floor 欄位 normalize 成 int 或 None（不再存「1F」「4F」「1F~2F/4F」這種字串）
        if fmin is not None and fmax is not None and fmin == fmax:
            normalized_floor = fmin
        else:
            normalized_floor = None
        if floor != normalized_floor:
            updates["floor"] = normalized_floor
            n_floor_normalized += 1
        if updates:
            try:
                col.document(d.id).update(updates)
                n_updated += 1
            except Exception as e:
                logger.warning(f"  update fail {d.id}: {e}")
    logger.info(f"[{label}] {n_total} 筆 / 更新 {n_updated} / 補 total_floors {n_total_filled} / floor normalize {n_floor_normalized}")


def main():
    from database.db import get_firestore
    db = get_firestore()
    backfill_collection(db.collection("properties"), "central properties")

    # 跑每個 user 的 manual + bookmarks
    users = list(db.collection("users").stream())
    for u in users:
        uid = u.id
        backfill_collection(db.collection("users").document(uid).collection("manual"), f"user.{uid[:8]}.manual")
        backfill_collection(db.collection("users").document(uid).collection("bookmarks"), f"user.{uid[:8]}.bookmarks")


if __name__ == "__main__":
    main()
