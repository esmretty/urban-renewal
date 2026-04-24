"""
一次性 migration：把現有單人版 Firestore 資料拆成中央 properties + 我的 watchlist + 我的 manual。

流程：
  1. 輸入你的 Google uid（從 /api/me 取，或登入後 Firebase console 看）
  2. 掃 properties collection
  3. 每個 manual_* doc → 整包搬到 users/{uid}/manual/{id}，從中央刪
  4. 每個 591 source_id doc → 從中保留共用欄位在中央；把使用者 override 欄位搬到 users/{uid}/watchlist/{id}
  5. 保留原中央 doc 的共用分析結果（清掉 override 欄位）

使用：
  python -m scripts.migrate_to_multi_user --uid=<YOUR_UID> [--dry-run]
"""
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from database.db import init_db, get_col, get_user_watchlist, get_user_manual, get_user_doc
from database.models import USER_OVERRIDE_FIELDS


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uid", required=True, help="你的 Firebase uid（用 /api/me 可取）")
    ap.add_argument("--email", default="", help="（可選）你的 email，寫入 user profile")
    ap.add_argument("--dry-run", action="store_true", help="只列出會搬什麼，不寫入")
    args = ap.parse_args()

    init_db()
    col = get_col()
    watch_col = get_user_watchlist(args.uid)
    manual_col = get_user_manual(args.uid)

    # 建 user profile（若不存在）
    user_ref = get_user_doc(args.uid)
    if not args.dry_run:
        user_ref.set({
            "email": args.email,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "migrated_at": datetime.now(timezone.utc).isoformat(),
        }, merge=True)

    docs = list(col.get())
    print(f"=== Migration: 共 {len(docs)} 筆 central docs ===")
    if args.dry_run:
        print("  (DRY RUN — 不會寫入)")

    moved_manual = 0
    split_watchlist = 0
    already_clean = 0

    for d in docs:
        src_id = d.id
        data = d.to_dict() or {}

        if src_id.startswith("manual_"):
            # 整筆搬到 users/{uid}/manual
            if not args.dry_run:
                manual_col.document(src_id).set(data)
                col.document(src_id).delete()
            moved_manual += 1
            print(f"  [manual] {src_id} → users/{args.uid}/manual/")
            continue

        # 591 物件：拆出 override 欄位到 watchlist
        watch_doc = {k: data[k] for k in USER_OVERRIDE_FIELDS if k in data and data[k] is not None}
        # 沒任何 override 就不建 watchlist，但既然 doc 在中央 collection 表示這物件我抓過，
        # 還是建一筆空引用（added_at=當下）讓 Home 看得到
        if "added_at" not in watch_doc:
            watch_doc["added_at"] = data.get("scraped_at") or datetime.now(timezone.utc).isoformat()

        if not args.dry_run:
            watch_col.document(src_id).set(watch_doc)
            # 從中央 doc 清掉 override 欄位
            updates = {k: None for k in USER_OVERRIDE_FIELDS if k in data}
            if updates:
                col.document(src_id).update(updates)
        split_watchlist += 1
        kept = list(watch_doc.keys())
        print(f"  [split ] {src_id} → watchlist 欄位={kept}")

    print("\n=== Migration 結果 ===")
    print(f"  manual_* 搬到 users/{args.uid}/manual: {moved_manual}")
    print(f"  591 source_id 拆到 watchlist: {split_watchlist}")
    print(f"  中央 properties 總筆數（搬後）: {len(docs) - moved_manual}")


if __name__ == "__main__":
    main()
