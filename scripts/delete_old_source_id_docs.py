"""刪除 migration 後的舊 doc（doc_id = source_id 格式如 591_xxx）。

前置條件：
- migration script 已跑完，新 UUID docs 已建立
- backup-temp 已備份完整資料

特性：
- 預設 dry-run 只列出會刪什麼，不實際刪
- --execute 才真的刪
- 安全檢查：每筆舊 doc 都要確認新 UUID doc 存在且 source_id 匹配
"""
import argparse
import json
import sys
import time
import pathlib

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from google.cloud import firestore
from google.oauth2 import service_account

PROJECT_ID = "urban-renewal-32f02"
TARGET_DB = "(default)"
CRED_PATH = pathlib.Path(__file__).parent.parent / "firebase-credentials.json"
MAPPING_FILE = pathlib.Path(__file__).parent.parent / "data" / "uuid_migration_mapping.json"

USER_SUBCOLLECTIONS = ["watchlist", "manual", "bookmarks"]


def make_client(database):
    creds = service_account.Credentials.from_service_account_file(str(CRED_PATH))
    return firestore.Client(project=PROJECT_ID, database=database, credentials=creds)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="預覽，不實際刪")
    ap.add_argument("--execute", action="store_true", help="實際執行刪除")
    args = ap.parse_args()

    if not (args.dry_run or args.execute):
        print("ERROR: 必須指定 --dry-run 或 --execute")
        return 1

    dry_run = args.dry_run
    print(f"Mode: {'DRY RUN' if dry_run else 'EXECUTE'}")

    # 載入 migration mapping
    if not MAPPING_FILE.exists():
        print(f"ERROR: 找不到 mapping 檔 {MAPPING_FILE}，請先跑 migration")
        return 1
    with open(MAPPING_FILE, "r", encoding="utf-8") as f:
        mapping = json.load(f)
    print(f"載入 {len(mapping)} 筆 old_id → new_uuid mapping\n")

    client = make_client(TARGET_DB)
    col = client.collection("properties")

    # 安全檢查 + 刪除
    to_delete_props = []
    to_delete_subs = []
    issues = []

    print("=== 安全檢查 properties ===")
    for old_id, new_uuid in mapping.items():
        # 1. 確認新 UUID doc 存在
        new_doc = col.document(new_uuid).get()
        if not new_doc.exists:
            issues.append(f"新 doc 不存在 {old_id}→{new_uuid}，不刪")
            continue
        # 2. 確認 source_id 欄位正確
        new_data = new_doc.to_dict()
        if new_data.get("source_id") != old_id:
            issues.append(f"新 doc {new_uuid} 的 source_id={new_data.get('source_id')} 不等於 {old_id}，不刪")
            continue
        # 3. 確認 id 欄位正確
        if new_data.get("id") != new_uuid:
            issues.append(f"新 doc {new_uuid} 的 id={new_data.get('id')} 不等於自身，不刪")
            continue
        # 4. 確認舊 doc 還在
        old_doc = col.document(old_id).get()
        if not old_doc.exists:
            issues.append(f"舊 doc {old_id} 已不存在（也許之前刪過）")
            continue
        to_delete_props.append(old_id)

    print(f"  ✓ {len(to_delete_props)} 筆舊 properties 通過檢查可以刪")
    if issues:
        print(f"  ⚠️ {len(issues)} 筆有問題，跳過：")
        for i in issues[:10]:
            print(f"    {i}")

    print()
    print("=== 安全檢查 user sub-collections ===")
    users_col = client.collection("users")
    users = list(users_col.stream())
    for user_doc in users:
        uid = user_doc.id
        for sub in USER_SUBCOLLECTIONS:
            sub_col = users_col.document(uid).collection(sub)
            for sd in sub_col.stream():
                old_id = sd.id
                if old_id not in mapping:
                    continue   # 孤兒 / 自建物件，不動
                new_uuid = mapping[old_id]
                # 確認 new_uuid sub-doc 存在
                new_sub_doc = sub_col.document(new_uuid).get()
                if not new_sub_doc.exists:
                    issues.append(f"user {uid[:8]}/{sub}/{new_uuid} 不存在，跳過刪除 {old_id}")
                    continue
                to_delete_subs.append((uid, sub, old_id))

    print(f"  ✓ {len(to_delete_subs)} 筆舊 sub-coll docs 通過檢查可以刪")

    if dry_run:
        print(f"\n[DRY RUN] 會刪除：")
        print(f"  - {len(to_delete_props)} 筆舊 properties (source_id 格式)")
        print(f"  - {len(to_delete_subs)} 筆舊 user sub-coll docs")
        print(f"\n（孤兒 watchlist + manual 完全不動）")
        print(f"\n要實際執行請加 --execute")
        return 0

    # 實際刪
    print(f"\n=== 實際執行 ===")
    start = time.time()

    BATCH = 400
    batch = client.batch()
    batch_count = 0
    deleted = 0
    for old_id in to_delete_props:
        batch.delete(col.document(old_id))
        batch_count += 1
        deleted += 1
        if batch_count >= BATCH:
            batch.commit()
            batch = client.batch()
            batch_count = 0
            print(f"  刪除 properties... {deleted}")
    if batch_count > 0:
        batch.commit()
    print(f"  ✓ 刪除 properties {deleted} 筆")

    # 刪 sub-coll
    deleted_subs = 0
    batch = client.batch()
    batch_count = 0
    for uid, sub, old_id in to_delete_subs:
        batch.delete(users_col.document(uid).collection(sub).document(old_id))
        batch_count += 1
        deleted_subs += 1
        if batch_count >= BATCH:
            batch.commit()
            batch = client.batch()
            batch_count = 0
    if batch_count > 0:
        batch.commit()
    print(f"  ✓ 刪除 sub-coll {deleted_subs} 筆")

    elapsed = time.time() - start
    print(f"\n耗時: {elapsed:.1f} 秒")
    print("\n剩餘狀態：")
    remaining = sum(1 for _ in col.stream())
    print(f"  properties: {remaining} 筆（應該都是新 UUID 格式）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
