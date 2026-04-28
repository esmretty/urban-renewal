"""把 (default) database 的所有資料複製到 backup-temp database。

執行前確認：
- 沒有正在跑的 scraper（避免 backup 中途資料變動）
- backup-temp database 已建立且為空
"""
import sys
import time
import pathlib

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from google.cloud import firestore
from google.oauth2 import service_account

PROJECT_ID = "urban-renewal-32f02"
SOURCE_DB = "(default)"
TARGET_DB = "backup-temp"
CRED_PATH = pathlib.Path(__file__).parent.parent / "firebase-credentials.json"

# 要複製的 top-level collections
TOP_LEVEL_COLLECTIONS = ["properties", "users", "settings", "scheduler_history",
                          "run_logs", "retry_queue", "line_notifications"]
# users 的 sub-collections
USER_SUBCOLLECTIONS = ["watchlist", "manual", "bookmarks"]

BATCH_SIZE = 400  # Firestore batch limit is 500


def make_client(database):
    creds = service_account.Credentials.from_service_account_file(str(CRED_PATH))
    return firestore.Client(project=PROJECT_ID, database=database, credentials=creds)


def copy_collection(src_client, tgt_client, collection_path, label=""):
    """把一個 collection 的所有 docs 從 src 複製到 tgt。
    collection_path 例：'properties' 或 'users/UID/watchlist'。
    回傳複製的 doc 數。"""
    src_col = src_client.collection(collection_path)
    tgt_col = tgt_client.collection(collection_path)

    docs = list(src_col.stream())
    if not docs:
        return 0

    count = 0
    batch = tgt_client.batch()
    batch_count = 0
    for doc in docs:
        data = doc.to_dict()
        tgt_ref = tgt_col.document(doc.id)
        batch.set(tgt_ref, data)
        batch_count += 1
        count += 1
        if batch_count >= BATCH_SIZE:
            batch.commit()
            batch = tgt_client.batch()
            batch_count = 0
            print(f"  {label} {collection_path}: {count} docs...")

    if batch_count > 0:
        batch.commit()

    return count


def main():
    print(f"Source: projects/{PROJECT_ID}/databases/{SOURCE_DB}")
    print(f"Target: projects/{PROJECT_ID}/databases/{TARGET_DB}")
    print()

    src = make_client(SOURCE_DB)
    tgt = make_client(TARGET_DB)

    # 安全檢查：target 一定要是空的
    print("[安全檢查] 確認 backup-temp 是空的...")
    for col in TOP_LEVEL_COLLECTIONS:
        existing = list(tgt.collection(col).limit(1).stream())
        if existing:
            print(f"  ✗ ABORT: backup-temp 的 {col} collection 已有資料！")
            print(f"    這個 script 不會覆蓋，請先清空 backup-temp 或建一個全新 db。")
            return 1
    print("  ✓ backup-temp 為空，可以複製\n")

    grand_total = 0
    start = time.time()

    for col in TOP_LEVEL_COLLECTIONS:
        print(f"=== {col} ===")
        if col == "users":
            # 1. 先複製 users 本身
            n = copy_collection(src, tgt, "users", label="users")
            print(f"  users 主資料：{n} 個用戶")
            grand_total += n

            # 2. 對每個 user 複製 sub-collections
            users = list(src.collection("users").stream())
            for u in users:
                uid = u.id
                for sub in USER_SUBCOLLECTIONS:
                    sub_path = f"users/{uid}/{sub}"
                    sub_n = copy_collection(src, tgt, sub_path, label=f"  {uid[:8]}")
                    if sub_n > 0:
                        print(f"  users/{uid[:12]}.../{sub}: {sub_n} docs")
                    grand_total += sub_n
        else:
            n = copy_collection(src, tgt, col, label=col)
            print(f"  {n} docs")
            grand_total += n
        print()

    elapsed = time.time() - start
    print(f"=== 完成 ===")
    print(f"總複製 docs: {grand_total}")
    print(f"耗時: {elapsed:.1f} 秒")
    return 0


if __name__ == "__main__":
    sys.exit(main())
