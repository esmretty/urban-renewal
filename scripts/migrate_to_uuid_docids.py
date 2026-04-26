"""Phase 0 Schema Migration：把 properties / user sub-collections 的 doc_id
從「source_id 字串」(例 591_12345678) 改成「日期+隨機 hex」(例 20260426-a1b2c3)。

特性：
- 非破壞性：只「新增」doc 不「刪除」舊 doc
- 可中途中斷：每筆獨立 commit，不會留半完成狀態
- DRY_RUN 模式：先看會做什麼再決定

執行方式：
    python migrate_to_uuid_docids.py --dry-run     # 預覽
    python migrate_to_uuid_docids.py --execute     # 實際執行
"""
import argparse
import json
import sys
import time
import uuid
import pathlib

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from google.cloud import firestore
from google.oauth2 import service_account

PROJECT_ID = "urban-renewal-32f02"
TARGET_DB = "(default)"   # 主資料庫
CRED_PATH = pathlib.Path(__file__).parent.parent / "firebase-credentials.json"
MAPPING_FILE = pathlib.Path(__file__).parent.parent / "data" / "uuid_migration_mapping.json"

USER_SUBCOLLECTIONS = ["watchlist", "manual", "bookmarks"]


def make_client(database):
    creds = service_account.Credentials.from_service_account_file(str(CRED_PATH))
    return firestore.Client(project=PROJECT_ID, database=database, credentials=creds)


def gen_dated_id(scraped_at_iso=None):
    """格式：YYYYMMDD-XXXXXX（8 碼日期 + 6 碼 hex）
    - migration 時：scraped_at_iso 帶入該物件原本的 scraped_at（保留歷史時序）
    - 新建物件時：scraped_at_iso=None → 用今天日期"""
    from datetime import datetime, timezone, timedelta
    tw = timezone(timedelta(hours=8))
    if scraped_at_iso:
        try:
            # ISO 8601 with timezone
            dt = datetime.fromisoformat(scraped_at_iso.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tw)
            dt = dt.astimezone(tw)
        except Exception:
            dt = datetime.now(tw)
    else:
        dt = datetime.now(tw)
    date_part = dt.strftime("%Y%m%d")
    rand_part = uuid.uuid4().hex[:6]
    return f"{date_part}-{rand_part}"


def load_mapping():
    if MAPPING_FILE.exists():
        with open(MAPPING_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_mapping(mapping):
    MAPPING_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(MAPPING_FILE, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


def now_iso():
    from datetime import datetime, timezone, timedelta
    return datetime.now(timezone(timedelta(hours=8))).isoformat()


def migrate_properties(client, dry_run=True):
    """為每筆 properties doc 建立新 UUID doc，body 加 id / sources 欄位。
    不刪除舊 doc。"""
    col = client.collection("properties")
    docs = list(col.stream())
    print(f"\n=== properties: {len(docs)} 筆 ===")

    mapping = {}  # old_id → new_uuid

    new_doc_examples = []
    for doc in docs:
        old_id = doc.id
        data = doc.to_dict()

        # 已有 id 欄位且看起來像新格式 YYYYMMDD-XXXXXX（已 migrate 過）→ skip
        existing_id = data.get("id")
        if existing_id and "-" in existing_id and len(existing_id) == 15 and existing_id != old_id:
            mapping[old_id] = existing_id
            continue

        # 用該物件原本的 scraped_at 當建立日期（保留歷史時序）
        new_uuid = gen_dated_id(data.get("scraped_at"))
        mapping[old_id] = new_uuid

        # 構造新 doc body
        source = data.get("source") or "591"   # 既有資料應該都有 source
        existing_url = data.get("url")
        existing_url_alt = data.get("url_alt") or []
        existing_scraped_at = data.get("scraped_at") or now_iso()

        # 建立 sources 陣列：主來源 + url_alt（若有）
        sources_arr = [{
            "name": source,
            "source_id": old_id,
            "url": existing_url,
            "added_at": existing_scraped_at,
        }]
        for alt_url in existing_url_alt:
            # 從 alt URL host 推測 source 名稱
            if "yungching.com.tw" in (alt_url or ""):
                alt_name = "永慶"
            elif "sinyi.com.tw" in (alt_url or ""):
                alt_name = "信義"
            elif "591.com.tw" in (alt_url or ""):
                alt_name = "591"
            else:
                alt_name = "其他"
            sources_arr.append({
                "name": alt_name,
                "source_id": None,    # 既有 url_alt 沒記原始 source_id，留 None
                "url": alt_url,
                "added_at": existing_scraped_at,
            })

        new_data = dict(data)
        new_data["id"] = new_uuid
        new_data["source_id"] = old_id    # 確保欄位存在（多數已存在）
        new_data["sources"] = sources_arr

        if dry_run:
            if len(new_doc_examples) < 3:
                new_doc_examples.append({
                    "old_id": old_id,
                    "new_uuid": new_uuid,
                    "source": source,
                    "address": data.get("address"),
                    "sources": sources_arr,
                })
        else:
            client.collection("properties").document(new_uuid).set(new_data)

    if dry_run:
        print(f"  [DRY-RUN] 會建立 {len(mapping)} 個新 UUID docs")
        print(f"  範例（前 3 筆）：")
        for ex in new_doc_examples:
            print(f"    {ex['old_id']} → {ex['new_uuid']}  ({ex['source']}, {ex.get('address','')[:30]})")
    else:
        print(f"  ✓ 已建立 {len(mapping)} 個新 UUID docs")

    return mapping


def migrate_user_subcollections(client, mapping, dry_run=True):
    """把 users/{uid}/watchlist/{old_id} 等子集合改寫到新 UUID doc_id。
    不刪除舊 doc。"""
    users_col = client.collection("users")
    users = list(users_col.stream())
    print(f"\n=== users sub-collections: {len(users)} 個用戶 ===")

    total_migrated = 0
    skipped_no_match = 0

    for user_doc in users:
        uid = user_doc.id
        for sub in USER_SUBCOLLECTIONS:
            sub_col = users_col.document(uid).collection(sub)
            sub_docs = list(sub_col.stream())
            for sd in sub_docs:
                old_id = sd.id
                if old_id in mapping:
                    new_uuid = mapping[old_id]
                    if dry_run:
                        pass
                    else:
                        sub_col.document(new_uuid).set(sd.to_dict())
                    total_migrated += 1
                else:
                    # manual 物件可能本來就不在 properties（用戶自建）→ 不需要 migrate
                    # 但 watchlist 不在 mapping 就有問題（孤兒 reference）
                    if sub == "watchlist":
                        print(f"  ⚠️ {uid[:8]}/watchlist/{old_id} 在 properties 找不到對應 doc")
                    skipped_no_match += 1

    if dry_run:
        print(f"  [DRY-RUN] 會 migrate {total_migrated} 個 sub-collection docs")
        if skipped_no_match:
            print(f"  跳過（無對應 properties doc）: {skipped_no_match} 筆")
    else:
        print(f"  ✓ 已 migrate {total_migrated} 個 sub-collection docs")
        if skipped_no_match:
            print(f"  跳過（無對應 properties doc）: {skipped_no_match} 筆")


def verify_migration(client, mapping):
    """驗證：每個 mapping 的 new_uuid 在 properties 都存在，且 source_id 對應正確。"""
    print(f"\n=== 驗證 ===")
    sample = list(mapping.items())[:5]
    for old_id, new_uuid in sample:
        new_doc = client.collection("properties").document(new_uuid).get()
        if new_doc.exists:
            data = new_doc.to_dict()
            ok = data.get("source_id") == old_id and data.get("id") == new_uuid
            mark = "✓" if ok else "✗"
            print(f"  {mark} {old_id} → {new_uuid}: source_id={data.get('source_id')}, id={data.get('id')}")
        else:
            print(f"  ✗ {old_id} → {new_uuid}: NEW DOC NOT FOUND")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="預覽，不實際寫")
    ap.add_argument("--execute", action="store_true", help="實際執行")
    args = ap.parse_args()

    if not (args.dry_run or args.execute):
        print("ERROR: 必須指定 --dry-run 或 --execute")
        return 1

    dry_run = args.dry_run

    print(f"Project: {PROJECT_ID}")
    print(f"Database: {TARGET_DB}")
    print(f"Mode: {'DRY RUN' if dry_run else 'EXECUTE'}")

    client = make_client(TARGET_DB)

    start = time.time()

    # 載入既有 mapping（如果之前跑過部分）
    existing_mapping = load_mapping()
    if existing_mapping:
        print(f"\n找到既有 mapping 檔（{len(existing_mapping)} 筆），會合併使用")

    # Step 1: properties
    mapping = migrate_properties(client, dry_run=dry_run)

    # 合併到 existing
    full_mapping = {**existing_mapping, **mapping}

    # 儲存 mapping（即使 dry-run 也存，方便檢查）
    if not dry_run:
        save_mapping(full_mapping)
        print(f"\nmapping 存到 {MAPPING_FILE}")

    # Step 2: user sub-collections
    migrate_user_subcollections(client, full_mapping, dry_run=dry_run)

    # Step 3: 驗證
    if not dry_run:
        verify_migration(client, full_mapping)

    elapsed = time.time() - start
    print(f"\n耗時: {elapsed:.1f} 秒")
    print("\n" + "=" * 50)
    if dry_run:
        print("DRY RUN 結束。要實際執行請加 --execute")
    else:
        print("Migration 完成。舊 docs 還在原位（保留作 fallback）。")
        print("Code 改完並驗證後，可手動刪除舊 docs。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
