"""調查孤兒 watchlist：用戶收藏了但 properties 找不到對應的物件。"""
import sys
import pathlib
import json

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from google.cloud import firestore
from google.oauth2 import service_account

PROJECT_ID = "urban-renewal-32f02"
CRED_PATH = pathlib.Path(__file__).parent.parent / "firebase-credentials.json"

ORPHAN_IDS = [
    "591_19248480",
    "591_19434259",
    "591_20064537",
    "591_20061352",
    "591_20061787",
]


def make_client(database):
    creds = service_account.Credentials.from_service_account_file(str(CRED_PATH))
    return firestore.Client(project=PROJECT_ID, database=database, credentials=creds)


def main():
    default_db = make_client("(default)")
    backup_db = make_client("backup-temp")

    print("=== 在 default DB 找 ===")
    for oid in ORPHAN_IDS:
        doc = default_db.collection("properties").document(oid).get()
        print(f"  {oid}: {'EXISTS' if doc.exists else 'NOT FOUND'}")

    print("\n=== 在 backup-temp DB 找（看備份前是否就沒了）===")
    for oid in ORPHAN_IDS:
        doc = backup_db.collection("properties").document(oid).get()
        print(f"  {oid}: {'EXISTS' if doc.exists else 'NOT FOUND'}")

    print("\n=== 用 query 查 source_id 欄位（避免 doc_id 不對但欄位對的情況）===")
    for oid in ORPHAN_IDS:
        results = list(default_db.collection("properties").where("source_id", "==", oid).limit(2).stream())
        if results:
            for r in results:
                print(f"  {oid}: 找到 doc_id={r.id} (source_id 欄位匹配)")
        else:
            print(f"  {oid}: 完全沒有任何匹配")

    print("\n=== 看用戶 watchlist 的內容（added_at 等資訊）===")
    users = list(default_db.collection("users").stream())
    for user_doc in users:
        uid = user_doc.id
        wl_col = default_db.collection("users").document(uid).collection("watchlist")
        for orphan_id in ORPHAN_IDS:
            wl_doc = wl_col.document(orphan_id).get()
            if wl_doc.exists:
                data = wl_doc.to_dict()
                print(f"  user {uid[:12]}... watchlist/{orphan_id}:")
                print(f"    {json.dumps(data, ensure_ascii=False, default=str, indent=4)}")

    print("\n=== 查 591 網站這幾個 ID 還活著嗎 ===")
    import urllib.request
    for oid in ORPHAN_IDS:
        houseid = oid.replace("591_", "")
        url = f"https://sale.591.com.tw/home/house/detail/2/{houseid}.html"
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            })
            resp = urllib.request.urlopen(req, timeout=10)
            html = resp.read(50000).decode("utf-8", errors="ignore")
            # 看內容判定是否「物件已下架」
            if "已下架" in html or "找不到" in html or "已售出" in html:
                status = "已下架/售出"
            elif resp.status == 200 and len(html) > 5000:
                status = "頁面還活著"
            else:
                status = f"未知 (status={resp.status}, len={len(html)})"
            print(f"  {oid}: {status}")
        except urllib.error.HTTPError as e:
            print(f"  {oid}: HTTP {e.code} ({e.reason})")
        except Exception as e:
            print(f"  {oid}: ERROR {e}")


if __name__ == "__main__":
    main()
