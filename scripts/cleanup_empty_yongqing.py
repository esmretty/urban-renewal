"""清掉之前 enrich 失敗留在 DB 的「空殼」永慶物件（地址、價格全 None）。

使用方式：
    python cleanup_empty_yongqing.py --dry-run    # 預覽會刪幾筆
    python cleanup_empty_yongqing.py --execute    # 實際刪
"""
import argparse
import sys
import pathlib

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from google.cloud import firestore
from google.oauth2 import service_account


def make_client():
    creds = service_account.Credentials.from_service_account_file(
        str(pathlib.Path(__file__).parent.parent / "firebase-credentials.json")
    )
    return firestore.Client(project="urban-renewal-32f02", database="(default)", credentials=creds)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()
    if not (args.dry_run or args.execute):
        print("ERROR: 必須指定 --dry-run 或 --execute")
        return 1
    dry_run = args.dry_run

    client = make_client()
    col = client.collection("properties")

    # 篩選條件：source=永慶 + 地址或價格為 None（核心欄位空殼判定）
    targets = []
    yongqing_count = 0
    for doc in col.stream():
        d = doc.to_dict() or {}
        if d.get("source") != "永慶":
            continue
        yongqing_count += 1
        # 空殼：address 跟 price_ntd 都 None
        if d.get("address") is None and d.get("price_ntd") is None:
            targets.append(doc)

    print(f"=== 永慶物件總數: {yongqing_count} ===")
    print(f"=== 空殼物件（address + price_ntd 都 None）: {len(targets)} ===")
    if not targets:
        print("沒有空殼物件需要清理")
        return 0
    print()
    print("前 10 筆範例：")
    for d in targets[:10]:
        data = d.to_dict() or {}
        print(f"  {d.id}: source_id={data.get('source_id')} url={data.get('url','')[:60]} scraped_at={data.get('scraped_at','')[:19]}")

    if dry_run:
        print(f"\n[DRY RUN] 會刪 {len(targets)} 筆。實際執行請用 --execute")
        return 0

    print(f"\n=== 開始刪除 {len(targets)} 筆 ===")
    BATCH = 400
    batch = client.batch()
    n = 0
    for d in targets:
        batch.delete(d.reference)
        n += 1
        if n % BATCH == 0:
            batch.commit()
            batch = client.batch()
            print(f"  已刪 {n}")
    if n % BATCH != 0:
        batch.commit()
    print(f"\n✓ 共刪 {n} 筆空殼永慶物件")
    return 0


if __name__ == "__main__":
    sys.exit(main())
