"""把 retry queue 補抓誤標 source_origin=user_url 的物件清掉。

判定：source_origin=user_url 但沒 submitted_by_uid → 表示不是用戶主動貼 URL 送出，
是 retry queue 自動補抓 batch 失敗的物件（標籤錯了）。

執行：
  python scripts/fix_mislabeled_user_url.py        # dry-run
  python scripts/fix_mislabeled_user_url.py --apply # 實際清標
"""
import sys, os, argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import init_db, get_col


def main(apply: bool):
    init_db()
    col = get_col()
    docs = list(col.stream())
    print(f"total docs: {len(docs)}")

    fixable = []
    for d in docs:
        x = d.to_dict() or {}
        if x.get("source_origin") != "user_url":
            continue
        if x.get("submitted_by_uid"):
            continue   # 真的是用戶送的，留著
        fixable.append({
            "doc_id": d.id,
            "src": x.get("source_id"),
            "city": x.get("city"),
            "district": x.get("district"),
            "addr": x.get("address_inferred") or x.get("address") or "",
        })

    print(f"\n誤標 user_url 的物件（無 submitted_by_uid）：{len(fixable)} 筆")
    for p in fixable[:50]:
        print(f"  {p['doc_id']:18s} {p['src']:25s} {p['city']}{p['district']} | {p['addr'][:40]}")
    if len(fixable) > 50:
        print(f"  ... ({len(fixable) - 50} more)")

    if not apply:
        print("\n[dry-run] 加 --apply 會清掉 source_origin 欄位（admin 物件列表才看得到）")
        return

    print(f"\n清掉 {len(fixable)} 筆 source_origin...")
    fixed = 0
    for p in fixable:
        try:
            col.document(p["doc_id"]).update({"source_origin": None})
            fixed += 1
        except Exception as e:
            print(f"  ✗ {p['doc_id']}: {e}")
    print(f"完成：{fixed}/{len(fixable)} 筆已修。")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    main(apply=args.apply)
