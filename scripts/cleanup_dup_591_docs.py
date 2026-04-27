"""清理 591 source_id 重複 doc：保留 NEW UUID doc、刪除 OLD `591_xxx` doc。

背景：UUID migration 後，VM 跑 NEW code（doc_id = UUID）但有另一個服務在跑 pre-migration code
（doc_id = `591_xxx`），導致同個 source_id 在 DB 出現兩筆 doc。

策略：
1. 找出所有 591_xxx 重複組（同一 591 source_id 對到兩個 doc）
2. 保留 NEW UUID doc（schema 較完整）
3. 刪 OLD doc 前：
   - merge OLD 有但 NEW 沒的欄位（避免遺失分析結果）
   - 掃所有 users.watchlist，把指向 OLD doc_id 的改指 NEW doc_id

執行：
  python scripts/cleanup_dup_591_docs.py            # dry-run（預設）
  python scripts/cleanup_dup_591_docs.py --apply    # 真的執行
"""
import sys
import argparse
sys.path.insert(0, r'd:\Coding\urban-renewal')
from collections import defaultdict
from database.db import init_db, get_firestore, get_col

def main(apply: bool):
    init_db()
    db = get_firestore()
    docs = list(get_col().stream())
    print(f"total properties: {len(docs)}")

    groups = defaultdict(list)
    for d in docs:
        data = d.to_dict() or {}
        did = d.id
        sid_field = data.get('source_id')
        sources = data.get('sources') or []
        s591 = None
        if did.startswith('591_'):
            s591 = did
        elif sid_field and sid_field.startswith('591_'):
            s591 = sid_field
        else:
            for s in sources:
                if isinstance(s, dict) and s.get('source') == '591' and s.get('source_id'):
                    s591 = s['source_id']; break
                if isinstance(s, dict) and s.get('name') == '591' and s.get('source_id'):
                    s591 = s['source_id']; break
        if s591:
            groups[s591].append((did, data))

    dups = {k: v for k, v in groups.items() if len(v) > 1}
    print(f"duplicate 591 sid groups: {len(dups)}")

    if not dups:
        print("no duplicates, exit")
        return

    # 為了改 watchlist，先把所有 users.watchlist 撈起來
    users = list(db.collection('users').stream())
    print(f"total users: {len(users)}")

    plan = []  # list of (sid, keep_doc_id, delete_doc_id, merge_fields, watchlist_migrations)
    for sid, entries in sorted(dups.items()):
        # 分類：UUID doc vs 591_xxx doc
        uuid_doc = next((e for e in entries if not e[0].startswith('591_')), None)
        old_doc = next((e for e in entries if e[0].startswith('591_')), None)
        if not uuid_doc or not old_doc:
            # 兩筆都不是預期格式，跳過
            print(f"  SKIP {sid}: unexpected pattern: {[e[0] for e in entries]}")
            continue
        keep_id, keep_data = uuid_doc
        del_id, del_data = old_doc

        # merge：OLD 有非空、NEW 沒有 / 是 None / 是空字串 的欄位 → 補入 NEW
        merge_fields = {}
        for k, v in del_data.items():
            if v is None or v == "" or v == [] or v == {}:
                continue
            kv = keep_data.get(k)
            if kv is None or kv == "" or kv == [] or kv == {}:
                merge_fields[k] = v
        # 不要覆蓋 NEW 的核心 schema 欄位
        for skip_key in ('id', 'sources', 'source_id', 'archived', 'source', 'created_at', 'doc_id'):
            merge_fields.pop(skip_key, None)

        # 找有指向 del_id 的 watchlist
        wl_migrations = []
        for u in users:
            wl = u.to_dict().get('watchlist') or []
            if not isinstance(wl, list):
                continue
            for entry in wl:
                eid = entry.get('property_id') if isinstance(entry, dict) else entry
                if eid == del_id:
                    wl_migrations.append((u.id, eid))

        plan.append((sid, keep_id, del_id, merge_fields, wl_migrations))

    print("\n=== PLAN ===")
    for sid, keep, del_id, merge, wl in plan:
        merge_keys = list(merge.keys())
        print(f"  {sid}")
        print(f"    KEEP   {keep}")
        print(f"    DELETE {del_id}")
        if merge_keys:
            print(f"    MERGE fields into KEEP: {merge_keys}")
        if wl:
            print(f"    WATCHLIST migrate: {wl}")

    if not apply:
        print("\n[dry-run] 沒做事。加 --apply 才會真的動 DB。")
        return

    print("\n=== APPLYING ===")
    for sid, keep, del_id, merge, wl in plan:
        # 1. merge 欄位到 KEEP
        if merge:
            get_col().document(keep).update(merge)
            print(f"  merged {len(merge)} fields into {keep}")
        # 2. watchlist 改指向
        for uid, _old_pid in wl:
            udoc = db.collection('users').document(uid)
            usnap = udoc.get()
            wl_list = (usnap.to_dict() or {}).get('watchlist') or []
            new_wl = []
            for entry in wl_list:
                if isinstance(entry, dict):
                    if entry.get('property_id') == del_id:
                        new_entry = dict(entry); new_entry['property_id'] = keep
                        new_wl.append(new_entry)
                    else:
                        new_wl.append(entry)
                else:
                    new_wl.append(keep if entry == del_id else entry)
            udoc.update({'watchlist': new_wl})
            print(f"  watchlist user={uid}: {del_id} → {keep}")
        # 3. 刪 OLD doc
        get_col().document(del_id).delete()
        print(f"  deleted {del_id}")
    print(f"\nDONE: cleaned {len(plan)} duplicate groups")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true', help='實際執行（不加只 dry-run）')
    args = ap.parse_args()
    main(args.apply)
