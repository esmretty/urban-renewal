"""一次性遷移腳本：把 Firestore 裡所有 timestamp 字串欄位統一轉成 Taipei aware ISO。

處理規則：
- 無 tz suffix → 視為 UTC，轉 +08:00
- +00:00 / Z → 轉 +08:00
- 已經是 +08:00 → 不動
- parse 失敗 → 保留原值（log warning）

使用方式：
  python scripts/migrate_timestamps_to_tw.py --dry-run   # 先預覽
  python scripts/migrate_timestamps_to_tw.py             # 真跑
"""
import sys
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from database.db import init_db, get_firestore, get_col, get_user_watchlist, get_user_manual
from database.time_utils import TW_TZ, to_tw

# 要處理的欄位白名單（避免誤改非時間字串）
SCALAR_FIELDS = {
    "scraped_at", "scrape_session_at", "analysis_completed_at",
    "published_at", "updated_at", "_added_at", "added_at",
    "created_at", "migrated_at", "zoning_lookup_at",
    "last_run_at", "last_updated", "finished_at", "started_at",
}
# 陣列中每個元素都是時間字串
LIST_FIELDS = {"published_at_alt"}
# 陣列中每個元素是 dict，裡面某 key 是時間字串
NESTED_LIST_FIELDS = {
    "price_history": "scraped_at",
}


def convert_iso(s: str):
    """回 (new_str, changed_bool, ok_bool)。無法 parse → (s, False, False)"""
    if not isinstance(s, str):
        return s, False, False
    try:
        raw = s.replace("Z", "+00:00") if s.endswith("Z") else s
        dt = datetime.fromisoformat(raw)
    except Exception:
        return s, False, False
    tw = to_tw(dt).isoformat()
    return tw, (tw != s), True


def process_doc(data: dict) -> tuple[dict, list]:
    """回 (update_patch, changed_keys)。patch 只含需要改的 key。"""
    patch = {}
    changed = []
    # scalar
    for k in SCALAR_FIELDS:
        if k in data:
            new, ch, ok = convert_iso(data[k])
            if ch:
                patch[k] = new
                changed.append(k)
    # list of scalars
    for k in LIST_FIELDS:
        arr = data.get(k)
        if isinstance(arr, list):
            new_arr = []
            any_ch = False
            for s in arr:
                new_s, ch, _ = convert_iso(s)
                new_arr.append(new_s)
                any_ch = any_ch or ch
            if any_ch:
                patch[k] = new_arr
                changed.append(k)
    # nested list of dicts
    for k, subkey in NESTED_LIST_FIELDS.items():
        arr = data.get(k)
        if isinstance(arr, list):
            new_arr = []
            any_ch = False
            for entry in arr:
                if isinstance(entry, dict) and subkey in entry:
                    new_s, ch, _ = convert_iso(entry[subkey])
                    if ch:
                        entry = {**entry, subkey: new_s}
                        any_ch = True
                new_arr.append(entry)
            if any_ch:
                patch[k] = new_arr
                changed.append(k)
    return patch, changed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只印預覽，不實際寫入")
    ap.add_argument("--limit-samples", type=int, default=5, help="dry-run 顯示的樣本數")
    args = ap.parse_args()

    init_db()
    fs = get_firestore()

    targets = []
    # 1) properties (central)
    for d in get_col().get():
        targets.append(("properties", d.reference, d.to_dict() or {}))

    # 2) 所有 users 的 watchlist + manual + profile
    for u in fs.collection("users").get():
        uid = u.id
        targets.append((f"users/{uid}", u.reference, u.to_dict() or {}))
        for w in get_user_watchlist(uid).get():
            targets.append((f"users/{uid}/watchlist", w.reference, w.to_dict() or {}))
        for m in get_user_manual(uid).get():
            targets.append((f"users/{uid}/manual", m.reference, m.to_dict() or {}))

    # 3) settings/scheduler
    sched_doc = fs.collection("settings").document("scheduler").get()
    if sched_doc.exists:
        targets.append(("settings/scheduler", sched_doc.reference, sched_doc.to_dict() or {}))

    total = len(targets)
    to_patch = []
    for coll, ref, data in targets:
        patch, changed = process_doc(data)
        if patch:
            to_patch.append((coll, ref, patch, changed, data))

    print(f"\n掃描完成：總共 {total} 筆 doc，{len(to_patch)} 筆需要轉 tz")

    if args.dry_run:
        print(f"\n─── 樣本預覽（前 {args.limit_samples} 筆）───")
        for coll, ref, patch, changed, data in to_patch[: args.limit_samples]:
            print(f"\n[{coll}] {ref.path}")
            for k in changed:
                if k in data:
                    old_v = data.get(k)
                    new_v = patch.get(k)
                    # 只顯示 scalar 的 diff，陣列太長就略
                    if isinstance(old_v, str):
                        print(f"  {k}:  {old_v}  →  {new_v}")
                    else:
                        print(f"  {k}:  (list/nested, {len(old_v)} items 有變動)")
        print(f"\n(dry-run 結束，沒寫 DB。確認後移除 --dry-run 真跑)")
        return

    print("\n開始寫入...")
    ok = fail = 0
    for coll, ref, patch, changed, _data in to_patch:
        try:
            ref.update(patch)
            ok += 1
        except Exception as e:
            print(f"  ✗ {ref.path}: {e}")
            fail += 1
    print(f"\n寫入完成：成功 {ok} / 失敗 {fail}")


if __name__ == "__main__":
    main()
