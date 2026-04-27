"""Backfill `source_ids` 平面索引欄位給既有 properties。

背景：之前 dup_merge 只寫 url_alt 沒寫 source_ids → find_doc_by_source_id 找不到
那些被 merge 進來的 sid → 同樣 13 筆每次 batch 都被當 new。

修法：對每筆 doc 收集所有曾經對應過的 source_id：
  1. 主來源 source_id 欄位
  2. sources array 內每筆 .source_id
  3. url_alt 裡能解析出 591 source_id 的（從 URL 抓 detail/2/12345678.html → 591_12345678）

寫進新欄位 source_ids: List[str]。下次 find_doc_by_source_id 就找得到了。

執行：
  python scripts/backfill_source_ids.py            # dry-run
  python scripts/backfill_source_ids.py --apply    # 真寫
"""
import sys
import re
import argparse
sys.path.insert(0, r'd:\Coding\urban-renewal')
from database.db import init_db, get_col


_RX_591_DETAIL = re.compile(r"sale\.591\.com\.tw/home/house/detail/2/(\d+)")
_RX_YC_DETAIL = re.compile(r"buy\.yungching\.com\.tw/house/(\d+)")


def _extract_sid_from_url(url: str) -> str | None:
    if not url:
        return None
    m = _RX_591_DETAIL.search(url)
    if m:
        return f"591_{m.group(1)}"
    m = _RX_YC_DETAIL.search(url)
    if m:
        return f"yongqing_{m.group(1)}"
    return None


def main(apply: bool):
    init_db()
    col = get_col()
    docs = list(col.stream())
    print(f"total docs: {len(docs)}")

    plan = []
    for d in docs:
        data = d.to_dict() or {}
        sid_set = set()
        if data.get("source_id"):
            sid_set.add(data["source_id"])
        for s in (data.get("sources") or []):
            if isinstance(s, dict) and s.get("source_id"):
                sid_set.add(s["source_id"])
        if data.get("url"):
            x = _extract_sid_from_url(data["url"])
            if x: sid_set.add(x)
        for u in (data.get("url_alt") or []):
            x = _extract_sid_from_url(u)
            if x: sid_set.add(x)

        existing_sids = set(data.get("source_ids") or [])
        if sid_set != existing_sids:
            plan.append((d.id, sorted(existing_sids), sorted(sid_set)))

    print(f"need update: {len(plan)} docs")
    for did, before, after in plan[:30]:
        added = set(after) - set(before)
        print(f"  {did}: + {sorted(added)}")
    if len(plan) > 30:
        print(f"  ... ({len(plan) - 30} more)")

    if not apply:
        print("\n[dry-run] 加 --apply 才會真寫")
        return

    print("\n=== APPLYING ===")
    for did, _, after in plan:
        col.document(did).update({"source_ids": list(after)})
    print(f"done: updated {len(plan)} docs")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    main(args.apply)
