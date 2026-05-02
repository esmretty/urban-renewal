"""一次性 migration：清掉 sources[] 裡 url=None / url='' 的廢 entries。

之前 add_source_to_doc 沒擋住 caller 傳 None/空字串 url 的 case，導致 44 筆 docs
sources 列裡混雜著 {url: None, alive: True} 廢 entries。

此 script 跑：
  1. 掃 properties collection
  2. 對每個 doc，過濾掉 url 是 falsy 的 source entries
  3. 若有變動 → update sources[] + source_keys[]
  4. 印出處理筆數

執行：python scripts/cleanup_garbage_sources.py
"""
import sys
sys.path.insert(0, ".")

from database.db import init_db, get_col, get_firestore
from database.models import compute_source_keys
from database.time_utils import now_tw_iso

init_db()
col = get_col()

cleaned = 0
total_removed = 0
total = 0
for d in col.stream():
    x = d.to_dict() or {}
    sources = x.get("sources") or []
    if not sources:
        continue
    total += 1
    # 過濾：保留有 url 且 url 非空字串
    new_sources = [s for s in sources if s.get("url") and str(s.get("url")).strip()]
    removed = len(sources) - len(new_sources)
    if removed == 0:
        continue
    cleaned += 1
    total_removed += removed
    addr = x.get("address") or x.get("address_inferred") or ""
    print(f"  {d.id} ({addr[:30]}): 移除 {removed} 個廢 entry → 剩 {len(new_sources)} 個 alive source")
    update = {
        "sources": new_sources,
        "source_keys": compute_source_keys(new_sources),
        "_garbage_cleanup_at": now_tw_iso(),
    }
    col.document(d.id).update(update)

print(f"\n=== 完成 ===")
print(f"掃過 {total} 個 doc（有 sources）")
print(f"清理 {cleaned} 個 doc，共移除 {total_removed} 筆廢 entries")
