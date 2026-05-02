"""一次性 migration：把 sources[] 中英文 name (sinyi/yongqing) 改成中文 (信義/永慶)，
然後 dedup（同一 source_key 只留一筆，alive=True 優先）。

之前 add_source_to_doc 沒 normalize name，caller 傳什麼就存什麼，
導致同一物件 sources 有 name='sinyi' + name='信義' 兩筆 → 前端 group by raw name
顯示兩個 badge（其中英文那個 fall through 到 src-badge-other 灰色 default）。

執行：python scripts/cleanup_dup_source_names.py
"""
import sys
sys.path.insert(0, ".")

from database.db import init_db, get_col
from database.models import (
    make_source_key, compute_source_keys,
    canonical_source_name, display_source_name,
)
from database.time_utils import now_tw_iso

init_db()
col = get_col()

renamed = 0
deduped = 0
total_changed_docs = 0
for d in col.stream():
    x = d.to_dict() or {}
    sources = x.get("sources") or []
    if not sources:
        continue
    # 1) normalize name 英文 → 中文
    for s in sources:
        raw_name = s.get("name") or ""
        new_name = display_source_name(raw_name)
        if new_name != raw_name:
            s["name"] = new_name
            renamed += 1
    # 2) dedup：相同 source_key 合併（alive=True 勝；保留最早 added_at）
    by_key = {}
    for s in sources:
        key = make_source_key(s.get("name"), s.get("source_id"))
        if key not in by_key:
            by_key[key] = s
        else:
            existing = by_key[key]
            # 合併：alive=True 勝；added_at 取較早；url 取非空
            if s.get("alive") is True and existing.get("alive") is False:
                existing["alive"] = True
            for k in ("url",):
                if not existing.get(k) and s.get(k):
                    existing[k] = s[k]
            ea = existing.get("added_at") or ""
            sa = s.get("added_at") or ""
            if sa and (not ea or sa < ea):
                existing["added_at"] = sa
            deduped += 1
    new_sources = list(by_key.values())
    if len(new_sources) == len(sources):
        # name 改名也算 changed
        if any(orig.get("name") != new.get("name") for orig, new in zip(x.get("sources") or [], sources)):
            pass
        else:
            continue
    addr = x.get("address") or x.get("address_inferred") or ""
    print(f"  {d.id} ({addr[:30]}): sources {len(sources)}→{len(new_sources)}")
    col.document(d.id).update({
        "sources": new_sources,
        "source_keys": compute_source_keys(new_sources),
        "_dup_name_cleanup_at": now_tw_iso(),
    })
    total_changed_docs += 1

print(f"\n=== 完成 ===")
print(f"改名 entries: {renamed}")
print(f"dedup entries: {deduped}")
print(f"修改 docs: {total_changed_docs}")
