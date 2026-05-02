"""Audit DB 找 sources 連結消失的物件，看 alive=False 是怎麼被設的"""
import sys
sys.path.insert(0, ".")
from database.db import init_db, get_col
init_db()
col = get_col()

no_source = []
all_dead = []
total = 0
for d in col.stream():
    x = d.to_dict()
    if x.get("archived"):
        continue
    total += 1
    sources = x.get("sources") or []
    alive_sources = [s for s in sources if s.get("url") and s.get("alive") is not False]
    if not sources:
        no_source.append((d.id, x))
    elif not alive_sources:
        all_dead.append((d.id, x))

print(f"總有效物件 (非 archived): {total}")
print(f"sources=[] 的: {len(no_source)}")
print(f"sources 全部 alive=False 的: {len(all_dead)}")
print()
print("=== 無 sources 樣本 (前 5) ===")
for sid, x in no_source[:5]:
    addr = (x.get("address") or x.get("address_inferred") or "")[:30]
    print(f"  {sid}: addr={addr!r}")
    print(f"    source_id={x.get('source_id')!r}, source={x.get('source')!r}")
    print(f"    scraped_at={x.get('scraped_at')!r}, analysis_completed_at={x.get('analysis_completed_at')!r}")
    print(f"    sources={x.get('sources')!r}")

print()
print("=== sources 都 alive=False 樣本 (前 5) ===")
for sid, x in all_dead[:5]:
    addr = (x.get("address") or x.get("address_inferred") or "")[:30]
    print(f"  {sid}: addr={addr!r}")
    for s in x.get("sources") or []:
        print(f"    source name={s.get('name')!r} alive={s.get('alive')!r}")
        print(f"           url={s.get('url')!r}")
        print(f"           deactivated_at={s.get('deactivated_at')!r} dead_reason={s.get('dead_reason')!r}")
