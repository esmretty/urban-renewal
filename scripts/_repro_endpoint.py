"""Local 重現 broken commit 的 500 — 不修 code，先把 broken query 整段邏輯抓回來重跑。

模擬 central_search 從 Firestore 拿 docs 後的下游流程，確認是不是這段爆。
"""
import sys, traceback
sys.path.insert(0, ".")

from database.db import init_db, get_col
from database.models import merge_watchlist_with_central
from google.cloud.firestore_v1 import FieldFilter

init_db()
col = get_col()

# 前端 default params
DISTRICTS = ["大安區", "信義區", "文山區"]
min_price_wan = 0
max_price_wan = 5000

print("=== 走 broken query 的整套 pipeline ===\n")

try:
    print("1. Firestore where(district in)+where(price<=)...")
    q = col.where(filter=FieldFilter("district", "in", DISTRICTS))
    if max_price_wan is not None and max_price_wan > 0:
        q = q.where(filter=FieldFilter("price_ntd", "<=", int(max_price_wan * 10000)))
    if min_price_wan is not None and min_price_wan > 0:
        q = q.where(filter=FieldFilter("price_ntd", ">=", int(min_price_wan * 10000)))
    docs = list(q.get())
    print(f"   OK, N={len(docs)}")
except Exception as e:
    print(f"   FAIL: {type(e).__name__}: {e}")
    traceback.print_exc()
    sys.exit(1)

print("\n2. to_dict() 整批轉 dict...")
try:
    raw_items = []
    for d in docs:
        data = d.to_dict() or {}
        data["__doc_id"] = d.id
        raw_items.append(data)
    print(f"   OK, items={len(raw_items)}")
except Exception as e:
    print(f"   FAIL: {type(e).__name__}: {e}")
    traceback.print_exc()
    sys.exit(1)

print("\n3. Python filter loop...")
try:
    items = []
    for data in raw_items:
        if data.get("source_origin") == "user_url":
            continue
        items.append(data)
    print(f"   OK, after filter N={len(items)}")
except Exception as e:
    print(f"   FAIL: {type(e).__name__}: {e}")
    traceback.print_exc()
    sys.exit(1)

print("\n4. sort by list_rank, scrape_session_at...")
try:
    items.sort(key=lambda x: (x.get("list_rank") if x.get("list_rank") is not None else 9999))
    items.sort(key=lambda x: x.get("scrape_session_at") or "", reverse=True)
    print(f"   OK")
except Exception as e:
    print(f"   FAIL: {type(e).__name__}: {e}")
    traceback.print_exc()
    sys.exit(1)

print("\n5. JSON serialize (FastAPI 預設 json.dumps)...")
try:
    import json
    j = json.dumps({"total": len(items), "items": items[:1000]}, default=str)
    print(f"   OK, JSON 大小 = {len(j)/1024:.0f} KB")
except Exception as e:
    print(f"   FAIL: {type(e).__name__}: {e}")
    traceback.print_exc()
    sys.exit(1)

print("\n=== 全部通過 — bug 不在這段 ===")
print("=== 接下來要看 FastAPI 端 (Pydantic / response_model / middleware) ===")
