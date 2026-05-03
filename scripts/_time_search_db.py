"""量測 /api/central_search 的 DB 讀取耗時（不含 client/auth/network）。

對齊 api/app.py:central_search 的 `docs = list(col.get())` 那一段。
跑：python scripts/_time_search_db.py
"""
import sys, time
sys.path.insert(0, ".")

from database.db import init_db, get_col

init_db()
col = get_col()

# 跑 3 次取中位數，避免 Firestore cold start 干擾
runs = []
for i in range(3):
    t0 = time.perf_counter()
    docs = list(col.get())
    elapsed = time.perf_counter() - t0
    runs.append(elapsed)
    print(f"  run {i+1}: {elapsed:.3f}s ({len(docs)} docs)")

runs_sorted = sorted(runs)
median = runs_sorted[1]
print(f"\n中位數: {median:.3f}s")
print(f"範圍: {min(runs):.3f}s ~ {max(runs):.3f}s")

# 額外量「toDict」這一段（因為 server 還會跑 .to_dict() N 次 + Python filter）
t0 = time.perf_counter()
items = []
for d in docs:
    data = d.to_dict() or {}
    items.append(data)
to_dict_time = time.perf_counter() - t0
print(f"\nto_dict() N={len(docs)}: {to_dict_time:.3f}s")
print(f"小計（DB抓 + to_dict）: {median + to_dict_time:.3f}s")
