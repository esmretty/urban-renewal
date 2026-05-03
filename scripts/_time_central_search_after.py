"""量測修改後 central_search 的 DB 路徑（含新 where filter）。

對齊 api/app.py:central_search 的新邏輯：
  - dist_set 有挑 → col.where(district in [...]).get()
  - dist_set 沒挑 → col.get() 全收

跑 3 次取中位數。
"""
import sys, time
sys.path.insert(0, ".")

from database.db import init_db, get_col
from google.cloud.firestore_v1 import FieldFilter

init_db()
col = get_col()

TEST_DISTRICTS = ["大安區", "信義區", "文山區"]


def _run(label, fn, runs=3):
    times = []
    n = 0
    for _ in range(runs):
        t0 = time.perf_counter()
        n = fn()
        times.append(time.perf_counter() - t0)
    times_sorted = sorted(times)
    median = times_sorted[runs // 2]
    print(f"{label}")
    print(f"  N={n} | median={median*1000:.0f}ms | range={min(times)*1000:.0f}-{max(times)*1000:.0f}ms")
    return median


def baseline():
    docs = list(col.get())
    return len([d for d in docs if (d.to_dict() or {}).get("district") in TEST_DISTRICTS])


def with_where():
    docs = list(col.where(filter=FieldFilter("district", "in", TEST_DISTRICTS)).get())
    return len(docs)


# warmup
list(col.limit(1).get())

a = _run("A) baseline col.get() — 修改前", baseline)
print()
b = _run("B) col.where(district in [...]) — 修改後", with_where)
print(f"\n加速：{a/b:.1f}× ({a*1000:.0f}ms → {b*1000:.0f}ms)")
print(f"server response 預估：{b*1000 + 50:.0f}ms (含 to_dict + filter + JSON serialize)")
