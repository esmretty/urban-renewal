"""量測 Firestore query index 推到 DB 端的效益。

對照 3 種讀取模式：
  A) baseline = 現行做法：col.get() 全部讀完，Python filter
  B) where(district, in, [...]) — 推一層
  C) where(district, in) + where(price_ntd, <=) — 推兩層（要 composite index）

跑：python scripts/_time_query_index.py
"""
import sys, time
sys.path.insert(0, ".")

from database.db import init_db, get_col

init_db()
col = get_col()

# 測試條件：3 個區 + 5000 萬以下
TEST_DISTRICTS = ["大安區", "信義區", "文山區"]
MAX_PRICE = 50_000_000   # 5000 萬


def time_run(label, fn, runs=3):
    times = []
    for i in range(runs):
        t0 = time.perf_counter()
        result = fn()
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
    times_sorted = sorted(times)
    median = times_sorted[runs // 2]
    print(f"{label}")
    print(f"  N={len(result)} | median={median*1000:.0f}ms | range={min(times)*1000:.0f}-{max(times)*1000:.0f}ms")
    return median, len(result)


# A) baseline
def fetch_all():
    docs = list(col.get())
    items = []
    for d in docs:
        data = d.to_dict() or {}
        if data.get("district") not in TEST_DISTRICTS:
            continue
        if data.get("price_ntd") and data.get("price_ntd") > MAX_PRICE:
            continue
        items.append(data)
    return items


# B) where(district in [...])
def fetch_district_only():
    from google.cloud.firestore_v1 import FieldFilter
    q = col.where(filter=FieldFilter("district", "in", TEST_DISTRICTS))
    docs = list(q.get())
    items = []
    for d in docs:
        data = d.to_dict() or {}
        if data.get("price_ntd") and data.get("price_ntd") > MAX_PRICE:
            continue
        items.append(data)
    return items


# C) where(district in) + where(price_ntd <=)
def fetch_district_and_price():
    from google.cloud.firestore_v1 import FieldFilter
    q = (col.where(filter=FieldFilter("district", "in", TEST_DISTRICTS))
            .where(filter=FieldFilter("price_ntd", "<=", MAX_PRICE)))
    docs = list(q.get())
    items = [d.to_dict() or {} for d in docs]
    return items


print(f"條件：district in {TEST_DISTRICTS}, price_ntd <= {MAX_PRICE/10000:.0f}萬\n")

# warmup
list(col.limit(1).get())

a_time, a_n = time_run("A) baseline (全抓 + Python filter)", fetch_all)
print()
try:
    b_time, b_n = time_run("B) where(district in [...])", fetch_district_only)
except Exception as e:
    print(f"B) 失敗：{e}")
    b_time, b_n = None, None
print()
try:
    c_time, c_n = time_run("C) where(district in) + where(price <=)", fetch_district_and_price)
except Exception as e:
    print(f"C) 失敗（可能需要 composite index）：{e}")
    c_time, c_n = None, None

print("\n=== 加速比 ===")
if b_time:
    print(f"B vs A: {a_time/b_time:.1f}× ({a_time*1000:.0f}ms → {b_time*1000:.0f}ms)")
if c_time:
    print(f"C vs A: {a_time/c_time:.1f}× ({a_time*1000:.0f}ms → {c_time*1000:.0f}ms)")
