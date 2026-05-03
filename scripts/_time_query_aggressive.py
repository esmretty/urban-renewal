"""進階測試：當 price filter 真的會 cut 結果時，composite index 多大幫助？"""
import sys, time
sys.path.insert(0, ".")

from database.db import init_db, get_col
from google.cloud.firestore_v1 import FieldFilter

init_db()
col = get_col()

DISTRICTS = ["大安區", "信義區", "文山區"]


def _time(label, fn, runs=3):
    n = 0
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        n = fn()
        times.append(time.perf_counter() - t0)
    sorted_t = sorted(times)
    print(f"  {label:48s} N={n:4d} | median={sorted_t[runs//2]*1000:.0f}ms")
    return sorted_t[runs//2]


# warmup
list(col.limit(1).get())

for max_wan in [5000, 2000, 1500, 1000, 500]:
    max_ntd = max_wan * 10000
    print(f"\n=== max_price = {max_wan}萬 ===")
    a = _time(f"A baseline col.get()", lambda: len([d for d in col.get() if (d.to_dict() or {}).get("district") in DISTRICTS and ((d.to_dict() or {}).get("price_ntd") or 0) <= max_ntd]))
    b = _time(f"B where(district in)", lambda mn=max_ntd: len([d for d in col.where(filter=FieldFilter("district", "in", DISTRICTS)).get() if ((d.to_dict() or {}).get("price_ntd") or 0) <= mn]))
    c = _time(f"C where(district in) + where(price<=)",
              lambda mn=max_ntd: len(list(col.where(filter=FieldFilter("district", "in", DISTRICTS)).where(filter=FieldFilter("price_ntd", "<=", mn)).get())))
    print(f"  → B vs A: {a/b:.1f}× | C vs A: {a/c:.1f}× | C vs B: {b/c:.2f}×")
