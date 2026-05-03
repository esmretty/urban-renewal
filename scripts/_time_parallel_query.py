"""比較 in[N] vs parallel == 兩種 Firestore 查詢的耗時。

用戶實測 prod where("district","in",[15 districts]) 要 1.8s，
本機 (3 districts) 卻只要 220ms。可能是 Firestore in 的 fan-out 慢。
"""
import sys, time
sys.path.insert(0, ".")

from database.db import init_db, get_col
from google.cloud.firestore_v1 import FieldFilter
import concurrent.futures as _cf

init_db()
col = get_col()

# 全部 15 區（重現用戶情境）
ALL_DISTRICTS = ["中正區","大同區","中山區","松山區","大安區","萬華區","信義區",
                 "內湖區","南港區","文山區","板橋區","新莊區","新店區","中和區","永和區"]
MAX_NTD = 50_000_000   # 5000 萬


def time_runs(label, fn, runs=3):
    times = []
    n = 0
    for _ in range(runs):
        t0 = time.perf_counter()
        n = fn()
        times.append(time.perf_counter() - t0)
    sorted_t = sorted(times)
    median = sorted_t[runs // 2]
    print(f"  {label:42s} N={n:4d} | median={median*1000:.0f}ms | range={min(times)*1000:.0f}-{max(times)*1000:.0f}ms")
    return median


def in_query():
    q = col.where(filter=FieldFilter("district", "in", ALL_DISTRICTS)).where(filter=FieldFilter("price_ntd", "<=", MAX_NTD))
    return len(list(q.get()))


def parallel_eq_queries():
    def _q1(d):
        q = col.where(filter=FieldFilter("district", "==", d)).where(filter=FieldFilter("price_ntd", "<=", MAX_NTD))
        return len(list(q.get()))
    with _cf.ThreadPoolExecutor(max_workers=16) as ex:
        return sum(ex.map(_q1, ALL_DISTRICTS))


# warmup
list(col.limit(1).get())

print(f"\n=== 15 districts + price <= 5000萬 ===")
in_t = time_runs("A) where(district in [15])", in_query)
par_t = time_runs("B) parallel where(district==X) ×15", parallel_eq_queries)
print(f"\nB vs A: {in_t/par_t:.1f}× ({in_t*1000:.0f}ms -> {par_t*1000:.0f}ms)")

print(f"\n=== 3 districts + price <= 5000萬 (對照組) ===")
SMALL = ["大安區", "信義區", "文山區"]
def in3():
    q = col.where(filter=FieldFilter("district", "in", SMALL)).where(filter=FieldFilter("price_ntd", "<=", MAX_NTD))
    return len(list(q.get()))
def par3():
    def _q1(d):
        q = col.where(filter=FieldFilter("district", "==", d)).where(filter=FieldFilter("price_ntd", "<=", MAX_NTD))
        return len(list(q.get()))
    with _cf.ThreadPoolExecutor(max_workers=8) as ex:
        return sum(ex.map(_q1, SMALL))

a = time_runs("A) where(district in [3])", in3)
b = time_runs("B) parallel where(district==X) ×3", par3)
print(f"\nB vs A: {a/b:.1f}× ({a*1000:.0f}ms -> {b*1000:.0f}ms)")
