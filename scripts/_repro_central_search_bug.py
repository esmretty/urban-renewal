"""重現 /api/central_search 加 price filter 後 500 的 bug。

直接 call 函式內部的 query 邏輯，跑前端會送的 params 組合。
"""
import sys, traceback
sys.path.insert(0, ".")

from database.db import init_db, get_col
from google.cloud.firestore_v1 import FieldFilter

init_db()
col = get_col()


def attempt(label, builder):
    print(f"\n=== {label} ===")
    try:
        q = builder()
        docs = list(q.get())
        print(f"  OK N={len(docs)}")
    except Exception as e:
        print(f"  FAIL {type(e).__name__}: {e}")
        traceback.print_exc()


# 前端 default：3 區 + price min=0 + price max=5000
DISTRICTS = ["大安區", "信義區", "文山區"]

attempt("district in (no price)",
    lambda: col.where(filter=FieldFilter("district", "in", DISTRICTS)))

attempt("district in + price <= 5000萬",
    lambda: col.where(filter=FieldFilter("district", "in", DISTRICTS))
              .where(filter=FieldFilter("price_ntd", "<=", 50_000_000)))

attempt("district in + price <= 5000萬 + price >= 0",
    lambda: col.where(filter=FieldFilter("district", "in", DISTRICTS))
              .where(filter=FieldFilter("price_ntd", "<=", 50_000_000))
              .where(filter=FieldFilter("price_ntd", ">=", 0)))

# 還可能：用戶給了 max_bld_price_per_ping (單價門檻)
# 前端送 max_bld_price_per_ping=300 跟 max_land_price_per_ping=600 — 但這些只在 Python 端 filter
# 所以不會影響我們的 where()

# 再試一個邊界：很多區 (16 個) + price filter
ALL_TARGET = ["中正區","大同區","中山區","松山區","大安區","萬華區","信義區",
              "內湖區","南港區","文山區","板橋區","新莊區","新店區","中和區","永和區"]
attempt("district in (15 districts) + price <= 5000萬",
    lambda: col.where(filter=FieldFilter("district", "in", ALL_TARGET))
              .where(filter=FieldFilter("price_ntd", "<=", 50_000_000)))
