"""End-to-end 測 /api/central_search HTTP endpoint。

用 FastAPI TestClient 真正打 HTTP request；override auth dependency 以繞過 Firebase token。
這樣的測試會抓到 q-shadowing 這類 bug，因為走的是完整 endpoint 路徑（含 query string parsing）。
"""
import sys, json
sys.path.insert(0, ".")

from database.db import init_db
init_db()

from fastapi.testclient import TestClient
import api.app as _app_mod
from api.app import app, get_current_user

# Override auth：middleware 直接 call get_current_user(request)，不只是 Depends，
# 所以 dependency_overrides 不夠 — 必須 monkey-patch 模組內的 get_current_user。
async def _fake_get_current_user(request=None):
    return {"uid": "test_uid_does_not_exist", "email": "test@test"}

_app_mod.get_current_user = _fake_get_current_user
app.dependency_overrides[get_current_user] = _fake_get_current_user

client = TestClient(app)

cases = [
    {
        "name": "默認 — 3 區 + 預設 price 範圍 (0-5000萬)",
        "params": {"districts": "大安區,信義區,文山區",
                   "min_price_wan": 0, "max_price_wan": 5000},
    },
    {
        "name": "緊縮 — 1500萬以下 (composite index 走得通)",
        "params": {"districts": "大安區,信義區,文山區",
                   "min_price_wan": 0, "max_price_wan": 1500},
    },
    {
        "name": "全部目標區 (15 個)",
        "params": {"districts": "中正區,大同區,中山區,松山區,大安區,萬華區,信義區,內湖區,南港區,文山區,板橋區,新莊區,新店區,中和區,永和區",
                   "min_price_wan": 0, "max_price_wan": 5000},
    },
    {
        "name": "1 區 + 路名搜尋",
        "params": {"districts": "大安區", "road": "信義路",
                   "min_price_wan": 0, "max_price_wan": 5000},
    },
    {
        "name": "**關鍵字 q (q-shadowing bug 觸發點)**",
        "params": {"q": "信義", "districts": "大安區,信義區"},
    },
    {
        "name": "沒挑區 (fallback 全收 path)",
        "params": {"min_price_wan": 0, "max_price_wan": 5000},
    },
    {
        "name": "緊條件 — 樓層 1-3F + 地坪>=10",
        "params": {"districts": "大安區,信義區",
                   "floors": "1,2,3", "min_land_ping": 10},
    },
]

failed = 0
for c in cases:
    print(f"\n--- {c['name']} ---")
    r = client.get("/api/central_search", params=c["params"])
    if r.status_code != 200:
        failed += 1
        print(f"  FAIL  HTTP {r.status_code}")
        print(f"  body: {r.text[:500]}")
        continue
    try:
        body = r.json()
    except Exception as e:
        failed += 1
        print(f"  FAIL  invalid JSON: {e}")
        print(f"  body: {r.text[:200]}")
        continue
    total = body.get("total", -1)
    items = body.get("items", [])
    json_kb = len(r.content) / 1024
    print(f"  PASS  HTTP {r.status_code}  total={total}  items={len(items)}  json={json_kb:.0f}KB")

print(f"\n{'=' * 60}")
if failed:
    print(f"{failed}/{len(cases)} cases FAILED — DO NOT DEPLOY")
    sys.exit(1)
print(f"All {len(cases)} cases passed — safe to deploy")
