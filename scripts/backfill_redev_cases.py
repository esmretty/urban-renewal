"""Backfill `redev_cases` 欄位給既有雙北物件。

每筆物件依 city 走對應 query (台北 GeoServer WFS / 新北 NtpcURInfo)，
有 lat+lng 就跑，跑完直接 update Firestore doc。

Usage:
  python scripts/backfill_redev_cases.py            # full run
  python scripts/backfill_redev_cases.py --dry-run  # 不寫 DB，只 print 會做啥
  python scripts/backfill_redev_cases.py --limit 50 # 只跑前 N 筆 (測試用)
  python scripts/backfill_redev_cases.py --only-missing  # 已有 redev_cases 欄位的跳過
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database.db import get_firestore
from api.gis_overlay import query_tpe_renewal_cases, query_ntpc_renewal_cases, make_ntpc_query_client


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="不寫 DB，只 print")
    ap.add_argument("--limit", type=int, default=0, help="0=全部")
    ap.add_argument("--only-missing", action="store_true", help="已有 redev_cases 的跳過")
    ap.add_argument("--cities", default="台北市,新北市", help="comma-separated city list")
    ap.add_argument("--ntpc-only", action="store_true", help="只跑新北 (台北已 backfill 過，且 NTPC 之前 cookie bug 全 0)")
    args = ap.parse_args()
    if args.ntpc_only:
        args.cities = "新北市"

    cities = set([c.strip() for c in args.cities.split(",") if c.strip()])
    db = get_firestore()
    col = db.collection("properties")

    # 一次撈 minimal fields — backfill 不需要全 doc
    docs = []
    for d in col.select(["city", "latitude", "longitude", "redev_cases"]).get():
        data = d.to_dict() or {}
        if data.get("city") not in cities:
            continue
        if not data.get("latitude") or not data.get("longitude"):
            continue
        if args.only_missing and isinstance(data.get("redev_cases"), list):
            continue
        docs.append((d.id, data))

    if args.limit > 0:
        docs = docs[:args.limit]

    print(f"backfill 對象：{len(docs)} 筆 (cities={cities}, only_missing={args.only_missing}, dry_run={args.dry_run})")

    counts = {"台北市": 0, "新北市": 0, "skipped": 0, "errors": 0}
    enriched_with_cases = 0
    total_cases = 0
    t0 = time.time()

    # 重用 NTPC httpx.Client：第一次 connection setup ~3s，後續 ~0.03s。
    # 不重用的話 715 筆 NTPC 跑 ~50min，重用後 ~5min
    ntpc_client = make_ntpc_query_client()
    try:
        for i, (pid, data) in enumerate(docs, 1):
            city = data["city"]
            lat = data["latitude"]
            lng = data["longitude"]
            try:
                if city == "台北市":
                    cases = query_tpe_renewal_cases(lat, lng)
                elif city == "新北市":
                    cases = query_ntpc_renewal_cases(lat, lng, client=ntpc_client)
                else:
                    counts["skipped"] += 1
                    continue
                counts[city] += 1
                if cases:
                    enriched_with_cases += 1
                    total_cases += len(cases)
                if not args.dry_run:
                    col.document(pid).update({"redev_cases": cases})
                if i % 25 == 0 or i == len(docs):
                    rate = i / max(1.0, (time.time() - t0))
                    eta_s = (len(docs) - i) / max(0.001, rate)
                    print(f"  [{i}/{len(docs)}] city={city} cases={len(cases)} | rate={rate:.1f}/s ETA={eta_s:.0f}s", flush=True)
            except Exception as e:
                counts["errors"] += 1
                print(f"  ! {pid} ({city}) err: {e}", flush=True)
    finally:
        ntpc_client.close()

    print()
    print(f"=== 完成 ===")
    print(f"  台北市: {counts['台北市']}")
    print(f"  新北市: {counts['新北市']}")
    print(f"  skipped: {counts['skipped']}")
    print(f"  errors: {counts['errors']}")
    print(f"  有套疊都更案件: {enriched_with_cases} 筆 (共 {total_cases} 個 case)")
    print(f"  耗時: {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
