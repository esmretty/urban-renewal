"""Backfill renewal_v2 — 重算所有有 zoning + land_area_ping + price_ntd 的物件。

修了 2 件事後需要重跑：
  1. road_width_m 限縮容積率（之前後端沒套用，前端有）
  2. multiple 分母從「開價」改成「欲出價（開價×0.9）」

執行：
  python scripts/backfill_renewal_v2.py            # dry-run，列出影響筆數 + 倍數變化
  python scripts/backfill_renewal_v2.py --apply    # 實際寫回 DB
"""
import sys, argparse
sys.path.insert(0, r'd:\Coding\urban-renewal')

from database.db import init_db, get_col
from analysis.scorer import calculate_renewal_scenarios, resolve_effective_zoning


def main(apply: bool):
    init_db()
    col = get_col()
    docs = list(col.stream())
    print(f"total docs: {len(docs)}")

    plan = []
    skipped_no_zoning = 0
    skipped_no_land = 0
    skipped_no_price = 0
    skipped_unknown_zone = 0
    for d in docs:
        data = d.to_dict() or {}
        if not data.get("zoning"):
            skipped_no_zoning += 1
            continue
        if not data.get("land_area_ping"):
            skipped_no_land += 1
            continue
        if not data.get("price_ntd"):
            skipped_no_price += 1
            continue

        eff_zone = resolve_effective_zoning(data.get("zoning"), data.get("zoning_original"))
        new_v2 = calculate_renewal_scenarios(
            land_area_ping=data["land_area_ping"],
            zoning=eff_zone,
            district=data.get("district"),
            price_ntd=data["price_ntd"],
            road_width_m=data.get("road_width_m"),
        )
        if not new_v2.get("scenarios"):
            skipped_unknown_zone += 1
            continue

        old_v2 = data.get("renewal_v2") or {}
        old_scen = old_v2.get("scenarios") or {}
        new_scen = new_v2["scenarios"]
        old_max = max(
            (s.get("multiple") or 0) for s in old_scen.values()
        ) if old_scen else 0
        new_max = max(
            (s.get("multiple") or 0) for s in new_scen.values()
        ) if new_scen else 0

        # 沒變化（< 0.01）→ 不重寫
        if abs(old_max - new_max) < 0.01:
            continue

        plan.append({
            "doc_id": d.id,
            "src": (data.get("source_id") or "")[:30],
            "addr": (data.get("address_inferred") or data.get("address") or "")[:30],
            "rw": data.get("road_width_m"),
            "old_max": round(old_max, 2),
            "new_max": round(new_max, 2),
            "new_v2": new_v2,
        })

    print(f"\n  skipped: no_zoning={skipped_no_zoning}, no_land={skipped_no_land}, "
          f"no_price={skipped_no_price}, unknown_zone={skipped_unknown_zone}")
    print(f"  changed: {len(plan)} 筆\n")

    plan.sort(key=lambda x: abs(x["old_max"] - x["new_max"]), reverse=True)
    for p in plan[:30]:
        print(f"  {p['doc_id']:18s} {p['src']:18s} rw={p['rw']!s:>5} "
              f"max_mult: {p['old_max']:.2f} -> {p['new_max']:.2f}  {p['addr']}")
    if len(plan) > 30:
        print(f"  ... ({len(plan) - 30} more)")

    if not apply:
        print("\n[dry-run] 加 --apply 才會實際寫入。")
        return

    print(f"\n寫入 {len(plan)} 筆...")
    written = 0
    for p in plan:
        try:
            col.document(p["doc_id"]).update({"renewal_v2": p["new_v2"]})
            written += 1
        except Exception as e:
            print(f"  ✗ {p['doc_id']}: {e}")
    print(f"完成：{written}/{len(plan)} 筆已更新。")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="實際寫入 DB（預設 dry-run）")
    args = ap.parse_args()
    main(apply=args.apply)
