"""Backfill building_age_completed_year 給既有 properties。

對每筆 doc：
  - 已有 completed_year → 跳過
  - 沒 completed_year 但有 building_age → completed_year = scrape 時年份 - building_age
    （用 scraped_at / scrape_session_at 推算「當時」抓到的屋齡對應的完工年）
  - 沒 building_age → 跳過（無資料可推）

執行：
  python scripts/backfill_completed_year.py            # dry-run
  python scripts/backfill_completed_year.py --apply    # 實際寫
"""
import sys, argparse
sys.path.insert(0, r'd:\Coding\urban-renewal')
from datetime import datetime
from database.db import init_db, get_col


def _scrape_year(data: dict) -> int:
    """從 scraped_at / scrape_session_at 抓「物件抓進來那刻的年份」。
    沒有 → 用今年（保守估計）。"""
    for k in ("scrape_session_at", "scraped_at", "analysis_completed_at"):
        v = data.get(k)
        if not v:
            continue
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).year
        except Exception:
            pass
    return datetime.now().year


def main(apply: bool):
    init_db()
    col = get_col()
    docs = list(col.stream())
    print(f"total docs: {len(docs)}")

    plan = []
    for d in docs:
        x = d.to_dict() or {}
        if x.get("building_age_completed_year"):
            continue   # 已有
        age = x.get("building_age")
        if age is None:
            continue
        try:
            a = int(round(float(age)))
        except Exception:
            continue
        if a < 0 or a > 200:
            continue
        scrape_yr = _scrape_year(x)
        completed = scrape_yr - a
        plan.append((d.id, age, scrape_yr, completed))

    print(f"need backfill: {len(plan)} docs")
    for did, age, sy, cy in plan[:20]:
        print(f"  {did}: 屋齡 {age} 年（{sy} 年抓到）→ 完工 {cy} 年")
    if len(plan) > 20:
        print(f"  ... ({len(plan) - 20} more)")

    if not apply:
        print("\n[dry-run] 加 --apply 才會真寫")
        return

    print("\n=== APPLYING ===")
    for did, _age, _sy, completed in plan:
        col.document(did).update({
            "building_age_completed_year": completed,
            "building_age_source": "backfill_from_age",
        })
    print(f"done: updated {len(plan)} docs")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    main(args.apply)
