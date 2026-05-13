"""Run calculate_renewal_scenarios for each test case, emit results as JSON.
Output goes to stdout for the cross-check script to consume."""
from __future__ import annotations
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.stdout.reconfigure(encoding="utf-8")

from analysis.scorer import calculate_renewal_scenarios


def is_qualified_for_fz_dugen(doc: dict) -> bool:
    """模擬 analysis_pipeline.py:1215-1218 的邏輯：台北市 + 1974 前蓋的。
    優先用 completed_year，退而用 building_age (current_year - age)。"""
    if doc.get("city") != "台北市":
        return False
    year = doc.get("building_age_completed_year")
    if not year:
        age = doc.get("building_age")
        if age is not None:
            try:
                year = datetime.now().year - int(age)
            except (TypeError, ValueError):
                return False
    if not year:
        return False
    try:
        return int(year) <= 1974
    except (TypeError, ValueError):
        return False


def run_case(case: dict) -> dict:
    doc = case["doc"]
    rv = calculate_renewal_scenarios(
        land_area_ping=doc.get("land_area_ping"),
        zoning=doc.get("zoning"),
        district=doc.get("district"),
        price_ntd=doc.get("price_ntd"),
        new_house_price_wan_per_ping=case.get("new_house_price_wan_per_ping"),
        is_qualified_for_fz_dugen=is_qualified_for_fz_dugen(doc),
        road_width_m=doc.get("road_width_m"),
        floor=doc.get("floor"),
        floor_range_min=doc.get("floor_range_min"),
        building_area_ping=doc.get("building_area_ping"),
    )
    scenarios = rv.get("scenarios") or {}
    return {
        "name": case["name"],
        "effective_far_pct": rv.get("effective_far_pct"),
        "share_ratio": rv.get("share_ratio"),
        "floor_premium": rv.get("floor_premium"),
        "weishau_multiple": (scenarios.get("危老") or {}).get("multiple"),
        "dugen_multiple": (scenarios.get("都更") or {}).get("multiple"),
        "fz_dugen_multiple": (scenarios.get("防災都更") or {}).get("multiple"),
        "weishau_share_ping": (scenarios.get("危老") or {}).get("share_ping"),
        "dugen_share_ping": (scenarios.get("都更") or {}).get("share_ping"),
        "fz_dugen_share_ping": (scenarios.get("防災都更") or {}).get("share_ping"),
        "note": rv.get("note"),
    }


def main():
    cases_file = ROOT / "tests" / "scenarios_cases.json"
    cases = json.loads(cases_file.read_text(encoding="utf-8"))
    results = [run_case(c) for c in cases]
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
