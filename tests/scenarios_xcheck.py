"""Run Python + JS scenario calculators against frozen baseline. Fail on drift.

Baseline lives at tests/scenarios_expected.json (生成方式: python tests/scenarios_run_python.py > 該檔)。
任何一方算出來的結果跟 baseline 不一致 → exit 1，predeploy_smoke 擋下 push。

更新 baseline 的時機：
- 公式被刻意改了（例: share_ratio 表更新、bonus 比例調整）
- 改完 Python + JS 兩邊都更新後，重新生成 baseline:
    python tests/scenarios_run_python.py > tests/scenarios_expected.json
- 同時手動 review 新的 baseline 數字是否合理（這是 spec 的 single source of truth）
"""
from __future__ import annotations
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.stdout.reconfigure(encoding="utf-8")


def run_python() -> list[dict]:
    r = subprocess.run(
        [sys.executable, str(ROOT / "tests" / "scenarios_run_python.py")],
        capture_output=True, text=True, encoding="utf-8",
    )
    if r.returncode != 0:
        print("Python runner failed:\n" + r.stderr)
        sys.exit(2)
    return json.loads(r.stdout)


def run_node() -> list[dict]:
    r = subprocess.run(
        ["node", str(ROOT / "tests" / "scenarios_run_js.mjs")],
        capture_output=True, text=True, encoding="utf-8",
    )
    if r.returncode != 0:
        print("Node runner failed:\n" + r.stderr)
        sys.exit(2)
    return json.loads(r.stdout)


def close_enough(a, b, tol):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(a) - float(b)) <= tol
    except (TypeError, ValueError):
        return a == b


# 每邊各自要比的欄位 + 容差 (Python 算 3 scenario, JS 算 2，欄位不一樣)
PY_FIELDS = {
    "effective_far_pct": 1,
    "share_ratio": 0.01,
    "weishau_multiple": 0.01,
    "dugen_multiple": 0.01,
    "fz_dugen_multiple": 0.01,
    "weishau_share_ping": 0.5,
    "dugen_share_ping": 0.5,
    "fz_dugen_share_ping": 0.5,
}
JS_FIELDS = {
    "effective_far_pct": 1,
    "share_ratio": 0.01,
    "weishau_multiple": 0.01,
    "dugen_active_multiple": 0.01,
    "weishau_share_ping": 0.5,
    "dugen_active_share_ping": 0.5,
}


def compare_against_baseline(actual: list[dict], baseline: list[dict], fields: dict, label: str) -> list[tuple]:
    """回傳 list of (case_idx, name, field, expected, actual) for any drift。"""
    diffs = []
    if len(actual) != len(baseline):
        diffs.append((0, "<count>", "case_count", len(baseline), len(actual)))
        return diffs
    for i, (a, b) in enumerate(zip(actual, baseline)):
        for field, tol in fields.items():
            if field not in b:
                continue
            if not close_enough(a.get(field), b.get(field), tol):
                diffs.append((i, b["name"], field, b.get(field), a.get(field)))
    return diffs


def main():
    baseline_file = ROOT / "tests" / "scenarios_expected.json"
    if not baseline_file.exists():
        print(f"baseline 不存在: {baseline_file}")
        print("第一次跑請先: python tests/scenarios_run_python.py > tests/scenarios_expected.json")
        return 1
    baseline = json.loads(baseline_file.read_text(encoding="utf-8"))

    py = run_python()
    js_raw = run_node()

    # JS 只算「都更 或 防災都更 一個」(依 fangzai)。
    # 對 JS 而言，「active dugen scenario」= fangzai ? fz_dugen : dugen
    # 把 baseline 對應欄位 collapse 成「active」做比對，不比 JS 沒算的那條
    def baseline_active_dugen(b: dict, is_fz: bool) -> dict:
        # 從 baseline 抽出 JS 視角的「該物件 active 都更倍數」
        return {
            **b,
            "dugen_active_multiple": b.get("fz_dugen_multiple") if is_fz else b.get("dugen_multiple"),
            "dugen_active_share_ping": b.get("fz_dugen_share_ping") if is_fz else b.get("dugen_share_ping"),
        }

    js = []
    baseline_for_js = []
    for jr, b in zip(js_raw, baseline):
        is_fz = jr.get("is_fangzai")
        js.append({
            "name": jr["name"],
            "effective_far_pct": jr["effective_far_pct"],
            "share_ratio": jr["share_ratio"],
            "weishau_multiple": jr["weishau_multiple"],
            "dugen_active_multiple": jr["dugen_or_fz_multiple"],
            "weishau_share_ping": jr["weishau_share_ping"],
            "dugen_active_share_ping": jr["dugen_or_fz_share_ping"],
        })
        baseline_for_js.append(baseline_active_dugen(b, is_fz))

    py_diffs = compare_against_baseline(py, baseline, PY_FIELDS, "Python")
    js_diffs = compare_against_baseline(js, baseline_for_js, JS_FIELDS, "JS")

    print(f"=== {len(baseline)} cases ===")
    print(f"  Python vs baseline: {len(py_diffs)} 個漂掉")
    print(f"  JS vs baseline:     {len(js_diffs)} 個漂掉")

    if py_diffs or js_diffs:
        print()
        if py_diffs:
            print("--- Python drift ---")
            for i, name, field, exp, act in py_diffs:
                print(f"  [{i+1}] {name}: {field}: expected={exp} actual={act}")
        if js_diffs:
            print("--- JS drift ---")
            for i, name, field, exp, act in js_diffs:
                print(f"  [{i+1}] {name}: {field}: expected={exp} actual={act}")
        print()
        print("如果這是刻意改公式 → 更新 baseline:")
        print("  python tests/scenarios_run_python.py > tests/scenarios_expected.json")
        print("  (然後手動 review 新數字、commit)")
        return 1

    print("\n✓ Python + JS 都跟 baseline 一致")
    return 0


if __name__ == "__main__":
    sys.exit(main())
