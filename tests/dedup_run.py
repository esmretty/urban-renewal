"""Run all dedup test cases against `is_same_property`.

Usage:
    python tests/dedup_run.py             # 只跑 expected baseline
    python tests/dedup_run.py --audit     # 加跑 DB 1449 doc pair 全 audit

加新 test case：直接編輯 tests/dedup_cases.json，每筆需含:
  {
    "name": "簡短描述",
    "a": { ... doc dict ... },
    "b": { ... doc dict ... },
    "expected": true | false,
    "why": "為什麼"
  }
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.stdout.reconfigure(encoding="utf-8")

from database.dedup import is_same_property


def run_cases() -> int:
    cases = json.loads((ROOT / "tests" / "dedup_cases.json").read_text(encoding="utf-8"))
    print(f"=== 跑 {len(cases)} 個 expected case ===")
    fails = []
    for i, c in enumerate(cases):
        actual = is_same_property(c["a"], c["b"])
        if actual != c["expected"]:
            fails.append((i + 1, c["name"], c["expected"], actual, c.get("why", "")))
    if not fails:
        print(f"  ✓ 全部 {len(cases)} case 通過")
        return 0
    print(f"  ✗ {len(fails)}/{len(cases)} case 失敗:")
    for i, name, exp, act, why in fails:
        print(f"    [{i}] {name}")
        print(f"        expected={exp} actual={act}")
        print(f"        why: {why}")
    return 1


def run_audit() -> int:
    """跑 prod DB 全 doc pairs，比對「現有系統視為 dup (已 merge 成同 doc)」vs unified rule。"""
    import os
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "firebase-credentials.json")
    from database.db import get_col
    col = get_col()
    docs_raw = list(col.limit(2000).get())
    docs = [(d.id, d.to_dict() or {}) for d in docs_raw]
    print(f"\n=== Audit {len(docs)} prod DB doc ===")

    # 找跨來源 merged doc — 「現有系統認為 dup」的真實案例
    merged_ground_truth = []
    for doc_id, d in docs:
        keys = d.get("source_keys") or []
        prefixes = {k.split(":")[0] for k in keys if ":" in k}
        if len(prefixes) >= 2:
            merged_ground_truth.append((doc_id, d))
    print(f"  跨來源 merged doc: {len(merged_ground_truth)} 個 (這些必須通過新規則才算 OK)")

    # 對 merged doc，因為已合併成 1 個，我們只能驗「這 doc 自己跟自己 → 應該 same_property==True」
    # （平凡測試，但至少證明 unified function 不會對「現有認 dup 的最終 address」說 not dup）
    print(f"  (merged doc 已合併為 1 筆，無法直接 cross-validate；只能跑 self-check)")

    # 真正有用的 audit: 對所有 doc pair 跑 unified rule, 找出
    # (1) unified 認 dup 但現在是分開的 doc → 漏 merge 的候選
    # (2) unified 不認 dup 但屬於同一 merged doc 的 (不可能因為 merged 已合成一筆 — N/A)
    candidates_to_merge = []
    n = len(docs)
    print(f"  跑 {n}*({n}-1)/2 = {n*(n-1)//2} pairs ...")
    for i in range(n):
        for j in range(i + 1, n):
            if is_same_property(docs[i][1], docs[j][1]):
                # 兩個 doc 已存在 (沒合併) 但 unified 認 dup
                candidates_to_merge.append((docs[i][0], docs[j][0], docs[i][1], docs[j][1]))

    print(f"\n  ⚠ {len(candidates_to_merge)} 對 doc unified rule 認 dup 但目前是分開的:")
    for i, (a_id, b_id, a, b) in enumerate(candidates_to_merge[:20]):
        print(f"\n  [{i+1}] doc_a={a_id[:8]} addr={a.get('address')!r} floor={a.get('floor')} price={a.get('price_ntd')} bld={a.get('building_area_ping')}")
        print(f"      doc_b={b_id[:8]} addr={b.get('address')!r} floor={b.get('floor')} price={b.get('price_ntd')} bld={b.get('building_area_ping')}")
    if len(candidates_to_merge) > 20:
        print(f"\n  ... 還有 {len(candidates_to_merge) - 20} 個 (太多)")
    return 0


def main():
    rc = run_cases()
    if "--audit" in sys.argv:
        run_audit()
    return rc


if __name__ == "__main__":
    sys.exit(main())
