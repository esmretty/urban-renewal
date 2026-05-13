"""Pre-deploy smoke test — 在 git push 之前跑，抓 router refactor 漏掉的 cross-module reference。

兩條 check（總共 ~2 秒）：
  1. import 所有 router module — 抓 module-top-level 的 `from api.app import _x` 失敗
  2. cross-module hasattr — 掃 router 檔內所有 `_app.X` / `from api.app import X`，驗 X 在 api.app 還在

不直接 invoke endpoint（會打 Firestore/GeoServer 等外部 service，~3 分鐘）。
§1 + §2 已能抓 95% 的 refactor 漏掉，剩 5%「endpoint body 內條件式 lazy reference」太罕見。

過往踩雷：sprint 2 step 6 (commit 6a47365) regex strip admin_scheduler 過頭把 3 個
module-level 常數一併刪掉，但 router 還用 `_app.SCHEDULER_X` lazy reference。
import 不炸、Lala AST 等價、Nicole route/auth 全綠，prod 一打 endpoint 才 500。
這支腳本就是補抓這類 bug，~2 秒結束。
"""
from __future__ import annotations
import importlib
import re
import sys
import time
import tokenize
from pathlib import Path

# Windows cp950 default stdout 會把 ✓ ✗ 等 unicode 炸成 UnicodeEncodeError；
# deploy.sh 從 bash 開 subshell 跑這支時尤其明顯。強制 UTF-8 輸出。
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))   # 讓 `python scripts/predeploy_smoke.py` 能 import api


def _strip_strings_and_comments(src: str) -> str:
    """用 tokenizer 把 string literal / comment 整段抽掉，避免 docstring 內的 `_app.X` 被誤判為引用。
    保留行號（每個 token 的換行依原樣放回去）。"""
    import io
    out_lines: dict[int, list[str]] = {}
    max_line = src.count("\n") + 1
    try:
        buf = io.BytesIO(src.encode("utf-8"))
        toks = tokenize.tokenize(buf.readline)
        for tok in toks:
            if tok.type in (tokenize.ENCODING, tokenize.STRING, tokenize.COMMENT,
                            tokenize.NEWLINE, tokenize.NL, tokenize.INDENT, tokenize.DEDENT,
                            tokenize.ENDMARKER):
                continue
            if hasattr(tokenize, "FSTRING_MIDDLE") and tok.type == tokenize.FSTRING_MIDDLE:
                continue
            line_no = tok.start[0]
            out_lines.setdefault(line_no, []).append(tok.string)
    except Exception:
        return src
    return "\n".join(" ".join(out_lines.get(i, [])) for i in range(1, max_line + 1))


def _router_files() -> list[Path]:
    files = list((ROOT / "api" / "routers").glob("*.py"))
    files = [f for f in files if not f.name.startswith("_") and f.name != "__init__.py"]
    for extra in ("gis_overlay.py", "cadastral_search.py", "external_checks.py", "user_reads.py"):
        p = ROOT / "api" / extra
        if p.exists():
            files.append(p)
    return files


def collect_app_refs() -> dict[str, list[tuple[str, int]]]:
    """掃所有 router 檔（去掉 string/comment），收集 `_app.X` + `from api.app import X` 引用。"""
    # 容許 tokenizer 重組後的 token 之間有空白（`_app` `.` `name` 三 token）
    pat_dot = re.compile(r'(?<![A-Za-z0-9_])_app\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)')
    pat_import = re.compile(r'from\s+api\s*\.\s*app\s+import\s+([A-Za-z0-9_,\s]+)')

    refs: dict[str, list[tuple[str, int]]] = {}
    for f in _router_files():
        raw = f.read_text(encoding="utf-8")
        clean = _strip_strings_and_comments(raw)
        rel = str(f.relative_to(ROOT)).replace("\\", "/")

        # 走 cleaned source 抓 _app.X，行號用 cleaned 跟 raw 都 OK（tokenizer 保留換行）
        for i, line in enumerate(clean.split("\n"), 1):
            for m in pat_dot.finditer(line):
                refs.setdefault(m.group(1), []).append((rel, i))
        # from api.app import X1, X2 — 也走 cleaned，但要把整段 multi-line import 拍平
        for m in pat_import.finditer(clean):
            line_no = clean[: m.start()].count("\n") + 1
            names_blob = m.group(1)
            for chunk in names_blob.split(","):
                actual = chunk.strip().split(" as ")[0].strip()
                if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", actual or ""):
                    refs.setdefault(actual, []).append((rel, line_no))
    return refs


def check_router_imports() -> int:
    """§1 import 每個 router module — 抓 module-top-level 的 import 錯誤。"""
    print("[predeploy] §1 import router modules")
    fails = []
    for f in _router_files():
        # api/routers/X.py -> api.routers.X ; api/Y.py -> api.Y
        rel = f.relative_to(ROOT).with_suffix("")
        mod_name = ".".join(rel.parts)
        try:
            importlib.import_module(mod_name)
        except Exception as e:
            fails.append((mod_name, e))
    if fails:
        print(f"  ✗ {len(fails)} 個 module import 失敗:")
        for n, e in fails:
            print(f"      {n}: {type(e).__name__}: {e}")
        return 1
    print(f"  ✓ {len(_router_files())} 個 router module 全部 import 成功")
    return 0


def check_hasattr() -> int:
    """§2 cross-module hasattr — 抓 lazy `_app.X` 引用缺失。"""
    print("[predeploy] §2 cross-module hasattr (_app.X / from api.app import X)")
    import api.app as _app

    refs = collect_app_refs()
    missing = [(n, s) for n, s in refs.items() if not hasattr(_app, n)]
    print(f"  收集 {len(refs)} 個 router→api.app 引用")
    if missing:
        print(f"  ✗ 缺失 {len(missing)} 個:")
        for name, sites in missing:
            print(f"      {name}")
            for f, i in sites[:3]:
                print(f"        - {f}:{i}")
        return 1
    print("  ✓ 全部 hasattr 通過")
    return 0


def check_pyflakes() -> int:
    """§3 pyflakes 抓 undefined name (silently-failing 變數引用)。

    上次踩雷: F 搬 scrape/manual_analyze 時 admin_scrape.py 內 `db.collection(...)`
    + `get_user_watchlist(uid)` 都是 undefined name。前者是 pre-existing latent bug，
    後者是 F 漏 import。都被 try/except 吞掉, endpoint 仍回 200 ok → 前端看到「成功」
    但 DB write 沒發生。AST parse 不會 catch (因為 Python 對函式體 name 是 lazy 解析)。

    這條只 fail on "undefined name" — 忽略 unused import / f-string 等 cosmetic warning。
    """
    import subprocess
    print("[predeploy] §3 pyflakes (undefined name only)")
    targets = [str(f) for f in _router_files()]
    targets.append(str(ROOT / "api" / "app.py"))
    r = subprocess.run(
        [sys.executable, "-m", "pyflakes", *targets],
        capture_output=True, text=True, encoding="utf-8",
    )
    # pyflakes 對任何 warning 都 exit 1，要自己 filter 只看 undefined name
    undef_lines = [l for l in r.stdout.split("\n") if "undefined name" in l]
    if undef_lines:
        print(f"  ✗ undefined name 共 {len(undef_lines)} 處:")
        for l in undef_lines[:10]:
            print(f"      {l}")
        if len(undef_lines) > 10:
            print(f"      ... 還有 {len(undef_lines) - 10} 條")
        return 1
    print(f"  ✓ 0 個 undefined name")
    return 0


def check_scenarios_xcheck() -> int:
    """§4 跑 Python + JS 算式對 baseline 比對 — 抓「前後端算式漂掉」這類 drift。
    跑 ~5 秒 (Python 1s + Node 0.5s + 比對)。baseline: tests/scenarios_expected.json"""
    import subprocess
    print("[predeploy] §4 scenarios cross-check (Python + JS 算式 baseline 比對)")
    r = subprocess.run(
        [sys.executable, str(ROOT / "tests" / "scenarios_xcheck.py")],
        capture_output=True, text=True, encoding="utf-8",
    )
    if r.returncode != 0:
        print(r.stdout)
        return 1
    # 萃取 「✓ ... 一致」摘要行
    for line in r.stdout.split("\n"):
        if "Python vs baseline" in line or "JS vs baseline" in line or "✓" in line:
            print(f"  {line.strip()}")
    return 0


def check_dedup_baseline() -> int:
    """§5 跑 dedup expected case baseline — 抓「is_same_property 行為漂掉」這類 drift。
    跑 ~1 秒（純 in-memory 邏輯）。baseline: tests/dedup_cases.json"""
    import subprocess
    print("[predeploy] §5 dedup rule baseline (is_same_property 22 expected cases)")
    r = subprocess.run(
        [sys.executable, str(ROOT / "tests" / "dedup_run.py")],
        capture_output=True, text=True, encoding="utf-8",
    )
    if r.returncode != 0:
        print(r.stdout)
        return 1
    for line in r.stdout.split("\n"):
        if "✓" in line or "✗" in line:
            print(f"  {line.strip()}")
    return 0


def main() -> int:
    t0 = time.time()
    rc = 0
    rc |= check_router_imports()
    rc |= check_hasattr()
    rc |= check_pyflakes()
    rc |= check_scenarios_xcheck()
    rc |= check_dedup_baseline()
    elapsed = time.time() - t0
    if rc:
        print(f"\n[predeploy] FAIL ({elapsed:.1f}s) — 修好再 push")
    else:
        print(f"\n[predeploy] OK ({elapsed:.1f}s)")
    return rc


if __name__ == "__main__":
    sys.exit(main())
