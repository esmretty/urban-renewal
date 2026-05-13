"""Sprint 2 step F migration: scrape/manual_analyze → api/routers/admin_scrape.py.

Run once. Reads api/app.py, extracts 17 blocks + _SCRAPE_PROGRESS_FILE constant,
writes api/routers/admin_scrape.py, strips those ranges from app.py.

Does NOT update callers (admin_data_ops.py imports, app.py scheduler tick) —
that's a separate step using Edit tool.
"""
from __future__ import annotations
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
APP_PY = ROOT / "api" / "app.py"
ROUTER_PY = ROOT / "api" / "routers" / "admin_scrape.py"

src = APP_PY.read_text(encoding="utf-8")
lines = src.split("\n")


def find_top_level_block(start_def_line_1idx):
    start = start_def_line_1idx
    while start > 1 and lines[start - 2].startswith("@"):
        start -= 1
    for i in range(start_def_line_1idx, len(lines)):
        if i + 1 == start_def_line_1idx:
            continue
        l = lines[i]
        if l and not l.startswith((" ", "\t")) and l.strip() and not l.startswith("#"):
            return (start, i)
    return (start, len(lines))


patterns = [
    r"^def _safe_put_progress\(",
    r"^def _reset_scrape_progress\(",
    r"^class ScrapeRequest\(BaseModel\)",
    r"^async def trigger_scrape\(",
    r"^async def scrape_status\(",
    r"^async def _sse_generator\(",
    r"^async def _run_scrape_task\(",
    r"^def _scrape_and_analyze\(",
    r"^class ManualAnalyzeReq\(BaseModel\)",
    r"^async def analyze_manual\(",
    r"^async def _run_manual_analysis\(",
    r"^class ScrapeUrlRequest\(BaseModel\)",
    r"^async def scrape_url\(",
    r"^class KillSessionReq\(BaseModel\)",
    r"^async def admin_kill_session\(",
    r"^async def admin_kill_scrape\(",
    r"^def _scrape_single_url\(",
]

blocks = []
for pat in patterns:
    for i, l in enumerate(lines, 1):
        if re.search(pat, l):
            blocks.append(find_top_level_block(i))
            break
    else:
        raise SystemExit(f"pattern not found: {pat}")

# _SCRAPE_PROGRESS_FILE constant + preceding comments
for i, l in enumerate(lines, 1):
    if re.search(r"^_SCRAPE_PROGRESS_FILE\s*=", l):
        start = i
        while start > 1 and lines[start - 2].strip().startswith("#"):
            start -= 1
        blocks.append((start, i))
        break

blocks.sort()

prev_end = 0
for s, e in blocks:
    if s <= prev_end:
        raise SystemExit(f"overlap: ({s},{e}) overlaps prev end {prev_end}")
    prev_end = e

to_remove = set()
extracted_chunks = []
for s, e in blocks:
    chunk = "\n".join(lines[s - 1:e])
    extracted_chunks.append(chunk)
    for k in range(s, e + 1):
        to_remove.add(k)

MUTABLE_STATE = {"_scrape_running", "_cancel_requested", "_url_inflight", "_url_waiting", "_url_sem"}
APP_PRIVATE_HELPERS = {
    "_ensure_user_profile",
    "_is_replacement_change",
    "_safe_doc",
    "_scrape_single_url_591_inner",
    "_scrape_single_url_sinyi",
    "_scrape_single_url_yongqing",
    "_verify_and_prune_sources",
}


def transform_chunk(chunk: str) -> str:
    chunk = re.sub(r"^@app\.(get|post|delete|put|patch)\(", r"@router.\1(", chunk, flags=re.M)
    # Drop entire `global ...` line if any transformed state name appears anywhere on it
    # (necessary because some original lines list state name as 2nd identifier, e.g.
    # `global _url_running, _cancel_requested` — `_url_running` is a leftover undefined name).
    state_alt = r"(?:_scrape_running|_cancel_requested|_url_sem|_url_inflight|_url_waiting)"
    chunk = re.sub(rf"^\s*global\s+[^\n]*{state_alt}[^\n]*\n", "", chunk, flags=re.M)
    for name in MUTABLE_STATE:
        chunk = re.sub(rf"(?<![\w.])({name})(?!\w)", f"_app.{name}", chunk)
    return chunk


def add_late_imports(chunk: str) -> str:
    used_helpers = {h for h in APP_PRIVATE_HELPERS if re.search(rf"(?<![\w.])({h})\s*\(", chunk)}
    needs_app = "_app." in chunk

    if not used_helpers and not needs_app:
        return chunk

    m = re.match(r"^((?:async )?def [^\n]+\n)", chunk)
    if not m:
        return chunk

    indent = "    "
    imports = []
    if needs_app:
        imports.append(f"{indent}import api.app as _app")
    if used_helpers:
        imports.append(f"{indent}from api.app import {', '.join(sorted(used_helpers))}")
    inject = "\n".join(imports) + "\n"
    return chunk[: m.end(1)] + inject + chunk[m.end(1):]


new_chunks = []
for c in extracted_chunks:
    c = transform_chunk(c)
    if re.match(r"^(async )?def ", c):
        c = add_late_imports(c)
    new_chunks.append(c)

header = '''"""
Scrape / manual_analyze endpoints — Sprint 2 step F (拆 router 收尾) 從 api/app.py 拆出。

6 個 endpoint：
  - POST /api/scrape                      批次爬取觸發 (admin)
  - GET  /api/scrape/status               SSE 進度推播
  - POST /api/scrape_url                  單筆 URL 分析 (user)
  - POST /api/manual_analyze              手動地址分析 (user)
  - POST /admin/scrape/kill               中斷正在跑的 batch (admin)
  - POST /admin/scrape/kill_session       中斷 zombie session (admin)

4 個 Pydantic：ScrapeRequest / ManualAnalyzeReq / ScrapeUrlRequest / KillSessionReq

9 個 helper：_safe_put_progress / _reset_scrape_progress / _sse_generator /
            _run_scrape_task / _scrape_and_analyze (1248 行核心 batch 引擎) /
            _run_manual_analysis / _scrape_single_url / 兩個 admin_kill_*

mutable scalar state (_scrape_running / _cancel_requested / _url_sem 等) 仍在
api/app.py 內 (admin_scheduler.py 也透過 _app._scrape_running 讀)，這支 router 用
`import api.app as _app; _app.X` pattern 取最新值。

_SCRAPE_PROGRESS_FILE 路徑常數搬過來。
"""
from __future__ import annotations
import asyncio
import json
import logging
from typing import Optional, AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException, Request, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.auth import require_admin, get_current_user
from database.db import get_col, get_firestore
from database.time_utils import now_tw, now_tw_iso
from config import BASE_DIR

logger = logging.getLogger(__name__)
router = APIRouter()
'''

body = "\n\n".join(new_chunks)
new_router_content = header + "\n\n" + body.rstrip() + "\n"
ROUTER_PY.write_text(new_router_content, encoding="utf-8")

new_lines = [l for i, l in enumerate(lines, 1) if i not in to_remove]
new_src = "\n".join(new_lines)
APP_PY.write_text(new_src, encoding="utf-8")

print(f"✓ admin_scrape.py: {new_router_content.count(chr(10))} lines")
print(f"✓ api/app.py: {len(new_lines)} lines (was {len(lines)})")
print(f"✓ removed: {len(to_remove)} lines, {len(blocks)} blocks")
