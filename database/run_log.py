"""統一的 action log：手動 + scheduler 每次動作都記錄到具體物件層級。

存兩處：
1. 本機 JSONL 檔（data/logs/actions.YYYY-MM-DD.jsonl）— 給 server admin 用 grep / Read 查
2. Firestore run_logs collection — 給 admin UI 顯示

每筆 entry shape:
{
  "at": "2026-04-27T19:00:01+08:00",
  "trigger": "scheduler_scan_0" | "manual_batch" | "manual_url" | "manual_reanalyze" | "verify_alive_manual" | "verify_alive_scheduler" | "retry_queue",
  "action": "batch_start" | "batch_end" | "new" | "enrich" | "dup_merge" | "replacement" |
            "cross_source" | "verify_alive_archive" | "retry_attempt" | "error",
  "source_id": "591_xxx" | None,
  "doc_id": "20260427-xxx" | None,
  "message": "human-readable",
  "details": {...任意 dict...}
}
"""
from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_BASE_DIR = Path(__file__).resolve().parent.parent
_LOG_DIR = _BASE_DIR / "data" / "logs"
_FS_COLLECTION = "run_logs"
_FS_MAX_KEEP = 5000   # 超過自動清舊（簡單 cap）


def _ensure_dir():
    _LOG_DIR.mkdir(parents=True, exist_ok=True)


def _today_log_path() -> Path:
    from database.time_utils import now_tw
    d = now_tw().strftime("%Y-%m-%d")
    return _LOG_DIR / f"actions.{d}.jsonl"


def log_action(
    trigger: str,
    action: str,
    *,
    source_id: Optional[str] = None,
    doc_id: Optional[str] = None,
    message: str = "",
    details: Optional[dict] = None,
) -> None:
    """寫一筆 action log。永不 raise（raise 也會被 swallow）— logging 不可影響主流程。"""
    try:
        from database.time_utils import now_tw_iso
        entry: dict[str, Any] = {
            "at": now_tw_iso(),
            "trigger": trigger,
            "action": action,
        }
        if source_id: entry["source_id"] = source_id
        if doc_id: entry["doc_id"] = doc_id
        if message: entry["message"] = message
        if details: entry["details"] = details

        # 1. 寫本機 JSONL
        try:
            _ensure_dir()
            with _today_log_path().open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
        except Exception as e:
            logger.warning("[run_log] JSONL write failed: %s", e)

        # 2. 寫 Firestore（best-effort，失敗不影響）
        try:
            from database.db import get_firestore
            get_firestore().collection(_FS_COLLECTION).add(entry)
        except Exception as e:
            logger.warning("[run_log] Firestore write failed: %s", e)
    except Exception as e:
        logger.warning("[run_log] unexpected: %s", e)


def list_recent(limit: int = 200, trigger_prefix: Optional[str] = None) -> list[dict]:
    """取最近 N 筆 log entry（依 at desc）。給 admin endpoint 用。"""
    try:
        from database.db import get_firestore
        from google.cloud.firestore_v1.base_query import FieldFilter
        q = get_firestore().collection(_FS_COLLECTION).order_by("at", direction="DESCENDING").limit(int(limit))
        if trigger_prefix:
            # Firestore range query 一次只能對一個欄位，所以 trigger 過濾改後處理
            pass
        items = []
        for d in q.stream():
            x = d.to_dict() or {}
            x["_id"] = d.id
            if trigger_prefix and not (x.get("trigger") or "").startswith(trigger_prefix):
                continue
            items.append(x)
        return items
    except Exception as e:
        logger.warning("[run_log] list_recent failed: %s", e)
        return []


def prune_old(keep_max: int = _FS_MAX_KEEP) -> int:
    """超過 keep_max 筆就刪最舊的（best-effort）。回傳刪了幾筆。"""
    try:
        from database.db import get_firestore
        col = get_firestore().collection(_FS_COLLECTION)
        # 以 at 升序，刪到只剩 keep_max
        all_docs = list(col.order_by("at").stream())
        excess = len(all_docs) - keep_max
        if excess <= 0:
            return 0
        deleted = 0
        for d in all_docs[:excess]:
            try:
                col.document(d.id).delete()
                deleted += 1
            except Exception:
                pass
        return deleted
    except Exception as e:
        logger.warning("[run_log] prune failed: %s", e)
        return 0
