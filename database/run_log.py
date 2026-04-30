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


def build_doc_log_details(item: dict, doc_data: Optional[dict] = None, **extras) -> dict:
    """組 log_action 的 details dict — 統一抓 batch/manual/url 各流程「處理一筆物件」的關鍵欄位。

    包含：來源連結、標題、地址、價格、建坪/地坪、樓層、屋齡、分區、評分、AI 建議、旗標。
    讓 admin 在執行紀錄詳情頁能看到完整脈絡，不用再去 properties collection 對照。
    """
    doc_data = doc_data or {}
    sources = doc_data.get("sources") or item.get("sources") or []
    url = None
    if sources and isinstance(sources, list) and isinstance(sources[0], dict):
        url = sources[0].get("url")
    if not url:
        url = item.get("url") or doc_data.get("source_url")
    addr = (
        doc_data.get("address_inferred")
        or doc_data.get("address")
        or item.get("address_inferred")
        or item.get("address")
    )
    out = {
        "title": item.get("title") or doc_data.get("title"),
        "url": url,
        "address": addr,
        "price_ntd": doc_data.get("price_ntd") or item.get("price_ntd"),
        "building_area_ping": doc_data.get("building_area_ping") or item.get("building_area_ping"),
        "land_area_ping": doc_data.get("land_area_ping") or item.get("land_area_ping"),
        "total_floors": doc_data.get("total_floors") or item.get("total_floors"),
        "floor": doc_data.get("floor") or item.get("floor"),
        "building_age": doc_data.get("building_age") or item.get("building_age"),
        "zoning": doc_data.get("zoning"),
        "score_total": doc_data.get("score_total"),
        "ai_recommendation": doc_data.get("ai_recommendation"),
        "is_remote_area": doc_data.get("is_remote_area"),
        "unsuitable_for_renewal": doc_data.get("unsuitable_for_renewal"),
    }
    out.update(extras)
    # 過濾 None 值，控制 Firestore doc 大小
    return {k: v for k, v in out.items() if v is not None and v != ""}


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


# 標示 session 開始/結束的 action types（用於分組）
_START_ACTIONS = {"batch_start", "verify_alive_start"}
_END_ACTIONS = {"batch_end", "verify_alive_end"}


def list_sessions(limit: int = 50) -> list[dict]:
    """把 action logs 依 trigger + start/end 分組成 session。
    每個 session = 從 *_start 到 *_end 之間（同 trigger）的所有 action。
    回傳：依 started_at desc 排序，最多 limit 個 session。"""
    try:
        from database.db import get_firestore
        # 拉一段時間內所有 logs（避免漏掉 session 起點）
        all_docs = list(
            get_firestore().collection(_FS_COLLECTION)
            .order_by("at", direction="DESCENDING").limit(2000).stream()
        )
        # 依 trigger 分群、找成對 start/end
        # 簡化：按時間順序掃，遇 start 開新 session、遇 end 關掉 session
        all_logs = []
        for d in all_docs:
            x = d.to_dict() or {}
            x["_id"] = d.id
            all_logs.append(x)
        all_logs.sort(key=lambda x: x.get("at") or "")   # asc

        # open_sessions: trigger → session dict（最近一個未關的）
        open_sessions: dict = {}
        sessions: list = []
        for log in all_logs:
            t = log.get("trigger") or ""
            a = log.get("action") or ""
            if a in _START_ACTIONS:
                # 若同 trigger 有未關的 session → 強制關掉（孤兒）
                prev = open_sessions.pop(t, None)
                if prev:
                    prev["status"] = "interrupted"
                    sessions.append(prev)
                # 開新 session
                open_sessions[t] = {
                    "trigger": t,
                    "started_at": log.get("at"),
                    "ended_at": None,
                    "status": "running",
                    "start_log": log,
                    "end_log": None,
                    "actions": [],
                    "counts": {},   # action_type → count
                }
            elif a in _END_ACTIONS:
                sess = open_sessions.pop(t, None)
                if sess:
                    sess["ended_at"] = log.get("at")
                    sess["status"] = "done"
                    sess["end_log"] = log
                    sess["counts"][a] = sess["counts"].get(a, 0) + 1
                    sessions.append(sess)
                else:
                    # 孤兒 end（找不到對應 start）
                    sessions.append({
                        "trigger": t, "started_at": None, "ended_at": log.get("at"),
                        "status": "orphan_end", "start_log": None, "end_log": log,
                        "actions": [log], "counts": {a: 1},
                    })
            else:
                # 內部 action：歸入 open session（若有同 trigger）
                sess = open_sessions.get(t)
                if sess:
                    sess["actions"].append(log)
                    sess["counts"][a] = sess["counts"].get(a, 0) + 1

        # 還沒關的 session 也加進結果（標 running）
        for sess in open_sessions.values():
            sessions.append(sess)

        # 依 started_at desc 排序
        sessions.sort(key=lambda s: s.get("started_at") or "", reverse=True)
        return sessions[:limit]
    except Exception as e:
        logger.warning("[run_log] list_sessions failed: %s", e)
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
