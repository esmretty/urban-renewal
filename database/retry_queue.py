"""失敗重試佇列。
任何 source 的物件 enrich 失敗（detail page 抓不到核心欄位、timeout 等）
→ 寫入 Firestore collection `retry_queue` → 背景 loop 10 分鐘後自動重抓。

寫入時機：scraper 偵測到 enrich 失敗（非 404 / 物件不存在 — 物件本身要是真的存在）。
重抓成功 → 從 queue 移除。
連續失敗 → 重新排程到下一次 retry，最多 N 次後放棄。
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

QUEUE_COLLECTION = "retry_queue"
DEFAULT_RETRY_DELAY_MIN = 10
MAX_ATTEMPTS = 4   # 包含初次失敗 → 後面再 retry 3 次（共 4 次嘗試）


def _tw_now():
    return datetime.now(timezone(timedelta(hours=8)))


def _iso(dt):
    return dt.isoformat()


def enqueue(source_id: str, source: str, url: str, error: str,
            retry_delay_min: int = DEFAULT_RETRY_DELAY_MIN,
            extra_context: Optional[dict] = None) -> str:
    """把一個失敗物件放進重試佇列。
    若已存在同 source_id 的 entry → 更新 attempts 並重新排程。
    回傳 doc_id。"""
    from database.db import get_firestore
    from google.cloud.firestore_v1.base_query import FieldFilter
    db = get_firestore()
    col = db.collection(QUEUE_COLLECTION)
    now = _tw_now()
    retry_at = now + timedelta(minutes=retry_delay_min)

    # 找既有 entry
    existing = list(col.where(filter=FieldFilter("source_id", "==", source_id)).limit(1).stream())
    if existing:
        ref = existing[0].reference
        cur = existing[0].to_dict() or {}
        attempts = int(cur.get("attempts") or 1) + 1
        if attempts > MAX_ATTEMPTS:
            # 超過上限 → 標 abandoned 不再重試
            ref.update({
                "status": "abandoned",
                "attempts": attempts,
                "last_error": error[:500],
                "last_failed_at": _iso(now),
            })
            logger.warning(f"[retry-queue] {source_id} 達 {MAX_ATTEMPTS} 次嘗試，放棄")
            return existing[0].id
        ref.update({
            "attempts": attempts,
            "last_error": error[:500],
            "last_failed_at": _iso(now),
            "retry_at": _iso(retry_at),
            "status": "pending",
        })
        return existing[0].id

    # 新 entry：attempts 從 2 開始（caller 在 enqueue 之前已試過 1 次失敗）
    # 這樣 MAX_ATTEMPTS=4 → 進 queue 後最多再 retry 2 次（attempts=3, 4），第 3 次會抵達 5 abandon
    # 避免「batch 1 次 + queue 4 次 = 5 次」這種太多次的情況
    data = {
        "source_id": source_id,
        "source": source,
        "url": url,
        "first_failed_at": _iso(now),
        "last_failed_at": _iso(now),
        "retry_at": _iso(retry_at),
        "attempts": 2,
        "status": "pending",
        "last_error": error[:500],
    }
    if extra_context:
        data["context"] = extra_context
    ref = col.document()
    ref.set(data)
    logger.info(f"[retry-queue] enqueue {source_id} (attempt 1) retry_at={_iso(retry_at)}")
    return ref.id


def dequeue(doc_id: str):
    """重抓成功 → 從 queue 移除。"""
    from database.db import get_firestore
    get_firestore().collection(QUEUE_COLLECTION).document(doc_id).delete()


def list_pending(limit: int = 200) -> list:
    """列出所有 pending 的 entry（給 admin UI / 排程 loop 用）。"""
    from database.db import get_firestore
    from google.cloud.firestore_v1.base_query import FieldFilter
    docs = list(get_firestore().collection(QUEUE_COLLECTION)
                .where(filter=FieldFilter("status", "==", "pending"))
                .limit(limit).stream())
    out = []
    for d in docs:
        data = d.to_dict() or {}
        data["_id"] = d.id
        out.append(data)
    return out


def list_due(now=None, limit: int = 50) -> list:
    """列出 retry_at <= now 的 pending entry — 給排程 loop 拿來重抓。"""
    if now is None:
        now = _tw_now()
    pending = list_pending(limit=200)
    due = [e for e in pending if e.get("retry_at") and e["retry_at"] <= _iso(now)]
    return due[:limit]


def list_all_for_admin(limit: int = 100) -> list:
    """admin UI 用：列出所有 entry（含 abandoned / pending），按 first_failed_at 倒序。"""
    from database.db import get_firestore
    docs = list(get_firestore().collection(QUEUE_COLLECTION).limit(limit).stream())
    out = []
    for d in docs:
        data = d.to_dict() or {}
        data["_id"] = d.id
        out.append(data)
    out.sort(key=lambda x: x.get("first_failed_at") or "", reverse=True)
    return out
