"""
Firebase Firestore 資料庫連線管理。
"""
import os

import logging
from typing import Optional, Tuple
import firebase_admin
from firebase_admin import credentials, firestore as fs

from config import BASE_DIR

logger = logging.getLogger(__name__)

_initialized = False
_client = None


def _cred_path():
    """找到 Firebase 憑證檔 firebase-credentials.json。"""
    p = BASE_DIR / "firebase-credentials.json"
    if p.exists():
        return p
    raise FileNotFoundError(
        f"找不到 firebase-credentials.json，請把 Firebase service account JSON 放在 {BASE_DIR}"
    )


def init_db():
    """初始化 Firebase Admin SDK。"""
    global _initialized, _client
    if _initialized:
        return
    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate(str(_cred_path()))
        firebase_admin.initialize_app(cred)
        logger.info("Firebase initialized")
    _client = fs.client()
    _initialized = True


def get_firestore():
    """取得 Firestore client。"""
    if not _initialized:
        init_db()
    return _client


def get_col():
    """取得中央 properties collection（全世界共用的 591 分析快取）。"""
    return get_firestore().collection("properties")


def gen_dated_id(when_iso: Optional[str] = None) -> str:
    """生成物件 doc_id：格式 YYYYMMDD-XXXXXX（8 碼日期 + 6 碼隨機 hex）。
    when_iso 帶入 scraped_at 之類的 ISO timestamp 字串 → 用該日期。
    為 None → 用今天日期。"""
    import uuid as _uuid
    from datetime import datetime, timezone, timedelta
    tw = timezone(timedelta(hours=8))
    dt = None
    if when_iso:
        try:
            dt = datetime.fromisoformat(when_iso.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tw)
            dt = dt.astimezone(tw)
        except Exception:
            dt = None
    if dt is None:
        dt = datetime.now(tw)
    return f"{dt.strftime('%Y%m%d')}-{_uuid.uuid4().hex[:6]}"


def find_doc_by_source_id(source_id: str) -> tuple:
    """用 source_id 欄位查 properties，回傳 (doc_id, dict) 或 (None, None)。
    Migration 後 doc_id 不再是 source_id 字串，改成 UUID 格式 → 一律用 query 找。"""
    from google.cloud.firestore_v1.base_query import FieldFilter
    if not source_id:
        return (None, None)
    docs = list(get_col().where(filter=FieldFilter("source_id", "==", source_id)).limit(1).stream())
    if not docs:
        return (None, None)
    return (docs[0].id, docs[0].to_dict())


def get_user_doc(uid: str):
    """取得 users/{uid} doc 參考。"""
    return get_firestore().collection("users").document(uid)


def get_user_watchlist(uid: str):
    """users/{uid}/watchlist collection — 我抓過的 591 房源引用（含 overrides）。"""
    return get_user_doc(uid).collection("watchlist")


def get_user_manual(uid: str):
    """users/{uid}/manual collection — 我手動輸入的私人物件（完整 doc，不進中央）。"""
    return get_user_doc(uid).collection("manual")


def get_user_bookmarks(uid: str):
    """users/{uid}/bookmarks collection — 我從探索 tab 標書籤的中央 591 物件（含 overrides）。"""
    return get_user_doc(uid).collection("bookmarks")
