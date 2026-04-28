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


def find_cross_source_duplicate(item: dict):
    """跨來源 dup 偵測：給 scraper 在 enrich 完成後用。
    用 (district, road_seg, building_area_ping ±0.01, price_ntd 完全 match)
    在 properties collection 找既有 doc。回傳 doc_id 或 None。
    跟 api/app.py 的 find_duplicate 同邏輯，但獨立函式可被 scraper import。"""
    try:
        addr = item.get("address") or ""
        district = item.get("district") or ""
        bld = item.get("building_area_ping")
        price = item.get("price_ntd")
        if not (addr and district and bld and price):
            return None
        import re as _re
        a = _re.sub(r"^(台北市|臺北市|新北市|桃園市|台中市|臺中市|高雄市|台南市|臺南市|基隆市|新竹市)", "", addr)
        a = _re.sub(r"^[一-龥]{1,3}區", "", a)
        m = _re.search(r"([一-龥]{1,5}(?:路|街|大道)(?:[一二三四五六七八九十]段)?(?:\d+巷)?)", a)
        if not m:
            return None
        road = m.group(1)
        from google.cloud.firestore_v1.base_query import FieldFilter
        col = get_col()
        cand = list(col
                    .where(filter=FieldFilter("district", "==", district))
                    .where(filter=FieldFilter("price_ntd", "==", int(price)))
                    .stream())
        for d in cand:
            dd = d.to_dict() or {}
            if dd.get("source_id") == item.get("source_id"):
                continue   # 自己 — 不算 cross
            da = dd.get("address") or ""
            if road in da and abs((dd.get("building_area_ping") or 0) - bld) < 0.01:
                return d.id
        return None
    except Exception:
        return None


def find_doc_by_source_id(source_id: str) -> tuple:
    """用 source_id 找 properties，回傳 (doc_id, dict) 或 (None, None)。
    Migration 後 doc_id 不再是 source_id 字串，改成 UUID 格式。
    一個 UUID doc 可能掛多個 source_id（dup merge / 跨來源），所以查兩個地方：
      1. 主來源 source_id 欄位（== query）
      2. source_ids 平面陣列欄位（array_contains query）— 含所有 merged 進來的 sid
    這樣同一 591 sid 不會反覆被當成「新物件」每次又 dup_merge 一次。"""
    from google.cloud.firestore_v1.base_query import FieldFilter
    if not source_id:
        return (None, None)
    col = get_col()
    # 1. 直接用 source_id 欄位
    docs = list(col.where(filter=FieldFilter("source_id", "==", source_id)).limit(1).stream())
    if docs:
        return (docs[0].id, docs[0].to_dict())
    # 2. 查 source_ids 平面陣列（dup merge 時會 append 進來）
    docs = list(col.where(filter=FieldFilter("source_ids", "array_contains", source_id)).limit(1).stream())
    if docs:
        return (docs[0].id, docs[0].to_dict())
    return (None, None)


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
