"""
Firebase Firestore 資料庫連線管理。
"""

import logging
from typing import Optional
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
    """跨來源 dup 偵測：給 scraper enrich 完成後用。
    用 Firestore query 拉同 district + 同 price 的候選，再呼叫 database.dedup.is_same_property
    做最終比對。回傳 doc_id 或 None。
    規則 single source of truth: database/dedup.py。"""
    try:
        from database.dedup import is_same_property
        district = item.get("district") or ""
        price = item.get("price_ntd")
        if not (district and price):
            return None
        from google.cloud.firestore_v1.base_query import FieldFilter
        from database.models import make_source_key
        item_key = make_source_key(item.get("source") or "591", item.get("source_id") or "")
        col = get_col()
        cand = list(col
                    .where(filter=FieldFilter("district", "==", district))
                    .where(filter=FieldFilter("price_ntd", "==", int(price)))
                    .stream())
        for d in cand:
            dd = d.to_dict() or {}
            # 跳過自己（已含此 source_key 的 doc）
            if item_key in (dd.get("source_keys") or []):
                continue
            if is_same_property(item, dd):
                return d.id
        return None
    except Exception:
        return None


def recheck_and_archive_if_cross_dup(new_doc_id: str, trigger_label: str = "post_write_recheck",
                                     dry_run: bool = False) -> Optional[str]:
    """寫完新 doc 後 call：用最新 DB 狀態再跑一次 cross-source 確認，命中既有 doc 就 retro merge。

    解 dedup audit 抓到的 25 對 silent 漏 merge — root cause 是 batch find_duplicate
    依賴 batch_start 那刻 snapshot 的 _existing_items in-memory cache，同 batch 內後續寫的
    doc 沒進 snapshot、或 transient race 都會漏。post-write recheck 用 Firestore 即時 query
    最終把關。

    行為：
      - 拉 new_doc 重跑 find_cross_source_duplicate 邏輯（已含 self-skip via source_keys）
      - 命中既有 existing → existing 補上 new doc 的 sources + last_change_at + latest_event
      - new doc 標 `archived=True` + `merged_into=existing_id`（**不真刪**：保護 user
        watchlist / reads 等 reference 完整性；前端列表 archived 不顯示，等於 merge）
      - 回傳：合併到的 existing_doc_id，或 None（沒找到）

    dry_run=True：只回 existing_id 不寫 DB（給 audit / 測試用）"""
    try:
        col = get_col()
        new_doc = col.document(new_doc_id).get().to_dict() or {}
        if not new_doc:
            return None
        if new_doc.get("archived"):
            return None   # 已 archived 不再處理
        from database.dedup import is_same_property
        from google.cloud.firestore_v1.base_query import FieldFilter
        district = new_doc.get("district") or ""
        price = new_doc.get("price_ntd")
        if not (district and price):
            return None
        cand = list(col
                    .where(filter=FieldFilter("district", "==", district))
                    .where(filter=FieldFilter("price_ntd", "==", int(price)))
                    .stream())
        new_source_keys = set(new_doc.get("source_keys") or [])
        for d in cand:
            if d.id == new_doc_id:
                continue
            dd = d.to_dict() or {}
            if dd.get("archived"):
                continue   # 已 archived 的 existing 不算 dup target
            # 避免 self-merge：如果 candidate doc 跟 new_doc 有任何共同 source_key (例如歷史殘留)
            # 跳過 — 該情況用其他 path 處理 (Sprint 3.3 dedup 統一以後不會發生)
            cand_keys = set(dd.get("source_keys") or [])
            if new_source_keys & cand_keys:
                continue
            if is_same_property(new_doc, dd):
                existing_id = d.id
                if dry_run:
                    return existing_id
                # 把 new doc 的 sources 併入 existing
                from database.models import add_source_to_doc, compute_source_keys
                from database.time_utils import now_tw_iso
                existing_sources = list(dd.get("sources") or [])
                new_sources = new_doc.get("sources") or []
                changed = False
                first_new_name = None
                first_new_sid = None
                for s in new_sources:
                    name = s.get("name") or ""
                    sid = s.get("source_id") or ""
                    url = s.get("url") or ""
                    if not (name and sid and url):
                        continue
                    if first_new_name is None:
                        first_new_name = name
                        first_new_sid = sid
                    tmp = {"sources": existing_sources}
                    if add_source_to_doc(tmp, name, sid, url, s.get("added_at")):
                        existing_sources = tmp["sources"]
                        changed = True
                updates = {}
                if changed:
                    updates["sources"] = existing_sources
                    updates["source_keys"] = compute_source_keys(existing_sources)
                updates["last_change_at"] = now_tw_iso()
                updates["latest_event"] = {
                    "type": "cross_source",
                    "source": first_new_name or "?",
                    "at": now_tw_iso(),
                }
                # 永慶/信義 raw zoning 為主 — 用戶要求若 new doc 是永慶/信義 且 zoning 來源
                # 是 detail_raw (仲介後台寫的具體第X種分區)，蓋掉既有 GeoServer 結果
                new_zoning = new_doc.get("zoning")
                new_zoning_src = (new_doc.get("zoning_source") or "")
                if new_zoning and new_zoning_src.endswith("_detail_raw") and new_zoning != dd.get("zoning"):
                    updates["zoning"] = new_zoning
                    updates["zoning_original"] = new_doc.get("zoning_original") or new_zoning
                    updates["zoning_source"] = new_zoning_src
                    updates["zoning_source_url"] = new_doc.get("zoning_source_url")
                    # 清掉 stale multi-zone 狀態 (避免既有 zoning_list 跟新 raw zoning 衝突)
                    updates["zoning_list"] = None
                    updates["zoning_ratios"] = None
                col.document(existing_id).update(updates)
                col.document(new_doc_id).update({
                    "archived": True,
                    "archived_reason": f"post_write_recheck dup of {existing_id}",
                    "archived_at": now_tw_iso(),
                    "merged_into": existing_id,
                })
                try:
                    from database.run_log import log_action
                    log_action(trigger_label, "cross_source",
                               source_id=(f"{first_new_name}_{first_new_sid}" if first_new_sid else None),
                               doc_id=existing_id,
                               message=f"post-write recheck 命中 doc {existing_id[:8]}，{new_doc_id[:8]} archived 併入",
                               details={"merged_from": new_doc_id, "merged_into": existing_id,
                                        "added_sources_count": sum(1 for s in new_sources if s.get("url"))})
                except Exception:
                    pass
                return existing_id
        return None
    except Exception as e:
        logger.exception(f"recheck_and_archive_if_cross_dup({new_doc_id}) failed: {e}")
        return None


def find_doc_by_source_key(source_name: str, site_id: str) -> tuple:
    """用 (source_name, site_id) 找 properties doc，回傳 (doc_id, dict) 或 (None, None)。
    Schema：每 doc 有 source_keys[] 平面索引（如 ["591:20114614", "yongqing:8893"]）。
    Firestore array_contains 查 source_keys 命中即回。
    一個 doc 可掛多個 source（591 重發 / 跨來源 dedup）→ 只要任一 key match 都會找到同 doc。
    """
    from google.cloud.firestore_v1.base_query import FieldFilter
    from database.models import make_source_key
    key = make_source_key(source_name, site_id)
    if not key or key.endswith(":"):
        return (None, None)
    col = get_col()
    docs = list(col.where(filter=FieldFilter("source_keys", "array_contains", key)).limit(1).stream())
    if docs:
        return (docs[0].id, docs[0].to_dict())
    return (None, None)


def find_doc_by_source_id(source_id: str) -> tuple:
    """[Backwards-compat shim] 拆 source_id ("591_20114614") 後 delegate 到 find_doc_by_source_key。
    新 code 請直接用 find_doc_by_source_key(name, site_id)。"""
    if not source_id:
        return (None, None)
    # source_id 通常是 "591_20114614" 或 "yongqing_8893" 形式
    parts = source_id.split("_", 1)
    if len(parts) == 2:
        return find_doc_by_source_key(parts[0], parts[1])
    return find_doc_by_source_key("591", source_id)


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
