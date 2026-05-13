"""
Admin 物件檢視 endpoints (簡單 CRUD) — Sprint #2 step 5a 從 api/app.py 拆出。

5 個 endpoint：
  - GET    /admin/properties                 列出中央 DB docs (filter by source_origin)
  - GET    /admin/manual_properties          列全部用戶 manual docs
  - GET    /admin/properties/{property_id}   讀單筆中央 doc
  - DELETE /admin/properties/{property_id}   永久刪中央 doc
  - GET    /admin/manual/{uid}/{property_id} 讀指定用戶 manual doc

Late imports (from api.app)：
  - invalidate_query_cache (delete 後清 cache)
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.auth import require_admin
from database.db import get_col, get_firestore, get_user_manual

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/admin/properties")
async def admin_properties(
    source: Optional[str] = Query(None),
    admin: dict = Depends(require_admin),
):
    """列出中央 DB docs。
    source=batch    → 排除 source_origin == 'user_url' （= admin batch 抓進來的）
    source=user_url → 只回 source_origin == 'user_url' （用戶貼 URL 送出，附 submitted_by_email）
    省略 source     → 全部
    """
    col = get_col()
    docs = list(col.get())
    items = []
    for d in docs:
        data = d.to_dict() or {}
        data["id"] = d.id
        if source == "batch" and data.get("source_origin") == "user_url":
            continue
        if source == "user_url" and data.get("source_origin") != "user_url":
            continue
        items.append(data)
    # 對 user_url 物件，補上送件人 email（從 doc 上的 submitted_by_uid → users 反查）
    if source == "user_url" and items:
        fs = get_firestore()
        uid_email_cache = {}
        def _email_of(uid):
            if not uid:
                return None
            if uid in uid_email_cache:
                return uid_email_cache[uid]
            try:
                u = fs.collection("users").document(uid).get()
                em = (u.to_dict() or {}).get("email") if u.exists else None
            except Exception:
                em = None
            uid_email_cache[uid] = em
            return em
        for it in items:
            it["submitted_by_email"] = _email_of(it.get("submitted_by_uid"))
    items.sort(key=lambda x: (x.get("list_rank") if x.get("list_rank") is not None else 9999))
    items.sort(key=lambda x: x.get("scrape_session_at") or x.get("scraped_at") or "", reverse=True)
    return {"total": len(items), "items": items}


@router.get("/admin/manual_properties")
async def admin_manual_properties(admin: dict = Depends(require_admin)):
    """列出全部用戶的 manual 物件（users/{uid}/manual/{src_id}）。
    每筆附 submitted_by_email + submitted_by_uid。"""
    fs = get_firestore()
    items = []
    for u in fs.collection("users").get():
        uid = u.id
        udata = u.to_dict() or {}
        email = udata.get("email")
        for m in fs.collection("users").document(uid).collection("manual").get():
            d = m.to_dict() or {}
            d["id"] = m.id
            d["submitted_by_uid"] = uid
            d["submitted_by_email"] = email
            items.append(d)
    items.sort(key=lambda x: x.get("scraped_at") or "", reverse=True)
    return {"total": len(items), "items": items}


@router.get("/admin/properties/{property_id:path}")
async def admin_get_property(property_id: str, admin: dict = Depends(require_admin)):
    doc = get_col().document(property_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="物件不存在")
    data = doc.to_dict()
    data["id"] = doc.id
    return data


@router.delete("/admin/properties/{property_id:path}")
async def admin_delete_property(property_id: str, admin: dict = Depends(require_admin)):
    """從中央 DB 真刪除（非軟刪）。"""
    from api.app import invalidate_query_cache
    ref = get_col().document(property_id)
    if not ref.get().exists:
        raise HTTPException(status_code=404, detail="物件不存在")
    ref.delete()
    invalidate_query_cache()
    logger.warning("[admin] %s 永久刪除 %s", admin.get("email"), property_id)
    return {"status": "ok", "deleted": property_id}


@router.get("/admin/manual/{uid}/{property_id:path}")
async def admin_get_manual(uid: str, property_id: str, admin: dict = Depends(require_admin)):
    """admin 讀取指定用戶的 manual doc（給 reanalyze 輪詢用）。"""
    doc = get_user_manual(uid).document(property_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="物件不存在")
    data = doc.to_dict() or {}
    data["id"] = doc.id
    data["submitted_by_uid"] = uid
    return data
