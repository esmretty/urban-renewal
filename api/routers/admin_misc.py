"""
Admin 雜項 endpoints — Sprint #2 step 3 從 api/app.py 拆出。

7 個 endpoint：
  Users:
    - GET    /admin/users          列出 users collection
    - DELETE /admin/users/{uid}    刪除指定用戶所有私人資料
  Email 白名單：
    - GET    /admin/email_whitelist
    - POST   /admin/email_whitelist/add
    - POST   /admin/email_whitelist/remove
  維護模式：
    - GET    /admin/maintenance
    - POST   /admin/maintenance

Late imports (from api.app)：
  - _get_email_whitelist (app.py 核心 auth flow 也用，留在 app.py 為主要源)
  - _get_maintenance_state / _maintenance_file_path (public.py 也用)
"""
import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.auth import require_admin
from database.db import (
    get_user_watchlist, get_user_manual, get_user_doc, get_firestore,
)
from database.time_utils import now_tw_iso

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Users ────────────────────────────────────────────────────────────────
@router.delete("/admin/users/{uid}")
async def admin_delete_user(uid: str, admin: dict = Depends(require_admin)):
    """刪除指定用戶所有私人資料（watchlist + manual + profile）。不動中央。"""
    counts = {}
    for name, col in [
        ("watchlist", get_user_watchlist(uid)),
        ("manual", get_user_manual(uid)),
    ]:
        c = 0
        for d in col.get():
            d.reference.delete()
            c += 1
        counts[name] = c
    # 刪 profile doc（若存在）
    profile_ref = get_user_doc(uid)
    profile_existed = profile_ref.get().exists
    if profile_existed:
        profile_ref.delete()
        counts["profile"] = 1
    else:
        counts["profile"] = 0

    total_deleted = sum(counts.values())
    if total_deleted == 0:
        return {"status": "empty", "uid": uid, "message": "此用戶無資料可刪。"}
    logger.warning("[admin] %s 刪除 uid=%s 所有資料: %s", admin.get("email"), uid, counts)
    return {"status": "ok", "uid": uid, "deleted": counts}


@router.get("/admin/users")
async def admin_users(admin: dict = Depends(require_admin)):
    """列出 users collection。"""
    from api.auth import resolve_tier, tier_name
    out = []
    try:
        users_col = get_firestore().collection("users")
        for u in users_col.get():
            data = u.to_dict() or {}
            email = data.get("email") or ""
            # tier 若 doc 沒存（舊資料）→ 用 email 推算
            tier = data.get("tier") if data.get("tier") is not None else resolve_tier(email)
            out.append({
                "uid": u.id,
                "email": email,
                "display_name": data.get("display_name"),
                "created_at": data.get("created_at"),
                "tier": tier,
                "tier_name_zh": tier_name(tier, "zh"),
                "tier_name_en": tier_name(tier, "en"),
            })
    except Exception as e:
        logger.warning("admin_users failed: %s", e)
    return {"items": out}


# ── Email 白名單（admin 管理） ──────────────────────────────────────────────
class WhitelistReq(BaseModel):
    email: str


@router.get("/admin/email_whitelist")
async def get_email_whitelist(admin: dict = Depends(require_admin)):
    """列出 email 白名單（新用戶首次登入時會被檢查）。"""
    from api.app import _get_email_whitelist
    emails = sorted(_get_email_whitelist())
    return {"emails": emails}


@router.post("/admin/email_whitelist/add")
async def add_email_whitelist(body: WhitelistReq, admin: dict = Depends(require_admin)):
    from api.app import _get_email_whitelist, invalidate_email_whitelist_cache
    email = (body.email or "").strip().lower()
    if not email or "@" not in email:
        raise HTTPException(400, "請提供有效 email")
    current = _get_email_whitelist()
    current.add(email)
    get_firestore().collection("settings").document("email_whitelist").set(
        {"emails": sorted(current), "updated_at": now_tw_iso(),
         "updated_by_email": admin.get("email") or ""},
        merge=True,
    )
    invalidate_email_whitelist_cache()
    logger.warning("[whitelist] %s 新增 %s", admin.get("email"), email)
    return {"status": "ok", "emails": sorted(current)}


@router.post("/admin/email_whitelist/remove")
async def remove_email_whitelist(body: WhitelistReq, admin: dict = Depends(require_admin)):
    from api.app import _get_email_whitelist, invalidate_email_whitelist_cache
    email = (body.email or "").strip().lower()
    current = _get_email_whitelist()
    current.discard(email)
    get_firestore().collection("settings").document("email_whitelist").set(
        {"emails": sorted(current), "updated_at": now_tw_iso(),
         "updated_by_email": admin.get("email") or ""},
        merge=True,
    )
    invalidate_email_whitelist_cache()
    logger.warning("[whitelist] %s 移除 %s", admin.get("email"), email)
    return {"status": "ok", "emails": sorted(current)}


# ── 網站維護模式（admin 切換） ──────────────────────────────────────────────
class MaintenanceReq(BaseModel):
    enabled: bool
    message: Optional[str] = None


@router.get("/admin/maintenance")
async def get_maintenance(admin: dict = Depends(require_admin)):
    """回傳目前維護模式狀態 + 訊息（本 server 的）。"""
    from api.app import _get_maintenance_state
    return _get_maintenance_state()


@router.post("/admin/maintenance")
async def set_maintenance(body: MaintenanceReq, admin: dict = Depends(require_admin)):
    """切換維護模式 + 自訂訊息。寫入本地檔案，不影響其他 server。"""
    from api.app import _maintenance_file_path
    state = {
        "enabled": bool(body.enabled),
        "message": (body.message or "").strip(),
        "updated_at": now_tw_iso(),
        "updated_by_email": admin.get("email") or "",
    }
    path = _maintenance_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    logger.warning("[maintenance] %s 設定 enabled=%s message=%r (本機檔案)",
                   admin.get("email"), state["enabled"], state["message"])
    return {"status": "ok", **state}
