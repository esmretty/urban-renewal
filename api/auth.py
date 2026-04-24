"""
Firebase Auth 驗證 + FastAPI dependency。

用法：
    from api.auth import get_current_user, require_admin

    @app.get("/api/me")
    async def me(user = Depends(get_current_user)):
        return user

    @app.delete("/admin/properties/{id}")
    async def admin_delete(id: str, admin = Depends(require_admin)):
        ...
"""
import os
import logging
from typing import Optional

from fastapi import Depends, HTTPException, Request
from firebase_admin import auth as fb_auth

from database.db import init_db as _ensure_fb_initialized

logger = logging.getLogger(__name__)


# ── 用戶階級系統 ──────────────────────────────────────────────────────────────
TIER_OWNER = 1
TIER_SYSTEM_ADMIN = 2
TIER_ADMIN = 3
TIER_L1 = 4
TIER_L2 = 5
TIER_L3 = 6
TIER_PREMIUM = 7

TIER_NAMES_ZH = {
    TIER_OWNER: "所有者",
    TIER_SYSTEM_ADMIN: "系統管理者",
    TIER_ADMIN: "管理者",
    TIER_L1: "普通會員",
    TIER_L2: "白金會員",
    TIER_L3: "鑽石會員",
    TIER_PREMIUM: "黑卡會員",
}
TIER_NAMES_EN = {
    TIER_OWNER: "Owner",
    TIER_SYSTEM_ADMIN: "System Admin",
    TIER_ADMIN: "Admin",
    TIER_L1: "Level 1 Member",
    TIER_L2: "Level 2 Member",
    TIER_L3: "Level 3 Member",
    TIER_PREMIUM: "Premium Member",
}

# 可登入 admin 後台的階級（owner + system admin）
ADMIN_PORTAL_TIERS = {TIER_OWNER, TIER_SYSTEM_ADMIN}

# 特殊指定：email → tier
# 從環境變數 OWNER_EMAILS / SYSTEM_ADMIN_EMAILS 讀取（逗號分隔，大小寫不敏感）。
# 真實 email 放 .env（不進 git），.env.example 只放 placeholder。
def _parse_email_tier_env() -> dict:
    mapping = {}
    for raw in os.getenv("OWNER_EMAILS", "").split(","):
        e = raw.strip().lower()
        if e:
            mapping[e] = TIER_OWNER
    for raw in os.getenv("SYSTEM_ADMIN_EMAILS", "").split(","):
        e = raw.strip().lower()
        if e:
            mapping[e] = TIER_SYSTEM_ADMIN
    return mapping

EMAIL_TO_TIER = _parse_email_tier_env()

DEFAULT_TIER = TIER_L1   # 未指定的 email 預設為「普通會員」


def resolve_tier(email: str) -> int:
    """由 email 推算階級（未指定 → 預設普通會員）。"""
    if not email:
        return DEFAULT_TIER
    return EMAIL_TO_TIER.get(email.lower(), DEFAULT_TIER)


def tier_name(tier: int, lang: str = "zh") -> str:
    table = TIER_NAMES_ZH if lang == "zh" else TIER_NAMES_EN
    return table.get(tier, "")


def _extract_token(request: Request) -> Optional[str]:
    """從 Authorization header 抽出 Bearer token。"""
    h = request.headers.get("Authorization") or request.headers.get("authorization") or ""
    if not h.startswith("Bearer "):
        return None
    return h[7:].strip() or None


async def get_current_user(request: Request) -> dict:
    """
    FastAPI dependency：驗證 Firebase ID token，回 user 資料 dict。
    沒 token 或驗不過 → 401。
    """
    _ensure_fb_initialized()   # 保險：firebase_admin.initialize_app 一定已跑過
    token = _extract_token(request)
    if not token:
        raise HTTPException(status_code=401, detail="缺少登入憑證")
    try:
        decoded = fb_auth.verify_id_token(token, check_revoked=False, clock_skew_seconds=10)
    except Exception as e:
        logger.warning("Firebase token 驗證失敗: %s", e)
        raise HTTPException(status_code=401, detail="登入憑證無效")
    email = (decoded.get("email") or "").lower()
    tier = resolve_tier(email)
    return {
        "uid": decoded.get("uid") or decoded.get("user_id"),
        "email": email,
        "display_name": decoded.get("name"),
        "picture": decoded.get("picture"),
        "tier": tier,
        "tier_name_zh": tier_name(tier, "zh"),
        "tier_name_en": tier_name(tier, "en"),
        "is_admin": tier in ADMIN_PORTAL_TIERS,
    }


async def require_admin(user: dict = Depends(get_current_user)) -> dict:
    """只允許 owner / system admin 階級登入 admin 後台。"""
    if user.get("tier") not in ADMIN_PORTAL_TIERS:
        raise HTTPException(status_code=403, detail="無管理者權限")
    return user
