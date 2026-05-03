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


# Token verify cache — Firebase token verify ~100-200ms 每次太貴 (RSA 簽章驗證 + JWKS lookup)。
# Firebase ID token 預設 1 hr 有效；我們 cache token → user_dict 直到 token exp 為止 (or 5 min, 取小)。
# Cache 命中時跳過 verify_id_token，省 ~100-150ms 每個 API call。
# 安全 trade-off：若用戶在別處被強制登出/revoke，最久 5 分鐘才會失效。
# 對 dev tool 場景可接受；未來要更嚴可加 ETag-style invalidation。
_TOKEN_CACHE: dict = {}             # token_str -> (cache_until_ts, user_dict)
_TOKEN_CACHE_MAX = 256
_TOKEN_CACHE_TTL = 5 * 60           # 5 min
_TOKEN_CACHE_HIT = 0
_TOKEN_CACHE_MISS = 0


def _get_token_cache_stats() -> dict:
    return {"hit": _TOKEN_CACHE_HIT, "miss": _TOKEN_CACHE_MISS,
            "size": len(_TOKEN_CACHE)}


async def get_current_user(request: Request) -> dict:
    """
    FastAPI dependency：驗證 Firebase ID token，回 user 資料 dict。
    沒 token 或驗不過 → 401。
    Cache 命中時跳過 verify_id_token (省 ~100-150ms)。
    """
    global _TOKEN_CACHE_HIT, _TOKEN_CACHE_MISS
    _ensure_fb_initialized()   # 保險：firebase_admin.initialize_app 一定已跑過
    token = _extract_token(request)
    if not token:
        raise HTTPException(status_code=401, detail="缺少登入憑證")

    import time as _t
    now = _t.time()

    # Cache hit：直接回 user_dict，跳過 RSA 驗證
    cached = _TOKEN_CACHE.get(token)
    if cached and cached[0] > now:
        _TOKEN_CACHE_HIT += 1
        return cached[1]

    # Cache miss：跑完整 verify
    _TOKEN_CACHE_MISS += 1
    try:
        decoded = fb_auth.verify_id_token(token, check_revoked=False, clock_skew_seconds=10)
    except Exception as e:
        logger.warning("Firebase token 驗證失敗: %s", e)
        raise HTTPException(status_code=401, detail="登入憑證無效")
    email = (decoded.get("email") or "").lower()
    tier = resolve_tier(email)
    user_dict = {
        "uid": decoded.get("uid") or decoded.get("user_id"),
        "email": email,
        "display_name": decoded.get("name"),
        "picture": decoded.get("picture"),
        "tier": tier,
        "tier_name_zh": tier_name(tier, "zh"),
        "tier_name_en": tier_name(tier, "en"),
        "is_admin": tier in ADMIN_PORTAL_TIERS,
    }

    # Cache 寫回：min(token_exp, now + 5min)；exp 來自 token 本身的 'exp' claim (epoch sec)
    token_exp = decoded.get("exp") or (now + _TOKEN_CACHE_TTL)
    cache_until = min(float(token_exp), now + _TOKEN_CACHE_TTL)
    if len(_TOKEN_CACHE) >= _TOKEN_CACHE_MAX:
        # 簡單 evict：清掉所有過期的；若還滿，再清最舊一個
        for k in [k for k, v in _TOKEN_CACHE.items() if v[0] <= now]:
            _TOKEN_CACHE.pop(k, None)
        if len(_TOKEN_CACHE) >= _TOKEN_CACHE_MAX:
            oldest = min(_TOKEN_CACHE, key=lambda k: _TOKEN_CACHE[k][0])
            _TOKEN_CACHE.pop(oldest, None)
    _TOKEN_CACHE[token] = (cache_until, user_dict)
    return user_dict


async def require_admin(user: dict = Depends(get_current_user)) -> dict:
    """只允許 owner / system admin 階級登入 admin 後台。"""
    if user.get("tier") not in ADMIN_PORTAL_TIERS:
        raise HTTPException(status_code=403, detail="無管理者權限")
    return user
