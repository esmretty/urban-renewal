"""
公開 / 半公開 endpoints — Sprint #2 從 api/app.py 拆出。

10 個 endpoint：
  - GET /                        主前台 v2（cache HTML + 動態 SHA query）
  - GET /v2                      / 的 alias
  - GET /school_lookup.html      學區查詢測試頁 (static)
  - GET /login.html              登入頁
  - GET /maintenance.html        維護頁
  - GET /admin.html              admin 後台
  - GET /api/firebase_config     前端 Firebase config
  - GET /api/me                  登入用戶資訊（含維護狀態）
  - GET /api/maintenance_status  公開維護狀態 query
  - GET /api/version             目前 prod git SHA (60 秒 cache)

Dependencies：
  - api.auth.get_current_user
  - config.BASE_DIR
  - api.app._ensure_user_profile / _get_maintenance_state (late import 避免 circular)
"""
import time
import subprocess
import os
from pathlib import Path

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse, Response

from config import BASE_DIR
from api.auth import get_current_user

router = APIRouter()

FRONTEND_DIR = BASE_DIR / "frontend"
SERVER_DIR = BASE_DIR / "server"


# ── index2.html 動態版號替換 + cache（搬自 api/app.py） ──────────────────
_INDEX2_HTML_CACHE = {"sha": None, "html": None, "mtime": 0.0}


def _read_version_sha() -> str:
    """讀 VERSION 檔 (deploy.sh 寫的 git short SHA)，fallback git rev-parse。"""
    try:
        v = (BASE_DIR / "VERSION").read_text(encoding="utf-8").strip()
        if v:
            return v
    except Exception:
        pass
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=str(BASE_DIR), timeout=2
        ).decode().strip()
    except Exception:
        return "unknown"


def _serve_index2_with_version() -> Response:
    """讀 index2.html 把 ?v=1 替換成當前 git SHA，避免 iOS PWA WKWebView 永遠 cache 舊版。
    HTML 本身回 no-cache 確保拿到新 SHA URL；CSS/JS 因為 URL 不同 → 強制重抓。"""
    html_path = FRONTEND_DIR / "index2.html"
    try:
        mt = html_path.stat().st_mtime
    except OSError:
        mt = 0.0
    sha = _read_version_sha()
    if (_INDEX2_HTML_CACHE["sha"] != sha
            or _INDEX2_HTML_CACHE["mtime"] != mt
            or _INDEX2_HTML_CACHE["html"] is None):
        try:
            raw = html_path.read_text(encoding="utf-8")
        except Exception:
            return FileResponse(str(html_path))
        _INDEX2_HTML_CACHE["html"] = raw.replace("?v=1", f"?v={sha}")
        _INDEX2_HTML_CACHE["sha"] = sha
        _INDEX2_HTML_CACHE["mtime"] = mt
    return Response(
        content=_INDEX2_HTML_CACHE["html"],
        media_type="text/html",
        headers={"Cache-Control": "no-cache, must-revalidate"},
    )


# ── /api/version 自己的 cache（不同於 _INDEX2_HTML_CACHE）─────────────
_VERSION_CACHE = {"sha": None, "ts": 0.0}


# ── HTML 入口 ────────────────────────────────────────────────────────────
@router.get("/")
async def root():
    """主前台 (v2)。"""
    return _serve_index2_with_version()


@router.get("/v2")
async def root_v2():
    """v2 別名 — 對外仍可用 /v2 訪問新版 (跟 / 同檔)。"""
    return _serve_index2_with_version()


@router.get("/school_lookup.html")
async def school_lookup_page():
    """學區查詢測試頁（公開、無需登入）— 用來驗證 lookup 正確性。"""
    return FileResponse(str(FRONTEND_DIR / "school_lookup.html"))


@router.get("/login.html")
async def login_page():
    return FileResponse(str(FRONTEND_DIR / "login.html"))


@router.get("/maintenance.html")
async def maintenance_page():
    return FileResponse(str(FRONTEND_DIR / "maintenance.html"))


@router.get("/admin.html")
async def admin_page():
    return FileResponse(
        str(SERVER_DIR / "admin.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


# ── API ──────────────────────────────────────────────────────────────────
@router.get("/api/firebase_config")
def api_firebase_config():
    """前端用的 Firebase client config。apiKey 不是機密，可暴露。"""
    return {
        "apiKey": os.getenv("FIREBASE_WEB_API_KEY", ""),
        "authDomain": "urban-renewal-32f02.firebaseapp.com",
        "projectId": "urban-renewal-32f02",
        "appId": os.getenv("FIREBASE_APP_ID", ""),
    }


@router.get("/api/me")
async def api_me(user: dict = Depends(get_current_user)):
    """回傳登入用戶資訊；未登入會被 middleware 擋在 401。
    第一次登入時會檢查白名單，不在白名單丟 403。
    若維護模式啟用，一律回傳 maintenance 欄位讓前端 redirect（包括 admin，admin 仍可進後台）。"""
    # late import 避免 circular import (api.app 也會 include 本 router)
    from api.app import _ensure_user_profile, _get_maintenance_state
    _ensure_user_profile(user)
    maint = _get_maintenance_state()
    if maint.get("enabled"):
        user["maintenance"] = {"enabled": True, "message": maint.get("message") or ""}
    return user


@router.get("/api/maintenance_status")
async def maintenance_status_public():
    """公開查詢維護狀態（不需登入）— 維護頁用來 poll 是否已恢復。"""
    from api.app import _get_maintenance_state
    maint = _get_maintenance_state()
    return {"enabled": bool(maint.get("enabled")), "message": maint.get("message") or ""}


@router.get("/api/version")
def api_version():
    """回傳目前 deploy 的 git short SHA — admin UI / 對版用。
    來源優先：repo VERSION 檔（deploy.sh 寫）→ git rev-parse fallback → 'unknown'。
    cache 60 秒避免每次 poll 都 fork process。"""
    now = time.time()
    if _VERSION_CACHE["sha"] and (now - _VERSION_CACHE["ts"] < 60):
        return {"sha": _VERSION_CACHE["sha"]}
    sha = "unknown"
    try:
        vfile = BASE_DIR / "VERSION"
        if vfile.exists():
            sha = vfile.read_text(encoding="utf-8").strip().split()[0][:12] or "unknown"
        else:
            r = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(BASE_DIR), capture_output=True, text=True, timeout=2,
            )
            if r.returncode == 0:
                sha = r.stdout.strip() or "unknown"
    except Exception:
        pass
    _VERSION_CACHE["sha"] = sha
    _VERSION_CACHE["ts"] = now
    return {"sha": sha}
