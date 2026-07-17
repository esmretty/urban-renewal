"""
FastAPI 後端 API。

主要路由：
  GET  /api/properties          列出所有物件（可篩選）
  GET  /api/properties/{id}     取得單一物件詳情
  POST /api/scrape               觸發爬取 + 基本分析
  GET  /api/scrape/status        取得爬取進度（SSE）
  POST /api/analyze/{id}         對單一物件執行深度分析（Phase 2）
  GET  /api/stats                統計資料
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional, AsyncGenerator, List

from fastapi import FastAPI, Query, HTTPException, Depends, Response, Request
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import os

from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from database.db import init_db, get_col, get_firestore, get_user_doc, get_user_watchlist, get_user_manual
from database.time_utils import now_tw, now_tw_iso
from google.cloud.firestore_v1 import FieldFilter
from database.models import sanitize_for_firestore, merge_watchlist_with_central
from config import BASE_DIR
from api.auth import get_current_user, require_admin, ADMIN_PORTAL_TIERS, TIER_L1

logger = logging.getLogger(__name__)


# ── 免驗證的 public 路徑 ─────────────────────────────────────────────────────
# 其他所有 /api/* /admin/* 都需要 Firebase token
_PUBLIC_PATHS = {
    "/",
    "/login.html",
    "/maintenance.html",
    "/favicon.ico",
    "/api/firebase_config",
    "/api/target_regions",
    "/api/district_new_house_price",   # 各區新成屋中位數，純市場統計，前端 boot 早於 auth 就要拿
    "/api/school_district/lookup",     # 學區查詢測試頁，純地理對照無個資
    "/api/school_district/supported",
    "/api/school_district/by_district",
    "/api/school_district/polygons",
    "/api/school_district/polygons_all",
    "/school_lookup.html",              # 學區查詢測試頁 HTML
    "/api/maintenance_status",   # 維護頁 polling 用，公開不需 auth
    "/api/busy_state",           # deploy.sh pre-check 用，無個資
    "/api/version",              # 版本號（commit short SHA），admin UI 對版用，無敏感資訊
    "/api/line/webhook",         # LINE Messaging API webhook 接收端 — LINE platform 不帶 Firebase token，
                                 # 改用 endpoint 內 HMAC-SHA256 簽章驗證 (LINE_CHANNEL_SECRET) 取代 Firebase auth
    "/admin.html",            # admin 也走自己的登入頁
}
_PUBLIC_PREFIXES = (
    "/static/",
    "/server/",               # admin portal 靜態資源
    "/data/screenshots/",
    "/api/gis_overlay/",      # 政府 GIS 圖磚 forward proxy，內容公開無個資；
                              # Leaflet tile fetch 不帶 Authorization header → 必須 public
)
# Admin-only paths — middleware verify Firebase token + 進一步 check is_admin (2026-05-13 安全 audit)
# 避免外部人士 GET /openapi.json 拿到「全 119 endpoint + schema」洩漏 API 設計
_ADMIN_ONLY_PATHS = {
    "/docs",                  # FastAPI 預設 Swagger UI
    "/redoc",                 # FastAPI 預設 ReDoc UI
    "/openapi.json",          # OpenAPI 機器可讀 JSON
    "/docs/oauth2-redirect",  # Swagger UI OAuth redirect helper
}


async def _auth_middleware(request, call_next):
    """全域攔截 /api/* /admin/* → 驗 Firebase token。"""
    path = request.url.path
    # CORS preflight 一律放行
    if request.method == "OPTIONS":
        return await call_next(request)
    if path in _PUBLIC_PATHS or any(path.startswith(p) for p in _PUBLIC_PREFIXES):
        return await call_next(request)
    # admin-only paths (/docs /redoc /openapi.json) + 所有 /api/* /admin/* 都先驗 token
    is_admin_only = path in _ADMIN_ONLY_PATHS
    if is_admin_only or path.startswith("/api/") or path.startswith("/admin/"):
        # Time auth verify (token cache hit 通常 <1ms; miss ~100-200ms)
        import time as _at
        _at0 = _at.perf_counter()
        try:
            user = await get_current_user(request)
        except HTTPException as e:
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=e.status_code, content={"detail": e.detail})
        # ── 全站 invite-only：驗 email 在 whitelist (admin 免檢) ──
        # 修 2026-05-13 audit: 之前只在 /api/me /api/watchlist /api/manual_analyze /api/scrape_url
        # 5 個 endpoint 內 call _ensure_user_profile 做 whitelist check，其他 ~25 個 user endpoint
        # 沒擋 → 任意 Gmail 拿 valid Firebase token 直接 curl /api/properties /central_search
        # 可繞過 whitelist 拉全 DB。改在 middleware 統一擋。
        if not user.get("is_admin"):
            _email = (user.get("email") or "").lower()
            if _email not in _get_email_whitelist_cached():
                from fastapi.responses import JSONResponse
                return JSONResponse(
                    status_code=403,
                    content={"detail": "此帳號尚未獲邀，請聯絡管理者將您加入白名單。"},
                )
        # admin-only 加 check is_admin (2026-05-13 audit — 避免 /docs /openapi.json 公開洩漏 API surface)
        if is_admin_only and not user.get("is_admin"):
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=403, content={"detail": "需要管理員權限才能存取 API 文件"})
        request.state.user = user
        request.state.auth_ms = (_at.perf_counter() - _at0) * 1000
    return await call_next(request)


def _safe_doc(d: dict) -> dict:
    """包 sanitize_for_firestore：任何寫入前都過一次，擋循環/超深嵌套。"""
    return sanitize_for_firestore(d)


def _is_manual_id(property_id: str) -> bool:
    return property_id.startswith("manual_")


def _extract_road_segment(addr: str) -> str:
    """抽出地址的路段（如「台北市大安區雲和街三段」→「雲和街三段」）。"""
    if not addr:
        return ""
    import re as _re
    inner = _re.sub(r"^(台北市|臺北市|新北市|桃園市|台中市|臺中市|高雄市|台南市|臺南市|基隆市|新竹市|新竹縣)", "", addr)
    inner = _re.sub(r"^[一-龥]{1,3}區", "", inner)
    m = _re.search(r"([一-龥]{1,5}(?:路|街|大道)(?:[一二三四五六七八九十]段)?)", inner)
    return m.group(1) if m else ""


def _is_replacement_change(existing: dict, incoming: dict) -> bool:
    """情況 B 偵測：同 source_id 但已變成另一物件。
    判定標準（任一成立）：
      - 路段不同（路名不一樣 OR 段不一樣）
      - 建坪差 ≥ 0.5 坪
    （屋齡不看，591 跟永慶對屋齡計算可能有小誤差）"""
    old_road = _extract_road_segment(existing.get("address") or existing.get("title") or "")
    new_road = _extract_road_segment(incoming.get("address") or incoming.get("title") or "")
    if old_road and new_road and old_road != new_road:
        return True
    old_area = existing.get("building_area_ping")
    new_area = incoming.get("building_area_ping")
    if old_area and new_area and abs(float(old_area) - float(new_area)) >= 0.5:
        return True
    return False


# 各 host 的 403/429 cooldown timestamp（epoch sec）；避免同一 host 連續 hit
_VERIFY_403_BY_HOST: dict = {}
_VERIFY_COOLDOWN_SEC = 30


def _verify_source_alive(url: str, timeout: int = 8) -> tuple:
    """情況 D：驗證一個來源 URL 是否還活著。
    回傳 (is_alive: bool, reason: str)
    保守原則：HTTP 錯誤 / timeout 一律當成「還活著」(回傳 True)，避免誤刪。
    只有明確 404/410 + 「下架/已售出」字樣才判定 dead。

    Anti-bot 處理：對同一 host（特別是永慶）若剛被 403/429 → 短暫 cooldown
    避免 verify 把人家整個 ban 掉。"""
    if not url:
        return (False, "no url")
    import time as _t
    try:
        from urllib.parse import urlparse
        host = urlparse(url).netloc.lower()
    except Exception:
        host = ""
    # 若同 host 剛被 403 → cooldown
    last403 = _VERIFY_403_BY_HOST.get(host, 0)
    if last403 and (_t.time() - last403) < _VERIFY_COOLDOWN_SEC:
        wait = _VERIFY_COOLDOWN_SEC - (_t.time() - last403)
        logger.info(f"[verify-alive] {host} cooldown {wait:.1f}s（剛 403）")
        _t.sleep(wait)
    try:
        import requests
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0"
        r = requests.get(url, headers={"User-Agent": ua}, timeout=timeout,
                         verify=False, allow_redirects=True)
        if r.status_code in (404, 410):
            return (False, f"HTTP {r.status_code}")
        if r.status_code in (403, 429):
            # 記錄 cooldown，下次同 host 要等 _VERIFY_COOLDOWN_SEC 秒
            _VERIFY_403_BY_HOST[host] = _t.time()
            return (True, f"HTTP {r.status_code} (rate-limited - keep)")
        if r.status_code != 200:
            return (True, f"HTTP {r.status_code} (uncertain - keep)")
        # 591 / 永慶下架頁偵測
        text = r.text[:8000]
        if any(kw in text for kw in ["已下架", "已售出", "物件已不存在", "找不到此物件"]):
            return (False, "頁面顯示已下架/售出")
        return (True, "alive")
    except Exception as e:
        return (True, f"error (uncertain - keep): {str(e)[:80]}")


def _verify_and_prune_sources(doc_id: str, doc_data: dict, skip_source_id: str = None) -> dict:
    """情況 D：對 doc 的所有 sources 跑 _verify_source_alive，**toggle alive flag**（不刪 source）。
    skip_source_id：剛剛觸發事件的那個 source（不重複驗它）
    回傳：updates dict（含修改後的 sources / 全死則 archived flag）；無變動回 {}
    新邏輯：dead source 仍保留在 sources[] 中（alive=false），全部 dead 才 archive 整 doc。
    """
    from database.models import compute_source_keys, all_sources_dead, make_source_key
    sources = list(doc_data.get("sources") or [])
    if not sources:
        return {}
    changed = False
    new_sources = []
    for s in sources:
        s = dict(s)
        sid_with_prefix = s.get("source_id") or ""
        # skip_source_id 可能是「591_X」或純 site_id；都試
        skip_keys = set()
        if skip_source_id:
            skip_keys.add(skip_source_id)
            if "_" in skip_source_id:
                skip_keys.add(skip_source_id.split("_", 1)[1])
        if sid_with_prefix in skip_keys:
            new_sources.append(s)
            continue
        url = s.get("url")
        if not url:
            new_sources.append(s)
            continue
        alive, reason = _verify_source_alive(url, timeout=8)
        if s.get("alive") is not False and not alive:
            s["alive"] = False
            changed = True
            logger.info(f"[verify-sources] {doc_id} 標 dead 來源 {make_source_key(s.get('name'), sid_with_prefix)}: {reason}")
        elif s.get("alive") is False and alive:
            # 死而復活（罕見：591 重新上架）
            s["alive"] = True
            changed = True
            logger.info(f"[verify-sources] {doc_id} 復活來源 {make_source_key(s.get('name'), sid_with_prefix)}")
        new_sources.append(s)
    if not changed:
        return {}
    updates = {"sources": new_sources, "source_keys": compute_source_keys(new_sources)}
    # 全部 sources 都 dead → archive 整 doc（之後重抓會 unarchive）
    if all_sources_dead({"sources": new_sources}):
        updates["archived"] = True
        updates["archived_at"] = now_tw_iso()
        updates["archived_reason"] = "所有來源連結已失效"
    return updates


class _NoopRef:
    """非觀察清單物件的寫入目標：靜默丟棄，避免污染中央或意外建立 watchlist。"""
    def set(self, *a, **k): pass
    def update(self, *a, **k): pass
    def delete(self, *a, **k): pass


def _user_override_ref(user: dict, property_id: str):
    """
    manual_ 開頭 → users/{uid}/manual/{id}（總是寫，因為 manual 本身就是私人 doc）。
    其他 → users/{uid}/watchlist/{id}：只在物件「已加入觀察清單」時才寫入；
            否則回 NoopRef（前端的 ephemeral 修改不持久化），避免「在搜尋 tab 隨手調個數字
            就被自動加進觀察清單」的副作用。物件被移除清單後，watchlist doc 也一併消失，
            所有 user override 自動清掉。
    """
    uid = user["uid"]
    if _is_manual_id(property_id):
        return get_user_manual(uid).document(property_id)
    ref = get_user_watchlist(uid).document(property_id)
    try:
        if not ref.get().exists:
            return _NoopRef()
    except Exception:
        return _NoopRef()
    return ref


def _get_email_whitelist() -> set:
    """讀 Firestore settings/email_whitelist.emails（全小寫 set）。"""
    try:
        snap = get_firestore().collection("settings").document("email_whitelist").get()
        if not snap.exists:
            return set()
        emails = (snap.to_dict() or {}).get("emails") or []
        return {str(e).strip().lower() for e in emails if e}
    except Exception as e:
        logger.warning("_get_email_whitelist failed: %s", e)
        return set()


# 1 分鐘 TTL cache — middleware 每個 request 都要 check whitelist、
# 不能每次都打 Firestore (50-200ms 太貴)。admin 加新 email 後最多等 60 秒生效。
_EMAIL_WHITELIST_CACHE: dict = {"data": None, "fetched_at": 0.0}
_EMAIL_WHITELIST_TTL = 60


def _get_email_whitelist_cached() -> set:
    import time as _t
    now = _t.time()
    if _EMAIL_WHITELIST_CACHE["data"] is not None and (now - _EMAIL_WHITELIST_CACHE["fetched_at"]) <= _EMAIL_WHITELIST_TTL:
        return _EMAIL_WHITELIST_CACHE["data"]
    data = _get_email_whitelist()
    _EMAIL_WHITELIST_CACHE["data"] = data
    _EMAIL_WHITELIST_CACHE["fetched_at"] = now
    return data


def invalidate_email_whitelist_cache():
    """admin 加/刪 email 後呼叫，下次 middleware check 立即吃到新值。"""
    _EMAIL_WHITELIST_CACHE["data"] = None
    _EMAIL_WHITELIST_CACHE["fetched_at"] = 0.0


# 維護模式狀態（本地檔案，per-server 切換不影響 production）。
# 多處共用：public.py 的 /api/me + /api/maintenance_status；admin_misc.py 的
# /admin/maintenance GET/POST。留在 app.py 為單一定義源。
def _maintenance_file_path():
    from config import DATA_DIR
    return DATA_DIR / "maintenance.json"


def _get_maintenance_state() -> dict:
    """讀本地檔案 data/maintenance.json（per-server，不共用）。
    刻意不放 Firestore：本機 debug 時切換不該影響 production VM。"""
    import json as _json
    path = _maintenance_file_path()
    if not path.exists():
        return {"enabled": False, "message": ""}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return _json.load(f) or {"enabled": False, "message": ""}
    except Exception as e:
        logger.warning("[maintenance] load state failed: %s", e)
        return {"enabled": False, "message": ""}


def _ensure_user_profile(user: dict):
    """第一次看到該 uid 就建 profile doc；已存在但缺 tier 欄位則補上。
    新用戶必須在 email 白名單（或是 owner / system admin 的 env 指定）才准建立 profile，
    否則丟 403。新用戶一律以 Level 1 會員建檔；owner / sys_admin 的環境變數會在後續
    resolve_tier 被提升（透過下方 update 分支）。"""
    try:
        ref = get_user_doc(user["uid"])
        snap = ref.get()
        resolved_tier = user.get("tier")
        email = (user.get("email") or "").lower()
        if not snap.exists:
            # 白名單把關（owner / sys_admin 免檢）
            if resolved_tier not in ADMIN_PORTAL_TIERS:
                if email not in _get_email_whitelist():
                    logger.warning("[whitelist] 拒絕新用戶 %s（不在白名單）", email)
                    raise HTTPException(
                        status_code=403,
                        detail="此帳號尚未獲邀，請聯絡管理者將您加入白名單。",
                    )
            initial_tier = resolved_tier if resolved_tier in ADMIN_PORTAL_TIERS else TIER_L1
            ref.set({
                "email": email,
                "display_name": user.get("display_name"),
                "photo_url": user.get("picture"),
                "tier": initial_tier,
                "created_at": now_tw_iso(),
            })
            # 確保回傳給 handler 的 user dict 反映實際 DB tier（避免誤把未被 env 提升的人當 admin）
            user["tier"] = initial_tier
        else:
            # 若舊 doc 沒 tier 或與 email 推算值不同（e.g. 新增 EMAIL_TO_TIER 映射），更新
            d = snap.to_dict() or {}
            if d.get("tier") != resolved_tier:
                ref.update({"tier": resolved_tier})
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("_ensure_user_profile failed for %s: %s", user.get("uid"), e)


def _read_user_property(user: dict, property_id: str) -> Optional[dict]:
    """
    讀取一筆物件（給用戶角度看）。
      - manual_ → users/{uid}/manual/{id}
      - 其他 → central + users/{uid}/watchlist/{id} merge
    找不到回 None。
    """
    uid = user["uid"]
    if _is_manual_id(property_id):
        doc = get_user_manual(uid).document(property_id).get()
        if not doc.exists:
            return None
        d = doc.to_dict() or {}
        d["id"] = doc.id
        # manual 物件永遠視為已在「觀察清單」內（它本來就是用戶私人收藏，
        # 跟 central+watchlist 結構不同但語意等效）。
        # 讓前端的「欲出價 / bonus / 新成屋價」等 override 儲存判斷能正確進行。
        d["_in_watchlist"] = True
        return d
    central = get_col().document(property_id).get()
    if not central.exists:
        return None
    cdata = central.to_dict() or {}
    merged = dict(cdata)
    wdoc = get_user_watchlist(uid).document(property_id).get()
    if wdoc.exists:
        merged = merge_watchlist_with_central(merged, wdoc.to_dict() or {})
        merged["_in_watchlist"] = True
    merged["id"] = property_id
    _apply_inferred_choice(merged)
    return merged


def _apply_inferred_choice(doc: dict) -> None:
    """若 doc 含用戶選擇的 inferred_address_choice（已 merge 進 doc）且命中候選清單，
    把 address_inferred / land_area_ping / land_area_sqm 改為該選項的值。
    若選的是 is_reverse_geo=true 的「座標反查」選項 → 地坪清空（該選項沒 land_ping）。"""
    choice = doc.get("inferred_address_choice")
    cands = doc.get("address_inferred_candidates_detail") or []
    if not choice or not cands:
        return
    matched = next((c for c in cands if c.get("address") == choice), None)
    if not matched:
        return
    # candidate 的 address 含 "台北市大安區" 前綴；address_inferred 對齊既有資料
    # (pipeline LVR / reverse-geo 寫進 doc 時都是無前綴形式) → strip 再賦值
    from helpers.text_norm import strip_region_prefix
    doc["address_inferred"] = strip_region_prefix(choice, doc.get("city", ""), doc.get("district", ""))
    land = matched.get("land_ping")
    if land is not None:
        doc["land_area_ping"] = land
        doc["land_area_sqm"] = round(land * 3.30578, 2)
    elif matched.get("is_reverse_geo"):
        # 座標反查選項：無地坪資料 → 清空
        doc["land_area_ping"] = None
        doc["land_area_sqm"] = None


_scrape_running: bool = False
_cancel_requested: bool = False


# 單筆 URL 分析併發控制（不再跟批次互斥，允許批次跑時用戶貼網址照常處理）
MAX_URL_CONCURRENCY = int(os.getenv("MAX_URL_CONCURRENCY", "2"))
_url_sem: Optional[asyncio.Semaphore] = None
_url_inflight: int = 0
_url_waiting: int = 0

# 定時 batch scheduler 狀態（設定全部存 Firestore settings/scheduler，不用 env var）
_scheduler_last_run_at: Optional[str] = None
_scheduler_last_status: str = ""
_scheduler_next_tick_at: Optional[str] = None
_sched_wake_event: Optional[asyncio.Event] = None   # 啟用時 set → loop 立刻重算倒數
SCHEDULER_MAX_COMMANDS = 3
SCHEDULER_MAX_DISTRICTS_PER_CMD = 5
SCHEDULER_INTER_COMMAND_SLEEP_SEC = 30


SCHEDULER_ALLOWED_INTERVAL_HR = (1, 3, 6, 12, 24)
SCHEDULER_VERIFY_INTERVAL_HR = (12, 24, 72, 360)   # 偵測下架可選的間隔
SCHEDULER_UPDATE_PRICES_INTERVAL_HR = (24, 168, 720)   # 更新單價可選間隔（每天/週/月）
SCHEDULER_GIS_OVERLAY_INTERVAL_HR = (360, 720, 1440, 4320)   # 更新圖層 cache：15/30/60/180 天


def _load_scheduler_config() -> dict:
    """讀 Firestore settings/scheduler。不存在時回預設（enabled=False + config.py 預設命令）。"""
    from config import SCHEDULED_SCRAPE_DISTRICTS, SCHEDULED_SCRAPE_LIMIT
    default = {
        "enabled": False,
        "interval_hr": 1,
        "commands": [{
            "districts": list(SCHEDULED_SCRAPE_DISTRICTS),
            "limit": SCHEDULED_SCRAPE_LIMIT,
        }],
    }
    try:
        doc = get_firestore().collection("settings").document("scheduler").get()
        if doc.exists:
            d = doc.to_dict() or {}
            # 舊資料若還存 interval_min，轉成 interval_hr（向後相容）
            if "interval_hr" not in d and "interval_min" in d:
                try:
                    d["interval_hr"] = max(1, int(d["interval_min"]) // 60)
                except Exception:
                    d["interval_hr"] = 1
            if d.get("interval_hr") not in SCHEDULER_ALLOWED_INTERVAL_HR:
                d["interval_hr"] = 1
            for k, v in default.items():
                if k not in d or d[k] in (None, "", []):
                    d[k] = v
            return d
    except Exception as e:
        logger.warning("[scheduler] load config failed: %s", e)
    return default


def _compute_next_tick(interval_hr: int) -> datetime:
    """回傳下一個「台北整點」tick 的 Taipei aware datetime。
    interval_hr ∈ {1,3,6,12,24}；tick 發生在 hour 是 interval_hr 倍數的整點（台北時區）。
    """
    if interval_hr not in SCHEDULER_ALLOWED_INTERVAL_HR:
        interval_hr = 1
    tw_now = now_tw()
    next_mult = ((tw_now.hour // interval_hr) + 1) * interval_hr
    day_offset, hour_in_day = divmod(next_mult, 24)
    next_tw = tw_now.replace(hour=0, minute=0, second=0, microsecond=0) \
              + timedelta(days=day_offset, hours=hour_in_day)
    return next_tw


async def _retry_queue_loop():
    """失敗重試 loop：每 60 秒掃 retry_queue，找 retry_at <= now 的 entry 重抓。
    重抓成功 → dequeue；失敗 → enqueue() 內部會更新 attempts + 排下次 retry。"""
    from database.retry_queue import list_due, dequeue, enqueue
    while True:
        try:
            await asyncio.sleep(60)
            try:
                due = list_due(limit=10)   # 每 tick 最多處理 10 筆，避免一次塞太多
            except Exception as e:
                logger.warning(f"[retry-queue] list_due 失敗: {e}")
                continue
            if not due:
                continue
            logger.info(f"[retry-queue] 處理 {len(due)} 筆到期 entry")
            for entry in due:
                src_id = entry.get("source_id")
                url = entry.get("url")
                doc_id_in_queue = entry.get("_id")
                if not (src_id and url):
                    continue
                try:
                    # 用既有 _scrape_single_url 重抓（會自動分流 591/永慶）
                    # mark_user_url=False：retry queue 補抓的物件原本是 batch 失敗來的，
                    # 不該標 user_url（標了 admin 物件列表會看不到）
                    from api.routers.admin_scrape import _scrape_single_url
                    res = await asyncio.to_thread(_scrape_single_url, url, src_id, False, mark_user_url=False)
                    # 「合法 skip」(非公寓樓層 > 5)：scraper 已知這 src_id 永遠不該建 doc
                    # → dequeue 且不再重試，避免無限循環
                    if isinstance(res, dict) and res.get("status") == "skipped_non_apartment":
                        dequeue(doc_id_in_queue)
                        logger.info(f"[retry-queue] ⏭ {src_id} 合法 skip（非公寓），從佇列移除")
                        continue
                    # 重抓完成 → 驗證是否真的有 doc + 有核心欄位
                    from database.db import find_doc_by_source_id as _fd
                    new_doc_id, doc_data = _fd(src_id)
                    if new_doc_id and doc_data and doc_data.get("price_ntd") and doc_data.get("address"):
                        dequeue(doc_id_in_queue)
                        logger.info(f"[retry-queue] ✓ 重抓成功 {src_id}，從佇列移除")
                    else:
                        # 重抓還是抓不全 → 重新 enqueue（attempts +1，到 MAX_ATTEMPTS 自動 abandon）
                        enqueue(src_id, entry.get("source") or "591", url,
                                error="retry: still missing core fields")
                        logger.warning(f"[retry-queue] ⚠ 重抓 {src_id} 仍失敗，已 re-enqueue")
                except Exception as e:
                    logger.warning(f"[retry-queue] 重抓 {src_id} 例外: {e}")
                    try:
                        enqueue(src_id, entry.get("source") or "591", url,
                                error=f"retry exception: {str(e)[:200]}")
                    except Exception:
                        pass
                # 兩個 retry entry 之間休息 5 秒
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            logger.info("[retry-queue] loop cancelled (server shutdown)")
            break
        except Exception as e:
            logger.exception(f"[retry-queue] loop iteration 失敗: {e}")
            await asyncio.sleep(30)


def _next_interval_boundary(now_dt, interval_hr: int):
    """回傳下一個 interval 整點邊界（從當天 00:00 起算）。
    例：now=13:47, interval=3 → 15:00；now=15:00:00.001, interval=3 → 18:00
    """
    from datetime import timedelta as _td
    midnight = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    hours_since = (now_dt - midnight).total_seconds() / 3600
    # 下一個 boundary：往上取整。若剛好在 boundary 上 (hours_since 為整數倍)，前進到下一個
    next_idx = int(hours_since // interval_hr) + 1
    return midnight + _td(hours=next_idx * interval_hr)


def _cmd_state_get(cmd_idx: int) -> dict:
    """讀某 command 的 last_run_at 等狀態（per-command）。"""
    try:
        doc = get_firestore().collection("settings").document("scheduler_state").get()
        if doc.exists:
            data = doc.to_dict() or {}
            return data.get(f"cmd_{cmd_idx}") or {}
    except Exception as e:
        logger.warning(f"[scheduler] _cmd_state_get fail: {e}")
    return {}


def _cmd_state_set(cmd_idx: int, **kwargs):
    """更新某 command 的狀態。"""
    try:
        get_firestore().collection("settings").document("scheduler_state").set(
            {f"cmd_{cmd_idx}": kwargs}, merge=True
        )
    except Exception as e:
        logger.warning(f"[scheduler] _cmd_state_set fail: {e}")


async def _run_gis_overlay_refresh(target_layers: list, trigger_label: str) -> str:
    """更新圖層 cache：清掉指定 layer disk cache。
    用 batch_start/end action log 對齊 update_prices/scan 的 session 紀錄框架，
    這樣會自動出現在「Admin 命令執行紀錄」表格裡。"""
    from database.run_log import log_action
    from api.gis_overlay import _disk_cache_clear
    log_action(trigger_label, "batch_start",
               message=f"開始更新圖層 cache（{len(target_layers)} 個 layer）",
               details={"layers": target_layers})
    total_deleted = 0
    layer_results = []
    try:
        for name in target_layers:
            try:
                deleted = _disk_cache_clear(name)
                total_deleted += deleted
                layer_results.append({"layer": name, "deleted": deleted})
            except Exception as e:
                logger.warning(f"[gis_overlay_refresh] 清 {name} 失敗: {e}")
                layer_results.append({"layer": name, "deleted": 0, "error": str(e)[:100]})
    finally:
        msg = f"更新圖層完成：{len(target_layers)} 個 layer 共刪除 {total_deleted} 個 cache file"
        log_action(trigger_label, "batch_end",
                   message=msg,
                   details={"total_deleted": total_deleted, "layer_results": layer_results})
    return msg


async def _run_update_prices_command(trigger_label: str = "update_prices_scheduler") -> dict:
    """自動更新預售屋單價命令：下載最新 LVR CSV + 重算各區中位數寫 Firestore。
    log 詳細紀錄：舊 LVR 期次 / 新 LVR 期次 / 樣本筆數 / 各區單價變化。
    回傳 dict 含 district_count / total_samples / diff / latest_season。失敗 raise。"""
    from database.run_log import log_action
    started_ts = now_tw_iso()
    # 用 batch_start / batch_end 沿用既有 list_sessions 框架（admin 執行紀錄看得到）
    log_action(trigger_label, "batch_start",
               message="開始更新預售屋單價",
               details={"source": "update_prices"})
    logger.info(f"[update-prices] 開始下載最新 LVR + 重算各區中位數")

    def _do():
        from scraper.download_lvr import download_recent
        from analysis.presale_price import update_district_prices
        try:
            download_recent(4)   # 下載最近 4 季 + current（既有的不會 redownload）
        except Exception as e:
            logger.warning(f"[update-prices] download_recent 部分失敗（仍試重算既有 CSV）: {e}")
        return update_district_prices(max_seasons=5)

    try:
        payload = await asyncio.to_thread(_do)
    except Exception as e:
        logger.exception(f"[update-prices] 失敗: {e}")
        log_action(trigger_label, "batch_end",
                   message=f"更新失敗：{e}",
                   details={"error": str(e)[:300]})
        raise

    by_district = payload.get("by_district") or {}
    samples = payload.get("samples") or {}
    diff = payload.get("diff") or {}
    total_samples = sum(samples.values()) if samples else 0
    prev_season = payload.get("previous_latest_season")
    new_season = payload.get("latest_season")
    row_count = payload.get("row_count", 0)

    # 各區變化的人類可讀摘要
    diff_lines = []
    changed_count = 0
    for d in sorted(diff.keys()):
        e = diff[d]
        old_v = e.get("old")
        new_v = e.get("new")
        delta = e.get("delta")
        if old_v is None:
            diff_lines.append(f"  + {d}: 新增 {new_v} 萬/坪 (樣本 {e.get('new_samples') or 0})")
            changed_count += 1
        elif new_v is None:
            diff_lines.append(f"  - {d}: 移除（原 {old_v} 萬/坪）")
            changed_count += 1
        elif delta and abs(delta) >= 0.1:
            sign = "+" if delta > 0 else ""
            diff_lines.append(
                f"  ◆ {d}: {old_v} → {new_v} 萬/坪 ({sign}{delta:.1f}, "
                f"樣本 {e.get('old_samples') or 0} → {e.get('new_samples') or 0})"
            )
            changed_count += 1
        else:
            diff_lines.append(f"  · {d}: {new_v} 萬/坪 (無變化, 樣本 {e.get('new_samples') or 0})")

    summary = (
        f"完成：LVR 期次 {prev_season or '(無紀錄)'} → {new_season}，"
        f"{len(by_district)} 區 / {total_samples} 筆樣本 / 變動 {changed_count} 區"
    )
    logger.info(f"[update-prices] {summary}")
    for line in diff_lines:
        logger.info(f"[update-prices] {line}")

    log_action(
        trigger_label, "batch_end",
        message=summary,
        details={
            "district_count": len(by_district),
            "total_samples": total_samples,
            "row_count": row_count,
            "previous_latest_season": prev_season,
            "latest_season": new_season,
            "previous_updated_at": payload.get("previous_updated_at"),
            "updated_at": payload.get("updated_at"),
            "changed_count": changed_count,
            "diff": diff,                       # 各區詳細 old/new/delta（用於 admin UI 詳情）
            "diff_summary": diff_lines,         # 人類可讀字串（log / UI 直接 render）
        },
    )
    return {
        "district_count": len(by_district),
        "total_samples": total_samples,
        "row_count": row_count,
        "previous_latest_season": prev_season,
        "latest_season": new_season,
        "changed_count": changed_count,
        "diff": diff,
        "diff_summary": diff_lines,
        "by_district": by_district,
        "updated_at": payload.get("updated_at"),
    }


async def _run_verify_alive_command(progress=None, trigger_label: str = "verify_alive_scheduler"):
    """偵測下架命令：掃所有非 archived properties，HTTP 驗活每個 source URL。
    全部 sources 都失效 → archive doc。

    進度同步寫進 Firestore settings/verify_alive_progress（給 admin UI live poll）。"""
    from database.run_log import log_action
    col = get_col()
    docs = list(col.stream())
    total = len(docs)
    archived_count = 0
    skipped = 0
    pruned_count = 0   # 部分來源失效、被 prune 的 doc 數
    archived_items = []   # 最近 archive 的物件，給 UI live 顯示
    started = now_tw_iso()
    logger.info(f"[verify-alive] 開始掃 {total} 筆物件")
    log_action(trigger_label, "verify_alive_start", message=f"開始掃 {total} 筆")
    if progress:
        progress(f"開始偵測下架（掃 {total} 筆）", 0)

    # progress 寫入 helper（best-effort，失敗不影響主流程）
    _prog_doc_ref = get_firestore().collection("settings").document("verify_alive_progress")
    def _write_progress(current, finished=False, error=None):
        try:
            payload = {
                "trigger": trigger_label,
                "started_at": started,
                "current": current,
                "total": total,
                "archived_count": archived_count,
                "skipped": skipped,
                "pruned_count": pruned_count,
                "archived_items": archived_items[-30:],   # 最近 30 筆
                "finished": finished,
                "updated_at": now_tw_iso(),
            }
            if error: payload["error"] = str(error)
            _prog_doc_ref.set(payload)
        except Exception as e:
            logger.warning("[verify-alive] write progress failed: %s", e)
    _write_progress(current=0)

    try:
        for i, d in enumerate(docs):
            data = d.to_dict() or {}
            if data.get("archived") is True:
                skipped += 1
                if (i + 1) % 5 == 0:
                    _write_progress(current=i + 1)
                continue
            from database.models import primary_source_id, compute_source_keys, all_sources_dead
            sources = list(data.get("sources") or [])
            if not sources:
                if (i + 1) % 5 == 0:
                    _write_progress(current=i + 1)
                continue

            # 驗活每個 source URL（不要早 break — 全部驗完才能 toggle alive）
            alive_results = []   # list of (source_dict, alive_bool, reason)
            for s in sources:
                url = s.get("url")
                if not url:
                    alive_results.append((s, True, "no url"))
                    continue
                alive, reason = _verify_source_alive(url, timeout=8)
                alive_results.append((s, alive, reason))
                await asyncio.sleep(2)

            # 更新 sources[].alive；不刪除 source（保留歷史，alive=False 即代表失效）
            new_sources = []
            any_change = False
            for s, is_alive, _r in alive_results:
                s2 = dict(s)
                old_alive = s2.get("alive")
                if is_alive:
                    if old_alive is False:
                        s2["alive"] = True   # 死而復活
                        any_change = True
                    elif old_alive is None:
                        s2["alive"] = True
                else:
                    if old_alive is not False:
                        s2["alive"] = False
                        any_change = True
                new_sources.append(s2)

            now_all_dead = all_sources_dead({"sources": new_sources})

            if now_all_dead and not data.get("archived"):
                # 整 doc archive
                col.document(d.id).update({
                    "archived": True,
                    "archived_at": now_tw_iso(),
                    "archived_reason": "verify-alive: 所有來源 URL 都失效（404/410/已下架字樣）",
                    "sources": new_sources,
                    "source_keys": compute_source_keys(new_sources),
                })
                archived_count += 1
                archived_items.append({
                    "doc_id": d.id,
                    "source_id": primary_source_id(data),
                    "address": data.get("address") or data.get("address_inferred") or "",
                    "at": now_tw_iso(),
                })
                logger.info(f"[verify-alive] archived {d.id}")
                log_action(trigger_label, "verify_alive_archive",
                           source_id=primary_source_id(data), doc_id=d.id,
                           message=f"archived（所有來源失效）",
                           details={"address": data.get("address")})
                _write_progress(current=i + 1)
            elif any_change:
                # 部分死亡（或復活）→ 只更新 sources，doc 留著
                dead_count = sum(1 for s in new_sources if s.get("alive") is False)
                col.document(d.id).update({
                    "sources": new_sources,
                    "source_keys": compute_source_keys(new_sources),
                })
                pruned_count += 1
                # 列出實際被標 dead 的 source（admin UI message 才不會誤導為主來源被標 dead）
                dead_keys = [
                    f"{s.get('name') or '?'}/{s.get('source_id') or '?'}"
                    for s in new_sources if s.get("alive") is False
                ]
                logger.info(f"[verify-alive] toggled alive in {d.id} (dead={dead_count}: {dead_keys})")
                log_action(trigger_label, "verify_alive_prune",
                           doc_id=d.id, source_id=primary_source_id(data),
                           message=f"標 {dead_count} 個失效來源：{', '.join(dead_keys) or '(無)'}",
                           details={"dead_count": dead_count, "dead_source_keys": dead_keys, "address": data.get("address")})
                _write_progress(current=i + 1)
            elif (i + 1) % 5 == 0:
                _write_progress(current=i + 1)

            if (i + 1) % 20 == 0 and progress:
                progress(f"已掃 {i+1}/{total}，archive {archived_count} 筆，prune {pruned_count}", 50.0 * (i+1) / total)
            # 動態 sleep：剛打過 yungching 的 doc → 等久一點避免 anti-bot
            _has_yc = any("yungching.com.tw" in (s.get("url") or "") for s in sources)
            await asyncio.sleep(3.0 if _has_yc else 0.5)
    except Exception as _e:
        logger.exception(f"[verify-alive] 掃描中斷：{_e}")
        _write_progress(current=i + 1 if 'i' in locals() else 0, finished=True, error=_e)
        raise

    msg = f"完成偵測下架：掃 {total} / 跳過 {skipped} / archived {archived_count} / pruned {pruned_count}"
    logger.info(f"[verify-alive] {msg}")
    if progress:
        progress(msg, 100)
    # 紀錄全域 last_verify_alive_at（給 dashboard 警告用）
    get_firestore().collection("settings").document("scheduler_state").set(
        {"last_verify_alive_at": now_tw_iso(), "last_verify_alive_archived": archived_count}, merge=True
    )
    _write_progress(current=total, finished=True)
    log_action(trigger_label, "verify_alive_end",
               message=f"完成：掃 {total} / 跳過 {skipped} / archived {archived_count} / pruned {pruned_count}",
               details={"total": total, "skipped": skipped, "archived": archived_count, "pruned": pruned_count})
    return {"started": started, "finished": now_tw_iso(), "total": total,
            "archived": archived_count, "skipped": skipped, "pruned": pruned_count}


async def _scheduled_scrape_loop():
    """新版 per-cmd loop：每 60 秒檢查每個 cmd 是否到 due（now - last_run_at >= interval_hr）。
    Due 的 cmd 一次跑一個（多個同時 due 也只跑一個，下次 tick 才跑下個）。
    Cmd type:
      - "scan": 掃描新物件（既有邏輯）
      - "verify_alive": 偵測下架
    """
    global _scheduler_last_run_at, _scheduler_last_status, _scheduler_next_tick_at
    while True:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            logger.info("[scheduler] loop cancelled (server shutdown)")
            break
        try:
            cfg = _load_scheduler_config()
            # per-type enabled：未設定回退 legacy enabled
            legacy_en = bool(cfg.get("enabled"))
            scan_en = bool(cfg.get("scan_enabled", legacy_en))
            verify_en = bool(cfg.get("verify_alive_enabled", legacy_en))
            update_prices_en = bool(cfg.get("update_prices_enabled", legacy_en))
            gis_overlay_en = bool(cfg.get("gis_overlay_refresh_enabled", legacy_en))
            if not (scan_en or verify_en or update_prices_en or gis_overlay_en):
                continue   # 全部 type 都關，沒事做
            if _scrape_running:
                continue   # 上次還沒跑完，等下個 tick
            cmds = cfg.get("commands") or []
            if not cmds:
                continue

            # 找最早 due 的 cmd（依 next_due_at 比對，next_due_at 一律落在 interval 整點）
            # 跳過該 type 已停用的 cmd
            now = now_tw()
            due_cmd = None
            due_idx = None
            for idx, cmd in enumerate(cmds):
                cmd_type = (cmd.get("type") or "scan").lower()
                if cmd_type == "scan" and not scan_en: continue
                if cmd_type == "verify_alive" and not verify_en: continue
                if cmd_type == "update_prices" and not update_prices_en: continue
                if cmd_type == "gis_overlay_refresh" and not gis_overlay_en: continue
                interval_hr = int(cmd.get("interval_hr") or cfg.get("interval_hr") or 24)
                state = _cmd_state_get(idx)
                nxt = state.get("next_due_at")
                if not nxt:
                    # 沒記錄 → 設成下個整點，等到那時才跑（不立刻跑，否則套用設定就觸發）
                    _cmd_state_set(idx, next_due_at=_next_interval_boundary(now, interval_hr).isoformat())
                    continue
                try:
                    nxt_dt = datetime.fromisoformat(nxt)
                    if now >= nxt_dt:
                        due_cmd = cmd
                        due_idx = idx
                        break
                except Exception:
                    pass
            if not due_cmd:
                continue

            # 先把 next_due_at 推到下一個 interval boundary
            # 為了避免 batch 跑到一半被 admin 強制 kill / 異常崩掉時，next_due_at 還卡在這一刻，
            # 下次 loop tick 又立刻被觸發同 slot。pre-advance 後若 batch 正常完成，
            # 後段 post-complete 的 _cmd_state_set 會再用「完成時間」重算一次（拿較晚的 boundary）。
            _pre_interval = int(due_cmd.get("interval_hr") or cfg.get("interval_hr") or 3)
            _cmd_state_set(due_idx,
                next_due_at=_next_interval_boundary(now, _pre_interval).isoformat(),
            )

            cmd_type = due_cmd.get("type") or "scan"
            started_at_iso = now_tw_iso()
            _scheduler_last_run_at = started_at_iso

            if cmd_type == "verify_alive":
                logger.info(f"[scheduler] 命令 {due_idx} 觸發：偵測下架")
                try:
                    result = await _run_verify_alive_command()
                    _scheduler_last_status = (
                        f"偵測下架：掃 {result['total']} / archived {result['archived']}"
                    )
                except Exception as e:
                    logger.exception(f"[scheduler] verify_alive 失敗: {e}")
                    _scheduler_last_status = f"偵測下架失敗: {e}"
                # 更新 last_run_at + next_due_at（下次要在整點）
                _v_interval = int(due_cmd.get("interval_hr") or 24)
                _cmd_state_set(due_idx,
                    last_run_at=now_tw_iso(),
                    last_status=_scheduler_last_status,
                    next_due_at=_next_interval_boundary(now_tw(), _v_interval).isoformat(),
                )
                continue

            if cmd_type == "update_prices":
                logger.info(f"[scheduler] 命令 {due_idx} 觸發：自動更新預售屋單價")
                try:
                    result = await _run_update_prices_command()
                    _scheduler_last_status = (
                        f"更新單價：{result.get('district_count', 0)} 區（共 {result.get('total_samples', 0)} 筆樣本）"
                    )
                except Exception as e:
                    logger.exception(f"[scheduler] update_prices 失敗: {e}")
                    _scheduler_last_status = f"更新單價失敗: {e}"
                _u_interval = int(due_cmd.get("interval_hr") or 720)
                _cmd_state_set(due_idx,
                    last_run_at=now_tw_iso(),
                    last_status=_scheduler_last_status,
                    next_due_at=_next_interval_boundary(now_tw(), _u_interval).isoformat(),
                )
                continue

            if cmd_type == "gis_overlay_refresh":
                logger.info(f"[scheduler] 命令 {due_idx} 觸發：更新圖層 cache")
                target_layers = list(due_cmd.get("layers") or [])
                trigger_label = "gis_overlay_refresh_scheduler"
                _scheduler_last_status = await _run_gis_overlay_refresh(target_layers, trigger_label)
                _g_interval = int(due_cmd.get("interval_hr") or 720)
                _cmd_state_set(due_idx,
                    last_run_at=now_tw_iso(),
                    last_status=_scheduler_last_status,
                    next_due_at=_next_interval_boundary(now_tw(), _g_interval).isoformat(),
                )
                continue

            # cmd_type == "scan" → 既有掃描新物件邏輯（fall through）
            cmds = [due_cmd]   # 本 tick 只跑這 1 個
            started_at_iso = now_tw_iso()
            _scheduler_last_run_at = started_at_iso
            logger.info("[scheduler] 開始執行 %d 個命令", len(cmds))
            done_count = 0
            per_command_records = []
            total_new = total_enrich = total_skip_dup = total_price = 0
            run_status = "ok"
            run_error = None
            for i, cmd in enumerate(cmds):
                if i > 0:
                    logger.info("[scheduler] 命令間休息 %d 秒", SCHEDULER_INTER_COMMAND_SLEEP_SEC)
                    await asyncio.sleep(SCHEDULER_INTER_COMMAND_SLEEP_SEC)
                dists = list(cmd.get("districts") or [])[:SCHEDULER_MAX_DISTRICTS_PER_CMD]
                lim = int(cmd.get("limit") or 30)
                # sources（新格式）優先；fallback 舊欄位 source（單一值）
                cmd_sources = cmd.get("sources")
                if not cmd_sources:
                    cmd_sources = [cmd.get("source") or "591"]
                # 固定執行順序 591 → yongqing → sinyi
                ORDER = ["591", "yongqing", "sinyi"]
                cmd_sources = [s for s in ORDER if s in cmd_sources]

                # stats 累計用（以最後一個 source 的 stats 為準也 OK，但 history 紀錄會合併）
                stats = {"new_count": 0, "enrich_count": 0, "skip_dup_count": 0,
                         "price_update_count": 0, "error": None}
                for src_idx, src in enumerate(cmd_sources):
                    if src_idx > 0:
                        logger.info("[scheduler] 同命令切換來源 %s → %s（休 %d 秒）",
                                    cmd_sources[src_idx-1], src, SCHEDULER_INTER_COMMAND_SLEEP_SEC)
                        await asyncio.sleep(SCHEDULER_INTER_COMMAND_SLEEP_SEC)
                    logger.info("[scheduler] 命令 %d/%d source %d/%d: %s 抓 %s × %d 筆",
                                i + 1, len(cmds), src_idx + 1, len(cmd_sources), src, dists, lim)
                    from api.routers.admin_scrape import _run_scrape_task
                    src_stats = await _run_scrape_task(
                        headless=True, districts=dists, limit=lim,
                        thresholds={}, triggered_by_uid=None,
                        source=src,
                        trigger_label=f"scheduler_scan_{i}",
                    )
                    # 累加統計
                    for k in ("new_count", "enrich_count", "skip_dup_count", "price_update_count"):
                        stats[k] += int(src_stats.get(k) or 0)
                    if src_stats.get("error"):
                        stats["error"] = src_stats["error"]
                cmd_err = stats.get("error")
                cmd_record = {
                    "index": i,
                    "districts": dists,
                    "limit": lim,
                    "new_count": int(stats.get("new_count") or 0),
                    "enrich_count": int(stats.get("enrich_count") or 0),
                    "skip_dup_count": int(stats.get("skip_dup_count") or 0),
                    "price_update_count": int(stats.get("price_update_count") or 0),
                    "status": "fail" if cmd_err else "ok",
                    "error": cmd_err,
                }
                per_command_records.append(cmd_record)
                total_new += cmd_record["new_count"]
                total_enrich += cmd_record["enrich_count"]
                total_skip_dup += cmd_record["skip_dup_count"]
                total_price += cmd_record["price_update_count"]
                if cmd_err:
                    run_status = "fail"
                    run_error = cmd_err
                done_count += 1
            # 寫 scheduler_history（近 7 天紀錄供 admin UI 看）
            try:
                get_firestore().collection("scheduler_history").add({
                    "started_at": started_at_iso,
                    "finished_at": now_tw_iso(),
                    "commands": per_command_records,
                    "total_new": total_new,
                    "total_enrich": total_enrich,
                    "total_skip_dup": total_skip_dup,
                    "total_price_update": total_price,
                    "status": run_status,
                    "error": run_error,
                    "trigger": "scheduler",
                })
            except Exception as he:
                logger.warning(f"[scheduler] 寫 history 失敗: {he}")
            _scheduler_last_status = (
                f"完成 {done_count}/{len(cmds)} 個命令（新增 {total_new} / 補 {total_enrich} / 重複 {total_skip_dup}）"
            )
            logger.info("[scheduler] 全部完成 %s", _scheduler_last_status)
            # 更新 last_run_at + next_due_at（下次必在整點）
            _s_interval = int(due_cmd.get("interval_hr") or cfg.get("interval_hr") or 3)
            _cmd_state_set(due_idx,
                last_run_at=now_tw_iso(),
                last_status=_scheduler_last_status,
                next_due_at=_next_interval_boundary(now_tw(), _s_interval).isoformat(),
            )
        except Exception as e:
            logger.exception("[scheduler] 定時 batch 失敗: %s", e)
            _scheduler_last_status = f"失敗: {e}"


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _url_sem, _sched_wake_event
    _url_sem = asyncio.Semaphore(MAX_URL_CONCURRENCY)
    _sched_wake_event = asyncio.Event()
    init_db()
    # 暖機：建立 gRPC 連線 + 預填 cache（同步 block 直到完成才讓 uvicorn 開始接 request）
    # 之前 cache warmup 跑背景 task 但用戶會在 1-2s 內打進來，cache 還是冷的；
    # 改成同步等完成，第一個訪客就拿到熱 cache
    logger.info("Firebase 連線中...")
    import asyncio as _aio
    await _aio.to_thread(lambda: list(get_col().limit(1).get()))
    logger.info("Firebase 連線完成")
    # 預填 target_regions counts cache + central_search 預設 districts cache（同步等完）
    try:
        await _aio.to_thread(lambda: api_target_regions(with_counts=True))
        logger.info("[warmup] target_regions counts cache 預填完成")
    except Exception as e:
        logger.warning("[warmup] target_regions 預填失敗: %s", e)
    try:
        # ⚠️ 必須跟前端 V1_DISTRICTS.enabled / inline early-fetch 列表一字不差，
        #    否則 cache key 不同 → 永遠 MISS。9 個是「預設啟用」的最常見 query。
        DEFAULT_FRONTEND_DISTRICTS = [
            "大安區", "信義區", "中山區", "中正區", "文山區",
            "新店區", "永和區", "中和區", "板橋區",
        ]
        col = get_col()
        await _aio.to_thread(lambda: _query_districts_cached(col, DEFAULT_FRONTEND_DISTRICTS, None, None))
        logger.info("[warmup] central_search query cache 預填完成（%d districts）", len(DEFAULT_FRONTEND_DISTRICTS))
    except Exception as e:
        logger.warning("[warmup] central_search 預填失敗: %s", e)
    # Scheduler / retry-queue 預設 ON（保留本機 dev 體驗）；
    # systemd 主服務（多 worker）必須設 RUN_SCHEDULER=false → 由 sidecar service 獨家跑，
    # 否則 N 個 worker 會各跑一份 → LINE 通知 N 連發、retry 重複 N 次。
    _run_sched = os.getenv("RUN_SCHEDULER", "true").lower() not in ("false", "0", "no")
    _legacy_disable = os.getenv("DISABLE_SCHEDULER", "").lower() in ("1", "true", "yes")
    if _legacy_disable or not _run_sched:
        logger.info("[scheduler] disabled in this process (RUN_SCHEDULER=%s, DISABLE_SCHEDULER=%s)", _run_sched, _legacy_disable)
        sched_task = None
        retry_task = None
    else:
        sched_task = asyncio.create_task(_scheduled_scrape_loop())
        logger.info("[scheduler] 定時 batch loop 已啟動（設定全在 Firestore settings/scheduler）")
        retry_task = asyncio.create_task(_retry_queue_loop())
        logger.info("[retry-queue] 失敗重試 loop 已啟動（每 60 秒掃 due，10 分鐘後重抓）")
    try:
        yield
    finally:
        for t in (sched_task, retry_task):
            if t:
                t.cancel()
                try: await t
                except asyncio.CancelledError: pass


app = FastAPI(title="都更神探R", version="2.0.0", lifespan=lifespan)

# CORS 白名單：只允許正式網域、localhost 開發、以及 env 指定的額外 origin
# env 變數 CORS_EXTRA_ORIGINS 逗號分隔（e.g. "https://staging.example.com,https://preview.foo")
_cors_origins = [
    "https://taipei.retty-ai.com",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
]
for _extra in os.getenv("CORS_EXTRA_ORIGINS", "").split(","):
    _extra = _extra.strip()
    if _extra:
        _cors_origins.append(_extra)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)
# Gzip 壓縮（1KB 以上才壓）— /api/central_search 的 1.6MB JSON 預計壓到 ~300KB
# 注意：必須加在 CORS 之後（middleware stack LIFO），否則 OPTIONS preflight 會被 gzip 干擾
from fastapi.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=1024)

# 登入驗證 middleware（排在 CORS/Gzip 之後才能正確處理 OPTIONS preflight）
app.middleware("http")(_auth_middleware)

FRONTEND_DIR = BASE_DIR / "frontend"
SERVER_DIR = BASE_DIR / "server"
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR / "static")), name="static")
app.mount("/server/static", StaticFiles(directory=str(SERVER_DIR / "static")), name="server_static")
app.mount("/data/screenshots", StaticFiles(directory=str(BASE_DIR / "data" / "screenshots")), name="screenshots")

# GIS overlay proxy — v2 地圖模式 optional 圖層用 (隔離 module，revert 拿掉這 1 行 + 刪 file 即可)
from api.gis_overlay import router as gis_overlay_router
app.include_router(gis_overlay_router)
from api.cadastral_search import router as cadastral_search_router
app.include_router(cadastral_search_router)
from api.routers.school_district import router as school_district_router
app.include_router(school_district_router)
from api.routers.public import router as public_router
app.include_router(public_router)
from api.routers.admin_misc import router as admin_misc_router
app.include_router(admin_misc_router)
from api.routers.property_actions import router as property_actions_router
app.include_router(property_actions_router)
from api.routers.admin_data_view import router as admin_data_view_router
app.include_router(admin_data_view_router)
from api.routers.admin_data_ops import router as admin_data_ops_router
app.include_router(admin_data_ops_router)
from api.routers.admin_scheduler import router as admin_scheduler_router
app.include_router(admin_scheduler_router)
from api.routers.admin_line import router as admin_line_router
app.include_router(admin_line_router)
from api.routers.admin_scrape import router as admin_scrape_router
app.include_router(admin_scrape_router)
from api.external_checks import router as external_checks_router
app.include_router(external_checks_router)
from api.user_reads import router as user_reads_router
app.include_router(user_reads_router)



_TARGET_REGIONS_COUNTS_CACHE = {"ts": 0.0, "data": None}
_TARGET_REGIONS_COUNTS_TTL = 120   # 2 分鐘 — 物件數變化頻率低，每次 page load 都打 Firestore 浪費



@app.get("/api/target_regions")
def api_target_regions(with_counts: bool = False):
    """回傳目標分析範圍（前端下拉選單用）— 含台北市跟新北市。
    with_counts=true → 多回每個區的物件數量，前端可隱藏「沒資料的區」。
    counts 部分 server 端 cache 120s。
    """
    from config import target_regions_for_frontend
    regions = target_regions_for_frontend()
    if not with_counts:
        return regions

    import time as _t
    now = _t.time()
    cached = _TARGET_REGIONS_COUNTS_CACHE.get("data")
    if cached and (now - _TARGET_REGIONS_COUNTS_CACHE["ts"]) < _TARGET_REGIONS_COUNTS_TTL:
        return {"regions": regions, "counts": cached}

    # 算每個 (city, district) 的物件數 — 只算 batch source（user_url 隱私不算進去）
    from collections import Counter
    counts: Counter = Counter()
    for d in get_col().select(["city", "district", "source_origin"]).get():
        data = d.to_dict() or {}
        if data.get("source_origin") == "user_url":
            continue
        c = data.get("city")
        ds = data.get("district")
        if c and ds:
            counts[(c, ds)] += 1
    counts_dict = {f"{c}|{d}": n for (c, d), n in counts.items()}
    _TARGET_REGIONS_COUNTS_CACHE["ts"] = now
    _TARGET_REGIONS_COUNTS_CACHE["data"] = counts_dict
    return {"regions": regions, "counts": counts_dict}


@app.get("/api/district_new_house_price")
def api_district_new_house_price():
    """前端用：取各行政區新成屋預設單價（萬/坪）。
    優先序：Firestore (LVR 預售屋自動更新中位數) → config.py 寫死常數。
    1 小時 cache 在 server 端，前端不必擔心打太頻繁。"""
    from analysis.presale_price import get_all_district_prices
    return get_all_district_prices()


# ── Admin API（只有 admin email 能打）───────────────────────────────────────

@app.get("/admin/stats")
async def admin_stats(admin: dict = Depends(require_admin)):
    """admin 資料總覽：物件總數 / 已分析 / 錯誤 / 用戶數，
    + 各 (source, city, district) 計數矩陣（一個 doc 多 source 會在多家都計入一次）。

    「中央物件總數」對齊前台 applyFilters 後 user 看到的 max — 排除：
      - archived / analysis_error / analysis_in_progress / deleted
      - source_origin=user_url (用戶貼 URL 私有物件)
      - district 不在前台啟用區 (台北 5 / 新北 4)
    其他欄位 (archived_count / analysis_error 等) 保留 raw DB count 給 admin 除錯用。
    """
    from database.models import canonical_source_name
    from config import TARGET_REGIONS
    # 對齊前台 V1_DISTRICTS.enabled (app2.js:36) — admin 計數也看得到完整覆蓋
    _FRONTEND_ENABLED_DISTRICTS = {
        "大安區", "信義區", "中山區", "中正區", "文山區",
        "新店區", "永和區", "中和區", "板橋區",
    }
    col = get_col()
    # 只拉統計需要的 ~10 個欄位（projection 省 protobuf decode；不影響邏輯）
    _STATS_FIELDS = [
        "analysis_status", "analysis_error", "archived", "is_foreclosure",
        "deleted", "analysis_in_progress", "source_origin",
        "city", "district", "sources",
    ]
    docs = list(col.select(_STATS_FIELDS).get())
    total = 0   # 前台可見的 (跟 applyFilters 一致)
    total_raw = len(docs)   # raw DB count (給 admin 除錯參考)
    done = err = archived = fc = 0
    # 計數矩陣：matrix[city][district][source] = count
    # source ∈ {"591", "yongqing", "sinyi", "manual", "其他"}
    matrix: dict = {}
    src_totals: dict = {"591": 0, "yongqing": 0, "sinyi": 0, "manual": 0, "其他": 0}
    for d in docs:
        data = d.to_dict() or {}
        st = data.get("analysis_status")
        if st == "done":
            done += 1
        if data.get("analysis_error"):
            err += 1
        if data.get("archived"):
            archived += 1
        if data.get("is_foreclosure"):
            fc += 1
        city = (data.get("city") or "").strip()
        district = (data.get("district") or "").strip()
        # 前台可見 count：跟 app2.js applyFilters 同邏輯
        # !deleted && !analysis_error && !analysis_in_progress && archived !== true
        # + district 在啟用區 + 不是 user_url 私有物件
        if (not data.get("deleted")
            and not data.get("analysis_error")
            and not data.get("analysis_in_progress")
            and not data.get("archived")
            and data.get("source_origin") != "user_url"
            and district in _FRONTEND_ENABLED_DISTRICTS):
            total += 1
        if not (city and district):
            continue
        sources = data.get("sources") or []
        # 一個 doc 每個 source 計一次（cross-source dup 會在多家算到）
        for s in sources:
            sname = canonical_source_name(s.get("name") or "")
            if sname not in src_totals:
                sname = "其他"
            matrix.setdefault(city, {}).setdefault(district, {"591": 0, "yongqing": 0, "sinyi": 0, "manual": 0, "其他": 0})
            matrix[city][district][sname] += 1
            src_totals[sname] += 1
    # 用戶數：users collection（可能還沒建）
    users_count = 0
    try:
        users_col = get_firestore().collection("users")
        users_count = len(list(users_col.get()))
    except Exception:
        pass
    # 給前端固定順序的 (city, [districts]) 列表（用 TARGET_REGIONS 排序，
    # DB 出現但不在 TARGET_REGIONS 的 city/district 也附在後面）
    region_order = []
    for city, conf in TARGET_REGIONS.items():
        dists = list(conf["districts"].keys())
        # 加入 DB 實際有資料但不在 TARGET_REGIONS 的 district
        if city in matrix:
            extra = [dd for dd in matrix[city].keys() if dd not in dists]
            dists = dists + extra
        region_order.append({"city": city, "districts": dists})
    # DB 有資料但不在 TARGET_REGIONS 的 city
    for city in matrix.keys():
        if city not in {r["city"] for r in region_order}:
            region_order.append({"city": city, "districts": list(matrix[city].keys())})
    return {
        "total_properties": total,          # 前台可見 (對齊 applyFilters)
        "total_properties_raw": total_raw,  # raw DB count (含封存/錯誤/非啟用區/user_url)
        "analysis_done": done,
        "analysis_error": err,
        "archived_count": archived,
        "foreclosure_count": fc,
        "total_users": users_count,
        "by_region_source": matrix,         # {city: {district: {591, yongqing, sinyi, manual, 其他}}}
        "src_totals": src_totals,           # 各家總數
        "region_order": region_order,       # [{city, districts:[...]}]
    }


@app.get("/admin/system_usage")
async def admin_system_usage(admin: dict = Depends(require_admin)):
    """admin 控制台用：磁碟空間 + screenshot 用量總覽。
    回傳：
      disk: {total_gb, used_gb, free_gb, free_pct}
      screenshots: {file_count, total_mb, oldest_age_days, by_kind: {roadwidth: N, ocr_temp: N, other: N}}
      data_dir: {total_mb}  # data/ 整體用量（screenshots + LVR DB + cache + logs）
    """
    import shutil, time
    out: dict = {"disk": {}, "screenshots": {}, "data_dir": {}}
    # ── 磁碟整體 ─
    try:
        du = shutil.disk_usage(str(BASE_DIR))
        out["disk"] = {
            "total_gb": round(du.total / 1024**3, 1),
            "used_gb": round(du.used / 1024**3, 1),
            "free_gb": round(du.free / 1024**3, 1),
            "free_pct": round(du.free / du.total * 100, 1) if du.total else 0,
        }
    except Exception as e:
        out["disk"] = {"error": str(e)}
    # ── screenshots 目錄 ─
    try:
        from config import SCREENSHOTS_DIR
        sdir = SCREENSHOTS_DIR
        if sdir.exists():
            now = time.time()
            total_size = 0
            file_count = 0
            oldest_mtime = now
            kinds = {"roadwidth": 0, "ocr_temp": 0, "other": 0}
            kind_size = {"roadwidth": 0, "ocr_temp": 0, "other": 0}
            for p in sdir.iterdir():
                if not p.is_file():
                    continue
                try:
                    st = p.stat()
                except OSError:
                    continue
                file_count += 1
                total_size += st.st_size
                if st.st_mtime < oldest_mtime:
                    oldest_mtime = st.st_mtime
                name = p.name
                if name.endswith("_roadwidth.png"):
                    kinds["roadwidth"] += 1
                    kind_size["roadwidth"] += st.st_size
                elif "_detail" in name or "_addr" in name or "_house" in name or "_tile_" in name:
                    kinds["ocr_temp"] += 1
                    kind_size["ocr_temp"] += st.st_size
                else:
                    kinds["other"] += 1
                    kind_size["other"] += st.st_size
            out["screenshots"] = {
                "file_count": file_count,
                "total_mb": round(total_size / 1024**2, 1),
                "oldest_age_days": round((now - oldest_mtime) / 86400, 1) if file_count else None,
                "by_kind": kinds,
                "by_kind_mb": {k: round(v / 1024**2, 1) for k, v in kind_size.items()},
            }
        else:
            out["screenshots"] = {"file_count": 0, "total_mb": 0}
    except Exception as e:
        out["screenshots"] = {"error": str(e)}
    # ── data/ 整體 ─（screenshots + LVR DB + logs；**跳過 data/cache/**）
    # data/cache/ 是 GIS tile 預烤產物（~360K 個 PNG / 800MB），每次 stat 一輪要 10+ 秒，
    # 不該每次 admin refresh 都掃。改用 os.scandir 在頂層 yield，遇到 cache 整顆 skip。
    try:
        import os
        data_root = BASE_DIR / "data"
        cache_total = 0
        non_cache_total = 0
        if data_root.exists():
            def _walk(d):
                nonlocal cache_total, non_cache_total
                try:
                    for entry in os.scandir(d):
                        if entry.is_dir(follow_symlinks=False):
                            if entry.name == "cache":
                                # 只算頂層大小，不展開
                                for sub in os.scandir(entry.path):
                                    if sub.is_file(follow_symlinks=False):
                                        try: cache_total += sub.stat().st_size
                                        except OSError: pass
                                    elif sub.is_dir(follow_symlinks=False):
                                        # cache 第二層也 sum 但不再深入
                                        for f in os.scandir(sub.path):
                                            try:
                                                if f.is_file(follow_symlinks=False):
                                                    cache_total += f.stat().st_size
                                            except OSError: pass
                                continue
                            _walk(entry.path)
                        elif entry.is_file(follow_symlinks=False):
                            try: non_cache_total += entry.stat().st_size
                            except OSError: pass
                except OSError: pass
            _walk(str(data_root))
            out["data_dir"] = {
                "total_mb": round((cache_total + non_cache_total) / 1024**2, 1),
                "cache_mb": round(cache_total / 1024**2, 1),
                "non_cache_mb": round(non_cache_total / 1024**2, 1),
            }
        else:
            out["data_dir"] = {"total_mb": 0}
    except Exception as e:
        out["data_dir"] = {"error": str(e)}
    # ── CPU / RAM ─（用 psutil；非阻塞 cpu_percent，靠下方 helper 取近 1 秒平均）
    try:
        import psutil
        # cpu_percent(interval=None) 第一次回 0，需要 prime → 用 _get_cpu_percent 處理
        cpu_pct = _get_cpu_percent()
        load1 = load5 = load15 = None
        try:
            la = psutil.getloadavg()
            load1, load5, load15 = round(la[0], 2), round(la[1], 2), round(la[2], 2)
        except (AttributeError, OSError):
            pass  # Windows 無 getloadavg
        vm = psutil.virtual_memory()
        sw = psutil.swap_memory()
        out["cpu"] = {
            "percent": round(cpu_pct, 1) if cpu_pct is not None else None,
            "count_logical": psutil.cpu_count(logical=True),
            "count_physical": psutil.cpu_count(logical=False),
            "load_1m": load1,
            "load_5m": load5,
            "load_15m": load15,
        }
        out["ram"] = {
            "total_gb": round(vm.total / 1024**3, 2),
            "used_gb": round(vm.used / 1024**3, 2),
            "available_gb": round(vm.available / 1024**3, 2),
            "percent": round(vm.percent, 1),
        }
        out["swap"] = {
            "total_gb": round(sw.total / 1024**3, 2),
            "used_gb": round(sw.used / 1024**3, 2),
            "percent": round(sw.percent, 1) if sw.total else 0.0,
        }
        # 本 process 自身用量（容器/VM 共用主機時更具參考性）
        try:
            proc = psutil.Process()
            with proc.oneshot():
                rss_mb = round(proc.memory_info().rss / 1024**2, 1)
                proc_cpu = round(proc.cpu_percent(interval=None), 1)
            out["process"] = {
                "rss_mb": rss_mb,
                "cpu_percent": proc_cpu,
                "num_threads": proc.num_threads(),
            }
        except Exception as e:
            out["process"] = {"error": str(e)}
    except Exception as e:
        out["cpu"] = {"error": str(e)}
        out["ram"] = {"error": str(e)}
    return out


# 背景 thread 持續用 5 秒窗 sample CPU；admin/stats 只讀 cache 不自己 trigger 取樣。
# 之前實作用 interval=None「自上次呼叫到現在」的平均 → admin/stats handler 本身的 DB query
# 算進 sample window，回傳值永遠停在 15-25%（觀察者效應）。改成獨立背景 thread 取樣，
# admin/stats 只讀最新 cache，跟自己 request 完全解耦。
_CPU_PCT_CACHE = {"value": None}
_CPU_SAMPLER_STARTED = {"v": False}

def _cpu_sampler_loop():
    import psutil
    psutil.cpu_percent(interval=None)  # prime first call (always returns 0)
    while True:
        try:
            v = psutil.cpu_percent(interval=5.0)  # blocks 5s, returns true avg
            _CPU_PCT_CACHE["value"] = round(v, 1)
        except Exception:
            pass

def _get_cpu_percent():
    if not _CPU_SAMPLER_STARTED["v"]:
        _CPU_SAMPLER_STARTED["v"] = True
        import threading
        threading.Thread(target=_cpu_sampler_loop, daemon=True, name="cpu-sampler").start()
    v = _CPU_PCT_CACHE["value"]
    if v is not None:
        return v
    # cache 還沒 prime（worker 剛起來、或 refresh 打到另一個 worker 的冷 cache）→
    # 同步 block 0.5s 直接取一筆，避免前端顯示 "?%"。block 期間 handler 自己不做事，
    # 不會被觀察者效應污染。
    try:
        import psutil
        v = round(psutil.cpu_percent(interval=0.5), 1)
        _CPU_PCT_CACHE["value"] = v
        return v
    except Exception:
        return None


@app.post("/admin/system_usage/refetch_zoning_batch")
async def admin_refetch_zoning_batch(city: Optional[str] = None,
                                       limit: int = 500,
                                       admin: dict = Depends(require_admin)):
    """對「當前缺 zoning 的物件」批次重跑 GeoServer 查詢。

    用途：政府 GeoServer (zonegeo.udd.gov.taipei) 偶爾服務異常（SSL timeout），
    那段時間入庫的物件 zoning_source='not_found' 且不會自動修。等 GeoServer 恢復後
    admin 按此按鈕批次補查。

    篩選：zoning IS NULL AND city==指定 (預設全部台北市，新北市 GeoServer 沒覆蓋不重跑)。
    對每筆 lookup_zoning + query_road_width_taipei，成功 update doc。回統計 dict。
    limit 上限 500 避免超時；跑完前端可再按一次拉剩下的。
    """
    from analysis.gov_gis import lookup_zoning_by_coord, query_road_width_taipei
    city = city or "台北市"
    col = get_col()
    candidates = []
    for d in col.where(filter=FieldFilter("city", "==", city)).stream():
        x = d.to_dict() or {}
        if x.get("archived") or x.get("deleted"):
            continue
        if x.get("zoning"):
            continue
        if x.get("zoning_source") not in (None, "", "not_found", "geoserver_unreachable"):
            continue   # 多分區 case (zoning_source=yongqing_detail_multi 等) 有 zoning_list 不需 refetch
        if not (x.get("latitude") and x.get("longitude")):
            continue
        candidates.append((d.id, x))
        if len(candidates) >= int(limit):
            break

    total = len(candidates)
    updated_z = 0; updated_r = 0; still_fail = 0
    errs = []
    for doc_id, x in candidates:
        lat, lng = x["latitude"], x["longitude"]
        updates = {}
        # zoning
        try:
            z = lookup_zoning_by_coord(lat, lng, city)
            if z.get("zoning") or (z.get("zone_list") and len(z["zone_list"]) > 0):
                zone_list = z.get("zone_list")
                if zone_list and len(zone_list) > 1:
                    updates["zoning"] = None   # 多分區 by design
                else:
                    updates["zoning"] = z.get("zoning")
                updates["zoning_original"] = z.get("original_zone")
                updates["zoning_source"] = z.get("zoning_source")
                updates["zoning_source_url"] = z.get("zoning_source_url")
                updates["zoning_list"] = zone_list
                updates["zoning_error"] = None
                updates["zoning_lookup_at"] = now_tw_iso()
                updated_z += 1
            else:
                still_fail += 1
        except Exception as e:
            errs.append(f"{doc_id}: zoning {e}")
        # road_width (同 GeoServer server；zoning 通就 road_width 通)
        if updated_z and updates.get("zoning") and city == "台北市":
            try:
                rw = query_road_width_taipei(lat, lng)
                if rw and rw.get("road_width_m"):
                    updates["road_width_m"] = rw["road_width_m"]
                    updates["road_width_name"] = rw.get("road_width_name")
                    updates["road_width_all"] = rw.get("road_width_all")
                    updated_r += 1
            except Exception as e:
                errs.append(f"{doc_id}: road {e}")
        if updates:
            col.document(doc_id).update(updates)
    logger.warning(
        f"[admin] {admin.get('email')} refetch_zoning_batch city={city}: "
        f"total={total} zoning_updated={updated_z} road_updated={updated_r} "
        f"still_fail={still_fail}"
    )
    return {
        "total_candidates": total,
        "zoning_updated": updated_z,
        "road_updated": updated_r,
        "still_fail": still_fail,
        "errors": errs[:10],
    }


@app.post("/admin/system_usage/cleanup_archived_roadwidth")
async def admin_cleanup_archived_roadwidth(admin: dict = Depends(require_admin)):
    """清掉「對應 doc 已 archived 或不存在」的 _roadwidth.png 截圖。

    cleanup_orphan_ocr 不碰 roadwidth（前端 detail 按鈕仍在用活著的物件），
    但 archived 物件的 roadwidth 不會再被開、占空間，可安全清。

    判定：
      - 從所有 central doc 的 source_keys 建 {file_src_id: archived_bool} map
        file_src_id 格式 = "{name}_{site_id}" 對應檔名 `{name}_{site_id}_roadwidth.png`
      - 掃 screenshots/*_roadwidth.png 逐檔比對：
        * doc archived → 刪
        * doc 不存在（reanalyze 換 doc_id 殘留 / 已 hard delete）→ 刪
        * doc 活著 → 保留
    """
    from config import SCREENSHOTS_DIR
    sdir = SCREENSHOTS_DIR
    if not sdir.exists():
        return {"deleted": 0, "freed_mb": 0, "by_reason": {}, "kept_active": 0}
    # 1. 建 src_id → archived map（一次拉全 central doc）
    col = get_col()
    src_to_archived: dict = {}   # "591_20151338" → bool (True=archived)
    for d in col.stream():
        dd = d.to_dict() or {}
        archived = bool(dd.get("archived"))
        for key in dd.get("source_keys") or []:
            if ":" not in key:
                continue
            name, site_id = key.split(":", 1)
            file_src_id = f"{name}_{site_id}"
            # 同 src_id 多 doc 命中時保守取「至少一個活著就算活著」
            if file_src_id in src_to_archived:
                src_to_archived[file_src_id] = src_to_archived[file_src_id] and archived
            else:
                src_to_archived[file_src_id] = archived
    deleted = 0
    freed = 0
    by_reason = {"archived": 0, "no_doc": 0}
    kept_active = 0
    errs: list[str] = []
    suffix = "_roadwidth.png"
    for p in sdir.iterdir():
        if not p.is_file():
            continue
        if not p.name.endswith(suffix):
            continue
        file_src_id = p.name[:-len(suffix)]
        status = src_to_archived.get(file_src_id)
        if status is None:
            reason = "no_doc"
        elif status:
            reason = "archived"
        else:
            kept_active += 1
            continue
        try:
            size = p.stat().st_size
            p.unlink()
            deleted += 1
            freed += size
            by_reason[reason] += 1
        except Exception as e:
            errs.append(f"{p.name}: {e}")
    logger.warning(
        f"[admin] {admin.get('email')} 清 archived roadwidth：刪 {deleted} 檔 / "
        f"{round(freed/1024**2,1)} MB；archived={by_reason['archived']} no_doc={by_reason['no_doc']}；活著保留 {kept_active}"
    )
    return {
        "deleted": deleted,
        "freed_mb": round(freed / 1024**2, 1),
        "by_reason": by_reason,
        "kept_active": kept_active,
        "errors": errs[:20],
    }


@app.post("/admin/system_usage/cleanup_orphan_ocr")
async def admin_cleanup_orphan_ocr(admin: dict = Depends(require_admin)):
    """清掉 screenshots 裡的孤兒 OCR 截圖（_detail / _addr / _house / *_tile_*）。
    這些檔正常情況分析完會被 _cleanup_ephemeral_screenshots 清掉，但若某筆中途 raise
    沒走到 cleanup 就會孤兒。本 endpoint 一次掃光。**不刪 _roadwidth.png**（前端按鈕仍在用）。"""
    from config import SCREENSHOTS_DIR
    sdir = SCREENSHOTS_DIR
    if not sdir.exists():
        return {"deleted": 0, "freed_mb": 0}
    deleted = 0
    freed = 0
    errs: list[str] = []
    for p in sdir.iterdir():
        if not p.is_file():
            continue
        name = p.name
        if name.endswith("_roadwidth.png"):
            continue
        if ("_detail" in name) or ("_addr" in name) or ("_house" in name) or ("_tile_" in name):
            try:
                size = p.stat().st_size
                p.unlink()
                deleted += 1
                freed += size
            except Exception as e:
                errs.append(f"{name}: {e}")
    logger.warning(f"[admin] {admin.get('email')} 清孤兒 OCR 截圖：{deleted} 檔 / {round(freed/1024**2,1)} MB")
    return {
        "deleted": deleted,
        "freed_mb": round(freed / 1024**2, 1),
        "errors": errs[:20],   # 最多回 20 條 error
    }


def _gis_overlay_layers_for_admin() -> list:
    from api.gis_overlay import _disk_cache_layers
    return _disk_cache_layers()


def _gis_overlay_layer_meta(name: str) -> dict:
    """delegate 給 gis_overlay._layer_admin_meta (含 display_name + data_source + group)。"""
    from api.gis_overlay import _layer_admin_meta
    return _layer_admin_meta(name)


def _ensure_layer_display_map() -> None:
    """deprecated — kept for compat; 改用 _gis_overlay_layer_meta。"""


# ── 物件列表 ──────────────────────────────────────────────────────────────────

@app.get("/api/properties")
def list_properties(
    city: Optional[str] = Query(None),
    district: Optional[str] = Query(None),
    building_type: Optional[str] = Query(None),
    min_score: float = Query(0),
    max_score: float = Query(100),
    min_age: Optional[int] = Query(None),
    recommendation: Optional[str] = Query(None),
    sort_by: str = Query("list_rank"),
    sort_dir: str = Query("asc"),
    limit: int = Query(200),
    offset: int = Query(0),
    slim: bool = Query(False),                        # True: 卡片列表用，剝除重欄位
    user: dict = Depends(get_current_user),
):
    """
    列出「當前用戶的清單」=
      (users/{uid}/watchlist/* join 中央 properties/*)  +  users/{uid}/manual/*
    完全不回別人的 watchlist 或 manual。
    """
    uid = user["uid"]
    items = []
    # 1) watchlist 引用 → join 中央 properties
    watch_docs = {d.id: d.to_dict() or {} for d in get_user_watchlist(uid).get()}
    if watch_docs:
        central_col = get_col()
        for src_id, wdata in watch_docs.items():
            c = central_col.document(src_id).get()
            if not c.exists:
                continue
            cdata = c.to_dict() or {}
            merged = merge_watchlist_with_central(cdata, wdata)
            merged["id"] = src_id
            merged["_added_at"] = wdata.get("added_at") or cdata.get("scraped_at")
            merged["_in_watchlist"] = True
            _apply_inferred_choice(merged)
            items.append(merged)
    # 2) 私人 manual 物件（完整 doc）— manual 本身就是私人收藏，等同已在觀察清單
    for d in get_user_manual(uid).get():
        data = d.to_dict() or {}
        data["id"] = d.id
        # manual 用 scraped_at 當作「加入時間」，讓排序一致
        data["_added_at"] = data.get("scraped_at") or data.get("scrape_session_at")
        data["_in_watchlist"] = True
        items.append(data)

    # 手動輸入物件永遠不被 server-side filter 隱藏（資料通常不完整會被誤殺）
    def _is_manual(it):
        srcs = it.get("sources") or []
        return any(s.get("name") == "manual" for s in srcs)
    if city:
        items = [i for i in items if _is_manual(i) or i.get("city") == city]
    if district:
        items = [i for i in items if _is_manual(i) or i.get("district") == district]
    if building_type:
        items = [i for i in items if _is_manual(i) or i.get("building_type") == building_type]
    if min_score > 0:
        items = [i for i in items if _is_manual(i) or (i.get("score_total") or 0) >= min_score]
    if max_score < 100:
        items = [i for i in items if _is_manual(i) or (i.get("score_total") or 0) <= max_score]
    if min_age:
        items = [i for i in items if _is_manual(i) or (i.get("building_age") or 0) >= min_age]
    if recommendation:
        items = [i for i in items if _is_manual(i) or i.get("ai_recommendation") == recommendation]

    # 排序
    if sort_by in ("list_rank", "added_at"):
        # 預設：按 _added_at 降序（新加入的在前；manual 跟 watchlist 一起按加入時間排）
        items.sort(key=lambda x: x.get("_added_at") or "", reverse=True)
    elif sort_by == "last_change_at":
        # 「物件有變動」排序：按 last_change_at 降序，無變動的沉底
        # last_change_at fallback 到 scrape_session_at（避免完全沒事件的物件全擠在底）
        def _ev(x):
            return x.get("last_change_at") or x.get("scrape_session_at") or x.get("scraped_at") or ""
        items.sort(key=_ev, reverse=True)
    else:
        reverse = sort_dir == "desc"
        def _val(x):
            v = x.get(sort_by)
            if v is None and sort_by == "published_at":
                v = x.get("scraped_at")
            return v
        # None 永遠沉底（不受 reverse 影響）
        has_val = [x for x in items if _val(x) is not None]
        no_val = [x for x in items if _val(x) is None]
        has_val.sort(key=_val, reverse=reverse)
        items = has_val + no_val

    total = len(items)
    out_items = items[offset: offset + limit]
    if slim:
        out_items = [_strip_for_list(it) for it in out_items]
    return {"total": total, "items": out_items}


# slim=true 時不回給卡片列表的重欄位（用戶點開詳情才需要的）
# 注意：lvr_records 不在這裡（用戶要求保留在卡片列表）
# 注意：renewal_v2 不在這裡（卡片倍數計算需要 base_far_pct/share_ratio，這些都在 renewal_v2 裡）
#       改成在 _strip_for_list 內只剝掉 renewal_v2.scenarios 子 dict（佔 ~63% 大小）
LIST_DROP_FIELDS = {
    "road_width_all",                      # 周邊所有路寬清單（詳情頁用，卡片只用採用的 road_width_m）
    "ai_reason",                            # AI 分析推薦理由文字
    "ai_analysis",                          # AI 完整分析文字
    "nearby_mrts",                          # 附近捷運站清單
    "address_inferred_candidates_detail",   # 地址候選完整詳情（詳情頁修正地址用）
    "screenshot_roadwidth",                 # 路寬判讀截圖
    "screenshot_cadastral",                 # 地籍套繪截圖
    "screenshot_zoning",                    # 分區圖截圖
    "screenshot_renewal",                   # 都更案截圖
    "road_width_vision_reason",             # 路寬 Vision 判讀說明文
}


def _strip_for_list(d: dict) -> dict:
    """slim=true 時把列表不需要的重欄位剝掉。
    renewal_v2 整個保留會帶 ~530B/doc，但只需要其中 base_far_pct/share_ratio/parking_value_wan
    幾個小欄位給卡片倍數計算用 — scenarios 子 dict 佔 63% 大小，移除即可省 ~330B/doc。
    """
    out = {k: v for k, v in d.items() if k not in LIST_DROP_FIELDS}
    rv2 = out.get("renewal_v2")
    if isinstance(rv2, dict) and "scenarios" in rv2:
        out["renewal_v2"] = {k: v for k, v in rv2.items() if k != "scenarios"}
    return out


# /api/central_search 列表用的欄位白名單 — Firestore .select() projection 用
# 跟 LIST_DROP_FIELDS 互補：DROP 是「拿回來後剝掉」、KEEP 是「Firestore 端就不傳」
# 加 select() 後預期省 Firestore protobuf decode + Python dict 化的時間
# ★ 漏列任何前端列表/filter/sort 用到的 field → 該卡片會缺欄位 → 投資決策 bug
# ★ 改前端 cardHTML 用新 field 時，同步加進這 list
LIST_KEEP_FIELDS = [
    # 基本識別 / source
    "source_id", "source_keys", "sources", "source_origin",
    "submitted_by_uid", "submitted_by_email", "user_url",
    # 物件屬性
    "district", "city", "building_type",
    "building_age", "building_age_completed_year",
    "building_area_ping", "land_area_ping", "price_ntd",
    "address", "title",
    "floor", "total_floors", "floor_range_min", "floor_range_max",
    "land_area_inconsistent", "image_url",
    # 狀態 (client filter / hide)
    "archived", "archived_at", "archived_reason",
    "deleted", "analysis_status", "analysis_error", "analysis_in_progress",
    # 抗性
    "is_basement", "is_remote_area", "unsuitable_for_renewal", "is_foreclosure",
    # 分數 / AI 推薦
    "score_total", "ai_recommendation",
    # 學區
    "school_elementary", "school_junior_high",
    # 地理 / 地址推測
    "latitude", "longitude", "source_latitude", "source_longitude",
    "address_inferred", "address_road_fixed", "address_suspicious",
    # 路寬 (列表卡片要顯示)
    "road_width_m", "road_width_m_override", "road_width_name", "road_width_unknown",
    # 分區 / 都更
    "zoning", "zoning_original", "zoning_candidates", "zoning_error",
    "zoning_list", "zoning_source", "zoning_source_url",
    "zoning_ratios", "zoning_ratios_locked",
    "renewal_v2",   # 列表倍數計算用，scenarios 子 dict 在 _strip_for_list 內剝
    "redev_cases",
    # 用戶 override (merge_watchlist_with_central / _apply_inferred_choice 用)
    "bonus_dugen", "bonus_weishau", "rebuild_coeff", "floor_premium",
    "new_house_price_wan_override", "desired_price_wan",
    # Timestamps
    "scrape_session_at", "scraped_at", "published_at", "last_change_at",
    "added_at", "is_price_changed",
    # 雜
    "nearest_mrt_dist_m",
    "lvr_records",   # 用戶要求保留在卡片列表
]


def _query_districts_parallel(col, dist_list, max_price_wan, min_price_wan):
    """切 N districts query 用 Firestore `in` operator (現代 SDK 支援 ≤30 值)。

    舊版本曾因為 `in` 內部 serialize fan-out 改用 N 個 parallel `==`，
    但實測：N=15 parallel 仍要 ~1.3 秒（gRPC 多路複用瓶頸）。
    現在 Python SDK ≥ 2.x 對 `in [≤30]` 已優化成單 query，1 個 round-trip 就拿全部。
    超過 30 districts → 切 chunks 平行 fire（保留舊 fallback）。

    .select(LIST_KEEP_FIELDS): Firestore 端 projection，少傳少 parse — central_search
    永遠 slim=true (前端固定)，列表用不到 ai_analysis / screenshot_* 等重欄位。
    """
    from google.cloud.firestore_v1 import FieldFilter

    max_ntd = int(max_price_wan * 10000) if (max_price_wan and max_price_wan > 0) else None
    min_ntd = int(min_price_wan * 10000) if (min_price_wan and min_price_wan > 0) else None

    def _q_in(chunk):
        q = col.where(filter=FieldFilter("district", "in", list(chunk)))
        if max_ntd is not None:
            q = q.where(filter=FieldFilter("price_ntd", "<=", max_ntd))
        if min_ntd is not None:
            q = q.where(filter=FieldFilter("price_ntd", ">=", min_ntd))
        q = q.select(LIST_KEEP_FIELDS)
        return list(q.get())

    if len(dist_list) <= 30:
        return _q_in(dist_list)

    # >30 districts：切成 30 一組 chunks 平行 fire
    import concurrent.futures as _cf
    chunks = [dist_list[i:i+30] for i in range(0, len(dist_list), 30)]
    docs = []
    seen = set()
    with _cf.ThreadPoolExecutor(max_workers=min(len(chunks), 4)) as ex:
        for sub in ex.map(_q_in, chunks):
            for d in sub:
                if d.id not in seen:
                    seen.add(d.id)
                    docs.append(d)
    return docs


# Server-side in-memory cache for Firestore query results (NOT response — watchlist join is per-user)
# Key: 規範化 query params 的 hash；Value: (timestamp, list of (doc_id, doc_data) tuples)
# 純 lazy fill：TTL 內 hit 直接回；過期 → 真等。沒有背景 thread。
_FIRESTORE_QUERY_CACHE: dict = {}
_FIRESTORE_QUERY_CACHE_TTL = 120   # 2 分鐘
_FIRESTORE_QUERY_CACHE_MAX = 128


def _cache_key_for_query(dist_list, max_price_wan, min_price_wan):
    """規範化成 cache key — 只跟 Firestore query 相關的參數，跟 user/uid 無關。"""
    if dist_list:
        ds = ",".join(sorted(dist_list))
    else:
        ds = "*"
    return f"{ds}|{max_price_wan or 0}|{min_price_wan or 0}"


def invalidate_query_cache():
    """admin 改物件、scraper 寫入後呼叫，強制下次 query 走 DB。"""
    _FIRESTORE_QUERY_CACHE.clear()


def _query_districts_cached(col, dist_list, max_price_wan, min_price_wan):
    """純 lazy cache。回傳 (docs_data_list, cache_state)。
    cache_state: "fresh" (hit) / "miss"
    """
    import time as _t
    key = _cache_key_for_query(dist_list, max_price_wan, min_price_wan)
    now = _t.time()

    cached = _FIRESTORE_QUERY_CACHE.get(key)
    if cached and (now - cached[0]) < _FIRESTORE_QUERY_CACHE_TTL:
        return cached[1], "fresh"

    # miss — do real query
    if dist_list and 0 < len(dist_list) <= 30:
        docs = _query_districts_parallel(col, dist_list, max_price_wan, min_price_wan)
    else:
        docs = list(col.get())
    docs_data = [(d.id, d.to_dict() or {}) for d in docs]

    if len(_FIRESTORE_QUERY_CACHE) >= _FIRESTORE_QUERY_CACHE_MAX:
        oldest_key = min(_FIRESTORE_QUERY_CACHE, key=lambda k: _FIRESTORE_QUERY_CACHE[k][0])
        _FIRESTORE_QUERY_CACHE.pop(oldest_key, None)

    _FIRESTORE_QUERY_CACHE[key] = (now, docs_data)
    return docs_data, "miss"


@app.get("/api/central_search")
def central_search(
    request: Request,
    response: Response,
    q: Optional[str] = Query(None),
    road: Optional[str] = Query(None),
    districts: Optional[str] = Query(None),           # 逗號分隔：中正區,大安區,...
    building_types: Optional[str] = Query(None),      # 逗號分隔：公寓,透天厝,...
    floors: Optional[str] = Query(None),              # 逗號分隔：1,2,3,4,5（物件樓層）
    min_price_wan: Optional[float] = Query(None),
    max_price_wan: Optional[float] = Query(None),
    max_bld_price_per_ping: Optional[float] = Query(None),
    max_land_price_per_ping: Optional[float] = Query(None),
    min_land_ping: Optional[float] = Query(None),
    limit: int = Query(1000),
    slim: bool = Query(False),                        # True: 卡片列表用，剝除 ~470KB 重欄位
    user: dict = Depends(get_current_user),
):
    """
    探索 tab 的搜尋 API：所有條件在 server 端過濾後才回傳。
    每筆附 `_in_watchlist`(bool) 讓前端標記。

    回傳 Server-Timing header 把 server 端各 phase 的耗時 surface 給前端 timer。
    """
    import time as _t
    _phase_t = {}
    _t0 = _t.perf_counter()
    def _tick(name):
        nonlocal _t0
        now = _t.perf_counter()
        _phase_t[name] = (now - _t0) * 1000
        _t0 = now

    uid = user["uid"]
    # filter params 先 parse 完，這樣 main query 才能跟 watchlist 同時 fire
    dist_set = {d.strip() for d in districts.split(",") if d.strip()} if districts else None
    btype_set = {t.strip() for t in building_types.split(",") if t.strip()} if building_types else None
    floor_set = None
    wants_basement = False    # 用戶有沒有勾「地下室」chip (value='B')
    if floors:
        try:
            _f_tokens = [f.strip() for f in floors.split(",") if f.strip()]
            int_parts = []
            for tok in _f_tokens:
                if tok.upper() == "B":
                    wants_basement = True
                else:
                    try:
                        int_parts.append(int(tok))
                    except ValueError: pass
            floor_set = set(int_parts) if int_parts else None
        except ValueError:
            floor_set = None

    col = get_col()
    # 把 filter 推到 Firestore 端 (composite index: district + price_ntd)：
    #   - district in [...]：cuts 全收 ~543 → 符合區的子集 (~200)，2× 加速
    #   - price_ntd <= max_wan：再切一刀，full speedup 視 max 而定：
    #       max=5000萬 (default): 3×；max=1500萬: 10×；max=1000萬: 26×
    # dist_set 為 None（用戶沒挑）時 fallback 全收。
    # 注意：變數名 fs_q（不要叫 q —— 跟函式參數 q: Optional[str] 衝突會被 shadow，
    #       導致下面 `if q: kw = q.strip()` 拿到 Firestore Query 物件而 AttributeError）
    # ── 並行打兩個 Firestore query：watchlist + 主 query ─
    # 兩個各自一條 gRPC stream，等同網路 round-trip 折半
    cache_state = "miss"
    docs_data = []
    my_watchlist = {}
    my_watchlist_ids = set()
    dist_list_for_cache = list(dist_set) if (dist_set and len(dist_set) <= 30) else None

    def _fetch_watchlist():
        try:
            return {d.id: (d.to_dict() or {}) for d in get_user_watchlist(uid).get()}
        except Exception as _e:
            logger.warning(f"[central_search] watchlist fetch 失敗：{_e}")
            return {}

    def _fetch_main():
        try:
            return _query_districts_cached(col, dist_list_for_cache, max_price_wan, min_price_wan)
        except Exception as _e:
            logger.warning(f"[central_search] cached/parallel query 失敗，fallback 全收：{_e}")
            return ([(d.id, d.to_dict() or {}) for d in col.get()], "miss")

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_watch = pool.submit(_fetch_watchlist)
        f_main = pool.submit(_fetch_main)
        my_watchlist = f_watch.result()
        my_watchlist_ids = set(my_watchlist.keys())
        docs_data, cache_state = f_main.result()
    _tick("firestore_query")
    _phase_t["docs_n"] = len(docs_data)
    _phase_t["cache_state"] = cache_state  # fresh / stale / miss
    _phase_t["cache_hit"] = 1 if cache_state in ("fresh", "stale") else 0
    items = []
    for doc_id, data in docs_data:
        # cache hit 時 docs_data 是共用的（多用戶共享） — 後面要 mutate (加 _in_watchlist / id)
        # → 必須 shallow copy 避免污染 cache 給下一個 request 的副作用
        data = dict(data)
        # 「狀態」相關的隱藏（archived / analysis_error / analysis_in_progress / skipped）
        # 全部交給 client 過濾，admin 跟 client 看到的 API response 一致。
        # server 只保留隱私邊界：用戶貼 URL 送出的物件不該洩漏給其他用戶
        # （舊資料沒 source_origin 欄位 → 當作 batch 不過濾）
        if data.get("source_origin") == "user_url":
            continue
        if dist_set is not None and data.get("district") not in dist_set:
            continue
        if btype_set is not None and data.get("building_type") not in btype_set:
            continue
        # 樓層維度：地下室物件 (is_basement=True) 走 wants_basement，其餘走 floor_set
        is_bsmt = bool(data.get("is_basement"))
        if floor_set is not None or wants_basement:
            if is_bsmt:
                if not wants_basement:
                    continue   # 用戶沒勾「地下室」chip → 地下室物件不顯示
            else:
                if floor_set is None:
                    continue   # 用戶只勾「地下室」沒勾任何整數樓層 → 非地下室物件全濾掉
                # 優先用 floor_range_min/max（樓中樓物件 1F~2F 用戶搜 1F 或 2F 都該命中交集）
                fr_min = data.get("floor_range_min")
                fr_max = data.get("floor_range_max")
                if fr_min is not None and fr_max is not None:
                    # 物件 [fr_min, fr_max] 跟 user 選的 floor_set 有交集才 match
                    if not any(fr_min <= f <= fr_max for f in floor_set):
                        continue
                else:
                    # 舊 doc fallback：parse floor 字串嘗試取數字
                    try:
                        f_val = int(data.get("floor")) if data.get("floor") is not None else None
                    except (TypeError, ValueError):
                        f_val = None
                    # 缺樓層資料 pass-through；有資料才比對
                    if f_val is not None and f_val not in floor_set:
                        continue
        # 缺資料一律 pass-through（不因為「缺欄位」就被刷掉）
        pn = data.get("price_ntd")
        if min_price_wan is not None and pn and pn / 10000 < min_price_wan:
            continue
        if max_price_wan is not None and pn and pn / 10000 > max_price_wan:
            continue
        if max_bld_price_per_ping is not None:
            bp = data.get("building_area_ping")
            if bp and pn and (pn / bp / 10000) > max_bld_price_per_ping:
                continue
        if max_land_price_per_ping is not None:
            lp = data.get("land_area_ping")
            if lp and pn and (pn / lp / 10000) > max_land_price_per_ping:
                continue
        if min_land_ping is not None and data.get("land_area_ping") is not None \
                and data["land_area_ping"] < min_land_ping:
            continue
        if road:
            r = road.strip()
            # 比對範圍：address + address_inferred + title
            # 多數 591 物件 address 只到路段，巷弄資訊在 address_inferred 裡（LVR/reverse 推測出來的）
            # 沒含 address_inferred 的話，用戶搜「永吉路278巷」會全部漏 match
            if r:
                _addr_blob = (
                    (data.get("address") or "")
                    + " " + (data.get("address_inferred") or "")
                    + " " + (data.get("title") or "")
                )
                if r not in _addr_blob:
                    continue
        if q:
            kw = q.strip().lower()
            blob = " ".join(str(data.get(k) or "") for k in ("address", "title", "district")).lower()
            if kw not in blob:
                continue
        data["id"] = doc_id
        data["_in_watchlist"] = doc_id in my_watchlist_ids
        if data["_in_watchlist"]:
            data = merge_watchlist_with_central(data, my_watchlist.get(doc_id, {}))
            data["id"] = doc_id
            data["_in_watchlist"] = True
        items.append(data)
    _tick("py_filter")
    # 與前端「新進優先」一致：scrape_session_at desc 為主、list_rank asc 為次
    items.sort(key=lambda x: (x.get("list_rank") if x.get("list_rank") is not None else 9999))
    items.sort(key=lambda x: x.get("scrape_session_at") or "", reverse=True)
    out_items = items[:limit]
    if slim:
        out_items = [_strip_for_list(it) for it in out_items]
    _tick("sort_strip")

    # Server-Timing header 把 server 端各 phase 耗時 surface 給前端 timer
    # 格式：name;dur=ms, name;dur=ms (W3C standard)
    # 順序：watchlist → firestore_query → py_filter → sort_strip
    _docs_n = _phase_t.pop("docs_n", 0)
    _was_cached = _phase_t.pop("cache_hit", 0)
    _cache_state = _phase_t.pop("cache_state", "miss")
    _st_parts = [f"{name};dur={ms:.0f}" for name, ms in _phase_t.items()]
    _cache_desc = "cache_HIT" if _cache_state == "fresh" else "cache_MISS"
    # auth time (Firebase token verify) 從 middleware 帶過來
    _auth_ms = getattr(request.state, "auth_ms", 0)
    _st_parts.append(f"auth;dur={_auth_ms:.0f}")
    # 注意：dur 一律放第一個 param —— frontend regex 認 `name;dur=N` 在開頭
    _st_parts.append(f"cache;dur=0;desc=\"{_cache_desc}\"")
    _st_parts.append(f"docs;dur=0;desc=\"n={_docs_n}\"")
    response.headers["Server-Timing"] = ", ".join(_st_parts)
    response.headers["Access-Control-Expose-Headers"] = "Server-Timing"

    # total_watchlist：用戶觀察清單真實總數（含被 dist/price 過濾掉的）—
    # 前端 badge 用這個顯示，避免「探索 tab 過濾掉的 watchlist 物件不被算到」的 race
    return {
        "total": len(items),
        "total_watchlist": len(my_watchlist_ids),
        "items": out_items,
    }


# ── 用戶 filter 偏好（探索 tab 條件持久化） ─────────────────────────────────
# 跨裝置同步：用戶在手機 / 桌面切換帳號用同樣的 filter。寫到
#   users/{uid}/preferences/filters
# 只儲存「key:value」形式 dict，不做 schema 驗證 (前端負責 normalize)
class FilterPrefsReq(BaseModel):
    prefs: dict


@app.get("/api/user/filter_prefs")
async def get_filter_prefs(user: dict = Depends(get_current_user)):
    """取出當前用戶的 filter 偏好。沒存過回 {prefs: {}}。"""
    uid = user["uid"]
    try:
        doc = (
            get_firestore().collection("users").document(uid)
            .collection("preferences").document("filters").get()
        )
        if doc.exists:
            return {"prefs": (doc.to_dict() or {}).get("prefs") or {}}
    except Exception as e:
        logger.warning(f"get_filter_prefs failed uid={uid}: {e}")
    return {"prefs": {}}


@app.post("/api/user/filter_prefs")
async def set_filter_prefs(body: FilterPrefsReq, user: dict = Depends(get_current_user)):
    """寫入當前用戶的 filter 偏好（完整覆蓋，不 merge）。
    payload size 限 8 KB（filter 偏好不該大）。"""
    uid = user["uid"]
    import json as _json
    payload = _json.dumps(body.prefs or {}, ensure_ascii=False)
    if len(payload.encode("utf-8")) > 8192:
        raise HTTPException(400, "filter prefs payload too large (>8KB)")
    try:
        get_firestore().collection("users").document(uid)\
            .collection("preferences").document("filters").set(
                {"prefs": body.prefs or {}, "updated_at": now_tw_iso()},
                merge=False,
            )
    except Exception as e:
        logger.warning(f"set_filter_prefs failed uid={uid}: {e}")
        raise HTTPException(500, "filter_prefs save failed")
    return {"status": "ok"}


class WatchlistAddReq(BaseModel):
    """加入觀察清單時可一併寫入 ephemeral override（用戶在搜尋 tab 曾改過數字）。"""
    desired_price_wan: Optional[float] = None
    floor_premium: Optional[float] = None
    bonus_weishau: Optional[float] = None
    bonus_dugen: Optional[float] = None
    rebuild_coeff: Optional[float] = None
    new_house_price_wan_override: Optional[float] = None
    road_width_m_override: Optional[float] = None
    zoning_ratios: Optional[list] = None


@app.post("/api/watchlist/{property_id:path}")
async def add_to_watchlist(property_id: str, body: Optional[WatchlistAddReq] = None,
                           user: dict = Depends(get_current_user)):
    """加入觀察清單（探索 tab 按 ★ 或 URL 送出命中中央快取時呼叫）。
    body 可選：把之前在搜尋 tab 改過但未持久化的 override 一起寫進 watchlist。"""
    _ensure_user_profile(user)
    uid = user["uid"]
    if not get_col().document(property_id).get().exists:
        raise HTTPException(status_code=404, detail="此物件不在中央 DB")
    data = {"added_at": now_tw_iso()}
    if body is not None:
        for k, v in body.dict(exclude_none=True).items():
            data[k] = v
    get_user_watchlist(uid).document(property_id).set(data, merge=True)
    return {"status": "ok"}


@app.delete("/api/watchlist/{property_id:path}")
async def remove_from_watchlist(property_id: str, user: dict = Depends(get_current_user)):
    """移出觀察清單（連同個人 overrides 一併刪除）。

    特殊：若該物件來源是 user_url（用戶貼網址）或 manual（用戶輸入地址）
    → 從中央 DB 也一併硬刪（因為這類物件只屬於送件人，不應該為他保留）。
    Batch / scheduler 抓進來的中央物件不刪 — 別人可能也要看。"""
    uid = user["uid"]
    get_user_watchlist(uid).document(property_id).delete()

    # manual：是 users/{uid}/manual/{id} 下的個人 doc，順手刪
    if property_id.startswith("manual_"):
        try:
            get_user_manual(uid).document(property_id).delete()
            logger.info(f"[delete] manual doc {property_id} hard-deleted (user={uid})")
        except Exception as e:
            logger.warning(f"manual delete failed {property_id}: {e}")
        return {"status": "ok"}

    # 中央物件：只有 source_origin=user_url 才硬刪
    try:
        doc_ref = get_col().document(property_id)
        snap = doc_ref.get()
        if snap.exists:
            data = snap.to_dict() or {}
            if data.get("source_origin") == "user_url" and data.get("submitted_by_uid") == uid:
                doc_ref.delete()
                logger.info(f"[delete] user_url doc {property_id} hard-deleted (送件人={uid})")
    except Exception as e:
        logger.warning(f"central doc delete check failed {property_id}: {e}")
    return {"status": "ok"}


@app.get("/api/properties/{property_id:path}")
def get_property(property_id: str, user: dict = Depends(get_current_user)):
    """
    取得單一物件（從使用者視角）：
      - manual_ 開頭 → users/{uid}/manual/{id}
      - 其他 → 中央 properties + users/{uid}/watchlist merge
    """
    result = _read_user_property(user, property_id)
    if result is None:
        raise HTTPException(status_code=404, detail="物件不存在")
    return result


# ── 觸發爬取 ──────────────────────────────────────────────────────────────────

from typing import Optional as _Opt

@app.post("/api/analyze/{property_id:path}")
async def analyze_pending(property_id: str, admin: dict = Depends(require_admin)):
    """
    對一個被跳過分析的物件 (analysis_status=pending)，
    手動觸發完整分析 pipeline（AI + zoning lookup + renewal 試算）。
    """
    col = get_col()
    doc = col.document(property_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="物件不存在")
    p = doc.to_dict()
    if p.get("analysis_status") == "done":
        return {"status": "already_done", "message": "已分析過"}
    # 立刻在 DB 標記分析中 → 前端（含 reload 後）能 render loading bar
    col.document(property_id).update({"analysis_in_progress": True})
    asyncio.create_task(_run_pending_analysis(property_id))
    return {"status": "started", "message": f"物件 {property_id} 分析已開始"}


async def _run_pending_analysis(property_id: str):
    """用共用 pipeline 跑完整分析（跟批次/URL 送出走同一條路）。"""
    col = get_col()
    doc_snap = col.document(property_id).get()
    if not doc_snap.exists:
        return
    p = doc_snap.to_dict()

    def _do():
        from api.analysis_pipeline import analyze_single_property
        # 用 DB 現有資料組 item
        item = dict(p)
        item["source_id"] = property_id
        initial_coords = None
        if p.get("latitude") and p.get("longitude"):
            initial_coords = (p["latitude"], p["longitude"])

        try:
            result = analyze_single_property(
                item=item,
                initial_coords=initial_coords,
                detail_text="",
            )
            doc_data = result["doc_data"]
            doc_data["analysis_status"] = "done"
            doc_data["analysis_in_progress"] = False
            col.document(property_id).set(_safe_doc(doc_data))
            logger.info(f"完成分析 {property_id}")
        except Exception as e:
            logger.exception(f"分析失敗 {property_id}: {e}")
            # 失敗也要清掉 in_progress 讓 UI 脫困
            col.document(property_id).update({"analysis_in_progress": False})
        finally:
            # invalidate central_search query cache — 跟 admin_reanalyze 同理
            invalidate_query_cache()

    await asyncio.to_thread(_do)


# ── 手動輸入地址送出分析 ──────────────────────────────────────────────────────

@app.post("/api/_debug/hide_legacy_manual")
def hide_legacy_manual(admin: dict = Depends(require_admin)):
    """一次把舊 id 格式（manual_YYYYMMDD_xxx）的手動 doc 軟刪除。Admin-only (2026-05-13 安全 audit)。"""
    col = get_col()
    import re as _re
    legacy_pat = _re.compile(r"^manual_\d{8}_")
    hidden = []
    for doc in col.get():
        if legacy_pat.match(doc.id):
            doc.reference.update({"deleted": True})
            hidden.append(doc.id)
    return {"status": "ok", "hidden_count": len(hidden), "hidden_ids": hidden}


@app.get("/api/_debug/lvr_probe")
def debug_lvr_probe(city: str, district: str, road_keyword: str, admin: dict = Depends(require_admin)):
    """
    直接掃 LVR SQLite 看指定 city/district 下，含 road_keyword 的所有紀錄。
    用來定位「明明網路上有 LVR、我們 DB 卻找不到」的問題。
    Admin-only (2026-05-13 安全 audit)。
    """
    from analysis.lvr_index import init_db as lvr_init
    from api.manual_analyze import normalize_address, _strip_section
    conn = lvr_init()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) FROM lvr WHERE city=? AND district=?
        """,
        (city, district),
    )
    district_count = cur.fetchone()[0]
    cur.execute(
        """
        SELECT address, area_ping, land_ping, building_type, txn_date
        FROM lvr
        WHERE city=? AND district=? AND address LIKE ?
        ORDER BY txn_date DESC
        LIMIT 50
        """,
        (city, district, f"%{road_keyword}%"),
    )
    rows = cur.fetchall()
    conn.close()
    return {
        "city": city,
        "district": district,
        "district_total_rows": district_count,
        "matching_road_keyword": road_keyword,
        "matches": [
            {
                "address_raw": r[0],
                "address_normalized": normalize_address(r[0]),
                "address_loose": _strip_section(normalize_address(r[0])),
                "area_ping": r[1],
                "land_ping": r[2],
                "building_type": r[3],
                "txn_date": r[4],
            }
            for r in rows
        ],
    }


@app.get("/api/_debug/manual_docs")
def debug_manual_docs(admin: dict = Depends(require_admin)):
    """直接列出 Firestore 所有 source_id 以 manual_ 開頭的 doc。繞過所有 server-side filter。
    Admin-only (2026-05-13 安全 audit — 之前 logged-in user 可拿到其他用戶 manual 物件、跨用戶外洩)。"""
    col = get_col()
    docs = list(col.get())
    out = []
    for doc in docs:
        if doc.id.startswith("manual_"):
            d = doc.to_dict() or {}
            out.append({
                "id": doc.id,
                "address": d.get("address"),
                "city": d.get("city"),
                "district": d.get("district"),
                "analysis_status": d.get("analysis_status"),
                "analysis_in_progress": d.get("analysis_in_progress"),
                "deleted": d.get("deleted"),
                "scrape_session_at": d.get("scrape_session_at"),
                "sources": d.get("sources"),
                "analysis_error": d.get("analysis_error"),
            })
    return {
        "total_docs": len(docs),
        "manual_count": len(out),
        "manuals": out,
    }


class ManualReanalyzeReq(BaseModel):
    """重新分析時前端可選帶的覆寫值（歧義對話框選了候選戶後傳建坪/地坪過來）。"""
    building_area_ping: Optional[float] = None
    land_area_ping: Optional[float] = None


@app.post("/api/manual/{property_id:path}/reanalyze")
async def reanalyze_manual(
    property_id: str,
    req: Optional[ManualReanalyzeReq] = None,
    user: dict = Depends(get_current_user),
):
    """重跑 manual 物件的完整 pipeline。
    跟 /api/manual_analyze (新建) 走**同一條 validate 路線** — 任何形式 reanalyze 都會經過
    LVR ambiguity / lvr_mismatch / district_mismatch / not_found 檢查，不再黑箱直接吞舊壞值。
    """
    if not property_id.startswith("manual_"):
        raise HTTPException(status_code=400, detail="只能重分析 manual 物件")
    uid = user["uid"]
    manual_col = get_user_manual(uid)
    doc = manual_col.document(property_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="物件不存在")
    old = doc.to_dict() or {}

    # 用戶若是從歧義對話框選了候選戶 → req 帶 override；否則用 OLD doc 的值當輸入
    bld = (req.building_area_ping if req else None) or old.get("building_area_ping")
    land = (req.land_area_ping if req else None) or old.get("land_area_ping")
    price_wan = (old.get("price_ntd") / 10000) if old.get("price_ntd") else None

    # 同一條 validate（跟新建 manual 共用）— 攔截歧義、地址不存在、城區不符等情況
    from api.manual_analyze import validate_manual_input
    v = validate_manual_input(
        city=old.get("city"),
        district=old.get("district"),
        address=old.get("address"),
        building_area_ping=bld,
        land_area_ping=land,
        price_wan=price_wan,
        use_source="auto",
    )
    if v["status"] != "ok":
        # 帶上 mode + property_id，讓前端能用 reanalyze 端口而非 new submit 重送
        out = dict(v)
        out["mode"] = "reanalyze"
        out["property_id"] = property_id
        return out

    # validate 通過 → 用 normalized item 跑 pipeline（保留 OLD doc 的 source_id）
    item = dict(v["item"])
    item["source_id"] = property_id
    item.setdefault("source", "manual")
    item.setdefault("title", old.get("title") or item.get("address"))
    # OLD doc 上有但 validate 沒回來的欄位（building_age 等）可保留
    for _k in ("building_age", "building_type", "total_floors", "floor"):
        if not item.get(_k) and old.get(_k):
            item[_k] = old[_k]

    manual_col.document(property_id).update({"analysis_in_progress": True})
    from api.routers.admin_scrape import _run_manual_analysis
    asyncio.create_task(_run_manual_analysis(uid, property_id, item))
    logger.info(f"[manual reanalyze] uid={uid} src_id={property_id} (after validate)")
    return {"status": "started", "source_id": property_id}


@app.post("/api/properties/{property_id:path}/reanalyze")
async def reanalyze_recommendation(property_id: str):
    """條件變動後重新產生分析建議（不重跑 AI 文字分析，只重算結構化建議）。"""
    col = get_col()
    doc = col.document(property_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="物件不存在")
    p = doc.to_dict()

    def _do():
        from analysis.claude_analyzer import generate_final_recommendation

        text_analysis = {
            "key_strengths": [],
            "key_risks": [],
            "renewal_path": p.get("ai_recommendation"),
            "summary": p.get("ai_analysis") or "",
        }
        # 即時算 renewal v2（不從 DB 讀，因為 DB 不存 — CLAUDE.md 規則 8）
        from analysis.scorer import calculate_renewal_scenarios as _calc_rv2, resolve_effective_zoning as _rez
        _rv2 = _calc_rv2(
            land_area_ping=p.get("land_area_ping"),
            zoning=_rez(p.get("zoning"), p.get("zoning_original")),
            district=p.get("district"),
            price_ntd=p.get("price_ntd"),
            road_width_m=p.get("road_width_m_override") or p.get("road_width_m"),
            lat=p.get("latitude") or p.get("source_latitude"),
            lng=p.get("longitude") or p.get("source_longitude"),
            zoning_list=p.get("zoning_list"),
            zoning_ratios=p.get("zoning_ratios"),
            floor=p.get("floor"),
            floor_range_min=p.get("floor_range_min"),
            floor_premium=p.get("floor_premium"),
            building_area_ping=p.get("building_area_ping"),
        )
        final = generate_final_recommendation(
            property_data=p,
            score={},
            renewal_calc={"v2": _rv2},
            text_analysis=text_analysis,
        )
        col.document(property_id).update({
            "ai_recommendation": final["recommendation"],
            "ai_reason": final["reason"],
        })
        return {
            "ai_recommendation": final["recommendation"],
            "ai_reason": final["reason"],
        }

    return await asyncio.to_thread(_do)


@app.post("/api/properties/{property_id:path}/refresh_redev_cases")
async def refresh_redev_cases_endpoint(property_id: str, user: dict = Depends(get_current_user)):
    """重新查詢該物件位置上的都更案件 (auto-enrich 單欄位 refresh)。
    輕量 — 不重跑 Vision / Claude / zoning lookup，只打 NtpcURInfo / Taipei GeoServer 一次。
    回傳更新後的 redev_cases list。"""
    col = get_col()
    doc = col.document(property_id).get()
    if not doc.exists:
        raise HTTPException(404, "物件不存在")
    p = doc.to_dict() or {}
    lat = p.get("latitude")
    lng = p.get("longitude")
    city = p.get("city")
    if not lat or not lng:
        raise HTTPException(400, "物件缺座標，無法查詢都更案件")

    def _do():
        from api.gis_overlay import query_tpe_renewal_cases, query_ntpc_renewal_cases
        if city == "台北市":
            cases = query_tpe_renewal_cases(lat, lng)
        elif city == "新北市":
            cases = query_ntpc_renewal_cases(lat, lng)
        else:
            cases = []
        col.document(property_id).update({"redev_cases": cases})
        return cases

    cases = await asyncio.to_thread(_do)
    invalidate_query_cache()
    return {"status": "ok", "redev_cases": cases}


@app.post("/api/properties/{property_id:path}/scan_road_width")
async def scan_road_width(property_id: str):
    """
    精確掃描臨路寬度：Playwright 開 zonemap 截圖 + Vision 判斷建物面對哪條路。
    """
    col = get_col()
    doc = col.document(property_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="物件不存在")
    p = doc.to_dict()
    if p.get("city") != "台北市":
        return {"error": "目前僅支援台北市"}

    def _do():
        from analysis.geocoder import geocode_address
        from analysis.gov_gis import query_road_width_taipei
        from scraper.browser_manager import get_browser_context
        import json as _json

        # address 已是純地址（無 city/district 前綴），geocode 前拼回
        _pure = p.get("address_inferred") or p.get("address") or ""
        best_addr = f"{p.get('city','')}{p.get('district','')}{_pure}" if _pure else ""
        coord = None
        if "號" in _pure:
            coord = geocode_address(best_addr)
        if not coord:
            coord = (p.get("latitude"), p.get("longitude"))
        if not coord or not coord[0]:
            return {"error": "缺座標"}
        lat, lng = coord

        # GeoServer 查附近路寬（作為參考）
        rw = query_road_width_taipei(lat, lng, address_hint=best_addr)
        all_roads = rw.get("all_roads", []) if rw else []

        # Playwright 開 zonemap 截圖
        screenshot_path = BASE_DIR / "data" / "screenshots" / f"{property_id}_roadwidth.png"
        try:
            import re as _re3
            addr_parsed = {}
            m = _re3.search(r"([\u4e00-\u9fa5]+(?:路|街|大道)(?:[一二三四五六七八九十]段)?)", best_addr)
            if m:
                addr_parsed["road"] = m.group(1)
            m = _re3.search(r"(\d+)巷", best_addr)
            if m:
                addr_parsed["lane"] = m.group(1)
            m = _re3.search(r"(\d+)弄", best_addr)
            if m:
                addr_parsed["alley"] = m.group(1)
            m = _re3.search(r"(\d+)號", best_addr)
            if m:
                addr_parsed["number"] = m.group(1)
            m = _re3.search(r"([\u4e00-\u9fa5]{2,3}區)", best_addr)
            addr_district = m.group(1) if m else (p.get("district") or "")

            with get_browser_context(headless=True) as ctx:
                page = ctx.new_page()
                page.set_viewport_size({"width": 1920, "height": 1080})
                page.goto(
                    "https://zonemap.udd.gov.taipei/ZoneMapOP/indexZoneMap_op.aspx",
                    wait_until="networkidle", timeout=60000,
                )
                import time
                time.sleep(6)
                # 開側欄 → 開圖層 → 門牌搜尋
                page.click(".fa-bars", timeout=5000)
                time.sleep(1)
                page.evaluate(r"""() => {
                    const cbs = document.querySelectorAll('.sliderbut input[type=checkbox]');
                    [2, 3, 4].forEach(i => {
                        if (cbs[i] && !cbs[i].checked) {
                            cbs[i].checked = true;
                            cbs[i].dispatchEvent(new Event('change', {bubbles: true}));
                        }
                    });
                }""")
                time.sleep(1)
                # 門牌搜尋（有地址時用搜尋定位+標記地號）
                if addr_parsed.get("road") and addr_parsed.get("number"):
                    page.click('a[href="#sidebarSearch"]', timeout=5000)
                    time.sleep(1)
                    page.select_option("#OtherQMemu", value="tqM6")
                    time.sleep(1)
                    _dist = addr_district
                    _road = addr_parsed.get("road", "")
                    _lane = addr_parsed.get("lane", "")
                    _alley = addr_parsed.get("alley", "")
                    _num = addr_parsed.get("number", "")
                    page.evaluate(f"""() => {{
                        const panel = document.querySelector('#tqM6');
                        if (!panel) return;
                        const sel = panel.querySelector('select');
                        for (const o of sel.options) {{
                            if (o.text.includes('{_dist}')) {{ sel.value = o.value; sel.dispatchEvent(new Event('change')); break; }}
                        }}
                        const groups = panel.querySelectorAll('.form-group');
                        groups.forEach(g => {{
                            const label = (g.querySelector('label')?.innerText || '').trim();
                            const inp = g.querySelector('input');
                            if (!inp) return;
                            if (label.startsWith('道路')) {{ inp.value = '{_road}'; inp.dispatchEvent(new Event('input')); }}
                            else if (label === '巷') {{ inp.value = '{_lane}'; inp.dispatchEvent(new Event('input')); }}
                            else if (label === '弄') {{ inp.value = '{_alley}'; inp.dispatchEvent(new Event('input')); }}
                            else if (label.startsWith('號')) {{ inp.value = '{_num}'; inp.dispatchEvent(new Event('input')); }}
                        }});
                        const btn = panel.querySelector('.btn-danger');
                        if (btn) btn.click();
                    }}""")
                    time.sleep(5)
                # 關側欄
                page.click(".fa-bars", timeout=5000)
                time.sleep(2)
                # 如果門牌搜尋沒定位，fallback 座標定位
                if not (addr_parsed.get("road") and addr_parsed.get("number")):
                    page.evaluate(f"""() => {{
                        const view = window.map.getView();
                        view.setCenter([{lng}, {lat}]);
                        view.setZoom(20);
                    }}""")
                    time.sleep(5)
                page.screenshot(path=str(screenshot_path), full_page=False)
                page.close()
        except Exception as e:
            logger.warning(f"zonemap 截圖失敗: {e}")
            if rw:
                col.document(property_id).update({
                    "road_width_m": rw["road_width_m"],
                    "road_width_name": rw["road_name"],
                    "road_width_all": all_roads[:5],
                })
                return {"road_width_m": rw["road_width_m"], "road_name": rw["road_name"], "all_roads": all_roads[:5]}
            return {"error": "zonemap 截圖失敗"}

        # Vision 判斷
        roads_ref = ", ".join(f"{r['road_name']} {r['road_width_m']}m(距{r['distance_m']}m)" for r in all_roads[:6])
        vision_prompt = f"""這是台北市都市計畫地圖的截圖，中心點是一棟建物，地址約為「{best_addr}」。

請判斷這棟建物（地圖中心位置）面對的是哪條路，以及那條路的計畫道路寬度。

地圖上的道路寬度標示通常寫在路的旁邊或路中（例如「8M」「11M」「18M」）。
地籍線（細線）圍出的區塊是建物用地，道路是地籍線之間的空白區域。

GeoServer 查到附近的道路供參考：{roads_ref}

請回傳 JSON，不要其他文字：
{{"road_name": "建物面對的路名", "road_width_m": <數字>, "reason": "判斷理由（一句話）"}}"""

        try:
            from analysis.claude_analyzer import _encode_image, client, MODEL_VISION
            import re as _re2
            img_b64, media_type = _encode_image(str(screenshot_path))
            if not img_b64:
                raise RuntimeError("截圖編碼失敗")
            resp = client.messages.create(
                model=MODEL_VISION, max_tokens=400,
                messages=[{"role": "user", "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": img_b64}},
                    {"type": "text", "text": vision_prompt},
                ]}],
            )
            vision_text = resp.content[0].text.strip()
            logger.info(f"Vision road_width ({property_id}): {vision_text[:300]}")
            m_json = _re2.search(r"\{.*\}", vision_text, _re2.DOTALL)
            vision_result = m_json.group(0) if m_json else None
            if vision_result:
                parsed = _json.loads(vision_result)
                road_name = parsed.get("road_name", "")
                road_width = parsed.get("road_width_m")
                reason = parsed.get("reason", "")
                if road_name and road_width:
                    col.document(property_id).update({
                        "road_width_m": float(road_width),
                        "road_width_name": road_name,
                        "road_width_all": all_roads[:5],
                        "screenshot_roadwidth": f"/data/screenshots/{property_id}_roadwidth.png",
                        "road_width_vision_reason": reason,
                    })
                    return {
                        "road_width_m": float(road_width),
                        "road_name": road_name,
                        "reason": reason,
                        "all_roads": all_roads[:5],
                        "screenshot": f"/data/screenshots/{property_id}_roadwidth.png",
                    }
        except Exception as e:
            logger.warning(f"Vision 判斷路寬失敗: {e}")

        # Vision 失敗 → fallback GeoServer
        if rw:
            col.document(property_id).update({
                "road_width_m": rw["road_width_m"],
                "road_width_name": rw["road_name"],
                "road_width_all": all_roads[:5],
                "screenshot_roadwidth": f"/data/screenshots/{property_id}_roadwidth.png",
            })
            return {"road_width_m": rw["road_width_m"], "road_name": rw["road_name"], "all_roads": all_roads[:5]}
        return {"error": "判斷失敗"}

    return await asyncio.to_thread(_do)


@app.get("/api/busy_state")
def busy_state():
    return {
        "batch_running": _scrape_running,
        "url_inflight": _url_inflight,
        "url_waiting": _url_waiting,
        "url_slots": MAX_URL_CONCURRENCY,
        # 向下相容舊前端欄位
        "scrape_running": _scrape_running,
        "url_running": _url_inflight > 0,
    }


@app.post("/api/cancel")
async def cancel_task(admin: dict = Depends(require_admin)):
    global _cancel_requested
    _cancel_requested = True
    return {"status": "ok"}


def _scrape_single_url_yongqing(url: str, src_id: str, is_reanalyze: bool = False, *, mark_user_url: bool = True):
    """單筆永慶 URL 分析。比 591 簡單很多：純 HTTP + Playwright（拿座標）+ pipeline。"""
    from scraper.scraper_yongqing import scrape_yongqing_single
    from scraper.browser_manager import get_browser_context
    from api.analysis_pipeline import analyze_single_property
    from database.models import merge_property_doc
    from database.db import find_doc_by_source_id, gen_dated_id

    item = scrape_yongqing_single(url)
    if not item:
        return {"status": "error", "message": "永慶詳情頁解析失敗（可能下架或頁面結構變了）"}

    # 樓高 > 5 → 非公寓，不分析（fallback 用 floor 避免 total_floors=None 漏過濾）
    _tf = item.get("total_floors") or 0
    try: _f = int(item.get("floor")) if item.get("floor") else 0
    except Exception: _f = 0
    eff = max(_tf, _f)
    if eff > 5:
        try:
            from database.retry_queue import dequeue_by_source_id
            dequeue_by_source_id(src_id)
        except Exception: pass
        return {"status": "skipped_non_apartment", "source_id": src_id,
                "message": f"樓層 {item.get('floor')}/{item.get('total_floors')} > 5，非公寓，跳過分析"}
    # ≤5F 且非透天 → 一律標公寓（HOUSELANDTYPE 雜類在源頭就清掉）
    if item.get("building_type") not in ("透天", "店面"):
        item["building_type"] = "公寓"

    item["scrape_session_at"] = now_tw_iso()
    item["list_rank"] = 0

    # 跑 analysis pipeline（共用 591 的）
    with get_browser_context(headless=True) as ctx:
        initial_coords = (item.get("latitude"), item.get("longitude")) if item.get("latitude") else None
        result = analyze_single_property(
            item=item,
            ocr_ctx=ctx,
            initial_coords=initial_coords,
            detail_text="",
        )
    doc = result["doc_data"]

    # 找既有 doc（用 source_id 欄位）
    existing_doc_id, old = find_doc_by_source_id(src_id)
    col = get_col()

    from database.run_log import log_action as _la, build_doc_log_details as _bld
    _trig = "manual_reanalyze" if is_reanalyze else ("manual_url" if mark_user_url else "manual_url")
    if existing_doc_id:
        if is_reanalyze:
            # 保留排序位置欄位 + user_url 標記欄位（避免 reanalyze 把私人物件「升級」成中央物件）
            for _keep in ("scrape_session_at", "list_rank", "scraped_at",
                          "source_origin", "submitted_by_uid", "submitted_by_email", "added_at_user"):
                if old.get(_keep) is not None:
                    doc[_keep] = old.get(_keep)
            doc["id"] = existing_doc_id
            col.document(existing_doc_id).set(_safe_doc(doc))
            try: _la(_trig, "reanalyze", source_id=src_id, doc_id=existing_doc_id,
                    message="永慶物件重新分析完成（完整替換）",
                    details=_bld({"source": "永慶", "url": url, "title": item.get("title")}, doc))
            except Exception: pass
            return {"status": "ok", "source_id": src_id, "id": existing_doc_id, "message": "永慶物件重新分析完成（完整替換）"}
        merged, conflicts = merge_property_doc(old, doc)
        merged["id"] = existing_doc_id
        if merged.get("archived") is True:
            merged["archived"] = False
        col.document(existing_doc_id).set(_safe_doc(merged))
        msg = "永慶物件已存在中央 DB，已合併"
        if conflicts:
            msg += f"（衝突保留舊值：{', '.join(conflicts)}）"
        try: _la(_trig, "enrich", source_id=src_id, doc_id=existing_doc_id,
                message=msg,
                details=_bld({"source": "永慶", "url": url, "title": item.get("title")}, merged, conflicts=conflicts))
        except Exception: pass
        return {"status": "ok", "source_id": src_id, "id": existing_doc_id, "message": msg}

    # 新物件
    new_doc_id = doc.get("id") or gen_dated_id()
    doc["id"] = new_doc_id
    if not is_reanalyze and mark_user_url:
        doc["source_origin"] = "user_url"
    col.document(new_doc_id).set(_safe_doc(doc))
    try: _la(_trig, "new", source_id=src_id, doc_id=new_doc_id,
            message="永慶物件分析完成（新增）",
            details=_bld({"source": "永慶", "url": url, "title": item.get("title")}, doc))
    except Exception: pass
    # post-write cross-source recheck — 防 dedup 因 snapshot/race 漏 catch（dedup audit 25 對 case）
    try:
        from database.db import recheck_and_archive_if_cross_dup as _recheck
        _recheck(new_doc_id, trigger_label=_trig)
    except Exception: pass
    return {"status": "ok", "source_id": src_id, "id": new_doc_id, "message": "永慶物件分析完成（新增）"}


def _scrape_single_url_sinyi(url: str, src_id: str, is_reanalyze: bool = False, *, mark_user_url: bool = True):
    """單筆信義 URL 分析。
    信義列表頁 SSR 已含完整資料（座標+價格+地址+建坪+地坪），
    所以走輕量 path：scrape_sinyi_single（從 detail 頁的 NEXT_DATA 找該物件）→ pipeline。
    若 detail 找不到該物件，會從相關物件 fallback。"""
    from scraper.scraper_sinyi import scrape_sinyi_single
    from scraper.browser_manager import get_browser_context
    from api.analysis_pipeline import analyze_single_property
    from database.models import merge_property_doc
    from database.db import find_doc_by_source_id, gen_dated_id

    item = scrape_sinyi_single(url)
    if not item:
        return {"status": "skipped_non_apartment", "source_id": src_id,
                "message": "此物件為預售屋或非分析對象，已跳過"}
    if not item.get("price_ntd") or not item.get("address"):
        return {"status": "error",
                "message": "信義詳情頁解析失敗（contentData 不完整，可能已下架）"}

    # 樓高 > 5 → 非公寓（fallback 用 floor，避免 totalfloor=None 漏過濾）
    _tf = item.get("total_floors") or 0
    try: _f = int(item.get("floor")) if item.get("floor") else 0
    except Exception: _f = 0
    eff = max(_tf, _f)
    if eff > 5:
        try:
            from database.retry_queue import dequeue_by_source_id
            dequeue_by_source_id(src_id)
        except Exception: pass
        return {"status": "skipped_non_apartment", "source_id": src_id,
                "message": f"樓層 {item.get('floor')}/{item.get('total_floors')} > 5，非公寓，跳過分析"}

    item["scrape_session_at"] = now_tw_iso()
    item["list_rank"] = 0

    with get_browser_context(headless=True) as ctx:
        initial_coords = (item.get("latitude"), item.get("longitude")) if item.get("latitude") else None
        result = analyze_single_property(
            item=item,
            ocr_ctx=ctx,
            initial_coords=initial_coords,
            detail_text="",
        )
    doc = result["doc_data"]

    existing_doc_id, old = find_doc_by_source_id(src_id)
    col = get_col()

    from database.run_log import log_action as _la, build_doc_log_details as _bld
    _trig = "manual_reanalyze" if is_reanalyze else "manual_url"
    if existing_doc_id:
        if is_reanalyze:
            # 保留排序位置欄位 + user_url 標記欄位（避免 reanalyze 把私人物件「升級」成中央物件）
            for _keep in ("scrape_session_at", "list_rank", "scraped_at",
                          "source_origin", "submitted_by_uid", "submitted_by_email", "added_at_user"):
                if old.get(_keep) is not None:
                    doc[_keep] = old.get(_keep)
            doc["id"] = existing_doc_id
            col.document(existing_doc_id).set(_safe_doc(doc))
            try: _la(_trig, "reanalyze", source_id=src_id, doc_id=existing_doc_id,
                    message="信義物件重新分析完成（完整替換）",
                    details=_bld({"source": "信義", "url": url, "title": item.get("title")}, doc))
            except Exception: pass
            return {"status": "ok", "source_id": src_id, "id": existing_doc_id, "message": "信義物件重新分析完成（完整替換）"}
        merged, conflicts = merge_property_doc(old, doc)
        merged["id"] = existing_doc_id
        if merged.get("archived") is True:
            merged["archived"] = False
        col.document(existing_doc_id).set(_safe_doc(merged))
        msg = "信義物件已存在中央 DB，已合併"
        if conflicts:
            msg += f"（衝突保留舊值：{', '.join(conflicts)}）"
        try: _la(_trig, "enrich", source_id=src_id, doc_id=existing_doc_id,
                message=msg,
                details=_bld({"source": "信義", "url": url, "title": item.get("title")}, merged, conflicts=conflicts))
        except Exception: pass
        return {"status": "ok", "source_id": src_id, "id": existing_doc_id, "message": msg}

    new_doc_id = doc.get("id") or gen_dated_id()
    doc["id"] = new_doc_id
    if not is_reanalyze and mark_user_url:
        doc["source_origin"] = "user_url"
    col.document(new_doc_id).set(_safe_doc(doc))
    try: _la(_trig, "new", source_id=src_id, doc_id=new_doc_id,
            message="信義物件分析完成（新增）",
            details=_bld({"source": "信義", "url": url, "title": item.get("title")}, doc))
    except Exception: pass
    # post-write cross-source recheck — 防 dedup 因 snapshot/race 漏 catch
    try:
        from database.db import recheck_and_archive_if_cross_dup as _recheck
        _recheck(new_doc_id, trigger_label=_trig)
    except Exception: pass
    return {"status": "ok", "source_id": src_id, "id": new_doc_id, "message": "信義物件分析完成（新增）"}


def _scrape_single_url_591_inner(url: str, src_id: str, is_reanalyze: bool = False, *, mark_user_url: bool = True):
    """591 詳情頁分析 body — 由 _scrape_single_url 外層包 try/finally 確保 cleanup。
    本函式內部仍保留多個 inline cleanup（idempotent，重複呼叫無害），用於 early-return path
    讓 cleanup 點離 return 近、語意清楚。"""
    from scraper.browser_manager import get_browser_context
    from scraper.scraper_591 import screenshot_detail_page
    from analysis.claude_analyzer import (
        extract_full_detail_from_screenshot,
    )

    col = get_col()
    from api.analysis_pipeline import _cleanup_ephemeral_screenshots as _cleanup_shots

    # ────────────────────────────────────────────────────────────────────────
    # 591 Mobile API fast path：mobile API 涵蓋詳情頁 100% 欄位（title / remark /
    # community / posttime / lat/lng / 建坪/土地/屋齡/樓層/價格 等），完全省 Playwright +
    # Vision OCR（單筆 ~100s → ~5s）。失敗才走 Playwright fallback。
    # ────────────────────────────────────────────────────────────────────────
    _mobile_data_url = None
    try:
        from config import USE_591_MOBILE_API as _USE_MOBILE_URL
    except ImportError:
        _USE_MOBILE_URL = True
    if _USE_MOBILE_URL:
        try:
            from scraper.scraper_591_mobile import fetch_mobile_detail
            _hid_url = src_id.split("591_", 1)[-1] if src_id.startswith("591_") else src_id
            _mobile_data_url = fetch_mobile_detail(_hid_url)
            if _mobile_data_url:
                _mobile_data_url.pop("_mobile_raw", None)
                logger.info(f"  ⚡ 591 mobile API 抓到 detail，跳過 Playwright ({src_id})")
        except Exception as _me:
            logger.warning(f"  591 mobile API 例外，fallback Playwright ({src_id}): {_me}")
            _mobile_data_url = None

    # 共用 contextmanager：mobile 成功就不開 Chromium；失敗才開
    import contextlib
    @contextlib.contextmanager
    def _maybe_browser(need: bool):
        if not need:
            yield None
            return
        with get_browser_context(headless=True) as _ctx:
            yield _ctx

    with _maybe_browser(not _mobile_data_url) as ctx:
        # 兩條路會填的變數
        data = None              # docTitle/title/bodyText/image_url/community_address/page_lat/page_lng
        vision = {}              # building/land/age/floor/price_wan
        detail_ret = None        # SimpleNamespace-ish (addr_path / house_path / community_raw)
        shot = None
        _community_addr_from_screenshot = ""
        _page_coords = None
        _house_crop_single = None
        published_text = None
        updated_text = None

        if _mobile_data_url:
            # ─── Mobile fast path：純 HTTP，無 Playwright ───
            m = _mobile_data_url
            # body_text 給法拍偵測 + city/district fallback；
            # **city+district 必須放最前面**，避免下游 regex `[一-龥]{2,3}區`
            # 抓到 title/remark 廣告詞的「實踐學區」「忠孝商圈」之類當成 district
            _body_text = ((m.get("city") or "") + (m.get("district") or "") + "\n"
                          + (m.get("title") or "") + "\n"
                          + (m.get("remark") or ""))
            data = {
                "docTitle": m.get("title") or "",
                "title": m.get("title") or "",
                "bodyText": _body_text,
                # 用高解析主圖 (photos[0] = !1000x.water2.jpg)，不用 thumbnail (190x150 放大會糊)
                "image_url": (m.get("photos") or [m.get("thumbnail_url") or ""])[0],
                "community_address": m.get("community_address") or "",
                "page_lat": m.get("source_latitude"),
                "page_lng": m.get("source_longitude"),
            }
            vision = {
                "building_area_ping": m.get("building_area_ping"),
                "land_area_ping": m.get("land_area_ping"),
                "building_age": m.get("building_age"),
                "floor": m.get("floor"),
                # mobile parser 從 floor 字串 ('7F/9F') 拆出 total_floors int — 給下游
                # floor>=6 skip check 用 (沒 total_floors 時 check 拿到 0 失效)
                "total_floors": m.get("total_floors"),
                # mobile API address：可能完整 (含號) 或 hide_addr_detail=1 case 只到「街X段」
                "address": m.get("address") or "",
            }
            if m.get("price_ntd"):
                vision["price_wan"] = m["price_ntd"] // 10000
            published_text = m.get("published_at")
            updated_text = m.get("updated_at_591")
            _page_coords = (m.get("source_latitude"), m.get("source_longitude"))
            # 模擬 detail_ret 接口（下游 getattr(detail_ret, "addr_path", None) 等使用）
            from types import SimpleNamespace
            # community_raw 對應 desktop 詳情頁「社區」label RAW value（仲介寫「【店長推薦】」
            # 是法拍特徵），不是 title! mobile API community 欄位是屋主填的社區名，等價較弱但安全
            detail_ret = SimpleNamespace(
                published_text=published_text,
                updated_text=updated_text,
                addr_path=None,
                house_path=None,
                community_raw=(m.get("community") or ""),
            )
            # 591 不存在 / 已下架：fetch_mobile_detail status≠1 已 return None → 進這裡的就還在線
        else:
            # ─── Fallback：Playwright + Vision OCR（mobile API 限流 / 改 schema 走這條）───
            page = ctx.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(2500)
                # 從詳情頁抓基本欄位
                data = page.evaluate(r"""() => {
                  const text = (sel) => {
                    const e = document.querySelector(sel);
                    return e ? (e.innerText || '').trim() : '';
                  };
                  // 抓首圖：找物件主圖（class=img_main）或相簿區
                  let imgUrl = '';
                  const imgs = document.querySelectorAll(
                    '.img_main, .swiper-slide img, [class*="photo"] img, [class*="album"] img, [class*="gallery"] img'
                  );
                  for (const i of imgs) {
                    const el = i.tagName === 'IMG' ? i : i.querySelector('img') || i;
                    const src = el.getAttribute('data-src') || el.getAttribute('data-original') || el.getAttribute('src') || '';
                    if (!src || src.startsWith('data:')) continue;
                    if (/\/build\/static\/|\/header\/|\/icon|\/newload/i.test(src)) continue;
                    if (!/\.(jpg|jpeg|png|webp)/i.test(src)) continue;
                    imgUrl = src.startsWith('//') ? 'https:' + src : src;
                    break;
                  }
                  // 社區欄位的地址（純文字，不受 CSS 位移防爬影響）
                  let communityAddr = '';
                  const addrEls = document.querySelectorAll('.info-addr-value');
                  for (const el of addrEls) {
                    const t = (el.innerText || '').trim();
                    if (t && /\d+號/.test(t) && /路|街|大道|巷|弄/.test(t)) { communityAddr = t; break; }
                  }
                  // 591 原生座標（從地圖 iframe URL 抓）
                  let pageLat = null, pageLng = null;
                  const scripts = document.querySelectorAll('script');
                  for (const s of scripts) {
                    const t = s.textContent || '';
                    const m = t.match(/rsMapIframe\?lat=([\d.]+)&lng=([\d.]+)/);
                    if (m) { pageLat = parseFloat(m[1]); pageLng = parseFloat(m[2]); break; }
                  }
                  let pageTitle = (document.title || '').replace(/\s*[-|]\s*591.*$/,'').trim();
                  if (!pageTitle || /591/.test(pageTitle) || /不存在/.test(pageTitle)) {
                    const h1 = document.querySelector('h1.detail-title, h1.info-title, h1');
                    pageTitle = h1 ? (h1.innerText || '').trim() : '';
                  }
                  if (['所有物件','地圖找地','地圖查實價'].includes(pageTitle)) pageTitle = '';
                  return {
                    docTitle: document.title || '',
                    title: pageTitle,
                    bodyText: document.body.innerText.slice(0, 6000),
                    image_url: imgUrl,
                    community_address: communityAddr,
                    page_lat: pageLat,
                    page_lng: pageLng,
                  };
                }""")
            finally:
                page.close()

            # 591 錯誤頁偵測：物件下架/刪除時會回 "對不起，您訪問的頁面不存在"
            _dtitle = (data.get("docTitle") or "")
            _body_head = (data.get("bodyText") or "")[:300]
            if ("不存在" in _dtitle) or ("您查詢的物件不存在" in _body_head) or ("已關閉或者被刪除" in _body_head):
                logger.warning(f"591 物件已下架 {src_id}: {_dtitle!r} → 從 DB 移除")
                try:
                    col.document(src_id).delete()
                except Exception as _de:
                    logger.warning(f"移除下架物件失敗 {src_id}: {_de}")
                return {
                    "status": "removed",
                    "message": f"591 物件已下架/刪除（{src_id}），已自動從中央 DB 移除。",
                    "removed": True,
                }

            # 用全頁截圖 + 完整 Vision OCR 抓所有詳情頁欄位（591 防爬，regex 無效）
            detail_ret = screenshot_detail_page(ctx, url, src_id)
            shot, _community_addr_from_screenshot, _page_coords = detail_ret[:3]
            published_text = getattr(detail_ret, "published_text", None)
            updated_text = getattr(detail_ret, "updated_text", None)
            _house_crop_single = getattr(detail_ret, "house_path", None)

            # shot + house_crop 平行 OCR 然後合併，house_crop 補漏
            from concurrent.futures import ThreadPoolExecutor as _TPE_URL
            _paths_u = [p for p in (shot, _house_crop_single) if p]
            if _paths_u:
                with _TPE_URL(max_workers=len(_paths_u)) as _ex:
                    _results_u = list(_ex.map(extract_full_detail_from_screenshot, _paths_u))
                vision = _results_u[0] if _results_u else {}
                for _r in _results_u[1:]:
                    for k, v in (_r or {}).items():
                        if v not in (None, "", 0) and vision.get(k) in (None, "", 0):
                            vision[k] = v

        # 若 screenshot_detail_page 的進階 DOM selector 抓到更完整地址，覆蓋簡陋的 inline 結果
        # looks_like_real_address 擋廣告詞（屋主自填「近XX1號出口」這類無路名字串）
        from database.models import looks_like_real_address as _lkra_reanalyze
        if _lkra_reanalyze(_community_addr_from_screenshot, require_number=True):
            if not data.get("community_address") or not _lkra_reanalyze(data.get("community_address"), require_number=True):
                data["community_address"] = _community_addr_from_screenshot

        # DOM 完全抓不到地址（591 用 <wc-ir-obfuscate-address-1> 防爬）→ 走窄裁切 OCR consensus
        # 這裡必須在 city/district 判斷之前先抓城市/行政區（從 body 或卡片）
        if not data.get("community_address"):
            from database.models import extract_district as _extract_dist, looks_like_real_address as _lkra_ocr
            _city_guess = next((c for c in ("台北市", "新北市") if c in (data.get("bodyText") or "")), None)
            _dist_guess = _extract_dist(data.get("bodyText") or "") or None
            _addr_crop = getattr(detail_ret, "addr_path", None) if detail_ret else None
            if _addr_crop and _city_guess and _dist_guess:
                from analysis.claude_analyzer import extract_address_consensus
                _ocr_addr = extract_address_consensus(_addr_crop, _city_guess, _dist_guess)
                # OCR 看詳情頁可能讀到屋主自填的「近XX1號出口」廣告詞 → 也要過 helper filter
                if _ocr_addr and _lkra_ocr(_ocr_addr, require_number=False):
                    data["community_address"] = _ocr_addr
                    logger.info(f"  OCR consensus 抓到地址: {_ocr_addr!r}")
                elif _ocr_addr:
                    logger.info(f"  OCR consensus 抓到 {_ocr_addr!r} 但沒路名結構 → 拒收")

        # Vision OCR 是主要資料來源（591 詳情頁防爬，regex 不可靠）
        # body text 只用來補 city/district/address 那種沒被防爬的欄位
        import re as _re
        body = data.get("bodyText", "")
        title = data.get("title") or body.split("\n", 1)[0][:60]
        # Mobile API 給結構化的 region/section（純值，不誤判）→ 優先用
        # 否則才走 bodyText regex（給 Playwright fallback path 用）
        if _mobile_data_url:
            city = _mobile_data_url.get("city")
            district = _mobile_data_url.get("district")
        else:
            city = next((c for c in ("台北市", "新北市") if c in body), None)
            district_m = _re.search(r"([\u4e00-\u9fa5]{2,3}區)", body)
            district = district_m.group(1) if district_m else None

        # DOM 社區地址若含「XX區」→ 優先用這個 district（比 body text 準）
        _community = (data.get("community_address") or "").strip()
        if _community:
            from database.models import extract_district as _extract_dist
            _dom_dist = _extract_dist(_community)
            if _dom_dist:
                district = _dom_dist

        # 地址優先順序：DOM 社區欄位（純文字、不會 OCR 誤讀）> Vision > body text
        # 若 DOM / Vision 兩邊都有地址但路名不同，代表 Vision 有誤讀 → 信 DOM
        v_addr = (vision.get("address") or "").strip()
        community_addr = (data.get("community_address") or "").strip()

        def _extract_road(a):
            m = _re.search(r"([\u4e00-\u9fa5]+(?:路|街|大道))", a or "")
            return m.group(1) if m else ""

        # community_addr / v_addr 都已經各自被 helper filter 擋過（line 4495 + OCR fallback），
        # 但 vision.get("address") 來自 extract_full_detail_from_screenshot 沒過 filter → 這邊再擋一次
        from database.models import looks_like_real_address as _lkra_final
        _v_addr_ok = _lkra_final(v_addr, require_number=False) if v_addr else False
        if not _v_addr_ok and v_addr:
            logger.info(f"  Vision 整頁 OCR 抓到 {v_addr!r} 但沒路名結構 → 拒收")
            v_addr = ""
        if community_addr:
            best_addr = community_addr
            # 若 Vision 跟 DOM 路名不同 → 記 log 提醒
            if v_addr and _extract_road(community_addr) != _extract_road(v_addr):
                logger.warning(
                    f"[OCR 差異] DOM='{_extract_road(community_addr)}' vs Vision='{_extract_road(v_addr)}' "
                    f"→ 以 DOM 為準（{community_addr}）"
                )
        else:
            best_addr = v_addr
        if best_addr:
            # 從地址推 city/district（若原本沒有）
            if not city:
                city = next((c for c in ("台北市", "新北市") if c in best_addr), None)
            if not district:
                m = _re.search(r"([\u4e00-\u9fa5]{2,3}區)", best_addr)
                district = m.group(1) if m else district

        price_wan = vision.get("price_wan")
        price_ntd = price_wan * 10000 if price_wan else None
        bld = vision.get("building_area_ping")
        age = int(vision["building_age"]) if vision.get("building_age") else None

        # 地址：normalize 格式 → 補樓層 → strip city/district 前綴 存純地址
        if best_addr:
            from analysis.claude_analyzer import _clean_address_garbage
            from database.models import strip_region_prefix
            best_addr = _clean_address_garbage(best_addr)
            floor_str = vision.get("floor")
            if "號" in best_addr and "樓" not in best_addr and floor_str:
                # floor 可能 "2" / "2/4" / "2/4F"，只取斜線前第一組數字
                _f_main = str(floor_str).split("/")[0]
                _f_m = _re.search(r"\d+", _f_main)
                floor_num = _f_m.group(0) if _f_m else ""
                if floor_num:
                    best_addr = best_addr + f"{floor_num}樓"
            best_addr = strip_region_prefix(best_addr, city or "", district or "")

        item = {
            "source": "591",
            "source_id": src_id,
            "url": url,
            "image_url": data.get("image_url") or None,
            "title": title,
            "city": city,
            "district": district,
            "address": best_addr or None,
            "building_type": "公寓",   # 591 filter 已選公寓，不靠 Vision 判斷
            "total_floors": vision.get("total_floors"),
            "floor": vision.get("floor"),
            "building_age": age,
            "building_area_ping": bld,
            "land_area_ping": vision.get("land_area_ping"),
            "price_ntd": price_ntd,
            "zoning": vision.get("zoning"),
            "_published_text": published_text,     # 591 詳情頁「刊登時間」文字
            "_updated_text": updated_text,         # 591 詳情頁「最後更新」文字
            # 591「社區」欄位 RAW value，給 detect_foreclosure 偵測「【」廣告詞用（跟 batch 路徑一致）
            # mobile path 用 m.community_raw（從 casesname 規範化），desktop fallback 用 detail_ret
            "_community_raw": (
                (_mobile_data_url.get("community_raw") if _mobile_data_url else "")
                or getattr(detail_ret, "community_raw", "")
                or ""
            ),
            # 刊登者身分（給 detect_foreclosure rule 3 mobile path 判定 — title 含 ＃ + identity
            # 「代理人」= 法拍仲介匿名 pattern）。Desktop OCR path 沒這欄位、走原 raw 比對。
            "poster_identity": (_mobile_data_url.get("poster_identity") if _mobile_data_url else None),
            "poster_linkman": (_mobile_data_url.get("poster_linkman") if _mobile_data_url else None),
        }

        # 必要欄位至少要有 city/district/price/bld 才能入庫
        if not (city and district and price_ntd and bld):
            _cleanup_shots(src_id)
            return {"status": "error", "message": f"從詳情頁解析不到完整資料（city={city}, district={district}, price={price_ntd}, bld={bld}）"}

        # 限定在目標區域內（admin 重新分析跳過此檢查）
        from config import is_target_region, TARGET_REGIONS
        if not is_reanalyze and not is_target_region(city, district):
            allowed = ", ".join(
                f"{c}（{'/'.join(v['districts'].keys())}）"
                for c, v in TARGET_REGIONS.items()
            )
            _cleanup_shots(src_id)
            return {"status": "error", "message": f"{city}{district} 不在分析範圍內。目前僅支援：{allowed}"}

        # 只用總樓層過濾（591 filter 已選公寓；OCR 建物類型不可靠，易誤判）
        # 不論 reanalyze 與否：總樓層 >=6 永遠不可能是公寓（公寓定義 5F 以下無電梯），
        # 是邏輯衝突資料。reanalyze 命中時加 db.delete — 把既存誤入庫 doc 從 DB 移除。
        _tf = item.get("total_floors") or 0
        try: _tf = int(_tf)
        except Exception: _tf = 0
        try: _f = int(item.get("floor")) if item.get("floor") else 0
        except Exception: _f = 0
        eff = max(_tf, _f)
        if eff >= 6:
            _cleanup_shots(src_id)
            if is_reanalyze:
                try:
                    col.document(src_id).delete()
                    logger.warning(f"已從 DB 移除非公寓 {src_id} (樓層 {item.get('floor')}/{_tf}F)")
                except Exception as _de:
                    logger.warning(f"移除非公寓 doc 失敗 {src_id}: {_de}")
            try:
                from database.retry_queue import dequeue_by_source_id
                dequeue_by_source_id(src_id)
            except Exception: pass
            return {"status": "skipped_non_apartment", "source_id": src_id,
                    "message": f"樓層 {item.get('floor')}/{item.get('total_floors')} ≥6，非公寓（5F 以下），跳過。"
                               + (" Doc 已從 DB 移除。" if is_reanalyze else "")}
        # 源頭已 filter 公寓，直接標公寓
        if not item.get("building_type"):
            item["building_type"] = "公寓"

        # ── 地址真實性驗證：避免 OCR 誤讀（例如 東豐街 讀成 栗豐街） ──
        # 只驗證「有到號」的地址；沒到號的走 LVR fuzzy 處理
        # 注意：item["address"] 已是純地址，geocode 前要拼回 city+district
        addr_pure = item.get("address") or ""
        addr_to_verify = f"{city or ''}{district or ''}{addr_pure}" if addr_pure else ""
        if "號" in addr_pure:
            from analysis.geocoder import geocode_with_district
            geo_candidates = geocode_with_district(addr_to_verify)
            if not geo_candidates:
                _cleanup_shots(src_id)
                return {
                    "status": "error",
                    "message": f"地址「{addr_to_verify}」地理編碼失敗，可能是 OCR 誤讀。請到 591 頁確認地址，或改用「輸入地址」手動送出。",
                }
            # 驗證 geocode 回來的區是不是跟 OCR 抓的 district 一致
            matched = [g for g in geo_candidates if g.get("city") == city and g.get("district") == district]
            if not matched:
                # 不一致 → 給建議但拒收（這個 pipeline 不走互動）
                sug = geo_candidates[0]
                _cleanup_shots(src_id)
                return {
                    "status": "error",
                    "message": (
                        f"OCR 讀到「{city}{district}{addr_to_verify}」，但 Google 地圖定位到"
                        f"「{sug.get('city') or '?'}{sug.get('district') or '?'}」。"
                        "可能 OCR 誤讀地址字元（例如 東/栗、南/雨 等），請到 591 頁再檢查。"
                    ),
                }

        # ── 呼叫共用分析 pipeline ──
        from api.analysis_pipeline import analyze_single_property
        initial_coords = (data.get("page_lat"), data.get("page_lng"))
        if not initial_coords[0]:
            initial_coords = None
        # 591 原生座標（頁面 JS 提供）→ 存進 item，供後續 OCR 誤讀偵測
        if initial_coords:
            item["source_latitude"] = initial_coords[0]
            item["source_longitude"] = initial_coords[1]

        result = analyze_single_property(
            item=item,
            ocr_ctx=ctx,
            initial_coords=initial_coords,
            detail_text=body,
        )
        doc = result["doc_data"]

        # ─ 合併（依欄位類型分級） ─
        from database.models import merge_property_doc
        from database.db import find_doc_by_source_id
        # 用 source_id 欄位 query 找既有 doc（migration 後 doc_id 是 UUID 不是 src_id）
        existing_doc_id, old = find_doc_by_source_id(src_id)
        if existing_doc_id:
            if is_reanalyze:
                # admin 重新分析：完全以新抓結果替換，不保留舊值（避免舊錯資料污染）
                # 例外：保留排序位置欄位 + user_url 標記（避免私人物件被「升級」成中央物件）
                for _keep in ("scrape_session_at", "list_rank", "scraped_at",
                              "source_origin", "submitted_by_uid", "submitted_by_email", "added_at_user"):
                    if old.get(_keep) is not None:
                        doc[_keep] = old.get(_keep)
                doc["id"] = existing_doc_id   # 保留既有 UUID
                col.document(existing_doc_id).set(_safe_doc(doc))
                return {"status": "ok", "source_id": src_id, "message": "重新分析完成（完整替換）"}
            merged, conflicts = merge_property_doc(old, doc)
            merged["id"] = existing_doc_id
            col.document(existing_doc_id).set(_safe_doc(merged))
            parts = ["已存在物件，已合併"]
            if conflicts:
                parts.append(f"欄位衝突：{', '.join(conflicts)}（保留舊值）")
            return {"status": "ok", "source_id": src_id, "message": "；".join(parts)}
        else:
            # 首次進中央的「用戶貼 URL 送出」物件 → 標 source_origin=user_url，
            # 讓搜尋 tab 過濾掉（搜尋 tab 只顯示 admin batch 抓進來的）
            # mark_user_url=False 時不標（例：retry queue 補抓 batch 失敗物件，那是 batch 來源）
            if not is_reanalyze and mark_user_url:
                doc["source_origin"] = "user_url"
            new_doc_id = doc.get("id")    # make_property_doc 已生成
            if not new_doc_id:
                from database.db import gen_dated_id
                new_doc_id = gen_dated_id()
                doc["id"] = new_doc_id
            col.document(new_doc_id).set(_safe_doc(doc))
            # post-write cross-source recheck — 防 dedup 因 snapshot/race 漏 catch
            try:
                from database.db import recheck_and_archive_if_cross_dup as _recheck
                _recheck(new_doc_id, trigger_label=("manual_reanalyze" if is_reanalyze else "manual_url"))
            except Exception: pass
            return {"status": "ok", "source_id": src_id, "message": "完整分析完成（新增）"}


@app.post("/api/clear_db")
async def clear_db(admin: dict = Depends(require_admin)):
    """軟刪除中央 properties：標記 archived=true，不真刪。
    這樣用戶 watchlist 不會變成孤兒。日後重抓到的物件會自動 unarchive（透過 force_reanalyze 或同 source_id 寫入時覆蓋）。"""
    from google.cloud.firestore_v1.base_query import FieldFilter
    col = get_col()
    now = now_tw_iso()
    count = 0
    # 只處理 archived != True 的（已 archive 的不再重複設）
    docs = list(col.where(filter=FieldFilter("archived", "in", [False, None])).stream())
    if not docs:
        # 退回掃全部（剛遷移完可能 archived 欄位完全不存在）
        docs = list(col.stream())
    BATCH = 400
    batch = get_firestore().batch()
    bn = 0
    for d in docs:
        data = d.to_dict() or {}
        if data.get("archived") is True:
            continue
        batch.update(d.reference, {
            "archived": True,
            "archived_at": now,
            "archived_by_email": admin.get("email") or "",
        })
        bn += 1
        count += 1
        if bn >= BATCH:
            batch.commit()
            batch = get_firestore().batch()
            bn = 0
    if bn > 0:
        batch.commit()
    logger.warning("[clear_db] %s 軟刪除 (archived=true) %d 筆", admin.get("email"), count)
    return {"status": "ok", "archived": count}


@app.post("/api/deep_analyze/{property_id:path}")
async def deep_analyze(property_id: str, admin: dict = Depends(require_admin)):
    doc = get_col().document(property_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="物件不存在")
    p = doc.to_dict()
    if p.get("deep_analysis_done"):
        return {"status": "already_done", "message": "此物件已完成深度分析"}
    if not p.get("latitude") or not p.get("longitude"):
        raise HTTPException(status_code=400, detail="缺少座標，無法執行地圖截圖")
    asyncio.create_task(_run_deep_analysis(property_id))
    return {"status": "started", "message": f"物件 {property_id} 深度分析已開始"}


async def _run_deep_analysis(property_id: str):
    from analysis.map_screenshotter import run_deep_analysis_screenshots
    from analysis.claude_analyzer import analyze_maps

    col = get_col()
    doc = col.document(property_id).get()
    if not doc.exists:
        return
    p = doc.to_dict()

    screenshots = await asyncio.to_thread(
        run_deep_analysis_screenshots,
        property_id, p.get("address", ""), p.get("latitude"), p.get("longitude"),
    )
    vision = await asyncio.to_thread(
        analyze_maps,
        p,
        screenshots.get("screenshot_cadastral"),
        screenshots.get("screenshot_zoning"),
        screenshots.get("screenshot_renewal"),
    )

    updates = {
        "screenshot_cadastral": screenshots.get("screenshot_cadastral"),
        "screenshot_zoning": screenshots.get("screenshot_zoning"),
        "screenshot_renewal": screenshots.get("screenshot_renewal"),
        "deep_analysis_done": True,
    }
    if vision:
        cadastral = vision.get("cadastral", {})
        zoning = vision.get("zoning", {})
        renewal_map = vision.get("renewal", {})
        if cadastral.get("parcel_area_sqm"):
            updates["land_area_sqm"] = cadastral["parcel_area_sqm"]
        if cadastral.get("road_frontage_m"):
            updates["road_width_m"] = cadastral["road_frontage_m"]
        if zoning.get("zone_type"):
            updates["zoning"] = zoning["zone_type"]
        if zoning.get("legal_far"):
            updates["legal_far"] = zoning["legal_far"]
        if renewal_map.get("in_renewal_zone") is not None:
            updates["in_renewal_zone"] = renewal_map["in_renewal_zone"]

    col.document(property_id).update(updates)


# ── 統計 ──────────────────────────────────────────────────────────────────────

@app.get("/api/stats")
def get_stats():
    docs = list(get_col().get())
    items = [doc.to_dict() or {} for doc in docs]
    total = len(items)
    strong = sum(1 for i in items if i.get("ai_recommendation") == "強烈推薦")
    consider = sum(1 for i in items if i.get("ai_recommendation") == "值得考慮")
    price_changed = sum(1 for i in items if i.get("is_price_changed"))
    scores = [i["score_total"] for i in items if i.get("score_total") is not None]
    avg = sum(scores) / len(scores) if scores else 0
    scraped_ats = sorted(
        [i["scraped_at"] for i in items if i.get("scraped_at")], reverse=True
    )
    return {
        "total_properties": total,
        "strong_recommend": strong,
        "consider": consider,
        "price_changed": price_changed,
        "average_score": round(avg, 1),
        "last_scrape": scraped_ats[0] if scraped_ats else None,
    }
