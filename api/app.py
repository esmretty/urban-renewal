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
from datetime import datetime, timezone, timedelta
from typing import Optional, AsyncGenerator, List

from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import os

from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from database.db import init_db, get_col, get_firestore, get_user_doc, get_user_watchlist, get_user_manual, get_user_bookmarks
from database.time_utils import now_tw, now_tw_iso, TW_TZ
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
    "/api/maintenance_status",   # 維護頁 polling 用，公開不需 auth
    "/admin.html",            # admin 也走自己的登入頁
}
_PUBLIC_PREFIXES = (
    "/static/",
    "/server/",               # admin portal 靜態資源
    "/data/screenshots/",
)


async def _auth_middleware(request, call_next):
    """全域攔截 /api/* /admin/* → 驗 Firebase token。"""
    path = request.url.path
    # CORS preflight 一律放行
    if request.method == "OPTIONS":
        return await call_next(request)
    if path in _PUBLIC_PATHS or any(path.startswith(p) for p in _PUBLIC_PREFIXES):
        return await call_next(request)
    if path.startswith("/api/") or path.startswith("/admin/"):
        try:
            user = await get_current_user(request)
        except HTTPException as e:
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=e.status_code, content={"detail": e.detail})
        # 把 user 塞進 request.state 給 handler 用
        request.state.user = user
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
    doc["address_inferred"] = choice
    land = matched.get("land_ping")
    if land is not None:
        doc["land_area_ping"] = land
        doc["land_area_sqm"] = round(land * 3.30578, 2)
    elif matched.get("is_reverse_geo"):
        # 座標反查選項：無地坪資料 → 清空
        doc["land_area_ping"] = None
        doc["land_area_sqm"] = None


_scrape_queue: Optional[asyncio.Queue] = None
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


def _safe_put_progress(msg_json: str):
    """非阻塞寫入 progress queue。queue 滿時先吃掉最舊訊息再放。
    定時 batch 沒 admin 監聽時，queue 會 fill up，用 drop-oldest 防止記憶體爆。"""
    if _scrape_queue is None:
        return
    try:
        _scrape_queue.put_nowait(msg_json)
    except asyncio.QueueFull:
        try: _scrape_queue.get_nowait()
        except Exception: pass
        try: _scrape_queue.put_nowait(msg_json)
        except Exception: pass


SCHEDULER_ALLOWED_INTERVAL_HR = (1, 3, 6, 12, 24)


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
    from analysis.lvr_index import _haversine   # noqa
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
            from database.models import primary_source_id, primary_url, compute_source_keys, all_sources_dead
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
                logger.info(f"[verify-alive] toggled alive in {d.id} (dead={dead_count})")
                log_action(trigger_label, "verify_alive_prune",
                           doc_id=d.id, source_id=primary_source_id(data),
                           message=f"標 {dead_count} 個失效來源 (alive=False)",
                           details={"dead_count": dead_count, "address": data.get("address")})
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
            if not (scan_en or verify_en or update_prices_en):
                continue   # 三個 type 都關，沒事做
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
    global _scrape_queue, _url_sem, _sched_wake_event
    _scrape_queue = asyncio.Queue(maxsize=500)
    _url_sem = asyncio.Semaphore(MAX_URL_CONCURRENCY)
    _sched_wake_event = asyncio.Event()
    init_db()
    # 暖機：建立 gRPC 連線
    logger.info("Firebase 連線中...")
    import asyncio as _aio
    await _aio.to_thread(lambda: list(get_col().limit(1).get()))
    logger.info("Firebase 連線完成")
    if os.getenv("DISABLE_SCHEDULER", "").lower() in ("1", "true", "yes"):
        logger.info("[scheduler] DISABLE_SCHEDULER=true，本次啟動不執行定時 batch（本機 debug 模式）")
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
# 登入驗證 middleware（排在 CORS 之後才能正確處理 OPTIONS preflight）
app.middleware("http")(_auth_middleware)

FRONTEND_DIR = BASE_DIR / "frontend"
SERVER_DIR = BASE_DIR / "server"
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR / "static")), name="static")
app.mount("/server/static", StaticFiles(directory=str(SERVER_DIR / "static")), name="server_static")
app.mount("/data/screenshots", StaticFiles(directory=str(BASE_DIR / "data" / "screenshots")), name="screenshots")


@app.get("/")
async def root():
    return FileResponse(str(FRONTEND_DIR / "index.html"))


@app.get("/api/target_regions")
def api_target_regions():
    """回傳目標分析範圍（前端下拉選單用）— 含台北市跟新北市 4 區。"""
    from config import target_regions_for_frontend
    return target_regions_for_frontend()


@app.get("/api/district_new_house_price")
def api_district_new_house_price():
    """前端用：取各行政區新成屋預設單價（萬/坪）。
    優先序：Firestore (LVR 預售屋自動更新中位數) → config.py 寫死常數。
    1 小時 cache 在 server 端，前端不必擔心打太頻繁。"""
    from analysis.presale_price import get_all_district_prices
    return get_all_district_prices()


@app.post("/admin/update_district_prices")
@app.post("/admin/update_district_prices/run-now")
async def admin_update_district_prices(admin: dict = Depends(require_admin)):
    """admin 觸發：下載最新 LVR + parse 預售屋 CSV 重算各區單價中位數寫 Firestore。
    支援兩個路徑：
      - POST /admin/update_district_prices （legacy）
      - POST /admin/update_district_prices/run-now （admin UI 立即執行按鈕）
    """
    try:
        result = await _run_update_prices_command(trigger_label="update_prices_manual")
        logger.warning(
            f"[admin] {admin.get('email')} 觸發 update_district_prices/run-now："
            f"{result['district_count']} 區 / {result['total_samples']} 筆樣本"
        )
        return {"status": "ok", **result}
    except Exception as e:
        logger.exception(f"[admin] update_district_prices 失敗: {e}")
        raise HTTPException(500, f"更新失敗：{e}")


@app.get("/api/firebase_config")
def api_firebase_config():
    """前端用的 Firebase client config。apiKey 不是機密，可暴露。"""
    import os
    return {
        "apiKey": os.getenv("FIREBASE_WEB_API_KEY", ""),
        "authDomain": "urban-renewal-32f02.firebaseapp.com",
        "projectId": "urban-renewal-32f02",
        "appId": os.getenv("FIREBASE_APP_ID", ""),
    }


@app.get("/api/me")
async def api_me(user: dict = Depends(get_current_user)):
    """回傳登入用戶資訊；未登入會被 middleware 擋在 401。
    第一次登入時會檢查白名單，不在白名單丟 403。
    若維護模式啟用，一律回傳 maintenance 欄位讓前端 redirect（包括 admin，admin 仍可進後台）。"""
    _ensure_user_profile(user)
    maint = _get_maintenance_state()
    if maint.get("enabled"):
        user["maintenance"] = {"enabled": True, "message": maint.get("message") or ""}
    return user


@app.get("/api/maintenance_status")
async def maintenance_status_public():
    """公開查詢維護狀態（不需登入）— 維護頁用來 poll 是否已恢復。"""
    maint = _get_maintenance_state()
    return {"enabled": bool(maint.get("enabled")), "message": maint.get("message") or ""}


@app.get("/login.html")
async def login_page():
    return FileResponse(str(FRONTEND_DIR / "login.html"))


@app.get("/maintenance.html")
async def maintenance_page():
    return FileResponse(str(FRONTEND_DIR / "maintenance.html"))


@app.get("/admin.html")
async def admin_page():
    return FileResponse(
        str(SERVER_DIR / "admin.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


# ── Admin API（只有 admin email 能打）───────────────────────────────────────

@app.get("/admin/stats")
async def admin_stats(admin: dict = Depends(require_admin)):
    col = get_col()
    docs = list(col.get())
    total = len(docs)
    done = err = 0
    for d in docs:
        data = d.to_dict() or {}
        st = data.get("analysis_status")
        if st == "done":
            done += 1
        if data.get("analysis_error"):
            err += 1
    # 用戶數：users collection（可能還沒建）
    users_count = 0
    try:
        users_col = get_firestore().collection("users")
        users_count = len(list(users_col.get()))
    except Exception:
        pass
    return {
        "total_properties": total,
        "analysis_done": done,
        "analysis_error": err,
        "total_users": users_count,
    }


@app.get("/admin/properties")
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


@app.get("/admin/manual_properties")
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


@app.get("/admin/properties/{property_id:path}")
async def admin_get_property(property_id: str, admin: dict = Depends(require_admin)):
    doc = get_col().document(property_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="物件不存在")
    data = doc.to_dict()
    data["id"] = doc.id
    return data


@app.delete("/admin/properties/{property_id:path}")
async def admin_delete_property(property_id: str, admin: dict = Depends(require_admin)):
    """從中央 DB 真刪除（非軟刪）。"""
    ref = get_col().document(property_id)
    if not ref.get().exists:
        raise HTTPException(status_code=404, detail="物件不存在")
    ref.delete()
    logger.warning("[admin] %s 永久刪除 %s", admin.get("email"), property_id)
    return {"status": "ok", "deleted": property_id}


@app.get("/admin/scheduler/status")
async def scheduler_status(admin: dict = Depends(require_admin)):
    """回傳定時 batch 目前狀態 + 設定，給 admin UI 顯示。"""
    from config import TARGET_REGIONS
    cfg = _load_scheduler_config()
    # 讀 per-cmd 狀態 + last_verify_alive_at
    state_doc = get_firestore().collection("settings").document("scheduler_state").get()
    state = (state_doc.to_dict() or {}) if state_doc.exists else {}
    cmds_with_state = []
    for i, c in enumerate(cfg.get("commands") or []):
        c2 = dict(c)
        cs = state.get(f"cmd_{i}") or {}
        c2["last_run_at"] = cs.get("last_run_at")
        c2["last_status"] = cs.get("last_status")
        c2["next_due_at"] = cs.get("next_due_at")   # 加 next_due_at 給 UI 顯示
        cmds_with_state.append(c2)
    legacy_en = bool(cfg.get("enabled"))
    return {
        "enabled": legacy_en,                                            # 舊欄位保留
        "scan_enabled": bool(cfg.get("scan_enabled", legacy_en)),        # per-type
        "verify_alive_enabled": bool(cfg.get("verify_alive_enabled", legacy_en)),
        "update_prices_enabled": bool(cfg.get("update_prices_enabled", legacy_en)),
        "interval_hr": int(cfg.get("interval_hr") or 1),
        "commands": cmds_with_state,
        "last_run_at": _scheduler_last_run_at,
        "last_status": _scheduler_last_status,
        "next_tick_at": _scheduler_next_tick_at,
        "currently_running": _scrape_running,
        "last_verify_alive_at": state.get("last_verify_alive_at"),
        "last_verify_alive_archived": state.get("last_verify_alive_archived"),
        # UI 選項用
        "allowed_districts": [d for r in TARGET_REGIONS.values() for d in (r.get("districts") or {}).keys()],
        "allowed_interval_hr": list(SCHEDULER_ALLOWED_INTERVAL_HR),
        "allowed_verify_interval_hr": list(SCHEDULER_VERIFY_INTERVAL_HR),
        "allowed_update_prices_interval_hr": list(SCHEDULER_UPDATE_PRICES_INTERVAL_HR),
        "max_commands": SCHEDULER_MAX_COMMANDS,
        "max_districts_per_command": SCHEDULER_MAX_DISTRICTS_PER_CMD,
        "inter_command_sleep_sec": SCHEDULER_INTER_COMMAND_SLEEP_SEC,
    }


@app.get("/admin/scheduler/history")
async def scheduler_history(days: int = 7, admin: dict = Depends(require_admin)):
    """回傳近 N 天的排程執行紀錄（預設 7 天，按開始時間倒序）。"""
    from datetime import timedelta as _td
    cutoff = (now_tw() - _td(days=max(1, min(days, 30)))).isoformat()
    items = []
    try:
        docs = get_firestore().collection("scheduler_history") \
            .where(filter=FieldFilter("started_at", ">=", cutoff)).get()
        for d in docs:
            data = d.to_dict() or {}
            data["id"] = d.id
            items.append(data)
        items.sort(key=lambda x: x.get("started_at") or "", reverse=True)
    except Exception as e:
        logger.warning(f"scheduler history query failed: {e}")
    return {"days": days, "count": len(items), "items": items}


@app.get("/admin/verify_alive/progress")
async def admin_verify_alive_progress(admin: dict = Depends(require_admin)):
    """回傳偵測下架的 live 進度：current/total/archived_count + 最近 archive 的物件清單。
    若 progress doc 超過 60 秒沒更新但 finished=False → 視為 stale（可能是 server restart 中斷），
    回 stale=True 讓前端停止 poll。"""
    try:
        doc = get_firestore().collection("settings").document("verify_alive_progress").get()
        if not doc.exists:
            return {"running": False}
        data = doc.to_dict() or {}
        finished = data.get("finished", False)
        running = not finished
        # stale check
        stale = False
        if running:
            updated_at = data.get("updated_at")
            if updated_at:
                try:
                    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
                    _ts = _dt.fromisoformat(updated_at.replace("Z", "+00:00"))
                    if _ts.tzinfo is None:
                        _ts = _ts.replace(tzinfo=_tz.utc)
                    if _dt.now(_tz.utc) - _ts > _td(seconds=60):
                        stale = True
                        running = False   # 前端停 poll
                except Exception:
                    pass
        return {
            "running": running,
            "stale": stale,
            **data,
        }
    except Exception as e:
        logger.warning("[verify-alive] read progress failed: %s", e)
        return {"running": False, "error": str(e)}


@app.get("/admin/run-logs")
async def admin_run_logs(limit: int = 200, trigger_prefix: Optional[str] = None,
                          admin: dict = Depends(require_admin)):
    """回傳最近的 action log（手動 + scheduler 都記錄）。
    每筆含 trigger / action / source_id / doc_id / message / details。
    trigger_prefix 可過濾：'manual' / 'scheduler' / 'verify_alive' / 'retry_queue'。"""
    from database.run_log import list_recent
    items = list_recent(limit=min(int(limit), 1000), trigger_prefix=trigger_prefix)
    return {"count": len(items), "items": items}


@app.get("/admin/line/status")
async def admin_line_status(admin: dict = Depends(require_admin)):
    """LINE 通知設定狀態：token / user_id 是否設定 + 倍數門檻 + 最近通知統計。"""
    import os as _os
    token = _os.getenv("LINE_CHANNEL_TOKEN", "").strip()
    user_id = _os.getenv("LINE_USER_ID", "").strip()
    # 讀門檻（預設 2.8）
    threshold = 2.8
    try:
        cfg = get_firestore().collection("settings").document("line_config").get()
        if cfg.exists:
            threshold = float((cfg.to_dict() or {}).get("threshold_multiple", 2.8))
    except Exception: pass
    notified_count = 0
    last_notified = None
    try:
        from google.cloud.firestore_v1.base_query import FieldFilter
        docs = list(get_col().where(filter=FieldFilter("line_notified_at", ">", "")).limit(500).stream())
        notified_count = len(docs)
        if docs:
            last_notified = max(d.to_dict().get("line_notified_at", "") for d in docs)
    except Exception as e:
        logger.warning(f"line status query: {e}")
    return {
        "configured": bool(token and user_id),
        "token_set": bool(token),
        "token_preview": (token[:6] + "..." + token[-4:]) if len(token) > 10 else "(empty)",
        "user_id_set": bool(user_id),
        "user_id_preview": (user_id[:6] + "...") if len(user_id) > 7 else "(empty)",
        "threshold_multiple": threshold,
        "notified_property_count": notified_count,
        "last_notified_at": last_notified,
        "trigger_threshold": f"都更/危老/防災都更 任一情境 ≥ {threshold} 倍",
    }


class LineThresholdReq(BaseModel):
    threshold: float


@app.post("/admin/line/threshold")
async def admin_set_line_threshold(body: LineThresholdReq, admin: dict = Depends(require_admin)):
    """Admin 設定 LINE 通知觸發倍數門檻。"""
    if body.threshold < 1.0 or body.threshold > 10.0:
        raise HTTPException(400, "threshold 必須介於 1.0~10.0")
    get_firestore().collection("settings").document("line_config").set({
        "threshold_multiple": float(body.threshold),
        "updated_at": now_tw_iso(),
        "updated_by_email": admin.get("email") or "",
    }, merge=True)
    logger.warning(f"[admin] {admin.get('email')} 設 LINE threshold = {body.threshold}")
    return {"status": "ok", "threshold_multiple": float(body.threshold)}


@app.get("/admin/line/notifications")
async def admin_line_notifications(limit: int = 50, admin: dict = Depends(require_admin)):
    """列出 LINE 通知發送紀錄（含每筆物件 id/地址/倍數/送達狀態）。"""
    docs = list(get_firestore().collection("line_notifications")
                .order_by("at", direction="DESCENDING").limit(min(int(limit), 200)).stream())
    items = []
    for d in docs:
        x = d.to_dict() or {}
        x["_id"] = d.id
        items.append(x)
    return {"count": len(items), "items": items}


@app.post("/admin/line/test")
async def admin_line_test(admin: dict = Depends(require_admin)):
    """送一則 LINE 測試訊息給 LINE_USER_ID。確認 token / user_id 設定 OK。"""
    from analysis.line_notify import push_line
    from datetime import datetime as _dt
    msg = (
        f"🔔 都更神探R 測試訊息\n"
        f"\n"
        f"觸發者：{admin.get('email', 'admin')}\n"
        f"時間：{_dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"\n"
        f"如果你看到這則訊息，表示 LINE 通知設定 OK ✓"
    )
    ok = push_line(msg)
    if ok:
        return {"status": "ok", "message": "測試訊息已送出，請查看 LINE"}
    import os as _os
    if not _os.getenv("LINE_CHANNEL_TOKEN") or not _os.getenv("LINE_USER_ID"):
        return {"status": "error", "message": "未設定 LINE_CHANNEL_TOKEN 或 LINE_USER_ID 環境變數"}
    return {"status": "error", "message": "送出失敗（檢查 token 是否有效、bot 是否已加好友）"}


@app.get("/admin/run-sessions")
async def admin_run_sessions(limit: int = 50, admin: dict = Depends(require_admin)):
    """回傳最近的「執行紀錄」session（依 batch_start/end 或 verify_alive_start/end 分組）。
    每個 session 含 trigger / started_at / ended_at / status / counts / actions。
    actions 是該 session 內所有 per-object log 條目（new/enrich/dup_merge/archive/prune 等）。"""
    from database.run_log import list_sessions
    sessions = list_sessions(limit=min(int(limit), 200))
    return {"count": len(sessions), "items": sessions}


class SchedulerToggleReq(BaseModel):
    enabled: bool
    type: Optional[str] = None   # "scan" / "verify_alive" / None=兩個都 toggle（legacy）


@app.post("/admin/scheduler/toggle")
async def scheduler_toggle(body: SchedulerToggleReq, admin: dict = Depends(require_admin)):
    """啟用/停用定時 batch（per-type）。存 Firestore 讓 runtime toggle 跨重啟保留。
    啟用時會 wake loop → 倒數立刻重算（避免沿用關閉期間累積的舊倒數）。
    type 帶值（"scan" / "verify_alive"）時只 toggle 該 type；不帶 = 兩個都 toggle（legacy）。"""
    update = {
        "updated_at": now_tw_iso(),
        "updated_by_email": admin.get("email") or "",
    }
    t = (body.type or "").lower()
    if t == "scan":
        update["scan_enabled"] = body.enabled
    elif t == "verify_alive":
        update["verify_alive_enabled"] = body.enabled
    elif t == "update_prices":
        update["update_prices_enabled"] = body.enabled
    else:
        # legacy：三個都 toggle，並維持舊 enabled 欄位
        update["enabled"] = body.enabled
        update["scan_enabled"] = body.enabled
        update["verify_alive_enabled"] = body.enabled
        update["update_prices_enabled"] = body.enabled
    get_firestore().collection("settings").document("scheduler").set(update, merge=True)
    logger.warning("[scheduler] %s 設定 type=%s enabled=%s", admin.get("email"), t or "all", body.enabled)
    if body.enabled and _sched_wake_event is not None:
        _sched_wake_event.set()
    return {"status": "ok", "type": t or "all", "enabled": body.enabled}


class CommandSpec(BaseModel):
    # type: "scan" = 掃描新物件（既有），"verify_alive" = 偵測下架
    type: Optional[str] = "scan"
    interval_hr: Optional[int] = None   # per-command interval；None 表示用全域 default

    # 「掃描新物件」用：
    districts: Optional[List[str]] = None
    limit: Optional[int] = None
    sources: Optional[List[str]] = None
    source: Optional[str] = None        # 舊欄位 backward compat


class SchedulerConfigReq(BaseModel):
    interval_hr: int
    commands: List[CommandSpec]


SCHEDULER_VERIFY_INTERVAL_HR = (12, 24, 72, 360)   # 偵測下架可選的間隔
SCHEDULER_UPDATE_PRICES_INTERVAL_HR = (24, 168, 720)   # 更新單價可選間隔（每天/週/月）


@app.post("/admin/scheduler/config")
async def scheduler_set_config(body: SchedulerConfigReq, admin: dict = Depends(require_admin)):
    """套用 admin UI 整份排程設定（commands list；每命令各自 interval）。"""
    from config import TARGET_REGIONS
    # 容量限制：scan 最多 SCHEDULER_MAX_COMMANDS 個、verify_alive 1 個、update_prices 1 個
    type_counts = {"scan": 0, "verify_alive": 0, "update_prices": 0}
    for c in body.commands:
        t = (c.type or "scan").lower()
        if t in type_counts:
            type_counts[t] += 1
    if type_counts["scan"] > SCHEDULER_MAX_COMMANDS:
        raise HTTPException(400, f"掃描命令最多 {SCHEDULER_MAX_COMMANDS} 個")
    if type_counts["verify_alive"] > 1:
        raise HTTPException(400, "偵測下架命令最多 1 個")
    if type_counts["update_prices"] > 1:
        raise HTTPException(400, "更新預售屋單價命令最多 1 個")
    allowed = {d for r in TARGET_REGIONS.values() for d in (r.get("districts") or {}).keys()}
    cleaned = []
    VALID_SOURCES = ("591", "yongqing", "sinyi")
    for idx, c in enumerate(body.commands):
        cmd_type = (c.type or "scan").lower()
        if cmd_type not in ("scan", "verify_alive", "update_prices"):
            raise HTTPException(400, f"命令 {idx+1}：type 必須是 scan / verify_alive / update_prices（收到 {c.type!r}）")

        if cmd_type == "scan":
            # 掃描新物件：要 districts + limit + sources
            districts = c.districts or []
            if not districts:
                continue   # 空命令跳過
            if len(districts) > SCHEDULER_MAX_DISTRICTS_PER_CMD:
                raise HTTPException(400, f"命令 {idx+1} 最多選 {SCHEDULER_MAX_DISTRICTS_PER_CMD} 區")
            for d in districts:
                if d not in allowed:
                    raise HTTPException(400, f"命令 {idx+1}：「{d}」不是合法行政區")
            # 跨城市檢查：同一命令裡 districts 必須屬於同一城市
            # （台北/新北 配額演算法不同，混搭先抓滿的城市會佔光配額）
            from config import TARGET_REGIONS as _TR
            _dist_to_city = {d: c for c, info in _TR.items() for d in (info.get("districts") or {}).keys()}
            cmd_cities = {_dist_to_city.get(d) for d in districts if _dist_to_city.get(d)}
            if len(cmd_cities) > 1:
                raise HTTPException(
                    400,
                    f"命令 {idx+1} 不能跨城市（{'、'.join(sorted(cmd_cities))}），請拆成多個命令"
                )
            limit = int(c.limit or 30)
            if limit < 1 or limit > 300:
                raise HTTPException(400, f"命令 {idx+1}：limit 必須 1~300")
            raw_sources = c.sources if c.sources else ([c.source] if c.source else ["591"])
            cmd_sources = [(s or "").lower() for s in raw_sources if s]
            for s in cmd_sources:
                if s not in VALID_SOURCES:
                    raise HTTPException(400, f"命令 {idx+1}：source 必須是 {VALID_SOURCES} 之一")
            if not cmd_sources:
                raise HTTPException(400, f"命令 {idx+1}：至少要勾一個來源")
            cmd_sources = [s for s in VALID_SOURCES if s in cmd_sources]
            interval_hr = int(c.interval_hr or 3)
            if interval_hr not in SCHEDULER_ALLOWED_INTERVAL_HR:
                raise HTTPException(400, f"命令 {idx+1}：interval_hr 必須為 {list(SCHEDULER_ALLOWED_INTERVAL_HR)}")
            cleaned.append({
                "type": "scan",
                "interval_hr": interval_hr,
                "districts": list(districts),
                "limit": limit,
                "sources": cmd_sources,
            })
        elif cmd_type == "update_prices":
            # 自動更新預售屋單價：只要 interval_hr
            interval_hr = int(c.interval_hr or 720)
            if interval_hr not in SCHEDULER_UPDATE_PRICES_INTERVAL_HR:
                raise HTTPException(
                    400,
                    f"命令 {idx+1}（更新單價）：interval_hr 必須為 {list(SCHEDULER_UPDATE_PRICES_INTERVAL_HR)}",
                )
            cleaned.append({
                "type": "update_prices",
                "interval_hr": interval_hr,
            })
        else:
            # verify_alive：只要 interval_hr
            interval_hr = int(c.interval_hr or 24)
            if interval_hr not in SCHEDULER_VERIFY_INTERVAL_HR:
                raise HTTPException(400, f"命令 {idx+1}（偵測下架）：interval_hr 必須為 {list(SCHEDULER_VERIFY_INTERVAL_HR)}")
            cleaned.append({
                "type": "verify_alive",
                "interval_hr": interval_hr,
            })

    if not cleaned:
        raise HTTPException(400, "至少需要 1 個有效命令")
    get_firestore().collection("settings").document("scheduler").set({
        "interval_hr": int(body.interval_hr or 3),   # 留著向下相容（已 deprecated）
        "commands": cleaned,
        "updated_at": now_tw_iso(),
        "updated_by_email": admin.get("email") or "",
    }, merge=True)
    # 套用後每個 cmd 重設 next_due_at 為「下個整點 boundary」
    # 這樣不管何時套用，下一次執行一定落在整點
    now_dt = now_tw()
    state_updates = {}
    for idx, c in enumerate(cleaned):
        ihr = int(c.get("interval_hr") or 3)
        nxt = _next_interval_boundary(now_dt, ihr)
        state_updates[f"cmd_{idx}"] = {
            "next_due_at": nxt.isoformat(),
            # 保留既有 last_run_at（如果有）
            **{k: v for k, v in (_cmd_state_get(idx).items())
               if k not in ("next_due_at",)}
        }
    if state_updates:
        get_firestore().collection("settings").document("scheduler_state").set(state_updates, merge=True)
    logger.warning("[scheduler] %s 套用設定 commands=%s （next_due_at 已對齊整點）",
                   admin.get("email"), cleaned)
    if _sched_wake_event is not None:
        _sched_wake_event.set()
    return {"status": "ok", "commands": cleaned}


@app.post("/admin/properties/{property_id:path}/reanalyze")
async def admin_reanalyze(property_id: str, source: str = "all", admin: dict = Depends(require_admin)):
    """完整重爬 + 重新分析。
    source 參數：
      - "all"（預設）：重抓該物件所有來源（依 sources array 順序）
      - "591" / "永慶" / "信義"：只重抓指定來源"""
    col = get_col()
    doc = col.document(property_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="物件不存在")
    p = doc.to_dict() or {}

    # 收集要重抓的來源清單
    sources_arr = p.get("sources") or []
    if not sources_arr and p.get("url"):
        # 舊 schema fallback
        sources_arr = [{
            "name": p.get("source") or "591",
            "source_id": p.get("source_id") or property_id,
            "url": p.get("url"),
        }]
    if source != "all":
        sources_arr = [s for s in sources_arr if s.get("name") == source]
    if not sources_arr:
        raise HTTPException(status_code=400, detail=f"找不到來源 {source} 的可用 URL")

    col.document(property_id).update({"analysis_in_progress": True})

    async def _do():
        try:
            for s in sources_arr:
                _src_id = s.get("source_id") or property_id
                _url = s.get("url")
                if not _url:
                    continue
                logger.warning("[admin] %s 重抓 %s (source=%s)", admin.get("email"), _src_id, s.get("name"))
                await asyncio.to_thread(_scrape_single_url, _url, _src_id, True)
            logger.warning("[admin] %s 完成重新分析 %s (sources=%s)", admin.get("email"), property_id, [s.get("name") for s in sources_arr])
        except Exception as e:
            logger.exception(f"[admin] 重新分析失敗 {property_id}: {e}")
        finally:
            # 無論成功失敗，一律清掉 analysis_in_progress（_scrape_single_url 的 early return 不會清）
            try:
                col.document(property_id).update({"analysis_in_progress": False})
            except Exception:
                pass
    asyncio.create_task(_do())
    logger.warning("[admin] %s 觸發重新分析（完整重爬） %s", admin.get("email"), property_id)
    return {"status": "started"}


@app.get("/admin/manual/{uid}/{property_id:path}")
async def admin_get_manual(uid: str, property_id: str, admin: dict = Depends(require_admin)):
    """admin 讀取指定用戶的 manual doc（給 reanalyze 輪詢用）。"""
    from database.db import get_user_manual
    doc = get_user_manual(uid).document(property_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="物件不存在")
    data = doc.to_dict() or {}
    data["id"] = doc.id
    data["submitted_by_uid"] = uid
    return data


@app.get("/admin/ocr_misread_scan")
async def admin_ocr_misread_scan(admin: dict = Depends(require_admin)):
    """掃全庫，比對 DB address 的路名 vs 591 原生座標反查的路名。
    若兩者差距大 → 疑似 OCR 誤讀或 Claude 誤糾正，回傳清單讓 admin 人工重新分析。"""
    import re, httpx
    key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not key:
        raise HTTPException(400, "GOOGLE_MAPS_API_KEY 未設定")

    def extract_road(s: str):
        if not s: return None
        t = re.sub(r"^\d{3,6}", "", s)
        t = re.sub(r"^(台灣|臺灣)", "", t)
        t = re.sub(r"^(台北市|臺北市|新北市|桃園市|基隆市)", "", t)
        t = re.sub(r"^[一-龥]{1,3}區", "", t)
        m = re.search(r"^([一-龥]{1,5}(?:路|街|大道)(?:[一二三四五六七八九十]段)?)", t)
        return m.group(1) if m else None

    suspects = []
    checked = 0
    skipped_no_src = 0
    for d in get_col().where(filter=FieldFilter("city", "==", "台北市")).get():
        data = d.to_dict()
        slat, slng = data.get("source_latitude"), data.get("source_longitude")
        if not slat or not slng:
            skipped_no_src += 1
            continue
        db_addr = data.get("address_inferred") or data.get("address") or ""
        db_road = extract_road(db_addr)
        if not db_road:
            continue
        try:
            async with httpx.AsyncClient(timeout=10) as cli:
                r = await cli.get(
                    "https://maps.googleapis.com/maps/api/geocode/json",
                    params={"latlng": f"{slat},{slng}", "key": key, "language": "zh-TW"},
                )
            results = (r.json() or {}).get("results") or []
            if not results: continue
            fa = results[0].get("formatted_address", "")
            rev_road = extract_road(fa)
            checked += 1
            if not rev_road or db_road == rev_road:
                continue
            base = re.sub(r"[一二三四五六七八九十]段$", "", db_road)
            rev_base = re.sub(r"[一二三四五六七八九十]段$", "", rev_road)
            if base == rev_base:
                continue   # 段延伸，視為相同
            suspects.append({
                "id": d.id,
                "title": data.get("title"),
                "district": data.get("district"),
                "db_address": db_addr,
                "db_road": db_road,
                "source_reverse": fa,
                "source_reverse_road": rev_road,
                "submitted_by_email": data.get("submitted_by_email"),
                "source_origin": data.get("source_origin"),
            })
        except Exception as e:
            logger.warning(f"OCR scan {d.id} failed: {e}")
    return {
        "checked": checked,
        "skipped_no_source_coords": skipped_no_src,
        "suspects": suspects,
        "note": (
            f"已用 591 原生座標反查對照 DB 路名。 "
            f"{skipped_no_src} 筆舊物件沒有原生座標（在加入此欄位前分析），下次重新分析才會有。"
        ),
    }


@app.post("/admin/manual/{uid}/{property_id:path}/reanalyze")
async def admin_reanalyze_manual(uid: str, property_id: str, admin: dict = Depends(require_admin)):
    """admin 重分析其他用戶的 manual 物件（/admin/manual_properties tab 用）。
    跟用戶端 reanalyze 走同一條 validate；若觸發歧義 admin 端會看到 status!=started，
    需要請該用戶自己處理（admin 不該替別人選戶）。"""
    if not property_id.startswith("manual_"):
        raise HTTPException(status_code=400, detail="只能重分析 manual 物件")
    from database.db import get_user_manual
    manual_col = get_user_manual(uid)
    doc = manual_col.document(property_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail=f"uid={uid} 沒有此 manual 物件")
    old = doc.to_dict() or {}

    # 同一條 validate
    from api.manual_analyze import validate_manual_input
    price_wan = (old.get("price_ntd") / 10000) if old.get("price_ntd") else None
    v = validate_manual_input(
        city=old.get("city"),
        district=old.get("district"),
        address=old.get("address"),
        building_area_ping=old.get("building_area_ping"),
        land_area_ping=old.get("land_area_ping"),
        price_wan=price_wan,
        use_source="auto",
    )
    if v["status"] != "ok":
        out = dict(v)
        out["mode"] = "admin_reanalyze"
        out["property_id"] = property_id
        out["uid"] = uid
        return out

    item = dict(v["item"])
    item["source_id"] = property_id
    item.setdefault("source", "manual")
    item.setdefault("title", old.get("title") or item.get("address"))
    for _k in ("building_age", "building_type", "total_floors", "floor"):
        if not item.get(_k) and old.get(_k):
            item[_k] = old[_k]

    manual_col.document(property_id).update({"analysis_in_progress": True})
    asyncio.create_task(_run_manual_analysis(uid, property_id, item))
    logger.warning("[admin] %s 觸發 manual 重分析 uid=%s src_id=%s (after validate)", admin.get("email"), uid, property_id)
    return {"status": "started", "source_id": property_id}


@app.post("/admin/migrate_bookmarks_to_watchlist")
async def admin_migrate_bookmarks(admin: dict = Depends(require_admin)):
    """
    一次性搬遷：所有 users/{uid}/bookmarks/* 搬到 users/{uid}/watchlist/*。
    bookmarks 概念合併進 watchlist 後留下的舊資料補救。
    """
    fs = get_firestore()
    users_col = fs.collection("users")
    summary = []
    for u in users_col.get():
        uid = u.id
        b_col = fs.collection("users").document(uid).collection("bookmarks")
        w_col = fs.collection("users").document(uid).collection("watchlist")
        moved = 0
        for bdoc in b_col.get():
            data = bdoc.to_dict() or {}
            # bookmarked_at → added_at（語意對應）
            if "bookmarked_at" in data and "added_at" not in data:
                data["added_at"] = data.pop("bookmarked_at")
            w_col.document(bdoc.id).set(data, merge=True)
            bdoc.reference.delete()
            moved += 1
        if moved:
            summary.append({"uid": uid, "moved": moved})
    logger.warning("[admin] %s 搬遷 bookmarks→watchlist: %s", admin.get("email"), summary)
    return {"status": "ok", "summary": summary, "total_moved": sum(s["moved"] for s in summary)}


@app.post("/admin/purge_non_apartments")
async def admin_purge_non_apartments(admin: dict = Depends(require_admin)):
    """一鍵清除中央 DB 所有非公寓物件（大樓/透天/店面/華廈/辦公/11F+）。"""
    col = get_col()
    forbidden = ["大樓", "透天", "店面", "店舖", "華廈", "辦公"]
    deleted = []
    for d in col.get():
        data = d.to_dict() or {}
        bt = (data.get("building_type") or "").strip()
        tf = data.get("total_floors") or 0
        try: tf = int(tf)
        except Exception: tf = 0
        hit_reason = None
        if any(f in bt for f in forbidden):
            hit_reason = f"類型={bt}"
        elif bt and "公寓" not in bt:
            hit_reason = f"類型={bt}"
        elif tf >= 11:
            hit_reason = f"{tf}F"
        if hit_reason:
            d.reference.delete()
            deleted.append({"id": d.id, "reason": hit_reason,
                            "address": data.get("address"), "building_type": bt})
    logger.warning("[admin] %s 清除非公寓 %d 筆", admin.get("email"), len(deleted))
    return {"status": "ok", "deleted_count": len(deleted), "deleted": deleted[:100]}


# ── 重複物件合併（一次性清理） ──────────────────────────────────────────────
def _dedup_compute_groups():
    """掃中央 DB，回傳 [{key, docs: [...full doc...]}] 有 >1 筆的重複群組。
    key = (district, road_short_without_prefix, bld_band_0_1, price_band_10000)"""
    import re as _re
    def _key(addr, district, bld, price_ntd):
        a = addr or ""
        a = _re.sub(r"^(台北市|臺北市|新北市|桃園市|台中市|臺中市|高雄市|台南市|臺南市|基隆市|新竹市)", "", a)
        a = _re.sub(r"^[一-龥]{1,3}區", "", a)
        m = _re.search(r"([一-龥]{1,5}(?:路|街|大道)(?:[一二三四五六七八九十]段)?(?:\d+巷)?)", a)
        road = m.group(1) if m else ""
        bld_band = round((bld or 0) * 10) / 10
        price_band = round((price_ntd or 0) / 10000)
        return (district or "", road, bld_band, price_band)

    groups = {}
    for d in get_col().get():
        data = d.to_dict() or {}
        # 要有最低資料才納入比對（否則空 key 全部會被視為同組）
        if not data.get("building_area_ping") or not data.get("price_ntd"):
            continue
        k = _key(data.get("address"), data.get("district"),
                 data.get("building_area_ping"), data.get("price_ntd"))
        if not k[1]:  # 沒抓到 road 就不比（避免把不同建物誤合）
            continue
        data["_id"] = d.id
        groups.setdefault(k, []).append(data)
    # 只留 >1 筆
    return [{"key": list(k), "docs": v} for k, v in groups.items() if len(v) > 1]


def _doc_richness(d: dict) -> int:
    """算 doc 豐富度：已填關鍵欄位數。用來挑 keeper。"""
    keys = ("address", "address_inferred", "latitude", "longitude",
            "land_area_ping", "building_age", "floor", "total_floors",
            "zoning", "road_width_m", "ai_analysis", "nearest_mrt",
            "screenshot_roadwidth")
    return sum(1 for k in keys if d.get(k))


@app.get("/admin/dedupe_scan")
async def admin_dedupe_scan(admin: dict = Depends(require_admin)):
    """(只掃不動資料) 列出可合併的重複群組。回傳：
    [{"key": [district, road, bld, price], "docs": [{id, source_id, address, url, richness, ...}]}]
    """
    groups = _dedup_compute_groups()
    out = []
    for g in groups:
        docs = sorted(g["docs"], key=_doc_richness, reverse=True)
        from database.models import primary_url, primary_source_id
        out.append({
            "key": g["key"],
            "count": len(docs),
            "keeper_id": docs[0]["_id"],
            "docs": [
                {
                    "id": d["_id"],
                    "source_id": primary_source_id(d),
                    "url": primary_url(d),
                    "address": d.get("address"),
                    "address_inferred": d.get("address_inferred"),
                    "title": (d.get("title") or "")[:50],
                    "building_area_ping": d.get("building_area_ping"),
                    "price_ntd": d.get("price_ntd"),
                    "richness": _doc_richness(d),
                    "scrape_session_at": d.get("scrape_session_at"),
                }
                for d in docs
            ],
        })
    return {"groups": out, "total_groups": len(out),
            "total_duplicates_to_remove": sum(len(g["docs"]) - 1 for g in groups)}


class DedupeMergeReq(BaseModel):
    confirm: bool = False   # 必須 True 才真的動


@app.post("/admin/dedupe_merge")
async def admin_dedupe_merge(body: DedupeMergeReq, admin: dict = Depends(require_admin)):
    """
    把每組重複物件合併到「最豐富的 keeper」：
      - keeper 保留
      - 其他 doc 的 sources[] 累進 keeper 的 sources[]
      - 其他 doc 的欄位若 keeper 還沒填 → 補到 keeper（地址/坪數等）
      - 其他 doc 刪除
    必須帶 {"confirm": true} 才真的動。
    """
    if not body.confirm:
        return {"status": "noop", "message": "confirm=false，未執行合併。請用 /admin/dedupe_scan 檢視後再送 confirm=true。"}

    from database.models import add_source_to_doc, compute_source_keys
    col = get_col()
    groups = _dedup_compute_groups()
    merged_count = 0
    deleted_ids = []
    for g in groups:
        docs = sorted(g["docs"], key=_doc_richness, reverse=True)
        keeper = docs[0]
        keeper_id = keeper["_id"]
        # 把其他 docs 的 sources[] 累進 keeper.sources[]
        keeper_clone = {"sources": list(keeper.get("sources") or [])}
        published_at_alt = list(keeper.get("published_at_alt") or [])
        keeper_updates = {}
        sources_changed = False
        fill_fields = ("address", "address_inferred", "address_inferred_confidence",
                       "address_inferred_candidates", "address_inferred_candidates_detail",
                       "latitude", "longitude", "land_area_ping", "land_area_source",
                       "land_area_sqm", "building_age", "floor", "total_floors",
                       "zoning", "zoning_original", "zoning_source", "road_width_m",
                       "road_width_name", "road_width_vision_reason",
                       "screenshot_roadwidth", "nearest_mrt", "nearest_mrt_dist_m",
                       "nearest_mrt_exit", "nearby_mrts", "ai_analysis", "ai_recommendation",
                       "ai_reason", "image_url")
        for other in docs[1:]:
            # 累進 sources[]
            for s in (other.get("sources") or []):
                if add_source_to_doc(keeper_clone, s.get("name") or "591", s.get("source_id") or "",
                                     s.get("url"), s.get("added_at")):
                    sources_changed = True
            for p in (other.get("published_at_alt") or []):
                if p and p not in published_at_alt:
                    published_at_alt.append(p)
            if other.get("published_at") and other["published_at"] not in published_at_alt:
                published_at_alt.append(other["published_at"])
            # 補 keeper 缺的欄位
            for f in fill_fields:
                if (keeper.get(f) in (None, "", [], {}) and keeper_updates.get(f) in (None, "", [], {})
                        and other.get(f) not in (None, "", [], {})):
                    keeper_updates[f] = other[f]
        if sources_changed:
            keeper_updates["sources"] = keeper_clone["sources"]
            keeper_updates["source_keys"] = compute_source_keys(keeper_clone["sources"])
        if published_at_alt != (keeper.get("published_at_alt") or []):
            keeper_updates["published_at_alt"] = published_at_alt
        if keeper_updates:
            try:
                col.document(keeper_id).update(keeper_updates)
            except Exception as e:
                logger.warning(f"dedupe update keeper 失敗 {keeper_id}: {e}")
                continue
        # 刪其他
        for other in docs[1:]:
            try:
                col.document(other["_id"]).delete()
                deleted_ids.append(other["_id"])
            except Exception as e:
                logger.warning(f"dedupe delete 失敗 {other['_id']}: {e}")
        merged_count += 1

    logger.warning("[admin] %s dedupe_merge：合併 %d 組，刪除 %d 筆",
                   admin.get("email"), merged_count, len(deleted_ids))
    return {"status": "ok", "merged_groups": merged_count,
            "deleted_count": len(deleted_ids), "deleted_ids": deleted_ids[:50]}


@app.delete("/admin/users/{uid}")
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


@app.get("/admin/users")
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


@app.get("/admin/email_whitelist")
async def get_email_whitelist(admin: dict = Depends(require_admin)):
    """列出 email 白名單（新用戶首次登入時會被檢查）。"""
    emails = sorted(_get_email_whitelist())
    return {"emails": emails}


@app.post("/admin/email_whitelist/add")
async def add_email_whitelist(body: WhitelistReq, admin: dict = Depends(require_admin)):
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
    logger.warning("[whitelist] %s 新增 %s", admin.get("email"), email)
    return {"status": "ok", "emails": sorted(current)}


@app.post("/admin/email_whitelist/remove")
async def remove_email_whitelist(body: WhitelistReq, admin: dict = Depends(require_admin)):
    email = (body.email or "").strip().lower()
    current = _get_email_whitelist()
    current.discard(email)
    get_firestore().collection("settings").document("email_whitelist").set(
        {"emails": sorted(current), "updated_at": now_tw_iso(),
         "updated_by_email": admin.get("email") or ""},
        merge=True,
    )
    logger.warning("[whitelist] %s 移除 %s", admin.get("email"), email)
    return {"status": "ok", "emails": sorted(current)}


# ── 網站維護模式（admin 切換） ──────────────────────────────────────────────
class MaintenanceReq(BaseModel):
    enabled: bool
    message: Optional[str] = None


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


@app.get("/admin/maintenance")
async def get_maintenance(admin: dict = Depends(require_admin)):
    """回傳目前維護模式狀態 + 訊息（本 server 的）。"""
    return _get_maintenance_state()


@app.post("/admin/maintenance")
async def set_maintenance(body: MaintenanceReq, admin: dict = Depends(require_admin)):
    """切換維護模式 + 自訂訊息。寫入本地檔案，不影響其他 server。"""
    import json as _json
    state = {
        "enabled": bool(body.enabled),
        "message": (body.message or "").strip(),
        "updated_at": now_tw_iso(),
        "updated_by_email": admin.get("email") or "",
    }
    path = _maintenance_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        _json.dump(state, f, ensure_ascii=False, indent=2)
    logger.warning("[maintenance] %s 設定 enabled=%s message=%r (本機檔案)",
                   admin.get("email"), state["enabled"], state["message"])
    return {"status": "ok", **state}


# ── 失敗重試佇列（admin） ───────────────────────────────────────────────────
@app.get("/admin/retry_queue")
async def get_retry_queue(admin: dict = Depends(require_admin)):
    """列出所有重試佇列 entry（pending + abandoned）"""
    from database.retry_queue import list_all_for_admin
    items = list_all_for_admin(limit=200)
    return {"count": len(items), "items": items}


@app.post("/admin/retry_queue/{queue_id}/run-now")
async def run_retry_now(queue_id: str, admin: dict = Depends(require_admin)):
    """手動立刻重抓某筆 — admin 不想等 10 分鐘。"""
    from database.retry_queue import dequeue, enqueue
    from database.db import find_doc_by_source_id as _fd
    db = get_firestore()
    doc = db.collection("retry_queue").document(queue_id).get()
    if not doc.exists:
        raise HTTPException(404, "queue entry 不存在")
    entry = doc.to_dict() or {}
    src_id = entry.get("source_id")
    url = entry.get("url")
    if not (src_id and url):
        raise HTTPException(400, "queue entry 缺 source_id / url")

    async def _do():
        try:
            await asyncio.to_thread(_scrape_single_url, url, src_id, False)
            new_doc_id, doc_data = _fd(src_id)
            if new_doc_id and doc_data and doc_data.get("price_ntd"):
                dequeue(queue_id)
                logger.info(f"[retry-queue] admin {admin.get('email')} 手動重抓成功 {src_id}")
            else:
                enqueue(src_id, entry.get("source") or "591", url,
                        error="admin manual retry: still missing core fields")
        except Exception as e:
            logger.warning(f"[retry-queue] admin 手動重抓 {src_id} 失敗: {e}")
            try:
                enqueue(src_id, entry.get("source") or "591", url,
                        error=f"admin manual retry exception: {str(e)[:200]}")
            except Exception:
                pass

    asyncio.create_task(_do())
    return {"status": "started", "source_id": src_id}


@app.delete("/admin/retry_queue/{queue_id}")
async def delete_retry_queue_entry(queue_id: str, admin: dict = Depends(require_admin)):
    """admin 從重試佇列移除（放棄不再重試）。"""
    from database.retry_queue import dequeue
    dequeue(queue_id)
    logger.warning(f"[retry-queue] admin {admin.get('email')} 移除 queue entry {queue_id}")
    return {"status": "ok"}


@app.post("/admin/verify_alive/run-now")
async def admin_verify_alive_now(admin: dict = Depends(require_admin)):
    """admin 手動立即執行偵測下架（不等排程）。背景跑、不阻塞 response。"""
    async def _do():
        try:
            await _run_verify_alive_command(trigger_label="verify_alive_manual")
            logger.warning(f"[verify-alive] {admin.get('email')} 手動觸發完成")
        except Exception as e:
            logger.exception(f"[verify-alive] 手動觸發失敗: {e}")
    asyncio.create_task(_do())
    return {"status": "started"}


@app.delete("/admin/retry_queue")
async def clear_retry_queue(admin: dict = Depends(require_admin)):
    """admin 清空整個重試佇列（一鍵全刪）。"""
    db = get_firestore()
    col = db.collection("retry_queue")
    count = 0
    BATCH = 400
    batch = db.batch()
    bn = 0
    for d in col.stream():
        batch.delete(d.reference)
        bn += 1
        count += 1
        if bn >= BATCH:
            batch.commit()
            batch = db.batch()
            bn = 0
    if bn > 0:
        batch.commit()
    logger.warning(f"[retry-queue] admin {admin.get('email')} 全部移除 {count} 筆")
    return {"status": "ok", "deleted": count}


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
    return {"total": total, "items": items[offset: offset + limit]}


@app.get("/api/central_search")
def central_search(
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
    user: dict = Depends(get_current_user),
):
    """
    探索 tab 的搜尋 API：所有條件在 server 端過濾後才回傳。
    每筆附 `_in_watchlist`(bool) 讓前端標記。
    """
    uid = user["uid"]
    my_watchlist = {d.id: (d.to_dict() or {}) for d in get_user_watchlist(uid).get()}
    my_watchlist_ids = set(my_watchlist.keys())

    dist_set = {d.strip() for d in districts.split(",") if d.strip()} if districts else None
    btype_set = {t.strip() for t in building_types.split(",") if t.strip()} if building_types else None
    floor_set = None
    if floors:
        try:
            floor_set = {int(f.strip()) for f in floors.split(",") if f.strip()}
        except ValueError:
            floor_set = None

    col = get_col()
    docs = list(col.get())
    items = []
    for d in docs:
        data = d.to_dict() or {}
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
        if floor_set is not None:
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
            if r and r not in (data.get("address") or "") and r not in (data.get("title") or ""):
                continue
        if q:
            kw = q.strip().lower()
            blob = " ".join(str(data.get(k) or "") for k in ("address", "title", "district")).lower()
            if kw not in blob:
                continue
        data["id"] = d.id
        data["_in_watchlist"] = d.id in my_watchlist_ids
        if data["_in_watchlist"]:
            data = merge_watchlist_with_central(data, my_watchlist.get(d.id, {}))
            data["id"] = d.id
            data["_in_watchlist"] = True
        items.append(data)
    # 與前端「新進優先」一致：scrape_session_at desc 為主、list_rank asc 為次
    items.sort(key=lambda x: (x.get("list_rank") if x.get("list_rank") is not None else 9999))
    items.sort(key=lambda x: x.get("scrape_session_at") or "", reverse=True)
    return {"total": len(items), "items": items[:limit]}


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

class ScrapeRequest(BaseModel):
    headless: bool = True
    districts: list[str] = []
    limit: int = 0
    source: str = "591"        # "591" / "yongqing" / "sinyi"
    # 分析門檻（超過則存 pending，不跑分析）
    max_floors: _Opt[int] = None
    max_total_price_wan: _Opt[int] = None
    max_price_per_building_ping_wan: _Opt[int] = None
    max_price_per_land_ping_wan: _Opt[int] = None


@app.post("/api/scrape")
async def trigger_scrape(req: ScrapeRequest, user: dict = Depends(require_admin)):
    """觸發 591 批次爬取（僅 admin）。"""
    global _scrape_running
    if _scrape_running:
        return {"status": "already_running", "message": "爬取已在進行中"}
    # 批次不再跟單筆 URL 分析互斥（後者用 semaphore 控併發）

    limit = req.limit if req.limit > 0 else 30

    thresholds = {
        k: v for k, v in {
            "max_floors": req.max_floors,
            "max_total_price_wan": req.max_total_price_wan,
            "max_price_per_building_ping_wan": req.max_price_per_building_ping_wan,
            "max_price_per_land_ping_wan": req.max_price_per_land_ping_wan,
        }.items() if v is not None
    }

    _ensure_user_profile(user)
    source = (req.source or "591").lower()
    if source not in ("591", "yongqing", "sinyi"):
        raise HTTPException(400, f"未知 source: {req.source}")
    asyncio.create_task(_run_scrape_task(
        headless=req.headless, districts=req.districts,
        limit=limit, thresholds=thresholds,
        triggered_by_uid=user["uid"],
        source=source,
        trigger_label="manual_batch",
    ))
    label = "、".join(req.districts) if len(req.districts) <= 3 else f"{len(req.districts)} 區"
    src_label = {"yongqing": "永慶", "sinyi": "信義"}.get(source, "591")
    return {"status": "started", "message": f"開始爬取 {src_label} {label}（最多 {limit} 筆）", "limit": limit}


@app.get("/api/scrape/status")
async def scrape_status():
    """SSE：推播爬取進度訊息。"""
    return StreamingResponse(
        _sse_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _sse_generator() -> AsyncGenerator[str, None]:
    yield "data: {\"msg\": \"連線成功，等待爬取任務...\"}\n\n"
    while True:
        try:
            msg = await asyncio.wait_for(_scrape_queue.get(), timeout=30)
            yield f"data: {msg}\n\n"
            if '"done"' in msg or '"error"' in msg:
                break
        except asyncio.TimeoutError:
            yield "data: {\"msg\": \"heartbeat\"}\n\n"


async def _run_scrape_task(headless: bool = True, districts: list = None, limit: int = 30, thresholds: dict = None, triggered_by_uid: Optional[str] = None, source: str = "591", trigger_label: str = "manual_batch"):
    global _scrape_running, _cancel_requested
    _scrape_running = True
    _cancel_requested = False
    # 重設共享 cancel flag — 讓 scraper inner loops 也能 check
    from scraper.cancel_state import reset as _cancel_reset
    _cancel_reset()
    import json

    loop = asyncio.get_running_loop()

    def progress(msg: str, percent: Optional[float] = None, new_item: bool = False):
        payload = {"msg": msg}
        if percent is not None:
            payload["percent"] = round(percent, 1)
        if new_item:
            payload["new_item"] = True
        loop.call_soon_threadsafe(
            _safe_put_progress,
            json.dumps(payload, ensure_ascii=False),
        )

    stats = None
    try:
        stats = await asyncio.to_thread(_scrape_and_analyze, headless, progress, districts or [], limit, thresholds, triggered_by_uid, source, trigger_label)
        _safe_put_progress(
            json.dumps({"msg": "爬取完成！", "done": True, "percent": 100}, ensure_ascii=False)
        )
    except Exception as e:
        logger.error(f"Scrape task error: {e}", exc_info=True)
        _safe_put_progress(
            json.dumps({"msg": f"錯誤：{e}", "error": True}, ensure_ascii=False)
        )
        stats = {"error": str(e)}
    finally:
        _scrape_running = False
    return stats or {}


def _scrape_and_analyze(headless: bool, progress_callback, districts: list = None, limit: int = 30, thresholds: dict = None, triggered_by_uid: Optional[str] = None, source: str = "591", trigger_label: str = "manual_batch"):
    """同步執行爬取 + 分析（在 asyncio.to_thread 中跑）。
    source: "591" / "yongqing" / "sinyi"。決定要爬哪個來源。"""
    from database.run_log import log_action
    districts = districts or []
    log_action(trigger_label, "batch_start", message=f"{source} / {','.join(districts) or 'all'} / limit={limit}",
               details={"source": source, "districts": districts, "limit": limit, "triggered_by_uid": triggered_by_uid})
    from scraper.scraper_591 import scrape_591
    from analysis.geocoder import geocode_address, get_nearest_mrt
    from analysis.scorer import calculate_score, calculate_renewal_value
    from analysis.claude_analyzer import analyze_property_text, generate_final_recommendation
    from database.models import make_property_doc

    col = get_col()

    # 判斷是否為第一次執行（DB 是否有資料）
    sample = list(col.limit(1).get())
    _is_first = len(sample) == 0

    def check_exists(source_id: str):
        """回傳已存在的 doc dict（含 id 欄位），或 None。
        Migration 後 doc_id 是 UUID 不是 source_id，用 source_id 欄位 query 找。"""
        from database.db import find_doc_by_source_id
        doc_id, data = find_doc_by_source_id(source_id)
        if data is None:
            return None
        # 確保 id 欄位存在（舊 docs 可能沒有 id 欄位 → 補上 doc_id）
        data.setdefault("id", doc_id)
        return data

    # 載入 DB 現有物件的 key 資料，用於重複物件偵測
    import re as _re_road
    _existing_items = []
    for _doc in col.get():
        _d = _doc.to_dict()
        _existing_items.append({
            "id": _d.get("id") or _doc.id,           # 物件唯一 ID（UUID 格式）
            "source_keys": list(_d.get("source_keys") or []),  # 用於跳過自己
            "price_ntd": _d.get("price_ntd"),
            "building_area_ping": _d.get("building_area_ping"),
            "address": _d.get("address") or "",
            "floor": _d.get("floor"),                 # 樓層 — 同棟不同戶建坪可能一樣，必須用樓層區分
            "total_floors": _d.get("total_floors"),
        })

    def _extract_road_name(addr):
        if not addr:
            return ""
        # 先砍城市/區前綴，避免 [一-龥]+ 貪婪把「中山區合江街」當一塊抓走
        inner = _re_road.sub(r"^(台北市|臺北市|新北市|桃園市|基隆市|新竹市|新竹縣|宜蘭縣)", "", addr)
        inner = _re_road.sub(r"^[一-龥]{1,3}區", "", inner)
        m = _re_road.search(r"([一-龥]{1,5}(?:路|街|大道)(?:[一二三四五六七八九十]段)?)", inner)
        return m.group(1) if m else ""

    def find_duplicate(item):
        """價格 + 建坪 ±0.01 + 地址路名 + 樓層 全部一樣 → 同物件，回 doc id；否則 None。
        加 floor 是為了區分同棟不同戶（建坪可能相同但 floor 不同 = 不同戶，例如 2樓 vs 3樓）"""
        from database.models import make_source_key
        price = item.get("price_ntd")
        area = item.get("building_area_ping")
        road = _extract_road_name(item.get("address") or item.get("title") or "")
        floor = item.get("floor")
        if not price or not area or not road:
            return None
        my_key = make_source_key(item.get("source") or "591", item.get("source_id") or "")
        for ex in _existing_items:
            if my_key in (ex.get("source_keys") or []):
                continue   # 自己 — 跳過
            if not (ex["price_ntd"] and abs(ex["price_ntd"] - price) < 1):
                continue
            if not (ex["building_area_ping"] and abs(ex["building_area_ping"] - area) <= 0.01):
                continue
            if _extract_road_name(ex["address"]) != road:
                continue
            # 樓層比對：兩邊都有值且不等 → 不同戶；單邊空或都空 → 信其他條件當同
            if floor is not None and ex.get("floor") is not None:
                if str(floor).strip() != str(ex["floor"]).strip():
                    continue
            return ex["id"]   # 回傳 UUID 給呼叫端用 col.document(id)
        return None

    label = "、".join(districts) if districts else "全部地區"
    progress_callback(f"開始爬取 591（{label}，最多 {limit} 筆）", 0)

    # 包裝 callback 把爬取階段的進度計算好（0% → 50%）
    def scrape_progress(msg: str, percent: Optional[float] = None, **kw):
        if percent is None:
            import re as _re
            m = _re.search(r"第\s*(\d+)\s*筆", msg)
            if m:
                count = int(m.group(1))
                percent = min((count / max(limit, 1)) * 50, 50)
        progress_callback(msg, percent, **kw)

    if source == "yongqing":
        from scraper.scraper_yongqing import scrape_yongqing
        result = scrape_yongqing(
            headless=headless,
            progress_callback=scrape_progress,
            districts_filter=districts,
            check_exists=check_exists,
            limit=limit,
        )
        source_label = "永慶"
    elif source == "sinyi":
        from scraper.scraper_sinyi import scrape_sinyi
        result = scrape_sinyi(
            headless=headless,
            progress_callback=scrape_progress,
            districts_filter=districts,
            check_exists=check_exists,
            limit=limit,
        )
        source_label = "信義"
    else:
        result = scrape_591(
            headless=headless,
            progress_callback=scrape_progress,
            districts_filter=districts,
            check_exists=check_exists,
            limit=limit,
        )
        source_label = "591"

    new_items = result["new"]
    price_updates = result["price_updates"]
    if not new_items and not price_updates:
        try:
            if source == "yongqing":
                from scraper import scraper_yongqing as _s
            elif source == "sinyi":
                from scraper import scraper_sinyi as _s
            else:
                from scraper import scraper_591 as _s
            _reason = _s.LAST_FETCH_ERROR
        except Exception:
            _reason = None
        msg = f"⚠ {source_label} 爬取 0 筆"
        if _reason:
            msg += f"（{_reason}）"
        else:
            msg += "（listing 無新物件；若同樣 region+section 多次都 0 筆，可能已被限流）"
        progress_callback(msg + "，請稍後重試", 100)
        # ★ 修補 bug：0 筆早 return 也要寫 batch_end，否則 admin 執行紀錄 session
        # 永遠停在「進行中」（4/28 00:01 那筆 stuck session 就是這樣來的）
        log_action(trigger_label, "batch_end",
                   message=f"{source_label} 抓到 0 筆（無新物件 / 限流 / 連線異常）",
                   details={"new_count": 0, "enrich_count": 0, "skip_dup_count": 0,
                            "price_update_count": 0, "fetch_error": _reason or None})
        return {"new_count": 0, "enrich_count": 0, "skip_dup_count": 0, "price_update_count": 0}
    progress_callback(f"爬取階段完成，抓到 {len(new_items)} 筆新物件", 50)

    # 處理價格變動
    from database.db import find_doc_by_source_id as _find_dup
    for pu in price_updates:
        existing_doc_id, existing = _find_dup(pu["source_id"])
        if not existing_doc_id:
            continue
        ref = col.document(existing_doc_id)
        history = existing.get("price_history") or []
        history.append({
            "price": pu["old_price"],
            "scraped_at": existing.get("scraped_at"),
        })
        # 漲跌方向（用於前端 badge）
        old_p = pu["old_price"] or 0
        new_p = pu["new_price"] or 0
        change_direction = "down" if new_p < old_p else ("up" if new_p > old_p else "same")
        now_iso = now_tw_iso()
        ref.update({
            "price_ntd": pu["new_price"],
            "price_per_ping": pu.get("new_price_per_ping"),
            "price_history": history,
            "is_price_changed": True,
            "scraped_at": now_iso,
            # 物件變動事件
            "last_change_at": now_iso,
            "latest_event": {
                "type": "price_change",
                "direction": change_direction,
                "from": old_p,
                "to": new_p,
                "at": now_iso,
            },
            # 重抓到 = 物件還活著，清 archived
            "archived": False,
        })
        old_wan = int(pu["old_price"] // 10000) if pu["old_price"] else "?"
        new_wan = int(pu["new_price"] // 10000) if pu["new_price"] else "?"
        arrow = "↓" if change_direction == "down" else "↑"
        progress_callback(
            f"⚠️ 價格變動 {arrow}：{pu.get('district', '')} {pu.get('title', '')[:20]}"
            f"  {old_wan}萬 → {new_wan}萬"
        )

    # 分析並儲存新物件（50% → 100%）
    from analysis.claude_analyzer import extract_full_detail_from_screenshot
    from scraper.scraper_591 import screenshot_detail_page
    from scraper.browser_manager import get_browser_context
    from scraper.zoning_lookup import lookup_zoning
    from analysis.lvr_index import triangulate_address, _extract_road_seg, ensure_fresh as _lvr_refresh

    # 確保 LVR 索引為最新（失敗不阻塞爬取）
    try:
        _lvr_refresh()
    except Exception as e:
        logger.warning(f"LVR 索引刷新失敗（不影響爬取）：{e}")

    new_count = 0
    enrich_count = 0
    skip_dup_count = 0
    total_to_analyze = len(new_items)

    # 預先載入既有所有記錄做 dedup 索引
    from database.models import doc_richness
    _dup_index = {}  # key (district, road_short, area_band, price_band) -> [doc_dict, ...]
    def _dup_key(d):
        addr = (d.get("address") or "")
        # 取「路+巷」級別
        import re as _re
        # 先剝掉「台北市/臺北市/新北市/…」及「XX區」前綴，避免 greedy regex 把「大安區信義」當成路名
        # （新 item 的 address 帶完整前綴、DB 既有物件經 pipeline strip_region_prefix 後不帶 → key 不一致）
        addr = _re.sub(r"^(台北市|臺北市|新北市|桃園市|台中市|臺中市|高雄市|台南市|臺南市|基隆市|新竹市)", "", addr)
        addr = _re.sub(r"^[一-龥]{1,3}區", "", addr)
        m = _re.search(r"([\u4e00-\u9fa5]{1,5}(?:路|街|大道)(?:[一二三四五六七八九十]段)?(?:\d+巷)?)", addr)
        road = m.group(1) if m else ""
        bld = round((d.get("building_area_ping") or 0) * 10) / 10  # 0.1 坪精度
        price_wan = round((d.get("price_ntd") or 0) / 10000)
        # floor 加入 key — 同棟不同戶建坪可能一樣，用樓層區分
        floor = str(d.get("floor") or "").strip()
        return (d.get("district") or "", road, bld, price_wan, floor)
    for _doc in col.get():
        _d = _doc.to_dict() or {}
        _d["_id"] = _doc.id
        k = _dup_key(_d)
        _dup_index.setdefault(k, []).append(_d)

    # 從最舊的開始分析，這樣前端 list_rank 最小的（最新的）最後入庫，排在最上面
    new_items.reverse()

    # 開個新 browser context 給 detail page 截圖用
    with get_browser_context(headless=headless) as ocr_ctx:
        for idx, item in enumerate(new_items, 1):
            if _cancel_requested:
                progress_callback("⛔ 使用者取消", 100)
                break
            try:
                pct = 50 + (idx / max(total_to_analyze, 1)) * 50
                is_enrich = item.get("_enrich_existing", False)
                is_force_reanalyze = item.get("_force_reanalyze", False)
                src_id = item["source_id"]

                if is_force_reanalyze:
                    progress_callback(
                        f"  🔄 強制重抓（原因：{item.get('_change_reason', '?')}）",
                        pct,
                    )

                # 重複物件檢查：合併 URL 到現有物件；若舊物件缺關鍵欄位，嘗試從新 URL 詳情頁補
                # force_reanalyze 跳過 dup 檢查 — 我們明確要重抓這個 source_id，不要被併到別的 doc
                if not is_enrich and not is_force_reanalyze:
                    dup_sid = find_duplicate(item)
                    if dup_sid:
                        skip_dup_count += 1
                        dup_doc = col.document(dup_sid).get()
                        if dup_doc.exists:
                            dd = dup_doc.to_dict()
                            # 判斷舊 doc 是否有關鍵缺欄位，若有則嘗試 OCR 新 URL 詳情頁補上
                            _critical = ["land_area_ping", "building_age", "zoning", "total_floors", "floor"]
                            _missing = [k for k in _critical if dd.get(k) in (None, "", 0)]
                            if _missing:
                                try:
                                    _dup_detail = screenshot_detail_page(ocr_ctx, item["url"], src_id)
                                    if _dup_detail and not getattr(_dup_detail, "delisted", False):
                                        _dup_shot, _, _ = _dup_detail[:3]
                                        _dup_house = getattr(_dup_detail, "house_path", None)
                                        _crop = _dup_house or _dup_shot
                                        if _crop:
                                            _vd = extract_full_detail_from_screenshot(_crop)
                                            _fill = {k: _vd[k] for k in _missing if _vd.get(k) not in (None, "", 0)}
                                            if _fill:
                                                col.document(dup_sid).update(_fill)
                                                progress_callback(
                                                    f"  ↻ 重複物件補資料 {dup_sid}: {', '.join(_fill.keys())}",
                                                    pct,
                                                )
                                                dd.update(_fill)   # 本地 dd 同步，後面 url_alt 不會再 re-read
                                except Exception as _de:
                                    logger.warning(f"dup enrich OCR 失敗 {src_id}: {_de}")

                            # 唯一真相：sources[] + source_keys[]。把新 src 加進 sources（如果還不在）
                            from database.models import add_source_to_doc, compute_source_keys, _parse_published_at
                            _new_src_name = item.get("source") or "591"
                            _pub_iso = (
                                _parse_published_at(item.get("_published_text"))
                                or item.get("scrape_session_at")
                                or now_tw_iso()
                            )
                            dd_clone = {"sources": list(dd.get("sources") or [])}
                            added = add_source_to_doc(dd_clone, _new_src_name, src_id, item.get("url"), _pub_iso)
                            update_payload = {}
                            if added:
                                update_payload["sources"] = dd_clone["sources"]
                                update_payload["source_keys"] = compute_source_keys(dd_clone["sources"])
                                # 跨來源新 source（非 591 加入到 591 doc 等）→ 觸發前端 badge
                                if _new_src_name != "591" and not any(
                                    s.get("name") == _new_src_name for s in (dd.get("sources") or [])
                                ):
                                    update_payload["last_change_at"] = now_tw_iso()
                                    update_payload["latest_event"] = {
                                        "type": "cross_source",
                                        "source": _new_src_name,
                                        "at": now_tw_iso(),
                                    }
                            if update_payload:
                                col.document(dup_sid).update(update_payload)
                        progress_callback(f"  ⏭ 重複物件（已合併網址）：{(item.get('title') or '')[:25]}", pct)
                        progress_callback(
                            f"    └ 新 ID {item.get('source_id')} → 併入 {dup_sid}",
                            pct,
                        )
                        log_action(trigger_label, "dup_merge",
                                   source_id=src_id, doc_id=dup_sid,
                                   message=f"併入 {dup_sid}（{item.get('title','')[:25]}）",
                                   details={"merged_into": dup_sid})
                        continue

                action = "補資料" if is_enrich else "分析"
                progress_callback(
                    f"{action} {idx}/{total_to_analyze}：{item.get('district')} {item.get('title', '')[:25]}",
                    pct,
                )

                # 永慶：scraper 已抓齊所有欄位 + 座標準確，跳過 591 專屬 OCR 流程
                if item.get("source") in ("永慶", "信義"):
                    _src_name = item.get("source")   # "永慶" 或 "信義"
                    page_coords = (item.get("latitude"), item.get("longitude")) if item.get("latitude") else None
                    progress_callback(f"  ✓ {_src_name}物件，座標 {page_coords}，跳過 591 OCR 流程", pct)

                    # 跨來源去重：找有沒有既有 591 doc 是同物件
                    # （地址路段 + 建坪 ±0.01 + 價格 完全 match）
                    yc_dup_id = find_duplicate(item)
                    if yc_dup_id:
                        # 命中既有 doc → 不建新 doc，補欄位 + 加 url_alt + 觸發跨來源事件
                        skip_dup_count += 1
                        dup_doc = col.document(yc_dup_id).get()
                        if dup_doc.exists:
                            dd = dup_doc.to_dict() or {}
                            updates = {}
                            # 補空欄位 + 永慶/信義可覆蓋的欄位（zoning / 座標 / 社區名）
                            if not dd.get("land_area_ping") and item.get("land_area_ping"):
                                updates["land_area_ping"] = item["land_area_ping"]
                            if item.get("zoning_original"):
                                updates["zoning_original"] = item["zoning_original"]
                            if item.get("latitude") and item.get("longitude"):
                                updates["source_latitude"] = item["latitude"]
                                updates["source_longitude"] = item["longitude"]
                            if item.get("community_name") and not dd.get("community_name"):
                                updates["community_name"] = item["community_name"]

                            # 把新 source 加進 sources[]（唯一真相）
                            from database.models import add_source_to_doc, compute_source_keys
                            dd_clone = {"sources": list(dd.get("sources") or [])}
                            if add_source_to_doc(dd_clone, _src_name, item["source_id"], item["url"]):
                                updates["sources"] = dd_clone["sources"]
                                updates["source_keys"] = compute_source_keys(dd_clone["sources"])

                            # 跨來源新上架事件
                            updates["last_change_at"] = now_tw_iso()
                            updates["latest_event"] = {
                                "type": "cross_source",
                                "source": _src_name,
                                "at": now_tw_iso(),
                            }

                            if updates.get("archived") is True:
                                updates["archived"] = False

                            col.document(yc_dup_id).update(updates)
                            progress_callback(
                                f"  🔗 {_src_name}物件併入既有 doc {yc_dup_id} "
                                f"(補 {len([k for k in updates if k not in ('last_change_at','latest_event','sources','source_keys')])} 欄位 + 加來源連結)",
                                pct,
                            )
                            # action log
                            try:
                                from database.run_log import log_action
                                log_action(trigger_label, "cross_source",
                                           source_id=src_id, doc_id=yc_dup_id,
                                           message=f"{_src_name}併入既有 doc",
                                           details={"merged_into": yc_dup_id, "source": _src_name})
                            except Exception: pass
                            # 情況 D：跨來源新上架後，順便驗活該 doc 的其他來源連結
                            try:
                                refreshed = col.document(yc_dup_id).get().to_dict() or {}
                                prune_updates = _verify_and_prune_sources(
                                    yc_dup_id, refreshed, skip_source_id=src_id
                                )
                                if prune_updates:
                                    col.document(yc_dup_id).update(prune_updates)
                                    progress_callback(
                                        f"  🧹 連結驗活：標記失效來源",
                                        pct,
                                    )
                            except Exception as _ve:
                                logger.warning(f"verify sources failed for {yc_dup_id}: {_ve}")
                        continue   # 不建新 doc

                    # 沒命中 → 建新 doc
                    import time as _time_yc
                    _t0_yc = _time_yc.time()
                    def _step_yc(msg):
                        elapsed = _time_yc.time() - _t0_yc
                        progress_callback(f"  [{elapsed:.1f}s] {msg}", pct)
                    from api.analysis_pipeline import analyze_single_property as _analyze_yc
                    yc_result = _analyze_yc(
                        item=item,
                        ocr_ctx=ocr_ctx,
                        step_fn=_step_yc,
                        initial_coords=page_coords,
                        detail_text="",
                        thresholds=None,
                    )
                    yc_doc = yc_result["doc_data"]
                    yc_doc_id = yc_doc.get("id")
                    if not yc_doc_id:
                        from database.db import gen_dated_id as _gen_id
                        yc_doc_id = _gen_id()
                        yc_doc["id"] = yc_doc_id
                    # 第一次入 DB 事件
                    yc_doc["last_change_at"] = now_tw_iso()
                    yc_doc["latest_event"] = {"type": "new", "source": _src_name, "at": now_tw_iso()}
                    col.document(yc_doc_id).set(_safe_doc(yc_doc))
                    new_count += 1
                    progress_callback(f"  ✓ {_src_name}物件已寫入 DB ({yc_doc_id})", pct)
                    try:
                        from database.run_log import log_action, build_doc_log_details
                        log_action(trigger_label, "new",
                                   source_id=src_id, doc_id=yc_doc_id,
                                   message=f"{_src_name} 新物件入庫：{yc_doc.get('address_inferred') or yc_doc.get('address') or ''}",
                                   details=build_doc_log_details(item, yc_doc, source=_src_name))
                    except Exception: pass
                    continue   # 不走下面 591 OCR 流程

                # 591：詳情頁截圖 + Vision OCR
                progress_callback(f"  📷 截圖詳情頁...", pct)
                _detail_ret = screenshot_detail_page(ocr_ctx, item["url"], src_id)
                # 下架偵測：listing 列表還在快取顯示卡片，但詳情頁已是 404 → 刪 DB 並跳過
                if getattr(_detail_ret, "delisted", False) or (isinstance(_detail_ret, tuple) and len(_detail_ret) >= 2 and _detail_ret[1] == "__DELISTED__"):
                    try:
                        db.collection("properties").document(src_id).delete()
                        logger.warning(f"已移除下架物件 {src_id}")
                    except Exception as _de:
                        logger.warning(f"移除下架物件失敗 {src_id}: {_de}")
                    progress_callback(f"  ⚠️ 物件已下架，跳過", pct)
                    continue
                shot_path, community_addr, page_coords = _detail_ret[:3]
                _addr_crop = getattr(_detail_ret, "addr_path", None)
                _house_crop = getattr(_detail_ret, "house_path", None)
                # 591 原生座標（詳情頁 JS 提供的 lat/lng）→ 存進 item，以供 OCR 誤讀偵測比對用
                if page_coords and page_coords[0] and page_coords[1]:
                    item["source_latitude"] = page_coords[0]
                    item["source_longitude"] = page_coords[1]
                # 詳情頁抓到的更新時間 → 寫進 item 讓 make_property_doc 轉 updated_at
                _upd_txt = getattr(_detail_ret, "updated_text", None)
                _pub_txt_detail = getattr(_detail_ret, "published_text", None)
                if _upd_txt:
                    item["_updated_text"] = _upd_txt
                if _pub_txt_detail and not item.get("_published_text"):
                    item["_published_text"] = _pub_txt_detail
                # 社區地址（DOM 純文字）優先於卡片地址
                # looks_like_real_address 擋廣告詞（屋主自填「近XX1號出口」這類無路名字串）
                from database.models import looks_like_real_address
                if looks_like_real_address(community_addr, require_number=True):
                    # 0) 若 DOM 地址含「XX區」且跟 card 的 district 不同 → 以 DOM 為準
                    #    （591 列表 query 用 section=X 搜出來有時會跨區，卡片 district 不可靠）
                    from database.models import extract_district as _extract_dist
                    _dom_dist = _extract_dist(community_addr)
                    if _dom_dist:
                        if _dom_dist != item.get("district"):
                            logger.info(
                                f"  [district 修正] card={item.get('district')!r} → DOM={_dom_dist!r} ({src_id})"
                            )
                            item["district"] = _dom_dist
                    # 1) normalize 格式 + 去綴字
                    from analysis.claude_analyzer import _clean_address_garbage
                    from database.models import strip_region_prefix
                    community_addr = _clean_address_garbage(community_addr)
                    # 2) 補樓層（floor 可能是 "2" / "2/4" / "2/4F"，只取斜線前第一組數字）
                    if "樓" not in community_addr and item.get("floor"):
                        import re as _re_f
                        _f_main = str(item["floor"]).split("/")[0]
                        _f_m = _re_f.search(r"\d+", _f_main)
                        f_num = _f_m.group(0) if _f_m else ""
                        if f_num:
                            community_addr = community_addr + f"{f_num}樓"
                    # 3) 最後統一 strip city/district 前綴（存純地址）
                    community_addr = strip_region_prefix(
                        community_addr, item.get("city") or "", item.get("district") or ""
                    )
                    item["address"] = community_addr
                vision_data = {}
                # 全頁截圖 + 房屋欄位窄裁切兩張都跑 OCR，合併結果（兩張平行跑）：
                # 觀察：全頁 OCR 偶會漏 land_area_ping / zoning（文字過小），house_crop 反而抓得到；
                # 反之也可能 house_crop 沒切到某欄位而 full 有 → 互補填。
                from concurrent.futures import ThreadPoolExecutor
                _paths = [p for p in (shot_path, _house_crop) if p]
                if _paths:
                    with ThreadPoolExecutor(max_workers=len(_paths)) as _ex:
                        _results = list(_ex.map(extract_full_detail_from_screenshot, _paths))
                    vision_data = _results[0] if _results else {}
                    for _r in _results[1:]:
                        for k, v in (_r or {}).items():
                            if v not in (None, "", 0) and vision_data.get(k) in (None, "", 0):
                                vision_data[k] = v
                # 不從 Vision 抓 building_type（591 filter 已保證是公寓；OCR 易把 5F 誤判華廈）
                # land_area_ping 特別重要 — 加 log 追蹤每次 Vision 結果（之前 20109672 漏 bug）
                _v_land = vision_data.get("land_area_ping")
                _i_land = item.get("land_area_ping")
                logger.info(f"[{src_id}] Vision land_area_ping={_v_land}, DOM regex land_area_ping={_i_land}, paths={[p for p in [shot_path, _house_crop] if p]}")
                for k in ("land_area_ping", "zoning", "building_age", "total_floors", "floor"):
                    if vision_data.get(k) and not item.get(k):
                        item[k] = vision_data[k]
                # land_area_ping 特殊處理：Vision 比 DOM regex 可信
                # （DOM 591 詳情頁的數字常被 CSS 防爬打亂，regex 可能抓到亂值）
                # 若 Vision 跟 DOM 都有但不同 → 信任 Vision
                if _v_land and _i_land and abs(_v_land - _i_land) > 0.5:
                    logger.warning(f"[{src_id}] DOM land={_i_land} vs Vision land={_v_land} 不一致，採用 Vision")
                    item["land_area_ping"] = _v_land
                # 源頭已 filter 公寓，直接標公寓（admin 重分析可保留舊 type）
                if not item.get("building_type"):
                    item["building_type"] = "公寓"

                # 地址：若 DOM 已抓到「含號 + 含路名」的完整地址 → 跳過 OCR（省 API 錢 + 避免 OCR 誤覆蓋）
                # 否則用「物件基本資料」窄裁切圖跑 consensus OCR
                # 注意：用 helper 過濾「廣告詞含號但無路名」的 dirty 卡片地址（屋主把 community-name 填廣告詞）
                from database.models import looks_like_real_address as _lkra_dom
                _dom_has_full = _lkra_dom(item.get("address"), require_number=True)
                if _dom_has_full:
                    logger.info(f"  DOM 已有完整地址，跳過 OCR address: {item.get('address')!r}")
                elif item.get("address") and "號" in (item.get("address") or ""):
                    # 卡片 address 含號但無路名 → 廣告詞 → 清掉再走 OCR
                    logger.info(f"  卡片 address {item.get('address')!r} 看似廣告詞（無路名）→ 清掉走 OCR")
                    item["address"] = ""
                elif _addr_crop and item.get("city") and item.get("district"):
                    from analysis.claude_analyzer import extract_address_consensus, _clean_address_garbage
                    from database.models import looks_like_real_address as _lkra_batch_ocr
                    ocr_addr = extract_address_consensus(_addr_crop, item["city"], item["district"])
                    # OCR 看詳情頁可能讀到屋主自填的廣告詞（如「近XX1號出口」）→ 過 helper 才採用
                    if ocr_addr and not _lkra_batch_ocr(ocr_addr, require_number=False):
                        logger.info(f"  OCR 抓到 {ocr_addr!r} 但沒路名結構 → 拒收 ({src_id})")
                        ocr_addr = None
                    if ocr_addr:
                        # OCR 地址若含「XX區」→ **信 OCR 的 district** 覆蓋 card district。
                        # 591 card 多區混查有時會 mislabel section（e.g. 物件在中山區卻標大安區）
                        from database.models import extract_district as _extract_dist
                        _ocr_dist = _extract_dist(ocr_addr)
                        if _ocr_dist and _ocr_dist != item.get("district"):
                            logger.info(
                                f"  [district 修正 OCR] card={item.get('district')!r} → OCR={_ocr_dist!r} ({src_id})"
                            )
                            item["district"] = _ocr_dist
                        # 補上 city/district 前綴（OCR 通常只給「路名+巷號」）
                        if not ocr_addr.startswith(item["city"]):
                            ocr_addr = f"{item['city']}{item['district']}{ocr_addr}"
                        ocr_addr = _clean_address_garbage(ocr_addr)
                        item["address"] = ocr_addr

                # ─ 詳情頁 scrape 失敗檢查：缺核心欄位（價格 / 行政區）→ 視為頁面沒拿到結構化資料，整筆丟棄
                if not item.get("price_ntd") or not (item.get("district") or "").strip():
                    progress_callback(
                        f"  ⛔ 跳過 scrape 失敗（缺價格或行政區）：{src_id} {(item.get('title') or '')[:25]}",
                        pct,
                    )
                    continue

                # ─ 只用總樓層過濾（591 filter 已選公寓；OCR 建物類型不可靠，易誤判）──
                # total_floors >= 6 視為非公寓（公寓定義：5F 以下無電梯）
                _total_f = item.get("total_floors") or 0
                try: _total_f = int(_total_f)
                except Exception: _total_f = 0
                if _total_f >= 6:
                    progress_callback(
                        f"  ⛔ 跳過非公寓（{_total_f}F≥6）：{(item.get('title') or '')[:25]}",
                        pct,
                    )
                    continue

                # ─ 重複物件偵測：同 district + road + 建坪 + 價格 ─
                # force_reanalyze 跳過：我們是明確對同一個 source_id 重抓，不該再被 dup 收到別的 doc 去
                if not is_enrich and not is_force_reanalyze:
                    from database.models import make_source_key
                    k = _dup_key(item)
                    my_src_key = make_source_key(item.get("source") or "591", item.get("source_id") or "")
                    candidates = [d for d in _dup_index.get(k, [])
                                  if my_src_key not in (d.get("source_keys") or [])]
                    if candidates:
                        best_old = max(candidates, key=doc_richness)
                        new_richness = doc_richness(item)
                        old_richness = doc_richness(best_old)

                        # 合併 URL：把新 source 加進 keeper 的 sources[]（如果還不在）
                        # → 下次 find_doc_by_source_key 找得到，不會再被當「新」處理
                        def _merge_url_to_keeper(keeper_doc, new_item):
                            from database.models import add_source_to_doc, compute_source_keys, _parse_published_at as _pp
                            out = {}
                            new_url = new_item.get("url")
                            new_sid = new_item.get("source_id")
                            new_name = new_item.get("source") or "591"
                            if not (new_url and new_sid):
                                return out
                            keeper_clone = {"sources": list(keeper_doc.get("sources") or [])}
                            _pub_iso = (
                                _pp(new_item.get("_published_text"))
                                or new_item.get("scrape_session_at")
                                or now_tw_iso()
                            )
                            if add_source_to_doc(keeper_clone, new_name, new_sid, new_url, _pub_iso):
                                out["sources"] = keeper_clone["sources"]
                                out["source_keys"] = compute_source_keys(keeper_clone["sources"])
                            return out

                        url_updates = _merge_url_to_keeper(best_old, item)

                        if new_richness > old_richness:
                            updates = dict(url_updates)
                            for k2 in ("land_area_ping", "building_age", "address",
                                       "image_url", "latitude", "longitude"):
                                if item.get(k2) and not best_old.get(k2):
                                    updates[k2] = item[k2]
                            if updates:
                                col.document(best_old["_id"]).update(updates)
                                best_old.update(updates)
                            skip_dup_count += 1
                            msg_tail = "，已合併網址" if url_updates else ""
                            progress_callback(
                                f"  ↻ 重複（補資料到 {best_old['_id']}{msg_tail}）：{(item.get('title') or '')[:25]}",
                                pct,
                            )
                            progress_callback(
                                f"    └ 新 ID {item.get('source_id')} → 併入 {best_old['_id']}",
                                pct,
                            )
                            log_action(trigger_label, "dup_merge",
                                       source_id=src_id, doc_id=best_old['_id'],
                                       message=f"併入 {best_old['_id']} 並補欄位",
                                       details={"merged_into": best_old['_id']})
                            continue
                        else:
                            if url_updates:
                                col.document(best_old["_id"]).update(url_updates)
                                best_old.update(url_updates)
                            skip_dup_count += 1
                            msg_tail = "（已合併網址）" if url_updates else ""
                            progress_callback(
                                f"  × 重複捨棄{msg_tail}：{(item.get('title') or '')[:25]}",
                                pct,
                            )
                            progress_callback(
                                f"    └ 新 ID {item.get('source_id')} → 併入 {best_old['_id']}",
                                pct,
                            )
                            log_action(trigger_label, "dup_merge",
                                       source_id=src_id, doc_id=best_old['_id'],
                                       message=f"併入 {best_old['_id']}（捨棄）",
                                       details={"merged_into": best_old['_id'], "discarded": True})
                            continue

                # ─ enrich 模式：用 merge 規則合併（用戶覆寫不動、衝突欄位 log）─
                if is_enrich:
                    existing = item["_existing_doc"]
                    incoming = {
                        "list_rank": item.get("list_rank"),
                        "scrape_session_at": item.get("scrape_session_at"),
                        "land_area_ping": item.get("land_area_ping"),
                        "building_age": item.get("building_age"),
                        "address": item.get("address"),
                        "total_floors": item.get("total_floors"),
                        "floor": item.get("floor"),
                        "image_url": item.get("image_url"),
                    }
                    if not existing.get("zoning_lookup_at") and existing.get("city") in ("台北市", "新北市"):
                        try:
                            z = lookup_zoning(
                                address=existing.get("address") or item.get("address"),
                                lat=existing.get("latitude"),
                                lng=existing.get("longitude"),
                                building_area_ping=existing.get("building_area_ping") or item.get("building_area_ping"),
                                city=existing.get("city"),
                                ctx=ocr_ctx,
                            )
                            incoming.update({
                                "zoning": z["zoning"],
                                "zoning_candidates": z["zoning_candidates"],
                                "zoning_source": z["zoning_source"],
                                "zoning_source_url": z.get("zoning_source_url"),
                                "zoning_lookup_at": z["zoning_lookup_at"],
                                "zoning_error": z.get("error"),
                            "zoning_original": z.get("original_zone"),
                                "address_probable": z["address_probable"],
                            })
                        except Exception as ze:
                            logger.warning(f"zoning lookup 失敗 {src_id}: {ze}")

                    from database.models import merge_property_doc
                    merged, conflicts = merge_property_doc(existing, incoming)
                    # 重抓到 = 物件還活著，清除 archived 旗標
                    if merged.get("archived") is True:
                        merged["archived"] = False
                    # 一律寫 last_enrich_attempt_at（即使 merge 沒實際改任何欄位）
                    # 用途：避免「永遠補不到的欄位」反覆觸發 enrich（591 card 本來就沒地坪 → 怎麼 enrich 都缺）
                    merged["last_enrich_attempt_at"] = now_tw_iso()
                    # existing 也算一筆「動到」(force write 即使內容沒變)，所以拿掉舊的 != 判斷
                    existing_doc_id = existing.get("id")
                    if not existing_doc_id:
                        from database.db import find_doc_by_source_id
                        existing_doc_id, _ = find_doc_by_source_id(src_id)
                    if existing_doc_id:
                        col.document(existing_doc_id).set(_safe_doc(merged))
                        enrich_count += 1
                        if conflicts:
                            progress_callback(f"  ⚠ {src_id} 欄位衝突保留舊值：{','.join(conflicts)}", pct)
                        log_action(trigger_label, "enrich",
                                   source_id=src_id, doc_id=existing_doc_id,
                                   message=f"補欄位{('（衝突: '+ ','.join(conflicts) + '）') if conflicts else ''}",
                                   details={"conflicts": conflicts, "address": existing.get("address")})
                    else:
                        logger.error(f"enrich 找不到 doc id for source_id={src_id}，跳過")
                        log_action(trigger_label, "error", source_id=src_id,
                                   message="enrich 找不到 doc")
                    continue

                # ─ 全新物件：呼叫共用 pipeline ─
                import time as _time
                _t0 = _time.time()
                def _step(msg):
                    elapsed = _time.time() - _t0
                    progress_callback(f"  [{elapsed:.1f}s] {msg}", pct)

                from api.analysis_pipeline import analyze_single_property
                # 中央 server 一律分析，不再以 threshold 跳過；過濾交給 client UI（避免 pending 物件留在中央）
                result = analyze_single_property(
                    item=item,
                    ocr_ctx=ocr_ctx,
                    step_fn=_step,
                    initial_coords=page_coords,
                    detail_text=item.get("_raw_text") or "",
                    thresholds=None,
                )
                doc_data = result["doc_data"]

                # ── 情況 B：換物件偵測 ────────────────────────────
                # 同 source_id 重抓後，路段不同 OR 建坪 ±0.5 變化 → 視為「ID 換成另一物件」
                # 處理：從原 doc 移除這個 source 連結；對新內容跑 find_duplicate
                #       命中別 doc → 加進那 doc 並觸發 cross_source 事件
                #       不命中 → 建新 doc（自然進列表頂端）
                is_replacement = False
                if is_force_reanalyze and item.get("_existing_doc"):
                    is_replacement = _is_replacement_change(item["_existing_doc"], doc_data)

                if is_replacement:
                    from database.models import (
                        remove_source_from_doc, add_source_to_doc, compute_source_keys,
                        all_sources_dead,
                    )
                    old_doc = item["_existing_doc"]
                    old_doc_id = old_doc.get("id")
                    if not old_doc_id:
                        from database.db import find_doc_by_source_id as _fd
                        old_doc_id, _ = _fd(src_id)
                    # 1. 從原 doc 移除這個 source
                    if old_doc_id:
                        old_clone = {"sources": list(old_doc.get("sources") or [])}
                        src_name = item.get("source") or "591"
                        remove_source_from_doc(old_clone, src_name, src_id)
                        updates = {
                            "sources": old_clone["sources"],
                            "source_keys": compute_source_keys(old_clone["sources"]),
                        }
                        if not old_clone["sources"]:
                            # 沒其他來源 → archive 整 doc（之後重抓會 unarchive）
                            updates["archived"] = True
                            updates["archived_at"] = now_tw_iso()
                            updates["archived_reason"] = f"所有來源失效（{src_id} 換成另一物件且無其他來源）"
                        col.document(old_doc_id).update(updates)
                        progress_callback(
                            f"  🔁 換物件：原 doc {old_doc_id[:8]}... 移除 {src_id}（{item.get('_change_reason','')}）",
                            pct,
                        )

                    # 2. 對新內容跑 find_duplicate
                    new_dup_id = find_duplicate(item)
                    if new_dup_id:
                        # 加進別的既有 doc + cross_source 事件
                        target_dup_doc = col.document(new_dup_id).get()
                        if target_dup_doc.exists:
                            dd = target_dup_doc.to_dict() or {}
                            dd_clone = {"sources": list(dd.get("sources") or [])}
                            src_name = item.get("source") or "591"
                            add_source_to_doc(dd_clone, src_name, src_id, item.get("url"))
                            col.document(new_dup_id).update({
                                "sources": dd_clone["sources"],
                                "source_keys": compute_source_keys(dd_clone["sources"]),
                                "last_change_at": now_tw_iso(),
                                "latest_event": {
                                    "type": "cross_source",
                                    "source": src_name,
                                    "at": now_tw_iso(),
                                },
                                "archived": False,
                            })
                            progress_callback(
                                f"  🔗 換物件後內容併入既有 doc {new_dup_id[:8]}...",
                                pct,
                            )
                        skip_dup_count += 1
                        continue   # 不建新 doc

                    # 沒命中 → 走全新建 doc 流程（fall through to 全新物件 set 區塊）
                    # 把 _existing_doc 清掉，避免下面又被 merge
                    item.pop("_existing_doc", None)
                    is_force_reanalyze = False   # 局部覆寫，讓下面走「全新」路徑

                # ── 決定要寫入的 doc_id ────────────────────────────
                # - force_reanalyze（非換物件）重用既有 doc 的 id
                # - 全新物件用 doc_data 自己生成的 id（make_property_doc 已產生）
                if is_force_reanalyze and item.get("_existing_doc"):
                    target_doc_id = item["_existing_doc"].get("id")
                    if not target_doc_id:
                        from database.db import find_doc_by_source_id
                        target_doc_id, _ = find_doc_by_source_id(src_id)
                else:
                    target_doc_id = doc_data.get("id")

                if not target_doc_id:
                    logger.error(f"無法決定 doc_id for source_id={src_id}，跳過寫入")
                    continue

                # 確保 doc_data 的 id 欄位跟實際寫入的 doc_id 一致
                doc_data["id"] = target_doc_id

                # 物件變動事件：第一次入 DB（換物件後也算新物件 → 這邊也會觸發）
                _now_iso = now_tw_iso()
                if not is_force_reanalyze:
                    doc_data["last_change_at"] = _now_iso
                    doc_data["latest_event"] = {"type": "new", "source": item.get("source", "591"), "at": _now_iso}

                # force_reanalyze：用 merge 保留 price_history / sources / user overrides / scrape_session_at 等
                # （merge_property_doc 會跳過 sources/source_keys，由 caller 端明確處理）
                is_reanalyze = bool(is_force_reanalyze and item.get("_existing_doc"))
                if is_reanalyze:
                    from database.models import merge_property_doc, compute_source_keys
                    merged, conflicts = merge_property_doc(item["_existing_doc"], doc_data)
                    merged["id"] = target_doc_id
                    # sources：保留既有 sources（不被 doc_data 的單筆 sources_arr overwrite）
                    # source_keys 同步 rebuild
                    merged["sources"] = list(item["_existing_doc"].get("sources") or [])
                    merged["source_keys"] = compute_source_keys(merged["sources"])
                    col.document(target_doc_id).set(_safe_doc(merged))
                    if conflicts:
                        progress_callback(
                            f"  ⚠ 重抓後欄位衝突保留舊值：{','.join(conflicts)}",
                            pct,
                        )
                    doc_data = merged
                else:
                    col.document(target_doc_id).set(_safe_doc(doc_data))
                # 將剛寫入的 doc 加進 _dup_index，讓同 session 內後續 item 能比對到
                try:
                    _new_d = dict(doc_data)
                    _new_d["_id"] = target_doc_id
                    _dup_index.setdefault(_dup_key(_new_d), []).append(_new_d)
                except Exception as _die:
                    logger.debug(f"dup_index 更新失敗 {src_id}: {_die}")

                # log_action：reanalyze 跟 new 區分（修 user 說的「同物件被誤標 new」）
                from database.run_log import build_doc_log_details
                if is_reanalyze:
                    log_action(trigger_label, "reanalyze",
                               source_id=src_id, doc_id=target_doc_id,
                               message=f"重新分析（{item.get('_change_reason','')}）：{(doc_data.get('address_inferred') or doc_data.get('address') or '')[:40]}",
                               details=build_doc_log_details(
                                   item, doc_data,
                                   change_reason=item.get('_change_reason'),
                                   conflicts=conflicts if 'conflicts' in dir() and conflicts else None,
                               ))
                else:
                    new_count += 1
                    log_action(trigger_label, "new",
                               source_id=src_id, doc_id=target_doc_id,
                               message=f"新物件入庫：{(doc_data.get('address_inferred') or doc_data.get('address') or '')[:40]}",
                               details=build_doc_log_details(item, doc_data))
                _existing_items.append({
                    "id": target_doc_id,
                    "source_keys": list(doc_data.get("source_keys") or []),
                    "price_ntd": item.get("price_ntd"),
                    "building_area_ping": item.get("building_area_ping"),
                    "address": item.get("address") or "",
                })
                status_msg = {
                    "done": f"✓ 已入庫 {new_count} 筆：{(doc_data.get('address_inferred') or doc_data.get('address') or '')[:30]}",
                    "skipped": f"  ⏭ 跳過：{result.get('skip_reason', '')}",
                    "foreclosure": f"  ⚖ 法拍：{', '.join(result.get('foreclosure_reasons') or [])}",
                }.get(result["status"], "")
                progress_callback(status_msg, pct, new_item=(result["status"] == "done"))

            except Exception as e:
                logger.exception(f"分析失敗 {item.get('source_id')}: {e}")

    progress_callback(
        f"完成：新增 {new_count} 筆，補資料 {enrich_count} 筆，重複捨棄 {skip_dup_count} 筆，價格變動 {len(price_updates)} 筆",
        100,
    )
    log_action(trigger_label, "batch_end",
               message=f"新 {new_count} / 補 {enrich_count} / 重複 {skip_dup_count} / 改價 {len(price_updates)}",
               details={"new_count": new_count, "enrich_count": enrich_count,
                        "skip_dup_count": skip_dup_count, "price_update_count": len(price_updates)})
    return {
        "new_count": new_count,
        "enrich_count": enrich_count,
        "skip_dup_count": skip_dup_count,
        "price_update_count": len(price_updates),
    }


# ── 深度分析（Phase 2） ───────────────────────────────────────────────────────

@app.post("/api/analyze/{property_id:path}")
async def analyze_pending(property_id: str):
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

    await asyncio.to_thread(_do)


# ── 手動輸入地址送出分析 ──────────────────────────────────────────────────────

class ManualAnalyzeReq(BaseModel):
    city: str
    district: str
    address: str
    building_area_ping: Optional[float] = None
    land_area_ping: Optional[float] = None
    price_wan: Optional[float] = None
    use_source: Optional[str] = "auto"   # auto / user / lvr（mismatch 時前端選）


@app.post("/api/manual_analyze")
async def analyze_manual(req: ManualAnalyzeReq, user: dict = Depends(get_current_user)):
    """
    手動輸入地址觸發分析（私人）。
    不寫中央；結果存 users/{uid}/manual/{manual_id}。
    """
    from api.manual_analyze import validate_manual_input, make_manual_source_id

    v = validate_manual_input(
        city=req.city,
        district=req.district,
        address=req.address,
        building_area_ping=req.building_area_ping,
        land_area_ping=req.land_area_ping,
        price_wan=req.price_wan,
        use_source=req.use_source or "auto",
    )
    if v["status"] != "ok":
        return v

    _ensure_user_profile(user)
    item = v["item"]
    src_id = make_manual_source_id(item["city"], item["district"], item["address"])
    uid = user["uid"]
    logger.warning(f"[manual] uid={uid} city={item['city']} district={item['district']} "
                   f"addr={item['address']!r} → src_id={src_id}")
    manual_col = get_user_manual(uid)
    existing = manual_col.document(src_id).get()
    if existing.exists:
        edata = existing.to_dict() or {}
        if edata.get("analysis_in_progress"):
            return {"status": "already_running", "source_id": src_id,
                    "message": "此地址分析進行中，請稍候再查看"}

    # 建立 placeholder doc（讓前端馬上有 row + loading bar）
    now = now_tw_iso()
    placeholder = _safe_doc({
        "id": src_id,                # manual 物件的 id 等於 src_id（manual_<timestamp>）
        "source_id": src_id,
        "sources": [{"name": "manual", "source_id": src_id, "url": None, "added_at": now}],
        "city": item["city"],
        "district": item["district"],
        "address": item["address"],
        "title": item["address"],
        "building_area_ping": item.get("building_area_ping"),
        "land_area_ping": item.get("land_area_ping"),
        "price_ntd": item.get("price_ntd"),
        "total_floors": item.get("total_floors"),
        "floor": item.get("floor"),
        "building_age": item.get("building_age"),
        "building_type": item.get("building_type") or "公寓",
        "source": "manual",
        "analysis_status": "done",
        "analysis_in_progress": True,
        "scraped_at": now,
        "scrape_session_at": now,
        "list_rank": 0,
    })
    manual_col.document(src_id).set(placeholder)

    asyncio.create_task(_run_manual_analysis(uid, src_id, item))
    return {"status": "started", "source_id": src_id, "item": item}


async def _run_manual_analysis(uid: str, src_id: str, item: dict):
    """背景跑共用 pipeline，結果寫 users/{uid}/manual/{src_id}，不進中央。"""
    manual_col = get_user_manual(uid)
    def _do():
        try:
            from api.analysis_pipeline import analyze_single_property
            from scraper.browser_manager import get_browser_context
            full_item = dict(item)
            full_item["source_id"] = src_id
            now_iso = now_tw_iso()
            full_item["scrape_session_at"] = now_iso
            full_item["scraped_at"] = now_iso
            full_item["list_rank"] = 0
            full_item["source"] = "manual"
            # 開 browser context 讓 pipeline 能跑 zonemap 截圖 + road_width Vision
            # + zoning lookup。沒有這個 ctx 就會跳過，導致 road_width_name 只靠 GeoServer
            # bbox 最近那條路（交叉口/巷弄會錯），且 screenshot_roadwidth 不會產生。
            with get_browser_context(headless=True) as ctx:
                result = analyze_single_property(
                    item=full_item,
                    ocr_ctx=ctx,
                    initial_coords=None,
                    detail_text="",
                )
            doc_data = result["doc_data"]
            doc_data["analysis_status"] = "done"
            doc_data["analysis_in_progress"] = False
            doc_data["source"] = "manual"
            # pipeline 回傳若未帶 session / rank 欄位則補回去（保險）
            doc_data.setdefault("scrape_session_at", now_iso)
            doc_data.setdefault("scraped_at", now_iso)
            doc_data.setdefault("list_rank", 0)
            # manual 物件的 id 強制等於 src_id（manual_<timestamp>），不要被 make_property_doc 自動生成的 UUID 蓋掉
            doc_data["id"] = src_id
            doc_data["sources"] = [{"name": "manual", "source_id": src_id, "url": None, "added_at": now_iso}]
            # 重分析時保留「物件在清單的位置」相關欄位（scrape_session_at / _added_at 等），
            # 避免用戶按重分析後物件跳到列表最上/最下。
            _old_snap = manual_col.document(src_id).get()
            if _old_snap.exists:
                _old = _old_snap.to_dict() or {}
                for _keep in ("scrape_session_at", "scraped_at", "list_rank", "_added_at", "created_at"):
                    if _old.get(_keep) is not None:
                        doc_data[_keep] = _old[_keep]
            manual_col.document(src_id).set(_safe_doc(doc_data))
            logger.info(f"完成手動分析 uid={uid} src_id={src_id}")
        except Exception as e:
            logger.exception(f"手動分析失敗 {src_id}: {e}")
            manual_col.document(src_id).update({
                "analysis_in_progress": False,
                "analysis_status": "done",
                "analysis_error": str(e)[:500],
            })
    await asyncio.to_thread(_do)


class NewHousePriceOverride(BaseModel):
    new_house_price_wan_per_ping: float


class RoadWidthOverride(BaseModel):
    road_width_m: float


class BonusOverride(BaseModel):
    which: str       # "weishau" | "dugen"
    value: float     # e.g., 0.30 / 0.50 / 0.80


class CoeffOverride(BaseModel):
    value: float


@app.post("/api/properties/{property_id:path}/bonus")
async def override_bonus(property_id: str, body: BonusOverride, user: dict = Depends(get_current_user)):
    field = "bonus_weishau" if body.which == "weishau" else "bonus_dugen"
    _user_override_ref(user, property_id).set({field: body.value}, merge=True)
    return {"status": "ok", field: body.value}


@app.post("/api/properties/{property_id:path}/rebuild_coeff")
async def override_rebuild_coeff(property_id: str, body: CoeffOverride, user: dict = Depends(get_current_user)):
    _user_override_ref(user, property_id).set({"rebuild_coeff": body.value}, merge=True)
    return {"status": "ok", "rebuild_coeff": body.value}


@app.post("/api/_debug/hide_legacy_manual")
def hide_legacy_manual():
    """一次把舊 id 格式（manual_YYYYMMDD_xxx）的手動 doc 軟刪除。"""
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
def debug_lvr_probe(city: str, district: str, road_keyword: str):
    """
    直接掃 LVR SQLite 看指定 city/district 下，含 road_keyword 的所有紀錄。
    用來定位「明明網路上有 LVR、我們 DB 卻找不到」的問題。
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
def debug_manual_docs():
    """直接列出 Firestore 所有 source_id 以 manual_ 開頭的 doc。繞過所有 server-side filter。"""
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


@app.post("/api/properties/{property_id:path}/hide")
async def hide_property(property_id: str, user: dict = Depends(get_current_user)):
    """刪除使用者清單中的物件。
    - manual / 中央 user_url 物件：硬刪 DB（不留 archive，跟 DELETE /api/watchlist 一致）
    - 其他中央物件（batch/scheduler）：軟刪（個人 deleted=True flag，不影響 DB）"""
    uid = user["uid"]
    if property_id.startswith("manual_"):
        try:
            get_user_manual(uid).document(property_id).delete()
            logger.info(f"[hide] manual {property_id} hard-deleted (user={uid})")
        except Exception as e:
            logger.warning(f"hide manual delete failed {property_id}: {e}")
        return {"status": "ok"}
    # 中央 user_url 也硬刪
    try:
        doc_ref = get_col().document(property_id)
        snap = doc_ref.get()
        if snap.exists:
            data = snap.to_dict() or {}
            if data.get("source_origin") == "user_url" and data.get("submitted_by_uid") == uid:
                doc_ref.delete()
                logger.info(f"[hide] user_url doc {property_id} hard-deleted (送件人={uid})")
                return {"status": "ok"}
    except Exception as e:
        logger.warning(f"hide central doc check failed {property_id}: {e}")
    # 其他中央物件 → 軟刪
    _user_override_ref(user, property_id).set({"deleted": True}, merge=True)
    return {"status": "ok"}


class FloorPremiumOverride(BaseModel):
    floor_premium: float   # 0.00 ~ 0.80


@app.post("/api/properties/{property_id:path}/floor_premium")
async def override_floor_premium(property_id: str, body: FloorPremiumOverride, user: dict = Depends(get_current_user)):
    v = max(0.0, min(0.80, float(body.floor_premium)))
    _user_override_ref(user, property_id).set({"floor_premium": v}, merge=True)
    return {"status": "ok", "floor_premium": v}


@app.post("/api/properties/{property_id:path}/road_width")
async def override_road_width(property_id: str, body: RoadWidthOverride, user: dict = Depends(get_current_user)):
    """使用者手動覆寫臨路寬度（只存自己 watchlist，不影響中央）。"""
    _user_override_ref(user, property_id).set({"road_width_m_override": body.road_width_m}, merge=True)
    return {"status": "ok", "road_width_m": body.road_width_m}


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


@app.post("/api/properties/{property_id:path}/zoning_ratios")
async def set_zoning_ratios(property_id: str, body: dict, user: dict = Depends(get_current_user)):
    """使用者手動設定多分區的坪數比例（個人覆寫）。"""
    ratios = body.get("zoning_ratios", [])
    _user_override_ref(user, property_id).set({"zoning_ratios": ratios}, merge=True)
    return {"status": "ok", "zoning_ratios": ratios}


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


class ScrapeUrlRequest(BaseModel):
    url: str


@app.post("/api/scrape_url")
async def scrape_url(req: ScrapeUrlRequest, user: dict = Depends(get_current_user)):
    """
    單一 591 URL 送出：
      1) 先查中央，如果已經分析過（done 且無 error）→ 直接把 src_id 加進本人 watchlist，不重跑 pipeline
      2) 否則 → 跑 pipeline 寫中央，再加 watchlist
    """
    global _url_running, _cancel_requested
    _cancel_requested = False
    import re as _re
    url_lower = req.url.lower()
    if "buy.yungching.com.tw" in url_lower:
        m = _re.search(r"/house/(\d{6,8})", req.url)
        if not m:
            return {"status": "error", "message": "永慶 URL 找不到 /house/{ID} 格式"}
        src_id = f"yongqing_{m.group(1)}"
        url_source = "yongqing"
    elif "sinyi.com.tw" in url_lower:
        m = _re.search(r"/buy/house/([A-Z0-9]{4,8})", req.url, _re.IGNORECASE)
        if not m:
            return {"status": "error", "message": "信義 URL 找不到 /buy/house/{ID} 格式"}
        src_id = f"sinyi_{m.group(1).upper()}"
        url_source = "sinyi"
    elif "sale.591.com.tw" in url_lower or "591.com.tw" in url_lower:
        m = _re.search(r"/(\d{6,})", req.url)
        if not m:
            return {"status": "error", "message": "591 URL 中找不到物件 ID"}
        src_id = f"591_{m.group(1)}"
        url_source = "591"
    else:
        return {"status": "error", "message": "目前僅支援 591 (sale.591.com.tw)、永慶 (buy.yungching.com.tw)、信義 (sinyi.com.tw) 網址"}

    _ensure_user_profile(user)
    uid = user["uid"]

    # 先查中央快取（用 source_id 欄位 query，因為 doc_id 已是 UUID 不再是 source_id）
    from database.db import find_doc_by_source_id
    existing_doc_id, cdata = find_doc_by_source_id(src_id)
    if existing_doc_id and cdata:
        # cache 路徑只擋兩種：
        #   1. 預售屋（沒實際物件）
        #   2. 樓層 max > 5（非公寓）
        # 不擋「套房/華廈/電梯大樓」字面 — 那些只是 building_type 名稱差異，
        # 1F 套房也是老建物，可分析
        _bld_type = cdata.get("building_type") or ""
        _tf = cdata.get("total_floors") or 0
        try: _tf = int(_tf)
        except Exception: _tf = 0
        try: _f = int(cdata.get("floor")) if cdata.get("floor") else 0
        except Exception: _f = 0
        eff = max(_tf, _f)
        if eff > 5 or _bld_type == "預售屋":
            return {"status": "skipped_non_apartment", "source_id": src_id,
                    "message": f"此物件非分析對象（type={_bld_type or '?'}, 樓層={cdata.get('floor')}/{cdata.get('total_floors')}），跳過"}
        if cdata.get("analysis_status") == "done" and not cdata.get("analysis_error"):
            # 直接引用，不重跑（用 doc 的 UUID 當 watchlist key）
            try:
                get_user_watchlist(uid).document(existing_doc_id).set({
                    "added_at": now_tw_iso(),
                }, merge=True)
            except Exception as e:
                logger.warning("watchlist add failed: %s", e)
            return {
                "status": "ok",
                "source_id": src_id,
                "id": existing_doc_id,
                "from_cache": True,
                "message": "中央已有分析結果，直接加入您的清單",
            }

    # 中央沒有 / 有但不完整 → 跑完整 pipeline
    # 使用 asyncio.Semaphore 控併發上限（預設 2），不再跟批次互斥
    # 超過上限時 await 自動排隊，用戶看到只是 request 花比較久，不會 reject
    global _url_inflight, _url_waiting
    if _url_sem is None:
        return {"status": "error", "message": "server 初始化中，請稍後"}
    _url_waiting += 1
    try:
        async with _url_sem:
            _url_waiting -= 1
            _url_inflight += 1
            try:
                result = await asyncio.to_thread(_scrape_single_url, req.url, src_id)
                # 抓完再用 source_id 查 UUID（_scrape_single_url 內部已寫到 UUID doc）
                new_doc_id, _ = find_doc_by_source_id(src_id)
                if new_doc_id:
                    try:
                        get_user_watchlist(uid).document(new_doc_id).set({
                            "added_at": now_tw_iso(),
                        }, merge=True)
                    except Exception as e:
                        logger.warning("watchlist add failed: %s", e)
                    # 標記送件人（admin tab 顯示用）：只在 doc 還沒標過時設（preserve 第一個送的人）
                    try:
                        _ref = get_col().document(new_doc_id)
                        _snap = _ref.get()
                        if _snap.exists and not (_snap.to_dict() or {}).get("submitted_by_uid"):
                            _ref.update({
                                "submitted_by_uid": uid,
                                "submitted_by_email": user.get("email") or "",
                            })
                    except Exception as e:
                        logger.warning("submitted_by update failed: %s", e)
                if isinstance(result, dict):
                    result["from_cache"] = False
                return result
            finally:
                _url_inflight -= 1
    except Exception:
        # semaphore 沒拿到就被例外中斷的情況，調整 waiting
        if _url_waiting > 0:
            _url_waiting -= 1
        raise


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
async def cancel_task():
    global _cancel_requested
    _cancel_requested = True
    return {"status": "ok"}


class KillSessionReq(BaseModel):
    trigger: str
    started_at: str
    source: Optional[str] = None


@app.post("/admin/scrape/kill_session")
async def admin_kill_session(body: KillSessionReq, admin: dict = Depends(require_admin)):
    """中斷單一 session（標記完成、補 batch_end log）。
    不真的殺背景 process（process 可能還在跑），只清理 admin UI 顯示的 zombie row。
    若該 session 還在跑、且正是當前 _scrape_running 的那一個 → 也設 cancel flag。"""
    global _cancel_requested, _scrape_running
    src = body.source or "?"
    try:
        from database.run_log import log_action
        log_action(body.trigger, "batch_end",
                   message=f"manual kill by admin {admin.get('email','?')} ({src} batch from {body.started_at[11:19] if len(body.started_at) >= 19 else body.started_at})",
                   details={"closed_manually": True, "killed_by_admin": True,
                            "admin_email": admin.get("email"),
                            "original_source": src, "original_started_at": body.started_at})
    except Exception as e:
        return {"status": "error", "message": str(e)}
    # 也發 cancel 信號（如果剛好是 currently running 那個會中斷）
    if _scrape_running:
        _cancel_requested = True
        _scrape_running = False
    logger.warning(f"[admin] {admin.get('email')} 中斷 session {body.trigger} {body.started_at}")
    return {"status": "ok", "message": f"已標記 session 完成（{src}）"}


@app.post("/admin/scrape/kill")
async def admin_kill_scrape(admin: dict = Depends(require_admin)):
    """中斷正在跑的 batch（軟取消 + 補 batch_end log + 重設 running flag）。
    後端執行緒可能因 HTTP 等待而沒立即停，但前端紀錄會立刻顯示完成。
    下一次 scheduler tick 也能正常起新 batch（不被卡住）。"""
    global _cancel_requested, _scrape_running
    _cancel_requested = True
    # 設共享 cancel flag — scraper inner loops 立刻看得到
    from scraper.cancel_state import set_cancelled as _cancel_set
    _cancel_set(True)
    was_running = _scrape_running
    _scrape_running = False   # 強制 reset 讓 scheduler 能起新 batch

    # 找最近沒對應 batch_end 的 batch_start，補 batch_end log
    closed_sessions = []
    try:
        from database.run_log import log_action, list_sessions
        sessions = list_sessions(limit=20)
        for sess in sessions:
            if sess.get("status") == "running":
                trigger = sess.get("trigger", "")
                started = sess.get("started_at", "")
                src = (sess.get("start_log", {}) or {}).get("details", {}).get("source", "?")
                log_action(trigger, "batch_end",
                           message=f"manual kill by admin {admin.get('email','?')}（原 {src} batch 從 {started[11:19]} 開始）",
                           details={"closed_manually": True, "killed_by_admin": True,
                                    "admin_email": admin.get("email"), "original_source": src})
                closed_sessions.append({"trigger": trigger, "source": src, "started_at": started})
    except Exception as e:
        logger.warning(f"kill scrape: close sessions failed: {e}")

    logger.warning(f"[admin] {admin.get('email')} 中斷 batch (was_running={was_running}, closed={len(closed_sessions)} sessions)")
    return {
        "status": "ok",
        "was_running": was_running,
        "closed_sessions": closed_sessions,
        "message": f"已發送中斷信號 + 重設 running flag。關閉了 {len(closed_sessions)} 個未完成 session。",
    }


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

    if existing_doc_id:
        if is_reanalyze:
            for _keep in ("scrape_session_at", "list_rank", "scraped_at"):
                doc[_keep] = old.get(_keep)
            doc["id"] = existing_doc_id
            col.document(existing_doc_id).set(_safe_doc(doc))
            return {"status": "ok", "source_id": src_id, "id": existing_doc_id, "message": "永慶物件重新分析完成（完整替換）"}
        merged, conflicts = merge_property_doc(old, doc)
        merged["id"] = existing_doc_id
        if merged.get("archived") is True:
            merged["archived"] = False
        col.document(existing_doc_id).set(_safe_doc(merged))
        msg = "永慶物件已存在中央 DB，已合併"
        if conflicts:
            msg += f"（衝突保留舊值：{', '.join(conflicts)}）"
        return {"status": "ok", "source_id": src_id, "id": existing_doc_id, "message": msg}

    # 新物件
    new_doc_id = doc.get("id") or gen_dated_id()
    doc["id"] = new_doc_id
    if not is_reanalyze and mark_user_url:
        doc["source_origin"] = "user_url"
    col.document(new_doc_id).set(_safe_doc(doc))
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

    if existing_doc_id:
        if is_reanalyze:
            for _keep in ("scrape_session_at", "list_rank", "scraped_at"):
                doc[_keep] = old.get(_keep)
            doc["id"] = existing_doc_id
            col.document(existing_doc_id).set(_safe_doc(doc))
            return {"status": "ok", "source_id": src_id, "id": existing_doc_id, "message": "信義物件重新分析完成（完整替換）"}
        merged, conflicts = merge_property_doc(old, doc)
        merged["id"] = existing_doc_id
        if merged.get("archived") is True:
            merged["archived"] = False
        col.document(existing_doc_id).set(_safe_doc(merged))
        msg = "信義物件已存在中央 DB，已合併"
        if conflicts:
            msg += f"（衝突保留舊值：{', '.join(conflicts)}）"
        return {"status": "ok", "source_id": src_id, "id": existing_doc_id, "message": msg}

    new_doc_id = doc.get("id") or gen_dated_id()
    doc["id"] = new_doc_id
    if not is_reanalyze and mark_user_url:
        doc["source_origin"] = "user_url"
    col.document(new_doc_id).set(_safe_doc(doc))
    return {"status": "ok", "source_id": src_id, "id": new_doc_id, "message": "信義物件分析完成（新增）"}


def _scrape_single_url(url: str, src_id: str, is_reanalyze: bool = False, *, mark_user_url: bool = True):
    """同步：開瀏覽器 + 抓單一 URL + 跑分析。
    is_reanalyze=True：admin 重新分析路徑，跳過「公寓 only」「目標區域」等過濾，
                      強制更新既有 doc（admin 特權，用於修正舊資料）。
    支援 591 / 永慶 / 信義 三種 URL（依 src_id prefix 分流）。"""
    # 永慶 URL → 走永慶單筆分析路徑（純 HTTP + Playwright，不需 Vision OCR）
    if src_id.startswith("yongqing_"):
        return _scrape_single_url_yongqing(url, src_id, is_reanalyze, mark_user_url=mark_user_url)
    if src_id.startswith("sinyi_"):
        return _scrape_single_url_sinyi(url, src_id, is_reanalyze, mark_user_url=mark_user_url)

    from scraper.browser_manager import get_browser_context
    from scraper.scraper_591 import _parse_card  # 既有 card 解析（不適用詳情頁）
    from scraper.scraper_591 import screenshot_detail_page
    from analysis.claude_analyzer import (
        extract_detail_from_screenshot,
        extract_full_detail_from_screenshot,
        analyze_property_text,
        generate_final_recommendation,
    )
    from analysis.geocoder import geocode_address, get_nearest_mrt
    from analysis.scorer import calculate_score, calculate_renewal_value
    from scraper.zoning_lookup import lookup_zoning
    from database.models import make_property_doc, should_skip_analysis, make_minimal_doc
    from datetime import datetime

    col = get_col()
    from api.analysis_pipeline import _cleanup_ephemeral_screenshots as _cleanup_shots
    with get_browser_context(headless=True) as ctx:
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
              // 必要條件：含「號」+「路/街/大道/巷」結構（避免屋主把社區欄位填成「近XX1號出口」廣告詞）
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
              // 物件標題：document.title 較穩（591 詳情頁 tab 標題就是物件名，含「- 591售屋網」尾綴）
              // DOM 抓 h1 時排除麵包屑類元素（會誤抓到「所有物件」「地圖找地」等導航文字）
              let pageTitle = (document.title || '').replace(/\s*[-|]\s*591.*$/,'').trim();
              if (!pageTitle || /591/.test(pageTitle) || /不存在/.test(pageTitle)) {
                // fallback：明確抓 h1（不抓任何 class*="title" 的通用元素）
                const h1 = document.querySelector('h1.detail-title, h1.info-title, h1');
                pageTitle = h1 ? (h1.innerText || '').trim() : '';
              }
              // 排除已知麵包屑字串
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
        # 直接從 DB 移除該筆（省得留一堆無效資料）
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
        # shot + house_crop 平行 OCR 然後合併，house_crop 補漏（全頁 OCR 偶會漏 land_area_ping）
        from concurrent.futures import ThreadPoolExecutor as _TPE_URL
        _paths_u = [p for p in (shot, _house_crop_single) if p]
        vision = {}
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
        # admin 重新分析跳過此檢查，讓 admin 能修正既有物件的資料
        if not is_reanalyze:
            _tf = item.get("total_floors") or 0
            try: _tf = int(_tf)
            except Exception: _tf = 0
            try: _f = int(item.get("floor")) if item.get("floor") else 0
            except Exception: _f = 0
            eff = max(_tf, _f)
            if eff >= 6:
                _cleanup_shots(src_id)
                try:
                    from database.retry_queue import dequeue_by_source_id
                    dequeue_by_source_id(src_id)
                except Exception: pass
                return {"status": "skipped_non_apartment", "source_id": src_id,
                        "message": f"樓層 {item.get('floor')}/{item.get('total_floors')} ≥6，非公寓（5F 以下），跳過。"}
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
                # 例外：scrape_session_at / list_rank / scraped_at 一律保留舊值（即使舊值是 None），
                # 物件在列表排序中的位置絕對不因 reanalyze 而變動。
                for _keep in ("scrape_session_at", "list_rank", "scraped_at"):
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
            return {"status": "ok", "source_id": src_id, "message": "完整分析完成（新增）"}


class DesiredPriceOverride(BaseModel):
    desired_price_wan: float


@app.post("/api/properties/{property_id:path}/desired_price")
async def override_desired_price(property_id: str, body: DesiredPriceOverride, user: dict = Depends(get_current_user)):
    """欲出價（萬），個人設定。"""
    _user_override_ref(user, property_id).set({"desired_price_wan": body.desired_price_wan}, merge=True)
    return {"status": "ok", "desired_price_wan": body.desired_price_wan}


class InferredChoiceOverride(BaseModel):
    address: str


@app.post("/api/properties/{property_id:path}/inferred_choice")
async def override_inferred_choice(property_id: str, body: InferredChoiceOverride, user: dict = Depends(get_current_user)):
    """用戶從 address_inferred_candidates_detail 中挑一個當作推測地址（個人設定）。
    地址必須命中候選清單；儲存後讀取時 _read_user_property 會 swap address_inferred + land_area_ping。"""
    p = _read_user_property(user, property_id)
    if p is None:
        raise HTTPException(status_code=404, detail="物件不存在")
    cands = p.get("address_inferred_candidates_detail") or []
    matched = next((c for c in cands if c.get("address") == body.address), None)
    if not matched:
        raise HTTPException(status_code=400, detail="所選地址不在候選清單中")
    _user_override_ref(user, property_id).set({"inferred_address_choice": body.address}, merge=True)
    return {"status": "ok", "address": body.address, "land_ping": matched.get("land_ping")}


@app.post("/api/properties/{property_id:path}/new_house_price")
async def override_new_house_price(property_id: str, body: NewHousePriceOverride, user: dict = Depends(get_current_user)):
    """覆寫新成屋單價（個人設定）— renewal v2 結果由前端即時算，不存 DB（CLAUDE.md 規則 8）。"""
    from analysis.scorer import calculate_renewal_scenarios, resolve_effective_zoning
    p = _read_user_property(user, property_id)
    if p is None:
        raise HTTPException(status_code=404, detail="物件不存在")
    # 只存「輸入欄位」（用戶覆寫的單價）；結果即時回給前端，不寫進 DB
    _user_override_ref(user, property_id).set({
        "new_house_price_wan_override": body.new_house_price_wan_per_ping,
    }, merge=True)
    v2 = calculate_renewal_scenarios(
        land_area_ping=p.get("land_area_ping"),
        zoning=resolve_effective_zoning(p.get("zoning"), p.get("zoning_original")),
        district=p.get("district"),
        price_ntd=p.get("price_ntd"),
        new_house_price_wan_per_ping=body.new_house_price_wan_per_ping,
        road_width_m=p.get("road_width_m_override") or p.get("road_width_m"),
    )
    return {"status": "ok", "renewal_v2": v2}


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
async def deep_analyze(property_id: str):
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
    from analysis.scorer import calculate_score, calculate_renewal_value

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
