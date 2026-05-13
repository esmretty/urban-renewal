"""
Scrape / manual_analyze endpoints — Sprint 2 step F (拆 router 收尾) 從 api/app.py 拆出。

6 個 endpoint：
  - POST /api/scrape                      批次爬取觸發 (admin)
  - GET  /api/scrape/status               SSE 進度推播
  - POST /api/scrape_url                  單筆 URL 分析 (user)
  - POST /api/manual_analyze              手動地址分析 (user)
  - POST /admin/scrape/kill               中斷正在跑的 batch (admin)
  - POST /admin/scrape/kill_session       中斷 zombie session (admin)

4 個 Pydantic：ScrapeRequest / ManualAnalyzeReq / ScrapeUrlRequest / KillSessionReq

9 個 helper：_safe_put_progress / _reset_scrape_progress / _sse_generator /
            _run_scrape_task / _scrape_and_analyze (1248 行核心 batch 引擎) /
            _run_manual_analysis / _scrape_single_url / 兩個 admin_kill_*

mutable scalar state (_scrape_running / _cancel_requested / _url_sem 等) 仍在
api/app.py 內 (admin_scheduler.py 也透過 _app._scrape_running 讀)，這支 router 用
`import api.app as _app; _app.X` pattern 取最新值。

_SCRAPE_PROGRESS_FILE 路徑常數搬過來。
"""
from __future__ import annotations
import asyncio
import json
import logging
from typing import Optional, AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException, Request, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.auth import require_admin, get_current_user
from database.db import get_col, get_firestore, get_user_watchlist, get_user_manual
from database.time_utils import now_tw, now_tw_iso
from config import BASE_DIR

logger = logging.getLogger(__name__)
router = APIRouter()


# Scrape 進度跨 process 共享檔（prod 跑 3 個 uvicorn worker，POST 跟 SSE 可能落在不同 worker；
# 用 file (O_APPEND atomic write < 4KB) 取代 in-memory asyncio.Queue 才能跨 process 看到同一份）
_SCRAPE_PROGRESS_FILE = BASE_DIR / "data" / "scrape_progress.jsonl"

def _safe_put_progress(msg_json: str):
    """寫一條 progress 訊息到共享檔（跨 uvicorn worker process）。

    舊版用 in-memory asyncio.Queue；prod 跑 3 個 worker 時 POST 跟 SSE 落在不同
    process → SSE 永遠收不到訊息（只看得到初始「連線成功」+ heartbeat）。
    改用 O_APPEND 寫檔，所有 worker 共讀同一份 file。
    file 在新 batch 啟動時由 _reset_scrape_progress 截斷，size 不會無限長。"""
    try:
        _SCRAPE_PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with _SCRAPE_PROGRESS_FILE.open("a", encoding="utf-8") as f:
            f.write(msg_json + "\n")
    except Exception as e:
        logger.warning(f"_safe_put_progress write failed: {e}")



def _reset_scrape_progress():
    """新 batch 啟動前清空 progress file，讓 SSE 從乾淨狀態 tail。"""
    try:
        _SCRAPE_PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _SCRAPE_PROGRESS_FILE.write_text("", encoding="utf-8")
    except Exception as e:
        logger.warning(f"_reset_scrape_progress failed: {e}")



class ScrapeRequest(BaseModel):
    headless: bool = True
    districts: list[str] = []
    limit: int = 0
    source: str = "591"        # "591" / "yongqing" / "sinyi"
    # 分析門檻（超過則存 pending，不跑分析）
    max_floors: Optional[int] = None
    max_total_price_wan: Optional[int] = None
    max_price_per_building_ping_wan: Optional[int] = None
    max_price_per_land_ping_wan: Optional[int] = None



@router.post("/api/scrape")
async def trigger_scrape(req: ScrapeRequest, user: dict = Depends(require_admin)):
    import api.app as _app
    from api.app import _ensure_user_profile
    """觸發 591 批次爬取（僅 admin）。"""
    if _app._scrape_running:
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
    _reset_scrape_progress()
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



@router.get("/api/scrape/status")
async def scrape_status():
    """SSE：推播爬取進度訊息。"""
    return StreamingResponse(
        _sse_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )



async def _sse_generator() -> AsyncGenerator[str, None]:
    """Tail _SCRAPE_PROGRESS_FILE 把訊息推給瀏覽器。
    用 byte offset 追蹤已讀位置，500ms 輪詢一次新內容。
    file 被新 batch 截斷時自動 reset offset。30s 沒新訊息 yield heartbeat 防 proxy timeout。"""
    yield "data: {\"msg\": \"連線成功，等待爬取任務...\"}\n\n"
    offset = 0
    idle_ticks = 0
    POLL_SEC = 0.5
    HEARTBEAT_AFTER = int(30 / POLL_SEC)
    while True:
        try:
            if _SCRAPE_PROGRESS_FILE.exists():
                size = _SCRAPE_PROGRESS_FILE.stat().st_size
                if size < offset:
                    offset = 0   # 被截斷，重新 tail
                if size > offset:
                    with _SCRAPE_PROGRESS_FILE.open("rb") as f:
                        f.seek(offset)
                        chunk = f.read()
                    offset += len(chunk)
                    last_was_terminal = False
                    for raw in chunk.split(b"\n"):
                        if not raw.strip():
                            continue
                        try:
                            line = raw.decode("utf-8")
                        except UnicodeDecodeError:
                            continue
                        yield f"data: {line}\n\n"
                        if '"done"' in line or '"error"' in line:
                            last_was_terminal = True
                    if last_was_terminal:
                        return
                    idle_ticks = 0
                    continue
            idle_ticks += 1
            if idle_ticks >= HEARTBEAT_AFTER:
                yield "data: {\"msg\": \"heartbeat\"}\n\n"
                idle_ticks = 0
            await asyncio.sleep(POLL_SEC)
        except Exception as e:
            logger.warning(f"SSE tail loop error: {e}")
            await asyncio.sleep(1)



async def _run_scrape_task(headless: bool = True, districts: list = None, limit: int = 30, thresholds: dict = None, triggered_by_uid: Optional[str] = None, source: str = "591", trigger_label: str = "manual_batch"):
    import api.app as _app
    _app._scrape_running = True
    _app._cancel_requested = False
    # 重設共享 cancel flag — 讓 scraper inner loops 也能 check
    from scraper.cancel_state import reset as _cancel_reset
    _cancel_reset()
    import json

    def progress(msg: str, percent: Optional[float] = None, new_item: bool = False):
        payload = {"msg": msg}
        if percent is not None:
            payload["percent"] = round(percent, 1)
        if new_item:
            payload["new_item"] = True
        # file IO 是 thread-safe (O_APPEND atomic)，可直接從 worker thread 寫，
        # 不必再 loop.call_soon_threadsafe bounce 回 event loop
        _safe_put_progress(json.dumps(payload, ensure_ascii=False))

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
        _app._scrape_running = False
    return stats or {}



def _scrape_and_analyze(headless: bool, progress_callback, districts: list = None, limit: int = 30, thresholds: dict = None, triggered_by_uid: Optional[str] = None, source: str = "591", trigger_label: str = "manual_batch"):
    import api.app as _app
    from api.app import _is_replacement_change, _safe_doc, _verify_and_prune_sources
    """同步執行爬取 + 分析（在 asyncio.to_thread 中跑）。
    source: "591" / "yongqing" / "sinyi"。決定要爬哪個來源。"""
    from database.run_log import log_action
    districts = districts or []
    log_action(trigger_label, "batch_start", message=f"{source} / {','.join(districts) or 'all'} / limit={limit}",
               details={"source": source, "districts": districts, "limit": limit, "triggered_by_uid": triggered_by_uid})
    from scraper.scraper_591 import scrape_591

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
            # verify_and_fix_road 改名過的物件，DB.address 是 fixed 後的路名
            # 但 591 listing 永遠回 raw 路名 → dedup 比對需要 fallback raw 對 raw
            "address_road_fixed": _d.get("address_road_fixed"),
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
            # 路名比對：先試 ex.address；若 ex 有 address_road_fixed (verify_and_fix 改名過) →
            # 再試「改名前的 raw 路名」(對齊 591 listing 給的原始路名 — 否則永遠 mismatch)
            if _extract_road_name(ex["address"]) != road:
                raw_road = _extract_road_name(((ex.get("address_road_fixed") or {}).get("from")) or "")
                if not raw_road or raw_road != road:
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
    filtered_out = result.get("filtered_out") or []   # listing 階段被過濾掉的（總層數>5、商辦等）

    # log 每一筆 listing 階段被過濾的物件 — 讓 admin 從 session 詳情頁看得到「為什麼 limit=10 但結果只 8 筆」
    for fo in filtered_out:
        try:
            log_action(trigger_label, "skip_filter",
                       source_id=fo.get("source_id"),
                       message=f"列表過濾：{fo.get('reason') or '不符目標類型'}（{(fo.get('title') or '')[:25]}）",
                       details={
                           "url": fo.get("url"),
                           "title": fo.get("title"),
                           "district": fo.get("district"),
                           "city": fo.get("city"),
                           "reason": fo.get("reason"),
                           "filter_stage": fo.get("filter_stage"),
                       })
        except Exception: pass

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
    from scraper.zoning_lookup import lookup_zoning
    from analysis.lvr_index import ensure_fresh as _lvr_refresh

    # 確保 LVR 索引為最新（失敗不阻塞爬取）
    try:
        _lvr_refresh()
    except Exception as e:
        logger.warning(f"LVR 索引刷新失敗（不影響爬取）：{e}")

    new_count = 0
    enrich_count = 0
    skip_dup_count = 0
    total_to_analyze = len(new_items)

    # ── 每個 item 的處理結果（給 batch_end log 用，讓 admin 看「無動作的 URL + 原因」）
    _outcomes = []   # [{src_id, url, title, district, outcome, reason}, ...]
    def _record_outcome(it: dict, outcome: str, reason: str = ""):
        _outcomes.append({
            "src_id": it.get("source_id") or "",
            "url": it.get("url") or "",
            "title": (it.get("title") or "")[:60],
            "district": it.get("district") or "",
            "outcome": outcome,    # new / enrich / dup_merged / delisted / skip_non_apartment / replaced / cancelled / error
            "reason": reason[:120] if reason else "",
        })

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
    # 注意：原本用 `with get_browser_context() as ocr_ctx` 包整個迴圈 — Chromium 跨頁
    # 的 memory cache 會累積（每筆 +200~400MB），跑滿 quota 必 OOM。改用 yield (ctx, browser)
    # 版本，loop 內每 RECYCLE_EVERY 筆 close+reopen context 強制 Chromium 釋放 page cache。
    from scraper.browser_manager import get_browser_context_with_browser, _build_ctx
    from scraper.cancel_state import take_version as _cancel_take_version, is_cancelled as _cancel_is
    _my_cancel_version = _cancel_take_version()   # batch 開頭記住自己的 version；kill 後 bump 對舊 batch 觸發 break
    RECYCLE_EVERY = 5
    with get_browser_context_with_browser(headless=headless) as (ocr_ctx_init, _browser):
        ocr_ctx = ocr_ctx_init
        for idx, item in enumerate(new_items, 1):
            if _cancel_is(_my_cancel_version) or _app._cancel_requested:
                progress_callback("⛔ 使用者取消", 100)
                break
            # 每 RECYCLE_EVERY 筆關掉舊 ctx 重開（Chromium memory cache 釋放）
            if idx > 1 and (idx - 1) % RECYCLE_EVERY == 0:
                try:
                    ocr_ctx.close()
                except Exception as _ce:
                    logger.warning(f"  [mem] ctx close err: {_ce}")
                try:
                    import gc as _gc_recyc; _gc_recyc.collect()
                except Exception: pass
                ocr_ctx = _build_ctx(_browser)
                try:
                    import psutil as _psutil_r
                    _rss_r = _psutil_r.Process().memory_info().rss / 1048576
                    logger.info(f"  [mem] recycled ctx at iter#{idx} post_recycle_rss={_rss_r:.0f}MB")
                except Exception: pass
            try:
                pct = 50 + (idx / max(total_to_analyze, 1)) * 50
                is_enrich = item.get("_enrich_existing", False)
                is_force_reanalyze = item.get("_force_reanalyze", False)
                src_id = item["source_id"]
                # leak-hunt point A：每筆進迴圈時的 RSS
                try:
                    import psutil as _psutil
                    _rss_a = _psutil.Process().memory_info().rss / 1048576
                    logger.info(f"[mem] iter#{idx}/{total_to_analyze} src={src_id} A_start rss={_rss_a:.0f}MB")
                except Exception:
                    _rss_a = 0.0

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
                                    # 先試 mobile API（純 HTTP、無 Playwright）
                                    _vd = None
                                    try:
                                        from scraper.scraper_591_mobile import fetch_mobile_detail
                                        _hid_dup = src_id.split("591_", 1)[-1] if src_id.startswith("591_") else src_id
                                        _md = fetch_mobile_detail(_hid_dup)
                                        if _md:
                                            _md.pop("_mobile_raw", None)
                                            _vd = {
                                                "land_area_ping": _md.get("land_area_ping"),
                                                "building_age": _md.get("building_age"),
                                                "zoning": _md.get("zoning"),
                                                "total_floors": None,   # mobile 給 floor str，下游再 parse
                                                "floor": _md.get("floor"),
                                            }
                                    except Exception as _me_dup:
                                        logger.warning(f"  dup mobile API 失敗 {src_id}: {_me_dup}")
                                    # mobile 失敗才走 Playwright OCR
                                    if not _vd:
                                        _dup_detail = screenshot_detail_page(ocr_ctx, item["url"], src_id)
                                        if _dup_detail and not getattr(_dup_detail, "delisted", False):
                                            _dup_shot, _, _ = _dup_detail[:3]
                                            _dup_house = getattr(_dup_detail, "house_path", None)
                                            _crop = _dup_house or _dup_shot
                                            if _crop:
                                                _vd = extract_full_detail_from_screenshot(_crop)
                                    if _vd:
                                        _fill = {k: _vd[k] for k in _missing if _vd.get(k) not in (None, "", 0)}
                                        if _fill:
                                            col.document(dup_sid).update(_fill)
                                            progress_callback(
                                                f"  ↻ 重複物件補資料 {dup_sid}: {', '.join(_fill.keys())}",
                                                pct,
                                            )
                                            dd.update(_fill)   # 本地 dd 同步
                                except Exception as _de:
                                    logger.warning(f"dup enrich 失敗 {src_id}: {_de}")

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
                        from database.run_log import build_doc_log_details
                        log_action(trigger_label, "dup_merge",
                                   source_id=src_id, doc_id=dup_sid,
                                   message=f"併入 {dup_sid}（{item.get('title','')[:25]}）",
                                   details=build_doc_log_details(item, None, merged_into=dup_sid))
                        _record_outcome(item, "dup_merged", f"併入既有 doc {dup_sid}")
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
                                from database.run_log import log_action, build_doc_log_details
                                log_action(trigger_label, "cross_source",
                                           source_id=src_id, doc_id=yc_dup_id,
                                           message=f"{_src_name}併入既有 doc",
                                           details=build_doc_log_details(item, dd, merged_into=yc_dup_id, source=_src_name))
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
                    _record_outcome(item, "new", f"{_src_name} 新物件 doc={yc_doc_id}")
                    continue   # 不走下面 591 OCR 流程

                # 591：先試 mobile BFF API (純 JSON 無防爬，feature parity)
                # 若 mobile API 抓到 → **完全跳過 Playwright 詳情頁截圖** + Vision OCR (省 ~100s/筆)
                # 失敗 → fallback 原 Playwright + Vision OCR 流程
                _mobile_data_591 = None
                try:
                    from config import USE_591_MOBILE_API as _USE_MOBILE
                except ImportError:
                    _USE_MOBILE = True
                if _USE_MOBILE:
                    try:
                        from scraper.scraper_591_mobile import fetch_mobile_detail
                        _hid = src_id.split("591_", 1)[-1] if src_id.startswith("591_") else src_id
                        _mobile_data_591 = fetch_mobile_detail(_hid)
                        if _mobile_data_591:
                            _mobile_data_591.pop("_mobile_raw", None)
                            progress_callback(f"  ⚡ 591 mobile API 抓到 detail，跳過 Playwright", pct)
                    except Exception as _me:
                        logger.warning(f"  591 mobile API 例外，fallback Playwright ({src_id}): {_me}")
                        _mobile_data_591 = None

                # cancel check：admin kill 在 batch 內也能在 step 邊界 break
                if _cancel_is(_my_cancel_version):
                    progress_callback("⛔ 使用者取消（截圖前）", 100)
                    break

                # 共用變數（mobile path 跟 Playwright path 都會填）
                shot_path = None
                _addr_crop = None
                _house_crop = None
                community_addr = ""
                page_coords = None
                _detail_ret = None

                if _mobile_data_591:
                    # ─── Mobile fast path：純 HTTP，無 Playwright ───
                    m591 = _mobile_data_591
                    community_addr = m591.get("community_address") or ""
                    page_coords = (m591.get("source_latitude"), m591.get("source_longitude"))
                    if page_coords[0] and page_coords[1]:
                        item["source_latitude"] = page_coords[0]
                        item["source_longitude"] = page_coords[1]
                    # listing API photo_url 是 !400x300.jpg，前端詳情頁放大會糊
                    # → 用 mobile photos[0]（已升級到 !2000x.water2.jpg）覆蓋主圖
                    if m591.get("photos"):
                        item["image_url"] = m591["photos"][0]
                    # _community_raw 對應 desktop 詳情頁「社區」label 的 RAW value
                    # （仲介在社區 label 寫「【店長推薦】XX」是法拍特徵），不是 title!
                    # 591 mobile API 真正對應「社區」label 的 raw value 是 casesname 欄位
                    # （「community」是純社區名通常空字串，「casesname」才是仲介廣告詞欄）。
                    # scraper_591_mobile 把 casesname 規範化成 community_raw 欄位 expose 出來
                    item["_community_raw"] = m591.get("community_raw") or m591.get("community") or ""
                    # _raw_text 給 detect_foreclosure rule 1（標題含 # + 代理人）
                    _fc_text_591 = (m591.get("title") or "") + "\n" + (m591.get("remark") or "")
                    if _fc_text_591.strip():
                        item["_raw_text"] = _fc_text_591
                    # 上架/更新時間
                    if m591.get("updated_at_591"):
                        item["_updated_text"] = m591["updated_at_591"]
                    if m591.get("published_at") and not item.get("_published_text"):
                        item["_published_text"] = m591["published_at"]
                else:
                    # ─── Fallback：Playwright 詳情頁截圖 ───
                    progress_callback(f"  📷 截圖詳情頁...", pct)
                    _detail_ret = screenshot_detail_page(ocr_ctx, item["url"], src_id)
                    # 下架偵測：listing 列表還在快取顯示卡片，但詳情頁已是 404 → 刪 DB 並跳過
                    if getattr(_detail_ret, "delisted", False) or (isinstance(_detail_ret, tuple) and len(_detail_ret) >= 2 and _detail_ret[1] == "__DELISTED__"):
                        try:
                            get_col().document(src_id).delete()
                            logger.warning(f"已移除下架物件 {src_id}")
                        except Exception as _de:
                            logger.warning(f"移除下架物件失敗 {src_id}: {_de}")
                        progress_callback(f"  ⚠️ 物件已下架，跳過", pct)
                        log_action(trigger_label, "skip_delisted",
                                   source_id=src_id,
                                   message=f"物件已下架（591 詳情頁回 404）：{(item.get('title') or '')[:25]}",
                                   details={"url": item.get("url"), "title": item.get("title"),
                                            "district": item.get("district")})
                        _record_outcome(item, "delisted", "591 詳情頁回 404 — 已從 DB 移除")
                        continue
                    shot_path, community_addr, page_coords = _detail_ret[:3]
                    _addr_crop = getattr(_detail_ret, "addr_path", None)
                    _house_crop = getattr(_detail_ret, "house_path", None)
                    if page_coords and page_coords[0] and page_coords[1]:
                        item["source_latitude"] = page_coords[0]
                        item["source_longitude"] = page_coords[1]
                    item["_community_raw"] = getattr(_detail_ret, "community_raw", "") or ""
                    _body = getattr(_detail_ret, "body_text", "") or ""
                    if _body:
                        item["_raw_text"] = _body
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
                if _mobile_data_591:
                    # === Mobile API path：跳過 Vision OCR，用 mobile JSON 填 vision_data ===
                    # 拿到的 floor / area / age / land 直接放，下游邏輯 (parse_floor_range,
                    # 地址決策, etc.) 完全不變
                    vision_data = {
                        "building_area_ping": _mobile_data_591.get("building_area_ping"),
                        "land_area_ping": _mobile_data_591.get("land_area_ping"),
                        "building_age": _mobile_data_591.get("building_age"),
                        "floor": _mobile_data_591.get("floor"),
                        # mobile API 從 region+section+street+addr_number 拼出完整地址
                        # (e.g. 「新北市中和區景平路162號」)；listing API 卡片只到路名級
                        # 此前漏填 → batch path 落地 doc 永遠卡在路名級沒到號
                        "address": _mobile_data_591.get("address") or "",
                        # total_floors 透過 parse_floor_range 從 floor 字串拆出（'B1/5F' → total=5）
                    }
                    # 也補 item — mobile 給的 community_name / 座標 等可立即覆蓋
                    if _mobile_data_591.get("community_name"):
                        item["community_name"] = _mobile_data_591["community_name"]
                    if _mobile_data_591.get("source_latitude") and _mobile_data_591.get("source_longitude"):
                        item["source_latitude"] = _mobile_data_591["source_latitude"]
                        item["source_longitude"] = _mobile_data_591["source_longitude"]
                else:
                    # === Desktop OCR path（保留 fallback）===
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
                # cancel check：Vision 完到 analyze 前
                if _cancel_is(_my_cancel_version):
                    progress_callback("⛔ 使用者取消（Vision 後）", 100)
                    break
                # leak-hunt point B：截圖+Vision OCR 完，看是否吃了大量 RSS
                try:
                    _rss_b = _psutil.Process().memory_info().rss / 1048576
                    logger.info(f"[mem] iter#{idx} src={src_id} B_postVision rss={_rss_b:.0f}MB delta={_rss_b-_rss_a:+.0f}")
                except Exception: pass
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
                # floor 特殊處理：591 listing API 偶爾誤標（例 591_20125871 listing 給 'B1/5F'
                # 但 detail page 描述是「1+2樓店面 + 附屬地下室」實際是 1F）→ 信 Vision OCR
                # detail page 的可見字串。比對：listing 標 B 但 Vision 看到非 B → 採 Vision
                _v_floor = vision_data.get("floor")
                _i_floor = item.get("floor")
                if _v_floor and _i_floor and str(_v_floor).strip() != str(_i_floor).strip():
                    from database.models import is_basement_floor as _is_bsmt
                    _listing_says_basement = _is_bsmt(_i_floor)
                    _vision_says_basement = _is_bsmt(_v_floor)
                    if _listing_says_basement and not _vision_says_basement:
                        logger.warning(f"[{src_id}] listing floor={_i_floor!r} 標地下室但 Vision OCR={_v_floor!r} 看到非地下室 → 採用 Vision (591 listing 誤標)")
                        item["floor"] = _v_floor
                # 源頭已 filter 公寓，直接標公寓（admin 重分析可保留舊 type）
                if not item.get("building_type"):
                    item["building_type"] = "公寓"

                # 地址決策（CLAUDE.md 規則 1：禁止 AI 幻覺、禁止填假地址）：
                # 591「社區」label 是社區名稱可能含假路名（例 591_20123399「中佳北新路二段35號公寓」
                # 路名根本不存在）；「地址」label 真值用 Web Component 包，DOM 看不到，必須 OCR。
                # 規則（user 指定）：
                #   1) 兩個都到號 → 地址欄(OCR)優先；OCR 無效才試社區欄；都無效 → 無資料
                #   2) 只有社區到號 → 驗證；無效當沒資料
                #   3) 只有地址欄到號 → 驗證；無效拿掉號（保留路+巷）
                # 永遠跑 OCR（之前條件式跳過 → 假地址漏網）。
                from database.models import looks_like_real_address as _lkra_dom, extract_district as _extract_dist
                from analysis.claude_analyzer import extract_address_consensus, _clean_address_garbage
                from analysis.geocoder import geocode_with_district as _geo_with_dist

                _dom_addr = item.get("address") or ""
                _ocr_addr = None
                if _addr_crop and item.get("city") and item.get("district"):
                    _ocr_raw = extract_address_consensus(_addr_crop, item["city"], item["district"])
                    if _ocr_raw and _lkra_dom(_ocr_raw, require_number=False):
                        _ocr_addr = _ocr_raw

                def _validate_addr(addr, require_num: bool = True):
                    """確認地址在 city+district 內 geocode 得到（即真實存在）。
                    require_num=True：要含號才算（含號才能精確定位）；False：巷級也接受。"""
                    if not addr:
                        return False
                    if require_num and "號" not in addr:
                        return False
                    full = addr if addr.startswith(item.get("city") or "") else f"{item.get('city','')}{item.get('district','')}{addr}"
                    try:
                        return bool(_geo_with_dist(full))
                    except Exception:
                        return False

                _dom_has_num = bool(_dom_addr) and "號" in _dom_addr and _lkra_dom(_dom_addr, require_number=True)
                _ocr_has_num = bool(_ocr_addr) and "號" in _ocr_addr

                final_addr = None
                final_source = None
                if _ocr_has_num and _dom_has_num:
                    # rule 3: 地址欄(OCR)優先
                    if _validate_addr(_ocr_addr):
                        final_addr, final_source = _ocr_addr, "ocr"
                    elif _validate_addr(_dom_addr):
                        final_addr, final_source = _dom_addr, "dom_community"
                    # 都無效 → final_addr None（後面拿無號的）
                elif _dom_has_num and not _ocr_has_num:
                    # rule 1: 只有社區到號
                    if _validate_addr(_dom_addr):
                        final_addr, final_source = _dom_addr, "dom_community"
                elif _ocr_has_num and not _dom_has_num:
                    # rule 2: 只有地址欄到號
                    if _validate_addr(_ocr_addr):
                        final_addr, final_source = _ocr_addr, "ocr"
                    else:
                        # 拿掉號
                        import re as _re_st
                        _stripped = _re_st.sub(r"\s*\d+(?:之\d+)?號.*$", "", _ocr_addr).rstrip()
                        if _stripped:
                            final_addr, final_source = _stripped, "ocr_no_num"
                # fallback：上面都沒匹配 → 取無號的候選（巷級）
                # user: 最終地址必須真實存在 → 巷級也要 geocode 驗證，假路名直接拒收
                if not final_addr:
                    for cand, src_label in [(_ocr_addr, "ocr_no_num"), (_dom_addr, "dom_no_num")]:
                        if cand:
                            import re as _re_st2
                            _s = _re_st2.sub(r"\s*\d+(?:之\d+)?號.*$", "", cand).rstrip()
                            if _s and _validate_addr(_s, require_num=False):
                                final_addr, final_source = _s, src_label
                                break

                logger.info(f"  地址決策 ({src_id}): dom={_dom_addr!r} ocr={_ocr_addr!r} → final={final_addr!r} src={final_source}")

                if final_addr and final_source == "ocr":
                    # OCR 地址若含「XX區」→ 信 OCR district
                    _ocr_dist = _extract_dist(final_addr)
                    if _ocr_dist and _ocr_dist != item.get("district"):
                        logger.info(f"  [district 修正 OCR] card={item.get('district')!r} → OCR={_ocr_dist!r} ({src_id})")
                        item["district"] = _ocr_dist

                if final_addr:
                    if not final_addr.startswith(item.get("city") or ""):
                        final_addr = f"{item.get('city','')}{item.get('district','')}{final_addr}"
                    final_addr = _clean_address_garbage(final_addr)
                    item["address"] = final_addr
                else:
                    # 兩邊都失敗 → 清掉，下游 LVR / reverse_geo 會試從座標反推
                    item["address"] = ""

                # ─ 詳情頁 scrape 失敗檢查：缺核心欄位（價格 / 行政區）→ 視為頁面沒拿到結構化資料，整筆丟棄
                if not item.get("price_ntd") or not (item.get("district") or "").strip():
                    progress_callback(
                        f"  ⛔ 跳過 scrape 失敗（缺價格或行政區）：{src_id} {(item.get('title') or '')[:25]}",
                        pct,
                    )
                    log_action(trigger_label, "skip_scrape_failed",
                               source_id=src_id,
                               message=f"詳情頁 scrape 失敗（缺價格或行政區）：{(item.get('title') or '')[:25]}",
                               details={"url": item.get("url"), "title": item.get("title"),
                                        "district": item.get("district"),
                                        "price_ntd": item.get("price_ntd"),
                                        "missing": [
                                            f for f, v in (("price_ntd", item.get("price_ntd")),
                                                            ("district", item.get("district"))) if not v
                                        ]})
                    _record_outcome(item, "scrape_failed", "詳情頁缺價格或行政區")
                    continue

                # ─ 只用總樓層過濾（591 filter 已選公寓；OCR 建物類型不可靠，易誤判）──
                # total_floors >= 6 視為非公寓（公寓定義：5F 以下無電梯）
                # reanalyze 抓到非公寓 → 同時 delete 既存 doc（避免「曾經誤入庫」物件
                # 再次 reanalyze 時仍留在 DB）
                _total_f = item.get("total_floors") or 0
                try: _total_f = int(_total_f)
                except Exception: _total_f = 0
                if _total_f >= 6:
                    try:
                        get_col().document(src_id).delete()
                        logger.warning(f"已從 DB 移除非公寓 {src_id} (總樓層 {_total_f}F)")
                    except Exception as _de:
                        logger.warning(f"移除非公寓 doc 失敗 {src_id}: {_de}")
                    progress_callback(
                        f"  ⛔ 移除非公寓（{_total_f}F≥6）：{(item.get('title') or '')[:25]}",
                        pct,
                    )
                    log_action(trigger_label, "skip_non_apartment",
                               source_id=src_id,
                               message=f"非公寓（總樓層 {_total_f}F ≥ 6）— 已從 DB 移除：{(item.get('title') or '')[:25]}",
                               details={"url": item.get("url"), "title": item.get("title"),
                                        "district": item.get("district"),
                                        "total_floors": _total_f})
                    _record_outcome(item, "non_apartment", f"總樓層 {_total_f}F ≥ 6 — 已移除")
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
                            from database.run_log import build_doc_log_details as _bld_log
                            log_action(trigger_label, "dup_merge",
                                       source_id=src_id, doc_id=best_old['_id'],
                                       message=f"併入 {best_old['_id']} 並補欄位",
                                       details=_bld_log(item, best_old, merged_into=best_old['_id']))
                            _record_outcome(item, "dup_merged", f"併入 {best_old['_id']} 並補欄位")
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
                            from database.run_log import build_doc_log_details as _bld_log
                            log_action(trigger_label, "dup_merge",
                                       source_id=src_id, doc_id=best_old['_id'],
                                       message=f"併入 {best_old['_id']}（捨棄）",
                                       details=_bld_log(item, best_old, merged_into=best_old['_id'], discarded=True))
                            _record_outcome(item, "dup_discarded", f"重複捨棄 → 併入 {best_old['_id']}")
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
                        from database.run_log import build_doc_log_details as _bld_log
                        log_action(trigger_label, "enrich",
                                   source_id=src_id, doc_id=existing_doc_id,
                                   message=f"補欄位{('（衝突: '+ ','.join(conflicts) + '）') if conflicts else ''}",
                                   details=_bld_log(item, merged, conflicts=conflicts))
                        _record_outcome(item, "enrich", f"補欄位 → doc {existing_doc_id}")
                    else:
                        logger.error(f"enrich 找不到 doc id for source_id={src_id}，跳過")
                        log_action(trigger_label, "error", source_id=src_id,
                                   message="enrich 找不到 doc",
                                   details={"url": item.get("url"), "title": item.get("title")})
                        _record_outcome(item, "error", "enrich 找不到 doc")
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
                # leak-hunt point C：analyze_single_property 完成（含 LVR / AI / zoning / road_width）
                try:
                    _rss_c = _psutil.Process().memory_info().rss / 1048576
                    logger.info(f"[mem] iter#{idx} src={src_id} C_postAnalyze rss={_rss_c:.0f}MB delta={_rss_c-_rss_a:+.0f}")
                except Exception: pass

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
                        _record_outcome(item, "replaced", f"換物件 → 內容併入 doc {new_dup_id}")
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
                    _record_outcome(item, "new", f"新物件入庫 → doc {target_doc_id}")
                _existing_items.append({
                    "id": target_doc_id,
                    "source_keys": list(doc_data.get("source_keys") or []),
                    "price_ntd": doc_data.get("price_ntd") or item.get("price_ntd"),
                    "building_area_ping": doc_data.get("building_area_ping") or item.get("building_area_ping"),
                    "address": doc_data.get("address") or item.get("address") or "",
                    "floor": doc_data.get("floor") or item.get("floor"),
                    "total_floors": doc_data.get("total_floors") or item.get("total_floors"),
                    # 同 batch 內後續物件 dedup 用：if first item 走 verify_and_fix 改名，address 是 fixed 後的路名
                    # 後續同物件 raw 路名跟它對不上 → 靠 address_road_fixed.from fallback (find_duplicate 內處理)
                    "address_road_fixed": doc_data.get("address_road_fixed"),
                })
                status_msg = {
                    "done": f"✓ 已入庫 {new_count} 筆：{(doc_data.get('address_inferred') or doc_data.get('address') or '')[:30]}",
                    "skipped": f"  ⏭ 跳過：{result.get('skip_reason', '')}",
                    "foreclosure": f"  ⚖ 法拍：{', '.join(result.get('foreclosure_reasons') or [])}",
                }.get(result["status"], "")
                progress_callback(status_msg, pct, new_item=(result["status"] == "done"))

            except Exception as e:
                logger.exception(f"分析失敗 {item.get('source_id')}: {e}")
            finally:
                # 不論該筆成功 / skip / raise，一律清掉 OCR 截圖
                # （_detail / _addr / _house / *_tile_*）— 避免 exception path 漏清
                # 累積到硬碟。analyze_single_property 內部成功路徑也會清，重複呼叫無害。
                try:
                    from api.analysis_pipeline import _cleanup_ephemeral_screenshots as _cleanup_shots_iter
                    _cleanup_shots_iter(src_id)
                except Exception: pass
                # 每筆做完強制 gc — 釋放 PIL Image / Vision response / Anthropic SDK
                # 持有的 base64 image 等大物件，避免跨 iter 累積成 OOM
                try:
                    import gc as _gc
                    _gc.collect()
                except Exception: pass
                # leak-hunt point D：gc 完印 RSS（看 net growth）
                try:
                    _rss_d = _psutil.Process().memory_info().rss / 1048576
                    logger.info(f"[mem] iter#{idx} src={src_id} D_iterEnd_postGC rss={_rss_d:.0f}MB delta={_rss_d-_rss_a:+.0f} (net per-iter growth)")
                except Exception: pass

    progress_callback(
        f"完成：新增 {new_count} 筆，補資料 {enrich_count} 筆，重複捨棄 {skip_dup_count} 筆，價格變動 {len(price_updates)} 筆",
        100,
    )
    # 即使新增/補資料都是 0，也把所有 attempted item 的 outcome（含 reason）寫進 details，
    # 讓 admin「執行紀錄」能看到「分析了哪些 URL，為什麼都沒入庫」
    _outcome_summary = {}
    for o in _outcomes:
        _outcome_summary[o["outcome"]] = _outcome_summary.get(o["outcome"], 0) + 1
    log_action(trigger_label, "batch_end",
               message=f"新 {new_count} / 補 {enrich_count} / 重複 {skip_dup_count} / 改價 {len(price_updates)}",
               details={"new_count": new_count, "enrich_count": enrich_count,
                        "skip_dup_count": skip_dup_count, "price_update_count": len(price_updates),
                        "outcome_summary": _outcome_summary,
                        "outcomes": _outcomes[:200],   # 全部 attempted items；上限 200 防 Firestore 1MB doc 上限
                        "total_attempted": len(_outcomes)})
    return {
        "new_count": new_count,
        "enrich_count": enrich_count,
        "skip_dup_count": skip_dup_count,
        "price_update_count": len(price_updates),
    }


# ── 深度分析（Phase 2） ───────────────────────────────────────────────────────


class ManualAnalyzeReq(BaseModel):
    city: str
    district: str
    address: str
    building_area_ping: Optional[float] = None
    land_area_ping: Optional[float] = None
    price_wan: Optional[float] = None
    use_source: Optional[str] = "auto"   # auto / user / lvr（mismatch 時前端選）



@router.post("/api/manual_analyze")
async def analyze_manual(req: ManualAnalyzeReq, user: dict = Depends(get_current_user)):
    """
    手動輸入地址觸發分析（私人）。
    不寫中央；結果存 users/{uid}/manual/{manual_id}。
    """
    from api.app import _ensure_user_profile, _safe_doc
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
    from api.app import _safe_doc
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



class ScrapeUrlRequest(BaseModel):
    url: str



@router.post("/api/scrape_url")
async def scrape_url(req: ScrapeUrlRequest, user: dict = Depends(get_current_user)):
    import api.app as _app
    from api.app import _ensure_user_profile
    """
    單一 591 URL 送出：
      1) 先查中央，如果已經分析過（done 且無 error）→ 直接把 src_id 加進本人 watchlist，不重跑 pipeline
      2) 否則 → 跑 pipeline 寫中央，再加 watchlist
    """
    _app._cancel_requested = False
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
    if _app._url_sem is None:
        return {"status": "error", "message": "server 初始化中，請稍後"}
    _app._url_waiting += 1
    try:
        async with _app._url_sem:
            _app._url_waiting -= 1
            _app._url_inflight += 1
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
                _app._url_inflight -= 1
    except Exception:
        # semaphore 沒拿到就被例外中斷的情況，調整 waiting
        if _app._url_waiting > 0:
            _app._url_waiting -= 1
        raise



class KillSessionReq(BaseModel):
    trigger: str
    started_at: str
    source: Optional[str] = None



@router.post("/admin/scrape/kill_session")
async def admin_kill_session(body: KillSessionReq, admin: dict = Depends(require_admin)):
    import api.app as _app
    """中斷單一 session（標記完成、補 batch_end log）。
    不真的殺背景 process（process 可能還在跑），只清理 admin UI 顯示的 zombie row。
    若該 session 還在跑、且正是當前 _app._scrape_running 的那一個 → 也設 cancel flag。"""
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
    if _app._scrape_running:
        from scraper.cancel_state import bump_version as _cancel_bump
        _cancel_bump()
        _app._cancel_requested = True
        # 不再強制 reset _app._scrape_running，讓 batch 自己 break + finally cleanup
    logger.warning(f"[admin] {admin.get('email')} 中斷 session {body.trigger} {body.started_at}")
    return {"status": "ok", "message": f"已標記 session 完成（{src}）"}



@router.post("/admin/scrape/kill")
async def admin_kill_scrape(admin: dict = Depends(require_admin)):
    import api.app as _app
    """中斷正在跑的 batch — version-based cancel。
    舊設計：bump 全域 boolean flag + 強制 reset _app._scrape_running=False 讓 scheduler 起新 batch
    → race：新 batch 起來時 reset flag = False，舊 batch 看不到 cancel 信號 → 兩 batch 並行。
    新設計：bump cancel_state 的 version counter；舊 batch 在下個 step check 點發現 version
    變動 → break；break 後 _run_scrape_task 的 finally 自動 reset _app._scrape_running，
    scheduler 才會在下一輪 tick 起新 batch（拿到的 version 已是 bump 後新值，不被舊 kill 影響）。
    """
    _app._cancel_requested = True
    from scraper.cancel_state import bump_version as _cancel_bump
    _cancel_bump()   # 全部 in-flight batch 在下個 check 點會 break
    was_running = _app._scrape_running
    # 不再強制 reset _app._scrape_running — 讓舊 batch 自己 break + finally 處理 cleanup
    # （之前 reset 會讓 scheduler 立刻起新 batch 跟舊 batch 並行）

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



def _scrape_single_url(url: str, src_id: str, is_reanalyze: bool = False, *, mark_user_url: bool = True):
    from api.app import _scrape_single_url_591_inner, _scrape_single_url_sinyi, _scrape_single_url_yongqing
    """同步：開瀏覽器 + 抓單一 URL + 跑分析。
    is_reanalyze=True：admin 重新分析路徑，跳過「公寓 only」「目標區域」等過濾，
                      強制更新既有 doc（admin 特權，用於修正舊資料）。
    支援 591 / 永慶 / 信義 三種 URL（依 src_id prefix 分流）。
    591 path 用 outer try/finally 保證 cleanup — 即使 inner raise 或走任何 return path
    都會清掉 OCR 截圖（_detail / _addr / _house / tile_*）。"""
    # 永慶 URL → 走永慶單筆分析路徑（純 HTTP + Playwright，不需 Vision OCR）
    if src_id.startswith("yongqing_"):
        return _scrape_single_url_yongqing(url, src_id, is_reanalyze, mark_user_url=mark_user_url)
    if src_id.startswith("sinyi_"):
        return _scrape_single_url_sinyi(url, src_id, is_reanalyze, mark_user_url=mark_user_url)
    # 591 path：用 wrapper 保證 cleanup（避免漏掉 success return / uncaught exception 兩種 path）
    try:
        return _scrape_single_url_591_inner(url, src_id, is_reanalyze, mark_user_url=mark_user_url)
    finally:
        try:
            from api.analysis_pipeline import _cleanup_ephemeral_screenshots as _cleanup_shots_outer
            _cleanup_shots_outer(src_id)
        except Exception: pass
