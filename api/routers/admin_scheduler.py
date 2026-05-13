"""
Admin scheduler / retry / verify_alive / update_prices endpoints
— Sprint #2 step 6 從 api/app.py 拆出。

14 個 endpoint：
  Scheduler:
    - GET  /admin/scheduler/status        (含很多 module-level state 讀取)
    - GET  /admin/scheduler/history
    - POST /admin/scheduler/toggle
    - POST /admin/scheduler/config
  Verify alive:
    - GET  /admin/verify_alive/progress
    - POST /admin/verify_alive/run-now
  Update prices:
    - POST /admin/update_district_prices (兩個 alias decorator)
    - POST /admin/update_district_prices/run-now
  Retry queue:
    - GET    /admin/retry_queue
    - POST   /admin/retry_queue/{queue_id}/run-now
    - DELETE /admin/retry_queue/{queue_id}
    - DELETE /admin/retry_queue
  Run logs / sessions:
    - GET  /admin/run-logs
    - GET  /admin/run-sessions

Pydantic 模型：SchedulerToggleReq / CommandSpec / SchedulerConfigReq

依賴：許多 module-level state (_scheduler_last_run_at 等) 都 mutable scalar globals，
每次都用 `import api.app as _app; _app.X` 方式拿最新值 (不能 `from api.app import X`
那會抓 import 時的 snapshot)。同 module-level 常數 (SCHEDULER_ALLOWED_*) 也走 late
import，保持 single source of truth。
"""
import asyncio
import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from google.cloud.firestore_v1.base_query import FieldFilter

from api.auth import require_admin
from database.db import get_firestore
from database.time_utils import now_tw_iso, now_tw

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Pydantic models (搬自 api/app.py) ───────────────────────────────────
class SchedulerToggleReq(BaseModel):
    enabled: bool
    type: Optional[str] = None   # "scan" / "verify_alive" / None=兩個都 toggle（legacy）


class CommandSpec(BaseModel):
    # type: "scan" = 掃描新物件，"verify_alive" = 偵測下架, "update_prices" = 更新預售屋單價,
    #       "gis_overlay_refresh" = 更新圖層 cache (清掉指定 layer disk cache)
    type: Optional[str] = "scan"
    interval_hr: Optional[int] = None   # per-command interval；None 表示用全域 default

    # 「掃描新物件」用：
    districts: Optional[List[str]] = None
    limit: Optional[int] = None
    sources: Optional[List[str]] = None
    source: Optional[str] = None        # 舊欄位 backward compat
    # 「更新圖層」用：
    layers: Optional[List[str]] = None


class SchedulerConfigReq(BaseModel):
    interval_hr: int
    commands: List[CommandSpec]


# ── Endpoints ───────────────────────────────────────────────────────────
@router.post("/admin/update_district_prices")
@router.post("/admin/update_district_prices/run-now")
async def admin_update_district_prices(admin: dict = Depends(require_admin)):
    """admin 觸發：下載最新 LVR + parse 預售屋 CSV 重算各區單價中位數寫 Firestore。
    支援兩個路徑：
      - POST /admin/update_district_prices （legacy）
      - POST /admin/update_district_prices/run-now （admin UI 立即執行按鈕）
    """
    from api.app import _run_update_prices_command
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


@router.get("/admin/scheduler/status")
async def scheduler_status(admin: dict = Depends(require_admin)):
    """回傳定時 batch 目前狀態 + 設定，給 admin UI 顯示。"""
    # late import — 多 module-level state 要用 _app.X 取最新值 (不可 from import)
    import api.app as _app
    from config import TARGET_REGIONS
    _app._ensure_layer_display_map()
    cfg = _app._load_scheduler_config()
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
        "gis_overlay_refresh_enabled": bool(cfg.get("gis_overlay_refresh_enabled", legacy_en)),
        "interval_hr": int(cfg.get("interval_hr") or 1),
        "commands": cmds_with_state,
        "last_run_at": _app._scheduler_last_run_at,
        "last_status": _app._scheduler_last_status,
        "next_tick_at": _app._scheduler_next_tick_at,
        "currently_running": _app._scrape_running,
        "last_verify_alive_at": state.get("last_verify_alive_at"),
        "last_verify_alive_archived": state.get("last_verify_alive_archived"),
        # UI 選項用
        "allowed_districts": [d for r in TARGET_REGIONS.values() for d in (r.get("districts") or {}).keys()],
        "allowed_interval_hr": list(_app.SCHEDULER_ALLOWED_INTERVAL_HR),
        "allowed_verify_interval_hr": list(_app.SCHEDULER_VERIFY_INTERVAL_HR),
        "allowed_update_prices_interval_hr": list(_app.SCHEDULER_UPDATE_PRICES_INTERVAL_HR),
        "allowed_gis_overlay_interval_hr": list(_app.SCHEDULER_GIS_OVERLAY_INTERVAL_HR),
        "gis_overlay_layers": [_app._gis_overlay_layer_meta(n) for n in _app._gis_overlay_layers_for_admin()],
        "max_commands": _app.SCHEDULER_MAX_COMMANDS,
        "max_districts_per_command": _app.SCHEDULER_MAX_DISTRICTS_PER_CMD,
        "inter_command_sleep_sec": _app.SCHEDULER_INTER_COMMAND_SLEEP_SEC,
    }


@router.get("/admin/scheduler/history")
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


@router.get("/admin/verify_alive/progress")
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


@router.get("/admin/run-logs")
async def admin_run_logs(limit: int = 200, trigger_prefix: Optional[str] = None,
                         admin: dict = Depends(require_admin)):
    """回傳最近的 action log（手動 + scheduler 都記錄）。
    每筆含 trigger / action / source_id / doc_id / message / details。
    trigger_prefix 可過濾：'manual' / 'scheduler' / 'verify_alive' / 'retry_queue'。"""
    from database.run_log import list_recent
    items = list_recent(limit=min(int(limit), 1000), trigger_prefix=trigger_prefix)
    return {"count": len(items), "items": items}


@router.get("/admin/run-sessions")
async def admin_run_sessions(limit: int = 50, admin: dict = Depends(require_admin)):
    """回傳最近的「執行紀錄」session（依 batch_start/end 或 verify_alive_start/end 分組）。
    每個 session 含 trigger / started_at / ended_at / status / counts / actions。
    actions 是該 session 內所有 per-object log 條目（new/enrich/dup_merge/archive/prune 等）。"""
    from database.run_log import list_sessions
    sessions = list_sessions(limit=min(int(limit), 200))
    return {"count": len(sessions), "items": sessions}


@router.post("/admin/scheduler/toggle")
async def scheduler_toggle(body: SchedulerToggleReq, admin: dict = Depends(require_admin)):
    """啟用/停用定時 batch（per-type）。存 Firestore 讓 runtime toggle 跨重啟保留。
    啟用時會 wake loop → 倒數立刻重算（避免沿用關閉期間累積的舊倒數）。
    type 帶值（"scan" / "verify_alive"）時只 toggle 該 type；不帶 = 兩個都 toggle（legacy）。"""
    import api.app as _app
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
    elif t == "gis_overlay_refresh":
        update["gis_overlay_refresh_enabled"] = body.enabled
    else:
        # legacy：四個都 toggle，並維持舊 enabled 欄位
        update["enabled"] = body.enabled
        update["scan_enabled"] = body.enabled
        update["verify_alive_enabled"] = body.enabled
        update["update_prices_enabled"] = body.enabled
        update["gis_overlay_refresh_enabled"] = body.enabled
    get_firestore().collection("settings").document("scheduler").set(update, merge=True)
    logger.warning("[scheduler] %s 設定 type=%s enabled=%s", admin.get("email"), t or "all", body.enabled)
    if body.enabled and _app._sched_wake_event is not None:
        _app._sched_wake_event.set()
    return {"status": "ok", "type": t or "all", "enabled": body.enabled}


@router.post("/admin/scheduler/config")
async def scheduler_set_config(body: SchedulerConfigReq, admin: dict = Depends(require_admin)):
    """套用 admin UI 整份排程設定（commands list；每命令各自 interval）。"""
    import api.app as _app
    from config import TARGET_REGIONS
    # 容量限制：scan 最多 SCHEDULER_MAX_COMMANDS 個、verify_alive 1 個、update_prices 1 個
    type_counts = {"scan": 0, "verify_alive": 0, "update_prices": 0, "gis_overlay_refresh": 0}
    for c in body.commands:
        t = (c.type or "scan").lower()
        if t in type_counts:
            type_counts[t] += 1
    if type_counts["scan"] > _app.SCHEDULER_MAX_COMMANDS:
        raise HTTPException(400, f"掃描命令最多 {_app.SCHEDULER_MAX_COMMANDS} 個")
    if type_counts["verify_alive"] > 1:
        raise HTTPException(400, "偵測下架命令最多 1 個")
    if type_counts["update_prices"] > 1:
        raise HTTPException(400, "更新預售屋單價命令最多 1 個")
    if type_counts["gis_overlay_refresh"] > 1:
        raise HTTPException(400, "更新圖層命令最多 1 個")
    allowed = {d for r in TARGET_REGIONS.values() for d in (r.get("districts") or {}).keys()}
    cleaned = []
    VALID_SOURCES = ("591", "yongqing", "sinyi")
    # 圖層更新合法 layer 清單（從 gis_overlay 拿）
    from api.gis_overlay import _disk_cache_layers
    valid_overlay_layers = set(_disk_cache_layers())
    for idx, c in enumerate(body.commands):
        cmd_type = (c.type or "scan").lower()
        if cmd_type not in ("scan", "verify_alive", "update_prices", "gis_overlay_refresh"):
            raise HTTPException(400, f"命令 {idx+1}：type 必須是 scan / verify_alive / update_prices / gis_overlay_refresh（收到 {c.type!r}）")

        if cmd_type == "scan":
            # 掃描新物件：要 districts + limit + sources
            districts = c.districts or []
            if not districts:
                continue   # 空命令跳過
            if len(districts) > _app.SCHEDULER_MAX_DISTRICTS_PER_CMD:
                raise HTTPException(400, f"命令 {idx+1} 最多選 {_app.SCHEDULER_MAX_DISTRICTS_PER_CMD} 區")
            for d in districts:
                if d not in allowed:
                    raise HTTPException(400, f"命令 {idx+1}:「{d}」不是合法行政區")
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
                raise HTTPException(400, f"命令 {idx+1}:limit 必須 1~300")
            raw_sources = c.sources if c.sources else ([c.source] if c.source else ["591"])
            cmd_sources = [(s or "").lower() for s in raw_sources if s]
            for s in cmd_sources:
                if s not in VALID_SOURCES:
                    raise HTTPException(400, f"命令 {idx+1}:source 必須是 {VALID_SOURCES} 之一")
            if not cmd_sources:
                raise HTTPException(400, f"命令 {idx+1}:至少要勾一個來源")
            cmd_sources = [s for s in VALID_SOURCES if s in cmd_sources]
            interval_hr = int(c.interval_hr or 3)
            if interval_hr not in _app.SCHEDULER_ALLOWED_INTERVAL_HR:
                raise HTTPException(400, f"命令 {idx+1}:interval_hr 必須為 {list(_app.SCHEDULER_ALLOWED_INTERVAL_HR)}")
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
            if interval_hr not in _app.SCHEDULER_UPDATE_PRICES_INTERVAL_HR:
                raise HTTPException(
                    400,
                    f"命令 {idx+1}(更新單價):interval_hr 必須為 {list(_app.SCHEDULER_UPDATE_PRICES_INTERVAL_HR)}",
                )
            cleaned.append({
                "type": "update_prices",
                "interval_hr": interval_hr,
            })
        elif cmd_type == "gis_overlay_refresh":
            # 更新圖層 cache：要 interval_hr + layers list
            interval_hr = int(c.interval_hr or 720)
            if interval_hr not in _app.SCHEDULER_GIS_OVERLAY_INTERVAL_HR:
                raise HTTPException(
                    400,
                    f"命令 {idx+1}(更新圖層):interval_hr 必須為 {list(_app.SCHEDULER_GIS_OVERLAY_INTERVAL_HR)}",
                )
            layers = c.layers or []
            invalid = [n for n in layers if n not in valid_overlay_layers]
            if invalid:
                raise HTTPException(400, f"命令 {idx+1}(更新圖層):不存在的 layer {invalid}")
            cleaned.append({
                "type": "gis_overlay_refresh",
                "interval_hr": interval_hr,
                "layers": list(layers),
            })
        else:
            # verify_alive：只要 interval_hr
            interval_hr = int(c.interval_hr or 24)
            if interval_hr not in _app.SCHEDULER_VERIFY_INTERVAL_HR:
                raise HTTPException(400, f"命令 {idx+1}(偵測下架):interval_hr 必須為 {list(_app.SCHEDULER_VERIFY_INTERVAL_HR)}")
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
        nxt = _app._next_interval_boundary(now_dt, ihr)
        state_updates[f"cmd_{idx}"] = {
            "next_due_at": nxt.isoformat(),
            # 保留既有 last_run_at（如果有）
            **{k: v for k, v in (_app._cmd_state_get(idx).items())
               if k not in ("next_due_at",)}
        }
    if state_updates:
        get_firestore().collection("settings").document("scheduler_state").set(state_updates, merge=True)
    logger.warning("[scheduler] %s 套用設定 commands=%s （next_due_at 已對齊整點）",
                   admin.get("email"), cleaned)
    if _app._sched_wake_event is not None:
        _app._sched_wake_event.set()
    return {"status": "ok", "commands": cleaned}


# ── 失敗重試佇列（admin） ───────────────────────────────────────────────────
@router.get("/admin/retry_queue")
async def get_retry_queue(admin: dict = Depends(require_admin)):
    """列出所有重試佇列 entry（pending + abandoned）"""
    from database.retry_queue import list_all_for_admin
    items = list_all_for_admin(limit=200)
    return {"count": len(items), "items": items}


@router.post("/admin/retry_queue/{queue_id}/run-now")
async def run_retry_now(queue_id: str, admin: dict = Depends(require_admin)):
    """手動立刻重抓某筆 — admin 不想等 10 分鐘。"""
    from api.routers.admin_scrape import _scrape_single_url
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


@router.delete("/admin/retry_queue/{queue_id}")
async def delete_retry_queue_entry(queue_id: str, admin: dict = Depends(require_admin)):
    """admin 從重試佇列移除（放棄不再重試）。"""
    from database.retry_queue import dequeue
    dequeue(queue_id)
    logger.warning(f"[retry-queue] admin {admin.get('email')} 移除 queue entry {queue_id}")
    return {"status": "ok"}


@router.post("/admin/verify_alive/run-now")
async def admin_verify_alive_now(admin: dict = Depends(require_admin)):
    """admin 手動立即執行偵測下架（不等排程）。背景跑、不阻塞 response。"""
    from api.app import _run_verify_alive_command
    async def _do():
        try:
            await _run_verify_alive_command(trigger_label="verify_alive_manual")
            logger.warning(f"[verify-alive] {admin.get('email')} 手動觸發完成")
        except Exception as e:
            logger.exception(f"[verify-alive] 手動觸發失敗: {e}")
    asyncio.create_task(_do())
    return {"status": "started"}


@router.delete("/admin/retry_queue")
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
