"""
LINE 通知系統 endpoints — Sprint #2 step 7 / C 從 api/app.py 拆出。

16 個 endpoint：
  Webhook (公開):
    - POST /api/line/webhook              LINE Messaging API 回呼 (HMAC 驗證)
  狀態 / 設定:
    - GET  /admin/line/status             token / target / 倍數門檻 / 通知統計
    - POST /admin/line/target             設 push 目標 ID
    - GET  /admin/line/recent_events      webhook 最近收到的 source ID 清單
    - GET  /admin/line/webhook_diag       上次簽章驗證失敗診斷
    - GET  /admin/line/secret_fingerprint LINE channel secret sha256 fingerprint
    - POST /admin/line/secret             設 LINE channel secret
    - POST /admin/line/threshold          設倍數門檻 + 觸發情境
    - POST /admin/line/skip_flags         設「不可觸發」抗性/法拍 skip flag
    - POST /admin/line/road_blacklist     設黑名單路段
    - GET  /admin/line/notifications      列發送紀錄 (含 total 給翻頁)
  訊息模板:
    - GET  /admin/line/template
    - POST /admin/line/template
    - POST /admin/line/template_preview
    - POST /admin/line/template_test_send
  測試:
    - POST /admin/line/test               送一則測試訊息

7 個 Pydantic 模型 (全 LINE 專用)：
  LineTargetReq / LineSecretReq / LineThresholdReq / LineSkipFlagsReq /
  LineRoadBlacklistReq / LineTemplateReq / LineTemplatePreviewReq

Module-local helper / constant：
  _get_line_channel_secret / _VALID_TRIGGER_SCENARIOS / DEFAULT_LINE_TEMPLATE
"""
import os
import json
import hashlib
import hmac
import base64
import logging
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from google.cloud.firestore_v1.base_query import FieldFilter

from api.auth import require_admin
from database.db import get_firestore, get_col
from database.time_utils import now_tw_iso

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Pydantic 模型 ────────────────────────────────────────────────────────
class LineTargetReq(BaseModel):
    target_id: str   # "U..." / "C..." / "R..." LINE 任一類型 ID


class LineSecretReq(BaseModel):
    channel_secret: str   # LINE Console > Basic settings > Channel secret 那串


class LineThresholdReq(BaseModel):
    threshold: float
    trigger_scenario: Optional[str] = None   # "危老" / "都更" / "防災都更"；不傳則保留原值


class LineSkipFlagsReq(BaseModel):
    skip_remote_area: bool = True
    skip_unsuitable: bool = True
    skip_foreclosure: bool = True
    skip_floors_5plus: bool = True
    skip_basement: bool = True


class LineRoadBlacklistReq(BaseModel):
    road_blacklist: List[str] = []


class LineTemplateReq(BaseModel):
    template: str


class LineTemplatePreviewReq(BaseModel):
    template: str


# ── Module constants ────────────────────────────────────────────────────
_VALID_TRIGGER_SCENARIOS = {"危老", "都更", "防災都更"}

DEFAULT_LINE_TEMPLATE = """🏠您好，發現高價值物件：

📍 {city}{district}
   {address_with_floor}
💰 總價：{price_wan} 萬

📊 都更試算倍數：
{scenarios_text}

觸發：{scenario} {multiple} 倍

🔗 來源連結：
{sources_text}"""


# ── Helpers ─────────────────────────────────────────────────────────────
def _get_line_channel_secret() -> str:
    """讀 LINE Channel Secret (用於 webhook HMAC 驗證)。
    優先序：Firestore settings/line_config.channel_secret > env LINE_CHANNEL_SECRET。
    走 Firestore 是因為 user 沒辦法 SSH 改 .env，從 admin UI 直接貼 secret 存進 DB 最方便。"""
    try:
        cfg = get_firestore().collection("settings").document("line_config").get()
        if cfg.exists:
            v = (cfg.to_dict() or {}).get("channel_secret", "")
            if v:
                return v.strip()
    except Exception:
        pass
    return os.getenv("LINE_CHANNEL_SECRET", "").strip()


# ── Endpoints ────────────────────────────────────────────────────────────
@router.get("/admin/line/status")
async def admin_line_status(admin: dict = Depends(require_admin)):
    """LINE 通知設定狀態：token / user_id 是否設定 + 倍數門檻 + 最近通知統計。"""
    token = os.getenv("LINE_CHANNEL_TOKEN", "").strip()
    secret = _get_line_channel_secret()
    user_id_env = os.getenv("LINE_USER_ID", "").strip()
    target_id_db = ""   # 從 Firestore 讀的 (admin UI 設的，優先序高於 env)
    # 讀門檻（預設 2.8）+ 不可觸發旗標（admin 勾選，預設全勾）
    threshold = 2.8
    skip_flags = {
        "skip_remote_area": True,
        "skip_unsuitable": True,
        "skip_foreclosure": True,
        "skip_floors_5plus": True,
        "skip_basement": True,
    }
    trigger_scenario = "都更"
    road_blacklist = []
    try:
        cfg = get_firestore().collection("settings").document("line_config").get()
        if cfg.exists:
            cfg_data = cfg.to_dict() or {}
            threshold = float(cfg_data.get("threshold_multiple", 2.8))
            trigger_scenario = cfg_data.get("trigger_scenario") or "都更"
            target_id_db = (cfg_data.get("target_id") or "").strip()
            for k in skip_flags:
                if k in cfg_data:
                    skip_flags[k] = bool(cfg_data.get(k))
            _bl = cfg_data.get("road_blacklist")
            if isinstance(_bl, list):
                road_blacklist = [str(x) for x in _bl if x]
    except Exception: pass
    notified_count = 0
    last_notified = None
    try:
        docs = list(get_col().where(filter=FieldFilter("line_notified_at", ">", "")).limit(500).stream())
        notified_count = len(docs)
        if docs:
            last_notified = max(d.to_dict().get("line_notified_at", "") for d in docs)
    except Exception as e:
        logger.warning(f"line status query: {e}")
    effective_target = target_id_db or user_id_env
    target_type = "個人 (U)" if effective_target.startswith("U") else \
                  "群組 (C)" if effective_target.startswith("C") else \
                  "多人聊天室 (R)" if effective_target.startswith("R") else \
                  "未設定" if not effective_target else "未知"
    return {
        "configured": bool(token and effective_target),
        "token_set": bool(token),
        "token_preview": (token[:6] + "..." + token[-4:]) if len(token) > 10 else "(empty)",
        "secret_set": bool(secret),
        "user_id_set": bool(effective_target),
        "user_id_preview": (effective_target[:6] + "..." + effective_target[-3:]) if len(effective_target) > 10 else "(empty)",
        "target_id_source": "Firestore (admin UI)" if target_id_db else ("env LINE_USER_ID" if user_id_env else "未設定"),
        "target_type": target_type,
        "threshold_multiple": threshold,
        "trigger_scenario": trigger_scenario,
        "skip_flags": skip_flags,
        "notified_property_count": notified_count,
        "last_notified_at": last_notified,
        "trigger_threshold": f"{trigger_scenario} ≥ {threshold} 倍",
        "road_blacklist": road_blacklist,
    }


@router.post("/api/line/webhook")
async def line_webhook(request: Request):
    """LINE Messaging API Webhook 接收端。
    主要用途：bot 被邀進群組 / 群組裡有訊息時，記下 source.groupId 給 admin UI 顯示，
    讓 admin 可以一鍵把 group_id 設為通知目標 (不必 SSH 改 .env)。

    安全：HMAC-SHA256 驗證 X-Line-Signature header (用 channel secret)。
    驗證失敗 → 401。LINE 期望 200 回應，否則會 retry。"""
    # Diagnostic: 寫每次 hit 到 Firestore (給 admin UI 看 server 有沒有被打到)
    def _record_hit(result: str, **extra):
        try:
            data = {"at": now_tw_iso(), "result": result, **extra}
            get_firestore().collection("settings").document("line_webhook_diag").set(data, merge=False)
        except Exception:
            pass
    secret = _get_line_channel_secret()
    if not secret:
        logger.warning("LINE webhook 收到請求但 channel secret 未設定 (Firestore + env 都沒有)")
        _record_hit("secret_not_configured", user_agent=request.headers.get("user-agent", "")[:80])
        return {"status": "secret_not_configured"}
    body_bytes = await request.body()
    sig_header = request.headers.get("x-line-signature", "")
    expected_sig = base64.b64encode(
        hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).digest()
    ).decode("utf-8")
    if not hmac.compare_digest(sig_header, expected_sig):
        sec_fp = hashlib.sha256(secret.encode("utf-8")).hexdigest()[:12]
        body_preview = body_bytes[:60].decode("utf-8", errors="replace")
        _record_hit(
            "signature_mismatch",
            received_sig_prefix=(sig_header[:20] + "...") if sig_header else "(empty)",
            expected_sig_prefix=expected_sig[:20] + "...",
            secret_fingerprint_used=sec_fp,
            secret_len=len(secret),
            body_len=len(body_bytes),
            body_preview_60=body_preview,
            user_agent=request.headers.get("user-agent", "")[:80],
        )
        logger.warning(f"LINE webhook signature mismatch: got={sig_header[:20]}... expected={expected_sig[:20]}... body_len={len(body_bytes)}")
        raise HTTPException(401, "signature mismatch")
    _record_hit(
        "signature_ok",
        body_len=len(body_bytes),
        body_preview_60=body_bytes[:60].decode("utf-8", errors="replace"),
        user_agent=request.headers.get("user-agent", "")[:80],
    )
    try:
        payload = json.loads(body_bytes.decode("utf-8") or "{}")
    except Exception as e:
        logger.warning(f"LINE webhook payload parse fail: {e}")
        return {"status": "bad_payload"}
    events = payload.get("events") or []
    seen = []
    for ev in events:
        src = ev.get("source") or {}
        s_type = src.get("type") or ""
        s_id = src.get("groupId") or src.get("roomId") or src.get("userId") or ""
        if not s_id:
            continue
        seen.append({
            "type": s_type, "id": s_id, "event_type": ev.get("type") or "",
            "at": now_tw_iso(),
        })
    if seen:
        try:
            ref = get_firestore().collection("settings").document("line_recent_events")
            doc = ref.get()
            existing = (doc.to_dict() or {}).get("events", []) if doc.exists else []
            keys_seen = {(e["type"], e["id"]) for e in seen}
            kept = [e for e in existing if (e.get("type"), e.get("id")) not in keys_seen]
            merged = seen + kept
            ref.set({"events": merged[:20], "updated_at": now_tw_iso()}, merge=True)
            for e in seen:
                logger.info(f"[line webhook] {e['event_type']} from {e['type']} {e['id']}")
        except Exception as e:
            logger.exception(f"line webhook write fail: {e}")
    return {"status": "ok", "received": len(events)}


@router.post("/admin/line/target")
async def admin_set_line_target(body: LineTargetReq, admin: dict = Depends(require_admin)):
    """Admin 設定 LINE 推播目標 ID (寫進 Firestore settings/line_config.target_id)。
    push_line() 會優先讀這個值，沒有才 fallback env LINE_USER_ID。完全不用 SSH/restart。"""
    tid = (body.target_id or "").strip()
    if not tid or len(tid) < 5:
        raise HTTPException(400, "target_id 太短")
    if not tid[0].upper() in ("U", "C", "R"):
        raise HTTPException(400, "target_id 必須以 U (個人) / C (群組) / R (多人聊天室) 開頭")
    get_firestore().collection("settings").document("line_config").set({
        "target_id": tid,
        "target_updated_at": now_tw_iso(),
        "target_updated_by_email": admin.get("email") or "",
    }, merge=True)
    logger.warning(f"[admin] {admin.get('email')} 設 LINE target_id={tid[:8]}...{tid[-4:]}")
    return {"status": "ok", "target_id": tid}


@router.get("/admin/line/recent_events")
async def admin_line_recent_events(admin: dict = Depends(require_admin)):
    """回最近 20 筆 webhook 收到的 source IDs，admin UI 顯示讓使用者選為 target。"""
    try:
        doc = get_firestore().collection("settings").document("line_recent_events").get()
        events = (doc.to_dict() or {}).get("events", []) if doc.exists else []
    except Exception:
        events = []
    return {"events": events}


@router.get("/admin/line/webhook_diag")
async def admin_line_webhook_diag(admin: dict = Depends(require_admin)):
    """回最近一次 webhook 簽章驗證失敗的診斷資訊 (secret/body 各自 fingerprint)。
    admin UI 用來判斷:secret 抄錯、body 被中介軟體改了、還是 LINE 那邊送的 sig 不對。"""
    try:
        doc = get_firestore().collection("settings").document("line_webhook_diag").get()
        if not doc.exists:
            return {"has_diag": False}
        d = doc.to_dict() or {}
        d["has_diag"] = True
        return d
    except Exception as e:
        return {"has_diag": False, "error": str(e)}


@router.get("/admin/line/secret_fingerprint")
async def admin_line_secret_fingerprint(admin: dict = Depends(require_admin)):
    """回 LINE channel secret 的 sha256 fingerprint (前 12 字)，
    給 admin 對比 LINE Console 的 Channel secret 有沒有抄錯 (不洩漏完整值)。"""
    s = _get_line_channel_secret()
    if not s:
        return {"set": False, "fingerprint": "", "length": 0, "source": "未設定"}
    fp = hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]
    src = "env LINE_CHANNEL_SECRET"
    try:
        cfg = get_firestore().collection("settings").document("line_config").get()
        if cfg.exists and (cfg.to_dict() or {}).get("channel_secret"):
            src = "Firestore (admin UI 設定)"
    except Exception:
        pass
    return {"set": True, "fingerprint": fp, "length": len(s), "source": src}


@router.post("/admin/line/secret")
async def admin_set_line_secret(body: LineSecretReq, admin: dict = Depends(require_admin)):
    """Admin 把 LINE Channel Secret 寫進 Firestore settings/line_config.channel_secret。
    這樣不用 SSH 改 .env 也能讓 webhook HMAC 驗證 work。"""
    sec = (body.channel_secret or "").strip()
    if not sec or len(sec) < 16:
        raise HTTPException(400, "channel_secret 太短 (LINE 一般 32 字)")
    get_firestore().collection("settings").document("line_config").set({
        "channel_secret": sec,
        "channel_secret_updated_at": now_tw_iso(),
        "channel_secret_updated_by_email": admin.get("email") or "",
    }, merge=True)
    logger.warning(f"[admin] {admin.get('email')} 設 LINE channel_secret (len={len(sec)})")
    return {"status": "ok", "length": len(sec)}


@router.post("/admin/line/threshold")
async def admin_set_line_threshold(body: LineThresholdReq, admin: dict = Depends(require_admin)):
    """Admin 設定 LINE 通知觸發倍數門檻 + 比對的情境（危老/都更/防災都更）。"""
    if body.threshold < 1.0 or body.threshold > 10.0:
        raise HTTPException(400, "threshold 必須介於 1.0~10.0")
    payload = {
        "threshold_multiple": float(body.threshold),
        "updated_at": now_tw_iso(),
        "updated_by_email": admin.get("email") or "",
    }
    if body.trigger_scenario:
        if body.trigger_scenario not in _VALID_TRIGGER_SCENARIOS:
            raise HTTPException(400, f"trigger_scenario 必須是 {_VALID_TRIGGER_SCENARIOS} 之一")
        payload["trigger_scenario"] = body.trigger_scenario
    get_firestore().collection("settings").document("line_config").set(payload, merge=True)
    logger.warning(
        f"[admin] {admin.get('email')} 設 LINE threshold={body.threshold} scenario={body.trigger_scenario}"
    )
    return {
        "status": "ok",
        "threshold_multiple": float(body.threshold),
        "trigger_scenario": body.trigger_scenario,
    }


@router.post("/admin/line/skip_flags")
async def admin_set_line_skip_flags(body: LineSkipFlagsReq, admin: dict = Depends(require_admin)):
    """Admin 設定 LINE「不可觸發」旗標 — 對應 doc 欄位有命中時 skip 通知。
    5 個 flag 預設全 True（4 抗性 + 法拍）。"""
    payload = {
        "skip_remote_area": bool(body.skip_remote_area),
        "skip_unsuitable": bool(body.skip_unsuitable),
        "skip_foreclosure": bool(body.skip_foreclosure),
        "skip_floors_5plus": bool(body.skip_floors_5plus),
        "skip_basement": bool(body.skip_basement),
        "updated_at": now_tw_iso(),
        "updated_by_email": admin.get("email") or "",
    }
    get_firestore().collection("settings").document("line_config").set(payload, merge=True)
    logger.warning(f"[admin] {admin.get('email')} 設 LINE skip_flags = {payload}")
    return {"status": "ok", "skip_flags": {k: payload[k] for k in (
        "skip_remote_area", "skip_unsuitable", "skip_foreclosure", "skip_floors_5plus", "skip_basement"
    )}}


@router.post("/admin/line/road_blacklist")
async def admin_set_line_road_blacklist(body: LineRoadBlacklistReq, admin: dict = Depends(require_admin)):
    """Admin 設定 LINE 通知的黑名單路段。
    地址含任一條目子字串就 skip 通知 (即使倍數達門檻)。"""
    cleaned = [s.strip() for s in (body.road_blacklist or []) if s and s.strip()]
    seen = set()
    unique = []
    for s in cleaned:
        if len(s) > 60:
            raise HTTPException(400, f"條目「{s[:20]}...」過長 (>60 字元)")
        if s not in seen:
            seen.add(s)
            unique.append(s)
    if len(unique) > 100:
        raise HTTPException(400, "最多 100 條黑名單")
    payload = {
        "road_blacklist": unique,
        "updated_at": now_tw_iso(),
        "updated_by_email": admin.get("email") or "",
    }
    get_firestore().collection("settings").document("line_config").set(payload, merge=True)
    logger.warning(f"[admin] {admin.get('email')} 設 LINE road_blacklist {len(unique)} 條：{unique[:10]}{'...' if len(unique)>10 else ''}")
    return {"status": "ok", "road_blacklist": unique}


@router.get("/admin/line/notifications")
async def admin_line_notifications(
    limit: int = 50,
    offset: int = 0,
    admin: dict = Depends(require_admin),
):
    """列出 LINE 通知發送紀錄。支援 limit + offset 翻頁。
    回傳 total_count 給前端算總頁數。"""
    coll = get_firestore().collection("line_notifications")
    try:
        agg = coll.count()
        snap = list(agg.get())
        total = int(snap[0][0].value) if snap and snap[0] else 0
    except Exception:
        total = len(list(coll.limit(1000).stream()))
    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))
    q = coll.order_by("at", direction="DESCENDING")
    if offset > 0:
        q = q.offset(offset)
    docs = list(q.limit(limit).stream())
    items = []
    for d in docs:
        x = d.to_dict() or {}
        x["_id"] = d.id
        items.append(x)
    return {"count": len(items), "total": total, "limit": limit, "offset": offset, "items": items}


@router.get("/admin/line/template")
async def admin_get_line_template(admin: dict = Depends(require_admin)):
    """讀目前的 LINE 通知訊息模板 (admin 自訂；空 → 用 DEFAULT)。"""
    cfg_doc = get_firestore().collection("settings").document("line_config").get()
    template = ""
    if cfg_doc.exists:
        template = (cfg_doc.to_dict() or {}).get("message_template") or ""
    return {
        "template": template or DEFAULT_LINE_TEMPLATE,
        "is_default": not template,
        "default_template": DEFAULT_LINE_TEMPLATE,
        "variables": [
            {"key": "{address}", "desc": "推測地址 (如「虎林街57之15號」)"},
            {"key": "{address_with_floor}", "desc": "地址 + 樓層 (如「虎林街...（1/4F）」)"},
            {"key": "{city}", "desc": "城市 (台北市 / 新北市)"},
            {"key": "{district}", "desc": "行政區"},
            {"key": "{floor_str}", "desc": "樓層字串 (如「1/4F」)"},
            {"key": "{price_wan}", "desc": "售價 (萬，逗號千分位)"},
            {"key": "{building_age}", "desc": "屋齡"},
            {"key": "{land_ping}", "desc": "土地坪數"},
            {"key": "{building_ping}", "desc": "建物坪數"},
            {"key": "{zoning}", "desc": "使用分區"},
            {"key": "{scenario}", "desc": "觸發情境 (危老 / 都更 / 防災都更)"},
            {"key": "{multiple}", "desc": "觸發最大倍數 (如 3.24)"},
            {"key": "{scenarios_text}", "desc": "各情境倍數列表 (多行)"},
            {"key": "{sources_text}", "desc": "來源連結列表 (多行)"},
            {"key": "{send_time}", "desc": "發送日期+時間 (MM/DD HH:MM，台灣時區)"},
            {"key": "{address_map_link}", "desc": "Google Maps 地址連結 (點擊看地圖)"},
            {"key": "{detail_page_link}", "desc": "本系統 detail page 連結 (點擊在 app 內打開該物件)"},
        ],
    }


@router.post("/admin/line/template")
async def admin_set_line_template(body: LineTemplateReq, admin: dict = Depends(require_admin)):
    """admin 儲存自訂訊息模板。空字串 → 還原為 DEFAULT_LINE_TEMPLATE。"""
    tpl = (body.template or "").strip()
    if len(tpl) > 4000:
        raise HTTPException(400, "模板太長（>4000 字元，超過 LINE 單則訊息上限）")
    payload = {
        "message_template": tpl,
        "updated_at": now_tw_iso(),
        "updated_by_email": admin.get("email") or "",
    }
    get_firestore().collection("settings").document("line_config").set(payload, merge=True)
    logger.warning(f"[admin] {admin.get('email')} 更新 LINE message_template ({len(tpl)} 字元)")
    return {"status": "ok", "template": tpl, "is_default": not tpl}


@router.post("/admin/line/template_preview")
async def admin_preview_line_template(body: LineTemplatePreviewReq, admin: dict = Depends(require_admin)):
    """用範例物件 render 模板，回傳預覽訊息字串給前端顯示。"""
    from analysis.line_notify import render_template
    sample_doc = {
        "address_inferred": "虎林街57之15號",
        "city": "台北市",
        "district": "信義區",
        "price_ntd": 11880000,
        "floor": 1,
        "total_floors": 4,
        "building_age": 51,
        "land_area_ping": 10.71,
        "building_area_ping": 38.42,
        "zoning": "第三種商業區",
        "sources": [
            {"name": "永慶", "url": "https://buy.yungching.com.tw/house/7342047", "alive": True},
        ],
    }
    sample_rv2 = {
        "scenarios": {
            "危老": {"multiple": 3.24},
            "都更": {"multiple": 4.49},
        },
    }
    try:
        msg = render_template(body.template or DEFAULT_LINE_TEMPLATE, sample_doc, 4.49, "都更", sample_rv2)
        return {"status": "ok", "preview": msg}
    except KeyError as e:
        return {"status": "error", "message": f"模板含未知變數：{e}"}
    except Exception as e:
        return {"status": "error", "message": f"模板格式錯誤：{e}"}


@router.post("/admin/line/template_test_send")
async def admin_line_template_test_send(body: LineTemplatePreviewReq, admin: dict = Depends(require_admin)):
    """用範例物件 render 模板後實際 push 到 LINE — 給 admin 在 admin 後台「試發」當前未存模板看效果。"""
    from analysis.line_notify import render_template, push_line
    sample_doc = {
        "address_inferred": "虎林街57之15號",
        "city": "台北市",
        "district": "信義區",
        "price_ntd": 11880000,
        "floor": 1,
        "total_floors": 4,
        "building_age": 51,
        "land_area_ping": 10.71,
        "building_area_ping": 38.42,
        "zoning": "第三種商業區",
        "sources": [
            {"name": "永慶", "url": "https://buy.yungching.com.tw/house/7342047", "alive": True},
        ],
    }
    sample_rv2 = {
        "scenarios": {
            "危老": {"multiple": 3.24},
            "都更": {"multiple": 4.49},
        },
    }
    try:
        msg = render_template(body.template or DEFAULT_LINE_TEMPLATE, sample_doc, 4.49, "都更", sample_rv2)
    except KeyError as e:
        return {"status": "error", "message": f"模板含未知變數：{e}"}
    except Exception as e:
        return {"status": "error", "message": f"模板格式錯誤：{e}"}
    msg = "🧪【試發】此為模板測試，非真實物件\n\n" + msg
    ok = push_line(msg)
    if ok:
        return {"status": "ok", "message": "已試發到 LINE，請查看訊息"}
    if not os.getenv("LINE_CHANNEL_TOKEN") or not os.getenv("LINE_USER_ID"):
        return {"status": "error", "message": "未設定 LINE_CHANNEL_TOKEN 或 LINE_USER_ID"}
    return {"status": "error", "message": "送出失敗（檢查 token 是否有效、bot 是否已加好友）"}


@router.post("/admin/line/test")
async def admin_line_test(admin: dict = Depends(require_admin)):
    """送一則 LINE 測試訊息給 LINE_USER_ID。確認 token / user_id 設定 OK。"""
    from analysis.line_notify import push_line
    msg = (
        f"🔔 都更神探R 測試訊息\n"
        f"\n"
        f"觸發者：{admin.get('email', 'admin')}\n"
        f"時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"\n"
        f"如果你看到這則訊息，表示 LINE 通知設定 OK ✓"
    )
    ok = push_line(msg)
    if ok:
        return {"status": "ok", "message": "測試訊息已送出，請查看 LINE"}
    if not os.getenv("LINE_CHANNEL_TOKEN") or not os.getenv("LINE_USER_ID"):
        return {"status": "error", "message": "未設定 LINE_CHANNEL_TOKEN 或 LINE_USER_ID 環境變數"}
    return {"status": "error", "message": "送出失敗（檢查 token 是否有效、bot 是否已加好友）"}
