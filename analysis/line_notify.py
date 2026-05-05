"""LINE Messaging API 推播通知。

設定步驟（你需做一次）：
1. 到 https://developers.line.biz/ 建一個 Provider → 建 Messaging API channel（免費）
2. 在 channel 設定「Issue」一個 long-lived Channel Access Token
3. 在 channel 「Basic settings」最下面拿你自己的 User ID（格式 Uxxxxxxxxxx...）
4. 用手機 LINE 掃 channel QR code 加你的 bot 為好友（不加好友 push 會 fail）
5. .env 加兩個值：
     LINE_CHANNEL_TOKEN=你的 token
     LINE_USER_ID=Uxxxxx...
6. 本機 + VM 各加一份 .env 設定（本機用本機收，VM 用 VM 收 — 通常 VM 才需要設定，本機可以不設）

未設定 token / user_id 時 push_line 會 silent skip，不會 raise。
"""
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

LINE_API_PUSH_URL = "https://api.line.me/v2/bot/message/push"


def push_line(message: str, user_id: Optional[str] = None) -> bool:
    """推播一則文字訊息到指定 LINE user。
    回傳 True 表示送出成功，False 表示 silent skip 或失敗。"""
    token = os.getenv("LINE_CHANNEL_TOKEN", "").strip()
    target = (user_id or os.getenv("LINE_USER_ID", "")).strip()
    if not token or not target:
        logger.debug("LINE 未設定 (LINE_CHANNEL_TOKEN / LINE_USER_ID)，skip")
        return False
    try:
        import httpx
        # LINE 文字訊息單則上限 5000 chars，超過截斷
        if len(message) > 4900:
            message = message[:4900] + "..."
        payload = {
            "to": target,
            "messages": [{"type": "text", "text": message}],
        }
        r = httpx.post(
            LINE_API_PUSH_URL,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            timeout=10,
        )
        if r.status_code == 200:
            logger.info("LINE push 成功")
            return True
        logger.warning("LINE push 失敗 status=%d body=%s", r.status_code, r.text[:300])
        return False
    except Exception as e:
        logger.warning("LINE push exception: %s", e)
        return False


DEFAULT_LINE_TEMPLATE = """🏠您好，發現高價值物件：

📍 {city}{district}
   {address_with_floor}
💰 總價：{price_wan} 萬

📊 都更試算倍數：
{scenarios_text}

觸發：{scenario} {multiple} 倍

🔗 來源連結：
{sources_text}"""


def _build_template_vars(doc: dict, multiple: float, scenario: str,
                          rv2: Optional[dict] = None) -> dict:
    """準備所有可用變數給 template format 用。"""
    # 發送時間（台灣時區，admin 在模板用 {send_time} 代入）
    try:
        from datetime import datetime, timezone, timedelta
        _tw_now = datetime.now(timezone(timedelta(hours=8)))
        send_time = _tw_now.strftime("%m/%d %H:%M")
    except Exception:
        send_time = ""

    addr = doc.get("address_inferred") or doc.get("address") or doc.get("title") or "(地址未知)"
    city = doc.get("city") or ""
    district = doc.get("district") or ""
    # Google Maps 連結 (用完整地址 city+district+addr 做關鍵字搜尋)
    try:
        from urllib.parse import quote
        full_addr = f"{city}{district}{addr}"
        address_map_link = f"https://www.google.com/maps/search/?api=1&query={quote(full_addr)}"
    except Exception:
        address_map_link = ""
    # 系統內 detail page 深層連結 (前端讀 ?id= 會自動打開該物件 detail drawer)
    doc_id = doc.get("id") or doc.get("source_id") or ""
    base_url = os.getenv("PUBLIC_BASE_URL", "https://taipei.retty-ai.com")
    detail_page_link = f"{base_url}/?id={doc_id}" if doc_id else base_url
    price_wan = round((doc.get("price_ntd") or 0) / 10000)
    _fl = doc.get("floor")
    _tf = doc.get("total_floors")
    if _fl and _tf:
        floor_str = f"{_fl}/{_tf}F"
    elif _tf:
        floor_str = f"{_tf}F（總樓層）"
    elif _fl:
        floor_str = f"{_fl}F"
    else:
        floor_str = ""
    addr_with_floor = f"{addr}（{floor_str}）" if floor_str else addr

    sources_arr = doc.get("sources") or []
    src_lines = "\n".join(
        f"  • {s.get('name', '?')}: {s.get('url', '')}"
        for s in sources_arr
        if s.get("url") and s.get("alive") is not False
    ) or "  • (無連結)"

    scenarios = (rv2 or {}).get("scenarios") or {}
    scen_lines = []
    for name, s in scenarios.items():
        m = s.get("multiple")
        if m is not None:
            scen_lines.append(f"  • {name}: {m:.2f} 倍")
    scen_text = "\n".join(scen_lines) or "  • (無試算)"

    return {
        "address": addr,
        "address_with_floor": addr_with_floor,
        "city": city,
        "district": district,
        "floor_str": floor_str,
        "price_wan": f"{price_wan:,}",
        "building_age": doc.get("building_age") or "",
        "land_area_ping": doc.get("land_area_ping") or "",
        "building_area_ping": doc.get("building_area_ping") or "",
        "zoning": doc.get("zoning") or "",
        "scenario": scenario or "",
        "multiple": f"{multiple:.2f}" if multiple is not None else "",
        "scenarios_text": scen_text,
        "sources_text": src_lines,
        "send_time": send_time,
        "address_map_link": address_map_link,
        "detail_page_link": detail_page_link,
    }


def render_template(template: str, doc: dict, multiple: float, scenario: str,
                     rv2: Optional[dict] = None) -> str:
    """用變數 dict format template；未知變數會 raise KeyError 給呼叫端 catch。"""
    vars_dict = _build_template_vars(doc, multiple, scenario, rv2)
    # str.format_map + defaultdict-ish 防 KeyError 反而想 strict — 讓呼叫端知道哪個變數寫錯
    return template.format(**vars_dict)


def _load_message_template() -> str:
    """讀 admin 設定的自訂模板；fallback DEFAULT。"""
    try:
        from database.db import get_firestore
        cfg = get_firestore().collection("settings").document("line_config").get()
        if cfg.exists:
            tpl = (cfg.to_dict() or {}).get("message_template")
            if tpl and tpl.strip():
                return tpl
    except Exception as e:
        logger.warning("讀 line_config.message_template 失敗：%s", e)
    return DEFAULT_LINE_TEMPLATE


def notify_high_value_property(doc: dict, multiple: float, scenario: str,
                                rv2: Optional[dict] = None) -> bool:
    """高價值物件通知 — 物件 ≥ 門檻倍數時呼叫一次。
    doc: 物件 dict（要含 address / price_ntd / sources / id 等）
    multiple: 觸發的最大倍數
    scenario: 觸發的情境（危老 / 都更 / 防災都更）
    rv2: 即時算出來的 renewal_v2 dict（含 scenarios）— 不從 doc 讀，因為 DB 不存

    模板由 admin 在 /admin → LINE 通知 → 訊息模板 自訂；未設定 fallback DEFAULT_LINE_TEMPLATE。
    """
    template = _load_message_template()
    try:
        msg = render_template(template, doc, multiple, scenario, rv2)
    except Exception as e:
        logger.warning("LINE 模板 render 失敗 (fallback default)：%s", e)
        msg = render_template(DEFAULT_LINE_TEMPLATE, doc, multiple, scenario, rv2)
    return push_line(msg)
