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


def notify_high_value_property(doc: dict, multiple: float, scenario: str,
                                rv2: Optional[dict] = None) -> bool:
    """高價值物件通知 — 物件 ≥ 門檻倍數時呼叫一次。
    doc: 物件 dict（要含 address / price_ntd / sources / id 等）
    multiple: 觸發的最大倍數
    scenario: 觸發的情境（危老 / 都更 / 防災都更）
    rv2: 即時算出來的 renewal_v2 dict（含 scenarios）— 不從 doc 讀，因為 DB 不存
    """
    addr = doc.get("address_inferred") or doc.get("address") or doc.get("title") or "(地址未知)"
    city = doc.get("city") or ""
    district = doc.get("district") or ""
    price_wan = round((doc.get("price_ntd") or 0) / 10000)

    # 列出所有來源連結（只列 alive=True 的，無 alive flag 的視為 alive）
    sources_arr = doc.get("sources") or []
    src_lines = "\n".join(
        f"  • {s.get('name', '?')}: {s.get('url', '')}"
        for s in sources_arr
        if s.get("url") and s.get("alive") is not False
    ) or "  • (無連結)"

    # 列出已算出的所有情境倍數（從呼叫端傳入，不從 doc 讀；renewal_v2 不存 DB）
    scenarios = (rv2 or {}).get("scenarios") or {}
    scen_lines = []
    for name, s in scenarios.items():
        m = s.get("multiple")
        if m is not None:
            scen_lines.append(f"  • {name}: {m:.2f} 倍")
    scen_text = "\n".join(scen_lines) or "  • (無試算)"

    msg = (
        f"🏠您好，發現高價值物件：\n"
        f"\n"
        f"📍 {city}{district}\n"
        f"   {addr}\n"
        f"💰 總價：{price_wan:,} 萬\n"
        f"\n"
        f"📊 都更試算倍數：\n"
        f"{scen_text}\n"
        f"\n"
        f"觸發：{scenario} {multiple:.2f} 倍\n"
        f"\n"
        f"🔗 來源連結：\n"
        f"{src_lines}"
    )
    return push_line(msg)
