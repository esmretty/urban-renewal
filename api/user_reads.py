"""已讀紀錄 server-side 端點 — 取代純 localStorage，跨裝置同步。

隔離原則（同 gis_overlay / cadastral_search 設計）：
  - 整個 module 自包含
  - app.py 1 行 `app.include_router(user_reads.router)` 掛載
  - revert 時刪本 file + 拿掉那 1 行 include 即可

API:
  GET /api/me/reads → { items: {id: ts}, count }
  POST /api/me/reads body {ids: [...]} → batch mark (server now ts)，最多 1000 筆/批

Storage: users/{uid}.reads = { property_id: iso_ts } (Firestore field map)
  ~ 5000 entries × 54 chars ~ 270KB，遠低於 Firestore 1MB doc limit
"""
from __future__ import annotations

import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.auth import get_current_user
from database.db import get_user_doc
from database.time_utils import now_tw_iso

logger = logging.getLogger(__name__)
router = APIRouter()

# 每筆 POST 上限 (避免一次塞太多)
_MAX_BATCH = 1000


class MarkReadsBody(BaseModel):
    ids: List[str]


@router.get("/api/me/reads")
async def get_my_reads(user: dict = Depends(get_current_user)):
    """回該 user 的所有 已讀紀錄。"""
    uid = user["uid"]
    try:
        snap = get_user_doc(uid).get()
    except Exception as e:
        logger.warning(f"get_my_reads fetch doc 失敗 uid={uid}: {e}")
        raise HTTPException(502, "讀取已讀紀錄失敗")
    if not snap.exists:
        return {"items": {}, "count": 0}
    data = snap.to_dict() or {}
    reads = data.get("reads") or {}
    return {"items": reads, "count": len(reads)}


@router.post("/api/me/reads")
async def mark_reads(body: MarkReadsBody, user: dict = Depends(get_current_user)):
    """Batch mark：用 nested dict + merge=True，server 同步寫入 users/{uid}.reads.{id}。"""
    uid = user["uid"]
    raw_ids = body.ids or []
    # filter empty/非字串/過長
    ids = []
    for i in raw_ids:
        if not isinstance(i, str):
            continue
        s = i.strip()
        if not s or len(s) > 200:
            continue
        ids.append(s)
    if not ids:
        return {"marked": 0}
    if len(ids) > _MAX_BATCH:
        raise HTTPException(400, f"一次最多 {_MAX_BATCH} 筆")
    ts = now_tw_iso()
    reads_update = {i: ts for i in ids}
    try:
        # set merge=True 自動 handle doc 不存在的情況 (新建)
        # nested dict {"reads": {id: ts}} 配 merge → 只 merge reads 內的 keys，不動其他 field
        get_user_doc(uid).set({"reads": reads_update}, merge=True)
    except Exception as e:
        logger.warning(f"mark_reads write 失敗 uid={uid}: {e}")
        raise HTTPException(502, "寫入已讀紀錄失敗")
    return {"marked": len(ids)}
