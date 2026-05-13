"""
物件個人 override / action endpoints — Sprint #2 step 4 從 api/app.py 拆出。

9 個 endpoint，都是 POST /api/properties/{id}/* 給用戶調整自己 watchlist 的覆寫值：
  - /hide                  軟/硬刪除（依物件來源）
  - /bonus                 容積獎勵率覆寫 (危老/都更)
  - /rebuild_coeff         重建係數覆寫
  - /floor_premium         樓層加成覆寫
  - /road_width            臨路寬度覆寫
  - /zoning_ratios         多分區坪數比例覆寫
  - /desired_price         欲出價設定
  - /inferred_choice       推測地址候選挑選
  - /new_house_price       新成屋單價覆寫 + 即時試算結果

策略：core helper _user_override_ref / _read_user_property 留 app.py
(多處使用) — 本檔用 late import 取。
"""
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.auth import get_current_user
from database.db import get_col, get_user_manual

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Pydantic 模型 ────────────────────────────────────────────────────────
class NewHousePriceOverride(BaseModel):
    new_house_price_wan_per_ping: float


class RoadWidthOverride(BaseModel):
    road_width_m: float


class BonusOverride(BaseModel):
    which: str       # "weishau" | "dugen"
    value: float     # e.g., 0.30 / 0.50 / 0.80


class CoeffOverride(BaseModel):
    value: float


class FloorPremiumOverride(BaseModel):
    floor_premium: float   # 0.00 ~ 0.80


class DesiredPriceOverride(BaseModel):
    desired_price_wan: float


class InferredChoiceOverride(BaseModel):
    address: str


# ── Endpoints ────────────────────────────────────────────────────────────
@router.post("/api/properties/{property_id:path}/bonus")
async def override_bonus(property_id: str, body: BonusOverride, user: dict = Depends(get_current_user)):
    from api.app import _user_override_ref
    field = "bonus_weishau" if body.which == "weishau" else "bonus_dugen"
    _user_override_ref(user, property_id).set({field: body.value}, merge=True)
    return {"status": "ok", field: body.value}


@router.post("/api/properties/{property_id:path}/rebuild_coeff")
async def override_rebuild_coeff(property_id: str, body: CoeffOverride, user: dict = Depends(get_current_user)):
    from api.app import _user_override_ref
    _user_override_ref(user, property_id).set({"rebuild_coeff": body.value}, merge=True)
    return {"status": "ok", "rebuild_coeff": body.value}


@router.post("/api/properties/{property_id:path}/hide")
async def hide_property(property_id: str, user: dict = Depends(get_current_user)):
    """刪除使用者清單中的物件。
    - manual / 中央 user_url 物件：硬刪 DB（不留 archive，跟 DELETE /api/watchlist 一致）
    - 其他中央物件（batch/scheduler）：軟刪（個人 deleted=True flag，不影響 DB）"""
    from api.app import _user_override_ref
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


@router.post("/api/properties/{property_id:path}/floor_premium")
async def override_floor_premium(property_id: str, body: FloorPremiumOverride, user: dict = Depends(get_current_user)):
    from api.app import _user_override_ref
    v = max(0.0, min(0.80, float(body.floor_premium)))
    _user_override_ref(user, property_id).set({"floor_premium": v}, merge=True)
    return {"status": "ok", "floor_premium": v}


@router.post("/api/properties/{property_id:path}/road_width")
async def override_road_width(property_id: str, body: RoadWidthOverride, user: dict = Depends(get_current_user)):
    """使用者手動覆寫臨路寬度（只存自己 watchlist，不影響中央）。"""
    from api.app import _user_override_ref
    _user_override_ref(user, property_id).set({"road_width_m_override": body.road_width_m}, merge=True)
    return {"status": "ok", "road_width_m": body.road_width_m}


@router.post("/api/properties/{property_id:path}/zoning_ratios")
async def set_zoning_ratios(property_id: str, body: dict, user: dict = Depends(get_current_user)):
    """使用者手動設定多分區的坪數比例（個人覆寫）。"""
    from api.app import _user_override_ref
    ratios = body.get("zoning_ratios", [])
    _user_override_ref(user, property_id).set({"zoning_ratios": ratios}, merge=True)
    return {"status": "ok", "zoning_ratios": ratios}


@router.post("/api/properties/{property_id:path}/desired_price")
async def override_desired_price(property_id: str, body: DesiredPriceOverride, user: dict = Depends(get_current_user)):
    """欲出價（萬），個人設定。"""
    from api.app import _user_override_ref
    _user_override_ref(user, property_id).set({"desired_price_wan": body.desired_price_wan}, merge=True)
    return {"status": "ok", "desired_price_wan": body.desired_price_wan}


@router.post("/api/properties/{property_id:path}/inferred_choice")
async def override_inferred_choice(property_id: str, body: InferredChoiceOverride, user: dict = Depends(get_current_user)):
    """用戶從 address_inferred_candidates_detail 中挑一個當作推測地址（個人設定）。
    地址必須命中候選清單；儲存後讀取時 _read_user_property 會 swap address_inferred + land_area_ping。"""
    from api.app import _user_override_ref, _read_user_property
    p = _read_user_property(user, property_id)
    if p is None:
        raise HTTPException(status_code=404, detail="物件不存在")
    cands = p.get("address_inferred_candidates_detail") or []
    matched = next((c for c in cands if c.get("address") == body.address), None)
    if not matched:
        raise HTTPException(status_code=400, detail="所選地址不在候選清單中")
    _user_override_ref(user, property_id).set({"inferred_address_choice": body.address}, merge=True)
    return {"status": "ok", "address": body.address, "land_ping": matched.get("land_ping")}


@router.post("/api/properties/{property_id:path}/new_house_price")
async def override_new_house_price(property_id: str, body: NewHousePriceOverride, user: dict = Depends(get_current_user)):
    """覆寫新成屋單價（個人設定）— renewal v2 結果由前端即時算，不存 DB（CLAUDE.md 規則 8）。"""
    from api.app import _user_override_ref, _read_user_property
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
        zoning_list=p.get("zoning_list"),
        zoning_ratios=p.get("zoning_ratios"),
        floor=p.get("floor"),
        floor_range_min=p.get("floor_range_min"),
        floor_premium=p.get("floor_premium"),
        building_area_ping=p.get("building_area_ping"),
    )
    return {"status": "ok", "renewal_v2": v2}
