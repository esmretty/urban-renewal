"""
都更潛力評分模型 + 都更換回試算。

評分滿分 100 分，分五個維度：
  - 屋齡       25 分
  - 容積潛力   25 分
  - 基地面積   20 分
  - 捷運距離   15 分（TOD）
  - 臨路寬度   10 分
  - 整合潛力    5 分（Phase 2，暫給中間值）
"""
import math
import logging
from datetime import datetime
from typing import Optional

from config import (
    SCORE_WEIGHTS,
    FAR_BY_ZONE,
    BONUS_FAR_RATE,
    MIN_BUILDING_AGE,
    MIN_SITE_AREA_URD,
    MIN_SITE_AREA_DANGEROUS,
    DEFAULT_NEW_HOUSE_PRICE_PER_PING,
    CONSTRUCTION_COST_PER_PING,
    TAIPEI_BASE_FAR_PCT,
    REBUILD_BUILD_COEFF,
    REBUILD_SCENARIOS,
    DISTRICT_NEW_HOUSE_PRICE_WAN,
)
from analysis.share_ratio_table import lookup as lookup_share

logger = logging.getLogger(__name__)

CURRENT_YEAR = datetime.now().year


# ── 各維度評分函式（各自回傳 0.0–1.0） ────────────────────────────────────────

def score_age(building_age: Optional[int]) -> float:
    """屋齡：越老越高分。"""
    if building_age is None:
        return 0.5  # 未知給中間值
    if building_age >= 50:
        return 1.0
    if building_age >= 45:
        return 0.90
    if building_age >= 40:
        return 0.80
    if building_age >= 35:
        return 0.70
    if building_age >= 30:
        return 0.55
    if building_age >= 25:
        return 0.30
    if building_age >= 20:
        return 0.12
    return 0.0  # 屋齡 < 20 年，幾乎不可能都更


def score_far_potential(zoning: Optional[str], legal_far: Optional[float]) -> float:
    """
    容積獎勵潛力：分區越高階（商業 > 住四 > 住三…），換回利潤空間越大。
    同時考慮現況低開發的情況。
    """
    if legal_far is None and zoning:
        legal_far = FAR_BY_ZONE.get(zoning)

    if legal_far is None:
        return 0.5  # 未知給中間值

    # FAR → 分數映射（台北市分區）
    if legal_far >= 6.0:
        return 1.0   # 商四
    if legal_far >= 4.8:
        return 0.90  # 商三
    if legal_far >= 3.6:
        return 0.80  # 商二
    if legal_far >= 3.0:
        return 0.70  # 住四/商一
    if legal_far >= 2.25:
        return 0.55  # 住三
    if legal_far >= 1.6:
        return 0.35  # 住二
    return 0.15      # 住一


def score_land_size(land_area_sqm: Optional[float]) -> float:
    """
    基地面積（m²）：
      - ≥ 2000 m²：理想都更規模
      - 1000–2000 m²：可都更
      - 500–1000 m²：可危老
      - 200–500 m²：危老勉強
      - < 200 m²：不符合
    注意：land_area_sqm 此階段多為推估值，Phase 2 由地籍圖確認。
    """
    if land_area_sqm is None:
        return 0.4  # 未知給偏低中間值
    if land_area_sqm >= 2000:
        return 1.0
    if land_area_sqm >= 1500:
        return 0.85
    if land_area_sqm >= 1000:
        return 0.70
    if land_area_sqm >= 600:
        return 0.50
    if land_area_sqm >= 300:
        return 0.30
    if land_area_sqm >= 200:
        return 0.15
    return 0.0


def score_tod(nearest_mrt_dist_m: Optional[float]) -> float:
    """捷運距離：越近越高分（TOD 容積獎勵）。"""
    if nearest_mrt_dist_m is None:
        return 0.3
    if nearest_mrt_dist_m <= 250:
        return 1.0
    if nearest_mrt_dist_m <= 500:
        return 0.85
    if nearest_mrt_dist_m <= 800:
        return 0.60
    if nearest_mrt_dist_m <= 1200:
        return 0.35
    if nearest_mrt_dist_m <= 1800:
        return 0.15
    return 0.0


def score_road_width(road_width_m: Optional[float]) -> float:
    """
    臨路寬度：
      - ≥ 15 m：可能享容積獎勵，道路臨接條件好
      - 8–15 m：一般
      - < 6 m：不利都更
    Phase 1 多為未知（從591無法取得），給中間值。
    """
    if road_width_m is None:
        return 0.5
    if road_width_m >= 20:
        return 1.0
    if road_width_m >= 15:
        return 0.85
    if road_width_m >= 10:
        return 0.65
    if road_width_m >= 8:
        return 0.45
    if road_width_m >= 6:
        return 0.25
    return 0.10


def score_consolidation() -> float:
    """
    Phase 2：需地籍圖視覺分析。
    目前回傳中間值 0.5。
    """
    return 0.5


# ── 主要評分函式 ──────────────────────────────────────────────────────────────

def calculate_score(
    building_age: Optional[int] = None,
    zoning: Optional[str] = None,
    legal_far: Optional[float] = None,
    land_area_sqm: Optional[float] = None,
    nearest_mrt_dist_m: Optional[float] = None,
    road_width_m: Optional[float] = None,
) -> dict:
    """
    計算都更潛力總分及各維度分數。
    回傳 dict：{total, age, far, land, tod, road, consolidation}
    所有分數為 0–100 的整數。
    """
    w = SCORE_WEIGHTS
    s_age = score_age(building_age)
    s_far = score_far_potential(zoning, legal_far)
    s_land = score_land_size(land_area_sqm)
    s_tod = score_tod(nearest_mrt_dist_m)
    s_road = score_road_width(road_width_m)
    s_consol = score_consolidation()

    total = (
        s_age * w["building_age"]
        + s_far * w["far_potential"]
        + s_land * w["land_size"]
        + s_tod * w["tod_distance"]
        + s_road * w["road_width"]
        + s_consol * w["consolidation"]
    )

    def pct(v: float) -> float:
        return round(v * 100, 1)

    return {
        "total": pct(total),
        "age": pct(s_age),
        "far": pct(s_far),
        "land": pct(s_land),
        "tod": pct(s_tod),
        "road": pct(s_road),
        "consolidation": pct(s_consol),
    }


# ── 都更換回試算 ───────────────────────────────────────────────────────────────

def calculate_renewal_value(
    land_area_sqm: Optional[float],
    legal_far: Optional[float],
    building_age: Optional[int],
    nearest_mrt_dist_m: Optional[float],
    price_ntd: Optional[float],
    zoning: Optional[str] = None,
    city: Optional[str] = None,
    new_house_price_per_ping: float = DEFAULT_NEW_HOUSE_PRICE_PER_PING,
    construction_cost_per_ping: float = CONSTRUCTION_COST_PER_PING,
) -> dict:
    """
    都更/危老換回試算。

    簡化假設：
    1. 地主分回比例 ≈ (都更後總值 - 建設成本) / 都更後總值
    2. 個人持分依現況建物坪數在全棟中的比例估算（後續由使用者輸入細化）
    3. 公設比 30%

    回傳 dict，含：
      - renewal_type: 都更 / 危老 / 不符合
      - bonus_rate: 採用的容積獎勵率
      - new_total_area_ping: 都更後全棟可分配坪數
      - landlord_ratio: 地主分回比例
      - estimated_return_ping: 個人估計換回坪數 (假設全棟只有1戶)
      - estimated_return_value: 換回市值（元）
      - renewal_profit: 效益 = 換回市值 - 買入價
      - notes: 試算備註
    """
    notes = []

    if land_area_sqm is None:
        return {"renewal_type": "資料不足", "notes": "缺少土地面積，無法試算。"}

    # 決定都更類型
    if building_age is not None and building_age >= MIN_BUILDING_AGE:
        if land_area_sqm >= MIN_SITE_AREA_URD:
            renewal_type = "都更"
        elif land_area_sqm >= MIN_SITE_AREA_DANGEROUS:
            renewal_type = "危老"
        else:
            return {
                "renewal_type": "不符合",
                "notes": f"土地面積僅 {land_area_sqm:.0f} m²，低於危老最小門檻 {MIN_SITE_AREA_DANGEROUS} m²。",
            }
    elif building_age is not None and building_age < MIN_BUILDING_AGE:
        return {
            "renewal_type": "不符合",
            "notes": f"屋齡 {building_age} 年，未達都更/危老門檻 {MIN_BUILDING_AGE} 年。",
        }
    else:
        renewal_type = "都更（屋齡未知）"
        notes.append("屋齡未知，試算採保守估計。")

    # 容積率
    if legal_far is None and zoning:
        legal_far = FAR_BY_ZONE.get(zoning)
    if legal_far is None:
        legal_far = 2.25  # 保守假設住三
        notes.append("容積率未知，試算採住三乙（225%）保守估計。")

    # 容積獎勵率（防災都更只適用台北市；新北市用一般都更獎勵）
    if renewal_type == "危老":
        bonus = BONUS_FAR_RATE["危老"]
        notes.append("危老重建：容積獎勵率採 40%。")
    elif city == "台北市":
        bonus = BONUS_FAR_RATE["防災都更"]
        notes.append("台北市防災都更：容積獎勵率採 30%。")
    else:
        bonus = BONUS_FAR_RATE["一般都更"]
        notes.append(f"{city or ''} 一般都更：容積獎勵率採 15%（防災都更僅限台北市）。")

    # TOD 加碼
    if nearest_mrt_dist_m is not None:
        if nearest_mrt_dist_m <= 250:
            bonus += BONUS_FAR_RATE["TOD_250m"]
            notes.append("捷運站 250m 內，TOD 容積加碼 +10%。")
        elif nearest_mrt_dist_m <= 500:
            bonus += BONUS_FAR_RATE["TOD_500m"]
            notes.append("捷運站 500m 內，TOD 容積加碼 +5%。")

    # 試算
    PUBLIC_AREA_RATIO = 0.30   # 公設比 30%
    PING_PER_SQM = 1 / 3.30578  # 1 m² = 0.3025 坪

    # 都更後全棟容積（m²）
    total_far_sqm = land_area_sqm * legal_far * (1 + bonus)

    # 全棟可分配坪數（扣除公設）
    total_distributable_ping = (total_far_sqm * PING_PER_SQM) * (1 - PUBLIC_AREA_RATIO)

    # 建設成本
    total_construction_cost = total_distributable_ping * construction_cost_per_ping

    # 都更後全棟市值
    total_new_value = total_distributable_ping * new_house_price_per_ping

    # 地主分回比例
    if total_new_value > 0:
        landlord_ratio = max(0.0, (total_new_value - total_construction_cost) / total_new_value)
    else:
        landlord_ratio = 0.0

    # 個人換回坪數（假設此筆物件為全棟唯一戶，或以建物坪數為全棟100%）
    # 實際上需依土地持分比例，這裡假設買入本物件即擁有全部土地持分
    personal_return_ping = total_distributable_ping * landlord_ratio
    personal_return_value = personal_return_ping * new_house_price_per_ping
    renewal_profit = (personal_return_value - price_ntd) if price_ntd else None

    notes.append(
        f"試算假設：買方持有全部土地（適用整棟購入或確認持分後調整）。"
        f"實際分回坪數需乘以個人土地持分比例。"
    )

    return {
        "renewal_type": renewal_type,
        "bonus_rate": round(bonus, 2),
        "legal_far_used": legal_far,
        "new_total_floor_sqm": round(total_far_sqm, 1),
        "new_total_distributable_ping": round(total_distributable_ping, 1),
        "total_construction_cost": round(total_construction_cost),
        "total_new_value": round(total_new_value),
        "landlord_ratio": round(landlord_ratio, 3),
        "estimated_return_ping": round(personal_return_ping, 1),
        "estimated_return_value": round(personal_return_value),
        "renewal_profit": round(renewal_profit) if renewal_profit is not None else None,
        "notes": " ".join(notes),
    }


def resolve_effective_zoning(zoning: Optional[str], zoning_original: Optional[str]) -> Optional[str]:
    """依用戶規則決定計算容積率用的「有效分區」：
    - 住宅區 + (特)/(遷) → 忽略 zoning_original，一律用該住宅區本身（剝掉括號後綴）
    - 商業區 + (特)/(遷) → zoning_original 是合法分區 → 用它；否則用該商業區本身
    - 無(特)/(遷) 後綴 → zoning_original 有且合法優先，否則 zoning
    判定「無法辨識」標準：字串不在 TAIPEI_BASE_FAR_PCT 裡。
    """
    import re as _re
    z = zoning or ""
    orig = zoning_original or ""
    has_special = bool(_re.search(r"\((特|遷|核|抄)\)", z))
    base = _re.sub(r"\((特|遷|核|抄)\)", "", z).strip()

    if has_special and "商" in z:
        # 商業區(特): 原分區合法優先
        if orig in TAIPEI_BASE_FAR_PCT:
            return orig
        return base if base in TAIPEI_BASE_FAR_PCT else z
    if has_special and "住" in z:
        # 住宅區(特): 不信原分區，一律用 base
        return base if base in TAIPEI_BASE_FAR_PCT else z
    # 無特殊後綴：老邏輯（原分區優先）
    if orig in TAIPEI_BASE_FAR_PCT:
        return orig
    return z


def calculate_renewal_scenarios(
    *,
    land_area_ping: Optional[float],
    zoning: Optional[str],
    district: Optional[str],
    price_ntd: Optional[float],
    new_house_price_wan_per_ping: Optional[float] = None,
    is_qualified_for_fz_dugen: bool = False,
    road_width_m: Optional[float] = None,
) -> dict:
    """
    用「重建坪數計算機」邏輯 + 房價→分回比例對照表，計算危老/都更/防災都更三情境。

    Returns:
        {
            "base_far_pct": int | None,      # 基準容積率(%)
            "new_house_price_wan": float,     # 採用的新成屋單價(萬/坪)
            "new_house_price_source": str,    # default_district / override
            "share_ratio": float | None,      # 分回比例（從表查）
            "parking_value_wan": float | None,# 車位市值（萬）
            "scenarios": {
                "危老":      { bonus, share_ping, house_value_wan, total_value_wan, profit_wan, multiple },
                "都更":      { ... },
                "防災都更":  { ... } 若 is_qualified_for_fz_dugen
            },
            "note": str  # 說明使用了哪些假設
        }
    """
    out = {
        "base_far_pct": None,
        "new_house_price_wan": None,
        "new_house_price_source": None,
        "share_ratio": None,
        "parking_value_wan": None,
        "scenarios": {},
        "note": "",
    }

    if not land_area_ping or land_area_ping <= 0:
        out["note"] = "缺土地坪數，無法試算。"
        return out

    # 1. 基準容積率（路寬限縮：基準FAR×2(m) ≤ 路寬(m) → 上限 = 路寬 × 50%）
    base_far_pct = TAIPEI_BASE_FAR_PCT.get(zoning) if zoning else None
    if base_far_pct is None:
        out["note"] = f"未知分區 {zoning!r}，無法試算。"
        return out
    out["base_far_pct"] = base_far_pct
    if road_width_m and road_width_m > 0:
        cap = road_width_m * 50
        far_pct = min(base_far_pct, round(cap))
        out["effective_far_pct"] = far_pct
        out["road_width_capped"] = far_pct < base_far_pct
    else:
        far_pct = base_far_pct
        out["effective_far_pct"] = far_pct
        out["road_width_capped"] = False

    # 2. 新成屋單價（優先用 override，否則用該區預設）
    price = new_house_price_wan_per_ping
    if price:
        out["new_house_price_source"] = "override"
    else:
        price = DISTRICT_NEW_HOUSE_PRICE_WAN.get(district)
        out["new_house_price_source"] = "default_district" if price else "missing"
    if not price:
        out["note"] = f"沒有 {district!r} 的新成屋單價預設值，無法試算。"
        return out
    out["new_house_price_wan"] = price

    # 3. 查表得分回比例 + 車位市值
    ratio, parking = lookup_share(price)
    out["share_ratio"] = round(ratio, 3) if ratio else None
    out["parking_value_wan"] = round(parking, 1) if parking else None

    # 4. 跑情境
    scenarios = {}
    targets = ["危老", "都更"]
    if is_qualified_for_fz_dugen:
        targets.append("防災都更")
    for name in targets:
        bonus = REBUILD_SCENARIOS[name]
        # 公式：分回坪 = 土地 × 容積率(%)/100 × bonus × 1.57 × 分回比例
        new_built_ping = land_area_ping * (far_pct / 100.0) * bonus * REBUILD_BUILD_COEFF
        share_ping = new_built_ping * (ratio or 0)
        house_value_wan = share_ping * price
        # 假設分回 1 個平面車位
        total_value_wan = house_value_wan + (parking or 0)
        # multiple 分母用「欲出價」(開價 × 0.9) — 跟前端 UI 一致（前端 desiredPriceWan = 開價×0.9）
        list_price_wan = (price_ntd / 10000) if price_ntd else None
        desired_price_wan = (list_price_wan * 0.9) if list_price_wan else None
        profit_wan = (total_value_wan - desired_price_wan) if desired_price_wan else None
        multiple = (total_value_wan / desired_price_wan) if desired_price_wan and desired_price_wan > 0 else None
        scenarios[name] = {
            "bonus": bonus,
            "new_built_ping": round(new_built_ping, 1),
            "share_ping": round(share_ping, 1),
            "house_value_wan": round(house_value_wan, 1),
            "parking_value_wan": round(parking or 0, 1),
            "total_value_wan": round(total_value_wan, 1),
            "profit_wan": round(profit_wan, 1) if profit_wan is not None else None,
            "multiple": round(multiple, 2) if multiple else None,
            "denominator_wan": round(desired_price_wan, 1) if desired_price_wan else None,
            "denominator_basis": "desired_price_0.9x",
        }
    out["scenarios"] = scenarios

    notes = []
    if out["new_house_price_source"] == "default_district":
        notes.append(f"新成屋單價採 {district} 預設 {price} 萬/坪（可手動覆寫）")
    elif out["new_house_price_source"] == "override":
        notes.append(f"新成屋單價使用人工覆寫值 {price} 萬/坪")
    notes.append("分回比例由房價查表內插；含 1 個平面車位市值")
    out["note"] = "；".join(notes)
    return out


def get_recommendation(score_total: float, renewal_profit: Optional[float]) -> tuple[str, str]:
    """
    根據總分和都更效益給出建議。
    回傳 (recommendation: str, reason: str)
    """
    if score_total >= 80:
        rec = "強烈推薦"
        reason = f"都更潛力極高（{score_total:.0f}分）。"
    elif score_total >= 65:
        rec = "值得考慮"
        reason = f"都更潛力良好（{score_total:.0f}分），建議進一步實地調查。"
    elif score_total >= 50:
        rec = "一般"
        reason = f"有一定都更潛力（{score_total:.0f}分），需深度評估地籍條件。"
    else:
        rec = "不建議"
        reason = f"都更潛力偏低（{score_total:.0f}分），條件不理想。"

    if renewal_profit is not None:
        profit_wan = renewal_profit / 10000
        if renewal_profit > 0:
            reason += f" 試算都更效益約 +{profit_wan:,.0f} 萬（換回市值 - 買入價）。"
        else:
            reason += f" 注意：試算都更效益為 {profit_wan:,.0f} 萬（負值），需謹慎評估。"

    return rec, reason
