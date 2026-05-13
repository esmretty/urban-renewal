"""
學區查詢相關 endpoints — Sprint #2 從 api/app.py 拆出。

5 個 endpoint 全自包含（不依賴 app.py 內部 helper / state）：
  - GET /api/school_district/lookup            測試頁地址 / 座標 / 里 通用查詢
  - GET /api/school_district/supported         下拉選單支援的 (city, district) 清單
  - GET /api/school_district/by_district       某區所有里 → 學區對照
  - GET /api/school_district/polygons          某區 polygon (geojson) + 學校
  - GET /api/school_district/polygons_all      所有 supported (city,district) 的 polygon 合集

Dependencies：
  - analysis.school_district (主 lookup module)
  - config.BASE_DIR (讀 villages_polygon.geojson)
  - module-local _POLYGON_CACHE / _NLSC_VILLAGE_ALIAS / _normalize_nlsc_village

公開 endpoint，無 auth。
"""
import re
import json
from typing import Optional

from fastapi import APIRouter, Query

from config import BASE_DIR

router = APIRouter()


# ── 模組層 cache + helper（搬自 api/app.py）─────────────────────────────
_POLYGON_CACHE = {"data": None}

# NLSC _raw_twvillage.json 自身缺字 → lookup canonical name 的 alias 表。
# 例：NLSC 「五?里」實際是「五峰里」(峰字 dump 失敗)、「灰?里」是「灰磘里」(磘字)
# 注意 NLSC 缺字 placeholder 不統一—中和用 ■「■」、新店用  PUA。
# 用 _normalize_nlsc_village 統一替成「?」，alias key 才一致。
_NLSC_UNKNOWN_RE = re.compile(r"[^一-鿿 -~]")


def _normalize_nlsc_village(v: str) -> str:
    """把 NLSC dump 內非標準中文/ASCII 的字符（缺字 placeholder ■、PUA 區）替成「?」。"""
    return _NLSC_UNKNOWN_RE.sub("?", v or "")


_NLSC_VILLAGE_ALIAS = {
    ("新北市", "新店區", "五?里"): "五峰里",
    ("新北市", "中和區", "灰?里"): "灰磘里",
    ("新北市", "中和區", "瓦?里"): "瓦磘里",
    ("新北市", "永和區", "新?里"): "新廍里",
    ("新北市", "板橋區", "公?里"): "公館里",
}


# ── Endpoints ────────────────────────────────────────────────────────────
@router.get("/api/school_district/lookup")
def api_school_district_lookup(
    address: Optional[str] = Query(None),
    lat: Optional[float] = Query(None),
    lng: Optional[float] = Query(None),
    city: Optional[str] = Query(None),
    district: Optional[str] = Query(None),
    village: Optional[str] = Query(None),
):
    """測試頁專用：地址 / 座標 / (city+district+village) 任一組合都可查學區。
    優先序：village > coord > address。"""
    from analysis import school_district as sd
    if village and city and district:
        r = sd.lookup_by_village(city, district, village)
        detail = sd.lookup_by_village_detail(city, district, village)
        return {"mode": "village", "city": city, "district": district, "village": village,
                "school_elementary": r["elementary"], "school_junior_high": r["junior_high"],
                "rows_detail": detail}
    if lat is not None and lng is not None:
        return {"mode": "coord", **sd.lookup_by_coord(lat, lng, city or "", district or "")}
    if address:
        return {"mode": "address", **sd.lookup_by_address(address)}
    return {"error": "請提供 address / lat+lng / city+district+village 任一組合"}


@router.get("/api/school_district/supported")
def api_school_district_supported():
    """回 {city: [district, ...]} 給測試頁下拉選單。"""
    from analysis import school_district as sd
    return sd.get_supported_districts()


@router.get("/api/school_district/by_district")
def api_school_district_by_district(city: str = Query(...), district: str = Query(...)):
    """測試頁地圖用：回某區所有里的學區對照表。"""
    from analysis import school_district as sd
    villages = sd.get_district_villages(city, district)
    return {"city": city, "district": district, "villages": villages}


@router.get("/api/school_district/polygons")
def api_school_district_polygons(city: str = Query(...), district: str = Query(...)):
    """回某區所有里的 polygon (GeoJSON FeatureCollection) + 每個 feature merge 學校資料。
    給測試頁地圖按學校上色用。"""
    from analysis import school_district as sd
    if _POLYGON_CACHE["data"] is None:
        try:
            poly_path = BASE_DIR / "data" / "school_districts" / "villages_polygon.geojson"
            with open(poly_path, encoding="utf-8") as f:
                _POLYGON_CACHE["data"] = json.load(f)
        except Exception as e:
            return {"error": f"polygon 檔案載入失敗: {e}"}
    all_features = (_POLYGON_CACHE["data"] or {}).get("features") or []
    villages_lookup = sd.get_district_villages(city, district)
    out = []
    for ft in all_features:
        p = ft.get("properties") or {}
        if p.get("county") != city or p.get("town") != district:
            continue
        v = p.get("village")
        info = villages_lookup.get(v) or {}
        merged = dict(ft)
        merged["properties"] = {
            "county": p.get("county"),
            "town": p.get("town"),
            "village": v,
            "elementary": info.get("elementary") or [],
            "junior_high": info.get("junior_high") or [],
        }
        out.append(merged)
    return {"type": "FeatureCollection", "features": out}


@router.get("/api/school_district/polygons_all")
def api_school_district_polygons_all():
    """回所有 supported (city, district) 的 polygon 合集 — 給「全區學區地圖」用。
    一次 response 含台北/新北所有里的 polygon + 學校資料。"""
    from analysis import school_district as sd
    if _POLYGON_CACHE["data"] is None:
        try:
            poly_path = BASE_DIR / "data" / "school_districts" / "villages_polygon.geojson"
            with open(poly_path, encoding="utf-8") as f:
                _POLYGON_CACHE["data"] = json.load(f)
        except Exception as e:
            return {"error": f"polygon 檔案載入失敗: {e}"}
    all_features = (_POLYGON_CACHE["data"] or {}).get("features") or []
    supported = sd.get_supported_districts()  # {city: [district, ...]}
    supported_pairs = {(c, d) for c, dists in supported.items() for d in dists}
    villages_cache: dict = {}   # (city, district) → villages dict
    out = []
    for ft in all_features:
        p = ft.get("properties") or {}
        c, t, v = p.get("county"), p.get("town"), p.get("village")
        if (c, t) not in supported_pairs:
            continue
        # NLSC 自身缺字 → alias 對到 lookup canonical name (五?里→五峰里, 灰?里→灰磘里)
        canonical_v = _NLSC_VILLAGE_ALIAS.get((c, t, _normalize_nlsc_village(v)), v)
        if (c, t) not in villages_cache:
            villages_cache[(c, t)] = sd.get_district_villages(c, t)
        info = villages_cache[(c, t)].get(canonical_v) or {}
        out.append({
            "type": "Feature",
            "geometry": ft.get("geometry"),
            "properties": {
                "county": c, "town": t, "village": canonical_v,  # 顯示 canonical
                "elementary": info.get("elementary") or [],
                "junior_high": info.get("junior_high") or [],
            },
        })
    return {"type": "FeatureCollection", "features": out}
