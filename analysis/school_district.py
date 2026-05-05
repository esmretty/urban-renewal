"""學區查詢：座標 / 地址 → 國小、國中學區。

資料來源：
- data/school_districts/lookup.json（由 scripts/build_school_district_lookup.py 解析
  台北市 CSV + 新北市 PDF 產生）
- 座標→里名：analysis.gov_gis.query_section_parcel (NLSC TownVillagePointQuery)
- 地址→座標：analysis.geocoder.geocode_address (Google Maps geocoding)

設計：
- 一個里多校情況（共同學區、自由學區、鄰級切割聚合）→ list 多筆學校，依字典序回傳
- 我們從座標只能拿到「里」不能拿到「鄰」，里級多校時聚合所有可能學校
- 涵蓋：台北市 12 區 + 新北市 5 區（板橋/新莊/新店/中和/永和）
"""
import json
import logging
from pathlib import Path
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

_LOOKUP_PATH = Path(__file__).resolve().parent.parent / "data" / "school_districts" / "lookup.json"
_LOOKUP_CACHE: Optional[dict] = None


def _load_lookup() -> dict:
    """Lazy load lookup JSON。失敗時 cache 空 dict（之後不再嘗試讀檔）。"""
    global _LOOKUP_CACHE
    if _LOOKUP_CACHE is None:
        try:
            with open(_LOOKUP_PATH, encoding="utf-8") as fp:
                _LOOKUP_CACHE = json.load(fp)
        except Exception as e:
            logger.warning("school_district lookup 載入失敗 (%s): %s", _LOOKUP_PATH, e)
            _LOOKUP_CACHE = {}
    return _LOOKUP_CACHE


def lookup_by_village(city: str, district: str, village: str) -> Dict[str, List[str]]:
    """直接用 (city, district, village) 查學區。
    回 {"elementary": [...], "junior_high": [...]}；查不到回空 list。"""
    if not (city and district and village):
        return {"elementary": [], "junior_high": []}
    lk = _load_lookup()
    info = lk.get(city, {}).get(district, {}).get(village)
    if info:
        return {
            "elementary": list(info.get("elementary") or []),
            "junior_high": list(info.get("junior_high") or []),
        }
    return {"elementary": [], "junior_high": []}


def lookup_by_coord(lat: float, lng: float, city: str = "", district: str = "") -> dict:
    """座標 → 國小、國中學區（用 NLSC 拿 villageName 再 lookup）。

    Returns:
      {
        "village": str | None,        # NLSC 回的里名
        "city": str,                   # 最終用於 lookup 的 city（NLSC 優先）
        "district": str,               # 同上 district
        "school_elementary": [str],
        "school_junior_high": [str],
        "source": str,                 # debug：'lookup_hit' / 'lookup_miss' / 'no_coord' / 'nlsc_*'
      }
    """
    out = {
        "village": None,
        "city": city or "",
        "district": district or "",
        "school_elementary": [],
        "school_junior_high": [],
        "source": None,
    }
    if not (lat and lng):
        out["source"] = "no_coord"
        return out
    try:
        from analysis.gov_gis import query_section_parcel
        sp = query_section_parcel(lat, lng)
    except Exception as e:
        logger.warning("query_section_parcel 失敗: %s", e)
        out["source"] = "nlsc_error"
        return out
    if not sp:
        out["source"] = "nlsc_empty"
        return out
    out["village"] = sp.get("village")
    nlsc_city = sp.get("city") or city
    nlsc_district = sp.get("district") or district
    out["city"] = nlsc_city
    out["district"] = nlsc_district
    if not out["village"]:
        out["source"] = "no_village"
        return out
    sd = lookup_by_village(nlsc_city, nlsc_district, out["village"])
    out["school_elementary"] = sd["elementary"]
    out["school_junior_high"] = sd["junior_high"]
    out["source"] = "lookup_hit" if (sd["elementary"] or sd["junior_high"]) else "lookup_miss"
    return out


def lookup_by_address(address: str) -> dict:
    """地址 → 學區。先 Google geocode → 座標 → lookup_by_coord。

    Returns: lookup_by_coord output + extra:
      "geocoded_lat" / "geocoded_lng" / "geocoded_address"
    """
    out = {
        "input_address": address,
        "geocoded_lat": None,
        "geocoded_lng": None,
        "geocoded_address": None,
        "village": None,
        "city": "",
        "district": "",
        "school_elementary": [],
        "school_junior_high": [],
        "source": None,
    }
    if not address:
        out["source"] = "no_address"
        return out
    try:
        from analysis.geocoder import geocode_with_district
        cands = geocode_with_district(address)
    except Exception as e:
        logger.warning("geocode_with_district 失敗 (%s): %s", address, e)
        out["source"] = "geocode_error"
        return out
    if not cands:
        out["source"] = "geocode_empty"
        return out
    # 取第一個 (Google 最高 confidence)；若有 city/district info 帶進
    g = cands[0]
    out["geocoded_lat"] = g.get("lat")
    out["geocoded_lng"] = g.get("lng")
    out["geocoded_address"] = g.get("formatted_address") or g.get("address")
    coord_result = lookup_by_coord(
        g.get("lat"), g.get("lng"),
        city=g.get("city") or "",
        district=g.get("district") or "",
    )
    # merge coord result keys
    for k in ("village", "city", "district", "school_elementary", "school_junior_high", "source"):
        out[k] = coord_result.get(k, out.get(k))
    return out


def get_district_villages(city: str, district: str) -> Dict[str, dict]:
    """取某個區所有里的學區資料 — 給「學區地圖」用。
    回 {village_name: {"elementary": [...], "junior_high": [...]}}。
    """
    lk = _load_lookup()
    return lk.get(city, {}).get(district, {})


def get_supported_districts() -> Dict[str, List[str]]:
    """回 {city: [district, ...]} 給前端下拉選單列出可查的區。"""
    lk = _load_lookup()
    return {city: sorted(dists.keys()) for city, dists in lk.items()}
