"""學區查詢：座標 / 地址 → 國小、國中學區。

資料來源：
- data/school_districts/lookup.json (v2 schema, row-based)
  由 scripts/build_school_district_lookup.py 從 4 個 CSV 建：
    ntpc_elementary.csv / ntpc_junior_high.csv /
    taipei_elementary.csv / taipei_junior_high.csv
- 座標→里名：analysis.gov_gis.query_section_parcel (NLSC TownVillagePointQuery)
- 地址→座標：analysis.geocoder.geocode_address (Google Maps geocoding)

Schema (v2):
  {
    "_meta": {...},
    "rows": [
      {
        "city": "新北市",
        "district": "板橋區",          // 物件所在區（lookup key）
        "school_district": "板橋區",   // 學校所在區（跨區共同學區時不同）
        "kind": "elementary" | "junior_high",
        "school": "實踐國小",
        "zone_type": "基本" | "自由" | "共同",
        "village": "後埔里",            // 純里名（lookup key）
        "village_raw": "後埔里14–17鄰", // 原始 CSV 字串（detail 顯示用）
        "neighborhoods_raw": "14–17鄰", // 鄰級切割描述
        "source_page": 7,
        "extra": {...}                  // 校址、電話等（taipei_jh）
      }, ...
    ]
  }
"""
import json
import logging
from pathlib import Path
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

_LOOKUP_PATH = Path(__file__).resolve().parent.parent / "data" / "school_districts" / "lookup.json"
_LOOKUP_CACHE: Optional[dict] = None
_INDEX_CACHE: Optional[dict] = None  # (city, district, village) → list[row]


def _load_lookup() -> dict:
    """Lazy load lookup JSON。失敗時 cache 空 dict。"""
    global _LOOKUP_CACHE
    if _LOOKUP_CACHE is None:
        try:
            with open(_LOOKUP_PATH, encoding="utf-8") as fp:
                _LOOKUP_CACHE = json.load(fp)
        except Exception as e:
            logger.warning("school_district lookup 載入失敗 (%s): %s", _LOOKUP_PATH, e)
            _LOOKUP_CACHE = {"rows": []}
    return _LOOKUP_CACHE


def _normalize_village(village: str) -> str:
    """村里名異字 normalize — NLSC 政府 polygon 回「五峯里」但學區 CSV 用「五峰里」。
    走 zhtw_normalize 統一 (含 臺/台、峯/峰、簡體繁體) 後再當 key 比對。"""
    if not village:
        return village
    from helpers.text_norm import zhtw_normalize
    return zhtw_normalize(village)


def _build_index() -> dict:
    """Build (city, district, village) → list[row] 索引。Lazy + cached。
    village 用 _normalize_village 統一異字後當 key (避免「五峯里」vs「五峰里」mismatch)。"""
    global _INDEX_CACHE
    if _INDEX_CACHE is not None:
        return _INDEX_CACHE
    lk = _load_lookup()
    rows = lk.get("rows") or []
    idx: dict = {}
    for r in rows:
        key = (r.get("city"), r.get("district"), _normalize_village(r.get("village")))
        idx.setdefault(key, []).append(r)
    _INDEX_CACHE = idx
    return idx


def _normalize_city(city: str) -> str:
    """臺/台 互換 — NLSC 回「臺北市」但 lookup key 是「台北市」。"""
    if not city:
        return city
    return city.replace("臺", "台")


import re as _re_dedup
_REGION_PREFIX_RE = _re_dedup.compile(r"^[一-龥]{1,4}區")


def lookup_by_village_detail(city: str, district: str, village: str) -> List[dict]:
    """完整版 lookup — 回 list of row dict（含 zone_type, village_raw, neighborhoods_raw, school_district 等）。
    給 detail page 「如實顯示學區與鄰的關係」用。

    Dedup 邏輯：
    - source CSV 同一筆學區關係常從多視角列出（例「板橋區實踐國小」row 列「中和區民生里」
      + 「中和區實踐國小」row 列「民生里」是同筆）。dedup key=
      (school, kind, zone_type, neighborhoods_raw)，prefer raw 不含區前綴的版本
      （已知物件所在區，prefix 對 detail 多餘）。
    """
    if not (city and district and village):
        return []
    city = _normalize_city(city)
    village = _normalize_village(village)
    idx = _build_index()
    rows = idx.get((city, district, village)) or []
    groups: dict = {}
    for r in rows:
        k = (r.get("school"), r.get("kind"), r.get("zone_type"), r.get("neighborhoods_raw"))
        if k not in groups:
            groups[k] = r
            continue
        existing = groups[k]
        existing_has_prefix = bool(_REGION_PREFIX_RE.match(existing.get("village_raw") or ""))
        new_has_prefix = bool(_REGION_PREFIX_RE.match(r.get("village_raw") or ""))
        # 既存帶 prefix 但新的沒有 → 換成新的（更乾淨的 raw）
        if existing_has_prefix and not new_has_prefix:
            groups[k] = r
    return list(groups.values())


def lookup_by_village(city: str, district: str, village: str) -> Dict[str, List[str]]:
    """Backward-compatible 版 — 只回校名 list，dedup。
    回 {"elementary": [...], "junior_high": [...]}；查不到回空 list。
    """
    rows = lookup_by_village_detail(city, district, village)
    es: list = []
    jh: list = []
    seen_es: set = set()
    seen_jh: set = set()
    for r in rows:
        sch = r.get("school")
        if not sch:
            continue
        if r.get("kind") == "elementary" and sch not in seen_es:
            seen_es.add(sch)
            es.append(sch)
        elif r.get("kind") == "junior_high" and sch not in seen_jh:
            seen_jh.add(sch)
            jh.append(sch)
    return {"elementary": sorted(es), "junior_high": sorted(jh)}


def lookup_by_coord(lat: float, lng: float, city: str = "", district: str = "") -> dict:
    """座標 → 國小、國中學區（用 NLSC 拿 villageName 再 lookup）。

    Returns:
      {
        "village": str | None,
        "city": str,
        "district": str,
        "school_elementary": [str],     # 校名 list (backward compat)
        "school_junior_high": [str],
        "rows_detail": [row, ...],       # 新增：完整 row info 給 detail page 用
        "source": str,
      }
    """
    out = {
        "village": None,
        "city": city or "",
        "district": district or "",
        "school_elementary": [],
        "school_junior_high": [],
        "rows_detail": [],
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
    out["lat"] = lat
    out["lng"] = lng
    nlsc_city = _normalize_city(sp.get("city") or city)
    nlsc_district = sp.get("district") or district
    out["city"] = nlsc_city
    out["district"] = nlsc_district
    if not out["village"]:
        out["source"] = "no_village"
        return out
    detail = lookup_by_village_detail(nlsc_city, nlsc_district, out["village"])
    out["rows_detail"] = detail
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
        "rows_detail": [],
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
    g = cands[0]
    out["geocoded_lat"] = g.get("lat")
    out["geocoded_lng"] = g.get("lng")
    out["geocoded_address"] = g.get("formatted_address") or g.get("address")
    coord_result = lookup_by_coord(
        g.get("lat"), g.get("lng"),
        city=g.get("city") or "",
        district=g.get("district") or "",
    )
    for k in ("village", "city", "district", "school_elementary", "school_junior_high", "rows_detail", "source"):
        out[k] = coord_result.get(k, out.get(k))
    return out


def get_district_villages(city: str, district: str) -> Dict[str, dict]:
    """取某個區所有里的學區資料 — 給「學區地圖」用。
    回 {village_name: {"elementary": [str], "junior_high": [str]}}。
    """
    if not (city and district):
        return {}
    city = _normalize_city(city)
    lk = _load_lookup()
    rows = lk.get("rows") or []
    out: dict = {}
    for r in rows:
        if r.get("city") != city or r.get("district") != district:
            continue
        v = r.get("village")
        if not v:
            continue
        slot = out.setdefault(v, {"elementary": set(), "junior_high": set()})
        kind = r.get("kind")
        sch = r.get("school")
        if kind in ("elementary", "junior_high") and sch:
            slot[kind].add(sch)
    return {v: {"elementary": sorted(s["elementary"]), "junior_high": sorted(s["junior_high"])}
            for v, s in out.items()}


def get_supported_districts() -> Dict[str, List[str]]:
    """回 {city: [district, ...]} 給前端下拉選單列出可查的區。"""
    lk = _load_lookup()
    rows = lk.get("rows") or []
    out: dict = {}
    for r in rows:
        c = r.get("city")
        d = r.get("district")
        if c and d:
            out.setdefault(c, set()).add(d)
    return {c: sorted(s) for c, s in out.items()}
