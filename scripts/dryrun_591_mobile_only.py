"""Dry run: 模擬「mobile API 完全取代 Playwright + Vision OCR」路徑各階段時間。

測試物件：591_19344152 (台北市文山區忠順街一段，hide_addr_detail=1)
這也是用戶最近從 URL 提交的物件，現實際測試耗時 100+ 秒。

流程（模擬 _scrape_single_url 但不開 Playwright）：
  1. mobile API call (fetch_mobile_detail)
  2. zoning lookup (lat/lng → ArcGIS)
  3. WMS GetMap zonemap (新北 NTPC ArcGIS / 台北 GeoServer WMS)
  4. geocode / nearest MRT
  5. road width WFS lookup
  6. LVR triangulate (建坪 ±0.01 對齊找完整門牌)
  7. final make_property_doc 組裝

不寫任何 DB，不改 production，純粹 stage timing。
"""
from __future__ import annotations
import sys, time
sys.path.insert(0, ".")

import logging
logging.basicConfig(level=logging.WARNING)

URL = "https://sale.591.com.tw/home/house/detail/2/19344152.html"
HID = "19344152"

stages = []
def stage(name, fn):
    t0 = time.time()
    res = fn()
    dt = time.time() - t0
    stages.append((name, dt))
    print(f"  [{dt:6.2f}s] {name}")
    return res

print("=" * 60)
print(f"Dry run: 591_{HID} mobile-only path")
print("=" * 60)

# 1) mobile API (新版會 map 完整欄位；這邊用既有 fetch_mobile_detail，比較 baseline)
def _step_mobile():
    from scraper.scraper_591_mobile import fetch_mobile_detail
    return fetch_mobile_detail(HID)
mobile = stage("mobile API call", _step_mobile)
if not mobile:
    print("⚠ mobile API 失敗，stop")
    sys.exit(1)

print(f"      → city/district: {mobile.get('city')}/{mobile.get('district')}")
print(f"      → address: {mobile.get('address')!r}  hide_addr={mobile.get('_addr_hidden_by_591')}")
print(f"      → 建坪 {mobile.get('building_area_ping')} / 土地 {mobile.get('land_area_ping')} / 屋齡 {mobile.get('building_age')}")
print(f"      → floor {mobile.get('floor')} → 解析後 floor_range_min/max?")
print(f"      → lat/lng: {mobile.get('source_latitude')}/{mobile.get('source_longitude')}")

lat = mobile.get("source_latitude")
lng = mobile.get("source_longitude")
city = mobile.get("city")
district = mobile.get("district")
addr = mobile.get("address") or ""

# 2) zoning lookup (ArcGIS 點查)
def _step_zoning():
    from analysis.gov_gis import lookup_zoning_by_coord
    return lookup_zoning_by_coord(lat, lng, city)
zoning_result = stage("zoning lookup (ArcGIS)", _step_zoning)
print(f"      → zoning: {zoning_result.get('zoning')}")

# 3) WMS GetMap zonemap 截圖
def _step_wms():
    from analysis.gov_gis import fetch_zoning_map_image_taipei, fetch_zoning_map_image_newtaipei
    import os
    os.makedirs("data/screenshots", exist_ok=True)
    out = f"data/screenshots/_dryrun_{HID}_roadwidth.png"
    if city == "台北市":
        return fetch_zoning_map_image_taipei(lat, lng, out)
    return fetch_zoning_map_image_newtaipei(lat, lng, out)
wms_ok = stage("WMS zonemap 截圖", _step_wms)
print(f"      → wms_ok={wms_ok}")

# 4) geocode 反查 + nearest MRT
def _step_mrt():
    from analysis.geocoder import get_nearest_mrt
    return get_nearest_mrt(lat, lng)
mrts = stage("get_nearest_mrt", _step_mrt)
print(f"      → MRTs: {len(mrts) if mrts else 0} 站")

# 5) road width WFS query (台北 only)
def _step_road_width():
    from analysis.gov_gis import query_road_width_taipei
    if city != "台北市":
        return None
    return query_road_width_taipei(lat, lng, address_hint=addr)
rw = stage("road_width WFS query (台北 only)", _step_road_width)
print(f"      → road_width result: {rw and list(rw.keys())[:6]}")

# 6) LVR triangulate（建坪+地址 → 找完整門牌）
def _step_lvr():
    from analysis.lvr_index import triangulate_address
    bld = mobile.get("building_area_ping")
    if not bld or not addr or not district:
        return None
    return triangulate_address(
        city=city,
        district=district,
        road_seg=addr,
        total_floors=4,
        building_area_ping=bld,
        floor=mobile.get("floor"),
        coord=(lat, lng) if lat and lng else None,
    )
lvr = stage("LVR triangulate", _step_lvr)
print(f"      → LVR candidates: {len(lvr) if lvr else 0}")

# 7) make_property_doc 組裝
def _step_makedoc():
    # 跳過完整 make_property_doc（需要 scores/renewal/final 物件，dry run 不需要組這些）
    # 只 mock 一下 dict assembly 的時間（其實只是 dict 操作，~0ms）
    return {
        "source_id": f"591_{HID}",
        "city": city, "district": district, "address": addr,
        "land_area_ping": mobile.get("land_area_ping"),
        "building_area_ping": mobile.get("building_area_ping"),
        "building_age": mobile.get("building_age"),
        "price_ntd": mobile.get("price_ntd"),
        "floor": mobile.get("floor"),
        "title": mobile.get("title"),
    }
doc = stage("make_property_doc 組裝", _step_makedoc)
print(f"      → doc keys: {len(doc) if doc else 0}")

# === total ===
total = sum(dt for _, dt in stages)
print()
print("=" * 60)
print(f"  TOTAL: {total:.2f}s ({len(stages)} stages)")
print("=" * 60)
print()
print("=== 對比 ===")
print(f"  目前 production (Playwright + OCR):  100-110s")
print(f"  Dry run (mobile API + WMS):         {total:.1f}s")
print(f"  省下:                                {100 - total:.1f}s ({(1 - total/100)*100:.0f}%)")
