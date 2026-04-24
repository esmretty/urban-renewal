"""
測試 zoning lookup：跑一筆 DB 裡的記錄看結果。
用法：python scraper/test_zoning_lookup.py [source_id]
預設跑 591_20039030（辛亥路三段157巷）
"""
import sys
import json
import logging
from scraper.zoning_lookup import lookup_zoning
from database.db import init_db, get_col

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    src_id = sys.argv[1] if len(sys.argv) > 1 else "591_20039030"
    init_db()
    doc = get_col().document(src_id).get()
    if not doc.exists:
        print(f"找不到 {src_id}")
        return
    d = doc.to_dict()
    print(f"測試記錄：{src_id}")
    print(f"  address           = {d.get('address')}")
    print(f"  city              = {d.get('city')}")
    print(f"  building_area_ping= {d.get('building_area_ping')}")
    print(f"  lat, lng          = {d.get('latitude')}, {d.get('longitude')}")
    print()

    result = lookup_zoning(
        address=d.get("address"),
        lat=d.get("latitude"),
        lng=d.get("longitude"),
        building_area_ping=d.get("building_area_ping"),
        city=d.get("city"),
    )
    print("結果：")
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
