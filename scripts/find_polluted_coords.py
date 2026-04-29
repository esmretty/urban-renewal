"""掃 properties 找「lat/lng 跟 city 對不上」的污染物件。

bug：之前 LVR triangulate 回的 address 沒帶 city/district 前綴（例：「信義路82號1樓」），
geocode_address 直接打給 Google 會解到全台同名路（永和信義路 → 高雄信義路 → lat 22.88）。

合理範圍：
  台北市 lat 24.95~25.20, lng 121.45~121.65
  新北市 lat 24.55~25.30, lng 121.30~122.00（範圍較廣，含偏遠區）

執行：
  python scripts/find_polluted_coords.py        # 列出污染清單
  python scripts/find_polluted_coords.py --reset # 把那些 doc 的 lat/lng/zoning/road_width
                                                  # 全清空 + 標 needs_reanalysis=true，admin 可看
"""
import sys, os, argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import init_db, get_col

CITY_BOUNDS = {
    "台北市": {"lat": (24.95, 25.20), "lng": (121.45, 121.65)},
    "新北市": {"lat": (24.55, 25.30), "lng": (121.30, 122.00)},
}


def main(reset: bool):
    init_db()
    col = get_col()
    docs = list(col.stream())
    print(f"total docs: {len(docs)}")

    polluted = []
    for d in docs:
        x = d.to_dict() or {}
        city = x.get("city") or ""
        lat = x.get("latitude")
        lng = x.get("longitude")
        if city not in CITY_BOUNDS:
            continue
        if lat is None or lng is None:
            continue
        b = CITY_BOUNDS[city]
        if not (b["lat"][0] <= lat <= b["lat"][1] and b["lng"][0] <= lng <= b["lng"][1]):
            polluted.append({
                "doc_id": d.id,
                "src": x.get("source_id", "?"),
                "city": city,
                "district": x.get("district"),
                "addr": x.get("address_inferred") or x.get("address") or "",
                "lat": lat,
                "lng": lng,
                "near_mrt_dist": x.get("nearest_mrt_dist_m"),
            })

    print(f"\n=== 污染物件（lat/lng 不在 city 範圍）：{len(polluted)} 筆 ===")
    for p in polluted:
        print(f"  {p['doc_id']:18s} {p['src']:25s} {p['city']}{p['district']} | "
              f"({p['lat']:.4f}, {p['lng']:.4f}) | mrt 距離 {p['near_mrt_dist']!s}m | {p['addr'][:40]}")

    if not reset:
        print("\n[dry-run] 加 --reset 會把污染欄位清空 + 標 needs_reanalysis=true（user 重新分析後會修）")
        return

    print(f"\n清污染中...")
    cleaned = 0
    for p in polluted:
        try:
            col.document(p["doc_id"]).update({
                "latitude": None,
                "longitude": None,
                "zoning": None,
                "zoning_source": None,
                "road_width_m": None,
                "road_width_name": None,
                "screenshot_roadwidth": None,
                "nearest_mrt": None,
                "nearest_mrt_dist_m": None,
                "needs_reanalysis": True,
                "needs_reanalysis_reason": f"座標跑到 city ({p['city']}) 範圍外（bug 修前 LVR geocode 沒帶前綴），請重新分析",
            })
            cleaned += 1
        except Exception as e:
            print(f"  ✗ {p['doc_id']}: {e}")
    print(f"完成：清掉 {cleaned} 筆 doc 的污染欄位。前端會看到「分析資料缺失」，user 按重新分析會修。")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="實際清污染欄位（預設 dry-run）")
    args = ap.parse_args()
    main(reset=args.reset)
