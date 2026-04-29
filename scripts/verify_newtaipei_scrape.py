"""一次性驗證腳本：跑 永和+中和 各 2 筆 完整 pipeline，把最終 doc 印出對照人工檢查。

不寫 DB（dry-run）— 只跑 listing fetch + detail screenshot + Vision OCR + zoning + LVR，
最後把 final doc_data dump 出來看每個欄位是否合理。

執行（在 VM 上）：
  python scripts/verify_newtaipei_scrape.py
"""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import init_db
from scraper.scraper_591 import _fetch_listing_page_api
from scraper.browser_manager import get_browser_context
from api.analysis_pipeline import analyze_single_property


def main():
    init_db()
    items = _fetch_listing_page_api(
        region_id="3", section_id="37,38", shape="1", first_row=0,
        city="新北市",
        target_districts={"永和區", "中和區"},
    )
    print(f"listing API: {len(items)} 筆")
    samples = (
        [it for it in items if it["district"] == "永和區"][:2]
        + [it for it in items if it["district"] == "中和區"][:2]
    )

    def step(msg):
        print(f"  {msg}")

    with get_browser_context(headless=True) as ocr_ctx:
        for it in samples:
            print()
            print("=" * 80)
            sid = it["source_id"]
            print(f"[{sid}] {it['district']} | {it['title'][:40]}")
            print(f"  url: {it['url']}")
            print(f"  --- listing API 欄位 ---")
            for k in ("district", "address", "total_floors", "floor",
                      "building_age", "building_area_ping", "price_ntd"):
                print(f"    {k}: {it.get(k)}")
            try:
                result = analyze_single_property(
                    item=dict(it),
                    ocr_ctx=ocr_ctx,
                    step_fn=step,
                    initial_coords=None,
                    detail_text="",
                    thresholds=None,
                )
            except Exception as e:
                print(f"  PIPELINE FAILED: {e}")
                continue

            doc = result.get("doc_data", {})
            print(f"  --- pipeline 完整跑後 doc_data 重要欄位 ---")
            for k in ("district", "address", "address_inferred",
                     "address_inference_confidence",
                     "land_area_ping", "building_area_ping", "building_age",
                     "total_floors", "floor", "price_ntd",
                     "latitude", "longitude",
                     "zoning", "zoning_original", "zoning_source",
                     "road_width_m", "road_width_name",
                     "nearest_mrt", "nearest_mrt_dist_m",
                     "score_total"):
                print(f"    {k}: {doc.get(k)}")
            print(f"  status: {result.get('status')}")


if __name__ == "__main__":
    main()
