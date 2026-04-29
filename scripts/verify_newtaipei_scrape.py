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
    samples = []

    # === 591 永和+中和 各 2 筆 ===
    items = _fetch_listing_page_api(
        region_id="3", section_id="37,38", shape="1", first_row=0,
        city="新北市",
        target_districts={"永和區", "中和區"},
    )
    print(f"591 listing: {len(items)} 筆")
    samples += [it for it in items if it["district"] == "永和區"][:2]
    samples += [it for it in items if it["district"] == "中和區"][:2]

    # === 永慶 永和 1 筆 + 中和 1 筆 ===
    try:
        from scraper.scraper_yongqing import _build_list_url, _parse_listing_page
        import httpx
        url_yc = _build_list_url(["新北市-永和區", "新北市-中和區"], ["無電梯公寓"], page=1)
        r = httpx.get(url_yc, timeout=30, follow_redirects=True, verify=False, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0",
            "Accept-Language": "zh-TW,zh;q=0.9"})
        yc_items = _parse_listing_page(r.text)
        print(f"永慶 listing (永和+中和): {len(yc_items)} 筆")
        samples += yc_items[:2]
    except Exception as e:
        print(f"永慶 listing failed: {e}")

    # === 信義 永和 1 筆 + 中和 1 筆 ===
    try:
        from scraper.scraper_sinyi import _build_list_url as _bs, _fetch as _fs, _parse_next_data, _item_from_listing
        url_s = _bs(["永和區", "中和區"], "公寓", page=1)
        html_s = _fs(url_s)
        nd = _parse_next_data(html_s) if html_s else None

        def find_listings(obj, depth=0):
            if depth > 8 or obj is None:
                return None
            if isinstance(obj, dict):
                for k in ("list", "listings", "results", "data", "items", "houseList"):
                    v = obj.get(k)
                    if isinstance(v, list) and len(v) > 5 and all(isinstance(x, dict) for x in v[:3]):
                        if "houseNo" in v[0] or "caseId" in v[0]:
                            return v
                for v in obj.values():
                    r = find_listings(v, depth + 1)
                    if r:
                        return r
            elif isinstance(obj, list):
                for it in obj:
                    r = find_listings(it, depth + 1)
                    if r:
                        return r
        listings = find_listings(nd)
        s_items = []
        for it in (listings or []):
            x = _item_from_listing(it)
            if x:
                s_items.append(x)
        print(f"信義 listing (永和+中和): {len(s_items)} 筆")
        # 各區一筆
        yh_pick = next((x for x in s_items if x.get("district") == "永和區"), None)
        zh_pick = next((x for x in s_items if x.get("district") == "中和區"), None)
        if yh_pick:
            samples.append(yh_pick)
        if zh_pick:
            samples.append(zh_pick)
    except Exception as e:
        print(f"信義 listing failed: {e}")

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
