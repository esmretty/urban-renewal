"""
都更神探R - 主程式入口

使用方式：
  python main.py              → 啟動 Web 介面（http://localhost:8000）
  python main.py --scrape     → 只執行爬取 + 分析（不啟動伺服器）
  python main.py --scrape --district 新店區  → 只爬指定地區
  python main.py --port 8080  → 指定 port
"""
import sys
import logging
import argparse
import uvicorn

# 載入 .env（LINE_CHANNEL_TOKEN / LINE_USER_ID / Anthropic 等）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass   # 沒裝 python-dotenv 也不擋啟動

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_server(port: int = 8000):
    logger.info(f"啟動都更神探R → http://localhost:{port}")
    uvicorn.run(
        "api.app:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="warning",
    )


def run_scrape_cli(district: str = "", headless: bool = False, limit: int = 0):
    from database.db import init_db, get_col
    from database.models import make_property_doc
    from scraper.scraper_591 import scrape_591
    from analysis.geocoder import geocode_address, get_nearest_mrt
    from analysis.scorer import calculate_score, calculate_renewal_value
    from analysis.claude_analyzer import analyze_property_text, generate_final_recommendation
    from scraper.zoning_lookup import lookup_zoning
    from scraper.browser_manager import get_browser_context
    from database.models import should_skip_analysis, make_minimal_doc
    from datetime import datetime
    from database.time_utils import now_tw_iso
    from config import TARGET_REGIONS

    init_db()
    col = get_col()

    # 自動決定配額
    if limit <= 0:
        n = 1 if district else sum(len(d["districts"]) for d in TARGET_REGIONS.values())
        limit = 10 if n == 1 else (20 if n == 2 else 30)

    logger.info(f"=== 都更神探R 爬取開始（最多 {limit} 筆）===")

    def progress(msg: str):
        logger.info(msg)

    def check_exists(source_id: str):
        doc = col.document(source_id).get()
        return doc.to_dict() if doc.exists else None

    result = scrape_591(
        headless=headless,
        progress_callback=progress,
        district_filter=district,
        check_exists=check_exists,
        limit=limit,
    )

    new_items = result["new"]
    price_updates = result["price_updates"]

    logger.info(f"爬取完成：{len(new_items)} 筆新物件，{len(price_updates)} 筆價格變動")

    # 處理價格變動
    for pu in price_updates:
        ref = col.document(pu["source_id"])
        doc = ref.get()
        if doc.exists:
            existing = doc.to_dict()
            history = existing.get("price_history") or []
            history.append({"price": pu["old_price"], "scraped_at": existing.get("scraped_at")})
            ref.update({
                "price_ntd": pu["new_price"],
                "price_history": history,
                "is_price_changed": True,
                "scraped_at": now_tw_iso(),
            })
            logger.info(
                f"  ⚠️ 價格變動：{pu['district']} {pu['title'][:20]} "
                f"{int(pu['old_price']//10000)}萬→{int(pu['new_price']//10000)}萬"
            )

    # 分析並儲存新物件（共用一個 browser context 給 zoning lookup）
    new_count = 0
    with get_browser_context(headless=headless) as zoning_ctx:
     for item in new_items:
      try:
            logger.info(f"  分析：{item.get('district')} {item.get('title', '')[:30]}")

            lat, lng = None, None
            if item.get("address"):
                # address 為純地址，geocode 前拼回 city/district
                _full = f"{item.get('city','')}{item.get('district','')}{item['address']}"
                coords = geocode_address(_full)
                if coords:
                    lat, lng = coords

            nearest_mrt, mrt_dist = get_nearest_mrt(lat, lng) if lat else (None, None)

            # 嚴格：只用真實的土地坪數轉 m²，不從建物坪數估算（防幻覺）
            land_sqm = None
            if item.get("land_area_ping"):
                land_sqm = item["land_area_ping"] * 3.30578

            # 判定是否跳過分析（CLI 用預設門檻，前端爬取可自訂）
            skip, reason = should_skip_analysis(item, None)
            if skip:
                minimal = make_minimal_doc(
                    item=item, lat=lat, lng=lng,
                    nearest_mrt=nearest_mrt, mrt_dist=mrt_dist,
                    land_sqm=land_sqm, skip_reason=reason,
                )
                col.document(item["source_id"]).set(minimal)
                new_count += 1
                logger.info(f"    ⏭ 跳過分析：{reason}")
                continue

            scores = calculate_score(
                building_age=item.get("building_age"),
                land_area_sqm=land_sqm,
                nearest_mrt_dist_m=mrt_dist,
            )
            renewal = calculate_renewal_value(
                land_area_sqm=land_sqm,
                legal_far=None,
                building_age=item.get("building_age"),
                nearest_mrt_dist_m=mrt_dist,
                price_ntd=item.get("price_ntd"),
                city=item.get("city"),
            )
            text_analysis = analyze_property_text({
                **item,
                "nearest_mrt": nearest_mrt,
                "nearest_mrt_dist_m": mrt_dist,
            })
            final = generate_final_recommendation(
                property_data=item,
                score=scores,
                renewal_calc=renewal,
                text_analysis=text_analysis,
            )

            doc_data = make_property_doc(
                item=item, scores=scores, renewal=renewal,
                text_analysis=text_analysis, final=final,
                lat=lat, lng=lng, nearest_mrt=nearest_mrt,
                mrt_dist=mrt_dist, land_sqm=land_sqm,
            )

            # 台北市/新北市：查土地分區（用推測地址座標更準確）
            if item.get("city") in ("台北市", "新北市"):
                try:
                    z_lat, z_lng = lat, lng
                    if doc_data.get("address_inferred"):
                        _inf_full = f"{item.get('city','')}{item.get('district','')}{doc_data['address_inferred']}"
                        ic = geocode_address(_inf_full)
                        if ic:
                            z_lat, z_lng = ic
                    z = lookup_zoning(
                        address=item.get("address"),
                        lat=z_lat, lng=z_lng,
                        building_area_ping=item.get("building_area_ping"),
                        city=item.get("city"),
                        ctx=zoning_ctx,
                    )
                    doc_data.update({
                        "zoning": z["zoning"],
                        "zoning_candidates": z["zoning_candidates"],
                        "zoning_source": z["zoning_source"],
                        "zoning_source_url": z.get("zoning_source_url"),
                        "zoning_lookup_at": z["zoning_lookup_at"],
                        "zoning_error": z.get("error"),
                        "address_probable": z["address_probable"],
                        "zoning_original": z.get("original_zone"),
                    })
                    logger.info(f"    zoning: {z['zoning']} ({z['zoning_source']})")
                except Exception as ze:
                    logger.warning(f"    zoning lookup 失敗: {ze}")

            col.document(item["source_id"]).set(doc_data)
            new_count += 1

      except Exception as e:
        logger.error(f"分析失敗 {item.get('source_id')}: {e}")

    logger.info(f"=== 完成：新增 {new_count} 筆，價格變動 {len(price_updates)} 筆 ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="都更神探R")
    parser.add_argument("--scrape", action="store_true", help="只執行爬取（不啟動伺服器）")
    parser.add_argument("--port", type=int, default=8000, help="Web 伺服器 port（預設 8000）")
    parser.add_argument("--district", type=str, default="", help="只爬指定地區，例如 大安區")
    parser.add_argument("--headless", action="store_true", default=False, help="無頭模式（預設 False）")
    parser.add_argument("--limit", type=int, default=0, help="最多抓幾筆（0=依地區數自動決定）")
    args = parser.parse_args()

    if args.scrape:
        run_scrape_cli(district=args.district, headless=args.headless, limit=args.limit)
    else:
        run_server(port=args.port)
