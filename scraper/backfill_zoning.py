"""
批次回填現有 DB 記錄的土地分區。
只處理台北市記錄、且尚未跑過 lookup 的。

用法：
  python -m scraper.backfill_zoning               # 全部跑
  python -m scraper.backfill_zoning --limit 3     # 只跑 3 筆
  python -m scraper.backfill_zoning --redo        # 強制重跑（覆蓋既有 lookup 結果）
"""
import argparse
import logging

from database.db import init_db, get_col
from scraper.zoning_lookup import lookup_zoning
from analysis.geocoder import geocode_address

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="最多跑幾筆（0=全部）")
    parser.add_argument("--redo", action="store_true", help="強制重跑已有 lookup 結果")
    args = parser.parse_args()

    init_db()
    col = get_col()
    docs = list(col.get())
    targets = []
    for d in docs:
        data = d.to_dict()
        if data.get("city") != "台北市":
            continue
        if not args.redo and data.get("zoning_lookup_at"):
            continue
        targets.append((d.id, data))

    if args.limit > 0:
        targets = targets[: args.limit]

    logger.info(f"共 {len(targets)} 筆待處理（台北市 + 未 lookup）")

    for i, (src_id, data) in enumerate(targets, 1):
        logger.info(f"[{i}/{len(targets)}] {src_id} {data.get('address')}")
        try:
            z_lat, z_lng = data.get("latitude"), data.get("longitude")
            _city = data.get("city", "")
            _dist = data.get("district", "")
            if data.get("address_inferred"):
                _inf_full = f"{_city}{_dist}{data['address_inferred']}"
                ic = geocode_address(_inf_full)
                if ic:
                    z_lat, z_lng = ic
                    logger.info(f"  使用推測地址座標: {_inf_full}")
            _addr_full = f"{_city}{_dist}{data.get('address') or ''}" if data.get('address') else None
            z = lookup_zoning(
                address=_addr_full,
                lat=z_lat,
                lng=z_lng,
                building_area_ping=data.get("building_area_ping"),
                city=data.get("city"),
                ctx=None,
            )
            col.document(src_id).update({
                "zoning": z["zoning"],
                "zoning_candidates": z["zoning_candidates"],
                "zoning_source": z["zoning_source"],
                "zoning_source_url": z.get("zoning_source_url"),
                "zoning_lookup_at": z["zoning_lookup_at"],
                "zoning_error": z.get("error"),
                "address_probable": z["address_probable"],
                "zoning_original": z.get("original_zone"),
            })
            logger.info(
                f"  → {z['zoning']!r} ({z['zoning_source']}) "
                f"候選 {len(z['zoning_candidates'])} 筆"
            )
        except Exception as e:
            logger.error(f"  失敗：{e}", exc_info=True)


if __name__ == "__main__":
    main()
