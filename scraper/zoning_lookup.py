"""
土地分區 lookup — v3：直接用座標查台北市 GeoServer WFS。

舊版的 5168 + TCD Playwright + Vision OCR 流程已棄用（保留 tcd_zoning.py 為參考）。
新版只走 analysis.gov_gis：座標 → 政府公開 GeoServer → 使用分區。

對外介面 lookup_zoning(...) 簽名不變，讓上游程式（api/app.py / main.py / backfill_zoning.py）
不必改動。
"""
import logging
from datetime import datetime
from typing import Optional

from analysis.gov_gis import lookup_zoning_by_coord, query_section_parcel
from database.time_utils import now_tw_iso

logger = logging.getLogger(__name__)


def lookup_zoning(
    *,
    address: str,
    lat: Optional[float],
    lng: Optional[float],
    building_area_ping: Optional[float] = None,  # 保留簽名相容；新流程不用
    city: str,
    ctx=None,                                      # 保留簽名相容；新流程不需 browser
) -> dict:
    """
    Args 與舊版相同（簽名相容）；實作改成單純座標 → GeoServer WFS。

    Returns:
        {
            "zoning": str | None,                # 標準名「第三種住宅區」
            "zoning_candidates": list,           # 永遠 []（新流程沒有候選）
            "address_probable": str | None,      # 永遠 None（新流程不用反查地址）
            "zoning_source": str,                # arcgis_taipei / not_found / unsupported_city / no_coord
            "zoning_source_url": str | None,
            "zoning_lookup_at": str,
            "error": str | None,
        }
    """
    z = lookup_zoning_by_coord(lat, lng, city)
    return {
        "zoning": z["zoning"],
        "zoning_candidates": [],
        "address_probable": None,
        "zoning_source": z["zoning_source"],
        "zoning_source_url": z["zoning_source_url"],
        "zoning_lookup_at": now_tw_iso(),
        "error": z["error"],
        "zone_label": z.get("zone_label"),
        "zone_code": z.get("zone_code"),
        "original_zone": z.get("original_zone"),
        "zone_list": z.get("zone_list"),
    }
