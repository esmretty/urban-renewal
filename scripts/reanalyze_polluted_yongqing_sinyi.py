"""對 12 筆永慶/信義座標被污染的 doc 跑 reanalyze。

座標污染條件：lat/lng 跟 source_lat/source_lng 偏移 > 11m，
原因是舊版 pipeline section 5.4 elif lvr_address 對永慶/信義 也 geocode_address(reverse 結果)
覆蓋 lat/lng（已於 commit 24d5815 修復）。

用法：python scripts/reanalyze_polluted_yongqing_sinyi.py
（無 prompt，直接跑 12 筆）
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main():
    from database.db import get_firestore
    from api.app import _scrape_single_url

    db = get_firestore()
    col = db.collection("properties")

    # 找污染 doc：永慶/信義 source + lat/lng 跟 source_lat/source_lng 偏移 > 11m (≈ 0.0001 度)
    docs = list(col.stream())
    polluted = []
    for d in docs:
        dd = d.to_dict() or {}
        sources = dd.get("sources") or []
        if not any(s.get("name") in ("永慶", "信義") for s in sources):
            continue
        slat = dd.get("source_latitude")
        slng = dd.get("source_longitude")
        lat = dd.get("latitude")
        lng = dd.get("longitude")
        if not (slat and lat and slng and lng):
            continue
        d_lat = abs(slat - lat)
        d_lng = abs(slng - lng)
        if d_lat > 0.0001 or d_lng > 0.0001:
            # 取第一個 alive yongqing/sinyi source URL
            src_url = None
            src_id_full = None
            for s in sources:
                if s.get("alive") is False:
                    continue
                name = s.get("name")
                if name not in ("永慶", "信義"):
                    continue
                src_url = s.get("url")
                sid_raw = s.get("source_id") or ""
                if name == "永慶":
                    src_id_full = sid_raw if sid_raw.startswith("yongqing_") else f"yongqing_{sid_raw}"
                elif name == "信義":
                    src_id_full = sid_raw if sid_raw.startswith("sinyi_") else f"sinyi_{sid_raw}"
                if src_url and src_id_full:
                    break
            if src_url and src_id_full:
                polluted.append({
                    "doc_id": d.id,
                    "url": src_url,
                    "src_id": src_id_full,
                    "before_lat": lat, "before_lng": lng,
                    "src_lat": slat, "src_lng": slng,
                    "address": dd.get("address_inferred") or dd.get("address") or "",
                    "district": dd.get("district") or "",
                })

    logger.info(f"找到 {len(polluted)} 筆污染 doc 需要 reanalyze")
    for i, p in enumerate(polluted):
        logger.info(f"  [{i+1}/{len(polluted)}] {p['doc_id']} | {p['district']} {p['address'][:40]} | {p['src_id']}")

    if not polluted:
        return

    results = []
    for i, p in enumerate(polluted):
        logger.info(f"\n=== [{i+1}/{len(polluted)}] reanalyze {p['doc_id']} | {p['src_id']} ===")
        try:
            r = _scrape_single_url(p["url"], p["src_id"], is_reanalyze=True, mark_user_url=False)
            logger.info(f"  結果: {r}")
            # 重抓後 doc 狀態
            new_dd = col.document(p["doc_id"]).get().to_dict() or {}
            new_lat = new_dd.get("latitude")
            new_lng = new_dd.get("longitude")
            new_slat = new_dd.get("source_latitude")
            new_slng = new_dd.get("source_longitude")
            ok = False
            if new_lat and new_slat:
                d_lat = abs(new_lat - new_slat)
                d_lng = abs(new_lng - new_slng) if new_lng and new_slng else 0
                ok = (d_lat <= 0.0001 and d_lng <= 0.0001)
            results.append({
                "doc_id": p["doc_id"],
                "address": p["address"],
                "district": p["district"],
                "before_lat": p["before_lat"], "before_lng": p["before_lng"],
                "after_lat": new_lat, "after_lng": new_lng,
                "src_lat": new_slat, "src_lng": new_slng,
                "drift_after_m_lat": (abs(new_lat - new_slat) * 111000) if (new_lat and new_slat) else None,
                "drift_after_m_lng": (abs(new_lng - new_slng) * 100000) if (new_lng and new_slng) else None,
                "fixed": ok,
                "address_inferred_after": new_dd.get("address_inferred"),
            })
        except Exception as e:
            logger.exception(f"  失敗: {e}")
            results.append({"doc_id": p["doc_id"], "error": str(e)[:200], "fixed": False})

    # 報告
    print("\n\n=== Self-verify 報告 ===")
    fixed_count = sum(1 for r in results if r.get("fixed"))
    print(f"成功修正：{fixed_count}/{len(results)}")
    print()
    for r in results:
        if r.get("error"):
            print(f"❌ {r['doc_id']}: ERROR {r['error']}")
            continue
        status = "✅" if r["fixed"] else "❌"
        dlat_m = r.get("drift_after_m_lat") or 0
        dlng_m = r.get("drift_after_m_lng") or 0
        print(f"{status} {r['doc_id']} | {r['district']} {(r['address'] or '')[:30]}")
        print(f"   before: ({r['before_lat']:.6f}, {r['before_lng']:.6f})")
        print(f"   after:  ({r['after_lat']:.6f}, {r['after_lng']:.6f}) | source: ({r['src_lat']:.6f}, {r['src_lng']:.6f}) | 偏移 lat={dlat_m:.0f}m lng={dlng_m:.0f}m")
        if r.get("address_inferred_after"):
            print(f"   addr:   {r['address_inferred_after']}")


if __name__ == "__main__":
    main()
