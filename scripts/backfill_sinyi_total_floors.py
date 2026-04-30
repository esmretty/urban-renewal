"""對信義 source 缺 total_floors 的 doc 補 total_floors。

之前 scraper 抓錯 key (用 totalfloor 在 contentData 永遠 None；正確 key 是 floors)，
fix 後新分析不再缺，但既有 doc 需要補。

直接從信義 NEXT_DATA contentData.floors 取值寫回，不用全 reanalyze（省 AI 費用）。
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def fetch_sinyi_floors(house_no: str):
    """從信義 NEXT_DATA contentData 抽 floors（總樓層）。失敗回 None。"""
    import httpx, json, re, urllib3
    urllib3.disable_warnings()
    url = f"https://www.sinyi.com.tw/buy/house/{house_no}"
    try:
        r = httpx.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0"},
            follow_redirects=True, timeout=15, verify=False,
        )
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>', r.text, re.DOTALL)
        if not m:
            return None
        data = json.loads(m.group(1))
        cd = data.get("props", {}).get("initialReduxState", {}).get("buyReducer", {}).get("contentData") or {}
        v = cd.get("floors") or cd.get("totalfloor")
        if v:
            return int(v)
    except Exception as e:
        logger.warning(f"  fetch sinyi {house_no} failed: {e}")
    return None


def main():
    from database.db import get_firestore
    db = get_firestore()
    col = db.collection("properties")

    # 找信義 source + total_floors=None 的 doc
    targets = []
    for d in col.stream():
        dd = d.to_dict() or {}
        if dd.get("total_floors") is not None:
            continue
        sources = dd.get("sources") or []
        for s in sources:
            if s.get("name") == "信義" and s.get("alive") is not False:
                sid = s.get("source_id") or ""
                house_no = sid.replace("sinyi_", "") if sid.startswith("sinyi_") else sid
                if house_no:
                    targets.append((d.id, house_no, dd))
                    break

    logger.info(f"找到 {len(targets)} 筆信義 source 缺 total_floors")

    for did, house_no, dd in targets:
        addr = dd.get("address_inferred") or dd.get("address") or ""
        logger.info(f"\n  {did} | sinyi_{house_no} | {addr[:40]}")
        floors = fetch_sinyi_floors(house_no)
        if floors:
            col.document(did).update({"total_floors": floors})
            logger.info(f"    → 補 total_floors = {floors}")
        else:
            logger.warning(f"    → 抓不到 floors，skip")


if __name__ == "__main__":
    main()
