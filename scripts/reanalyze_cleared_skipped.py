"""對「以前被舊邏輯標 analysis_status=skipped」+「score/AI 欄位被清空」的物件，
跑一次 force_reanalyze 把欄位補回。新 pipeline 會保留 is_remote_area / unsuitable_for_renewal
旗標，但不再短路 AI/score（資料完整給 client 自決）。

只跑 1 次。Idempotent — 重跑不會壞事，只是多花一次 Vision/Claude 費用。

用法：python scripts/reanalyze_cleared_skipped.py
"""
import sys
from pathlib import Path

# 讓 import 抓到 project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main():
    from database.db import get_firestore
    from api.app import _scrape_single_url

    db = get_firestore()
    col = db.collection("properties")

    # 鎖定條件：score_total=None 且至少有一個旗標（避免誤打到正常 doc）
    targets = []
    for d in col.stream():
        dd = d.to_dict() or {}
        if dd.get("score_total") is not None:
            continue
        if not (dd.get("is_remote_area") or dd.get("unsuitable_for_renewal")):
            continue
        sources = dd.get("sources") or []
        if not sources:
            logger.warning(f"  {d.id}: 無 sources，跳過")
            continue
        # 取第一個 alive=True 的 source
        first_src = next((s for s in sources if s.get("alive") is not False and s.get("url")), None)
        if not first_src:
            logger.warning(f"  {d.id}: 無 alive sources，跳過")
            continue
        targets.append({
            "doc_id": d.id,
            "url": first_src["url"],
            "src_id": (
                f"{first_src.get('name', '591')}_{first_src['source_id']}"
                if first_src.get("name") in ("yongqing", "sinyi", "永慶", "信義")
                else first_src["source_id"]
            ),
            "address": dd.get("address_inferred") or dd.get("address") or "(無地址)",
            "flags": [k for k in ("is_remote_area", "unsuitable_for_renewal") if dd.get(k)],
        })

    logger.info(f"找到 {len(targets)} 筆需要 reanalyze")
    for i, t in enumerate(targets):
        logger.info(f"  [{i+1}/{len(targets)}] {t['doc_id']} | {t['address']} | flags={t['flags']}")

    if not targets:
        logger.info("無需處理，結束")
        return

    for i, t in enumerate(targets):
        logger.info(f"\n=== [{i+1}/{len(targets)}] {t['doc_id']} | {t['address']} ===")
        try:
            # is_reanalyze=True: 跳過 listing 過濾（公寓 only 等），強制重跑既有 doc
            # mark_user_url=False: 不要標 source_origin=user_url（這是 admin 維護操作）
            result = _scrape_single_url(t["url"], t["src_id"], is_reanalyze=True, mark_user_url=False)
            logger.info(f"  結果：{result}")
        except Exception as e:
            logger.exception(f"  失敗：{e}")

    # 二次驗證
    logger.info("\n=== 重跑後狀態 ===")
    for t in targets:
        d = col.document(t["doc_id"]).get()
        if not d.exists:
            logger.warning(f"  {t['doc_id']}: doc 不見了！")
            continue
        dd = d.to_dict()
        logger.info(
            f"  {t['doc_id']}: score_total={dd.get('score_total')}, "
            f"ai_recommendation={dd.get('ai_recommendation')}, "
            f"is_remote_area={dd.get('is_remote_area')}, "
            f"unsuitable_for_renewal={dd.get('unsuitable_for_renewal')}"
        )


if __name__ == "__main__":
    main()
