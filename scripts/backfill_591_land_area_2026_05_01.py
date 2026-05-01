"""對所有 591 source + land_area_ping=None 的 doc 跑 backfill。

之前 OCR 用 2×2 切片把 row 切爛漏抓土地坪數，commit e40a697 改成整張 OCR
後實測命中率 4/6，現在補抓既有資料。

每筆動作：
1. Playwright 開 591 detail page → 截圖 (走既有 screenshot_detail_page)
2. 用新 extract_full_detail_from_screenshot (整張 OCR) 抓 land_area_ping
3. 抓到 → col.document(doc_id).update({'land_area_ping': X})
4. 抓不到 → log skip

不動其他欄位。每 5 筆 recycle Playwright context 避免 leak（雖已修但保險）。
"""
import sys, time, gc, logging, os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Windows cp950 console 對 emoji 會 raise UnicodeEncodeError → 強制 utf-8 輸出
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

import psutil
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.WARNING, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

from scraper.browser_manager import get_browser_context_with_browser, _build_ctx
from scraper.scraper_591 import screenshot_detail_page
from analysis.claude_analyzer import extract_full_detail_from_screenshot
from database.db import get_firestore


def main():
    db = get_firestore()
    col = db.collection('properties')

    # 撈 targets
    targets = []
    for d in col.stream():
        dd = d.to_dict() or {}
        if dd.get('deleted'): continue
        sources = dd.get('sources') or []
        src_591 = next((s for s in sources if s.get('name') == '591' and s.get('alive') is not False), None)
        if not src_591: continue
        if dd.get('land_area_ping') is not None: continue
        if dd.get('is_remote_area') or dd.get('is_foreclosure') or dd.get('unsuitable_for_renewal'): continue
        url = src_591.get('url')
        if not url: continue
        sid = src_591.get('source_id') or ''
        targets.append((d.id, sid, url, dd))
    print(f'共 {len(targets)} 筆要 backfill', flush=True)

    p = psutil.Process()
    n_filled = 0
    n_skipped = 0

    with get_browser_context_with_browser(headless=True) as (ocr_ctx_init, browser):
        ocr_ctx = ocr_ctx_init
        for idx, (doc_id, sid, url, dd) in enumerate(targets, 1):
            # 每 5 筆 recycle ctx 避免 Chromium memory cache（雖然 PIL leak 已修還是保險）
            if idx > 1 and (idx - 1) % 5 == 0:
                try:
                    ocr_ctx.close()
                except Exception: pass
                gc.collect()
                ocr_ctx = _build_ctx(browser)

            rss = p.memory_info().rss / 1048576
            print(f'\n[{idx}/{len(targets)}] {doc_id} sid={sid} rss={rss:.0f}MB', flush=True)
            print(f'  addr={(dd.get("address_inferred") or dd.get("address") or "")[:30]}, bld={dd.get("building_area_ping")}', flush=True)

            try:
                ret = screenshot_detail_page(ocr_ctx, url, f'backfill_{doc_id}')
                if not ret:
                    print('  ❌ 截圖失敗', flush=True)
                    n_skipped += 1
                    continue
                if getattr(ret, 'delisted', False):
                    print('  ⚠️ 已下架', flush=True)
                    n_skipped += 1
                    continue
                shot_path, _, _ = ret[:3]
                house_path = getattr(ret, 'house_path', None)
                # 優先用 house_path（聚焦在房屋介紹 section，更準）
                target_path = house_path if (house_path and os.path.exists(house_path)) else shot_path
                if not target_path or not os.path.exists(target_path):
                    print('  ❌ 截圖檔不存在', flush=True)
                    n_skipped += 1
                    continue

                vision = extract_full_detail_from_screenshot(target_path)
                land = vision.get('land_area_ping')
                bld = vision.get('building_area_ping')
                print(f'  OCR: land={land}, bld={bld}', flush=True)
                if land is not None:
                    col.document(doc_id).update({'land_area_ping': float(land)})
                    n_filled += 1
                    print(f'  ✓ updated land_area_ping={land}', flush=True)
                else:
                    n_skipped += 1
                    print('  ⏭ 沒抓到（屋主可能真沒填）', flush=True)
            except Exception as e:
                logger.exception(f'  ❌ 例外 {doc_id}: {e}')
                n_skipped += 1

    print(f'\n=== Done: filled={n_filled}, skipped={n_skipped} ===')


if __name__ == '__main__':
    main()
