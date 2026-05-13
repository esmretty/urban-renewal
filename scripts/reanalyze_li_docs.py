"""對 5 筆「之前 address 含里被污染」的 doc 重跑完整 reanalyze pipeline，
拿到正確 road_width / 地籍圖截圖。

執行：python scripts/reanalyze_li_docs.py

每筆會重新打 Playwright 開 591 / 永慶 / 信義詳情頁，重抓地籍圖 + Vision OCR
辨識物件所在路名 + 查路寬。預估每筆 ~10-20 秒。
"""
import os, sys, time, json

# 強制 stdout 用 UTF-8 (避免 Windows cp950 console 印 CJK / unicode 符號 crash)
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "firebase-credentials.json")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from database.db import init_db, get_col

DOC_IDS = [
    "20260505-5285cc",  # 三民路117巷6弄 (新店)
    "20260505-56e25d",  # 文化路二段410巷 (板橋, 永慶)
    "20260505-8d0325",  # 木柵路一段260巷 (文山)
    "20260505-b36c65",  # 合江街105巷 (中山)
    "20260505-c40ecb",  # 安康路三段355巷 (新店)
]


def fetch_state(col, did):
    d = col.document(did).get()
    if not d.exists:
        return None
    data = d.to_dict() or {}
    return {
        "id": did,
        "city": data.get("city"),
        "address_inferred": data.get("address_inferred"),
        "road_width_name": data.get("road_width_name"),
        "road_width_m": data.get("road_width_m"),
        "has_screenshot_roadwidth": bool(data.get("screenshot_roadwidth")),
    }


def main():
    init_db()
    col = get_col()

    print("\n=== BEFORE ===")
    before = {did: fetch_state(col, did) for did in DOC_IDS}
    for did, s in before.items():
        print(f"  [{did}] road_width_name={s['road_width_name']!r} road_width_m={s['road_width_m']}")

    # Lazy import to delay Playwright init
    from api.routers.admin_scrape import _scrape_single_url

    print("\n=== REANALYZE ===")
    for did in DOC_IDS:
        d = col.document(did).get()
        if not d.exists:
            print(f"  [{did}] ❌ doc not found")
            continue
        data = d.to_dict() or {}
        sources = data.get("sources") or []
        if not sources:
            print(f"  [{did}] ❌ no sources")
            continue
        for s in sources:
            src_id = s.get("source_id") or did
            url = s.get("url")
            if not url:
                continue
            print(f"  [{did}] reanalyze source={s.get('name')} url={url[:80]}")
            t0 = time.time()
            try:
                _scrape_single_url(url, src_id, is_reanalyze=True)
                print(f"    [OK] done in {time.time()-t0:.1f}s")
            except Exception as e:
                print(f"    [FAIL] {e}")

    print("\n=== AFTER ===")
    after = {did: fetch_state(col, did) for did in DOC_IDS}
    for did, s in after.items():
        bs = before[did]
        print(f"  [{did}]")
        print(f"     road_width_name: {bs['road_width_name']!r}")
        print(f"                  →  {s['road_width_name']!r}")
        print(f"     road_width_m  : {bs['road_width_m']} → {s['road_width_m']}")
        print(f"     screenshot    : {bs['has_screenshot_roadwidth']} → {s['has_screenshot_roadwidth']}")

    # 寫一份 JSON 給 user 之後可看
    with open(os.path.join(ROOT, "scripts", "_reanalyze_li_result.json"), "w", encoding="utf-8") as f:
        json.dump({"before": before, "after": after}, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
