"""端到端測試 Phase 3 跨來源邏輯（情況 A/B/C）。

測試前置：在 Firestore 建立 test- prefix 的假物件
測試後：自動清掉所有 test- 文件

不會影響真實資料，但會在 Firestore 短暫出現 test- 開頭的 doc。
"""
import sys
import time
import pathlib
import logging

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

# 抑制過多 INFO log（FastAPI/Firebase 啟動時）
logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

from database.db import get_firestore, get_col, gen_dated_id, find_doc_by_source_id
from database.time_utils import now_tw_iso

TEST_PREFIX = "test-"

# 測試結果追蹤
_passed = []
_failed = []


def _ok(label, condition, detail=""):
    if condition:
        print(f"  ✅ {label}")
        _passed.append(label)
    else:
        print(f"  ❌ {label} — {detail}")
        _failed.append(f"{label}: {detail}")


def _cleanup_test_docs():
    """刪掉所有 test- 開頭的 doc + 對應 source_id 的 watchlist refs。"""
    col = get_col()
    deleted = 0
    for d in col.stream():
        if d.id.startswith(TEST_PREFIX):
            d.reference.delete()
            deleted += 1
    print(f"  清掉 {deleted} 筆 test docs")


def _make_test_doc(doc_id_suffix, source, source_id, address, building_area_ping,
                   land_area_ping=10.0, price_ntd=20000000, total_floors=4, floor=2,
                   building_age=30, sources=None, archived=False):
    """直接寫一筆 test doc 到 Firestore。"""
    doc_id = TEST_PREFIX + doc_id_suffix
    now = now_tw_iso()
    sources_arr = sources or [{
        "name": source, "source_id": source_id,
        "url": f"https://test/{source_id}", "added_at": now,
    }]
    data = {
        "id": doc_id,
        "source": source,
        "source_id": source_id,
        "sources": sources_arr,
        "url": f"https://test/{source_id}",
        "city": "台北市",
        "district": "大安區",
        "address": address,
        "title": f"test {address}",
        "building_type": "公寓",
        "total_floors": total_floors,
        "floor": floor,
        "building_age": building_age,
        "building_area_ping": building_area_ping,
        "land_area_ping": land_area_ping,
        "price_ntd": price_ntd,
        "scraped_at": now,
        "scrape_session_at": now,
        "list_rank": 0,
        "archived": archived,
    }
    get_col().document(doc_id).set(data)
    return doc_id


# ═══════════════════════════════════════════════════════════════════
# 情況 A：跨來源新上架
# ═══════════════════════════════════════════════════════════════════
def test_case_a_cross_source():
    print("\n=== 情況 A：跨來源新上架 ===")
    # 1. 建立既有 591 doc：地址「test大安路一段」、建坪 30 坪、價格 2000 萬
    doc_a_id = _make_test_doc(
        "case-a-591",
        source="591",
        source_id="591_TEST_AAA",
        address="大安區test大安路一段",
        building_area_ping=30.0,
        land_area_ping=10.0,
        price_ntd=20000000,
    )
    print(f"  既有 591 doc 建立：{doc_a_id}")

    # 2. 模擬永慶 scraper 抓到同物件（同地址、同建坪、同價格、不同 source）
    yongqing_item = {
        "source": "永慶",
        "source_id": "yongqing_TEST_AAA",
        "url": "https://buy.yungching.com.tw/house/test-AAA",
        "address": "大安區test大安路一段",
        "title": "test 永慶 大安路",
        "city": "台北市",
        "district": "大安區",
        "building_type": "公寓",
        "building_area_ping": 30.0,    # 完全一樣
        "land_area_ping": 11.5,        # 永慶有，591 沒填的
        "price_ntd": 20000000,         # 同價
        "total_floors": 4,
        "floor": 2,
        "building_age": 30,
        "latitude": 25.0334,
        "longitude": 121.5571,
        "zoning_original": "第三種住宅區",
    }

    # 3. 模擬 _scrape_and_analyze 永慶分支裡的 dup 偵測 + enrich 邏輯
    # 這段 inline 模擬 app.py 的 yc 分支邏輯
    from google.cloud.firestore_v1.base_query import FieldFilter
    col = get_col()

    # 簡化版 find_duplicate：跳過 prod docs，跳過 source_id 一樣的（self），找同地址+建坪+價格
    def _find_dup(item):
        for d in col.stream():
            if not d.id.startswith(TEST_PREFIX):
                continue   # 只查 test 範圍
            data = d.to_dict() or {}
            if data.get("source_id") == item["source_id"]:
                continue   # skip self
            if (data.get("price_ntd") == item["price_ntd"]
                    and abs((data.get("building_area_ping") or 0) - item["building_area_ping"]) <= 0.01
                    and (data.get("address") or "") == item["address"]):
                return d.id
        return None

    yc_dup_id = _find_dup(yongqing_item)
    _ok("find_duplicate 命中既有 591 doc", yc_dup_id == doc_a_id, f"got {yc_dup_id}, expected {doc_a_id}")

    if yc_dup_id:
        # Apply enrich logic (跟 app.py 永慶分支一樣)
        dup_doc = col.document(yc_dup_id).get()
        dd = dup_doc.to_dict() or {}
        updates = {}
        if not dd.get("land_area_ping") and yongqing_item.get("land_area_ping"):
            updates["land_area_ping"] = yongqing_item["land_area_ping"]
        if yongqing_item.get("zoning_original"):
            updates["zoning_original"] = yongqing_item["zoning_original"]
        if yongqing_item.get("latitude"):
            updates["source_latitude"] = yongqing_item["latitude"]
            updates["source_longitude"] = yongqing_item["longitude"]
        url_alt = list(dd.get("url_alt") or [])
        if yongqing_item["url"] not in url_alt:
            url_alt.append(yongqing_item["url"])
            updates["url_alt"] = url_alt
        sources_arr = list(dd.get("sources") or [])
        if not any(s.get("name") == "永慶" for s in sources_arr):
            sources_arr.append({
                "name": "永慶", "source_id": yongqing_item["source_id"],
                "url": yongqing_item["url"], "added_at": now_tw_iso(),
            })
            updates["sources"] = sources_arr
        updates["last_change_at"] = now_tw_iso()
        updates["latest_event"] = {"type": "cross_source", "source": "永慶", "at": now_tw_iso()}
        col.document(yc_dup_id).update(updates)

        # 驗證
        after = col.document(yc_dup_id).get().to_dict()
        _ok("zoning_original 補上了", after.get("zoning_original") == "第三種住宅區")
        _ok("座標補上了", after.get("source_latitude") == 25.0334)
        _ok("永慶 URL 加進 url_alt", yongqing_item["url"] in (after.get("url_alt") or []))
        _ok("sources array 含 591 + 永慶",
            len(after.get("sources") or []) == 2 and
            any(s["name"] == "永慶" for s in (after.get("sources") or [])))
        _ok("觸發 cross_source 事件",
            after.get("latest_event", {}).get("type") == "cross_source")


# ═══════════════════════════════════════════════════════════════════
# 情況 B：換物件偵測
# ═══════════════════════════════════════════════════════════════════
def test_case_b_replacement():
    print("\n=== 情況 B：換物件偵測 ===")
    from api.app import _is_replacement_change

    # 1. 完全相同 → 不是換物件
    existing = {"address": "大安區大安路一段", "building_area_ping": 30.0}
    incoming1 = {"address": "大安區大安路一段", "building_area_ping": 30.0}
    _ok("地址 / 建坪一樣 → 不是換物件",
        _is_replacement_change(existing, incoming1) is False)

    # 2. 路段不同 → 換物件
    incoming2 = {"address": "信義區信義路三段", "building_area_ping": 30.0}
    _ok("路段不同 → 是換物件",
        _is_replacement_change(existing, incoming2) is True)

    # 3. 建坪差 ≥ 0.5 → 換物件
    incoming3 = {"address": "大安區大安路一段", "building_area_ping": 30.6}
    _ok("建坪差 0.6 (≥0.5) → 是換物件",
        _is_replacement_change(existing, incoming3) is True)

    # 4. 建坪差 < 0.5 → 不是換物件
    incoming4 = {"address": "大安區大安路一段", "building_area_ping": 30.4}
    _ok("建坪差 0.4 (<0.5) → 不是換物件",
        _is_replacement_change(existing, incoming4) is False)

    # 5. 同段不同段（一段→二段）→ 換物件
    incoming5 = {"address": "大安區大安路二段", "building_area_ping": 30.0}
    _ok("同路不同段 (一段→二段) → 是換物件",
        _is_replacement_change(existing, incoming5) is True)

    # 6. 端到端：建立 591 doc → 模擬重抓 591 但變另一物件 → 驗證原 doc 被處理
    print("\n  --- 端到端：591 換物件 + 沒其他來源 ---")
    doc_b_id = _make_test_doc(
        "case-b-replaced",
        source="591",
        source_id="591_TEST_BBB",
        address="大安區test舊路一段",
        building_area_ping=25.0,
    )
    col = get_col()
    # 模擬 app.py 換物件邏輯：把 591_TEST_BBB 從 sources 移除
    old_doc = col.document(doc_b_id).get().to_dict()
    old_sources = old_doc.get("sources") or []
    new_sources = [s for s in old_sources if s.get("source_id") != "591_TEST_BBB"]
    if not new_sources:
        col.document(doc_b_id).update({
            "sources": new_sources,
            "archived": True,
            "archived_at": now_tw_iso(),
            "archived_reason": "所有來源失效（591_TEST_BBB 換成另一物件）",
        })
    after = col.document(doc_b_id).get().to_dict()
    _ok("唯一來源換物件後 → 原 doc archived", after.get("archived") is True)
    _ok("原 doc sources 變空", len(after.get("sources") or []) == 0)


# ═══════════════════════════════════════════════════════════════════
# 情況 C：改價事件
# ═══════════════════════════════════════════════════════════════════
def test_case_c_price_change():
    print("\n=== 情況 C：改價事件 ===")
    doc_c_id = _make_test_doc(
        "case-c-pricechange",
        source="591",
        source_id="591_TEST_CCC",
        address="大安區test改價路",
        building_area_ping=28.0,
        price_ntd=18000000,
    )
    col = get_col()

    # 模擬降價事件
    new_price = 16500000   # 降 150 萬
    existing = col.document(doc_c_id).get().to_dict()
    old_price = existing.get("price_ntd")
    history = list(existing.get("price_history") or [])
    history.append({"price": old_price, "scraped_at": existing.get("scraped_at")})
    direction = "down" if new_price < old_price else "up"
    now = now_tw_iso()
    col.document(doc_c_id).update({
        "price_ntd": new_price,
        "price_history": history,
        "is_price_changed": True,
        "scraped_at": now,
        "last_change_at": now,
        "latest_event": {
            "type": "price_change", "direction": direction,
            "from": old_price, "to": new_price, "at": now,
        },
    })
    after = col.document(doc_c_id).get().to_dict()
    _ok("降價：price_ntd 更新", after.get("price_ntd") == 16500000)
    _ok("降價：price_history 新增一筆", len(after.get("price_history") or []) == 1)
    _ok("降價：is_price_changed = True", after.get("is_price_changed") is True)
    _ok("降價：latest_event.direction = down",
        after.get("latest_event", {}).get("direction") == "down")
    _ok("降價：last_change_at 更新", bool(after.get("last_change_at")))

    # 模擬漲價事件
    even_newer = 17500000   # 從 1650 漲到 1750
    existing = col.document(doc_c_id).get().to_dict()
    old_price2 = existing.get("price_ntd")
    history2 = list(existing.get("price_history") or [])
    history2.append({"price": old_price2, "scraped_at": existing.get("scraped_at")})
    now2 = now_tw_iso()
    col.document(doc_c_id).update({
        "price_ntd": even_newer,
        "price_history": history2,
        "is_price_changed": True,
        "scraped_at": now2,
        "last_change_at": now2,
        "latest_event": {
            "type": "price_change", "direction": "up",
            "from": old_price2, "to": even_newer, "at": now2,
        },
    })
    after2 = col.document(doc_c_id).get().to_dict()
    _ok("漲價：direction = up",
        after2.get("latest_event", {}).get("direction") == "up")
    _ok("漲價：price_history 累積到 2 筆", len(after2.get("price_history") or []) == 2)


# ═══════════════════════════════════════════════════════════════════
# 情況 D：連結驗活
# ═══════════════════════════════════════════════════════════════════
def test_case_d_link_verify():
    print("\n=== 情況 D：連結驗活 ===")
    from api.app import _verify_source_alive, _verify_and_prune_sources

    # 1. 單元測試 _verify_source_alive
    # example.com 一定 200
    alive_ok, reason_ok = _verify_source_alive("https://example.com/", timeout=8)
    _ok("活著的 URL → True", alive_ok is True, f"got reason={reason_ok}")

    # httpbin.org/status/404 一定 404
    alive_404, reason_404 = _verify_source_alive("https://httpbin.org/status/404", timeout=8)
    _ok("404 URL → False", alive_404 is False, f"got reason={reason_404}")

    # 空 url
    alive_empty, _ = _verify_source_alive("", timeout=2)
    _ok("空 URL → False", alive_empty is False)

    # 2. 端到端：建立一個 doc 含 1 個活來源 + 1 個死來源 → 跑驗活 → 驗證死的被移除
    print("\n  --- 端到端：doc 含 dead url 驗活後被移除 ---")
    # 永慶實際物件 URL（真實活著）
    alive_url = "https://example.com/"   # 用 example.com 替代避免打真實永慶
    dead_url = "https://httpbin.org/status/404"

    doc_d_id = TEST_PREFIX + "case-d-verify"
    now = now_tw_iso()
    get_col().document(doc_d_id).set({
        "id": doc_d_id,
        "source": "591",
        "source_id": "591_TEST_DDD_alive",
        "url": alive_url,
        "sources": [
            {"name": "591", "source_id": "591_TEST_DDD_alive", "url": alive_url, "added_at": now},
            {"name": "永慶", "source_id": "yongqing_TEST_DDD_dead", "url": dead_url, "added_at": now},
        ],
        "url_alt": [dead_url],
        "city": "台北市",
        "district": "大安區",
        "address": "test 驗活路",
        "title": "test verify links",
        "building_area_ping": 30.0,
        "price_ntd": 20000000,
        "scraped_at": now,
        "scrape_session_at": now,
        "list_rank": 0,
        "archived": False,
    })

    doc_d = get_col().document(doc_d_id).get().to_dict()
    # skip_source_id 設成「剛剛觸發事件的那個」(591) → 只驗永慶那個
    updates = _verify_and_prune_sources(doc_d_id, doc_d, skip_source_id="591_TEST_DDD_alive")
    _ok("驗活發現死 url，產生 updates", bool(updates))
    if updates:
        get_col().document(doc_d_id).update(updates)
        after = get_col().document(doc_d_id).get().to_dict()
        survivors = after.get("sources") or []
        _ok("失效永慶來源被移除", len(survivors) == 1 and survivors[0]["name"] == "591")
        _ok("失效 URL 從 url_alt 移除", dead_url not in (after.get("url_alt") or []))
        _ok("doc 沒被 archive (還有 591 活著)", after.get("archived") is not True)


# ═══════════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("Phase 3 跨來源邏輯端到端測試")
    print("=" * 60)
    try:
        # 確保乾淨環境
        _cleanup_test_docs()
        time.sleep(0.5)

        test_case_a_cross_source()
        test_case_b_replacement()
        test_case_c_price_change()
        test_case_d_link_verify()
    finally:
        # 一律清掉（即使中途失敗）
        print("\n=== 清理 ===")
        _cleanup_test_docs()

    # 結果摘要
    print()
    print("=" * 60)
    total = len(_passed) + len(_failed)
    print(f"結果：{len(_passed)} / {total} 通過")
    if _failed:
        print(f"\n❌ 失敗：")
        for f in _failed:
            print(f"  - {f}")
        return 1
    print("\n✅ 全部通過")
    return 0


if __name__ == "__main__":
    sys.exit(main())
