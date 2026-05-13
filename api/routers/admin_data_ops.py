"""
Admin 批次操作 / reanalyze / dedupe endpoints — Sprint #2 step 5b 從 api/app.py 拆出。

7 個 endpoint：
  - POST /admin/properties/{id}/reanalyze       完整重爬 + 重新分析
  - POST /admin/manual/{uid}/{id}/reanalyze     重分析其他用戶的 manual 物件
  - GET  /admin/ocr_misread_scan                掃全庫 OCR 誤讀疑似
  - POST /admin/migrate_bookmarks_to_watchlist  一次性 bookmarks → watchlist 遷移
  - POST /admin/purge_non_apartments            清除中央 DB 所有非公寓物件
  - GET  /admin/dedupe_scan                     列出可合併的重複群組
  - POST /admin/dedupe_merge                    合併重複物件 (confirm=true)

搬走的 helpers：
  - _dedup_compute_groups (dedupe scan + merge 共用)
  - _doc_richness (dedupe scan + merge 共用)

搬走的 pydantic model：
  - DedupeMergeReq

Late imports (from api.app)：
  - _scrape_single_url (reanalyze 用)
  - _run_manual_analysis (manual reanalyze 用)
  - invalidate_query_cache (delete 後清 cache)
"""
import os
import re
import asyncio
import logging
import httpx

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from google.cloud.firestore_v1.base_query import FieldFilter

from api.auth import require_admin
from database.db import get_col, get_firestore, get_user_manual

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Dedupe 內部 helpers (搬自 api/app.py) ──────────────────────────────
def _dedup_compute_groups():
    """掃中央 DB，回傳 [{key, docs: [...full doc...]}] 有 >1 筆的重複群組。
    key = (district, road_short_without_prefix, bld_band_0_1, price_band_10000)"""
    def _key(addr, district, bld, price_ntd):
        a = addr or ""
        a = re.sub(r"^(台北市|臺北市|新北市|桃園市|台中市|臺中市|高雄市|台南市|臺南市|基隆市|新竹市)", "", a)
        a = re.sub(r"^[一-龥]{1,3}區", "", a)
        m = re.search(r"([一-龥]{1,5}(?:路|街|大道)(?:[一二三四五六七八九十]段)?(?:\d+巷)?)", a)
        road = m.group(1) if m else ""
        bld_band = round((bld or 0) * 10) / 10
        price_band = round((price_ntd or 0) / 10000)
        return (district or "", road, bld_band, price_band)

    groups = {}
    for d in get_col().get():
        data = d.to_dict() or {}
        # 要有最低資料才納入比對（否則空 key 全部會被視為同組）
        if not data.get("building_area_ping") or not data.get("price_ntd"):
            continue
        k = _key(data.get("address"), data.get("district"),
                 data.get("building_area_ping"), data.get("price_ntd"))
        if not k[1]:  # 沒抓到 road 就不比（避免把不同建物誤合）
            continue
        data["_id"] = d.id
        groups.setdefault(k, []).append(data)
    # 只留 >1 筆
    return [{"key": list(k), "docs": v} for k, v in groups.items() if len(v) > 1]


def _doc_richness(d: dict) -> int:
    """算 doc 豐富度：已填關鍵欄位數。用來挑 keeper。"""
    keys = ("address", "address_inferred", "latitude", "longitude",
            "land_area_ping", "building_age", "floor", "total_floors",
            "zoning", "road_width_m", "ai_analysis", "nearest_mrt",
            "screenshot_roadwidth")
    return sum(1 for k in keys if d.get(k))


# ── Pydantic ────────────────────────────────────────────────────────────
class DedupeMergeReq(BaseModel):
    confirm: bool = False   # 必須 True 才真的動


# ── Endpoints ───────────────────────────────────────────────────────────
@router.post("/admin/properties/{property_id:path}/reanalyze")
async def admin_reanalyze(property_id: str, source: str = "all", admin: dict = Depends(require_admin)):
    """完整重爬 + 重新分析。
    source 參數：
      - "all"（預設）：重抓該物件所有來源（依 sources array 順序）
      - "591" / "永慶" / "信義"：只重抓指定來源"""
    from api.routers.admin_scrape import _scrape_single_url
    from api.app import invalidate_query_cache
    col = get_col()
    doc = col.document(property_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="物件不存在")
    p = doc.to_dict() or {}

    # 收集要重抓的來源清單
    sources_arr = p.get("sources") or []
    if not sources_arr and p.get("url"):
        # 舊 schema fallback
        sources_arr = [{
            "name": p.get("source") or "591",
            "source_id": p.get("source_id") or property_id,
            "url": p.get("url"),
        }]
    if source != "all":
        sources_arr = [s for s in sources_arr if s.get("name") == source]
    if not sources_arr:
        raise HTTPException(status_code=400, detail=f"找不到來源 {source} 的可用 URL")

    col.document(property_id).update({"analysis_in_progress": True})

    async def _do():
        try:
            for s in sources_arr:
                _src_id = s.get("source_id") or property_id
                _url = s.get("url")
                if not _url:
                    continue
                logger.warning("[admin] %s 重抓 %s (source=%s)", admin.get("email"), _src_id, s.get("name"))
                await asyncio.to_thread(_scrape_single_url, _url, _src_id, True)
            logger.warning("[admin] %s 完成重新分析 %s (sources=%s)", admin.get("email"), property_id, [s.get("name") for s in sources_arr])
        except Exception as e:
            logger.exception(f"[admin] 重新分析失敗 {property_id}: {e}")
        finally:
            # 無論成功失敗，一律清掉 analysis_in_progress（_scrape_single_url 的 early return 不會清）
            try:
                col.document(property_id).update({"analysis_in_progress": False})
            except Exception:
                pass
            # invalidate central_search query cache — 讓下次前端 ctrl+f5 list 拿到 fresh doc
            invalidate_query_cache()
    asyncio.create_task(_do())
    logger.warning("[admin] %s 觸發重新分析（完整重爬） %s", admin.get("email"), property_id)
    return {"status": "started"}


@router.get("/admin/ocr_misread_scan")
async def admin_ocr_misread_scan(admin: dict = Depends(require_admin)):
    """掃全庫，比對 DB address 的路名 vs 591 原生座標反查的路名。
    若兩者差距大 → 疑似 OCR 誤讀或 Claude 誤糾正，回傳清單讓 admin 人工重新分析。"""
    key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not key:
        raise HTTPException(400, "GOOGLE_MAPS_API_KEY 未設定")

    def extract_road(s: str):
        if not s: return None
        t = re.sub(r"^\d{3,6}", "", s)
        t = re.sub(r"^(台灣|臺灣)", "", t)
        t = re.sub(r"^(台北市|臺北市|新北市|桃園市|基隆市)", "", t)
        t = re.sub(r"^[一-龥]{1,3}區", "", t)
        m = re.search(r"^([一-龥]{1,5}(?:路|街|大道)(?:[一二三四五六七八九十]段)?)", t)
        return m.group(1) if m else None

    suspects = []
    checked = 0
    skipped_no_src = 0
    for d in get_col().where(filter=FieldFilter("city", "==", "台北市")).get():
        data = d.to_dict()
        slat, slng = data.get("source_latitude"), data.get("source_longitude")
        if not slat or not slng:
            skipped_no_src += 1
            continue
        db_addr = data.get("address_inferred") or data.get("address") or ""
        db_road = extract_road(db_addr)
        if not db_road:
            continue
        try:
            async with httpx.AsyncClient(timeout=10) as cli:
                r = await cli.get(
                    "https://maps.googleapis.com/maps/api/geocode/json",
                    params={"latlng": f"{slat},{slng}", "key": key, "language": "zh-TW"},
                )
            results = (r.json() or {}).get("results") or []
            if not results: continue
            fa = results[0].get("formatted_address", "")
            rev_road = extract_road(fa)
            checked += 1
            if not rev_road or db_road == rev_road:
                continue
            base = re.sub(r"[一二三四五六七八九十]段$", "", db_road)
            rev_base = re.sub(r"[一二三四五六七八九十]段$", "", rev_road)
            if base == rev_base:
                continue   # 段延伸，視為相同
            suspects.append({
                "id": d.id,
                "title": data.get("title"),
                "district": data.get("district"),
                "db_address": db_addr,
                "db_road": db_road,
                "source_reverse": fa,
                "source_reverse_road": rev_road,
                "submitted_by_email": data.get("submitted_by_email"),
                "source_origin": data.get("source_origin"),
            })
        except Exception as e:
            logger.warning(f"OCR scan {d.id} failed: {e}")
    return {
        "checked": checked,
        "skipped_no_source_coords": skipped_no_src,
        "suspects": suspects,
        "note": (
            f"已用 591 原生座標反查對照 DB 路名。 "
            f"{skipped_no_src} 筆舊物件沒有原生座標（在加入此欄位前分析），下次重新分析才會有。"
        ),
    }


@router.post("/admin/manual/{uid}/{property_id:path}/reanalyze")
async def admin_reanalyze_manual(uid: str, property_id: str, admin: dict = Depends(require_admin)):
    """admin 重分析其他用戶的 manual 物件（/admin/manual_properties tab 用）。
    跟用戶端 reanalyze 走同一條 validate；若觸發歧義 admin 端會看到 status!=started，
    需要請該用戶自己處理（admin 不該替別人選戶）。"""
    from api.routers.admin_scrape import _run_manual_analysis
    if not property_id.startswith("manual_"):
        raise HTTPException(status_code=400, detail="只能重分析 manual 物件")
    manual_col = get_user_manual(uid)
    doc = manual_col.document(property_id).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail=f"uid={uid} 沒有此 manual 物件")
    old = doc.to_dict() or {}

    # 同一條 validate
    from api.manual_analyze import validate_manual_input
    price_wan = (old.get("price_ntd") / 10000) if old.get("price_ntd") else None
    v = validate_manual_input(
        city=old.get("city"),
        district=old.get("district"),
        address=old.get("address"),
        building_area_ping=old.get("building_area_ping"),
        land_area_ping=old.get("land_area_ping"),
        price_wan=price_wan,
        use_source="auto",
    )
    if v["status"] != "ok":
        out = dict(v)
        out["mode"] = "admin_reanalyze"
        out["property_id"] = property_id
        out["uid"] = uid
        return out

    item = dict(v["item"])
    item["source_id"] = property_id
    item.setdefault("source", "manual")
    item.setdefault("title", old.get("title") or item.get("address"))
    for _k in ("building_age", "building_type", "total_floors", "floor"):
        if not item.get(_k) and old.get(_k):
            item[_k] = old[_k]

    manual_col.document(property_id).update({"analysis_in_progress": True})
    asyncio.create_task(_run_manual_analysis(uid, property_id, item))
    logger.warning("[admin] %s 觸發 manual 重分析 uid=%s src_id=%s (after validate)", admin.get("email"), uid, property_id)
    return {"status": "started", "source_id": property_id}


@router.post("/admin/migrate_bookmarks_to_watchlist")
async def admin_migrate_bookmarks(admin: dict = Depends(require_admin)):
    """
    一次性搬遷：所有 users/{uid}/bookmarks/* 搬到 users/{uid}/watchlist/*。
    bookmarks 概念合併進 watchlist 後留下的舊資料補救。
    """
    fs = get_firestore()
    users_col = fs.collection("users")
    summary = []
    for u in users_col.get():
        uid = u.id
        b_col = fs.collection("users").document(uid).collection("bookmarks")
        w_col = fs.collection("users").document(uid).collection("watchlist")
        moved = 0
        for bdoc in b_col.get():
            data = bdoc.to_dict() or {}
            # bookmarked_at → added_at（語意對應）
            if "bookmarked_at" in data and "added_at" not in data:
                data["added_at"] = data.pop("bookmarked_at")
            w_col.document(bdoc.id).set(data, merge=True)
            bdoc.reference.delete()
            moved += 1
        if moved:
            summary.append({"uid": uid, "moved": moved})
    logger.warning("[admin] %s 搬遷 bookmarks→watchlist: %s", admin.get("email"), summary)
    return {"status": "ok", "summary": summary, "total_moved": sum(s["moved"] for s in summary)}


@router.post("/admin/purge_non_apartments")
async def admin_purge_non_apartments(admin: dict = Depends(require_admin)):
    """一鍵清除中央 DB 所有非公寓物件（大樓/透天/店面/華廈/辦公/11F+）。"""
    from api.app import invalidate_query_cache
    col = get_col()
    forbidden = ["大樓", "透天", "店面", "店舖", "華廈", "辦公"]
    deleted = []
    for d in col.get():
        data = d.to_dict() or {}
        bt = (data.get("building_type") or "").strip()
        tf = data.get("total_floors") or 0
        try: tf = int(tf)
        except Exception: tf = 0
        hit_reason = None
        if any(f in bt for f in forbidden):
            hit_reason = f"類型={bt}"
        elif bt and "公寓" not in bt:
            hit_reason = f"類型={bt}"
        elif tf >= 11:
            hit_reason = f"{tf}F"
        if hit_reason:
            d.reference.delete()
            deleted.append({"id": d.id, "reason": hit_reason,
                            "address": data.get("address"), "building_type": bt})
    if deleted:
        invalidate_query_cache()
    logger.warning("[admin] %s 清除非公寓 %d 筆", admin.get("email"), len(deleted))
    return {"status": "ok", "deleted_count": len(deleted), "deleted": deleted[:100]}


@router.get("/admin/dedupe_scan")
async def admin_dedupe_scan(admin: dict = Depends(require_admin)):
    """(只掃不動資料) 列出可合併的重複群組。回傳：
    [{"key": [district, road, bld, price], "docs": [{id, source_id, address, url, richness, ...}]}]
    """
    groups = _dedup_compute_groups()
    out = []
    for g in groups:
        docs = sorted(g["docs"], key=_doc_richness, reverse=True)
        from database.models import primary_url, primary_source_id
        out.append({
            "key": g["key"],
            "count": len(docs),
            "keeper_id": docs[0]["_id"],
            "docs": [
                {
                    "id": d["_id"],
                    "source_id": primary_source_id(d),
                    "url": primary_url(d),
                    "address": d.get("address"),
                    "address_inferred": d.get("address_inferred"),
                    "title": (d.get("title") or "")[:50],
                    "building_area_ping": d.get("building_area_ping"),
                    "price_ntd": d.get("price_ntd"),
                    "richness": _doc_richness(d),
                    "scrape_session_at": d.get("scrape_session_at"),
                }
                for d in docs
            ],
        })
    return {"groups": out, "total_groups": len(out),
            "total_duplicates_to_remove": sum(len(g["docs"]) - 1 for g in groups)}


@router.post("/admin/dedupe_merge")
async def admin_dedupe_merge(body: DedupeMergeReq, admin: dict = Depends(require_admin)):
    """
    把每組重複物件合併到「最豐富的 keeper」：
      - keeper 保留
      - 其他 doc 的 sources[] 累進 keeper 的 sources[]
      - 其他 doc 的欄位若 keeper 還沒填 → 補到 keeper（地址/坪數等）
      - 其他 doc 刪除
    必須帶 {"confirm": true} 才真的動。
    """
    if not body.confirm:
        return {"status": "noop", "message": "confirm=false，未執行合併。請用 /admin/dedupe_scan 檢視後再送 confirm=true。"}

    from database.models import add_source_to_doc, compute_source_keys
    col = get_col()
    groups = _dedup_compute_groups()
    merged_count = 0
    deleted_ids = []
    for g in groups:
        docs = sorted(g["docs"], key=_doc_richness, reverse=True)
        keeper = docs[0]
        keeper_id = keeper["_id"]
        # 把其他 docs 的 sources[] 累進 keeper.sources[]
        keeper_clone = {"sources": list(keeper.get("sources") or [])}
        published_at_alt = list(keeper.get("published_at_alt") or [])
        keeper_updates = {}
        sources_changed = False
        fill_fields = ("address", "address_inferred", "address_inferred_confidence",
                       "address_inferred_candidates", "address_inferred_candidates_detail",
                       "latitude", "longitude", "land_area_ping", "land_area_source",
                       "land_area_sqm", "building_age", "floor", "total_floors",
                       "zoning", "zoning_original", "zoning_source", "road_width_m",
                       "road_width_name", "road_width_vision_reason",
                       "screenshot_roadwidth", "nearest_mrt", "nearest_mrt_dist_m",
                       "nearest_mrt_exit", "nearby_mrts", "ai_analysis", "ai_recommendation",
                       "ai_reason", "image_url")
        for other in docs[1:]:
            # 累進 sources[]
            for s in (other.get("sources") or []):
                if add_source_to_doc(keeper_clone, s.get("name") or "591", s.get("source_id") or "",
                                     s.get("url"), s.get("added_at")):
                    sources_changed = True
            for p in (other.get("published_at_alt") or []):
                if p and p not in published_at_alt:
                    published_at_alt.append(p)
            if other.get("published_at") and other["published_at"] not in published_at_alt:
                published_at_alt.append(other["published_at"])
            # 補 keeper 缺的欄位
            for f in fill_fields:
                if (keeper.get(f) in (None, "", [], {}) and keeper_updates.get(f) in (None, "", [], {})
                        and other.get(f) not in (None, "", [], {})):
                    keeper_updates[f] = other[f]
        if sources_changed:
            keeper_updates["sources"] = keeper_clone["sources"]
            keeper_updates["source_keys"] = compute_source_keys(keeper_clone["sources"])
        if published_at_alt != (keeper.get("published_at_alt") or []):
            keeper_updates["published_at_alt"] = published_at_alt
        if keeper_updates:
            try:
                col.document(keeper_id).update(keeper_updates)
            except Exception as e:
                logger.warning(f"dedupe update keeper 失敗 {keeper_id}: {e}")
                continue
        # 刪其他
        for other in docs[1:]:
            try:
                col.document(other["_id"]).delete()
                deleted_ids.append(other["_id"])
            except Exception as e:
                logger.warning(f"dedupe delete 失敗 {other['_id']}: {e}")
        merged_count += 1

    logger.warning("[admin] %s dedupe_merge：合併 %d 組，刪除 %d 筆",
                   admin.get("email"), merged_count, len(deleted_ids))
    return {"status": "ok", "merged_groups": merged_count,
            "deleted_count": len(deleted_ids), "deleted_ids": deleted_ids[:50]}
