"""對所有 591 物件 confidence=geocode_reverse_loose 的 doc，
用新邏輯 (source_lat reverse) 算出新 address_inferred，比對舊值看差距。

執行：
  python scripts/backfill_591_reverse_loose.py            # dry-run
  python scripts/backfill_591_reverse_loose.py --apply    # 寫回 DB
"""
import sys, argparse
from math import radians, sin, cos, sqrt, atan2
sys.path.insert(0, r'd:\Coding\urban-renewal')
from database.db import init_db, get_col
from database.models import primary_source_id


def haversine(a, b):
    R = 6371000
    dlat = radians(b[0]-a[0]); dlng = radians(b[1]-a[1])
    h = sin(dlat/2)**2 + cos(radians(a[0]))*cos(radians(b[0]))*sin(dlng/2)**2
    return 2*R*atan2(sqrt(h), sqrt(1-h))


def is_591_doc(d):
    """doc 主來源是 591 嗎？"""
    sources = d.get("sources") or []
    if not sources:
        return False
    return (sources[0].get("name") or "").strip() == "591"


def extract_road_seg(addr):
    import re
    if not addr: return ""
    a = re.sub(r"^(台北市|臺北市|新北市)", "", addr)
    a = re.sub(r"^[一-龥]{1,3}區", "", a)
    m = re.search(r"([一-龥]{1,5}(?:路|街|大道)(?:[一二三四五六七八九十]段)?(?:\d+巷)?)", a)
    return m.group(1) if m else ""


def main(apply: bool):
    from analysis.lvr_index import _reverse_geocode_loose
    from analysis.geocoder import geocode_address
    from database.models import strip_region_prefix
    from analysis.claude_analyzer import _clean_address_garbage

    init_db()
    col = get_col()
    docs = list(col.stream())
    print(f"total docs: {len(docs)}")

    candidates = []
    for d in docs:
        x = d.to_dict() or {}
        if not is_591_doc(x):
            continue
        if x.get("address_inferred_confidence") != "geocode_reverse_loose":
            continue
        if not (x.get("source_latitude") and x.get("source_longitude")):
            continue
        candidates.append((d.id, x))

    print(f"\n=== candidates (591 + confidence=geocode_reverse_loose + 有 source 座標): {len(candidates)} ===")

    plan = []
    for did, x in candidates:
        old_inferred = x.get("address_inferred") or ""
        addr = x.get("address") or ""
        road_seg = extract_road_seg(addr) or extract_road_seg(old_inferred)
        if not road_seg:
            continue
        src_lat = x.get("source_latitude")
        src_lng = x.get("source_longitude")
        try:
            new_rev = _reverse_geocode_loose(src_lat, src_lng, road_seg)
        except Exception as e:
            new_rev = f"ERR:{e}"
            continue
        if not new_rev:
            continue
        cleaned = _clean_address_garbage(new_rev)
        new_inferred = strip_region_prefix(cleaned, x.get("city") or "", x.get("district") or "")

        # 也算地理距離（geocode 新 vs 舊地址）— 評估改動幅度
        old_geo = geocode_address(f"{x.get('city')}{x.get('district')}{old_inferred}") if old_inferred else None
        new_geo = geocode_address(f"{x.get('city')}{x.get('district')}{new_inferred}")
        dist_m = None
        if old_geo and new_geo:
            dist_m = round(haversine(old_geo, new_geo))

        plan.append({
            "doc_id": did,
            "old": old_inferred,
            "new": new_inferred,
            "src_coord": (src_lat, src_lng),
            "dist_m": dist_m,
            "road_seg": road_seg,
            "address_orig": addr,
        })

    print(f"\n=== 比對結果 (按 dist 排序，相同/null 排前) ===")
    plan.sort(key=lambda p: -(p["dist_m"] or 0))
    same = [p for p in plan if p["old"] == p["new"]]
    diff = [p for p in plan if p["old"] != p["new"]]
    print(f"  舊 == 新 (沒變): {len(same)} 筆")
    print(f"  舊 != 新 (有更動): {len(diff)} 筆")

    print(f"\n=== 有更動的 {len(diff)} 筆 (按距離降序) ===")
    for p in diff:
        d_str = f"{p['dist_m']}m" if p['dist_m'] is not None else "?"
        print(f"  {p['doc_id']}  距 {d_str:6}  {p['old']:30} → {p['new']}")

    if not apply:
        print(f"\n[dry-run] 加 --apply 才會真寫")
        return

    print(f"\n=== APPLYING ===")
    n = 0
    for p in diff:
        col.document(p["doc_id"]).update({
            "address_inferred": p["new"],
        })
        n += 1
    print(f"done: updated {n} docs")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    main(apply=args.apply)
