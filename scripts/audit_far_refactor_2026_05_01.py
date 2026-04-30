"""Audit FAR refactor regression：對新北 5 區（板橋/新莊/中和/永和/新店）所有 doc 算
舊邏輯 effFar vs 新邏輯 effFar，列出差異。

舊邏輯：flat TAIPEI_FAR_PCT lookup（前端原本表，新北只有住宅區300/商業區440/商業區(板橋)460）
新邏輯：lookup_far(zoning, district, lat, lng) per-district + 浮洲 polygon

執行：python scripts/audit_far_refactor_2026_05_01.py
"""
from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# 舊 frontend TAIPEI_FAR_PCT (refactor 前的內容；用來模擬「舊邏輯」)
OLD_FAR_PCT = {
    "第一種住宅區": 60, "第二種住宅區": 120, "第三種住宅區": 225, "第三種住宅區(特)": 225,
    "第三之一種住宅區": 300, "第三之二種住宅區": 400, "第四之一種住宅區": 400,
    "第四種住宅區": 300, "住宅用地": 200,
    "第一種商業區": 360, "第二種商業區": 630, "第三種商業區": 560, "第三種商業區(特)": 560,
    "第四種商業區": 800,
    # 舊邏輯把新北也 squash 進台北 table（同 key 共用）
    "住宅區": 300, "商業區": 440, "商業區(板橋)": 460,
}


def old_eff_far(zoning_str_or_list, ratios=None):
    """舊邏輯：flat lookup + 多分區加權平均（同 frontend effectiveFarPctWeighted 舊行為）"""
    # 多分區
    if isinstance(zoning_str_or_list, list) and len(zoning_str_or_list) > 1:
        zl = zoning_str_or_list
        ratios = ratios or [100 / len(zl)] * len(zl)
        total = sum(ratios) or 1
        weighted = 0
        for i, z in enumerate(zl):
            zname = z if isinstance(z, str) else (z.get("original_zone") or z.get("zone_name"))
            far = OLD_FAR_PCT.get(zname)
            if far is None:
                # 舊邏輯多分區任一 None 就整個 None（這就是 multi-zone bug）
                # 但如果是 dict 形式 (.original_zone undefined) 會永遠 None
                # 為了公平比對，這裡只看 string list
                return None
            weighted += far * (ratios[i] / total)
        return round(weighted)
    # 單分區
    z = zoning_str_or_list if isinstance(zoning_str_or_list, str) else (
        zoning_str_or_list[0] if zoning_str_or_list else None
    )
    return OLD_FAR_PCT.get(z) if z else None


def new_eff_far(zoning_str_or_list, district, lat, lng, ratios=None, is_remote_area=False):
    """新邏輯：lookup_far per-district + 浮洲 polygon"""
    from config import lookup_far

    def lookup(zoning):
        # 模擬 frontend lookupFar(zoning, p) — p.is_remote_area 由 doc 旗標提供
        if is_remote_area and district == "板橋區":
            from config import NEW_TAIPEI_FAR_PCT
            return NEW_TAIPEI_FAR_PCT["_banqiao_fujou"].get(zoning)
        return lookup_far(zoning, district, lat, lng)

    if isinstance(zoning_str_or_list, list) and len(zoning_str_or_list) > 1:
        zl = zoning_str_or_list
        ratios = ratios or [100 / len(zl)] * len(zl)
        total = sum(ratios) or 1
        weighted = 0
        for i, z in enumerate(zl):
            zname = z if isinstance(z, str) else (z.get("original_zone") or z.get("zone_name"))
            far = lookup(zname)
            if far is None:
                return None
            weighted += far * (ratios[i] / total)
        return round(weighted)
    z = zoning_str_or_list if isinstance(zoning_str_or_list, str) else (
        zoning_str_or_list[0] if zoning_str_or_list else None
    )
    return lookup(z) if z else None


def main():
    from database.db import get_firestore
    db = get_firestore()
    col = db.collection("properties")

    target_districts = {"板橋區", "新莊區", "中和區", "永和區", "新店區"}
    rows = []
    for d in col.stream():
        dd = d.to_dict() or {}
        if dd.get("city") != "新北市":
            continue
        district = dd.get("district")
        if district not in target_districts:
            continue
        zoning = dd.get("zoning")
        zoning_list = dd.get("zoning_list")
        ratios = dd.get("zoning_ratios")
        lat = dd.get("latitude") or dd.get("source_latitude")
        lng = dd.get("longitude") or dd.get("source_longitude")
        is_remote = bool(dd.get("is_remote_area"))

        # 多分區優先用 zoning_list；單分區用 zoning
        z_input = zoning_list if (zoning_list and len(zoning_list) > 1) else zoning

        old_far = old_eff_far(z_input, ratios)
        new_far = new_eff_far(z_input, district, lat, lng, ratios, is_remote)

        rows.append({
            "doc_id": d.id,
            "district": district,
            "zoning": zoning,
            "zoning_list": zoning_list,
            "is_remote": is_remote,
            "old": old_far,
            "new": new_far,
            "delta": (new_far - old_far) if (old_far is not None and new_far is not None) else None,
            "address": (dd.get("address_inferred") or dd.get("address") or "")[:35],
        })

    # 列印 by district
    for dist in sorted(target_districts):
        sub = [r for r in rows if r["district"] == dist]
        if not sub:
            continue
        print(f"\n{'='*100}")
        print(f"=== {dist}（{len(sub)} 筆）===")
        print(f"{'='*100}")
        # 區分：「有變化」「無變化」「新邏輯改 None」「舊邏輯就 None」
        for r in sorted(sub, key=lambda x: (x["zoning"] or "", x["doc_id"])):
            old_s = f"{r['old']:>4}" if r['old'] is not None else "  --"
            new_s = f"{r['new']:>4}" if r['new'] is not None else "  --"
            delta_s = ""
            if r['delta'] is not None and r['delta'] != 0:
                sign = "+" if r['delta'] > 0 else ""
                delta_s = f"  Δ{sign}{r['delta']}"
            elif r['old'] is None and r['new'] is not None:
                delta_s = "  (舊→無, 新→有)"
            elif r['old'] is not None and r['new'] is None:
                delta_s = "  (舊→有, 新→無) ⚠"
            elif r['delta'] == 0:
                delta_s = "  (持平)"
            zlist = ""
            if r['zoning_list'] and len(r['zoning_list']) > 1:
                zlist = f" [multi: {','.join(str(z) for z in r['zoning_list'])}]"
            remote_s = " 🏔" if r['is_remote'] else ""
            print(f"  {r['doc_id']} | {r['zoning'] or '?':10}{zlist:30} | old={old_s} new={new_s}{delta_s}{remote_s} | {r['address']}")

    # 總結
    print(f"\n\n{'='*100}")
    print("總結")
    print(f"{'='*100}")
    total = len(rows)
    same = sum(1 for r in rows if r['delta'] == 0)
    changed = sum(1 for r in rows if r['delta'] not in (0, None))
    new_resolved = sum(1 for r in rows if r['old'] is None and r['new'] is not None)
    new_lost = sum(1 for r in rows if r['old'] is not None and r['new'] is None)
    both_none = sum(1 for r in rows if r['old'] is None and r['new'] is None)
    print(f"  總 doc 數：{total}")
    print(f"  effFar 持平：{same}")
    print(f"  effFar 變化：{changed}")
    print(f"  舊→None / 新→有值 (修好)：{new_resolved}")
    print(f"  舊→有值 / 新→None (regression!)：{new_lost}")
    print(f"  雙邊 None：{both_none}")

    if new_lost:
        print("\n⚠ 有 regression（舊邏輯能算、新邏輯算不出）：")
        for r in rows:
            if r['old'] is not None and r['new'] is None:
                print(f"  {r['doc_id']} | {r['district']} | zoning={r['zoning']} | zoning_list={r['zoning_list']}")


if __name__ == "__main__":
    main()
