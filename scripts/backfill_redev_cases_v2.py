"""Backfill `redev_cases` v2 — 區級 polygon 預抓 + shapely 本地比對。

跟 v1 (per-property × 4 ByXY × ~1s) 比，v2 跑 1187 雙北物件 ~30 秒。

設計：
  1. 台北 8 sub_types：走 GeoServer WFS GetFeature (typeNames=Taipei:uro-redevelop-ALL-5
     + 115/63yAgoBud)，一次抓全市 polygon (~5000 個 feature)
  2. 新北 4 sub_types × 5 區 (板橋/新莊/新店/中和/永和)：走 GetXxxCasebyDist 拿 polygon
  3. 全部 polygon parse 成 Shapely Polygon (TWD97 EPSG:3826)
  4. 用 STRtree 加速空間索引
  5. 對每筆物件：lat/lng → TWD97 → STRtree query → 命中的 polygon 對應 sub_type 寫進 redev_cases

DB schema:
  redev_cases: [{sub_type, sub_type_label, case_id}]   # 無 summary / applicant
  summary 由前端 detail page「了解細節」按鈕即時 fetch (內容會變不該存 DB stale)

Usage:
  python scripts/backfill_redev_cases_v2.py            # full
  python scripts/backfill_redev_cases_v2.py --dry-run
"""
import argparse
import sys
import time
import re
import base64
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding='utf-8')

import httpx
from shapely.geometry import Polygon, MultiPolygon, Point
from shapely.strtree import STRtree

from database.db import get_firestore
from analysis.gov_gis import wgs84_to_twd97
import api.gis_overlay as g


# 5 個 NTPC sub_type 對應 ArcGIS REST layer (NtpcURInfo MapServer 子 layer ID + case_id 欄位)
# 比 CasebyDist 完整：CasebyDist 對跨區大 case (例如「捷運中和線沿線」case 90) 不會回 polygon，
# ArcGIS REST /query 拿到的才是 map tile 真正用的所有 polygon (跟視覺一致)。
NTPC_LAYER_META = [
    # (sub_type, ntpc_layer_name, label, arcgis_id_field)
    ("ama",     "都市更新事業計畫案", "都市更新事業計畫案", "UN01"),
    ("easy",    "簡易都更",           "簡易都更",           "EasyUrbanMainID"),
    ("danger",  "危老重建",           "危老重建",           "DangerReconstructionMainID"),
    ("amdm",    "防災案件",           "防災案件",           "SGAID"),
    ("rzoning", "劃定更新地區",       "劃定更新地區",       "RZoningMainID"),
]


# 雙北我們關注的區 (歷史保留 — district pre-fetch 已不用，但 NTPC_QUERY_TYPES 留著給萬一回退用)
TPE_DISTRICTS = ['中正區','大同區','中山區','松山區','大安區','萬華區','信義區','內湖區','南港區','文山區']
NTPC_DISTRICTS = ['板橋區','新店區','中和區','永和區']

# 台北 sub_type → WFS layer 對應 (uro-redevelop-ALL-5 layer 屬性 / 獨立 typeName)
TPE_REDEV_LAYER_MAP = {
    "10": ("pub_renew",     "公劃更新地區"),
    "12": ("pub_business",  "公劃內事業"),
    "20": ("self_announce", "公告自劃"),
    "30": ("self_approved", "核准自劃"),
    "40": ("planned",       "都計劃定"),
    "44": ("chloride",      "高氯離子混凝土"),
    "48": ("urgent",        "迅行劃定"),
    "50": ("invalid",       "已失效或廢止"),
}

# 新北 sub_type → CasebyDist keyword 對應 (對齊 map tile 底層 ArcGIS table)
NTPC_QUERY_TYPES = [
    # (sub_type, by_dist_kw, label, id_field)
    ("ama",     "GetUnitsCasebyDist",        "都市更新事業計畫案", "UN01"),
    ("easy",    "GetEasyUrbanCasebyDist",    "簡易都更",           "ID"),
    ("danger",  "GetDangerCasebyDist",       "危老重建",           "ID"),
    ("amdm",    "GetSGACasebyDist",          "防災案件",           "ID"),
    ("rzoning", "GetRZoningUAreaCasebyDist", "劃定更新地區",       "ID"),
]


def _parse_wkt_polygon(wkt: str):
    """簡易 WKT POLYGON 解析。回 shapely Polygon 或 None。"""
    if not wkt:
        return None
    try:
        from shapely import wkt as _wkt
        return _wkt.loads(wkt)
    except Exception:
        return None


def fetch_tpe_polygons(t0):
    """從 GeoServer WFS 一次拿台北全部都更 polygon。回 [(polygon, sub_type, sub_label, case_id)] list。"""
    polys = []
    queries = [
        ("Taipei:uro-redevelop-ALL-5", None),
        ("Taipei:115PublicPlanREArea-5", "115_revised"),
        ("Taipei:63yAgoBud", "63y_building"),
    ]
    for type_name, fixed_sub in queries:
        url = "https://zonegeo.udd.gov.taipei/geoserver/Taipei/wfs"
        try:
            r = httpx.get(url, params={
                "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                "typeNames": type_name, "outputFormat": "application/json",
                "count": 10000,
            }, timeout=60, verify=False)
            j = r.json()
        except Exception as e:
            print(f"  [TPE WFS] {type_name} 失敗: {e}", flush=True)
            continue
        feats = j.get("features", [])
        n_added = 0
        for feat in feats:
            geom = feat.get("geometry", {})
            props = feat.get("properties", {}) or {}
            coords = geom.get("coordinates")
            gtype = geom.get("type")
            if not coords:
                continue
            try:
                if gtype == "Polygon":
                    poly = Polygon(coords[0], coords[1:])
                elif gtype == "MultiPolygon":
                    parts = [Polygon(c[0], c[1:]) for c in coords]
                    poly = MultiPolygon(parts)
                else:
                    continue
            except Exception:
                continue
            if fixed_sub:
                sub_id = fixed_sub
                sub_label = "115年修訂公劃" if sub_id == "115_revised" else "63年以前建築物"
            else:
                layer_id = str(props.get("layer") or "")
                m = TPE_REDEV_LAYER_MAP.get(layer_id)
                if not m:
                    continue
                sub_id, sub_label = m
            case_id = props.get("ID") or props.get("NO") or ""
            polys.append((poly, sub_id, sub_label, str(case_id)))
            n_added += 1
        print(f"  TPE WFS {type_name}: {n_added} polygons ({time.time()-t0:.1f}s)", flush=True)
    return polys


def fetch_ntpc_polygons(t0):
    """新北：直接打 ArcGIS REST /query 拿每個 layer 全部 polygon (跟 map tile 同源)。
    跟 CasebyDist 比：CasebyDist 對「跨區大型 case」(如捷運中和線沿線 ID=90) 不回 polygon，
    這條 path 才能拿到 map tile 看得到的全部範圍。每 layer 1 call (paginate) ~0.5-1s。"""
    polys = []
    for sub_id, layer_name, label, id_field in NTPC_LAYER_META:
        meta = g._get_ntpcurinfo_layer_meta(layer_name)
        if not meta or not meta.get("agstoken"):
            print(f"  [NTPC] {layer_name} meta 抓不到，skip", flush=True)
            continue
        url = meta["mapsrvurl"] + "/" + meta["layerids"] + "/query"
        offset = 0
        page = 2000
        n = 0
        while True:
            try:
                r = httpx.get(url, params={
                    "token": meta["agstoken"],
                    "where": "1=1", "outFields": "*",
                    "returnGeometry": "true", "outSR": "3826",
                    "f": "json",
                    "resultOffset": str(offset), "resultRecordCount": str(page),
                }, timeout=30, verify=False)
                j = r.json()
            except Exception as e:
                print(f"  [NTPC] {layer_name} fetch 失敗: {e}", flush=True)
                break
            feats = j.get("features", [])
            if not feats:
                break
            for f in feats:
                attrs = f.get("attributes", {}) or {}
                geom = f.get("geometry", {}) or {}
                rings = geom.get("rings") or []
                if not rings:
                    continue
                try:
                    if len(rings) == 1:
                        poly = Polygon(rings[0])
                    else:
                        valid_rings = [Polygon(rg) for rg in rings if len(rg) >= 4]
                        if not valid_rings:
                            continue
                        poly = MultiPolygon(valid_rings) if len(valid_rings) > 1 else valid_rings[0]
                except Exception:
                    continue
                cid = attrs.get(id_field) or attrs.get("ID") or attrs.get("SID") or ""
                polys.append((poly, sub_id, label, str(cid)))
                n += 1
            if not j.get("exceededTransferLimit"):
                break
            offset += page
        print(f"  NTPC {sub_id} ({layer_name}): {n} polygons ({time.time()-t0:.1f}s)", flush=True)
    return polys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--cities", default="台北市,新北市")
    args = ap.parse_args()

    t0 = time.time()
    cities = set([c.strip() for c in args.cities.split(",") if c.strip()])

    # Step 1: 預抓所有 polygon
    print(f"=== Step 1: 預抓 polygon ({list(cities)}) ===", flush=True)
    polys = []
    if "台北市" in cities:
        polys += fetch_tpe_polygons(t0)
    if "新北市" in cities:
        polys += fetch_ntpc_polygons(t0)
    print(f"\n總共 {len(polys)} 個 polygon ({time.time()-t0:.1f}s)\n", flush=True)

    # Step 2: 建 STRtree (shapely 2.x: STRtree 接 list of geometries，query() 回 index)
    print("=== Step 2: 建 STRtree 空間索引 ===", flush=True)
    geoms = [p[0] for p in polys]
    tree = STRtree(geoms) if geoms else None
    poly_meta = [(p[1], p[2], p[3]) for p in polys]   # (sub_id, sub_label, case_id) for each idx
    print(f"  done ({time.time()-t0:.1f}s)\n", flush=True)

    # Step 3: 對每筆物件 point-in-polygon
    print("=== Step 3: 比對物件位置 ===", flush=True)
    db = get_firestore()
    col = db.collection("properties")
    docs = []
    for d in col.select(["city", "latitude", "longitude"]).get():
        data = d.to_dict() or {}
        if data.get("city") not in cities:
            continue
        if not data.get("latitude") or not data.get("longitude"):
            continue
        docs.append((d.id, data))

    print(f"  待比對物件: {len(docs)} 筆", flush=True)
    counts = {"hit": 0, "miss": 0, "errors": 0}
    sub_type_counts = {}
    t_match = time.time()

    for i, (pid, data) in enumerate(docs, 1):
        try:
            x, y = wgs84_to_twd97(data["latitude"], data["longitude"])
            point = Point(x, y)
            cases = []
            seen = set()  # dedup by (sub_id, case_id)
            if tree:
                # STRtree.query 在 shapely 2.x 回 indexes (numpy array)
                hit_idxs = tree.query(point)
                for idx in hit_idxs:
                    poly = geoms[int(idx)]
                    if not poly.contains(point) and not poly.intersects(point):
                        continue
                    sub_id, sub_label, case_id = poly_meta[int(idx)]
                    key = (sub_id, case_id)
                    if key in seen:
                        continue
                    seen.add(key)
                    cases.append({
                        "sub_type": sub_id,
                        "sub_type_label": sub_label,
                        "case_id": case_id,
                        "applicant": "",
                        "summary": "",   # 詳情前端 detail page 「了解細節」按鈕即時 fetch
                    })
            if cases:
                counts["hit"] += 1
                for c in cases:
                    sub_type_counts[c["sub_type"]] = sub_type_counts.get(c["sub_type"], 0) + 1
            else:
                counts["miss"] += 1
            if not args.dry_run:
                col.document(pid).update({"redev_cases": cases})
            if i % 100 == 0 or i == len(docs):
                print(f"  [{i}/{len(docs)}] hit={counts['hit']} miss={counts['miss']} ({time.time()-t_match:.1f}s)", flush=True)
        except Exception as e:
            counts["errors"] += 1
            print(f"  ! {pid} err: {e}", flush=True)

    print(f"\n=== 完成 ({time.time()-t0:.1f}s) ===", flush=True)
    print(f"  套疊到的物件: {counts['hit']}", flush=True)
    print(f"  無套疊: {counts['miss']}", flush=True)
    print(f"  errors: {counts['errors']}", flush=True)
    print(f"  by sub_type:", flush=True)
    for st, n in sorted(sub_type_counts.items(), key=lambda x: -x[1]):
        print(f"    {st}: {n}", flush=True)


if __name__ == "__main__":
    main()
