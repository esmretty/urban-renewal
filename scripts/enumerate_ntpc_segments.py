"""一次性腳本：列出新北 4 區所有段名 (dump 給 cadastral_search.py hardcode 用)。

策略：
  NTPC ArcGIS WAF 擋 multi-field WHERE + SQL injection 擋 outStatistics 含 %field name，
  但「WHERE LANDNO='N' + 區 bbox」可行。
  → 對每區 query 多個常見 LANDNO (1, 5, 10, 50, 100, 500, 1000, 2000)，每個 query 回
    最多 1000 features in bbox；收集所有 distinct SECTNAME。
  → 同一 LANDNO 可能多個 段都有 → 一次查到很多段名。
  → 配合 tighter 區 bbox (from villages_polygon.geojson) → 不會掃到鄰區段名。

跑一次：python scripts/enumerate_ntpc_segments.py > scripts/_ntpc_segments.json
然後手動把結果貼進 api/cadastral_search.py 的 _NTPC_SEGMENTS dict。
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding='utf-8')

import httpx
from api.gis_overlay import _get_ntpcurinfo_layer_meta
from analysis.gov_gis import wgs84_to_twd97

# 從 villages_polygon 算每區 tight bbox (lng/lat → TWD97)
def _district_bbox_twd97(district: str) -> tuple[float, float, float, float]:
    gj_path = Path(__file__).resolve().parent.parent / "data" / "school_districts" / "villages_polygon.geojson"
    with open(gj_path, encoding="utf-8") as f:
        gj = json.load(f)
    lngs, lats = [], []
    for feat in gj.get("features", []):
        props = feat.get("properties") or {}
        if props.get("town") != district or props.get("county") != "新北市":
            continue
        geom = feat.get("geometry") or {}
        def _iter(c):
            if isinstance(c[0], (int, float)):
                yield c
                return
            for sub in c:
                yield from _iter(sub)
        for lng, lat in _iter(geom.get("coordinates", [])):
            lngs.append(lng); lats.append(lat)
    if not lngs:
        raise ValueError(f"找不到 {district} 的 polygon")
    # WGS84 → TWD97 (lat, lng → x, y)
    x_min, y_min = wgs84_to_twd97(min(lats), min(lngs))
    x_max, y_max = wgs84_to_twd97(max(lats), max(lngs))
    return (min(x_min, x_max), min(y_min, y_max), max(x_min, x_max), max(y_min, y_max))


def main():
    meta = _get_ntpcurinfo_layer_meta('地籍圖')
    if not meta:
        print("拿不到 NtpcURInfo meta", file=sys.stderr)
        sys.exit(1)

    DISTRICTS = ["板橋區", "新店區", "中和區", "永和區"]
    # 用多個 LANDNO probe，每個 query 回最多 1000 features in bbox
    # LANDNO 1 大概率每段都有 (主號 1)，後續 5/10/100... 補沒覆蓋到的段
    LANDNO_PROBES = ['1', '5', '10', '50', '100', '200', '500', '1000']
    SECTNAME_FIELD = "NTPCUPGIS_SDE.NTPCGIS2.%Land.SECTNAME"
    LANDNO_FIELD = "NTPCUPGIS_SDE.NTPCGIS2.%Land.LANDNO"

    out = {}
    with httpx.Client(timeout=30, verify=False) as client:
        for dist in DISTRICTS:
            xmin, ymin, xmax, ymax = _district_bbox_twd97(dist)
            print(f"\n[{dist}] bbox=({xmin:.0f},{ymin:.0f},{xmax:.0f},{ymax:.0f})", file=sys.stderr, flush=True)
            seen_sects = set()
            for landno in LANDNO_PROBES:
                # quote field name with double-quotes (must contain % literal)
                where = f'"{LANDNO_FIELD}"=\'{landno}\''
                try:
                    r = client.get(meta['mapsrvurl'] + '/0/query', params={
                        'f': 'json', 'token': meta['agstoken'],
                        'where': where,
                        'outFields': '*', 'returnGeometry': 'false',
                        'geometry': f'{xmin:.0f},{ymin:.0f},{xmax:.0f},{ymax:.0f}',
                        'geometryType': 'esriGeometryEnvelope', 'inSR': '3826',
                        'spatialRel': 'esriSpatialRelIntersects',
                    })
                except Exception as e:
                    print(f"  LANDNO={landno} fail: {e}", file=sys.stderr)
                    continue
                if r.status_code != 200:
                    print(f"  LANDNO={landno} http={r.status_code}", file=sys.stderr)
                    continue
                try:
                    d = r.json()
                except Exception:
                    print(f"  LANDNO={landno} non-JSON", file=sys.stderr)
                    continue
                if d.get('error'):
                    print(f"  LANDNO={landno} error: {d['error']}", file=sys.stderr)
                    continue
                feats = d.get('features', [])
                new_sects = set()
                for f in feats:
                    s = (f.get('attributes') or {}).get(SECTNAME_FIELD)
                    if s and s not in seen_sects:
                        new_sects.add(s)
                seen_sects |= new_sects
                print(f"  LANDNO={landno}: +{len(new_sects)} new ({len(feats)} hits, total {len(seen_sects)})", file=sys.stderr, flush=True)
            out[dist] = sorted(seen_sects)
            print(f"[{dist}] 總計 {len(seen_sects)} 段", file=sys.stderr, flush=True)
    json.dump(out, sys.stdout, ensure_ascii=False, indent=2)
    print(file=sys.stderr)


if __name__ == "__main__":
    main()
