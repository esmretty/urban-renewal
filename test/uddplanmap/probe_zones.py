"""
Probe：查 (24.9944539, 121.5439555) 附近的都市計畫使用分區 (UrbanPlan layer 1)。
目標：找出「道路用地」對應的 NAME / ID 值，給 find_redevelopment_block 當分界依據。

用法：python -m test.uddplanmap.probe_zones
"""
import math
import sys
from collections import Counter
from pathlib import Path

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

LAT, LNG = 24.9944539, 121.5439555
RADIUS_M = 250
URL = "https://www.historygis.udd.gov.taipei/arcgis/rest/services/Urban/UrbanPlan/MapServer/1/query"


def wgs84_to_webmerc(lat, lng):
    x = lng * 20037508.34 / 180.0
    y = math.log(math.tan((90 + lat) * math.pi / 360.0)) * 20037508.34 / math.pi
    return x, y


def main():
    x, y = wgs84_to_webmerc(LAT, LNG)
    env = f"{x - RADIUS_M},{y - RADIUS_M},{x + RADIUS_M},{y + RADIUS_M}"
    r = requests.get(URL, params={
        "geometry": env,
        "geometryType": "esriGeometryEnvelope",
        "inSR": "102100",
        "outSR": "102100",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "NAME,ID,FILL,LEVEL",
        "returnGeometry": "false",
        "f": "json",
    }, timeout=30, verify=False)
    feats = r.json().get("features", [])
    names = Counter(f["attributes"].get("NAME", "?") for f in feats)
    print(f"total zones: {len(feats)}")
    for n, c in names.most_common():
        print(f"  {n!r}: {c}")

    # 輸出樣本 attributes
    print("\nsample attributes (first 5):")
    for f in feats[:5]:
        print(f"  {f['attributes']}")


if __name__ == "__main__":
    main()
