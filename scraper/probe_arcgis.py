"""
驗證 wgs84_to_twd97 + ArcGIS layer 90 query。
"""
import math
import httpx


def wgs84_to_twd97(lat, lng):
    """
    WGS84 (degrees) → TWD97 TM2 (m).
    台灣 zone：central meridian 121°, false_easting 250000, k0=0.9999
    GRS80 ellipsoid: a=6378137, f=1/298.257222101
    """
    a = 6378137.0
    f = 1 / 298.257222101
    e2 = f * (2 - f)
    e_prime2 = e2 / (1 - e2)
    k0 = 0.9999
    lon0 = math.radians(121.0)
    fe = 250000.0
    fn = 0.0

    phi = math.radians(lat)
    lam = math.radians(lng)

    n = a / math.sqrt(1 - e2 * math.sin(phi) ** 2)
    t = math.tan(phi) ** 2
    c = e_prime2 * math.cos(phi) ** 2
    A = math.cos(phi) * (lam - lon0)

    M = a * (
        (1 - e2 / 4 - 3 * e2**2 / 64 - 5 * e2**3 / 256) * phi
        - (3 * e2 / 8 + 3 * e2**2 / 32 + 45 * e2**3 / 1024) * math.sin(2 * phi)
        + (15 * e2**2 / 256 + 45 * e2**3 / 1024) * math.sin(4 * phi)
        - (35 * e2**3 / 3072) * math.sin(6 * phi)
    )

    x = fe + k0 * n * (
        A
        + (1 - t + c) * A**3 / 6
        + (5 - 18 * t + t**2 + 72 * c - 58 * e_prime2) * A**5 / 120
    )
    y = fn + k0 * (
        M
        + n * math.tan(phi) * (
            A**2 / 2
            + (5 - t + 9 * c + 4 * c**2) * A**4 / 24
            + (61 - 58 * t + t**2 + 600 * c - 330 * e_prime2) * A**6 / 720
        )
    )
    return x, y


def query_arcgis(lat, lng):
    x, y = wgs84_to_twd97(lat, lng)
    print(f"  TWD97: ({x:.2f}, {y:.2f})")
    r = httpx.get(
        "https://www.historygis.udd.gov.taipei/arcgis/rest/services/UrbanPlan2/UrbanPlan2/MapServer/2/query",
        params={
            "geometry": f"{x},{y}",
            "geometryType": "esriGeometryPoint",
            "inSR": "3826",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "*",
            "returnGeometry": "false",
            "f": "json",
        },
        timeout=15,
        verify=False,    # 政府網站憑證有時不規範
    )
    data = r.json()
    feats = data.get("features", [])
    if feats:
        for fea in feats:
            print("  ↳", fea.get("attributes"))
    else:
        print("  ↳ 空")


SAMPLES = [
    ("辛亥路三段157巷", 25.0169715, 121.5490196),
    ("嘉興街附近", 25.0216015, 121.5511856),
    ("信義路三段147巷", 25.0348151, 121.5419269),
    ("通化街123巷", 25.0290566, 121.5548571),
]

for label, lat, lng in SAMPLES:
    print(f"=== {label} ({lat}, {lng}) ===")
    try:
        query_arcgis(lat, lng)
    except Exception as e:
        print(f"  ERROR: {e}")
