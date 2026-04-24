"""
測試：從 景興路102巷14號 flood-fill「4F 以下連棟基地」，把範圍畫到截圖上。

規則（由使用者定義）：
  - 只納入 Build_NO ≤ 4 的建物
  - 相鄰定義：牆到牆 ≤ 2m （巷/馬路都 >2m → 自然形成分界）
  - 忽略面積 < 30m² 的附屬物
  - 排除 T（鐵皮/臨時）結構
"""
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from analysis.uddplanmap_screenshot import capture, DEFAULT_LAYERS   # noqa: E402
from analysis.building_info import find_redevelopment_block          # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")

LAT, LNG = 24.9944539, 121.5439555   # 景興路102巷14號
OUT_DIR = Path(__file__).parent

# 1) 算出 flood-fill 範圍
block = find_redevelopment_block(LAT, LNG, max_floors=4, adjacency_m=2.0, min_area_sqm=30.0)
print("block summary:")
print(f"  seed                 = {block['seed_label']}")
print(f"  building_count       = {block['building_count']}")
print(f"  total_area_sqm       = {block['total_area_sqm']}")
print(f"  total_area_ping      = {block['total_area_ping']}")
print(f"  floor_hist           = {block['floor_hist']}")

# 2) 疊畫到 close-up z=20 上
capture(
    LAT, LNG,
    out_path=str(OUT_DIR / "jingxing_redev_block.png"),
    zoom=20,
    layers=DEFAULT_LAYERS,
    polygons_webmerc=block["rings_webmerc"],
)
print(f"screenshot: {OUT_DIR / 'jingxing_redev_block.png'}")
