"""
測試：景興路102巷14號 — 拍兩張 (overview + 高解析度含樓高 4R/5R)

使用者規則：永遠打開
  - 基本圖層/土地使用分區圖 (UrbanPlan) 透明度 30%
  - 基本圖層/地籍圖 (Urban_Land) 透明度 50%
加上都市更新審議相關圖層 + 63 年以前老屋。

執行：從 repo 根目錄執行 `python -m test.uddplanmap.test_jingxing_shots`
"""
import logging
import sys
from pathlib import Path

# 讓 repo 根目錄在 sys.path 以便 import analysis.*
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from analysis.uddplanmap_screenshot import capture, DEFAULT_LAYERS   # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")

LAT, LNG = 24.9944539, 121.5439555  # 景興路102巷14號
OUT_DIR = Path(__file__).parent

# Overview：用 DEFAULT_LAYERS（已含 UrbanPlan 30% + Urban_Land 50% + 都更 + 老屋）

# Close-up：EMap z=22 tile 內建 4R/5R 樓高 + 地號 + 門牌。
# 依照使用者規則同時保留分區 30% + 地籍 50%，但 close-up 時樓高 text 是 EMap tile 裡的，
# Urban_Land 50% 會略微壓過建物 text；若要 4R/5R 最清楚可手動把 Urban_Land 調更低。
CLOSEUP_LAYERS = list(DEFAULT_LAYERS)

capture(
    LAT, LNG,
    out_path=str(OUT_DIR / "jingxing_overview.png"),
    zoom=17,
    extra_shots=[
        (20, str(OUT_DIR / "jingxing_closeup_z20.png"), CLOSEUP_LAYERS),
    ],
)
