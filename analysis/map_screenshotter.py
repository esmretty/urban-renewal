"""
Phase 2：政府地圖截圖模組。

截圖對象：
1. easymap.land.moi.gov.tw  → 地籍圖（parcel/cadastral map）
2. 台北市都市計畫GIS         → 容積分區（zoning & FAR）
3. 台北市都更處地圖           → 都更潛力範圍

現況：Phase 1 為 stub，回傳 None。
Phase 2 實作時，每個函式都會：
  1. 用 Playwright 開啟對應網站
  2. 搜尋輸入地址/座標
  3. 等待地圖載入
  4. 截圖存檔至 SCREENSHOTS_DIR
  5. 回傳截圖路徑
"""
import logging
from pathlib import Path
from typing import Optional

from config import SCREENSHOTS_DIR
from scraper.browser_manager import get_browser_context, human_delay

logger = logging.getLogger(__name__)

SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)


def screenshot_cadastral_map(
    source_id: str,
    address: str,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
) -> Optional[str]:
    """
    【Phase 2 - 尚未實作】
    截取地籍圖（easymap.land.moi.gov.tw）。
    回傳截圖路徑，失敗回傳 None。

    目標網站：內政部地政司 地籍圖資網路便民服務系統
    URL: https://easymap.land.moi.gov.tw/
    操作流程：
      1. 開啟網站
      2. 點擊「圖查」或「地址查詢」
      3. 輸入地址
      4. 等待地圖定位
      5. 調整縮放至適當比例（能看到約 200m×200m 的範圍）
      6. 截圖

    TODO Phase 2 實作提醒：
    - easymap 有 session token，每次需重新取得
    - 縮放層級影響判讀，建議 zoom=18（約能看到鄰地狀況）
    - 截圖後用 Claude Vision 分析：地號面積、相鄰地塊、臨路狀況
    """
    logger.info(f"[Phase 2 未實作] 地籍圖截圖 - {address}")
    return None


def screenshot_zoning_map(
    source_id: str,
    lat: float,
    lng: float,
) -> Optional[str]:
    """
    【Phase 2 - 尚未實作】
    截取都市計畫容積分區圖。
    回傳截圖路徑，失敗回傳 None。

    候選資料來源：
    1. 國土規劃地圖 (maps.nlsc.gov.tw) - 全國通用
    2. 台北市政府 GIS (www.tpcgis.gov.tw) - 台北市專用，資料最詳細

    操作流程（以 maps.nlsc.gov.tw 為例）：
      1. 開啟網站，導覽至都市計畫圖層
      2. 輸入座標或地址定位
      3. 開啟「都市計畫分區」圖層
      4. 截圖

    TODO Phase 2 實作提醒：
    - 需要開啟正確的圖層（都市計畫分區，不是道路或地形圖）
    - 截圖後用 Claude Vision 分析：使用分區顏色/標示 → 推算容積率
    - 可對照 config.FAR_BY_ZONE 映射表
    """
    logger.info(f"[Phase 2 未實作] 容積分區截圖 - ({lat}, {lng})")
    return None


def screenshot_renewal_map(
    source_id: str,
    lat: float,
    lng: float,
) -> Optional[str]:
    """
    【Phase 2 - 尚未實作】
    截取台北市都更地圖（顯示現有都更案件範圍）。
    回傳截圖路徑，失敗回傳 None。

    目標網站：台北市都市更新處
    URL: https://uro.gov.taipei/

    TODO Phase 2 實作提醒：
    - 台北市都更處有互動式地圖，需找到正確的地圖頁面
    - 也可查 https://www.udd.gov.taipei/ 大台北地區都更地圖
    - 截圖後用 Claude Vision 分析：
        * 物件是否在既有都更劃定範圍內
        * 鄰近是否有進行中的都更案
        * 是否在防災型都更優先區域
    """
    logger.info(f"[Phase 2 未實作] 都更地圖截圖 - ({lat}, {lng})")
    return None


def run_deep_analysis_screenshots(
    source_id: str,
    address: str,
    lat: Optional[float],
    lng: Optional[float],
) -> dict:
    """
    執行完整的截圖流程（Phase 2 進入點）。
    回傳 dict: {cadastral, zoning, renewal}，值為截圖路徑或 None。
    """
    results = {
        "screenshot_cadastral": None,
        "screenshot_zoning": None,
        "screenshot_renewal": None,
    }

    if lat is None or lng is None:
        logger.warning(f"No coordinates for {source_id}, skipping screenshots")
        return results

    results["screenshot_cadastral"] = screenshot_cadastral_map(source_id, address, lat, lng)
    results["screenshot_zoning"] = screenshot_zoning_map(source_id, lat, lng)
    results["screenshot_renewal"] = screenshot_renewal_map(source_id, lat, lng)

    return results
