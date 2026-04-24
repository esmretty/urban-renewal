"""
下載 內政部實價登錄 批次資料（CSV bundle），解壓只保留台北/新北買賣+預售。

本期 URL: https://plvr.land.moi.gov.tw/Download?type=zip&fileName=lvr_landcsv.zip
歷史期次: https://plvr.land.moi.gov.tw/DownloadSeason?season={rocS}&type=zip&fileName=lvr_landcsv.zip
  - 例：114S3 = 民國 114 年第 3 季

下載解壓策略：
  1. 下載完整 zip（~10-30 MB）
  2. 只抽 a_lvr_land_A.csv / a_lvr_land_B.csv / f_lvr_land_A.csv / f_lvr_land_B.csv
     - _A = 不動產買賣（成屋）
     - _B = 預售屋買賣
  3. 存到 data/lvr/{season}/
"""
import io
import logging
import zipfile
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
LVR_DIR = BASE_DIR / "data" / "lvr"
LVR_DIR.mkdir(parents=True, exist_ok=True)

# 要保留的檔案（台北/新北，買賣+預售；忽略租賃 _C）
KEEP_FILES = {
    "a_lvr_land_a.csv", "a_lvr_land_b.csv",
    "f_lvr_land_a.csv", "f_lvr_land_b.csv",
}


def download_season(season: Optional[str] = None) -> Path:
    """
    下載一期 LVR CSV bundle，只解壓 台北/新北 的檔案。
    season=None → 本期；否則 "114S3" 這種格式。
    回傳：存放該期 CSV 的目錄 Path。
    """
    if season:
        url = f"https://plvr.land.moi.gov.tw/DownloadSeason?season={season}&type=zip&fileName=lvr_landcsv.zip"
        label = season
    else:
        url = "https://plvr.land.moi.gov.tw/Download?type=zip&fileName=lvr_landcsv.zip"
        label = "current"

    out_dir = LVR_DIR / label
    logger.info(f"下載 LVR bundle: {url}")
    r = httpx.get(url, timeout=120, verify=False, follow_redirects=True)
    r.raise_for_status()
    if len(r.content) < 10000:
        raise RuntimeError(f"bundle too small ({len(r.content)} bytes)，可能 season={season} 無效")
    out_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        extracted = 0
        for name in zf.namelist():
            lower = name.lower().split("/")[-1]
            if lower in KEEP_FILES:
                with zf.open(name) as src, open(out_dir / lower, "wb") as dst:
                    dst.write(src.read())
                extracted += 1
        logger.info(f"[{label}] 解壓 {extracted} 個檔案到 {out_dir}")
    return out_dir


def latest_seasons(n: int = 4) -> list[str]:
    """
    回傳最近 N 期的 season 代碼字串清單（民國年 S 季），從最新往舊。
    邏輯：目前月份 → 已結束的最新一季。
    """
    from datetime import datetime
    now = datetime.now()
    roc_year = now.year - 1911
    month = now.month
    # 目前在第幾季（1-4）
    cur_q = (month - 1) // 3 + 1
    # 已結束的最新一季 = cur_q - 1（若 cur_q=1 則去年 Q4）
    if cur_q == 1:
        last_q = 4
        last_year = roc_year - 1
    else:
        last_q = cur_q - 1
        last_year = roc_year

    seasons = []
    y, q = last_year, last_q
    for _ in range(n):
        seasons.append(f"{y}S{q}")
        q -= 1
        if q == 0:
            q = 4
            y -= 1
    return seasons


def download_recent(n_seasons: int = 4) -> list[Path]:
    """下載最近 N 期，回傳各期目錄。"""
    dirs = []
    for s in latest_seasons(n_seasons):
        try:
            dirs.append(download_season(s))
        except Exception as e:
            logger.warning(f"下載 {s} 失敗：{e}")
    # 再試本期（剛發布或尚在發布中的）
    try:
        dirs.append(download_season(None))
    except Exception as e:
        logger.warning(f"下載本期失敗：{e}")
    return dirs


def download_all() -> list[Path]:
    """下載實價登錄全量（101S3 ~ 最新），回傳各期目錄。"""
    all_seasons = []
    for y in range(101, 200):
        for q in range(1, 5):
            if y == 101 and q < 3:
                continue
            all_seasons.append(f"{y}S{q}")
    # 只保留到當前季
    from datetime import datetime
    roc_now = datetime.now().year - 1911
    q_now = (datetime.now().month - 1) // 3 + 1
    all_seasons = [s for s in all_seasons if int(s[:3]) < roc_now or
                   (int(s[:3]) == roc_now and int(s[-1]) <= q_now)]
    dirs = []
    for s in all_seasons:
        try:
            dirs.append(download_season(s))
        except Exception as e:
            logger.debug(f"{s} 略過: {e}")
    try:
        dirs.append(download_season(None))
    except Exception:
        pass
    return dirs


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    download_all()
