"""預售屋實價登錄 → 各行政區單價（萬/坪）中位數 → Firestore。

資料源：內政部不動產交易實價登錄「預售屋」（lvr_land_b.csv）
  - a_lvr_land_b.csv = 台北市
  - f_lvr_land_b.csv = 新北市
  - 既有 scraper/download_lvr.py 已 handle 下載 / 解壓

寫入：Firestore `settings/district_new_house_price`
  {
    "updated_at": "2026-04-29T16:00:00+08:00",
    "source": "...",
    "by_district": {"中正區": 110.5, "大安區": 152.3, ...},
    "samples": {"中正區": 87, "大安區": 142, ...},
  }

scorer.py 讀此 doc（1 hr cache）算 renewal_v2，找不到 fallback 到 config.py 的 const。
"""
import csv
import logging
import statistics
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# 元/m² → 萬/坪
# 1 坪 = 3.30578 m²，所以 元/m² × 3.30578 = 元/坪；除 10000 = 萬/坪
SQM_TO_PING_WAN_FACTOR = 3.30578 / 10000.0

# 我們系統覆蓋的 15 個目標行政區（保留台北 10 + 新北 5）
TARGET_DISTRICTS = {
    "中正區", "大同區", "中山區", "松山區", "大安區", "萬華區",
    "信義區", "內湖區", "南港區", "文山區",
    "板橋區", "新莊區", "新店區", "中和區", "永和區",
}

# 過濾交易標的：純車位/純土地排除（這些單價不能代表新成屋住宅）
KEEP_DEAL = {"房地(土地+建物)", "房地(土地+建物)+車位", "建物"}

# 離群過濾：預售屋住宅單價合理範圍 30 ~ 300 萬/坪
PRICE_MIN_WAN = 30
PRICE_MAX_WAN = 300

# 樣本數低於此值不採用此區（資料不可信）
MIN_SAMPLES = 5


def _read_lvr_b_csv(csv_path: Path) -> list[dict]:
    """讀 lvr_land_b CSV。內政部 CSV 第二行是英文 metadata header（"The unit ..."）要略過。"""
    out = []
    try:
        with open(csv_path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for r in reader:
                dist = (r.get("鄉鎮市區") or "").strip()
                if not dist or dist.startswith("The"):
                    continue
                out.append(r)
    except Exception as e:
        logger.warning(f"讀 LVR CSV 失敗 {csv_path}: {e}")
    return out


def compute_district_medians(rows: list[dict]) -> dict[str, dict]:
    """從 LVR rows 算每區單價中位數 + 樣本數。"""
    by_dist: dict[str, list[float]] = {}
    for r in rows:
        dist = (r.get("鄉鎮市區") or "").strip()
        if dist not in TARGET_DISTRICTS:
            continue
        deal = (r.get("交易標的") or "").strip()
        if deal not in KEEP_DEAL:
            continue
        try:
            unit_per_sqm = float(r.get("單價元平方公尺") or 0)
        except ValueError:
            continue
        if unit_per_sqm <= 0:
            continue
        unit_wan_per_ping = unit_per_sqm * SQM_TO_PING_WAN_FACTOR
        if unit_wan_per_ping < PRICE_MIN_WAN or unit_wan_per_ping > PRICE_MAX_WAN:
            continue
        by_dist.setdefault(dist, []).append(unit_wan_per_ping)

    out = {}
    for dist, prices in by_dist.items():
        if len(prices) < MIN_SAMPLES:
            continue
        out[dist] = {
            "median": round(statistics.median(prices), 1),
            "mean": round(statistics.mean(prices), 1),
            "n": len(prices),
        }
    return out


def collect_recent_seasons(base_dir: Path, max_seasons: int = 5) -> list[dict]:
    """掃 data/lvr/ 下最近 N 季的預售屋 CSV，回所有 rows。"""
    lvr_root = base_dir / "data" / "lvr"
    if not lvr_root.exists():
        return []
    # 季資料夾排序（114S2 < 114S3 < ... < 115S1, current 最後）
    season_dirs = []
    current_dir = None
    for d in lvr_root.iterdir():
        if not d.is_dir():
            continue
        if d.name == "current":
            current_dir = d
        else:
            season_dirs.append(d)
    season_dirs.sort(key=lambda d: d.name, reverse=True)
    selected = season_dirs[:max_seasons]
    if current_dir:
        selected.append(current_dir)

    all_rows = []
    for sd in selected:
        for fn in ("a_lvr_land_b.csv", "f_lvr_land_b.csv"):
            p = sd / fn
            if p.exists():
                rows = _read_lvr_b_csv(p)
                all_rows.extend(rows)
                logger.info(f"  讀 {sd.name}/{fn}: {len(rows)} 筆")
    return all_rows


def _detect_latest_season(base_dir: Path) -> Optional[str]:
    """掃 data/lvr/ 找最新季資料夾名（如 '115S1'，或若 current 較新則 'current'）。"""
    lvr_root = base_dir / "data" / "lvr"
    if not lvr_root.exists():
        return None
    season_dirs = []
    has_current = False
    for d in lvr_root.iterdir():
        if not d.is_dir():
            continue
        if d.name == "current":
            has_current = True
        else:
            season_dirs.append(d.name)
    season_dirs.sort(reverse=True)
    latest = season_dirs[0] if season_dirs else None
    # 若 current 存在，回傳「最新季 + current」字樣讓 caller 看
    if has_current and latest:
        return f"{latest}+current"
    return latest


def update_district_prices(*, max_seasons: int = 5, base_dir: Optional[Path] = None) -> dict:
    """主流程：parse 既有 LVR CSV → 算中位數 → 寫 Firestore。
    回傳含 diff 的 payload（含 previous_by_district / previous_updated_at / previous_season /
    latest_season / 各區舊值新值對照）。
    base_dir: 可選，預設用 BASE_DIR；測試時可指定。
    """
    if base_dir is None:
        from config import BASE_DIR
        base_dir = BASE_DIR

    # 1. 寫前先讀舊值（給 diff log）
    previous: dict = {}
    try:
        from database.db import get_firestore
        old_doc = get_firestore().collection("settings").document("district_new_house_price").get()
        if old_doc.exists:
            previous = old_doc.to_dict() or {}
    except Exception as e:
        logger.warning(f"讀舊 district_new_house_price 失敗（diff 不會包含舊值）: {e}")

    old_by_district = previous.get("by_district") or {}
    old_samples = previous.get("samples") or {}
    old_updated_at = previous.get("updated_at")
    old_latest_season = previous.get("latest_season")

    # 2. 算新值
    rows = collect_recent_seasons(base_dir, max_seasons=max_seasons)
    logger.info(f"預售屋 LVR 總筆數: {len(rows)}")
    medians = compute_district_medians(rows)
    if not medians:
        raise RuntimeError("沒有任何區算出有效中位數（樣本數均 < 5），檢查 LVR data")

    by_district = {d: v["median"] for d, v in sorted(medians.items())}
    samples = {d: v["n"] for d, v in sorted(medians.items())}
    new_latest_season = _detect_latest_season(base_dir)

    # 3. 算 diff
    all_districts = sorted(set(old_by_district.keys()) | set(by_district.keys()))
    diff = {}
    for d in all_districts:
        old_v = old_by_district.get(d)
        new_v = by_district.get(d)
        diff[d] = {
            "old": old_v,
            "new": new_v,
            "old_samples": old_samples.get(d),
            "new_samples": samples.get(d),
            "delta": (round(new_v - old_v, 1) if (isinstance(old_v, (int, float)) and isinstance(new_v, (int, float))) else None),
        }

    payload = {
        "updated_at": datetime.now().astimezone().isoformat(),
        "source": f"內政部不動產交易實價登錄 預售屋（最近 {max_seasons} 季+current）",
        "by_district": by_district,
        "samples": samples,
        "latest_season": new_latest_season,
        "previous_latest_season": old_latest_season,
        "previous_updated_at": old_updated_at,
        "row_count": len(rows),
        "diff": diff,
    }
    try:
        from database.db import get_firestore
        get_firestore().collection("settings").document("district_new_house_price").set(payload)
        logger.info(
            f"寫 Firestore settings/district_new_house_price OK，{len(by_district)} 區"
            f"（LVR 期：{old_latest_season} → {new_latest_season}，{len(rows)} 筆樣本）"
        )
    except Exception as e:
        logger.warning(f"寫 Firestore 失敗（payload 仍回傳給 caller）: {e}")
    return payload


# ── 給 scorer.py 讀的 cache 介面 ─────────────────────────────────────
_PRICE_CACHE: dict = {"data": None, "samples": None, "fetched_at": 0.0}
_CACHE_TTL_SEC = 3600   # 1 小時 cache，避免每筆 renewal_v2 都打 Firestore


def get_district_new_house_price(district: str) -> Optional[float]:
    """回傳該區「最新預售屋單價」（萬/坪）。
    優先 Firestore（auto update），找不到 fallback 到 config.py 的常數。"""
    import time
    now = time.time()
    if not _PRICE_CACHE["data"] or (now - _PRICE_CACHE["fetched_at"]) > _CACHE_TTL_SEC:
        try:
            from database.db import get_firestore
            doc = get_firestore().collection("settings").document("district_new_house_price").get()
            if doc.exists:
                data = doc.to_dict() or {}
                _PRICE_CACHE["data"] = data.get("by_district") or {}
                _PRICE_CACHE["samples"] = data.get("samples") or {}
                _PRICE_CACHE["fetched_at"] = now
        except Exception as e:
            logger.debug(f"讀 Firestore district price cache 失敗（fallback const）: {e}")
    if _PRICE_CACHE["data"] and district in _PRICE_CACHE["data"]:
        return _PRICE_CACHE["data"][district]
    # fallback
    from config import DISTRICT_NEW_HOUSE_PRICE_WAN
    return DISTRICT_NEW_HOUSE_PRICE_WAN.get(district)


def get_all_district_prices() -> dict:
    """給 frontend 用：回完整 dict（觸發 cache refresh）。
    回 {by_district: {區: 價}, samples: {區: n}, source: str}"""
    import time
    now = time.time()
    if not _PRICE_CACHE["data"] or (now - _PRICE_CACHE["fetched_at"]) > _CACHE_TTL_SEC:
        try:
            from database.db import get_firestore
            doc = get_firestore().collection("settings").document("district_new_house_price").get()
            if doc.exists:
                data = doc.to_dict() or {}
                _PRICE_CACHE["data"] = data.get("by_district") or {}
                _PRICE_CACHE["samples"] = data.get("samples") or {}
                _PRICE_CACHE["fetched_at"] = now
                return {
                    "by_district": _PRICE_CACHE["data"],
                    "samples": _PRICE_CACHE["samples"],
                    "source": data.get("source"),
                    "updated_at": data.get("updated_at"),
                }
        except Exception:
            pass
    if _PRICE_CACHE["data"]:
        return {
            "by_district": _PRICE_CACHE["data"],
            "samples": _PRICE_CACHE["samples"] or {},
        }
    # 完全 fallback 到 const
    from config import DISTRICT_NEW_HOUSE_PRICE_WAN
    return {
        "by_district": dict(DISTRICT_NEW_HOUSE_PRICE_WAN),
        "samples": {},
        "source": "config.py 預設值（尚未 update_district_prices）",
    }
