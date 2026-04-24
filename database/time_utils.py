"""時間統一模組：全專案 timestamp 統一為台灣時區 aware datetime。

使用方式：
    from database.time_utils import now_tw_iso, now_tw, to_tw, TW_TZ

    doc["scraped_at"] = now_tw_iso()   # "2026-04-24T09:11:26+08:00"
"""
from datetime import datetime, timezone, timedelta


TW_TZ = timezone(timedelta(hours=8))


def now_tw() -> datetime:
    """台北時區的 aware datetime（UTC+8）。"""
    return datetime.now(TW_TZ)


def now_tw_iso() -> str:
    """台北時區的 ISO 字串，格式 '2026-04-24T01:11:26+08:00'。"""
    return now_tw().isoformat()


def to_tw(dt: datetime) -> datetime:
    """把任何 aware/naive datetime 轉成台北 aware。
    naive 視為 UTC（既有資料絕大多數是 datetime.utcnow() 存下的）。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TW_TZ)


def parse_to_tw(iso_str: str) -> datetime:
    """parse ISO 字串 → 台北 aware。
    naive（無 tz suffix）視為 UTC；aware 直接 astimezone。
    parse 失敗 raise ValueError。"""
    # Python 3.11+ fromisoformat 支援 'Z'；保險替換一下
    s = iso_str.replace("Z", "+00:00") if iso_str.endswith("Z") else iso_str
    dt = datetime.fromisoformat(s)
    return to_tw(dt)
