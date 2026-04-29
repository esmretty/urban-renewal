"""共享 cancel flag — 讓 batch / scheduler / scraper 都能 check 用戶是否按了「中斷」。

之前 _cancel_requested 是 api/app.py 的 module-level global，scraper 看不到，
只有 _scrape_and_analyze 分析迴圈檢查。但 scraper 的 listing fetch / detail enrich /
Playwright wait 等 loop 完全沒檢查 → user 按 kill 後 scraper thread 繼續跑直到自然結束。

用法：
    from scraper.cancel_state import is_cancelled, set_cancelled, reset

    # 在 batch 開始時
    reset()

    # 在 scraper inner loop 頂端
    if is_cancelled():
        break

    # admin kill endpoint
    set_cancelled(True)

注意：是 process-level flag，不適合多用戶情境。本專案 single-user / single-batch
語意，用這個簡單模型剛好。
"""

_CANCELLED: bool = False


def is_cancelled() -> bool:
    return _CANCELLED


def set_cancelled(v: bool = True) -> None:
    global _CANCELLED
    _CANCELLED = v


def reset() -> None:
    set_cancelled(False)
