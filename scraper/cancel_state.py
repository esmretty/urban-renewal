"""共享 cancel flag (version-based) — 讓 batch / scraper 都能 check 用戶是否按了「中斷」。

舊設計（boolean flag + reset）有 race condition：admin kill 設 _CANCELLED=True，
舊 batch 還沒看到 break 點時，scheduler 起新 batch 又 reset() 為 False，
舊 batch 之後 check 看到 False → 不 break → 兩 batch 並行。

新設計：用 monotonic version counter，每次 kill 把 version +1。
- batch 開頭 take_version() 記住自己的 my_version
- batch 內 is_cancelled(my_version) 檢查「version 是否變動」
- admin kill bump_version() — 所有當前 in-flight batch 都會在下個 check 點 break
- 新 batch 起來自己 take_version() 拿到最新值，不受先前 kill 影響

舊 API (is_cancelled() / set_cancelled() / reset()) 保留向後相容，但 scraper inner
loops 已用「last known version」取代 — kill 一次後 inner loops 自然發現變動。

用法（新）：
    from scraper.cancel_state import take_version, is_cancelled, bump_version

    # batch 開頭
    my_v = take_version()

    # 任何 check 點
    if is_cancelled(my_v):
        break

    # admin kill endpoint
    bump_version()
"""

_VERSION: int = 0   # 全域單調遞增；每次 admin kill +1
_LAST_RESET: int = 0   # legacy reset() 記下當時 version，給沒升級的 caller 用


def take_version() -> int:
    """batch 開頭 call — 拿當前 version 作為「我的 batch 起始點」。"""
    return _VERSION


def is_cancelled(my_version: int = None) -> bool:
    """傳 my_version → 比對 batch 起始版本，已被 bump 過就視為 cancelled。
    傳 None（legacy）→ 比對 _LAST_RESET，跟舊 boolean reset() 行為相容。"""
    if my_version is None:
        return _VERSION != _LAST_RESET
    return _VERSION != my_version


def bump_version() -> None:
    """admin kill — bump version，所有當前 in-flight batch 下個 check 點都會 break。"""
    global _VERSION
    _VERSION += 1


# ── Legacy API（向後相容）──
def set_cancelled(v: bool = True) -> None:
    """Legacy：等同 bump_version() 一次（v=True）or no-op (v=False)。"""
    if v:
        bump_version()


def reset() -> None:
    """Legacy：把 _LAST_RESET 同步為當前 version，等同清除「未 ack 的 cancel」。"""
    global _LAST_RESET
    _LAST_RESET = _VERSION
