"""
房價 → (地主分回比例, 平面車位市值) 對照表（萬/坪 → %, 萬）。
中間值線性內插；超出範圍 clip 到端點。
"""
from typing import Optional

# (房價 萬/坪, 分回比例 %, 車位 萬)
RAW_TABLE = [
    (60, 45, 234),
    (65, 47, 241),
    (70, 48, 248),
    (75, 49, 255),
    (80, 50, 262),
    (85, 51, 269),
    (90, 52, 276),
    (95, 53, 283),
    (100, 54, 290),
    (105, 55, 297),
    (110, 56, 304),
    (115, 57, 311),
    (120, 58, 318),
    (130, 60, 332),
    (140, 61, 339),
    (150, 62, 360),
    (160, 63, 374),
    (170, 64, 388),
    (180, 65, 402),
]


def lookup(price_wan_per_ping: Optional[float]) -> tuple[Optional[float], Optional[float]]:
    """
    房價（萬/坪）→ (分回比例 0–1, 車位市值 萬)
      - None → (None, None)
      - <= 60 → 用 60 那筆
      - >= 180 → 用 180 那筆
      - 中間值 → 線性內插
    """
    if price_wan_per_ping is None:
        return (None, None)
    p = float(price_wan_per_ping)
    if p <= RAW_TABLE[0][0]:
        return (RAW_TABLE[0][1] / 100.0, float(RAW_TABLE[0][2]))
    if p >= RAW_TABLE[-1][0]:
        return (RAW_TABLE[-1][1] / 100.0, float(RAW_TABLE[-1][2]))
    for i in range(len(RAW_TABLE) - 1):
        p1, r1, c1 = RAW_TABLE[i]
        p2, r2, c2 = RAW_TABLE[i + 1]
        if p1 <= p <= p2:
            f = (p - p1) / (p2 - p1) if p2 > p1 else 0.0
            return ((r1 + (r2 - r1) * f) / 100.0, c1 + (c2 - c1) * f)
    return (None, None)
