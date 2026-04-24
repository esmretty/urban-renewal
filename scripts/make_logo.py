"""把 R_logo_sq.jpg 米白底去掉，輸出透明 PNG。"""
from PIL import Image
import numpy as np

SRC = "frontend/static/R_logo_sq.jpg"
DST = "frontend/static/logo.png"

im = Image.open(SRC).convert("RGBA")
arr = np.array(im)
r, g, b, a = arr[..., 0], arr[..., 1], arr[..., 2], arr[..., 3]

# 米白 / 近白：所有 channel >= 235 且差異不大 → 視為背景
bright = (r >= 230) & (g >= 225) & (b >= 215)
diff = (np.abs(r.astype(int) - g.astype(int)) < 25) & \
       (np.abs(g.astype(int) - b.astype(int)) < 25) & \
       (np.abs(r.astype(int) - b.astype(int)) < 30)
bg = bright & diff

# 邊緣柔化：對 bg mask 做距離計算 → 半透明 ring
new_a = np.where(bg, 0, 255).astype(np.uint8)

# 邊緣 1px 半透明（避免鋸齒）— 不用 scipy，自己 shift
def dilate(mask):
    out = mask.copy()
    out[1:, :] |= mask[:-1, :]
    out[:-1, :] |= mask[1:, :]
    out[:, 1:] |= mask[:, :-1]
    out[:, :-1] |= mask[:, 1:]
    return out
edge = dilate(bg) & ~bg
new_a[edge] = 128

arr[..., 3] = new_a
out = Image.fromarray(arr, mode="RGBA")
# 裁掉透明邊
bbox = out.getbbox()
out = out.crop(bbox)
out.save(DST)
print("saved:", DST, out.size)
