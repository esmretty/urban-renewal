"""一次性 migration：把 image_url 從 !2000x.water2.jpg 降到 !1280x.water2.jpg。

之前抓進 DB 的 591 物件主圖是 813KB 的 !2000x，太大載入慢；
新邏輯（commit b4d5c1b）改成 !1280x（418KB）。
此 script 把已存的 14 個 !2000x docs 也降級。

執行：python scripts/downgrade_image_url_2000x_to_1280x.py
"""
import sys, re
sys.path.insert(0, ".")
from database.db import init_db, get_col
from database.time_utils import now_tw_iso

init_db()
col = get_col()

PATTERN = re.compile(r"!\d+x\.water\d?\.(jpg|png|webp)")

n_updated = 0
for d in col.stream():
    x = d.to_dict() or {}
    url = x.get("image_url") or ""
    if not url or "!2000x" not in url:
        continue
    new_url = PATTERN.sub(r"!1280x.water2.\1", url)
    if new_url == url:
        continue
    addr = x.get("address") or x.get("address_inferred") or ""
    print(f"  {d.id} ({addr[:30]}): {url[-50:]} → {new_url[-50:]}")
    col.document(d.id).update({
        "image_url": new_url,
        "_image_url_downgrade_at": now_tw_iso(),
    })
    n_updated += 1

print(f"\n=== 完成：降級 {n_updated} docs (image_url !2000x → !1280x) ===")
