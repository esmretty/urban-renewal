"""一次性清理：刪除 Firestore settings/maintenance 文件。
維護模式已改為 per-server 檔案儲存，這個 doc 不再使用。"""
import sys
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, "d:/Coding/urban-renewal")

from database.db import get_firestore

doc_ref = get_firestore().collection("settings").document("maintenance")
doc = doc_ref.get()
if doc.exists:
    data = doc.to_dict()
    print(f"找到舊 doc 內容：{data}")
    doc_ref.delete()
    print("✓ 已刪除 settings/maintenance")
else:
    print("沒有 settings/maintenance doc，不用清理")
