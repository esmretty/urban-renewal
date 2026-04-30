# 物件分析路徑全景圖

每次有人問「這個入口跑了什麼」，先看這頁。所有 entry point → 走哪條 fetch → validate → pipeline。

## 架構分層

```
[entry endpoint]
       │
       ▼
[Source adapter ─ fetch raw data]
       │     591 OCR / 永慶 HTTP / 信義 HTTP / 表單 / DB doc
       ▼
[Validate / Normalize]
       │     LVR triangulate? ambiguity check? 列表階段過濾?
       ▼
[analyze_single_property pipeline] ← 最終共用核心
       │     score / AI / renewal / zoning / road_width / Vision
       ▼
[Persist + log_action]
       │     寫 properties (or users/{uid}/manual) + Firestore run_logs
       ▼
   admin session 詳情頁 / 用戶前端
```

## 9 個入口點清單

| # | Endpoint | 觸發場景 | Fetch | Validate | Logs |
|---|---|---|---|---|---|
| 1 | `/api/scrape` | admin 按 batch | `_scrape_and_analyze` (591/永慶/信義 listing) | listing-stage `_is_target_type` + loop-stage skip checks | 完整（new/enrich/dup_merge/cross_source/skip_filter/skip_delisted/skip_scrape_failed/skip_non_apartment）|
| 2 | Scheduler batch | 定時 | 同 #1 | 同 #1 | 同 #1 |
| 3 | `/api/scrape_url` | 用戶貼 URL | `_scrape_single_url` → 591/永慶/信義 inline | 各路線局部 normalize | 較弱（沒走過 build_doc_log_details）|
| 4 | `/admin/properties/{id}/reanalyze` | admin 重抓 591/yc/信義 | 同 #3 (force_reanalyze=True) | 同 #3 | 同 #3 |
| 5 | `/api/manual_analyze` | 用戶手填地址 | 表單 input | `validate_manual_input`：not_found / district_mismatch / lvr_mismatch / **ambiguous_unit** | 走 build_doc_log_details |
| 6 | `/api/manual/{id}/reanalyze` | 用戶按重分析自己的 manual | 從 OLD doc 載 | **同 #5（已統一）** | 同 #5 |
| 7 | `/admin/manual/{uid}/{id}/reanalyze` | admin 重分析他人的 manual | 同 #6 | **同 #5（已統一）** | 同 #5 |
| 8 | `/api/analyze/{id}` | pending 物件按「分析」 | DB 既有 doc | 無（doc 已有完整欄位）| 無（pipeline 內部寫）|
| 9 | `/api/deep_analyze/{id}` | 深度分析（後處理）| 既有 doc | 無 | 無（獨立路線，不在統一範圍）|

## Validate 為何不能完全統一

Manual flow 的 validate（`validate_manual_input`）有 4 種「需要用戶選擇」的回應：

- `ambiguous_unit`: 同地址 LVR 多戶（建坪/地坪持分不同）→ 用戶必須挑哪戶
- `lvr_mismatch`: 用戶輸入跟 LVR 差 ±0.01 坪以上 → 用戶選 user 還是 lvr
- `district_mismatch`: 地址解析的 district ≠ 用戶選的 → 用戶挑正確的區
- `not_found`: LVR + Google 都查不到 → 用戶從建議地址挑

這些**需要同步互動**。Batch / URL / scheduler 是背景跑，沒人即時回應，硬塞會 deadlock。

所以分工：
- **Manual entry (5/6/7)**: 走 `validate_manual_input` 全套 dialog
- **Batch / URL entry (1/2/3/4)**: scraper 自己 OCR + 列表過濾就決定，遇到歧義就照規則走（OCR 的建坪+地坪通常已足夠唯一），不彈 dialog
- **Pending entry (8)**: doc 已有資料，pipeline 直接跑

## log_action 統一格式

所有「動到一筆物件」的 action 都用 `database.run_log.build_doc_log_details(item, doc_data, **extras)` 產生 details，包含：

| 欄位 | 來源 |
|---|---|
| url | sources[0].url 或 item.url |
| title | item.title (591 卡片標題) |
| address | doc_data.address_inferred / address |
| price_ntd | 總價 |
| building_area_ping / land_area_ping | 建坪 / 地坪 |
| total_floors / floor | 樓層 |
| building_age | 屋齡 |
| zoning | 土地分區 |
| score_total | 評分 |
| ai_recommendation | AI 建議 |
| is_remote_area / unsuitable_for_renewal | 旗標 |

可加 extras：`merged_into`, `change_reason`, `conflicts`, `discarded`, `reason`, `filter_stage`, `missing` 等視 action 而定。

## Action 種類

- `batch_start` / `batch_end`：session 起點/終點
- `new`：新物件入庫
- `enrich`：補既有 doc 缺的欄位
- `reanalyze`：force_reanalyze 完整重跑
- `dup_merge`：偵測到重複，併入既有 doc
- `cross_source`：跨來源（永慶/信義）併入既有 591 doc
- `replacement`：同 source_id 換物件（路段或建坪變動 ≥ 0.5 坪）
- `skip_filter`：listing 階段被 `_is_target_type` 過濾（總層數>5、商辦等）
- `skip_delisted`：591 詳情頁回 404，物件已下架
- `skip_scrape_failed`：詳情頁缺價格 / 行政區，scrape 失敗
- `skip_non_apartment`：總樓層 ≥ 6F，非公寓
- `verify_alive_archive` / `verify_alive_prune`：偵測下架自動處理
- `error`：未預期錯誤

## 復原方式

任何階段如果發現問題：
```bash
git tag                              # 看可用 backup tag
git reset --hard <tag>               # 例: pre-task-AB-2026-04-30
bash deploy.sh                       # push + remote pull + restart
```
