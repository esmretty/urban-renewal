# 都更神探R - Project Policies

## 【核心原則】禁止 AI 幻覺

此專案會顯示物件資料給用戶做投資決策，**任何捏造的數字、地址、或屬性都可能導致財務損失**。

### 規則

1. **爬蟲資料層**
   - 只儲存實際從 591 卡片 / 詳情頁明確抓到的欄位
   - 抓不到 → 存 null，絕不填入預設值或「推估」
   - 詳情頁的地址、屋齡、坪數只能從**特定 DOM 元素**抓（不用全文 regex，避免誤抓相關推薦的其他物件）

2. **AI 分析層（Claude prompt）**
   - System prompt 必須明確禁止捏造事實（屋齡、地址、分區、捷運、都更案名、建物名稱等）
   - 資料「未知/null」時，回答必須指出「資料缺失無法判斷」
   - 移除所有「estimated_*」欄位（那些在鼓勵猜測）
   - 建議（advice）可以，但絕不能偽裝成「已知事實」

3. **UI 顯示層**
   - 缺資料欄位顯示 `—`，不要顯示猜測值
   - 若某物件的關鍵欄位缺太多，標示「資料不完整」提醒用戶

4. **Debug 時先驗證假設再提解法**
   - 用戶回報 bug 時，不要憑看一眼資料就斷言「X 是根因，要改 Y」
   - **先做一個實測**確認自己的假設是對的（例如跑 simulate、直接查 API、比對同類物件的實際資料）
   - 實測結果 vs 假設不一致 → 撤回假設，重找
   - 連續兩次猜錯還繼續猜 → 停下來實測，不要再試第三次
   - 提修法給用戶之前，心裡要能清楚回答「這個 bug 的根因是什麼？」不是「我認為可能是...」
   - 例：用戶說「A 物件對、B 物件錯」→ 先 simulate B 物件在當前 code 下會得到什麼，對照「既有 DB 結果 vs simulate 結果」判斷是 code bug 還是 transient／歷史資料；**不要先提推測性修法方案**

5. **改動會影響 Vision OCR 的參數前，要先想清楚對所有輸入類型的影響**
   - Vision OCR 吃的圖可能尺寸差很大：addr crop 約 1200×500 窄條 vs detail shot 約 2320×3700+ 長頁
   - 一個「固定 tile 尺寸」或「切 2×2」的參數對一種圖 work 不代表對另一種圖也 work
   - 改 tile / crop 參數前：想至少 2 種代表性輸入尺寸，手動算會切出什麼（`right/bottom 會不會超出邊界`、`rows 會不會被 skip`）
   - 改完後實際驗證一筆 smallest case + 一筆 biggest case
   - 別只在一種 case 上測過就 push → 會像 20084920 那樣地址整個消失

6. **主動把重大成就 / 經驗教訓寫進 CLAUDE.md**
   - 每次解掉一個有深度的 bug、踩過值得記的坑、或完成一個重大架構改動 → 主動把「這次學到什麼」「以後該如何防」寫進 CLAUDE.md
   - 不用用戶每次叮嚀「記一下」 — 覺得這個教訓下次會再吃虧、這個原則之後還會被討論 → 直接主動加
   - 加在這個 policy 清單裡（編號 N，保持規則遞增）
   - 寫法要具體可操作：不是「要小心 OCR」而是「改 tile 參數前先驗算 min/max 輸入尺寸」
   - 專案里程碑（上線、重大重構、重大功能完成）也可以加簡短紀錄，日後回顧有跡可循

7. **建坪比對的 ±0.01 原則不可放寬**
   - 同一戶房子的建坪永遠是唯一的數字（權狀登記），差 0.01 以上就代表是**不同戶**，即便同棟隔壁戶
   - LVR triangulate 的 `area_tolerance_ping=0.01` 是正確的，**絕對不可以為了「多撈一些候選」去放寬**
   - 若 LVR 在同建坪 ±0.01 找不到 match → 代表 LVR 沒這戶的記錄（這戶可能從未交易過）→ 正確行為是 `address_inferred=None` 或跑 reverse_geo fallback
   - **禁止**以「±3 坪」「百分比 tolerance」等方式把其他戶的 LVR 記錄列成候選讓用戶選 — 會誤導用戶挑到不同戶的地址
   - **建坪相同不代表同戶：必須再比 floor**：同棟不同樓層常常是相同建坪（同 layout），差別只在樓層。dedup（`find_duplicate` / `find_cross_source_duplicate` / 同 batch 的 `_dup_key`）一定要把 `floor` 列入比對；只有「價格 + 建坪±0.01 + 路名 + 樓層」全部一致才算同戶。
   - 已實作位置：[api/app.py](api/app.py) `find_duplicate`、`_dup_key`；[database/db.py](database/db.py) `find_cross_source_duplicate`
   - 過去踩雷：虎林街 591_20114607（3F）跟 591_20114614（2F）同價同建坪同路名，因為沒比 floor 被誤合併成 1 doc

8. **動態計算的數字（倍數、分回坪、有效容積率…）不存 DB**
   `renewal_v2.scenarios.*.multiple` 這類「由多個輸入欄位即時算出來的結果」**絕不可以存成 DB 定數**。
   存定數會吃 4 種苦：
   - 算式改了 → DB 全錯，要寫 backfill
   - 用戶覆寫某輸入欄（例：路寬、欲出價、新成屋單價）→ DB 跟前端不同步
   - 加新邏輯（例：路寬限縮容積率）→ 又要 backfill
   - **LINE 通知讀 DB 的 multiple → 跟用戶在 app 看到的數字不一樣 → 用戶質疑「這 LINE 怎麼亂報？」**

   **正解**：DB 只存「投入欄位」（`land_area_ping`、`zoning`、`road_width_m`、`price_ntd`、`district`、`building_age`、`zoning_original`…）。倍數/分回坪/有效容積率等「動態結果」一律前端從這些欄位即時算；後端要用（例：LINE 通知判斷門檻）也即時呼叫同一個算式。

   **判斷標準（會不會該存 DB）**：
   - 來源固定不變、抓一次就定（地址、坪數、屋齡、原始分區…）→ 存 DB
   - 由多個欄位推導、輸入一變結果就變（容積率、倍數、分回坪、效益萬…）→ **不要存**

   **算式單一來源**：前端 JS 跟後端 Python 各有一份算式 → 必然飄移。**改其中一邊一定要同步檢查另一邊**。理想做法是：
   - 把算式抽成 single source of truth（後端 endpoint 暴露算結果，前端直接 fetch；或前端跟後端共用同一個算式 + CI 加 cross-test）
   - 至少在 PR 描述列出「我這次改了 effective FAR / multiple / 分回比例 任何一條 → 已同步檢查 [前端 effectiveFarPct / 後端 calculate_renewal_scenarios / LINE notify hook] 三處」
   - 違反檢查：grep `multiple\|share_ping\|effective_far` 超過一個檔案有實作 → 兩處邏輯一定要對得起來

9. **你寫的所有 code / commit / 解釋都會被 Codex 做敵意審查**
   每一個 PR 跟解釋都會經過第三方 AI（Codex）逐行挑刺，找漏掉的 edge case、不一致的邏輯、違反專案規則的地方。
   這意味著：
   - **不能省略偵錯**：寫完別只在 happy path 跑一次就交。grep 一遍受影響的檔案、確認所有 caller 都改完、想 2~3 個邊界 case（空值、極端值、跨城市差異、重抓、用戶覆寫）有沒有壞掉
   - **不能拿模糊措詞掩飾不確定**：如果你不確定某條 path 還會不會被走到、某個欄位還在不在用，**直接 grep 確認**，不要寫「應該不會」「可能沒用了」打混
   - **不能只說「修了 X」**：要寫清楚「修了 X，影響 [檔案 A 第 N 行 / 檔案 B 第 M 行]，已驗證 [具體輸入] → [具體輸出]」。Codex 會把你含糊的句子當靶
   - **不能留半完工**：「等下再回來收尾」「這部分先這樣」就是給審查者送把柄。要嘛做完、要嘛不動，不要做一半
   - 寫 commit message 也一樣 — 「修了 bug」這種無資訊量的訊息會被打槍。要寫根因、修法、影響範圍、驗證方式

5. **用戶可見文字（reason / tooltip / 說明欄位）絕不出現開發用語**
   任何會被用戶看到的字串（`*_reason`、`*_note`、modal 裡的說明、toast 訊息、alert 文字等），**必須以「一般投資人視角」撰寫**，禁止以下內部實作術語：
   - 技術元件名：`Vision`、`OCR`、`regex`、`GeoServer`、`pipeline`、`fallback`、`CQL`、`bbox`、`Firestore`、`LVR`（可用「實價登錄」代替）
   - 流程描述：「規則優先」、「跳過 Vision」、「走到 fallback」、「重跑 pipeline」、「修正後」
   - Bug 根因紀錄：「regex 貪婪吃前綴」、「OCR 誤讀」、「肇因於...」等 debug 語氣
   - 應改成面向用戶的**結果描述**：
     - ❌「地址在「永吉路278巷」上 → 直接取該路寬度 8m（規則優先，跳過 Vision）」
     - ✅「地址位於「永吉路278巷」上，依政府路寬圖資該路寬度為 8m。」
     - ❌「GeoServer 未收錄該路，寬度不明」
     - ✅「該路未登記於政府路寬圖資，寬度不明」
   這類文字**寫進 DB 欄位或前端 innerText 前**一律自問：「普通投資人看到這句會懂嗎？會覺得是 bug 訊息嗎？」如果會 → 改成中性說明。

   **修 bug 時的分工**：
   - **log / logger.warning / logger.info / 註解**：可以寫工程語氣、debug 細節（給開發者看）
   - **DB 欄位值 / alert / toast / modal 顯示**：必須乾淨中性（給用戶看）
   - 兩者不要混用。清理既有髒資料時，也要把用戶可見欄位一起洗乾淨，不能只改新邏輯。

### 違反檢查

在 PR / 修改時若發現下列情況，必須立刻修正：
- Claude prompt 裡要求「估計」「推測」具體數字
- 爬蟲用自由文字 regex 抓可能有多個候選的資料（地址、電話等）
- UI 顯示的欄位不是從 DB 來、而是前端算出來的假值
- 測試用假資料寫死在程式碼
- **從一個欄位「換算/估計」另一個關鍵欄位**（例：建物坪數 × 0.5 假裝是土地面積）
  - 換算只能在「同一概念，單位轉換」時做（例：坪 ↔ m²）
  - 不同概念的欄位互推絕對禁止

## 【Deploy 規則】改完 code 自動 deploy

除非用戶**明示或暗示不要 deploy**（例如「先別 push」「等我看一下」「只是想討論」「WIP / 半成品」「等等再上」「local 試就好」等明確訊號），**否則改完 code 一律自動跑 `bash deploy.sh`**，不需再問。

- commit message 必須標清楚「更新原因」（這版為什麼改、解了什麼、影響哪些 path），不可只寫「fix bug」這種空訊息 — 跟 policy 9（敵意審查）一致
- deploy 前必須已通過自我驗證（policy 4）：smoke test / 跑過受影響 path / grep 過所有 caller
- deploy 是 production 動作（GCE VM `taipei.retty-ai.com`），失敗 / 出錯要立即回報，不可吞掉錯誤
- **deploy 完一定要回報「版本號」**（git short SHA）給用戶確認，例：「deploy 完成，版本 `a6f29b2`，admin 左上「管理後台」badge 旁邊應該也顯示同一個 sha」。版本號來自 `deploy.sh` 最後印的 local commit / server `/api/version`，兩邊要對得上才算 deploy 成功
- admin 後台 topbar「管理後台」badge 旁邊 chip 會顯示 `/api/version` 回傳的 sha — 用戶可以直接視覺對版，所以**不可以擅自關掉這個顯示**
- 不該 auto deploy 的訊號：
  - 用戶明說「先別 deploy」「等我確認」「先 local 試」
  - 改的是 WIP，自己心裡知道沒測完 / 還會再改
  - 改的是 destructive migration / drop column / 影響歷史資料的 backfill — 這類一律先問
  - 改的是 `deploy.sh` 自己 / `.env` / secrets / systemd config — 先確認

## 【語言規則】全程中文

除非用戶明確要求英文回覆，**一律用中文跟用戶說話**。包括：
- 對話回應、解釋、確認問題
- 執行工作中的進度更新 / 自言自語式的狀態報告（「先檢查 X」「跑了 Y，結果 Z」）
- commit message、PR description、log 訊息（中英混雜也 OK，但對話文字以中文為主）

英文是專有名詞 / 程式碼識別名 / 技術術語（function name / API path / package name 等）保留 — 不要硬翻。

## 【名詞定義】抗性物件

「**抗性物件**」= 本身具有讓都更困難 / 投資風險增加的**結構性特性**，不是單純的優劣評分問題，
而是**該物件的法規 / 地理位置 / 樓層性質**就決定它「都更不容易成」「不該被推薦為高價值」。

注意：**法拍屋不是抗性物件**（它是「資料異常」類別 — 法拍價遠低於市價會誤導倍數計算），
也獨立有自己的 LINE skip flag / 前端 chip / 卡片標示，但跟抗性 panel 分開處理。

### 4 種抗性類型（未來可能擴增）

| 類型 | DB 欄位 | 偵測來源 | 為何抗性 |
|---|---|---|---|
| **五樓蓋+** | `total_floors >= 5` | 591 listing API floor 欄位 | 5F 公寓有頂樓加蓋 / 戶數多協議難，都更幾乎跑不動 |
| **偏遠路段** | `is_remote_area=True` | `REMOTE_POLYGONS_NEW_TAIPEI` polygon 判定 | 過天險（河、山）的偏遠地段，房價/需求斷層 |
| **特殊土地** | `unsuitable_for_renewal=True` | ArcGIS / Taipei GeoServer | 保護區/河道用地/機關用地等非住商工，法規不能都更 |
| **地下室** | `is_basement=True` | 591 listing floor **每段 token 都以 'B' 開頭**（純地下室）| 地下室不會被都更分回（停車/儲藏/避難），無投資價值。1F+地下室不算抗性，當 1F 計算（地下室僅是附屬空間） |

### 一致性規則

任何新加的「抗性類型」必須：
1. 在 [database/models.py:PREFER_NEW_FIELDS](database/models.py) 註冊欄位
2. Pipeline ([api/analysis_pipeline.py](api/analysis_pipeline.py)) 在 doc_data 寫入旗標
3. LINE 不可觸發旗標 (`settings/line_config`) 加對應 `skip_<X>` flag (預設 True)
4. 前端「抗性物件過濾器」加對應 checkbox（預設勾選）
5. 物件卡片「優勢/抗性」欄位顯示對應 chip
6. CLAUDE.md 上面那張表新增一列

### 視覺化規範（前端）

- 物件卡片有「優勢/抗性」欄位（取代舊「說明」欄，寬度 ×1.5）顯示 chip list
- **「五樓蓋以上」chip 永遠排第一個 + 土黃色底**（最常見、最易混淆，故置頂）
- 其他抗性 chip 灰色底
- 卡片地址欄不再 inline 顯示抗性 badge（避免分散）
- **法拍屋的標示獨立**（不在抗性 chip list；保留地址欄 inline `fc-badge` 跟獨立過濾 chip）

## 技術架構快照

- **DB**: Firebase Firestore（`properties` collection，document ID = source_id）
- **爬蟲**: Playwright + 591.com.tw
- **分析**: Claude Haiku (文字) / Sonnet (Vision)
- **前端**: Bootstrap 5 + Leaflet.js（目前預設列表模式）

## 目標地區

- 台北市：中正/大同/中山/松山/大安/萬華/信義/內湖/南港/文山（排除北投、士林）
- 新北市：板橋/新莊/新店/中和/永和

## 抓取策略

- URL: `?regionid={region}&section={section_ids}&shape=1,3&order=posttime_desc&firstRow={offset}`
  - `shape=1` = 公寓, `shape=3` = 透天厝（`shape=4` 店面未驗證，暫停）
  - 多個 section 用逗號分隔：`section=26,30,32,33,34`（新北 5 區一次請求）
- 配額：1 區 10 筆 / 2 區 20 筆 / 3+ 區 30 筆，或使用者自訂

## Replace 策略（不要 delete DB）

當增加新欄位或改 scraper 邏輯時，**不要**叫使用者刪 DB，而是讓 scraper 自己補：

對每張卡片 (source_id) 三種處理：
1. **DB 沒有** → 完整 pipeline（卡片 + Vision + AI 分析 + 存）
2. **DB 有但缺欄位** → 標記 `_enrich_existing`，只跑 Vision + update 缺的欄位（不重跑 AI 分析）
3. **DB 有且完整** → 跳過，零成本

「資料完整」定義在 `database/models.py` 的 `REQUIRED_FIELDS`：
`price_ntd, building_area_ping, land_area_ping, building_age, address`

停止條件：連續 5 筆都是「完整舊資料」就停（已追上）。

## 詳情頁防爬處理

591 詳情頁的關鍵數字（土地坪數、屋齡）用 CSS 位移防爬，HTML 文字錯亂。
解法：詳情頁全頁截圖 → Claude Haiku Vision OCR → 結構化 JSON 回傳。
不可用 regex 從詳情頁文字抓數字（會抓到亂碼或誤觸推薦物件的資料）。
