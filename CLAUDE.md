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

6. **建坪比對的 ±0.01 原則不可放寬**
   - 同一戶房子的建坪永遠是唯一的數字（權狀登記），差 0.01 以上就代表是**不同戶**，即便同棟隔壁戶
   - LVR triangulate 的 `area_tolerance_ping=0.01` 是正確的，**絕對不可以為了「多撈一些候選」去放寬**
   - 若 LVR 在同建坪 ±0.01 找不到 match → 代表 LVR 沒這戶的記錄（這戶可能從未交易過）→ 正確行為是 `address_inferred=None` 或跑 reverse_geo fallback
   - **禁止**以「±3 坪」「百分比 tolerance」等方式把其他戶的 LVR 記錄列成候選讓用戶選 — 會誤導用戶挑到不同戶的地址

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
