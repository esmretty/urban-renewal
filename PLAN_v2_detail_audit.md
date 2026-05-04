# v2 Detail Page 缺漏 / 排版差異審計

## 為什麼這份 plan 存在
v2 detail page 之前我邊改邊讓你 review，越改越偏離 v1。你叫我自己跑 v1/v2 比較 — playwright 因 Google OAuth 登入無法自動化 (5 分鐘等不到登入完成)，改用 v1/v2 source code 1:1 比對寫這份 audit。

---

## A. **重大缺漏** (v1 有 v2 沒做)

### A1. 都更換回試算 — visual layout 完全不同
- **v1 (`renewalV2HTML`, app.js:1704–1900+)**:
  - **rv2-land** 土地持分大數字 + 分區縮寫
  - **rv2-formula** 直式乘法公式 (× 有效容積率 / × 容積獎勵 / × 都更係數 / × 分回比例 / × 新成屋房價 / × 樓層加成 / + 分回車位)，每行 op + label + value
  - 容積獎勵：危老 / 都更 各一個 select dropdown (整數 % options)
  - 分回車位：危老 / 都更 各別顯示「X 萬 (N 位)」
  - **rv2-result** 危老 / 都更 各一個 col，每 col 顯示：總值大字 → 分回坪圈 / 倍數圈 / 損益圈
- **v2 (`detailHTML`, app2.js:922+)**:
  - 2 個 `.v2-scn` 簡化方塊 (倍數大字 + share/total 細字)
  - params table 4 個 input
  - formula hint 一行純文字
- **影響**：v1 是「教學式」公式 (用戶看得到每步乘除)，v2 只給結果

### A2. AI 分析「分回價值」section 動態 bid dropdown
- **v1 (`renderBidSection`, app.js:2201+)**:
  - 偵測 AI text 內 `【分回價值】` 區段 → render 「危老 X 萬 (Y 倍) / 都更 X 萬 (Y 倍)」
  - 加 dropdown「危老出價建議 [≤ 3.0/3.2/3.5/4.0/4.5/5.0 倍] → ≤ N 萬」(讓用戶選想要的倍數，自動算最高出價)
- **v2**: 只 strip 掉 `<bid_selector>` raw tag，無 dropdown，無動態出價建議

### A3. 推測地址候選下拉 (`inferredAddressCellHTML`)
- **v1 (app.js:2152+)**:
  - 多候選時顯示 `<select>`，用戶可挑選正確地址 → 觸發 saveInferredChoice → swap address_inferred + land_area_ping
  - 含 `<a class="map-link">📍</a>` Google Maps 連結
- **v2**: 只顯示 `address_inferred` 純文字 + `(N 候選)` hint，**無法切換**

### A4. 使用分區 cell 多分區編輯 + (特)/(遷) 警示 (`zoningCellHTML`)
- **v1 (app.js:1239+)**:
  - 多分區：每區獨立 input 改坪數 (含「鎖定 vs 自動同步」邏輯)
  - 含 (特)/(遷)/(核)/(抄) 後綴 → 顯示「實際容積採 X 計算 / 真實容積請查都發局」說明
  - 候選列表 (zoning_candidates) 可展開
  - 顯示 GeoServer 來源 label + 連結 ↗
  - 顯示 zoning_error
  - 顯示 zoning_original (跟 effective 不同時才顯示)
- **v2**: 只顯示 `zoning` 純字串 + `(原: X)` hint

### A5. 臨路寬度 cell 完整功能 (`roadWidthCellHTML`, app.js:2330+)
- **v1**:
  - input 寬度 (m)
  - **「重新掃描路寬」**按鈕 (call /api/properties/{id}/scan_road_width)
  - **「地籍圖」按鈕** (toggleRoadPreview 切 inline 截圖預覽 + Vision reason 文字)
  - 寬度不明的「私巷或特窄巷弄」說明
  - 路名不在 GeoServer 提示「該路未登記於政府路寬圖資」
- **v2**: 只 input + 螢幕截圖外連結 (📷 地籍圖 ↗)，**無「重新掃描」按鈕、無 inline 預覽、無 vision reason**

### A6. 法拍/偏遠/特殊 title badge
- **v1 (app.js:1055-1078, showDetailModal)**: title 後面附帶紅/灰色 fc-badge:「法拍屋 / 偏遠路段 / 特殊土地分區」
- **v2**: title 只顯示地址，badge 只在卡片有

### A7. Source link buttons 含「上架日期」
- **v1 (app.js:1084)**: `<a>591 頁面 ↗ <span class="src-pubdate">2026/05/03</span></a>`
- **v2**: `srcBadgesHTML` big 變體只顯示 `591 ↗`，無日期

### A8. Manual 物件「重新分析」按鈕
- **v1 (app.js:1105-1111)**: `id.startsWith('manual_')` 的物件顯示 🔄 重新分析 按鈕 (處理中時 disabled + ⏳)
- **v2**: 完全沒這個

### A9. 地址 / 地坪 警告
- **v1**: 地址含 `address_road_fixed` 顯示「已自動修正：A → B」/`address_suspicious` 顯示「⚠ 路名可能不存在於此行政區」/`land_area_inconsistent` 顯示「實登候選地坪差異大」
- **v2**: 只有 `isLandSuspicious` (建坪 vs 地坪) 警告

---

## B. **排版差距**

### B1. Drawer 寬度 vs Modal 寬度
- **v1**: bootstrap modal `modal-xl`，typically max-width 1140-1320px
- **v2**: drawer `--drawer-w: 880px` (PC), 720 (13"), full width (mobile)
- **影響**: v2 row 內 col-md-7 / col-md-5 在 880px 寬下擠成 513:367，比 v1 798:570 緊很多 → 試算公式、AI 文字都更擠

### B2. 字體系統
- **v1**: Bootstrap 預設 + `.renewal-table.table-sm` (font-size: 0.875rem ≈ 14px)
- **v2**: 自訂 .v2-d-tbl 15px、.v2-d-h 15px、AI 15px
- **影響**: 大致接近，但細節 padding/line-height 不同

### B3. Photo size
- **v1**: `.modal-photo` 寬 100% 自然撐開 (col-md-5 寬約 470px)，圖片可顯示完整
- **v2**: max-height 240px object-fit:cover，圖片裁切
- 用戶最近要求「圖片小一點」所以這合理，但跟 v1 不同

### B4. Title 區塊
- **v1**: bootstrap modal-header 顯示物件地址 (大字 + close X)
- **v2**: 自訂 `.v2-d-title` 18px + sources 右上 (用戶要求)，**沒** close X (mobile 有「← 返回」按鈕)

### B5. Sources position
- **v1**: 在 title 下方一行 (modal-591-wrap)，buttons 是 .tb-btn--ghost 樣式
- **v2**: 在 title 同一行右側，藍底白字 `.v2-src-badge--big` (用戶要求)

---

## C. **行為差異**

### C1. saveOverride 後試算 re-render
- **v1**: 用 _rerenderRenewal() / _syncDetailToList() — 只 re-render 試算 panel + 列表倍數
- **v2**: 整個 detail 重新 render (`_renderDetailFromCurrent`) — 用戶輸入時可能 input 失去 focus

### C2. ephemeral edit warning
- 兩邊都有 — closeDetail 時 if `_ephemeral_edit_made && !_in_watchlist` toast 提示
- v2 toast 出現在右下，v1 是 showFadingToast (不同樣式)

### C3. LVR popup
- 兩邊都用「實」icon hover 觸發
- v1 strip city/dist via `stripCityDist`，v2 自己有 stripCD 函式 (更全面)
- 視覺一致

---

## D. **應該如何修補 (依重要性排)**

### Priority 1 (用戶肉眼可見、互動阻礙)
1. **A2** — AI「分回價值」dropdown：用戶看到 raw bid_selector tag 後現在 strip 掉了，但 dropdown 沒做 → port renderBidSection
2. **A1** — renewalV2HTML 視覺 port：v1 直式公式比 v2 簡化方塊好看 100 倍。port `.rv2` 整套樣式 + 結構
3. **A3** — inferredAddressCellHTML：候選下拉是核心功能 (用戶要修正地址)
4. **A6** — title 法拍/偏遠/特殊 badge

### Priority 2 (能用但功能缺)
5. **A4** — zoningCellHTML 多分區編輯 + (特)/(遷) 警示
6. **A5** — roadWidthCellHTML「重新掃描」+ inline 截圖預覽
7. **A7** — sources 上架日期
8. **A9** — 地址/地坪 warning 文字

### Priority 3 (Edge case)
9. **A8** — manual 重新分析按鈕
10. **B1** — drawer 寬度可考慮加大到 1100-1200px (PC)，跟 v1 modal 對齊

---

## E. **建議解法**

### E1. 直接 port v1 的 helper 函式進 v2 (再試一次)
之前我試過 (commit 26abf20)，user revert 因為「整體不對」。但若**只 port helper functions** (renewalV2HTML / inferredAddressCellHTML / zoningCellHTML / roadWidthCellHTML / formatAiReason / renderBidSection / save handlers) **不載 style.css** — v2 自己的 CSS 不會被污染。

challenges：
- helper functions 用 `.rv2-*` `.ai-section-*` `.lvr-*` 等 v1 class — 需 port 對應 CSS 進 style2.css (避免載 style.css 整個污染)
- helper 全域 (window.renewalV2HTML) — 從 app.js 抽取共用比較乾淨，但 refactor 風險高
- 折衷：把 v1 helper code copy-paste 進 app2.js IIFE 內，class name 加 v2- prefix 避免衝突；CSS 也對應 prefix copy 一份

### E2. CSS port 範圍
從 style.css 必要 copy:
- `.renewal-table` — basic table 行間隔線、row spacing
- `.basic-info-grid`, `.basic-info-col` — 2 table 並排
- `.modal-h` — h6 header style
- `.inferred-tag`, `.addr-fixed-note`, `.addr-suspicious`, `.land-warn` — warning 文字
- `.lvr-icon` — 已有，跳過
- `.inline-edit` — input 樣式
- `.rv2*` — 整套試算公式 (~30 規則)
- `.ai-section*`, `.ai-red`, `.chk-yes`, `.chk-no` — AI markup
- `.zone-badge`, `.zone-orig`, `.zone-special-note`, `.zone-ratio-note`, `.zone-ping-input`, `.zone-ping-error` — 分區 cell
- `.modal-tools`, `.modal-tools__city` — 政府連結
- `.fc-badge` — 法拍 badge

### E3. 漸進式 plan (每步 deploy + 用戶 review)
**Step 1**: title 法拍/偏遠/特殊 badge (簡單) + sources 加日期
**Step 2**: port renewalV2HTML 視覺 (直式公式 + 危老/都更 col) + 對應 .rv2 CSS
**Step 3**: port renderBidSection 動態 dropdown 進 AI section
**Step 4**: port inferredAddressCellHTML 候選下拉 + 對應 save handler
**Step 5**: port zoningCellHTML 多分區編輯 + zoneCandsBlock + (特) 警示
**Step 6**: port roadWidthCellHTML 重新掃描 + inline 截圖預覽
**Step 7**: 細節 (地址 warning、地坪 warning、manual 重新分析)

---

## F. 驗證方式
1. 跑 `python -m http.server` 或 `bash deploy.sh`
2. 打開 v1 + v2 同物件 detail 並列比對 (3 個物件：591 中央 / manual 手動 / 永慶/信義)
3. 檢查清單 (要逐項勾):
   - [ ] title 含法拍/偏遠/特殊 badge
   - [ ] source 含日期
   - [ ] 試算直式公式顯示
   - [ ] 試算 result 危老/都更 並列各顯示 4 metric
   - [ ] AI 分回價值 section 顯示 dropdown
   - [ ] 推測地址多候選 → select
   - [ ] 多分區 → 各區坪數 input
   - [ ] 路寬 → 重新掃描按鈕 + 地籍圖 inline preview
   - [ ] 樓中樓 / 1F / 高樓層 floor 顯示正確
   - [ ] 試算公式跟 v1 算出一樣的倍數 (取 5 個物件 spot check)

---

## G. 用戶決策

下一步請選：
1. **照 Priority 順序逐步 port** (1→2→3→...，每步 deploy)
2. **一次 port 全部** (commits 大但快)
3. **只 port Priority 1** (前 4 個)，剩下接受 v2 簡化版
4. **放棄 1:1 對齊，回頭優化 v2 自有設計** (但用戶剛說過要對齊 v1)

選 1/2/3，我就開動。
