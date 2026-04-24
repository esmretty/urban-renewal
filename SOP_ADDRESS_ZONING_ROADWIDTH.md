# SOP：從 591 物件推斷真實地址、土地分區、臨路寬度

> 本文件記錄都更神探R系統的三大核心推斷流程，供日後維護或其他 AI 系統參考。
> 最後更新：2026-04-17

---

## 概觀

591 房屋網的物件資訊有三個先天缺陷：
1. **地址只到巷**（如「大安區信義路三段147巷」），不給門牌號碼
2. **座標偏移**（房仲會把座標偏移 100-300m）
3. **關鍵數字被 CSS 防爬打亂**（坪數、屋齡等需要 Vision OCR）

本系統用以下三步驟解決：

```
591 物件
  ↓
Step 1：推斷真實地址（LVR 實價登錄三角定位 + Google reverse geocode）
  ↓
Step 2：查詢土地使用分區（台北市 GeoServer WFS）
  ↓
Step 3：查詢臨路寬度（台北市 GeoServer WFS road layer）
```

---

## Step 1：推斷真實地址

### 資料來源

- **內政部實價登錄 CSV**（`https://plvr.land.moi.gov.tw/DownloadOpenData`）
  - 每季更新，下載 zip 解壓取 `a_lvr_land_a.csv`（台北市成屋）、`a_lvr_land_b.csv`（台北市預售）、`f_lvr_land_a.csv`（新北市成屋）、`f_lvr_land_b.csv`（新北市預售）
  - 關鍵欄位：`土地位置建物門牌`（完整門牌）、`建物移轉總面積平方公尺`（權狀坪數）、`總樓層數`、`建築完成年月`、`土地移轉總面積平方公尺`（土地坪數）、`總價元`、`備註`（特殊交易原因）
  - 匯入本地 sqlite 索引（`data/lvr_index.db`）

- **LVR 覆蓋策略**
  - **首次**：全量下載（民國 101S3 ~ 最新，約 55 季 / 13 年 / 100+ 萬筆），一次性約需 2-3 分鐘
  - **日常**：如果 sqlite 已有大量資料（> 5 萬筆），每 7 天只增量下載「本期」（~2 秒）
  - **觸發時機**：每次爬取 pipeline 啟動時由 `ensure_fresh()` 自動判斷，透過 `.last_refresh` marker 控制
  - 覆蓋 13 年資料的原因：老公寓可能 5-10 年才成交一次，短期資料覆蓋率不夠

- **Google Maps Geocoding API**（reverse geocode 用，需 API key）

### 三角定位流程

```
輸入：city, district, road_seg, total_floors, building_area_ping, building_age, 591_coord, floor
```

#### 1a. LVR 精確比對

```sql
SELECT address, area_ping, land_ping, year_completed, txn_date,
       price_total, zone_urban, building_type, note
FROM lvr
WHERE city = ? AND district = ? AND road_seg = ? AND total_floors = ?
  AND area_ping BETWEEN ? AND ?   -- 容差 ±0.005（小數第二位完全吻合）
ORDER BY txn_date DESC
```

- **area_ping 容差 = 0.005**（僅容許 float 精度誤差，坪數必須完全一樣，不可有小數點差異）
- 用 building_age ±2 年進一步篩選（若有屋齡資料）
- 同時抓出所有匹配紀錄的 `land_ping`（土地坪數）供下游補充

結果判定：
| 匹配數 | 處理 |
|--------|------|
| 1 筆（unique） | ★ 高信心。直接取該筆完整門牌。 |
| 2+ 筆（multi） | 用 Google reverse geocode 591 座標，取含同路名且距離最近的 → 在 LVR 候選中挑同巷弄的 |
| 0 筆（none） | 進入 1b fallback |

#### 1b. Reverse geocode fallback

當 LVR 無精確匹配時：

```
Google reverse geocode(591_lat, 591_lng)
→ 回傳多筆結果（每筆含自身座標）
→ 過濾只留 street_address / premise 類型
→ 過濾只留含 591 路名的結果
→ 按每筆結果自身座標到 591 座標的直線距離排序
→ 取距離最近的那筆
```

**重點**：不是只看 Google 排序順序，而是用**每個候選自身的 geometry.location 到 591 座標的直線距離**排序。因為 Google 回傳順序可能把對面街的門牌排在前面。

#### 1c. 多候選排除法（進階，距離差 < 20m 時）

1. **LVR 排除**：候選地址若在 LVR 有紀錄但坪數不吻合 → 排除
2. **奇偶門牌**：台灣街道奇數偶數分布在兩側，可輔助判斷同側/對側
3. **591 照片 Vision**（殺手鐧）：下載 591 listing 的外觀照片，用 Claude Vision 判斷建築物特徵。例：候選 A 是空地/工地、候選 B 是完整老公寓 → 直接排除 A
   - 實測案例：嘉興街 399之1號（空地）vs 390號（老公寓），Vision 從照片看出建物外觀 → 確認 390 號

#### 1d. 樓層處理

- LVR 歷史紀錄的樓層可能跟 591 在售樓層不同（同棟不同樓層的歷史成交）
- **一律用 591 的樓層**替換 LVR 地址中的樓層
- 例：LVR 給「景隆街48號三樓」+ 591 在售 5F → 最終「景隆街48號5樓」
- 替換邏輯：用 regex 去掉地址尾部的「X樓」「X層」，再接上 591 的 floor

#### 1e. 地址全形 → 半形

- LVR 門牌用全形數字（「４８號」），前端顯示前統一轉半形（「48號」）
- JS 端 `toHalfWidth()` 處理；Python 端 LVR 原始值保留全形不動（DB 裡維持原貌，前端轉）

#### 1f. LVR 土地坪數補充

- LVR 匹配紀錄中若有 `land_ping`（土地移轉總面積），一併回傳
- 591 沒有土地坪數 → 用 LVR 的值補上，標記 `land_area_source = "lvr"`
- 591 有但與 LVR 不同（差 > 0.01 坪）→ LVR 值另存 `land_area_lvr`，前端用 `*` 標註差異

#### 1g. LVR 成交紀錄附帶

- 所有匹配的 LVR 紀錄（不只最佳那筆）都存入 `lvr_records`（最多 20 筆）
- 每筆含：地址、建坪、地坪、成交價、交易日期、特殊交易標記
- 前端物件列表可展開「實價登錄 N 筆」查看歷史成交
- 特殊交易（備註欄非空）以紅色 + ⚠ 標示，mouseover 顯示原因

### 關鍵注意事項

- **591 卡片坪數 vs Vision OCR 坪數**：591 列表頁卡片顯示的是「權狀坪數」（含公設），與 LVR 的「建物移轉總面積」一致。但 Vision OCR 從詳情頁抓時，要讀**頁面上方房屋介紹區的「權狀坪數」欄位**，嚴禁自己把主建物+附屬建物+公設加起來算（容易漏抓公設或計算錯誤）。
- **LVR CSV 編碼**：UTF-8 with BOM，讀取時用 `encoding="utf-8-sig"`
- **LVR 門牌用全形數字**：「４８號」不是「48號」
- **LVR 樓層用中文**：「三層」「四層」，需 parse

### 程式碼位置

| 模組 | 功能 |
|------|------|
| `scraper/download_lvr.py` | 下載 LVR CSV bundle（`download_all()` 全量 / `download_season()` 單期 / `download_recent(n)` 最近 N 期） |
| `analysis/lvr_index.py :: ensure_fresh()` | 自動判斷是否需要重新下載（首次全量，之後增量，7 天 marker） |
| `analysis/lvr_index.py :: build_index()` | CSV → sqlite |
| `analysis/lvr_index.py :: triangulate_address()` | 三角定位主函式 |
| `analysis/lvr_index.py :: _reverse_geocode_lane()` | Google reverse geocode fallback（含距離排序） |
| `analysis/lvr_index.py :: _pick_closest_by_address()` | 多候選巷弄比對 |

---

## Step 2：查詢土地使用分區

### 資料來源

- **台北市都發局 GeoServer WFS**
  - URL：`https://zonegeo.udd.gov.taipei/geoserver/Taipei/ows`
  - typename：`Taipei:ublock97-TWD97`
  - 座標系：EPSG:3826（TWD97 TM2）
  - 回傳欄位：`usecod`（代碼如 R3）、`usenam`（簡稱如「住3」）、`usemem`（全名如「第三種住宅區」）

- **NLSC 國土測繪中心 API**（輔助，可取得段地號）
  - URL：`https://api.nlsc.gov.tw/other/TownVillagePointQuery/{lng}/{lat}`
  - 回傳：縣市、行政區、段名、村里

### 查詢流程

```
輸入：lat, lng, city（使用 591 原始座標即可，分區多邊形覆蓋範圍大）

1. WGS84 → TWD97 TM2 投影（純數學 inline，不需 pyproj）
   - 橢球體：GRS80 (a=6378137, f=1/298.257222101)
   - central meridian = 121°
   - false easting = 250,000
   - scale factor = 0.9999

2. WFS BBOX 查詢（漸擴）
   for half_size in [5, 15, 30, 60, 120]:
     GET zonegeo.udd.gov.taipei/geoserver/Taipei/ows
       ?service=WFS
       &request=GetFeature
       &version=1.0.0
       &outputFormat=json
       &typename=Taipei:ublock97-TWD97
       &bbox={x-half},{y-half},{x+half},{y+half},EPSG:3826
       &maxFeatures=20

3. 過濾「實質分區」（住/商/工/農/保護/風景 等）
   - 優先回傳「住X」「商X」「工X」等可建蔽類別
   - 排除公共設施用地（公園、國小、道路、高速公路、變電所等）
   - 公設用地只在沒有實質分區時當 fallback

4. 正規化
   - 「住3」→「第三種住宅區」
   - 「商2」→「第二種商業區」
   - 支援「住3-1」→「第三之一種住宅區」等變體
   - 對照表在 gov_gis.py :: ZONE_LABEL_TO_NAME
```

### 為什麼要漸擴 BBOX？

- 座標可能在地籍邊界（建物邊緣、路口、高速公路旁）
- 小 BBOX（5m）可能剛好只碰到道路用地 → 要擴大才涵蓋到旁邊的住宅分區
- 實測：辛亥路三段 5m 只碰到高速公路，30m 才出現住3

### 新北市

目前 `zonegeo.udd.gov.taipei` 只有台北市資料。新北市暫無已知公開 GeoServer WFS endpoint，回傳 `unsupported_city`。

### 程式碼位置

| 模組 | 功能 |
|------|------|
| `analysis/gov_gis.py :: wgs84_to_twd97()` | 座標投影（純 Python，~30 行） |
| `analysis/gov_gis.py :: query_zoning_taipei()` | WFS 分區查詢 |
| `analysis/gov_gis.py :: is_real_zone()` | 判斷是否為實質分區（排除公設用地） |
| `analysis/gov_gis.py :: normalize_zone_name()` | 分區名正規化 |
| `analysis/gov_gis.py :: lookup_zoning_by_coord()` | 高層 API |
| `scraper/zoning_lookup.py :: lookup_zoning()` | Pipeline 介面（呼叫 gov_gis） |

---

## Step 3：查詢臨路寬度

### 資料來源

- **台北市都發局 GeoServer WFS**（同 Step 2 的 server）
  - typename：`Taipei:roadsize-TWD97`
  - 回傳欄位：`road_width`（如「8M」「15M」）、`road_name1`（路名）

### 查詢流程

```
輸入：lat, lng
  ⚠ 使用 Step 1 推測地址 geocode 後的座標，不用 591 原始座標

1. WGS84 → TWD97（同 Step 2）

2. WFS BBOX 查詢（漸擴）
   for half_size in [20, 40, 80, 150]:
     GET zonegeo.udd.gov.taipei/geoserver/Taipei/ows
       ?service=WFS
       &request=GetFeature
       &version=1.0.0
       &outputFormat=json
       &typename=Taipei:roadsize-TWD97
       &bbox={x-half},{y-half},{x+half},{y+half},EPSG:3826
       &maxFeatures=10

3. 取最近的道路（不過濾路名！）
   - 台灣的門牌地址和實際臨路可以在不同街
   - 例：嘉興街390號 建物正面臨的是樂業街79巷（6M）
   - 回傳 all_roads 清單（含路名和路寬），最近的排第一
   - 讓用戶在 UI 確認哪條是實際臨路

4. parse road_width
   - "8M" → 8.0
   - "15M" → 15.0
```

### 為什麼用推測地址座標而非 591 座標？

- 591 座標被房仲偏移 100-300m，查路寬會查到完全不相關的路
- Google geocode 推測地址（如「嘉興街390號」）的座標更精準
- 用推測座標查路寬才能找到正確的臨路

### 臨路寬度對都更的影響

- 容積率上限受路寬限制：基準容積率 × 2(m) ≤ 路寬(m)
- 例：住三（225%）需路寬 ≥ 4.5m；住四（300%）需 ≥ 6.0m
- 若路寬不足 → 有效容積率 = 路寬 × 50（%）
- 前端「都更換回試算」會用此值自動計算「有效容積率」，路寬不足時顯示紅字警示

### 程式碼位置

| 模組 | 功能 |
|------|------|
| `analysis/gov_gis.py :: query_road_width_taipei()` | WFS 路寬查詢 |
| `api/app.py` pipeline | 在地址推斷後 geocode 推測地址 → 查路寬 → 存 `road_width_m` / `road_width_name` / `road_width_all` |

---

## 完整 Pipeline 串接順序

```python
# 每一筆 591 物件的處理順序：

# 0. 從 591 卡片 / 詳情頁 Vision OCR 取得基本資料
item = {city, district, address(巷級), building_area_ping, total_floors, floor, building_age, ...}

# 1. Geocode 591 地址 → 粗略座標
lat, lng = geocode_address(item["address"])

# 2. 土地分區（用 591 座標，分區多邊形覆蓋大精度夠用）
zoning = lookup_zoning_by_coord(lat, lng, city)

# 3. LVR 地址反推（用 591 坪數、樓層、路名、座標三角定位）
addr_result = triangulate_address(
    city, district, road_seg, total_floors,
    building_area_ping, building_age,
    coord=(lat, lng), floor=floor
)
inferred_address = addr_result["address"]
lvr_records = addr_result["lvr_records"]      # 歷史成交紀錄
lvr_land_ping = addr_result["lvr_land_ping"]  # LVR 土地坪數

# 4. LVR 土地坪數補充
if lvr_land_ping:
    if not item["land_area_ping"]:
        item["land_area_ping"] = lvr_land_ping  # 591 沒有 → 用 LVR 的
    elif abs(item["land_area_ping"] - lvr_land_ping) > 0.01:
        item["land_area_lvr"] = lvr_land_ping   # 不同 → 另存對照

# 5. 臨路寬度（用推測地址 geocode 後的精確座標查）
if inferred_address and city == "台北市":
    precise_coord = geocode_address(inferred_address)
    road_width = query_road_width_taipei(precise_coord)

# 6. 存入 DB
doc["address"] = inferred_address or item["address"]  # 推測地址取代 591 地址
doc["zoning"] = zoning
doc["road_width_m"] = road_width["road_width_m"]
doc["lvr_records"] = lvr_records
```

---

## 外部 API 依賴一覽

| API | URL | 用途 | 費用 | 限制 |
|-----|-----|------|------|------|
| Google Geocoding | `maps.googleapis.com/maps/api/geocode/json` | 地址↔座標 + reverse geocode | 免費 $200/月（~40k calls） | 需 API key |
| NLSC TownVillagePointQuery | `api.nlsc.gov.tw/other/TownVillagePointQuery/{lng}/{lat}` | 座標→段地號/村里 | 免費 | 無需 key；無 rate limit 標明 |
| 台北市 GeoServer (分區) | `zonegeo.udd.gov.taipei/geoserver/Taipei/ows`<br>typename=`Taipei:ublock97-TWD97` | WFS 使用分區查詢 | 免費 | SSL 不規範(需 verify=False)；只有台北市 |
| 台北市 GeoServer (路寬) | 同上<br>typename=`Taipei:roadsize-TWD97` | WFS 路寬查詢 | 免費 | 同上 |
| 內政部實價登錄 CSV | `plvr.land.moi.gov.tw/DownloadOpenData` | 歷史成交紀錄 | 免費 | 每季更新；可直接 HTTP 下載 zip |
| Anthropic Claude Sonnet | `api.anthropic.com/v1/messages` | Vision OCR（591 詳情頁截圖） | 按量計費 | 需 API key |

---

## 已知限制與未來改進

1. **新北市分區 + 路寬**：`zonegeo.udd.gov.taipei` 只有台北市。新北需另找 GeoServer（`urban.planning.ntpc.gov.tw` 後端尚未成功 probe）
2. **LVR 坪數精確 match 限制**：591 卡片坪數偶爾跟 LVR 有 0.5-1 坪差異（定義不同），嚴格 match 會 miss → 此時 fallback 到 reverse geocode
3. **591 座標偏移**：reverse geocode fallback 精度取決於房仲偏移幅度（通常 100-300m 內），在窄巷弄密集區可能指向隔壁棟
4. **路寬圖層覆蓋**：部分窄巷弄不在 `roadsize-TWD97` layer 裡（需擴大 BBOX 到 150m 才找到），極小巷弄可能完全沒資料
5. **台灣門牌與臨路不一致**：門牌地址在 A 街、建物正面臨 B 巷，這是台灣普遍現象。路寬查詢不過濾路名就是為了處理這個
6. **Vision OCR 坪數取值**：591 詳情頁頂部的「權狀坪數」才是正確值，不要自己加總主建物+附屬+公設（老公寓公設常為 0，看起來對但邏輯錯）
