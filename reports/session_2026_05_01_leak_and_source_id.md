# Session 報告 — OOM Leak 修復 + source_id schema 修法 Plan

日期：2026-05-01
觸發：使用者報告昨晚 3am scheduler batch 跑了 60 筆後 OOM

---

## Part 1：OOM Leak — 已修並 deploy

### 證據鏈（一行一條，從觀察到根因）

1. 用戶看到「卡在第 12 筆」 → 我猜 OOM
2. SSH 進 VM 看 journal：`Apr 30 19:48:50 ... A process of this unit has been killed by the OOM killer.` → 確認 OOM
3. 加 4 個 `psutil` RSS log 點到主迴圈 (`A_start / B_postVision / C_postAnalyze / D_iterEnd`)，本地跑 quota=12 batch → 看到每筆 net growth +300-400 MB
4. 第一輪修：`with Image.open(...) as im:` 加 PIL context manager + `gc.collect()` → 沒效，仍 +230-389 MB/iter
5. 第二輪修：每 5 筆 recycle browser context → 沒效，recycle 後 RSS 沒下降 → 證明 leak 不在 Chromium 子進程
6. **隔離測試**：跑 `extract_full_detail_from_screenshot(同一張圖) × 8 次` → 每次 0 漏 → leak 不在 Vision/Anthropic SDK
7. **隔離測試 2**：跑 `screenshot_detail_page(各 url) × 5 次`，**沒呼叫 Vision** → 仍漏 220-440 MB/call → leak 在 PIL
8. 看 PIL 警告 `Image size (99548160 pixels) exceeds limit ...` → 591 全頁截圖 99M pixels = 400 MB raw RGBA
9. 查 PIL 原始碼：`Image.__exit__` 只 close file pointer，**不釋放 self.im pixel buffer** → 必須顯式 `im.close()`

### 真根因（一句話）

> **Python `with Image.open(p) as im:` 是 misleading API — `__exit__` 只關 file pointer，不釋放 decoded pixel buffer。** 591 詳情頁 full_page 截圖每張 400 MB raw，每筆漏一張 → 跑滿 60 筆必爆 4GB VM。

### 修法（commit 0b0bdaa，已 deploy）

| 檔案 | 修法 |
|---|---|
| [scraper/scraper_591.py:935](scraper/scraper_591.py#L935) | `Image.open(full_path)` 改 try/finally + `im.close()`，所有 `im.crop().save()` 也包 try/finally close() |
| [analysis/claude_analyzer.py:_split_image_into_tiles](analysis/claude_analyzer.py#L301) | 同上 pattern |
| [analysis/claude_analyzer.py:_encode_image](analysis/claude_analyzer.py#L570) | resize/convert 路徑也補 close() |
| [analysis/gov_gis.py:render_renewal_zone_map](analysis/gov_gis.py#L685) | `composed`/`base`/`landuse_img` 全 close() |
| [scraper/browser_manager.py](scraper/browser_manager.py) | 加 `get_browser_context_with_browser` yield (ctx, browser) + `_build_ctx` helper |
| [api/app.py:_scrape_and_analyze 主迴圈](api/app.py#L2912) | 每 5 筆 close+rebuild ctx（額外保險）+ try/finally `gc.collect()` |

### 驗證結果

| 指標 | 修前 | 修後 |
|---|---|---|
| iter 1 net growth | +311 MB | +32 MB |
| iter 5 net growth | +321 MB | +21 MB |
| iter 10 net growth | +318 MB | +25 MB |
| 跑 10 筆累積 | 2.5+ GB | 0.24 GB |
| 預估 60 筆累積 | OOM | ~1.5 GB（4GB VM 安全） |

leak 從 **+300-400 MB/iter** 降到 **+18-32 MB/iter**（~92% 減少）。殘餘 ~25 MB/iter 主要是 Playwright page 殘留 + httpx connection pool，不影響 60 筆 quota。

### 留下的觀測

- 主迴圈 4 個 `[mem]` log point 保留 — 之後做 regression 偵測直接看 journal
- `psutil>=5.9.0` 寫進 [requirements.txt](requirements.txt)，deploy 自動帶上

---

## Part 2：source_id schema 不一致 — 修法 Plan（**未動 code**）

### 為什麼這也很重要

跟 OOM 看似獨立，但其實是同一個 batch 失靈的另一條臂：**早停條件「連續 5 筆已收錄」永遠不成立**。原因是 batch 的 `find_doc_by_source_id('591_20119800')` 用「有 prefix」格式 query Firestore，但 DB 多數舊資料存「沒 prefix」的 `'20119800'` → 永遠查不到 → 全當新物件分析 → 跑滿 quota 60 → OOM（修完 leak 後仍會浪費 Vision API 錢 + 重複工）。

### 全 DB 盤查結果（386 docs，非 deleted）

| 類別 | 計數 | 說明 |
|---|---|---|
| sources[] 全部 with prefix | 65 | 較新寫入的 doc |
| sources[] 全部 bare | **286** | 舊 doc，主要是 591 |
| sources[] **混存兩種格式** | **35** | 同 doc 既有 `'591_xxx'` 又有 `'xxx'` — schema 嚴重亂 |

各 source name 的 source_id 格式：

| source name | with prefix | without prefix |
|---|---|---|
| **591** | 49 | **459** ← 90% 是 bare |
| 信義 (中文) | 29 | 0 |
| 永慶 (中文) | 35 | 0 |
| sinyi (英文舊名) | 0 | 1 |
| yongqing (英文舊名) | 0 | 9 |

**=> 591 source 是元兇**：90% 舊資料 bare，新邏輯 prefix → 完全 miss。永慶/信義 都已是 prefix（ok）。

### 修法 Plan（按優先順序）

#### Step 1 — Backfill：把所有 bare 591 source_id 改成 prefix

寫 `scripts/backfill_source_id_prefix_2026_05_01.py`：

```python
# 對每個 doc，掃 sources[]：name=='591' AND source_id 不以 '591_' 開頭
# → source_id 改成 f'591_{sid}'
# 同步更新 source_keys[]（重新 compute_source_keys）
# 同樣對 'sinyi'/'yongqing' (英文舊名) 也順手 normalize
# --dry-run 先印出哪些 doc 會被改 + 變動細節
# --apply 才寫回
```

驗證方法：
- dry-run 應該印出 459 + 1 + 9 = 469 個 source 條目要改
- 386 docs 中應該有 ~321 個 doc 受影響（286 bare + 35 mixed）
- apply 後重跑 audit 應顯示「全部 with prefix」=386

#### Step 2 — Lookup helper：兼容兩種 format（過渡期）

[database/db.py:find_doc_by_source_id](database/db.py) 已存在但只查單一格式。改成查兩種：

```python
def find_doc_by_source_id(sid: str) -> tuple[Optional[str], Optional[dict]]:
    """既支援 '591_xxx' 也支援 'xxx'，一次 query 兩種變體。"""
    # 拆出 raw houseid（去掉前綴）
    raw_id = sid.split('_', 1)[-1] if '_' in sid else sid
    # 也算出 prefixed 版（從 sid 推 source name）
    if sid.startswith('591_'): variants = [sid, raw_id]
    elif sid.startswith(('yongqing_','sinyi_')): variants = [sid, raw_id]
    else: variants = [sid, f'591_{sid}', f'yongqing_{sid}', f'sinyi_{sid}']
    # 對 sources[] array contain 任一 variant 都算命中
    for v in variants:
        q = col.where(...).limit(1)
        ...
```

理由：backfill 後就只剩 prefix 格式，但人工輸入或舊 import 還可能漏。lookup helper 永遠要兩邊都試 → 0 漏。

#### Step 3 — Schema validator：強制新寫入只用 prefix

`database/models.py:add_source_to_doc` 進去前 normalize：

```python
def _normalize_source_id(name: str, sid: str) -> str:
    expect_prefix = {'591':'591_','永慶':'yongqing_','信義':'sinyi_'}.get(name)
    if expect_prefix and not sid.startswith(expect_prefix):
        return expect_prefix + sid
    return sid
```

加在 `add_source_to_doc` 開頭，不管 caller 傳什麼格式都正規化。**避免未來再混存**。

#### Step 4 — Test 重跑 audit

backfill 跑完 + lookup helper 上線後：

```python
# 跑同一支 audit script
# 預期：sources[] 全部 with prefix = 386, mixed = 0, bare = 0
# 各 source: 591 with=508, sinyi=30, yongqing=44, 中文舊名 0
```

順便 dry-run 跑一個 591 listing → DB 比對腳本：
```python
# 抓 591 listing firstRow=0,30,60,90 各 30 筆
# 對每個 sid 用新 lookup helper 查
# 預期：重複率 從目前的 3.5% 跳到 ~30-50%
```

如果重複率明顯升高，「連續 N 筆已收錄」早停條件就會 work。

### 風險

- Backfill 動 DB 是不可逆，**先 export 一份備份**：
  ```bash
  gcloud firestore export gs://...
  ```
- `compute_source_keys` 重算可能改 `source_keys` 欄位順序 → 不影響 query，但 cross-source dedup 要驗一次
- backfill 期間若有 batch 同時跑 → 寫衝突風險 → 先暫停 scheduler 再 apply

### 預估時間

- Step 1 backfill：寫 30 分 + dry-run 5 分 + apply 1 分（單次跑）
- Step 2 lookup helper：寫 + 單測 30 分
- Step 3 validator：10 分
- Step 4 audit：5 分
- **共 ~80 分鐘**

要不要我先做 Step 1 的 dry-run 給你看清單再決定？

---

## Part 3：附帶 — leak hunt 副產品

跑 leak hunt 期間順手寫進 DB 的物件：

- 中和區 / 板橋區 / 永和區 / 新店區：10 筆（leakhunt_local + leakhunt_round1）
- 中山/大安/松山/信義：3 筆（leakhunt_round2b）
- 萬華/大同/中正/文山：7 筆（leakhunt_round3）
- 南港/內湖：10 筆（leakhunt_final）

**共 30 筆新物件入庫**，分析品質跟正常 batch 一樣（log 看 ✓ 已入庫）。如果你想 audit 看哪些是測試造成的，trigger_label 是 `leakhunt_*` 系列。

---

## TL;DR

1. **OOM leak 已修 + deploy**：PIL Image 必須顯式 close()，從 +300MB/iter 降到 +25MB/iter
2. **source_id schema 不一致**是 batch 早停失靈的另一個 bug，**已產出 4 步修法 plan**，等你看完再動
3. 4GB VM 現在跑 60 筆 batch 預估 ~2.1GB，安全。但 source_id 不修的話還是會浪費 Vision API 錢分析早就有的物件
