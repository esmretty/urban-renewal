/* ==========================================================================
   都更神探R · v2 — app2.js
   全新前台邏輯，與 app.js 完全隔離。
   - 重用 auth_gate.js（無 UI 依賴）
   - 重用既有 API endpoint（/api/properties, /api/central_search, /api/me, etc.）
   - 全部命名 prefix v2 避免污染 global scope（用 IIFE + window.v2 namespace）
   ========================================================================== */
(function () {
  'use strict';

  // ── State ────────────────────────────────────────────────────────────────
  // 每個 view 的物件 cache 各自獨立 — 切 tab 不重抓
  const state = {
    view: 'explore',           // 'explore' | 'watchlist'
    allProperties: [],         // 當前 view 用的（render 即從這個來）
    exploreLoaded: false,      // explore 是否已抓過
    watchlistLoaded: false,    // watchlist 是否已抓過
    exploreItems: [],          // explore 抓的全部物件 (server 不再 filter，client 來 filter)
    watchlistItems: [],        // 用戶觀察清單
    filteredSorted: [],
    page: 1,
    pageSize: 50,
    selectedId: null,
    targetRegions: {},
    districtPicks: new Set(),
    sortDir: 'desc',
    gridCity: '台北市',   // mobile 兩城切換用 (≤1024px)
  };

  // 跟 v1 hardcode 的 enabled/disabled district 對齊（v1 index.html 寫死的）
  // 啟用：可勾選；停用：灰色不可選（先不爬 / 資料不足）
  const V1_DISTRICTS = {
    "台北市": {
      enabled:  ["大安區", "信義區", "中山區", "中正區", "文山區"],
      disabled: ["萬華區", "松山區", "大同區", "南港區"],
      labels:   { "中山區": "中山", "大安區": "大安", "信義區": "信義", "中正區": "中正",
                  "文山區": "文山", "萬華區": "萬華", "松山區": "松山", "大同區": "大同", "南港區": "南港" },
    },
    "新北市": {
      enabled:  ["新店區", "永和區", "中和區", "板橋區"],
      disabled: [],
      labels:   { "新店區": "新店(市區)", "永和區": "永和", "中和區": "中和", "板橋區": "板橋(市區)" },
    },
  };

  // ── Helpers ──────────────────────────────────────────────────────────────
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  // ── 已讀紀錄 (localStorage 共用 key 'urban_read_props')，v1/v2 共用 ────────
  // value = JSON object { source_id: timestamp_iso }, 上限 5000 筆 LRU evict
  const READ_KEY = 'urban_read_props';
  let _readMap = null;
  function _loadReadMap() {
    if (_readMap) return _readMap;
    try { _readMap = JSON.parse(localStorage.getItem(READ_KEY) || '{}'); }
    catch { _readMap = {}; }
    return _readMap;
  }
  function _saveReadMap() {
    if (!_readMap) return;
    // 上限 5000 → 砍最舊的 1000 筆
    const keys = Object.keys(_readMap);
    if (keys.length > 5000) {
      const sorted = keys.sort((a, b) => (_readMap[a] || '').localeCompare(_readMap[b] || ''));
      sorted.slice(0, 1000).forEach(k => delete _readMap[k]);
    }
    try { localStorage.setItem(READ_KEY, JSON.stringify(_readMap)); }
    catch (e) { console.warn('saveReadMap failed', e); }
  }
  function isRead(id) { return !!_loadReadMap()[id]; }
  function markRead(id) {
    if (!id) return;
    _loadReadMap()[id] = new Date().toISOString();
    _saveReadMap();
  }

  // ── Filter 偏好持久化 (per uid，僅 explore 用) ─────────────────────────────
  function _filterKey() {
    const uid = (window.currentUser && window.currentUser.uid) || 'anon';
    return `explore-filters-v2:${uid}`;
  }
  function _saveFilters() {
    const obj = {
      road: $('#v2-road')?.value || '',
      dists: Array.from(state.districtPicks),
      btypes: $$('.v2-filter-btype:not(:disabled)').filter(c => c.checked).map(c => c.value),
      floors: $$('#v2-floor-chips input[data-floor]').filter(c => c.checked).map(c => c.value),
      pmin: $('#v2-price-min')?.value || '',
      pmax: $('#v2-price-max')?.value || '',
      maxBld: $('#v2-bld-price-max')?.value || '',
      maxLand: $('#v2-land-price-max')?.value || '',
      minLand: $('#v2-land-min')?.value || '',
      sortBy: $('#v2-sort')?.value || 'list_rank',
      sortDir: state.sortDir,
      minMultOn: $('#v2-min-mult-on')?.checked || false,
      minMultVal: $('#v2-min-mult-val')?.value || '3.0',
      hideF5: $('#v2-hide-floors5plus')?.checked || false,
      hideRem: $('#v2-hide-remote')?.checked || false,
      hideUns: $('#v2-hide-unsuitable')?.checked || false,
      hideBas: $('#v2-hide-basement')?.checked || false,
      hideFc: $('#v2-hide-foreclosure')?.checked || false,
    };
    try { localStorage.setItem(_filterKey(), JSON.stringify(obj)); } catch {}
  }
  function _restoreFilters() {
    let obj;
    try { obj = JSON.parse(localStorage.getItem(_filterKey()) || 'null'); } catch {}
    if (!obj) return;
    const setVal = (id, v) => {
      const el = $('#' + id);
      if (el && v !== undefined && v !== null && v !== '') el.value = v;
    };
    const setChk = (id, v) => {
      const el = $('#' + id);
      if (el && typeof v === 'boolean') el.checked = v;
    };
    setVal('v2-road', obj.road);
    setVal('v2-price-min', obj.pmin);
    setVal('v2-price-max', obj.pmax);
    setVal('v2-bld-price-max', obj.maxBld);
    setVal('v2-land-price-max', obj.maxLand);
    setVal('v2-land-min', obj.minLand);
    setVal('v2-sort', obj.sortBy);
    if (obj.sortDir === 'asc' || obj.sortDir === 'desc') {
      state.sortDir = obj.sortDir;
      const sd = $('#v2-sort-dir');
      if (sd) sd.textContent = obj.sortDir === 'desc' ? '↓' : '↑';
    }
    if (typeof obj.minMultVal === 'string' || typeof obj.minMultVal === 'number') {
      const el = $('#v2-min-mult-val'); if (el) el.value = obj.minMultVal;
    }
    setChk('v2-min-mult-on', obj.minMultOn);
    setChk('v2-hide-floors5plus', obj.hideF5);
    setChk('v2-hide-remote', obj.hideRem);
    setChk('v2-hide-unsuitable', obj.hideUns);
    setChk('v2-hide-basement', obj.hideBas);
    setChk('v2-hide-foreclosure', obj.hideFc);
    // 地區：寫進 state.districtPicks (chip render 之後才能 reflect)
    if (Array.isArray(obj.dists) && obj.dists.length > 0) {
      state.districtPicks.clear();
      obj.dists.forEach(k => state.districtPicks.add(k));
    }
    // 類型 chips
    if (Array.isArray(obj.btypes)) {
      const set = new Set(obj.btypes);
      $$('.v2-filter-btype:not(:disabled)').forEach(c => { c.checked = set.has(c.value); });
    }
    // 樓層 chips
    if (Array.isArray(obj.floors)) {
      const set = new Set(obj.floors);
      $$('#v2-floor-chips input[data-floor]').forEach(c => { c.checked = set.has(c.value); });
      const fa = $('#v2-floor-all');
      if (fa) {
        const all = $$('#v2-floor-chips input[data-floor]');
        fa.checked = all.length > 0 && all.every(c => c.checked);
      }
    }
  }
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[c]));
  const fmt0 = (n) => n == null ? '—' : Number(n).toLocaleString('zh-TW', { maximumFractionDigits: 0 });
  const fmt1 = (n) => n == null ? '—' : Number(n).toLocaleString('zh-TW', { maximumFractionDigits: 1 });
  const fmt2 = (n) => n == null ? '—' : Number(n).toLocaleString('zh-TW', { maximumFractionDigits: 2 });

  function toast(msg, kind = '') {
    const host = $('#v2-toast-host');
    if (!host) return;
    const el = document.createElement('div');
    el.className = `v2-toast ${kind ? `v2-toast--${kind}` : ''}`;
    el.textContent = msg;
    host.appendChild(el);
    setTimeout(() => el.remove(), 3500);
  }

  // ── Type icon ────────────────────────────────────────────────────────────
  const TYPE_ICON = {
    '公寓': '🏢', '透天厝': '🏠', '華廈': '🏬', '大樓': '🏙', '店面': '🏪',
  };
  const typeIcon = (t) => TYPE_ICON[t] || '🏚';

  // ── Floor formatting ─────────────────────────────────────────────────────
  function formatFloor(p) {
    const fmin = p.floor_range_min;
    const fmax = p.floor_range_max;
    const tot = p.total_floors;
    if (fmin && fmax && fmin !== fmax) {
      return tot ? `${fmin}-${fmax}/${tot}F` : `${fmin}-${fmax}F`;
    }
    if (p.floor != null) {
      return tot ? `${p.floor}/${tot}F` : `${p.floor}F`;
    }
    if (p.is_basement) return 'B';
    return tot ? `?/${tot}F` : '—';
  }

  // ── AI 分析文字 render (對齊 v1 formatAiReason) ──
  // 「分回價值」section 特殊處理 → renderBidSection (動態算分回值 + 出價建議 dropdown)
  function renderAiText(text, p, prices) {
    if (!text) return '';
    return text.split(/\n\n+/).map(section => {
      const m = section.match(/^【(.+?)】\s*([\s\S]*)/);
      if (m) {
        const title = m[1];
        // 「分回價值」改用動態渲染 (從 input 欄位即時算)
        if (title === '分回價值' && p && prices) {
          return `<div class="v2-ai-sec"><div class="v2-ai-sec__title">${esc(title)}</div>
            <div class="v2-ai-sec__body" id="v2-ai-bid-section">${renderBidSection(p, prices)}</div></div>`;
        }
        let body = esc(m[2].trim());
        body = body.replace(/(\d+\.\d+)×/g, '$1倍').replace(/(\d+)×/g, '$1倍');
        body = body.replace(/&lt;chk-y&gt;([\s\S]*?)&lt;\/chk-y&gt;/g, '<span style="color:#16a34a;font-weight:700">✓</span> $1');
        body = body.replace(/&lt;chk-n&gt;([\s\S]*?)&lt;\/chk-n&gt;/g, '<span style="color:#dc2626;font-weight:700">✗</span> <span style="color:#9ca3af">$1</span>');
        body = body.replace(/&lt;red&gt;([\s\S]*?)&lt;\/red&gt;/g, '<span style="color:#dc2626;font-weight:600">$1</span>');
        // bid_selector tag 內含 &quot; (escaped quotes)，所以用 [\s\S]*? non-greedy 而不是 [^&]*
        body = body.replace(/&lt;bid_selector[\s\S]*?&gt;/g, '');
        body = body.replace(/\n/g, '<br>');
        return `<div class="v2-ai-sec"><div class="v2-ai-sec__title">${esc(title)}</div><div class="v2-ai-sec__body">${body}</div></div>`;
      }
      return `<div class="v2-ai-sec"><div class="v2-ai-sec__body">${esc(section).replace(/\n/g, '<br>')}</div></div>`;
    }).join('');
  }

  // ── 「分回價值」section 動態渲染 (對齊 v1 renderBidSection) ─────────────────
  // 顯示：危老 X 萬 (Y 倍) / 都更 X 萬 (Y 倍)
  //      • 危老出價建議：[3.2 倍 ▾] ≤ N 萬
  //      • 都更出價建議：[3.2 倍 ▾] ≤ N 萬
  // dropdown onchange 不打 server，純前端 JS 即時算 ≤ N 萬
  function renderBidSection(p, prices) {
    if (!p) return '—';
    if (p.is_foreclosure || p.is_remote_area || p.unsuitable_for_renewal) {
      const reason = p.is_foreclosure ? '法拍屋'
        : p.is_remote_area ? '新北偏遠路段'
        : '特殊土地分區（非住商工）';
      return `此物件標記為「${reason}」，出價試算不適用。`;
    }
    const land = p.land_area_ping;
    const zoning = effectiveZoning(p);
    const newPrice = p.new_house_price_wan_override ?? prices[p.district];
    if (!land || !zoning || !newPrice) return '缺資料，無法計算';
    const farPct = effectiveFar(p);
    const coeff = p.rebuild_coeff ?? 1.57;
    const [ratio, parking] = lookupShareRatio(newPrice);
    const isFangzai = p.city === '台北市' && currentAge(p) && (new Date().getFullYear() - currentAge(p)) <= 1974;
    const bonusW = p.bonus_weishau ?? 0.30;
    const bonusD = p.bonus_dugen ?? (isFangzai ? 0.80 : 0.50);
    const is1F = Number(p.floor) === 1 || Number(p.floor_range_min) === 1;
    const floorPremium = p.floor_premium ?? (is1F ? 0.20 : 0);
    const effectivePrice = newPrice * (1 + floorPremium);
    const calcVal = (b) => {
      const share = land * (farPct / 100) * (1 + b) * coeff * (ratio || 0);
      return share * effectivePrice + (share / 40) * (parking || 0);
    };
    const wVal = Math.round(calcVal(bonusW));
    const dVal = Math.round(calcVal(bonusD));
    // priceWan 用欲出價 (沒填則 fallback 開價)
    const priceWan = parseFloat(p.desired_price_wan ?? (p.price_ntd ? Math.round(p.price_ntd / 10000 * 0.9 / 10) * 10 : 0)) || 0;
    const fmt = (n) => n.toLocaleString('zh-TW', { maximumFractionDigits: 0 });
    const hasPrice = priceWan > 0;
    const multW = hasPrice ? `（${(wVal / priceWan).toFixed(2)}倍）` : '';
    const multD = hasPrice ? `（${(dVal / priceWan).toFixed(2)}倍）` : '';

    let html = wVal
      ? `危老 ${fmt(wVal)}萬${multW}　都更 ${fmt(dVal)}萬${multD}`
      : `都更 ${fmt(dVal)}萬${multD}`;

    if (!hasPrice) {
      html += `<div class="v2-bid-row v2-bid-row--muted">（尚未填入欲出價，無法給出價建議）</div>`;
      return html;
    }

    const opts = [3.0, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 4.0, 4.2, 4.5, 5.0];
    const mkOpts = (sel) => opts.map(v =>
      `<option value="${v}" ${Math.abs(v - sel) < 0.01 ? 'selected' : ''}>${v.toFixed(1)} 倍</option>`).join('');
    const wMax = wVal ? Math.round(wVal / 3.2) : 0;
    const dMax = Math.round(dVal / 3.2);
    if (wVal) {
      html += `<div class="v2-bid-row">• 危老出價建議：<select class="v2-bid-select" onchange="this.nextElementSibling.textContent='≤ '+Math.round(${wVal}/parseFloat(this.value)).toLocaleString()+' 萬'">${mkOpts(3.2)}</select> <span class="v2-bid-max">≤ ${fmt(wMax)} 萬</span></div>`;
    }
    html += `<div class="v2-bid-row">• 都更出價建議：<select class="v2-bid-select" onchange="this.nextElementSibling.textContent='≤ '+Math.round(${dVal}/parseFloat(this.value)).toLocaleString()+' 萬'">${mkOpts(3.2)}</select> <span class="v2-bid-max">≤ ${fmt(dMax)} 萬</span></div>`;
    return html;
  }

  // ── 分區縮寫 helper (對齊 v1 zoneAbbr 並擴充處理 (特)/(遷)/(核)/(抄) 後綴) ──
  // 第一/二/三/四種住宅區 → 住一/住二/住三/住四
  // 第三之一種住宅區 → 住三之一
  // 第四種商業區 → 商四 / 第二種工業區 → 工二
  // 住宅區/商業區/住宅用地 → 住/商/住地
  // 後綴 (特)/(遷)/(核)/(抄) 保留貼在縮寫後面
  function zoneAbbr(z) {
    if (!z) return '—';
    const suffixMatch = z.match(/(\((?:特|遷|核|抄)\))/);
    const suffix = suffixMatch ? suffixMatch[1] : '';
    const base = z.replace(/\((?:特|遷|核|抄)\)/g, '').trim();
    const m = base.match(/^第([一二三四五六])(?:之([一二三四五六]))?種(住宅|商業|工業)區$/);
    if (m) {
      const [, n, sub, kind] = m;
      const k = kind === '住宅' ? '住' : kind === '商業' ? '商' : '工';
      return sub ? `${k}${n}之${sub}${suffix}` : `${k}${n}${suffix}`;
    }
    if (base === '住宅區') return '住' + suffix;
    if (base === '商業區') return '商' + suffix;
    if (base === '工業區') return '工' + suffix;
    if (base === '住宅用地') return '住地' + suffix;
    return base + suffix;
  }

  // ── 屋齡 helper (對齊 v1 currentAge) ─────────────────────────────────────
  function currentAge(p) {
    if (!p) return null;
    const yc = p.building_age_completed_year;
    if (yc && Number.isFinite(yc) && yc > 1900) {
      return new Date().getFullYear() - yc;
    }
    return p.building_age ?? null;
  }

  // ── 商業區判定（對齊 v1 _isCommercialEffective）──────────────────────────
  function _isCommercialEffective(p) {
    const zList = p.zoning_list;
    if (Array.isArray(zList) && zList.length > 1) {
      const ratios = p.zoning_ratios || zList.map(() => 100 / zList.length);
      for (let i = 0; i < zList.length; i++) {
        const z = (typeof zList[i] === 'string') ? zList[i] : (zList[i].original_zone || zList[i].zone_name);
        const r = Number(ratios[i]) || 0;
        if (z && z.includes('商業區') && r > 0) return true;
      }
      return false;
    }
    return !!(p.zoning && p.zoning.includes('商業區'));
  }

  // ── 優勢 chip (對齊 v1 computeAdvantageChips) — TOD / 防災型 / 商業區 ────
  function computeAdvantageChips(p) {
    const chips = [];
    if (p.nearest_mrt_dist_m != null && p.nearest_mrt_dist_m <= 500) {
      chips.push({ label: 'ＴＯＤ', cls: 'v2-achip v2-achip--tod' });
    }
    const age = currentAge(p);
    if (p.city === '台北市' && age && (new Date().getFullYear() - age) <= 1974) {
      chips.push({ label: '防災型', cls: 'v2-achip v2-achip--fangzai' });
    }
    if (_isCommercialEffective(p)) {
      chips.push({ label: '商業區', cls: 'v2-achip v2-achip--commercial' });
    }
    return chips;
  }

  // ── Resistance chip computation ──────────────────────────────────────────
  function computeChips(p) {
    const chips = [];
    if (p.total_floors && p.total_floors >= 5 && p.building_type !== '透天厝') {
      chips.push({ label: '⚠ 五樓蓋+', cls: 'v2-rchip--floors5plus' });
    }
    if (p.is_remote_area) chips.push({ label: '⚠ 偏遠路段', cls: '' });
    if (p.unsuitable_for_renewal) chips.push({ label: '⚠ 特殊土地', cls: '' });
    if (p.is_basement) chips.push({ label: '⚠ 地下室', cls: '' });
    if (p.is_foreclosure) chips.push({ label: '⚖ 法拍', cls: 'v2-rchip--foreclosure' });
    return chips;
  }

  // ── Multiple computation (lightweight; matches scorer but only what we need) ──
  // Pull district→price mapping from API; cached on first load.
  // API 回傳 { by_district: {大安區: 150, ...}, updated_at: '...' } — 必須拿 by_district 不能直接 [district]
  // Fallback：API 失敗/格式異常時用寫死常數 (跟 v1 / config.py DISTRICT_NEW_HOUSE_PRICE_WAN 對齊)
  const DISTRICT_PRICE_FALLBACK = {
    "中正區":110,"大同區":95,"中山區":110,"松山區":130,"大安區":150,
    "萬華區":80,"信義區":145,"內湖區":110,"南港區":110,"文山區":90,
    "板橋區":75,"新莊區":65,"新店區":75,"中和區":70,"永和區":75,
  };
  let DISTRICT_PRICE_CACHE = null;
  async function getDistrictPrices() {
    if (DISTRICT_PRICE_CACHE) return DISTRICT_PRICE_CACHE;
    try {
      const r = await fetch('/api/district_new_house_price');
      const data = await r.json();
      const by = (data && data.by_district) || {};
      // 合併 fallback：API 沒回的區用寫死常數兜底
      DISTRICT_PRICE_CACHE = { ...DISTRICT_PRICE_FALLBACK, ...by };
    } catch {
      DISTRICT_PRICE_CACHE = { ...DISTRICT_PRICE_FALLBACK };
    }
    return DISTRICT_PRICE_CACHE;
  }

  function isLandSuspicious(p) {
    if (p.building_type === '透天厝') return false;
    const land = Number(p.land_area_ping) || 0;
    const build = Number(p.building_area_ping) || 0;
    return build > 0 && land > build;
  }

  // ── FAR / share-ratio 表（跟 app.js / config.py 對齊，CLAUDE.md 規則 8: DB 不存 → 前端即時算） ──
  const SHARE_TABLE = [
    [60,0.45,234],[65,0.47,241],[70,0.48,248],[75,0.49,255],[80,0.50,262],
    [85,0.51,269],[90,0.52,276],[95,0.53,283],[100,0.54,290],[105,0.55,297],
    [110,0.56,304],[115,0.57,311],[120,0.58,318],[130,0.60,332],[140,0.61,339],
    [150,0.62,360],[160,0.63,374],[170,0.64,388],[180,0.65,402],
  ];
  const TAIPEI_FAR_PCT = {
    "第一種住宅區":60,"第二種住宅區":120,"第三種住宅區":225,"第三種住宅區(特)":225,
    "第三之一種住宅區":300,"第三之二種住宅區":400,"第四之一種住宅區":400,
    "第四種住宅區":300,"住宅用地":200,
    "第一種商業區":360,"第二種商業區":630,"第三種商業區":560,"第三種商業區(特)":560,
    "第四種商業區":800,
  };
  const NEW_TAIPEI_FAR_PCT = {
    "_banqiao_fujou": { "住宅區": 240, "住宅區(再)": 160, "商業區": 300 },
    "新店區": {
      "第二種住宅區": 120, "第三種住宅區": 280, "第四種住宅區": 300,
      "第一種商業區": 420, "第二種商業區": 440,
      "住宅區": 300, "商業區": 440,
    },
    "土城區": { "第一種住宅區": 180, "第二種住宅區": 240, "第一種商業區": 240, "第二種商業區": 320 },
    "樹林區": { "第一種住宅區": 260, "第二種住宅區": 250, "商業區": 380 },
    "汐止區": { "第一種住宅區": 200, "第二種住宅區": 240, "商業區": 320 },
    "淡水區": { "第二種住宅區": 225, "第三種住宅區": 360, "第四種住宅區": 240, "第一種商業區": 360, "第二種商業區": 400 },
    "八里區": { "第一種住宅區": 200, "第二種住宅區": 200, "第一種商業區": 300, "第二種商業區": 300 },
  };
  const _NTPC_5 = ["板橋區","新莊區","中和區","永和區","三重區"];

  function lookupFar(zoning, p) {
    if (!zoning || !p) return null;
    const district = p.district;
    if (district === "板橋區" && p.is_remote_area) {
      return NEW_TAIPEI_FAR_PCT["_banqiao_fujou"][zoning] ?? null;
    }
    if (NEW_TAIPEI_FAR_PCT[district]) {
      const sub = NEW_TAIPEI_FAR_PCT[district][zoning];
      if (sub != null) return sub;
      if (zoning.includes("商")) return NEW_TAIPEI_FAR_PCT[district]["商業區"] ?? null;
      if (zoning.includes("住")) return NEW_TAIPEI_FAR_PCT[district]["住宅區"] ?? null;
      return null;
    }
    if (_NTPC_5.includes(district)) {
      if (zoning.includes("商")) return district === "板橋區" ? 460 : 440;
      if (zoning.includes("住")) return 300;
      return null;
    }
    return TAIPEI_FAR_PCT[zoning] ?? null;
  }

  function lookupShareRatio(price) {
    if (!price) return [null, null];
    if (price <= SHARE_TABLE[0][0]) return [SHARE_TABLE[0][1], SHARE_TABLE[0][2]];
    if (price >= SHARE_TABLE[SHARE_TABLE.length-1][0])
      return [SHARE_TABLE[SHARE_TABLE.length-1][1], SHARE_TABLE[SHARE_TABLE.length-1][2]];
    for (let i = 0; i < SHARE_TABLE.length - 1; i++) {
      const [p1, r1, k1] = SHARE_TABLE[i];
      const [p2, r2, k2] = SHARE_TABLE[i+1];
      if (price >= p1 && price <= p2) {
        const t = (price - p1) / (p2 - p1);
        return [r1 + (r2 - r1) * t, k1 + (k2 - k1) * t];
      }
    }
    return [null, null];
  }

  // 處理特殊 zoning 後綴 (特/遷/核/抄)，對齊 v1 effectiveZoning
  // (特)/(遷) 商業區 → 優先用 zoning_original，否則剝掉後綴
  // (特)/(遷) 住宅區 → 一律剝掉後綴用本身住宅區
  function effectiveZoning(p) {
    const z = p.zoning || '';
    const orig = p.zoning_original || '';
    const hasSpecial = /\((?:特|遷|核|抄)\)/.test(z);
    const base = z.replace(/\((?:特|遷|核|抄)\)/g, '').trim();
    if (hasSpecial && z.includes('商')) {
      if (orig && lookupFar(orig, p) != null) return orig;
      return lookupFar(base, p) != null ? base : z;
    }
    if (hasSpecial && z.includes('住')) {
      return lookupFar(base, p) != null ? base : z;
    }
    if (orig && lookupFar(orig, p) != null) return orig;
    return z;
  }

  // 多分區加權 FAR — 對齊 v1 effectiveFarPctWeighted
  // 注意：單分區走 effectiveZoning(p) 而非 raw p.zoning (處理特殊後綴 case)
  function effectiveFar(p) {
    const zList = p.zoning_list;
    if (zList && zList.length > 1) {
      const ratios = p.zoning_ratios || zList.map(() => 100 / zList.length);
      const total = ratios.reduce((a, b) => a + (Number(b) || 0), 0) || 1;
      let w = 0;
      for (let i = 0; i < zList.length; i++) {
        const z = (typeof zList[i] === 'string') ? zList[i] : (zList[i].original_zone || zList[i].zone_name);
        const f = lookupFar(z, p);
        if (f == null) return null;
        w += f * ((Number(ratios[i]) || 0) / total);
      }
      return Math.round(w);
    }
    return lookupFar(effectiveZoning(p), p);
  }

  // 即時算倍數 — 跟 v1 computeRowMultiples 對齊：
  //   - bonusW (危老) 0.30 / bonusD (都更或防災都更) 台北 1974 前 0.80，否則 0.50
  //   - 回傳 max(危老, 都更) — v2 卡片只顯示一個倍數，取較高那個
  // DB 不存 multiple (CLAUDE.md 規則 8)
  function rowMultiple(p, prices) {
    if (p.is_foreclosure || p.is_remote_area || p.unsuitable_for_renewal || isLandSuspicious(p)) {
      return null;
    }
    const land = Number(p.land_area_ping) || 0;
    const price = p.new_house_price_wan_override ?? prices[p.district];
    const farPct = effectiveFar(p);
    if (!land || !farPct || !price) return null;
    const coeff = p.rebuild_coeff ?? 1.57;
    const [ratio, parking] = lookupShareRatio(price);
    if (!ratio) return null;
    const is1F = Number(p.floor) === 1 || Number(p.floor_range_min) === 1;
    const floorPremium = p.floor_premium ?? (is1F ? 0.20 : 0);
    const effectivePrice = price * (1 + floorPremium);
    // 防災型：台北市 1974 前蓋的，都更獎勵 0.80 (vs 一般 0.50)
    const age = currentAge(p);
    const isFangzai = p.city === '台北市' && age && (new Date().getFullYear() - age) <= 1974;
    const bonusW = p.bonus_weishau ?? 0.30;            // 危老
    const bonusD = p.bonus_dugen ?? (isFangzai ? 0.80 : 0.50);  // 都更/防災型
    const calcVal = (b) => {
      const share = land * (farPct/100) * (1+b) * coeff * ratio;
      return share * effectivePrice + (share / 40) * (parking || 0);
    };
    const listWan = (p.price_ntd || 0) / 10000;
    const desired = p.desired_price_wan ?? (listWan ? Math.round(listWan * 0.9 / 10) * 10 : 0);
    if (!desired) return null;
    const w = calcVal(bonusW) / desired;
    const d = calcVal(bonusD) / desired;
    return Math.max(w, d);   // v2 顯示 max
  }

  // ── Source badges (size 可選 'sm' / 'big') ────────────────────────────
  function srcBadgesHTML(sources, size) {
    if (!sources || !sources.length) return '';
    const bigCls = size === 'big' ? ' v2-src-badge--big' : '';
    return sources.map(s => {
      const name = s.name || '';
      const alive = s.alive !== false;
      const aliveCls = alive ? 'v2-src-badge--alive' : 'v2-src-badge--dead';
      const url = s.url ? esc(s.url) : '';
      const label = size === 'big' ? `${esc(name)} ↗` : esc(name);
      const inner = `<span class="v2-src-badge ${aliveCls}${bigCls}">${label}</span>`;
      return url
        ? `<a href="${url}" target="_blank" rel="noopener" onclick="event.stopPropagation()">${inner}</a>`
        : inner;
    }).join('');
  }

  // ── Render: card grid，分台北/新北兩欄 ────────────────────────────────────
  async function renderGrid() {
    const grid = $('#v2-grid');
    const empty = $('#v2-empty');
    const list = state.filteredSorted;
    const total = list.length;
    $('#v2-result-count').innerHTML = `共 <strong>${total}</strong> 筆`;

    if (total === 0) {
      grid.innerHTML = '';
      empty.style.display = '';
      $('#v2-pagination').innerHTML = '';
      return;
    }
    empty.style.display = 'none';

    const prices = await getDistrictPrices();

    // 先按城市切，每個城市獨立分頁（每頁各取 pageSize 筆，兩邊不互相吃配額）
    const tpeAll = list.filter(p => p.city === '台北市');
    const ntpAll = list.filter(p => p.city === '新北市');
    const otherAll = list.filter(p => p.city !== '台北市' && p.city !== '新北市');

    const start = (state.page - 1) * state.pageSize;
    const end = start + state.pageSize;
    const tpe = tpeAll.slice(start, end);
    const ntp = ntpAll.slice(start, end);
    const other = otherAll.slice(start, end);

    const colHTML = (city, items, totalCount) => {
      if (!items.length) {
        if (!totalCount) return ''; // 完全沒這城市的資料 → 不畫欄
        return `<div class="v2-grid-col" data-city="${esc(city)}">
          <div class="v2-grid-col__title">${esc(city)} <span class="v2-grid-col__count">本頁無</span></div>
        </div>`;
      }
      const pageInfo = totalCount > items.length ? `${start + 1}-${start + items.length} / ${totalCount}` : `${totalCount} 筆`;
      return `<div class="v2-grid-col" data-city="${esc(city)}">
        <div class="v2-grid-col__title">${esc(city)} <span class="v2-grid-col__count">${pageInfo}</span></div>
        <div class="v2-grid-col__cards">${items.map(p => cardHTML(p, prices)).join('')}</div>
      </div>`;
    };

    let html = colHTML('台北市', tpe, tpeAll.length) + colHTML('新北市', ntp, ntpAll.length);
    if (otherAll.length) {
      html += colHTML('其他', other, otherAll.length);
    }
    grid.innerHTML = html;
    // mobile：依 state.gridCity 控制顯示哪一城（CSS 用 [data-active-city] 過濾）
    grid.setAttribute('data-active-city', state.gridCity || '台北市');
    // 分頁基準取兩城市較多那邊（任一城市還有資料就能翻頁）
    renderPagination(Math.max(tpeAll.length, ntpAll.length, otherAll.length));

    // bind clicks
    $$('.v2-card').forEach(el => {
      const id = el.dataset.id;
      el.addEventListener('click', (e) => {
        if (e.target.closest('a')) return;
        if (e.target.closest('.v2-card__star')) return;
        openDetail(id);
      });
    });
    $$('.v2-card__star').forEach(el => {
      el.addEventListener('click', (e) => {
        e.stopPropagation();
        toggleWatchlist(el.dataset.id);
      });
    });
  }

  function cardHTML(p, prices) {
    const id = p.source_id || p.id || '';
    // 用推測地址（LVR 推到巷弄/門牌）優先，591 raw address 只到路段不夠精確
    const addr = p.address_inferred || p.address || '—';
    const priceWan = p.price_ntd ? Math.round(p.price_ntd / 10000) : null;
    const perBld = (p.price_ntd && p.building_area_ping)
      ? (p.price_ntd / 10000 / p.building_area_ping).toFixed(1) : null;
    const mult = rowMultiple(p, prices);
    let multCls = 'v2-card__mult';
    if (mult != null) {
      if (mult >= 3.0) multCls += ' v2-card__mult--good';
      else if (mult >= 2.0) multCls += ' v2-card__mult--mid';
    }
    const advChips = computeAdvantageChips(p);
    const chips = computeChips(p);
    const inWatchlist = !!(p._in_watchlist || p.user_url || p.added_at_user);

    // 第一次抓進 DB 的日期 badge — scrape_session_at 是 batch session 時間 (preserve 不被 reanalyze 重寫，CLAUDE.md PREFER_NEW_FIELDS 註解)
    // 24 小時內顯示紅色 NEW，更舊的顯示「M/D」灰色
    let dateBadge = '';
    if (p.scrape_session_at) {
      const t = new Date(p.scrape_session_at);
      if (!isNaN(t)) {
        const ageMs = Date.now() - t.getTime();
        const within24h = ageMs >= 0 && ageMs < 24 * 3600 * 1000;
        const m = t.getMonth() + 1, d = t.getDate();
        const yy = t.getFullYear();
        const sameYr = yy === new Date().getFullYear();
        const dateStr = sameYr ? `${m}/${d}` : `${yy.toString().slice(2)}/${m}/${d}`;
        dateBadge = within24h
          ? `<span class="v2-card__date v2-card__date--new" title="${t.toLocaleString('zh-TW')}">NEW</span>`
          : `<span class="v2-card__date" title="第一次抓進 DB：${t.toLocaleString('zh-TW')}">${dateStr}</span>`;
      }
    }
    const archivedClass = p.archived ? 'v2-card--archived' : '';
    const readClass = isRead(id) ? 'v2-card--read' : '';
    // 高倍數紅框 (≥3.5x) — mult 上面已算
    const hotClass = (mult != null && mult >= 3.5) ? 'v2-card--hot' : '';
    // 城市色彩區別（左側色條）
    const cityClass = p.city === '台北市' ? 'v2-card--tpe'
                    : p.city === '新北市' ? 'v2-card--ntpc' : '';

    // ── 2-line dense layout ──
    // Line 1: icon + 區·地址 (ellipsis) | 總價 + 建單價 | 倍數 | ⭐
    // Line 2: 建/地/齡/層/區/路 + chips + sources
    return `
      <article class="v2-card ${archivedClass} ${readClass} ${hotClass} ${cityClass}" data-id="${esc(id)}">
        <div class="v2-card__line1">
          <span class="v2-card__type">${typeIcon(p.building_type)}</span>
          <span class="v2-card__addr">
            <span class="v2-card__district">${esc(p.district || '')}</span>·${esc(addr)}
          </span>
          <span class="v2-card__price-block">
            <span class="v2-card__price">${priceWan ? fmt0(priceWan) : '—'}<small>萬</small></span>
            ${(p.lvr_records && p.lvr_records.length) ? `<span class="v2-lvr-icon v2-lvr-icon--sm" title="附近實價登錄" onmouseenter="v2.showLvrPopup(event, '${esc(id)}')" onmouseleave="v2.hideLvrPopup()" onclick="event.stopPropagation()">實</span>` : ''}
            ${perBld ? `<span class="v2-card__price-per">${perBld}/建</span>` : ''}
          </span>
          <span class="${multCls}" title="都更倍數">
            ${mult != null ? mult.toFixed(1) : '—'}<small>×</small>
          </span>
          <button class="v2-card__star ${inWatchlist ? 'v2-card__star--active' : ''}" data-id="${esc(id)}" title="加入觀察清單">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>
            </svg>
          </button>
        </div>
        ${p.title ? `<div class="v2-card__title-sub" title="${esc(p.title)}">${esc(p.title)}</div>` : ''}
        <div class="v2-card__line2">
          <span class="v2-stat" title="建坪"><b>建</b>${fmt1(p.building_area_ping)}</span>
          <span class="v2-stat" title="地坪 (原土地分區縮寫)"><b>地</b>${fmt1(p.land_area_ping)}${(p.zoning_original || p.zoning) ? ` <span class="v2-stat__zone">(${esc(zoneAbbr(p.zoning_original || p.zoning))})</span>` : ''}</span>
          <span class="v2-stat" title="屋齡"><b>齡</b>${p.building_age != null ? p.building_age : '—'}</span>
          <span class="v2-stat" title="樓層"><b>層</b>${formatFloor(p)}</span>
          ${p.road_width_m ? `<span class="v2-stat" title="路寬"><b>路</b>${p.road_width_m}m</span>` : ''}
          ${advChips.length ? advChips.map(c => `<span class="${c.cls}">${c.label}</span>`).join('') : ''}
          ${chips.length ? chips.map(c => `<span class="v2-rchip ${c.cls}">${c.label}</span>`).join('') : ''}
          ${p.sources && p.sources.length ? `<span class="v2-card__sources">${srcBadgesHTML(p.sources)}${dateBadge}</span>` : (dateBadge ? `<span class="v2-card__sources">${dateBadge}</span>` : '')}
        </div>
      </article>`;
  }

  function renderPagination(total) {
    const pages = Math.ceil(total / state.pageSize);
    const cur = state.page;
    if (pages <= 1) { $('#v2-pagination').innerHTML = ''; return; }
    const out = [];
    out.push(`<button class="v2-page-btn" ${cur === 1 ? 'disabled' : ''} onclick="v2.gotoPage(${cur-1})">‹</button>`);
    const pageList = computePageList(cur, pages);
    pageList.forEach(p => {
      if (p === '...') {
        out.push(`<span style="padding:0 6px;color:var(--c-text-light)">…</span>`);
      } else {
        const active = p === cur ? 'v2-page-btn--active' : '';
        out.push(`<button class="v2-page-btn ${active}" onclick="v2.gotoPage(${p})">${p}</button>`);
      }
    });
    out.push(`<button class="v2-page-btn" ${cur === pages ? 'disabled' : ''} onclick="v2.gotoPage(${cur+1})">›</button>`);
    $('#v2-pagination').innerHTML = out.join('');
  }
  function computePageList(cur, pages) {
    if (pages <= 7) return Array.from({length: pages}, (_, i) => i+1);
    const out = [1];
    if (cur > 3) out.push('...');
    for (let p = Math.max(2, cur-1); p <= Math.min(pages-1, cur+1); p++) out.push(p);
    if (cur < pages - 2) out.push('...');
    out.push(pages);
    return out;
  }

  function renderSkeletons(n) {
    const skel = `
      <article class="v2-card v2-skel">
        <div class="v2-card__head">
          <div class="v2-card__title-line"><span class="v2-card__addr">&nbsp;</span></div>
          <div class="v2-card__price-line"><span class="v2-card__price">&nbsp;</span></div>
        </div>
        <div class="v2-card__multi"><span class="v2-card__multi-num">&nbsp;</span></div>
        <div class="v2-card__stats">
          <span class="v2-stat"><span class="v2-stat__value">&nbsp;</span></span>
          <span class="v2-stat"><span class="v2-stat__value">&nbsp;</span></span>
          <span class="v2-stat"><span class="v2-stat__value">&nbsp;</span></span>
        </div>
        <div class="v2-card__footer">&nbsp;</div>
      </article>`;
    $('#v2-grid').innerHTML = skel.repeat(n);
  }

  // ── Districts: load + render chips ──────────────────────────────────────
  // 用 with_counts=true 拿物件數量，前端隱藏「沒資料的區」
  async function loadDistricts() {
    try {
      const r = await fetch('/api/target_regions?with_counts=true');
      const data = await r.json();
      // 兼容兩種 response：with_counts 時是 {regions, counts}；否則是純 regions
      if (data && data.regions) {
        state.targetRegions = data.regions;
        state.districtCounts = data.counts || {};
      } else {
        state.targetRegions = data;
        state.districtCounts = {};
      }
      renderDistrictChips();
    } catch (e) { console.warn('target_regions failed', e); }
  }
  function renderDistrictChips() {
    // 對齊 v1 enabled districts；layout 用「台北左欄 / 新北右欄」並排
    // disabled districts 不顯示
    const host = $('#v2-district-chips');
    if (!host) return;
    const counts = state.districtCounts || {};

    const cityCol = (city) => {
      const cfg = V1_DISTRICTS[city];
      if (!cfg) return '';
      const allChecked = cfg.enabled.every(d => state.districtPicks.has(`${city}|${d}`));
      const chipsHtml = cfg.enabled.map(d => {
        const key = `${city}|${d}`;
        const checked = state.districtPicks.has(key) ? 'checked' : '';
        const label = cfg.labels[d] || d;
        return `<label class="v2-chip">
          <input type="checkbox" data-city="${esc(city)}" data-district="${esc(d)}" ${checked} onchange="v2.toggleDistrict('${esc(city)}','${esc(d)}', this.checked)">
          <span>${esc(label)}</span>
        </label>`;
      }).join('');
      const cityShort = city.replace('市', '');
      return `<div class="v2-city-col" data-city="${esc(city)}">
        <div class="v2-city-col__title">
          ${esc(cityShort)}
          <label class="v2-city-all" title="全部 ${esc(cityShort)}">
            <input type="checkbox" ${allChecked ? 'checked' : ''} onchange="v2.toggleAllInCity('${esc(city)}', this.checked)">
            <span>全部</span>
          </label>
        </div>
        <div class="v2-city-col__chips">${chipsHtml}</div>
      </div>`;
    };

    host.innerHTML = `<div class="v2-city-grid">${cityCol('台北市')}${cityCol('新北市')}</div>`;
  }

  // 一鍵全選/取消當前城市的 enabled districts（對齊 v1 toggleAllDists）
  function toggleAllInCity(city, on) {
    const cfg = V1_DISTRICTS[city];
    if (!cfg) return;
    cfg.enabled.forEach(d => {
      const key = `${city}|${d}`;
      if (on) state.districtPicks.add(key);
      else state.districtPicks.delete(key);
    });
    renderDistrictChips();
    applyFilters();
  }

  // ── Filter + sort ────────────────────────────────────────────────────────
  // ── Filter + Sort 統一 pipeline ─────────────────────────────────────────
  // 任何 filter/排序 input 變動都呼叫 applyFilters()。
  //   1. 從 state.allProperties (raw) 起跑
  //   2. 套用 view-specific filter (explore tab 才套 sidebar filter；watchlist 不套)
  //   3. 套 min-mult filter
  //   4. 排序
  //   5. 寫進 state.filteredSorted + render
  // 整條 pipeline idempotent，每次都從 raw 起跑 — 不會「第一次有效第二次無效」的累積 bug
  async function applyFilters() {
    if (state.view === 'explore') _saveFilters();   // 自動存 explore filter 偏好
    const prices = await getDistrictPrices();
    let list = state.allProperties.filter(p =>
      !p.deleted && !p.analysis_error && !p.analysis_in_progress && p.archived !== true
    );

    // 探索 tab 才套 sidebar filter；最愛 tab 不套 (用戶明示)
    if (state.view === 'explore') {
      list = _applyExploreSidebarFilters(list);
    }

    // min-mult filter (跟 sidebar 同階；最愛 tab 也不套，因為 input 在 sidebar)
    if (state.view === 'explore') {
      const minMultOn = $('#v2-min-mult-on')?.checked;
      const minMultVal = parseFloat($('#v2-min-mult-val')?.value);
      if (minMultOn && !isNaN(minMultVal) && minMultVal > 0) {
        list = list.filter(p => {
          const m = rowMultiple(p, prices);
          return m != null && m >= minMultVal;
        });
      }
    }

    // 排序 (兩個 view 都套；watchlist 走 v2-sort 的選擇 — 預設 list_rank)
    list = _sortList(list, prices);

    state.filteredSorted = list;
    state.page = 1;
    renderGrid();
  }

  // sidebar filter 一律從 list 起跑 → 不會有累積 bug
  function _applyExploreSidebarFilters(list) {
    // district picks
    if (state.districtPicks.size > 0) {
      list = list.filter(p => state.districtPicks.has(`${p.city}|${p.district}`));
    }
    // building_type
    const btypePicks = $$('.v2-filter-btype:not(:disabled)').filter(c => c.checked).map(c => c.value);
    const btypeAll = $$('.v2-filter-btype:not(:disabled)').length;
    if (btypePicks.length > 0 && btypePicks.length < btypeAll) {
      list = list.filter(p => btypePicks.includes(p.building_type));
    }
    // road
    const road = ($('#v2-road')?.value || '').trim();
    if (road) {
      list = list.filter(p => (p.address || '').includes(road));
    }
    // price range
    const pmin = Number($('#v2-price-min')?.value) || 0;
    const pmax = Number($('#v2-price-max')?.value) || Infinity;
    list = list.filter(p => {
      const w = (p.price_ntd || 0) / 10000;
      return w >= pmin && w <= pmax;
    });
    // floor chips
    const floorInputs = $$('#v2-floor-chips input[data-floor]');
    const floorPicks = floorInputs.filter(c => c.checked).map(c => c.value);
    if (floorPicks.length > 0 && floorPicks.length < floorInputs.length) {
      const wantsBasement = floorPicks.includes('B');
      const intPicks = floorPicks.filter(v => v !== 'B');
      list = list.filter(p => {
        if (p.is_basement) return wantsBasement;
        if (intPicks.length === 0) return false;
        const fmin = p.floor_range_min, fmax = p.floor_range_max;
        if (fmin != null && fmax != null) {
          return intPicks.some(v => fmin <= +v && +v <= fmax);
        }
        const f = p.floor;
        if (f == null) return true;
        return intPicks.includes(String(f));
      });
    }
    // bld / land single price
    const bldMax = Number($('#v2-bld-price-max')?.value);
    if (bldMax > 0) {
      list = list.filter(p => !p.price_ntd || !p.building_area_ping
        || (p.price_ntd / 10000 / p.building_area_ping) < bldMax);
    }
    const landMax = Number($('#v2-land-price-max')?.value);
    if (landMax > 0) {
      list = list.filter(p => !p.price_ntd || !p.land_area_ping
        || (p.price_ntd / 10000 / p.land_area_ping) < landMax);
    }
    const landMin = Number($('#v2-land-min')?.value) || 0;
    if (landMin > 0) {
      list = list.filter(p => (p.land_area_ping || 0) >= landMin);
    }
    // resistance
    if ($('#v2-hide-floors5plus')?.checked) {
      list = list.filter(p => !(p.total_floors >= 5 && p.building_type !== '透天厝'));
    }
    if ($('#v2-hide-remote')?.checked) list = list.filter(p => !p.is_remote_area);
    if ($('#v2-hide-unsuitable')?.checked) list = list.filter(p => !p.unsuitable_for_renewal);
    if ($('#v2-hide-basement')?.checked) list = list.filter(p => !p.is_basement);
    if ($('#v2-hide-foreclosure')?.checked) list = list.filter(p => !p.is_foreclosure);
    return list;
  }

  // 排序：list_rank / last_change_at / published_at / profit_multiple / price_per_*
  function _sortList(list, prices) {
    const mode = $('#v2-sort')?.value || 'list_rank';
    const reverse = state.sortDir === 'desc';
    list = list.slice();   // 不 mutate input
    if (mode === 'list_rank') {
      list.sort((a, b) => (a.list_rank ?? 9999) - (b.list_rank ?? 9999));
      list.sort((a, b) => (b.scrape_session_at || '').localeCompare(a.scrape_session_at || ''));
      return list;
    }
    const valOf = (p) => {
      switch (mode) {
        case 'last_change_at': return p.last_change_at || p.scrape_session_at || p.scraped_at || '';
        case 'published_at': return p.published_at || p.scraped_at || '';
        case 'profit_multiple': return rowMultiple(p, prices);
        case 'price_per_building_ping':
          return (p.price_ntd && p.building_area_ping) ? (p.price_ntd / 10000 / p.building_area_ping) : null;
        case 'price_per_land_ping':
          return (p.price_ntd && p.land_area_ping) ? (p.price_ntd / 10000 / p.land_area_ping) : null;
        case 'price_ntd': return p.price_ntd;
        case 'building_age': return p.building_age;
      }
      return null;
    };
    const has = list.filter(p => valOf(p) != null);
    const noVal = list.filter(p => valOf(p) == null);
    has.sort((a, b) => {
      const va = valOf(a), vb = valOf(b);
      if (typeof va === 'string') return reverse ? vb.localeCompare(va) : va.localeCompare(vb);
      return reverse ? (vb - va) : (va - vb);
    });
    return has.concat(noVal);
  }

  // 兼容名稱：讓既有 inline onchange="v2.applySort()" 不爆 (轉跑 applyFilters)
  const applySort = applyFilters;

  function toggleSortDir() {
    state.sortDir = state.sortDir === 'desc' ? 'asc' : 'desc';
    const btn = $('#v2-sort-dir');
    if (btn) btn.textContent = state.sortDir === 'desc' ? '↓' : '↑';
    applyFilters();
  }

  // Mobile-only city tab 切換 (城市 cards 並排在窄螢幕擠不下時切換顯示)
  function switchGridCity(city) {
    state.gridCity = city;
    const grid = $('#v2-grid');
    if (grid) grid.setAttribute('data-active-city', city);
    $$('.v2-grid-toggle__pill').forEach(p => {
      p.classList.toggle('v2-grid-toggle__pill--active', p.dataset.city === city);
    });
  }

  // ── Detail drawer ────────────────────────────────────────────────────────
  async function openDetail(id) {
    state.selectedId = id;
    markRead(id);
    const card = document.querySelector(`.v2-card[data-id="${CSS.escape(id)}"]`);
    if (card) card.classList.add('v2-card--read');

    // STEP 1: 立刻開抽屜 (無條件)，避免任何 await/exception 卡住
    $('#v2-drawer-title').textContent = '載入中…';
    $('#v2-drawer-body').innerHTML = `<div class="v2-drawer__loading">載入中…</div>`;
    $('#v2-drawer').classList.add('v2-open');
    $('#v2-drawer-backdrop').classList.add('v2-open');

    const slim = state.allProperties.find(x => (x.source_id || x.id) === id);
    if (!slim) {
      $('#v2-drawer-body').innerHTML = `<div class="v2-detail-empty">找不到物件 ${esc(id)}</div>`;
      return;
    }

    // STEP 2: 用 slim 試渲染 (try/catch 出錯顯錯誤)
    try {
      const prices = await getDistrictPrices();
      $('#v2-drawer-title').textContent = slim.address_inferred || slim.address || '物件詳情';
      $('#v2-drawer-body').innerHTML = detailHTML(slim, prices);
    } catch (e) {
      console.error('detailHTML render failed (slim):', e);
      $('#v2-drawer-body').innerHTML = `<div class="v2-detail-empty">渲染失敗：${esc(String(e.message || e))}</div>`;
    }

    // STEP 3: 背景升級 full doc (含 ai_reason 等 fat 欄位)，失敗也不影響 slim 顯示
    try {
      const r = await fetch(`/api/properties/${encodeURIComponent(id)}`);
      if (!r.ok) return;
      const full = await r.json();
      if (state.selectedId !== id) return;
      const idx = state.allProperties.findIndex(x => (x.source_id || x.id) === id);
      if (idx >= 0) {
        state.allProperties[idx] = { ...state.allProperties[idx], ...full };
      }
      const prices2 = await getDistrictPrices();
      $('#v2-drawer-title').textContent = full.address_inferred || full.address || '物件詳情';
      $('#v2-drawer-body').innerHTML = detailHTML(state.allProperties[idx] || full, prices2);
    } catch (e) {
      console.warn('openDetail upgrade fetch failed', e);
    }
  }
  function closeDetail() {
    // 對齊 v1：關閉時若 ephemeral edit + 不在 watchlist → toast 提示沒儲存
    const id = state.selectedId;
    if (id) {
      const p = state.allProperties.find(x => (x.source_id || x.id) === id);
      if (p && p._ephemeral_edit_made && !p._in_watchlist) {
        toast('您剛才的數字改動沒有自動儲存。請先把本物件加入最愛，之後改動會自動儲存。', 'error');
      }
      if (p) p._ephemeral_edit_made = false;
    }
    $('#v2-drawer').classList.remove('v2-open');
    $('#v2-drawer-backdrop').classList.remove('v2-open');
    state.selectedId = null;
  }

  function detailHTML(p, prices) {
    const id = p.source_id || p.id || '';
    const priceWan = p.price_ntd ? Math.round(p.price_ntd / 10000) : null;
    const desired = p.desired_price_wan ?? (priceWan ? Math.round(priceWan * 0.9 / 10) * 10 : null);
    const newPrice = p.new_house_price_wan_override ?? prices[p.district];
    const farPct = effectiveFar(p);
    const perBld = (p.price_ntd && p.building_area_ping)
      ? (p.price_ntd / 10000 / p.building_area_ping).toFixed(1) : null;
    const perLand = (p.price_ntd && p.land_area_ping)
      ? (p.price_ntd / 10000 / p.land_area_ping).toFixed(1) : null;
    const age = currentAge(p);

    // ── 試算各情境 ──
    const land = Number(p.land_area_ping) || 0;
    const isLandSus = isLandSuspicious(p);
    const skip = !!(p.is_foreclosure || p.is_remote_area || p.unsuitable_for_renewal || isLandSus);
    const coeff = p.rebuild_coeff ?? 1.57;
    const [ratio, parking] = lookupShareRatio(newPrice);
    const is1F = Number(p.floor) === 1 || Number(p.floor_range_min) === 1;
    const floorPremium = p.floor_premium ?? (is1F ? 0.20 : 0);
    const effectivePrice = (newPrice || 0) * (1 + floorPremium);
    const isFangzai = p.city === '台北市' && age && (new Date().getFullYear() - age) <= 1974;
    const bonusW = p.bonus_weishau ?? 0.30;
    const bonusD = p.bonus_dugen ?? (isFangzai ? 0.80 : 0.50);
    const calcScene = (b) => {
      if (skip || !land || !farPct || !ratio) return null;
      const share = land * (farPct/100) * (1+b) * coeff * (ratio || 0);
      const total = share * effectivePrice + (share / 40) * (parking || 0);
      return { share, total, mult: desired ? total / desired : null };
    };
    const sW = calcScene(bonusW);
    const sD = calcScene(bonusD);
    const scnHTML = (label, s, b) => {
      if (!s) {
        return `<div class="v2-scn"><div class="v2-scn__name">${label}</div>
          <div class="v2-scn__mult v2-scn__mult--na">—</div>
          <div class="v2-scn__detail">${skip ? (p.is_foreclosure?'法拍跳過':p.is_remote_area?'偏遠跳過':p.unsuitable_for_renewal?'特殊土地':isLandSus?'地坪可疑':'資料不足') : '資料不足'}</div></div>`;
      }
      const cls = s.mult >= 3.0 ? 'v2-scn__mult--good' : s.mult >= 2.0 ? 'v2-scn__mult--mid' : '';
      return `<div class="v2-scn"><div class="v2-scn__name">${label} <small>+${(b*100)|0}%</small></div>
        <div class="v2-scn__mult ${cls}">${s.mult ? s.mult.toFixed(2) : '—'}<small>×</small></div>
        <div class="v2-scn__detail">分回 ${s.share.toFixed(1)} 坪 ／ 估值 ${fmt0(s.total)} 萬</div></div>`;
    };

    // ── 圖片 ──
    const img = p.image_url
      ? `<div class="v2-detail-image-wrap"><img class="v2-detail-image" src="${esc(p.image_url)}" alt=""></div>`
      : '';

    // ── 推測地址候選清單 ── (badge 放下一行)
    const cands = Array.isArray(p.address_inferred_candidates_detail) ? p.address_inferred_candidates_detail : [];
    const inferredTag = p.address_inferred ? (
      p.address_inferred_confidence === 'unique' ? '<div class="v2-inferred-tag">★實登</div>' :
      p.address_inferred_confidence === 'multi' ? '<div class="v2-inferred-tag">推測</div>' :
      '<div class="v2-inferred-tag">≈推測</div>'
    ) : '';

    // ── 附近捷運 ── 一個一行
    const mrtList = Array.isArray(p.nearby_mrts) && p.nearby_mrts.length
      ? p.nearby_mrts.map(m => `<div class="v2-mrt-line">${esc(m.name)}（${Math.round(m.dist_m)}m）</div>`).join('')
      : '—';

    // ── 用戶可改欄位 (對齊 v1 行為：永遠 editable，save 時若不在 watchlist
    //    設 _ephemeral_edit_made flag，closeDetail 時提示) ──
    const editIn = (field, val, step, suffix) =>
      `<input type="number" class="v2-d-input" min="0" step="${step}" value="${val ?? ''}"
        onchange="v2.saveOverride('${esc(id)}','${field}',this.value)">${suffix ? `<span class="v2-d-hint"> ${suffix}</span>` : ''}`;
    const editPct = (field, val) =>
      `<input type="number" class="v2-d-input" min="0" max="100" step="5" value="${val != null ? Math.round(val*100) : ''}"
        onchange="v2.saveOverride('${esc(id)}','${field}',this.value/100)">% `;

    // ── LVR 實價登錄 (前 5 筆) ──
    const lvrRecs = Array.isArray(p.lvr_records) ? p.lvr_records.slice(0, 5) : [];
    const lvrHTML = lvrRecs.length
      ? `<table class="v2-lvr-tbl"><thead><tr><th>交易日</th><th>總價(萬)</th><th>建坪</th><th>單價</th><th>地址</th></tr></thead><tbody>${
          lvrRecs.map(r => `<tr>
            <td>${esc(r.txn_date || '—')}</td>
            <td>${r.price_total ? fmt0(r.price_total) : '—'}</td>
            <td>${r.area_ping ? fmt1(r.area_ping) : '—'}</td>
            <td>${(r.price_total && r.area_ping) ? (r.price_total / r.area_ping).toFixed(1) : '—'}</td>
            <td title="${esc(r.address || '')}">${esc((r.address || '').slice(0, 14))}</td>
          </tr>`).join('')}</tbody></table>`
      : '<div class="v2-detail-empty">無實價登錄記錄</div>';

    // ── AI 分析 ──
    const aiText = p.ai_reason || p.ai_analysis || '';

    // ── 政府連結 ──
    const govLinks = p.city === '台北市'
      ? `<a class="v2-govlink" href="https://bim.udd.gov.taipei/UDDPlanMap/" target="_blank" rel="noopener">都市計畫圖 ↗</a>
         <a class="v2-govlink" href="https://zonemap.udd.gov.taipei/ZoneMapOP/" target="_blank" rel="noopener">地籍套繪圖 ↗</a>`
      : p.city === '新北市'
        ? `<a class="v2-govlink" href="https://urban.planning.ntpc.gov.tw/NtpcURInfo/" target="_blank" rel="noopener">城鄉資訊 ↗</a>`
        : '';

    // LVR 「實」icon (v1 行為：hover 顯示彈窗)
    const lvrIcon = lvrRecs.length
      ? `<span class="v2-lvr-icon" onmouseenter="v2.showLvrPopup(event, '${esc(id)}')" onmouseleave="v2.hideLvrPopup()" onclick="event.stopPropagation()">實</span>`
      : '';

    // 優勢 / 抗性 chip — 跟首頁卡片同款 (cardHTML 已 compute 過，detail 自己再算一次)
    const _adv = computeAdvantageChips(p);
    const _resist = computeChips(p);
    const advChipsHTML = _adv.length
      ? _adv.map(c => `<span class="${c.cls}">${c.label}</span>`).join(' ')
      : '<span class="v2-d-hint">—</span>';
    const resistChipsHTML = _resist.length
      ? _resist.map(c => `<span class="v2-rchip ${c.cls}">${c.label}</span>`).join(' ')
      : '<span class="v2-d-hint">—</span>';

    return `
      <!-- Header bar：物件資訊標題 + 來源連結 -->
      <div class="v2-d-header">
        <h5 class="v2-d-title">物件資訊</h5>
        ${p.sources && p.sources.length ? `<div class="v2-d-sources-bar">${srcBadgesHTML(p.sources, 'big')}</div>` : ''}
      </div>

      <!-- Row 1: 物件資訊 (左 7) | 圖片 (右 4) -->
      <div class="v2-d-row">
        <div class="v2-d-col v2-d-col--7">
          <div class="v2-d-basic-grid">
            <table class="v2-d-tbl">
              <tr><td>原始地址</td><td>${esc(p.address || p.title || '—')}</td></tr>
              <tr><td>推測地址</td><td>${inferredAddressCellHTML(p)}${inferredTag}</td></tr>
              <tr><td>類型 / 樓層</td><td>${esc(p.building_type || '—')} · ${formatFloor(p)}</td></tr>
              <tr><td>屋齡</td><td>${age != null ? age + ' 年' : '未知'}${p.building_age_completed_year ? ` <span class="v2-d-hint">(${p.building_age_completed_year} 完工)</span>` : ''}</td></tr>
              <tr><td>售價</td><td><span class="v2-d-price">${priceWan ? fmt0(priceWan) + '萬' : '—'}</span>${lvrIcon}</td></tr>
              <tr><td>欲出價</td><td>${editIn('desired_price_wan', desired, 10, '萬')}</td></tr>
              <tr><td>建坪</td><td>${p.building_area_ping ? p.building_area_ping + ' 坪' : '—'}${perBld ? `<div class="v2-d-hint">${perBld} 萬/建坪</div>` : ''}</td></tr>
              <tr><td>地坪</td><td>${p.land_area_ping ? p.land_area_ping + ' 坪' : '—'}${perLand ? `<div class="v2-d-hint">${perLand} 萬/地坪</div>` : ''}${p.land_area_source === 'lvr' ? ' <span class="v2-d-hint">(實登)</span>' : ''}${isLandSus ? '<div class="v2-d-warn">⚠ 地坪過大（&gt; 建坪），可能不可信</div>' : ''}</td></tr>
            </table>
            <table class="v2-d-tbl">
              <tr><td>附近捷運</td><td>${mrtList}</td></tr>
              <tr><td>使用分區</td><td>${zoningCellHTML(p)}</td></tr>
              <tr><td>容積率</td><td>${farPct ? farPct + '%' : '—'}</td></tr>
              <tr><td>臨路寬度</td><td>${editIn('road_width_m_override', p.road_width_m_override ?? p.road_width_m, 0.5, 'm')}${p.road_width_unknown ? ' <span class="v2-d-warn-inline">(寬度不明)</span>' : ''}${p.screenshot_roadwidth ? ` <a href="${esc(p.screenshot_roadwidth)}" target="_blank" rel="noopener" class="v2-d-screenshot-link">📷 地籍圖 ↗</a>` : ''}</td></tr>
              <tr><td>優勢</td><td class="v2-d-chips-cell">${advChipsHTML}</td></tr>
              <tr><td>抗性</td><td class="v2-d-chips-cell">${resistChipsHTML}</td></tr>
            </table>
          </div>
          ${govLinks ? `<div class="v2-d-govlinks">${govLinks}</div>` : ''}
        </div>
        <div class="v2-d-col v2-d-col--5">
          ${img || '<div class="v2-detail-image-wrap"><div class="v2-detail-image-empty">無照片</div></div>'}
        </div>
      </div>

      <!-- Row 2: 都更換回試算 (左 7) | 分析建議 (右 5) — 直式公式對齊 v1 -->
      <div class="v2-d-row">
        <div class="v2-d-col v2-d-col--7">
          <h6 class="v2-d-h">都更換回試算</h6>
          ${renewalSectionHTML(p, prices)}
        </div>
        <div class="v2-d-col v2-d-col--5">
          <h6 class="v2-d-h">分析建議</h6>
          ${aiText ? `<div class="v2-d-ai-text">${renderAiText(aiText, p, prices)}</div>` : '<div class="v2-detail-empty">尚無分析建議</div>'}
        </div>
      </div>

      <!-- Row 3: 操作 -->
      <div class="v2-d-actions">
        <button class="v2-btn v2-btn--ghost v2-btn--sm" onclick="window.open('/?focus=${esc(id)}', '_blank')">在舊版開啟</button>
        <button class="v2-btn v2-btn--primary v2-btn--sm" onclick="v2.toggleWatchlist('${esc(id)}')">
          ${p._in_watchlist ? '從最愛移除' : '加入最愛'}
        </button>
      </div>
    `;
  }

  // ── 使用分區 cell (對齊 v1 zoningCellHTML — 多分區編輯 + (特)/(遷) 警示 + 候選展開) ──
  function zoningCellHTML(p) {
    const z = p.zoning;
    const cands = p.zoning_candidates || [];
    if (!z && !cands.length) return '<span class="v2-d-hint">待查</span>';

    const sourceLabel = {
      'arcgis_taipei': '北市都市計畫 GeoServer',
      'arcgis_newtaipei': '新北市 GeoServer',
      'not_found': 'GeoServer 查無相符多邊形',
      'no_coord': '缺座標，無法查詢',
      'unsupported_city': '城市暫未支援（請手動）',
      '5168': '5168 實價登錄',
      'tcd_via_5168': '北市地籍套繪圖（5168）',
      'tcd_via_reverse_geo': '北市地籍套繪圖（反查）',
      'tcd_vision_failed': '舊版 OCR 失敗',
      'coord_mismatch': '座標與地址不匹配',
      'lookup_failed': '查詢失敗',
    }[p.zoning_source] || p.zoning_source || '';
    const srcLink = p.zoning_source_url
      ? `<a href="${encodeURI(p.zoning_source_url)}" target="_blank" rel="noopener" class="v2-d-zone-srclink">↗</a>`
      : '';
    const errorLine = p.zoning_error
      ? `<div class="v2-d-zone-error">${esc(p.zoning_error)}</div>` : '';

    const orig = p.zoning_original;
    const zoneList = p.zoning_list;
    const id = p.source_id || p.id || '';
    let badge;
    if (zoneList && zoneList.length > 1) {
      const locked = !!p.zoning_ratios_locked;
      const n = zoneList.length;
      const totalLand = Number(p.land_area_ping) || 0;
      const ratios = p.zoning_ratios || zoneList.map(() => 100 / n);
      const toPing = (r) => totalLand > 0 ? (totalLand * (Number(r) || 0) / 100) : 0;
      badge = zoneList.map((zl, i) => {
        const eff = (typeof zl === 'string') ? zl : (zl.original_zone || zl.zone_name);
        const far = lookupFar(eff, p) ?? '?';
        const v = toPing(ratios[i]).toFixed(2);
        const dis = locked ? 'disabled' : '';
        return `<span class="v2-d-zone-badge">${esc(eff)} (${far}%)</span>
          <input type="number" class="v2-d-zone-ping" min="0" max="${totalLand}" step="0.01" value="${v}" ${dis}
            onchange="v2.setZonePing('${esc(id)}', ${i}, this.value)">坪`;
      }).join(' / ');
      badge += `<div class="v2-d-zone-err" id="v2-zone-err-${esc(id)}" style="display:none"></div>`;
      badge += locked
        ? `<div class="v2-d-zone-note">依謄本登錄坪數鎖定（總 ${totalLand} 坪）</div>`
        : `<div class="v2-d-zone-note">總土地 ${totalLand} 坪。請依實際坪數輸入（任一改動，其他自動同步）</div>`;
    } else if (z) {
      badge = `<span class="v2-d-zone-badge">${esc(z)}</span>`;
      if (orig && orig !== z) {
        badge += ` <span class="v2-d-zone-orig">原：${esc(orig)}</span>`;
      }
    } else {
      badge = '<span class="v2-d-hint">—</span>';
    }
    if (z && /\((?:特|遷|核|抄)\)/.test(z)) {
      const eff = effectiveZoning(p);
      const effFar = lookupFar(eff, p);
      if (effFar != null && eff !== z) {
        badge += `<div class="v2-d-zone-special">實際容積採「${esc(eff)}」${effFar}% 計算。此地塊有(特)/(遷)加註，真實容積請查都發局都市計畫書。</div>`;
      } else {
        badge += `<div class="v2-d-zone-special">此地塊有(特)/(遷)加註，容積率逐案而定，請查都發局都市計畫書。</div>`;
      }
    }
    const candsBlock = cands.length
      ? `<details class="v2-d-zone-cands">
          <summary>展開 ${cands.length} 個候選</summary>
          <table class="v2-d-zone-cands-tbl">
            ${cands.map(c => `
              <tr class="${c.is_most_likely ? 'v2-d-zone-cands-tbl__top' : ''}">
                <td>${c.is_most_likely ? '★ ' : ''}${esc(c.address || '')}</td>
                <td>${esc(c.zoning || '—')}</td>
                <td>${c.distance_m != null ? c.distance_m + ' m' : '—'}</td>
              </tr>`).join('')}
          </table>
        </details>` : '';
    const sourceHint = sourceLabel
      ? `<div class="v2-d-hint v2-d-zone-source">來源：${esc(sourceLabel)}${srcLink}</div>` : '';
    return `${badge}${errorLine}${sourceHint}${candsBlock}`;
  }

  // 多分區 改坪數 → 同步調整其他區 + POST zoning_ratios (對齊 v1 setZonePing)
  function setZonePing(id, idx, val) {
    const p = state.allProperties.find(x => (x.source_id || x.id) === id);
    if (!p || !p.zoning_list || p.zoning_ratios_locked) return;
    const total = Number(p.land_area_ping) || 0;
    if (total <= 0) return;
    const n = p.zoning_list.length;
    let v = parseFloat(val);
    if (isNaN(v) || v < 0) v = 0;

    const errEl = document.getElementById(`v2-zone-err-${id}`);
    if (v > total) {
      if (errEl) {
        errEl.textContent = `⚠ 單一分區坪數 ${v.toFixed(2)} 超過總土地 ${total} 坪`;
        errEl.style.display = '';
      }
      return;
    }
    if (errEl) { errEl.textContent = ''; errEl.style.display = 'none'; }

    const pings = p.zoning_list.map((_, i) => {
      const r = (p.zoning_ratios || p.zoning_list.map(() => 100 / n))[i];
      return total * (Number(r) || 0) / 100;
    });
    pings[idx] = v;
    if (n === 2) {
      pings[1 - idx] = total - v;
    } else {
      const rest = total - v;
      const otherSum = pings.reduce((a, b, i) => i === idx ? a : a + b, 0);
      if (otherSum > 0) {
        for (let i = 0; i < n; i++) if (i !== idx) pings[i] = pings[i] / otherSum * rest;
      } else {
        for (let i = 0; i < n; i++) if (i !== idx) pings[i] = rest / (n - 1);
      }
    }
    const ratios = pings.map(pp => total > 0 ? (pp / total) * 100 : 0);
    p.zoning_ratios = ratios;
    if (!p._in_watchlist) p._ephemeral_edit_made = true;
    document.querySelectorAll('.v2-d-zone-ping').forEach((el, i) => {
      if (i !== idx && i < pings.length) el.value = pings[i].toFixed(2);
    });
    applyFilters();
    _renderDetailFromCurrent();
    fetch(`/api/properties/${encodeURIComponent(id)}/zoning_ratios`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ zoning_ratios: ratios }),
    }).catch(e => console.error('setZonePing', e));
  }

  // ── 推測地址 helper (對齊 v1 inferredAddressCellHTML + saveInferredChoice) ──
  // 多候選時 → <select>；用戶切換 → POST inferred_choice → 後端 swap address+land_ping
  function stripCityDist(addr) {
    if (!addr) return '';
    return String(addr)
      .replace(/^(?:臺北市|台北市|新北市|桃園市|台中市|臺中市|高雄市|台南市|臺南市|基隆市|新竹市|新竹縣)/, '')
      .replace(/^[一-龥]{1,3}(?:區|鄉|鎮|市)/, '')
      .trim();
  }
  function fullAddress(p) {
    const base = p.address_inferred || p.address || '';
    if (!base) return '';
    if (/^(?:臺北市|台北市|新北市)/.test(base)) return base;
    return (p.city || '') + (p.district || '') + base;
  }
  function inferredAddressCellHTML(p) {
    const cands = Array.isArray(p.address_inferred_candidates_detail) ? p.address_inferred_candidates_detail : [];
    const current = p.address_inferred || p.address || p.title || '';
    const mapLink = `<a href="https://www.google.com/maps/search/${encodeURIComponent(fullAddress(p))}" target="_blank" rel="noopener" class="v2-d-map-link" title="Google Maps">📍</a>`;
    if (cands.length <= 1) {
      return `${esc(stripCityDist(current))} ${mapLink}`;
    }
    const opts = cands.map(c => {
      const sel = c.address === current ? 'selected' : '';
      const label = stripCityDist(c.address) + (c.is_reverse_geo ? '（座標反查）' : '');
      return `<option value="${esc(c.address)}" ${sel}>${esc(label)}</option>`;
    }).join('');
    return `<select class="v2-d-input v2-d-inferred-select" onchange="v2.saveInferredChoice('${esc(p.source_id || p.id || '')}', this.value)">${opts}</select> ${mapLink}`;
  }
  async function saveInferredChoice(id, address) {
    try {
      const r = await fetch(`/api/properties/${encodeURIComponent(id)}/inferred_choice`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ address }),
      });
      if (!r.ok) { toast('儲存失敗', 'error'); return; }
      const data = await r.json();
      const idx = state.allProperties.findIndex(x => (x.source_id || x.id) === id);
      if (idx >= 0) {
        const patch = { address_inferred: address };
        if (data.land_ping != null) {
          patch.land_area_ping = data.land_ping;
          patch.land_area_sqm = Math.round(data.land_ping * 3.30578 * 100) / 100;
        } else {
          patch.land_area_ping = null;
          patch.land_area_sqm = null;
        }
        Object.assign(state.allProperties[idx], patch);
      }
      // 重新 render detail (含試算 — land 變了倍數要重算) + 重 applyFilters 讓首頁卡片更新
      _renderDetailFromCurrent();
      applyFilters();
      toast('已儲存', 'success');
    } catch (e) {
      console.error('saveInferredChoice', e);
      toast('儲存失敗：' + e.message, 'error');
    }
  }

  // ── 都更換回試算 visual (對齊 v1 renewalV2HTML 直式公式 + 結果 col) ─────
  // 結構：[土地持分大塊 + 分區abbr]  [直式公式 (×/× ... +)]
  //                                  [危老 col: 總值 + 分回坪 + 倍數 + 損益]
  //                                  [都更 col: ...]
  function renewalSectionHTML(p, prices) {
    if (p.is_foreclosure || p.is_remote_area || p.unsuitable_for_renewal || isLandSuspicious(p)) {
      const reason = p.is_foreclosure ? '法拍屋'
        : p.is_remote_area ? '新北偏遠路段'
        : p.unsuitable_for_renewal ? '特殊土地分區（非住商工）'
        : '土地坪數可能不可信（大於建坪）';
      return `<div class="v2-d-alert">此物件標記為「${reason}」，都更倍數試算不適用，故不顯示。</div>`;
    }
    const id = p.source_id || p.id || '';
    const land = p.land_area_ping;
    const zoning = effectiveZoning(p);
    const zoneList = p.zoning_list;
    const multiZone = zoneList && zoneList.length > 1;
    const baseFar = multiZone ? effectiveFar(p) : lookupFar(zoning, p);
    const effFar = baseFar;   // 用戶設計不實作路寬限縮
    const coeff = p.rebuild_coeff ?? 1.57;
    const newPrice = p.new_house_price_wan_override ?? prices[p.district];
    if (!land || !zoning || !newPrice) {
      const missing = [
        !land ? '土地坪數' : null,
        !zoning ? '使用分區' : null,
        !newPrice ? '新成屋房價' : null,
      ].filter(Boolean).join(' / ');
      return `<div class="v2-d-alert">⚠ 缺資料：${esc(missing)}，無法試算。</div>`;
    }
    const [ratio, parking] = lookupShareRatio(newPrice);
    const isFangzai = p.city === '台北市' && currentAge(p) && (new Date().getFullYear() - currentAge(p)) <= 1974;
    const bonusW = p.bonus_weishau ?? 0.30;
    const bonusD = p.bonus_dugen ?? (isFangzai ? 0.80 : 0.50);
    const is1F = Number(p.floor) === 1 || Number(p.floor_range_min) === 1;
    const floorPremium = p.floor_premium ?? (is1F ? 0.20 : 0);
    const effectivePrice = newPrice * (1 + floorPremium);
    const calcShare = (b) => land * (effFar / 100) * (1 + b) * coeff * (ratio || 0);
    const parkingCount = (b) => calcShare(b) / 40;
    const parkingValue = (b) => parkingCount(b) * (parking || 0);
    const calcVal = (b) => calcShare(b) * effectivePrice + parkingValue(b);
    const shareW = calcShare(bonusW), valW = calcVal(bonusW);
    const shareD = calcShare(bonusD), valD = calcVal(bonusD);
    const desired = parseFloat(p.desired_price_wan ?? (p.price_ntd ? Math.round(p.price_ntd / 10000 * 0.9 / 10) * 10 : 0)) || 0;

    const bonusOptsW = (sel) => [0.10, 0.20, 0.30, 0.40].map(b =>
      `<option value="${b}" ${Math.abs(sel - b) < 0.001 ? 'selected' : ''}>${(b * 100).toFixed(0)}%</option>`).join('');
    const bonusOptsD = (sel) => [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00].map(b =>
      `<option value="${b}" ${Math.abs(sel - b) < 0.001 ? 'selected' : ''}>${(b * 100).toFixed(0)}%</option>`).join('');

    const r = (op, lbl, val, note = '') => `
      <div class="v2-rv2-r">
        <span class="v2-rv2-op">${op}</span>
        <span class="v2-rv2-lbl">${lbl}</span>
        <span class="v2-rv2-val">${note ? `<span class="v2-rv2-note">${note}</span>` : ''}<span>${val}</span></span>
      </div>`;

    return `
      <div class="v2-rv2">
        <div class="v2-rv2-land">
          <div class="v2-rv2-land__lbl">土地持分</div>
          <div class="v2-rv2-land__val">${land}<span class="v2-rv2-land__unit">坪</span></div>
          <div class="v2-rv2-land__abbr">${esc(zoneAbbr(p.zoning_original || zoning))}</div>
        </div>
        <div class="v2-rv2-body">
          <div class="v2-rv2-formula">
            ${r('×', '有效容積率', `${effFar}%`)}
            <div class="v2-rv2-r">
              <span class="v2-rv2-op">×</span>
              <span class="v2-rv2-lbl">容積獎勵</span>
              <span class="v2-rv2-val v2-rv2-val--bonus">
                <span class="v2-rv2-tag">危老</span>
                <select class="v2-rv2-edit" onchange="v2.saveOverride('${esc(id)}','bonus_weishau',this.value)">${bonusOptsW(bonusW)}</select>
                <span class="v2-rv2-tag">都更</span>
                <select class="v2-rv2-edit" onchange="v2.saveOverride('${esc(id)}','bonus_dugen',this.value)">${bonusOptsD(bonusD)}</select>
              </span>
            </div>
            <div class="v2-rv2-r">
              <span class="v2-rv2-op">×</span>
              <span class="v2-rv2-lbl">都更係數</span>
              <span class="v2-rv2-val">
                <input type="number" class="v2-rv2-edit" step="0.01" value="${coeff}"
                  onchange="v2.saveOverride('${esc(id)}','rebuild_coeff',this.value)">
              </span>
            </div>
            ${r('×', '分回比例', ratio != null ? (ratio * 100).toFixed(1) + '%' : '—')}
            <div class="v2-rv2-r">
              <span class="v2-rv2-op">×</span>
              <span class="v2-rv2-lbl">新成屋房價<span class="v2-rv2-lbl-unit">(萬/坪)</span></span>
              <span class="v2-rv2-val">
                <input type="number" class="v2-rv2-edit" step="5" value="${newPrice}"
                  onchange="v2.saveOverride('${esc(id)}','new_house_price_wan_override',this.value)">
                <span class="v2-rv2-note">${p.new_house_price_wan_override ? '(已覆寫)' : '(區域平均，可改)'}</span>
              </span>
            </div>
            <div class="v2-rv2-r">
              <span class="v2-rv2-op">×</span>
              <span class="v2-rv2-lbl">樓層加成${is1F ? '<span class="v2-rv2-lbl-unit">(1F 預設20%)</span>' : ''}</span>
              <span class="v2-rv2-val">
                <input type="number" class="v2-rv2-edit" min="0" max="80" step="5" value="${Math.round(floorPremium * 100)}"
                  onchange="v2.saveOverride('${esc(id)}','floor_premium',this.value/100)"> %
              </span>
            </div>
            <div class="v2-rv2-r">
              <span class="v2-rv2-op">+</span>
              <span class="v2-rv2-lbl">分回車位</span>
              <span class="v2-rv2-val v2-rv2-val--bonus">
                <span class="v2-rv2-tag">危老</span>
                <span class="v2-rv2-parking">
                  <span class="v2-rv2-parking__val">${parkingValue(bonusW).toFixed(0)} 萬</span>
                  <span class="v2-rv2-parking__cnt">(${parkingCount(bonusW).toFixed(2)} 位)</span>
                </span>
                <span class="v2-rv2-tag">都更</span>
                <span class="v2-rv2-parking">
                  <span class="v2-rv2-parking__val">${parkingValue(bonusD).toFixed(0)} 萬</span>
                  <span class="v2-rv2-parking__cnt">(${parkingCount(bonusD).toFixed(2)} 位)</span>
                </span>
              </span>
            </div>
          </div>
          <div class="v2-rv2-result">
            ${[
              { tag: '危老', val: valW, share: shareW },
              { tag: isFangzai ? '防災都更' : '都更', val: valD, share: shareD },
            ].map(s => {
              const mult = desired ? (s.val / desired).toFixed(2) : '—';
              const profit = desired ? (s.val - desired).toFixed(0) : '—';
              const profitSign = desired && (s.val - desired) >= 0 ? '+' : '';
              const negCls = desired && (s.val - desired) < 0 ? 'v2-rv2-circ--neg' : '';
              return `
                <div class="v2-rv2-rcol">
                  <div class="v2-rv2-rtag">${s.tag}</div>
                  <div class="v2-rv2-rval">${s.val.toFixed(0)} 萬</div>
                  <div class="v2-rv2-circles">
                    <div class="v2-rv2-circ">
                      <div class="v2-rv2-circ__num">${s.share.toFixed(2)}</div>
                      <div class="v2-rv2-circ__lbl">分回坪</div>
                    </div>
                    <div class="v2-rv2-circ">
                      <div class="v2-rv2-circ__num">${mult}×</div>
                      <div class="v2-rv2-circ__lbl">倍數</div>
                    </div>
                    <div class="v2-rv2-circ ${negCls}">
                      <div class="v2-rv2-circ__num">${profitSign}${profit}</div>
                      <div class="v2-rv2-circ__lbl">效益萬</div>
                    </div>
                  </div>
                </div>`;
            }).join('')}
          </div>
        </div>
      </div>`;
  }

  // ── 個人 override 儲存 (對齊 v1 行為) ─────────────────────────────────────
  // v1: input 永遠 editable，save 永遠 POST。POST 寫到 user 的 watchlist 子文件，
  //     不在 watchlist 就設 _ephemeral_edit_made flag。
  // closeDetail 時 if flag && !inWatchlist → toast 「沒有自動儲存」警示
  async function saveOverride(id, field, value) {
    const p = state.allProperties.find(x => (x.source_id || x.id) === id);
    if (!p) return;
    const v = value === '' || value == null ? null : Number(value);
    let endpoint, body;
    switch (field) {
      case 'desired_price_wan':
        endpoint = `/api/properties/${encodeURIComponent(id)}/desired_price`;
        body = { desired_price_wan: v };
        break;
      case 'bonus_weishau':
        endpoint = `/api/properties/${encodeURIComponent(id)}/bonus`;
        body = { which: 'weishau', value: v };
        break;
      case 'bonus_dugen':
        endpoint = `/api/properties/${encodeURIComponent(id)}/bonus`;
        body = { which: 'dugen', value: v };
        break;
      case 'rebuild_coeff':
        endpoint = `/api/properties/${encodeURIComponent(id)}/rebuild_coeff`;
        body = { value: v };
        break;
      case 'floor_premium':
        endpoint = `/api/properties/${encodeURIComponent(id)}/floor_premium`;
        body = { floor_premium: v };
        break;
      case 'road_width_m_override':
        endpoint = `/api/properties/${encodeURIComponent(id)}/road_width`;
        body = { road_width_m: v };
        break;
      case 'new_house_price_wan_override':
        endpoint = `/api/properties/${encodeURIComponent(id)}/new_house_price`;
        body = { value: v };
        break;
      default:
        toast('未知欄位：' + field, 'error');
        return;
    }
    // 立刻寫回 local state + applyFilters → 卡片倍數即時連動
    p[field] = v;
    if (!p._in_watchlist) p._ephemeral_edit_made = true;   // 標 flag, closeDetail 時提示
    applyFilters();
    // 重新 render detail 讓試算數字更新
    _renderDetailFromCurrent();

    // POST 給後端 (永遠發；後端會寫到 user watchlist override，等同自動加入)
    try {
      const r = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) throw new Error('HTTP ' + r.status);
    } catch (e) {
      console.error('saveOverride', e);
    }
  }

  // 重新 render 當前 detail (override 後試算 / 倍數要更新)
  async function _renderDetailFromCurrent() {
    const id = state.selectedId;
    if (!id) return;
    const p = state.allProperties.find(x => (x.source_id || x.id) === id);
    if (!p) return;
    const prices = await getDistrictPrices();
    $('#v2-drawer-body').innerHTML = detailHTML(p, prices);
  }

  // ── LVR popup helper (對齊 v1 showLvrPopup hover 行為) ───────────────────
  let _lvrPopupTimer = null;
  function showLvrPopup(event, id) {
    const p = state.allProperties.find(x => (x.source_id || x.id) === id);
    if (!p || !Array.isArray(p.lvr_records) || !p.lvr_records.length) return;
    const recs = p.lvr_records.slice(0, 8);
    let pop = document.getElementById('v2-lvr-popup');
    if (!pop) {
      pop = document.createElement('div');
      pop.id = 'v2-lvr-popup';
      pop.className = 'v2-lvr-popup';
      pop.addEventListener('mouseenter', () => clearTimeout(_lvrPopupTimer));
      pop.addEventListener('mouseleave', hideLvrPopup);
      document.body.appendChild(pop);
    }
    // price_total 元 → 萬；地址砍市/區前綴
    const stripCD = (a) => {
      if (!a) return '—';
      let s = String(a).trim();
      s = s.replace(/^(?:臺北市|台北市|新北市|桃園市|台中市|臺中市|高雄市|台南市|臺南市|基隆市|新竹市|新竹縣|苗栗縣|彰化縣|南投縣|雲林縣|嘉義市|嘉義縣|屏東縣|宜蘭縣|花蓮縣|台東縣|臺東縣)/, '');
      s = s.replace(/^[一-龥]{1,3}(?:區|鄉|鎮|市)/, '');
      return s.trim() || '—';
    };
    pop.innerHTML = `<div class="v2-lvr-popup__title">附近實價登錄 (${recs.length} 筆)</div>
      <table class="v2-lvr-tbl">
        <thead><tr><th>交易日</th><th>總價</th><th>建坪</th><th>地坪</th><th>單價</th><th>地址</th></tr></thead>
        <tbody>${recs.map(r => {
          const totalWan = r.price_total ? r.price_total / 10000 : null;
          const perPingWan = (totalWan && r.area_ping) ? (totalWan / r.area_ping) : null;
          return `<tr>
            <td>${esc(r.txn_date || '—')}</td>
            <td>${totalWan != null ? fmt0(totalWan) + '萬' : '—'}</td>
            <td>${r.area_ping ? fmt1(r.area_ping) : '—'}</td>
            <td>${r.land_ping ? fmt1(r.land_ping) : '—'}</td>
            <td>${perPingWan != null ? perPingWan.toFixed(1) + '萬' : '—'}</td>
            <td class="v2-lvr-addr" title="${esc(r.address || '')}">${esc(stripCD(r.address))}</td>
          </tr>`;
        }).join('')}</tbody>
      </table>`;
    const rect = event.target.getBoundingClientRect();
    pop.style.top = (rect.bottom + 6) + 'px';
    pop.style.left = Math.min(rect.left, window.innerWidth - 480) + 'px';
    pop.style.display = 'block';
  }
  function hideLvrPopup() {
    _lvrPopupTimer = setTimeout(() => {
      const pop = document.getElementById('v2-lvr-popup');
      if (pop) pop.style.display = 'none';
    }, 200);
  }

  // ── Watchlist add/remove ─────────────────────────────────────────────────
  async function toggleWatchlist(id) {
    const p = state.allProperties.find(x => (x.source_id || x.id) === id);
    if (!p) return;
    const isIn = !!(p._in_watchlist || p.user_url || p.added_at_user);
    try {
      let r;
      if (isIn) {
        r = await fetch(`/api/watchlist/${encodeURIComponent(id)}`, { method: 'DELETE' });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        toast('已從觀察清單移除');
      } else {
        r = await fetch(`/api/watchlist/${encodeURIComponent(id)}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({}),
        });
        if (!r.ok) {
          const txt = await r.text().catch(() => '');
          throw new Error('HTTP ' + r.status + ' ' + txt.slice(0, 100));
        }
        toast('已加入觀察清單', 'success');
      }
      // 即時更新 state + UI（不全部 reload，太慢）
      p._in_watchlist = !isIn;
      const card = document.querySelector(`.v2-card[data-id="${CSS.escape(id)}"] .v2-card__star`);
      if (card) card.classList.toggle('v2-card__star--active', !isIn);
      // 更新 watchlist tab 計數
      const cnt = $('#v2-watchlist-count');
      if (cnt) {
        const n = state.exploreItems.filter(x => x._in_watchlist).length;
        cnt.textContent = n > 0 ? String(n) : '';
      }
      // 觀察清單 tab cache 失效 (下次切過去會重抓)
      state.watchlistLoaded = false;
    } catch (e) {
      console.error('toggleWatchlist', e);
      toast('操作失敗：' + e.message, 'error');
    }
  }

  // ── Load properties ──────────────────────────────────────────────────────
  // explore 不傳 districts 給 server (對齊用戶要求 #5: 一次抓全部、client 端 filter)
  async function loadProperties() {
    renderSkeletons(8);
    try {
      let url;
      if (state.view === 'watchlist') {
        url = '/api/properties?limit=500&slim=true';
      } else {
        // 不再帶 districts/price 給 server — 全部交給 client filter
        // 用 15 個目標區一起抓，client 再依用戶勾選 filter
        const params = new URLSearchParams();
        params.set('districts', Object.values(V1_DISTRICTS)
          .flatMap(cfg => cfg.enabled).join(','));
        params.set('slim', 'true');
        params.set('limit', '1000');
        url = '/api/central_search?' + params.toString();
      }
      const r = await fetch(url);
      const data = await r.json();
      const items = data.items || [];
      if (state.view === 'watchlist') {
        state.watchlistItems = items;
        state.watchlistLoaded = true;
      } else {
        state.exploreItems = items;
        state.exploreLoaded = true;
      }
      state.allProperties = items;
      // 觀察清單計數：watchlist tab 直接用全部；explore tab 用 _in_watchlist
      const cnt = $('#v2-watchlist-count');
      if (cnt) {
        const n = state.view === 'watchlist'
          ? items.filter(p => !p.deleted).length
          : items.filter(p => p._in_watchlist).length;
        cnt.textContent = n > 0 ? String(n) : '';
      }
      applyFilters();
    } catch (e) {
      console.error('loadProperties', e);
      toast('載入失敗', 'error');
    }
  }

  // 「重新搜尋」按鈕：強制重抓中央 (跳過 client cache)
  // 用戶要求 #5：filter 純 client 端跑；只有此 button 才打 server
  async function runSearch() {
    if (state.view !== 'explore') switchView('explore');
    state.exploreLoaded = false;     // force refetch
    await loadProperties();
  }

  // ── City tab switch (narrow viewport) ────────────────────────────────────
  function switchCityTab(city) {
    state.activeCityTab = city;
    const grid = $('#v2-district-chips .v2-city-grid');
    if (grid) grid.dataset.activeCity = city;
    $$('.v2-city-pill').forEach(p => {
      p.classList.toggle('v2-city-pill--active', p.dataset.city === city);
    });
  }

  // ── Sidebar / view toggling ──────────────────────────────────────────────
  // 切 tab：sidebar swap (explore filter / watchlist capture) + 顯示對應 cache
  // 不主動重抓 — 重抓只能透過「重新搜尋」按鈕 or 第一次進該 tab
  function switchView(view) {
    state.view = view;
    $$('.v2-tab').forEach(t => t.classList.toggle('v2-tab--active', t.dataset.view === view));
    // sidebar 區塊切換
    const sxp = $('#v2-sidebar-explore');
    const swl = $('#v2-sidebar-watchlist');
    if (sxp) sxp.style.display = view === 'explore' ? '' : 'none';
    if (swl) swl.style.display = view === 'watchlist' ? '' : 'none';
    if (view === 'watchlist') populateManualDistricts();
    state.page = 1;
    // 用對應 view 的 cache，沒抓過才 fetch
    if (view === 'explore') {
      state.allProperties = state.exploreItems;
      if (!state.exploreLoaded) loadProperties();
      else applyFilters();
    } else {
      state.allProperties = state.watchlistItems;
      if (!state.watchlistLoaded) loadProperties();
      else applyFilters();
    }
  }

  // ── URL / Manual analyze (v2 watchlist 專用) ────────────────────────────
  // 跟 v1 行為對齊：URL 支援 591/永慶/信義；manual 支援台北/新北
  const _MANUAL_DISTRICTS = {
    "台北市": ["大安區","信義區","中山區","中正區","文山區","松山區","萬華區","大同區","內湖區","南港區"],
    "新北市": ["新店區","永和區","中和區","板橋區","新莊區"],
  };
  function populateManualDistricts() {
    const city = $('#v2-manual-city')?.value;
    const sel = $('#v2-manual-district');
    if (!sel || !city) return;
    sel.innerHTML = (_MANUAL_DISTRICTS[city] || []).map(d =>
      `<option value="${esc(d)}">${esc(d)}</option>`).join('');
  }

  async function triggerScrapeUrl() {
    const inp = $('#v2-scrape-url');
    const url = (inp?.value || '').trim();
    if (!url) { toast('請輸入網址', 'error'); return; }
    const okPatterns = [
      /sale\.591\.com\.tw\/.*\d{6,}/,
      /buy\.yungching\.com\.tw\/house\/\d{6,8}/,
      /sinyi\.com\.tw\/buy\/house\/[A-Z0-9]{4,8}/i,
    ];
    if (!okPatterns.some(re => re.test(url))) {
      toast('看起來不是 591/永慶/信義 詳情頁網址', 'error');
      return;
    }
    inp.disabled = true;
    toast('URL 分析送出中…');
    try {
      const r = await fetch('/api/scrape_url', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url }),
      });
      const data = await r.json();
      if (data.status === 'ok') {
        toast('處理完成', 'success');
        inp.value = '';
        loadProperties();
      } else if (data.status === 'busy') {
        toast('佇列繁忙：' + (data.message || ''), 'error');
      } else if (data.status === 'skipped_non_apartment') {
        toast('跳過：' + (data.message || '非公寓 (>5F)'), 'error');
        inp.value = '';
      } else if (data.status === 'error') {
        toast('分析失敗：' + (data.message || data.detail || 'unknown'), 'error');
      } else {
        toast('未預期回應 (' + data.status + ')', 'error');
      }
    } catch (e) {
      toast('失敗：' + e.message, 'error');
    } finally {
      inp.disabled = false;
    }
  }

  async function triggerManualAnalyze() {
    const city = $('#v2-manual-city')?.value || '';
    const district = $('#v2-manual-district')?.value || '';
    const address = ($('#v2-manual-address')?.value || '').trim();
    const bld = parseFloat($('#v2-manual-bld')?.value);
    const land = parseFloat($('#v2-manual-land')?.value);
    const price = parseFloat($('#v2-manual-price')?.value);
    if (!address) { toast('請輸入地址', 'error'); return; }
    const nonTarget = /^(桃園|基隆|新竹|苗栗|台中|臺中|彰化|南投|雲林|嘉義|台南|臺南|高雄|屏東|宜蘭|花蓮|台東|臺東|澎湖|金門|連江)/;
    if (!['台北市', '新北市'].includes(city) || nonTarget.test(address)) {
      toast('目前僅支援台北/新北地址', 'error');
      return;
    }
    toast('地址分析送出中…');
    try {
      const r = await fetch('/api/manual_analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          city, district, address,
          building_area_ping: isNaN(bld) ? null : bld,
          land_area_ping: isNaN(land) ? null : land,
          price_wan: isNaN(price) ? null : price,
          use_source: 'auto',
        }),
      });
      const data = await r.json();
      if (data.status === 'started') {
        loadProperties();
        toast('分析開始（後台處理中，數秒後重新整理）', 'success');
        // 簡化版：不輪詢，10 秒後重抓清單
        setTimeout(() => loadProperties(), 10000);
      } else if (data.status === 'already_in_db') {
        toast('該地址已在 DB（已加入觀察清單）', 'success');
        loadProperties();
      } else if (data.status === 'district_mismatch') {
        toast('地址與所選區不符：' + (data.message || ''), 'error');
      } else if (data.status === 'not_found') {
        toast('找不到該地址：' + (data.message || ''), 'error');
      } else if (data.status === 'lvr_mismatch') {
        toast('LVR 對不到候選：' + (data.message || ''), 'error');
      } else {
        toast('回應 (' + data.status + ')：' + (data.message || ''), 'error');
      }
    } catch (e) {
      toast('失敗：' + e.message, 'error');
    }
  }
  function toggleDistrict(city, district, checked) {
    const key = `${city}|${district}`;
    if (checked) state.districtPicks.add(key); else state.districtPicks.delete(key);
    applyFilters();
  }
  function gotoPage(p) {
    state.page = p;
    renderGrid();
    $('.v2-main').scrollTo?.({ top: 0, behavior: 'smooth' });
  }
  function resetFilters() {
    // 對齊 v1 default：所有 enabled district 全勾，公寓勾，1F-5F 勾(B不勾)，
    // 抗性 hide：偏遠/特殊/法拍勾，5F+/B 不勾
    state.districtPicks.clear();
    Object.entries(V1_DISTRICTS).forEach(([city, cfg]) => {
      cfg.enabled.forEach(d => state.districtPicks.add(`${city}|${d}`));
    });
    $('#v2-road').value = '';
    $('#v2-price-min').value = 0;
    $('#v2-price-max').value = 5000;
    $('#v2-bld-price-max').value = 300;
    $('#v2-land-price-max').value = 600;
    $('#v2-land-min').value = 0;
    $('#v2-min-mult-on') && ($('#v2-min-mult-on').checked = false);
    $('#v2-min-mult-val') && ($('#v2-min-mult-val').value = 3.0);
    $('#v2-hide-floors5plus').checked = false;
    $('#v2-hide-remote').checked = true;
    $('#v2-hide-unsuitable').checked = true;
    $('#v2-hide-basement').checked = false;
    $('#v2-hide-foreclosure').checked = false;
    $$('.v2-filter-btype:not(:disabled)').forEach(c => { c.checked = c.value === '公寓'; });
    // floor chips：B + 1-5 全勾
    $$('#v2-floor-chips input[data-floor]').forEach(c => { c.checked = true; });
    const fa = $('#v2-floor-all');
    if (fa) fa.checked = true;
    $('#v2-sort').value = 'list_rank';
    state.sortDir = 'desc';
    const sd = $('#v2-sort-dir'); if (sd) sd.textContent = '↓';
    renderDistrictChips();
    applyFilters();
  }
  function openSidebar() {
    $('#v2-sidebar').classList.add('v2-open');
    $('#v2-sidebar-backdrop').classList.add('v2-open');
  }
  function closeSidebar() {
    $('#v2-sidebar').classList.remove('v2-open');
    $('#v2-sidebar-backdrop').classList.remove('v2-open');
  }

  // ── Logout ───────────────────────────────────────────────────────────────
  function logout() {
    if (window.logoutUser) window.logoutUser();
    else window.location.replace('/login.html');
  }

  // ── Boot ─────────────────────────────────────────────────────────────────
  async function boot() {
    // mobile menu button
    $('#v2-menu-btn')?.addEventListener('click', openSidebar);

    // Wait for auth
    if (!window.__authReady) {
      await new Promise(resolve => {
        document.addEventListener('auth:ready', resolve, { once: true });
      });
    }
    // 預設勾選所有 enabled district (對齊 v1 default 全勾)
    Object.entries(V1_DISTRICTS).forEach(([city, cfg]) => {
      cfg.enabled.forEach(d => state.districtPicks.add(`${city}|${d}`));
    });
    // 還原上次 filter 偏好（覆蓋 default）— 必須在 loadDistricts 渲染 chips 之前
    _restoreFilters();
    await loadDistricts();
    await loadProperties();
  }

  // ── Public namespace (matches inline onclick handlers) ──────────────────
  // Floor chip controls
  function toggleAllFloors(masterCheckbox) {
    const checked = masterCheckbox.checked;
    $$('#v2-floor-chips input[data-floor]').forEach(c => { c.checked = checked; });
    applyFilters();
  }
  function onFloorChange() {
    // 同步「全部」master 狀態：所有 floor 都勾 → master 勾；任一沒勾 → master 不勾
    const all = $$('#v2-floor-chips input[data-floor]');
    const allChecked = all.every(c => c.checked);
    const master = $('#v2-floor-all');
    if (master) master.checked = allChecked;
    applyFilters();
  }

  window.v2 = {
    switchView, toggleDistrict, applyFilters, applySort, runSearch,
    resetFilters, gotoPage, openSidebar, closeSidebar,
    openDetail, closeDetail, toggleWatchlist, logout,
    toggleAllFloors, onFloorChange,
    toggleAllInCity, toggleSortDir,
    triggerScrapeUrl, triggerManualAnalyze, populateManualDistricts,
    switchGridCity,
    showLvrPopup, hideLvrPopup,
    saveOverride, saveInferredChoice, setZonePing,
  };

  // Boot
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
