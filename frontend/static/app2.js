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
    // 以下 3 欄位給 map_mode.js (frontend/static/map_mode.js) 使用，邏輯不在本檔
    viewMode: 'list',    // 'list' | 'map'
    _mapInst: null,      // Leaflet map instance (lazy init by map_mode.js)
    _mapMarkers: [],     // 當前 markers (clear/redraw 用)
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
      labels:   { "新店區": "新店", "永和區": "永和", "中和區": "中和", "板橋區": "板橋" },
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
  // localStorage 仍當「即時 cache」(同 tab 重整 instant restore)；DB 是 cross-device 真值。
  // 寫 DB debounced 1.2 秒（避免 slider 拖動時噴大量 POST），用戶關 tab 前用 sendBeacon flush。
  let _saveDbDebounce = null;
  let _saveDbPendingObj = null;
  function _collectFilterObj() {
    return {
      // 路名 / 學區不儲存 — 每次重整當作空白，避免被舊輸入卡住結果
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
      viewMode: state.viewMode,         // 'list' | 'map' — 持久化用戶選擇
    };
  }
  async function _flushFilterPrefsToDB(obj) {
    if (!obj) return;
    try {
      const r = await fetch('/api/user/filter_prefs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prefs: obj }),
      });
      if (!r.ok) console.warn('filter_prefs save HTTP', r.status);
    } catch (e) {
      console.warn('filter_prefs save failed:', e.message || e);
    }
  }
  function _saveFilters() {
    const obj = _collectFilterObj();
    // 1) localStorage 立即寫 — 同 tab 重整看到瞬間 restore
    try { localStorage.setItem(_filterKey(), JSON.stringify(obj)); } catch {}
    // 2) DB 寫 debounced — 縮短到 500ms，避免 mobile 用戶改完馬上切走時 pending 沒 flush
    //    (sendBeacon 在 mobile Safari 不可靠)
    _saveDbPendingObj = obj;
    clearTimeout(_saveDbDebounce);
    _saveDbDebounce = setTimeout(() => {
      const o = _saveDbPendingObj;
      _saveDbPendingObj = null;
      _flushFilterPrefsToDB(o);
    }, 500);
  }
  // 用戶切走 tab / 進背景時 flush — 用 fetch keepalive (sendBeacon 沒帶 auth)
  // visibilitychange 在 mobile 比 beforeunload 可靠
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden' && _saveDbPendingObj) {
      const o = _saveDbPendingObj;
      _saveDbPendingObj = null;
      clearTimeout(_saveDbDebounce);
      try {
        fetch('/api/user/filter_prefs', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ prefs: o }),
          keepalive: true,
        });
      } catch (_e) { /* best effort */ }
    }
  });
  // 關 tab 前若有 pending 寫入，用 sendBeacon flush（不依賴 fetch await）
  window.addEventListener('beforeunload', () => {
    if (!_saveDbPendingObj) return;
    try {
      const blob = new Blob(
        [JSON.stringify({ prefs: _saveDbPendingObj })],
        { type: 'application/json' }
      );
      navigator.sendBeacon('/api/user/filter_prefs', blob);
    } catch (_e) {}
  });
  // _restoreFilters：DB 優先，DB 拿不到 fallback localStorage（離線 / 401 也能 work）
  async function _restoreFilters() {
    let obj = null;
    try {
      const r = await fetch('/api/user/filter_prefs');
      if (r.ok) {
        const data = await r.json();
        if (data && data.prefs && Object.keys(data.prefs).length > 0) obj = data.prefs;
      }
    } catch (_e) { /* 離線 / 網路錯 → fallback */ }
    if (!obj) {
      try { obj = JSON.parse(localStorage.getItem(_filterKey()) || 'null'); } catch {}
    }
    if (!obj) return;
    const setVal = (id, v) => {
      const el = $('#' + id);
      if (el && v !== undefined && v !== null && v !== '') el.value = v;
    };
    const setChk = (id, v) => {
      const el = $('#' + id);
      if (el && typeof v === 'boolean') el.checked = v;
    };
    // 路名 / 學區故意不 restore（每次重整都空白）
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
    // viewMode 還原：非授權 email 強制走列表模式（避免歷史 prefs 殘留繞過 access control）
    // ALLOWED email 由 map_mode.js 提供 window.v2._ALLOWED_MAP_EMAIL — 沒載入 map_mode.js 永遠走 list
    if (obj.viewMode === 'map' || obj.viewMode === 'list') {
      const _email = (window.currentUser && window.currentUser.email) || '';
      const _allowedEmail = (window.v2 && window.v2._ALLOWED_MAP_EMAIL) || '';
      const _allowed = _allowedEmail && _email === _allowedEmail;
      const _wantedMode = (obj.viewMode === 'map' && _allowed) ? 'map' : 'list';
      // 延遲套用：boot 結束 + state.allProperties 載入後才 setViewMode（避免 #v2-map 還沒 paint）
      setTimeout(() => {
        if (window.v2 && typeof window.v2.setViewMode === 'function') {
          window.v2.setViewMode(_wantedMode);
        }
      }, 200);
    }
    // restore 完直接同步 slider DOM（input value 改了但 slider listener 沒被 dispatch
    // → mobile 上 slider 仍顯示 default 值，看起來像 restore 失效）
    _syncSlidersFromInputs();
  }
  // 把已 enhance 的 number input 當前 value 寫回對應的 slider + label
  function _syncSlidersFromInputs() {
    document.querySelectorAll('.v2-sidebar input.v2-input--num[data-slider-enhanced]').forEach(inp => {
      const wrap = inp.nextElementSibling;
      if (!wrap || !wrap.classList.contains('v2-mobile-slider-wrap')) return;
      const slider = wrap.querySelector('.v2-mobile-slider');
      const valSpan = wrap.querySelector('.v2-mobile-slider-label__val');
      const suffixSpan = wrap.querySelector('.v2-mobile-slider-label__suffix');
      if (!slider) return;
      const piecewise = inp.dataset.sliderCurve === 'piecewise';
      const allowUnlimited = inp.dataset.sliderAllowUnlimited === '1';
      const inpN = parseFloat(inp.value) || 0;
      const unl = allowUnlimited && isUnlimitedVal(inpN);
      if (unl) {
        slider.value = parseInt(slider.max, 10);
      } else {
        slider.value = piecewise ? _piecewiseValToPos(inpN) : (inp.value || 0);
      }
      if (valSpan) valSpan.textContent = unl ? '不限' : inp.value;
      if (suffixSpan) suffixSpan.style.display = unl ? 'none' : '';
    });
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
  // skip 跟物件資訊重複的 section：樓高 / 屋齡 (左 table 已顯示) + 其他
  // (其他 section 主要是地段/捷運說明，跟物件資訊地址欄已知；優勢/抗性 chips 在
  //  「其他資訊」區頂部已顯示，不需 ai 補述)
  const _SKIP_AI_SECTIONS = new Set(['樓高', '屋齡', '其他']);

  function renderAiText(text, p, prices) {
    // 優勢/抗性 chips 追加到 ai-sec 列表最後 (對齊 v1 ai-section grid 結構)
    const _adv = p ? computeAdvantageChips(p) : [];
    const _res = p ? computeChips(p) : [];
    const advSec = _adv.length
      ? `<div class="v2-ai-sec"><div class="v2-ai-sec__title">優勢</div>
          <div class="v2-ai-sec__body">${_adv.map(c => `<span class="${c.cls}">${c.label}</span>`).join(' ')}</div></div>`
      : '';
    const resSec = _res.length
      ? `<div class="v2-ai-sec"><div class="v2-ai-sec__title">抗性</div>
          <div class="v2-ai-sec__body">${_res.map(c => `<span class="v2-rchip ${c.cls}">${c.label}</span>`).join(' ')}</div></div>`
      : '';
    if (!text && !advSec && !resSec) return '<div class="v2-detail-empty">尚無分析建議</div>';
    if (!text) return advSec + resSec;
    return text.split(/\n\n+/).map(section => {
      const m = section.match(/^【(.+?)】\s*([\s\S]*)/);
      if (m) {
        const title = m[1];
        if (_SKIP_AI_SECTIONS.has(title)) return '';
        // 「分回價值」section 用動態 dropdown 渲染 (從 input 即時算)；title 改顯示「出價試算」
        if (title === '分回價值' && p && prices) {
          return `<div class="v2-ai-sec"><div class="v2-ai-sec__title">出價試算</div>
            <div class="v2-ai-sec__body" id="v2-ai-bid-section">${renderBidSection(p, prices)}</div></div>`;
        }
        let body = esc(m[2].trim());
        body = body.replace(/(\d+\.\d+)×/g, '$1倍').replace(/(\d+)×/g, '$1倍');
        body = body.replace(/&lt;chk-y&gt;([\s\S]*?)&lt;\/chk-y&gt;/g, '<span style="color:#16a34a;font-weight:700">✓</span> $1');
        body = body.replace(/&lt;chk-n&gt;([\s\S]*?)&lt;\/chk-n&gt;/g, '<span style="color:#dc2626;font-weight:700">✗</span> <span style="color:#9ca3af">$1</span>');
        body = body.replace(/&lt;red&gt;([\s\S]*?)&lt;\/red&gt;/g, '<span style="color:#dc2626;font-weight:600">$1</span>');
        body = body.replace(/&lt;bid_selector[\s\S]*?&gt;/g, '');
        body = body.replace(/\n/g, '<br>');
        return `<div class="v2-ai-sec"><div class="v2-ai-sec__title">${esc(title)}</div><div class="v2-ai-sec__body">${body}</div></div>`;
      }
      return `<div class="v2-ai-sec"><div class="v2-ai-sec__body">${esc(section).replace(/\n/g, '<br>')}</div></div>`;
    }).filter(Boolean).join('') + advSec + resSec;
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
    const newPrice = p.new_house_price_wan_override ?? prices[p.district];
    const farPct = effectiveFar(p);
    // farPct null 涵蓋「single zoning 空」+「multi zoning_list 加權失敗」兩種 case，比看 zoning string 嚴謹
    if (!land || farPct == null || !newPrice) return '缺資料，無法計算';
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

  // ── 算式 / 常數抽到 frontend/static/shared.js (UrbanShared.X) ─────────────
  // zoneAbbr / currentAge / 後面 lookupFar 等都是 alias，動算式請改 shared.js
  const zoneAbbr    = UrbanShared.zoneAbbr;
  const currentAge  = UrbanShared.currentAge;

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

  // 透天厝豁免：透天的土地永遠 > 建坪 (整塊地都自己的)，不算抓錯
  function isLandSuspicious(p) {
    if (p.building_type === '透天厝') return false;
    return UrbanShared.isLandAreaSuspicious(p);
  }

  // ── 算式 / 常數全部抽到 shared.js (UrbanShared.X) — 改算式請動 shared.js ──
  const SHARE_TABLE         = UrbanShared.SHARE_RATIO_TABLE;
  const TAIPEI_FAR_PCT      = UrbanShared.TAIPEI_FAR_PCT;
  const NEW_TAIPEI_FAR_PCT  = UrbanShared.NEW_TAIPEI_FAR_PCT;
  const lookupFar           = UrbanShared.lookupFar;
  const lookupShareRatio    = UrbanShared.lookupShareRatio;
  const effectiveZoning     = UrbanShared.effectiveZoning;
  const applyRoadCap        = UrbanShared.applyRoadCap;
  const baseFar             = UrbanShared.baseFarPctWeighted;
  const effectiveFar        = UrbanShared.effectiveFarPctWeighted;

  // rowMultiple(p, prices) — v2 卡片顯示 max(危老, 都更) 一個數字
  // 跟 v1 computeRowMultiples 用同一條 UrbanShared.computeMultiples，差只在
  // v2 取 max、v1 回 {w, d} 兩個分開
  function rowMultiple(p, prices) {
    const r = UrbanShared.computeMultiples(p, prices[p.district]);
    if (r.w == null || r.d == null) return null;
    return Math.max(r.w, r.d);
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
    // 地圖模式：跳過 list render；count 由 map_mode.js 的 renderMap 負責更新
    if (state.viewMode === 'map' && window.v2 && typeof window.v2._renderMap === 'function') {
      return window.v2._renderMap();
    }
    const grid = $('#v2-grid');
    const empty = $('#v2-empty');
    const list = state.filteredSorted;
    const total = list.length;
    // 手機 (≤1024px) 切到單城市 tab 時，「共 N 筆」只算該城市；桌面看全部
    const _isMobileCol = window.matchMedia('(max-width: 1024px)').matches;
    const _tpeN = list.filter(p => p.city === '台北市').length;
    const _ntpN = list.filter(p => p.city === '新北市').length;
    let _countN = total;
    if (_isMobileCol) {
      const active = state.gridCity || '台北市';
      _countN = active === '台北市' ? _tpeN : (active === '新北市' ? _ntpN : total);
    }
    $('#v2-result-count').innerHTML = `共 <strong>${_countN}</strong> 筆`;
    // 手機 city tab pill 上標筆數 (e.g. 「台北市 12」)；桌面 pill 不顯示故無影響
    const _tpePill = document.querySelector('.v2-grid-toggle__pill[data-city="台北市"]');
    const _ntpPill = document.querySelector('.v2-grid-toggle__pill[data-city="新北市"]');
    if (_tpePill) _tpePill.textContent = `台北市 (${_tpeN})`;
    if (_ntpPill) _ntpPill.textContent = `新北市 (${_ntpN})`;

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
    // 分析中 placeholder card
    if (p._pending_analysis) {
      const kindLbl = p._pending_kind === 'url' ? 'URL 分析' : '地址分析';
      return `<article class="v2-card v2-card--pending" data-id="${esc(id)}">
        <div class="v2-card__pending-wrap">
          <div class="v2-card__pending-spinner"></div>
          <div class="v2-card__pending-text">
            <div class="v2-card__pending-label">${kindLbl}中…</div>
            <div class="v2-card__pending-target" title="${esc(p._pending_label || '')}">${esc(p._pending_label || '')}</div>
          </div>
        </div>
      </article>`;
    }
    // 用推測地址（LVR 推到巷弄/門牌）優先，591 raw address 只到路段不夠精確
    const addr = p.address_inferred || p.address || '—';
    const priceWan = p.price_ntd ? Math.round(p.price_ntd / 10000) : null;
    const perBld = (p.price_ntd && p.building_area_ping)
      ? (p.price_ntd / 10000 / p.building_area_ping).toFixed(1) : null;
    const mult = rowMultiple(p, prices);
    // 倍數 tier (對齊 CSS class)：>=4 桃紅 / >=3 綠 / >=2 土黃 / <2 灰
    let multCls = 'v2-card__mult';
    if (mult != null) {
      if (mult >= 4.0) multCls += ' v2-card__mult--hot';
      else if (mult >= 3.0) multCls += ' v2-card__mult--good';
      else if (mult >= 2.0) multCls += ' v2-card__mult--mid';
    }
    const advChips = computeAdvantageChips(p);
    const chips = computeChips(p);
    const inWatchlist = !!(p._in_watchlist || p.user_url || p.added_at_user);

    // 第一次抓進 DB 的日期 badge — scrape_session_at 是 batch session 時間 (preserve 不被 reanalyze 重寫，CLAUDE.md PREFER_NEW_FIELDS 註解)
    // 24 小時內顯示紅色 NEW，更舊的顯示「M/D」灰色
    let dateBadge = '';
    let isNewObject = false;
    let newTitle = '';
    if (p.scrape_session_at) {
      const t = new Date(p.scrape_session_at);
      if (!isNaN(t)) {
        const ageMs = Date.now() - t.getTime();
        const within24h = ageMs >= 0 && ageMs < 24 * 3600 * 1000;
        const m = t.getMonth() + 1, d = t.getDate();
        const yy = t.getFullYear();
        const sameYr = yy === new Date().getFullYear();
        const dateStr = sameYr ? `${m}/${d}` : `${yy.toString().slice(2)}/${m}/${d}`;
        if (within24h) {
          isNewObject = true;
          newTitle = t.toLocaleString('zh-TW');
        } else {
          dateBadge = `<span class="v2-card__date" title="第一次抓進 DB：${t.toLocaleString('zh-TW')}">${dateStr}</span>`;
        }
      }
    }
    const archivedClass = p.archived ? 'v2-card--archived' : '';
    const readClass = isRead(id) ? 'v2-card--read' : '';
    // 高倍數紅框 (≥3.5x) — mult 上面已算
    const hotClass = (mult != null && mult >= 3.5) ? 'v2-card--hot' : '';
    // 城市色彩區別（左側色條）
    const cityClass = p.city === '台北市' ? 'v2-card--tpe'
                    : p.city === '新北市' ? 'v2-card--ntpc' : '';

    // ── 事件 badge (B4 audit 對齊 v1)：latest_event 7 天內顯示 ─────────────────
    // 漲價 / 降價 / 改價 / cross_source 新上架；type='new' 不打 (NEW badge 已處理)
    let evBadge = '';
    const ev = p.latest_event;
    if (ev && ev.at) {
      const ageMs = Date.now() - new Date(ev.at).getTime();
      const within7Days = ageMs >= 0 && ageMs < 7 * 24 * 3600 * 1000;
      if (within7Days) {
        if (ev.type === 'price_change') {
          const fromW = Math.round((ev.from || 0) / 10000);
          const toW = Math.round((ev.to || 0) / 10000);
          if (ev.direction === 'up') {
            evBadge = `<span class="v2-event-badge v2-event-badge--up" title="從 ${fromW}萬 漲到 ${toW}萬">漲價</span>`;
          } else if (ev.direction === 'down') {
            evBadge = `<span class="v2-event-badge v2-event-badge--down" title="從 ${fromW}萬 降到 ${toW}萬">降價</span>`;
          } else {
            evBadge = `<span class="v2-event-badge v2-event-badge--down">改價</span>`;
          }
        } else if (ev.type === 'cross_source') {
          evBadge = `<span class="v2-event-badge v2-event-badge--cross">${esc(ev.source || '')}新上架</span>`;
        }
      }
    }
    // B5 fallback：沒 latest_event 但有舊版 is_price_changed 旗標 → 仍顯示降價
    if (!evBadge && p.is_price_changed) {
      evBadge = `<span class="v2-event-badge v2-event-badge--down">降價</span>`;
    }
    // B6: 中央 DB 封存 (admin 清掉的物件) — 跟「已下架 overlay」是不同事
    const archivedBadge = p.archived
      ? `<span class="v2-event-badge v2-event-badge--archived" title="此物件已從中央資料庫封存（admin 清理過）">已封存</span>`
      : '';

    // ── 2-line dense layout ──
    // Line 1: icon + 區·地址 (ellipsis) | 總價 + 建單價 | 倍數 | ⭐
    // Line 2: 建/地/齡/層/區/路 + chips + sources
    // B6: 591 偵測下架的物件 → 整卡灰 + 蓋層「已下架」(article 要 position:relative)
    const archivedOverlay = p.archived
      ? '<div class="v2-card__archived-overlay"><span>已下架</span></div>'
      : '';

    return `
      <article class="v2-card ${archivedClass} ${readClass} ${hotClass} ${cityClass}" data-id="${esc(id)}">
        ${archivedOverlay}
        ${isNewObject ? `<div class="v2-card__corner-new" title="新進物件 (24 小時內第一次抓進 DB)：${esc(newTitle)}">N</div>` : ''}
        <div class="v2-card__line1">
          <span class="v2-card__type">${typeIcon(p.building_type)}</span>
          <span class="v2-card__addr">
            <span class="v2-card__district">${esc(p.district || '')}</span><span class="v2-card__sep" aria-hidden="true"></span>${esc(addr)}
          </span>
          <span class="v2-card__price-block">
            <span class="v2-card__price">${priceWan ? fmt0(priceWan) : '—'}<small>萬</small></span>
            ${(p.lvr_records && p.lvr_records.length) ? `<span class="v2-lvr-icon v2-lvr-icon--sm" onmouseenter="v2.showLvrPopup(event, '${esc(id)}')" onmouseleave="v2.hideLvrPopup()" onclick="event.stopPropagation()">實</span>` : ''}
          </span>
          <span class="${multCls}" title="都更倍數">
            ${mult != null ? mult.toFixed(1) : '—'}<small>×</small>
          </span>
          ${state.view === 'watchlist'
            ? `<button class="v2-card__delete" onclick="event.stopPropagation(); v2.deleteRow('${esc(id)}')" title="從觀察清單移除">✕</button>`
            : `<button class="v2-card__star ${inWatchlist ? 'v2-card__star--active' : ''}" data-id="${esc(id)}" title="加入觀察清單">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>
                </svg>
              </button>`}
        </div>
        ${(p.title || perBld) ? `<div class="v2-card__sub-row">
          <span class="v2-card__title-sub" title="${esc(p.title || '')}">${esc(p.title || '')}</span>
          ${perBld ? `<span class="v2-card__price-per">${perBld}/建</span>` : ''}
        </div>` : ''}
        <div class="v2-card__line2">
          <span class="v2-stat" title="建坪"><b>建</b>${fmt1(p.building_area_ping)}</span>
          <span class="v2-stat" title="地坪 (原土地分區縮寫)"><b>地</b>${fmt1(p.land_area_ping)}${(p.zoning_original || p.zoning) ? ` <span class="v2-stat__zone">(${esc(zoneAbbr(p.zoning_original || p.zoning))})</span>` : ''}</span>
          <span class="v2-stat" title="屋齡"><b>齡</b>${p.building_age != null ? p.building_age : '—'}</span>
          <span class="v2-stat" title="樓層"><b>層</b>${formatFloor(p)}</span>
          ${p.road_width_m ? `<span class="v2-stat" title="路寬"><b>路</b>${p.road_width_m}m</span>` : ''}
          ${advChips.length ? advChips.map(c => `<span class="${c.cls}">${c.label}</span>`).join('') : ''}
          ${chips.length ? chips.map(c => `<span class="v2-rchip ${c.cls}">${c.label}</span>`).join('') : ''}
          ${evBadge}${archivedBadge}
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
    // school：比對 doc 上的 school_elementary / school_junior_high 學區清單
    // 若 list 內完全沒 doc 帶 school 欄位 → 學區資料尚未 backfill，filter 視為 no-op
    // (避免 user 設了 filter 卻 0 結果)
    const school = ($('#v2-school')?.value || '').trim();
    if (school) {
      const anyHasSchool = list.some(p =>
        (Array.isArray(p.school_elementary) && p.school_elementary.length) ||
        (Array.isArray(p.school_junior_high) && p.school_junior_high.length)
      );
      if (anyHasSchool) {
        list = list.filter(p => {
          const es = (p.school_elementary || []).join(' ');
          const jh = (p.school_junior_high || []).join(' ');
          return es.includes(school) || jh.includes(school);
        });
      }
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
      // 對齊 v1 (app.js:2473-2477)：用 _added_at (watchlist 加入時間) 優先 fallback 到
      // scrape_session_at；同批次內再按 list_rank。watchlist tab 的 manual / user_url
      // 物件沒 scrape_session_at 但有 _added_at，所以一定要 _added_at 先看
      const dirMul = reverse ? 1 : -1;   // desc = 新→舊 (預設)
      list.sort((a, b) => {
        const ka = a._added_at || a.scrape_session_at || '';
        const kb = b._added_at || b.scrape_session_at || '';
        if (ka !== kb) return kb.localeCompare(ka) * dirMul;
        return ((a.list_rank ?? 9999) - (b.list_rank ?? 9999)) * dirMul;
      });
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
    // 手機切城市時，「共 N 筆」也要跟著切（renderGrid 會根據 state.gridCity 重算）
    if (window.matchMedia('(max-width: 1024px)').matches) {
      renderGrid();
    }
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
      _updateFavBtn(slim);
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
      _updateFavBtn(state.allProperties[idx] || full);
    } catch (e) {
      console.warn('openDetail upgrade fetch failed', e);
    }
    // 學區欄位 — async lookup 更新 (先 render 載入中…，這邊填實際值)
    _loadSchoolDistrict(id);
  }

  // 排序學校名：純中文排前、含括號/數字/-/英文等符號排後；同類別字典序
  function _sortSchoolNames(names) {
    const allChinese = (s) => /^[一-鿿]+$/.test(s);
    return (names || []).slice().sort((a, b) => {
      const aPure = allChinese(a) ? 0 : 1;
      const bPure = allChinese(b) ? 0 : 1;
      if (aPure !== bPure) return aPure - bPure;
      return String(a).localeCompare(String(b), 'zh-Hant');
    });
  }
  async function _loadSchoolDistrict(id) {
    const p = state.allProperties.find(x => (x.source_id || x.id) === id);
    if (!p) return;
    const lat = p.source_latitude ?? p.latitude;
    const lng = p.source_longitude ?? p.longitude;
    const host = document.getElementById(`v2-d-school-${id}`);
    if (!host) return;
    const renderRow = (kind, schools) => {
      const rows = host.querySelectorAll('.v2-d-school-row');
      const row = kind === '國小' ? rows[0] : rows[1];
      if (!row) return;
      const list = row.querySelector('.v2-d-school-list');
      if (!list) return;
      if (!schools || !schools.length) {
        list.innerHTML = '<span style="color:#888">—</span>';
        return;
      }
      list.innerHTML = _sortSchoolNames(schools).map(s =>
        `<span class="v2-d-school-tag">${esc(s)}</span>`
      ).join(' ');
    };
    if (!lat || !lng) {
      renderRow('國小', []); renderRow('國中', []);
      return;
    }
    try {
      const r = await fetch(`/api/school_district/lookup?lat=${lat}&lng=${lng}`);
      if (!r.ok) {
        renderRow('國小', []); renderRow('國中', []);
        return;
      }
      const data = await r.json();
      renderRow('國小', data.school_elementary || []);
      renderRow('國中', data.school_junior_high || []);
    } catch (_e) {
      renderRow('國小', []); renderRow('國中', []);
    }
  }

  // drawer header「★ 加入最愛 / 從最愛移除」按鈕同步 + sources (591/永慶 連結)
  function _updateFavBtn(p) {
    const btn = $('#v2-drawer-fav');
    if (!btn) return;
    btn.style.display = '';
    const inW = !!(p._in_watchlist || p.user_url || p.added_at_user);
    btn.classList.toggle('v2-drawer__fav--active', inW);
    const lbl = btn.querySelector('.v2-drawer__fav-label');
    if (lbl) lbl.textContent = inW ? '從最愛移除' : '加入最愛';
    // 來源連結 (591/永慶/信義)：放在 fav button 旁，drawer header 上 (對齊 v1 modal header)
    const srcWrap = $('#v2-drawer-sources');
    if (srcWrap) {
      if (p.sources && p.sources.length) {
        srcWrap.innerHTML = srcBadgesHTML(p.sources);
        srcWrap.style.display = '';
      } else {
        srcWrap.innerHTML = '';
        srcWrap.style.display = 'none';
      }
    }
  }

  // header fav 按鈕點擊：對 state.selectedId call toggleWatchlist 然後 update btn
  async function toggleDetailWatchlist() {
    const id = state.selectedId;
    if (!id) return;
    await toggleWatchlist(id);
    const p = state.allProperties.find(x => (x.source_id || x.id) === id);
    if (p) _updateFavBtn(p);
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
    // 收起 fav button + sources
    const fav = $('#v2-drawer-fav');
    if (fav) fav.style.display = 'none';
    const srcWrap = $('#v2-drawer-sources');
    if (srcWrap) { srcWrap.innerHTML = ''; srcWrap.style.display = 'none'; }
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
    const inferredTag = '';   // (用戶要求拿掉「推測」/「≈推測」/「★實登」標籤)

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
    // LVR 地址砍市/區前綴；超寬時 wrap 不省略，⚠ icon 永遠在最右獨立 cell 不會被擠掉
    const stripCDLvr = (a) => {
      if (!a) return '—';
      let s = String(a).trim();
      s = s.replace(/^(?:臺北市|台北市|新北市|桃園市|台中市|臺中市|高雄市|台南市|臺南市|基隆市|新竹市|新竹縣|苗栗縣|彰化縣|南投縣|雲林縣|嘉義市|嘉義縣|屏東縣|宜蘭縣|花蓮縣|台東縣|臺東縣)/, '');
      s = s.replace(/^[一-龥]{1,3}(?:區|鄉|鎮|市)/, '');
      return s.trim() || '—';
    };
    const lvrHTML = lvrRecs.length
      ? `<table class="v2-lvr-tbl"><thead><tr><th>交易日</th><th>總價(萬)</th><th>建坪</th><th>單價</th><th>地址</th><th></th></tr></thead><tbody>${
          lvrRecs.map(r => {
            const totalWan = r.price_total ? r.price_total / 10000 : null;
            return `<tr>
              <td>${esc(r.txn_date || '—')}</td>
              <td>${totalWan != null ? Math.round(totalWan) : '—'}</td>
              <td>${r.area_ping ? r.area_ping.toFixed(2) : '—'}</td>
              <td>${(totalWan != null && r.area_ping) ? Math.round(totalWan / r.area_ping) : '—'}</td>
              <td class="v2-lvr-addr" title="${esc(r.address || '')}">${esc(stripCDLvr(r.address))}</td>
              <td class="v2-lvr-warn-cell">${r.is_special ? `<span class="v2-lvr-warn">⚠<span class="v2-lvr-tip">${esc(r.note || '特殊交易')}</span></span>` : ''}</td>
            </tr>`;
          }).join('')}</tbody></table>`
      : '<div class="v2-detail-empty">無實價登錄記錄</div>';

    // ── AI 分析 ──
    const aiText = p.ai_reason || p.ai_analysis || '';

    // ── 臨路寬度 cell — 地籍圖改成內部 overlay；說明文字放 overlay 內 ──
    // 台北市 + 沒 screenshot → 顯示「重新掃描路寬」按鈕 (對齊 v1 scanRoadWidth)
    const roadShotBtn = p.screenshot_roadwidth
      ? ` <button class="v2-d-road-show" onclick="event.stopPropagation(); v2.openRoadOverlay('${esc(id)}')">地籍圖 ↗</button>`
      : (p.city === '台北市'
          ? ` <button class="v2-d-road-scan" onclick="event.stopPropagation(); v2.scanRoadWidth('${esc(id)}', this)">重新掃描路寬</button>`
          : '');

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
      <!-- Row 1: 物件資訊 (左 7) | 圖片 (右 5) — 對齊 v1 col-md-7 + col-md-5 -->
      <div class="v2-d-row">
        <div class="v2-d-col v2-d-col--7">
          <h6 class="v2-d-h v2-d-h--left">物件資訊</h6>
          <div class="v2-d-basic-grid v2-d-basic-grid--v1">
            <div class="v2-d-basic-col">
              <table class="v2-d-tbl">
                <tr><td>原始地址</td><td class="v2-d-addr-val">${esc(stripCityDist(p.address || p.title))}${p.address_road_fixed ? `<div class="v2-d-addr-fixed">已自動修正：${esc(p.address_road_fixed.from)} → ${esc(p.address_road_fixed.to)}</div>` : ''}${p.address_suspicious ? `<div class="v2-d-warn">⚠ 路名可能不存在於此行政區，請自行確認</div>` : ''}</td></tr>
                <tr><td>推測地址</td><td class="v2-d-addr-val">${inferredAddressCellHTML(p)}</td></tr>
                <tr><td>類型 / 樓層</td><td>${esc(p.building_type || '—')} ・ ${formatFloor(p)}</td></tr>
                <tr><td>屋齡</td><td>${age != null ? age + ' 年' + (p.building_age_completed_year ? ` <span class="v2-d-hint">（${p.building_age_completed_year} 年完工）</span>` : '') : '未知'}</td></tr>
                <tr><td>售價</td><td class="v2-d-price-cell">${priceWan ? `NT$ ${fmt0(priceWan)} 萬` : '—'}${lvrIcon}</td></tr>
                <tr><td>建坪</td><td>${p.building_area_ping ? p.building_area_ping + ' 坪' : '—'}${perBld ? ` <span class="v2-d-hint">(${perBld} 萬 / 建坪)</span>` : ''}</td></tr>
                <tr><td>地坪</td><td>${p.land_area_ping ? p.land_area_ping + ' 坪' : '—'}${perLand ? ` <span class="v2-d-hint">(${perLand} 萬 / 地坪)</span>` : ''}${p.land_area_source === 'lvr' ? ' <span class="v2-d-hint">(實登)</span>' : ''}${isLandSus ? '<div class="v2-d-warn">⚠ 坪數過大（大於建坪），可能不可信</div>' : ''}${p.land_area_inconsistent ? '<div class="v2-d-warn">⚠ 此物件的實登候選地坪差異大，可能不是同一棟建築；選擇後請務必驗證。</div>' : ''}</td></tr>
              </table>
            </div>
            <div class="v2-d-basic-col">
              <table class="v2-d-tbl">
                <tr><td>附近捷運站</td><td>${
                  Array.isArray(p.nearby_mrts) && p.nearby_mrts.length
                    ? p.nearby_mrts.map(m => `${esc(m.name)}（${Math.round(m.dist_m)}m）`).join('<br>')
                    : '—'
                }</td></tr>
                <tr><td>使用分區</td><td>${zoningCellHTML(p)}</td></tr>
                <tr><td>臨路寬度</td><td><input type="number" class="v2-d-input v2-d-input--narrow" min="0" step="0.5" value="${(p.road_width_m_override ?? p.road_width_m) ?? ''}"
                  onchange="v2.saveOverride('${esc(id)}','road_width_m_override',this.value)"> m${roadShotBtn}${p.road_width_unknown ? ' <span class="v2-d-warn-inline">（寬度不明，有可能為私巷或特窄巷弄）</span>' : ''}${roadNameHint(p) ? `<div class="v2-d-road-name-hint">${esc(roadNameHint(p))}</div>` : ''}</td></tr>
                <tr><td>學區</td><td><div class="v2-d-school" id="v2-d-school-${esc(id)}">
                  <div class="v2-d-school-row"><span class="v2-d-school-kind">國小</span><span class="v2-d-school-list">載入中…</span></div>
                  <div class="v2-d-school-row"><span class="v2-d-school-kind">國中</span><span class="v2-d-school-list">載入中…</span></div>
                </div></td></tr>
              </table>
            </div>
          </div>
        </div>
        <div class="v2-d-col v2-d-col--5">
          ${img || '<div class="v2-detail-image-wrap"><div class="v2-detail-image-empty">無照片</div></div>'}
        </div>
      </div>

      <!-- Row 2: 都更換回試算 (左 7) | 分析建議 (右 5) — 對齊 v1 -->
      <div class="v2-d-row">
        <div class="v2-d-col v2-d-col--7">
          <h6 class="v2-d-h">都更換回試算</h6>
          ${renewalSectionHTML(p, prices)}
        </div>
        <div class="v2-d-col v2-d-col--5">
          <h6 class="v2-d-h">其他資訊</h6>
          <div class="v2-d-ai-text">${renderAiText(aiText || '', p, prices)}</div>
        </div>
      </div>

      <!-- Row 3: 操作 (加入最愛已移到 drawer header，此區暫保留結構萬一未來放別的按鈕) -->
    `;
  }

  // ── 使用分區 cell (對齊 v1 zoningCellHTML — 多分區編輯 + (特)/(遷) 警示 + 候選展開) ──
  function zoningCellHTML(p) {
    const z = p.zoning;
    const cands = p.zoning_candidates || [];
    const zList = p.zoning_list;
    // multi case backend 不寫 zoning string（避免武斷 splitter），zoning_list 才是 source of truth
    if (!z && !cands.length && !(zList && zList.length)) return '<span class="v2-d-hint">待查</span>';

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
        return `<div class="v2-d-zone-multi-row">
          <span class="v2-d-zone-badge">${esc(zoneAbbr(eff))} (${far}%)</span>
          <input type="number" class="v2-d-zone-ping" min="0" max="${totalLand}" step="0.01" value="${v}" ${dis}
            onchange="v2.setZonePing('${esc(id)}', ${i}, this.value)">坪
        </div>`;
      }).join('');
      badge += `<div class="v2-d-zone-err" id="v2-zone-err-${esc(id)}" style="display:none"></div>`;
      badge += locked
        ? `<div class="v2-d-zone-note">依謄本登錄坪數鎖定（總 ${totalLand} 坪）</div>`
        : `<div class="v2-d-zone-note">總土地 ${totalLand} 坪。請依實際坪數輸入（任一改動，其他自動同步）</div>`;
    } else if (z) {
      // 分區條件色：商業區→紅、住宅/工業/其他→黃
      const cls = z.includes('商') ? 'v2-d-zone-badge--commercial' : 'v2-d-zone-badge--default';
      const far = lookupFar(z, p);
      const farStr = far != null ? ` (${far}%)` : '';
      badge = `<span class="v2-d-zone-badge ${cls}">${esc(zoneAbbr(z))}${farStr}</span>`;
      if (orig && orig !== z) {
        badge += `<div class="v2-d-zone-orig">原：${esc(orig)}</div>`;
      }
    } else {
      badge = '<span class="v2-d-hint">—</span>';
    }
    // (特)/(遷)/(核)/(抄) 加註說明 — 對齊 v1
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
    // 用戶要求拿掉「來源：…」整段 (sourceLabel/srcLink 仍 build 但不 render)
    return `${badge}${errorLine}${candsBlock}`;
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
    if (!p._in_watchlist) {
      p._ephemeral_edit_made = true;
      p._pending_overrides = p._pending_overrides || {};
      p._pending_overrides.zoning_ratios = ratios;
    }
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
  // 路名 hint — 對齊 v1 roadNameHint：優先 road_width_name，再從 road_width_all 列表找最接近地址路名的
  function roadNameHint(p) {
    if (!p) return '';
    if (p.road_width_name) return p.road_width_name;
    const allRoads = p.road_width_all || [];
    if (!allRoads.length) return '';
    const addr = p.address_inferred || p.address || '';
    const addrRoad = (addr.match(/([一-龥]+(?:路|街|大道)[一-龥]*段?(?:\d+巷)?(?:\d+弄)?)/) || [])[1] || '';
    if (addrRoad) {
      const exact = allRoads.find(r => r.road_name === addrRoad);
      if (exact) return exact.road_name;
      const fuzzy = allRoads.find(r => r.road_name && r.road_name.includes(addrRoad.replace(/\d+巷$/, '').replace(/\d+弄$/, '')));
      if (fuzzy) return fuzzy.road_name;
    }
    return (allRoads[0] && allRoads[0].road_name) || '';
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
      // 不在 watchlist 時：後端 NoopRef 吃掉，標 flag 讓 closeDetail 提示
      const stillNotInWl = idx >= 0 && !state.allProperties[idx]._in_watchlist;
      if (stillNotInWl) {
        state.allProperties[idx]._ephemeral_edit_made = true;
        state.allProperties[idx]._pending_inferred_choice = address;
      }
      // 重新 render detail (含試算 — land 變了倍數要重算) + 重 applyFilters 讓首頁卡片更新
      _renderDetailFromCurrent();
      applyFilters();
      // toast：在 watchlist 才真的存進 DB；否則顯示「暫時套用」而不是「已儲存」
      if (stillNotInWl) {
        toast('已暫時套用 (尚未加入最愛 → 不會儲存)', 'info');
      } else {
        toast('已儲存', 'success');
      }
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
    const baseFarPct = baseFar(p);
    const effFar = effectiveFar(p);
    const roadCapped = (baseFarPct != null && effFar != null && effFar < baseFarPct);
    const roadW = p.road_width_m_override ?? p.road_width_m;
    const coeff = p.rebuild_coeff ?? 1.57;
    const newPrice = p.new_house_price_wan_override ?? prices[p.district];
    // farPct null 涵蓋「single zoning 空」+「multi zoning_list 加權失敗」兩種 case
    if (!land || effFar == null || !newPrice) {
      const missing = [
        !land ? '土地坪數' : null,
        effFar == null ? '使用分區' : null,
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

    // 出價建議 (從 renderBidSection 抽出 dropdown row 而已，不重複顯示「危老 X 萬 都更 X 萬」)
    const bidOpts = [3.0, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 4.0, 4.2, 4.5, 5.0];
    const mkBidOpts = (sel) => bidOpts.map(v =>
      `<option value="${v}" ${Math.abs(v - sel) < 0.01 ? 'selected' : ''}>${v.toFixed(1)} 倍</option>`).join('');
    const desiredForBid = parseFloat(p.desired_price_wan ?? (p.price_ntd ? Math.round(p.price_ntd / 10000 * 0.9 / 10) * 10 : 0)) || 0;
    const fmtN = (n) => Math.round(n).toLocaleString('zh-TW');
    const wValRound = Math.round(valW), dValRound = Math.round(valD);
    const bidHTML = desiredForBid <= 0
      ? `<div class="v2-bid-row v2-bid-row--muted">尚未填入欲出價，無法給出價建議</div>`
      : `${valW > 0 ? `<div class="v2-bid-row">• 危老出價建議：<select class="v2-bid-select" onchange="this.nextElementSibling.textContent='≤ '+Math.round(${wValRound}/parseFloat(this.value)).toLocaleString()+' 萬'">${mkBidOpts(3.2)}</select> <span class="v2-bid-max">≤ ${fmtN(wValRound / 3.2)} 萬</span></div>` : ''}
         <div class="v2-bid-row">• 都更出價建議：<select class="v2-bid-select" onchange="this.nextElementSibling.textContent='≤ '+Math.round(${dValRound}/parseFloat(this.value)).toLocaleString()+' 萬'">${mkBidOpts(3.2)}</select> <span class="v2-bid-max">≤ ${fmtN(dValRound / 3.2)} 萬</span></div>`;

    // 結果欄 render — 1:1 對齊 v1：tag (頂) / val (中央大字 萬) / 三圓圈 (分回坪/倍數/效益)
    const renderResult = (tag, val, share) => {
      const mult = desired ? (val / desired).toFixed(2) : '—';
      const profit = desired ? (val - desired).toFixed(0) : '—';
      const profitSign = desired && (val - desired) >= 0 ? '+' : '';
      const negCls = desired && (val - desired) < 0 ? 'v2-rv2-circ--neg' : '';
      return `
        <div class="v2-rv2-rcol">
          <div class="v2-rv2-rtag">${tag}</div>
          <div class="v2-rv2-rval">${val.toFixed(0)} 萬</div>
          <div class="v2-rv2-circles">
            <div class="v2-rv2-circ">
              <div class="v2-rv2-circ__num">${share.toFixed(2)}</div>
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
    };

    return `
      <div class="v2-rv2 v2-rv2--2col">
        <div class="v2-rv2-land v2-rv2-land--top">
          <div class="v2-rv2-land__left">
            <span class="v2-rv2-land__lbl">土地持分</span>
            <span class="v2-rv2-land__val">${land}<span class="v2-rv2-land__unit">坪</span></span>
          </div>
          <div class="v2-rv2-land__bid">
            <label class="v2-rv2-land__bid-lbl">出價設定</label>
            <input type="number" class="v2-d-input" min="0" step="10" value="${desired ?? ''}"
              onchange="v2.saveOverride('${esc(id)}','desired_price_wan',this.value)"> 萬
          </div>
        </div>
        <div class="v2-rv2-left">
          <div class="v2-rv2-formula">
            ${r('×', '有效容積率', `${effFar}%`,
                roadCapped ? `<span class="v2-rv2-warn">⚠ 受路寬 ${roadW}m 限縮</span>` : '')}
            <div class="v2-rv2-r">
              <span class="v2-rv2-op">×</span>
              <span class="v2-rv2-lbl">容積獎勵</span>
              <span class="v2-rv2-val v2-rv2-val--bonus">
                <span class="v2-rv2-bonus-line">
                  <span class="v2-rv2-tag">危老</span>
                  <span class="v2-rv2-slider">
                    <input type="range" class="v2-rv2-range" min="0" max="40" step="5" value="${Math.round(bonusW * 100)}"
                      oninput="this.nextElementSibling.textContent=this.value+'%'"
                      onchange="v2.saveOverride('${esc(id)}','bonus_weishau',this.value/100)">
                    <span class="v2-rv2-slider-val">${Math.round(bonusW * 100)}%</span>
                  </span>
                </span>
                <span class="v2-rv2-bonus-line">
                  <span class="v2-rv2-tag">都更</span>
                  <span class="v2-rv2-slider">
                    <input type="range" class="v2-rv2-range" min="0" max="100" step="5" value="${Math.round(bonusD * 100)}"
                      oninput="this.nextElementSibling.textContent=this.value+'%'"
                      onchange="v2.saveOverride('${esc(id)}','bonus_dugen',this.value/100)">
                    <span class="v2-rv2-slider-val">${Math.round(bonusD * 100)}%</span>
                  </span>
                </span>
              </span>
            </div>
            <div class="v2-rv2-r">
              <span class="v2-rv2-op">×</span>
              <span class="v2-rv2-lbl">都更係數</span>
              <span class="v2-rv2-val">
                <span class="v2-rv2-slider">
                  <input type="range" class="v2-rv2-range" min="1.50" max="1.60" step="0.01" value="${coeff}"
                    oninput="this.nextElementSibling.textContent=parseFloat(this.value).toFixed(2)"
                    onchange="v2.saveOverride('${esc(id)}','rebuild_coeff',this.value)">
                  <span class="v2-rv2-slider-val">${parseFloat(coeff).toFixed(2)}</span>
                </span>
              </span>
            </div>
            ${r('×', '分回比例', ratio != null ? (ratio * 100).toFixed(1) + '%' : '—')}
            <div class="v2-rv2-r">
              <span class="v2-rv2-op">×</span>
              <span class="v2-rv2-lbl">新成屋房價<span class="v2-rv2-lbl-unit">(萬/坪)</span></span>
              <span class="v2-rv2-val">
                <span class="v2-rv2-note">${p.new_house_price_wan_override ? '(已覆寫)' : '(此為區域平均單價，您可自行調整)'}</span>
                <span class="v2-rv2-slider">
                  <input type="range" class="v2-rv2-range" min="0" max="250" step="1" value="${Math.round(newPrice)}"
                    oninput="this.nextElementSibling.textContent=this.value"
                    onchange="v2.saveOverride('${esc(id)}','new_house_price_wan_override',this.value)">
                  <span class="v2-rv2-slider-val">${Math.round(newPrice)}</span>
                </span>
              </span>
            </div>
            <div class="v2-rv2-r">
              <span class="v2-rv2-op">×</span>
              <span class="v2-rv2-lbl">樓層加成${is1F ? '<span class="v2-rv2-lbl-unit">(1F 預設20%)</span>' : ''}</span>
              <span class="v2-rv2-val">
                <span class="v2-rv2-slider">
                  <input type="range" class="v2-rv2-range" min="0" max="50" step="5" value="${Math.round(floorPremium * 100)}"
                    oninput="this.nextElementSibling.textContent=this.value+'%'"
                    onchange="v2.saveOverride('${esc(id)}','floor_premium',this.value/100)">
                  <span class="v2-rv2-slider-val">${Math.round(floorPremium * 100)}%</span>
                </span>
              </span>
            </div>
            <div class="v2-rv2-r">
              <span class="v2-rv2-op">+</span>
              <span class="v2-rv2-lbl">分回車位</span>
              <span class="v2-rv2-val v2-rv2-val--parking">
                <span class="v2-rv2-parking-line">
                  <span class="v2-rv2-tag">危老</span>
                  <span class="v2-rv2-parking">
                    <span class="v2-rv2-parking__val">${parkingValue(bonusW).toFixed(0)} 萬</span>
                    <span class="v2-rv2-parking__cnt">(${parkingCount(bonusW).toFixed(2)} 位)</span>
                  </span>
                </span>
                <span class="v2-rv2-parking-line">
                  <span class="v2-rv2-tag">都更</span>
                  <span class="v2-rv2-parking">
                    <span class="v2-rv2-parking__val">${parkingValue(bonusD).toFixed(0)} 萬</span>
                    <span class="v2-rv2-parking__cnt">(${parkingCount(bonusD).toFixed(2)} 位)</span>
                  </span>
                </span>
              </span>
            </div>
          </div>
        </div>
        <div class="v2-rv2-right">
          <div class="v2-rv2-result v2-rv2-result--stack">
            ${renderResult('危老', valW, shareW)}
            ${renderResult('都更', valD, shareD)}
          </div>
        </div>
      </div>`;
  }

  // 出價建議區塊 — Row 2 右欄獨立 render (出價設定已移到都更換回試算右欄)
  function bidSectionHTML(p, prices) {
    if (p.is_foreclosure || p.is_remote_area || p.unsuitable_for_renewal || isLandSuspicious(p)) {
      return `<div class="v2-bid-section"><div class="v2-d-alert v2-d-alert--soft">此物件不適用都更/危老試算，故無出價建議。</div></div>`;
    }
    const land = p.land_area_ping;
    const newPrice = p.new_house_price_wan_override ?? prices[p.district];
    const farPct = effectiveFar(p);
    // farPct null 涵蓋「single zoning 空」+「multi zoning_list 加權失敗」兩種 case
    if (!land || farPct == null || !newPrice) {
      return `<div class="v2-d-alert v2-d-alert--soft">缺資料，無法給出價建議。</div>`;
    }
    const coeff = p.rebuild_coeff ?? 1.57;
    const [ratio, parking] = lookupShareRatio(newPrice);
    if (!ratio) return `<div class="v2-d-alert v2-d-alert--soft">缺分回比例，無法給出價建議。</div>`;
    const isFangzai = p.city === '台北市' && currentAge(p) && (new Date().getFullYear() - currentAge(p)) <= 1974;
    const bonusW = p.bonus_weishau ?? 0.30;
    const bonusD = p.bonus_dugen ?? (isFangzai ? 0.80 : 0.50);
    const is1F = Number(p.floor) === 1 || Number(p.floor_range_min) === 1;
    const floorPremium = p.floor_premium ?? (is1F ? 0.20 : 0);
    const effectivePrice = newPrice * (1 + floorPremium);
    const calcVal = (b) => {
      const share = land * (farPct / 100) * (1 + b) * coeff * ratio;
      return share * effectivePrice + (share / 40) * (parking || 0);
    };
    const valW = calcVal(bonusW);
    const valD = calcVal(bonusD);
    const wValRound = Math.round(valW), dValRound = Math.round(valD);
    const desired = parseFloat(p.desired_price_wan ?? (p.price_ntd ? Math.round(p.price_ntd / 10000 * 0.9 / 10) * 10 : 0)) || 0;
    const fmtN = (n) => Math.round(n).toLocaleString('zh-TW');
    const opts = [3.0, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 4.0, 4.2, 4.5, 5.0];
    const mkOpts = (sel) => opts.map(v =>
      `<option value="${v}" ${Math.abs(v - sel) < 0.01 ? 'selected' : ''}>${v.toFixed(1)} 倍</option>`).join('');
    if (desired <= 0) {
      return `<div class="v2-bid-section">
        <div class="v2-bid-row v2-bid-row--muted">尚未填入出價設定，無法給出建議</div>
      </div>`;
    }
    return `<div class="v2-bid-section">
      ${valW > 0 ? `<div class="v2-bid-row">危老出價建議：<select class="v2-bid-select" onchange="this.nextElementSibling.textContent='≤ '+Math.round(${wValRound}/parseFloat(this.value)).toLocaleString()+' 萬'">${mkOpts(3.2)}</select> <span class="v2-bid-max">≤ ${fmtN(wValRound / 3.2)} 萬</span></div>` : ''}
      <div class="v2-bid-row">都更出價建議：<select class="v2-bid-select" onchange="this.nextElementSibling.textContent='≤ '+Math.round(${dValRound}/parseFloat(this.value)).toLocaleString()+' 萬'">${mkOpts(3.2)}</select> <span class="v2-bid-max">≤ ${fmtN(dValRound / 3.2)} 萬</span></div>
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
        body = { new_house_price_wan_per_ping: v };
        break;
      default:
        toast('未知欄位：' + field, 'error');
        return;
    }
    // 立刻寫回 local state + applyFilters → 卡片倍數即時連動
    p[field] = v;
    if (!p._in_watchlist) {
      p._ephemeral_edit_made = true;   // 標 flag, closeDetail 時提示
      // 紀錄 pending override 給「加入 watchlist 後 backfill」用
      // (後端 _user_override_ref 在不在 watchlist 時走 NoopRef，POST 會被丟棄)
      p._pending_overrides = p._pending_overrides || {};
      p._pending_overrides[field] = v;
    }
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
        <thead><tr><th>交易日</th><th>總價</th><th>建坪</th><th>地坪</th><th>單價</th><th>地址</th><th></th></tr></thead>
        <tbody>${recs.map(r => {
          const totalWan = r.price_total ? r.price_total / 10000 : null;
          const perPingWan = (totalWan && r.area_ping) ? (totalWan / r.area_ping) : null;
          return `<tr>
            <td>${esc(r.txn_date || '—')}</td>
            <td>${totalWan != null ? Math.round(totalWan) + '萬' : '—'}</td>
            <td>${r.area_ping ? r.area_ping.toFixed(2) : '—'}</td>
            <td>${r.land_ping ? r.land_ping.toFixed(2) : '—'}</td>
            <td>${perPingWan != null ? Math.round(perPingWan) + '萬' : '—'}</td>
            <td class="v2-lvr-addr" title="${esc(r.address || '')}">${esc(stripCD(r.address))}</td>
            <td class="v2-lvr-warn-cell">${r.is_special ? `<span class="v2-lvr-warn">⚠<span class="v2-lvr-tip">${esc(r.note || '特殊交易')}</span></span>` : ''}</td>
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

  // ── watchlist tab 卡片刪除 (對齊 v1 deleteRow) — ✕ button + 雙確認 ─────────
  async function deleteRow(id) {
    const p = state.allProperties.find(x => (x.source_id || x.id) === id);
    const label = p ? (p.address_inferred || p.address || p.title || id) : id;
    if (!confirm(`確定從觀察清單移除？\n\n${label}\n\n移除後個人試算覆寫 (欲出價、新成屋單價等) 也會一併刪除。`)) return;
    try {
      const r = await fetch(`/api/watchlist/${encodeURIComponent(id)}`, { method: 'DELETE' });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      state.allProperties = state.allProperties.filter(x => (x.source_id || x.id) !== id);
      state.watchlistItems = state.watchlistItems.filter(x => (x.source_id || x.id) !== id);
      // explore tab cache 同 id 物件：去星 + 清掉所有 override fields
      // (DB 那邊 watchlist sub-doc 已 delete、override 跟著消失；前端 cache 也要清乾淨
      //  避免用戶切到探索 tab 還看到改過的數字)
      const ex = state.exploreItems.find(x => (x.source_id || x.id) === id);
      if (ex) {
        ex._in_watchlist = false;
        delete ex.desired_price_wan;
        delete ex.bonus_weishau;
        delete ex.bonus_dugen;
        delete ex.rebuild_coeff;
        delete ex.floor_premium;
        delete ex.road_width_m_override;
        delete ex.new_house_price_wan_override;
        delete ex.zoning_ratios;
        delete ex.inferred_address_choice;
        delete ex._ephemeral_edit_made;
        delete ex._pending_overrides;
        delete ex._pending_inferred_choice;
      }
      applyFilters();
      toast('已從觀察清單移除', 'success');
    } catch (e) {
      console.error('deleteRow', e);
      toast('移除失敗：' + e.message, 'error');
    }
  }

  // ── 重新掃描路寬 (對齊 v1 scanRoadWidth) — 台北市 + 沒 screenshot 時 ─────────
  // POST /api/properties/{id}/scan_road_width，progress 模擬 + 完成後更新 state + re-render detail
  async function scanRoadWidth(id, btn) {
    btn.disabled = true;
    btn.classList.add('v2-d-road-scan--scanning');
    btn.textContent = '0%';
    const steps = [
      [1000, '10%'], [3000, '30%'], [5000, '50%'],
      [8000, '70%'], [11000, '85%'],
    ];
    const timers = steps.map(([ms, label]) => setTimeout(() => {
      if (btn.classList.contains('v2-d-road-scan--scanning')) btn.textContent = label;
    }, ms));
    try {
      const res = await fetch(`/api/properties/${encodeURIComponent(id)}/scan_road_width`, { method: 'POST' });
      const data = await res.json();
      timers.forEach(clearTimeout);
      if (data.road_width_m != null) {
        const idx = state.allProperties.findIndex(x => (x.source_id || x.id) === id);
        if (idx >= 0) {
          Object.assign(state.allProperties[idx], {
            road_width_m: data.road_width_m,
            road_width_name: data.road_name || state.allProperties[idx].road_width_name,
            screenshot_roadwidth: data.screenshot || state.allProperties[idx].screenshot_roadwidth,
            road_width_vision_reason: data.reason || '',
          });
        }
        toast(`已掃描：${data.road_name || ''} ${data.road_width_m}m`, 'success');
        _renderDetailFromCurrent();   // 重 render 讓「地籍圖」按鈕替換掉「掃描中」
        applyFilters();                // 卡片倍數重算
      } else {
        btn.classList.remove('v2-d-road-scan--scanning');
        btn.classList.add('v2-d-road-scan--fail');
        btn.textContent = data.error || '掃描失敗';
        btn.disabled = true;
      }
    } catch (e) {
      timers.forEach(clearTimeout);
      btn.classList.remove('v2-d-road-scan--scanning');
      btn.classList.add('v2-d-road-scan--fail');
      btn.textContent = '掃描失敗';
      btn.disabled = true;
      console.error('scanRoadWidth', e);
    }
  }

  // ── 地籍圖 overlay (對齊 v1 openRoadOverlay 內部跳窗，不外開新分頁) ─────────
  function openRoadOverlay(id) {
    const existing = document.querySelector('.v2-road-overlay');
    if (existing) { existing.remove(); return; }
    const p = state.allProperties.find(x => (x.source_id || x.id) === id);
    if (!p || !p.screenshot_roadwidth) return;
    const overlay = document.createElement('div');
    overlay.className = 'v2-road-overlay';

    // 內容容器 (圖 + 說明文字垂直排列；點 backdrop 才關閉)
    const inner = document.createElement('div');
    inner.className = 'v2-road-overlay-inner';
    inner.addEventListener('click', (e) => e.stopPropagation());
    const img = document.createElement('img');
    img.src = p.screenshot_roadwidth;
    inner.appendChild(img);
    const reason = p.road_width_vision_reason || '';
    if (reason) {
      const r = document.createElement('div');
      r.className = 'v2-road-overlay-reason';
      r.textContent = reason;
      inner.appendChild(r);
    }
    overlay.appendChild(inner);

    // 右上角 X 關閉按鈕
    const closeBtn = document.createElement('button');
    closeBtn.className = 'v2-road-overlay-close';
    closeBtn.setAttribute('aria-label', '關閉');
    closeBtn.textContent = '×';
    closeBtn.addEventListener('click', (e) => { e.stopPropagation(); overlay.remove(); });
    overlay.appendChild(closeBtn);

    overlay.addEventListener('click', () => overlay.remove());
    document.body.appendChild(overlay);
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

        // Backfill：加 watchlist 之前用戶改的 override 全部被 NoopRef 吃掉，
        // 加入後重新 POST 一次讓它們真正寫進 watchlist sub-doc
        if (p._pending_overrides && Object.keys(p._pending_overrides).length) {
          const fieldToEndpoint = {
            desired_price_wan:           [`/api/properties/${encodeURIComponent(id)}/desired_price`,    (v) => ({ desired_price_wan: v })],
            bonus_weishau:               [`/api/properties/${encodeURIComponent(id)}/bonus`,            (v) => ({ which: 'weishau', value: v })],
            bonus_dugen:                 [`/api/properties/${encodeURIComponent(id)}/bonus`,            (v) => ({ which: 'dugen', value: v })],
            rebuild_coeff:               [`/api/properties/${encodeURIComponent(id)}/rebuild_coeff`,    (v) => ({ value: v })],
            floor_premium:               [`/api/properties/${encodeURIComponent(id)}/floor_premium`,    (v) => ({ floor_premium: v })],
            road_width_m_override:       [`/api/properties/${encodeURIComponent(id)}/road_width`,        (v) => ({ road_width_m: v })],
            new_house_price_wan_override:[`/api/properties/${encodeURIComponent(id)}/new_house_price`,   (v) => ({ value: v })],
            zoning_ratios:               [`/api/properties/${encodeURIComponent(id)}/zoning_ratios`,     (v) => ({ zoning_ratios: v })],
          };
          for (const [field, val] of Object.entries(p._pending_overrides)) {
            const ep = fieldToEndpoint[field];
            if (!ep) continue;
            try {
              await fetch(ep[0], {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(ep[1](val)),
              });
            } catch (e) { console.error('backfill', field, e); }
          }
          // 推測地址特殊路徑
          if (p._pending_inferred_choice) {
            try {
              await fetch(`/api/properties/${encodeURIComponent(id)}/inferred_choice`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ address: p._pending_inferred_choice }),
              });
            } catch (e) { console.error('backfill inferred_choice', e); }
            delete p._pending_inferred_choice;
          }
          delete p._pending_overrides;
          p._ephemeral_edit_made = false;
          toast('剛才的數字改動已補存到觀察清單', 'success');
        } else if (p._pending_inferred_choice) {
          try {
            await fetch(`/api/properties/${encodeURIComponent(id)}/inferred_choice`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ address: p._pending_inferred_choice }),
            });
          } catch (e) { console.error('backfill inferred_choice', e); }
          delete p._pending_inferred_choice;
          p._ephemeral_edit_made = false;
        }
      }
      // 即時更新 state + UI（不全部 reload，太慢）
      p._in_watchlist = !isIn;
      const card = document.querySelector(`.v2-card[data-id="${CSS.escape(id)}"] .v2-card__star`);
      if (card) card.classList.toggle('v2-card__star--active', !isIn);
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
    // PERF TIMER：開關 localStorage.setItem('debugPerf','1')
    const _DBG = localStorage.getItem('debugPerf') === '1';
    const _ts = {};
    const _mark = (k) => { if (_DBG) _ts[k] = performance.now(); };
    _mark('start');
    renderSkeletons(8);
    _mark('skeleton_done');
    try {
      let url;
      if (state.view === 'watchlist') {
        url = '/api/properties?limit=500&slim=true';
      } else {
        const params = new URLSearchParams();
        params.set('districts', Object.values(V1_DISTRICTS)
          .flatMap(cfg => cfg.enabled).join(','));
        params.set('slim', 'true');
        params.set('limit', '1000');
        url = '/api/central_search?' + params.toString();
      }
      _mark('fetch_start');
      window.__perfMark && window.__perfMark('central_search_fetch_start');
      // 探索 tab 第一次：若 inline early-fetch 已經 fire 過且成功，直接拿那份結果（省 ~1.5 秒）
      let data = null;
      let _serverTiming = '';
      let _bytes = 0;
      if (state.view !== 'watchlist' && window.__earlyDataPromise && !state._earlyDataConsumed) {
        state._earlyDataConsumed = true;
        try {
          const early = await window.__earlyDataPromise;
          if (early && Array.isArray(early.items)) {
            data = early;
            window.__perfMark && window.__perfMark('central_search_used_early_data', { items: early.items.length });
          }
        } catch (_e) { /* fall through to fresh fetch */ }
      }
      if (!data) {
        // 沒 early data → fallback fresh fetch；需要 auth_gate 包過的 window.fetch
        if (!window.__authReady && typeof _waitForAuthReady === 'function') {
          await _waitForAuthReady();
        }
        const r = await fetch(url);
        _mark('fetch_headers');
        _serverTiming = r.headers.get('server-timing') || '';
        _bytes = +r.headers.get('content-length') || 0;
        window.__perfMark && window.__perfMark('central_search_headers', { server_timing: _serverTiming });
        data = await r.json();
        window.__perfMark && window.__perfMark('central_search_json_parsed', { items: (data.items || []).length, bytes: r.headers.get('content-length') });
      } else {
        _mark('fetch_headers');
        window.__perfMark && window.__perfMark('central_search_headers_skipped_used_early', {});
      }
      _mark('json_parsed');
      const items = data.items || [];
      if (state.view === 'watchlist') {
        state.watchlistItems = items;
        state.watchlistLoaded = true;
      } else {
        state.exploreItems = items;
        state.exploreLoaded = true;
      }
      state.allProperties = items;
      applyFilters();
      _mark('filter_render_done');
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          _mark('painted');
          if (_DBG) _logV2Perf(_ts, items.length, _bytes, _serverTiming);
        });
      });
    } catch (e) {
      console.error('loadProperties', e);
      toast('載入失敗', 'error');
    } finally {
      _hideBootstrapLoader();
    }
  }

  // 首次載入全頁 loading overlay — fade out + remove
  let _bootstrapHidden = false;
  function _hideBootstrapLoader() {
    if (_bootstrapHidden) return;
    _bootstrapHidden = true;
    const el = document.getElementById('v2-bootstrap-loader');
    if (!el) return;
    el.classList.add('v2-bootstrap-loader--hide');
    setTimeout(() => { el.remove(); }, 400);
  }

  function _logV2Perf(ts, nItems, bytes, serverTiming) {
    const T = (a, b) => Math.round(ts[b] - ts[a]);
    const total = Math.round(ts.painted - ts.start);
    const ttfb = T('fetch_start', 'fetch_headers');
    const phases = [];
    let cacheStatus = '';
    if (serverTiming) {
      serverTiming.split(',').forEach(part => {
        const m = part.trim().match(/^([a-z_]+);dur=(\d+(?:\.\d+)?)(?:;desc="([^"]*)")?/i);
        if (m) {
          const name = m[1], ms = Math.round(+m[2]), desc = m[3] || '';
          if (name === 'cache') cacheStatus = desc;
          else phases.push([name, ms]);
        }
      });
    }
    const serverTotal = phases.reduce((s, [_, ms]) => s + ms, 0);
    const networkOverhead = Math.max(0, ttfb - serverTotal);
    const lines = [
      ['skeleton 渲染',          T('start', 'skeleton_done'),       '畫骨架佔位'],
      ['fetch+TTFB+headers',     ttfb,                              `server ${serverTotal}ms + 網路/auth ${networkOverhead}ms${cacheStatus ? ` [${cacheStatus}]` : ''}`],
    ];
    phases.forEach(([name, ms]) => lines.push([`  └─ ${name}`, ms, '']));
    lines.push(['response body 讀+JSON.parse', T('fetch_headers', 'json_parsed'), '']);
    lines.push(['filter+sort+render',         T('json_parsed', 'filter_render_done'), '']);
    lines.push(['等到 paint',                  T('filter_render_done', 'painted'),     'requestAnimationFrame×2']);
    lines.push(['─ TOTAL',                     total,                                  `${nItems} items, ${(bytes/1024).toFixed(1)} KB`]);
    console.log('%c[v2 perf]', 'color:#0e7490;font-weight:700', `${nItems} items / ${total}ms`);
    console.table(Object.fromEntries(lines.map(([k, ms, note]) => [k, { ms, note }])));
  }

  // 「重新搜尋」按鈕：強制重抓中央 (跳過 client cache)
  // 用戶要求 #5：filter 純 client 端跑；只有此 button 才打 server
  async function runSearch() {
    if (state.view !== 'explore') switchView('explore');
    state.exploreLoaded = false;     // force refetch
    _autoCloseSidebarOnMobile();
    await loadProperties();
  }

  // 手機按下任何 sidebar 動作鈕後，自動把 menu 縮回（看結果不被 menu 擋住）
  function _autoCloseSidebarOnMobile() {
    if (window.matchMedia('(max-width: 1024px)').matches) {
      closeSidebar();
    }
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

  // sidebar 內 inline message helper (錯誤訊息一律在 menu 出現，不用 toast)
  // kind: '' (默默) / 'info' (黃) / 'error' (紅) / 'success' (綠) / 'pending' (進行中)
  function setCapMsg(slotId, kind, text) {
    const el = document.getElementById(slotId);
    if (!el) return;
    if (!text) {
      el.className = 'v2-cap__msg';
      el.innerHTML = '';
      return;
    }
    el.className = 'v2-cap__msg v2-cap__msg--' + (kind || 'info');
    el.textContent = text;
  }

  // 在列表頂部塞一個「分析中…」placeholder card；返回 placeholder source_id 給 caller
  // 完成後 loadProperties() 抓真實 doc 自然取代 placeholder
  function _addPendingPlaceholder(label, kind /* 'url' | 'manual' */) {
    const pid = '_pending_' + kind + '_' + Date.now();
    const placeholder = {
      source_id: pid,
      _pending_analysis: true,
      _pending_label: label,
      _pending_kind: kind,
      address: label,
      address_inferred: label,
      district: '分析中',
      city: '台北市',
      _added_at: new Date().toISOString(),
    };
    state.allProperties = [placeholder, ...(state.allProperties || [])];
    state.exploreItems = [placeholder, ...(state.exploreItems || [])];
    applyFilters();
    return pid;
  }
  function _removePendingPlaceholder(pid) {
    if (!pid) return;
    state.allProperties = (state.allProperties || []).filter(p => (p.source_id || p.id) !== pid);
    state.exploreItems = (state.exploreItems || []).filter(p => (p.source_id || p.id) !== pid);
  }

  // 兩個 capture 卡的「送出分析」按鈕，分析期間一律 disable 兩顆，避免併發
  function _setCaptureButtonsDisabled(disabled) {
    ['v2-cap-scrape-btn', 'v2-cap-manual-btn'].forEach(id => {
      const b = document.getElementById(id);
      if (!b) return;
      b.disabled = !!disabled;
      // textContent 不動，僅靠 :disabled CSS 灰化即可
    });
  }

  async function triggerScrapeUrl() {
    const inp = $('#v2-scrape-url');
    const url = (inp?.value || '').trim();
    if (!url) { setCapMsg('v2-cap-scrape-msg', 'error', '請輸入網址'); return; }
    const okPatterns = [
      /sale\.591\.com\.tw\/.*\d{6,}/,
      /buy\.yungching\.com\.tw\/house\/\d{6,8}/,
      /sinyi\.com\.tw\/buy\/house\/[A-Z0-9]{4,8}/i,
    ];
    if (!okPatterns.some(re => re.test(url))) {
      setCapMsg('v2-cap-scrape-msg', 'error', '看起來不是 591/永慶/信義 詳情頁網址');
      return;
    }
    inp.disabled = true;
    _setCaptureButtonsDisabled(true);
    setCapMsg('v2-cap-scrape-msg', 'pending', '分析中…（佇列忙時可能要 30-60 秒）');
    const pid = _addPendingPlaceholder(url, 'url');
    _autoCloseSidebarOnMobile();
    try {
      const r = await fetch('/api/scrape_url', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url }),
      });
      const data = await r.json();
      if (data.status === 'ok') {
        setCapMsg('v2-cap-scrape-msg', 'success', '處理完成，已加入最愛');
        inp.value = '';
        _removePendingPlaceholder(pid);
        await loadProperties();
      } else if (data.status === 'busy') {
        setCapMsg('v2-cap-scrape-msg', 'error', '佇列繁忙：' + (data.message || ''));
        _removePendingPlaceholder(pid);
        applyFilters();
      } else if (data.status === 'skipped_non_apartment') {
        setCapMsg('v2-cap-scrape-msg', 'error', '跳過：' + (data.message || '非公寓 (>5F)'));
        inp.value = '';
        _removePendingPlaceholder(pid);
        applyFilters();
      } else if (data.status === 'error') {
        setCapMsg('v2-cap-scrape-msg', 'error', '分析失敗：' + (data.message || data.detail || 'unknown'));
        _removePendingPlaceholder(pid);
        applyFilters();
      } else {
        setCapMsg('v2-cap-scrape-msg', 'error', '未預期回應 (' + data.status + ')');
        _removePendingPlaceholder(pid);
        applyFilters();
      }
    } catch (e) {
      setCapMsg('v2-cap-scrape-msg', 'error', '失敗：' + e.message);
      _removePendingPlaceholder(pid);
      applyFilters();
    } finally {
      inp.disabled = false;
      _setCaptureButtonsDisabled(false);
    }
  }

  async function triggerManualAnalyze() {
    const city = $('#v2-manual-city')?.value || '';
    const district = $('#v2-manual-district')?.value || '';
    const address = ($('#v2-manual-address')?.value || '').trim();
    const bld = parseFloat($('#v2-manual-bld')?.value);
    const land = parseFloat($('#v2-manual-land')?.value);
    const price = parseFloat($('#v2-manual-price')?.value);
    if (!address) { setCapMsg('v2-cap-manual-msg', 'error', '請輸入地址'); return; }
    const nonTarget = /^(桃園|基隆|新竹|苗栗|台中|臺中|彰化|南投|雲林|嘉義|台南|臺南|高雄|屏東|宜蘭|花蓮|台東|臺東|澎湖|金門|連江)/;
    if (!['台北市', '新北市'].includes(city) || nonTarget.test(address)) {
      setCapMsg('v2-cap-manual-msg', 'error', '目前僅支援台北/新北地址');
      return;
    }
    setCapMsg('v2-cap-manual-msg', 'pending', '分析中…（後台處理約 10-30 秒）');
    _setCaptureButtonsDisabled(true);
    const label = `${city}${district}${address}`;
    const pid = _addPendingPlaceholder(label, 'manual');
    _autoCloseSidebarOnMobile();

    // 後端 status 通用錯誤訊息（讀 data.error，fallback data.message）
    const errMsg = (data) => data.error || data.message || '';

    // 進入 polling 後 re-enable 由 polling 結束接手；其他結束分支立即 re-enable
    let _enteredPolling = false;
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
        setCapMsg('v2-cap-manual-msg', 'pending', '分析中…等候後台完成（自動更新，不用重新整理）');
        _enteredPolling = true;
        _pollManualAnalysisDone(pid, 'v2-cap-manual-msg').finally(() => {
          _setCaptureButtonsDisabled(false);
        });
      } else if (data.status === 'already_in_db') {
        setCapMsg('v2-cap-manual-msg', 'success', '該地址已在 DB（已加入觀察清單）');
        _removePendingPlaceholder(pid);
        await loadProperties();
      } else if (data.status === 'district_mismatch') {
        // 後端回 candidates: [{city, district, address, formatted}, ...]
        // → 顯示錯誤訊息 + 可點選的候選區按鈕，幫用戶一鍵改正
        const cands = Array.isArray(data.candidates) ? data.candidates : [];
        _renderManualCandidates('v2-cap-manual-msg', errMsg(data) || '地址與所選區不符', cands);
        _removePendingPlaceholder(pid);
        applyFilters();
      } else if (data.status === 'not_found') {
        // 後端可能回 suggestions: [str, ...]（或舊格式對 fuzzy 建議）
        const sugg = Array.isArray(data.suggestions) ? data.suggestions : [];
        if (sugg.length) {
          _renderManualSuggestions('v2-cap-manual-msg', errMsg(data) || '找不到該地址', sugg);
        } else {
          setCapMsg('v2-cap-manual-msg', 'error', errMsg(data) || '找不到該地址');
        }
        _removePendingPlaceholder(pid);
        applyFilters();
      } else if (data.status === 'lvr_mismatch') {
        setCapMsg('v2-cap-manual-msg', 'error', errMsg(data) || '實價登錄資料對不到');
        _removePendingPlaceholder(pid);
        applyFilters();
      } else if (data.status === 'error') {
        setCapMsg('v2-cap-manual-msg', 'error', errMsg(data) || '輸入有誤');
        _removePendingPlaceholder(pid);
        applyFilters();
      } else {
        setCapMsg('v2-cap-manual-msg', 'error', '回應 (' + data.status + ')：' + errMsg(data));
        _removePendingPlaceholder(pid);
        applyFilters();
      }
    } catch (e) {
      setCapMsg('v2-cap-manual-msg', 'error', '失敗：' + e.message);
      _removePendingPlaceholder(pid);
      applyFilters();
    } finally {
      // 進 polling 的 case：由 polling .finally 處理，這裡跳過避免提早 re-enable
      if (!_enteredPolling) _setCaptureButtonsDisabled(false);
    }
  }

  // district_mismatch 時的 UI：訊息 + 候選按鈕（點擊 → 自動切到對應 city/district 並重送）
  function _renderManualCandidates(slotId, msg, candidates) {
    const el = document.getElementById(slotId);
    if (!el) return;
    const safe = (s) => String(s || '').replace(/[<>&"']/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;',"'":'&#39;'}[c]));
    const btns = candidates.map((c, i) =>
      `<button type="button" class="v2-cap__cand-btn" data-cand="${i}">${safe(c.formatted || (c.city + c.district))}</button>`
    ).join('');
    el.className = 'v2-cap__msg v2-cap__msg--error';
    el.innerHTML = `<div style="margin-bottom:6px;">${safe(msg)}</div>` +
      (btns ? `<div class="v2-cap__cand-list">${btns}</div>` : '');
    el.querySelectorAll('button[data-cand]').forEach(b => {
      b.addEventListener('click', () => {
        const idx = +b.getAttribute('data-cand');
        const c = candidates[idx];
        if (!c) return;
        // 切 city → 等 district 列表 populate → 切 district + address → 重送
        const cityEl = $('#v2-manual-city');
        const distEl = $('#v2-manual-district');
        const addrEl = $('#v2-manual-address');
        if (cityEl && c.city) {
          cityEl.value = c.city;
          if (typeof populateManualDistricts === 'function') populateManualDistricts();
        }
        setTimeout(() => {
          if (distEl && c.district) distEl.value = c.district;
          if (addrEl && c.address) addrEl.value = c.address;
          triggerManualAnalyze();
        }, 50);
      });
    });
  }

  // not_found 的 fuzzy 建議：純字串 list，點擊填入 address 欄位（不自動重送，讓用戶確認）
  function _renderManualSuggestions(slotId, msg, suggestions) {
    const el = document.getElementById(slotId);
    if (!el) return;
    const safe = (s) => String(s || '').replace(/[<>&"']/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;',"'":'&#39;'}[c]));
    const btns = suggestions.slice(0, 8).map((s, i) =>
      `<button type="button" class="v2-cap__cand-btn" data-sugg="${i}">${safe(s)}</button>`
    ).join('');
    el.className = 'v2-cap__msg v2-cap__msg--error';
    el.innerHTML = `<div style="margin-bottom:6px;">${safe(msg)}</div>` +
      (btns ? `<div class="v2-cap__cand-list">${btns}</div>` : '');
    el.querySelectorAll('button[data-sugg]').forEach(b => {
      b.addEventListener('click', () => {
        const idx = +b.getAttribute('data-sugg');
        const s = suggestions[idx];
        const addrEl = $('#v2-manual-address');
        if (addrEl && s) {
          addrEl.value = s;
          addrEl.focus();
        }
      });
    });
  }

  // 輪詢 manual analyze 完成 (5/10/15/20/30/45/60 秒重抓，看到新物件就停)
  async function _pollManualAnalysisDone(pid, msgSlot) {
    const intervals = [5000, 5000, 5000, 5000, 10000, 15000, 15000];   // total ~60s
    const before = new Set((state.allProperties || []).filter(p => !p._pending_analysis).map(p => p.source_id || p.id));
    for (const wait of intervals) {
      await new Promise(r => setTimeout(r, wait));
      try {
        const params = new URLSearchParams();
        params.set('districts', Object.values(V1_DISTRICTS).flatMap(c => c.enabled).join(','));
        params.set('slim', 'true');
        params.set('limit', '1000');
        const r = await fetch('/api/central_search?' + params.toString());
        const data = await r.json();
        const items = data.items || [];
        const newItem = items.find(it => !before.has(it.source_id || it.id));
        if (newItem) {
          state.exploreItems = items;
          state.allProperties = items;
          _removePendingPlaceholder(pid);
          applyFilters();
          setCapMsg(msgSlot, 'success', '分析完成，已加入最愛');
          return;
        }
      } catch (e) { /* keep polling */ }
    }
    // timeout
    _removePendingPlaceholder(pid);
    applyFilters();
    setCapMsg(msgSlot, 'error', '分析超時（>60 秒），請稍後手動重新整理或重抓');
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
    $('#v2-school') && ($('#v2-school').value = '');
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
    // 直接設 input.value 不會觸發 'input' event，mobile slider DOM 不會跟著更新
    // → 用戶體感「reset 沒 work」(數字改了但 slider 沒移動)。同步 slider 位置
    _syncSlidersFromInputs();
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

  function _waitForAuthReady() {
    if (window.__authReady) return Promise.resolve();
    return new Promise(resolve => document.addEventListener('auth:ready', resolve, { once: true }));
  }

  // 拉桿「不限」sentinel — 拉到最頂端時 input value 變這個大數字，filter `< val` 對任何值都 true
  const UNLIMITED_VAL = 999999;
  const isUnlimitedVal = (v) => Number(v) >= UNLIMITED_VAL;

  // Piecewise mapping for 總價 (0~3 億)：低值精細、高值粗
  // 低段拖時每格 50 萬，中段每格 ~200 萬，高段每格 ~500 萬
  // slider position 0..1000 (linear) ↔ real value 0..30000 (萬)
  // breakpoints: pos 0..500 → 0..3000, pos 500..800 → 3000..10000, pos 800..1000 → 10000..30000
  const _PIECEWISE_BPS = [
    { pos: 0,    val: 0     },
    { pos: 500,  val: 3000  },   // 500 步 → 3000 萬，每步 6 萬 (但 step=10 萬實際)
    { pos: 800,  val: 10000 },   // 300 步 → 7000 萬，每步 23 萬
    { pos: 1000, val: 30000 },   // 200 步 → 20000 萬，每步 100 萬
  ];
  function _piecewisePosToVal(pos) {
    pos = Math.max(0, Math.min(1000, pos));
    for (let i = 0; i < _PIECEWISE_BPS.length - 1; i++) {
      const a = _PIECEWISE_BPS[i], b = _PIECEWISE_BPS[i + 1];
      if (pos >= a.pos && pos <= b.pos) {
        const ratio = (pos - a.pos) / (b.pos - a.pos);
        return Math.round((a.val + ratio * (b.val - a.val)) / 10) * 10;  // round to 10 萬
      }
    }
    return _PIECEWISE_BPS[_PIECEWISE_BPS.length - 1].val;
  }
  function _piecewiseValToPos(val) {
    val = Math.max(0, Math.min(30000, val));
    for (let i = 0; i < _PIECEWISE_BPS.length - 1; i++) {
      const a = _PIECEWISE_BPS[i], b = _PIECEWISE_BPS[i + 1];
      if (val >= a.val && val <= b.val) {
        const ratio = (val - a.val) / (b.val - a.val);
        return Math.round(a.pos + ratio * (b.pos - a.pos));
      }
    }
    return _PIECEWISE_BPS[_PIECEWISE_BPS.length - 1].pos;
  }

  // PC + mobile 都把 sidebar number input 換成 slider — UX 一致
  // 條件：input 有 data-slider-max attr 才 enhance（避免動到地址分析卡的精確輸入）。
  // data-slider-curve="piecewise" → 非線性 mapping (0~3 億 區間)
  function _enhanceSlidersForMobile() {
    document.querySelectorAll('.v2-sidebar input.v2-input--num[type="number"]').forEach(inp => {
      if (inp.dataset.sliderEnhanced) return;
      const max = inp.dataset.sliderMax;
      if (!max) return;
      inp.dataset.sliderEnhanced = '1';
      const piecewise = inp.dataset.sliderCurve === 'piecewise';
      const allowUnlimited = inp.dataset.sliderAllowUnlimited === '1';
      const wrap = document.createElement('div');
      wrap.className = 'v2-mobile-slider-wrap';
      const label = document.createElement('div');
      label.className = 'v2-mobile-slider-label';
      const labelText = inp.dataset.sliderLabel || '';
      const suffix = inp.dataset.sliderSuffix || '';
      const initUnl = allowUnlimited && isUnlimitedVal(inp.value);
      label.innerHTML =
        `<span class="v2-mobile-slider-label__txt">${labelText}</span>` +
        `<span class="v2-mobile-slider-label__val">${initUnl ? '不限' : inp.value}</span>` +
        `<span class="v2-mobile-slider-label__suffix"${initUnl ? ' style="display:none"' : ''}>${suffix}</span>`;
      const slider = document.createElement('input');
      slider.type = 'range';
      slider.className = 'v2-mobile-slider';
      // 拉桿最頂端視為「不限」 — slider max 加一格 sentinel；piecewise 用 1001
      const stepN = Number(inp.step) || 1;
      if (piecewise) {
        slider.min = 0;
        slider.max = allowUnlimited ? 1001 : 1000;
        slider.step = 1;
        slider.value = initUnl ? slider.max : _piecewiseValToPos(parseFloat(inp.value) || 0);
      } else {
        slider.min = inp.min || 0;
        slider.max = allowUnlimited ? (Number(max) + stepN) : max;
        slider.step = stepN;
        slider.value = initUnl ? slider.max : inp.value;
      }
      const valSpan = label.querySelector('.v2-mobile-slider-label__val');
      const suffixSpan = label.querySelector('.v2-mobile-slider-label__suffix');
      const setLabel = (v) => {
        const unl = allowUnlimited && isUnlimitedVal(v);
        valSpan.textContent = unl ? '不限' : v;
        if (suffixSpan) suffixSpan.style.display = unl ? 'none' : '';
      };
      const oninputAttr = (inp.getAttribute('oninput') || '').trim();
      const callApply = () => {
        if (oninputAttr.includes('applySort')) applySort();
        else applyFilters();
      };
      slider.addEventListener('input', () => {
        const sliderN = parseInt(slider.value, 10);
        let realVal;
        if (allowUnlimited && sliderN === parseInt(slider.max, 10)) {
          realVal = UNLIMITED_VAL;
        } else if (piecewise) {
          realVal = _piecewisePosToVal(sliderN);
        } else {
          realVal = sliderN;
        }
        if (String(inp.value) !== String(realVal)) {
          inp.value = realVal;
          setLabel(realVal);
          callApply();
        }
      });
      inp.addEventListener('input', () => {
        const inpN = parseFloat(inp.value) || 0;
        let desiredPos;
        if (allowUnlimited && isUnlimitedVal(inpN)) {
          desiredPos = parseInt(slider.max, 10);
        } else if (piecewise) {
          desiredPos = _piecewiseValToPos(inpN);
        } else {
          desiredPos = inpN;
        }
        if (String(slider.value) !== String(desiredPos)) {
          slider.value = desiredPos;
        }
        setLabel(inp.value);
      });
      wrap.appendChild(label);
      wrap.appendChild(slider);
      inp.parentNode.insertBefore(wrap, inp.nextSibling);
    });
  }

  // 數字輸入 cap：onchange 時若超過 max / 低於 min 自動 clamp 並重新 trigger filter。
  // 桌面用戶 type 超過 max 也會被砍回上限；mobile slider 已經 max 限制不會超過。
  function capInput(el) {
    if (!el) return;
    const v = parseFloat(el.value);
    if (isNaN(v)) return;
    const max = parseFloat(el.max);
    const min = parseFloat(el.min);
    let changed = false;
    if (!isNaN(max) && v > max) { el.value = max; changed = true; }
    else if (!isNaN(min) && v < min) { el.value = min; changed = true; }
    if (changed) {
      // re-trigger 對應 callback (oninput attribute)
      const oninputAttr = (el.getAttribute('oninput') || '').trim();
      if (oninputAttr.includes('applySort')) applySort();
      else applyFilters();
    }
  }

  // 路名 / 學區語音輸入 — webkitSpeechRecognition (auto-end on speech pause)
  // 點 mic → 開始辨識 → 偵測到 silence 後自動 end → 把 final transcript 寫入 input
  // iOS WKWebView 的 OS 層 mic indicator 釋放有延遲是 known limitation，這版不處理。
  function _showVoiceStatus(text, kind) {
    let host = $('#v2-voice-banner');
    if (!host) {
      host = document.createElement('div');
      host.id = 'v2-voice-banner';
      document.body.appendChild(host);
    }
    host.className = 'v2-voice-banner' + (kind ? ' v2-voice-banner--' + kind : '');
    host.textContent = text;
    host.style.display = 'block';
    if (kind === 'error' || kind === 'success') {
      setTimeout(() => { if (host.textContent === text) host.style.display = 'none'; }, 3000);
    }
  }
  const _VOICE_ERR_TXT = {
    'no-speech': '沒偵測到語音 — 請靠近麥克風再試一次',
    'audio-capture': '麥克風無法使用 — 請檢查裝置麥克風',
    'not-allowed': '瀏覽器拒絕麥克風權限 — 請在設定→Safari/Chrome 開啟麥克風',
    'service-not-allowed': '系統拒絕語音服務 — 改用其他瀏覽器試試',
    'network': '語音辨識需要網路連線',
    'language-not-supported': '不支援中文辨識',
    'aborted': '語音辨識被取消',
  };

  function _startVoice(inputId, btnId) {
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    const inp = $('#' + inputId);
    const btn = $('#' + btnId);
    if (!SR) {
      _showVoiceStatus('此瀏覽器不支援語音輸入。Chrome 或 iOS Safari 較新版本才支援', 'error');
      return;
    }
    if (!inp) return;
    if (!window.isSecureContext) {
      _showVoiceStatus('語音輸入只能在 HTTPS 環境用', 'error');
      return;
    }
    const rec = new SR();
    rec.lang = 'zh-TW';
    rec.interimResults = true;
    rec.maxAlternatives = 1;
    rec.continuous = false;
    if (btn) btn.classList.add('v2-road-mic--active');
    let gotAnyResult = false;
    rec.onaudiostart = () => _showVoiceStatus('🎤 麥克風已開啟，請說話…', 'listening');
    rec.onspeechstart = () => _showVoiceStatus('🎙 偵測到聲音，繼續說…', 'listening');
    rec.onresult = (e) => {
      let interim = '', final = '';
      for (let i = e.resultIndex; i < e.results.length; i++) {
        const tr = e.results[i][0].transcript || '';
        if (e.results[i].isFinal) final += tr;
        else interim += tr;
      }
      if (interim) {
        gotAnyResult = true;
        _showVoiceStatus('辨識中：' + interim, 'listening');
      }
      if (final) {
        gotAnyResult = true;
        const text = final.trim().replace(/\s+/g, '').replace(/。$/, '');
        inp.value = text;
        inp.dispatchEvent(new Event('input', { bubbles: true }));
        _showVoiceStatus('✓ 已輸入：' + text, 'success');
      }
    };
    rec.onnomatch = () => _showVoiceStatus('沒辨識出內容，再試一次', 'error');
    rec.onerror = (ev) => {
      _showVoiceStatus(_VOICE_ERR_TXT[ev.error] || ('語音辨識錯誤：' + ev.error), 'error');
      if (btn) btn.classList.remove('v2-road-mic--active');
    };
    rec.onend = () => {
      if (btn) btn.classList.remove('v2-road-mic--active');
      if (!gotAnyResult) {
        const host = $('#v2-voice-banner');
        if (!host || host.className.indexOf('--error') < 0) {
          _showVoiceStatus('未偵測到語音，請靠近麥克風再試一次', 'error');
        }
      }
    };
    try {
      rec.start();
    } catch (e) {
      _showVoiceStatus('語音啟動失敗：' + (e.message || e), 'error');
      if (btn) btn.classList.remove('v2-road-mic--active');
    }
  }
  function startVoiceRoad() { _startVoice('v2-road', 'v2-road-mic'); }
  function startVoiceSchool() { _startVoice('v2-school', 'v2-school-mic'); }

  // ── Boot ─────────────────────────────────────────────────────────────────
  // 地圖模式 (setViewMode / renderMap / _initMap / _maybeShowViewToggle) 在獨立檔
  // frontend/static/map_mode.js — 透過 window.v2 attach 進來。本檔只留 hook 點：
  //   - state.viewMode/_mapInst/_mapMarkers (initial state)
  //   - renderGrid 開頭 short-circuit (call window.v2._renderMap)
  //   - _collectFilterObj / _restoreFilters viewMode 持久化 + access guard

  async function boot() {
    window.__perfMark && window.__perfMark('app2_boot_start');
    // mobile menu button
    $('#v2-menu-btn')?.addEventListener('click', openSidebar);

    // 預設勾選所有 enabled district (對齊 v1 default 全勾)
    Object.entries(V1_DISTRICTS).forEach(([city, cfg]) => {
      cfg.enabled.forEach(d => state.districtPicks.add(`${city}|${d}`));
    });
    // 手機把 sidebar number input 包成 slider（dom 寫死的，立刻可 enhance）
    _enhanceSlidersForMobile();
    // 還原上次 filter 偏好：_filterKey() 用 currentUser.uid 做 key，必須等 auth ready
    // 才能讀到正確 uid 的儲存值（auth_gate 之前 uid 是 'anon' → 拿不到）。
    // 為了不擋 data fetch，這裡把 restore 排到 auth_ready 後背景跑；ready 後若資料已載完
    // 就 re-apply 套 filter，沒載完則資料載完時 applyFilters 會用 restored 值。
    const _restoreP = _waitForAuthReady().then(async () => {
      await _restoreFilters();   // DB 優先，fallback localStorage
      if (typeof renderDistrictChips === 'function') renderDistrictChips();
      if (state.exploreLoaded || state.watchlistLoaded) applyFilters();
    });

    // loadDistricts 是 public endpoint，立刻 fire 不等 auth
    // loadProperties：有 early data 立刻；沒有才等 auth_ready (因為 fallback fetch 需要 token)
    window.__perfMark && window.__perfMark('parallel_fetch_start');
    const distP = loadDistricts().then(() => window.__perfMark && window.__perfMark('loadDistricts_done'));
    const propsP = (async () => {
      if (!window.__earlyDataPromise) {
        await _waitForAuthReady();
        window.__perfMark && window.__perfMark('app2_auth_ready_seen');
      }
      await loadProperties();
      window.__perfMark && window.__perfMark('loadProperties_done');
    })();

    await Promise.all([distP, propsP, _restoreP]);
    window.__perfMark && window.__perfMark('boot_complete');
    requestAnimationFrame(() => requestAnimationFrame(() => {
      window.__perfMark && window.__perfMark('first_paint_after_boot');
      _renderPerfPanel();
    }));
    // Deep-link：URL 帶 ?id=<doc_id> → 自動打開該物件 detail drawer
    // (LINE 通知 / 分享連結點進來會直接看到對應物件)
    try {
      const params = new URLSearchParams(location.search);
      const targetId = params.get('id');
      if (targetId) {
        const exists = (state.allProperties || []).some(
          p => (p.source_id || p.id) === targetId
        );
        if (exists) {
          openDetail(targetId);
        }
      }
    } catch (_e) { /* no-op */ }
  }

  function _renderPerfPanel() {
    if (!window.__PERF || !window.__PERF.enabled) return;
    if (document.getElementById('v2-perf-panel')) return;
    const marks = window.__PERF.marks;
    let html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;"><b>⏱ Boot timing</b><button onclick="document.getElementById(\'v2-perf-panel\').remove()" style="background:#444;color:#fff;border:0;padding:2px 6px;border-radius:3px;cursor:pointer;">×</button></div>';
    html += '<table style="font-family:Consolas,monospace;font-size:11px;border-collapse:collapse;color:#fff;">';
    html += '<tr style="color:#aaa;border-bottom:1px solid #555;"><th style="text-align:left;padding:2px 8px;">phase</th><th style="text-align:right;padding:2px 8px;">+ms</th><th style="text-align:right;padding:2px 8px;">Δms</th></tr>';
    let prev = 0;
    marks.forEach(m => {
      const delta = Math.round(m.t - prev);
      const cls = delta > 500 ? 'color:#f87171;' : delta > 200 ? 'color:#fbbf24;' : '';
      html += `<tr style="${cls}"><td style="padding:1px 8px;">${m.name}</td><td style="text-align:right;padding:1px 8px;">${m.t.toFixed(1)}</td><td style="text-align:right;padding:1px 8px;">+${delta}</td></tr>`;
      prev = m.t;
    });
    html += '</table>';
    html += '<div style="margin-top:6px;color:#aaa;font-size:10px;">關閉：localStorage.removeItem(\'v2_perf\') 或拿掉 ?perf=1</div>';
    const panel = document.createElement('div');
    panel.id = 'v2-perf-panel';
    panel.style.cssText = 'position:fixed;bottom:8px;right:8px;z-index:99999;background:rgba(20,20,28,0.96);color:#fff;padding:10px 14px;border-radius:6px;box-shadow:0 4px 20px rgba(0,0,0,0.4);max-width:520px;font-size:12px;';
    panel.innerHTML = html;
    document.body.appendChild(panel);
    // 同時 console.table 給開發者 copy
    console.table(marks.map(m => ({ phase: m.name, '+ms': m.t.toFixed(1), extra: m.extra ? JSON.stringify(m.extra) : '' })));
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

  // 強制重整（給 iOS 桌面 PWA 用）— 清 caches API + 加時間戳 → 不會吃到舊版
  async function hardReload() {
    try {
      if ('caches' in window) {
        const names = await caches.keys();
        await Promise.all(names.map(n => caches.delete(n)));
      }
      if ('serviceWorker' in navigator) {
        const regs = await navigator.serviceWorker.getRegistrations();
        await Promise.all(regs.map(r => r.unregister()));
      }
    } catch (_e) { /* 失敗也照樣 reload */ }
    const u = new URL(window.location.href);
    u.searchParams.set('_t', Date.now());
    window.location.replace(u.toString());
  }

  window.v2 = {
    switchView, toggleDistrict, applyFilters, applySort, runSearch,
    resetFilters, gotoPage, openSidebar, closeSidebar,
    openDetail, closeDetail, toggleWatchlist, toggleDetailWatchlist, logout,
    toggleAllFloors, onFloorChange,
    toggleAllInCity, toggleSortDir,
    triggerScrapeUrl, triggerManualAnalyze, populateManualDistricts,
    switchGridCity,
    showLvrPopup, hideLvrPopup,
    saveOverride, saveInferredChoice, setZonePing,
    // 給 map_mode.js (獨立檔) 用：state、helpers，map_mode.js 透過 window.v2 取
    state, getDistrictPrices, _saveFilters,
    openRoadOverlay, scanRoadWidth, deleteRow,
    hardReload,
    startVoiceRoad,
    startVoiceSchool,
    capInput,
  };

  // Boot
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
