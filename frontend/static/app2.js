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
  const state = {
    view: 'explore',           // 'explore' | 'watchlist'
    allProperties: [],
    filteredSorted: [],
    page: 1,
    pageSize: 24,
    selectedId: null,
    targetRegions: {},
    districtPicks: new Set(),  // city|district 字串
  };

  // ── Helpers ──────────────────────────────────────────────────────────────
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));
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
  let DISTRICT_PRICE_CACHE = null;
  async function getDistrictPrices() {
    if (DISTRICT_PRICE_CACHE) return DISTRICT_PRICE_CACHE;
    try {
      const r = await fetch('/api/district_new_house_price');
      DISTRICT_PRICE_CACHE = await r.json();
    } catch {
      DISTRICT_PRICE_CACHE = {};
    }
    return DISTRICT_PRICE_CACHE;
  }

  function isLandSuspicious(p) {
    if (p.building_type === '透天厝') return false;
    const land = Number(p.land_area_ping) || 0;
    const build = Number(p.building_area_ping) || 0;
    return build > 0 && land > build;
  }

  // 取後端已存的「投入欄位」即時算倍數 — 對齊 calculate_renewal_scenarios 的核心邏輯
  // 注意：CLAUDE.md 規則 8，DB 不存 multiple，此處跟 app.js 同樣即時算
  function rowMultiple(p, prices) {
    if (p.is_foreclosure || p.is_remote_area || p.unsuitable_for_renewal || isLandSuspicious(p)) {
      return null;
    }
    const land = Number(p.land_area_ping) || 0;
    const price = p.new_house_price_wan_override ?? prices[p.district];
    const farPct = Number(p.effective_far_pct ?? p.base_far_pct);
    if (!land || !farPct || !price) return null;
    const coeff = p.rebuild_coeff ?? 1.57;
    const ratio = lookupShareRatio(price)[0];
    const parking = lookupShareRatio(price)[1];
    if (!ratio) return null;
    const is1F = Number(p.floor) === 1 || Number(p.floor_range_min) === 1;
    const floorPremium = p.floor_premium ?? (is1F ? 0.20 : 0);
    const effectivePrice = price * (1 + floorPremium);
    const bonus = 0.50;   // 都更 default
    const share = land * (farPct/100) * (1+bonus) * coeff * ratio;
    const total = share * effectivePrice + (share / 40) * parking;
    const listWan = (p.price_ntd || 0) / 10000;
    const desired = listWan ? Math.round(listWan * 0.9 / 10) * 10 : 0;
    if (!desired) return null;
    return total / desired;
  }

  // 簡化 share_ratio_table（從 share_ratio_table.py 抄常用區段；中間值線性內插）
  const SHARE_TABLE = [
    [60, 0.45, 234], [70, 0.42, 234], [80, 0.40, 234],
    [90, 0.38, 234], [100, 0.36, 234], [110, 0.35, 234],
    [120, 0.34, 234], [130, 0.33, 234], [140, 0.32, 234],
    [150, 0.31, 234], [160, 0.30, 234], [170, 0.30, 240],
    [180, 0.29, 250], [200, 0.28, 270],
  ];
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

  // ── Source badges ────────────────────────────────────────────────────────
  function srcBadgesHTML(sources) {
    if (!sources || !sources.length) return '';
    return sources.map(s => {
      const name = s.name || '';
      const alive = s.alive !== false;
      const cls = alive ? 'v2-src-badge--alive' : 'v2-src-badge--dead';
      const url = s.url ? esc(s.url) : '';
      const inner = `<span class="v2-src-badge ${cls}">${esc(name)}</span>`;
      return url
        ? `<a href="${url}" target="_blank" rel="noopener" onclick="event.stopPropagation()">${inner}</a>`
        : inner;
    }).join('');
  }

  // ── Render: card ─────────────────────────────────────────────────────────
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
    const start = (state.page - 1) * state.pageSize;
    const slice = list.slice(start, start + state.pageSize);
    grid.innerHTML = slice.map(p => cardHTML(p, prices)).join('');
    renderPagination(total);

    // bind clicks
    $$('.v2-card').forEach(el => {
      const id = el.dataset.id;
      el.addEventListener('click', (e) => {
        if (e.target.closest('a')) return;       // source link clicks pass through
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
    const addr = p.address || p.address_inferred || '—';
    const priceWan = p.price_ntd ? Math.round(p.price_ntd / 10000) : null;
    const perBld = (p.price_ntd && p.building_area_ping)
      ? (p.price_ntd / 10000 / p.building_area_ping).toFixed(1) : null;
    const mult = rowMultiple(p, prices);
    let multCls = 'v2-card__multi-num';
    if (mult != null) {
      if (mult >= 3.0) multCls += ' v2-card__multi-num--good';
      else if (mult >= 2.0) multCls += ' v2-card__multi-num--mid';
    }
    const chips = computeChips(p);
    const inWatchlist = !!(p.user_url || p.added_at_user);
    const archivedClass = p.archived ? 'v2-card--archived' : '';

    return `
      <article class="v2-card ${archivedClass}" data-id="${esc(id)}">
        <div class="v2-card__head">
          <div class="v2-card__title-line">
            <span class="v2-card__type">${typeIcon(p.building_type)}</span>
            <span class="v2-card__addr">
              <span class="v2-card__district">${esc(p.district || '')}</span> · ${esc(addr)}
            </span>
          </div>
          <div class="v2-card__price-line">
            <span class="v2-card__price">${priceWan ? fmt0(priceWan) : '—'}<small>萬</small></span>
            ${perBld ? `<span class="v2-card__price-per">${perBld} 萬/建坪</span>` : ''}
          </div>
        </div>
        <div class="v2-card__multi">
          <div class="${multCls}">${mult != null ? mult.toFixed(1) : '—'}</div>
          <div class="v2-card__multi-label">都更倍數</div>
        </div>
        <div class="v2-card__stats">
          <span class="v2-stat"><span class="v2-stat__label">建</span><span class="v2-stat__value">${fmt1(p.building_area_ping)}</span></span>
          <span class="v2-stat"><span class="v2-stat__label">地</span><span class="v2-stat__value">${fmt1(p.land_area_ping)}</span></span>
          <span class="v2-stat"><span class="v2-stat__label">齡</span><span class="v2-stat__value">${p.building_age != null ? p.building_age : '—'}</span></span>
          <span class="v2-stat"><span class="v2-stat__label">層</span><span class="v2-stat__value">${formatFloor(p)}</span></span>
          <span class="v2-stat"><span class="v2-stat__label">區</span><span class="v2-stat__value">${esc((p.zoning || '—').replace('住宅區','住').replace('商業區','商'))}</span></span>
          ${p.road_width_m ? `<span class="v2-stat"><span class="v2-stat__label">路</span><span class="v2-stat__value">${p.road_width_m}m</span></span>` : ''}
        </div>
        <div class="v2-card__footer">
          ${chips.length ? `<div class="v2-card__chips">${chips.map(c => `<span class="v2-rchip ${c.cls}">${c.label}</span>`).join('')}</div>` : ''}
          ${p.sources && p.sources.length ? `<div class="v2-card__sources">${srcBadgesHTML(p.sources)}</div>` : ''}
          <button class="v2-card__star ${inWatchlist ? 'v2-card__star--active' : ''}" data-id="${esc(id)}" title="加入觀察清單">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>
            </svg>
          </button>
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
  async function loadDistricts() {
    try {
      const r = await fetch('/api/target_regions');
      const data = await r.json();
      state.targetRegions = data;
      renderDistrictChips();
    } catch (e) { console.warn('target_regions failed', e); }
  }
  function renderDistrictChips() {
    const host = $('#v2-district-chips');
    if (!host) return;
    const html = [];
    Object.entries(state.targetRegions).forEach(([city, districts]) => {
      districts.forEach(d => {
        const key = `${city}|${d}`;
        const checked = state.districtPicks.has(key) ? 'checked' : '';
        html.push(`<label class="v2-chip">
          <input type="checkbox" data-city="${esc(city)}" data-district="${esc(d)}" ${checked} onchange="v2.toggleDistrict('${esc(city)}','${esc(d)}', this.checked)">
          <span>${esc(d)}</span>
        </label>`);
      });
    });
    host.innerHTML = html.join('');
  }

  // ── Filter + sort ────────────────────────────────────────────────────────
  function applyFilters() {
    let list = state.allProperties.filter(p =>
      !p.deleted && !p.analysis_error && !p.analysis_in_progress && p.archived !== true
    );

    // district picks
    if (state.districtPicks.size > 0) {
      list = list.filter(p => {
        const key = `${p.city}|${p.district}`;
        return state.districtPicks.has(key);
      });
    }

    // road
    const road = ($('#v2-road').value || '').trim();
    if (road) {
      list = list.filter(p => (p.address || '').includes(road));
    }

    // price range
    const pmin = Number($('#v2-price-min').value) || 0;
    const pmax = Number($('#v2-price-max').value) || Infinity;
    list = list.filter(p => {
      const w = (p.price_ntd || 0) / 10000;
      return w >= pmin && w <= pmax;
    });

    // floor chips
    const floorPicks = $$('#v2-floor-chips input:checked').map(c => c.value);
    if (floorPicks.length > 0 && floorPicks.length < 5) {
      list = list.filter(p => {
        const f = p.floor;
        if (f == null) return false;
        return floorPicks.includes(String(f));
      });
    }

    // bld price
    const bldMax = Number($('#v2-bld-price-max').value);
    if (bldMax > 0) {
      list = list.filter(p => {
        if (!p.price_ntd || !p.building_area_ping) return true;
        return (p.price_ntd / 10000 / p.building_area_ping) < bldMax;
      });
    }

    // land price
    const landMax = Number($('#v2-land-price-max').value);
    if (landMax > 0) {
      list = list.filter(p => {
        if (!p.price_ntd || !p.land_area_ping) return true;
        return (p.price_ntd / 10000 / p.land_area_ping) < landMax;
      });
    }

    // land min
    const landMin = Number($('#v2-land-min').value) || 0;
    if (landMin > 0) {
      list = list.filter(p => (p.land_area_ping || 0) >= landMin);
    }

    // resistance
    if ($('#v2-hide-floors5plus').checked) {
      list = list.filter(p => !(p.total_floors >= 5 && p.building_type !== '透天厝'));
    }
    if ($('#v2-hide-remote').checked) list = list.filter(p => !p.is_remote_area);
    if ($('#v2-hide-unsuitable').checked) list = list.filter(p => !p.unsuitable_for_renewal);
    if ($('#v2-hide-basement').checked) list = list.filter(p => !p.is_basement);
    if ($('#v2-hide-foreclosure').checked) list = list.filter(p => !p.is_foreclosure);

    state.filteredSorted = list;
    applySort();
  }

  async function applySort() {
    const mode = $('#v2-sort').value;
    const prices = await getDistrictPrices();
    const list = state.filteredSorted.slice();

    const minMult = Number($('#v2-min-multiple').value) || 0;
    let workingList = list;
    if (minMult > 0) {
      workingList = workingList.filter(p => {
        const m = rowMultiple(p, prices);
        return m != null && m >= minMult;
      });
    }

    workingList.sort((a, b) => {
      switch (mode) {
        case 'multiple_desc': {
          const ma = rowMultiple(a, prices) ?? -1;
          const mb = rowMultiple(b, prices) ?? -1;
          return mb - ma;
        }
        case 'multiple_asc': {
          const ma = rowMultiple(a, prices) ?? Infinity;
          const mb = rowMultiple(b, prices) ?? Infinity;
          return ma - mb;
        }
        case 'price_asc': return (a.price_ntd || 0) - (b.price_ntd || 0);
        case 'price_desc': return (b.price_ntd || 0) - (a.price_ntd || 0);
        case 'age_desc': return (b.building_age || 0) - (a.building_age || 0);
        case 'land_desc': return (b.land_area_ping || 0) - (a.land_area_ping || 0);
        case 'published_desc':
          return (b.published_at || '').localeCompare(a.published_at || '');
      }
      return 0;
    });

    state.filteredSorted = workingList;
    state.page = 1;
    renderGrid();
  }

  // ── Detail drawer ────────────────────────────────────────────────────────
  async function openDetail(id) {
    state.selectedId = id;
    const slim = state.allProperties.find(x => (x.source_id || x.id) === id);
    if (!slim) return;

    // 1) 先用 slim 資料立刻開抽屜 — 避免空白等待感
    $('#v2-drawer-title').textContent = slim.address || slim.address_inferred || '物件詳情';
    $('#v2-drawer-body').innerHTML = `<div style="padding:24px;color:var(--c-text-muted);text-align:center">載入中…</div>`;
    $('#v2-drawer').classList.add('v2-open');
    $('#v2-drawer-backdrop').classList.add('v2-open');

    // 2) 背景 fetch 完整 doc（含 renewal_v2 / ai_reason / road_width_all 等重欄位）
    try {
      const r = await fetch(`/api/properties/${encodeURIComponent(id)}`);
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const full = await r.json();
      // 用戶可能在 fetch 期間已切到別的物件 → 過期 response 不渲染
      if (state.selectedId !== id) return;
      const prices = await getDistrictPrices();
      $('#v2-drawer-body').innerHTML = detailHTML(full, prices);
    } catch (e) {
      console.warn('openDetail full fetch failed', e);
      // fallback：用 slim 資料渲染（少了試算/AI reason，但基本資料還在）
      const prices = await getDistrictPrices();
      $('#v2-drawer-body').innerHTML = detailHTML(slim, prices);
    }
  }
  function closeDetail() {
    $('#v2-drawer').classList.remove('v2-open');
    $('#v2-drawer-backdrop').classList.remove('v2-open');
    state.selectedId = null;
  }

  function detailHTML(p, prices) {
    const mult = rowMultiple(p, prices);
    const priceWan = p.price_ntd ? Math.round(p.price_ntd / 10000) : null;
    const desired = priceWan ? Math.round(priceWan * 0.9 / 10) * 10 : null;
    const newPrice = p.new_house_price_wan_override ?? prices[p.district];
    const farPct = p.effective_far_pct ?? p.base_far_pct;

    const img = p.image_url
      ? `<img class="v2-detail-image" src="${esc(p.image_url)}" alt="">`
      : '';

    // Scenarios — use stored if available, otherwise estimate
    const scenarios = ['危老', '都更', '防災都更'].map(name => {
      let m = null;
      if (p.renewal_v2 && p.renewal_v2.scenarios && p.renewal_v2.scenarios[name]) {
        m = p.renewal_v2.scenarios[name].multiple;
      } else if (name === '都更') {
        m = mult;
      }
      let cls = '';
      if (m != null) {
        if (m >= 3.0) cls = 'v2-scenario__multi--good';
        else if (m >= 2.0) cls = 'v2-scenario__multi--mid';
      }
      return `<div class="v2-scenario">
        <div class="v2-scenario__name">${name}</div>
        <div class="v2-scenario__multi ${cls}">${m != null ? m.toFixed(2) : '—'}</div>
      </div>`;
    }).join('');

    return `
      ${img}
      <div class="v2-detail-section">
        <div class="v2-detail-section__title">基本資料</div>
        <div class="v2-detail-row"><span class="v2-detail-row__k">類型</span><span class="v2-detail-row__v">${typeIcon(p.building_type)} ${esc(p.building_type || '—')}</span></div>
        <div class="v2-detail-row"><span class="v2-detail-row__k">地址</span><span class="v2-detail-row__v">${esc(p.address || p.address_inferred || '—')}</span></div>
        <div class="v2-detail-row"><span class="v2-detail-row__k">總價</span><span class="v2-detail-row__v">${priceWan ? fmt0(priceWan) + '萬' : '—'}</span></div>
        <div class="v2-detail-row"><span class="v2-detail-row__k">欲出價</span><span class="v2-detail-row__v">${desired ? fmt0(desired) + '萬' : '—'}</span></div>
        <div class="v2-detail-row"><span class="v2-detail-row__k">建坪 / 地坪</span><span class="v2-detail-row__v">${fmt1(p.building_area_ping)} / ${fmt1(p.land_area_ping)}</span></div>
        <div class="v2-detail-row"><span class="v2-detail-row__k">屋齡</span><span class="v2-detail-row__v">${p.building_age != null ? p.building_age + '年' : '—'}</span></div>
        <div class="v2-detail-row"><span class="v2-detail-row__k">樓層</span><span class="v2-detail-row__v">${formatFloor(p)}</span></div>
        <div class="v2-detail-row"><span class="v2-detail-row__k">分區</span><span class="v2-detail-row__v">${esc(p.zoning || '—')}</span></div>
        <div class="v2-detail-row"><span class="v2-detail-row__k">容積率</span><span class="v2-detail-row__v">${farPct ? farPct + '%' : '—'}</span></div>
        <div class="v2-detail-row"><span class="v2-detail-row__k">臨路寬</span><span class="v2-detail-row__v">${p.road_width_m ? p.road_width_m + 'm' : '—'}</span></div>
      </div>

      <div class="v2-detail-section">
        <div class="v2-detail-section__title">都更試算</div>
        <div class="v2-scenarios">${scenarios}</div>
        <div style="margin-top: var(--sp-3); font-size: 12px; color: var(--c-text-muted);">
          新成屋單價 ${newPrice ? newPrice + ' 萬/坪' : '—'}（${p.new_house_price_wan_override ? '個人覆寫' : '區域預設'}）
        </div>
      </div>

      ${p.sources && p.sources.length ? `
      <div class="v2-detail-section">
        <div class="v2-detail-section__title">來源</div>
        <div class="v2-card__sources" style="border:none;padding:0;">${srcBadgesHTML(p.sources)}</div>
      </div>` : ''}

      <div class="v2-drawer-actions">
        <button class="v2-btn v2-btn--ghost v2-btn--sm" onclick="window.open('/?focus=${esc(p.source_id || '')}', '_blank')">在舊版開啟</button>
        <button class="v2-btn v2-btn--ghost v2-btn--sm" onclick="v2.toggleWatchlist('${esc(p.source_id || p.id || '')}')">
          ${p.user_url || p.added_at_user ? '從觀察清單移除' : '加入觀察清單'}
        </button>
      </div>
    `;
  }

  // ── Watchlist add/remove ─────────────────────────────────────────────────
  async function toggleWatchlist(id) {
    const p = state.allProperties.find(x => (x.source_id || x.id) === id);
    if (!p) return;
    const isIn = !!(p.user_url || p.added_at_user);
    try {
      if (isIn) {
        await fetch(`/api/properties/${encodeURIComponent(id)}/hide`, { method: 'POST' });
        toast('已從觀察清單移除');
      } else {
        await fetch(`/api/watchlist/${encodeURIComponent(id)}`, { method: 'POST' });
        toast('已加入觀察清單', 'success');
      }
      // refresh
      await loadProperties();
    } catch (e) {
      toast('操作失敗：' + e.message, 'error');
    }
  }

  // ── Load properties ──────────────────────────────────────────────────────
  async function loadProperties() {
    renderSkeletons(8);
    try {
      // slim=true → server 不回 renewal_v2/ai_reason/road_width_all 等重欄位 (~80% 縮減)
      // 點開詳情才 fetch 完整 doc
      const url = state.view === 'watchlist'
        ? '/api/properties?limit=500&slim=true'
        : '/api/properties?limit=500&slim=true';
      const r = await fetch(url);
      const data = await r.json();
      state.allProperties = data.items || [];
      const cnt = $('#v2-watchlist-count');
      if (cnt) {
        const n = state.allProperties.filter(p => !p.deleted && (p.user_url || p.added_at_user)).length;
        cnt.textContent = n > 0 ? String(n) : '';
      }
      applyFilters();
    } catch (e) {
      console.error('loadProperties', e);
      toast('載入失敗', 'error');
    }
  }

  async function runSearch() {
    if (state.view !== 'explore') {
      switchView('explore');
    }
    // 重新撈一次中央 search（透過 /api/central_search 帶條件）
    renderSkeletons(8);
    try {
      const params = new URLSearchParams();
      const districts = Array.from(state.districtPicks).map(k => k.split('|')[1]);
      if (districts.length) params.set('districts', districts.join(','));
      const road = ($('#v2-road').value || '').trim();
      if (road) params.set('road', road);
      const pmin = Number($('#v2-price-min').value) || 0;
      const pmax = Number($('#v2-price-max').value) || 0;
      if (pmin > 0) params.set('price_min_wan', String(pmin));
      if (pmax > 0) params.set('price_max_wan', String(pmax));
      params.set('slim', 'true');   // 列表只要卡片用的欄位
      const r = await fetch('/api/central_search?' + params.toString());
      const data = await r.json();
      state.allProperties = data.items || [];
      applyFilters();
    } catch (e) {
      console.error('runSearch', e);
      toast('搜尋失敗', 'error');
      // fallback: load watchlist
      loadProperties();
    }
  }

  // ── Sidebar / view toggling ──────────────────────────────────────────────
  function switchView(view) {
    state.view = view;
    $$('.v2-tab').forEach(t => t.classList.toggle('v2-tab--active', t.dataset.view === view));
    state.page = 1;
    loadProperties();
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
    state.districtPicks.clear();
    $('#v2-road').value = '';
    $('#v2-price-min').value = 0;
    $('#v2-price-max').value = 5000;
    $('#v2-bld-price-max').value = 300;
    $('#v2-land-price-max').value = 600;
    $('#v2-land-min').value = 0;
    $('#v2-min-multiple').value = 0;
    $('#v2-hide-floors5plus').checked = false;
    $('#v2-hide-remote').checked = true;
    $('#v2-hide-unsuitable').checked = true;
    $('#v2-hide-basement').checked = true;
    $('#v2-hide-foreclosure').checked = true;
    $$('#v2-floor-chips input').forEach(c => c.checked = true);
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
    await loadDistricts();
    await loadProperties();
  }

  // ── Public namespace (matches inline onclick handlers) ──────────────────
  window.v2 = {
    switchView, toggleDistrict, applyFilters, applySort, runSearch,
    resetFilters, gotoPage, openSidebar, closeSidebar,
    openDetail, closeDetail, toggleWatchlist, logout,
  };

  // Boot
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
