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
    sortDir: 'desc',           // 'asc' | 'desc'，跟 v1 toggleSortDir 對齊
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

  // 多分區加權 FAR — 對齊 v1 effectiveFarPctWeighted
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
    return lookupFar(p.zoning, p);
  }

  // 取後端已存的「投入欄位」即時算倍數 — DB 不存 multiple (CLAUDE.md 規則 8)
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
    const bonus = p.bonus_dugen ?? 0.50;
    const share = land * (farPct/100) * (1+bonus) * coeff * ratio;
    const total = share * effectivePrice + (share / 40) * parking;
    const listWan = (p.price_ntd || 0) / 10000;
    const desired = p.desired_price_wan ?? (listWan ? Math.round(listWan * 0.9 / 10) * 10 : 0);
    if (!desired) return null;
    return total / desired;
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
    const addr = p.address || p.address_inferred || '—';
    const priceWan = p.price_ntd ? Math.round(p.price_ntd / 10000) : null;
    const perBld = (p.price_ntd && p.building_area_ping)
      ? (p.price_ntd / 10000 / p.building_area_ping).toFixed(1) : null;
    const mult = rowMultiple(p, prices);
    let multCls = 'v2-card__mult';
    if (mult != null) {
      if (mult >= 3.0) multCls += ' v2-card__mult--good';
      else if (mult >= 2.0) multCls += ' v2-card__mult--mid';
    }
    const chips = computeChips(p);
    const inWatchlist = !!(p.user_url || p.added_at_user);
    const archivedClass = p.archived ? 'v2-card--archived' : '';

    // ── 2-line dense layout ──
    // Line 1: icon + 區·地址 (ellipsis) | 總價 + 建單價 | 倍數 | ⭐
    // Line 2: 建/地/齡/層/區/路 + chips + sources
    return `
      <article class="v2-card ${archivedClass}" data-id="${esc(id)}">
        <div class="v2-card__line1">
          <span class="v2-card__type">${typeIcon(p.building_type)}</span>
          <span class="v2-card__addr">
            <span class="v2-card__district">${esc(p.district || '')}</span>·${esc(addr)}
          </span>
          <span class="v2-card__price-block">
            <span class="v2-card__price">${priceWan ? fmt0(priceWan) : '—'}<small>萬</small></span>
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
        <div class="v2-card__line2">
          <span class="v2-stat" title="建坪"><b>建</b>${fmt1(p.building_area_ping)}</span>
          <span class="v2-stat" title="地坪"><b>地</b>${fmt1(p.land_area_ping)}</span>
          <span class="v2-stat" title="屋齡"><b>齡</b>${p.building_age != null ? p.building_age : '—'}</span>
          <span class="v2-stat" title="樓層"><b>層</b>${formatFloor(p)}</span>
          <span class="v2-stat" title="分區"><b>區</b>${esc((p.zoning || '—').replace('住宅區','住').replace('商業區','商'))}</span>
          ${p.road_width_m ? `<span class="v2-stat" title="路寬"><b>路</b>${p.road_width_m}m</span>` : ''}
          ${chips.length ? chips.map(c => `<span class="v2-rchip ${c.cls}">${c.label}</span>`).join('') : ''}
          ${p.sources && p.sources.length ? `<span class="v2-card__sources">${srcBadgesHTML(p.sources)}</span>` : ''}
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
        const n = counts[key] || 0;
        const label = cfg.labels[d] || d;
        return `<label class="v2-chip" title="${n} 筆物件">
          <input type="checkbox" data-city="${esc(city)}" data-district="${esc(d)}" ${checked} onchange="v2.toggleDistrict('${esc(city)}','${esc(d)}', this.checked)">
          <span>${esc(label)}${n ? ` <em class="v2-chip__n">${n}</em>` : ''}</span>
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

    // building_type — 對齊 v1：勾選的 type 才顯示（disabled 不算）
    const btypePicks = $$('.v2-filter-btype:not(:disabled)').filter(c => c.checked).map(c => c.value);
    const btypeAll = $$('.v2-filter-btype:not(:disabled)').length;
    if (btypePicks.length > 0 && btypePicks.length < btypeAll) {
      list = list.filter(p => btypePicks.includes(p.building_type));
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

    // floor chips：B1 / 1F-5F；data-floor 屬性區分（不含「全部」master）
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
        if (f == null) return true;   // 缺資料 pass-through
        return intPicks.includes(String(f));
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

    // resistance — 對齊 v1：hide-foreclosure / hide-resist-{floors5plus,remote,unsuitable,basement}
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

  // 對齊 v1 sort 8 個選項 + 升降序按鈕
  // list_rank: scrape_session_at desc + list_rank asc (新進優先)
  // last_change_at: last_change_at desc fallback scrape_session_at
  // published_at: published_at desc fallback scraped_at
  // profit_multiple: rowMultiple desc/asc，None 沉底
  // price_per_building_ping / price_per_land_ping / price_ntd / building_age: 數值 sort，None 沉底
  async function applySort() {
    const mode = $('#v2-sort').value;
    const reverse = state.sortDir === 'desc';
    const prices = await getDistrictPrices();
    const list = state.filteredSorted.slice();

    // 最低獲利倍數 toggle (對齊 v1 explore-min-profit)
    const minMultOn = $('#v2-min-mult-on')?.checked;
    const minMultVal = parseFloat($('#v2-min-mult-val')?.value);
    let workingList = list;
    if (minMultOn && !isNaN(minMultVal) && minMultVal > 0) {
      workingList = workingList.filter(p => {
        const m = rowMultiple(p, prices);
        return m != null && m >= minMultVal;
      });
    }

    const valOf = (p) => {
      switch (mode) {
        case 'list_rank': return null;   // 特殊處理在下方
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

    if (mode === 'list_rank') {
      // 對齊 v1：scrape_session_at desc + list_rank asc 為次（新進優先）
      workingList.sort((a, b) =>
        (a.list_rank ?? 9999) - (b.list_rank ?? 9999)
      );
      workingList.sort((a, b) =>
        (b.scrape_session_at || '').localeCompare(a.scrape_session_at || '')
      );
    } else {
      // 通用：None 永遠沉底
      const has = workingList.filter(p => valOf(p) != null);
      const noVal = workingList.filter(p => valOf(p) == null);
      has.sort((a, b) => {
        const va = valOf(a), vb = valOf(b);
        if (typeof va === 'string') return reverse ? vb.localeCompare(va) : va.localeCompare(vb);
        return reverse ? (vb - va) : (va - vb);
      });
      workingList = has.concat(noVal);
    }

    state.filteredSorted = workingList;
    state.page = 1;
    renderGrid();
  }

  function toggleSortDir() {
    state.sortDir = state.sortDir === 'desc' ? 'asc' : 'desc';
    const btn = $('#v2-sort-dir');
    if (btn) btn.textContent = state.sortDir === 'desc' ? '↓' : '↑';
    applySort();
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
      let url;
      if (state.view === 'watchlist') {
        // /api/properties = 用戶的觀察清單 (watchlist + manual)，server 端做 join
        url = '/api/properties?limit=500&slim=true';
      } else {
        // explore = 中央 DB 搜尋 (帶當前 sidebar filter)；用 /api/central_search
        const params = new URLSearchParams();
        const districts = Array.from(state.districtPicks).map(k => k.split('|')[1]);
        if (districts.length) params.set('districts', districts.join(','));
        const road = ($('#v2-road').value || '').trim();
        if (road) params.set('road', road);
        const pmin = Number($('#v2-price-min').value) || 0;
        const pmax = Number($('#v2-price-max').value) || 0;
        if (pmin > 0) params.set('min_price_wan', String(pmin));
        if (pmax > 0) params.set('max_price_wan', String(pmax));
        params.set('slim', 'true');
        params.set('limit', '500');
        url = '/api/central_search?' + params.toString();
      }
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
    $('#v2-hide-foreclosure').checked = true;
    $$('.v2-filter-btype:not(:disabled)').forEach(c => { c.checked = c.value === '公寓'; });
    // floor chips：B 不勾，1-5 勾
    $$('#v2-floor-chips input[data-floor]').forEach(c => {
      c.checked = c.value !== 'B';
    });
    const fa = $('#v2-floor-all');
    if (fa) fa.checked = true; // master 對應 1-5 全勾
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
  };

  // Boot
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
