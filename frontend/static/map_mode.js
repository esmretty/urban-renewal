/**
 * v2 地圖模式 — 獨立模組（access-controlled，僅 retty.liu@gmail.com 可見入口）
 *
 * 載入順序：index2.html 內 leaflet.js + shared.js + app2.js 之後（IIFE 自動執行）。
 *
 * 跟 app2.js 的 coupling：
 *   - 讀 window.v2.state  (state.filteredSorted / state.viewMode / state._mapInst / state._mapMarkers)
 *   - 讀 window.v2.getDistrictPrices() / window.v2.openDetail() / window.v2._saveFilters()
 *   - 寫 window.v2.setViewMode (export 給 HTML link onclick 用) +
 *        window.v2._maybeShowViewToggle / window.v2._renderMap (給 app2.js renderGrid short-circuit 呼叫)
 *
 * 對 app2.js 既有邏輯影響（最小化）：
 *   - app2.js 的 state 初始化加 viewMode/_mapInst/_mapMarkers 三個欄位
 *   - app2.js 的 renderGrid 開頭加 short-circuit if state.viewMode==='map' → call window.v2._renderMap
 *   - app2.js 的 _collectFilterObj / _restoreFilters 加 viewMode 欄位 (持久化)
 */
(function () {
  'use strict';

  // 切換 UI 入口的授權 email — 其他用戶看不到「列表模式 / 地圖模式」link
  const ALLOWED_VIEW_TOGGLE_EMAIL = 'retty.liu@gmail.com';

  // 透過 window.v2 取 app2.js 內部 state / helpers — 必須等 app2.js 先載完
  function _v2() { return window.v2 || {}; }
  function _state() { return _v2().state || null; }

  // HTML escape (跟 app2.js 內部 esc 同行為)
  const _esc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[c]));

  // ── Lazy init Leaflet (只 init 一次) ──
  function _initMap() {
    const st = _state();
    if (!st) return;
    if (st._mapInst) {
      // hidden→visible 後 tile 不會自動 reflow，要手動 invalidate
      setTimeout(() => st._mapInst.invalidateSize(), 50);
      return;
    }
    if (typeof L === 'undefined') {
      console.warn('[map_mode] Leaflet 未載入，地圖模式不可用');
      return;
    }
    const m = L.map('v2-map').setView([25.05, 121.55], 12);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© OpenStreetMap', maxZoom: 19,
    }).addTo(m);
    st._mapInst = m;
    st._mapMarkers = [];
  }

  // ── render 當前 filtered list 到地圖 ──
  // marker：倍數分色 (≥3.5 紅 / ≥2.5 黃 / 其他灰 / 無資料淡灰) + 倍數數字 e.g. "2.3x"
  // hover：bindTooltip 顯示地址/價格/地坪/屋齡摘要
  // click：openDetail(id) 跟卡片一致
  async function renderMap() {
    const st = _state();
    if (!st || !st._mapInst) return;
    const m = st._mapInst;
    // 清舊 markers
    (st._mapMarkers || []).forEach(mk => m.removeLayer(mk));
    st._mapMarkers = [];

    const v2 = _v2();
    const prices = (typeof v2.getDistrictPrices === 'function')
      ? await v2.getDistrictPrices() : {};
    const all = st.filteredSorted || [];
    const list = all.filter(p => p.latitude && p.longitude);
    const noCoordCount = all.length - list.length;

    const cnt = document.getElementById('v2-result-count');
    if (cnt) {
      cnt.innerHTML = `共 <strong>${all.length}</strong> 筆` +
        (noCoordCount > 0 ? ` <span class="v2-d-hint">（${noCoordCount} 筆無座標未標）</span>` : '');
    }

    const bounds = [];
    list.forEach(p => {
      const r = (typeof UrbanShared !== 'undefined' && UrbanShared.computeMultiples)
        ? UrbanShared.computeMultiples(p, prices[p.district]) : null;
      const mult = r ? Math.max(r.w || 0, r.d || 0) : null;
      const color = mult == null ? '#94a3b8'
        : mult >= 3.5 ? '#dc2626'
        : mult >= 2.5 ? '#f59e0b'
        : '#94a3b8';
      const label = mult != null ? mult.toFixed(1) + 'x' : '—';
      const icon = L.divIcon({
        html: `<div class="v2-map-marker" style="background:${color}">${label}</div>`,
        className: '', iconSize: null, iconAnchor: [22, 14],
      });
      const mk = L.marker([p.latitude, p.longitude], { icon }).addTo(m);
      const _addr = p.address_inferred || p.address || '';
      const _priceWan = p.price_ntd ? Math.round(p.price_ntd / 10000) + ' 萬' : '—';
      const _land = p.land_area_ping ? p.land_area_ping + ' 坪' : '?坪';
      const _age = (p.building_age != null) ? p.building_age + ' 年' : '?年';
      mk.bindTooltip(
        `<b>${_esc(_addr)}</b><br>${_priceWan} / 地坪 ${_esc(String(_land))} / 屋齡 ${_esc(String(_age))}`,
        { direction: 'top' }
      );
      mk.on('click', () => {
        if (typeof v2.openDetail === 'function') v2.openDetail(p.source_id || p.id);
      });
      st._mapMarkers.push(mk);
      bounds.push([p.latitude, p.longitude]);
    });
    if (bounds.length) m.fitBounds(bounds, { padding: [40, 40], maxZoom: 16 });
  }

  // ── 切換 view mode ──
  function setViewMode(mode) {
    if (mode !== 'list' && mode !== 'map') return;
    const st = _state();
    if (!st) return;
    st.viewMode = mode;
    const grid = document.getElementById('v2-grid');
    const mapEl = document.getElementById('v2-map');
    if (grid) grid.style.display = mode === 'list' ? '' : 'none';
    if (mapEl) mapEl.style.display = mode === 'map' ? '' : 'none';
    document.querySelectorAll('.v2-view-toggle__link').forEach(a =>
      a.classList.toggle('is-active', a.dataset.viewMode === mode));
    if (mode === 'map') {
      _initMap();
      renderMap();
    } else {
      // 切回列表 — 觸發 app2.js renderGrid (走它自己的 logic，這次 viewMode='list' 不再分流)
      const v2 = _v2();
      if (typeof v2.applyFilters === 'function') v2.applyFilters();
    }
    // 持久化進 filter prefs (跟 sidebar filter 一起存)
    const v2 = _v2();
    if (typeof v2._saveFilters === 'function') v2._saveFilters();
  }

  // ── Access control gate：auth ready 後檢查 email 才 reveal toggle UI ──
  function _maybeShowViewToggle() {
    const email = (window.currentUser && window.currentUser.email) || '';
    if (email === ALLOWED_VIEW_TOGGLE_EMAIL) {
      const tg = document.getElementById('v2-view-toggle');
      if (tg) tg.style.display = '';
    }
  }

  // ── 安裝到 window.v2 (等 app2.js 先載完才有 v2 namespace) ──
  function _install() {
    const v2 = window.v2;
    if (!v2) {
      // app2.js 還沒 expose v2 — 重試
      setTimeout(_install, 50);
      return;
    }
    v2.setViewMode = setViewMode;
    v2._renderMap = renderMap;        // app2.js renderGrid short-circuit 呼叫
    v2._maybeShowViewToggle = _maybeShowViewToggle;
    v2._ALLOWED_MAP_EMAIL = ALLOWED_VIEW_TOGGLE_EMAIL;  // 給 app2.js 的 _restoreFilters guard 用
    // auth 事件 ready 後檢查 email
    document.addEventListener('auth:ready', _maybeShowViewToggle);
    if (window.currentUser) _maybeShowViewToggle();
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _install);
  } else {
    _install();
  }
})();
