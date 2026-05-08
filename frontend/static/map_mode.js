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
    st._schoolLayer = null;
    st._schoolLabelLayer = null;
    st._schoolGeo = null;          // 全部 polygons (cached)
    st._schoolKind = 'off';        // 'off' | 'elementary' | 'junior_high'
    // toolbar radio 監聽
    document.addEventListener('change', (e) => {
      if (e.target && e.target.matches('input[name="v2-school-layer"]')) {
        st._schoolKind = e.target.value;
        _renderSchoolLayer();
      }
    });
  }

  // ── 同學校永遠同色 (string hash → HSL) ──
  // lightness 50% 而非 60% — 色調較深讓白字 label 對比夠看得清
  function _colorForSchool(name) {
    if (!name) return '#888';
    let h = 0;
    for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) | 0;
    return `hsl(${Math.abs(h) % 360}, 65%, 50%)`;
  }

  function _clearSchoolLayer() {
    const st = _state();
    if (!st || !st._mapInst) return;
    if (st._schoolLayer) { st._mapInst.removeLayer(st._schoolLayer); st._schoolLayer = null; }
    if (st._schoolLabelLayer) { st._mapInst.removeLayer(st._schoolLabelLayer); st._schoolLabelLayer = null; }
  }

  async function _renderSchoolLayer() {
    const st = _state();
    if (!st || !st._mapInst) return;
    const m = st._mapInst;
    const kind = st._schoolKind || 'off';
    const hint = document.getElementById('v2-school-layer-hint');
    _clearSchoolLayer();
    if (kind === 'off') {
      if (hint) hint.textContent = '';
      return;
    }
    // Lazy fetch polygons_all (only once)
    if (!st._schoolGeo) {
      if (hint) hint.textContent = '載入中…';
      try {
        const r = await fetch('/api/school_district/polygons_all');
        if (!r.ok) throw new Error('HTTP ' + r.status);
        st._schoolGeo = await r.json();
      } catch (e) {
        if (hint) hint.textContent = '學區資料載入失敗：' + e.message;
        return;
      }
    }
    const features = (st._schoolGeo && st._schoolGeo.features) || [];
    if (!features.length) {
      if (hint) hint.textContent = '無學區資料';
      return;
    }
    // GeoJSON polygon layer (按學校 hash 配色)
    st._schoolLayer = L.geoJSON(st._schoolGeo, {
      style: (ft) => {
        const schools = (ft.properties || {})[kind] || [];
        const main = schools[0] || '';
        return {
          color: '#444', weight: 0.8, opacity: 0.7,
          fillColor: _colorForSchool(main),
          // 著色加深 0.42 → 0.65：底色夠深讓白字 label 對比清楚
          fillOpacity: schools.length ? 0.65 : 0.08,
        };
      },
      onEachFeature: (ft, layer) => {
        const p = ft.properties || {};
        const es = (p.elementary || []).join('、') || '—';
        const jh = (p.junior_high || []).join('、') || '—';
        layer.bindTooltip(
          `<b>${_esc(p.village)}</b> (${_esc(p.county)}${_esc(p.town)})<br>` +
          `國小：${_esc(es)}<br>國中：${_esc(jh)}`,
          { sticky: true }
        );
      },
    }).addTo(m);
    // label：學校名置中 polygon，純文字 + 描邊，無邊框
    st._schoolLabelLayer = L.layerGroup();
    features.forEach(ft => {
      const schools = (ft.properties || {})[kind] || [];
      if (!schools.length) return;
      let center = null;
      try { center = L.geoJSON(ft).getBounds().getCenter(); } catch (_e) {}
      if (!center) return;
      // 每校一行 — 多校用 <br> 分隔，single 校直接 escape
      // wrapper iconSize=[0,0] → leaflet anchor 在 polygon center；inner div 用
      // absolute + translate(-50%,-50%) 自己中心對齊 anchor → 整文字方塊中心置中
      const inner = schools.map(s => _esc(s)).join('<br>');
      const ic = L.divIcon({
        className: '',  // 不用 leaflet 預設 class style
        html: `<div class="v2-school-label-inner">${inner}</div>`,
        iconSize: [0, 0],
      });
      L.marker(center, { icon: ic, interactive: false }).addTo(st._schoolLabelLayer);
    });
    st._schoolLabelLayer.addTo(m);
    if (hint) hint.textContent = `已載入 ${features.length} 里`;
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
      let mult = r ? Math.max(r.w || 0, r.d || 0) : null;
      // 倍數算不出 (缺 land/zoning/price 等) 會回 0 → 顯示 0.0x 沒意義，當 N/A
      if (mult != null && mult <= 0) mult = null;
      const color = mult == null ? '#94a3b8'
        : mult >= 3.5 ? '#dc2626'
        : mult >= 2.5 ? '#f59e0b'
        : '#94a3b8';
      const label = mult != null ? mult.toFixed(1) + 'x' : 'N/A';
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
    // toggle wrap (含 toolbar + map)；wrap 不在時 fallback toggle inner v2-map (backward compat)
    const mapWrap = document.getElementById('v2-map-wrap')
                  || document.getElementById('v2-map');
    if (grid) grid.style.display = mode === 'list' ? '' : 'none';
    if (mapWrap) mapWrap.style.display = mode === 'map' ? '' : 'none';
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
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _install);
  } else {
    _install();
  }
})();
