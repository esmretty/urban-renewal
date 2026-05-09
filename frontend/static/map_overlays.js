/**
 * v2 地圖模式 — optional 圖層 multi-layer manager
 *
 * 隔離原則（plan 約定）：
 *   - 整個 module 自包含（IIFE），attach 到 window.v2._overlays
 *   - 不改 map_mode.js 內部 function；只在 _initMap 末尾插 1 行 hook：
 *     `window.v2._overlays?.init(m)`
 *   - revert：刪本 file + index2.html script tag + map_mode.js 那 1 行 hook
 *
 * 結構：兩個獨立 toolbar group
 *   1. 地籍圖層 (#v2-map-overlays-toolbar) — 土地分區 / 地籍 / 建物套繪
 *   2. 都更圖層 (#v2-map-renewal-toolbar)  — 13 個 sub-layer + 全選 (UDDPlanMap 對齊)
 *
 * z-index 由下到上：分區 (401) → 地籍 (402) → 建物 (403) → 都更 (404)
 *   都更 13 個 sub 共用 pane 'v2-overlay-renewal' (z=404)，markers 預設 600 仍最上方。
 */
(function () {
  'use strict';

  // ── 地籍圖層 (cadastral group) — 跟既有結構一致 ──────────────────────
  const LAYERS = {
    zoning: {
      label: '土地分區',
      paneZ: 401,
      backends: ['zoning_tpe', 'zoning_ntpc'],
    },
    cadastral: {
      label: '地籍圖',
      paneZ: 402,
      backends: [
        { name: 'cadastral_lines_tpe', minZoom: 17 },
        { name: 'cadastral_numbers_tpe', minZoom: 17 },
      ],
      hint: 'z=17+，僅台北市',
    },
    building_floors: {
      label: '建物套繪圖',
      paneZ: 403,
      backends: [{ name: 'building_floors_tpe', minZoom: 17 }],
      hint: 'z=17+，僅台北市',
    },
  };

  // ── 都更圖層 (renewal group) — 對齊 UDDPlanMap Layer_Redevelop.json ────
  // sub-layer 全部走 GeoServer Taipei:uro-redevelop-ALL-5 + cql_filter (除 115/63y)
  // 全部共用 pane v2-overlay-renewal (zIndex 404)
  const RENEWAL_PANE_Z = 404;
  const RENEWAL_SUBS = [
    { id: 'pub_renew',     label: '公劃更新地區(依都更條例)',    color: '#FF0000', backend: 'redev_pub_renew' },
    { id: 'revoked',       label: '廢止89.91年公劃',          color: '#B84A5B', backend: 'redev_revoked' },
    { id: 'self_announce', label: '公告自劃(事業權變)',         color: '#0000FF', backend: 'redev_self_announce' },
    { id: 'self_approved', label: '核准自劃(事業權變)',         color: '#FF7F00', backend: 'redev_self_approved' },
    { id: 'planned',       label: '都計劃定更新地區',           color: '#FF00FF', backend: 'redev_planned' },
    { id: '107expired',    label: '107年公劃(停用)',           color: '#FFD800', backend: 'redev_107expired' },
    { id: '115_revised',   label: '115年修訂公劃',             color: '#FF9966', backend: 'redev_115_revised' },
    { id: 'chloride',      label: '高氯離子混凝土',             color: '#D0B17A', backend: 'redev_chloride' },
    { id: '63y_building',  label: '63年以前建築物',             color: '#1F4E79', backend: 'redev_63y_building' },
    { id: 'urgent',        label: '迅行劃定',                  color: '#FFD0FF', backend: 'redev_urgent' },
    { id: 'pub_business',  label: '公劃內事業(權變)',           color: '#6495ED', backend: 'redev_pub_business' },
    { id: 'taipei_view',   label: '臺北好好看 II',             color: '#A500A5', backend: 'redev_taipei_view' },
    { id: 'invalid',       label: '已失效/廢止',                color: '#00FFFF', backend: 'redev_invalid' },
  ];
  const RENEWAL_ALL_BACKEND = 'redev_all';   // 「全選」一鍵走 1 個 request 含全部子類型

  const OPACITY = 0.5;

  // 內部 state
  const _state = {
    map: null,
    inited: false,
    on: { zoning: false, cadastral: false, building_floors: false },   // cadastral group
    layerRefs: { zoning: [], cadastral: [], building_floors: [] },     // cadastral group
    renewal: {
      all: false,                  // 「全選」狀態
      subs: {},                    // { sub_id: bool } 個別勾選
      layerAll: null,              // 「全選」用單一 wms layer instance
      layerSubs: {},               // { sub_id: wmsLayer } 個別 sub layer instances
    },
  };

  // ════════════════════════════════════════════════════════════
  // 地籍圖層 toolbar render + toggle (沿用既有 logic，未改)
  // ════════════════════════════════════════════════════════════
  function _renderCadastralToolbar() {
    const host = document.getElementById('v2-map-overlays-toolbar');
    if (!host || host.dataset.rendered === '1') return;
    host.dataset.rendered = '1';
    host.innerHTML = '<span class="v2-overlays-label">地籍圖層：</span>' +
      Object.entries(LAYERS).map(([key, cfg]) => {
        const note = cfg.hint ? ` <span class="v2-overlays-note">(${cfg.hint})</span>` : '';
        return `<label class="v2-overlays-checkbox">` +
          `<input type="checkbox" data-overlay="${key}"> ${cfg.label}${note}</label>`;
      }).join('');
    host.addEventListener('change', (e) => {
      const t = e.target;
      if (!t || !t.matches('input[data-overlay]')) return;
      _toggleCadastral(t.dataset.overlay, t.checked);
    });
  }

  function _toggleCadastral(key, on) {
    const cfg = LAYERS[key];
    const m = _state.map;
    if (!cfg || !m) return;
    _state.on[key] = on;
    (_state.layerRefs[key] || []).forEach(l => m.removeLayer(l));
    _state.layerRefs[key] = [];
    if (!on) return;
    const paneName = 'v2-overlay-' + key;
    if (!m.getPane(paneName)) {
      const pane = m.createPane(paneName);
      pane.style.zIndex = String(cfg.paneZ);
      pane.style.pointerEvents = 'none';
    }
    cfg.backends.forEach(b => {
      const layer = _makeWmsLayer(b, paneName);
      layer.addTo(m);
      _state.layerRefs[key].push(layer);
    });
  }

  // ════════════════════════════════════════════════════════════
  // 都更圖層 toolbar render + toggle (新)
  // ════════════════════════════════════════════════════════════
  function _renderRenewalToolbar() {
    const host = document.getElementById('v2-map-renewal-toolbar');
    if (!host || host.dataset.rendered === '1') return;
    host.dataset.rendered = '1';
    const subItems = RENEWAL_SUBS.map(s =>
      `<label class="v2-renewal-sub" data-sub-id="${s.id}">` +
        `<input type="checkbox" data-renewal-sub="${s.id}">` +
        `<span class="v2-renewal-dot" style="background:${s.color}"></span>` +
        `<span class="v2-renewal-text">${s.label}</span>` +
      `</label>`
    ).join('');
    host.innerHTML = `
      <div class="v2-renewal-header">
        <span class="v2-overlays-label">都更圖層：</span>
        <label class="v2-renewal-all"><input type="checkbox" data-renewal-all> 全選</label>
        <span class="v2-overlays-note">僅台北市，對齊 UDDPlanMap</span>
      </div>
      <div class="v2-renewal-grid">${subItems}</div>`;
    host.addEventListener('change', (e) => {
      const t = e.target;
      if (!t) return;
      if (t.matches('input[data-renewal-all]')) {
        _toggleRenewalAll(t.checked);
      } else if (t.matches('input[data-renewal-sub]')) {
        _toggleRenewalSub(t.dataset.renewalSub, t.checked);
      }
    });
  }

  function _ensureRenewalPane() {
    const m = _state.map;
    if (!m) return null;
    if (!m.getPane('v2-overlay-renewal')) {
      const pane = m.createPane('v2-overlay-renewal');
      pane.style.zIndex = String(RENEWAL_PANE_Z);
      pane.style.pointerEvents = 'none';
    }
    return 'v2-overlay-renewal';
  }

  function _clearAllRenewal() {
    const m = _state.map;
    if (!m) return;
    if (_state.renewal.layerAll) {
      m.removeLayer(_state.renewal.layerAll);
      _state.renewal.layerAll = null;
    }
    Object.values(_state.renewal.layerSubs).forEach(l => m.removeLayer(l));
    _state.renewal.layerSubs = {};
  }

  // 「全選」toggle: 用 1 個 redev_all layer 取代 13 個 sub layer (省 12 個 request)
  function _toggleRenewalAll(on) {
    const m = _state.map;
    if (!m) return;
    _state.renewal.all = on;
    _clearAllRenewal();

    // 同步 sub-checkbox 視覺 (跟 state)
    _state.renewal.subs = {};
    RENEWAL_SUBS.forEach(s => { _state.renewal.subs[s.id] = on; });
    document.querySelectorAll('input[data-renewal-sub]').forEach(cb => { cb.checked = on; });

    if (!on) return;
    const paneName = _ensureRenewalPane();
    _state.renewal.layerAll = _makeWmsLayer(RENEWAL_ALL_BACKEND, paneName);
    _state.renewal.layerAll.addTo(m);
  }

  // 個別 sub toggle: 一旦用戶單獨勾，從「全選」模式切到「個別模式」
  function _toggleRenewalSub(subId, on) {
    const m = _state.map;
    const sub = RENEWAL_SUBS.find(s => s.id === subId);
    if (!m || !sub) return;
    _state.renewal.subs[subId] = on;

    // 如果之前在「全選」模式 → 切換到「個別模式」: 移除 redev_all，把已勾的 sub 一個個 add
    if (_state.renewal.all) {
      _state.renewal.all = false;
      const allCheckbox = document.querySelector('input[data-renewal-all]');
      if (allCheckbox) allCheckbox.checked = false;
      if (_state.renewal.layerAll) {
        m.removeLayer(_state.renewal.layerAll);
        _state.renewal.layerAll = null;
      }
      // 把當前所有 checked 的 sub 重新 add (不含剛取消的)
      const paneName = _ensureRenewalPane();
      RENEWAL_SUBS.forEach(s => {
        if (_state.renewal.subs[s.id]) {
          const layer = _makeWmsLayer(s.backend, paneName);
          layer.addTo(m);
          _state.renewal.layerSubs[s.id] = layer;
        }
      });
      return;
    }

    // 個別模式：add/remove 單個 sub layer
    const paneName = _ensureRenewalPane();
    if (on) {
      const layer = _makeWmsLayer(sub.backend, paneName);
      layer.addTo(m);
      _state.renewal.layerSubs[subId] = layer;
    } else {
      const layer = _state.renewal.layerSubs[subId];
      if (layer) {
        m.removeLayer(layer);
        delete _state.renewal.layerSubs[subId];
      }
    }
  }

  // ════════════════════════════════════════════════════════════
  // 共用 helper：build L.tileLayer.wms instance
  // ════════════════════════════════════════════════════════════
  function _makeWmsLayer(backend, paneName) {
    const name = typeof backend === 'string' ? backend : backend.name;
    const minZoom = (typeof backend === 'object' && backend.minZoom) ? backend.minZoom : undefined;
    const opts = {
      layers: name,
      format: 'image/png',
      transparent: true,
      opacity: OPACITY,
      pane: paneName,
      maxZoom: 22,
    };
    if (minZoom != null) opts.minZoom = minZoom;
    const layer = L.tileLayer.wms('/api/gis_overlay/' + name, opts);
    layer.on('tileerror', (e) => {
      console.debug('[overlay tile error]', name, e.tile && e.tile.src);
    });
    return layer;
  }

  // ════════════════════════════════════════════════════════════
  // public API
  // ════════════════════════════════════════════════════════════
  function init(map) {
    if (_state.inited) return;
    _state.inited = true;
    _state.map = map;
    _renderCadastralToolbar();
    _renderRenewalToolbar();
  }

  function _install() {
    if (!window.v2) {
      setTimeout(_install, 50);
      return;
    }
    window.v2._overlays = { init };
    const existingMap = window.v2.state && window.v2.state._mapInst;
    if (existingMap) init(existingMap);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _install);
  } else {
    _install();
  }
})();
