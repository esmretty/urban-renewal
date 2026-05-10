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

  // ── 地籍圖層 (cadastral group) ──────────────────────────────────
  // z-index 由下到上：分區(401) → 建物套繪(402) → 地籍(403) → 都更(404)
  // 用戶要求：建物套繪圖在地籍圖下方；都更圖層在地籍圖上方
  const LAYERS = {
    zoning: {
      label: '土地分區',
      paneZ: 401,
      backends: ['zoning_tpe', 'zoning_ntpc'],
    },
    building_floors: {
      label: '建物套繪圖',
      paneZ: 402,
      backends: [{ name: 'building_floors_tpe', minZoom: 18 }],
      hint: 'z=18+，僅台北市',
    },
    cadastral: {
      label: '地籍圖',
      paneZ: 403,
      backends: [
        { name: 'cadastral_lines_tpe', minZoom: 18 },     // 台北 GeoServer 詳細地籍線
        { name: 'cadastral_numbers_tpe', minZoom: 18 },   // 台北 GeoServer 地號文字
        { name: 'cadastral_full_ntpc', minZoom: 18 },     // 新北個別地塊+地號 (NTPC ArcGIS)
      ],
      hint: 'z=18+',
    },
  };

  // ── 都更圖層 (renewal group) — 雙北分兩 city section ────
  // 台北：GeoServer Taipei:uro-redevelop-ALL-5 + cql_filter (10 個 sub-layer)
  // 新北：NTPC NtpcURInfo 動態 ArcGIS layer (4 個 sub-layer)
  // 全部共用 pane v2-overlay-renewal (zIndex 404)
  // SVG filter 把 upstream tile alpha 對應的 polygon 區域 flood 成我們指定的 fill_color
  // (台北 server 是 grayscale；新北 server 是 default 綠色 — feComposite operator='in'
  //  只看 SourceAlpha 不看 RGB，所以即便 source 已染色也會被覆蓋成 fill_color)
  const RENEWAL_PANE_Z = 404;
  const RENEWAL_SUBS = [
    // 台北 (10)
    { id: 'pub_renew',     city: '台北市', label: '公劃更新地區(依都更條例)',    color: '#FF0000', backend: 'redev_pub_renew' },
    { id: 'self_announce', city: '台北市', label: '公告自劃(事業權變)',         color: '#0000FF', backend: 'redev_self_announce' },
    { id: 'self_approved', city: '台北市', label: '核准自劃(事業權變)',         color: '#FF7F00', backend: 'redev_self_approved' },
    { id: 'planned',       city: '台北市', label: '都計劃定更新地區',           color: '#FF00FF', backend: 'redev_planned' },
    { id: '115_revised',   city: '台北市', label: '115年修訂公劃',             color: '#FF9966', backend: 'redev_115_revised' },
    { id: 'chloride',      city: '台北市', label: '高氯離子混凝土',             color: '#D0B17A', backend: 'redev_chloride' },
    { id: '63y_building',  city: '台北市', label: '63年以前建築物',             color: '#1F4E79', backend: 'redev_63y_building' },
    { id: 'urgent',        city: '台北市', label: '迅行劃定',                  color: '#FFD0FF', backend: 'redev_urgent' },
    { id: 'pub_business',  city: '台北市', label: '公劃內事業(權變)',           color: '#6495ED', backend: 'redev_pub_business' },
    { id: 'invalid',       city: '台北市', label: '已失效/廢止',                color: '#00FFFF', backend: 'redev_invalid' },
    // 新北 (4)
    { id: 'ntpc_ama',    city: '新北市', label: '都市更新事業計畫案', color: '#FF7F00', backend: 'redev_ntpc_ama' },
    { id: 'ntpc_easy',   city: '新北市', label: '簡易都更',           color: '#0066CC', backend: 'redev_ntpc_easy' },
    { id: 'ntpc_danger', city: '新北市', label: '危老重建',           color: '#FF0000', backend: 'redev_ntpc_danger' },
    { id: 'ntpc_amdm',   city: '新北市', label: '防災案件',           color: '#9933CC', backend: 'redev_ntpc_amdm' },
  ];

  // city → 全選 checkbox 的 data-renewal-all 值 (區分兩個全選 checkbox)
  const RENEWAL_CITIES = [
    { tag: 'tpe',  label: '台北市', city: '台北市' },
    { tag: 'ntpc', label: '新北市', city: '新北市' },
  ];

  const OPACITY = 0.5;

  // 內部 state
  const _state = {
    map: null,
    inited: false,
    on: { zoning: false, cadastral: false, building_floors: false },   // cadastral group
    layerRefs: { zoning: [], cadastral: [], building_floors: [] },     // cadastral group
    renewal: {
      subs: {},                    // { sub_id: bool } 個別勾選狀態
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
    const allChecks = RENEWAL_CITIES.map(c =>
      `<label class="v2-renewal-all"><input type="checkbox" data-renewal-all="${c.tag}"> ${c.label}全選</label>`
    ).join('');
    const subItems = RENEWAL_SUBS.map(s =>
      `<label class="v2-renewal-sub" data-sub-id="${s.id}" data-sub-city="${s.city}">` +
        `<input type="checkbox" data-renewal-sub="${s.id}">` +
        `<span class="v2-renewal-dot" style="background:${s.color}"></span>` +
        `<span class="v2-renewal-text">${s.label}</span>` +
      `</label>`
    ).join('');
    host.innerHTML = `
      <div class="v2-renewal-header">
        <span class="v2-overlays-label">都更圖層：</span>
        ${allChecks}
        <button type="button" class="v2-renewal-expand" data-renewal-expanded="0" aria-label="展開細項">▾</button>
      </div>
      <div class="v2-renewal-grid" id="v2-renewal-grid" style="display:none;">${subItems}</div>`;
    host.addEventListener('change', (e) => {
      const t = e.target;
      if (!t) return;
      if (t.matches('input[data-renewal-all]')) {
        const tag = t.dataset.renewalAll;
        const cityCfg = RENEWAL_CITIES.find(c => c.tag === tag);
        if (cityCfg) _toggleRenewalAll(cityCfg.city, t.checked);
      } else if (t.matches('input[data-renewal-sub]')) {
        _toggleRenewalSub(t.dataset.renewalSub, t.checked);
      }
    });
    host.addEventListener('click', (e) => {
      const t = e.target;
      if (t && t.classList && t.classList.contains('v2-renewal-expand')) {
        e.preventDefault();
        const expanded = t.dataset.renewalExpanded === '1';
        t.dataset.renewalExpanded = expanded ? '0' : '1';
        t.textContent = expanded ? '▾' : '▴';
        const grid = document.getElementById('v2-renewal-grid');
        if (grid) grid.style.display = expanded ? 'none' : '';
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

  // 「全選」: 勾起 / 取消單一城市的所有 sub-checkbox (台北/新北 各自獨立全選)
  function _toggleRenewalAll(city, on) {
    RENEWAL_SUBS.forEach(s => {
      if (s.city !== city) return;
      const cb = document.querySelector(`input[data-renewal-sub="${s.id}"]`);
      if (cb && cb.checked !== on) {
        cb.checked = on;
        _toggleRenewalSub(s.id, on);
      }
    });
  }

  // 個別 sub toggle: add/remove 單個 sub layer。任一取消會 sync 全選 checkbox
  function _toggleRenewalSub(subId, on) {
    const m = _state.map;
    const sub = RENEWAL_SUBS.find(s => s.id === subId);
    if (!m || !sub) return;
    _state.renewal.subs[subId] = on;

    const paneName = _ensureRenewalPane();
    if (on) {
      // 已存在的先清，避免 toggle 過快累積
      const old = _state.renewal.layerSubs[subId];
      if (old) m.removeLayer(old);
      // className 給 CSS SVG filter 染色用 (server 是 grayscale，前端 colorize)
      const layer = _makeWmsLayer(sub.backend, paneName, 'redev-color-' + subId);
      layer.addTo(m);
      _state.renewal.layerSubs[subId] = layer;
    } else {
      const layer = _state.renewal.layerSubs[subId];
      if (layer) {
        m.removeLayer(layer);
        delete _state.renewal.layerSubs[subId];
      }
    }
    // sync 各城市「全選」checkbox：該城市所有 sub 都 checked → 勾起；任一 unchecked → 取消
    RENEWAL_CITIES.forEach(c => {
      const allCheckbox = document.querySelector(`input[data-renewal-all="${c.tag}"]`);
      if (!allCheckbox) return;
      const cityAllOn = RENEWAL_SUBS
        .filter(s => s.city === c.city)
        .every(s => _state.renewal.subs[s.id]);
      allCheckbox.checked = cityAllOn;
    });
  }

  // ════════════════════════════════════════════════════════════
  // 共用 helper：build L.tileLayer.wms instance
  // ════════════════════════════════════════════════════════════
  function _makeWmsLayer(backend, paneName, extraClassName) {
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
    if (extraClassName) opts.className = extraClassName;   // 給 SVG filter colorize 用
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
