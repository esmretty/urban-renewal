/**
 * v2 地圖模式 — optional 圖層 multi-layer manager
 *
 * 隔離原則（plan 約定）：
 *   - 整個 module 自包含（IIFE），attach 到 window.v2._overlays
 *   - 不改 map_mode.js 內部 function；只在 _initMap 末尾插 1 行 hook：
 *     `window.v2._overlays?.init(m)`
 *   - revert：刪本 file + index2.html script tag + map_mode.js 那 1 行 hook
 *
 * 提供 layers（每個 = 1 個 checkbox）：
 *   - zoning      土地分區（雙北 WMS forward）
 *   - cadastral   地籍圖   （台北 WMS；新北未實作，後端會回透明 PNG）
 *   - renewal     都更/危老 (Phase C 待 spike)
 *
 * 跟 map_mode.js 的 coupling（minimal）：
 *   - _initMap 末尾呼叫 window.v2._overlays.init(map) 1 行
 *   - 不依賴 map_mode.js 任何 internal state（自己存 _layerRefs / _on）
 *
 * z-index 由下到上（每 pane zIndex 比上一個大）：
 *   分區 (401) → 地籍 (402) → 都更 (403)
 *   markers 預設 zIndex 600 仍在最上方，物件可點。
 */
(function () {
  'use strict';

  // 三個圖層定義 — 每個含「同時要載入的 server-side layer 名稱」
  // server 會根據 bbox 自動 short-circuit 不在該市範圍的 request
  const LAYERS = {
    zoning: {
      label: '土地分區',
      paneZ: 401,
      backends: ['zoning_tpe', 'zoning_ntpc'],   // 兩個 WMS source 同時疊
    },
    cadastral: {
      label: '地籍圖',
      paneZ: 402,
      backends: ['cadastral_tpe'],   // Phase A 只台北；新北 phase A.5 補
    },
    renewal: {
      label: '都更/危老',
      paneZ: 403,
      backends: [],   // Phase C 待 spike，暫時 disabled
      disabled: true,
    },
  };

  const OPACITY = 0.4;

  // 內部 state — 每個 layer 對應 N 個 Leaflet wms layer instance（雙北各一份）
  const _state = {
    map: null,
    inited: false,
    on: { zoning: false, cadastral: false, renewal: false },
    layerRefs: { zoning: [], cadastral: [], renewal: [] },
  };

  function _renderToolbar() {
    const host = document.getElementById('v2-map-overlays-toolbar');
    if (!host) return;   // index2.html 沒這 div = 沒掛載 = no-op
    if (host.dataset.rendered === '1') return;   // 已 render 過
    host.dataset.rendered = '1';
    host.innerHTML = '<span class="v2-overlays-label">圖層：</span>' +
      Object.entries(LAYERS).map(([key, cfg]) => {
        const disabled = cfg.disabled ? ' disabled' : '';
        const note = cfg.disabled ? ' <span class="v2-overlays-note">(準備中)</span>' : '';
        return `<label class="v2-overlays-checkbox${cfg.disabled ? ' is-disabled' : ''}">` +
          `<input type="checkbox" data-overlay="${key}"${disabled}> ${cfg.label}${note}</label>`;
      }).join('');

    host.addEventListener('change', (e) => {
      const t = e.target;
      if (!t || !t.matches('input[data-overlay]')) return;
      const key = t.dataset.overlay;
      _toggle(key, t.checked);
    });
  }

  function _toggle(key, on) {
    const cfg = LAYERS[key];
    const m = _state.map;
    if (!cfg || !m) return;
    if (cfg.disabled) return;
    _state.on[key] = on;

    // 先清掉舊的 (避免 toggle 過快累積 layer ref)
    (_state.layerRefs[key] || []).forEach(l => m.removeLayer(l));
    _state.layerRefs[key] = [];

    if (!on) return;

    const paneName = 'v2-overlay-' + key;
    if (!m.getPane(paneName)) {
      const pane = m.createPane(paneName);
      pane.style.zIndex = String(cfg.paneZ);
      pane.style.pointerEvents = 'none';   // overlay 不擋 marker click
    }

    cfg.backends.forEach(backend => {
      // 每個 backend 一個 L.tileLayer.wms（Leaflet 會自己分塊抓）
      // server-side 會根據 bbox 自動只給該市範圍，跨市的 tile 會回透明 PNG (504)
      const layer = L.tileLayer.wms('/api/gis_overlay/' + backend, {
        layers: backend,   // server 用 path 認 layer，這個 query param 對 Leaflet 不重要但 WMS spec 要有
        format: 'image/png',
        transparent: true,
        opacity: OPACITY,
        pane: paneName,
        // L.tileLayer 預設 maxZoom=18，會比底圖 OSM (maxZoom=19) 早消失 → 顯式拉高
        // 22 留 buffer 給未來底圖換成更高 zoom 的 tile source
        maxZoom: 22,
        // server 預期 EPSG:3857 (Leaflet 預設)
      });
      // 政府 server 偶爾失敗 → tile 拿 504 / non-image → silent skip
      layer.on('tileerror', (e) => {
        // 不噴 alert / toast，只 console.debug；單 tile fail 是正常 (跨市)
        console.debug('[overlay tile error]', backend, e.tile?.src);
      });
      layer.addTo(m);
      _state.layerRefs[key].push(layer);
    });
  }

  // ── public API：給 map_mode.js _initMap 末尾呼叫 ─────────────────────
  function init(map) {
    if (_state.inited) return;
    _state.inited = true;
    _state.map = map;
    _renderToolbar();
  }

  // ── 安裝到 window.v2 (等 map_mode.js 先 attach v2 再掛) ──
  function _install() {
    if (!window.v2) {
      setTimeout(_install, 50);
      return;
    }
    window.v2._overlays = { init };
    // race-safe：若 map_mode.js _initMap 已經跑過（用戶在本 module 載入前就切到地圖模式），
    // map_mode.js 那行 hook 會 silent miss → 這裡補打一次
    const existingMap = window.v2.state && window.v2.state._mapInst;
    if (existingMap) init(existingMap);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _install);
  } else {
    _install();
  }
})();
