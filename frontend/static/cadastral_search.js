/**
 * 地塊搜尋 module — 地圖模式「輸入區+段+地號 → 飛到該地塊」功能。
 *
 * 隔離原則（同 map_overlays.js 設計）：
 *   - IIFE 純粹 attach 到 window.v2._cadastralSearch.init(map)
 *   - 不改既有 function、不註冊任何全域 listener 在 module 外
 *   - revert：刪此 file + index2.html 的 <script> tag + map_mode.js 的 1 行 init 呼叫
 *
 * UI：
 *   - 地圖左上角浮動「🔍 地號」按鈕（跟 overlay toolbar 旁邊）
 *   - 點開 → 展開 inline form（區 dropdown + 段 input + 地號 input + 搜尋按鈕）
 *   - 找到 → 飛到該地塊 + 紅色高亮 polygon
 *   - 找不到 → toast 提示
 *
 * MVP：只支援台北 8 區（新北資料源 NtpcURInfo 待後續實作）
 */
(function () {
  'use strict';

  // 12 區清單 + city mapping (台北 8 + 新北 4)
  const DISTRICTS = [
    { city: '台北市', name: '大安區' },
    { city: '台北市', name: '信義區' },
    { city: '台北市', name: '中山區' },
    { city: '台北市', name: '中正區' },
    { city: '台北市', name: '文山區' },
    { city: '台北市', name: '松山區' },
    { city: '台北市', name: '萬華區' },
    { city: '台北市', name: '大同區' },
    { city: '新北市', name: '板橋區' },
    { city: '新北市', name: '新店區' },
    { city: '新北市', name: '中和區' },
    { city: '新北市', name: '永和區' },
  ];
  function _cityOf(district) {
    const m = DISTRICTS.find(d => d.name === district);
    return m ? m.city : '台北市';
  }

  let _map = null;
  let _highlightLayer = null;
  let _loadingLayer = null;     // 點地塊查詢中時的灰色 placeholder
  let _expanded = false;
  // 前端 segment cache：{district: [segment, ...]}
  const _segCache = {};
  // 已查過的 plot result cache (in-memory)：[{ bbox, coords, data }]，最多 100 筆
  const _plotCache = [];
  const _PLOT_CACHE_MAX = 100;

  function init(map) {
    if (!map) return;
    _map = map;                 // 切換 view mode 時新 map → 用新 instance
    // DOM 只建一次（idempotent，多次 init 不重複加 UI）
    if (!document.getElementById('v2-cadsearch-toggle')) {
      _renderUI();
    }
    // 地圖空白處 click → 查該點地塊資料 (Leaflet 預設 marker click 不會 bubble 到 map.on(click))
    // 重複 init 時 off 舊 listener 再 on 新的，避免 attached 多次
    map.off('click', _onMapClick);
    map.on('click', _onMapClick);
  }

  async function _onMapClick(e) {
    if (!e || !e.latlng) return;
    // 嚴格擋：z<18 (地籍圖最小 zoom) 或地籍圖 toggle 沒勾 → silent return 不耗資源 (user 要求)
    // 用戶看不到地籍圖時不該觸發查詢
    if (!_map || _map.getZoom() < 18) return;
    try {
      if (window.v2 && window.v2._overlays && typeof window.v2._overlays.isOn === 'function') {
        if (!window.v2._overlays.isOn('cadastral')) return;
      }
    } catch (_e) { /* 取不到 toggle state 就保守不擋 — 至少 z 條件先把關 */ }
    const { lat, lng } = e.latlng;
    // 0. 先檢查 in-memory cache：之前查過的地塊 polygon 是否包含這個 click 點
    const cached = _findInPlotCache(lat, lng);
    if (cached) {
      _drawHighlight(cached);
      _showResultPopup(cached);
      return;
    }
    // 1. 立即在 click 位置畫個灰色 placeholder + 「讀取中」popup → 視覺回饋
    _showLoadingAt(lat, lng);
    await _queryAtPoint(lat, lng);
  }

  // 直接 DOM overlay 顯示 loading — 不經過 Leaflet renderer，避免被 tile redraw 排隊卡住
  // (Leaflet 的 popup/circleMarker addTo 會 trigger render cycle，tile 在 flight 時可能延遲)
  function _showLoadingAt(lat, lng) {
    _clearLoading();
    if (!_map) return;
    const point = _map.latLngToContainerPoint([lat, lng]);
    const mapEl = _map.getContainer();
    const rect = mapEl.getBoundingClientRect();
    const div = document.createElement('div');
    div.id = 'v2-cadsearch-loading';
    div.style.cssText = [
      'position:fixed',
      `left:${rect.left + point.x - 80}px`,
      `top:${rect.top + point.y + 20}px`,
      'z-index:1000',
      'background:#fff',
      'border:1px solid #999',
      'border-radius:6px',
      'padding:6px 12px',
      'box-shadow:0 2px 6px rgba(0,0,0,.12)',
      'font-size:12px',
      'color:#555',
      'pointer-events:none',
      'white-space:nowrap',
    ].join(';');
    div.innerHTML = '<span style="display:inline-block;width:11px;height:11px;border:2px solid #888;border-top-color:transparent;border-radius:50%;animation:v2-spin 0.8s linear infinite;vertical-align:middle;margin-right:6px;"></span>讀取地塊資料中…';
    document.body.appendChild(div);
    _loadingLayer = div;
  }

  function _clearLoading() {
    if (_loadingLayer) {
      try { _loadingLayer.remove(); } catch (_e) {}
      _loadingLayer = null;
    }
  }

  function _findInPlotCache(lat, lng) {
    for (const entry of _plotCache) {
      const [w, s, e, n] = entry.bbox;
      if (lat < s || lat > n || lng < w || lng > e) continue;   // 快速 bbox 排除
      if (_pointInGeoJSONPolygon(lat, lng, entry.coords)) return entry.data;
    }
    return null;
  }

  function _addToPlotCache(data) {
    if (!data || !data.polygon || !data.bbox) return;
    // 已存在 (用 district+segment+landno 判斷) → skip
    const key = `${data.district}_${data.segment}_${data.landno}`;
    if (_plotCache.some(c => c._key === key)) return;
    _plotCache.push({
      _key: key,
      bbox: data.bbox,
      coords: data.polygon.coordinates,
      data: data,
    });
    // FIFO trim
    while (_plotCache.length > _PLOT_CACHE_MAX) _plotCache.shift();
  }

  // ray-casting algorithm for point-in-polygon (GeoJSON Polygon = [outerRing, hole1, ...])
  function _pointInRing(lat, lng, ring) {
    let inside = false;
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const xi = ring[i][0], yi = ring[i][1];
      const xj = ring[j][0], yj = ring[j][1];
      const intersect = (yi > lat) !== (yj > lat)
        && lng < (xj - xi) * (lat - yi) / (yj - yi) + xi;
      if (intersect) inside = !inside;
    }
    return inside;
  }
  function _pointInGeoJSONPolygon(lat, lng, coords) {
    if (!coords || !coords.length) return false;
    // GeoJSON Polygon: [outerRing, hole1, hole2, ...]
    if (!_pointInRing(lat, lng, coords[0])) return false;
    for (let i = 1; i < coords.length; i++) {
      if (_pointInRing(lat, lng, coords[i])) return false;
    }
    return true;
  }

  async function _queryAtPoint(lat, lng) {
    try {
      const qs = new URLSearchParams({ lat: String(lat), lng: String(lng) }).toString();
      // priority:'high' 讓瀏覽器把 at_point 排在大量 tile fetch 之前 (Chrome 102+)
      // 舊瀏覽器忽略此 option，無 side effect
      const r = await fetch(`/api/cadastral_search/at_point?${qs}`, { priority: 'high' });
      _clearLoading();
      if (!r.ok) {
        if (_resultPopup) { try { _map.closePopup(_resultPopup); } catch (_e) {} }
        const detail = await r.json().catch(() => ({}));
        _toast(`查詢失敗 (${r.status})：${detail.detail || ''}`, 'error');
        return;
      }
      const data = await r.json();
      if (!data.ok) {
        if (_resultPopup) { try { _map.closePopup(_resultPopup); } catch (_e) {} }
        _toast(data.reason || '此位置無地籍資料', 'info');
        return;
      }
      _addToPlotCache(data);
      _drawHighlight(data);
      _showResultPopup(data);
    } catch (e) {
      _clearLoading();
      _toast(`網路錯誤：${e.message || e}`, 'error');
    }
  }

  function _renderUI() {
    // Container：放在 .v2-map-toolbar 旁邊（如果有），否則 fallback 到 map container 左上
    const host = document.querySelector('.v2-map-overlays-toolbar')
      || document.querySelector('.v2-map-toolbar')
      || _map.getContainer().parentElement;
    if (!host) return;

    const wrap = document.createElement('div');
    wrap.className = 'v2-cadsearch';
    wrap.innerHTML = `
      <button type="button" class="v2-cadsearch__toggle" id="v2-cadsearch-toggle"
              aria-label="地號" title="地號">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"
             stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
          <circle cx="11" cy="11" r="7"/>
          <line x1="21" y1="21" x2="16.5" y2="16.5"/>
        </svg>
        <span>地號</span>
      </button>
      <div class="v2-cadsearch__panel" id="v2-cadsearch-panel" style="display:none">
        <div class="v2-cadsearch__row">
          <label class="v2-cadsearch__label">區</label>
          <select id="v2-cadsearch-district" class="v2-cadsearch__input">
            <optgroup label="台北市">
              ${DISTRICTS.filter(d => d.city === '台北市').map(d => `<option value="${d.name}">${d.name}</option>`).join('')}
            </optgroup>
            <optgroup label="新北市">
              ${DISTRICTS.filter(d => d.city === '新北市').map(d => `<option value="${d.name}">${d.name}</option>`).join('')}
            </optgroup>
          </select>
        </div>
        <div class="v2-cadsearch__row">
          <label class="v2-cadsearch__label">段</label>
          <select id="v2-cadsearch-segment" class="v2-cadsearch__input">
            <option value="">載入中…</option>
          </select>
        </div>
        <div class="v2-cadsearch__row">
          <label class="v2-cadsearch__label">地號</label>
          <input type="text" id="v2-cadsearch-landno" class="v2-cadsearch__input"
                 placeholder="例：123 或 123-5" autocomplete="off"/>
        </div>
        <div class="v2-cadsearch__row v2-cadsearch__actions">
          <button type="button" class="v2-cadsearch__btn v2-cadsearch__btn--primary"
                  id="v2-cadsearch-go">搜尋</button>
          <button type="button" class="v2-cadsearch__btn" id="v2-cadsearch-clear">清除</button>
        </div>
        <div class="v2-cadsearch__hint">支援台北 8 區 + 新北 4 區（板橋/新店/中和/永和）</div>
      </div>
    `;
    host.appendChild(wrap);

    document.getElementById('v2-cadsearch-toggle').addEventListener('click', _toggle);
    document.getElementById('v2-cadsearch-go').addEventListener('click', _onSearch);
    document.getElementById('v2-cadsearch-clear').addEventListener('click', _onClear);
    // 區改變 → fetch 該區段名 dropdown
    document.getElementById('v2-cadsearch-district').addEventListener('change', _onDistrictChange);
    // Enter submit: 地號 input 接 Enter
    document.getElementById('v2-cadsearch-landno').addEventListener('keydown', e => {
      if (e.key === 'Enter') _onSearch();
    });
    // 初次載入：default district 的段名清單
    _loadSegments(DISTRICTS[0].name);
  }

  async function _onDistrictChange() {
    const district = document.getElementById('v2-cadsearch-district').value;
    await _loadSegments(district);
  }

  async function _loadSegments(district) {
    const segSel = document.getElementById('v2-cadsearch-segment');
    if (!segSel) return;
    // Cache hit → 直接填
    if (_segCache[district]) {
      _fillSegmentOptions(_segCache[district]);
      return;
    }
    // Cache miss → fetch
    segSel.innerHTML = '<option value="">載入中…</option>';
    segSel.disabled = true;
    try {
      const qs = new URLSearchParams({ city: _cityOf(district), district }).toString();
      const r = await fetch(`/api/cadastral_search/segments?${qs}`);
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        _toast(`段清單載入失敗 (${r.status})：${d.detail || ''}`, 'error');
        segSel.innerHTML = '<option value="">載入失敗</option>';
        return;
      }
      const data = await r.json();
      const segs = data.segments || [];
      _segCache[district] = segs;
      _fillSegmentOptions(segs);
    } catch (e) {
      _toast(`網路錯誤：${e.message || e}`, 'error');
      segSel.innerHTML = '<option value="">載入失敗</option>';
    } finally {
      segSel.disabled = false;
    }
  }

  function _fillSegmentOptions(segments) {
    const segSel = document.getElementById('v2-cadsearch-segment');
    if (!segSel) return;
    if (!segments.length) {
      segSel.innerHTML = '<option value="">(無資料)</option>';
      return;
    }
    segSel.innerHTML = '<option value="">— 請選擇段 —</option>'
      + segments.map(s => `<option value="${s}">${s}</option>`).join('');
  }

  function _toggle() {
    _expanded = !_expanded;
    document.getElementById('v2-cadsearch-panel').style.display = _expanded ? 'block' : 'none';
    if (_expanded) {
      // 開啟 panel 時聚焦到「段」input
      setTimeout(() => document.getElementById('v2-cadsearch-segment').focus(), 50);
    }
  }

  function _toast(msg, kind) {
    // 重用 v2.toast 如果有（app2.js 內部 fn），否則 fallback alert
    if (window.v2 && typeof window.v2._toast === 'function') {
      window.v2._toast(msg, kind || 'info');
    } else {
      // 用 v2-toast-host 直接渲染（跟 app2.js toast 同 host）
      const host = document.getElementById('v2-toast-host');
      if (host) {
        const el = document.createElement('div');
        el.className = `v2-toast v2-toast--${kind || 'info'}`;
        el.textContent = msg;
        host.appendChild(el);
        setTimeout(() => el.remove(), 4000);
      } else {
        alert(msg);
      }
    }
  }

  async function _onSearch() {
    const district = (document.getElementById('v2-cadsearch-district').value || '').trim();
    const segment = (document.getElementById('v2-cadsearch-segment').value || '').trim();
    const landno = (document.getElementById('v2-cadsearch-landno').value || '').trim();
    if (!segment) { _toast('請選擇段', 'error'); return; }
    if (!landno) { _toast('請輸入地號', 'error'); return; }

    const goBtn = document.getElementById('v2-cadsearch-go');
    goBtn.disabled = true;
    goBtn.textContent = '搜尋中…';
    try {
      // 用相對路徑 — auth_gate.js 的 fetch override 只攔 startsWith('/api/')，
      // 用絕對 URL 會繞過 Authorization header 注入 → 401「缺少登入憑證」
      const qs = new URLSearchParams({
        city: _cityOf(district), district, segment, landno,
      }).toString();
      const r = await fetch(`/api/cadastral_search/lookup?${qs}`);
      if (!r.ok) {
        const detail = await r.json().catch(() => ({}));
        _toast(`查詢失敗 (${r.status})：${detail.detail || '請稍後重試'}`, 'error');
        return;
      }
      const data = await r.json();
      if (!data.ok) {
        _toast(data.reason || '找不到該地塊', 'error');
        return;
      }
      _addToPlotCache(data);
      _drawHighlight(data);
      _flyToBbox(data.bbox);
      _showResultPopup(data);
    } catch (e) {
      _toast(`網路錯誤：${e.message || e}`, 'error');
    } finally {
      goBtn.disabled = false;
      goBtn.textContent = '搜尋';
    }
  }

  function _onClear() {
    if (_highlightLayer && _map) {
      try { _map.removeLayer(_highlightLayer); } catch (_e) { /* noop */ }
      _highlightLayer = null;
    }
    if (_resultPopup && _map) {
      try { _map.closePopup(_resultPopup); } catch (_e) {}
      _resultPopup = null;
    }
    document.getElementById('v2-cadsearch-segment').value = '';
    document.getElementById('v2-cadsearch-landno').value = '';
  }

  // 民國年月日 7 字元 (e.g., '0670409') → 西元日期 + 民國年
  function _rocToDateStr(roc) {
    if (!roc || roc.length !== 7) return roc || '—';
    const rocY = parseInt(roc.slice(0, 3), 10);
    const m = roc.slice(3, 5), d = roc.slice(5, 7);
    return `${rocY + 1911}/${m}/${d} (民國${rocY})`;
  }

  // 整數金額 → 萬元 / 億元 readable string
  function _toWanReadable(n) {
    if (n == null || !isFinite(n)) return '—';
    const wan = n / 10000;
    if (wan >= 10000) return (wan / 10000).toFixed(2) + ' 億元';
    if (wan >= 1) return wan.toFixed(0) + ' 萬元';
    return n.toLocaleString('zh-TW') + ' 元';
  }

  function _fmtInt(n) {
    return (n != null && isFinite(n)) ? Math.round(n).toLocaleString('zh-TW') : '—';
  }
  function _fmt2(n) {
    return (n != null && isFinite(n)) ? n.toFixed(2) : '—';
  }

  function _buildResultHTML(d) {
    const M_PER_PING = 3.305785;
    const area_sqm = d.area_sqm;
    const area_ping = d.area_ping;
    const val_m2 = d.land_value_per_sqm;
    const val_ping = val_m2 ? val_m2 * M_PER_PING : null;
    const prc_m2 = d.land_price_per_sqm;
    const prc_ping = prc_m2 ? prc_m2 * M_PER_PING : null;

    const rows = [];
    rows.push(`<tr><td>面積</td><td><b>${_fmt2(area_ping)} 坪</b> (${_fmtInt(area_sqm)} m²)</td></tr>`);
    if (d.zoning_name) {
      const zt = d.zoning_text || d.zoning_name;
      const approx = d.zoning_approx ? ' <span style="color:#888;">(鄰近分區，僅供參考)</span>' : '';
      rows.push(`<tr><td>使用分區</td><td><b style="color:#1d4ed8;">${zt}</b>${approx}</td></tr>`);
      if (d.zoning_all && d.zoning_all.length > 1) {
        const others = d.zoning_all.slice(1).map(z => z.text || z.name).join(', ');
        rows.push(`<tr><td></td><td style="color:#888; font-size:11px;">附近還有：${others}</td></tr>`);
      }
    }
    if (val_m2 != null) {
      rows.push(`<tr><td>公告現值</td><td>${_fmtInt(val_m2)} 元/m² (≈${_fmtInt(val_ping)} 元/坪)</td></tr>`);
    }
    if (prc_m2 != null) {
      rows.push(`<tr><td>公告地價</td><td>${_fmtInt(prc_m2)} 元/m² (≈${_fmtInt(prc_ping)} 元/坪)</td></tr>`);
    }
    if (d.ownership) {
      const own = d.ownership === '公有' ? `<b style="color:#c0392b;">公有</b>` : d.ownership;
      let extra = '';
      if (d.ownership_manager) extra += `<br/><span style="color:#888; font-size:11px;">管理機關：${d.ownership_manager}</span>`;
      if (d.ownership_owner) extra += `<br/><span style="color:#888; font-size:11px;">所有人：${d.ownership_owner}</span>`;
      rows.push(`<tr><td>權屬</td><td>${own}${extra}</td></tr>`);
    }
    if (d.announce_date_roc) {
      rows.push(`<tr><td>公告日期</td><td>${_rocToDateStr(d.announce_date_roc)}</td></tr>`);
    }
    return `
      <div class="v2-cadsearch-popup">
        <h4>${d.district} ${d.segment} ${d.landno}</h4>
        <table>${rows.join('')}</table>
        <div class="v2-cadsearch-popup__hint">資料來源：政府公告現值/地價（依年度公告值）</div>
      </div>
    `;
  }

  let _resultPopup = null;
  function _showResultPopup(data) {
    if (!_map || !window.L) return;
    if (_resultPopup) {
      try { _map.closePopup(_resultPopup); } catch (_e) {}
    }
    const html = _buildResultHTML(data);
    const [lng, lat] = data.center;
    _resultPopup = L.popup({
      maxWidth: 340,
      autoClose: false,
      closeOnClick: false,
      className: 'v2-cadsearch-popup-wrap',
    })
      .setLatLng([lat, lng])
      .setContent(html)
      .openOn(_map);
  }

  function _drawHighlight(result) {
    if (!_map || !window.L) return;
    if (_highlightLayer) {
      try { _map.removeLayer(_highlightLayer); } catch (_e) { /* noop */ }
    }
    _highlightLayer = L.geoJSON(result.polygon, {
      style: {
        color: '#dc2626',         // 紅色邊框
        weight: 3,
        fillColor: '#dc2626',
        fillOpacity: 0.25,
      },
    });
    _highlightLayer.addTo(_map);
  }

  function _flyToBbox(bbox) {
    if (!_map || !window.L || !bbox || bbox.length !== 4) return;
    const [w, s, e, n] = bbox;
    try {
      _map.flyToBounds([[s, w], [n, e]], { padding: [50, 50], maxZoom: 19, duration: 0.8 });
    } catch (_e) {
      // fallback：setView 中心
      const cx = (w + e) / 2;
      const cy = (s + n) / 2;
      _map.setView([cy, cx], 19);
    }
  }

  window.v2 = window.v2 || {};
  window.v2._cadastralSearch = { init };
})();
