/**
 * 地塊搜尋 module — 地圖模式「輸入區+段+地號 → 飛到該地塊」功能。
 *
 * 隔離原則（同 map_overlays.js 設計）：
 *   - IIFE 純粹 attach 到 window.v2._cadastralSearch.init(map)
 *   - 不改既有 function、不註冊任何全域 listener 在 module 外
 *   - revert：刪此 file + index2.html 的 <script> tag + map_mode.js 的 1 行 init 呼叫
 *
 * UI：
 *   - 地圖左上角浮動「🔍 搜尋地塊」按鈕（跟 overlay toolbar 旁邊）
 *   - 點開 → 展開 inline form（區 dropdown + 段 input + 地號 input + 搜尋按鈕）
 *   - 找到 → 飛到該地塊 + 紅色高亮 polygon
 *   - 找不到 → toast 提示
 *
 * MVP：只支援台北 8 區（新北資料源 NtpcURInfo 待後續實作）
 */
(function () {
  'use strict';

  const TPE_DISTRICTS = [
    '大安區', '信義區', '中山區', '中正區', '文山區',
    '松山區', '萬華區', '大同區',
  ];

  let _map = null;
  let _highlightLayer = null;
  let _expanded = false;

  function init(map) {
    if (!map) return;
    _map = map;                 // 切換 view mode 時新 map → 用新 instance
    // DOM 只建一次（idempotent，多次 init 不重複加 UI）
    if (!document.getElementById('v2-cadsearch-toggle')) {
      _renderUI();
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
              aria-label="搜尋地塊" title="搜尋地塊">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"
             stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
          <circle cx="11" cy="11" r="7"/>
          <line x1="21" y1="21" x2="16.5" y2="16.5"/>
        </svg>
        <span>搜尋地塊</span>
      </button>
      <div class="v2-cadsearch__panel" id="v2-cadsearch-panel" style="display:none">
        <div class="v2-cadsearch__row">
          <label class="v2-cadsearch__label">區</label>
          <select id="v2-cadsearch-district" class="v2-cadsearch__input">
            ${TPE_DISTRICTS.map(d => `<option value="${d}">${d}</option>`).join('')}
          </select>
        </div>
        <div class="v2-cadsearch__row">
          <label class="v2-cadsearch__label">段</label>
          <input type="text" id="v2-cadsearch-segment" class="v2-cadsearch__input"
                 placeholder="例：龍泉段一小段" autocomplete="off"/>
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
        <div class="v2-cadsearch__hint">目前只支援台北市，新北市資料源待後續整合</div>
      </div>
    `;
    host.appendChild(wrap);

    document.getElementById('v2-cadsearch-toggle').addEventListener('click', _toggle);
    document.getElementById('v2-cadsearch-go').addEventListener('click', _onSearch);
    document.getElementById('v2-cadsearch-clear').addEventListener('click', _onClear);
    // Enter submit: 段 / 地號 兩個 input 都接 Enter
    ['v2-cadsearch-segment', 'v2-cadsearch-landno'].forEach(id => {
      document.getElementById(id).addEventListener('keydown', e => {
        if (e.key === 'Enter') _onSearch();
      });
    });
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
    if (!segment) { _toast('請輸入段名', 'error'); return; }
    if (!landno) { _toast('請輸入地號', 'error'); return; }

    const goBtn = document.getElementById('v2-cadsearch-go');
    goBtn.disabled = true;
    goBtn.textContent = '搜尋中…';
    try {
      const u = new URL('/api/cadastral_search/lookup', location.origin);
      u.searchParams.set('city', '台北市');
      u.searchParams.set('district', district);
      u.searchParams.set('segment', segment);
      u.searchParams.set('landno', landno);
      const r = await fetch(u.toString());
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
      _drawHighlight(data);
      _flyToBbox(data.bbox);
      _toast(`找到 ${district} ${segment} ${landno}`, 'success');
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
    document.getElementById('v2-cadsearch-segment').value = '';
    document.getElementById('v2-cadsearch-landno').value = '';
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
