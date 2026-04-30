/**
 * 都更神探R 前端 JavaScript
 */

// ── XSS 防護：HTML escape helper ────────────────────────────────────────────
// 任何「會進 innerHTML 的後端/爬蟲/用戶文字」都要先過 esc()。
// 純常數字串、已經 encodeURIComponent 過的 URL、已知格式的數字不需要（但包了也不會壞）。
// 屋齡顯示：優先用 completed_year 重算當下屋齡（爬蟲時存的，會跟年份走），
// 缺漏才回退用 building_age（舊資料 / scrape 時沒寫 completed_year）
function currentAge(p) {
  if (!p) return null;
  const yc = p.building_age_completed_year;
  if (yc && Number.isFinite(yc) && yc > 1900) {
    return new Date().getFullYear() - yc;
  }
  return p.building_age ?? null;
}

function esc(s) {
  if (s === null || s === undefined) return "";
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

// ── 狀態 ─────────────────────────────────────────────────────────────────────
let allProperties = [];
let filteredProperties = [];
let selectedId = null;
let mapView = false;
let map = null;
let markers = {};
let detailModal = null;

// ── 初始化 ────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  initMap();
  detailModal = new bootstrap.Modal(document.getElementById("detailModal"));
  // modal 關閉時：若有 ephemeral 調整（物件不在觀察清單）→ 輕柔提示
  document.getElementById("detailModal").addEventListener("hidden.bs.modal", () => {
    if (_detailP && _detailP._ephemeral_edit_made && !_detailP._in_watchlist) {
      showFadingToast("您剛才的數字改動沒有自動儲存。\n請先把本物件加入觀察清單，之後任何數字改動都會自動儲存。");
    }
    if (_detailP) _detailP._ephemeral_edit_made = false;
  });
  restoreThresholds();
  // 「隱藏不易都更物件」chip 跨 tab 共用，從獨立 localStorage 還原（不綁 explore filter set）
  // 預設勾起來（隱藏偏遠路段 + 特殊土地分區）；舊 localStorage key "include-remote" 已 deprecated
  try {
    const hn = document.getElementById("hide-non-renewable");
    if (hn) {
      const v = localStorage.getItem("hide-non-renewable");
      hn.checked = (v === null) ? true : (v === "1");
    }
  } catch {}
  _initDistPicker();
  populateDistrictFilter();
  populateManualDistricts();
  // 預設打開「搜尋網上物件」tab，並依當前 filter 立刻跑一次搜尋
  _activeTab = "explore";
  document.body.dataset.tab = "explore";
  document.querySelectorAll(".tab-btn, .app-nav").forEach(btn => {
    btn.classList.toggle("tab-btn--active", btn.dataset.tab === "explore");
    btn.classList.toggle("active", btn.dataset.tab === "explore");
  });
  const exploreSearch = document.getElementById("explore-search");
  if (exploreSearch) exploreSearch.classList.remove("d-none");
  renderSkeleton(6);
  const _start = () => {
    _restoreExploreFilters();   // 用戶 filter 偏好還原（uid scoped）
    switchTab("explore");
    // 自動跑一次搜尋（用戶不用按按鈕）
    if (typeof window.runExploreSearch === "function") {
      window.runExploreSearch();
    }
    loadStats();
    // 從 server 拉 LVR 預售屋中位數覆寫 DISTRICT_NEW_HOUSE_PRICE（auth ready 後）
    fetchDistrictPrices();
  };
  if (window.__authReady) _start();
  else document.addEventListener("auth:ready", _start, { once: true });
});

// ── 搜尋 tab filter 持久化（per-uid localStorage） ────────────────────────────
function _exploreFilterKey() {
  const uid = (window.currentUser && window.currentUser.uid) || "anon";
  return `explore-filters:${uid}`;
}

function _saveExploreFilters() {
  const obj = {
    road: document.getElementById("filter-road")?.value || "",
    dists: Array.from(document.querySelectorAll(".filter-dist"))
      .filter(c => c.checked).map(c => `${c.dataset.city}|${c.value}`),
    btypes: Array.from(document.querySelectorAll(".filter-btype:not(:disabled)"))
      .filter(c => c.checked).map(c => c.value),
    floors: Array.from(document.querySelectorAll("#floor-chips input"))
      .filter(c => c.checked).map(c => c.value),
    pmin: document.getElementById("filter-price-min")?.value || "",
    pmax: document.getElementById("filter-price-max")?.value || "",
    maxBld: document.getElementById("filter-max-bld-price")?.value || "",
    maxLand: document.getElementById("filter-max-land-price")?.value || "",
    minLandP: document.getElementById("filter-min-land-ping")?.value || "",
    sortBy: document.getElementById("sort-by")?.value || "list_rank",
    sortDir: sortDir,
    hideBad: !!document.getElementById("explore-hide-bad")?.checked,
    hideNonRenewable: !!document.getElementById("hide-non-renewable")?.checked,
  };
  try { localStorage.setItem(_exploreFilterKey(), JSON.stringify(obj)); } catch {}
}

function _restoreExploreFilters() {
  let obj;
  try { obj = JSON.parse(localStorage.getItem(_exploreFilterKey()) || "null"); } catch {}
  if (!obj) return;
  const setVal = (id, v) => { const el = document.getElementById(id); if (el && v !== undefined && v !== null) el.value = v; };
  setVal("filter-road", obj.road);
  setVal("filter-price-min", obj.pmin);
  setVal("filter-price-max", obj.pmax);
  setVal("filter-max-bld-price", obj.maxBld);
  setVal("filter-max-land-price", obj.maxLand);
  setVal("filter-min-land-ping", obj.minLandP);
  setVal("sort-by", obj.sortBy);
  if (obj.sortDir === "asc" || obj.sortDir === "desc") {
    sortDir = obj.sortDir;
    const dirBtn = document.getElementById("sort-dir");
    if (dirBtn) dirBtn.textContent = sortDir === "desc" ? "↓" : "↑";
  }
  // 地區 restore：若 localStorage 的 dists 是空陣列或 missing，維持 HTML 預設（= 所有非 disabled 的台北市區都勾）
  if (Array.isArray(obj.dists) && obj.dists.length > 0) {
    const set = new Set(obj.dists);
    document.querySelectorAll(".filter-dist").forEach(c => {
      if (c.disabled) { c.checked = false; return; }
      c.checked = set.has(`${c.dataset.city}|${c.value}`);
    });
    // 同步「全部」chip（只看非 disabled 的區）
    document.querySelectorAll(".filter-dist-all").forEach(allEl => {
      const city = allEl.dataset.city;
      const chips = document.querySelectorAll(`.filter-dist[data-city="${city}"]:not(:disabled)`);
      const checked = Array.from(chips).filter(c => c.checked);
      allEl.checked = (checked.length === chips.length && chips.length > 0);
    });
  }
  if (Array.isArray(obj.btypes)) {
    const set = new Set(obj.btypes);
    document.querySelectorAll(".filter-btype").forEach(c => {
      if (c.disabled) { c.checked = false; return; }
      c.checked = set.has(c.value);
    });
  }
  if (Array.isArray(obj.floors)) {
    const set = new Set(obj.floors);
    document.querySelectorAll("#floor-chips input").forEach(c => {
      if (c.disabled) { c.checked = false; return; }
      c.checked = set.has(c.value);
    });
    const floorAllEl = document.getElementById("floor-all");
    if (floorAllEl) {
      const all = document.querySelectorAll("#floor-chips input");
      floorAllEl.checked = (Array.from(all).every(c => c.checked));
    }
  }
  const hb = document.getElementById("explore-hide-bad");
  if (hb && typeof obj.hideBad === "boolean") hb.checked = obj.hideBad;
  const hn = document.getElementById("hide-non-renewable");
  if (hn && typeof obj.hideNonRenewable === "boolean") hn.checked = obj.hideNonRenewable;
}

// ── Tab 切換 ──────────────────────────────────────────────────────────────
let _activeTab = "watchlist";   // watchlist | explore
let _exploreSearched = false;   // explore tab 是否已執行過搜尋
let _exploreResults = [];       // 探索 tab 暫存結果（不持久）

window.switchTab = function (tab) {
  _activeTab = tab;
  _currentPage = 1;
  document.querySelectorAll(".tab-btn, .app-nav").forEach(btn => {
    btn.classList.toggle("tab-btn--active", btn.dataset.tab === tab);
    btn.classList.toggle("active", btn.dataset.tab === tab);
  });
  // 觀察清單：只顯示 client-side filter bar
  // 搜尋網上物件：顯示 server-side 搜尋表單
  // 用 d-none class 切換（Bootstrap d-flex 用 !important，蓋過 inline style.display）
  const exploreSearch = document.getElementById("explore-search");
  if (exploreSearch) exploreSearch.classList.toggle("d-none", tab !== "explore");
  const hideBadWrap = document.getElementById("explore-hide-bad-wrap");
  if (hideBadWrap) hideBadWrap.style.display = (tab === "explore") ? "" : "none";
  const minProfitWrap = document.getElementById("explore-min-profit-wrap");
  if (minProfitWrap) minProfitWrap.style.display = (tab === "explore") ? "inline-flex" : "none";
  // 用 body[data-tab] CSS 切換 .wl-only 元件（觀察清單獨有的 filter）
  document.body.dataset.tab = tab;

  if (tab === "watchlist") {
    loadProperties();
  } else if (tab === "explore") {
    // 初始不讀，等用戶按搜尋
    allProperties = _exploreResults.slice();
    filterAndSort();
  }
};

// 把搜尋 tab 的全部 filter 值打包成 query string，送 server 做過濾
function _buildExploreParams() {
  const params = new URLSearchParams();
  const road = (document.getElementById("filter-road")?.value || "").trim();
  if (road) params.set("road", road);
  // 區
  const distInputs = document.querySelectorAll(".filter-dist");
  if (distInputs.length) {
    const picks = Array.from(distInputs).filter(c => c.checked).map(c => c.value);
    if (picks.length > 0 && picks.length < distInputs.length) {
      params.set("districts", picks.join(","));
    }
  }
  // 類型
  const btypeInputs = document.querySelectorAll(".filter-btype:not(:disabled)");
  const btypePicks = Array.from(btypeInputs).filter(c => c.checked).map(c => c.value);
  if (btypePicks.length > 0 && btypePicks.length < btypeInputs.length) {
    params.set("building_types", btypePicks.join(","));
  }
  // 樓層
  const floorInputs = document.querySelectorAll("#floor-chips input");
  const floorPicks = Array.from(floorInputs).filter(c => c.checked).map(c => c.value);
  if (floorPicks.length > 0 && floorPicks.length < floorInputs.length) {
    params.set("floors", floorPicks.join(","));
  }
  // 價格與坪數
  const pmin = document.getElementById("filter-price-min")?.value;
  if (pmin && parseFloat(pmin) > 0) params.set("min_price_wan", pmin);
  const pmax = document.getElementById("filter-price-max")?.value;
  if (pmax && parseFloat(pmax) > 0) params.set("max_price_wan", pmax);
  const maxBld = document.getElementById("filter-max-bld-price")?.value;
  if (maxBld && parseFloat(maxBld) > 0) params.set("max_bld_price_per_ping", maxBld);
  const maxLand = document.getElementById("filter-max-land-price")?.value;
  if (maxLand && parseFloat(maxLand) > 0) params.set("max_land_price_per_ping", maxLand);
  const minLandP = document.getElementById("filter-min-land-ping")?.value;
  if (minLandP && parseFloat(minLandP) > 0) params.set("min_land_ping", minLandP);
  params.set("limit", "1000");
  return params;
}

window.runExploreSearch = async function () {
  // 必須至少勾一個地區（過濾 disabled 的）
  const picks = Array.from(document.querySelectorAll(".filter-dist:not(:disabled)")).filter(c => c.checked);
  if (picks.length === 0) {
    alert("請至少勾選一個行政區。");
    return;
  }
  renderSkeleton(4);
  try {
    const r = await fetch("/api/central_search?" + _buildExploreParams().toString());
    const data = await r.json();
    _exploreResults = data.items || [];
    _exploreSearched = true;
    allProperties = _exploreResults.slice();
    _currentPage = 1;
    filterAndSort();
  } catch (e) { alert("搜尋失敗：" + e.message); }
};

window.toggleWatchlist = async function (id, btn) {
  const p = allProperties.find(x => x.id === id);
  const wasIn = p && p._in_watchlist;
  const url = `/api/watchlist/${encodeURIComponent(id)}`;
  try {
    let r;
    if (wasIn) {
      r = await fetch(url, { method: "DELETE" });
    } else {
      // 加入時：把該物件目前累積在 client 的 override（如搜尋 tab 調過的 desired_price 等）一起帶上
      const overrides = p ? _collectOverrides(p) : {};
      r = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(overrides),
      });
    }
    if (!r.ok) throw new Error("操作失敗");
    if (p) p._in_watchlist = !wasIn;
    if (btn) {
      btn.textContent = p && p._in_watchlist ? "★" : "☆";
      btn.classList.toggle("bookmarked", !!(p && p._in_watchlist));
    }
  } catch (e) { alert(e.message); }
};

const THRESHOLD_KEY = "urban-renewal-thresholds";
const DEFAULT_THRESHOLDS = { maxFloors: 5, maxTotal: 5000, maxBld: 130, maxLand: 300 };

function loadThresholds() {
  try {
    const raw = localStorage.getItem(THRESHOLD_KEY);
    if (raw) return { ...DEFAULT_THRESHOLDS, ...JSON.parse(raw) };
  } catch {}
  return { ...DEFAULT_THRESHOLDS };
}
function saveThresholds() {
  // Home 可能沒有 threshold 輸入（用戶版拿掉），admin.html 仍會呼叫此函式
  const get = (id, fallback) => {
    const el = document.getElementById(id);
    return el ? (parseInt(el.value, 10) || fallback) : fallback;
  };
  const t = {
    maxFloors: get("thresh-floors", DEFAULT_THRESHOLDS.maxFloors),
    maxTotal: get("thresh-total", DEFAULT_THRESHOLDS.maxTotal),
    maxBld: get("thresh-bld", DEFAULT_THRESHOLDS.maxBld),
    maxLand: get("thresh-land", DEFAULT_THRESHOLDS.maxLand),
  };
  localStorage.setItem(THRESHOLD_KEY, JSON.stringify(t));
}
function restoreThresholds() {
  const t = loadThresholds();
  const set = (id, v) => { const el = document.getElementById(id); if (el) el.value = v; };
  set("thresh-floors", t.maxFloors);
  set("thresh-total", t.maxTotal);
  set("thresh-bld", t.maxBld);
  set("thresh-land", t.maxLand);
}
function toggleSection(id) {
  const el = document.getElementById(id);
  if (el) el.classList.toggle("collapsed");
}

// 行政區 chip checkbox：「全部」聯動該城市所有 chip
const MAX_DISTRICTS = 5;

function _initDistPicker() {
  const picker = document.getElementById("dist-picker");
  if (!picker) return;
  picker.addEventListener("change", e => {
    const t = e.target;
    if (t.tagName !== "INPUT") return;
    const city = t.dataset.city;
    if (t.dataset.all) {
      const siblings = picker.querySelectorAll(`input[data-city="${city}"]:not([data-all]):not(:disabled)`);
      if (t.checked) {
        // 全部勾：最多 5 個
        let count = 0;
        siblings.forEach(cb => { cb.checked = count < MAX_DISTRICTS; count++; });
        t.checked = count <= MAX_DISTRICTS;
      } else {
        siblings.forEach(cb => cb.checked = false);
      }
    } else {
      // 檢查是否超過 5 個
      const checked = picker.querySelectorAll('input[type="checkbox"]:checked:not([data-all]):not(:disabled)');
      if (checked.length > MAX_DISTRICTS) {
        t.checked = false;
        alert(`最多只能選 ${MAX_DISTRICTS} 個行政區`);
        return;
      }
      const all = picker.querySelector(`input[data-city="${city}"][data-all]`);
      const siblings = picker.querySelectorAll(`input[data-city="${city}"]:not([data-all])`);
      const allChecked = Array.from(siblings).every(cb => cb.checked);
      if (all) all.checked = allChecked;
    }
  });
  // 初始：根據個別狀態同步「全部」
  ["台北市", "新北市"].forEach(city => {
    const all = picker.querySelector(`input[data-city="${city}"][data-all]`);
    const siblings = picker.querySelectorAll(`input[data-city="${city}"]:not([data-all])`);
    if (all && siblings.length) {
      all.checked = Array.from(siblings).every(cb => cb.checked);
    }
  });
}

// ── 地圖初始化 ────────────────────────────────────────────────────────────────
function initMap() {
  map = L.map("map").setView([25.03, 121.52], 12);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "© OpenStreetMap contributors",
    maxZoom: 19,
  }).addTo(map);
  // 預設列表模式，隱藏地圖
  document.getElementById("map-panel").classList.add("view-hidden");
}

// ── 載入物件資料 ──────────────────────────────────────────────────────────────
async function loadProperties() {
  renderSkeleton(8);
  try {
    const params = buildFilterParams();
    const res = await fetch(`/api/properties?${params}&limit=500`);
    const data = await res.json();
    allProperties = data.items || [];
    // 更新觀察清單 tab 的計數
    const cnt = document.getElementById("watchlist-count");
    if (cnt) cnt.textContent = allProperties.filter(p => !p.deleted).length;
    filterAndSort();
    _resumeAnalyzingWatchers();
  } catch (e) {
    console.error("loadProperties error:", e);
  }
}

async function loadStats() {
  try {
    const res = await fetch("/api/stats");
    const s = await res.json();
    const priceStr = s.price_changed ? ` | ⚠️ 降價 ${s.price_changed}` : "";
    document.getElementById("stats-badge").textContent =
      `共 ${s.total_properties} 筆 | 強烈推薦 ${s.strong_recommend} | 值得考慮 ${s.consider}${priceStr}`;
  } catch (e) {}
}

// ── 渲染物件列表 ──────────────────────────────────────────────────────────────
function renderList(props) {
  const el = document.getElementById("property-list");
  if (!props.length) {
    let icon = "🔍", msg = "沒有符合條件的物件";
    if (_activeTab === "explore" && !_exploreSearched) {
      icon = "🔎"; msg = "請設定條件後，點擊搜尋按鈕";
    } else if (_activeTab === "watchlist") {
      icon = "📭"; msg = "你尚未加入任何物件";
    }
    el.innerHTML = `<div class="text-center text-muted py-5">
      <div class="fs-2 mb-2">${icon}</div>
      <div>${msg}</div>
    </div>`;
    return;
  }

  const header = `
    <div class="property-row property-row--head property-row--head-top">
      <div class="c c-group-left">物件資料</div>
      <div class="c c-group-right">都更評估</div>
    </div>
    <div class="property-row property-row--head">
      <div class="c c-src">來源/時間</div>
      <div class="c c-type">類型</div>
      <div class="c c-city">市</div>
      <div class="c c-district">區</div>
      <div class="c c-addr">地址</div>
      <div class="c c-val">總價</div>
      <div class="c c-val">建物<br><span class="sub">建單價</span></div>
      <div class="c c-val">土地<br><span class="sub">地單價</span></div>
      <div class="c c-val">樓層</div>
      <div class="c c-val">屋齡</div>
      <div class="c c-note">說明</div>
      <div class="c c-val c-multi">獲利倍數</div>
      <div class="c c-del">${_activeTab === "explore" ? "加觀察" : "刪除"}</div>
    </div>`;
  el.innerHTML = `<div class="property-rows">${header}${props.map(p => rowHTML(p)).join("")}</div>`;
}

const SKIP_REASON_MAP = {
  "over_max_floors": "五層以上",
  "price_too_high": "總價過高",
  "building_ping_too_high": "建單價過高",
  "land_ping_too_high": "地單價過高",
  "5F_apartment": "五層以上",
  "foreclosure": "法拍屋",
};

/**
 * 列出所有違反 threshold 的原因（不只一個）。
 */
function computeSkipReasons(p, th) {
  // 只把「五層以上」視為弱勢物件理由（都更難度高）；其餘（總價/單價/地單價）是用戶主觀篩選偏好，不標弱勢
  const reasons = [];
  if (p.total_floors && p.total_floors >= th.maxFloors) reasons.push("五層以上 👎");
  // fallback: 後端 skip_reason 是 5F_apartment 或 over_max_floors 時才標
  if (!reasons.length && (p.skip_reason === "5F_apartment" || p.skip_reason === "over_max_floors")) {
    reasons.push("五層以上 👎");
  }
  return reasons;
}

function rowHTML(p) {
  const fmt0 = n => n == null ? "—" : n.toLocaleString("zh-TW", { maximumFractionDigits: 0 });
  const fmt1 = n => n == null ? "—" : n.toLocaleString("zh-TW", { maximumFractionDigits: 1 });

  const isPending = p.analysis_status === "pending";
  const score = p.score_total;
  const scoreClass = score >= 65 ? "score-high" : score >= 45 ? "score-mid" : score ? "score-low" : "score-none";
  const scoreDisplay = score != null ? Math.round(score) : "—";
  const rec = p.ai_recommendation || "";
  const recClass = `rec-${rec}`;

  const priceStr = p.price_ntd ? `${fmt0(p.price_ntd / 10000)} 萬` : "—";
  const perBuilding = (p.price_ntd && p.building_area_ping)
    ? fmt1(p.price_ntd / 10000 / p.building_area_ping) : "—";
  const perLand = (p.price_ntd && p.land_area_ping)
    ? fmt1(p.price_ntd / 10000 / p.land_area_ping) : "—";
  let floorStr = "—";
  if (p.floor && p.total_floors) floorStr = `${p.floor}/${p.total_floors}F`;
  else if (p.total_floors) floorStr = `${p.total_floors}F`;
  else if (p.floor) floorStr = `${p.floor}F`;

  const typeIcon = {
    "公寓": "🏢", "透天厝": "🏠", "華廈": "🏬", "大樓": "🏙", "店面": "🏪",
  }[p.building_type] || "🏚";
  const typeLabel = p.building_type || "";
  const titleText = p.address || p.title || "地址未知";
  const mrtHTML = p.nearest_mrt
    ? `<div class="mrt-name">${esc(p.nearest_mrt)}</div><div class="mrt-dist">${Math.round(p.nearest_mrt_dist_m || 0)}m</div>`
    : "—";
  // 變動 badge：根據 latest_event 顯示對應 badge（7 天內才顯示，避免歷史污染）
  // 樣式跟 NEW badge 一致（小色塊圓角白字），位置會放到 NEW 旁
  const ev = p.latest_event;
  let evBadge = "";
  if (ev && ev.at) {
    const ageMs = Date.now() - new Date(ev.at).getTime();
    const within7Days = ageMs < 7 * 24 * 3600 * 1000 && ageMs >= 0;
    if (within7Days) {
      if (ev.type === "price_change") {
        if (ev.direction === "up") {
          evBadge = `<span class="event-badge b-up" title="從 ${Math.round((ev.from||0)/10000)}萬 漲到 ${Math.round((ev.to||0)/10000)}萬">漲價</span>`;
        } else if (ev.direction === "down") {
          evBadge = `<span class="event-badge b-down" title="從 ${Math.round((ev.from||0)/10000)}萬 降到 ${Math.round((ev.to||0)/10000)}萬">降價</span>`;
        } else {
          evBadge = `<span class="event-badge b-down">改價</span>`;
        }
      } else if (ev.type === "cross_source") {
        evBadge = `<span class="event-badge b-cross">${esc(ev.source || "")}新上架</span>`;
      }
      // type=new 不打 badge（既有 NEW badge 處理）
    }
  }
  // 舊邏輯 fallback：如果沒 latest_event 但有 is_price_changed，仍顯示降價
  if (!evBadge && p.is_price_changed) {
    evBadge = `<span class="event-badge b-down">降價</span>`;
  }
  const archivedBadge = p.archived ? `<span class="event-badge b-archived" title="此物件已從中央資料庫封存（admin 清理過）">已封存</span>` : "";

  // 地址：優先顯示推測地址，fallback 到 591 原始地址
  const rawAddr = p.address_inferred || p.address || p.title || "";
  const roadOnly = toHalfWidth(
    rawAddr.replace(/^(台北市|臺北市|新北市)/, "")
      .replace(/^[\u4e00-\u9fa5]{1,3}區/, "")
  ) || "—";

  // 危老 / 都更 兩個倍數（已分析才算）
  const mults = (p.analysis_status === "done") ? computeRowMultiples(p) : { w: null, d: null };
  const multiCell = (mults.w != null || mults.d != null)
    ? `<div class="multi-stack">
         <div><i>危老</i>${mults.w != null ? mults.w.toFixed(2) + "倍" : "—"}</div>
         <div><i>都更</i>${mults.d != null ? mults.d.toFixed(2) + "倍" : "—"}</div>
       </div>`
    : "—";

  // 不再高亮資料欄；違反原因都集中顯示在「說明」欄
  const th = loadThresholds();
  const hotCls = () => "";

  // NEW badge：用 scrape_session_at 為準（batch 進場時間），24 小時內標 NEW。
  // admin reanalyze 因為 preserve 了 scrape_session_at，不會讓舊物件誤亮。
  const isNew = p.scrape_session_at &&
    (Date.now() - new Date(p.scrape_session_at).getTime() < 24 * 3600 * 1000);
  const newBadge = isNew ? `<span class="badge-new">NEW</span>` : '';
  const scoreCell = isPending
    ? `<button class="btn-analyze" onclick="event.stopPropagation();triggerAnalyze('${p.id}')"><span class="analyze-line1">判斷為不值得分析</span><span class="analyze-line2">可點此開始分析</span></button>`
    : `<span class="analysis-done">完成</span>`;
  // 說明 cell：列出違反 threshold 的原因（不論 analysis_status，server 端不再過濾）
  const skipReasons = computeSkipReasons(p, th);
  const skipReasonsHTML = skipReasons.map(r => `<div class="note-skip">${esc(r)}</div>`).join("");
  const noteCell = skipReasonsHTML || (isPending ? `<span class="text-muted">—</span>` : ``);
  const deprioritized = (_activeTab === "explore" && skipReasons.length > 0) ? "is-deprioritized" : "";
  // 獲利倍數 > 3.2 → 高亮整列（紅框 + 字體放大）
  const _maxMult = Math.max(mults.w ?? 0, mults.d ?? 0);
  const highValue = _maxMult > 3.2 ? "is-high-value" : "";
  // 搜尋 tab 非弱勢物件（沒 skip reasons）→ 紅底推薦色（高價值物件同時拿到紅框加碼）
  const recommended = (_activeTab === "explore" && skipReasons.length === 0) ? "is-recommended" : "";

  const analyzing = !!p.analysis_in_progress;
  return `
  <div class="property-row ${isPending ? "is-pending" : ""} ${analyzing ? "is-analyzing" : ""} ${deprioritized} ${highValue} ${recommended}"
       id="card-${p.id}"
       ${(isPending || analyzing) ? "" : `onclick="selectProperty('${p.id}')"`}
       style="${(isPending || analyzing) ? "cursor:default" : ""}"
  >
    ${analyzing ? '<div class="row-loading"><div class="row-loading-bar"></div><div class="row-loading-text">分析中…請稍候</div></div>' : ''}
    <div class="c c-src" data-label="來源">${srcLinksHTML(p)}</div>
    <div class="c c-type" data-label="類型">${typeIcon} ${esc(typeLabel)}</div>
    <div class="c c-city" data-label="縣市">${esc(p.city || "—")}</div>
    <div class="c c-district" data-label="區">${esc(p.district || "—")}</div>
    <div class="c c-addr" title="${esc(roadOnly)}${p.title ? '\n' + esc(p.title) : ''}" data-label="地址"><div class="addr-line1"><span class="addr-text">${esc(roadOnly)}${p.address_inferred ? '<span class="inferred-tag">推測</span>' : ''}${p.is_foreclosure ? '<span class="fc-badge" title="法拍屋">法拍屋</span>' : ''}${p.is_remote_area ? `<span class="fc-badge" style="background:#9aa0a6" title="依政府河道/天險範圍判定為偏遠位置">偏遠路段</span>` : ''}${p.unsuitable_for_renewal ? `<span class="fc-badge" style="background:#9aa0a6" title="${esc(p.unsuitable_reason || '土地分區非住、商、工用地')}">特殊土地分區</span>` : ''}<a href="https://www.google.com/maps/search/${encodeURIComponent(fullAddress(p))}" target="_blank" rel="noopener noreferrer" class="map-link" onclick="event.stopPropagation()" title="Google Maps">📍</a></span>${(evBadge || archivedBadge || newBadge) ? `<span class="addr-badges">${evBadge}${archivedBadge}${newBadge}</span>` : ''}</div>${p.title ? `<div class="addr-title">${esc(p.title)}</div>` : ''}</div>
    <div class="c c-val c-total ${hotCls('price')}" data-label="總價">${priceStr}${(p.lvr_records && p.lvr_records.length) ? `<span class="lvr-icon" onclick="event.stopPropagation()" onmouseenter="showLvrPopup(event, '${p.id}')" onmouseleave="hideLvrPopup()">實</span>` : ""}</div>
    <div class="c c-val c-bld-combo" data-label="建坪/單價">
      <div class="${hotCls('bldA')}">${p.building_area_ping ?? "—"} 坪</div>
      <div class="sub ${hotCls('bld')}">${perBuilding} 萬</div>
    </div>
    <div class="c c-val c-bld-combo" data-label="地坪/單價">
      <div class="${hotCls('landA')}">${p.land_area_ping ?? "—"} 坪${p.land_area_source === "lvr" ? " <span class='lvr-tag'>*實登</span>" : ""}${p.land_area_lvr && p.land_area_lvr !== p.land_area_ping ? ` <span class='lvr-tag' title='實價登錄: ${p.land_area_lvr}坪'>*</span>` : ""}</div>
      <div class="sub ${hotCls('land_p')}">${perLand} 萬</div>
    </div>
    <div class="c c-val c-floor" data-label="樓層">${floorStr}</div>
    <div class="c c-val c-age" data-label="屋齡">${currentAge(p) ?? "—"}</div>
    <div class="c c-note" data-label="說明">${noteCell}</div>
    <div class="c c-val c-multi" data-label="獲利倍數">${multiCell}</div>
    <div class="c c-del">${_rowActionHTML(p)}</div>
  </div>
  `;
}

function _rowActionHTML(p) {
  // 依目前 tab 決定右側按鈕：
  //   explore：☆/★ 加入 / 移出觀察清單
  //   watchlist：✕ 從觀察清單移除（manual 物件直接 hide）
  if (_activeTab === "explore") {
    const on = !!p._in_watchlist;
    return `<button class="btn-bookmark ${on ? 'bookmarked' : ''}" title="${on ? '從觀察清單移除' : '加入觀察清單'}"
       onclick="event.stopPropagation();toggleWatchlist('${p.id}', this)">${on ? '★' : '☆'}</button>`;
  }
  // watchlist
  return `<button class="btn-del-row" title="從觀察清單移除"
       onclick="event.stopPropagation();deleteRow('${p.id}')">✕</button>`;
}

async function deleteRow(id) {
  const p = (allProperties || []).find(x => x.id === id);
  const label = p ? `${p.address_inferred || p.address || p.title || id}` : id;
  if (!confirm(`確定從觀察清單移除？\n${label}`)) return;
  try {
    if (String(id).startsWith("manual_")) {
      // manual 是私人資料，用 hide 軟刪除
      await fetch(`/api/properties/${encodeURIComponent(id)}/hide`, { method: "POST" });
      if (p) p.deleted = true;
    } else {
      // 591 物件：從 watchlist 移除（連同個人 overrides 一併刪除）
      await fetch(`/api/watchlist/${encodeURIComponent(id)}`, { method: "DELETE" });
      allProperties = allProperties.filter(x => x.id !== id);
      // 同步：探索 tab 暫存結果裡的同 id 也去星（下次切回探索 tab 看就是 ☆）
      const ex = (_exploreResults || []).find(x => x.id === id);
      if (ex) ex._in_watchlist = false;
    }
    filterAndSort();
  } catch (e) {
    alert("失敗：" + e.message);
  }
}

function lvrBlock(p) {
  const recs = p.lvr_records;
  if (!recs || !recs.length) return "";
  return `
  <div class="lvr-strip" onclick="event.stopPropagation()">
    <details>
      <summary class="lvr-toggle">實價登錄 ${recs.length} 筆</summary>
      <div class="lvr-table-wrap">
        <table class="lvr-table">
          <tr><th>地址</th><th>建坪</th><th>地坪</th><th>成交價</th><th>交易日</th><th></th></tr>
          ${recs.map(r => `
          <tr${r.is_special ? ' class="lvr-special"' : ''}>
            <td>${esc(r.address || "—")}</td>
            <td>${r.area_ping ?? "—"}</td>
            <td>${r.land_ping ?? "—"}</td>
            <td>${r.price_total ? (r.price_total / 10000).toLocaleString("zh-TW", {maximumFractionDigits:0}) + " 萬" : "—"}</td>
            <td>${esc(r.txn_date || "—")}</td>
            <td>${r.is_special ? `<span class="lvr-warn" title="${esc(r.note || '')}">⚠</span>` : ""}</td>
          </tr>`).join("")}
        </table>
      </div>
    </details>
  </div>`;
}

function renderSkeleton(count = 6) {
  const el = document.getElementById("property-list");
  const skeletons = Array(count).fill('<div class="skeleton-card"></div>').join("");
  el.innerHTML = `<div class="property-grid">${skeletons}</div>`;
}

function cardHTML(p) {
  const score = p.score_total;
  const scoreClass = score >= 65 ? "score-high" : score >= 45 ? "score-mid" : score ? "score-low" : "score-none";
  const scoreDisplay = score != null ? Math.round(score) : "—";

  const rec = p.ai_recommendation || "";
  const recClass = `rec-${rec}`;

  const priceStr = p.price_ntd
    ? `${(p.price_ntd / 10000).toLocaleString("zh-TW", { maximumFractionDigits: 0 })} 萬`
    : "—";

  const buildingArea = p.building_area_ping;
  const landArea = p.land_area_ping;
  const fmt1 = n => n.toLocaleString("zh-TW", { maximumFractionDigits: 1 });
  const perBuildingPing = (p.price_ntd && buildingArea)
    ? `${fmt1(p.price_ntd / 10000 / buildingArea)} 萬/建坪` : null;
  const perLandPing = (p.price_ntd && landArea)
    ? `${fmt1(p.price_ntd / 10000 / landArea)} 萬/地坪` : null;
  const ageYears = currentAge(p);
  const mrtStr = p.nearest_mrt
    ? `🚇 ${esc(p.nearest_mrt)} ${Math.round(p.nearest_mrt_dist_m || 0)}m`
    : "";

  // 樓層：2/5F 這樣寫
  let floorStr = "";
  if (p.floor && p.total_floors) {
    floorStr = `${p.floor}/${p.total_floors}F`;
  } else if (p.total_floors) {
    floorStr = `${p.total_floors}F`;
  } else if (p.floor) {
    floorStr = `${p.floor}F`;
  }

  const renewalStr = p.renewal_profit_ntd != null
    ? `<span class="${p.renewal_profit_ntd >= 0 ? "text-success" : "text-danger"}">
        都更效益 ${formatWan(p.renewal_profit_ntd)}
       </span>`
    : "";

  const priceChangedBadge = p.is_price_changed
    ? `<span class="event-badge b-down">降價</span>`
    : "";

  const imgStr = p.image_url
    ? `<img class="property-thumb" src="${esc(p.image_url)}" alt="${esc(p.address || p.title || '')}" loading="lazy" onerror="this.style.display='none'">`
    : "";

  // 類型 icon（公寓 / 透天）
  const typeIcon = {
    "公寓": "🏢",
    "透天厝": "🏠",
    "華廈": "🏬",
    "大樓": "🏙",
    "店面": "🏪",
  }[p.building_type] || "🏚";
  const typeLabel = p.building_type || "";
  const titleText = p.address || p.title || "地址未知";

  return `
  <div class="property-card ${selectedId === p.id ? "selected" : ""}"
       id="card-${p.id}"
       onclick="selectProperty('${p.id}')">
    ${imgStr}
    <div class="card-head">
      <div class="score-badge ${scoreClass}">
        <span class="score-num">${scoreDisplay}</span><span class="score-unit">分</span>
      </div>
      <div class="card-head-main">
        <div class="card-type-row">
          <span class="type-chip">${typeIcon} ${esc(typeLabel)}</span>
          <span class="card-title" title="${esc(titleText)}">${esc(titleText)}</span>
          ${priceChangedBadge}
          <span class="card-price">${priceStr}</span>
        </div>
        <div class="card-badges">
          ${buildingArea ? `<span class="badge" data-label="建">${buildingArea}</span>` : ""}
          ${landArea ? `<span class="badge" data-label="土">${landArea}</span>` : ""}
          ${ageYears ? `<span class="badge">${ageYears} 年</span>` : ""}
          ${floorStr ? `<span class="badge">${floorStr}</span>` : ""}
        </div>
        ${(perBuildingPing || perLandPing) ? `
        <div class="card-perping">
          ${perBuildingPing ? `<span>${perBuildingPing}</span>` : ""}
          ${perLandPing ? `<span>${perLandPing}</span>` : ""}
        </div>` : ""}
        <div class="card-foot">
          <span class="small ${recClass}">${rec || "未分析"}</span>
          <span class="text-muted-sm">${mrtStr}</span>
        </div>
      </div>
    </div>
    ${renewalStr ? `<div class="small mt-1">${renewalStr}</div>` : ""}
  </div>`;
}

// ── 地圖標記 ──────────────────────────────────────────────────────────────────
function renderMapMarkers(props) {
  // 清除舊標記
  Object.values(markers).forEach(m => m.remove());
  markers = {};

  props.forEach(p => {
    if (!p.latitude || !p.longitude) return;
    const score = p.score_total;
    const color = score >= 65 ? "#4ade80" : score >= 45 ? "#fbbf24" : "#f87171";

    const icon = L.divIcon({
      html: `<div style="
        background:${color};color:#000;border-radius:50%;
        width:28px;height:28px;display:flex;align-items:center;
        justify-content:center;font-size:11px;font-weight:700;
        border:2px solid rgba(0,0,0,0.3);box-shadow:0 2px 4px rgba(0,0,0,0.5);">
        ${score != null ? Math.round(score) : "?"}
      </div>`,
      className: "",
      iconSize: [28, 28],
      iconAnchor: [14, 14],
    });

    const marker = L.marker([p.latitude, p.longitude], { icon })
      .addTo(map)
      .on("click", () => selectProperty(p.id));

    marker.bindTooltip(
      `${p.district} ${p.building_type}<br>
       ${p.price_ntd ? formatWan(p.price_ntd) : "—"} | 屋齡${currentAge(p) || "?"}年`,
      { direction: "top" }
    );

    markers[p.id] = marker;
  });
}

// ── 選取物件 ──────────────────────────────────────────────────────────────────
async function selectProperty(id) {
  // 更新選取狀態
  document.querySelectorAll(".property-card").forEach(c => c.classList.remove("selected"));
  const card = document.getElementById(`card-${id}`);
  if (card) {
    card.classList.add("selected");
    card.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }
  selectedId = id;

  // 地圖定位
  const p = allProperties.find(x => x.id === id);
  if (p?.latitude && map) {
    map.setView([p.latitude, p.longitude], 16);
  }

  // 載入並顯示詳情
  try {
    const res = await fetch(`/api/properties/${id}`);
    const prop = await res.json();
    showDetailModal(prop);
  } catch (e) {
    console.error("loadDetail error:", e);
  }
}

// ── 詳情 Modal ────────────────────────────────────────────────────────────────
function showDetailModal(p) {
  _detailP = { ...p };
  const titleText = stripCityDist(p.address_inferred || p.address || p.title);
  const titleEl = document.getElementById("modal-title");
  // 清掉舊 HTML，用 textContent 安全設標題，再補法拍屋 badge（若有）
  titleEl.innerHTML = "";
  titleEl.appendChild(document.createTextNode(titleText));
  if (p.is_foreclosure) {
    const fc = document.createElement("span");
    fc.className = "fc-badge";
    fc.style.marginLeft = "8px";
    fc.textContent = "法拍屋";
    titleEl.appendChild(fc);
  }
  if (p.is_remote_area) {
    const r = document.createElement("span");
    r.className = "fc-badge";
    r.style.marginLeft = "8px";
    r.style.background = "#9aa0a6";
    r.textContent = "偏遠路段";
    r.title = "依政府河道/天險範圍判定為偏遠位置";
    titleEl.appendChild(r);
  }
  if (p.unsuitable_for_renewal) {
    const u = document.createElement("span");
    u.className = "fc-badge";
    u.style.marginLeft = "8px";
    u.style.background = "#9aa0a6";
    u.textContent = "特殊土地分區";
    u.title = p.unsuitable_reason || "土地分區非住、商、工用地";
    titleEl.appendChild(u);
  }
  // 多來源連結區：讀 p.sources（新 schema）為主，fallback 用 p.url + p.url_alt（舊 schema）
  // 不同來源 (591 / 永慶 / 信義) 各自一顆按鈕並列
  const wrap = document.getElementById("modal-591-wrap");
  if (wrap) {
    const buildSourceBtn = (name, url, date) => {
      const cleanUrl = (name === "591") ? clean591Url(url) : url;
      const dStr = date ? _fmtPubDate(date) : "";
      return `<a href="${esc(cleanUrl)}" target="_blank" rel="noopener noreferrer" class="tb-btn tb-btn--ghost" style="margin-right:6px">${esc(name)} 頁面 ↗${dStr ? ` <span class="src-pubdate">${esc(dStr)}</span>` : ""}</a>`;
    };
    let html = "";
    // sources[] 是唯一真相；alive=false 的也顯示但灰底（用戶可知歷史來源）
    if (Array.isArray(p.sources) && p.sources.length > 0) {
      // 排序：alive 先，dead 後；同類別按 added_at（早→晚）
      const sorted = [...p.sources].sort((a, b) => {
        const aliveA = a.alive !== false ? 1 : 0;
        const aliveB = b.alive !== false ? 1 : 0;
        if (aliveA !== aliveB) return aliveB - aliveA;
        return (a.added_at || "").localeCompare(b.added_at || "");
      });
      html = sorted.map(s => buildSourceBtn(s.name || "?", s.url, s.added_at)).join("");
    }
    wrap.innerHTML = html;
  }

  // manual 物件顯示「重新分析」按鈕（591 物件不顯示，因為它們有 admin 後台管理）
  const manualReanalyzeBtn = document.getElementById("modal-manual-reanalyze");
  if (manualReanalyzeBtn) {
    const isManual = String(p.id || "").startsWith("manual_");
    manualReanalyzeBtn.style.display = isManual ? "" : "none";
    manualReanalyzeBtn.disabled = !!p.analysis_in_progress;
    manualReanalyzeBtn.textContent = p.analysis_in_progress ? "⏳ 分析中…" : "🔄 重新分析";
  }
  document.getElementById("modal-body").innerHTML = buildDetailHTML(p);
  detailModal.show();
}

function buildDetailHTML(p) {
  const fmt0 = n => n.toLocaleString("zh-TW", { maximumFractionDigits: 0 });
  const fmt1 = n => n.toLocaleString("zh-TW", { maximumFractionDigits: 1 });

  const priceStr = p.price_ntd ? `NT$ ${fmt0(p.price_ntd / 10000)} 萬` : "—";
  const perPingStr = p.price_per_ping ? `（每坪 ${(p.price_per_ping / 10000).toFixed(1)} 萬）` : "";
  const perBuilding = (p.price_ntd && p.building_area_ping)
    ? `${fmt1(p.price_ntd / 10000 / p.building_area_ping)} 萬 / 建坪` : null;
  const perLand = (p.price_ntd && p.land_area_ping)
    ? `${fmt1(p.price_ntd / 10000 / p.land_area_ping)} 萬 / 地坪` : null;

  let floorStr = "—";
  if (p.floor && p.total_floors) floorStr = `${p.floor}/${p.total_floors}F`;
  else if (p.total_floors) floorStr = `${p.total_floors}F`;
  else if (p.floor) floorStr = `${p.floor}F`;

  const recColor = {
    "強烈推薦": "success", "值得考慮": "warning",
    "一般": "secondary", "不建議": "danger"
  }[p.ai_recommendation] || "secondary";

  const imgBlock = p.image_url
    ? `<img src="${p.image_url}" class="modal-photo" alt="" onerror="this.style.display='none'">`
    : `<div class="modal-photo modal-photo--empty">無照片</div>`;

  return `
  <!-- ─ Row 1：左 基本資訊 / 右 照片 ─ -->
  <div class="row g-3 mb-3">
    <div class="col-md-7">
      <h6 class="modal-h">物件資訊</h6>
      <div class="basic-info-grid">
        <div class="basic-info-col">
          <table class="table table-sm renewal-table mb-0">
            <tbody>
              <tr><td>原始地址</td><td>${esc(stripCityDist(p.address || p.title))}${p.address_road_fixed ? `<div class="addr-fixed-note">已自動修正：${esc(p.address_road_fixed.from)} → ${esc(p.address_road_fixed.to)}</div>` : ""}${p.address_suspicious ? `<div class="addr-suspicious">⚠ 路名可能不存在於此行政區，請自行確認</div>` : ""}</td></tr>
              <tr><td>推測地址 ${p.address_inferred ? `<span class="inferred-tag">${p.address_inferred_confidence === "unique" ? "★實登" : p.address_inferred_confidence === "multi" ? "推測" : "≈推測"}</span>` : ""}</td><td>${inferredAddressCellHTML(p)}</td></tr>
              <tr><td>類型 / 樓層</td><td>${esc(p.building_type || "—")} ・ ${floorStr}</td></tr>
              <tr><td>屋齡</td><td>${currentAge(p) != null ? currentAge(p) + " 年" + (p.building_age_completed_year ? ` <span style="color:#888;font-size:11px">（${p.building_age_completed_year} 年完工）</span>` : "") : "未知"}</td></tr>
              <tr><td>售價</td><td class="fw-bold text-warning">${priceStr}${(p.lvr_records && p.lvr_records.length) ? ` <span class="lvr-icon" onclick="event.stopPropagation()" onmouseenter="showLvrPopup(event, '${p.id}')" onmouseleave="hideLvrPopup()">實</span>` : ""}</td></tr>
              <tr><td>欲出價</td>
                <td><input type="number" class="inline-edit" min="0" step="10"
                           value="${desiredPriceWan(p)}"
                           onchange="saveDesiredPrice('${p.id}', this.value)"> 萬
                </td>
              </tr>
              <tr><td>建坪</td><td>${p.building_area_ping ? p.building_area_ping + " 坪" : "—"}${perBuilding ? ` <span class="text-muted-sm">(${perBuilding})</span>` : ""}</td></tr>
              <tr><td>地坪</td><td><span class="land-ping-val">${p.land_area_ping ? p.land_area_ping + " 坪" : "—"}</span>${perLand ? ` <span class="text-muted-sm land-per">(${perLand})</span>` : ""}${p.land_area_source === "lvr" ? ` <span class="text-muted-sm" style="color:#888;">(實登)</span>` : ""}${p.land_area_inconsistent ? `<div class="land-warn">⚠ 此物件的實登候選地坪差異大，可能不是同一棟建築；選擇後請務必驗證。</div>` : ""}</td></tr>
            </tbody>
          </table>
        </div>
        <div class="basic-info-col">
          <table class="table table-sm renewal-table mb-0">
            <tbody>
              <tr><td>附近捷運站</td><td>${
                Array.isArray(p.nearby_mrts) && p.nearby_mrts.length
                  ? p.nearby_mrts.map(m => `${esc(m.name)}（${Math.round(m.dist_m)}m）`).join("<br>")
                  : "—"
              }</td></tr>
              <tr><td>使用分區</td><td>${zoningCellHTML(p)}</td></tr>
              <tr><td>臨路寬度</td><td>${roadWidthCellHTML(p)}</td></tr>
            </tbody>
          </table>
          ${p.city === "台北市" ? `
          <div class="modal-tools">
            <span class="modal-tools__city">台北市</span>
            <a href="https://bim.udd.gov.taipei/UDDPlanMap/" target="_blank" rel="noopener noreferrer">都市計畫圖 ↗</a>
            <a href="https://zonemap.udd.gov.taipei/ZoneMapOP/" target="_blank" rel="noopener noreferrer">地籍套繪圖 ↗</a>
          </div>` : p.city === "新北市" ? `
          <div class="modal-tools">
            <span class="modal-tools__city">新北市</span>
            <a href="https://urban.planning.ntpc.gov.tw/NtpcURInfo/" target="_blank" rel="noopener noreferrer">城鄉資訊 ↗</a>
          </div>` : ""}
        </div>
      </div>
    </div>
    <div class="col-md-5">
      ${imgBlock}
    </div>
  </div>

  <!-- ─ Row 2：左 試算 / 右 分析建議 ─ -->
  <div class="row g-3 mb-3">
    <div class="col-md-7">
      <h6 class="modal-h">都更換回試算</h6>
      ${renewalV2HTML(p)}
    </div>
    <div class="col-md-5">
      <h6 class="modal-h">分析建議</h6>
      ${p.ai_reason ? `<div class="ai-sections" id="ai-sections-content">${formatAiReason(p.ai_reason)}</div>` : (p.ai_analysis ? `<p class="text-info mb-2">${esc(p.ai_analysis)}</p>` : "")}
    </div>
  </div>`;
}

function scoreBreakdownHTML(p) {
  const items = [
    { label: "屋齡",     val: p.score_age,           max: 25 },
    { label: "容積潛力", val: p.score_far,            max: 25 },
    { label: "基地面積", val: p.score_land,           max: 20 },
    { label: "捷運距離", val: p.score_tod,            max: 15 },
    { label: "臨路寬度", val: p.score_road,           max: 10 },
    { label: "整合潛力", val: p.score_consolidation,  max: 5  },
  ];

  const totalColor = p.score_total >= 65 ? "#4ade80" : p.score_total >= 45 ? "#fbbf24" : "#f87171";

  const bars = items.map(item => {
    const pct = item.val != null ? (item.val / 100) * 100 : 0;
    const barColor = pct >= 65 ? "#4ade80" : pct >= 40 ? "#fbbf24" : "#f87171";
    return `<div class="score-bar-wrap mb-1">
      <span class="score-bar-label">${item.label}</span>
      <div class="score-bar-track">
        <div class="score-bar-fill" style="width:${pct}%;background:${barColor}"></div>
      </div>
      <span class="score-bar-val">${item.val != null ? Math.round(item.val) : "—"}</span>
    </div>`;
  }).join("");

  return `
  <div class="d-flex align-items-center gap-3 mb-2">
    <div style="font-size:2rem;font-weight:700;color:${totalColor}">
      ${p.score_total != null ? Math.round(p.score_total) : "—"}
    </div>
    <div class="flex-grow-1">${bars}</div>
  </div>`;
}

function zoningCellHTML(p) {
  const z = p.zoning;
  const src = p.zoning_source;
  const cands = p.zoning_candidates || [];
  if (!z && !cands.length) return `<span class="text-muted">待查</span>`;

  const sourceLabel = {
    "arcgis_taipei": "北市都市計畫 GeoServer",
    "arcgis_newtaipei": "新北市 GeoServer",
    "not_found": "GeoServer 查無相符多邊形",
    "no_coord": "缺座標，無法查詢",
    "unsupported_city": "城市暫未支援（請手動）",
    // 舊資料相容
    "5168": "5168 實價登錄",
    "tcd_via_5168": "北市地籍套繪圖（5168）",
    "tcd_via_reverse_geo": "北市地籍套繪圖（反查）",
    "tcd_vision_failed": "舊版 OCR 失敗",
    "coord_mismatch": "座標與地址不匹配",
    "lookup_failed": "查詢失敗",
  }[src] || src || "";
  const srcLink = p.zoning_source_url
    ? `<a href="${encodeURI(p.zoning_source_url)}" target="_blank" rel="noopener noreferrer" class="ms-1 small">↗</a>`
    : "";
  const errorLine = p.zoning_error
    ? `<div class="small text-danger mt-1">${esc(p.zoning_error)}</div>`
    : "";

  const orig = p.zoning_original;
  const zoneList = p.zoning_list;
  let badge;
  if (zoneList && zoneList.length > 1) {
    const locked = !!p.zoning_ratios_locked;
    const n = zoneList.length;
    const totalLand = Number(p.land_area_ping) || 0;
    const ratios = p.zoning_ratios || zoneList.map(() => 100 / n);
    // ratio (%) → ping
    const toPing = (r) => totalLand > 0 ? (totalLand * (Number(r) || 0) / 100) : 0;
    badge = zoneList.map((zl, i) => {
      // zoning_list 可能是 string list（ArcGIS 點查回傳）或 object list（舊永慶 schema）
      const eff = (typeof zl === 'string') ? zl : (zl.original_zone || zl.zone_name);
      const far = TAIPEI_FAR_PCT[eff] || "?";
      const v = toPing(ratios[i]).toFixed(2);
      const disabled = locked ? "disabled" : "";
      return `<span class="zone-badge">${esc(eff)} (${far}%)</span>
        <input type="number" class="zone-ping-input" min="0" max="${totalLand}" step="0.01" value="${v}" ${disabled}
          onchange="setZonePing('${p.id}', ${i}, this.value)">坪`;
    }).join(" / ");
    badge += `<div class="zone-ping-error" id="zone-ping-err-${p.id}" style="display:none"></div>`;
    badge += locked
      ? `<div class="zone-ratio-note">依謄本登錄坪數鎖定（總 ${totalLand} 坪）</div>`
      : `<div class="zone-ratio-note">總土地 ${totalLand} 坪。因無法取得謄本，請依實際坪數輸入（任一改動，另一個會自動同步）</div>`;
  } else if (z) {
    badge = `<span class="zone-badge">${esc(z)}</span>`;
    // 只在「現行分區 vs 原分區」實際不同時才顯示「原：…」（避免「住宅區 原：住宅區」這種重複）
    if (orig && orig !== z) {
      badge += ` <span class="zone-orig">原：${esc(orig)}</span>`;
    }
  } else {
    badge = `<span class="text-muted">—</span>`;
  }
  // 若 zoning 含 (特)/(遷) 後綴 → 附加「實際容積採 X 計算」說明，並提示容積率逐案而定
  if (z && /\((?:特|遷|核|抄)\)/.test(z)) {
    const eff = effectiveZoning(p);
    const effFar = TAIPEI_FAR_PCT[eff];
    if (effFar != null && eff !== z) {
      badge += `<div class="zone-special-note">實際容積採「${esc(eff)}」${effFar}% 計算。此地塊有(特)/(遷)加註，真實容積請查都發局都市計畫書。</div>`;
    } else {
      badge += `<div class="zone-special-note">此地塊有(特)/(遷)加註，容積率逐案而定，請查都發局都市計畫書。</div>`;
    }
  }

  const candsBlock = cands.length
    ? `<details class="mt-1">
        <summary class="small text-muted" style="cursor:pointer">展開 ${cands.length} 個候選</summary>
        <table class="table table-sm table-dark small mb-0 mt-1">
          <tbody>
            ${cands.map(c => `
              <tr class="${c.is_most_likely ? 'fw-bold text-warning' : ''}">
                <td>${c.is_most_likely ? "★ " : ""}${esc(c.address || "")}</td>
                <td>${esc(c.zoning || "—")}</td>
                <td class="text-muted">${c.distance_m != null ? c.distance_m + " m" : "—"}</td>
              </tr>`).join("")}
          </tbody>
        </table>
      </details>`
    : "";

  return `${badge}${errorLine}${candsBlock}`;
}

function renewalHTML(p) {
  if (!p.renewal_type) {
    return `<p class="text-muted small">資料不足，無法試算</p>`;
  }
  if (p.renewal_type === "不符合") {
    return `<div class="alert alert-danger py-2 small">${p.renewal_type}</div>`;
  }

  const profitColor = (p.renewal_profit_ntd || 0) >= 0 ? "text-success" : "text-danger";
  const profitStr = p.renewal_profit_ntd != null
    ? formatWan(p.renewal_profit_ntd)
    : "—";

  const roadWidthVal = (p.road_width_m_override ?? p.road_width_m) ?? "";
  return `
  <table class="table table-sm renewal-table">
    <tbody>
      <tr><td>重建路徑</td><td><span class="renewal-type-badge">${p.renewal_type}</span></td></tr>
      <tr>
        <td>臨路寬度</td>
        <td>
          <input type="number" min="0" step="0.5" value="${roadWidthVal}" placeholder="—"
                 class="inline-edit" id="edit-road-width-${p.id}"
                 onchange="saveRoadWidth('${p.id}', this.value)">
          <span class="ms-1">m</span>
        </td>
      </tr>
      <tr><td>採用容積獎勵率</td><td>${p.renewal_bonus_rate != null ? (p.renewal_bonus_rate * 100).toFixed(0) + "%" : "—"}</td></tr>
      <tr><td>估計換回坪數</td><td class="fw-bold">${p.renewal_new_area_ping ? p.renewal_new_area_ping + " 坪" : "—"}</td></tr>
      <tr><td>換回新屋市值</td><td class="fw-bold">${p.renewal_value_ntd ? formatWan(p.renewal_value_ntd) : "—"}</td></tr>
      <tr><td>現在買入價</td><td>${p.price_ntd ? formatWan(p.price_ntd) : "—"}</td></tr>
      <tr>
        <td>都更效益</td>
        <td class="fw-bold ${profitColor} fs-6">${profitStr}</td>
      </tr>
    </tbody>
  </table>
  <div class="text-muted-sm">⚠️ 試算假設：買方擁有全部土地持分。實際換回比例依持分調整。</div>`;
}

// ── 重建試算 v2 ───────────────────────────────────────────────────────────────
const SHARE_RATIO_TABLE = [
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
  // 新北市
  "住宅區":300,"商業區":440,"商業區(板橋)":460,
};
// fallback 預設值（萬/坪）— 當 API 還沒回 / 失敗時用
// 啟動後 fetchDistrictPrices() 會從 /api/district_new_house_price 拿 LVR 預售屋中位數覆寫
const DISTRICT_NEW_HOUSE_PRICE = {
  "中正區":110,"大同區":95,"中山區":110,"松山區":130,"大安區":150,
  "萬華區":80,"信義區":145,"內湖區":110,"南港區":110,"文山區":90,
  "板橋區":75,"新莊區":65,"新店區":75,"中和區":70,"永和區":75,
};

async function fetchDistrictPrices() {
  // 從 server 拉「LVR 預售屋中位數」覆寫前端常數，讓 modal 顯示的單價跟 backend 試算一致
  try {
    const r = await authedFetch("/api/district_new_house_price");
    if (!r.ok) return;
    const data = await r.json();
    const by = data.by_district || {};
    let updated = 0;
    for (const [dist, price] of Object.entries(by)) {
      if (typeof price === "number" && price > 0) {
        DISTRICT_NEW_HOUSE_PRICE[dist] = price;
        updated++;
      }
    }
    if (updated) {
      console.log(`[district price] 套用 ${updated} 區 LVR 預售屋中位數 (${data.updated_at || "?"})`);
    }
  } catch (e) {
    console.warn("fetchDistrictPrices failed:", e);
  }
}

function lookupShareRatio(priceWan) {
  if (!priceWan) return [null, null];
  const t = SHARE_RATIO_TABLE;
  if (priceWan <= t[0][0]) return [t[0][1], t[0][2]];
  if (priceWan >= t[t.length-1][0]) return [t[t.length-1][1], t[t.length-1][2]];
  for (let i = 0; i < t.length - 1; i++) {
    const [p1,r1,c1] = t[i]; const [p2,r2,c2] = t[i+1];
    if (p1 <= priceWan && priceWan <= p2) {
      const f = (priceWan - p1) / (p2 - p1);
      return [r1 + (r2-r1)*f, c1 + (c2-c1)*f];
    }
  }
  return [null, null];
}

function effectiveZoning(p) {
  // 多分區時回傳第一個（加權在 effectiveFarPctWeighted 處理）
  // 套用規則：
  //   住宅區(特)/(遷) → 忽略 original，一律用該住宅區本身（剝掉括號）
  //   商業區(特)/(遷) → original 合法優先，否則用該商業區本身
  //   無特殊後綴       → original 合法優先，否則用 zoning
  const z = p.zoning || "";
  const orig = p.zoning_original || "";
  const hasSpecial = /\((?:特|遷|核|抄)\)/.test(z);
  const base = z.replace(/\((?:特|遷|核|抄)\)/g, "").trim();
  if (hasSpecial && z.includes("商")) {
    if (orig && TAIPEI_FAR_PCT[orig] != null) return orig;
    return TAIPEI_FAR_PCT[base] != null ? base : z;
  }
  if (hasSpecial && z.includes("住")) {
    return TAIPEI_FAR_PCT[base] != null ? base : z;
  }
  if (orig && TAIPEI_FAR_PCT[orig] != null) return orig;
  return z;
}

function effectiveFarPctWeighted(p, roadWidthM) {
  const zoneList = p.zoning_list;
  if (zoneList && zoneList.length > 1) {
    // ratios 存為百分比 (0-100)；預設平均分配
    const ratiosPct = p.zoning_ratios || zoneList.map(() => 100 / zoneList.length);
    const total = ratiosPct.reduce((a, b) => a + (Number(b) || 0), 0) || 1;
    let weighted = 0;
    for (let i = 0; i < zoneList.length; i++) {
      const z = zoneList[i].original_zone || zoneList[i].zone_name;
      const far = effectiveFarPct(z, roadWidthM);
      if (far == null) return null;
      weighted += far * ((Number(ratiosPct[i]) || 0) / total);
    }
    return Math.round(weighted);
  }
  const z = effectiveZoning(p);
  return effectiveFarPct(z, roadWidthM);
}

function effectiveFarPct(zoning, roadWidthM) {
  const base = TAIPEI_FAR_PCT[zoning];
  if (base == null) return null;
  if (!roadWidthM || roadWidthM <= 0) return base;
  // 規則：路寬(m) ≥ 基準FAR×2(m)；不足則容積率上限 = 路寬 × 50 (%)
  const cap = roadWidthM * 50;
  return Math.min(base, Math.round(cap));
}

function desiredPriceWan(p) {
  if (p.desired_price_wan != null) return p.desired_price_wan;
  if (!p.price_ntd) return "";
  // 開價 9 折，四捨五入到 10 萬
  return Math.round((p.price_ntd / 10000) * 0.9 / 10) * 10;
}

async function saveDesiredPrice(id, val) {
  const v = parseFloat(val);
  if (isNaN(v) || v <= 0) return;
  if (_detailP) _detailP.desired_price_wan = v;
  if (_detailP && !_detailP._in_watchlist) _detailP._ephemeral_edit_made = true;
  _rerenderRenewal();
  _syncDetailToList();    // home row 的獲利倍數要立刻重算
  fetch(`/api/properties/${id}/desired_price`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ desired_price_wan: v }),
  }).catch(e => console.error("saveDesiredPrice", e));
}

// 計算危老/都更換回倍數（給 row 列表用，邏輯與內頁 renewalV2HTML 一致）
function computeRowMultiples(p) {
  const land = p.land_area_ping;
  const price = p.new_house_price_wan_override ?? DISTRICT_NEW_HOUSE_PRICE[p.district];
  const effFar = effectiveFarPctWeighted(p, p.road_width_m_override ?? p.road_width_m);
  if (!land || effFar == null || !price) return { w: null, d: null };
  const coeff = p.rebuild_coeff ?? 1.57;
  const [ratio, parking] = lookupShareRatio(price);
  const isFangzai = p.city === "台北市" && currentAge(p) && (new Date().getFullYear() - currentAge(p)) <= 1974;
  const bonusW = p.bonus_weishau ?? 0.30;
  const bonusD = p.bonus_dugen ?? (isFangzai ? 0.80 : 0.50);
  const is1F = Number(p.floor) === 1;
  const floorPremium = p.floor_premium ?? (is1F ? 0.20 : 0);
  const effectivePrice = price * (1 + floorPremium);
  const calcVal = b => {
    const share = land * (effFar/100) * (1+b) * coeff * (ratio||0);
    return share * effectivePrice + (share / 40) * (parking || 0);
  };
  const desired = parseFloat(desiredPriceWan(p)) || 0;
  if (!desired) return { w: null, d: null };
  return {
    w: (calcVal(bonusW) / desired),
    d: (calcVal(bonusD) / desired),
  };
}

function toHalfWidth(s) {
  if (!s) return s;
  return s.replace(/[\uff10-\uff19]/g, c => String.fromCharCode(c.charCodeAt(0) - 0xfee0))
          .replace(/\uff0d/g, '-').replace(/\uff0e/g, '.').replace(/\u3000/g, ' ');
}

function _fmtPubDate(iso) {
  if (!iso) return "";
  try {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return "";
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${y}/${m}/${dd}`;
  } catch { return ""; }
}

function srcLinksHTML(p) {
  // 用戶輸入地址生成（manual，無外部連結）→ 顯示「自行調查」
  if (String(p.id || "").startsWith("manual_")) {
    const t = _fmtPubDate(p._added_at || p.analysis_completed_at || p.scraped_at || null);
    return `<span style="display:inline-flex;flex-direction:column;align-items:center;gap:2px">
      <span class="src-badge-manual">自行調查</span>
      ${t ? `<span class="src-num-date" style="margin-left:0">${t}</span>` : ""}
    </span>`;
  }

  // 用戶貼 URL 送出（source_origin=user_url）— fall through 到下面的 icon badge 渲染，
  // 顯示對應網站 icon（591/永慶）比「自行調查」標籤更直覺。manual 物件才保留特殊處理。

  // 多來源：sources[] 是唯一真相；alive=false 不顯示（已失效歷史）
  let sourceList = [];
  if (Array.isArray(p.sources) && p.sources.length > 0) {
    sourceList = p.sources
      .filter(s => s.url && s.alive !== false)
      .map(s => ({ name: s.name || "591", url: s.url, date: s.added_at || null }));
  }
  if (!sourceList.length) return "—";

  // 同來源多連結合併（591 列表可能回同物件多筆）
  const grouped = {};
  for (const s of sourceList) {
    if (!grouped[s.name]) grouped[s.name] = [];
    grouped[s.name].push(s);
  }

  // 為每個來源產出一個 badge（並列顯示）
  // badge 是 app icon 圖檔；text-indent 把舊文字「591/永慶」隱藏，多筆用角標 ×N 顯示
  const badgeFor = (name, items) => {
    const bClass = name === "591" ? "src-badge591"
                 : name === "永慶" ? "src-badge-yongqing"
                 : name === "信義" ? "src-badge-sinyi"
                 : "src-badge-other";
    const countOverlay = items.length > 1
      ? `<span class="src-badge-count">${items.length}</span>` : "";
    const links = items.map((s, i) => {
      const d = _fmtPubDate(s.date);
      const cleanUrl = (name === "591") ? clean591Url(s.url) : s.url;
      const label = `#${i+1}`;
      return `<a href="${esc(cleanUrl)}" target="_blank" rel="noopener noreferrer" class="src-num" onclick="event.stopPropagation()" title="${esc(label)}">
        <span>${esc(label)}</span>${d ? `<span class="src-num-date">${esc(d)}</span>` : ""}
      </a>`;
    }).join("");
    return `<span class="src-hover-wrap" onclick="event.stopPropagation()">
      <span class="${bClass}" title="${esc(name)}">${esc(name)}${countOverlay}</span>
      <span class="src-hover-popup src-hover-popup--dated">${links}</span>
    </span>`;
  };
  return Object.entries(grouped).map(([name, items]) => badgeFor(name, items)).join('<span style="display:inline-block;width:6px"></span>');
}

function clean591Url(url) {
  if (!url) return "#";
  return url.replace(/\.html.*$/, ".html");
}

function stripCityDist(addr) {
  if (!addr) return "—";
  // 容錯：即使 address 仍是舊資料有前綴，也剝除 city/district（可能多次）
  return toHalfWidth(
    (addr + "").replace(/^(台北市|臺北市|新北市)+/, "")
               .replace(/^([\u4e00-\u9fa5]{1,3}區)+/, "")
  ) || "—";
}

// 拼回完整地址（給 Google Maps / geocoder 用）
function fullAddress(p) {
  const base = p.address_inferred || p.address || "";
  if (!base) return "";
  // 若 address 已含 city 前綴（舊資料），直接用
  if (/^(台北市|臺北市|新北市)/.test(base)) return base;
  return (p.city || "") + (p.district || "") + base;
}

function zoneAbbr(z) {
  if (!z) return "—";
  const m = z.match(/^第([一二三四五六])(?:之([一二三四五六]))?種(住宅|商業|工業)區$/);
  if (m) {
    const [, n, sub, kind] = m;
    const k = kind === "住宅" ? "住" : kind === "商業" ? "商" : "工";
    return sub ? `${k}${n}之${sub}` : `${k}${n}`;
  }
  return z;
}

function renewalV2HTML(p) {
  const land = p.land_area_ping;
  const zoning = effectiveZoning(p);
  const roadWidth = p.road_width_m_override ?? p.road_width_m;
  const multiZone = p.zoning_list && p.zoning_list.length > 1;
  // 多分區時顯示加權 baseFAR（未受路寬限縮）
  const baseFar = multiZone
    ? (function () {
        const n = p.zoning_list.length;
        const ratios = p.zoning_ratios || p.zoning_list.map(() => 100 / n);
        const total = ratios.reduce((a, b) => a + (Number(b) || 0), 0) || 1;
        let w = 0;
        for (let i = 0; i < n; i++) {
          const zn = p.zoning_list[i].original_zone || p.zoning_list[i].zone_name;
          const f = TAIPEI_FAR_PCT[zn];
          if (f == null) return null;
          w += f * ((Number(ratios[i]) || 0) / total);
        }
        return Math.round(w);
      })()
    : TAIPEI_FAR_PCT[zoning];
  const effFar = multiZone ? effectiveFarPctWeighted(p, roadWidth) : effectiveFarPct(zoning, roadWidth);
  const coeff = p.rebuild_coeff ?? 1.57;
  const price = p.new_house_price_wan_override ?? DISTRICT_NEW_HOUSE_PRICE[p.district] ?? null;
  const [ratio, parking] = lookupShareRatio(price);
  const isFangzai = p.city === "台北市" && currentAge(p) && (new Date().getFullYear() - currentAge(p)) <= 1974;
  const bonusW = p.bonus_weishau ?? 0.30;
  const bonusD = p.bonus_dugen ?? (isFangzai ? 0.80 : 0.50);
  // 樓層加成：1F 預設 20%，其他樓層 0%。用戶可改，0~80%，5% 為單位。
  const is1F = Number(p.floor) === 1;
  const floorPremium = p.floor_premium ?? (is1F ? 0.20 : 0);
  const id = p.id;

  if (!land || !zoning || !price) {
    return `<div class="text-muted small">⚠️ 缺資料：${[
      !land ? "土地坪數" : null,
      !zoning ? "使用分區" : null,
      !price ? "新成屋房價" : null,
    ].filter(Boolean).join(" / ")}，無法試算。</div>`;
  }

  const effectivePrice = price * (1 + floorPremium);
  const calcShare = (bonus) => land * (effFar/100) * (1 + bonus) * coeff * (ratio || 0);
  const parkingCount = (bonus) => calcShare(bonus) / 40;   // 每 40 坪分回配 1 個車位
  const parkingValue = (bonus) => parkingCount(bonus) * (parking || 0);
  const calcValue = (bonus) => calcShare(bonus) * effectivePrice + parkingValue(bonus);

  const shareW = calcShare(bonusW), valW = calcValue(bonusW);
  const shareD = calcShare(bonusD), valD = calcValue(bonusD);
  // 倍數與效益基準改用「欲出價」而非開價
  const priceWan = parseFloat(desiredPriceWan(p)) || 0;
  const multiStr = v => priceWan ? (v / priceWan).toFixed(2) + "×" : "—";
  const profitStr = v => priceWan ? `${v - priceWan >= 0 ? "+" : ""}${(v - priceWan).toFixed(0)} 萬` : "—";

  const bonusOptsW = (selected) => [0.10, 0.20, 0.30, 0.40].map(b =>
    `<option value="${b}" ${Math.abs(selected-b)<0.001 ? 'selected' : ''}>${(b*100).toFixed(0)}%</option>`
  ).join("");
  const bonusOptsD = (selected) => [0.10,0.20,0.30,0.40,0.50,0.60,0.70,0.80,0.90,1.00].map(b =>
    `<option value="${b}" ${Math.abs(selected-b)<0.001 ? 'selected' : ''}>${(b*100).toFixed(0)}%</option>`
  ).join("");

  // 直式乘法風格：左側 × 符號對齊，每列 label + (note + value 都右對齊)
  const r = (op, label, value, note = "") => `
    <div class="rv2-r">
      <span class="rv2-op">${op}</span>
      <span class="rv2-lbl">${label}</span>
      <span class="rv2-val">
        ${note ? `<span class="rv2-note">${note}</span>` : ""}
        <span>${value}</span>
      </span>
    </div>`;

  return `
  <div class="rv2">
    <div class="rv2-land">
      <div class="rv2-land__lbl">土地持分</div>
      <div class="rv2-land__val">${land} <span class="rv2-land__unit">坪</span></div>
      <div class="rv2-land__zone">
        <div class="rv2-land__abbr">${zoneAbbr(zoning)}</div>
      </div>
    </div>
    <div class="rv2-body">
    <div class="rv2-formula">
    ${r("×", "有效容積率", `${effFar}%`,
       effFar < baseFar ? `<span class="rv2-warn">⚠ 受路寬 ${roadWidth}m 限縮</span>` : "")}
    <div class="rv2-r">
      <span class="rv2-op">×</span>
      <span class="rv2-lbl">容積獎勵</span>
      <span class="rv2-val rv2-val--bonus">
        <span class="rv2-tag">危老</span>
        <select class="rv2-edit" onchange="setBonus('${id}','weishau',this.value)">
          ${bonusOptsW(bonusW)}
        </select>
        <span class="rv2-tag">都更</span>
        <select class="rv2-edit" onchange="setBonus('${id}','dugen',this.value)">
          ${bonusOptsD(bonusD)}
        </select>
      </span>
    </div>
    <div class="rv2-r">
      <span class="rv2-op">×</span>
      <span class="rv2-lbl">都更係數</span>
      <span class="rv2-val">
        <input type="number" class="rv2-edit" step="0.01" value="${coeff}"
               onchange="setRebuildCoeff('${id}', this.value)">
      </span>
    </div>
    ${r("×", "分回比例", ratio != null ? (ratio*100).toFixed(1) + "%" : "—")}
    <div class="rv2-r">
      <span class="rv2-op">×</span>
      <span class="rv2-lbl">新成屋房價<span class="rv2-lbl-unit">(萬/坪)</span></span>
      <span class="rv2-val">
        <span class="road-unknown-note">(此為區域平均單價，您可自行調整)</span>
        <input type="number" class="rv2-edit" step="5" value="${price}"
               onchange="setNewHousePrice('${id}', this.value)">
      </span>
    </div>
    <div class="rv2-r">
      <span class="rv2-op">×</span>
      <span class="rv2-lbl">樓層加成${is1F ? '<span class="rv2-lbl-unit">(1F 預設20%)</span>' : ''}</span>
      <span class="rv2-val">
        <input type="number" class="rv2-edit" min="0" max="80" step="5" value="${Math.round(floorPremium * 100)}"
               onchange="setFloorPremium('${id}', this.value)"> %
      </span>
    </div>
    <div class="rv2-r">
      <span class="rv2-op">+</span>
      <span class="rv2-lbl">分回車位</span>
      <span class="rv2-val rv2-val--bonus">
        <span class="rv2-tag">危老</span>
        <span class="rv2-parking">
          <span class="rv2-parking__val">${parkingValue(bonusW).toFixed(0)} 萬</span>
          <span class="rv2-parking__cnt">(${parkingCount(bonusW).toFixed(2)} 位)</span>
        </span>
        <span class="rv2-tag">都更</span>
        <span class="rv2-parking">
          <span class="rv2-parking__val">${parkingValue(bonusD).toFixed(0)} 萬</span>
          <span class="rv2-parking__cnt">(${parkingCount(bonusD).toFixed(2)} 位)</span>
        </span>
      </span>
    </div>

    </div><!-- /rv2-formula -->
    <div class="rv2-result">
      ${[
        { tag: "危老", val: valW, share: shareW },
        { tag: "都更", val: valD, share: shareD },
      ].map(s => {
        const v = s.val; const sh = s.share;
        const mult = priceWan ? (v / priceWan).toFixed(2) : "—";
        const profit = priceWan ? (v - priceWan).toFixed(0) : "—";
        const profitSign = priceWan && (v - priceWan) >= 0 ? "+" : "";
        return `
        <div class="rv2-rcol">
          <div class="rv2-rtag">${s.tag}</div>
          <div class="rv2-rval">${v.toFixed(0)} 萬</div>
          <div class="rv2-circles">
            <div class="rv2-circ">
              <div class="rv2-circ__num">${sh.toFixed(2)}</div>
              <div class="rv2-circ__lbl">分回坪</div>
            </div>
            <div class="rv2-circ">
              <div class="rv2-circ__num">${mult}×</div>
              <div class="rv2-circ__lbl">倍數</div>
            </div>
            <div class="rv2-circ ${priceWan && (v-priceWan)<0 ? 'rv2-circ--neg' : ''}">
              <div class="rv2-circ__num">${profitSign}${profit}</div>
              <div class="rv2-circ__lbl">效益萬</div>
            </div>
          </div>
        </div>`;
      }).join("")}
    </div>
    </div><!-- /rv2-body -->
  </div>`;
}

// 本地暫存當前內頁的物件，方便編輯時 instant re-render
let _detailP = null;

function _rerenderRenewal() {
  if (!_detailP) return;
  const target = document.querySelector(".modal-body .rv2");
  if (!target) return;
  const tmp = document.createElement("div");
  tmp.innerHTML = renewalV2HTML(_detailP);
  target.replaceWith(tmp.firstElementChild);
  // 同步更新分回價值區塊
  const bidEl = document.getElementById("ai-bid-section");
  if (bidEl) bidEl.innerHTML = renderBidSection();
}

function _syncDetailToList() {
  if (!_detailP) return;
  const idx = allProperties.findIndex(p => p.id === _detailP.id);
  if (idx >= 0) {
    Object.assign(allProperties[idx], _detailP);
    const card = document.getElementById("card-" + _detailP.id);
    if (card) {
      const tmp = document.createElement("div");
      tmp.innerHTML = rowHTML(allProperties[idx]);
      card.replaceWith(tmp.firstElementChild);
    }
  }
  // 同步到搜尋 tab 的快取，避免切 tab 回去看到舊倍數
  if (Array.isArray(_exploreResults)) {
    const eIdx = _exploreResults.findIndex(p => p.id === _detailP.id);
    if (eIdx >= 0) Object.assign(_exploreResults[eIdx], _detailP);
  }
}

// 把當前物件已知的 override 欄位挑出來（給 toggleWatchlist 推送用）
function _collectOverrides(p) {
  const keys = [
    "desired_price_wan", "floor_premium", "bonus_weishau", "bonus_dugen",
    "rebuild_coeff", "new_house_price_wan_override",
    "road_width_m_override", "zoning_ratios",
  ];
  const out = {};
  for (const k of keys) if (p && p[k] != null) out[k] = p[k];
  return out;
}

function _showReanalyzeBtn() {
  const wrap = document.getElementById("reanalyze-wrap");
  if (wrap) wrap.style.display = "block";
}

async function setFloorPremium(id, val) {
  // input 單位是 %（0-80），內部存 0-0.80
  let pct = parseFloat(val);
  if (isNaN(pct)) pct = 0;
  pct = Math.max(0, Math.min(80, pct));
  const v = pct / 100;
  if (_detailP) _detailP.floor_premium = v;
  if (_detailP && !_detailP._in_watchlist) _detailP._ephemeral_edit_made = true;
  _rerenderRenewal();
  _syncDetailToList();
  fetch(`/api/properties/${id}/floor_premium`, {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({ floor_premium: v }),
  }).catch(e => console.error("setFloorPremium", e));
}

async function setBonus(id, which, val) {
  const v = parseFloat(val);
  if (_detailP) _detailP[which === "weishau" ? "bonus_weishau" : "bonus_dugen"] = v;
  if (_detailP && !_detailP._in_watchlist) _detailP._ephemeral_edit_made = true;
  _rerenderRenewal();
  _syncDetailToList();
  fetch(`/api/properties/${id}/bonus`, {
    method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({ which, value: v }),
  }).catch(e => console.error("setBonus", e));
}
async function setRebuildCoeff(id, val) {
  const v = parseFloat(val);
  if (_detailP) _detailP.rebuild_coeff = v;
  if (_detailP && !_detailP._in_watchlist) _detailP._ephemeral_edit_made = true;
  _rerenderRenewal();
  _syncDetailToList();
  fetch(`/api/properties/${id}/rebuild_coeff`, {
    method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({ value: v }),
  }).catch(e => console.error("setRebuildCoeff", e));
}
async function setNewHousePrice(id, val) {
  const v = parseFloat(val);
  if (_detailP) _detailP.new_house_price_wan_override = v;
  if (_detailP && !_detailP._in_watchlist) _detailP._ephemeral_edit_made = true;
  _rerenderRenewal();
  _syncDetailToList();
  fetch(`/api/properties/${id}/new_house_price`, {
    method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({ new_house_price_wan_per_ping: v }),
  }).catch(e => console.error("setNewHousePrice", e));
}

function roadNameHint(p) {
  // 1) 後端 Vision 已判定過 road_width_name → 最高優先（避免下面的 allRoads.includes() 誤中巷弄）
  if (p.road_width_name) return p.road_width_name;
  // 2) 沒 Vision 結果才從 GeoServer 候選列表挑
  const allRoads = p.road_width_all || [];
  if (!allRoads.length) return "";
  const addr = p.address_inferred || p.address || "";
  const addrRoad = (addr.match(/([一-龥]+(?:路|街|大道)[一-龥]*段?(?:\d+巷)?(?:\d+弄)?)/) || [])[1] || "";
  if (addrRoad) {
    // 精準相等優先（避免「天祥路」includes 誤中「天祥路16巷」）
    const exact = allRoads.find(r => r.road_name === addrRoad);
    if (exact) return exact.road_name;
    const fuzzy = allRoads.find(r => r.road_name && r.road_name.includes(addrRoad.replace(/\d+巷$/, "").replace(/\d+弄$/, "")));
    if (fuzzy) return fuzzy.road_name;
  }
  return (allRoads[0] && allRoads[0].road_name) || "";
}

async function saveRoadWidth(propertyId, val) {
  const v = parseFloat(val);
  if (isNaN(v) || v <= 0) return;
  if (_detailP) _detailP.road_width_m_override = v;
  _rerenderRenewal();
  fetch(`/api/properties/${propertyId}/road_width`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ road_width_m: v }),
  }).catch(e => console.error("saveRoadWidth", e));
}

async function scanRoadWidth(propertyId, btn) {
  btn.disabled = true;
  btn.classList.add("scanning");
  btn.innerHTML = '<span class="scan-progress">0%</span>';
  // 模擬進度（實際約 15 秒）
  let pct = 0;
  const steps = [
    [1000, 10, "開啟地圖..."],
    [3000, 30, "載入圖層..."],
    [5000, 50, "定位座標..."],
    [8000, 70, "截圖中..."],
    [11000, 85, "AI 判讀..."],
  ];
  const progEl = btn.querySelector(".scan-progress");
  const timers = steps.map(([ms, p, label]) => setTimeout(() => {
    if (progEl) progEl.textContent = `${p}%`;
  }, ms));
  try {
    const res = await fetch(`/api/properties/${propertyId}/scan_road_width`, { method: "POST" });
    const data = await res.json();
    timers.forEach(clearTimeout);
    if (data.road_width_m != null) {
      if (_detailP) {
        _detailP.road_width_m = data.road_width_m;
        _detailP.road_width_name = data.road_name || _detailP.road_width_name;
        _detailP.screenshot_roadwidth = data.screenshot || _detailP.screenshot_roadwidth;
        _detailP.road_width_vision_reason = data.reason || "";
      }
      btn.classList.remove("scanning");
      btn.textContent = `${data.road_name || ""} ${data.road_width_m}m`;
      btn.classList.add("scan-done");
      const input = btn.closest("td").querySelector("input");
      if (input) input.value = data.road_width_m;
      // 路名提示更新
      const hint = btn.closest("td").querySelector(".road-name-hint");
      if (hint) hint.textContent = data.road_name || "";
      // 按鈕變成「查看地圖」預覽（有截圖）或永久禁用（無截圖）
      btn.onclick = null;
      btn.removeAttribute("onclick");
      if (data.screenshot) {
        btn.textContent = "地籍圖";
        btn.classList.remove("scan-done");
        btn.classList.add("btn-show-map");
        btn.disabled = false;
        btn.addEventListener("click", (e) => {
          e.stopPropagation();
          const existing = btn.closest("td").querySelector(".road-preview-img");
          if (existing) { existing.remove(); return; }
          const el = document.createElement("div");
          el.className = "road-preview-img road-preview-inline";
          const imgEl = document.createElement("img");
          imgEl.src = data.screenshot;
          imgEl.addEventListener("click", () => openRoadOverlay(data.screenshot, data.reason || ""));
          const reasonEl = document.createElement("div");
          reasonEl.className = "road-reason";
          reasonEl.textContent = data.reason || "";
          el.appendChild(imgEl);
          el.appendChild(reasonEl);
          btn.closest("td").appendChild(el);
        });
      } else {
        btn.textContent = "已掃描";
        btn.disabled = true;
      }
      _rerenderRenewal();
    } else {
      btn.classList.remove("scanning");
      btn.textContent = data.error || "掃描失敗";
      btn.classList.add("scan-fail");
    }
  } catch (e) {
    timers.forEach(clearTimeout);
    btn.classList.remove("scanning");
    btn.textContent = "掃描失敗";
    btn.classList.add("scan-fail");
    btn.disabled = true;
    btn.onclick = null;
    btn.removeAttribute("onclick");
  }
}

async function reanalyzeManualFull(overrideBody = null) {
  if (!_detailP) return;
  const id = _detailP.id;
  if (!String(id).startsWith("manual_")) {
    alert("只能重分析 manual 物件");
    return;
  }
  const btn = document.getElementById("modal-manual-reanalyze");
  if (btn) { btn.disabled = true; btn.textContent = "⏳ 分析中…"; }
  detailModal.hide();
  _startFakeProgress(`重新分析中：${stripCityDist(_detailP.address_inferred || _detailP.address || id)}`);
  try {
    const fetchOpts = { method: "POST" };
    if (overrideBody) {
      fetchOpts.headers = { "Content-Type": "application/json" };
      fetchOpts.body = JSON.stringify(overrideBody);
    }
    const res = await fetch(`/api/manual/${encodeURIComponent(id)}/reanalyze`, fetchOpts);
    const data = await res.json();
    // 跟新建一樣，若 validate 沒過 → 走 dialog 流程（ambiguous_unit / lvr_mismatch / district_mismatch / not_found）
    if (data.status !== "started") {
      _stopFakeProgress("送出完成");
      _handleManualResponse(data);
      // 把 detail panel 重開（reanalyze button 還鎖著），讓用戶看 dialog 取消的話能再點重分析
      if (btn) { btn.disabled = false; btn.textContent = "🔄 重新分析"; }
      return;
    }
    await _waitForManualAnalysisDone(id);
    _stopFakeProgress("分析完成");
    const r = await fetch(`/api/properties/${encodeURIComponent(id)}`);
    if (r.ok) {
      const fresh = await r.json();
      const idx = allProperties.findIndex(p => p.id === id);
      if (idx >= 0) Object.assign(allProperties[idx], fresh);
      filterAndSort();
      showDetailModal(fresh);
    }
  } catch (e) {
    _stopFakeProgress("失敗");
    alert("重分析失敗：" + e.message);
  }
}

async function reanalyzeRecommendation(propertyId, btn) {
  btn.disabled = true;
  btn.classList.add("reanalyzing");
  btn.innerHTML = '<span class="scan-progress">分析中...</span>';
  try {
    const res = await fetch(`/api/properties/${propertyId}/reanalyze`, { method: "POST" });
    const data = await res.json();
    if (data.ai_reason) {
      if (_detailP) {
        _detailP.ai_reason = data.ai_reason;
        _detailP.ai_recommendation = data.ai_recommendation;
      }
      const content = document.getElementById("ai-sections-content");
      if (content) content.innerHTML = formatAiReason(data.ai_reason);
      btn.closest("#reanalyze-wrap").style.display = "none";
    } else {
      btn.textContent = data.error || "分析失敗";
      btn.classList.add("scan-fail");
    }
  } catch (e) {
    btn.textContent = "分析失敗";
    btn.classList.add("scan-fail");
  }
}

function formatAiReason(text) {
  if (!text) return "";
  return text.split(/\n\n+/).map(section => {
    const m = section.match(/^【(.+?)】\s*([\s\S]*)/);
    if (m) {
      const title = m[1];
      // 分回價值：動態渲染，從 _detailP 的輸入欄位（land_area_ping/zoning/road_width_m/...）即時計算
      if (title === "分回價值") {
        return `<div class="ai-section"><div class="ai-section-title">${esc(title)}</div><div class="ai-section-body" id="ai-bid-section">${renderBidSection()}</div></div>`;
      }
      // 先對原始文字做 HTML escape，再把預期的 marker tag (&lt;chk-y&gt; 之類) 還原成真正的 HTML。
      // 這樣即使 AI 輸出被污染也只會顯示成純文字，不會執行腳本。
      let body = esc(m[2].trim());
      body = body.replace(/(\d+\.\d+)×/g, '$1倍');
      body = body.replace(/(\d+)×/g, '$1倍');
      body = body.replace(/&lt;chk-y&gt;([\s\S]*?)&lt;\/chk-y&gt;/g, '<span class="chk-yes">☑</span><span class="chk-yes-text">$1</span>');
      body = body.replace(/&lt;chk-n&gt;([\s\S]*?)&lt;\/chk-n&gt;/g, '<span class="chk-no">☐</span><span class="chk-no-text">$1</span>');
      body = body.replace(/&lt;red&gt;([\s\S]*?)&lt;\/red&gt;/g, '<span class="ai-red">$1</span>');
      // 移除舊的 bid_selector 標記（已改為動態）
      body = body.replace(/&lt;bid_selector[^&]*&gt;/g, '');
      body = body.replace(/\n•/g, '<br>•');
      body = body.replace(/^•/, '•');
      body = body.replace(/\n/g, '<br>');
      return `<div class="ai-section"><div class="ai-section-title">${esc(title)}</div><div class="ai-section-body">${body}</div></div>`;
    }
    return `<div class="ai-section"><div class="ai-section-body">${esc(section).replace(/\n/g, "<br>")}</div></div>`;
  }).join("");
}

function renderBidSection() {
  const p = _detailP;
  if (!p) return "—";
  const land = p.land_area_ping;
  const zoning = effectiveZoning(p);
  const price = p.new_house_price_wan_override || DISTRICT_NEW_HOUSE_PRICE[p.district] || null;
  const roadWidth = p.road_width_m;
  // 優先用 欲出價；若無再 fallback 到開價
  const priceWan = parseFloat(desiredPriceWan(p)) || (p.price_ntd ? p.price_ntd / 10000 : 0);
  if (!land || !zoning || !price) return "缺資料，無法計算";

  const baseFar = TAIPEI_FAR_PCT[zoning];
  const effFar = effectiveFarPct(zoning, roadWidth);
  const coeff = p.rebuild_coeff ?? 1.57;
  const [ratio, parking] = lookupShareRatio(price);
  const isFangzai = p.city === "台北市" && currentAge(p) && (new Date().getFullYear() - currentAge(p)) <= 1974;
  const bonusW = p.bonus_weishau ?? 0.30;
  const bonusD = p.bonus_dugen ?? (isFangzai ? 0.80 : 0.50);
  const is1F = Number(p.floor) === 1;
  const floorPremium = p.floor_premium ?? (is1F ? 0.20 : 0);
  const effectivePrice = price * (1 + floorPremium);
  const calcVal = b => {
    const share = land * (effFar/100) * (1+b) * coeff * (ratio||0);
    return share * effectivePrice + (share / 40) * (parking || 0);
  };
  const wVal = Math.round(calcVal(bonusW));
  const dVal = Math.round(calcVal(bonusD));

  const fmt = n => n.toLocaleString("zh-TW", {maximumFractionDigits: 0});
  // 分回價值永遠顯示（跟出價無關）
  const hasPrice = priceWan > 0;
  const multW = hasPrice ? `（${(wVal / priceWan).toFixed(2)}倍）` : "";
  const multD = hasPrice ? `（${(dVal / priceWan).toFixed(2)}倍）` : "";

  let html = wVal
    ? `危老 ${fmt(wVal)}萬${multW}　都更 ${fmt(dVal)}萬${multD}`
    : `都更 ${fmt(dVal)}萬${multD}`;

  if (!hasPrice) {
    html += `<div class="bid-row text-muted">（尚未填入開價，無法給出價建議）</div>`;
    return html;
  }

  const opts = [3.0,3.1,3.2,3.3,3.4,3.5,3.6,3.7,3.8,3.9,4.0,4.2,4.5,5.0];
  const mkOpts = (sel) => opts.map(v => `<option value="${v}" ${Math.abs(v-sel)<0.01?'selected':''}>${v.toFixed(1)}倍</option>`).join('');
  const wMax = wVal ? Math.round(wVal / 3.2) : 0;
  const dMax = Math.round(dVal / 3.2);
  if (wVal) {
    html += `<div class="bid-row">• 危老出價建議：<select class="bid-select" onchange="this.nextElementSibling.textContent='≤ '+Math.round(${wVal}/parseFloat(this.value)).toLocaleString()+' 萬'">${mkOpts(3.2)}</select> <span class="ai-red">≤ ${fmt(wMax)} 萬</span></div>`;
  }
  html += `<div class="bid-row">• 都更出價建議：<select class="bid-select" onchange="this.nextElementSibling.textContent='≤ '+Math.round(${dVal}/parseFloat(this.value)).toLocaleString()+' 萬'">${mkOpts(3.2)}</select> <span class="ai-red">≤ ${fmt(dMax)} 萬</span></div>`;
  return html;
}

function inferredAddressCellHTML(p) {
  const cands = Array.isArray(p.address_inferred_candidates_detail) ? p.address_inferred_candidates_detail : [];
  const current = p.address_inferred || p.address || p.title || "";
  const mapLink = `<a href="https://www.google.com/maps/search/${encodeURIComponent(fullAddress(p))}" target="_blank" rel="noopener noreferrer" class="map-link" title="Google Maps">📍</a>`;
  // 單筆候選或無候選 → 純文字顯示
  if (cands.length <= 1) {
    return `${esc(stripCityDist(current))} ${mapLink}`;
  }
  // 多筆 → 下拉選單（只放地址，不帶地坪；地坪切換會在下方欄位即時更新）
  const opts = cands.map(c => {
    const sel = c.address === current ? "selected" : "";
    const label = stripCityDist(c.address) + (c.is_reverse_geo ? "（座標反查）" : "");
    return `<option value="${esc(c.address)}" ${sel}>${esc(label)}</option>`;
  }).join("");
  return `<select class="inferred-choice-select" onchange="saveInferredChoice('${p.id}', this.value)">${opts}</select> ${mapLink}`;
}

async function saveInferredChoice(id, address) {
  try {
    const r = await authedFetch(`/api/properties/${encodeURIComponent(id)}/inferred_choice`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ address }),
    });
    if (!r.ok) { alert("儲存失敗"); return; }
    const data = await r.json();
    // 前端即時更新：同步 _detailP 與 allProperties，然後重新開 modal 以刷新所有依賴 land 的欄位
    const patch = { address_inferred: address };
    if (data.land_ping != null) {
      patch.land_area_ping = data.land_ping;
      patch.land_area_sqm = Math.round(data.land_ping * 3.30578 * 100) / 100;
    } else {
      // 座標反查選項：無地坪資料 → 清空欄位
      patch.land_area_ping = null;
      patch.land_area_sqm = null;
    }
    if (_detailP && _detailP.id === id) Object.assign(_detailP, patch);
    const idx = (allProperties || []).findIndex(x => x.id === id);
    if (idx >= 0) Object.assign(allProperties[idx], patch);
    // 重新渲染 modal (地坪/萬坪/都更試算等依 land_area_ping 計算的欄位都會更新)
    if (_detailP && _detailP.id === id) showDetailModal(_detailP);
  } catch (e) {
    console.error("saveInferredChoice error", e);
  }
}

// 切換 roadWidth 地籍圖預覽。從 allProperties 查 p 以避免把 url/reason 嵌入 onclick 字串造成 XSS。
window.toggleRoadPreview = function (btn, propertyId) {
  const td = btn.closest("td");
  if (!td) return;
  const existing = td.querySelector(".road-preview-inline");
  if (existing) { existing.remove(); return; }
  const p = (allProperties || []).find(x => x.id === propertyId)
         || (_detailP && _detailP.id === propertyId ? _detailP : null);
  if (!p || !p.screenshot_roadwidth) return;
  const container = document.createElement("div");
  container.className = "road-preview-img road-preview-inline";
  const img = document.createElement("img");
  img.src = p.screenshot_roadwidth;
  img.addEventListener("click", () => openRoadOverlay(p.screenshot_roadwidth, p.road_width_vision_reason || ""));
  const reasonEl = document.createElement("div");
  reasonEl.className = "road-reason";
  reasonEl.textContent = p.road_width_vision_reason || "";
  container.appendChild(img);
  container.appendChild(reasonEl);
  td.appendChild(container);
};

function roadWidthCellHTML(p) {
  // 只要 DB 有路寬值（含 road_width_unknown 狀態），或者有路名從巷名查到 → 就顯示
  // 後端 CQL 補查可以從「X路X段X巷」拿到路寬，不需要完整門牌
  const hasRoadWidth = p.road_width_m != null || p.road_width_name || p.road_width_unknown;
  const hasAddr = ((p.address_inferred || "").includes("號")) || ((p.address || "").includes("號"));
  if (!hasRoadWidth && !hasAddr) {
    return '<span class="text-muted">需精確地址才能查詢</span>';
  }

  let html;
  if (p.road_width_unknown) {
    // Vision 判了路名但 GeoServer 沒對應寬度 → 給用戶手動填入的 input + 警告
    html = `<input type="number" class="inline-edit" min="0" step="0.5"
      value="${p.road_width_m ?? ""}" placeholder="—"
      onchange="saveRoadWidth('${p.id}', this.value)"> m <span class="road-unknown-note">（寬度不明，有可能為私巷或特窄巷弄）</span>`;
  } else {
    html = `<input type="number" class="inline-edit" min="0" step="0.5"
      value="${p.road_width_m ?? ""}" placeholder="—"
      onchange="saveRoadWidth('${p.id}', this.value)"> m`;
  }

  if (p.screenshot_roadwidth) {
    html += ` <button class="btn-scan-road btn-show-map" onclick="event.stopPropagation(); toggleRoadPreview(this, '${esc(p.id)}')">地籍圖</button>`;
  } else if (p.city === "台北市") {
    // 沒截圖（zonemap 當時 timeout 或其他原因 fail）→ 提供手動重掃按鈕
    html += ` <button class="btn-scan-road" onclick="event.stopPropagation(); scanRoadWidth('${p.id}', this)">重新掃描路寬</button>`;
  }
  const hint = roadNameHint(p);
  if (hint) html += `<div class="road-name-hint-line">${esc(hint)}</div>`;
  return html;
}

function setZonePing(id, idx, val) {
  if (!_detailP || !_detailP.zoning_list) return;
  if (_detailP.zoning_ratios_locked) return;
  const total = Number(_detailP.land_area_ping) || 0;
  if (total <= 0) return;
  const n = _detailP.zoning_list.length;
  let v = parseFloat(val);
  if (isNaN(v) || v < 0) v = 0;

  const errEl = document.getElementById(`zone-ping-err-${id}`);
  if (v > total) {
    if (errEl) {
      errEl.textContent = `⚠ 單一分區坪數 ${v.toFixed(2)} 超過總土地 ${total} 坪`;
      errEl.style.display = "";
    }
    // 保留原本 ratio 不動
    return;
  }
  if (errEl) { errEl.textContent = ""; errEl.style.display = "none"; }

  // 坪數 → 比例
  const pings = _detailP.zoning_list.map((_, i) => {
    const r = (_detailP.zoning_ratios || _detailP.zoning_list.map(() => 100 / n))[i];
    return total * (Number(r) || 0) / 100;
  });
  pings[idx] = v;
  // 兩分區：另一個 = total - v
  if (n === 2) {
    pings[1 - idx] = total - v;
  } else {
    // 多分區：其餘按既有比例分配 total - v
    const rest = total - v;
    const otherSum = pings.reduce((a, b, i) => i === idx ? a : a + b, 0);
    if (otherSum > 0) {
      for (let i = 0; i < n; i++) {
        if (i !== idx) pings[i] = pings[i] / otherSum * rest;
      }
    } else {
      for (let i = 0; i < n; i++) if (i !== idx) pings[i] = rest / (n - 1);
    }
  }
  // 轉回 ratio (%)
  const ratios = pings.map(p => total > 0 ? (p / total) * 100 : 0);
  _detailP.zoning_ratios = ratios;

  // 同步其他輸入框顯示
  document.querySelectorAll(".zone-ping-input").forEach((el, i) => {
    if (i !== idx && i < pings.length) el.value = pings[i].toFixed(2);
  });
  if (_detailP && !_detailP._in_watchlist) _detailP._ephemeral_edit_made = true;
  _rerenderRenewal();
  _syncDetailToList();
  fetch(`/api/properties/${id}/zoning_ratios`, {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({ zoning_ratios: ratios }),
  }).catch(e => console.error("setZonePing", e));
}

function setZoneRatio(id, idx, val) {
  if (!_detailP || !_detailP.zoning_list) return;
  if (_detailP.zoning_ratios_locked) return;   // LVR 鎖定時不允許修改
  const n = _detailP.zoning_list.length;
  const ratios = _detailP.zoning_ratios
    ? _detailP.zoning_ratios.slice()
    : _detailP.zoning_list.map(() => 100 / n);
  let v = parseFloat(val);
  if (isNaN(v)) v = 0;
  v = Math.max(0, Math.min(100, v));
  ratios[idx] = v;
  // 兩分區 → 另一個自動補足 100
  if (n === 2) {
    ratios[1 - idx] = 100 - v;
  } else if (n > 2) {
    // 多分區：其餘等比縮放使總和=100
    const rest = 100 - v;
    const others = ratios.filter((_, i) => i !== idx);
    const sum = others.reduce((a, b) => a + (Number(b) || 0), 0);
    if (sum > 0) {
      for (let i = 0; i < n; i++) {
        if (i !== idx) ratios[i] = (ratios[i] / sum) * rest;
      }
    } else {
      for (let i = 0; i < n; i++) if (i !== idx) ratios[i] = rest / (n - 1);
    }
  }
  _detailP.zoning_ratios = ratios;
  // 同步 DOM 中其他 zone-ratio-input 的顯示值（使用者看到另一個欄位跟著變）
  document.querySelectorAll(".zone-ratio-input").forEach((el, i) => {
    if (i !== idx && i < ratios.length) el.value = Math.round(Number(ratios[i]) || 0);
  });
  if (_detailP && !_detailP._in_watchlist) _detailP._ephemeral_edit_made = true;
  _rerenderRenewal();
  _syncDetailToList();
  fetch(`/api/properties/${id}/zoning_ratios`, {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({ zoning_ratios: ratios }),
  }).catch(e => console.error("setZoneRatio", e));
}

function openRoadOverlay(src, reason) {
  const existing = document.querySelector(".road-preview-overlay");
  if (existing) { existing.remove(); return; }
  const overlay = document.createElement("div");
  overlay.className = "road-preview-overlay";
  const img = document.createElement("img");
  img.src = src;
  overlay.appendChild(img);
  if (reason) {
    const r = document.createElement("div");
    r.className = "road-reason";
    r.textContent = reason;
    overlay.appendChild(r);
  }
  overlay.addEventListener("click", () => overlay.remove());
  document.body.appendChild(overlay);
}

// ── 篩選 ──────────────────────────────────────────────────────────────────────
function buildFilterParams() {
  // 區/類型/分數全改 client-side 篩選，server 不收 filter params
  return "";
}

// 樓層「全部」chip：勾/取消連動 1F~5F
window.toggleAllFloors = function (el) {
  const checked = !!el.checked;
  document.querySelectorAll('#floor-chips input').forEach(c => { c.checked = checked; });
  applyFilters();
};

// 區域「全部」chip：勾/取消連動該城市的所有區 chips
window.toggleAllDists = function (city, el) {
  const checked = !!el.checked;
  // 只勾/只反勾「沒被 disable」的區，保留 disabled chip 狀態
  document.querySelectorAll(`.filter-dist[data-city="${city}"]`).forEach(c => {
    if (!c.disabled) c.checked = checked;
  });
  applyFilters();
};

let sortDir = "desc";

function applyFilters() {
  _currentPage = 1;
  if (_activeTab === "explore") {
    // 搜尋 tab 的 filter 只在按下「開始搜尋」時才送 server，變動 filter 不再自動重抓
    _saveExploreFilters();
    // 不觸發 runExploreSearch / filterAndSort，用戶需手動按「開始搜尋」
  } else {
    loadProperties();
  }
}
// 排序 / hide-bad：只重排現有資料，不動 server
window.applyClientOrder = function () {
  _currentPage = 1;
  // 「隱藏不易都更物件」chip 兩 tab 共用 → 獨立 localStorage 持久化（不綁 explore filter set）
  try {
    const hn = document.getElementById("hide-non-renewable");
    if (hn) localStorage.setItem("hide-non-renewable", hn.checked ? "1" : "0");
  } catch {}
  if (_activeTab === "explore") {
    _saveExploreFilters();
    allProperties = (_exploreResults || []).slice();
  }
  filterAndSort();
};

let _currentPage = 1;
const PAGE_SIZE = 100;

function filterAndSort() {
  // 一律排除軟刪除 + 分析中/分析失敗的（這些是「壞資料」狀態，不應出現在搜尋/清單畫面，
  //   server 不再幫忙過濾，admin 跟 client 看到的 API response 一致由 client 決定顯示）
  let list = allProperties.filter(p =>
    !p.deleted &&
    !p.analysis_error &&
    !p.analysis_in_progress &&
    p.archived !== true
  );

  // 「隱藏新北偏遠物件」chip 勾選時 → 過濾掉 is_remote_area（新北市偏遠路段）
  // 特殊土地分區（unsuitable_for_renewal）不再被此 chip 控制，由 badge 提醒用戶；
  // 兩 tab 共用，預設勾選
  if (document.getElementById("hide-non-renewable")?.checked) {
    list = list.filter(p => !p.is_remote_area);
  }

  // 搜尋 tab 的條件都在 server 端過濾，client 不再重跑同套邏輯，直接信任 _exploreResults
  // 搜尋 tab：勾「隱藏5層以上物件」時過濾掉（computeSkipReasons 只會回五層以上一條）
  if (_activeTab === "explore" && document.getElementById("explore-hide-bad")?.checked) {
    const _th = loadThresholds();
    list = list.filter(p => computeSkipReasons(p, _th).length === 0);
  }

  // 搜尋 tab：勾「僅顯示獲利倍數 ___ 倍以上」→ 取「都更」倍數為主；沒有則 危老；都無則過濾
  if (_activeTab === "explore" && document.getElementById("explore-min-profit-on")?.checked) {
    const minProfit = parseFloat(document.getElementById("explore-min-profit")?.value);
    if (!isNaN(minProfit) && minProfit > 0) {
      list = list.filter(p => {
        const m = computeRowMultiples(p);
        const v = m.d ?? m.w ?? null;
        return v != null && v >= minProfit;
      });
    }
  }

  // 搜尋 tab 的樓層/價格/坪數條件一律在 server 端過濾；此處只同步「全部」chip 視覺狀態
  if (_activeTab === "explore") {
    const allFloorChips = document.querySelectorAll('#floor-chips input');
    const floorChecks = Array.from(document.querySelectorAll('#floor-chips input:checked'));
    const floorAllEl = document.getElementById("floor-all");
    if (floorAllEl) floorAllEl.checked = (floorChecks.length === allFloorChips.length);
    // 同步每城市的「全部」chip（只看非 disabled 的區）
    document.querySelectorAll(".filter-dist-all").forEach(allEl => {
      const city = allEl.dataset.city;
      const chips = document.querySelectorAll(`.filter-dist[data-city="${city}"]:not(:disabled)`);
      const checked = Array.from(chips).filter(c => c.checked);
      allEl.checked = (checked.length === chips.length && chips.length > 0);
    });
  }

  // 排序
  const sortBy = document.getElementById("sort-by")?.value || "list_rank";
  const getKey = p => {
    if (sortBy === "price_per_land_ping") {
      return (p.price_ntd && p.land_area_ping) ? p.price_ntd / p.land_area_ping : null;
    }
    if (sortBy === "price_per_building_ping") {
      return (p.price_ntd && p.building_area_ping) ? p.price_ntd / p.building_area_ping : null;
    }
    if (sortBy === "profit_multiple") {
      const m = computeRowMultiples(p);
      // 取「都更」倍數為主；沒有則 危老
      return m.d ?? m.w ?? null;
    }
    if (sortBy === "list_rank") return p.list_rank ?? 9999;
    if (sortBy === "published_at") return p.published_at || p.scraped_at || null;
    if (sortBy === "last_change_at") return p.last_change_at || p.scrape_session_at || p.scraped_at || null;
    return p[sortBy] ?? null;
  };
  if (sortBy === "list_rank") {
    // 預設：新批次排前 + 同批次內原排序；↑↓ 反轉整組順序
    // 排序優先序：_added_at（backend 給的「進入清單時間」） > scrape_session_at > list_rank
    // user_url / manual 物件沒 scrape_session_at，但有 _added_at = 用戶加入清單時間 → 應該排上面
    const dirMul = sortDir === "desc" ? 1 : -1;   // desc = 新→舊（預設）
    list.sort((a, b) => {
      const ka = a._added_at || a.scrape_session_at || "";
      const kb = b._added_at || b.scrape_session_at || "";
      if (ka !== kb) return kb.localeCompare(ka) * dirMul;
      return ((a.list_rank ?? 9999) - (b.list_rank ?? 9999)) * dirMul;
    });
  } else {
    const dirMul = sortDir === "desc" ? -1 : 1;
    list.sort((a, b) => {
      const va = getKey(a), vb = getKey(b);
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (va < vb) return -1 * dirMul;
      if (va > vb) return 1 * dirMul;
      return 0;
    });
  }

  filteredProperties = list;
  // 分頁：clamp current page，切片給 renderList
  const totalPages = Math.max(1, Math.ceil(list.length / PAGE_SIZE));
  if (_currentPage > totalPages) _currentPage = totalPages;
  if (_currentPage < 1) _currentPage = 1;
  const startIdx = (_currentPage - 1) * PAGE_SIZE;
  const pageItems = list.slice(startIdx, startIdx + PAGE_SIZE);
  renderList(pageItems);
  renderMapMarkers(filteredProperties);
  _renderPagination(list.length, totalPages);
  document.getElementById("result-count").textContent =
    `共 ${list.length} 筆` + (totalPages > 1 ? `（第 ${_currentPage}/${totalPages} 頁）` : "");
}

function _renderPagination(total, totalPages) {
  let el = document.getElementById("page-controls");
  if (!el) {
    el = document.createElement("div");
    el.id = "page-controls";
    el.className = "page-controls";
    const list = document.getElementById("property-list");
    list.parentNode.insertBefore(el, list.nextSibling);
  }
  if (totalPages <= 1) { el.innerHTML = ""; return; }

  // 顯示 1 / 前後 2 / 最後；其餘用 …
  const pages = new Set([1, totalPages, _currentPage - 1, _currentPage, _currentPage + 1]);
  const nums = [...pages].filter(n => n >= 1 && n <= totalPages).sort((a, b) => a - b);
  const parts = [];
  parts.push(`<button class="pg-btn" ${_currentPage === 1 ? "disabled" : ""} onclick="goToPage(${_currentPage - 1})">上一頁</button>`);
  let prev = 0;
  for (const n of nums) {
    if (n - prev > 1) parts.push(`<span class="pg-sep">…</span>`);
    parts.push(`<button class="pg-btn ${n === _currentPage ? "pg-btn--active" : ""}" onclick="goToPage(${n})">${n}</button>`);
    prev = n;
  }
  parts.push(`<button class="pg-btn" ${_currentPage === totalPages ? "disabled" : ""} onclick="goToPage(${_currentPage + 1})">下一頁</button>`);
  el.innerHTML = parts.join("");
}

function goToPage(n) {
  _currentPage = n;
  filterAndSort();
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function toggleSortDir() {
  sortDir = sortDir === "desc" ? "asc" : "desc";
  document.getElementById("sort-dir").textContent = sortDir === "desc" ? "↓" : "↑";
  if (_activeTab === "explore") {
    applyClientOrder();
  } else {
    applyFilters();
  }
}

function updateScoreLabel(val) {
  document.getElementById("score-label").textContent = val;
}

function populateDistrictFilter() {
  // 舊版 <select id="filter-district"> 已改成 chip 勾選，這裡保留空 stub 以相容既有呼叫
  const sel = document.getElementById("filter-district");
  if (!sel) return;
  const districts = [
    "中正區", "大同區", "中山區", "松山區", "大安區", "萬華區",
    "信義區", "內湖區", "南港區", "文山區",
    "板橋區", "新店區", "中和區", "永和區",
  ];
  districts.forEach(d => {
    const opt = document.createElement("option");
    opt.value = d;
    opt.textContent = d;
    sel.appendChild(opt);
  });
}

// ── 爬取 ──────────────────────────────────────────────────────────────────────
let _abortController = null;

let _activeTaskId = null;

function _lockBothButtons(activeId) {
  _activeTaskId = activeId;
  const ids = ["btn-scrape", "btn-scrape-url", "btn-manual-submit"];
  ids.forEach(id => {
    const btn = document.getElementById(id);
    if (btn && id !== activeId) btn.disabled = true;
  });
}

function _unlockBothButtons() {
  _activeTaskId = null;
  const scrapeBtn = document.getElementById("btn-scrape");
  const urlBtn = document.getElementById("btn-scrape-url");
  const manualBtn = document.getElementById("btn-manual-submit");
  if (scrapeBtn) { scrapeBtn.disabled = false; scrapeBtn.textContent = "▶ 抓取新資料"; }
  if (urlBtn) { urlBtn.disabled = false; urlBtn.textContent = "送出"; }
  if (manualBtn) { manualBtn.disabled = false; manualBtn.textContent = "送出分析"; }
}

async function triggerScrapeUrl() {
  if (_activeTaskId) { alert("有任務正在執行中，請等待完成"); return; }
  const btn = document.getElementById("btn-scrape-url");
  const inp = document.getElementById("scrape-url");
  const url = inp.value.trim();
  if (!url) { alert("請輸入網址"); return; }
  // 接受 591 / 永慶 / 信義 物件詳情頁 URL
  const okPatterns = [
    /sale\.591\.com\.tw\/.*\d{6,}/,
    /buy\.yungching\.com\.tw\/house\/\d{6,8}/,
    /sinyi\.com\.tw\/buy\/house\/[A-Z0-9]{4,8}/i,
  ];
  if (!okPatterns.some(re => re.test(url))) {
    alert("看起來不是 591、永慶或信義物件詳情頁網址，請確認");
    return;
  }

  _lockBothButtons("btn-scrape-url");
  inp.disabled = true;
  _startFakeProgress(`URL 分析送出中：${url}（若佇列忙可能需等幾十秒）`);

  try {
    const res = await fetch("/api/scrape_url", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url }),
    });
    const data = await res.json();
    _stopFakeProgress(data.status === "ok" ? "處理完成" : "送出完成");
    inp.disabled = false;
    if (data.status === "ok") {
      inp.value = "";
      applyFilters();
      alert("處理完成" + (data.message ? `：${data.message}` : ""));
    } else if (data.status === "busy") {
      alert("目前任務較多，請稍候再送：" + (data.message || ""));
    } else if (data.status === "skipped_non_apartment") {
      // 永慶 / 591 樓高 > 5 → 不分析（非公寓，目前只支援 5F 以下）
      inp.value = "";
      alert("⏭ 跳過：" + (data.message || "此物件超過 5 層樓，目前僅分析 5F 以下公寓"));
    } else if (data.status === "error") {
      alert(`分析失敗：${data.message || data.detail || "unknown"}`);
    } else {
      alert(`未預期回應 (${data.status})：${data.message || data.detail || "unknown"}`);
    }
  } catch (e) {
    _stopFakeProgress("失敗");
    inp.disabled = false;
    alert("處理失敗：" + e.message);
  } finally {
    _unlockBothButtons();
  }
}

async function triggerScrape() {
  if (_activeTaskId) { alert("有任務正在執行中，請等待完成"); return; }
  const btn = document.getElementById("btn-scrape");
  const districts = Array.from(
    document.querySelectorAll('#dist-picker input[type="checkbox"]:checked:not([data-all])')
  ).map(cb => cb.value).filter(v => v && v !== "on");
  const limitInput = document.getElementById("scrape-limit").value;
  const limit = parseInt(limitInput, 10) || 0;

  if (!districts.length) {
    alert("請至少勾選一個行政區");
    return;
  }
  if (districts.length > MAX_DISTRICTS) {
    alert(`最多只能選 ${MAX_DISTRICTS} 個行政區`);
    return;
  }
  _lockBothButtons("btn-scrape");
  btn.textContent = "⏹ 取消";
  btn.disabled = false;
  let cancelled = false;
  btn.onclick = async () => {
    cancelled = true;
    btn.disabled = true;
    btn.textContent = "取消中...";
    try { await fetch("/api/cancel", { method: "POST" }); } catch {}
  };
  const label = districts.length > 3 ? `${districts.length} 區` : districts.join("、");
  showProgress(`連線後端，準備開始爬取 ${label}...`);

  try {
    const thresholds = loadThresholds();
    await fetch("/api/scrape", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        districts,
        limit,
        headless: true,
        max_floors: thresholds.maxFloors,
        max_total_price_wan: thresholds.maxTotal,
        max_price_per_building_ping_wan: thresholds.maxBld,
        max_price_per_land_ping_wan: thresholds.maxLand,
      }),
    });
    const es = new EventSource("/api/scrape/status");
    es.onmessage = (e) => {
      const data = JSON.parse(e.data);
      if (data.msg && data.msg !== "heartbeat") showProgress(data.msg, data.percent);
      if (data.new_item) loadProperties();
      if (data.done || data.error) {
        es.close();
        setTimeout(() => {
          hideProgress();
          _unlockBothButtons();
          btn.onclick = () => triggerScrape();
          loadProperties();
          loadStats();
        }, 800);
      }
    };
    es.onerror = () => {
      es.close();
      hideProgress();
      _unlockBothButtons();
      btn.onclick = () => triggerScrape();
    };
  } catch (e) {
    hideProgress();
    _unlockBothButtons();
    btn.onclick = () => triggerScrape();
    alert("爬取啟動失敗：" + e.message);
  }
}

let _progressStart = null;

function showProgress(msg, percent) {
  if (!_progressStart) _progressStart = Date.now();
  document.getElementById("progress-bar-wrap").classList.remove("d-none");
  document.getElementById("progress-msg").textContent = msg;
  if (typeof percent === "number") {
    document.getElementById("progress-bar-fill").style.width = percent + "%";
    document.getElementById("progress-pct").textContent = Math.round(percent) + "%";
  }
  // append to log
  if (msg && msg !== "heartbeat") {
    const log = document.getElementById("progress-log");
    const elapsed = ((Date.now() - _progressStart) / 1000).toFixed(1);
    const line = document.createElement("div");
    line.className = "log-line";
    line.innerHTML = `<span class="log-time">${elapsed}s</span>${esc(msg)}`;
    log.appendChild(line);
    log.classList.add("has-lines");
    log.scrollTop = log.scrollHeight;
  }
}

function hideProgress() {
  document.getElementById("progress-bar-wrap").classList.add("d-none");
  // 清空 log
  const log = document.getElementById("progress-log");
  log.innerHTML = "";
  log.classList.remove("has-lines");
  _progressStart = null;
}

// 螢幕中央浮現的輕柔提示（自動 fade out，不需用戶按任何鍵）
function showFadingToast(msg) {
  const el = document.createElement("div");
  el.className = "fading-toast";
  el.textContent = msg;
  el.style.cssText = `
    position: fixed; top: 40%; left: 50%; transform: translate(-50%, -50%);
    background: rgba(33, 37, 41, 0.92); color: #fff;
    padding: 18px 28px; border-radius: 10px; font-size: 15px; line-height: 1.55;
    white-space: pre-line; text-align: center; max-width: 88vw;
    box-shadow: 0 6px 28px rgba(0,0,0,0.35); z-index: 9999;
    opacity: 0; transition: opacity .25s ease;
    pointer-events: none;
  `;
  document.body.appendChild(el);
  requestAnimationFrame(() => { el.style.opacity = "1"; });
  setTimeout(() => {
    el.style.opacity = "0";
    setTimeout(() => el.remove(), 400);
  }, 3500);
}

// 單次 RPC 場景（URL / 手動送出）用假進度：S 曲線漸近 95%，完成後跳 100% 再收
let _fakeProgressTick = null;
function _startFakeProgress(msg) {
  if (_fakeProgressTick) clearInterval(_fakeProgressTick);
  showProgress(msg, 0);
  const start = Date.now();
  _fakeProgressTick = setInterval(() => {
    const elapsed = (Date.now() - start) / 1000;
    const pct = Math.min(95, 100 * (1 - Math.exp(-elapsed / 25)));
    document.getElementById("progress-bar-fill").style.width = pct.toFixed(1) + "%";
    document.getElementById("progress-pct").textContent = Math.floor(pct) + "%";
  }, 200);
}
function _stopFakeProgress(finalMsg) {
  if (_fakeProgressTick) { clearInterval(_fakeProgressTick); _fakeProgressTick = null; }
  showProgress(finalMsg || "完成", 100);
  setTimeout(hideProgress, 1200);
}

// ── 深度分析（Phase 2） ───────────────────────────────────────────────────────
// LVR 浮現 popup
let _lvrHideTimer = null;
function showLvrPopup(event, id) {
  clearTimeout(_lvrHideTimer);
  _lvrHideTimer = null;
  const old = document.getElementById("lvr-popup");
  if (old) old.remove();
  const p = allProperties.find(x => x.id === id);
  if (!p || !p.lvr_records || !p.lvr_records.length) return;
  const popup = document.createElement("div");
  popup.id = "lvr-popup";
  popup.className = "lvr-popup";
  popup.innerHTML = `
    <div class="lvr-popup__title">實價登錄 ${p.lvr_records.length} 筆</div>
    <table class="lvr-table">
      <tr><th>地址</th><th>建坪</th><th>地坪</th><th>成交價</th><th>日期</th><th></th></tr>
      ${p.lvr_records.map(r => `
      <tr${r.is_special ? ' class="lvr-special"' : ''}>
        <td>${esc(stripCityDist(r.address))}</td>
        <td>${r.area_ping ?? "—"}</td>
        <td>${r.land_ping ?? "—"}</td>
        <td>${r.price_total ? (r.price_total / 10000).toLocaleString("zh-TW",{maximumFractionDigits:0}) + "萬" : "—"}</td>
        <td>${esc(r.txn_date || "—")}</td>
        <td>${r.is_special ? `<span class="lvr-warn" data-note="${esc(r.note || '')}">⚠<span class="lvr-tip">${esc(r.note || '')}</span></span>` : ""}</td>
      </tr>`).join("")}
    </table>`;
  popup.addEventListener("mouseenter", () => { clearTimeout(_lvrHideTimer); _lvrHideTimer = null; });
  popup.addEventListener("mouseleave", () => { _lvrHideTimer = setTimeout(hideLvrPopup, 200); });
  const rect = event.target.getBoundingClientRect();
  popup.style.top = (rect.bottom + 4) + "px";
  popup.style.left = Math.max(0, rect.left - 200) + "px";
  document.body.appendChild(popup);
}
function hideLvrPopup() {
  clearTimeout(_lvrHideTimer);
  _lvrHideTimer = setTimeout(() => {
    const old = document.getElementById("lvr-popup");
    if (old) old.remove();
  }, 200);
}

// ── 手動輸入地址送出分析 ──────────────────────────────────────────────────
// 從 server 拉目標分析範圍（與爬取設定 config.TARGET_REGIONS 一致）
let MANUAL_DISTRICTS = {
  "台北市": ["中正區", "大同區", "中山區", "松山區", "大安區", "萬華區",
             "信義區", "南港區", "文山區"],
  "新北市": ["板橋區", "新店區", "中和區", "永和區"],
};
fetch("/api/target_regions").then(r => r.json()).then(d => {
  if (d && typeof d === "object") {
    MANUAL_DISTRICTS = d;
    // 更新 city 下拉、重 populate districts
    const citySel = document.getElementById("manual-city");
    if (citySel) {
      const prevCity = citySel.value;
      citySel.innerHTML = Object.keys(MANUAL_DISTRICTS)
        .map(c => `<option value="${c}">${c}</option>`).join("");
      if (Object.keys(MANUAL_DISTRICTS).includes(prevCity)) citySel.value = prevCity;
      populateManualDistricts();
    }
  }
}).catch(() => {});

function populateManualDistricts() {
  const city = document.getElementById("manual-city")?.value;
  const sel = document.getElementById("manual-district");
  if (!sel || !city) return;
  sel.innerHTML = "";
  (MANUAL_DISTRICTS[city] || []).forEach(d => {
    const opt = document.createElement("option");
    opt.value = d; opt.textContent = d;
    sel.appendChild(opt);
  });
}

async function triggerManualAnalyze(useSource = "auto", overrideAddress = null) {
  if (_activeTaskId && _activeTaskId !== "btn-manual-submit") {
    alert("有任務正在執行中，請等待完成");
    return;
  }
  const city = document.getElementById("manual-city").value;
  const district = document.getElementById("manual-district").value;
  const addrEl = document.getElementById("manual-address");
  const address = overrideAddress || addrEl.value.trim();
  const bld = parseFloat(document.getElementById("manual-bld").value);
  const land = parseFloat(document.getElementById("manual-land").value);
  const price = parseFloat(document.getElementById("manual-price").value);

  if (!address) { alert("請輸入地址"); return; }

  // 目前支援台北市 + 新北市（板橋/新店/永和/中和）
  // 用戶可能在 address 欄位打非目標縣市地址繞過 → 用地址前綴擋
  const nonTargetCityPattern = /^(桃園|基隆|新竹|苗栗|台中|臺中|彰化|南投|雲林|嘉義|台南|臺南|高雄|屏東|宜蘭|花蓮|台東|臺東|澎湖|金門|連江)/;
  if (!["台北市", "新北市"].includes(city) || nonTargetCityPattern.test(address)) {
    alert("目前僅支援台北市與新北市地址分析。");
    return;
  }

  _lockBothButtons("btn-manual-submit");
  _startFakeProgress(`地址分析送出中：${address}`);

  try {
    const res = await fetch("/api/manual_analyze", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        city, district, address,
        building_area_ping: isNaN(bld) ? null : bld,
        land_area_ping: isNaN(land) ? null : land,
        price_wan: isNaN(price) ? null : price,
        use_source: useSource,
      }),
    });
    const data = await res.json();
    // 非 started（district_mismatch / not_found / lvr_mismatch / error / already_*）→ 立刻停進度並走既有對話框
    if (data.status !== "started") {
      _stopFakeProgress("送出完成");
      _handleManualResponse(data);
      return;
    }
    // started：後端已建 placeholder doc，立刻重抓列表讓用戶看到 loading row；
    // 期間右上角 fake progress 繼續跑，輪詢到 analysis_in_progress=false 才 stop
    loadProperties();
    if (data.source_id) {
      await _waitForManualAnalysisDone(data.source_id);
    }
    _stopFakeProgress("分析完成");
    loadProperties();   // 完成後重抓，顯示完整分析結果
  } catch (e) {
    _stopFakeProgress("失敗");
    alert("失敗：" + e);
  } finally {
    _unlockBothButtons();
  }
}

async function _waitForManualAnalysisDone(src_id) {
  const timeout = 180000;   // 3 分鐘上限
  const start = Date.now();
  while (Date.now() - start < timeout) {
    try {
      const r = await fetch(`/api/properties/${encodeURIComponent(src_id)}`);
      if (r.ok) {
        const p = await r.json();
        if (!p.analysis_in_progress) return p;
      }
    } catch (e) { /* 繼續輪詢 */ }
    await new Promise(res => setTimeout(res, 2000));
  }
  return null;
}

function _handleManualResponse(data) {
  console.log("[manual] response:", data);
  if (data.status === "error") {
    alert(data.error);
    return;
  }
  if (data.status === "district_mismatch") {
    _showDistrictMismatch(data);
    return;
  }
  if (data.status === "not_found") {
    _showManualSuggestions(data.error, data.suggestions || []);
    return;
  }
  if (data.status === "lvr_mismatch") {
    _showLvrMismatchDialog(data);
    return;
  }
  if (data.status === "ambiguous_unit") {
    _showAmbiguousUnitDialog(data);
    return;
  }
  if (data.status === "already_exists" || data.status === "already_running") {
    alert("此地址有進行中的分析，已為您顯示在列表中");
    loadProperties();
    return;
  }
  if (data.status === "started") {
    // 立刻 refresh 物件列表，因後端已建 placeholder doc；row 會帶 loading bar
    loadProperties();
    alert("已送出分析，物件會出現在列表最上方");
    return;
  }
  alert("未知回應：" + JSON.stringify(data));
}

function _showDistrictMismatch(data) {
  const cands = data.candidates || [];
  if (!cands.length) { alert(data.error); return; }
  const picked = prompt(
    data.error + "\n\n" +
    cands.map((c, i) => `${i + 1}. ${c.formatted}`).join("\n") +
    "\n\n請輸入編號（1-" + cands.length + "）或按取消放棄",
  );
  if (!picked) return;
  const idx = parseInt(picked) - 1;
  if (isNaN(idx) || idx < 0 || idx >= cands.length) {
    alert("選項無效");
    return;
  }
  const c = cands[idx];
  // 把下拉切成正確的 city/district 後重送
  const citySel = document.getElementById("manual-city");
  const distSel = document.getElementById("manual-district");
  citySel.value = c.city;
  populateManualDistricts();
  distSel.value = c.district;
  triggerManualAnalyze("auto", c.address);
}

function _showManualSuggestions(msg, suggestions) {
  if (!suggestions.length) { alert(msg); return; }
  const picked = prompt(
    msg + "\n\n" + suggestions.map((s, i) => `${i + 1}. ${s}`).join("\n") +
    "\n\n請輸入編號（1-" + suggestions.length + "）或按取消放棄",
  );
  if (!picked) return;
  const idx = parseInt(picked) - 1;
  if (isNaN(idx) || idx < 0 || idx >= suggestions.length) {
    alert("選項無效");
    return;
  }
  // 用選到的地址重新送
  triggerManualAnalyze("auto", suggestions[idx]);
}

function _showLvrMismatchDialog(data) {
  const u = data.user_input;
  const l = data.lvr_record;
  const msg =
    `${data.error}\n\n` +
    `您輸入：建坪 ${u.building_area_ping ?? "—"} 坪 / 地坪 ${u.land_area_ping ?? "—"} 坪\n` +
    `LVR：建坪 ${l.area_ping ?? "—"} 坪 / 地坪 ${l.land_ping ?? "—"} 坪 (${l.txn_date || ""})\n\n` +
    `輸入 L 使用 LVR 資料；輸入 U 使用您剛輸入的資料；取消放棄。`;
  const ans = prompt(msg);
  if (!ans) return;
  const a = ans.trim().toLowerCase();
  if (a === "l") triggerManualAnalyze("lvr");
  else if (a === "u") triggerManualAnalyze("user");
  else alert("選項無效");
}

function _showAmbiguousUnitDialog(data) {
  const cands = data.candidates || [];
  if (!cands.length) { alert(data.error); return; }
  const _rocToYmd = (roc) => {
    if (!roc || roc.length < 7) return roc || "—";
    const y = parseInt(roc.slice(0, -4)) + 1911;
    return `${y}-${roc.slice(-4, -2)}-${roc.slice(-2)}`;
  };
  const lines = cands.map((c, i) => {
    const txn = _rocToYmd(c.latest_txn_date);
    const price = c.latest_price_total
      ? `${(c.latest_price_total / 10000).toLocaleString("zh-TW", { maximumFractionDigits: 0 })} 萬`
      : "—";
    const yc = c.year_completed ? `${c.year_completed} 年完工` : "屋齡未知";
    const tf = c.total_floors != null ? `${c.total_floors}F 棟` : "樓層未知";
    return `${i + 1}. 建坪 ${c.building_area_ping ?? "—"} 坪 / 地坪 ${c.land_area_ping ?? "—"} 坪 / ${tf} / ${yc}\n   (共 ${c.n_transactions} 筆成交，最新 ${txn} 賣 ${price})`;
  });
  const picked = prompt(
    data.error + "\n\n" + lines.join("\n\n") + "\n\n請輸入編號（1-" + cands.length + "）或按取消放棄",
  );
  if (!picked) return;
  const idx = parseInt(picked) - 1;
  if (isNaN(idx) || idx < 0 || idx >= cands.length) {
    alert("選項無效");
    return;
  }
  const c = cands[idx];
  // mode 來自後端 — reanalyze 跟 new submit 走不同 endpoint，不能用同一條 fallback
  if (data.mode === "reanalyze") {
    // reanalyze：把候選戶的 area 帶進 reanalyze body 重送，doc id 不變
    reanalyzeManualFull({
      building_area_ping: c.building_area_ping,
      land_area_ping: c.land_area_ping,
    });
  } else if (data.mode === "admin_reanalyze") {
    alert("admin 端遇到歧義 — 請該物件的擁有者自己在前端選戶後重分析。");
  } else {
    // new submit：把候選值填回 form，再用 use_source="auto" 重送 → 後端 SQL 篩到唯一戶
    if (c.building_area_ping != null) document.getElementById("manual-bld").value = c.building_area_ping;
    if (c.land_area_ping != null) document.getElementById("manual-land").value = c.land_area_ping;
    triggerManualAnalyze("auto");
  }
}

async function triggerAnalyze(id) {
  if (!confirm("執行完整分析？（會跑 AI + 土地分區查詢，約 1 分鐘）")) return;
  try {
    // 立刻 local 標記「分析中」讓 loading bar 馬上出現（不等後端回）
    const p = allProperties.find(x => x.id === id);
    if (p) { p.analysis_in_progress = true; p.analysis_status = "done-pending"; filterAndSort(); }

    const res = await fetch(`/api/analyze/${id}`, { method: "POST" });
    const data = await res.json();
    if (data.status !== "started" && data.status !== "already_done") {
      // server 拒絕 → 回滾 local 狀態
      if (p) { p.analysis_in_progress = false; filterAndSort(); }
      alert(data.message || "啟動失敗");
      return;
    }
    _watchAnalyzing(id);
  } catch (e) {
    alert("失敗：" + e);
  }
}

// 輪詢：分析中的物件每 6 秒重抓一次直到完成
const _watchTimers = {};
function _watchAnalyzing(id) {
  if (_watchTimers[id]) return;
  _watchTimers[id] = setInterval(async () => {
    try {
      const res = await fetch(`/api/properties/${id}`);
      const d = await res.json();
      const p = allProperties.find(x => x.id === id);
      if (!p) return;
      Object.assign(p, d);
      if (!d.analysis_in_progress) {
        clearInterval(_watchTimers[id]); delete _watchTimers[id];
      }
      filterAndSort();
    } catch (e) { /* 網路失敗忽略，下一輪再試 */ }
  }, 6000);
}

// 頁面載入時，把目前所有 analysis_in_progress=true 的物件都掛上 watcher
function _resumeAnalyzingWatchers() {
  (allProperties || []).forEach(p => {
    if (p.analysis_in_progress) _watchAnalyzing(p.id);
  });
}

async function triggerDeepAnalysis(id) {
  try {
    const res = await fetch(`/api/deep_analyze/${id}`, { method: "POST" });
    const data = await res.json();
    alert(data.message || "深度分析已啟動，請稍後重新整理");
  } catch (e) {
    alert("啟動失敗：" + e.message);
  }
}


// ── 工具 ─────────────────────────────────────────────────────────────────────
function formatWan(ntd) {
  if (ntd == null) return "—";
  const prefix = ntd < 0 ? "-" : "+";
  const abs = Math.abs(ntd);
  return `${prefix}${(abs / 10000).toLocaleString("zh-TW", { maximumFractionDigits: 0 })} 萬`;
}
