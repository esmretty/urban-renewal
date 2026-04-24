// Admin Portal 前端：Google 登入 + 驗證 admin email + CRUD UI

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { getAuth, GoogleAuthProvider, signInWithPopup, signOut, onAuthStateChanged }
  from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";

let _user = null;
let _allDocs = [];

async function boot() {
  const res = await fetch("/api/firebase_config");
  const cfg = await res.json();
  if (!cfg.apiKey) {
    document.getElementById("login-err").textContent =
      "後端未設定 FIREBASE_WEB_API_KEY";
    return;
  }
  const app = initializeApp(cfg);
  const auth = getAuth(app);

  onAuthStateChanged(auth, async (user) => {
    if (!user) {
      showLogin();
      return;
    }
    const token = await user.getIdToken();
    // 驗 admin：呼叫 /api/me 看 is_admin
    const meRes = await fetch("/api/me", {
      headers: { Authorization: "Bearer " + token },
    });
    if (!meRes.ok) {
      document.getElementById("login-err").textContent = "驗證失敗";
      await signOut(auth);
      return;
    }
    const me = await meRes.json();
    if (!me.is_admin) {
      document.getElementById("login-msg").textContent =
        `帳號 ${me.email} 無管理者權限`;
      document.getElementById("login-err").textContent =
        "請改用管理者帳號登入";
      await signOut(auth);
      return;
    }
    _user = me;
    _user.getIdToken = () => user.getIdToken();
    showApp();
  });

  document.getElementById("btn-google").disabled = false;
  document.getElementById("btn-google").addEventListener("click", async () => {
    try {
      await signInWithPopup(auth, new GoogleAuthProvider());
    } catch (e) {
      document.getElementById("login-err").textContent = "登入失敗：" + (e.message || e.code);
    }
  });

  window.logoutAdmin = async () => { await signOut(auth); window.location.reload(); };
}

function showLogin() {
  document.getElementById("login-screen").classList.remove("hidden");
  document.getElementById("app").classList.add("hidden");
}

function showApp() {
  document.getElementById("login-screen").classList.add("hidden");
  document.getElementById("app").classList.remove("hidden");
  document.getElementById("user-email").textContent = _user.email;
  loadAll();
}

async function authedFetch(url, init = {}) {
  const token = await _user.getIdToken();
  const headers = new Headers(init.headers || {});
  headers.set("Authorization", "Bearer " + token);
  return fetch(url, { ...init, headers });
}

let _propTab = "batch";   // batch | user_url | manual

window.switchPropTab = async function (tab) {
  _propTab = tab;
  document.querySelectorAll(".prop-tab").forEach(b => {
    b.classList.toggle("active", b.dataset.tab === tab);
  });
  await loadPropList();
  renderList();
};

async function loadPropList() {
  let endpoint;
  if (_propTab === "manual") endpoint = "/admin/manual_properties";
  else if (_propTab === "user_url") endpoint = "/admin/properties?source=user_url";
  else endpoint = "/admin/properties?source=batch";
  const r = await authedFetch(endpoint);
  const data = await r.json();
  _allDocs = data.items || [];
}

window.loadAll = async function () {
  const [statsR, usersR] = await Promise.all([
    authedFetch("/admin/stats"),
    authedFetch("/admin/users"),
  ]);
  const stats = await statsR.json();
  const users = await usersR.json();
  renderStats(stats);
  await loadPropList();
  renderList();
  renderUsers(users.items || []);
  loadSchedulerStatus();
  loadSchedulerHistory();
  loadWhitelist();
};


// ── Email 白名單 ──────────────────────────────────────────────────────────────
async function loadWhitelist() {
  const box = document.getElementById("whitelist-box");
  if (!box) return;
  try {
    const r = await authedFetch("/admin/email_whitelist");
    if (!r.ok) { box.innerHTML = `<div style="color:#c0392b;">載入失敗 (${r.status})</div>`; return; }
    const data = await r.json();
    renderWhitelist(data.emails || []);
  } catch (e) {
    box.innerHTML = `<div style="color:#c0392b;">載入失敗：${e.message}</div>`;
  }
}

function renderWhitelist(emails) {
  const box = document.getElementById("whitelist-box");
  if (!box) return;
  if (!emails.length) {
    box.innerHTML = `<div style="color:#7f8c8d;">白名單目前為空。沒有新用戶可以註冊。</div>`;
    return;
  }
  const rows = emails.map(e => `
    <tr>
      <td style="padding:4px 8px;">${e}</td>
      <td style="padding:4px 8px; text-align:right;">
        <button onclick="removeWhitelistEmail('${e.replace(/'/g, "\\'")}')"
                style="padding:2px 10px; color:#c0392b;">移除</button>
      </td>
    </tr>`).join("");
  box.innerHTML = `
    <table style="width:100%; max-width:480px; border-collapse:collapse;">
      <thead><tr style="border-bottom:1px solid #ddd;">
        <th style="padding:4px 8px; text-align:left;">Email（共 ${emails.length} 筆）</th>
        <th></th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

window.addWhitelistEmail = async function () {
  const input = document.getElementById("whitelist-email-input");
  const email = (input.value || "").trim().toLowerCase();
  if (!email || !email.includes("@")) { alert("請輸入有效 email"); return; }
  try {
    const r = await authedFetch("/admin/email_whitelist/add", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email }),
    });
    if (!r.ok) {
      const e = await r.json().catch(() => ({}));
      alert("新增失敗：" + (e.detail || r.status));
      return;
    }
    input.value = "";
    loadWhitelist();
  } catch (e) { alert("新增失敗：" + e.message); }
};

window.removeWhitelistEmail = async function (email) {
  if (!confirm(`確定移除「${email}」？（不影響既有帳號，但該 email 之後無法再首次登入）`)) return;
  try {
    const r = await authedFetch("/admin/email_whitelist/remove", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email }),
    });
    if (!r.ok) {
      const e = await r.json().catch(() => ({}));
      alert("移除失敗：" + (e.detail || r.status));
      return;
    }
    loadWhitelist();
  } catch (e) { alert("移除失敗：" + e.message); }
};

// ── 排程執行紀錄（近 7 天）─────────────────────────────────────────────────
let _schedHistoryTimer = null;
async function loadSchedulerHistory() {
  try {
    const r = await authedFetch("/admin/scheduler/history?days=7");
    if (r.ok) {
      const data = await r.json();
      renderSchedulerHistory(data.items || []);
    }
  } catch (e) { console.error("scheduler history", e); }
  if (_schedHistoryTimer) clearTimeout(_schedHistoryTimer);
  _schedHistoryTimer = setTimeout(loadSchedulerHistory, 60000);
}

function renderSchedulerHistory(items) {
  const box = document.getElementById("scheduler-history-box");
  if (!box) return;
  if (!items.length) {
    box.innerHTML = `<div style="color:#7f8c8d;">近 7 天還沒有排程執行紀錄</div>`;
    return;
  }
  const fmt = iso => {
    if (!iso) return "—";
    try { return new Date(iso).toLocaleString("zh-TW"); } catch { return iso; }
  };
  const fmtDur = (a, b) => {
    if (!a || !b) return "—";
    try {
      const sec = Math.round((new Date(b) - new Date(a)) / 1000);
      if (sec < 60) return `${sec}s`;
      const m = Math.floor(sec / 60), s = sec % 60;
      return s ? `${m}m ${s}s` : `${m}m`;
    } catch { return "—"; }
  };
  let html = `<table style="width:100%; border-collapse:collapse;">
    <thead><tr style="border-bottom:2px solid #ddd;">
      <th style="padding:6px 8px; text-align:left;">開始</th>
      <th style="padding:6px 8px; text-align:left;">耗時</th>
      <th style="padding:6px 8px; text-align:right;">新增</th>
      <th style="padding:6px 8px; text-align:right;">補資料</th>
      <th style="padding:6px 8px; text-align:right;">重複捨棄</th>
      <th style="padding:6px 8px; text-align:right;">命令數</th>
      <th style="padding:6px 8px; text-align:left;">狀態</th>
      <th style="padding:6px 8px; text-align:left;">明細</th>
    </tr></thead><tbody>`;
  items.forEach((it, idx) => {
    const ok = it.status === "ok";
    const detailId = `sched-hist-detail-${idx}`;
    html += `<tr style="border-bottom:1px solid #eee;">
      <td style="padding:6px 8px;">${fmt(it.started_at)}</td>
      <td style="padding:6px 8px;">${fmtDur(it.started_at, it.finished_at)}</td>
      <td style="padding:6px 8px; text-align:right;"><b>${it.total_new ?? 0}</b></td>
      <td style="padding:6px 8px; text-align:right;">${it.total_enrich ?? 0}</td>
      <td style="padding:6px 8px; text-align:right;">${it.total_skip_dup ?? 0}</td>
      <td style="padding:6px 8px; text-align:right;">${(it.commands || []).length}</td>
      <td style="padding:6px 8px; color:${ok ? '#27ae60' : '#c0392b'};">
        ${ok ? '✓ 完成' : '✗ 失敗'}
      </td>
      <td style="padding:6px 8px;">
        <a href="javascript:void(0)" onclick="document.getElementById('${detailId}').classList.toggle('hidden')">展開/收合</a>
      </td>
    </tr>
    <tr id="${detailId}" class="hidden"><td colspan="8" style="padding:8px 16px; background:#fafafa;">
      ${(it.commands || []).map((c, i) => `
        <div style="margin:4px 0;">
          <b>命令 ${i + 1}</b>：${(c.districts || []).join("、")} × ${c.limit} 筆
          → 新增 ${c.new_count}，補資料 ${c.enrich_count}，重複 ${c.skip_dup_count}，價格變動 ${c.price_update_count}
          ${c.status === "fail" ? `<span style="color:#c0392b;"> 失敗：${c.error}</span>` : ""}
        </div>`).join("")}
    </td></tr>`;
  });
  html += "</tbody></table>";
  box.innerHTML = html;
}


// ── 定時 batch 排程 ─────────────────────────────────────────────────────────
let _schedulerTimer = null;
// UI 編輯中的 draft（還沒套用的）。每欄位標記 "已套用 / 未套用"
let _schedDraft = { interval_hr: 1, commands: [] };
let _schedServer = { interval_hr: 1, commands: [] };   // server 當前真實值（套用對照）
let _schedMeta = { allowed: [], maxCmds: 3, maxDistricts: 5, interSleep: 30, intervalOpts: [1,3,6,12,24] };

async function loadSchedulerStatus() {
  let running = false;
  try {
    const r = await authedFetch("/admin/scheduler/status");
    if (!r.ok) return;
    const s = await r.json();
    _schedMeta = {
      allowed: s.allowed_districts || [],
      maxCmds: s.max_commands || 3,
      maxDistricts: s.max_districts_per_command || 5,
      interSleep: s.inter_command_sleep_sec || 30,
      intervalOpts: s.allowed_interval_hr || [1, 3, 6, 12, 24],
    };
    _schedServer = {
      interval_hr: s.interval_hr,
      commands: JSON.parse(JSON.stringify(s.commands || [])),
    };
    if (!_schedDraft.commands.length) {
      _schedDraft = JSON.parse(JSON.stringify(_schedServer));
    }
    renderScheduler(s);
    running = !!s.currently_running;
    // 排程（或手動）batch 正在跑、且我們目前還沒掛 SSE → 自動接上進度
    if (running && !_sseActive) {
      _watchScrapeSSE({ sourceLabel: "排程 batch 執行中…" });
    }
  } catch (e) { console.error("scheduler status", e); }
  if (_schedulerTimer) clearTimeout(_schedulerTimer);
  // 正在跑時加快輪詢頻率（3 秒）好立刻偵測到開始；閒置時 30 秒即可
  _schedulerTimer = setTimeout(loadSchedulerStatus, running ? 3000 : 30000);
}

function _fmtDate(iso) { return iso ? new Date(iso).toLocaleString("zh-TW") : "—"; }
function _countdown(iso) {
  if (!iso) return "—";
  const ms = new Date(iso).getTime() - Date.now();
  if (ms <= 0) return "即將觸發";
  const min = Math.floor(ms / 60000);
  const sec = Math.floor((ms % 60000) / 1000);
  return `${min} 分 ${sec} 秒後`;
}

function _isCmdApplied(idx) {
  const d = _schedDraft.commands[idx];
  const s = _schedServer.commands[idx];
  if (!d && !s) return true;
  if (!d || !s) return false;
  return JSON.stringify(d.districts.slice().sort()) === JSON.stringify((s.districts || []).slice().sort())
      && Number(d.limit) === Number(s.limit);
}

function _isIntervalApplied() {
  return Number(_schedDraft.interval_hr) === Number(_schedServer.interval_hr);
}

function renderScheduler(s) {
  const box = document.getElementById("scheduler-box");
  if (!box) return;
  const running = s.currently_running;
  const enabled = s.enabled;
  const stateText = running ? "🟢 進行中"
                  : (enabled ? "🟦 待機中" : "⚪ 已停用");
  const stateColor = running ? "#27ae60" : (enabled ? "#2980b9" : "#95a5a6");

  // 啟用/停用按鈕（綠=已啟用，紅=已停用；點一下切換）
  const toggleBtn = enabled
    ? `<button onclick="toggleScheduler(false)" style="background:#27ae60; color:#fff; border:none; padding:6px 14px; border-radius:4px; cursor:pointer; font-weight:600;">● 已啟用</button>`
    : `<button onclick="toggleScheduler(true)"  style="background:#c0392b; color:#fff; border:none; padding:6px 14px; border-radius:4px; cursor:pointer; font-weight:600;">● 已停用</button>`;

  // 停用時不顯示倒數（避免誤導「關掉還會跑」）；運行中或待機才顯示
  const tickDiv = enabled
    ? `<div>下次 tick：<b>${_countdown(s.next_tick_at)}</b>（${_fmtDate(s.next_tick_at)}）</div>`
    : `<div style="color:#95a5a6;">下次 tick：— (排程已停用)</div>`;

  const headerRow = `
    <div style="display:flex; gap:14px; flex-wrap:wrap; align-items:center; margin-bottom:12px;">
      ${toggleBtn}
      <b style="color:${stateColor}">${stateText}</b>
      ${tickDiv}
      <div style="color:#555;">最近執行：${_fmtDate(s.last_run_at)} ${s.last_status ? `（${s.last_status}）` : ""}</div>
    </div>`;

  // interval row
  const intervalApplied = _isIntervalApplied();
  const intervalOpts = _schedMeta.intervalOpts.map(h =>
    `<option value="${h}" ${Number(_schedDraft.interval_hr) === h ? "selected" : ""}>${h} 小時</option>`
  ).join("");
  const intervalRow = `
    <div style="margin-bottom:14px;">
      每
      <select id="sched-interval" style="padding:3px 6px;"
              onchange="_schedDraft.interval_hr = parseInt(this.value)||1; _touchApplyBtns()">
        ${intervalOpts}
      </select>
      跑一次（於台北時區整點觸發）
      <button onclick="applySchedulerConfig()" style="margin-left:8px; padding:3px 12px;">套用</button>
      <span id="sched-interval-applied" style="margin-left:8px; color:${intervalApplied ? '#27ae60' : '#c0392b'}; font-size:12px;">
        ${intervalApplied ? `✓ 已套用：每 ${_schedServer.interval_hr} 小時` : "⚠ 未套用（按下套用才生效）"}
      </span>
    </div>`;

  // commands
  const cmdHtml = _schedDraft.commands.map((cmd, i) => _renderCmdRow(cmd, i)).join("");
  const addBtnHtml = _schedDraft.commands.length < _schedMeta.maxCmds
    ? `<button onclick="schedAddCmd()" style="padding:4px 14px;">+ 新增命令</button>`
    : "";

  box.innerHTML = headerRow + intervalRow +
    `<div style="font-weight:600; margin-bottom:6px;">命令清單（依序執行，兩命令之間休息 ${_schedMeta.interSleep} 秒）</div>` +
    cmdHtml +
    `<div style="margin-top:8px;">${addBtnHtml}</div>`;
}

function _renderCmdRow(cmd, i) {
  const applied = _isCmdApplied(i);
  const server = _schedServer.commands[i];
  const appliedText = applied
    ? `<span style="color:#27ae60; font-size:12px;">✓ 已套用：${(server.districts || []).join("、")} / ${server.limit} 筆</span>`
    : `<span style="color:#c0392b; font-size:12px;">⚠ 未套用（按下套用才生效）</span>`;
  const chips = _schedMeta.allowed.map(d => {
    const checked = cmd.districts.includes(d) ? "checked" : "";
    return `<label style="margin-right:8px;"><input type="checkbox" ${checked}
       onchange="schedToggleDist(${i}, '${d}', this.checked)"> ${d}</label>`;
  }).join("");
  return `
    <div style="border:1px solid #e5e5e5; padding:10px 12px; border-radius:5px; margin-bottom:8px;">
      <div style="display:flex; align-items:center; gap:10px; margin-bottom:6px;">
        <b>命令 ${i + 1}</b>
        ${_schedDraft.commands.length > 1 ? `<button onclick="schedRemoveCmd(${i})" style="margin-left:auto; padding:2px 8px; color:#c0392b;">刪除</button>` : ""}
      </div>
      <div style="margin-bottom:6px;">
        ${chips}
        <span style="color:#7f8c8d; font-size:12px; margin-left:4px;">
          （已選 ${cmd.districts.length}/${_schedMeta.maxDistricts}）
        </span>
      </div>
      <div>
        每次最多
        <input type="number" min="1" max="300" value="${cmd.limit}" style="width:70px; padding:3px 6px;"
               oninput="_schedDraft.commands[${i}].limit = parseInt(this.value)||30; _touchApplyBtns()">
        筆
        <button onclick="applySchedulerConfig()" style="margin-left:8px; padding:3px 12px;">套用</button>
        <span style="margin-left:8px;">${appliedText}</span>
      </div>
    </div>`;
}

// 每次 draft 改動只需更新「已套用/未套用」文字，不重畫整段（避免失去 input focus）
function _touchApplyBtns() {
  // 簡化：直接重畫（input blur/focus 不是大問題，只有短暫一次）
  loadSchedulerStatus();
}

window.schedToggleDist = function (idx, d, on) {
  const cmd = _schedDraft.commands[idx];
  if (!cmd) return;
  const set = new Set(cmd.districts);
  if (on) {
    if (set.size >= _schedMeta.maxDistricts) {
      alert(`每個命令最多選 ${_schedMeta.maxDistricts} 區`);
      // rollback checkbox
      renderScheduler({ enabled: document.querySelector("#scheduler-box button")?.textContent?.includes("已啟用"),
                        currently_running: false, next_tick_at: null, last_run_at: null, last_status: "" });
      loadSchedulerStatus();
      return;
    }
    set.add(d);
  } else {
    set.delete(d);
  }
  cmd.districts = [..._schedMeta.allowed.filter(x => set.has(x))];   // 保持順序
  _touchApplyBtns();
};

window.schedAddCmd = function () {
  if (_schedDraft.commands.length >= _schedMeta.maxCmds) return;
  _schedDraft.commands.push({ districts: [], limit: 30 });
  _touchApplyBtns();
};

window.schedRemoveCmd = function (idx) {
  if (_schedDraft.commands.length <= 1) {
    alert("至少需保留 1 個命令");
    return;
  }
  _schedDraft.commands.splice(idx, 1);
  applySchedulerConfig();   // 刪除要立即寫回（否則「套用」哪一個會混亂）
};

window.applySchedulerConfig = async function () {
  try {
    // 以 DOM 當下值為準（避免 _schedDraft 被 poll 覆蓋的隱性 race）
    const selEl = document.getElementById("sched-interval");
    const domInterval = selEl ? parseInt(selEl.value) : NaN;
    const intervalToSend = Number.isFinite(domInterval) && domInterval > 0
      ? domInterval
      : (parseInt(_schedDraft.interval_hr) || 1);
    _schedDraft.interval_hr = intervalToSend;
    const payload = {
      interval_hr: intervalToSend,
      commands: _schedDraft.commands,
    };
    console.log("[scheduler] 套用 payload", payload);
    const r = await authedFetch("/admin/scheduler/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!r.ok) {
      let msg = `HTTP ${r.status}`;
      try {
        const body = await r.json();
        // FastAPI 422 的 detail 是 list of validation errors；HTTPException 則是 string
        if (typeof body.detail === "string") {
          msg = body.detail;
        } else if (Array.isArray(body.detail)) {
          msg = body.detail.map(e => `${(e.loc || []).join(".")}: ${e.msg}`).join("; ");
        } else if (body.detail) {
          msg = JSON.stringify(body.detail);
        }
      } catch {}
      alert("套用失敗：" + msg);
      return;
    }
    loadSchedulerStatus();
  } catch (e) { alert("套用失敗：" + e.message); }
};

window.runOcrScan = async function () {
  const box = document.getElementById("ocr-scan-result");
  box.innerHTML = `<div style="color:#7f8c8d;">掃描中…（每筆要打 Google reverse geocode，全庫約 1-3 分鐘）</div>`;
  try {
    const r = await authedFetch("/admin/ocr_misread_scan");
    if (!r.ok) {
      const e = await r.json().catch(() => ({}));
      box.innerHTML = `<div style="color:#c0392b;">掃描失敗：${e.detail || r.status}</div>`;
      return;
    }
    const data = await r.json();
    const suspects = data.suspects || [];
    let html = `
      <div style="color:#555; font-size:13px; margin-bottom:8px;">
        檢查 ${data.checked} 筆 / 缺原生座標跳過 ${data.skipped_no_source_coords} 筆 /
        <b style="color:${suspects.length ? '#c0392b' : '#27ae60'}">疑似 ${suspects.length} 筆</b>
      </div>
      <div style="color:#7f8c8d; font-size:12px; margin-bottom:8px;">${data.note || ""}</div>
    `;
    if (!suspects.length) {
      html += `<div style="color:#27ae60;">✓ 沒發現疑似誤讀物件</div>`;
    } else {
      html += `<table style="width:100%; font-size:13px;"><thead><tr>
        <th style="text-align:left; padding:4px 8px;">ID</th>
        <th style="text-align:left; padding:4px 8px;">標題</th>
        <th style="text-align:left; padding:4px 8px;">DB 路名</th>
        <th style="text-align:left; padding:4px 8px;">原生座標反查</th>
        <th style="padding:4px 8px;">動作</th>
      </tr></thead><tbody>`;
      for (const s of suspects) {
        const title = (s.title || "").replace(/</g, "&lt;").slice(0, 30);
        html += `<tr>
          <td style="padding:4px 8px;">${s.id}</td>
          <td style="padding:4px 8px;">${title}</td>
          <td style="padding:4px 8px; color:#c0392b;">${s.db_road}</td>
          <td style="padding:4px 8px; color:#27ae60;">${s.source_reverse_road}</td>
          <td style="padding:4px 8px;">
            <button onclick="quickReanalyze('${s.id}')" style="padding:3px 8px;">重新分析</button>
          </td>
        </tr>`;
      }
      html += "</tbody></table>";
    }
    box.innerHTML = html;
  } catch (e) {
    box.innerHTML = `<div style="color:#c0392b;">掃描失敗：${e.message}</div>`;
  }
};

window.toggleScheduler = async function (on) {
  try {
    const r = await authedFetch("/admin/scheduler/toggle", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: !!on }),
    });
    if (!r.ok) { alert("切換失敗"); return; }
    loadSchedulerStatus();
  } catch (e) { alert("切換失敗：" + e.message); }
};

function renderStats(s) {
  const cards = [
    { num: s.total_properties, lbl: "中央物件總數" },
    { num: s.analysis_done, lbl: "已分析" },
    { num: s.analysis_error, lbl: "分析錯誤" },
    { num: s.total_users, lbl: "用戶數" },
  ];
  document.getElementById("stats-box").innerHTML = cards.map(c =>
    `<div class="stat-card"><div class="stat-num">${c.num ?? "—"}</div><div class="stat-lbl">${c.lbl}</div></div>`
  ).join("");
}

window.renderList = function () {
  const q = (document.getElementById("q").value || "").toLowerCase().trim();
  const city = document.getElementById("filter-city").value;
  const status = document.getElementById("filter-status").value;
  let list = _allDocs;
  if (city) list = list.filter(d => d.city === city);
  if (status) list = list.filter(d => (d.analysis_status || "") === status);
  if (q) list = list.filter(d =>
    (d.id || "").toLowerCase().includes(q) ||
    (d.address || "").toLowerCase().includes(q) ||
    (d.district || "").toLowerCase().includes(q)
  );

  document.getElementById("count").textContent = `顯示 ${list.length} / ${_allDocs.length}`;
  // 動態 thead：非 batch tab 多顯示「送件人」欄
  const showSubmitter = (_propTab === "user_url" || _propTab === "manual");
  const headCols = ["ID", "連結", "City", "District", "地址", "類型", "樓層", "總價", "分析狀態", "抓取時間"];
  if (showSubmitter) headCols.push("送件人");
  headCols.push("動作");
  document.getElementById("thead").innerHTML =
    `<tr>${headCols.map(h => `<th>${h}</th>`).join("")}</tr>`;
  const colspan = headCols.length;
  const tbody = document.getElementById("tbody");
  if (!list.length) {
    tbody.innerHTML = `<tr><td colspan="${colspan}" style="text-align:center;color:#999;padding:24px">無資料</td></tr>`;
    return;
  }
  const fmtScrapeTime = iso => {
    if (!iso) return "—";
    try {
      const d = new Date(iso);
      if (isNaN(d.getTime())) return "—";
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      const hh = String(d.getHours()).padStart(2, "0");
      const mm = String(d.getMinutes()).padStart(2, "0");
      return `${y}/${m}/${dd} ${hh}:${mm}`;
    } catch { return "—"; }
  };
  tbody.innerHTML = list.map(d => {
    const isReanalyzing = _reanalyzingIds.has(d.id);
    const stCls = d.analysis_status === "done" ? "status-done"
      : d.analysis_status === "error" ? "status-error"
      : "status-pending";
    const price = d.price_ntd ? `${Math.round(d.price_ntd / 10000)} 萬` : "—";
    const floors = d.total_floors ? `${d.floor || ""}/${d.total_floors}F` : (d.floor ? `${d.floor}F` : "—");
    const scrapedAt = fmtScrapeTime(d.scrape_session_at || d.scraped_at);
    let statusCell;
    if (isReanalyzing) {
      const startMs = _reanalyzeStart.get(d.id) || Date.now();
      const elapsed = Math.floor((Date.now() - startMs) / 1000);
      const EST = 45;
      const pct = Math.min(95, Math.round((elapsed / EST) * 95));
      statusCell = `<div class="mini-loading"><div class="mini-loading-bar" style="width:${pct}%"></div><span style="font-size:11px;color:#666">分析中… ${pct}% (${elapsed}s)</span></div>`;
    } else {
      statusCell = `<span class="${stCls}">${d.analysis_status || "—"}</span>`;
    }
    // 591 連結：每一個 url 都給編號連結，格式「591: 1 2 3 ...」
    const allUrls = [d.url, ...(d.url_alt || [])].filter(Boolean);
    const linkBadge = allUrls.length === 0
      ? '<span style="color:#999">—</span>'
      : '591: ' + allUrls.map((u, i) =>
          `<a href="${u}" target="_blank" style="color:#2980b9;margin-right:6px">${i+1}</a>`
        ).join("");
    const submitterCell = showSubmitter
      ? `<td style="font-size:12px">${d.submitted_by_email || d.submitted_by_uid || '—'}</td>`
      : '';
    return `<tr>
      <td><code>${d.id}</code></td>
      <td style="white-space:nowrap">${linkBadge}</td>
      <td>${d.city || "—"}</td>
      <td>${d.district || "—"}</td>
      <td>${_cleanAddrDisplay(d)}</td>
      <td>${d.building_type || "—"}</td>
      <td>${floors}</td>
      <td>${price}</td>
      <td>${statusCell}</td>
      <td style="white-space:nowrap;font-size:12px;color:#666">${scrapedAt}</td>
      ${submitterCell}
      <td class="row-actions">
        <button onclick="openDoc('${d.id}')">檢視</button>
        <button onclick="quickReanalyze('${d.id}')" ${isReanalyzing ? "disabled" : ""}>重新分析</button>
        <button class="btn-del" onclick="quickDelete('${d.id}')">刪除</button>
      </td>
    </tr>`;
  }).join("");
};

// 正在重新分析的 id 集合；轉畫為 loading 狀態
const _reanalyzingIds = new Set();
const _reanalyzeStart = new Map();   // id → startTimestampMs

// 地址顯示清理：去除 city / district 重複前綴（e.g. 「台北市中正區中正區羅斯福路...」→「羅斯福路...」）
function _cleanAddrDisplay(d) {
  let addr = d.address_inferred || d.address || "";
  if (!addr) return "—";
  // 依序去 city（可能有1-2次）和 district（可能有1-2次），因為舊資料有重複前綴
  const city = d.city || "";
  const dist = d.district || "";
  if (city) {
    addr = addr.replace(new RegExp(`^(${city})+`), "");
  } else {
    addr = addr.replace(/^(台北市|臺北市|新北市)+/, "");
  }
  if (dist) {
    addr = addr.replace(new RegExp(`^(${dist})+`), "");
  } else {
    addr = addr.replace(/^([\u4e00-\u9fa5]{1,3}區)+/, "");
  }
  const inferredTag = d.address_inferred ? ' <span style="font-size:11px;color:#1a8754">(推測)</span>' : '';
  return (addr || "—") + inferredTag;
}

let _debounceTimer;
window.debouncedRender = function () {
  clearTimeout(_debounceTimer);
  _debounceTimer = setTimeout(renderList, 200);
};

function renderUsers(users) {
  if (!users.length) {
    document.getElementById("users-box").innerHTML = "尚無用戶";
    return;
  }
  document.getElementById("users-box").innerHTML = `<table>
    <thead><tr><th>UID</th><th>Email</th><th>名稱</th><th>階級</th><th>建立時間</th><th>動作</th></tr></thead>
    <tbody>${users.map(u => {
      const tierTxt = u.tier_name_zh ? `${u.tier_name_zh}${u.tier_name_en ? ` / ${u.tier_name_en}` : ""}` : "—";
      return `
      <tr>
        <td><code style="font-size:11px">${u.uid}</code></td>
        <td>${u.email || "—"}</td>
        <td>${u.display_name || "—"}</td>
        <td>${tierTxt}</td>
        <td>${u.created_at ? new Date(u.created_at).toLocaleString() : "—"}</td>
        <td class="row-actions">
          <button class="btn-del" onclick="deleteUserData('${u.uid}','${(u.email || '').replace(/'/g, '&apos;')}')">刪除此用戶資料</button>
        </td>
      </tr>`;
    }).join("")}
    </tbody></table>`;
}

window.deleteUserData = async function (uid, email) {
  const typed = prompt(
    `⚠ 將永久刪除用戶「${email || uid}」的所有私人資料：\n` +
    `  • watchlist（我的清單）\n` +
    `  • manual（私人物件）\n` +
    `  • bookmarks（書籤）\n` +
    `  • profile\n\n` +
    `中央 DB（properties）不受影響。\n` +
    `請輸入 DELETE 確認：`
  );
  if (typed !== "DELETE") {
    if (typed !== null) alert("未輸入 DELETE，已取消。");
    return;
  }
  try {
    const r = await authedFetch(`/admin/users/${encodeURIComponent(uid)}`, { method: "DELETE" });
    const data = await r.json();
    if (r.ok) {
      alert(`已刪除：${JSON.stringify(data.deleted)}`);
      loadAll();
    } else {
      alert("刪除失敗：" + (data.detail || r.status));
    }
  } catch (e) {
    alert("刪除失敗：" + e.message);
  }
};

window.purgeNonApartments = async function () {
  const typed = prompt(
    "⚠ 將永久刪除中央 DB 所有非公寓物件：\n" +
    "  building_type 含：大樓 / 透天 / 店面 / 華廈 / 辦公\n" +
    "  或 total_floors ≥ 11\n\n" +
    "請輸入 DELETE 確認："
  );
  if (typed !== "DELETE") {
    if (typed !== null) alert("未輸入 DELETE，已取消。");
    return;
  }
  try {
    const r = await authedFetch("/admin/purge_non_apartments", { method: "POST" });
    const data = await r.json();
    if (r.ok) {
      alert(`已清除 ${data.deleted_count} 筆非公寓物件`);
      loadAll();
    } else {
      alert("清除失敗：" + (data.detail || r.status));
    }
  } catch (e) {
    alert("清除失敗：" + e.message);
  }
};

window.openDoc = async function (id) {
  document.getElementById("modal").classList.remove("hidden");
  document.getElementById("modal-title").textContent = id;
  document.getElementById("modal-body").textContent = "loading...";
  const r = await authedFetch(`/admin/properties/${encodeURIComponent(id)}`);
  const d = await r.json();
  document.getElementById("modal-body").textContent = JSON.stringify(d, null, 2);
  document.getElementById("btn-delete").onclick = () => deleteDoc(id);
  document.getElementById("btn-reanalyze").onclick = () => reanalyzeDoc(id);
};

window.closeModal = function () {
  document.getElementById("modal").classList.add("hidden");
};

async function deleteDoc(id) {
  if (!confirm(`確定從中央 DB 永久刪除 ${id}？\n此動作無法復原。`)) return;
  const r = await authedFetch(`/admin/properties/${encodeURIComponent(id)}`, { method: "DELETE" });
  if (r.ok) {
    alert("已刪除");
    closeModal();
    loadAll();
  } else {
    const e = await r.json().catch(() => ({}));
    alert("刪除失敗：" + (e.detail || r.status));
  }
}

window.quickDelete = async function (id) {
  if (!confirm(`確定從中央 DB 永久刪除 ${id}？`)) return;
  const r = await authedFetch(`/admin/properties/${encodeURIComponent(id)}`, { method: "DELETE" });
  if (r.ok) { loadAll(); } else { alert("刪除失敗"); }
};

window.quickReanalyze = async function (id) {
  if (!confirm(`重新分析 ${id}？（會覆寫資料）`)) return;
  // manual 物件：走 admin manual reanalyze（需 uid），不進中央 properties
  let url;
  if (_propTab === "manual" || String(id).startsWith("manual_")) {
    const doc = _allDocs.find(d => d.id === id);
    const uid = doc && doc.submitted_by_uid;
    if (!uid) { alert("找不到物件所屬 uid"); return; }
    url = `/admin/manual/${encodeURIComponent(uid)}/${encodeURIComponent(id)}/reanalyze`;
  } else {
    url = `/admin/properties/${encodeURIComponent(id)}/reanalyze`;
  }
  const r = await authedFetch(url, { method: "POST" });
  if (!r.ok) {
    let detail = "";
    try { detail = (await r.json()).detail || ""; } catch {}
    alert("啟動失敗" + (detail ? "：" + detail : ""));
    return;
  }
  _reanalyzingIds.add(id);
  _reanalyzeStart.set(id, Date.now());
  // 每 0.5 秒更新進度顯示（純前端 tick，不打 API）
  const EST_MS = 45000;  // 經驗值：單筆 admin 重新分析約 30-60s
  const tickUI = () => {
    if (!_reanalyzingIds.has(id)) return;
    renderList();
    setTimeout(tickUI, 500);
  };
  tickUI();
  // 每 1.5 秒輪詢真實狀態；manual 走 admin manual 讀取 endpoint
  const isManual = _propTab === "manual" || String(id).startsWith("manual_");
  const pollDoc = _allDocs.find(d => d.id === id);
  const pollUid = pollDoc && pollDoc.submitted_by_uid;
  const pollUrl = isManual && pollUid
    ? `/admin/manual/${encodeURIComponent(pollUid)}/${encodeURIComponent(id)}`
    : `/admin/properties/${encodeURIComponent(id)}`;
  const poll = async () => {
    if (!_reanalyzingIds.has(id)) return;
    try {
      const rr = await authedFetch(pollUrl);
      if (rr.ok) {
        const doc = await rr.json();
        if (!doc.analysis_in_progress) {
          _reanalyzingIds.delete(id);
          _reanalyzeStart.delete(id);
          const idx = _allDocs.findIndex(x => x.id === id);
          if (idx >= 0) Object.assign(_allDocs[idx], doc);
          renderList();
          return;
        }
      }
    } catch (e) { console.error("poll error", e); }
    setTimeout(poll, 1500);
  };
  setTimeout(poll, 1500);
};

async function reanalyzeDoc(id) {
  if (!confirm(`重新分析 ${id}？（會覆寫中央資料）`)) return;
  const r = await authedFetch(`/admin/properties/${encodeURIComponent(id)}/reanalyze`, { method: "POST" });
  if (r.ok) { alert("已啟動重新分析"); closeModal(); }
  else { alert("啟動失敗"); }
}

// 全域：當前是否有 SSE watcher 在跑（避免排程自動開 + 手動按鈕同時開兩條）
let _sseActive = false;

async function _watchScrapeSSE({ sourceLabel = "" } = {}) {
  if (_sseActive) return;
  _sseActive = true;
  const btn = document.getElementById("btn-admin-scrape");
  if (btn) btn.disabled = true;
  document.getElementById("scrape-progress").classList.remove("hidden");
  if (sourceLabel) {
    document.getElementById("scrape-msg").textContent = sourceLabel;
  }
  const token = await _user.getIdToken();
  try {
    const streamResp = await fetch("/api/scrape/status", {
      headers: { Authorization: "Bearer " + token },
    });
    const reader = streamResp.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split("\n\n");
      buf = lines.pop();
      for (const block of lines) {
        const m = block.match(/^data:\s*(.+)$/m);
        if (!m) continue;
        try {
          const ev = JSON.parse(m[1]);
          if (typeof ev.percent === "number") {
            document.getElementById("scrape-bar-fill").style.width = ev.percent + "%";
          }
          if (ev.msg && ev.msg !== "heartbeat") {
            document.getElementById("scrape-msg").textContent = ev.msg;
            const log = document.getElementById("scrape-log");
            log.textContent += ev.msg + "\n";
            log.scrollTop = log.scrollHeight;
          }
          if (ev.done || ev.error) {
            if (btn) btn.disabled = false;
            _sseActive = false;
            loadAll();
            return;
          }
        } catch {}
      }
    }
  } catch (e) {
    console.error("SSE error:", e);
  }
  if (btn) btn.disabled = false;
  _sseActive = false;
  loadAll();
}

window.adminTriggerScrape = async function () {
  const districts = [...document.querySelectorAll(".scrape-dist:checked")].map(c => c.value);
  if (!districts.length) { alert("請至少勾一個行政區"); return; }
  const limit = parseInt(document.getElementById("scrape-limit").value) || 30;
  const body = {
    districts,
    limit,
    max_floors: parseInt(document.getElementById("thresh-floors").value) || null,
  };
  document.getElementById("scrape-log").textContent = "";
  document.getElementById("scrape-msg").textContent = "送出中…";
  document.getElementById("scrape-bar-fill").style.width = "0%";
  const r = await authedFetch("/api/scrape", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await r.json();
  if (data.status !== "started") {
    alert(data.message || "啟動失敗");
    return;
  }
  _watchScrapeSSE({ sourceLabel: "抓取中…" });
};

window.clearCentralDb = async function () {
  const typed = prompt(
    "⚠ 這會永久刪除中央 DB 所有 properties（分析快取），不影響任何用戶的 watchlist / manual。\n\n" +
    "確定要清空？請輸入 DELETE 確認："
  );
  if (typed !== "DELETE") {
    if (typed !== null) alert("未輸入 DELETE，已取消。");
    return;
  }
  try {
    const r = await authedFetch("/api/clear_db", { method: "POST" });
    const data = await r.json();
    if (r.ok) {
      alert(`已清空中央 DB（刪除 ${data.deleted ?? 0} 筆）`);
      loadAll();
    } else {
      alert("清空失敗：" + (data.detail || r.status));
    }
  } catch (e) {
    alert("清空失敗：" + e.message);
  }
};

boot().catch(e => {
  document.getElementById("login-err").textContent = "初始化失敗：" + e.message;
});
