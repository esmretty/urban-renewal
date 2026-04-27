// Admin Portal 前端：Google 登入 + 驗證 admin email + CRUD UI

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { getAuth, GoogleAuthProvider, signInWithPopup, signOut, onAuthStateChanged }
  from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";

// HTML escape helper：任何「會進 innerHTML 的後端/用戶文字」都要先 esc()。
function esc(s) {
  if (s === null || s === undefined) return "";
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

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
      const provider = new GoogleAuthProvider();
      provider.setCustomParameters({ prompt: "select_account" });
      await signInWithPopup(auth, provider);
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
  // 一律 no-store：admin 介面要看 live 真實狀態，瀏覽器不該 cache GET response
  return fetch(url, { ...init, headers, cache: "no-store" });
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
  loadMaintenance();
  loadRetryQueue();
  loadRunLogs();
  // 頁面載入時若 verify-alive 還在跑 → 自動恢復 polling 顯示進度
  _resumeVerifyAlivePollIfRunning();
};

async function _resumeVerifyAlivePollIfRunning() {
  try {
    const r = await authedFetch("/admin/verify_alive/progress");
    if (!r.ok) return;
    const p = await r.json();
    if (p.running) {
      _stopVerifyAlivePoll();
      _verifyAlivePollTimer = setInterval(_pollVerifyAliveOnce, 2000);
      _pollVerifyAliveOnce();
    }
  } catch {}
}

// ── 操作紀錄（手動 + scheduler 每次 action 都記錄到物件層級）──────────
window.loadRunLogs = async function () {
  const box = document.getElementById("runlog-box");
  const countEl = document.getElementById("runlog-count");
  if (!box) return;
  const filter = document.getElementById("runlog-filter")?.value || "";
  const limit = document.getElementById("runlog-limit")?.value || 200;
  try {
    const url = `/admin/run-logs?limit=${limit}` + (filter ? `&trigger_prefix=${encodeURIComponent(filter)}` : "");
    const r = await authedFetch(url);
    if (!r.ok) {
      box.innerHTML = `<div style="color:#c0392b;">載入失敗：HTTP ${r.status}</div>`;
      return;
    }
    const data = await r.json();
    if (countEl) countEl.textContent = `共 ${data.count} 筆`;
    if (!data.items.length) {
      box.innerHTML = `<div style="color:#888; padding:8px;">無紀錄</div>`;
      return;
    }
    const ACTION_COLOR = {
      batch_start: "#2980b9", batch_end: "#27ae60",
      new: "#27ae60", enrich: "#c78a00", dup_merge: "#7f8c8d",
      replacement: "#e67e22", cross_source: "#9b59b6",
      verify_alive_start: "#2980b9", verify_alive_end: "#27ae60",
      verify_alive_archive: "#c0392b",
      retry_attempt: "#c78a00", error: "#c0392b",
    };
    const ACTION_LABEL = {
      batch_start: "🚀 開始", batch_end: "🏁 結束",
      new: "✓ 新", enrich: "↻ 補", dup_merge: "× 合併",
      replacement: "🔁 換物件", cross_source: "🔗 跨來源",
      verify_alive_start: "🔍 開始驗活", verify_alive_end: "✓ 完成驗活",
      verify_alive_archive: "📦 archive",
      retry_attempt: "♻ 重試", error: "❌ 錯誤",
    };
    box.innerHTML = `<table style="width:100%; border-collapse:collapse;">
      <thead><tr style="background:#f0ece0; text-align:left;">
        <th style="padding:4px 6px; width:130px;">時間</th>
        <th style="padding:4px 6px; width:140px;">trigger</th>
        <th style="padding:4px 6px; width:90px;">action</th>
        <th style="padding:4px 6px; width:140px;">物件</th>
        <th style="padding:4px 6px;">訊息</th>
      </tr></thead>
      <tbody>${data.items.map(it => {
        const t = it.at ? new Date(it.at).toLocaleString("zh-TW", { hour12: false }) : "—";
        const acolor = ACTION_COLOR[it.action] || "#555";
        const alabel = ACTION_LABEL[it.action] || it.action;
        const sid = it.source_id ? esc(it.source_id) : "";
        const did = it.doc_id ? `<br><span style="color:#888; font-size:10px">${esc(it.doc_id.slice(0, 14))}</span>` : "";
        return `<tr style="border-bottom:1px solid #eee;">
          <td style="padding:3px 6px; color:#666; font-family:Consolas,monospace; font-size:11px">${esc(t)}</td>
          <td style="padding:3px 6px; color:#555">${esc(it.trigger || "")}</td>
          <td style="padding:3px 6px; color:${acolor}; font-weight:600">${esc(alabel)}</td>
          <td style="padding:3px 6px; font-family:Consolas,monospace; font-size:11px">${sid}${did}</td>
          <td style="padding:3px 6px;">${esc(it.message || "")}</td>
        </tr>`;
      }).join("")}</tbody>
    </table>`;
  } catch (e) {
    box.innerHTML = `<div style="color:#c0392b;">載入失敗：${esc(e.message)}</div>`;
  }
};


// ── 失敗重試佇列 ──────────────────────────────────────────────────────
async function loadRetryQueue() {
  const box = document.getElementById("retry-queue-box");
  const countEl = document.getElementById("retry-queue-count");
  if (!box) return;
  try {
    const r = await authedFetch("/admin/retry_queue");
    if (!r.ok) { box.innerHTML = `<div style="color:#c0392b;">載入失敗 (${r.status})</div>`; return; }
    const data = await r.json();
    renderRetryQueue(data.items || []);
    if (countEl) {
      const pending = (data.items || []).filter(i => i.status === "pending").length;
      const abandoned = (data.items || []).filter(i => i.status === "abandoned").length;
      countEl.textContent = pending || abandoned ? `(待重試 ${pending} 筆${abandoned ? `, 已放棄 ${abandoned} 筆` : ''})` : "";
    }
  } catch (e) {
    box.innerHTML = `<div style="color:#c0392b;">載入失敗：${esc(e.message)}</div>`;
  }
}

function renderRetryQueue(items) {
  const box = document.getElementById("retry-queue-box");
  if (!items.length) {
    box.innerHTML = '<div style="color:#27ae60; padding:8px;">✓ 重試佇列為空（沒有失敗物件待重抓）</div>';
    return;
  }
  const fmt = iso => {
    if (!iso) return "—";
    try {
      const d = new Date(iso);
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      const hh = String(d.getHours()).padStart(2, "0");
      const mm = String(d.getMinutes()).padStart(2, "0");
      return `${m}/${dd} ${hh}:${mm}`;
    } catch { return iso; }
  };
  const fmtRetryAt = iso => {
    if (!iso) return "—";
    const d = new Date(iso);
    const diffMin = Math.round((d.getTime() - Date.now()) / 60000);
    if (diffMin < -1) return `<span style="color:#c0392b">已過時 ${Math.abs(diffMin)} 分</span>`;
    if (diffMin <= 0) return `<span style="color:#c78a00">即將執行</span>`;
    if (diffMin < 60) return `<span style="color:#666">${diffMin} 分後</span>`;
    return `<span style="color:#888">${Math.round(diffMin / 60)} 小時後</span>`;
  };
  const rows = items.map(it => {
    const isAbandoned = it.status === "abandoned";
    const statusBadge = isAbandoned
      ? '<span style="background:#7f8c8d; color:#fff; padding:1px 6px; border-radius:3px; font-size:11px;">已放棄</span>'
      : '<span style="background:#c78a00; color:#fff; padding:1px 6px; border-radius:3px; font-size:11px;">待重試</span>';
    const srcBadge = `<span style="background:${it.source==='永慶'?'#00837f':'#ffa726'}; color:#fff; padding:1px 6px; border-radius:3px; font-size:11px;">${esc(it.source||'?')}</span>`;
    return `<tr>
      <td>${statusBadge}</td>
      <td>${srcBadge}</td>
      <td><a href="${esc(it.url||'')}" target="_blank" rel="noopener noreferrer" style="color:#2980b9; font-family:monospace; font-size:12px;">${esc(it.source_id||'?')}</a></td>
      <td style="font-size:12px; color:#666;">第 ${esc(String(it.attempts||1))} 次</td>
      <td style="font-size:12px;">${isAbandoned ? '—' : fmtRetryAt(it.retry_at)}</td>
      <td style="font-size:12px; color:#666;">${esc(fmt(it.first_failed_at))}</td>
      <td style="font-size:11px; color:#c0392b; max-width:280px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="${esc(it.last_error||'')}">${esc((it.last_error||'').slice(0,60))}</td>
      <td style="white-space:nowrap;">
        ${!isAbandoned ? `<button onclick="retryQueueRunNow('${esc(it._id)}')" style="padding:2px 8px; font-size:12px;">立即重試</button>` : ''}
        <button onclick="retryQueueRemove('${esc(it._id)}')" style="padding:2px 8px; font-size:12px; color:#c0392b;">移除</button>
      </td>
    </tr>`;
  }).join("");
  box.innerHTML = `<table style="width:100%; border-collapse:collapse;">
    <thead>
      <tr style="background:#f8f9fa; border-bottom:1px solid #ddd;">
        <th style="padding:6px 8px; text-align:left; font-size:12px;">狀態</th>
        <th style="padding:6px 8px; text-align:left; font-size:12px;">來源</th>
        <th style="padding:6px 8px; text-align:left; font-size:12px;">物件 ID</th>
        <th style="padding:6px 8px; text-align:left; font-size:12px;">嘗試</th>
        <th style="padding:6px 8px; text-align:left; font-size:12px;">下次重試</th>
        <th style="padding:6px 8px; text-align:left; font-size:12px;">首次失敗</th>
        <th style="padding:6px 8px; text-align:left; font-size:12px;">錯誤</th>
        <th style="padding:6px 8px; text-align:left; font-size:12px;">動作</th>
      </tr>
    </thead>
    <tbody>${rows}</tbody>
  </table>`;
}

window.retryQueueRunNow = async function (queueId) {
  if (!confirm("立即重試？")) return;
  try {
    const r = await authedFetch(`/admin/retry_queue/${encodeURIComponent(queueId)}/run-now`, { method: "POST" });
    if (!r.ok) { alert("觸發失敗 (" + r.status + ")"); return; }
    alert("已啟動，10~30 秒後完成");
    setTimeout(loadRetryQueue, 15000);
  } catch (e) { alert("失敗：" + e.message); }
};

window.retryQueueRemove = async function (queueId) {
  if (!confirm("從佇列移除（不再重試）？")) return;
  try {
    const r = await authedFetch(`/admin/retry_queue/${encodeURIComponent(queueId)}`, { method: "DELETE" });
    if (!r.ok) { alert("移除失敗 (" + r.status + ")"); return; }
    loadRetryQueue();
  } catch (e) { alert("失敗：" + e.message); }
};

window.retryQueueClearAll = async function () {
  if (!confirm("⚠ 確定全部移除？\n會清空整個重試佇列，待重試 + 已放棄全部刪掉，無法復原。")) return;
  try {
    const r = await authedFetch("/admin/retry_queue", { method: "DELETE" });
    if (!r.ok) { alert("清空失敗 (" + r.status + ")"); return; }
    const data = await r.json();
    alert(`已清空 ${data.deleted ?? 0} 筆`);
    loadRetryQueue();
  } catch (e) { alert("失敗：" + e.message); }
};


// ── 網站維護模式 ─────────────────────────────────────────────────────────────
async function loadMaintenance() {
  const badge = document.getElementById("maint-status-badge");
  const meta = document.getElementById("maint-status-meta");
  const msgInput = document.getElementById("maint-message-input");
  const btn = document.getElementById("maint-toggle-btn");
  if (!badge) return;
  try {
    const r = await authedFetch("/admin/maintenance");
    if (!r.ok) { badge.textContent = `載入失敗 (${r.status})`; badge.style.color = "#c0392b"; return; }
    const data = await r.json();
    const enabled = !!data.enabled;
    badge.textContent = enabled ? "🔧 維護中" : "✓ 正常運作";
    badge.style.color = enabled ? "#c78a00" : "#27ae60";
    if (meta) {
      const parts = [];
      if (data.updated_at) parts.push(`更新於 ${esc(data.updated_at).slice(0, 16).replace("T", " ")}`);
      if (data.updated_by_email) parts.push(`by ${esc(data.updated_by_email)}`);
      meta.textContent = parts.join(" · ");
    }
    if (msgInput && document.activeElement !== msgInput) {
      msgInput.value = data.message || "";
    }
    if (btn) {
      btn.textContent = enabled ? "關閉維護模式" : "啟用維護模式";
      btn.style.background = enabled ? "#27ae60" : "#c78a00";
    }
  } catch (e) {
    badge.textContent = `載入失敗：${e.message}`;
    badge.style.color = "#c0392b";
  }
}

window.toggleMaintenance = async function () {
  const msgInput = document.getElementById("maint-message-input");
  const badge = document.getElementById("maint-status-badge");
  const currentlyEnabled = badge && badge.textContent.includes("維護中");
  const willEnable = !currentlyEnabled;
  const message = msgInput ? msgInput.value.trim() : "";
  const verb = willEnable ? "啟用" : "關閉";
  if (!confirm(`確定要${verb}維護模式嗎？\n\n${willEnable ? "非 admin 用戶將被導向「系統維護中」頁面。" : "用戶將恢復正常存取首頁。"}`)) return;
  try {
    const r = await authedFetch("/admin/maintenance", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: willEnable, message }),
    });
    if (!r.ok) { alert(`${verb}失敗 (${r.status})`); return; }
    await loadMaintenance();
  } catch (e) {
    alert(`${verb}失敗：${e.message}`);
  }
};

window.saveMaintenanceMessage = async function () {
  const msgInput = document.getElementById("maint-message-input");
  const badge = document.getElementById("maint-status-badge");
  const currentlyEnabled = badge && badge.textContent.includes("維護中");
  const message = msgInput ? msgInput.value.trim() : "";
  try {
    const r = await authedFetch("/admin/maintenance", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: currentlyEnabled, message }),
    });
    if (!r.ok) { alert(`更新失敗 (${r.status})`); return; }
    await loadMaintenance();
    alert("訊息已更新");
  } catch (e) {
    alert(`更新失敗：${e.message}`);
  }
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
    box.innerHTML = `<div style="color:#c0392b;">載入失敗：${esc(e.message)}</div>`;
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
      <td style="padding:4px 8px;">${esc(e)}</td>
      <td style="padding:4px 8px; text-align:right;">
        <button onclick="removeWhitelistEmail('${esc(e.replace(/'/g, "\\'"))}')"
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
      <td style="padding:6px 8px;">${esc(fmt(it.started_at))}</td>
      <td style="padding:6px 8px;">${esc(fmtDur(it.started_at, it.finished_at))}</td>
      <td style="padding:6px 8px; text-align:right;"><b>${Number(it.total_new ?? 0)}</b></td>
      <td style="padding:6px 8px; text-align:right;">${Number(it.total_enrich ?? 0)}</td>
      <td style="padding:6px 8px; text-align:right;">${Number(it.total_skip_dup ?? 0)}</td>
      <td style="padding:6px 8px; text-align:right;">${(it.commands || []).length}</td>
      <td style="padding:6px 8px; color:${ok ? '#27ae60' : '#c0392b'};">
        ${ok ? '✓ 完成' : '✗ 失敗'}
      </td>
      <td style="padding:6px 8px;">
        <a href="javascript:void(0)" onclick="document.getElementById('${esc(detailId)}').classList.toggle('hidden')">展開/收合</a>
      </td>
    </tr>
    <tr id="${esc(detailId)}" class="hidden"><td colspan="8" style="padding:8px 16px; background:#fafafa;">
      ${(it.commands || []).map((c, i) => `
        <div style="margin:4px 0;">
          <b>命令 ${i + 1}</b>：${esc((c.districts || []).join("、"))} × ${Number(c.limit)} 筆
          → 新增 ${Number(c.new_count)}，補資料 ${Number(c.enrich_count)}，重複 ${Number(c.skip_dup_count)}，價格變動 ${Number(c.price_update_count)}
          ${c.status === "fail" ? `<span style="color:#c0392b;"> 失敗：${esc(c.error || '')}</span>` : ""}
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
let _schedMeta = { allowed: [], maxCmds: 5, maxDistricts: 5, interSleep: 30, intervalOpts: [1,3,6,12,24], verifyIntervalOpts: [12,24,72,360] };
let _lastVerifyAliveAt = null;

async function loadSchedulerStatus() {
  let running = false;
  try {
    const r = await authedFetch("/admin/scheduler/status");
    if (!r.ok) return;
    const s = await r.json();
    _schedMeta = {
      allowed: s.allowed_districts || [],
      maxCmds: s.max_commands || 5,
      maxDistricts: s.max_districts_per_command || 5,
      interSleep: s.inter_command_sleep_sec || 30,
      intervalOpts: s.allowed_interval_hr || [1, 3, 6, 12, 24],
      verifyIntervalOpts: s.allowed_verify_interval_hr || [12, 24, 72, 360],
    };
    _lastVerifyAliveAt = s.last_verify_alive_at || null;
    // 觸發 dashboard 警告檢查
    if (typeof refreshVerifyAliveWarning === "function") refreshVerifyAliveWarning();
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

function _normSources(cmd) {
  // 回傳排序後的 sources 陣列（固定順序）；舊資料 cmd.source 一律轉成 [source]
  const ORDER = ["591", "yongqing", "sinyi"];
  let arr = Array.isArray(cmd.sources) ? cmd.sources.slice()
           : (cmd.source ? [cmd.source] : ["591"]);
  arr = arr.filter(s => ORDER.includes(s));
  return ORDER.filter(s => arr.includes(s));
}

function _isCmdApplied(idx) {
  const d = _schedDraft.commands[idx];
  const s = _schedServer.commands[idx];
  if (!d && !s) return true;
  if (!d || !s) return false;
  return JSON.stringify((d.districts||[]).slice().sort()) === JSON.stringify((s.districts || []).slice().sort())
      && Number(d.limit) === Number(s.limit)
      && JSON.stringify(_normSources(d)) === JSON.stringify(_normSources(s));
}

function _isCmdAppliedNew(draft, server) {
  // 比對 draft 跟 server 是否完全一樣（含 type 和 interval_hr）
  if (!draft && !server) return true;
  if (!draft || !server) return false;
  const dType = draft.type || "scan";
  const sType = server.type || "scan";
  if (dType !== sType) return false;
  if (Number(draft.interval_hr || 0) !== Number(server.interval_hr || 0)) return false;
  if (dType === "verify_alive") return true;
  // scan 比對 districts / limit / sources
  return JSON.stringify((draft.districts||[]).slice().sort()) === JSON.stringify((server.districts || []).slice().sort())
      && Number(draft.limit) === Number(server.limit)
      && JSON.stringify(_normSources(draft)) === JSON.stringify(_normSources(server));
}

function _isIntervalApplied() {
  return Number(_schedDraft.interval_hr) === Number(_schedServer.interval_hr);
}

function renderScheduler(s) {
  // 兩個 tab 各自 render 自己的 cmds
  _renderSchedulerByType(s, "scan", document.getElementById("scheduler-box-scan"));
  _renderSchedulerByType(s, "verify_alive", document.getElementById("scheduler-box-verify"));
}

function _renderSchedulerByType(s, filterType, box) {
  if (!box) return;
  const running = s.currently_running;
  // per-type enabled（後端回；舊 client 也仍能看 s.enabled）
  const enabled = filterType === "scan"
    ? (s.scan_enabled !== undefined ? s.scan_enabled : s.enabled)
    : (s.verify_alive_enabled !== undefined ? s.verify_alive_enabled : s.enabled);
  const stateText = running ? "🟢 進行中"
                  : (enabled ? "🟦 待機中" : "⚪ 已停用");
  const stateColor = running ? "#27ae60" : (enabled ? "#2980b9" : "#95a5a6");

  const toggleBtn = enabled
    ? `<button onclick="toggleScheduler(false, '${filterType}')" class="sched-toggle on">● 已啟用</button>`
    : `<button onclick="toggleScheduler(true, '${filterType}')"  class="sched-toggle off">● 已停用</button>`;

  const headerRow = `
    <div class="sched-header-row">
      ${toggleBtn}
      <b style="color:${stateColor}">${stateText}</b>
    </div>`;

  // 過濾出該 tab 的命令（記住原始 idx 用於套用 / 刪除）
  const filteredCmds = [];
  _schedDraft.commands.forEach((cmd, i) => {
    const t = cmd.type || "scan";
    if (t === filterType) filteredCmds.push({ cmd, origIdx: i });
  });

  const cmdHtml = filteredCmds.length
    ? filteredCmds.map(({ cmd, origIdx }, dispIdx) => _renderCmdRow(cmd, origIdx, dispIdx + 1)).join("")
    : `<div style="color:#888; padding:8px; text-align:center;">尚無此類型命令</div>`;

  const addLabel = filterType === "scan" ? "+ 新增掃描命令" : "+ 新增偵測下架命令";
  const addBtn = `<button onclick="schedAddCmd('${filterType}')" style="padding:4px 12px;">${addLabel}</button>`;

  box.innerHTML = headerRow + cmdHtml + `<div style="margin-top:8px;">${addBtn}</div>`;
}

// poll verify-alive 進度（live）
let _verifyAlivePollTimer = null;
function _stopVerifyAlivePoll() {
  if (_verifyAlivePollTimer) { clearInterval(_verifyAlivePollTimer); _verifyAlivePollTimer = null; }
}

async function _pollVerifyAliveOnce() {
  const statusEl = document.getElementById("verify-alive-status");
  if (!statusEl) return;
  try {
    const r = await authedFetch("/admin/verify_alive/progress");
    if (!r.ok) return;
    const p = await r.json();
    if (!p.total && !p.running) return;
    const pct = p.total ? Math.round((p.current / p.total) * 100) : 0;
    const archivedList = (p.archived_items || []).slice(-10).reverse();
    const listHTML = archivedList.length
      ? `<div style="margin-top:6px; max-height:160px; overflow-y:auto; font-size:11px; background:#fff; padding:4px 6px; border-radius:3px;">
           <b>最近 archive：</b>
           ${archivedList.map(a => `<div style="border-bottom:1px solid #f0e0e0; padding:2px 0;">
             📦 ${esc(a.source_id || "")} <span style="color:#888">${esc((a.address || "").slice(0, 30))}</span>
           </div>`).join("")}
         </div>`
      : "";
    const stateIcon = p.running ? "⏳" : (p.error ? "❌" : "✅");
    const stateText = p.running ? "進行中" : (p.error ? `失敗：${esc(p.error)}` : "完成");
    statusEl.innerHTML = `
      <div style="margin-bottom:4px;">${stateIcon} ${stateText}：${p.current || 0}/${p.total || 0}（${pct}%），已 archive <b style="color:#c0392b">${p.archived_count || 0}</b> 筆，跳過 ${p.skipped || 0} 筆</div>
      <div style="background:#e8e4d8; height:8px; border-radius:4px; overflow:hidden;">
        <div style="background:#1e88e5; height:100%; width:${pct}%; transition:width 0.3s;"></div>
      </div>
      ${listHTML}
    `;
    if (!p.running) {
      _stopVerifyAlivePoll();
      // 完成後 refresh 物件列表 + 警告
      setTimeout(() => {
        if (typeof refreshVerifyAliveWarning === "function") refreshVerifyAliveWarning();
        if (typeof loadAll === "function") loadAll();
      }, 800);
    }
  } catch (e) {
    console.warn("[verify-alive] poll failed:", e);
  }
}

window.runVerifyAliveNow = async function () {
  const statusEl = document.getElementById("verify-alive-status");
  if (!confirm("立即執行偵測下架？\n會掃描所有非已封存物件並 HTTP 驗活，可能需要數分鐘。")) return;
  if (statusEl) statusEl.textContent = "⏳ 啟動中…";
  try {
    const r = await authedFetch("/admin/verify_alive/run-now", { method: "POST" });
    if (!r.ok) {
      if (statusEl) statusEl.textContent = `❌ 啟動失敗 (${r.status})`;
      return;
    }
    // 開始 live poll（每 2 秒，背景跑一定要清舊 timer）
    _stopVerifyAlivePoll();
    _verifyAlivePollTimer = setInterval(_pollVerifyAliveOnce, 2000);
    _pollVerifyAliveOnce();   // 立刻第一次
  } catch (e) {
    if (statusEl) statusEl.textContent = `❌ 失敗：${e.message}`;
  }
};

window.switchCmdTab = function (tab) {
  document.querySelectorAll(".cmd-tab").forEach(b => {
    b.classList.toggle("active", b.dataset.tab === tab);
  });
  document.querySelectorAll(".cmd-pane").forEach(p => {
    p.classList.toggle("hidden", p.dataset.pane !== tab);
  });
};

function _renderCmdRow(cmd, i, displayNum) {
  // i = origIdx（全域 index，用於 onchange / 刪除）
  // displayNum = 該 tab 內的顯示編號，從 1 開始
  const cmdType = cmd.type || "scan";
  const server = _schedServer.commands[i];
  if (displayNum === undefined) displayNum = i + 1;   // 向後相容
  // 套用狀態：比對 draft vs server（同 idx）
  const applied = _isCmdAppliedNew(cmd, server);
  const appliedBadge = !server
    ? `<span class="sched-badge unapplied">⚠ 未套用 (新增中)</span>`
    : applied
      ? `<span class="sched-badge applied">✓ 已套用</span>`
      : `<span class="sched-badge unapplied">⚠ 未套用</span>`;
  // 顯示「下次執行時間」（用戶要求：寫下一次而非上一次）
  const nextDueStr = (server && server.next_due_at)
    ? `<span class="sched-nextdue">下次：${_fmtDate(server.next_due_at)}（${_countdown(server.next_due_at)}）</span>`
    : `<span class="sched-nextdue muted">下次：套用後計算</span>`;
  const removeBtn = `<button onclick="schedRemoveCmd(${i})" class="sched-remove">刪除</button>`;

  if (cmdType === "verify_alive") {
    // 偵測下架命令：只選 interval
    const intervalOpts = _schedMeta.verifyIntervalOpts.map(h => {
      const sel = Number(cmd.interval_hr) === h ? "selected" : "";
      return `<option value="${h}" ${sel}>${h} 小時</option>`;
    }).join("");
    const appliedDetail = (server && _isCmdAppliedNew(cmd, server))
      ? `<span style="color:#27ae60; font-size:12px;">✓ 已套用：偵測下架 / 每 ${Number(server.interval_hr||24)} 小時</span>`
      : `<span style="color:#c0392b; font-size:12px;">⚠ 未套用（按下套用才生效）</span>`;
    return `
      <div class="sched-cmd sched-cmd--verify">
        <div class="sched-cmd-head">
          <b>命令 ${displayNum}</b>
          <span class="sched-type-badge verify">偵測下架</span>
          ${appliedBadge}
          ${removeBtn}
        </div>
        <div class="sched-cmd-desc">掃描非封存物件、HTTP 驗活，全失效就自動 archive。</div>
        <div class="sched-cmd-controls">
          <label>每
            <select onchange="_schedDraft.commands[${i}].interval_hr = parseInt(this.value)||24; _touchApplyBtns()">
              ${intervalOpts}
            </select>
            跑一次
          </label>
          <button onclick="applySchedulerConfig()" class="sched-apply">套用</button>
        </div>
        <div class="sched-cmd-footer">
          ${nextDueStr}
          <span class="sched-applied-detail">${appliedDetail}</span>
        </div>
      </div>`;
  }

  // scan 類型
  const ORDER = ["大安區", "信義區", "中山區", "中正區", "文山區", "松山區", "大同區", "萬華區", "南港區"];
  const ordered = ORDER.filter(d => _schedMeta.allowed.includes(d))
    .concat(_schedMeta.allowed.filter(d => !ORDER.includes(d)));
  const distChips = ordered.map(d => {
    const checked = (cmd.districts || []).includes(d) ? "checked" : "";
    const label = d.replace(/區$/, "");
    return `<label style="margin-right:8px;"><input type="checkbox" ${checked}
       onchange="schedToggleDist(${i}, '${d}', this.checked)"> ${label}</label>`;
  }).join("");

  const SOURCES = [
    { key: "591", name: "591" },
    { key: "yongqing", name: "永慶" },
    { key: "sinyi", name: "信義 (尚未支援)", disabled: true },
  ];
  const curSources = new Set(_normSources(cmd));
  const sourceChips = SOURCES.map(s => {
    const checked = curSources.has(s.key) ? "checked" : "";
    const dis = s.disabled ? "disabled" : "";
    return `<label style="margin-right:10px; ${s.disabled?'color:#aaa':''};"><input type="checkbox" ${checked} ${dis}
       onchange="schedToggleSource(${i}, '${s.key}', this.checked)"> ${esc(s.name)}</label>`;
  }).join("");

  const intervalOpts = _schedMeta.intervalOpts.map(h => {
    const sel = Number(cmd.interval_hr || 3) === h ? "selected" : "";
    return `<option value="${h}" ${sel}>${h} 小時</option>`;
  }).join("");

  const srcLabelCN = { "591": "591", "yongqing": "永慶", "sinyi": "信義" };
  const appliedDetail = (server && _isCmdAppliedNew(cmd, server))
    ? `<span style="color:#27ae60; font-size:12px;">✓ 已套用：${esc(_normSources(server).map(s=>srcLabelCN[s]||s).join("+"))} / ${esc((server.districts||[]).join("、"))} / ${Number(server.limit)} 筆 / 每 ${Number(server.interval_hr||3)} 小時</span>`
    : `<span style="color:#c0392b; font-size:12px;">⚠ 未套用（按下套用才生效）</span>`;
  return `
    <div class="sched-cmd sched-cmd--scan">
      <div class="sched-cmd-head">
        <b>命令 ${displayNum}</b>
        <span class="sched-type-badge scan">掃描新物件</span>
        ${appliedBadge}
        ${removeBtn}
      </div>
      <div class="sched-cmd-row">
        <span class="sched-label">來源</span>
        <div class="sched-chips">${sourceChips}</div>
      </div>
      <div class="sched-cmd-row">
        <span class="sched-label">行政區</span>
        <div class="sched-chips">
          ${distChips}
          <span class="sched-count">（${(cmd.districts||[]).length}/${_schedMeta.maxDistricts}）</span>
        </div>
      </div>
      <div class="sched-cmd-controls">
        <label>每次最多
          <input type="number" min="1" max="300" value="${cmd.limit || 30}"
                 oninput="_schedDraft.commands[${i}].limit = parseInt(this.value)||30; _touchApplyBtns()">
          筆
        </label>
        <label>每
          <select onchange="_schedDraft.commands[${i}].interval_hr = parseInt(this.value)||3; _touchApplyBtns()">
            ${intervalOpts}
          </select>
          跑一次
        </label>
        <button onclick="applySchedulerConfig()" class="sched-apply">套用</button>
      </div>
      <div class="sched-cmd-footer">
        ${nextDueStr}
        <span class="sched-applied-detail">${appliedDetail}</span>
      </div>
    </div>`;
}

// 偵測下架警告：超過 360hr 沒跑 → 把「危險區域」panel 變成紅色警告
function refreshVerifyAliveWarning() {
  const dangerPanel = document.querySelector(".panel-danger");
  if (!dangerPanel) return;
  // 移除上次的警告（若有）
  const existing = document.getElementById("verify-alive-warning");
  if (existing) existing.remove();
  if (!_lastVerifyAliveAt) {
    // 從未跑過 → 警告
    const warn = document.createElement("div");
    warn.id = "verify-alive-warning";
    warn.style.cssText = "background:#fff3cd; border:2px solid #ffc107; color:#856404; padding:10px 14px; border-radius:5px; margin-bottom:12px; font-size:14px; font-weight:600;";
    warn.innerHTML = "⚠️ <b>警告：從未執行偵測下架</b><br><span style=\"font-weight:normal; font-size:13px;\">建議在「定時 batch 排程」加一個「偵測下架」命令，避免下架物件累積。</span>";
    dangerPanel.insertBefore(warn, dangerPanel.firstChild);
    return;
  }
  const lastMs = new Date(_lastVerifyAliveAt).getTime();
  const elapsedHr = (Date.now() - lastMs) / 3600000;
  if (elapsedHr > 360) {
    const warn = document.createElement("div");
    warn.id = "verify-alive-warning";
    warn.style.cssText = "background:#f8d7da; border:2px solid #dc3545; color:#721c24; padding:10px 14px; border-radius:5px; margin-bottom:12px; font-size:14px; font-weight:600;";
    warn.innerHTML = `⚠️ <b>嚴重警告：${Math.round(elapsedHr)} 小時沒偵測下架（超過 360 小時）</b><br><span style="font-weight:normal; font-size:13px;">上次：${_fmtDate(_lastVerifyAliveAt)}<br>排程的「偵測下架」命令可能停了或從未啟用。請檢查定時 batch 排程。</span>`;
    dangerPanel.insertBefore(warn, dangerPanel.firstChild);
  }
}

window.schedToggleSource = function (idx, src, on) {
  const cmd = _schedDraft.commands[idx];
  if (!cmd) return;
  const cur = new Set(_normSources(cmd));
  if (on) cur.add(src);
  else cur.delete(src);
  if (cur.size === 0) {
    alert("至少要勾一個來源");
    return _touchApplyBtns();   // 重畫讓勾回去
  }
  cmd.sources = _normSources({ sources: [...cur] });
  delete cmd.source;            // 清掉舊欄位避免衝突
  _touchApplyBtns();
};

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

window.schedAddCmd = function (type) {
  type = type || "scan";
  if (_schedDraft.commands.length >= _schedMeta.maxCmds) {
    alert(`命令數已達上限 ${_schedMeta.maxCmds}`);
    return;
  }
  if (type === "verify_alive") {
    _schedDraft.commands.push({ type: "verify_alive", interval_hr: 24 });
  } else {
    // scan: 預設所有目前支援的來源都勾（591 + 永慶；信義尚未支援不算）
    _schedDraft.commands.push({
      type: "scan", districts: [], limit: 30,
      sources: ["591", "yongqing"], interval_hr: 3,
    });
  }
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
    // 全域 interval_hr 已 deprecated；保留欄位避免後端 422
    const payload = {
      interval_hr: parseInt(_schedDraft.interval_hr) || 3,
      commands: _schedDraft.commands,
    };
    console.log("[scheduler] POST payload:", JSON.stringify(payload, null, 2));
    const r = await authedFetch("/admin/scheduler/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    console.log("[scheduler] response status:", r.status);
    if (!r.ok) {
      let msg = `HTTP ${r.status}`;
      try {
        const body = await r.json();
        console.error("[scheduler] error body:", body);
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
    // 成功：強制清空 draft → 下次 loadSchedulerStatus 會從 server 重新 hydrate，
    // 確保 UI 顯示與 server 一致（不會殘留 stale draft 假裝已套用）
    _schedDraft = { interval_hr: 1, commands: [] };
    await loadSchedulerStatus();
    console.log("[scheduler] 套用後 server commands:", JSON.stringify(_schedServer.commands, null, 2));
    // 找出剛剛 POST 的 cmd，顯示新值給用戶
    const sentCmd = (payload.commands || []).find(c => c);
    if (sentCmd) {
      const ihr = sentCmd.interval_hr;
      console.log(`[scheduler] ✓ 套用成功，interval_hr=${ihr}`);
    }
  } catch (e) {
    console.error("[scheduler] apply failed:", e);
    alert("套用失敗：" + e.message);
  }
};

window.runOcrScan = async function () {
  const box = document.getElementById("ocr-scan-result");
  box.innerHTML = `<div style="color:#7f8c8d;">掃描中…（每筆要打 Google reverse geocode，全庫約 1-3 分鐘）</div>`;
  try {
    const r = await authedFetch("/admin/ocr_misread_scan");
    if (!r.ok) {
      const e = await r.json().catch(() => ({}));
      box.innerHTML = `<div style="color:#c0392b;">掃描失敗：${esc(e.detail || r.status)}</div>`;
      return;
    }
    const data = await r.json();
    const suspects = data.suspects || [];
    let html = `
      <div style="color:#555; font-size:13px; margin-bottom:8px;">
        檢查 ${Number(data.checked) || 0} 筆 / 缺原生座標跳過 ${Number(data.skipped_no_source_coords) || 0} 筆 /
        <b style="color:${suspects.length ? '#c0392b' : '#27ae60'}">疑似 ${suspects.length} 筆</b>
      </div>
      <div style="color:#7f8c8d; font-size:12px; margin-bottom:8px;">${esc(data.note || "")}</div>
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
        const title = (s.title || "").slice(0, 30);
        html += `<tr>
          <td style="padding:4px 8px;">${esc(s.id)}</td>
          <td style="padding:4px 8px;">${esc(title)}</td>
          <td style="padding:4px 8px; color:#c0392b;">${esc(s.db_road || "")}</td>
          <td style="padding:4px 8px; color:#27ae60;">${esc(s.source_reverse_road || "")}</td>
          <td style="padding:4px 8px;">
            <button onclick="quickReanalyze('${esc(s.id)}')" style="padding:3px 8px;">重新分析</button>
          </td>
        </tr>`;
      }
      html += "</tbody></table>";
    }
    box.innerHTML = html;
  } catch (e) {
    box.innerHTML = `<div style="color:#c0392b;">掃描失敗：${esc(e.message)}</div>`;
  }
};

window.toggleScheduler = async function (on, type) {
  // type: "scan" / "verify_alive" / undefined（兩個都 toggle）
  try {
    const r = await authedFetch("/admin/scheduler/toggle", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: !!on, type: type || null }),
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

// ── 物件列表分頁狀態 ─────────────────────────────────────
const PROP_PAGE_SIZE = 100;
let _propCurrentPage = 1;
let _propLastFiltered = [];   // 上次過濾後的完整列表（給翻頁用）
const _selectedIds = new Set();   // 跨頁累積的勾選 id

function _refreshBatchDelBtn() {
  const btn = document.getElementById("batch-del-btn");
  const clr = document.getElementById("clear-sel-btn");
  const n = _selectedIds.size;
  if (btn) {
    btn.textContent = `🗑 刪除選取 (${n})`;
    btn.disabled = n === 0;
  }
  if (clr) clr.disabled = n === 0;
}

window.toggleSelectOne = function (id, checked) {
  if (checked) _selectedIds.add(id); else _selectedIds.delete(id);
  _refreshBatchDelBtn();
  // 同步 header 全選 checkbox 狀態
  const head = document.getElementById("sel-all");
  if (head) {
    const pageIds = _propLastFiltered
      .slice((_propCurrentPage - 1) * PROP_PAGE_SIZE, _propCurrentPage * PROP_PAGE_SIZE)
      .map(d => d.id);
    const allChecked = pageIds.length > 0 && pageIds.every(i => _selectedIds.has(i));
    const someChecked = pageIds.some(i => _selectedIds.has(i));
    head.checked = allChecked;
    head.indeterminate = !allChecked && someChecked;
  }
};

window.toggleSelectAllPage = function (checked) {
  const pageIds = _propLastFiltered
    .slice((_propCurrentPage - 1) * PROP_PAGE_SIZE, _propCurrentPage * PROP_PAGE_SIZE)
    .map(d => d.id);
  pageIds.forEach(id => { if (checked) _selectedIds.add(id); else _selectedIds.delete(id); });
  // 同步該頁所有 row checkbox
  document.querySelectorAll(".row-sel-cb").forEach(cb => { cb.checked = checked; });
  _refreshBatchDelBtn();
};

window.clearSelection = function () {
  _selectedIds.clear();
  document.querySelectorAll(".row-sel-cb").forEach(cb => { cb.checked = false; });
  const head = document.getElementById("sel-all");
  if (head) { head.checked = false; head.indeterminate = false; }
  _refreshBatchDelBtn();
};

window.batchDeleteSelected = async function () {
  const ids = Array.from(_selectedIds);
  if (ids.length === 0) return;
  if (!confirm(`確定從中央 DB 永久刪除這 ${ids.length} 筆物件？此動作不可還原。`)) return;
  const btn = document.getElementById("batch-del-btn");
  if (btn) { btn.disabled = true; btn.textContent = `刪除中… 0/${ids.length}`; }
  let done = 0, failed = 0;
  // 並行 5 筆，避免一次打太多 request
  const CONCURRENCY = 5;
  const queue = ids.slice();
  async function worker() {
    while (queue.length) {
      const id = queue.shift();
      try {
        const r = await authedFetch(`/admin/properties/${encodeURIComponent(id)}`, { method: "DELETE" });
        if (r.ok) done++; else failed++;
      } catch { failed++; }
      if (btn) btn.textContent = `刪除中… ${done + failed}/${ids.length}`;
    }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));
  alert(`完成：成功 ${done} 筆，失敗 ${failed} 筆`);
  _selectedIds.clear();
  await loadAll();
};

window.propGoPage = function (delta) {
  const totalPages = Math.max(1, Math.ceil(_propLastFiltered.length / PROP_PAGE_SIZE));
  const newPage = Math.min(totalPages, Math.max(1, _propCurrentPage + delta));
  if (newPage === _propCurrentPage) return;
  _propCurrentPage = newPage;
  renderList();
};

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

  // 篩選結果若改變（filter / search 變動），重置回第 1 頁
  if (list.length !== _propLastFiltered.length) _propCurrentPage = 1;
  _propLastFiltered = list;
  const totalPages = Math.max(1, Math.ceil(list.length / PROP_PAGE_SIZE));
  if (_propCurrentPage > totalPages) _propCurrentPage = totalPages;

  // 翻頁切片
  const startIdx = (_propCurrentPage - 1) * PROP_PAGE_SIZE;
  const pagedList = list.slice(startIdx, startIdx + PROP_PAGE_SIZE);

  document.getElementById("count").textContent = `顯示 ${list.length} 筆，共 ${_allDocs.length} 筆`;
  const pagerInfo = document.getElementById("prop-pager-info");
  if (pagerInfo) pagerInfo.textContent = `第 ${_propCurrentPage} / ${totalPages} 頁（每頁 ${PROP_PAGE_SIZE} 筆）`;
  const prevBtn = document.getElementById("prop-pager-prev");
  const nextBtn = document.getElementById("prop-pager-next");
  if (prevBtn) prevBtn.disabled = _propCurrentPage <= 1;
  if (nextBtn) nextBtn.disabled = _propCurrentPage >= totalPages;
  // 把 list 替換成這頁的 slice，後面渲染就只跑這頁的資料
  list = pagedList;
  // 動態 thead：非 batch tab 多顯示「送件人」欄
  const showSubmitter = (_propTab === "user_url" || _propTab === "manual");
  // 移除 ID 欄；簡化欄位
  const headCols = ["連結", "City", "District", "地址", "類型", "樓層", "總價", "狀態", "抓取時間"];
  if (showSubmitter) headCols.push("送件人");
  headCols.push("動作");
  // 第一欄：全選 checkbox
  const headHTML = `<th style="width:32px;text-align:center"><input type="checkbox" id="sel-all" onchange="toggleSelectAllPage(this.checked)"></th>`
    + headCols.map(h => `<th>${h}</th>`).join("");
  document.getElementById("thead").innerHTML = `<tr>${headHTML}</tr>`;
  const colspan = headCols.length + 1;
  const tbody = document.getElementById("tbody");
  if (!list.length) {
    tbody.innerHTML = `<tr><td colspan="${colspan}" style="text-align:center;color:#999;padding:24px">無資料</td></tr>`;
    return;
  }
  // 抓取時間：不顯示「年」，只顯示 MM/DD HH:MM
  const fmtScrapeTime = iso => {
    if (!iso) return "—";
    try {
      const d = new Date(iso);
      if (isNaN(d.getTime())) return "—";
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      const hh = String(d.getHours()).padStart(2, "0");
      const mm = String(d.getMinutes()).padStart(2, "0");
      return `${m}/${dd} ${hh}:${mm}`;
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
      statusCell = `<div class="mini-loading"><div class="mini-loading-bar" style="width:${pct}%"></div><span style="font-size:11px;color:#666">分析中… ${pct}%</span></div>`;
    } else {
      statusCell = `<span class="${stCls}">${esc(d.analysis_status || "—")}</span>`;
    }
    // 連結：每一個 url 都給編號連結；前綴顯示來源（依 sources / source）
    const allUrls = [d.url, ...(d.url_alt || [])].filter(Boolean);
    let srcLabel = d.source || "591";
    if (Array.isArray(d.sources) && d.sources.length > 0) {
      srcLabel = [...new Set(d.sources.map(s => s.name).filter(Boolean))].join("/") || srcLabel;
    }
    const linkBadge = allUrls.length === 0
      ? '<span style="color:#999">—</span>'
      : `${esc(srcLabel)}: ` + allUrls.map((u, i) =>
          `<a href="${esc(u)}" target="_blank" rel="noopener noreferrer" style="color:#2980b9;margin-right:6px">${i+1}</a>`
        ).join("");
    const submitterCell = showSubmitter
      ? `<td style="font-size:12px">${esc(d.submitted_by_email || d.submitted_by_uid || '—')}</td>`
      : '';
    const isSel = _selectedIds.has(d.id);
    return `<tr>
      <td style="text-align:center"><input type="checkbox" class="row-sel-cb" ${isSel ? "checked" : ""} onchange="toggleSelectOne('${esc(d.id)}', this.checked)"></td>
      <td style="white-space:nowrap">${linkBadge}</td>
      <td>${esc(d.city || "—")}</td>
      <td>${esc(d.district || "—")}</td>
      <td style="white-space:nowrap">${_cleanAddrDisplayHTML(d)}</td>
      <td>${esc(d.building_type || "—")}</td>
      <td style="white-space:nowrap">${floors}</td>
      <td style="white-space:nowrap">${price}</td>
      <td>${statusCell}</td>
      <td style="white-space:nowrap;font-size:12px;color:#666">${esc(scrapedAt)}</td>
      ${submitterCell}
      <td class="row-actions" style="white-space:nowrap">
        <button onclick="quickReanalyze('${esc(d.id)}')" ${isReanalyzing ? "disabled" : ""}>重新分析</button>
        <button class="btn-del" onclick="quickDelete('${esc(d.id)}')">刪除</button>
      </td>
    </tr>`;
  }).join("");

  // 渲染後同步 header 全選 checkbox + 批次刪除按鈕狀態
  const headCb = document.getElementById("sel-all");
  if (headCb) {
    const pageIds = list.map(d => d.id);
    const allChecked = pageIds.length > 0 && pageIds.every(i => _selectedIds.has(i));
    const someChecked = pageIds.some(i => _selectedIds.has(i));
    headCb.checked = allChecked;
    headCb.indeterminate = !allChecked && someChecked;
  }
  _refreshBatchDelBtn();
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
  return addr || "—";
}

// XSS-safe 版本：回傳已 escape 的 HTML 字串，含推測 badge
function _cleanAddrDisplayHTML(d) {
  const plain = _cleanAddrDisplay(d);
  const badge = d.address_inferred ? ' <span style="font-size:11px;color:#1a8754">(推測)</span>' : '';
  return esc(plain) + badge;
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
      const tierTxt = u.tier_name_zh ? `${esc(u.tier_name_zh)}${u.tier_name_en ? ` / ${esc(u.tier_name_en)}` : ""}` : "—";
      return `
      <tr>
        <td><code style="font-size:11px">${esc(u.uid)}</code></td>
        <td>${esc(u.email || "—")}</td>
        <td>${esc(u.display_name || "—")}</td>
        <td>${tierTxt}</td>
        <td>${u.created_at ? esc(new Date(u.created_at).toLocaleString()) : "—"}</td>
        <td class="row-actions">
          <button class="btn-del" onclick="deleteUserData('${esc(u.uid)}','${esc((u.email || '').replace(/'/g, '&apos;'))}')">刪除此用戶資料</button>
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

  // 重抓 source 下拉：依該 doc 的 sources 列出可選來源
  const srcSelect = document.getElementById("reanalyze-source-select");
  if (srcSelect) {
    const sources = Array.isArray(d.sources) && d.sources.length
      ? d.sources
      : (d.source ? [{ name: d.source }] : []);
    const names = [...new Set(sources.map(s => s.name).filter(Boolean))];
    let html = '<option value="all">全部來源</option>';
    names.forEach(n => {
      html += `<option value="${esc(n)}">只重抓 ${esc(n)}</option>`;
    });
    srcSelect.innerHTML = html;
    srcSelect.style.display = (names.length > 1) ? "" : "none";
  }
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
  const srcSelect = document.getElementById("reanalyze-source-select");
  const source = (srcSelect && srcSelect.value) || "all";
  const verb = source === "all" ? "全部來源" : `只 ${source}`;
  if (!confirm(`重新分析 ${id}（${verb}）？（會覆寫中央資料）`)) return;
  const url = `/admin/properties/${encodeURIComponent(id)}/reanalyze?source=${encodeURIComponent(source)}`;
  const r = await authedFetch(url, { method: "POST" });
  if (r.ok) { alert(`已啟動重新分析（${verb}）`); closeModal(); }
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
  const sourceEl = document.getElementById("scrape-source");
  const source = sourceEl ? sourceEl.value : "591";
  const body = {
    districts,
    limit,
    source,
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
    "⚠ 這會把中央 DB 所有物件標為「已封存」(軟刪除)。\n" +
    "- 探索 tab 會看不到這些物件\n" +
    "- 用戶 watchlist 仍能看到（會顯示「已封存」標籤）\n" +
    "- 日後重抓到的物件會自動回復\n\n" +
    "確定執行？請輸入 ARCHIVE 確認："
  );
  if (typed !== "ARCHIVE") {
    if (typed !== null) alert("未輸入 ARCHIVE，已取消。");
    return;
  }
  try {
    const r = await authedFetch("/api/clear_db", { method: "POST" });
    const data = await r.json();
    if (r.ok) {
      alert(`已封存中央 DB（archived ${data.archived ?? 0} 筆）`);
      loadAll();
    } else {
      alert("封存失敗：" + (data.detail || r.status));
    }
  } catch (e) {
    alert("封存失敗：" + e.message);
  }
};

boot().catch(e => {
  document.getElementById("login-err").textContent = "初始化失敗：" + e.message;
});
