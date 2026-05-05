// Auth gate：index.html 一載入就先跑這支。
// - 沒登入 → 跳 login.html
// - 有登入 → 暴露 window.authedFetch / window.currentUser / window.logoutUser 給 app.js 用
//          把 window.fetch 整個覆寫成「一律帶 Authorization header」，這樣 app.js 不用改

// 自打包同源 bundle (esbuild 從 firebase npm 包打成 ~30KB gzip 單檔)
// 取代 https://www.gstatic.com/firebasejs/10.12.2/firebase-*.js 的 chained 跨域載入
// (~3 秒 → ~300ms)。升版改 package.json + 重跑 npm run build:firebase。
import { initializeApp, getAuth, onAuthStateChanged, signOut }
  from "/static/firebase-bundle.js";

// 跳到 /login.html 前先把當前 URL 存進 sessionStorage，登入完導回（保留 ?id= deep-link 等）
function _redirectToLogin(extraQuery) {
  try {
    const cur = window.location.pathname + window.location.search + window.location.hash;
    if (cur && cur !== "/login.html" && !cur.startsWith("/login.html")) {
      sessionStorage.setItem("post_login_redirect", cur);
    }
  } catch (_e) {}
  window.location.replace("/login.html" + (extraQuery || ""));
}

async function boot() {
  window.__perfMark && window.__perfMark('auth_gate_module_loaded');
  const res = await fetch("/api/firebase_config");
  window.__perfMark && window.__perfMark('firebase_config_fetched');
  const cfg = await res.json();
  if (!cfg.apiKey) {
    document.body.innerHTML =
      '<pre style="padding:30px;color:#c0392b">後端尚未設定 FIREBASE_WEB_API_KEY 環境變數。</pre>';
    return;
  }
  const app = initializeApp(cfg);
  const auth = getAuth(app);
  window.__perfMark && window.__perfMark('firebase_initialized');

  const ready = new Promise(resolve => {
    onAuthStateChanged(auth, (user) => {
      if (!user) {
        _redirectToLogin();
        return;
      }
      resolve(user);
    });
  });

  const user = await ready;
  window.__perfMark && window.__perfMark('onAuthStateChanged_resolved');
  const token = await user.getIdToken();
  window.__perfMark && window.__perfMark('getIdToken_done');

  // 把 token 存進 localStorage 給「下次 page load 的 inline early-fetch script」用
  // ID token TTL 1 小時，保守抓 50 分鐘可信任窗
  try {
    localStorage.setItem('v2_cached_token', token);
    localStorage.setItem('v2_cached_token_exp', String(Date.now() + 50 * 60 * 1000));
  } catch (_e) { /* localStorage 不可用就算了 */ }

  window.currentUser = {
    uid: user.uid,
    email: user.email,
    displayName: user.displayName,
    photoURL: user.photoURL,
    getIdToken: () => user.getIdToken(),   // 讓 app.js 需要時可以重拿
  };

  // 把 fetch 包成一律帶 Authorization header（對同源 /api/* /admin/* 才加）
  const origFetch = window.fetch.bind(window);
  window.fetch = async function (input, init = {}) {
    const url = typeof input === "string" ? input : input.url;
    const isApi = url.startsWith("/api/") || url.startsWith("/admin/");
    if (!isApi) return origFetch(input, init);

    const fresh = await user.getIdToken();
    const headers = new Headers(init.headers || {});
    if (!headers.has("Authorization")) headers.set("Authorization", "Bearer " + fresh);
    const resp = await origFetch(input, { ...init, headers });
    if (resp.status === 401) {
      _redirectToLogin();
    }
    return resp;
  };
  window.authedFetch = window.fetch;    // 兼容名稱

  // 頂部 user UI
  const menu = document.getElementById("user-menu");
  const avatar = document.getElementById("user-avatar");
  const emailEl = document.getElementById("user-email");
  if (menu) {
    menu.style.display = "";
    if (avatar && user.photoURL) avatar.src = user.photoURL;
    if (emailEl) emailEl.textContent = user.email || "";
  }

  window.logoutUser = async () => {
    await signOut(auth);
    // logout 不帶 redirect-back（用戶主動登出，不該回原 URL）
    try { sessionStorage.removeItem("post_login_redirect"); } catch (_e) {}
    window.location.replace("/login.html");
  };

  // 立刻 dispatch auth:ready — 讓 app2.js 可以並行 fire 它的資料請求，不再被 /api/me 卡住
  window.__authReady = true;
  window.__perfMark && window.__perfMark('auth_ready_dispatched');
  document.dispatchEvent(new CustomEvent("auth:ready", { detail: window.currentUser }));

  // /api/me 在背景跑：拿階級名稱 + 處理 403 (白名單) / 維護模式 redirect
  // 跟 app2.js 的資料載入並行，不再 sequential block 首頁渲染
  (async () => {
    try {
      const meResp = await window.authedFetch("/api/me");
      if (meResp.status === 403) {
        const body = await meResp.json().catch(() => ({}));
        const msg = body.detail || "此帳號尚未獲邀，請聯絡管理者將您加入白名單。";
        await signOut(auth);
        // 403 = 白名單擋 → 不帶 redirect-back（避免登入完又被擋一次）
        try { sessionStorage.removeItem("post_login_redirect"); } catch (_e) {}
        window.location.replace("/login.html?err=" + encodeURIComponent(msg));
        return;
      }
      if (meResp.ok) {
        const me = await meResp.json();
        window.currentUserTier = me;
        if (emailEl && me.tier_name_zh) {
          emailEl.textContent = `${me.email || ""}（${me.tier_name_zh}）`;
        }
        if (me.maintenance && me.maintenance.enabled) {
          window.location.replace("/maintenance.html");
          return;
        }
      }
    } catch (e) {
      console.warn("fetch /api/me failed:", e);
    }
  })();
}

boot().catch(e => {
  console.error("auth gate failed:", e);
  document.body.innerHTML =
    '<pre style="padding:30px;color:#c0392b">登入驗證失敗：' + (e.message || e) + '</pre>';
});
