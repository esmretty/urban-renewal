// Auth gate：index.html 一載入就先跑這支。
// - 沒登入 → 跳 login.html
// - 有登入 → 暴露 window.authedFetch / window.currentUser / window.logoutUser 給 app.js 用
//          把 window.fetch 整個覆寫成「一律帶 Authorization header」，這樣 app.js 不用改

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import { getAuth, onAuthStateChanged, signOut }
  from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";

async function boot() {
  const res = await fetch("/api/firebase_config");
  const cfg = await res.json();
  if (!cfg.apiKey) {
    document.body.innerHTML =
      '<pre style="padding:30px;color:#c0392b">後端尚未設定 FIREBASE_WEB_API_KEY 環境變數。</pre>';
    return;
  }
  const app = initializeApp(cfg);
  const auth = getAuth(app);

  const ready = new Promise(resolve => {
    onAuthStateChanged(auth, (user) => {
      if (!user) {
        window.location.replace("/login.html");
        return;
      }
      resolve(user);
    });
  });

  const user = await ready;
  const token = await user.getIdToken();

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
      window.location.replace("/login.html");
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

  // 從 /api/me 拿階級名稱顯示在 email 後面；
  // 403 = 新帳號不在白名單 → 登出並導回 login 頁顯示訊息
  try {
    const meResp = await window.authedFetch("/api/me");
    if (meResp.status === 403) {
      const body = await meResp.json().catch(() => ({}));
      const msg = body.detail || "此帳號尚未獲邀，請聯絡管理者將您加入白名單。";
      await signOut(auth);
      window.location.replace("/login.html?err=" + encodeURIComponent(msg));
      return;
    }
    if (meResp.ok) {
      const me = await meResp.json();
      window.currentUserTier = me;
      if (emailEl && me.tier_name_zh) {
        emailEl.textContent = `${me.email || ""}（${me.tier_name_zh}）`;
      }
    }
  } catch (e) {
    console.warn("fetch /api/me failed:", e);
  }

  window.logoutUser = async () => {
    await signOut(auth);
    window.location.replace("/login.html");
  };

  // 告訴 app.js auth 已就緒（app.js 有綁 DOMContentLoaded，通常這時候還沒跑完）
  window.__authReady = true;
  document.dispatchEvent(new CustomEvent("auth:ready", { detail: window.currentUser }));
}

boot().catch(e => {
  console.error("auth gate failed:", e);
  document.body.innerHTML =
    '<pre style="padding:30px;color:#c0392b">登入驗證失敗：' + (e.message || e) + '</pre>';
});
