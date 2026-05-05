// Firebase SDK 自打包入口 — esbuild 把這檔 bundle 成 firebase-bundle.js
// 用途：消除原本 import from "https://www.gstatic.com/firebasejs/10.12.2/firebase-*.js"
//       transitive 載 ~10 個跨域子模組造成的 ~3 秒 chained fetch。
// 改成同源單檔 → HTTP/2 + browser cache + nginx gzip 一次解決。
// 升 Firebase 版本：改 package.json 的 firebase 版號 + 重跑 npm install + npm run build:firebase
export { initializeApp } from "firebase/app";
export {
  getAuth,
  GoogleAuthProvider,
  signInWithPopup,
  signOut,
  onAuthStateChanged,
} from "firebase/auth";
