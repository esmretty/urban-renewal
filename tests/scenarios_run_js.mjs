// Load shared.js into a fake browser global and run computeMultiples for each case.
// shared.js is an IIFE `(function(global){...})(window)` — we feed our own global object.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

// Build a sandbox window-like global, run shared.js inside it.
const sandbox = {
    console,
    Math,
    Number,
    parseFloat,
    parseInt,
    Date,
    Object,
    Array,
    isFinite,
    isNaN,
    JSON,
};
sandbox.window = sandbox;
sandbox.globalThis = sandbox;
vm.createContext(sandbox);

const sharedJs = fs.readFileSync(path.join(ROOT, "frontend", "static", "shared.js"), "utf-8");
vm.runInContext(sharedJs, sandbox);

const { UrbanShared } = sandbox;
if (!UrbanShared) {
    console.error("UrbanShared not exposed after loading shared.js");
    process.exit(2);
}

const casesFile = path.join(ROOT, "tests", "scenarios_cases.json");
const cases = JSON.parse(fs.readFileSync(casesFile, "utf-8"));

function runCase(c) {
    const p = c.doc;
    const price = c.new_house_price_wan_per_ping;
    // JS computeMultiples returns { w, d, valW, valD, shareW, shareD }
    // It combines 都更/防災都更 into "d", choosing bonus based on isFangzai (city + age <= 1974)
    const res = UrbanShared.computeMultiples(p, price);
    // Also surface effective FAR for sanity
    const effFar = UrbanShared.effectiveFarPctWeighted(p);
    const shareRatio = (() => {
        const [r] = UrbanShared.lookupShareRatio(p.new_house_price_wan_override ?? price);
        return r;
    })();
    // Detect which "d" is computing (都更 vs 防災都更) — same as JS logic
    const age = UrbanShared.currentAge(p);
    const isFangzai = p.city === "台北市" && age && (new Date().getFullYear() - age) <= 1974;
    return {
        name: c.name,
        effective_far_pct: effFar,
        share_ratio: shareRatio == null ? null : Math.round(shareRatio * 1000) / 1000,
        is_fangzai: !!isFangzai,
        weishau_multiple: round2(res.w),
        // 'd' is 都更 OR 防災都更 depending on isFangzai
        dugen_or_fz_multiple: round2(res.d),
        weishau_share_ping: round1(res.shareW),
        dugen_or_fz_share_ping: round1(res.shareD),
    };
}

function round2(v) {
    return v == null ? null : Math.round(v * 100) / 100;
}
function round1(v) {
    return v == null ? null : Math.round(v * 10) / 10;
}

const results = cases.map(runCase);
console.log(JSON.stringify(results, null, 2));
