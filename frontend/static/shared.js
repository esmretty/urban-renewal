/**
 * v1 / v2 共用算式 (single source of truth)
 *
 * 為什麼有這個檔案：
 *   v1 (app.js) 跟 v2 (app2.js) 是兩份獨立 IIFE，曾經各自 reimplement
 *   lookupFar / effectiveFar / computeMultiples 等核心算式 → drift 過很多次
 *   (台北市路寬限縮、道路用地 0%、商業區泛稱 fallback 都踩過)。
 *
 *   這個檔案把「不論 v1/v2 都該一樣的純算式 + 法規常數」抽出來，window.UrbanShared
 *   暴露給 v1 跟 v2 各自呼叫。HTML/CSS layout 仍由 v1/v2 各自實作。
 *
 * 載入方式：index.html / index2.html 在 app.js / app2.js **之前** 載入 shared.js。
 *
 * 動這個檔案的規則：
 *   - 算式邏輯改 → v1 / v2 自動同步
 *   - 但還是要同步改後端 (analysis/scorer.py 的 calculate_renewal_scenarios + config.lookup_far)
 *   - CLAUDE.md 規則 8：前後端算式單一來源 — 前端三處變兩處 (shared/scorer.py)，已少一處
 */
(function (global) {
  'use strict';

  // ── 法規常數 ────────────────────────────────────────────────────────────────

  // 分回比例查表 (新成屋單價 萬/坪 → [屋主分回比例, 車位市值 萬])
  const SHARE_RATIO_TABLE = [
    [60,0.45,234],[65,0.47,241],[70,0.48,248],[75,0.49,255],[80,0.50,262],
    [85,0.51,269],[90,0.52,276],[95,0.53,283],[100,0.54,290],[105,0.55,297],
    [110,0.56,304],[115,0.57,311],[120,0.58,318],[130,0.60,332],[140,0.61,339],
    [150,0.62,360],[160,0.63,374],[170,0.64,388],[180,0.65,402],
  ];

  // 台北市法定容積率 (依《台北市土地使用分區管制自治條例》)
  const TAIPEI_FAR_PCT = {
    "第一種住宅區":60,"第二種住宅區":120,"第三種住宅區":225,"第三種住宅區(特)":225,
    "第三之一種住宅區":300,"第三之二種住宅區":400,"第四之一種住宅區":400,
    "第四種住宅區":300,"住宅用地":200,
    "第一種商業區":360,"第二種商業區":630,"第三種商業區":560,"第三種商業區(特)":560,
    "第四種商業區":800,
  };

  // 新北市法定容積率 (per-district)
  // 注意：用戶設計新北市路寬 < 8m 縮減規則不實作 (建商會擴基地吃旁邊路寬)
  const NEW_TAIPEI_FAR_PCT = {
    "_5_districts_default": { "住宅區": 300, "商業區": 440 },
    "_banqiao_overrides":   { "商業區": 460 },
    "_banqiao_fujou":       { "住宅區": 240, "住宅區(再)": 160, "商業區": 300 },
    "新店區": {
      "第二種住宅區": 120, "第三種住宅區": 280, "第四種住宅區": 300,
      "第一種商業區": 420, "第二種商業區": 440,
      "住宅區": 300, "商業區": 440,
    },
    "土城區": { "第一種住宅區": 180, "第二種住宅區": 240, "第一種商業區": 240, "第二種商業區": 320 },
    "樹林區": { "第一種住宅區": 260, "第二種住宅區": 250, "商業區": 380 },
    "汐止區": { "第一種住宅區": 200, "第二種住宅區": 240, "商業區": 320 },
    "淡水區": { "第二種住宅區": 225, "第三種住宅區": 360, "第四種住宅區": 240, "第一種商業區": 360, "第二種商業區": 400 },
    "八里區": { "第一種住宅區": 200, "第二種住宅區": 200, "第一種商業區": 300, "第二種商業區": 300 },
  };

  const _NEW_TAIPEI_5_DISTRICTS = ["板橋區", "新莊區", "中和區", "永和區", "三重區"];

  // 各區新成屋單價 fallback (萬/坪) — 啟動後會被 /api/district_new_house_price LVR 中位數覆寫
  const DISTRICT_NEW_HOUSE_PRICE_FALLBACK = {
    "中正區":110,"大同區":95,"中山區":110,"松山區":130,"大安區":150,
    "萬華區":80,"信義區":145,"內湖區":110,"南港區":110,"文山區":90,
    "板橋區":75,"新莊區":65,"新店區":75,"中和區":70,"永和區":75,
  };

  // 重建建坪係數：計入容積 1 + 免計容積估計 0.57 (跟後端 REBUILD_BUILD_COEFF 一致)
  const REBUILD_BUILD_COEFF = 1.57;

  // ── 算式 function ────────────────────────────────────────────────────────────

  // 統一 FAR lookup：根據 (zoning, p) 回對應容積率 (百分比)
  // p 提供 district + is_remote_area (浮洲判定) — 跟後端 config.lookup_far 對齊
  function lookupFar(zoning, p) {
    if (!zoning || !p) return null;
    // 先剝「(含道路用地)」「(含公共設施保留地)」之類括號註記，避免下面 regex 把
    // 「第四種住宅區(含道路用地)」整個誤判為不可建築用地 → 0%
    const zClean = zoning.replace(/\(含[^)]*\)/g, '').trim();
    // 不可建築用地 (道路/公共設施保留地/綠地/公園/河道/機關用地) → FAR = 0%
    // 不貢獻倍數但仍視為「合法值」(0 ≠ null)，讓多分區加權能繼續計算
    if (/^(道路用地|公共設施|保留地|綠地|公園|河道|機關用地)/.test(zClean)) return 0;
    zoning = zClean;   // 用剝過註記的 zoning 繼續查
    const district = p.district;
    // 1) 浮洲 (板橋偏遠 polygon 內)
    if (district === "板橋區" && p.is_remote_area) {
      return NEW_TAIPEI_FAR_PCT["_banqiao_fujou"][zoning] ?? null;
    }
    // 2) 新北市列名區 (含子類別) — 子類別精確 match 優先，miss 才 fallback 到泛稱
    if (NEW_TAIPEI_FAR_PCT[district]) {
      const subVal = NEW_TAIPEI_FAR_PCT[district][zoning];
      if (subVal != null) return subVal;
      if (zoning.includes("商")) return NEW_TAIPEI_FAR_PCT[district]["商業區"] ?? null;
      if (zoning.includes("住")) return NEW_TAIPEI_FAR_PCT[district]["住宅區"] ?? null;
      return null;
    }
    // 3+4) 板橋/中和/永和/新莊/三重 5 區
    if (_NEW_TAIPEI_5_DISTRICTS.includes(district)) {
      if (zoning.includes("商")) return district === "板橋區" ? 460 : 440;
      if (zoning.includes("住")) return 300;
      return null;
    }
    // 5) 台北市
    const direct = TAIPEI_FAR_PCT[zoning];
    if (direct != null) return direct;
    // 泛稱 fallback (GeoServer 偶會回沒子類別的「商業區」「住宅區」字串)
    if (zoning === "商業區") return TAIPEI_FAR_PCT["第一種商業區"] ?? null;
    if (zoning === "住宅區") return TAIPEI_FAR_PCT["第二種住宅區"] ?? null;
    return null;
  }

  // 處理特殊 zoning 後綴 (特/遷/核/抄)，回實際應該用來查 FAR 的 zoning 字串
  function effectiveZoning(p) {
    const z = p.zoning || "";
    const orig = p.zoning_original || "";
    const hasSpecial = /\((?:特|遷|核|抄)\)/.test(z);
    const base = z.replace(/\((?:特|遷|核|抄)\)/g, "").trim();
    if (hasSpecial && z.includes("商")) {
      if (orig && lookupFar(orig, p) != null) return orig;
      return lookupFar(base, p) != null ? base : z;
    }
    if (hasSpecial && z.includes("住")) {
      return lookupFar(base, p) != null ? base : z;
    }
    if (orig && lookupFar(orig, p) != null) return orig;
    return z;
  }

  // 路寬限縮 FAR：
  //   台北市：FAR 上限 = 路寬(m) × 50 (%)。基準 FAR > 上限時被限縮
  //   新北市：用戶設計不限縮 (建商會擴基地吃旁邊路寬)
  function applyRoadCap(baseFar, p) {
    if (baseFar == null) return null;
    if (p.city !== "台北市") return baseFar;
    const roadW = p.road_width_m_override ?? p.road_width_m;
    if (!roadW || roadW <= 0) return baseFar;
    const cap = Math.round(roadW * 50);
    return Math.min(baseFar, cap);
  }

  // base FAR (未套路寬限縮) — detail 顯示「原 N%」用
  function baseFarPctWeighted(p) {
    const zoneList = p.zoning_list;
    if (zoneList && zoneList.length > 1) {
      const ratiosPct = p.zoning_ratios || zoneList.map(() => 100 / zoneList.length);
      const total = ratiosPct.reduce((a, b) => a + (Number(b) || 0), 0) || 1;
      let weighted = 0;
      for (let i = 0; i < zoneList.length; i++) {
        const zItem = zoneList[i];
        const z = (typeof zItem === "string") ? zItem : (zItem.original_zone || zItem.zone_name);
        const far = lookupFar(z, p);
        if (far == null) return null;
        weighted += far * ((Number(ratiosPct[i]) || 0) / total);
      }
      return Math.round(weighted);
    }
    return lookupFar(effectiveZoning(p), p);
  }

  // effective FAR (已套路寬限縮) — 倍數試算用
  function effectiveFarPctWeighted(p) {
    return applyRoadCap(baseFarPctWeighted(p), p);
  }

  // 單一 zoning 的 effective FAR (路寬限縮 already applied)
  function effectiveFarPct(zoning, p) {
    return applyRoadCap(lookupFar(zoning, p), p);
  }

  // 分回比例：依新成屋單價線性插值，回 [屋主分回比例 0~1, 車位市值 萬]
  function lookupShareRatio(priceWan) {
    if (!priceWan) return [null, null];
    const t = SHARE_RATIO_TABLE;
    if (priceWan <= t[0][0]) return [t[0][1], t[0][2]];
    if (priceWan >= t[t.length-1][0]) return [t[t.length-1][1], t[t.length-1][2]];
    for (let i = 0; i < t.length - 1; i++) {
      const [p1, r1, c1] = t[i]; const [p2, r2, c2] = t[i+1];
      if (p1 <= priceWan && priceWan <= p2) {
        const f = (priceWan - p1) / (p2 - p1);
        return [r1 + (r2-r1)*f, c1 + (c2-c1)*f];
      }
    }
    return [null, null];
  }

  // 屋齡：完工年優先 (有完工年就以當年 - 完工年為準)，沒有才回 building_age
  // 對齊 v1 currentAge (app.js:10) — 順序很重要：因為 reanalyze 不會重抓 building_age，
  // 但完工年是穩定資料，用完工年算才會跟著時間自動 +1
  function currentAge(p) {
    if (!p) return null;
    const yc = p.building_age_completed_year;
    if (yc && Number.isFinite(yc) && yc > 1900) {
      return new Date().getFullYear() - yc;
    }
    return p.building_age ?? null;
  }

  // 土地坪數異常偵測：591 詳情頁有時誤抓 (極端例：建坪 23、土地 107)
  // 一般公寓土地持分 ≈ 建坪 × 0.3-0.5；土地 > 建坪幾乎一定是抓錯
  function isLandAreaSuspicious(p) {
    const land = Number(p.land_area_ping) || 0;
    const build = Number(p.building_area_ping) || 0;
    return build > 0 && land > build;
  }

  // 開價 9 折，四捨五入到 10 萬 — 欲出價的預設值
  function desiredPriceWan(p) {
    if (p.desired_price_wan != null) return p.desired_price_wan;
    if (!p.price_ntd) return "";
    return Math.round((p.price_ntd / 10000) * 0.9 / 10) * 10;
  }

  // 計算危老 / 都更 倍數 (給卡片列表 + LINE 通知共用)
  // p: property doc
  // newHousePriceWan: 新成屋單價 萬/坪 (從 prices[district] 或 fallback 取)
  // 回 { w: 危老倍數, d: 都更倍數, valW, valD, shareW, shareD } 或 null fields if 算不出來
  // 三種旗標跳過試算：is_foreclosure / is_remote_area / unsuitable_for_renewal / isLandAreaSuspicious
  function computeMultiples(p, newHousePriceWan) {
    if (p.is_foreclosure || p.is_remote_area || p.unsuitable_for_renewal || isLandAreaSuspicious(p)) {
      return { w: null, d: null, valW: null, valD: null, shareW: null, shareD: null };
    }
    const land = Number(p.land_area_ping) || 0;
    const price = p.new_house_price_wan_override ?? newHousePriceWan;
    const farPct = effectiveFarPctWeighted(p);
    if (!land || !farPct || !price) return { w: null, d: null, valW: null, valD: null, shareW: null, shareD: null };
    const coeff = p.rebuild_coeff ?? REBUILD_BUILD_COEFF;
    const [ratio, parking] = lookupShareRatio(price);
    if (!ratio) return { w: null, d: null, valW: null, valD: null, shareW: null, shareD: null };
    const is1F = Number(p.floor) === 1 || Number(p.floor_range_min) === 1;
    const floorPremium = p.floor_premium ?? (is1F ? 0.20 : 0);
    const effectivePrice = price * (1 + floorPremium);
    // 防災型：台北市 1974 前蓋的，都更獎勵 0.80 (vs 一般 0.50)
    const age = currentAge(p);
    const isFangzai = p.city === "台北市" && age && (new Date().getFullYear() - age) <= 1974;
    const bonusW = p.bonus_weishau ?? 0.30;
    const bonusD = p.bonus_dugen ?? (isFangzai ? 0.80 : 0.50);
    const calcShare = (b) => land * (farPct/100) * (1+b) * coeff * ratio;
    const calcVal = (b) => {
      const share = calcShare(b);
      return share * effectivePrice + (share / 40) * (parking || 0);
    };
    const desired = parseFloat(p.desired_price_wan ?? desiredPriceWan(p)) || 0;
    const shareW = calcShare(bonusW), valW = calcVal(bonusW);
    const shareD = calcShare(bonusD), valD = calcVal(bonusD);
    if (!desired) return { w: null, d: null, valW, valD, shareW, shareD };
    return {
      w: valW / desired,
      d: valD / desired,
      valW, valD, shareW, shareD,
    };
  }

  // 分區縮寫：第三種住宅區→住三、第四種商業區→商四、第三之一種住宅區→住三之一…
  // 後綴 (特)/(遷)/(核)/(抄) 保留貼在縮寫後面
  function zoneAbbr(z) {
    if (!z) return "—";
    const suffixMatch = z.match(/(\((?:特|遷|核|抄)\))/);
    const suffix = suffixMatch ? suffixMatch[1] : "";
    const base = z.replace(/\((?:特|遷|核|抄)\)/g, "").trim();
    const m = base.match(/^第([一二三四五六])(?:之([一二三四五六]))?種(住宅|商業|工業)區$/);
    if (m) {
      const [, n, sub, kind] = m;
      const k = kind === "住宅" ? "住" : kind === "商業" ? "商" : "工";
      return sub ? `${k}${n}之${sub}${suffix}` : `${k}${n}${suffix}`;
    }
    if (base === "住宅區") return "住" + suffix;
    if (base === "商業區") return "商" + suffix;
    if (base === "工業區") return "工" + suffix;
    if (base === "住宅用地") return "住地" + suffix;
    return base + suffix;
  }

  // ── export ─────────────────────────────────────────────────────────────────
  global.UrbanShared = {
    // constants
    SHARE_RATIO_TABLE, TAIPEI_FAR_PCT, NEW_TAIPEI_FAR_PCT,
    DISTRICT_NEW_HOUSE_PRICE_FALLBACK, REBUILD_BUILD_COEFF,
    // functions
    lookupFar, effectiveZoning, applyRoadCap,
    baseFarPctWeighted, effectiveFarPctWeighted, effectiveFarPct,
    lookupShareRatio, currentAge, isLandAreaSuspicious,
    desiredPriceWan, computeMultiples, zoneAbbr,
  };
})(window);
