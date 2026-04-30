"""
共用分析 pipeline：批次爬取和 URL 送出都呼叫這個函式。
修一處即可，不會再漏改。
"""
import logging
import re
from datetime import datetime, timezone
from database.time_utils import now_tw_iso

logger = logging.getLogger(__name__)


def _scan_road_width_vision(*, lat, lng, addr, district, src_id, all_roads, browser_ctx=None, skip_vision=False):
    """
    Playwright 開 zonemap → 開路寬圖層 → 門牌搜尋定位 → 截圖 → (可選) Vision 判斷。
    skip_vision=True：只截圖不跑 Vision（用於規則已命中的 case，保留截圖供肉眼驗證）
                     → 回 {"screenshot": path}
    skip_vision=False：完整 Vision 判斷 → 回 {"road_width_m", "road_name", "reason", "screenshot"}
    browser_ctx: 傳入現有的 BrowserContext 避免重複開瀏覽器。
    """
    from config import BASE_DIR
    import json as _json

    addr_parsed = {}
    m = re.search(r"([\u4e00-\u9fa5]+(?:路|街|大道)(?:[一二三四五六七八九十]段)?)", addr)
    if m:
        addr_parsed["road"] = m.group(1)
    m = re.search(r"(\d+)巷", addr)
    if m:
        addr_parsed["lane"] = m.group(1)
    m = re.search(r"(\d+)弄", addr)
    if m:
        addr_parsed["alley"] = m.group(1)
    # 支援複合門牌「8-1號」「8之1號」；zonemap 的號欄位接受「8之1」格式，hyphen 要轉成「之」
    m = re.search(r"(\d+(?:[-之]\d+)?)號", addr)
    if m:
        addr_parsed["number"] = m.group(1).replace("-", "之")
    m = re.search(r"([\u4e00-\u9fa5]{2,3}區)", addr)
    addr_district = m.group(1) if m else district

    screenshot_path = BASE_DIR / "data" / "screenshots" / f"{src_id}_roadwidth.png"

    if not browser_ctx:
        logger.warning("_scan_road_width_vision: 沒有 browser_ctx，跳過")
        return None
    ctx = browser_ctx
    page = ctx.new_page()
    page.set_viewport_size({"width": 1920, "height": 1080})
    page.goto(
        "https://zonemap.udd.gov.taipei/ZoneMapOP/indexZoneMap_op.aspx",
        wait_until="networkidle", timeout=60000,
    )
    import time
    time.sleep(6)
    # 開側欄 → 開圖層
    page.click(".fa-bars", timeout=15000)
    time.sleep(1)
    page.evaluate(r"""() => {
        const cbs = document.querySelectorAll('.sliderbut input[type=checkbox]');
        [2, 3, 4].forEach(i => {
            if (cbs[i] && !cbs[i].checked) {
                cbs[i].checked = true;
                cbs[i].dispatchEvent(new Event('change', {bubbles: true}));
            }
        });
    }""")
    time.sleep(1)
    # 門牌搜尋定位
    if addr_parsed.get("road") and addr_parsed.get("number"):
        page.click('a[href="#sidebarSearch"]', timeout=15000)
        time.sleep(1)
        page.select_option("#OtherQMemu", value="tqM6")
        time.sleep(1)
        _road = addr_parsed.get("road", "")
        _lane = addr_parsed.get("lane", "")
        _alley = addr_parsed.get("alley", "")
        _num = addr_parsed.get("number", "")
        # 拆主號 / 副號（zonemap 的號欄位是兩個 input，中間字面印「之」做分隔）
        _num_parts = _num.split("之")
        _num_main = _num_parts[0] if _num_parts else ""
        _num_sub = _num_parts[1] if len(_num_parts) >= 2 else ""
        page.evaluate(f"""() => {{
            const panel = document.querySelector('#tqM6');
            if (!panel) return;
            const sel = panel.querySelector('select');
            for (const o of sel.options) {{
                if (o.text.includes('{addr_district}')) {{ sel.value = o.value; sel.dispatchEvent(new Event('change')); break; }}
            }}
            const groups = panel.querySelectorAll('.form-group');
            groups.forEach(g => {{
                const label = (g.querySelector('label')?.innerText || '').trim();
                const inputs = g.querySelectorAll('input');
                if (!inputs.length) return;
                const inp = inputs[0];
                if (label.startsWith('道路')) {{ inp.value = '{_road}'; inp.dispatchEvent(new Event('input')); }}
                else if (label === '巷') {{ inp.value = '{_lane}'; inp.dispatchEvent(new Event('input')); }}
                else if (label === '弄') {{ inp.value = '{_alley}'; inp.dispatchEvent(new Event('input')); }}
                else if (label.startsWith('號')) {{
                    // 號欄位兩格：[主號]之[副號]號
                    inputs[0].value = '{_num_main}';
                    inputs[0].dispatchEvent(new Event('input'));
                    if (inputs.length >= 2) {{
                        inputs[1].value = '{_num_sub}';
                        inputs[1].dispatchEvent(new Event('input'));
                    }}
                }}
            }});
            const btn = panel.querySelector('.btn-danger');
            if (btn) btn.click();
        }}""")
        time.sleep(5)
    # 關側欄
    page.click(".fa-bars", timeout=15000)
    time.sleep(2)
    # fallback 座標定位
    if not (addr_parsed.get("road") and addr_parsed.get("number")):
        page.evaluate(f"""() => {{
            const view = window.map.getView();
            view.setCenter([{lng}, {lat}]);
            view.setZoom(20);
        }}""")
        time.sleep(5)
    page.screenshot(path=str(screenshot_path), full_page=False)
    page.close()

    # skip_vision：只截圖不跑 Vision
    if skip_vision:
        return {"screenshot": f"/data/screenshots/{src_id}_roadwidth.png"}

    # Vision 判斷
    from analysis.claude_analyzer import _encode_image, client, MODEL_VISION
    img_b64, media_type = _encode_image(str(screenshot_path))
    if not img_b64:
        return None

    roads_ref = ", ".join(f"{r['road_name']} {r['road_width_m']}m(距{r['distance_m']}m)" for r in all_roads)
    vision_prompt = f"""這是台北市都市計畫地圖的截圖，中心點是一棟建物，地址約為「{addr}」。

請判斷這棟建物（地圖中心位置）臨接哪些路，以及每條路的計畫道路寬度。

地圖上的道路寬度標示通常寫在路的旁邊或路中（例如「8M」「11M」「18M」）。
地籍線（細線）圍出的區塊是建物用地，道路是地籍線之間的空白區域。

GeoServer 查到附近的道路供參考：{roads_ref}

判斷規則：
- 先判斷建物是「單面臨路」還是「位於交叉口/角間，兩面以上臨路」。
- 若單面臨路：回傳該條路的資訊。
- 若位於交叉口（角間，兩面以上臨路）：at_corner=true，所有臨接的道路都要列在 roads；road_name/road_width_m 請選**其中寬度最大**的那條作為主要臨路。

請回傳 JSON，不要其他文字：
{{
  "at_corner": <true/false>,
  "roads": [{{"road_name": "路名1", "road_width_m": <數字>}}, ...],
  "road_name": "主要臨路（若交叉口：選最寬那條）",
  "road_width_m": <主要臨路寬度（若交叉口：最寬那條的寬度）>,
  "reason": "判斷理由（一句話）"
}}"""

    screenshot_rel = f"/data/screenshots/{src_id}_roadwidth.png"
    # (C) 無論 Vision 成敗，都會回 screenshot path 讓前端可以顯示地籍圖
    result = {"screenshot": screenshot_rel}

    try:
        resp = client.messages.create(
            model=MODEL_VISION, max_tokens=500,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": img_b64}},
                {"type": "text", "text": vision_prompt},
            ]}],
        )
        vision_text = resp.content[0].text.strip()
        logger.info(f"Vision road_width ({src_id}): {vision_text[:400]}")

        # (B) 韌性解析：優先嚴格 JSON；失敗時 fallback 到 regex 個別欄位抓取
        parsed = None
        m_json = re.search(r"\{.*\}", vision_text, re.DOTALL)
        if m_json:
            try:
                parsed = _json.loads(m_json.group(0))
            except Exception as je:
                logger.warning(f"Vision road_width JSON 解析失敗，改用 regex fallback: {je}")
        if parsed is None:
            # fallback regex：從自由文字抓出 road_name / road_width_m / reason / at_corner / roads
            parsed = {}
            nm = re.search(r'"road_name"\s*:\s*"([^"]+)"', vision_text)
            if nm: parsed["road_name"] = nm.group(1)
            wm = re.search(r'"road_width_m"\s*:\s*([\d.]+)', vision_text)
            if wm:
                try: parsed["road_width_m"] = float(wm.group(1))
                except Exception: pass
            rm = re.search(r'"reason"\s*:\s*"([^"]+)"', vision_text)
            if rm: parsed["reason"] = rm.group(1)
            cm = re.search(r'"at_corner"\s*:\s*(true|false)', vision_text)
            if cm: parsed["at_corner"] = (cm.group(1) == "true")
            # roads list：抓所有 {"road_name":"...", "road_width_m":N}
            parsed["roads"] = []
            for rr in re.finditer(r'\{\s*"road_name"\s*:\s*"([^"]+)"\s*,\s*"road_width_m"\s*:\s*([\d.]+)', vision_text):
                try:
                    parsed["roads"].append({"road_name": rr.group(1), "road_width_m": float(rr.group(2))})
                except Exception:
                    pass

        road_name = parsed.get("road_name", "")
        road_width = parsed.get("road_width_m")
        reason = parsed.get("reason", "")
        at_corner = bool(parsed.get("at_corner"))
        roads_listed = parsed.get("roads") or []
        # 交叉口保險：若 Vision 標 at_corner=true 但 roads 裡其實有更寬的，改用最寬那條
        if at_corner and isinstance(roads_listed, list) and len(roads_listed) >= 2:
            try:
                widest = max(
                    (r for r in roads_listed if r.get("road_width_m") is not None),
                    key=lambda r: float(r["road_width_m"]),
                    default=None,
                )
                if widest and float(widest["road_width_m"]) > float(road_width or 0):
                    logger.info(
                        f"交叉口選寬：Vision 原選 {road_name} {road_width}m → 改 {widest['road_name']} {widest['road_width_m']}m"
                    )
                    road_name = widest["road_name"]
                    road_width = widest["road_width_m"]
                    reason = f"位於交叉口，取臨接道路中最寬者：{road_name} {road_width}m；{reason}"
            except Exception:
                pass
        if road_name:
            # 權威來源：GeoServer。Vision 只負責挑路名，寬度數字**優先用 GeoServer** 對應路名的 width。
            # 若 GeoServer 沒收錄該路（e.g. Vision 細分到巷弄 GeoServer 沒有）→ 才標「寬度不明」。
            geo_match = next(
                (r for r in (all_roads or []) if (r.get("road_name") or "") == road_name),
                None,
            )
            if geo_match:
                # 用 GeoServer 的寬度，忽略 Vision 自己給的數字（Vision 可能 null 或瞎猜）
                result.update({
                    "road_width_m": float(geo_match.get("road_width_m")),
                    "road_name": road_name,
                    "reason": reason or "",
                })
            else:
                # GeoServer 沒收錄此路 → 寬度不明
                unknown_note = "（寬度不明，有可能為私巷或特窄巷弄）"
                result.update({
                    "road_name": road_name,
                    "reason": (reason + " " + unknown_note).strip() if reason else unknown_note,
                    "road_width_unknown": True,
                })
    except Exception as e:
        logger.warning(f"Vision road width 失敗: {e}")
    return result


def analyze_single_property(
    *,
    item: dict,
    ocr_ctx=None,
    step_fn=None,
    initial_coords: tuple = None,
    detail_text: str = "",
    thresholds: dict = None,
) -> dict:
    """
    共用分析 pipeline。

    Args:
        item: 已正規化的物件 dict（必須有 source_id, url, city, district, address 等）
        ocr_ctx: Playwright BrowserContext（給 zoning lookup 用，可為 None）
        step_fn: 進度回報函式 step_fn(msg: str)，可為 None
        initial_coords: (lat, lng) 起始座標（591 原生座標），沒有就靠 geocode
        detail_text: 原始文字（給法拍偵測用）
        thresholds: 跳過分析門檻 dict，None = 不做跳過檢查

    Returns:
        {
            "doc_data": dict,           # 組裝好的 Firestore document
            "status": "done" | "skipped" | "foreclosure",
            "skip_reason": str | None,
            "foreclosure_reasons": list | None,
        }
    """
    from analysis.geocoder import geocode_address, get_nearest_mrt
    from analysis.scorer import calculate_score, calculate_renewal_value, calculate_renewal_scenarios
    from analysis.claude_analyzer import analyze_property_text, generate_final_recommendation
    from analysis.lvr_index import triangulate_address, _extract_road_seg
    from analysis.gov_gis import query_road_width_taipei
    from scraper.zoning_lookup import lookup_zoning
    from database.models import (
        make_property_doc, make_minimal_doc,
        should_skip_analysis, detect_foreclosure,
    )

    def _step(msg):
        if step_fn:
            step_fn(msg)

    src_id = item["source_id"]
    city = item.get("city") or ""
    district = item.get("district") or ""

    # ── 1. 地址正規化：門牌格式統一 + 補樓層 + strip city/district 前綴（存純地址）──
    addr = item.get("address") or ""
    if addr:
        from analysis.claude_analyzer import _clean_address_garbage
        from database.models import strip_region_prefix
        addr = _clean_address_garbage(addr)
        if "號" in addr and "樓" not in addr and item.get("floor"):
            # floor 可能是 "2"、"3"、"2/4" 或 "2/4F"（主樓/總樓層）；只取斜線前的第一組數字
            f_raw = str(item["floor"])
            f_main = f_raw.split("/")[0]
            f_num_m = re.search(r"\d+", f_main)
            f_num = f_num_m.group(0) if f_num_m else ""
            if f_num:
                addr = addr + f"{f_num}樓"
        addr = strip_region_prefix(addr, city or "", district or "")
        item["address"] = addr

    # ── 1.5 路名真實性驗證 + Claude fuzzy 修正（諧音/錯字）──
    # 只在 city+district 內驗證；跨縣市同名路不算數。
    if item.get("address") and city and district:
        from analysis.geocoder import verify_and_fix_road
        from database.models import strip_region_prefix as _strip
        full_addr = f"{city}{district}{item['address']}"
        _step("驗證路名真實性...")
        vr = verify_and_fix_road(full_addr, city, district)
        if vr.get("status") == "fixed":
            # Claude 改了路名 → 用修正版覆寫 address
            new_addr = _strip(vr["address"], city, district)
            logger.info(
                f"[{src_id}] 路名修正：{vr.get('original_road')} → {vr.get('fixed_road')}  "
                f"地址 {item['address']!r} → {new_addr!r}"
            )
            item["address_road_fixed"] = {
                "from": vr.get("original_road"),
                "to": vr.get("fixed_road"),
            }
            item["address"] = new_addr
            # 路名改了 → 原本的 591 座標是基於「錯誤路名」geocode 出來的，一定對不上新地址。
            # 重新 geocode 新地址拿正確座標，覆蓋 initial_coords（後續 LVR / 路寬 / 分區 全靠這組座標）
            try:
                new_coords = geocode_address(vr["address"])
                if new_coords:
                    logger.info(f"[{src_id}] 路名修正後 re-geocode 座標：{initial_coords} → {new_coords}")
                    initial_coords = new_coords
                else:
                    logger.warning(f"[{src_id}] 路名修正後 re-geocode 失敗，沿用舊座標（可能導致分區/路寬偏差）")
            except Exception as ge:
                logger.warning(f"[{src_id}] re-geocode 例外：{ge}")
        elif vr.get("status") == "invalid":
            # 路在 city+district 內不存在且無法修正 → 標可疑，保留原地址讓用戶自己看
            logger.warning(f"[{src_id}] 地址可疑：{vr.get('reason')}")
            item["address_suspicious"] = True
            item["address_suspicious_reason"] = vr.get("reason")

    # ── 2. 法拍偵測 ──
    is_fc, fc_reasons = detect_foreclosure(item, detail_text)
    if is_fc:
        item["is_foreclosure"] = True
        item["foreclosure_reasons"] = fc_reasons

    # ── 3. 跳過分析檢查 ──
    if thresholds is not None:
        skip, reason = should_skip_analysis(item, thresholds)
        if skip:
            lat, lng = initial_coords or (None, None)
            if not (lat and lng) and item.get("address"):
                # 純地址 + city/district 拼回給 geocoder
                coords = geocode_address(f"{city}{district}{item['address']}")
                if coords:
                    lat, lng = coords
            nearest_mrt, mrt_dist = get_nearest_mrt(lat, lng) if lat else (None, None)
            land_sqm = (item["land_area_ping"] * 3.30578) if item.get("land_area_ping") else None
            minimal = make_minimal_doc(
                item=item, lat=lat, lng=lng,
                nearest_mrt=nearest_mrt, mrt_dist=mrt_dist,
                land_sqm=land_sqm, skip_reason=reason,
            )
            if is_fc:
                minimal["is_foreclosure"] = True
                minimal["foreclosure_reasons"] = fc_reasons
            _cleanup_ephemeral_screenshots(src_id)
            return {"doc_data": minimal, "status": "skipped", "skip_reason": reason, "foreclosure_reasons": None}

    # ── 4. 起始座標 ──
    lat, lng = initial_coords or (None, None)
    addr_pure = item.get("address") or ""   # 純地址（無 city/district 前綴）
    # 給 geocode / LVR 用的完整地址（LVR 內部會自己 strip，但 geocode 需要 prefix）
    addr_for_geo = f"{city}{district}{addr_pure}" if addr_pure else ""
    addr_has_number = "號" in addr_pure

    # ── 5. LVR 地址反推（先跑，分區和路寬要用推測座標）──
    _step("LVR 地址反推...")
    inferred_coord = None
    # _extract_road_seg 接受完整地址（它自己會 strip），傳 addr_for_geo 穩
    _, road_seg, lane_hint = _extract_road_seg(addr_for_geo)
    # 額外抽「弄」做為「同批建案」判定輔助（例：109巷25弄 → "25弄"）
    alley_hint = None
    _alley_m = re.search(r"\d+弄", addr_for_geo or "")
    if _alley_m:
        alley_hint = _alley_m.group(0)
    try:
        t = triangulate_address(
            city=city, district=district, road_seg=road_seg,
            total_floors=item.get("total_floors"),
            building_area_ping=item.get("building_area_ping"),
            coord=(lat, lng) if lat else None,
            floor=str(item.get("floor")) if item.get("floor") else None,
            lane_hint=lane_hint,
            alley_hint=alley_hint,
            land_area_ping=item.get("land_area_ping"),
        )
        item["lvr_records"] = t.get("lvr_records", [])[:20]
        # 屋齡回推：若 item 沒帶 building_age（如 manual 物件、或卡片沒寫），
        # 從 LVR records 的 year_completed 算「當前屋齡」(每次重抓會自動更新)
        if not item.get("building_age") and item.get("lvr_records"):
            yrs = [r.get("year_completed") for r in item["lvr_records"]
                   if isinstance(r.get("year_completed"), int) and r.get("year_completed") > 1900]
            if yrs:
                # 取最早年份（同棟可能有多筆，建築完工年該一致；保守取最小）
                completed = min(yrs)
                from datetime import datetime as _dt
                age = _dt.now().year - completed
                if 0 <= age <= 200:
                    item["building_age"] = age
                    item["building_age_source"] = "lvr_year_completed"
                    item["building_age_completed_year"] = completed
                    logger.info(f"[{src_id}] 屋齡從 LVR 回推：完工 {completed} 年 → 屋齡 {age}")
        lvr_land = t.get("lvr_land_ping")
        if lvr_land:
            if not item.get("land_area_ping"):
                item["land_area_ping"] = lvr_land
                item["land_area_source"] = t.get("land_area_source") or "lvr"
        # 地坪 loose match 警告（±0.01 沒中但 ±0.1 內，視為同物件）：
        # 只在 591 地址模糊（無號）時生效——有號時 591 地址可信，不做強制覆蓋
        if t.get("land_area_mismatch_warning") and not addr_has_number:
            if lvr_land and item.get("land_area_ping") != lvr_land:
                item["land_area_ping"] = lvr_land
                item["land_area_source"] = "lvr"
            item["land_area_mismatch_warning"] = True
        # LVR 分散警告（路級 + 無地坪 + LVR 地坪 std 大）：
        # 候選清單有但統計不一致，提醒用戶選擇前需驗證
        if t.get("land_area_inconsistent"):
            item["land_area_inconsistent"] = True
        # 候選下拉清單：
        #   - 591 沒完整地址 → 一定要列候選讓用戶挑
        #   - 591 有號、但 LVR 候選 ≥ 2 筆（同棟不同門牌，如 34/38 號）→ 也列候選，
        #     因為 OCR 對數字常誤讀（8↔9、3↔4），且同棟建物門牌多個時 591 寫的號不一定是這戶真正的號
        cds_detail = t.get("candidates_detail")
        if cds_detail and (not addr_has_number or len(cds_detail) >= 2):
            item["address_inferred_candidates_detail"] = cds_detail
            # 有號但被列候選 → 同時寫 address_inferred 預設值（LVR 最新交易優先）
            if addr_has_number and t.get("address"):
                from analysis.claude_analyzer import _clean_address_garbage
                from database.models import strip_region_prefix
                _cleaned = _clean_address_garbage(t["address"])
                item["address_inferred"] = strip_region_prefix(_cleaned, city or "", district or "")
                item["address_inferred_confidence"] = t.get("confidence")
                item["address_inferred_candidates"] = t["candidates"][:10] if t.get("candidates") else None

        # LVR 若 confidence=unique（同建坪+同土地坪唯一匹配）→ 即使 OCR 已有號也用 LVR 覆蓋。
        # OCR 對數字本就容易看錯（9↔8、2↔3、6↔8），而 LVR 是實登檔案、無模糊空間。
        lvr_address = t.get("address") if t.get("address") and "號" in t["address"] else None

        def _num_of(a):
            m = re.search(r"(\d+(?:[-之]\d+)?)號", a or "")
            return m.group(1).replace("之", "-") if m else None

        ocr_num = _num_of(addr_for_geo) if addr_has_number else None
        lvr_num = _num_of(lvr_address)

        if lvr_address and t.get("confidence") == "unique" and lvr_num and lvr_num != ocr_num:
            # LVR 強 override：門牌不一致 → LVR 為準
            logger.info(
                f"  LVR 強覆蓋地址：OCR {ocr_num}號 → LVR {lvr_num}號 (confidence=unique, 同建坪+同土地)"
            )
            from analysis.claude_analyzer import _clean_address_garbage
            from database.models import strip_region_prefix
            _cleaned = _clean_address_garbage(lvr_address)
            item["address_inferred"] = strip_region_prefix(_cleaned, city or "", district or "")
            item["address_inferred_confidence"] = "unique_override"
            item["address_inferred_candidates"] = t["candidates"][:10] if t["candidates"] else None
            # ⚠ LVR triangulate 回的 address 沒帶 city/district 前綴（例：「信義路82號1樓」）
            # 直接 geocode 會被 Google 解到全台同名路（永和信義路 → 高雄信義路）→ 必須拼前綴
            _lvr_full = f"{city or ''}{district or ''}{lvr_address}" if not lvr_address.startswith(("台北市","新北市")) else lvr_address
            inferred_coord = geocode_address(_lvr_full)
        elif addr_has_number:
            # OCR 已有號 + LVR 沒強烈反對 → 信任 OCR
            inferred_coord = geocode_address(addr_for_geo)
        elif lvr_address:
            # OCR 沒號 → 用 LVR 推測
            from analysis.claude_analyzer import _clean_address_garbage
            from database.models import strip_region_prefix
            _cleaned = _clean_address_garbage(lvr_address)
            item["address_inferred"] = strip_region_prefix(_cleaned, city or "", district or "")
            item["address_inferred_confidence"] = t["confidence"]
            item["address_inferred_candidates"] = t["candidates"][:10] if t["candidates"] else None
            _lvr_full = f"{city or ''}{district or ''}{lvr_address}" if not lvr_address.startswith(("台北市","新北市")) else lvr_address
            inferred_coord = geocode_address(_lvr_full)
    except Exception as te:
        logger.warning(f"LVR 失敗 {src_id}: {te}")

    # ── 5.5 LVR 無 match 但 address 有具體巷名 → 用地址 geocode 取代偏的 591 座標 ──
    # 591 座標常偏 50-150m，若落在隔壁 polygon → zoning 查錯。
    # 只要 address 有「X 路/街 + N 巷」層級，Google geocode 能給巷口座標（比 591 偏更準）。
    if not inferred_coord and addr_for_geo:
        import re as _re_lane
        if _re_lane.search(r"[一-龥]{1,5}(?:路|街|大道)(?:[一二三四五六七八九十]段)?\d+巷", addr_for_geo):
            try:
                _c = geocode_address(addr_for_geo)
                if _c:
                    inferred_coord = _c
                    logger.info(f"[{src_id}] LVR 無 match，用地址 geocode 校正座標: {_c}")
            except Exception as _ge:
                logger.warning(f"[{src_id}] 地址 geocode 失敗: {_ge}")

    # ── 5.6 永慶 / 信義（地址只到路段、但有準確座標）→ Google reverse geocode 拿具體門牌 ──
    # 條件：
    # 1. address 沒有「號」（只到路段或巷弄）
    # 2. source 是永慶/信義（座標精度高，房仲後台維護過 ±10m）
    # 591 座標常偏 50-150m → 不可信，不走 reverse geocode（保留原地址即可）
    # CLAUDE.md 規則 6 提過 591 座標問題。
    src_name = item.get("source") or "591"
    trust_source_coords = src_name in ("永慶", "信義")
    if not item.get("address_inferred") and not addr_has_number and trust_source_coords:
        src_lat = item.get("source_latitude") or item.get("latitude")
        src_lng = item.get("source_longitude") or item.get("longitude")
        had_real_source_coord = bool(src_lat and src_lng)
        # 永慶 Playwright 偶會 timeout 拿不到 page 座標 → 此時 src_lat 都是 None
        # fallback：用 geocode_address(街級地址) 拿一個近似源點，再用此源點 reverse_geocode
        # 比「整個 reverse_geocode 不跑、address_inferred=None」好（街級 reverse 有 confidence 標）
        if not had_real_source_coord and addr_for_geo:
            try:
                _c = geocode_address(addr_for_geo)
                if _c:
                    src_lat, src_lng = _c
                    logger.info(
                        f"[{src_id}] {src_name} 無 page 座標 → 街級 geocode 當源點: {_c}"
                    )
            except Exception as _ce:
                logger.warning(f"[{src_id}] 街級 geocode fallback 失敗: {_ce}")
        if src_lat and src_lng and road_seg:
            rev_addr = None
            try:
                from analysis.lvr_index import _reverse_geocode_lane, _reverse_geocode_loose
                # 第一次嘗試：嚴格模式（同路名 + 同巷 + 排除 RANGE_INTERPOLATED）
                rev_addr = _reverse_geocode_lane(src_lat, src_lng, road_seg, lane_hint or "")
                if not rev_addr:
                    # Fallback：寬鬆模式 — 直接打 Google reverse，只要結果含同路名+號就接受
                    # 適用永慶（有準確座標 + 該段沒巷弄）
                    rev_addr = _reverse_geocode_loose(src_lat, src_lng, road_seg)
            except Exception as _re:
                logger.warning(f"[{src_id}] reverse geocode 失敗: {_re}")

            if rev_addr:
                from analysis.claude_analyzer import _clean_address_garbage
                from database.models import strip_region_prefix
                _cleaned = _clean_address_garbage(rev_addr)
                item["address_inferred"] = strip_region_prefix(_cleaned, city or "", district or "")
                # 區分 confidence：page 座標 reverse vs 街級 geocode fallback reverse（後者誤差較大）
                item["address_inferred_confidence"] = (
                    "geocode_reverse" if had_real_source_coord else "geocode_reverse_loose"
                )
                logger.info(f"[{src_id}] reverse geocode 拿到具體地址: {rev_addr} (confidence={item['address_inferred_confidence']})")
            else:
                logger.info(f"[{src_id}] reverse geocode 無結果 (lat={src_lat}, lng={src_lng}, road={road_seg})")

    # ── 5.7 591 LVR 空 + 原地址含巷弄 → 保留原模糊地址（不硬給門牌）──
    # 591 座標不準 (±150m) 不該用 591 原座標 reverse_geocode；但保留巷弄地址可讓路寬/分區查得到。
    # 寧可顯示「松江路313巷」+ 標 confidence=lane_only，比硬給「松江路327號」誤導用戶好。
    import re as _re_lane2
    if not item.get("address_inferred") and src_name == "591" and item.get("address"):
        _orig = item.get("address") or ""
        if _re_lane2.search(r"[巷弄]", _orig):
            from database.models import strip_region_prefix
            item["address_inferred"] = strip_region_prefix(_orig, city or "", district or "")
            item["address_inferred_confidence"] = "lane_only"
            logger.info(f"[{src_id}] LVR 空 → 保留原巷弄地址當推測: {item['address_inferred']}")

    # ── 5.8 591 LVR 空 + 沒巷弄（只到路名）→ 用街級 geocode + reverse_geocode 拿近似號 ──
    # 注意：591 source_latitude 不可信（±150m），但 geocode_address(街級地址) 結果是 Google 級
    # 街中段座標，準度跟永慶 fallback 同源。reverse_geocode loose 拿一個 nearby 號當推測，
    # 標 confidence=geocode_reverse_loose 提示用戶誤差較大（前端 fallback 顯示「≈推測」）。
    if not item.get("address_inferred") and src_name == "591" and addr_for_geo and road_seg:
        try:
            _c_for_rev = geocode_address(addr_for_geo)
            if _c_for_rev:
                from analysis.lvr_index import _reverse_geocode_loose
                _rev = _reverse_geocode_loose(_c_for_rev[0], _c_for_rev[1], road_seg)
                if _rev:
                    from analysis.claude_analyzer import _clean_address_garbage
                    from database.models import strip_region_prefix
                    _cleaned = _clean_address_garbage(_rev)
                    item["address_inferred"] = strip_region_prefix(_cleaned, city or "", district or "")
                    item["address_inferred_confidence"] = "geocode_reverse_loose"
                    logger.info(f"[{src_id}] 591 街級 reverse 拿到推測地址: {_rev}")
        except Exception as _re591:
            logger.warning(f"[{src_id}] 591 街級 reverse fallback 失敗: {_re591}")

    # ── 6. 精準座標覆蓋 ──
    if inferred_coord:
        lat, lng = inferred_coord
    else:
        # re-geocode 失敗：留 591 原始座標 → zoning 會用那個座標查，可能偏到鄰近 polygon
        # 標記讓 admin 能用 regeocode_failed=true 找出所有受害 doc 批次重跑
        _addr_for_log = (
            item.get("address_inferred")
            or (t.get("address") if 't' in dir() and t and t.get("address") else None)
        )
        if _addr_for_log:
            logger.warning(
                f"[{src_id}] re-geocode 失敗 for {_addr_for_log!r} → "
                f"保留 591 原始座標 (lat={lat}, lng={lng})，zoning 可能偏"
            )
            item["regeocode_failed"] = True
            item["regeocode_failed_addr"] = _addr_for_log

    # 如果還沒有座標，嘗試 geocode 地址（即使只到巷級也給個大概位置）
    if not (lat and lng) and addr_for_geo:
        coords = geocode_address(addr_for_geo)
        if coords:
            lat, lng = coords

    land_sqm = (item["land_area_ping"] * 3.30578) if item.get("land_area_ping") else None

    # ── 7. MRT ──
    from analysis.geocoder import get_nearest_mrt_exit, get_nearby_mrt_stations
    nearest_mrt, mrt_exit, mrt_dist = get_nearest_mrt_exit(lat, lng) if lat else (None, None, None)
    nearby_mrts = get_nearby_mrt_stations(lat, lng, max_dist_m=1500, top_n=3) if lat else []

    # ── 8. 評分 + AI 分析 ──
    _step("評分計算...")
    scores = calculate_score(
        building_age=item.get("building_age"),
        land_area_sqm=land_sqm,
        nearest_mrt_dist_m=mrt_dist,
    )
    renewal = calculate_renewal_value(
        land_area_sqm=land_sqm, legal_far=None,
        building_age=item.get("building_age"),
        nearest_mrt_dist_m=mrt_dist,
        price_ntd=item.get("price_ntd"),
        city=city,
    )
    _step("AI 文字分析...")
    text_analysis = analyze_property_text({
        **item,
        "nearest_mrt": nearest_mrt,
        "nearest_mrt_dist_m": mrt_dist,
    })
    _step("AI 建議產出...")
    final = generate_final_recommendation(
        property_data=item, score=scores,
        renewal_calc=renewal, text_analysis=text_analysis,
    )

    # ── 9. 組裝 doc ──
    doc_data = make_property_doc(
        item=item, scores=scores, renewal=renewal,
        text_analysis=text_analysis, final=final,
        lat=lat, lng=lng,
        nearest_mrt=nearest_mrt, mrt_dist=mrt_dist, mrt_exit=mrt_exit,
        land_sqm=land_sqm,
    )
    doc_data["nearby_mrts"] = nearby_mrts   # 1500m 內最多 3 站；無則空 list

    # 偏遠區判定（新北市天險隔開的偏遠地段，依 config.REMOTE_POLYGONS_NEW_TAIPEI）
    # 前端預設過濾 is_remote_area=True，需勾「☐ 包含偏遠地段」才顯示
    from analysis.geocoder import is_remote_area_new_taipei
    doc_data["is_remote_area"] = is_remote_area_new_taipei(lat, lng, district)

    if is_fc:
        doc_data["is_foreclosure"] = True
        doc_data["foreclosure_reasons"] = fc_reasons
    # LVR 資料
    doc_data["lvr_records"] = item.get("lvr_records", [])
    if item.get("address_inferred"):
        doc_data["address_inferred"] = item["address_inferred"]
        doc_data["address_inferred_confidence"] = item.get("address_inferred_confidence")
        doc_data["address_inferred_candidates"] = item.get("address_inferred_candidates")
    if item.get("land_area_source"):
        doc_data["land_area_source"] = item["land_area_source"]
    if item.get("address_inferred_candidates_detail"):
        doc_data["address_inferred_candidates_detail"] = item["address_inferred_candidates_detail"]
    if item.get("land_area_mismatch_warning"):
        doc_data["land_area_mismatch_warning"] = True
    if item.get("land_area_inconsistent"):
        doc_data["land_area_inconsistent"] = True
    if item.get("address_road_fixed"):
        doc_data["address_road_fixed"] = item["address_road_fixed"]
    if item.get("address_suspicious"):
        doc_data["address_suspicious"] = True
        doc_data["address_suspicious_reason"] = item.get("address_suspicious_reason")
    if item.get("regeocode_failed"):
        doc_data["regeocode_failed"] = True
        doc_data["regeocode_failed_addr"] = item.get("regeocode_failed_addr")

    # ── 10. 路寬（GeoServer + zonemap 截圖 + Vision）──
    # 規則：lat 有值且在台北市就一律截 zonemap（供肉眼驗證）；
    #      Vision 判斷只在「地址沒對上任何 GeoServer 路」時才跑，省 API 錢
    _inferred_pure = item.get("address_inferred")
    precise_addr = (f"{city}{district}{_inferred_pure}" if _inferred_pure else addr_for_geo)
    if lat and city in ("台北市", "新北市"):
        try:
            _step("查臨路寬度...")
            if city == "新北市":
                from analysis.gov_gis import query_road_width_newtaipei
                rw = query_road_width_newtaipei(lat, lng, address_hint=precise_addr)
            else:
                rw = query_road_width_taipei(lat, lng, address_hint=precise_addr)
            if rw:
                doc_data["road_width_m"] = rw["road_width_m"]
                doc_data["road_width_name"] = rw["road_name"]
                doc_data["road_width_all"] = rw["all_roads"][:5]

            # 強規則：地址若明確寫在某「路/街/大道」上（非純巷弄），該路的寬度優先於 Vision
            # ★ 先砍城市/區前綴避免貪婪 regex 把「信義區永吉路」整段當成路名
            import re as _re_road
            _addr_stripped = _re_road.sub(
                r"^(台北市|臺北市|新北市|桃園市|基隆市|新竹市|新竹縣|宜蘭縣)", "", precise_addr
            )
            _addr_stripped = _re_road.sub(r"^[一-龥]{1,3}區", "", _addr_stripped)
            addr_road_match = _re_road.search(r"([一-龥]{1,5}(?:路|街|大道))([一二三四五六七八九十]段)?", _addr_stripped)
            addr_road = None
            if addr_road_match:
                addr_road = addr_road_match.group(1) + (addr_road_match.group(2) or "")

            # 擴展：抓到「X路Y段Z巷W弄」層級（含弄），優先從最具體層級做 exact match
            # 例：「永吉路278巷47弄15號」→ addr_lane_full="永吉路278巷47弄"
            addr_lane_match = _re_road.search(
                r"([一-龥]{1,5}(?:路|街|大道)(?:[一二三四五六七八九十]段)?\d+巷(?:\d+弄)?)", _addr_stripped
            )
            addr_lane_full = addr_lane_match.group(1) if addr_lane_match else None
            # 降級候選：若 full 是「X路Y段Z巷W弄」但找不到，退一步試「X路Y段Z巷」
            addr_lane_fallback = None
            if addr_lane_full and "弄" in addr_lane_full:
                addr_lane_fallback = addr_lane_full.rsplit("巷", 1)[0] + "巷"

            matched_main_road = None
            # 優先：完整「路+段+巷+弄」exact match
            if addr_lane_full and rw and rw.get("all_roads"):
                for r in rw["all_roads"]:
                    if (r.get("road_name") or "") == addr_lane_full:
                        matched_main_road = r
                        break
            # 降級：若 full 是「X路Y段Z巷W弄」沒 match，退一層試「X路Y段Z巷」
            if not matched_main_road and addr_lane_fallback and rw and rw.get("all_roads"):
                for r in rw["all_roads"]:
                    if (r.get("road_name") or "") == addr_lane_fallback:
                        matched_main_road = r
                        break
            # 若 all_roads 沒有這條巷 → 直接 CQL 補查（座標偏了導致 bbox 漏抓時的保險）
            if not matched_main_road and addr_lane_full:
                try:
                    import httpx as _httpx
                    from analysis.gov_gis import TAIPEI_WFS_URL, TAIPEI_ROADSIZE_TYPENAME
                    cr = _httpx.get(
                        TAIPEI_WFS_URL,
                        params={
                            "service": "WFS", "request": "GetFeature", "version": "1.0.0",
                            "outputFormat": "json", "typename": TAIPEI_ROADSIZE_TYPENAME,
                            "CQL_FILTER": f"road_name1='{addr_lane_full}'",
                            "maxFeatures": 3,
                        },
                        timeout=15, verify=False,
                    )
                    feats = (cr.json() or {}).get("features", [])
                    for f2 in feats:
                        wstr = (f2.get("properties", {}).get("road_width") or "").replace("M", "").replace("m", "").strip()
                        try:
                            matched_main_road = {
                                "road_name": addr_lane_full,
                                "road_width_m": float(wstr),
                                "distance_m": 0.0,
                            }
                            logger.info(
                                f"[{src_id}] 路寬 CQL 補查命中：{addr_lane_full} = {matched_main_road['road_width_m']}m"
                            )
                            existing = doc_data.get("road_width_all") or []
                            if not any((x.get("road_name") or "") == addr_lane_full for x in existing):
                                existing.insert(0, matched_main_road)
                                doc_data["road_width_all"] = existing[:5]
                            break
                        except Exception:
                            continue
                except Exception as ce:
                    logger.warning(f"[{src_id}] CQL 補查失敗: {ce}")
            # ★ 關鍵行為：地址明確在「巷弄」內（addr_lane_full 有值）但 all_roads + CQL 都沒命中
            # → 不退回主道模糊匹配（舊行為會把「虎林街256巷5號」錯標成「虎林街」）
            # → 直接標 road_width_unknown + road_width_name=addr_lane_full，符合「禁止幻覺」原則
            lane_missing_unknown = False
            if not matched_main_road and addr_lane_full:
                lane_missing_unknown = True
            # 若地址只到「路」級（非巷弄）且規則還沒命中 → 退回主道精確匹配
            # 只接受「exact」或「段延伸」（如地址「和平東路」可匹配 all_roads 中「和平東路一段」），
            # 不接受「X路 → X路Y巷」這類岔出去的巷弄（那是另一條路，不是同一條路的延伸）。
            elif not matched_main_road and addr_road and rw and rw.get("all_roads"):
                addr_road_base = addr_road.rstrip("段").rstrip("一二三四五六七八九十")
                for r in rw["all_roads"]:
                    rn = r.get("road_name") or ""
                    if not rn:
                        continue
                    # 1) exact match（地址寫「中山北路二段」= all_roads 有「中山北路二段」）
                    if rn == addr_road:
                        matched_main_road = r
                        break
                    # 2) 段延伸：地址只寫「和平東路」（沒段），all_roads 有「和平東路一段」→ 接受
                    #    條件：rn 必須是 addr_road_base + 「段」結尾，後面不能接「\d+巷」等巷弄
                    import re as _re_seg
                    if _re_seg.fullmatch(rf"{_re_seg.escape(addr_road_base)}[一二三四五六七八九十]段", rn):
                        matched_main_road = r
                        break
                # 若以上都沒命中 → 地址看似在「X路」上但 all_roads 真的沒這條
                # → 留給 Vision 處理（不硬 fallback 到岔巷，避免「詔安街24號」被錯標成「詔安街26巷」）

            # 無論規則命中與否，都截 zonemap 圖給肉眼驗證
            # 台北市：用 zonemap.udd.gov.taipei + Playwright（既有邏輯）
            # 新北市：用 NTPC ArcGIS export（直接 HTTP GET 拿 PNG，快很多）
            try:
                _step("截圖 zonemap 供驗證...")
                if city == "新北市":
                    from analysis.gov_gis import fetch_zoning_map_image_newtaipei
                    from config import BASE_DIR
                    out_path = BASE_DIR / "data" / "screenshots" / f"{src_id}_roadwidth.png"
                    if fetch_zoning_map_image_newtaipei(lat, lng, str(out_path)):
                        doc_data["screenshot_roadwidth"] = f"/data/screenshots/{src_id}_roadwidth.png"
                    scan_result = None   # 新北版不跑 Vision road-width OCR（已用 NTPC ArcGIS API 拿到精確值）
                else:
                    scan_result = _scan_road_width_vision(
                        lat=lat, lng=lng, addr=precise_addr,
                        district=district, src_id=src_id,
                        all_roads=rw["all_roads"][:6] if rw else [],
                        browser_ctx=ocr_ctx,
                        skip_vision=bool(matched_main_road),   # 規則命中 → 只截圖不跑 Vision
                    )
                    if scan_result and scan_result.get("screenshot"):
                        doc_data["screenshot_roadwidth"] = scan_result["screenshot"]
                # 地址在巷弄但 all_roads + CQL 都查不到 → 寬度不明（不 fallback 到主道）
                if lane_missing_unknown:
                    doc_data["road_width_name"] = addr_lane_full
                    # 查同段（同路+同段）其他巷弄的寬度當參考值給用戶眼測
                    nearby_hint = ""
                    try:
                        import re as _re_seg
                        # 從 addr_lane_full 抽出「X路Y段」prefix（例：仁愛路四段496巷 → 仁愛路四段）
                        seg_m = _re_seg.match(
                            r"([一-龥]+(?:路|街|大道)(?:[一二三四五六七八九十]段)?)",
                            addr_lane_full,
                        )
                        if seg_m:
                            seg_prefix = seg_m.group(1)
                            from analysis.gov_gis import TAIPEI_WFS_URL, TAIPEI_ROADSIZE_TYPENAME
                            import httpx as _httpx
                            r2 = _httpx.get(
                                TAIPEI_WFS_URL,
                                params={
                                    "service": "WFS", "request": "GetFeature", "version": "1.0.0",
                                    "outputFormat": "json", "typename": TAIPEI_ROADSIZE_TYPENAME,
                                    "CQL_FILTER": f"road_name1 LIKE '{seg_prefix}%巷'",
                                    "maxFeatures": 200,
                                },
                                timeout=10, verify=False,
                            )
                            seen_w = {}
                            for f3 in (r2.json() or {}).get("features", []):
                                p3 = f3.get("properties", {})
                                rn = p3.get("road_name1") or ""
                                # 只要「X路Y段Z巷」純路+段+巷（不要弄）
                                if not _re_seg.fullmatch(rf"{_re_seg.escape(seg_prefix)}\d+巷", rn):
                                    continue
                                w = (p3.get("road_width") or "").replace("M", "").replace("m", "").strip()
                                try: w_f = float(w)
                                except Exception: continue
                                seen_w.setdefault(rn, w_f)
                            if seen_w:
                                widths = sorted(set(seen_w.values()))
                                if len(widths) == 1:
                                    nearby_hint = f"（{seg_prefix}所有收錄巷弄都是 {widths[0]:.0f}m，可能寬度相近）"
                                else:
                                    # 多個值：算眾數 + 範圍
                                    from collections import Counter
                                    cnt = Counter(seen_w.values())
                                    common_w, common_n = cnt.most_common(1)[0]
                                    total = len(seen_w)
                                    nearby_hint = (
                                        f"（{seg_prefix}附近巷弄寬度多為 {common_w:.0f}m"
                                        f"（{common_n}/{total} 條），範圍 {min(widths):.0f}~{max(widths):.0f}m）"
                                    )
                    except Exception as _ne:
                        logger.debug(f"nearby hint 失敗: {_ne}")
                    doc_data["road_width_vision_reason"] = (
                        f"地址在「{addr_lane_full}」內，該巷弄未登記於政府路寬圖資，寬度不明。{nearby_hint}"
                    )
                    doc_data["road_width_unknown"] = True
                    doc_data.pop("road_width_m", None)
                # 規則命中：Vision 被跳過，寫入規則判定結果
                elif matched_main_road:
                    doc_data["road_width_m"] = float(matched_main_road["road_width_m"])
                    doc_data["road_width_name"] = matched_main_road["road_name"]
                    doc_data["road_width_vision_reason"] = (
                        f"地址位於「{matched_main_road['road_name']}」上，依政府路寬圖資該路寬度為 {matched_main_road['road_width_m']}m。"
                    )
                elif scan_result and "road_width_m" in scan_result:
                    # Vision 跑完有結果 + 路名有對上 GeoServer
                    doc_data["road_width_m"] = scan_result["road_width_m"]
                    doc_data["road_width_name"] = scan_result["road_name"]
                    doc_data["road_width_vision_reason"] = scan_result.get("reason", "")
                elif scan_result and scan_result.get("road_width_unknown"):
                    # Vision 判出路名但 GeoServer 沒收錄該巷弄 → 保留路名 + 標「寬度不明」，不寫 road_width_m
                    # 過濾 Vision 原始 reason 裡的「（X M）」寬度數字：GeoServer 沒這條路時，
                    # Vision 自己從圖上讀的寬度數字不可信（視覺 OCR 會誤讀）
                    doc_data["road_width_name"] = scan_result.get("road_name")
                    import re as _re_clean
                    _raw_reason = scan_result.get("reason", "")
                    _clean_reason = _re_clean.sub(
                        r"[（(]\s*\d+(?:\.\d+)?\s*[Mm]\s*[）)]", "", _raw_reason
                    ).strip()
                    doc_data["road_width_vision_reason"] = (
                        f"{_clean_reason} 該路未登記於政府路寬圖資，寬度不明。"
                        if _clean_reason else
                        "該路未登記於政府路寬圖資，寬度不明。"
                    )
                    doc_data["road_width_unknown"] = True
                    # 清掉之前從 GeoServer 拿到的 road_width_m（避免誤導）
                    doc_data.pop("road_width_m", None)
            except Exception as ve:
                logger.warning(f"zonemap scan 失敗 {src_id}: {ve}")
        except Exception as rwe:
            logger.warning(f"road width 失敗 {src_id}: {rwe}")

    # ── 11. 分區查詢 + renewal v2 即時算（不寫進 DB）──
    # CLAUDE.md 規則 8：renewal_v2 是動態計算結果，DB 只存輸入欄位（land/zoning/road_width/price），
    # 倍數/分回坪/有效容積率 由前端 + LINE 通知 hook 即時呼叫 calculate_renewal_scenarios 取得。
    rv2 = None
    if city in ("台北市", "新北市") and lat:
        try:
            _step("查土地分區...")
            z = lookup_zoning(
                address=addr_for_geo, lat=lat, lng=lng,
                building_area_ping=item.get("building_area_ping"),
                city=city, ctx=ocr_ctx,
            )
            zone_list = z.get("zone_list")
            # 物件座標跨多塊 polygon（如「住宅區+商業區」）→ zoning 顯示成「住宅區、商業區」全列出
            if zone_list and len(zone_list) > 1:
                zoning_display = "、".join(zone_list)
            else:
                zoning_display = z["zoning"]
            doc_data.update({
                "zoning": zoning_display,
                "zoning_candidates": z["zoning_candidates"],
                "zoning_source": z["zoning_source"],
                "zoning_source_url": z.get("zoning_source_url"),
                "zoning_lookup_at": z["zoning_lookup_at"],
                "zoning_error": z.get("error"),
                "zoning_original": z.get("original_zone"),
                "zoning_list": zone_list,
                "address_probable": z["address_probable"],
            })
            # 永慶詳情頁若標多分區（如「住宅區、商業區」表示基地跨分區）→ 取代 NTPC 點查單一分區
            # NTPC 點查只能拿到中心點所在 1 塊，但實際物件跨分區時用永慶版才精確
            yc_multi = item.get("_yongqing_zoning_multi")
            yc_orig = item.get("zoning_original")
            if yc_multi and len(yc_multi) > 1:
                doc_data["zoning"] = "、".join(yc_multi)
                doc_data["zoning_list"] = list(yc_multi)
                doc_data["zoning_original"] = yc_orig
                doc_data["zoning_source"] = "yongqing_detail_multi"
                logger.info(f"[{src_id}] 永慶 zoning 多分區（{yc_multi}）取代 NTPC 點查 ({z.get('zoning')})")

            # ── 都更可行性閘門（新北 4 區專屬） ──
            # 物件座標可能跨多塊 polygon（如「住宅區+商業區」）→ 任一 in SUITABLE 就算 suitable
            # 全部都是非實質用地（保護區/風景區/機關用地/河道用地等）→ unsuitable
            from analysis.scorer import is_zoning_suitable_for_renewal
            _zoning_for_check = doc_data.get("zoning_list") or doc_data.get("zoning")
            _suitable, _unsuitable_reason = is_zoning_suitable_for_renewal(district, _zoning_for_check)
            if not _suitable:
                doc_data["unsuitable_for_renewal"] = True
                doc_data["unsuitable_reason"] = _unsuitable_reason
                # 清掉都更/危老試算結果（保留分區/事實欄位）— 不適合都更就不該顯示倍數
                for _k in ("score_total", "score_age", "score_far", "score_land",
                           "score_tod", "score_road", "score_consolidation",
                           "renewal_type", "renewal_bonus_rate",
                           "renewal_new_area_ping", "renewal_value_ntd", "renewal_profit_ntd",
                           "ai_analysis", "ai_recommendation", "ai_reason"):
                    doc_data[_k] = None
                doc_data["analysis_status"] = "skipped"
                doc_data["skip_reason"] = "unsuitable_zoning"
                logger.info(f"[{src_id}] 不適合都更：{_unsuitable_reason}（{district}/{doc_data.get('zoning')}）")
                doc_data["analysis_completed_at"] = now_tw_iso()
                _cleanup_ephemeral_screenshots(src_id)
                return {"doc_data": doc_data, "status": "skipped",
                        "skip_reason": "unsuitable_zoning",
                        "foreclosure_reasons": fc_reasons if is_fc else None}

            # 有分區後即時算 renewal v2（local 變數，不存 DB）+ 重新產生建議
            final_land = item.get("land_area_ping") or doc_data.get("land_area_ping")
            if final_land:
                from analysis.scorer import resolve_effective_zoning
                effective_zoning = resolve_effective_zoning(z["zoning"], z.get("original_zone"))
                rv2 = calculate_renewal_scenarios(
                    land_area_ping=final_land,
                    zoning=effective_zoning,
                    district=district,
                    price_ntd=item.get("price_ntd"),
                    road_width_m=doc_data.get("road_width_m"),
                )
                final2 = generate_final_recommendation(
                    property_data={**item, **doc_data},
                    score=scores,
                    renewal_calc={"v2": rv2},
                    text_analysis=text_analysis,
                )
                doc_data["ai_recommendation"] = final2["recommendation"]
                doc_data["ai_reason"] = final2["reason"]
        except Exception as ze:
            logger.warning(f"zoning lookup 失敗 {src_id}: {ze}")

    doc_data["analysis_completed_at"] = now_tw_iso()

    # ── 高價值物件 LINE 通知 ──
    # 門檻：admin 在 settings/line_config.threshold_multiple 可調，預設 2.8
    # 任何情境（危老/都更/防災都更）的 multiple ≥ threshold → 推 LINE
    # 若已通知過（doc 有 line_notified_at）+ 倍數沒漲過顯著程度 → skip
    try:
        rv2_check = rv2 or {}
        scenarios_check = rv2_check.get("scenarios") or {}
        max_mult = 0.0
        max_scen = ""
        for name, s in scenarios_check.items():
            m = s.get("multiple")
            if m is not None and m > max_mult:
                max_mult = float(m)
                max_scen = name
        # 從 Firestore 讀門檻（預設 2.8）
        try:
            from database.db import get_firestore as _gf2
            _cfg = _gf2().collection("settings").document("line_config").get()
            _threshold = float((_cfg.to_dict() or {}).get("threshold_multiple", 2.8)) if _cfg.exists else 2.8
        except Exception:
            _threshold = 2.8
        if max_mult >= _threshold:
            # 跟既有 doc 比對：若已通知過 + 倍數差不多 → skip
            should_notify = True
            if item.get("_existing_doc"):
                old = item["_existing_doc"]
                last_notified_at = old.get("line_notified_at")
                last_notified_mult = old.get("line_notified_max_mult")
                if last_notified_at and last_notified_mult:
                    # 倍數沒漲超過 0.3 → 不重發
                    if max_mult - float(last_notified_mult) < 0.3:
                        should_notify = False
            if should_notify:
                from analysis.line_notify import notify_high_value_property
                ok = notify_high_value_property({**item, **doc_data}, max_mult, max_scen, rv2=rv2)
                doc_data["line_notified_at"] = now_tw_iso()
                doc_data["line_notified_max_mult"] = max_mult
                # 寫 LINE 通知紀錄到專屬 collection（含物件 id/地址，給 admin 看）
                try:
                    from database.db import get_firestore as _gf3
                    _gf3().collection("line_notifications").add({
                        "at": now_tw_iso(),
                        "doc_id": doc_data.get("id"),
                        "source_id": src_id,
                        "address": doc_data.get("address_inferred") or doc_data.get("address"),
                        "city": doc_data.get("city"),
                        "district": doc_data.get("district"),
                        "price_ntd": doc_data.get("price_ntd"),
                        "max_multiple": max_mult,
                        "scenario": max_scen,
                        "threshold_used": _threshold,
                        "delivered_ok": bool(ok),
                    })
                except Exception as _le2:
                    logger.warning(f"[{src_id}] LINE 通知 log 寫入失敗: {_le2}")
    except Exception as _le:
        logger.warning(f"[{src_id}] LINE 通知 hook 失敗: {_le}")

    # 清理一次性分析截圖（用戶永遠看不到的，只是 OCR/Vision 讀取素材）
    _cleanup_ephemeral_screenshots(src_id)
    return {
        "doc_data": doc_data,
        "status": "done",
        "skip_reason": None,
        "foreclosure_reasons": fc_reasons if is_fc else None,
    }


def _cleanup_ephemeral_screenshots(src_id: str) -> None:
    """刪除分析後不再需要的一次性截圖。
    保留：_roadwidth.png（前端地籍圖按鈕）、_cadastral/_zoning/_renewal.png（deep analysis，暫留）
    刪除：_detail.png / _addr.png / _house.png / _detail_tile_* / _house_tile_* / _detail_full.png"""
    from config import BASE_DIR
    folder = BASE_DIR / "data" / "screenshots"
    if not folder.exists():
        return
    patterns = [
        f"{src_id}_detail.png",
        f"{src_id}_detail_full.png",
        f"{src_id}_addr.png",
        f"{src_id}_house.png",
    ]
    deleted = 0
    for name in patterns:
        p = folder / name
        try:
            if p.exists():
                p.unlink()
                deleted += 1
        except Exception:
            pass
    # glob 刪 tile 切片：包含 detail / house / addr 三種來源的 2×2 切片
    for pattern in (f"{src_id}_detail_tile_*.png",
                    f"{src_id}_house_tile_*.png",
                    f"{src_id}_addr_tile_*.png"):
        for p in folder.glob(pattern):
            try: p.unlink(); deleted += 1
            except Exception: pass
    if deleted:
        logger.info(f"[{src_id}] cleaned {deleted} ephemeral screenshots")
