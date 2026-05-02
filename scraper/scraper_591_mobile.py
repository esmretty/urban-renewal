"""591 Mobile BFF API client — 取代 desktop 詳情頁 + Vision OCR 流程的「平行 path」。

== 為什麼有這個檔 ==
Desktop 詳情頁 (sale.591.com.tw) 用 Web Component (`<wc-text-floor0>` /
`<wc-ir-obfuscate-address-1>` 等) Shadow DOM 防爬，所有關鍵欄位（樓層、建坪、土地、
屋齡、地址）外部 DOM API 看不到 → 必須截圖 + Claude Vision OCR 才能讀取。
單筆耗時 50-90 秒、3 次 Vision API call、~$0.005-0.01。

Mobile API (bff-house.591.com.tw/v1/touch/sale/detail?id=S{houseid}) 回**純 JSON**，
所有欄位純文字無防爬。實測 591_20124339 直接給：
  data.floor             = '2F/4F'
  data.area_value        = '38.32'
  data.mainarea          = '38.32坪'
  data.area_intro_arr    = [{name:'土地坪數', value:'16.10坪'}, ...]
  data.age               = '59年'
  data.region/section/street/lane/alley/addr_number*
  data.price_value       = '2530'
  data.lat / data.lng

== 設計：平行 path ==
舊 OCR code (screenshot_detail_page / extract_full_detail_from_screenshot) 完全保留，
不在這條路徑碰到。如果哪天 591 封 mobile API → config.USE_591_MOBILE_API=False
立即切回舊 OCR path，no code change required。

== 用法 ==
    from scraper.scraper_591_mobile import fetch_mobile_detail

    detail = fetch_mobile_detail('20124339')   # 不需 'S' 前綴
    if detail:
        # detail 是 normalized item-shaped dict，可直接合併進 batch loop 的 item:
        #   item.update(detail)
        # 然後跳過 screenshot_detail_page + extract_full_detail_from_screenshot
        ...

回傳 dict 欄位（mapping 到既有 item schema）：
  floor, total_floors, building_area_ping, land_area_ping, building_age,
  city, district, address, source_latitude, source_longitude, community_name,
  building_type, price_ntd, price_per_ping, _mobile_raw (debug)

不存在的欄位不會出現在 dict 裡（caller 用 .get() 拿，跟現有 item 一樣）。
"""
from __future__ import annotations

import logging
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_API_BASE = "https://bff-house.591.com.tw/v1/touch/sale/detail"
_MOBILE_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
)
_HEADERS = {
    "User-Agent": _MOBILE_UA,
    "Accept": "application/json",
    "Accept-Language": "zh-TW,zh;q=0.9",
    "Referer": "https://m.591.com.tw/",
}


def _parse_ping(s) -> Optional[float]:
    """從「16.10坪」「-」等字串拆出數字；無效回 None。"""
    if not s:
        return None
    m = re.search(r"([\d.]+)", str(s))
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _parse_age(s) -> Optional[int]:
    """從「59年」拆出整數；屋齡 0 視為「屋主沒填」回 None（591 list 無預售屋）。"""
    if not s:
        return None
    m = re.search(r"(\d+)", str(s))
    if not m:
        return None
    try:
        n = int(m.group(1))
        return n if n > 0 else None
    except ValueError:
        return None


def _build_address(d: dict) -> str:
    """從 region/section/street/alley/lane/addr_number* 拼地址。
    591 mobile API schema：
      region   = '台北市'
      section  = '中正區'
      street   = '金門街'
      alley    = '34'    (= 34 巷；'x' 表示無)
      lane     = 'x'      (= 弄；'x' 表示無)
      addr_number  = 'x'  (主號；'x' 表示無)
      addr_number2 = '1'  (之 N 號；'x' 表示無)
      hide_addr_detail = '1' 時 591 後台已 mask 完整門牌
    """
    region = d.get("region") or ""
    section = d.get("section") or ""
    street = d.get("street") or ""
    alley = d.get("alley")
    lane = d.get("lane")
    n1 = d.get("addr_number")
    n2 = d.get("addr_number2")

    parts = [region, section, street]
    if alley and alley != "x":
        parts.append(f"{alley}巷")
    if lane and lane != "x":
        parts.append(f"{lane}弄")
    if n1 and n1 != "x":
        if n2 and n2 != "x":
            parts.append(f"{n1}之{n2}號")
        else:
            parts.append(f"{n1}號")
    elif n2 and n2 != "x":
        # 主號被 hide 但有「之 X」→ 不太合理，drop
        pass
    return "".join(parts)


def _land_area_ping_from_arr(area_intro_arr) -> Optional[float]:
    """從 data.area_intro_arr (list of {name, value}) 找土地坪數。
    591 不同物件 name 標法不一，可能是：
      '土地坪數'          (大部分案件)
      '土地（持分）坪數'  (永康街 591_20129938 等案件)
      '土地持分坪數' / '土地持分'
    比對策略：含「土地」AND（含「坪」OR含「持分」）→ 視為土地坪數欄位。
    """
    if not isinstance(area_intro_arr, list):
        return None
    for it in area_intro_arr:
        if not isinstance(it, dict):
            continue
        name = (it.get("name") or "").strip()
        if "土地" in name and ("坪" in name or "持分" in name):
            v = _parse_ping(it.get("value"))
            if v is not None:
                return v
    return None


def _land_area_ping_from_intro_str(area_intro: str) -> Optional[float]:
    """area_intro_arr 抓不到時，fallback 從 area_intro 字串拆。
    範例：'主建物38.09坪，附屬建物6.19坪，土地15.43坪'
          '主建物38.32坪，土地坪數16.10坪'"""
    if not area_intro:
        return None
    s = str(area_intro)
    # 找「土地」後第一個「\d+.\d+坪」
    m = re.search(r"土地[（(一-龥]*[）)]?(?:坪數)?\s*([\d.]+)\s*坪", s)
    if m:
        try:
            return float(m.group(1))
        except (ValueError, TypeError):
            pass
    return None


def fetch_mobile_detail(houseid: str, *, timeout: float = 20.0) -> Optional[dict]:
    """從 591 mobile BFF API 抓單筆 detail，回 normalized item dict。
    抓不到 / 物件下架 / 限流 → return None（caller 應 fallback 到舊 OCR path）。

    houseid 不需要 'S' 前綴；本函式自己加。

    Mobile API 涵蓋 desktop 詳情頁所有用得到的欄位（feature parity）：
      - 結構化資料：building/land/age/floor/price/lat/lng/community/shape
      - 法拍偵測：title / remark
      - 上架/更新時間：posttime / update（top-level body）
      - 建案資訊：casesname / cases_id / community_address
      - 圖片：photo / maxphoto / thumb
      - 標籤：feat_tag / good_house_tags
      - 格局/朝向/車位：layout / direction / parking
      - 聯絡人：linkman / identity
    """
    if not houseid:
        return None
    sid = str(houseid).lstrip("S")
    params = {
        "id": f"S{sid}",
        "is_business": 0,
        "region_id": 1,        # 台北；不影響 detail 內容（591 server 從 houseid 自己找）
        "device": "touch",
    }
    try:
        r = httpx.get(_API_BASE, params=params, headers=_HEADERS, timeout=timeout, verify=False)
    except (httpx.TimeoutException, httpx.RequestError) as e:
        logger.warning(f"  591 mobile API 連線失敗 ({sid}): {e}")
        return None
    if r.status_code == 429:
        logger.warning(f"  591 mobile API 被限流 (429) ({sid})")
        return None
    if r.status_code != 200:
        logger.warning(f"  591 mobile API http {r.status_code} ({sid})")
        return None
    try:
        body = r.json()
    except ValueError:
        logger.warning(f"  591 mobile API 回非 JSON ({sid})")
        return None
    # 591 mobile API status 可能是 int 1 / str "ok"；flag 通常 int
    _status = body.get("status")
    _flag = body.get("flag")
    _status_ok = (_status == 1) or (isinstance(_status, str) and _status.lower() == "ok")
    if not _status_ok:
        logger.info(f"  591 mobile API status={_status!r} flag={_flag!r} ({sid}) — 視為下架")
        return None
    d = body.get("data") or {}
    if not d:
        return None

    # === 欄位 mapping ===
    out: dict = {"_mobile_raw": d}   # debug/audit 用，存原始 raw payload

    # 樓層 — 直接給字串，下游 parse_floor_range 處理 'B1/5F' / '4F~5F/5F' 等
    if d.get("floor"):
        out["floor"] = d["floor"]

    # 建坪
    bld = _parse_ping(d.get("area_value") or d.get("area"))
    if bld is not None:
        out["building_area_ping"] = bld

    # 土地坪數（從 area_intro_arr 拆）
    land = _land_area_ping_from_arr(d.get("area_intro_arr"))
    if land is not None:
        out["land_area_ping"] = land

    # 屋齡
    age = _parse_age(d.get("age"))
    if age is not None:
        out["building_age"] = age

    # 城市/區
    if d.get("region"):
        out["city"] = d["region"]
    if d.get("section"):
        out["district"] = d["section"]

    # 地址
    addr = _build_address(d)
    if addr:
        out["address"] = addr
    if d.get("hide_addr_detail") in ("1", 1, True):
        out["_addr_hidden_by_591"] = True   # 給下游判定要不要 fallback OCR

    # 座標
    try:
        lat = float(d["lat"]) if d.get("lat") else None
        lng = float(d["lng"]) if d.get("lng") else None
        if lat and lng:
            out["source_latitude"] = lat
            out["source_longitude"] = lng
    except (ValueError, TypeError):
        pass

    # 社區名（591 用戶有填的話）
    if d.get("community"):
        out["community_name"] = d["community"]

    # 建物類型
    if d.get("shape"):
        out["building_type"] = d["shape"]

    # 價格
    pv = d.get("price_value")
    if pv:
        try:
            out["price_ntd"] = int(float(pv)) * 10000   # mobile 給「萬」
        except (ValueError, TypeError):
            pass
    if out.get("price_ntd") and out.get("building_area_ping"):
        out["price_per_ping"] = out["price_ntd"] / out["building_area_ping"]

    # === 取代 desktop screenshot_detail_page DOM 抓的欄位 ===

    # 標題：法拍偵測 + 通用顯示用
    if d.get("title"):
        out["title"] = d["title"]

    # 物件詳細描述（屋主自填）：補強法拍偵測（含「【法拍】」「銀拍」等關鍵字）
    if d.get("remark"):
        out["remark"] = d["remark"]

    # 上架時間（unix timestamp）→ ISO 字串
    pt = d.get("posttime")
    if pt:
        try:
            from datetime import datetime, timezone, timedelta
            ts = int(pt)
            tw = timezone(timedelta(hours=8))
            out["published_at"] = datetime.fromtimestamp(ts, tw).isoformat()
        except (ValueError, TypeError):
            pass

    # 更新時間（在 top-level body，不在 data 裡）
    upd = body.get("update")
    if upd:
        out["updated_at_591"] = str(upd)   # 已是 'YYYY-MM-DD HH:MM:SS' 格式

    # 建案資訊（591「建案」物件會有 casesname；一般物件 cases_id='0' 跳過）
    cases_id = str(d.get("cases_id") or d.get("casesID") or "0")
    if cases_id and cases_id != "0":
        if d.get("casesname"):
            out["case_name"] = d["casesname"]
        out["case_id"] = cases_id
        # community_address 是 list — 取第一筆當建案地址
        ca_list = d.get("community_address") or []
        if isinstance(ca_list, list) and ca_list:
            first = ca_list[0]
            if isinstance(first, str) and first.strip():
                out["community_address"] = first.strip()
            elif isinstance(first, dict):
                # 可能是 {address: '...'} 之類的格式
                for k in ("address", "addr", "value"):
                    if first.get(k):
                        out["community_address"] = str(first[k]).strip()
                        break

    # 圖片：取 maxphoto 第一張當主圖；photo 列拆成 list
    if d.get("photo"):
        photos_raw = str(d["photo"]).split("|*|")
        photos = [p for p in photos_raw if p and p.startswith("http")]
        if photos:
            out["photos"] = photos
            out["thumbnail_url"] = d.get("thumb") or photos[0]

    # 特性標籤：feat_tag / good_house_tags
    feat = d.get("feat_tag")
    if isinstance(feat, list) and feat:
        out["feat_tags"] = feat
    good = d.get("good_house_tags")
    if isinstance(good, list) and good:
        out["good_house_tags"] = good

    # 格局 / 朝向 / 管理費 / 車位
    if d.get("layout"):
        out["layout"] = d["layout"]
    if d.get("direction"):
        out["direction"] = d["direction"]
    if d.get("managefee"):
        out["managefee"] = d["managefee"]
    if d.get("parking"):
        out["parking"] = d["parking"]

    # 聯絡人 / 屋主標記（識別仲介或屋主）
    if d.get("linkman"):
        out["linkman"] = d["linkman"]
    if d.get("identity"):
        out["seller_identity"] = d["identity"]   # '屋主' / '仲介' / etc.

    # 法拍快速旗標：title / remark 含關鍵字 → 給下游 detect_foreclosure 用
    fc_signals = []
    for src_key in ("title", "remark"):
        v = (d.get(src_key) or "")
        if any(kw in v for kw in ("法拍", "銀拍", "金拍", "代標", "【拍", "拍定")):
            fc_signals.append(src_key)
    if fc_signals:
        out["_foreclosure_kw_in"] = fc_signals

    return out
