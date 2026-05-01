"""審計：新北市永慶/信義物件 DB zoning vs 網站原文土地分區。

用戶要求：找出所有新北市永慶/信義物件（排除三種例外 is_foreclosure/is_remote_area/
unsuitable_for_renewal），逐筆 fetch 原網頁的「土地使用分區」「使用分區」**完整字串**，
跟 DB 的 zoning 欄位做對照表。

輸出：
- print 對照表到 stdout
- 寫入 reports/zoning_audit_new_taipei_2026_05_01.md（給用戶看）
"""
import sys
import re
import json
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx
import urllib3
urllib3.disable_warnings()

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0"


def fetch_zoning_yongqing(url: str) -> tuple[str | None, str]:
    """回傳 (zoning_raw, note)。zoning_raw=None 表示抓不到。"""
    try:
        r = httpx.get(url, headers={"User-Agent": UA}, follow_redirects=True, timeout=20, verify=False)
        if r.status_code != 200:
            return None, f"http {r.status_code}"
        # 永慶 HTML: <h3>土地使用分區</h3></div><div class="item-detail">XXX</div>
        m = re.search(
            r'土地使用分區</h3>\s*</div>\s*<div[^>]*class="item-detail"[^>]*>([^<]+)</div>',
            r.text
        )
        if m:
            return m.group(1).strip(), "ok"
        # fallback: 「使用分區」周圍範圍
        m = re.search(r'使用分區[^a-zA-Z<]*</[^>]+>[^<]*<[^>]+>([一-龥、，,()（）一二三四五六七八九十第種住商工農業宅區、]{2,40})', r.text)
        if m:
            return m.group(1).strip(), "fallback regex"
        # check if 下架
        if "已下架" in r.text or "物件不存在" in r.text:
            return None, "下架"
        return None, "未在 HTML 找到「土地使用分區」"
    except Exception as e:
        return None, f"err: {e}"


def fetch_zoning_sinyi(url: str) -> tuple[str | None, str]:
    """回傳 (zoning_raw, note)。"""
    try:
        r = httpx.get(url, headers={"User-Agent": UA}, follow_redirects=True, timeout=20, verify=False)
        if r.status_code != 200:
            return None, f"http {r.status_code}"
        # 信義 HTML: <span class="...info-label...">使用分區</span><span>XXX;</span>
        m = re.search(
            r'<span[^>]*info-label[^>]*>使用分區</span>\s*<span[^>]*>([^<]+)</span>',
            r.text
        )
        if m:
            v = m.group(1).strip().rstrip(";").strip()
            return v, "ok"
        # fallback: NEXT_DATA
        nm = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>', r.text, re.DOTALL)
        if nm:
            try:
                data = json.loads(nm.group(1))
                cd = data.get("props", {}).get("initialReduxState", {}).get("buyReducer", {}).get("contentData") or {}
                for key in ("zoning", "useZoning", "landUseZoning", "useArea"):
                    v = cd.get(key)
                    if v:
                        return str(v).strip(), f"NEXT_DATA.{key}"
            except Exception:
                pass
        if "下架" in r.text or "查無此物件" in r.text:
            return None, "下架"
        return None, "未在 HTML 找到「使用分區」"
    except Exception as e:
        return None, f"err: {e}"


def main():
    from database.db import get_firestore
    db = get_firestore()
    col = db.collection("properties")

    targets = []
    for d in col.stream():
        dd = d.to_dict() or {}
        if dd.get("city") != "新北市":
            continue
        if dd.get("is_foreclosure") or dd.get("is_remote_area") or dd.get("unsuitable_for_renewal"):
            continue
        if dd.get("deleted") or dd.get("analysis_error") or dd.get("analysis_in_progress"):
            continue
        sources = dd.get("sources") or []
        for s in sources:
            n = s.get("name")
            if n in ("永慶", "信義") and s.get("alive") is not False:
                url = s.get("url")
                sid = s.get("source_id")
                if not (url and sid):
                    continue
                addr = dd.get("address_inferred") or dd.get("address") or ""
                targets.append({
                    "doc_id": d.id,
                    "src": n,
                    "sid": sid,
                    "url": url,
                    "district": dd.get("district") or "",
                    "addr": addr,
                    "db_zoning": dd.get("zoning") or "",
                    "db_zoning_original": dd.get("zoning_original") or "",
                })
                break

    print(f"共 {len(targets)} 筆要 audit", flush=True)

    def _do(t):
        if t["src"] == "永慶":
            raw, note = fetch_zoning_yongqing(t["url"])
        else:
            raw, note = fetch_zoning_sinyi(t["url"])
        t["site_zoning"] = raw
        t["fetch_note"] = note
        return t

    results = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for i, t in enumerate(ex.map(_do, targets), 1):
            results.append(t)
            print(f"  [{i}/{len(targets)}] {t['src']} {t['sid']} → {t['site_zoning']!r} ({t['fetch_note']})", flush=True)

    # 排序：先列 mismatch
    def _mismatch(t):
        site = (t["site_zoning"] or "").strip()
        db = (t["db_zoning_original"] or t["db_zoning"] or "").strip()
        if not site:
            return 0  # fetch 失敗
        # 完全相等不算 mismatch
        if site == db:
            return 1
        # site 含於 db 或 db 含於 site → 視為「資訊量差異」
        return 2

    results.sort(key=lambda t: (-_mismatch(t), t["src"], t["sid"]))

    # 輸出 markdown 表格
    lines = []
    lines.append("# 新北市 永慶/信義 zoning 對照（2026-05-01）")
    lines.append("")
    lines.append(f"共 **{len(results)}** 筆物件（排除 法拍 / 偏遠 / 非住商工 三種例外）")
    lines.append("")
    lines.append("| # | 來源 | 區 | 地址 | 網站「使用分區」原文 | DB zoning | DB zoning_original | 狀態 |")
    lines.append("|---|------|----|------|---------------------|-----------|--------------------|------|")
    for i, t in enumerate(results, 1):
        site = t["site_zoning"] or f"❌ {t['fetch_note']}"
        db_z = t["db_zoning"] or "—"
        db_zo = t["db_zoning_original"] or "—"
        if not t["site_zoning"]:
            status = "fetch 失敗"
        elif t["site_zoning"].strip() == (t["db_zoning_original"] or t["db_zoning"] or "").strip():
            status = "✅ 一致"
        else:
            status = "⚠️ 不同"
        addr_short = t["addr"][:25]
        lines.append(
            f"| {i} | {t['src']} | {t['district']} | {addr_short} | {site} | {db_z} | {db_zo} | {status} |"
        )

    out_md = "\n".join(lines) + "\n"
    out_dir = Path(__file__).resolve().parent.parent / "reports"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "zoning_audit_new_taipei_2026_05_01.md"
    out_path.write_text(out_md, encoding="utf-8")
    print(f"\n→ 報告寫入 {out_path}", flush=True)

    # 統計
    ok = sum(1 for t in results if t["site_zoning"] and t["site_zoning"].strip() == (t["db_zoning_original"] or t["db_zoning"] or "").strip())
    diff = sum(1 for t in results if t["site_zoning"] and t["site_zoning"].strip() != (t["db_zoning_original"] or t["db_zoning"] or "").strip())
    fail = sum(1 for t in results if not t["site_zoning"])
    print(f"\n統計：✅ 一致 {ok} 筆 / ⚠️ 不同 {diff} 筆 / ❌ fetch 失敗 {fail} 筆")


if __name__ == "__main__":
    main()
