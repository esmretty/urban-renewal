"""一次性 migration：把 properties collection 的 schema 收斂到「sources[] 唯一真相」。

每筆 doc：
1. 收集所有 url 來源：{url, url_alt[], sources[].url}
2. 依 url 推論 source name (591/yongqing/sinyi)，從 url 抽 site_id
3. 重建 sources[]：dedup by source_key，按 added_at 排序，alive 預設 True
4. 重建 source_keys[] 平面索引
5. 刪除 legacy 欄位：url, url_alt, source_id, source, source_ids, published_at_alt

執行：
  python scripts/migrate_unify_sources.py            # dry-run
  python scripts/migrate_unify_sources.py --apply    # 實際寫
"""
import sys
import re
import argparse
sys.path.insert(0, r'd:\Coding\urban-renewal')

from database.db import init_db, get_col
from database.models import compute_source_keys, make_source_key
from google.cloud import firestore


def detect_source_name(url: str, fallback: str = "591") -> str:
    if not url:
        return fallback
    if "591.com.tw" in url or "591." in url:
        return "591"
    if "yungching" in url:
        return "yongqing"
    if "sinyi" in url:
        return "sinyi"
    return fallback


def extract_site_id_from_url(url: str, name: str) -> str:
    """從 url 抽 site_id (e.g. .../detail/2/20114614.html → '20114614')"""
    if not url:
        return ""
    if name == "591":
        m = re.search(r"/(\d{6,10})\.html", url)
        return m.group(1) if m else ""
    if name == "yongqing":
        # 永慶 url: /house/12345
        m = re.search(r"/house/(\d+)", url)
        if m:
            return m.group(1)
        m = re.search(r"yungching\.com\.tw/[^/]*/(\d+)", url)
        return m.group(1) if m else ""
    if name == "sinyi":
        # 信義 url: /buy/house/9910BU
        m = re.search(r"/buy/house/([A-Z0-9]+)", url)
        return m.group(1) if m else ""
    return ""


def normalize_site_id(name: str, sid: str) -> str:
    """去掉 source name prefix（如 "591_20114614" → "20114614"）"""
    if not sid:
        return ""
    if sid.startswith(f"{name}_"):
        return sid.split("_", 1)[1]
    return sid


def rebuild_sources(doc: dict) -> list:
    """從 doc 收集所有 (source_name, site_id, url) 三元組，dedup 重建 sources。
    重要：url 跟 site_id 用 url-derived 的方式重新配對（避免 force_reanalyze 把 url 寫到不對的 source 上）。
    """
    candidates = []   # 收集 (name, site_id, url, added_at, alive) 候選

    archived = doc.get("archived", False)
    default_alive = not archived

    # 1. 既有 sources[] — 保留 (name, source_id, url, added_at, alive)
    for s in (doc.get("sources") or []):
        url = s.get("url")
        name = s.get("name") or detect_source_name(url)
        sid = normalize_site_id(name, s.get("source_id") or "")
        # 若 url 能 extract 出 site_id 跟 source_id 不一致 → 信 url（force_reanalyze 把 url 寫到了不對的 source）
        url_sid = extract_site_id_from_url(url, name) if url else ""
        if url_sid and sid and url_sid != sid:
            # source_id 是「歷史 source」（無 url），url 對應另一個 source
            candidates.append((name, sid, None, s.get("added_at") or "", s.get("alive", default_alive)))
            candidates.append((name, url_sid, url, s.get("added_at") or "", s.get("alive", default_alive)))
        else:
            final_sid = sid or url_sid
            if final_sid:
                candidates.append((name, final_sid, url, s.get("added_at") or "", s.get("alive", default_alive)))

    # 2. 主 source_id + 主 url（可能配不對）
    main_url = doc.get("url")
    main_sid_raw = doc.get("source_id") or ""
    main_name = doc.get("source") or detect_source_name(main_url)
    main_sid = normalize_site_id(main_name, main_sid_raw) if main_sid_raw else ""
    main_url_sid = extract_site_id_from_url(main_url, main_name) if main_url else ""
    # source_id 主欄位（可能無 url）
    if main_sid:
        candidates.append((main_name, main_sid, None, doc.get("scraped_at") or "", default_alive))
    # url 主欄位
    if main_url and main_url_sid:
        candidates.append((main_name, main_url_sid, main_url, doc.get("scraped_at") or "", default_alive))

    # 3. url_alt[]
    pub_alt = doc.get("published_at_alt") or []
    for i, u in enumerate(doc.get("url_alt") or []):
        if not u:
            continue
        name = detect_source_name(u)
        site_id = extract_site_id_from_url(u, name)
        if not site_id:
            continue
        added_at = pub_alt[i] if i < len(pub_alt) and pub_alt[i] else (doc.get("scraped_at") or "")
        candidates.append((name, site_id, u, added_at, default_alive))

    # 4. source_ids[] 平面索引（無 url）
    for sid_full in (doc.get("source_ids") or []):
        if not sid_full:
            continue
        if "_" in sid_full:
            name, sid_pure = sid_full.split("_", 1)
        else:
            name, sid_pure = "591", sid_full
        candidates.append((name, sid_pure, None, doc.get("scraped_at") or "", default_alive))

    # Dedup by source_key — 同 key 多筆時，優先保留有 url 的、然後最早 added_at
    by_key = {}
    for name, sid, url, added_at, alive in candidates:
        if not sid:
            continue
        key = make_source_key(name, sid)
        if key not in by_key:
            by_key[key] = {"name": name, "source_id": sid, "url": url,
                           "added_at": added_at, "alive": alive}
        else:
            existing = by_key[key]
            # 補 url（若 existing 沒 url）
            if not existing["url"] and url:
                existing["url"] = url
            # 取最早 added_at（保留歷史）
            if added_at and (not existing["added_at"] or added_at < existing["added_at"]):
                existing["added_at"] = added_at
            # alive：任一為 True 就是 True（被觀測到 alive 過就算）
            if alive:
                existing["alive"] = True

    out = list(by_key.values())
    # 排序：alive 先，dead 後；同類別 added_at 早→晚
    out.sort(key=lambda s: (s.get("alive") is False, s.get("added_at") or ""))
    return out


def main(apply: bool):
    init_db()
    col = get_col()
    docs = list(col.stream())
    print(f"total docs: {len(docs)}")

    plan = []
    for d in docs:
        x = d.to_dict() or {}
        new_sources = rebuild_sources(x)
        new_keys = compute_source_keys(new_sources)

        # 檢查是否有 legacy 欄位需要清掉
        legacy_present = any(f in x for f in ("url", "url_alt", "source_id", "source", "source_ids", "published_at_alt"))
        old_sources_count = len(x.get("sources") or [])

        if not new_sources and not legacy_present:
            continue   # 已 migrated 過

        plan.append({
            "doc_id": d.id,
            "old_url": x.get("url"),
            "old_url_alt": x.get("url_alt") or [],
            "old_sources_count": old_sources_count,
            "new_sources": new_sources,
            "new_keys": new_keys,
            "address": x.get("address") or "",
        })

    print(f"\n=== Migration plan ===")
    print(f"需要 migrate 的 doc: {len(plan)}")
    if plan:
        sample = plan[:5]
        print(f"\n前 {len(sample)} 筆預覽:")
        for p in sample:
            print(f"\n  {p['doc_id']} ({p['address'][:20]})")
            print(f"    舊 url={p['old_url']!r}")
            print(f"    舊 url_alt[{len(p['old_url_alt'])}]")
            print(f"    舊 sources[{p['old_sources_count']}]")
            print(f"    → 新 sources[{len(p['new_sources'])}]:")
            for s in p['new_sources']:
                alive_mark = "✓" if s.get("alive") else "✗"
                print(f"        {alive_mark} {s['name']}:{s['source_id']}  url={s.get('url') or '(無)'}")
            print(f"    → source_keys: {p['new_keys']}")

    if not apply:
        print(f"\n[dry-run] 加 --apply 才會真寫")
        return

    print(f"\n=== APPLYING ===")
    n_updated = 0
    n_failed = 0
    for p in plan:
        try:
            updates = {
                "sources": p["new_sources"],
                "source_keys": p["new_keys"],
                # 刪除 legacy 欄位
                "url": firestore.DELETE_FIELD,
                "url_alt": firestore.DELETE_FIELD,
                "source_id": firestore.DELETE_FIELD,
                "source": firestore.DELETE_FIELD,
                "source_ids": firestore.DELETE_FIELD,
                "published_at_alt": firestore.DELETE_FIELD,
            }
            col.document(p["doc_id"]).update(updates)
            n_updated += 1
        except Exception as e:
            print(f"  ✗ {p['doc_id']}: {e}")
            n_failed += 1
    print(f"\ndone: updated {n_updated} / failed {n_failed}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    main(apply=args.apply)
