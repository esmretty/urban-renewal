"""把 data.taipei 下載的 CSV (Big5) 轉成 JSON。
資料包含台北捷運 + 新北環狀線第一階段的所有 387 個出口座標。
不含安坑輕軌（那是新北捷運的另一個開放資料集）。"""
import csv
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "data" / "mrt_exits_raw.json"   # 其實是 Big5 CSV
DST = ROOT / "data" / "mrt_exits.json"


def _station_key(exit_name: str) -> str:
    """從『XX站出口N』抽站名。台北車站是特例（站本身叫台北車站）。"""
    if exit_name.startswith("台北車站"):
        return "台北車站"
    # 其他：取「站」前文字當 key
    m = re.match(r"^(.+?)站(?:出|M|B|[A-Z])", exit_name)
    if m:
        return m.group(1)
    # fallback: 去掉所有出口標識
    return re.sub(r"站?(出口|出入口)?\d*$", "", exit_name)


def main():
    with open(SRC, "r", encoding="big5") as f:
        rows = list(csv.DictReader(f))

    stations: dict = {}
    for r in rows:
        name = r["出入口名稱"].strip()
        try:
            lat = float(r["緯度"])
            lng = float(r["經度"])
        except (ValueError, KeyError):
            continue
        exit_num = r.get("出入口編號", "").strip()
        stn = _station_key(name)
        if not stn:
            continue
        stations.setdefault(stn, []).append({
            "exit": exit_num,
            "exit_name": name,
            "lat": lat,
            "lng": lng,
        })

    # 排序出口編號
    for stn, exits in stations.items():
        exits.sort(key=lambda x: (len(x["exit"]), x["exit"]))

    DST.write_text(json.dumps(stations, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"站數: {len(stations)}，總出口數: {sum(len(v) for v in stations.values())}")
    print(f"寫入: {DST}")


if __name__ == "__main__":
    main()
