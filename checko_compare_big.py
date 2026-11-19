"""
Расширенное сравнение Checko vs Synaps.
Берём N ООО (10-значный ИНН) и N ИП (12-значный), у которых Синапс НЕ нашёл телефон,
и смотрим, сколько телефонов даёт Checko.
"""
import json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from checko_parser.main import scrape_inns

N = 10

with open(ROOT / "synaps_state.json", encoding="utf-8") as f:
    state = json.load(f)["items"]

ooo, ip = [], []
for inn, r in state.items():
    if r.get("_status") != "ok" or str(r.get("номер 1", "")).strip():
        continue
    if len(inn) == 10 and len(ooo) < N:
        ooo.append(inn)
    elif len(inn) == 12 and len(ip) < N:
        ip.append(inn)
    if len(ooo) >= N and len(ip) >= N:
        break

inns = ooo + ip
print(f"ООО: {len(ooo)}, ИП: {len(ip)}, всего: {len(inns)}\n")

results = scrape_inns(inns, headless=True)

def summarize(label, lst):
    found = sum(1 for i in lst if isinstance(results.get(i), list) and results[i])
    total_ph = sum(len(results[i]) for i in lst if isinstance(results.get(i), list))
    print(f"{label}: компаний {len(lst)}, с телефоном {found}, всего телефонов {total_ph}")

print("\n" + "=" * 60)
summarize("ООО", ooo)
summarize("ИП ", ip)
print("=" * 60)