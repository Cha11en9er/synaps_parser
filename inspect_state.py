"""Инспекция synaps_state.json"""
import json, sys, os
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent

f = ROOT / "synaps_state.json"
print(f"{f.name}: {os.path.getsize(f)/1024/1024:.1f} MB")
with open(f, encoding="utf-8") as fh:
    raw = json.load(fh)
items = raw.get("items", {})
print(f"items: {len(items)}")
print(f"counts: {raw.get('counts')}")

ok = [r for r in items.values() if r.get("_status") == "ok"]
nf = [r for r in items.values() if r.get("_status") == "not_found"]
er = [r for r in items.values() if r.get("_status") == "error"]
print(f"ok={len(ok)} not_found={len(nf)} error={len(er)}")

if ok:
    r = ok[0]
    print("\nВсе ключи строки ok:")
    for k in r.keys():
        print(f"  {k!r} = {str(r[k])[:60]!r}")

# Макс телефонов
mx = 0
for r in ok:
    n = 0
    while f"номер {n+1}" in r:
        n += 1
    mx = max(mx, n)
print(f"\nМакс. телефонов в одной строке: {mx}")