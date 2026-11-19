"""Статистика телефонов: сколько мобильных, сколько городских."""
import json, re, sys
from collections import Counter
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent

with open(ROOT / "synaps_state.json", encoding="utf-8") as f:
    state = json.load(f)["items"]

ok = [r for r in state.values() if r.get("_status") == "ok"]
print(f"Всего спарсено (ok): {len(ok)}")

def phones_of(r):
    out = []
    n = 0
    while f"номер {n+1}" in r:
        v = str(r.get(f"номер {n+1}", "")).strip()
        if v:
            out.append(v)
        n += 1
    return out

with_ph = [r for r in ok if phones_of(r)]
print(f"С телефоном: {len(with_ph)}")
print(f"Без телефона: {len(ok) - len(with_ph)}")
print()

# Мобильные: 8 (9xx) или +7 (9xx), где код начинается с 9
MOB = re.compile(r"[78]\s*\(9\d{2}\)")

mob_companies = 0
city_companies = 0
total_phones = 0
mob_phones = 0
for r in with_ph:
    ps = phones_of(r)
    total_phones += len(ps)
    has_mob = any(MOB.search(p) for p in ps)
    if has_mob:
        mob_companies += 1
    else:
        city_companies += 1
    mob_phones += sum(1 for p in ps if MOB.search(p))

print(f"Компаний с мобильным номером: {mob_companies}")
print(f"Компаний только с городским:  {city_companies}")
print()
print(f"Всего телефонов: {total_phones}")
print(f"  из них мобильных: {mob_phones}")
print(f"  городских:        {total_phones - mob_phones}")