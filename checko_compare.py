"""
Сравнение: Checko vs Synaps на компаниях, где Синапс телефон НЕ нашёл.
Берём 10 ИНН из состояния (status=ok, без телефона) и прогоняем через Checko.
"""
import json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from checko_parser.main import scrape_inns

with open(ROOT / "synaps_state.json", encoding="utf-8") as f:
    state = json.load(f)["items"]

# 10 компаний: ok, без телефона
sample = []
for inn, r in state.items():
    if r.get("_status") != "ok":
        continue
    if str(r.get("номер 1", "")).strip():
        continue
    sample.append((inn, r.get("Название компании", "?")))
    if len(sample) >= 10:
        break

print("Проверяю на Checko (Синапс телефон не нашёл):")
for inn, name in sample:
    print(f"  {inn}  {name[:45]}")
print()

inns = [i for i, _ in sample]
names = dict(sample)

results = scrape_inns(inns, headless=True)

print("\n" + "=" * 70)
print("ИТОГ: Checko vs Synaps")
print("=" * 70)
found = 0
for inn, _ in sample:
    r = results.get(inn)
    if isinstance(r, BaseException):
        print(f"  {inn}  {names[inn][:30]:30}  ОШИБКА: {str(r)[:40]}")
    elif r:
        found += 1
        print(f"  {inn}  {names[inn][:30]:30}  Checko: {r}")
    else:
        print(f"  {inn}  {names[inn][:30]:30}  Checko: —")

print(f"\nИз 10 компаний Checko нашёл телефон у: {found}")