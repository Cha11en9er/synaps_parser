"""Проверка ИНН: формат + контрольная сумма. Сколько компаний можно отсеять без браузера?"""
import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent


def inn_checksum_ok(inn: str) -> bool:
    if len(inn) == 10:
        w = [2, 4, 10, 3, 5, 9, 4, 6, 8]
        c = sum(int(inn[i]) * w[i] for i in range(9)) % 11 % 10
        return c == int(inn[9])
    if len(inn) == 12:
        w1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
        w2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
        c1 = sum(int(inn[i]) * w1[i] for i in range(10)) % 11 % 10
        c2 = sum(int(inn[i]) * w2[i] for i in range(11)) % 11 % 10
        return c1 == int(inn[10]) and c2 == int(inn[11])
    return False


def _inn(v):
    return "".join(c for c in str(v or "") if c.isdigit())


with open(ROOT / "input/companies.json", encoding="utf-8") as f:
    companies = json.load(f)

from collections import Counter
lens = Counter()
bad_len = 0
bad_checksum = 0
valid = 0
seen = set()

for c in companies:
    inn = _inn(c.get("ИНН", ""))
    if not inn or inn in seen:
        continue
    seen.add(inn)
    lens[len(inn)] += 1
    if len(inn) not in (10, 12):
        bad_len += 1
    elif not inn_checksum_ok(inn):
        bad_checksum += 1
    else:
        valid += 1

print(f"Всего уникальных ИНН: {len(seen)}")
print(f"\nДлина ИНН:")
for ln in sorted(lens):
    print(f"  {ln} цифр: {lens[ln]}")
print(f"\nВалидных (10/12 + контрольная сумма): {valid}")
print(f"Неверной длины: {bad_len}")
print(f"Неверная контрольная сумма: {bad_checksum}")
print(f"\n=> Можно отсеять БЕЗ браузера: {bad_len + bad_checksum} ({100*(bad_len+bad_checksum)/len(seen):.1f}%)")