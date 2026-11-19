"""Детальная проверка part3 + строка 4506"""
import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent


def _inn(v):
    return "".join(c for c in str(v or "") if c.isdigit())


# Строка 4506 (1-based в файле, где 0 = первая компания)
with open(ROOT / "input/companies_part3.json", encoding="utf-8") as f:
    part3 = json.load(f)
print(f"companies_part3.json: {len(part3)} записей")
print(f"Ключи: {list(part3[0].keys())[:6]}")

row_idx = 4506
if row_idx < len(part3):
    r = part3[row_idx]
    print(f"\nСтрока #{row_idx} (0-based): ИНН={r.get('ИНН')}, Название={r.get('Название компании')}")

# Все ИНН part3
p3_inns = {_inn(c.get("ИНН", "")) for c in part3 if _inn(c.get("ИНН", ""))}
print(f"\nУникальных ИНН в part3: {len(p3_inns)}")

# Результаты part3
with open(ROOT / "result_part3.json", encoding="utf-8") as f:
    res3 = json.load(f)
res3_inns = {_inn(r.get("ИНН", "")) for r in res3 if _inn(r.get("ИНН", ""))}
print(f"Уникальных ИНН в result_part3: {len(res3_inns)}")

# Пересечение
print(f"В part3, но не в результате: {len(p3_inns - res3_inns)}")
print(f"В результате, но не в part3: {len(res3_inns - p3_inns)}")

# Перекрёстная проверка: ИНН part3 в результатах part1/part2 (дубли между частями)
p1 = {_inn(r.get("ИНН", "")) for r in json.load(open(ROOT / "result_part1.json", encoding="utf-8"))}
p2 = {_inn(r.get("ИНН", "")) for r in json.load(open(ROOT / "result_part2.json", encoding="utf-8"))}
print(f"\nПересечение result_part3 ∩ result_part1: {len(res3_inns & p1)}")
print(f"Пересечение result_part3 ∩ result_part2: {len(res3_inns & p2)}")

# Строка 4506 - есть ли в результатах?
if row_idx < len(part3):
    inn4506 = _inn(part3[row_idx].get("ИНН", ""))
    print(f"\nИНН строки #{row_idx} = {inn4506}")
    print(f"  в result_part3: {inn4506 in res3_inns}")
    print(f"  в result_part1: {inn4506 in p1}")
    print(f"  в result_part2: {inn4506 in p2}")

# Первые 20 ИНН part3 (в порядке файла)
print(f"\nПервые 20 ИНН part3 (порядок файла):")
for c in part3[:20]:
    print(f"  {_inn(c.get('ИНН',''))}  {c.get('Название компании','')[:40]}")