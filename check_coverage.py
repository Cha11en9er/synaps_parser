"""Сравнить ИНН из таблицы (Лист1) с результатами парсинга."""
import json, sys
from pathlib import Path
from collections import Counter
import gspread
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")


def _inn(v):
    return "".join(c for c in str(v or "") if c.isdigit())


# 1. ИНН из таблицы
gc = gspread.service_account(filename=str(ROOT / "synaps-table-6d9ee78612df.json"))
sh = gc.open_by_key("1YV2cKiWtplOnApS2QnwnwPjJ5oigvbKH5yrUjCSc3ko")
ws = sh.worksheet("Лист1")
vals = ws.get_all_values()
hdr = vals[0]
inn_col = next(i for i, h in enumerate(hdr) if (h or "").strip().lower() == "инн")
sheet_inns = []
for row in vals[1:]:
    if inn_col < len(row):
        i = _inn(row[inn_col])
        if i:
            sheet_inns.append(i)

print(f"Таблица Лист1: {len(sheet_inns)} ИНН (с непустыми)")
sc = Counter(sheet_inns)
sheet_dups = {k: v for k, v in sc.items() if v > 1}
print(f"  Уникальных в таблице: {len(sc)}  дублей в таблице: {len(sheet_dups)}")

# 2. ИНН из результатов
res_inns = []
for part in ["part1", "part2", "part3"]:
    p = ROOT / f"result_{part}.json"
    if p.is_file():
        with open(p, encoding="utf-8") as f:
            for r in json.load(f):
                i = _inn(r.get("ИНН", ""))
                if i:
                    res_inns.append(i)
res_set = set(res_inns)
print(f"Результаты: {len(res_inns)} строк, уникальных {len(res_set)}")

# 3. Сравнение
sheet_set = set(sc)
missing = sheet_set - res_set       # есть в таблице, нет в результатах
extra = res_set - sheet_set         # есть в результатах, нет в таблице
print(f"\n=== СРАВНЕНИЕ ===")
print(f"В таблице, но НЕ в результатах: {len(missing)}")
print(f"В результатах, но НЕ в таблице: {len(extra)}")

if missing:
    print("\nПервые 20 отсутствующих ИНН:")
    for i in list(missing)[:20]:
        print(f"  {i}")
if extra:
    print("\nПервые 10 лишних ИНН:")
    for i in list(extra)[:10]:
        print(f"  {i}")

# 4. Входные файлы vs таблица
print(f"\n=== ВХОДНЫЕ ФАЙЛЫ vs ТАБЛИЦА ===")
in_inns = set()
for part in ["part1", "part2", "part3"]:
    p = ROOT / f"input/companies_{part}.json"
    if p.is_file():
        with open(p, encoding="utf-8") as f:
            for c in json.load(f):
                i = _inn(c.get("ИНН", ""))
                if i:
                    in_inns.add(i)
print(f"Уникальных ИНН во входных part-файлах: {len(in_inns)}")
print(f"В таблице: {len(sheet_set)}")
print(f"В таблице, но НЕ во входных: {len(sheet_set - in_inns)}")
print(f"Во входных, но НЕ в таблице: {len(in_inns - sheet_set)}")