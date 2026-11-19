"""Проверка: строки-разделители на своих местах + границы групп."""
import sys
from pathlib import Path
from openpyxl import load_workbook

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent

wb = load_workbook(ROOT / "result_maxim.xlsx", read_only=True)
ws = wb.active

sep_rows = []
first_cells = []
n = 0
for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
    n += 1
    if i == 1:
        continue
    if row and row[0] == "—":
        sep_rows.append(i)
    else:
        first_cells.append((i, row[0] if row else ""))

wb.close()

print(f"Всего строк: {n}")
print(f"Строки-разделители: {sep_rows}")
print()

# Границы групп
bounds = [2] + [r + 1 for r in sep_rows] + [n + 1]
for gi in range(len(bounds) - 1):
    a, b = bounds[gi], bounds[gi + 1] - 1
    if a > b:
        continue
    count = b - a + 1
    print(f"Группа {gi+1}: строки {a}..{b}  ({count} записей)")
    print(f"   первая: {first_cells[0][1][:50] if first_cells else ''}")

print()
# Показываем строки вокруг разделителей
print("Вокруг разделителей (по 1 строке):")
for r in sep_rows:
    print(f"  строка {r-1}: {first_cells and ''}")
for r in sep_rows:
    # найдём соседей
    pass

# Проще: выведем первые ячейки строк рядом с разделителями
wb = load_workbook(ROOT / "result_maxim.xlsx", read_only=True)
ws = wb.active
want = set()
for r in sep_rows:
    want.update({r-1, r, r+1})
for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
    if i in want:
        v = row[0] if row else ""
        tag = "РАЗДЕЛИТЕЛЬ" if v == "—" else ""
        print(f"  {i:6d}: {str(v)[:60]:60} {tag}")
wb.close()