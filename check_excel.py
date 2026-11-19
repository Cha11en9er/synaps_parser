"""Проверка собранного Excel: структура + примеры строк."""
import sys
from pathlib import Path
from openpyxl import load_workbook

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent
path = ROOT / "result_maxim.xlsx"

if not path.is_file():
    raise SystemExit(f"Нет файла: {path}")

wb = load_workbook(path, read_only=True)
ws = wb.active
print(f"Лист: {ws.title}")
print(f"Размер: {ws.max_row} строк x {ws.max_column} столбцов")

rows = ws.iter_rows(values_only=True)
header = next(rows)
print(f"\nЗаголовки ({len(header)}):")
for i, h in enumerate(header, 1):
    print(f"  {i:3d}: {h}")

print("\n" + "=" * 70)
print("ПРИМЕРЫ СТРОК")
print("=" * 70)
count = 0
for row in rows:
    if count >= 3:
        break
    # Ищем первую строку со статусом "спарсено"
    status = row[-1]
    if status != "спарсено":
        continue
    count += 1
    print(f"\n--- Пример {count} ---")
    print(f"  Название:     {row[0]}")
    print(f"  ИНН:          {row[4]}")
    print(f"  Имя директора:{row[24]}")
    print(f"  номер 1:      {row[25]}")
    print(f"  номер 2:      {row[26] if len(row) > 26 else ''}")
    # Короткое название идёт после телефонов
    print(f"  Статус:       {status}")

wb.close()