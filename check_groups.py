"""Проверка содержимого групп."""
import sys
from pathlib import Path
from openpyxl import load_workbook

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent

wb = load_workbook(ROOT / "result_maxim.xlsx", read_only=True)
ws = wb.active

rows = list(ws.iter_rows(values_only=True))
wb.close()

hdr = rows[0]
i_name = hdr.index("Имя директора")
i_ph1 = hdr.index("номер 1")
i_short = hdr.index("Короткое название")
i_status = hdr.index("Статус парсинга")

print(f"Столбцов: {len(hdr)}")
print(f"Индексы: имя={i_name}, номер1={i_ph1}, короткое={i_short}, статус={i_status}\n")

# Границы групп (1-based номера строк)
bounds = [
    ("с номерами", 2, 1070),
    ("спарсено без номера", 1072, 6393),
    ("не спарсено", 6395, 18184),
    ("нет на Синапсе", 18186, 20778),
]

for label, a, b in bounds:
    # a,b — 1-based номера строк; в rows индекс = номер-1
    sample = rows[a - 1]
    has_phone = sum(1 for r in rows[a - 1:b] if str(r[i_ph1] or "").strip())
    print(f"--- {label} (строки {a}..{b}) ---")
    print(f"  пример: {sample[0][:40]}")
    print(f"    ИНН:      {sample[4]}")
    print(f"    Имя:      {sample[i_name]!r}")
    print(f"    номер 1:  {sample[i_ph1]!r}")
    print(f"    Короткое: {sample[i_short]!r}")
    print(f"    Статус:   {sample[i_status]!r}")
    print(f"  всего с телефоном в группе: {has_phone}")
    print()