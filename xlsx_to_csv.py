"""Конвертация собранного Excel в CSV (для импорта в Google Sheets)."""
import csv
import sys
from pathlib import Path
from openpyxl import load_workbook

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent

src = ROOT / "result_maxim.xlsx"
dst = ROOT / "result_maxim.csv"

if not src.is_file():
    raise SystemExit(f"Нет файла: {src}")

wb = load_workbook(src, read_only=True)
ws = wb.active

n = 0
with open(dst, "w", encoding="utf-8-sig", newline="") as f:
    w = csv.writer(f)
    for row in ws.iter_rows(values_only=True):
        w.writerow(["" if v is None else v for v in row])
        n += 1

wb.close()
print(f"CSV: {dst}  ({dst.stat().st_size/1024/1024:.1f} MB), строк: {n}")