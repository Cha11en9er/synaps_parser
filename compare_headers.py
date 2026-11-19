"""Сверить заголовки листа Максима с тем, что в моём Excel."""
import sys
from pathlib import Path
import gspread
from openpyxl import load_workbook
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

gc = gspread.service_account(filename=str(ROOT / "synaps-table-6d9ee78612df.json"))
sh = gc.open_by_key("1YV2cKiWtplOnApS2QnwnwPjJ5oigvbKH5yrUjCSc3ko")
sheet_hdr = sh.worksheet("Лист1").row_values(1)

wb = load_workbook(ROOT / "result_maxim.xlsx", read_only=True)
ws = wb.active
xl_hdr = next(ws.iter_rows(values_only=True))
wb.close()

print(f"В листе Максима: {len(sheet_hdr)} столбцов")
print(f"В моём Excel:    {len(xl_hdr)} столбцов")
print()
n = max(len(sheet_hdr), 24)
for i in range(n):
    a = sheet_hdr[i] if i < len(sheet_hdr) else "<нет>"
    b = xl_hdr[i] if i < len(xl_hdr) else "<нет>"
    mark = "OK " if a == b else "!!!"
    print(f"  {mark} {i+1:3d} | лист: {a[:45]!r}")
    if a != b:
        print(f"      | excel: {b[:45]!r}")