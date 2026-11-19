"""Список листов в таблице + первая строка каждого."""
import sys
from pathlib import Path
import gspread
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

gc = gspread.service_account(filename=str(ROOT / "synaps-table-6d9ee78612df.json"))
SHEET_ID = "1YV2cKiWtplOnApS2QnwnwPjJ5oigvbKH5yrUjCSc3ko"
sh = gc.open_by_key(SHEET_ID)
print(f"Таблица: {sh.title}\n")

for ws in sh.worksheets():
    print(f"--- Лист: '{ws.title}'  ({ws.row_count}x{ws.col_count}) ---")
    try:
        vals = ws.get("A1:E1")
        print(f"    A1:E1 = {vals}")
    except Exception as e:
        print(f"    ошибка чтения: {e}")
    print()