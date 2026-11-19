"""Заголовки листа ФИНАЛЬНЫЕ ДАННЫЕ."""
import sys
from pathlib import Path
import gspread
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

gc = gspread.service_account(filename=str(ROOT / "synaps-table-6d9ee78612df.json"))
sh = gc.open_by_key("1YV2cKiWtplOnApS2QnwnwPjJ5oigvbKH5yrUjCSc3ko")

for name in ("ФИНАЛЬНЫЕ ДАННЫЕ", "ФИНАЛЬНЫЕ 1"):
    try:
        ws = sh.worksheet(name)
        hdr = ws.row_values(1)
        print(f"--- '{name}' ({ws.row_count}x{ws.col_count}) ---")
        print(f"  заголовков: {len(hdr)}")
        for i, h in enumerate(hdr, 1):
            print(f"    {i:3d}: {h}")
        print()
    except Exception as e:
        print(f"'{name}': {e}")