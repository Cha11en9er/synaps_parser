"""Скачать данные из Лист1 → input/companies.json"""
import sys, json, os, time
from pathlib import Path
import gspread
from dotenv import load_dotenv
from gspread.exceptions import APIError

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
SHEET_ID = os.getenv("SHEET_ID", "")
CRED_PATH = ROOT / "synaps-table-6d9ee78612df.json"
OUT = ROOT / "input" / "companies.json"

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

gc = gspread.service_account(filename=str(CRED_PATH))
sh = gc.open_by_key(SHEET_ID)
print(f"Таблица: {sh.title}")

ws = sh.worksheet("Лист1")
vals = ws.get_all_values()
print(f"Лист1: {len(vals)} строк, {len(vals[0]) if vals else 0} колонок")

if not vals or len(vals) < 2:
    raise SystemExit("Нет данных")

headers = vals[0]
rows = vals[1:]

# Превращаем в список словарей
companies = []
for row in rows:
    d = {}
    for i, h in enumerate(headers):
        val = row[i] if i < len(row) else ""
        d[h] = val
    companies.append(d)

OUT.parent.mkdir(parents=True, exist_ok=True)
with open(OUT, "w", encoding="utf-8") as f:
    json.dump(companies, f, ensure_ascii=False, indent=2)

print(f"\nЗаписан JSON: {OUT}")
print(f"Компаний: {len(companies)}")
print(f"Первая: {dict(list(companies[0].items())[:5])}")
print(f"Последняя: {dict(list(companies[-1].items())[:3])}")