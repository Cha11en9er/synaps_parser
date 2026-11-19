"""Inspect sheets in detail"""
import sys
from pathlib import Path
import json
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

ROOT = Path(__file__).resolve().parent
CRED_PATH = ROOT / "synaps-table-6d9ee78612df.json"
SHEET_ID = "1YV2cKiWtplOnApS2QnwnwPjJ5oigvbKH5yrUjCSc3ko"

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = service_account.Credentials.from_service_account_file(str(CRED_PATH), scopes=SCOPES)
session = AuthorizedSession(creds)
session.timeout = 60

# Get full spreadsheet metadata
url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}?includeGridData=true&ranges=Лист1!1:5"
resp = session.get(url)
data = resp.json()

print("=== Spreadsheet Info ===")
print(f"Title: {data.get('properties', {}).get('title', '?')}")
print(f"Locale: {data.get('properties', {}).get('locale', '?')}")
print(f"Timezone: {data.get('properties', {}).get('timeZone', '?')}")

print("\n=== Sheets ===")
for s in data.get("sheets", []):
    props = s.get("properties", {})
    print(f"\n--- Sheet: '{props.get('title', '?')}' ---")
    print(f"  ID: {props.get('sheetId', '?')}")
    print(f"  Rows: {props.get('gridProperties', {}).get('rowCount', '?')}")
    print(f"  Cols: {props.get('gridProperties', {}).get('columnCount', '?')}")

# Try to read each sheet
for sheet_name in ["Лист1", "Лист2"]:
    try:
        url2 = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{sheet_name}!A1:AB5"
        resp2 = session.get(url2)
        if resp2.status_code == 200:
            d2 = resp2.json()
            rows = d2.get("values", [])
            print(f"\n=== Sheet '{sheet_name}' first {len(rows)} rows ===")
            for i, row in enumerate(rows):
                print(f"  Row {i+1}: {row[:5]}... ({len(row)} cols)")
        else:
            print(f"\n=== Sheet '{sheet_name}' -> {resp2.status_code}")
    except Exception as e:
        print(f"\n=== Sheet '{sheet_name}' -> ERROR: {e}")