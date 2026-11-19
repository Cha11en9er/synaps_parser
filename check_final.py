"""Check what's on the FINAL SHEET"""
import sys
from pathlib import Path
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

# List all sheets
url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}"
resp = session.get(url)
data = resp.json()
print("=== All Sheets ===")
for s in data.get("sheets", []):
    props = s.get("properties", {})
    print(f"  ID={props.get('sheetId')}: '{props.get('title')}' ({props.get('gridProperties',{}).get('rowCount')}x{props.get('gridProperties',{}).get('columnCount')})")

# Read the final sheet
final_sheet = "ФИНАЛЬНЫЕ ДАННЫЕ"
url2 = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{final_sheet}!A1:AR5"
resp2 = session.get(url2)
if resp2.status_code == 200:
    d2 = resp2.json()
    rows = d2.get("values", [])
    print(f"\n=== Sheet '{final_sheet}' first {len(rows)} rows ===")
    for i, row in enumerate(rows):
        print(f"  Row {i+1}: {len(row)} columns")
        # Show first 28 cols and last 15
        if len(row) > 28:
            print(f"    Cols 1-28: {row[:28]}")
            print(f"    Cols 29+: {row[28:]}")
        else:
            print(f"    All: {row}")
else:
    print(f"Error: {resp2.status_code} {resp2.text[:500]}")