"""Check current FINAL sheet structure"""
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

# Read FINAL SHEET
url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/ФИНАЛЬНЫЕ ДАННЫЕ!A1:AR5"
resp = session.get(url)
if resp.status_code == 200:
    d = resp.json()
    rows = d.get("values", [])
    print(f"=== Sheet 'ФИНАЛЬНЫЕ ДАННЫЕ' ===")
    for i, row in enumerate(rows):
        print(f"\n--- Row {i+1} ({len(row)} columns) ---")
        for j, cell in enumerate(row[:50]):
            col_letter = chr(65 + j) if j < 26 else chr(64 + (j-26)//26) + chr(65 + (j-26)%26)
            # Actually use a1 notation
            a1 = ""
            if j < 26:
                a1 = chr(65 + j)
            else:
                a1 = "A" + chr(65 + j - 26)
            val = str(cell)[:60]
            print(f"  {a1}{i+1}: {val}")
else:
    print(f"Error: {resp.status_code}")

# Also read row with phones to check
url2 = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/ФИНАЛЬНЫЕ ДАННЫЕ!A2:AR3"
resp2 = session.get(url2)
if resp2.status_code == 200:
    d2 = resp2.json()
    rows2 = d2.get("values", [])
    print(f"\n=== Row 2 & 3 data check ===")
    for i, row in enumerate(rows2):
        print(f"\nRow {i+2}:")
        for j, cell in enumerate(row):
            col = j + 1
            val = str(cell)[:80]
            if val:
                print(f"  col {col}: {val}")