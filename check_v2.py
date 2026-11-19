"""Check final column structure"""
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

# Header
url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/ФИНАЛЬНЫЕ ДАННЫЕ!A1:AX1"
resp = session.get(url)
headers = resp.json().get("values", [[]])[0] if resp.status_code == 200 else []
print(f"=== HEADER ({len(headers)} columns) ===")
for i, h in enumerate(headers):
    print(f"  col {i+1:3d}: {h}")

# Row 2 (first company) and Row 7 (first IP)
for rn in [2, 7]:
    url2 = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/ФИНАЛЬНЫЕ ДАННЫЕ!A{rn}:AX{rn}"
    resp2 = session.get(url2)
    if resp2.status_code == 200:
        row = resp2.json().get("values", [[]])[0]
        print(f"\n=== Row {rn} ({len(row)} cols) ===")
        for j, val in enumerate(row):
            if val:
                hdr = headers[j] if j < len(headers) else f"col{j+1}"
                print(f"  col {j+1:3d} ({hdr[:30]}): {str(val)[:60]}")