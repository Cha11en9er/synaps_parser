"""Check test results"""
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

# Read header
url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/ФИНАЛЬНЫЕ ДАННЫЕ!A1:AX1"
resp = session.get(url)
if resp.status_code == 200:
    d = resp.json()
    headers = d.get("values", [[]])[0]
    print(f"=== HEADER ({len(headers)} columns) ===")
    for i, h in enumerate(headers):
        col = i + 1
        print(f"  col {col:3d}: {h}")

# Read all 10 rows to check specific fields
print("\n=== ROWS 2-11: KEY FIELDS ===")
url2 = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/ФИНАЛЬНЫЕ ДАННЫЕ!A2:AX11"
resp2 = session.get(url2)
if resp2.status_code == 200:
    d2 = resp2.json()
    rows = d2.get("values", [])
    for i, row in enumerate(rows):
        rn = i + 2
        # Get relevant fields
        name = row[0] if len(row) > 0 else ""
        inn = row[4] if len(row) > 4 else ""
        dir_name = row[24] if len(row) > 24 else ""  # Y=25 (0-based 24)
        phone1 = row[25] if len(row) > 25 else ""    # Z=26 (0-based 25)
        short = row[26] if len(row) > 26 else "" if max(26, 25 + 1) else ""  # depends on phones
        # Actually find the phone column dynamically
        # Headers: col 1-24 = source, col 25=Имя, col 26=номер 1, ...
        # col after phones = Короткое название
        print(f"\nRow {rn}: {name[:45]}")
        print(f"  ИНН: {inn}")
        print(f"  Имя дир: {dir_name}")
        # Find phone columns
        for j in range(25, min(len(row), 27)):
            if row[j]:
                print(f"  {headers[j] if j < len(headers) else 'col'+str(j+1)}: {row[j][:50]}")
        # Find short name - should be column after phones
        for j in range(27, min(len(row), 30)):
            if row[j] and j < len(headers) and "номер" not in headers[j]:
                print(f"  {headers[j] if j < len(headers) else 'col'+str(j+1)}: {row[j][:50]}")
                break