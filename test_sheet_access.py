"""
Test access to Google Sheets via service account.
"""
import sys
import time
from pathlib import Path
import gspread
from gspread.exceptions import APIError

ROOT = Path(__file__).resolve().parent
CRED_PATH = ROOT / "synaps-table-6d9ee78612df.json"
SHEET_ID = "1YV2cKiWtplOnApS2QnwnwPjJ5oigvbKH5yrUjCSc3ko"

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
print("=" * 60)
print("TEST: Google Sheets Access via Service Account")
print("=" * 60)

if not CRED_PATH.is_file():
    print(f"ERROR: No credentials file: {CRED_PATH}")
    sys.exit(1)
print(f"OK: credentials file exists: {CRED_PATH}")

gc = gspread.service_account(filename=str(CRED_PATH))

for attempt in range(3):
    try:
        print(f"Attempt {attempt + 1}: open sheet {SHEET_ID}...")
        sh = gc.open_by_key(SHEET_ID)
        print(f"OK: Sheet opened: {sh.title}")

        worksheets = sh.worksheets()
        print(f"Sheets ({len(worksheets)}):")
        for ws in worksheets:
            print(f"  - '{ws.title}' ({ws.row_count}x{ws.col_count})")

        ws = sh.worksheet("таблица максим синапс")
        vals = ws.get_all_values()
        print(f"\nOK: Sheet '{ws.title}' read: {len(vals)} rows")
        if vals:
            print(f"  Headers ({len(vals[0])}): {vals[0][:5]}...")
            print(f"  Row 2 (sample): {vals[1][:5]}..." if len(vals) > 1 else "")

        # Create new sheet if needed
        try:
            ws_new = sh.worksheet("ФИНАЛЬНЫЕ ДАННЫЕ")
            print("Sheet 'ФИНАЛЬНЫЕ ДАННЫЕ' already exists")
        except gspread.WorksheetNotFound:
            ws_new = sh.add_worksheet(title="ФИНАЛЬНЫЕ ДАННЫЕ", rows=2000, cols=40)
            print("Created sheet 'ФИНАЛЬНЫЕ ДАННЫЕ'")

        print("\n*** ALL OK! ***")
        break

    except APIError as e:
        print(f"API ERROR: {e}")
        if "403" in str(e):
            print("\nNEED to grant access to service account!")
            print("  Email: table-writer-service-account@synaps-table.iam.gserviceaccount.com")
            print("  Open sheet -> Share -> Add editor")
            break
        if "429" in str(e):
            print("Rate limit, wait 30s...")
            time.sleep(30)
            continue
        if "404" in str(e):
            print("Sheet not found - check SHEET_ID")
            break
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        if attempt < 2:
            print("Wait 10s and retry...")
            time.sleep(10)

print("=" * 60)