"""Test Google Sheets access with longer timeout"""
import sys
import time
from pathlib import Path
import gspread
from gspread.exceptions import APIError

ROOT = Path(__file__).resolve().parent
CRED_PATH = ROOT / "synaps-table-6d9ee78612df.json"
SHEET_ID = "1YV2cKiWtplOnApS2QnwnwPjJ5oigvbKH5yrUjCSc3ko"

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Try with explicit timeout
import requests
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds = service_account.Credentials.from_service_account_file(
    str(CRED_PATH), scopes=SCOPES
)

session = AuthorizedSession(creds)
session.timeout = 60  # longer timeout

# Try open sheet
url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}"
print(f"GET {url}")
resp = session.get(url)
print(f"Status: {resp.status_code}")
if resp.status_code == 200:
    data = resp.json()
    print(f"OK! Title: {data.get('properties', {}).get('title', '?')}")
    sheets = data.get("sheets", [])
    for s in sheets:
        props = s.get("properties", {})
        print(f"  Sheet: '{props.get('title', '?')}' ({props.get('rowCount', '?')}x{props.get('columnCount', '?')})")
elif resp.status_code == 403:
    print("403 Forbidden - need to grant access to service account")
    print(f"Email: table-writer-service-account@synaps-table.iam.gserviceaccount.com")
elif resp.status_code == 404:
    print("404 Not Found - check SHEET_ID")
else:
    print(f"Response: {resp.text[:500]}")