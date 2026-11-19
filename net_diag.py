"""Network diagnostics"""
import sys
import urllib.request
import ssl
import socket

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ctx = ssl.create_default_context()

# DNS
hosts = ["google.com", "sheets.googleapis.com", "synapsenet.ru", "accounts.google.com"]
for h in hosts:
    try:
        ip = socket.getaddrinfo(h, 443)[0][4][0]
        print(f"DNS {h} -> {ip}")
    except Exception as e:
        print(f"DNS {h} -> FAIL: {e}")

# HTTP tests
urls = [
    "https://google.com",
    "https://sheets.googleapis.com/v4/spreadsheets/1RebnSi1oewrL7qEOa-w64m1EGZGMOoF4iBD_QORlTLM",
    "https://sheets.googleapis.com/v4/spreadsheets/1YV2cKiWtplOnApS2QnwnwPjJ5oigvbKH5yrUjCSc3ko",
    "https://synapsenet.ru",
]

for url in urls:
    try:
        resp = urllib.request.urlopen(url, timeout=15, context=ctx)
        print(f"GET {url[:60]} -> {resp.status}")
    except Exception as e:
        print(f"GET {url[:60]} -> {type(e).__name__}: {str(e)[:120]}")