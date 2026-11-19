"""Проверка доступности checko.ru и api.checko.ru (не браузером)."""
import sys
import requests

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

for name, url, params in [
    ("checko root", "https://checko.ru/", None),
    ("checko company-ooo", "https://checko.ru/company/yaska-1125003000110", None),
    ("api company (bad key)", "https://api.checko.ru/v2/company", {"key": "x", "inn": "7705913757"}),
    ("api entrepreneur (bad key)", "https://api.checko.ru/v2/entrepreneur", {"key": "x", "inn": "361205003650"}),
]:
    try:
        r = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=25)
        body = r.text[:200].replace("\n", " ")
        print(f"{name}: HTTP {r.status_code} | {body}")
    except Exception as e:
        print(f"{name}: ОШИБКА {e}")
