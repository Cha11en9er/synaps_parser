"""
Checko API v2 — получение телефонов по ИНН.

Ключ: в .env → CHECKO_API_KEY=...
Тариф: 100 запросов/сутки бесплатно, далее 0.15 ₽/запрос.

Использование:
  python checko_api.py --inn 7729742123
  python checko_api.py --input input/queue_part1.json --output checko_phones.json --limit 20
  python checko_api.py --test          # 5 тестовых ИНН

Метод выбирается по длине ИНН: 10 → /company (ООО), 12 → /entrepreneur (ИП).
ВНИМАНИЕ: у /entrepreneur телефонов в ответе НЕТ (только у /company).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

API_BASE = "https://api.checko.ru/v2"
TEST_INNS = ["7729742123", "7733782577", "7718844194"]


def _key() -> str:
    k = (os.getenv("CHECKO_API_KEY") or "").strip().strip('"').strip("'")
    if not k:
        raise SystemExit(
            "Нет CHECKO_API_KEY в .env.\n"
            "Получите ключ бесплатно: https://checko.ru/sign-up\n"
            "Затем добавьте в .env строку: CHECKO_API_KEY=ваш_ключ"
        )
    return k


def _inn(v) -> str:
    return "".join(c for c in str(v or "") if c.isdigit())


def fetch(inn: str, *, key: str, timeout: int = 30) -> dict:
    """Один запрос к Checko API. Возвращает {'status', 'phones', 'name', 'raw_meta'}."""
    inn = _inn(inn)
    method = "company" if len(inn) == 10 else "entrepreneur"
    url = f"{API_BASE}/{method}"
    try:
        r = requests.get(url, params={"key": key, "inn": inn}, timeout=timeout)
        r.raise_for_status()
        payload = r.json()
    except Exception as e:
        return {"status": "error", "error": str(e), "phones": [], "name": ""}

    meta = payload.get("meta", {}) or {}
    data = payload.get("data", {}) or {}
    if meta.get("status") != "ok" or not data:
        return {
            "status": "not_found",
            "error": meta.get("message", ""),
            "phones": [],
            "name": "",
            "today": meta.get("today_request_count"),
            "balance": meta.get("balance"),
        }

    contacts = data.get("Контакты", {}) or {}
    phones = [str(p).strip() for p in (contacts.get("Тел") or []) if str(p).strip()]
    emails = [str(p).strip() for p in (contacts.get("Емэйл") or []) if str(p).strip()]
    site = str(contacts.get("ВебСайт") or "").strip()

    name = str(data.get("НаимСокр") or data.get("ФИО") or "").strip()

    return {
        "status": "ok",
        "method": method,
        "phones": phones,
        "emails": emails,
        "site": site,
        "name": name,
        "today": meta.get("today_request_count"),
        "balance": meta.get("balance"),
    }


def main():
    ap = argparse.ArgumentParser(description="Checko API — телефоны по ИНН")
    ap.add_argument("--inn", type=str, default="", help="Один ИНН")
    ap.add_argument("--input", type=Path, default=None, help="JSON со списком компаний")
    ap.add_argument("--output", type=Path, default=ROOT / "checko_phones.json")
    ap.add_argument("--limit", type=int, default=0, help="Сколько ИНН обработать")
    ap.add_argument("--test", action="store_true", help="Тестовые ИНН")
    args = ap.parse_args()

    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    key = _key()

    # Список ИНН
    if args.inn:
        inns = [_inn(args.inn)]
    elif args.test:
        inns = TEST_INNS
    elif args.input:
        with open(args.input, encoding="utf-8") as f:
            raw = json.load(f)
        items = raw if isinstance(raw, list) else raw.get("companies", [])
        inns = []
        seen = set()
        for c in items:
            i = _inn(c.get("ИНН", "") if isinstance(c, dict) else c)
            if i and i not in seen:
                seen.add(i)
                inns.append(i)
    else:
        raise SystemExit("Укажите --inn, --input или --test")

    if args.limit and args.limit > 0:
        inns = inns[: args.limit]

    print(f"ИНН к обработке: {len(inns)}")
    print("=" * 60)

    results: dict[str, dict] = {}
    ok = 0
    nf = 0
    err = 0

    for idx, inn in enumerate(inns, 1):
        r = fetch(inn, key=key)
        results[inn] = r
        st = r.get("status")
        if st == "ok":
            ok += 1
            ph = r.get("phones", [])
            print(f"[{idx}/{len(inns)}] {inn} {r.get('method','?'):12} "
                  f"телефоны: {ph if ph else '—'}  "
                  f"(запросов сегодня: {r.get('today')}, баланс: {r.get('balance')})")
        elif st == "not_found":
            nf += 1
            print(f"[{idx}/{len(inns)}] {inn} — не найдено ({r.get('error','')})")
        else:
            err += 1
            print(f"[{idx}/{len(inns)}] {inn} — ошибка: {r.get('error','')}")
        # мягкая пауза, чтобы не упереться в лимит IP (32 req/s)
        time.sleep(0.15)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=1)

    print("\n" + "=" * 60)
    print(f"OK: {ok}  не найдено: {nf}  ошибок: {err}  всего: {len(inns)}")
    print(f"Результат: {args.output}")
    print("=" * 60)


if __name__ == "__main__":
    main()