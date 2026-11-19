"""
Тест новых контактов Checko (contacts.py): телефоны + email + сайты.

Берём 20 компаний из synaps_state.json (status=ok, без телефона Синапса):
10 ООО (ИНН 10 цифр) и 10 ИП (ИНН 12 цифр). Для каждой: карточка Checko →
блок контактов → переход «Еще N номер» на полную страницу контактов.

Запуск:
    .\\.venv\\Scripts\\python.exe checko_test_contacts.py
    $env:CHECKO_HEADED=1  # чтобы видеть браузер
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from checko_parser.main import search_company_by_inn
from checko_parser.contacts import fetch_contacts

OUT = ROOT / "checko_contacts_test.json"
PER_KIND = 10


def has_phone(row: dict) -> bool:
    return any(str(row.get(f"номер {i}", "") or "").strip() for i in range(1, 35))


def pick_targets() -> list[tuple[str, str, str]]:
    """[(inn, name, kind)] — ok без телефона, 10 ООО + 10 ИП."""
    with open(ROOT / "synaps_state.json", encoding="utf-8") as f:
        items = json.load(f)["items"]

    ooo: list[tuple[str, str, str]] = []
    ip: list[tuple[str, str, str]] = []
    for inn, row in items.items():
        if row.get("_status") != "ok" or has_phone(row):
            continue
        name = str(row.get("Название компании", "?") or "?")
        if len(inn) == 10 and len(ooo) < PER_KIND:
            ooo.append((inn, name, "ООО"))
        elif len(inn) == 12 and len(ip) < PER_KIND:
            ip.append((inn, name, "ИП"))
        if len(ooo) >= PER_KIND and len(ip) >= PER_KIND:
            break
    return ooo + ip


def main() -> None:
    targets = pick_targets()
    print(f"К тесту: {len(targets)} компаний "
          f"({sum(1 for t in targets if t[2] == 'ООО')} ООО + "
          f"{sum(1 for t in targets if t[2] == 'ИП')} ИП)")
    print("=" * 78)

    results: dict[str, dict] = {}
    stats = {"phones": 0, "emails": 0, "sites": 0, "with_contacts": 0, "errors": 0}

    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            for idx, (inn, name, kind) in enumerate(targets, 1):
                t0 = time.time()
                print(f"[{idx}/{len(targets)}] {kind} {inn}  {name[:45]}")
                try:
                    search_company_by_inn(page, inn)
                    contacts = fetch_contacts(page)
                except Exception as exc:  # noqa: BLE001
                    stats["errors"] += 1
                    results[inn] = {"name": name, "kind": kind, "error": str(exc)}
                    print(f"    ОШИБКА: {exc}")
                    continue

                dt = time.time() - t0
                phones = contacts.get("phones", [])
                emails = contacts.get("emails", [])
                sites = contacts.get("sites", [])
                if phones:
                    stats["phones"] += 1
                if emails:
                    stats["emails"] += 1
                if sites:
                    stats["sites"] += 1
                if phones or emails or sites:
                    stats["with_contacts"] += 1

                results[inn] = {
                    "name": name,
                    "kind": kind,
                    **contacts,
                    "seconds": round(dt, 1),
                }
                print(f"    тел: {phones if phones else '—'}")
                print(f"    email: {emails if emails else '—'}")
                print(f"    сайт: {sites if sites else '—'}   ({dt:.1f}s)")
        finally:
            browser.close()

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 78)
    print("ИТОГ")
    print("=" * 78)
    print(f"Всего компаний:        {len(targets)}")
    print(f"С любыми контактами:   {stats['with_contacts']}")
    print(f"С телефонами:          {stats['phones']}")
    print(f"С email:               {stats['emails']}")
    print(f"С сайтами:             {stats['sites']}")
    print(f"Ошибок:                {stats['errors']}")
    print(f"Результат: {OUT}")


if __name__ == "__main__":
    main()
