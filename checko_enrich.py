"""
Checko (checko.ru) — браузерное обогащение контактами: телефоны + email + сайты.

Работает по ООО (ИНН 10 цифр) из ``synaps_state.json``. У ИП на Checko контактов
нет (проверено: 0/10), поэтому по умолчанию они пропускаются.

Результат пишется инкрементально в ``checko_state.json`` (ключ = ИНН) — можно
перезапускать, готовые (``_status == "ok"``) пропускаются (докачка).

Запуск:
    .\\.venv\\Scripts\\python.exe checko_enrich.py --limit 100          # пилот 100 ООО
    .\\.venv\\Scripts\\python.exe checko_enrich.py --limit 0            # все ООО
    .\\.venv\\Scripts\\python.exe checko_enrich.py --kind all           # включая ИП
    $env:CHECKO_HEADED=1                                                # показать браузер
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from checko_parser.main import search_company_by_inn, CheckoBlocked
from checko_parser.contacts import fetch_contacts

STATE_FILE = ROOT / "checko_state.json"


def has_synaps_phone(row: dict) -> bool:
    return any(str(row.get(f"номер {i}", "") or "").strip() for i in range(1, 35))


def load_state(path: Path) -> dict:
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_state(path: Path, state: dict) -> None:
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=1)
    tmp.replace(path)


def build_targets(*, kind: str, only_no_phone: bool, state: dict) -> list[tuple[str, str]]:
    with open(ROOT / "synaps_state.json", encoding="utf-8") as f:
        items = json.load(f)["items"]

    targets: list[tuple[str, str]] = []
    for inn, row in items.items():
        if kind == "ooo" and len(inn) != 10:
            continue
        if kind == "ip" and len(inn) != 12:
            continue
        if kind == "ooo_ip" and len(inn) not in (10, 12):
            continue
        if only_no_phone and has_synaps_phone(row):
            continue
        # докачка: уже разобранные (ok) не берём повторно
        if state.get(inn, {}).get("_status") == "ok":
            continue
        targets.append((inn, str(row.get("Название компании", "?") or "?")))
    return targets


def main() -> None:
    ap = argparse.ArgumentParser(description="Checko: обогащение контактами (браузер)")
    ap.add_argument("--limit", type=int, default=100, help="Сколько компаний (0 = без лимита)")
    ap.add_argument("--kind", choices=["ooo", "ip", "ooo_ip", "all"], default="ooo",
                    help="Тип: ooo (10 цифр), ip (12), all")
    ap.add_argument("--only-no-phone", dest="only_no_phone", action="store_true", default=True,
                    help="Только там, где у Синапса нет телефона (по умолчанию)")
    ap.add_argument("--include-with-phone", dest="only_no_phone", action="store_false",
                    help="Включая компании, где телефон уже есть")
    ap.add_argument("--output", type=Path, default=STATE_FILE, help="Файл состояния")
    ap.add_argument("--delay", type=float, default=1.0, help="Пауза между компаниями, сек")
    args = ap.parse_args()

    state_path = args.output
    state = load_state(state_path)

    targets = build_targets(kind=args.kind, only_no_phone=args.only_no_phone, state=state)
    if args.limit and args.limit > 0:
        targets = targets[: args.limit]

    headless = (os.getenv("CHECKO_HEADED") or "").strip().lower() not in ("1", "true", "yes", "on")
    print(f"К обработке: {len(targets)} компаний (kind={args.kind}, only_no_phone={args.only_no_phone})")
    print(f"Состояние: {state_path}  (уже ok: {sum(1 for v in state.values() if v.get('_status') == 'ok')})")
    print("=" * 78)

    if not targets:
        print("Нечего делать.")
        return

    stats = {"ok": 0, "empty": 0, "error": 0, "phones": 0, "emails": 0, "sites": 0}
    t_start = time.time()

    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        try:
            for idx, (inn, name) in enumerate(targets, 1):
                t0 = time.time()
                rec: dict = {"name": name, "_status": "ok",
                             "phones": [], "emails": [], "sites": []}
                try:
                    search_company_by_inn(page, inn)
                    contacts = fetch_contacts(page)
                    rec.update(contacts)
                    rec["url"] = page.url
                except CheckoBlocked as exc:
                    print(f"\n!!! СТОП: {exc}")
                    print("Прогресс сохранён, можно продолжить позже (докачка).")
                    break
                except Exception as exc:  # noqa: BLE001
                    rec["_status"] = "error"
                    rec["error"] = str(exc)
                    stats["error"] += 1
                    print(f"[{idx}/{len(targets)}] {inn} ОШИБКА: {str(exc)[:70]}")
                else:
                    ph, em, si = rec["phones"], rec["emails"], rec["sites"]
                    if not (ph or em or si):
                        stats["empty"] += 1
                    stats["ok"] += 1
                    stats["phones"] += bool(ph)
                    stats["emails"] += bool(em)
                    stats["sites"] += bool(si)
                    print(f"[{idx}/{len(targets)}] {inn} {name[:32]:32} "
                          f"тел:{len(ph)} почт:{len(em)} сайт:{len(si)} ({time.time()-t0:.1f}s)")

                rec["seconds"] = round(time.time() - t0, 1)
                state[inn] = rec
                save_state(state_path, state)  # докачка при обрыве

                if args.delay > 0:
                    time.sleep(args.delay + random.uniform(0, 0.5))
        finally:
            browser.close()

    dt = time.time() - t_start
    done = stats["ok"] + stats["error"]
    print("\n" + "=" * 78)
    print("ИТОГ")
    print("=" * 78)
    print(f"Обработано:            {done}")
    print(f"  ok:                  {stats['ok']}")
    print(f"    с телефонами:      {stats['phones']}")
    print(f"    с email:           {stats['emails']}")
    print(f"    с сайтами:         {stats['sites']}")
    print(f"    пусто (без всего): {stats['empty']}")
    print(f"  ошибок:              {stats['error']}")
    if done:
        print(f"Время:                 {dt:.0f}s ({dt/done:.1f}s/компания)")
    print(f"Состояние:             {state_path}")


if __name__ == "__main__":
    main()
