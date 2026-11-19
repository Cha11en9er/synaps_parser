"""
Массовая проверка ИП на Checko: есть ли у КОГО-НИБУДЬ телефоны/контакты.

Для каждого ИНН: поиск → карточка ИП → дамп блока контактов (innerText, tel:,
.link-pseudo, HTML). Печатает компактно и считает статистику.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent

from checko_parser.main import search_company_by_inn
from checko_parser.contacts import extract_contacts

STATE = json.load(open(ROOT / "synaps_state.json", encoding="utf-8"))["items"]


def has_phone(r: dict) -> bool:
    return any(str(r.get(f"номер {i}", "") or "").strip() for i in range(1, 35))


ip = [(k, v) for k, v in STATE.items() if len(k) == 12 and v.get("_status") == "ok"]
with_ph = [k for k, v in ip if has_phone(v)][:20]
no_ph = [k for k, v in ip if not has_phone(v)][:10]
TARGETS = with_ph + no_ph

DUMP_JS = """() => {
    const row = document.querySelector('div.bs-row.gy-3.gx-4.mt-1');
    if (!row) return {found:false};
    const t = (row.innerText||'').replace(/\\s+/g,' ').trim();
    const tels = Array.from(row.querySelectorAll('a[href^="tel:"]')).map(a=>a.getAttribute('href'));
    const pseudo = Array.from(row.querySelectorAll('.link-pseudo')).map(x=>(x.innerText||'').trim()).filter(Boolean);
    const has_digit = /\\d{5,}/.test(t);
    return {found:true, text:t.slice(0,220), tels, pseudo, has_digit};
}"""

print(f"ИП к проверке: {len(TARGETS)} (с телефоном Синапс: {len(with_ph)}, без: {len(no_ph)})")
print("=" * 78)

stats = {"ok": 0, "err": 0, "with_tel": 0, "with_pseudo": 0, "nonempty_block": 0}

with Stealth().use_sync(sync_playwright()) as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    for idx, inn in enumerate(TARGETS, 1):
        name = str(STATE[inn].get("Название компании", ""))[:34]
        try:
            search_company_by_inn(page, inn)
            info = page.evaluate(DUMP_JS)
            c = extract_contacts(page)
            stats["ok"] += 1
            if c.get("phones"):
                stats["with_tel"] += 1
            if info.get("pseudo"):
                stats["with_pseudo"] += 1
            if info.get("has_digit"):
                stats["nonempty_block"] += 1
            flag = "ТЕЛ!" if c.get("phones") else ("digits" if info.get("has_digit") else "")
            print(f"[{idx:02}/{len(TARGETS)}] {inn} {name:34} "
                  f"tel:{c.get('phones')} pseudo:{info.get('pseudo')} {flag}")
            print(f"          url={page.url}")
            if info.get("has_digit"):
                print(f"          ТЕКСТ: {info.get('text')}")
        except Exception as exc:  # noqa: BLE001
            stats["err"] += 1
            print(f"[{idx:02}/{len(TARGETS)}] {inn} ОШИБКА: {str(exc)[:70]}")
    browser.close()

print("\n" + "=" * 78)
print("ИТОГ по ИП")
print("=" * 78)
print(json.dumps(stats, ensure_ascii=False))
