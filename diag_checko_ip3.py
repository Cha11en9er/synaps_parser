"""
Расширенная диагностика Checko для ИП: где вообще могут быть телефоны.

Проверяет несколько маршрутов для одного ИНН:
  1) страница после поиска (/person/ или сразу карточка)
  2) карточка /entrepreneur/<slug>-<огрнип>
  3) её «блок контактов»
  4) прямые URL-варианты:  .../contacts , .../svyazi , .../activity
и печатает все телефоноподобные строки из текста каждой страницы.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent

LANDING = "https://checko.ru/company/yaska-1125003000110"
SEARCH = 'input.form-control[placeholder*="ОГРН"]'
PHONE_RE = re.compile(r"(?:\+7|8)[\s\-()]?\d{3}[\s\-()]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}")

# ИП с телефоном в Синапсе (реально существующие контакты) + пара без
TEST = [
    "361205003650",  # Придыбайло
    "503208917420",  # Филимонова
    "502912417617",  # Воронкина
    "262400491946",  # Новиков
    "771585857109",  # Добычина
    "502745847760",  # Свириденко (без телефона)
]


def dump(page, tag: str) -> None:
    try:
        txt = page.evaluate("() => (document.body.innerText||'')")
    except Exception as e:
        print(f"    [{tag}] ошибка чтения: {e}")
        return
    phones = sorted(set(PHONE_RE.findall(txt)))
    has_row = page.evaluate("() => !!document.querySelector('div.bs-row.gy-3.gx-4.mt-1')")
    print(f"    [{tag}] url={page.url}")
    print(f"    [{tag}] блок контактов: {has_row}; телефоны в тексте: {phones[:8]}")


with Stealth().use_sync(sync_playwright()) as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    for inn in TEST:
        print("=" * 78)
        print(f"ИНН {inn}")
        try:
            page.goto(LANDING, wait_until="domcontentloaded")
            page.wait_for_timeout(800)
            inp = page.locator(SEARCH).first
            inp.wait_for(state="visible", timeout=20000)
            inp.fill(inn)
            page.wait_for_timeout(300)
            page.locator(".search-button").first.click()
            page.wait_for_timeout(3500)
            dump(page, "после поиска")

            # все ссылки-кандидаты
            links = page.evaluate(
                """() => Array.from(document.querySelectorAll('a[href]'))
                    .map(a=>({h:a.getAttribute('href'), t:(a.innerText||'').trim().slice(0,50)}))
                    .filter(x => x.h && (x.h.includes('/entrepreneur/') || x.h.includes('/person/')))"""
            )
            print(f"    ссылки entrepreneur/person: {json.dumps(links[:8], ensure_ascii=False)}")

            ent = None
            for l in links:
                h = l["h"]
                if "/entrepreneur/" in h and "/select" not in h:
                    ent = h if h.startswith("http") else "https://checko.ru" + h
                    break
            if not ent:
                # уже могли оказаться на карточке
                if "/entrepreneur/" in page.url and "/select" not in page.url:
                    ent = page.url
            print(f"    карточка ИП: {ent}")

            if ent:
                base = ent.split("/contacts")[0].split("/svyazi")[0]
                for suffix in ("", "/contacts", "/svyazi"):
                    url = base + suffix
                    page.goto(url, wait_until="domcontentloaded")
                    page.wait_for_timeout(2500)
                    dump(page, f"entrepreneur{suffix or '(карточка)'}")
        except Exception as exc:  # noqa: BLE001
            print(f"    ОШИБКА: {exc}")
    browser.close()
