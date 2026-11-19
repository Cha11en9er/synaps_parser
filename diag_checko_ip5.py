"""
Решающая проверка: есть ли данные о телефонах ИП в СЫРОМ HTML/JSON карточки Checko.

Для каждого ИНН: поиск → карточка ИП → сохраняем outerHTML и ищем телефоноподобные
данные везде (tel:, "Тел", phone, встроенный JSON). Плюс контроль — ООО с телефонами.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent

from checko_parser.main import search_company_by_inn

OUT_DIR = ROOT / "input" / "checko_dumps"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ИП с телефоном в Синапсе (заведомо реальные) + ООО-контроль
IP_INNS = ["361205003650", "503208917420", "502912417617", "771585857109"]
OOO_CTRL = ["7705913757"]  # ИРЕКС ТРИЭР — телефон известен

PHONE_RE = re.compile(r"(?:\+7|8)[\s\-()]?\d{3}[\s\-()]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}")


def analyze(page, inn: str, tag: str) -> None:
    url = page.url
    try:
        status = page.evaluate("() => document.readyState")
    except Exception:
        status = "?"
    html = page.evaluate("() => document.documentElement.outerHTML")
    title = page.title()
    body_txt = page.evaluate('() => (document.body.innerText||"")')
    if "Too many requests" in (body_txt or "") or "Too many requests" in (html or ""):
        print(f"  [{tag}] !!! 429 Too many requests")
        raise SystemExit("RATE LIMITED — прерываю")

    fpath = OUT_DIR / f"{inn}_{tag}.html"
    fpath.write_text(html or "", encoding="utf-8")

    tels = re.findall(r'tel:[^"\']+', html or "")
    keys = {k: len(re.findall(k, html or "", re.I))
            for k in (r'"Тел', r'"телефон', r'phone', r'"phon', r'контак', r'mailto:')}
    body_phones = sorted(set(PHONE_RE.findall(body_txt or "")))

    print(f"  [{tag}] url={url}")
    print(f"  [{tag}] title={title[:70]}")
    print(f"  [{tag}] html={len(html or '')}B; tel:={tels[:5]}")
    print(f"  [{tag}] ключи: {keys}")
    print(f"  [{tag}] телефоны в тексте: {body_phones[:8]}")

    # блок контактов
    block = page.evaluate(
        """() => {const r=document.querySelector('div.bs-row.gy-3.gx-4.mt-1');
                 return r ? (r.innerText||'').replace(/\\s+/g,' ').trim().slice(0,300) : null;}"""
    )
    print(f"  [{tag}] блок контактов: {block}")


def main() -> None:
    targets = [(i, "ip") for i in IP_INNS] + [(i, "ooo_ctrl") for i in OOO_CTRL]
    with Stealth().use_sync(sync_playwright()) as p:
        b = p.chromium.launch(headless=True)
        page = b.new_page()
        for inn, tag in targets:
            print("=" * 74)
            print(f"ИНН {inn} ({tag})")
            try:
                search_company_by_inn(page, inn)
                analyze(page, inn, tag)
                # проверим подстраницу контактов для ИП
                if tag == "ip" and "/entrepreneur/" in page.url:
                    base = page.url.split("/contacts")[0]
                    resp = page.goto(base + "/contacts", wait_until="domcontentloaded")
                    page.wait_for_timeout(1500)
                    code = resp.status if resp else None
                    print(f"  [/contacts] HTTP {code}; url={page.url}; "
                          f"title={page.title()[:60]}")
            except SystemExit:
                raise
            except Exception as exc:  # noqa: BLE001
                print(f"  ОШИБКА: {exc}")
        b.close()
    print("\nДампы HTML: input/checko_dumps/")


if __name__ == "__main__":
    main()
