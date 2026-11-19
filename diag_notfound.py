"""Диагностика: что показывает Синапс при поиске существующих/несуществующих ИНН."""
import json, sys, time
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from parser_synaps_browser import (
    _credentials, _ensure_logged_in, _goto, _synaps_landing_url,
    _ensure_org_search_visible, _org_search_button_locator,
    _org_page_loaded, _is_org_choice_list, _settle_after_landing_goto,
)

TEST_INNS = [
    ("7128007958", "существующая ООО"),
    ("267023136", "9 цифр - битый"),
    ("22802173100", "11 цифр - битый"),
    ("25602815806", "11 цифр - битый"),
    ("7100027369", "существующая ООО"),
]

main_url, mail, password = _credentials()
storage = ROOT / "ss_diag.json"
landing = _synaps_landing_url()

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context()
    page = ctx.new_page()
    _ensure_logged_in(page, main_url, mail, password)
    _goto(page, landing, wait_until="domcontentloaded")
    _settle_after_landing_goto(page)

    for inn, label in TEST_INNS:
        print(f"\n{'='*60}")
        print(f"ИНН {inn} ({label})")
        try:
            inp = _ensure_org_search_visible(page, landing)
            inp.click(); page.wait_for_timeout(150)
            inp.fill(""); inp.fill(inn); page.wait_for_timeout(150)
            btn = _org_search_button_locator(page)
            if btn.count() == 0:
                inp.press("Enter")
            else:
                btn.click()
            page.wait_for_timeout(4000)

            url = page.url
            has_card = _org_page_loaded(page)
            is_list = _is_org_choice_list(page)

            # Считаем элементы списка
            stats = page.evaluate("""() => {
                const statuses = document.querySelectorAll('.org-pc-status');
                const greens = document.querySelectorAll('.org-pc-status.org-pcs-green');
                const titles = document.querySelectorAll('a.org-pcr-title');
                const body = (document.body.innerText || '').toLowerCase();
                return {
                    statuses: statuses.length,
                    greens: greens.length,
                    titles: titles.length,
                    has_empty_text: body.includes('ничего не найдено') || body.includes('не найдено') || body.includes('ничего не найден'),
                    body_snippet: (document.body.innerText || '').replace(/\\s+/g,' ').slice(0, 300)
                };
            }""")

            print(f"  URL: {url[:100]}")
            print(f"  Карточка (.oc-op-reg-date): {has_card}")
            print(f"  Список (_is_org_choice_list): {is_list}")
            print(f"  .org-pc-status: {stats['statuses']}, зелёных: {stats['greens']}, ссылок: {stats['titles']}")
            print(f"  'не найдено' в тексте: {stats['has_empty_text']}")
            print(f"  Текст: {stats['body_snippet'][:200]}")
        except Exception as e:
            print(f"  ОШИБКА: {type(e).__name__}: {e}")

    ctx.close()
    browser.close()