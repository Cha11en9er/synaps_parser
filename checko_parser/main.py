"""
Парсер телефонов с checko.ru: поиск по ИНН в шапке, извлечение номеров из карточки компании.

Запуск теста (три ИНН в консоль):
    .\\.venv\\Scripts\\python.exe checko_parser\\main.py
"""

from __future__ import annotations

import os
import re
import sys
from typing import Any

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright
from playwright_stealth import Stealth

CHECKO_LANDING_URL = "https://checko.ru/company/yaska-1125003000110"
SEARCH_INPUT_SELECTOR = 'input.form-control[placeholder*="ОГРН"]'
CONTENT_ROW_SELECTOR = "div.bs-row.gy-3.gx-4.mt-1"
ACTION_DELAY_MS = 1500
SEARCH_TIMEOUT_MS = 20_000

TEST_INNS = ("7729742123", "7733782577", "7718844194")


def _ensure_utf8_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def _normalize_inn(raw: str) -> str:
    inn = re.sub(r"\D", "", (raw or "").strip())
    if not inn:
        raise ValueError("Пустой ИНН")
    return inn


def _pause(page: Page, ms: int = ACTION_DELAY_MS) -> None:
    if ms > 0:
        page.wait_for_timeout(ms)


def _phone_column_locator(page: Page):
    """Колонка «Телефон» / «Телефоны» в блоке контактов."""
    row = page.locator(CONTENT_ROW_SELECTOR).first
    cols = row.locator("div.col-12.col-lg-4")
    for i in range(cols.count()):
        col = cols.nth(i)
        label = col.locator("strong.fw-700.d-block.mb-1")
        if label.count() == 0:
            continue
        if re.search(r"телефон", label.first.inner_text() or "", re.I):
            return col
    return None


def extract_phones(page: Page) -> list[str]:
    """
    Телефоны из .link-pseudo в колонке «Телефон».
    Если номеров нет (прочерк «—»), возвращает пустой список.
    """
    col = _phone_column_locator(page)
    if col is None:
        return []

    seen: set[str] = set()
    phones: list[str] = []
    links = col.locator(".link-pseudo")
    for i in range(links.count()):
        text = _norm_space(links.nth(i).inner_text() or "")
        if not text or text in ("—", "-", "–"):
            continue
        if text not in seen:
            seen.add(text)
            phones.append(text)
    return phones


def search_company_by_inn(page: Page, inn_raw: str, *, landing_url: str = CHECKO_LANDING_URL) -> None:
    """Ввод ИНН в поиск, клик по кнопке, ожидание карточки компании."""
    inn = _normalize_inn(inn_raw)
    page.goto(landing_url, wait_until="domcontentloaded")
    _pause(page, 800)

    search_input = page.locator(SEARCH_INPUT_SELECTOR).first
    search_input.wait_for(state="visible", timeout=SEARCH_TIMEOUT_MS)
    search_input.fill(inn)
    _pause(page, 300)

    page.locator(".search-button").first.click()

    try:
        page.wait_for_selector(CONTENT_ROW_SELECTOR, state="visible", timeout=SEARCH_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        raise RuntimeError(f"После поиска по ИНН {inn} не загрузился блок контактов. URL: {page.url}")

    _pause(page)


def fetch_phones_by_inn(page: Page, inn_raw: str, *, landing_url: str = CHECKO_LANDING_URL) -> list[str]:
    search_company_by_inn(page, inn_raw, landing_url=landing_url)
    return extract_phones(page)


def scrape_inns(
    inns: list[str],
    *,
    headless: bool = True,
    landing_url: str = CHECKO_LANDING_URL,
) -> dict[str, list[str] | BaseException]:
    todo = list(dict.fromkeys(_normalize_inn(x) for x in inns if x and str(x).strip()))
    results: dict[str, list[str] | BaseException] = {}
    if not todo:
        return results

    _ensure_utf8_stdout()
    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        try:
            total = len(todo)
            for idx, inn in enumerate(todo, start=1):
                try:
                    print(f"[{idx}/{total}] ИНН {inn}")
                    phones = fetch_phones_by_inn(page, inn, landing_url=landing_url)
                    results[inn] = phones
                    print(f"  телефоны: {phones if phones else '—'}")
                except Exception as exc:
                    results[inn] = exc
                    print(f"  ошибка: {exc}")
        finally:
            browser.close()

    return results


def main() -> dict[str, Any]:
    headless = (os.getenv("CHECKO_HEADED") or "").strip().lower() not in ("1", "true", "yes", "on")
    return scrape_inns(list(TEST_INNS), headless=headless)


if __name__ == "__main__":
    main()
