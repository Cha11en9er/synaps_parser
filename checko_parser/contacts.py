"""
Извлечение контактов с Checko: телефоны, email, сайты (+ переход на полную страницу контактов).
"""
from __future__ import annotations

import re
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

CONTACTS_ROW = "div.bs-row.gy-3.gx-4.mt-1"
MORE_LINK = "a.link.more"


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def extract_contacts(page: Page) -> dict[str, list[str]]:
    """Телефоны/почты/сайты из блока контактов (или со страницы всех контактов)."""
    return page.evaluate(
        """() => {
        const out = {phones: [], emails: [], sites: []};
        const seen = {phones: new Set(), emails: new Set(), sites: new Set()};
        const row = document.querySelector('div.bs-row.gy-3.gx-4.mt-1');
        if (!row) return out;
        for (const a of row.querySelectorAll('a[href]')) {
            const h = a.getAttribute('href') || '';
            const t = (a.innerText || '').trim();
            if (h.startsWith('tel:')) {
                const v = t || h.slice(4);
                if (v && !seen.phones.has(v)) { seen.phones.add(v); out.phones.push(v); }
            } else if (h.startsWith('mailto:')) {
                const v = t || h.slice(7);
                if (v && !seen.emails.has(v)) { seen.emails.add(v); out.emails.push(v); }
            } else if (/^https?:\\/\\//.test(h)) {
                const v = t || h;
                if (v && !seen.sites.has(v)) { seen.sites.add(v); out.sites.push(v); }
            }
        }
        return out;
    }"""
    )


def _abs_url(href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    if href.startswith("/"):
        return "https://checko.ru" + href
    return href


def contacts_page_url(page: Page) -> str:
    """
    Ссылка на страницу со всеми контактами.

    Важно: селектор ``a.link.more`` встречается и в других блоках карточки
    (аналитика, история и т.п.), поэтому искать нужно ТОЛЬКО внутри блока
    контактов и только там, где ссылка ведёт на ``/contacts``; иначе легко
    уехать на ``/activity`` или ``/timeline``.
    """
    try:
        # 1) прямая ссылка на /contacts внутри блока контактов
        row = page.locator(CONTACTS_ROW).first
        if row.count() > 0:
            link = row.locator('a[href*="/contacts"]').first
            if link.count() > 0:
                url = _abs_url(link.get_attribute("href") or "")
                if url:
                    return url
            # 2) фолбэк: «Еще N номер/адрес» внутри блока контактов
            link = row.locator(MORE_LINK).first
            if link.count() > 0:
                url = _abs_url(link.get_attribute("href") or "")
                if "/contacts" in url:
                    return url
        # 3) фолбэк по всей странице — но только настоящая страница контактов
        link = page.locator('a[href$="/contacts"]').first
        if link.count() > 0:
            return _abs_url(link.get_attribute("href") or "")
        return ""
    except Exception:
        return ""


def fetch_contacts(page: Page, *, follow_more: bool = True) -> dict[str, list[str]]:
    """Контакты с карточки; при follow_more — плюс со страницы всех контактов."""
    data = extract_contacts(page)

    if follow_more:
        url = contacts_page_url(page)
        if url:
            try:
                page.goto(url, wait_until="domcontentloaded")
                page.wait_for_timeout(1500)
                extra = extract_contacts(page)
                for k in ("phones", "emails", "sites"):
                    for v in extra.get(k, []):
                        if v not in data[k]:
                            data[k].append(v)
            except Exception:
                pass

    return data
