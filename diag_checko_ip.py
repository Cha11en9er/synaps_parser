"""Диагностика Checko для ИП: куда ведёт поиск и где контакты."""
import sys, re
from pathlib import Path
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent

TEST = ["502745847760", "773210616204"]  # ИП
LANDING = "https://checko.ru/company/yaska-1125003000110"
SEARCH = 'input.form-control[placeholder*="ОГРН"]'

with Stealth().use_sync(sync_playwright()) as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    for inn in TEST:
        print("=" * 70)
        print(f"ИНН {inn}")
        page.goto(LANDING, wait_until="domcontentloaded")
        page.wait_for_timeout(1000)
        inp = page.locator(SEARCH).first
        inp.wait_for(state="visible", timeout=20000)
        inp.fill(inn)
        page.wait_for_timeout(300)
        page.locator(".search-button").first.click()
        page.wait_for_timeout(4000)

        print(f"URL после поиска: {page.url}")

        info = page.evaluate("""() => {
            const links = [];
            for (const a of document.querySelectorAll('a[href]')) {
                const h = a.getAttribute('href') || '';
                if (h.includes('/entrepreneur/') || h.includes('/person/') || h.includes('/company/')) {
                    links.push({h, t: (a.innerText||'').trim().slice(0,40)});
                }
            }
            return {
                title: document.title,
                has_contacts: !!document.querySelector('div.bs-row.gy-3.gx-4.mt-1'),
                text_len: (document.body.innerText||'').length,
                phone_word: (document.body.innerText||'').toLowerCase().includes('телефон'),
                phones: ((document.body.innerText||'').match(/[+7][\\s\\d()-]{10,}/g)||[]).slice(0,6),
                links: links.slice(0, 15),
            };
        }""")
        print(f"title: {info['title']}")
        print(f"есть блок контактов (bs-row): {info['has_contacts']}")
        print(f"'телефон' на странице: {info['phone_word']}")
        print(f"телефоны в тексте: {info['phones']}")
        print(f"ссылки:")
        for l in info["links"]:
            print(f"   {l['h'][:60]}  {l['t']}")
        print()

    browser.close()