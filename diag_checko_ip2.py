"""Проверка страницы ИП на Checko: есть ли телефоны."""
import sys
from pathlib import Path
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent

URLS = [
    "https://checko.ru/entrepreneur/sviridenko-anastasiya-318502700084251",
    "https://checko.ru/entrepreneur/kudinov-vitaly-321774600254587",
]

with Stealth().use_sync(sync_playwright()) as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    for url in URLS:
        print("=" * 70)
        print(url)
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(3000)
        info = page.evaluate("""() => {
            const rows = document.querySelectorAll('div.bs-row.gy-3.gx-4.mt-1');
            const cols = [];
            if (rows.length) {
                for (const c of rows[0].querySelectorAll('div.col-12.col-lg-4')) {
                    const lab = c.querySelector('strong.fw-700.d-block.mb-1');
                    const links = Array.from(c.querySelectorAll('.link-pseudo')).map(x => x.innerText.trim());
                    cols.push({label: lab ? lab.innerText.trim() : '', links});
                }
            }
            return {
                title: document.title,
                has_row: rows.length > 0,
                cols,
                phone_word: (document.body.innerText||'').toLowerCase().includes('телефон'),
            };
        }""")
        print(f"title: {info['title']}")
        print(f"блок контактов: {info['has_row']}")
        print(f"'телефон' на стр.: {info['phone_word']}")
        for c in info["cols"]:
            print(f"   [{c['label']}]: {c['links']}")
        print()

    browser.close()