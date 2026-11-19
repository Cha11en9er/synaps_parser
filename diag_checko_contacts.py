"""Что вообще есть в контактах на Checko: телефоны, почта, сайт и т.д."""
import sys
from pathlib import Path
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent

# 2 компании с данными
URLS = [
    "https://checko.ru/company/yaska-1125003000110",
    "https://checko.ru/company/ireks-trier-1107746239864",
]

with Stealth().use_sync(sync_playwright()) as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    for url in URLS:
        print("=" * 70)
        print(url)
        try:
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)
        except Exception as e:
            print(f"  ошибка перехода: {e}")
            continue

        info = page.evaluate("""() => {
            const row = document.querySelector('div.bs-row.gy-3.gx-4.mt-1');
            if (!row) return {found: false};
            const cols = [];
            for (const c of row.querySelectorAll('div.col-12.col-lg-4')) {
                const lab = c.querySelector('strong.fw-700.d-block.mb-1');
                const links = Array.from(c.querySelectorAll('.link-pseudo')).map(x => x.innerText.trim()).filter(Boolean);
                const text = (c.innerText||'').replace(/\\s+/g,' ').trim();
                const anchors = Array.from(c.querySelectorAll('a[href]')).map(a => ({h: a.getAttribute('href'), t: a.innerText.trim().slice(0,40)}));
                cols.push({
                    label: lab ? lab.innerText.trim() : '',
                    links,
                    text: text.slice(0,150),
                    anchors: anchors.slice(0,5),
                });
            }
            return {found: true, cols, row_html: row.outerHTML.slice(0, 1500)};
        }""")

        if not info.get("found"):
            print("  блок контактов не найден")
            continue

        print(f"  колонок: {len(info['cols'])}")
        for c in info["cols"]:
            print(f"   [{c['label']}]")
            print(f"      .link-pseudo: {c['links']}")
            print(f"      текст: {c['text']}")
            print(f"      ссылки: {c['anchors']}")
        print(f"\n  HTML блока (первые 800):\n{info['row_html'][:800]}")
        print()

    browser.close()