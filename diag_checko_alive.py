"""Проверка: не блокирует ли Checko (поиск + карточка)."""
import sys
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

with Stealth().use_sync(sync_playwright()) as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_page()
    try:
        r = pg.goto("https://checko.ru/company/yaska-1125003000110",
                    wait_until="domcontentloaded", timeout=40000)
        print("status:", r.status if r else None)
        pg.wait_for_timeout(4000)
        print("url:", pg.url)
        print("title:", pg.title())
        body = pg.evaluate('() => (document.body.innerText||"").slice(0,500)')
        print("body:", body)
        n = pg.locator('input.form-control[placeholder*="ОГРН"]').count()
        print("search input count:", n)
    except Exception as e:
        print("ОШИБКА:", e)
    b.close()
