"""Глубокая диагностика карточки: где вообще контакты."""
import json, sys, re
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from playwright.sync_api import sync_playwright
import parser_synaps_browser as psb

TEST = ["7705913757", "7751249971"]

with open(ROOT / "synaps_state.json", encoding="utf-8") as f:
    state = json.load(f)["items"]

main_url, mail, password = psb._credentials()
landing = psb._synaps_landing_url()

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context()
    page = ctx.new_page()
    psb._ensure_logged_in(page, main_url, mail, password)
    psb._goto(page, landing, wait_until="domcontentloaded")
    psb._settle_after_landing_goto(page)

    for inn in TEST:
        print("=" * 70)
        print(f"ИНН {inn}  ({state.get(inn,{}).get('Название компании','?')})")
        psb.navigate_to_organization_by_inn(page, inn, landing_url=landing)
        page.wait_for_timeout(2000)
        print(f"URL: {page.url}")
        print(f"Карточка (.oc-op-reg-date): {page.locator('.oc-op-reg-date').count()}")

        # Ищем всё, что связано с телефоном
        info = page.evaluate("""() => {
            const res = {
                blocks_all: document.querySelectorAll('.org-contacts-block').length,
                strong_texts: [],
                phone_class_els: [],
                has_phone_word: false,
                sections: [],
            };
            // все strong на странице
            for (const s of document.querySelectorAll('strong')) {
                const t = (s.innerText||'').trim();
                if (t) res.strong_texts.push(t);
            }
            // элементы с phone в классе
            for (const el of document.querySelectorAll('[class*="phone"], [id*="phone"]')) {
                res.phone_class_els.push({
                    tag: el.tagName,
                    cls: el.className,
                    id: el.id,
                    txt: (el.innerText||'').replace(/\\s+/g,' ').slice(0,120)
                });
            }
            // заголовки секций
            for (const h of document.querySelectorAll('.org-card-h2, h2, h3')) {
                const t = (h.innerText||'').trim();
                if (t) res.sections.push(t);
            }
            res.has_phone_word = (document.body.innerText||'').toLowerCase().includes('телефон');
            return res;
        }""")

        print(f"Блоков .org-contacts-block: {info['blocks_all']}")
        print(f"Слово 'телефон' на странице: {info['has_phone_word']}")
        print(f"Секции: {info['sections'][:15]}")
        print(f"Все <strong>: {info['strong_texts'][:20]}")
        print(f"Элементы с phone в классе/id ({len(info['phone_class_els'])}):")
        for e in info["phone_class_els"][:15]:
            print(f"   <{e['tag']} class='{e['cls'][:40]}' id='{e['id'][:20]}'> {e['txt'][:80]}")
        print()

    ctx.close()
    browser.close()