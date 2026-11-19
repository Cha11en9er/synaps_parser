"""Финальная проверка: прокрутить страницу, поискать кнопки раскрытия контактов."""
import json, sys
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from playwright.sync_api import sync_playwright
import parser_synaps_browser as psb

with open(ROOT / "synaps_state.json", encoding="utf-8") as f:
    state = json.load(f)["items"]

no_phone = [i for i, r in state.items()
            if r.get("_status") == "ok" and not str(r.get("номер 1", "")).strip()][:2]

main_url, mail, password = psb._credentials()
landing = psb._synaps_landing_url()

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context()
    page = ctx.new_page()
    psb._ensure_logged_in(page, main_url, mail, password)
    psb._goto(page, landing, wait_until="domcontentloaded")
    psb._settle_after_landing_goto(page)

    for inn in no_phone:
        name = state.get(inn, {}).get("Название компании", "?")
        print("=" * 70)
        print(f"ИНН {inn}  {name[:40]}")
        psb.navigate_to_organization_by_inn(page, inn, landing_url=landing)

        # Прокрутить вниз
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2500)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2500)

        info = page.evaluate("""() => {
            const txt = (document.body.innerText || '');
            const low = txt.toLowerCase();
            // кнопки/ссылки с 'контакт' или 'показать'
            const btns = [];
            for (const el of document.querySelectorAll('a, button, div[class*="show"], div[class*="but"]')) {
                const t = (el.innerText||'').trim().toLowerCase();
                if (t && (t.includes('контакт') || t.includes('показать') || t.includes('телефон'))) {
                    btns.push({tag: el.tagName, cls: (el.className||'').slice(0,50), t: t.slice(0,50)});
                }
            }
            return {
                len: txt.length,
                has_contact_word: low.includes('контакт'),
                has_phone_word: low.includes('телефон'),
                has_phone_num: /[78]\\s*\\(\\d{3,5}\\)\\s*\\d+/.test(txt),
                buttons: btns.slice(0, 15),
            };
        }""")
        print(f"  длина текста страницы: {info['len']}")
        print(f"  слово 'контакт': {info['has_contact_word']}")
        print(f"  слово 'телефон': {info['has_phone_word']}")
        print(f"  номер телефона: {info['has_phone_num']}")
        print(f"  кнопки/ссылки:")
        for b in info["buttons"]:
            print(f"    <{b['tag']} class='{b['cls']}'> {b['t']}")

    ctx.close()
    browser.close()