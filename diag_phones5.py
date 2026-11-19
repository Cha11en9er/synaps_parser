"""Проверка скрытых блоков: textContent + клик по «показать ещё»."""
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
            if r.get("_status") == "ok" and not str(r.get("номер 1", "")).strip()][:1]
with_ph = [i for i, r in state.items()
           if r.get("_status") == "ok" and str(r.get("номер 1", "")).strip()][:1]

main_url, mail, password = psb._credentials()
landing = psb._synaps_landing_url()

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context()
    page = ctx.new_page()
    psb._ensure_logged_in(page, main_url, mail, password)
    psb._goto(page, landing, wait_until="domcontentloaded")
    psb._settle_after_landing_goto(page)

    for tag, inns in (("БЕЗ ТЕЛЕФОНА", no_phone), ("С ТЕЛЕФОНОМ", with_ph)):
        for inn in inns:
            name = state.get(inn, {}).get("Название компании", "?")
            print("=" * 70)
            print(f"{tag}: ИНН {inn}  {name[:40]}")
            psb.navigate_to_organization_by_inn(page, inn, landing_url=landing)
            page.wait_for_timeout(1500)

            def probe(label):
                r = page.evaluate("""() => {
                    const tc = document.body.textContent || '';
                    const it = document.body.innerText || '';
                    const rx = /[78]\\s*\\(\\d{3,5}\\)\\s*\\d+(?:-\\d{2}-\\d{2})/g;
                    return {
                        tc_phones: (tc.match(rx)||[]).slice(0,6),
                        it_phones: (it.match(rx)||[]).slice(0,6),
                        tc_has_phone_word: tc.toLowerCase().includes('телефон'),
                        contacts_blocks: document.querySelectorAll('.org-contacts-block').length,
                    };
                }""")
                print(f"  [{label}] textContent телефоны: {r['tc_phones']}")
                print(f"  [{label}] innerText телефоны:   {r['it_phones']}")
                print(f"  [{label}] 'телефон' в textContent: {r['tc_has_phone_word']}")
                print(f"  [{label}] блоков контактов: {r['contacts_blocks']}")

            probe("до кликов")

            # Кликаем все «показать ещё»
            n = page.locator(".ocb-show-all").count()
            print(f"  кнопок .ocb-show-all: {n}")
            for i in range(n):
                try:
                    page.locator(".ocb-show-all").nth(i).click()
                    page.wait_for_timeout(600)
                except Exception as e:
                    print(f"    клик {i} не удался: {str(e)[:60]}")
            probe("после кликов")

    ctx.close()
    browser.close()