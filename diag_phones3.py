"""Контроль: 3 компании с телефоном + 3 без — сравнить наличие блока контактов."""
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

with_phone = [i for i, r in state.items()
              if r.get("_status") == "ok" and str(r.get("номер 1", "")).strip()][:3]
no_phone = [i for i, r in state.items()
            if r.get("_status") == "ok" and not str(r.get("номер 1", "")).strip()][:3]

print(f"С телефоном: {with_phone}")
print(f"Без телефона: {no_phone}\n")

main_url, mail, password = psb._credentials()
landing = psb._synaps_landing_url()

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context()
    page = ctx.new_page()
    psb._ensure_logged_in(page, main_url, mail, password)
    psb._goto(page, landing, wait_until="domcontentloaded")
    psb._settle_after_landing_goto(page)

    for tag, inns in (("С ТЕЛЕФОНОМ", with_phone), ("БЕЗ ТЕЛЕФОНА", no_phone)):
        print("#" * 60)
        print(f"# {tag}")
        print("#" * 60)
        for inn in inns:
            name = state.get(inn, {}).get("Название компании", "?")
            try:
                psb.navigate_to_organization_by_inn(page, inn, landing_url=landing)
                psb._expand_contacts_for_parse(page)
                psb._pause_micro(page)
                info = page.evaluate("""() => ({
                    blocks: document.querySelectorAll('.org-contacts-block').length,
                    has_phone_word: (document.body.innerText||'').toLowerCase().includes('телефон'),
                    phones: ((document.body.innerText||'').match(/[78]\\s*\\(\\d{3,5}\\)\\s*\\d+(?:-\\d{2}-\\d{2})/g)||[]).slice(0,5),
                })""")
                got = psb._extract_phones(page)
                print(f"  {inn} {name[:35]}")
                print(f"    блоков контактов: {info['blocks']}, 'телефон' на стр.: {info['has_phone_word']}")
                print(f"    телефонов на стр.: {info['phones']}")
                print(f"    извлечено: {got}")
            except Exception as e:
                print(f"  {inn}: ОШИБКА {type(e).__name__}: {str(e)[:80]}")
        print()

    ctx.close()
    browser.close()