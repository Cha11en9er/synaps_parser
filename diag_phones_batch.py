"""Проверка пачки компаний «без телефона»: есть ли телефон на странице вообще."""
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

# Берём 12 компаний ok без телефона
sample = [i for i, r in state.items()
          if r.get("_status") == "ok" and not str(r.get("номер 1", "")).strip()][:12]

main_url, mail, password = psb._credentials()
landing = psb._synaps_landing_url()

with_phone_on_page = 0
total = 0

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context()
    page = ctx.new_page()
    psb._ensure_logged_in(page, main_url, mail, password)
    psb._goto(page, landing, wait_until="domcontentloaded")
    psb._settle_after_landing_goto(page)

    for inn in sample:
        name = state.get(inn, {}).get("Название компании", "?")
        try:
            psb.navigate_to_organization_by_inn(page, inn, landing_url=landing)
            page.wait_for_timeout(800)
            r = page.evaluate("""() => {
                const tc = document.body.textContent || '';
                const rx = /[78]\\s*\\(\\d{3,5}\\)\\s*\\d+(?:-\\d{2}-\\d{2})/g;
                return {
                    phones: (tc.match(rx)||[]).slice(0,4),
                    blocks: document.querySelectorAll('.org-contacts-block').length,
                };
            }""")
            total += 1
            has = bool(r["phones"]) or r["blocks"] > 0
            if has:
                with_phone_on_page += 1
            mark = "ЕСТЬ ТЕЛЕФОН!" if has else "нет"
            print(f"  {inn:14} {name[:32]:32} {mark}  телефоны={r['phones']} блоки={r['blocks']}")
        except Exception as e:
            print(f"  {inn:14} {name[:32]:32} ошибка: {str(e)[:50]}")

    ctx.close()
    browser.close()

print(f"\nИтого проверено: {total}")
print(f"Из них телефон найден на странице: {with_phone_on_page}")
print(f"Действительно без телефона: {total - with_phone_on_page}")