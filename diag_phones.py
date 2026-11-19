"""Диагностика телефонов: у компаний «без номера» проверить карточку Синапс напрямую."""
import json, sys, re
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from playwright.sync_api import sync_playwright
import parser_synaps_browser as psb

# Берём 6 компаний со статусом ok, но без телефона
with open(ROOT / "synaps_state.json", encoding="utf-8") as f:
    state = json.load(f)["items"]

no_phone = []
for inn, r in state.items():
    if r.get("_status") != "ok":
        continue
    if not str(r.get("номер 1", "")).strip():
        no_phone.append(inn)
    if len(no_phone) >= 6:
        break

print(f"Проверяю {len(no_phone)} компаний без телефона: {no_phone}\n")

main_url, mail, password = psb._credentials()
storage = ROOT / "ss_phonecheck.json"
landing = psb._synaps_landing_url()

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context()
    page = ctx.new_page()
    psb._ensure_logged_in(page, main_url, mail, password)
    psb._goto(page, landing, wait_until="domcontentloaded")
    psb._settle_after_landing_goto(page)

    for inn in no_phone:
        print("=" * 60)
        print(f"ИНН {inn}")
        try:
            psb.navigate_to_organization_by_inn(page, inn, landing_url=landing)
            # раскрываем контакты
            psb._expand_contacts_for_parse(page)
            psb._pause_micro(page)

            # Есть ли блок контактов с телефоном?
            info = page.evaluate("""() => {
                const blocks = document.querySelectorAll('div.org-contacts-block');
                const out = [];
                for (const b of blocks) {
                    const strongs = Array.from(b.querySelectorAll('strong')).map(s => s.innerText.trim());
                    const txt = (b.innerText || '').replace(/\\s+/g, ' ').slice(0, 200);
                    out.push({strongs, txt});
                }
                const allText = (document.body.innerText || '');
                const phoneMatches = allText.match(/[78]\\s*\\(\\d{3,5}\\)\\s*\\d+(?:-\\d{2}-\\d{2})/g) || [];
                return {blocks: out, phoneMatches: phoneMatches.slice(0, 10)};
            }""")

            print(f"  Блоков контактов: {len(info['blocks'])}")
            for b in info["blocks"]:
                print(f"    strong: {b['strongs']}")
                print(f"    текст:  {b['txt'][:150]}")
            print(f"  Найдено телефонов в тексте страницы: {len(info['phoneMatches'])}")
            for ph in info["phoneMatches"]:
                print(f"    {ph}")

            extracted = psb._extract_phones(page)
            print(f"  _extract_phones вернул: {extracted}")
        except Exception as e:
            print(f"  ОШИБКА: {type(e).__name__}: {e}")

    ctx.close()
    browser.close()