"""
Синапс в браузере: авторизация, сохранение сессии (storage state), парсинг карточек (URL / ИНН).
Разовый логин без парсинга: python parser_login.py

Переменные окружения: MAIN_URL, MAIL (или EMAIL), PASS (или PASSWORD).

Ускорение (опционально, в .env): SYNAPS_ACTION_DELAY_MS (пауза после навигации, по умолчанию 700),
SYNAPS_MICRO_DELAY_MS (после мелких кликов, по умолчанию 200), SYNAPS_SUBPAGE_WAIT_MS,
SYNAPS_BANK_COMMENT_MS, SYNAPS_INN_SEARCH_WAIT_MS, SYNAPS_LOGIN_FORM_WAIT_MS, SYNAPS_LOGIN_NAV_MS,
SYNAPS_SEARCH_BOX_WAIT_MS; для DOM-дампов: SYNAPS_DOM_DUMP_WAIT_MS, SYNAPS_DOM_DUMP_NETWORKIDLE_MS.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv
from playwright.sync_api import Locator, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


def _env_int(name: str, default: int, *, lo: int = 0, hi: int | None = None) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        v = int(raw, 10)
    except ValueError:
        return default
    if v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


# Паузы (мс): при «рваной» вёрстке поднимите SYNAPS_ACTION_DELAY_MS (например 1500–2000).
def _action_delay_ms() -> int:
    return _env_int("SYNAPS_ACTION_DELAY_MS", 700, lo=0, hi=8000)


def _micro_delay_ms() -> int:
    """После мелких кликов (раскрыть контакты и т.п.)."""
    return _env_int("SYNAPS_MICRO_DELAY_MS", 200, lo=0, hi=3000)


def _subpage_selector_timeout_ms() -> int:
    return _env_int("SYNAPS_SUBPAGE_WAIT_MS", 12_000, lo=1000, hi=90_000)


def _bank_comment_timeout_ms() -> int:
    return _env_int("SYNAPS_BANK_COMMENT_MS", 14_000, lo=2000, hi=60_000)


def _inn_search_primary_timeout_ms() -> int:
    return _env_int("SYNAPS_INN_SEARCH_WAIT_MS", 18_000, lo=3000, hi=90_000)


def _dom_dump_selector_timeout_ms() -> int:
    return _env_int("SYNAPS_DOM_DUMP_WAIT_MS", 28_000, lo=5000, hi=120_000)


def _dom_dump_networkidle_ms() -> int:
    return _env_int("SYNAPS_DOM_DUMP_NETWORKIDLE_MS", 18_000, lo=3000, hi=90_000)


def _login_form_input_timeout_ms() -> int:
    return _env_int("SYNAPS_LOGIN_FORM_WAIT_MS", 22_000, lo=5000, hi=120_000)


def _login_navigation_timeout_ms() -> int:
    return _env_int("SYNAPS_LOGIN_NAV_MS", 45_000, lo=10_000, hi=180_000)


def _org_search_box_timeout_ms() -> int:
    return _env_int("SYNAPS_SEARCH_BOX_WAIT_MS", 18_000, lo=3000, hi=90_000)


# Файл сессии Playwright (куки + localStorage)
DEFAULT_STORAGE = Path(__file__).resolve().parent / "synaps_storage_state.json"
DOM_DUMPS_DIR = Path(__file__).resolve().parent / "dom_dumps"
DEFAULT_JSON_OUT = Path(__file__).resolve().parent / "organization_fields.json"

ORG_STABILIZE_URL = "https://synapsenet.ru/organizacii/1226900006101-ooo-zhilstroj"
# Стартовая страница с глобальным поиском (можно переопределить SYNAPS_LANDING_URL в .env)
DEFAULT_SYNAPS_LANDING_URL = "https://synapsenet.ru/organizacii/1187746623734-ekspodor-stroj"


def _synaps_landing_url() -> str:
    u = (os.getenv("SYNAPS_LANDING_URL") or "").strip().strip('"').strip("'")
    return u or DEFAULT_SYNAPS_LANDING_URL


def _org_profile_base(url: str) -> str:
    """База карточки (organizacii или searchorganization) без хвостов подстраниц."""
    u = url.split("?", 1)[0].rstrip("/")
    for suf in ("/vidy-deyatelnosti", "/ispolnitelnoe-proizvodstvo"):
        if u.endswith(suf):
            u = u[: -len(suf)]
    return u.rstrip("/")


def _dom_dump_slug_from_profile_base(profile_base: str) -> str:
    """Короткое имя файла из URL карточки (последний сегмент пути)."""
    seg = profile_base.rstrip("/").split("/")[-1] or "org"
    for ch in '<>:"/\\|?*\n\r\t':
        seg = seg.replace(ch, "_")
    return seg[:200] if seg else "org"


def _dom_dump_name_prefix(run_index: int, slug: str, source_url: str) -> str:
    """
    Уникальный префикс имени файла: порядок в задании + slug + короткий тег по исходному URL.
    Иначе две ссылки на одну организацию (organizacii/… и searchorganization/…) дают один slug
    и перезаписывают друг друга — остаётся только последний набор из 3 HTML.
    """
    tag = hashlib.sha256((source_url or "").strip().encode("utf-8")).hexdigest()[:8]
    if run_index > 0:
        return f"{run_index:02d}_{slug}_{tag}"
    return f"{slug}_{tag}"


def _dom_dumps_root() -> Path:
    p = (os.getenv("SYNAPS_DOM_DUMPS_DIR") or "").strip().strip('"').strip("'")
    if p:
        return Path(p).expanduser().resolve()
    return DOM_DUMPS_DIR.resolve()


def _stabilize_page_for_dom_dump(page: Page, *, kind: str) -> None:
    """Дождаться сети/маркеров, чтобы в HTML попал контент SPA, а не пустая оболочка."""
    t_sel = _dom_dump_selector_timeout_ms()
    t_net = _dom_dump_networkidle_ms()
    sel_main = ".oc-op-reg-date"
    try:
        if kind == "main":
            page.wait_for_selector(sel_main, state="visible", timeout=t_sel)
        elif kind == "okved":
            page.wait_for_selector(".org-card-h2, table.org-okved-table", timeout=t_sel)
        elif kind == "ip":
            page.wait_for_selector(".co-statistics-block, .org-card-h2", timeout=t_sel)
    except PlaywrightTimeoutError:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=t_net)
    except PlaywrightTimeoutError:
        pass
    _pause(page)


def _ensure_organizacii_profile(page: Page) -> None:
    """
    Ссылки из поиска …/searchorganization/organization/<slug> ведут на ту же карточку,
    но подстраницы ОКВЭД/ИП на сайте — под …/organizacii/<slug>. Переходим на канонический URL.
    """
    cur = (page.url or "").split("?", 1)[0].rstrip("/")
    if "/searchorganization/organization/" not in cur:
        return
    m = re.search(r"/searchorganization/organization/([^/?#]+)/?$", cur, re.I)
    if not m:
        return
    slug = m.group(1)
    target = f"https://synapsenet.ru/organizacii/{slug}"
    page.goto(target, wait_until="domcontentloaded")
    _pause_micro(page)
    try:
        page.wait_for_selector(".oc-op-reg-date", state="visible", timeout=_subpage_selector_timeout_ms())
    except PlaywrightTimeoutError:
        _pause(page)


def _sorted_dict(d: dict[str, Any]) -> dict[str, Any]:
    return dict(sorted(d.items(), key=lambda kv: kv[0]))

# Телефоны: 8 (...) и +7 (...) — одинаковый хвост после скобок.
_PHONE_RU_8 = re.compile(r"8\s*\(\d{3,5}\)\s*\d+(?:-\d{2}-\d{2})")
_PHONE_RU_PLUS7 = re.compile(r"\+7\s*\(\d{3,5}\)\s*\d+(?:-\d{2}-\d{2})")
_EMAIL_STRICT = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")
# ZWSP, BOM, word joiner, soft hyphen и т.п. — часто «ломают» сравнение дублей
_INVISIBLE_EMAIL_CHARS = re.compile(r"[\u200b-\u200f\ufeff\u2060\u00ad\u034f\ufeff]")
# Частые опечатки домена на карточке Synaps → одна запись в духе inbox.ru
_EMAIL_DOMAIN_TYPOS_TO_CANON: dict[str, str] = {
    "indox.ru": "inbox.ru",
    "inbo.ru": "inbox.ru",
}


def _canonicalize_email_domain(email: str) -> str:
    if "@" not in email:
        return email
    local, _, domain = email.rpartition("@")
    d = domain.lower().strip()
    fixed = _EMAIL_DOMAIN_TYPOS_TO_CANON.get(d, d)
    if fixed == d:
        return email
    return f"{local}@{fixed}"


def _clean_email_text(raw: str) -> str:
    """Убирает невидимые символы, нормализует Unicode и известные опечатки домена."""
    t = unicodedata.normalize("NFC", (raw or ""))
    t = _INVISIBLE_EMAIL_CHARS.sub("", t)
    t = t.replace("\xa0", " ")
    t = _norm_space(t)
    return _canonicalize_email_domain(t)


def clean_email_as_on_page(raw: str) -> str:
    """Как на сайте: без подмены indox/inbo → inbox (нужны отдельные строки в таблице)."""
    t = unicodedata.normalize("NFC", (raw or ""))
    t = _INVISIBLE_EMAIL_CHARS.sub("", t)
    t = t.replace("\xa0", " ")
    return _norm_space(t)


def email_sheet_line_key(addr: str) -> str:
    """Ключ дедупа почты в ячейке без слияния доменов."""
    return clean_email_as_on_page(addr).lower()


def _email_fingerprint(addr: str) -> str:
    """Ключ для дедупа (после очистки, lower)."""
    return _clean_email_text(addr).lower()


def _drop_inbox_if_same_local_has_indox(emails: list[str]) -> list[str]:
    """Если есть local@indox.ru, убираем local@inbox.ru (дубликат после правки опечатки)."""
    locals_indox: set[str] = set()
    for e in emails:
        el = clean_email_as_on_page(e).lower()
        if "@" in el and el.endswith("@indox.ru"):
            locals_indox.add(el.split("@", 1)[0])
    if not locals_indox:
        return emails
    out: list[str] = []
    for e in emails:
        el = clean_email_as_on_page(e).lower()
        if "@" in el and el.endswith("@inbox.ru"):
            if el.split("@", 1)[0] in locals_indox:
                continue
        out.append(e)
    return out


def _env(name: str, *fallbacks: str) -> str:
    for key in (name, *fallbacks):
        val = os.getenv(key)
        if val:
            return val.strip().strip('"').strip("'")
    raise RuntimeError(f"Не задана переменная окружения {name}")


def _pause(page: Page) -> None:
    page.wait_for_timeout(_action_delay_ms())


def _pause_micro(page: Page) -> None:
    ms = _micro_delay_ms()
    if ms > 0:
        page.wait_for_timeout(ms)


def _ensure_utf8_stdout() -> None:
    """Windows cp1251 ломается на ₽ и т.п. при print(json)."""
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _credentials() -> tuple[str, str, str]:
    load_dotenv(Path(__file__).resolve().parent / ".env")
    main_url = _env("MAIN_URL")
    mail = _env("MAIL", "EMAIL", "SYNAPS_MAIL")
    password = _env("PASS", "PASSWORD", "SYNAPS_PASSWORD")
    return main_url, mail, password


def _login_form_visible(page: Page) -> bool:
    return page.locator("input.demand-input.form-input-password").count() > 0


def _login_button_visible(page: Page) -> bool:
    loc = page.locator(".mh-enter-pro.click-link")
    return loc.count() > 0 and loc.first.is_visible()


def _perform_login(page: Page, mail: str, password: str) -> None:
    page.goto("https://synapsenet.ru/home/login", wait_until="domcontentloaded")
    _pause_micro(page)
    page.wait_for_selector("input.demand-input", timeout=_login_form_input_timeout_ms())
    # Почта: первый demand-input без класса пароля
    email = page.locator("input.demand-input:not(.form-input-password)").first
    email.fill(mail)
    _pause_micro(page)
    pwd = page.locator("input.demand-input.form-input-password").first
    pwd.fill(password)
    _pause_micro(page)
    page.locator(".demand-submit").first.click()
    page.wait_for_load_state("load", timeout=_login_navigation_timeout_ms())
    _pause(page)
    if _login_form_visible(page):
        raise RuntimeError("Похоже, вход не удался: форма логина всё ещё на странице.")


def _ensure_logged_in(page: Page, main_url: str, mail: str, password: str) -> None:
    page.goto(main_url, wait_until="domcontentloaded")
    _pause(page)

    if _login_form_visible(page):
        _perform_login(page, mail, password)
        return

    if _login_button_visible(page):
        page.locator(".mh-enter-pro.click-link").first.click()
        page.wait_for_url("**/home/login**", timeout=_login_form_input_timeout_ms())
        _pause_micro(page)
        _perform_login(page, mail, password)
        return

    # Кнопки входа нет — считаем, что сессия уже валидна
    _pause_micro(page)


def _org_page_loaded(page: Page) -> bool:
    return page.locator(".oc-op-reg-date").count() > 0


def _settle_after_org_goto(page: Page) -> None:
    """После goto на карточку: короткая пауза + ожидание маркера (вместо длинного sleep)."""
    _pause_micro(page)
    try:
        page.wait_for_selector(
            ".oc-op-reg-date",
            state="visible",
            timeout=_inn_search_primary_timeout_ms(),
        )
    except PlaywrightTimeoutError:
        _pause(page)


def _settle_after_landing_goto(page: Page) -> None:
    """После goto на лендинг для ИНН-поиска: дождаться поля поиска (без лишней паузы)."""
    _pause_micro(page)
    inp = _org_search_input_locator(page)
    if inp.count() == 0:
        _pause(page)
        return
    try:
        inp.wait_for(state="visible", timeout=_org_search_box_timeout_ms())
    except Exception:
        _pause(page)


def _org_search_input_locator(page: Page) -> Locator:
    return page.locator(
        'div.ocs-input-block input[placeholder="поиск организаций и ИП"], '
        'div.ocs-input-block input[aria-label="Название, ИНН или ОГРН"]',
    ).first


def _org_search_button_locator(page: Page) -> Locator:
    return page.locator("div.ocs-input-button").first


def _ensure_org_search_visible(page: Page, landing_url: str) -> Locator:
    """Поле поиска в шапке; при отсутствии — переход на стартовую страницу."""
    inp = _org_search_input_locator(page)
    if inp.count() > 0:
        try:
            if inp.is_visible():
                return inp
        except Exception:
            pass
    page.goto(landing_url, wait_until="domcontentloaded")
    _settle_after_landing_goto(page)
    inp = _org_search_input_locator(page)
    if inp.count() == 0:
        raise RuntimeError("Не найдено поле поиска организаций (.ocs-input-block input).")
    inp.wait_for(state="visible", timeout=_org_search_box_timeout_ms())
    return inp


def _click_first_organizacii_result_if_needed(page: Page) -> None:
    if _org_page_loaded(page):
        return
    links = page.locator('a[href*="/organizacii/"]')
    if links.count() == 0:
        return
    try:
        first = links.first
        if first.is_visible():
            first.click()
            _pause_micro(page)
    except Exception:
        pass


def navigate_to_organization_by_inn(page: Page, inn_raw: str, *, landing_url: str | None = None) -> None:
    """
    Ввод ИНН в глобальный поиск, клик по кнопке поиска, ожидание карточки организации.
    Работает со страницы лендинга или с уже открытой карточки (поиск в шапке).
    """
    landing = landing_url or _synaps_landing_url()
    inn = re.sub(r"\D", "", (inn_raw or "").strip())
    if not inn:
        raise ValueError("Пустой ИНН")

    inp = _ensure_org_search_visible(page, landing)
    inp.click()
    _pause_micro(page)
    inp.fill("")
    inp.fill(inn)
    _pause_micro(page)

    btn = _org_search_button_locator(page)
    if btn.count() == 0:
        inp.press("Enter")
    else:
        btn.click()

    t_primary = _inn_search_primary_timeout_ms()
    try:
        page.wait_for_selector(".oc-op-reg-date", state="visible", timeout=t_primary)
    except PlaywrightTimeoutError:
        _click_first_organizacii_result_if_needed(page)
        try:
            page.wait_for_selector(
                ".oc-op-reg-date",
                state="visible",
                timeout=max(4_000, t_primary // 2),
            )
        except PlaywrightTimeoutError:
            pass

    if not _org_page_loaded(page):
        raise RuntimeError(f"После поиска по ИНН {inn} карточка не открылась (нет .oc-op-reg-date). URL: {page.url}")


def _extract_reg_date(page: Page) -> str | None:
    loc = page.locator(".oc-op-reg-date")
    if loc.count() == 0:
        return None
    raw = loc.first.inner_text()
    # Берём первую дату вида ДД.ММ.ГГГГ
    m = re.search(r"\d{2}\.\d{2}\.\d{4}", raw)
    return m.group(0) if m else raw.strip()


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def _extract_capital_rub(page: Page) -> int | str | None:
    """P: число рублей из .ocb-capital-line или текст «отсутствует»."""
    loc = page.locator(".ocb-capital-line")
    if loc.count() == 0:
        return None
    row = loc.first
    amount_div = row.locator(":scope > div").nth(1)
    if amount_div.count() == 0:
        raw = _norm_space(row.inner_text())
    else:
        raw = _norm_space(amount_div.text_content() or "")
    low = raw.lower()
    if "отсутствует" in low and not re.search(r"\d", raw):
        return "отсутствует"
    digits = re.sub(r"\D", "", raw.replace(" ", "").replace("\xa0", ""))
    return int(digits) if digits else None


def _expand_contacts_for_parse(page: Page) -> None:
    """Перед Q/T: раскрыть телефоны и почты (иначе часть только в скрытом блоке)."""
    phone_btn = page.locator(".ocb-show-all.ocb-phone-but-script")
    if phone_btn.count() > 0 and phone_btn.first.is_visible():
        t = (phone_btn.first.inner_text() or "").lower()
        if "свернуть" not in t:
            phone_btn.first.click()
            _pause_micro(page)

    mail_btn = page.locator(".ocb-show-all.ocb-email-but-script")
    if mail_btn.count() > 0 and mail_btn.first.is_visible():
        t = (mail_btn.first.inner_text() or "").lower()
        if "свернуть" not in t:
            mail_btn.first.click()
            _pause_micro(page)


def _year_value_map_finance_table2(page: Page) -> dict[int, int]:
    """Вторая колонка (тыс. руб) по году, таблица data-fintype=\"2\"."""
    table = page.locator('table.oc-finance-table[data-fintype="2"]')
    if table.count() == 0:
        return {}
    out: dict[int, int] = {}
    rows = table.first.locator("tbody tr")
    for i in range(rows.count()):
        row = rows.nth(i)
        tds = row.locator("td")
        if tds.count() < 2:
            continue
        y_raw = _norm_space(tds.nth(0).text_content() or "")
        if not re.fullmatch(r"\d{4}", y_raw):
            continue
        year = int(y_raw)
        mid = _norm_space(tds.nth(1).text_content() or "")
        digits = re.sub(r"\D", "", mid)
        if digits:
            out[year] = int(digits)
    return out


def _finance_pct_display_key(td: Locator) -> str:
    """Текст третьей ячейки для ключа (процент / тире), без стрелок и пробелов."""
    raw = td.inner_text() or ""
    t = raw.replace("\xa0", " ")
    for ch in "\u2191\u2192\u2193\u2190↑↓→←":
        t = t.replace(ch, "")
    t = re.sub(r"\s+", "", _norm_space(t))
    if not t or t in ("—", "-", "–") or "—" in t:
        return "—"
    return t


def _finance_y_2025(page: Page) -> dict[str, str] | None:
    """
    Y: строка 2025, 3-я ячейка таблицы fintype=2.
    oc-finance-up → {показатель: "рост"}, oc-finance-down → {…: "падение"}; иначе None.
    """
    table = page.locator('table.oc-finance-table[data-fintype="2"]')
    if table.count() == 0:
        return None
    rows = table.first.locator("tbody tr")
    for i in range(rows.count()):
        row = rows.nth(i)
        tds = row.locator("td")
        if tds.count() < 3:
            continue
        if _norm_space(tds.nth(0).text_content() or "") != "2025":
            continue
        td3 = tds.nth(2)
        cls = td3.get_attribute("class") or ""
        key = _finance_pct_display_key(td3)
        if "oc-finance-up" in cls:
            return {key: "рост"}
        if "oc-finance-down" in cls:
            return {key: "падение"}
        return None
    return None


def _pack_year_field(yv: dict[int, int], year: int) -> dict[str, int] | None:
    if year not in yv:
        return None
    return {str(year): yv[year]}


def _extract_reliability_z(page: Page) -> str | None:
    """Z: надёжность из .oct-flag (green → высокая, orange → средняя)."""
    if page.locator(".oct-flag.octf-green").count() > 0:
        return "высокая"
    if page.locator(".oct-flag.octf-orange").count() > 0:
        return "средняя"
    return None


def _extract_director_ab(page: Page) -> dict[str, str] | None:
    """AB: {ФИО: текст из .org-last-change}."""
    block = page.locator(".org-director-block")
    if block.count() == 0:
        return None
    root = block.first
    change = root.locator(".org-last-change").first
    if change.count() == 0:
        return None
    ch = _norm_space(change.text_content() or "")
    # Имя — второй прямой div (после подписи «ГЕНЕРАЛЬНЫЙ ДИРЕКТОР»)
    name_el = root.locator(":scope > div").nth(1)
    name = _norm_space(name_el.text_content() or "") if name_el.count() else ""
    if not name or not ch:
        return None
    return _sorted_dict({name: ch})


def _dedupe_phone_list(phones: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for p in phones:
        key = re.sub(r"\s+", "", p.strip())
        if key and key not in seen:
            seen.add(key)
            out.append(p.strip())
    return out


def _dedupe_email_list(emails: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for e in emails:
        clean = clean_email_as_on_page(e)
        if not clean or not _EMAIL_STRICT.match(clean):
            continue
        key = email_sheet_line_key(clean)
        if key not in seen:
            seen.add(key)
            out.append(clean)
    return out


def _extract_okved_r(page: Page) -> dict[str, str]:
    """
    R: только основной ОКВЭД — первая org-card-h2 с текстом «Основной вид деятельности»,
    затем первая строка первой table.org-okved-table среди следующих sibling-узлов (до следующего org-card-h2).
    """
    rows = page.evaluate(
        """() => {
        const needle = "основной вид деятельности";
        const h2s = document.querySelectorAll(".org-card-h2");
        let anchor = null;
        for (const el of h2s) {
            const t = ((el.textContent || "").replace(/\\s+/g, " ").trim()).toLowerCase();
            if (t.includes(needle)) {
                anchor = el;
                break;
            }
        }
        if (!anchor) return [];
        let cur = anchor.nextElementSibling;
        while (cur) {
            if (cur.classList && cur.classList.contains("org-card-h2")) break;
            let tbl = null;
            if (cur.tagName === "TABLE" && cur.classList && cur.classList.contains("org-okved-table")) {
                tbl = cur;
            } else if (cur.querySelector) {
                tbl = cur.querySelector("table.org-okved-table");
            }
            if (tbl) {
                const tr = tbl.querySelector("tr");
                if (!tr) return [];
                const cells = tr.querySelectorAll("td, th");
                if (cells.length < 2) return [];
                const k = (cells[0].innerText || "").replace(/\\s+/g, " ").trim();
                const v = (cells[1].innerText || "").replace(/\\s+/g, " ").trim();
                return k ? [[k, v]] : [];
            }
            cur = cur.nextElementSibling;
        }
        return [];
    }"""
    )
    d: dict[str, str] = {}
    for pair in rows or []:
        if len(pair) >= 2 and pair[0]:
            d[str(pair[0])] = str(pair[1])
    return _sorted_dict(d)


def _extract_aa_statistics(page: Page) -> dict[str, str]:
    """
    AA: детальная сводка по долгам:
    - кол-во активных производств
    - общая сумма задолженности
    - % от уставного капитала (если есть)
    - % относительно прибыли (если есть)
    Учитываются формулировки «составляет 2,01%» и «составляет менее 0,01%».
    """
    active = "-"
    stats_blocks = page.locator(".org-bailiff-statistics .co-statistics-block")
    for i in range(stats_blocks.count()):
        b = stats_blocks.nth(i)
        label = _norm_space(b.locator(":scope > div").first.text_content() or "").lower()
        if "актив" in label:
            val = _norm_space(b.locator(":scope > div").nth(1).text_content() or "")
            active = val if val else "-"
            break

    seo = page.locator(".org-bailiff-seo-text").first
    seo_text = _norm_space(seo.text_content() or "") if seo.count() > 0 else ""

    total_sum = "-"
    m_sum = re.search(
        r"Общая\s+сумма\s+задолженности[^.]*?составляет\s+([\d\s\xa0]+)\s*руб",
        seo_text,
        re.I,
    )
    if m_sum:
        total_sum = _norm_space(m_sum.group(1).replace("\xa0", " "))

    cap_pct = "-"
    m_cap = re.search(
        r"составляет\s+(?:менее\s+)?([\d\s,\xa0]+)%\s*от\s+уставного\s+капитала",
        seo_text,
        re.I,
    )
    if m_cap:
        cap_pct = re.sub(r"\s+", "", m_cap.group(1).replace("\xa0", "")).replace(",", ".") + "%"

    profit_pct = "-"
    m_profit = re.search(
        r"Относительно\s+прибыли[^.]*?составляет\s+(?:менее\s+)?([\d\s,\xa0]+)%",
        seo_text,
        re.I | re.DOTALL,
    )
    if m_profit:
        profit_pct = re.sub(r"\s+", "", m_profit.group(1).replace("\xa0", "")).replace(",", ".") + "%"

    return _sorted_dict(
        {
            "кол-во": active,
            "сумма": total_sum,
            "задолжн. относит. устанвой капитал": cap_pct,
            "относительно прибыли": profit_pct,
        },
    )


def _has_ispolnitelnoe_section(page: Page, profile_base: str) -> bool:
    """
    Раздел «Долги» на главной: ссылка/data-href на …/ispolnitelnoe-proizvodstvo.
    Если пункта нет (напр. ИП без страницы долгов), в AA не подставляем данные с чужой страницы.
    """
    slug = profile_base.rstrip("/").split("/")[-1]
    if page.locator(f'[data-href*="{slug}/ispolnitelnoe-proizvodstvo"]').count() > 0:
        return True
    if page.locator('[data-href*="/ispolnitelnoe-proizvodstvo"]').count() > 0:
        return True
    if page.locator('a[href*="/ispolnitelnoe-proizvodstvo"]').count() > 0:
        return True
    return False


def _extract_employees_count_ac(page: Page) -> str | None:
    """AC: среднесписочная численность — внутри .org-smp-block ищем span с title про численность."""
    block = page.locator(".org-smp-block").first
    if block.count() == 0:
        return None

    def _digits_from_locator(loc: Locator) -> str | None:
        if loc.count() == 0:
            return None
        txt = _norm_space(loc.first.text_content() or "")
        m = re.search(r"\d+", txt)
        return m.group(0) if m else None

    # Как на сайте: внутри org-smp-block span с полным title.
    for sel in (
        'span[title="Среднесписочная численность"]',
        'span[title*="Среднесписочная численность"]',
    ):
        hit = _digits_from_locator(block.locator(sel))
        if hit:
            return hit

    # Запасной вариант: весь текст блока (другая вёрстка / без title на span).
    blob = _norm_space(block.inner_text() or "")
    if not re.search(r"численн|работник", blob, re.I):
        return None
    m2 = re.search(r"\b(\d+)\b", blob)
    return m2.group(1) if m2 else None


def _contacts_block_by_strong(page: Page, label: str):
    """Подпись на сайте может быть с разным регистром («Телефон»)."""
    pat = re.compile(re.escape(label), re.I)
    return page.locator("div.org-contacts-block").filter(
        has=page.locator("strong").filter(has_text=pat),
    ).first


def _email_candidates_from_text_chunk(s: str) -> list[str]:
    """Все подстроки вида user@host из текста (вложенные div, склейка inner_text)."""
    if not (s or "").strip():
        return []
    out: list[str] = []
    seen_line: set[str] = set()
    for m in re.finditer(
        r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
        s,
    ):
        t = clean_email_as_on_page(m.group(0))
        if not _EMAIL_STRICT.match(t):
            continue
        k = email_sheet_line_key(t)
        if k not in seen_line:
            seen_line.add(k)
            out.append(t)
    return out


def _phones_in_text_chunks(s: str) -> list[str]:
    out: list[str] = []
    for rx in (_PHONE_RU_8, _PHONE_RU_PLUS7):
        for m in rx.finditer(s or ""):
            out.append(_norm_space(m.group(0)))
    return out


def _extract_phones(page: Page) -> list[str]:
    """Q: все номера из блока контактов «телефон» (включая скрытый #ocb-phone-block-script)."""
    block = _contacts_block_by_strong(page, "телефон")
    if block.count() == 0:
        return []

    seen: set[str] = set()
    out: list[str] = []
    root = block.first
    for i in range(root.locator(":scope > div").count()):
        div = root.locator(":scope > div").nth(i)
        div_id = div.get_attribute("id") or ""
        if div_id == "ocb-phone-block-script":
            for j in range(div.locator(":scope > div").count()):
                inner = div.locator(":scope > div").nth(j)
                for p in _phones_in_text_chunks(inner.text_content() or ""):
                    if p not in seen:
                        seen.add(p)
                        out.append(p)
            continue
        for p in _phones_in_text_chunks(div.text_content() or ""):
            if p not in seen:
                seen.add(p)
                out.append(p)
    return _dedupe_phone_list(out)


def _extract_emails(page: Page) -> list[str]:
    """T: видимая строка и строки из #ocb-email-block-script — только целые email по тексту leaf-div."""
    block = _contacts_block_by_strong(page, "электронная почта")
    if block.count() == 0:
        return []

    def _push(raw: str, bucket: list[str], seen: set[str]) -> None:
        t = clean_email_as_on_page(raw)
        if not _EMAIL_STRICT.match(t):
            return
        key = email_sheet_line_key(t)
        if key not in seen:
            seen.add(key)
            bucket.append(t)

    seen: set[str] = set()
    out: list[str] = []
    root = block.first
    for i in range(root.locator(":scope > div").count()):
        div = root.locator(":scope > div").nth(i)
        div_id = div.get_attribute("id") or ""
        if div_id == "ocb-email-block-script":
            for j in range(div.locator(":scope > div").count()):
                _push(div.locator(":scope > div").nth(j).text_content() or "", out, seen)
            continue
        if div.locator("strong").count() > 0:
            continue
        _push(div.text_content() or "", out, seen)
    # Доп. адреса только в #ocb-email-block-script: обходим любую вложенность и целиком inner_text
    # (на случай, если не только прямые :scope > div листья или text_content отличается от видимого).
    scripts = root.locator("#ocb-email-block-script")
    for k in range(scripts.count()):
        el = scripts.nth(k)
        for t in _email_candidates_from_text_chunk(el.inner_text() or ""):
            _push(t, out, seen)
    merged = _dedupe_email_list(out)
    return _drop_inbox_if_same_local_has_indox(merged)


def _extract_legal_address(page: Page) -> str | None:
    """S: .oc-full-adress .copy-script"""
    loc = page.locator(".oc-full-adress .copy-script")
    if loc.count() == 0:
        return None
    return _norm_space(loc.first.text_content() or "")


def _fetch_bank_accounts_comment(page: Page) -> str | None:
    """U: кнопка #check-bank-account → текст .ba-rb-comment."""
    btn = page.locator("#check-bank-account.oba-check-bank")
    if btn.count() == 0:
        return None
    btn.first.click()
    _pause_micro(page)
    try:
        page.wait_for_selector(".ba-rb-comment", timeout=_bank_comment_timeout_ms())
    except PlaywrightTimeoutError:
        return None
    _pause_micro(page)
    cm = page.locator(".ba-rb-comment").first
    if cm.count() == 0:
        return None
    return _norm_space(cm.text_content() or "")


def extract_organization_json(
    page: Page,
    *,
    save_dom_snapshots: bool = False,
    dom_dump_run_index: int = 0,
    dom_source_url: str = "",
) -> dict[str, Any]:
    """
    Главная карточка: O–U, V–Z, AB; затем /vidy-deyatelnosti → R; /ispolnitelnoe-proizvodstvo → AA.
    Перед Q/T раскрываются списки телефонов и почты.
    Ключи верхнего уровня и вложенных dict (R, AA, AB, Y при наличии) — по алфавиту.

    При save_dom_snapshots=True в dom_dumps/ (или SYNAPS_DOM_DUMPS_DIR) пишутся три HTML: главная
    после всех кликов, ОКВЭД, ИП. dom_dump_run_index + dom_source_url дают уникальное имя файла
    при разных ссылках на одну и ту же карточку.
    """
    _ensure_organizacii_profile(page)
    profile_base = _org_profile_base(page.url)
    dom_slug = _dom_dump_slug_from_profile_base(profile_base)
    dom_base = _dom_dump_name_prefix(dom_dump_run_index, dom_slug, dom_source_url or page.url)

    _expand_contacts_for_parse(page)
    # Пока на главной: есть ли в меню раздел «Долги» (иначе /ispolnitelnoe-proizvodstvo может отсутствовать).
    has_ip_section = _has_ispolnitelnoe_section(page, profile_base)

    yv = _year_value_map_finance_table2(page)
    y25 = _finance_y_2025(page)
    v, w, x = _pack_year_field(yv, 2023), _pack_year_field(yv, 2024), _pack_year_field(yv, 2025)
    data: dict[str, Any] = {
        "O": _extract_reg_date(page),
        "P": _extract_capital_rub(page),
        "Q": _extract_phones(page),
        "S": _extract_legal_address(page),
        "T": _extract_emails(page),
        "AC": _extract_employees_count_ac(page),
        "V": _sorted_dict(v) if v else None,
        "W": _sorted_dict(w) if w else None,
        "X": _sorted_dict(x) if x else None,
        "Y": _sorted_dict(y25) if y25 else None,
        "Z": _extract_reliability_z(page),
        "AB": _extract_director_ab(page),
    }
    data["U"] = _fetch_bank_accounts_comment(page)

    if save_dom_snapshots:
        _stabilize_page_for_dom_dump(page, kind="main")
        _save_dom(page, f"{dom_base}__01_main_after_actions.html")

    page.goto(f"{profile_base}/vidy-deyatelnosti", wait_until="domcontentloaded")
    _pause_micro(page)
    if save_dom_snapshots:
        _stabilize_page_for_dom_dump(page, kind="okved")
    else:
        try:
            page.wait_for_selector(
                ".org-card-h2, table.org-okved-table",
                timeout=_subpage_selector_timeout_ms(),
            )
        except PlaywrightTimeoutError:
            pass
    data["R"] = _extract_okved_r(page)
    if save_dom_snapshots:
        _save_dom(page, f"{dom_base}__02_okved.html")

    if has_ip_section:
        page.goto(f"{profile_base}/ispolnitelnoe-proizvodstvo", wait_until="domcontentloaded")
        _pause_micro(page)
        if save_dom_snapshots:
            _stabilize_page_for_dom_dump(page, kind="ip")
        else:
            try:
                page.wait_for_selector(
                    ".co-statistics-block, .org-card-h2",
                    timeout=_subpage_selector_timeout_ms(),
                )
            except PlaywrightTimeoutError:
                pass
        data["AA"] = _extract_aa_statistics(page)
        if save_dom_snapshots:
            _save_dom(page, f"{dom_base}__03_ispolnitelnoe.html")
    else:
        data["AA"] = None

    page.goto(profile_base, wait_until="domcontentloaded")
    _pause_micro(page)
    if not save_dom_snapshots:
        try:
            page.wait_for_selector(
                ".oc-op-reg-date",
                state="visible",
                timeout=_subpage_selector_timeout_ms(),
            )
        except PlaywrightTimeoutError:
            _pause(page)
    else:
        _pause(page)
    return _sorted_dict(data)


def _save_dom(page: Page, filename: str) -> Path:
    root = _dom_dumps_root()
    root.mkdir(parents=True, exist_ok=True)
    path = root / filename
    html = page.content()
    path.write_text(html, encoding="utf-8")
    n = len(html)
    if n < 5000:
        print(
            f"  предупреждение DOM: всего {n} символов — часто это редирект на логин или пустая "
            f"оболочка SPA. Файл: {path}",
        )
    else:
        print(f"  DOM: {path.name} (~{n // 1024} КиБ) → {path}")
    return path


def resolved_dom_dumps_dir() -> Path:
    """Актуальный каталог дампов (учитывает SYNAPS_DOM_DUMPS_DIR)."""
    return _dom_dumps_root()


SHEET_JSON_KEYS: tuple[str, ...] = ("AA", "AB", "AC", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z")


def scrape_urls_sequentially(
    org_urls: list[str],
    *,
    headless: bool = False,
    storage_path: Path | None = None,
    save_dom_snapshots: bool = False,
    on_each_result: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any | BaseException]:
    """
    Один браузер и один контекст: логин один раз, затем по очереди все URL без закрытия окна между ними.
    Ключи результата — те же строки URL, что были переданы (в т.ч. searchorganization).

    on_each_result(url, data) — вызывается сразу после успешного парсинга каждого URL (удобно для
    немедленной записи в Google Sheets без ожидания конца всего списка).
    """
    main_url, mail, password = _credentials()
    storage = storage_path or DEFAULT_STORAGE
    results: dict[str, Any | BaseException] = {}
    _ensure_utf8_stdout()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context_args: dict = {}
        if storage.exists():
            context_args["storage_state"] = str(storage)
        context = browser.new_context(**context_args)
        page = context.new_page()

        def _recover_session() -> None:
            nonlocal context, page
            if storage.exists():
                storage.unlink()
            try:
                context.close()
            except Exception:
                pass
            context = browser.new_context()
            page = context.new_page()
            _ensure_logged_in(page, main_url, mail, password)
            context.storage_state(path=str(storage))

        try:
            _ensure_logged_in(page, main_url, mail, password)
            context.storage_state(path=str(storage))

            todo = [u.strip() for u in org_urls if u and u.strip()]
            total = len(todo)
            for idx, u in enumerate(todo, start=1):
                try:
                    print(f"[{idx}/{total}] {u}")
                    page.goto(u, wait_until="domcontentloaded")
                    _settle_after_org_goto(page)
                    if not _org_page_loaded(page) or "home/login" in (page.url or ""):
                        _recover_session()
                        page.goto(u, wait_until="domcontentloaded")
                        _settle_after_org_goto(page)
                    if not _org_page_loaded(page):
                        raise RuntimeError(f"Не удалось открыть карточку (нет .oc-op-reg-date): {u}")
                    results[u] = extract_organization_json(
                        page,
                        save_dom_snapshots=save_dom_snapshots,
                        dom_dump_run_index=idx,
                        dom_source_url=u,
                    )
                    if on_each_result is not None:
                        on_each_result(u, results[u])
                    context.storage_state(path=str(storage))
                except Exception as e:
                    results[u] = e
        finally:
            try:
                context.storage_state(path=str(storage))
            except Exception:
                pass
            context.close()
            browser.close()

    return results


def scrape_inns_sequentially(
    inns: list[str],
    *,
    headless: bool = False,
    storage_path: Path | None = None,
    save_dom_snapshots: bool = False,
    on_each_result: Callable[[str, dict[str, Any]], None] | None = None,
    landing_url: str | None = None,
) -> dict[str, Any | BaseException]:
    """
    Тот же один браузер и сессия: для каждого ИНН — ввод в поиск на сайте, переход на карточку, парсинг.
    Ключи результата — нормализованный ИНН (только цифры), как в переданном списке после нормализации.
    """
    main_url, mail, password = _credentials()
    storage = storage_path or DEFAULT_STORAGE
    landing = landing_url or _synaps_landing_url()
    results: dict[str, Any | BaseException] = {}
    _ensure_utf8_stdout()

    todo_raw = [re.sub(r"\D", "", (x or "").strip()) for x in inns if x and str(x).strip()]
    todo = list(dict.fromkeys(todo_raw))
    if not todo:
        return results

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context_args: dict = {}
        if storage.exists():
            context_args["storage_state"] = str(storage)
        context = browser.new_context(**context_args)
        page = context.new_page()

        def _recover_session() -> None:
            nonlocal context, page
            if storage.exists():
                storage.unlink()
            try:
                context.close()
            except Exception:
                pass
            context = browser.new_context()
            page = context.new_page()
            _ensure_logged_in(page, main_url, mail, password)
            context.storage_state(path=str(storage))

        try:
            _ensure_logged_in(page, main_url, mail, password)
            context.storage_state(path=str(storage))

            page.goto(landing, wait_until="domcontentloaded")
            _settle_after_landing_goto(page)

            total = len(todo)
            for idx, inn in enumerate(todo, start=1):
                try:
                    print(f"[{idx}/{total}] ИНН {inn}")
                    navigate_to_organization_by_inn(page, inn, landing_url=landing)
                    if not _org_page_loaded(page) or "home/login" in (page.url or ""):
                        _recover_session()
                        page.goto(landing, wait_until="domcontentloaded")
                        _settle_after_landing_goto(page)
                        navigate_to_organization_by_inn(page, inn, landing_url=landing)
                    if not _org_page_loaded(page):
                        raise RuntimeError(f"Карточка не открылась после поиска по ИНН {inn}")
                    src_tag = f"inn:{inn}"
                    results[inn] = extract_organization_json(
                        page,
                        save_dom_snapshots=save_dom_snapshots,
                        dom_dump_run_index=idx,
                        dom_source_url=src_tag,
                    )
                    if on_each_result is not None:
                        on_each_result(inn, results[inn])
                    context.storage_state(path=str(storage))
                except Exception as e:
                    results[inn] = e
        finally:
            try:
                context.storage_state(path=str(storage))
            except Exception:
                pass
            context.close()
            browser.close()

    return results


def run(
    *,
    headless: bool = False,
    storage_path: Path | None = None,
    urls: list[str] | None = None,
    loop_rounds: int = 1,
    loop_sleep_sec: float = 0,
    json_out: Path | None = None,
    save_dom: bool = True,
) -> dict[str, Any]:
    storage = storage_path or DEFAULT_STORAGE
    visit_urls = list(dict.fromkeys(urls or [ORG_STABILIZE_URL]))
    out_path = json_out or DEFAULT_JSON_OUT
    _ensure_utf8_stdout()

    scraped: dict[str, Any] = {}
    for round_idx in range(max(1, loop_rounds)):
        if round_idx > 0 and loop_sleep_sec > 0:
            time.sleep(loop_sleep_sec)
        results = scrape_urls_sequentially(
            visit_urls,
            headless=headless,
            storage_path=storage,
            save_dom_snapshots=save_dom,
        )
        if len(visit_urls) == 1:
            u = visit_urls[0]
            val = results[u]
            if isinstance(val, BaseException):
                raise val
            scraped = val
        else:
            scraped = {}
            for k, v in results.items():
                if isinstance(v, BaseException):
                    scraped[k] = {"_error": str(v)}
                else:
                    scraped[k] = v

    print(json.dumps(scraped, ensure_ascii=False, indent=2, sort_keys=True))
    out_path.write_text(
        json.dumps(scraped, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"JSON: {out_path}")

    if save_dom:
        n = len(visit_urls) * 3
        print(
            f"DOM-снимки: до {n} HTML ({len(visit_urls)} карточек × 3 страницы) в {resolved_dom_dumps_dir()}",
        )

    return scraped


if __name__ == "__main__":
    load_dotenv(Path(__file__).resolve().parent / ".env")
    raw = (os.getenv("SYNAPS_TEST_URLS") or "").strip()
    url_list = [u.strip() for u in raw.split(",") if u.strip()] if raw else None
    if not url_list:
        print(
            "Локальный тест: в .env задайте SYNAPS_TEST_URLS=url1,url2 (через запятую). "
            "Опционально: SYNAPS_TEST_HEADLESS=1, SYNAPS_TEST_ROUNDS=2, SYNAPS_TEST_SLEEP_BETWEEN_ROUNDS=1.5, "
            "SYNAPS_TEST_JSON_OUT=путь.json, SYNAPS_TEST_NO_DOM=1",
        )
        raise SystemExit(0)
    headless = (os.getenv("SYNAPS_TEST_HEADLESS") or "").strip().lower() in ("1", "true", "yes", "on")
    rounds = int((os.getenv("SYNAPS_TEST_ROUNDS") or "1").strip() or "1")
    sleep = float((os.getenv("SYNAPS_TEST_SLEEP_BETWEEN_ROUNDS") or "0").strip() or "0")
    json_out = (os.getenv("SYNAPS_TEST_JSON_OUT") or "").strip()
    jpath = Path(json_out) if json_out else None
    save_dom = (os.getenv("SYNAPS_TEST_NO_DOM") or "").strip().lower() not in ("1", "true", "yes", "on")
    run(
        headless=headless,
        urls=url_list,
        loop_rounds=max(1, rounds),
        loop_sleep_sec=max(0.0, sleep),
        json_out=jpath,
        save_dom=save_dom,
    )
