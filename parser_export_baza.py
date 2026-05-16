"""
Экспорт база: копия SHEET_UNIQUE → SHEET_FINAL, опционально перенос по ИНН с SHEET_IMPORT, затем Synaps.

Если в .env задан SHEET_IMPORT (лист со старыми спарсенными данными), перед парсингом поля O…AC
копируются на SHEET_FINAL по совпадению ИНН; строки без ИНН в импортёре обходятся браузером.
Без SHEET_IMPORT — только копия и парсинг с нуля.

Копирование на SHEET_FINAL — по совпадению ИНН (INN_PREFIX_COMPARE_ROWS) или PARSER_FORCE_COPY=1.

Строка: есть ссылка Synaps → URL; иначе ИНН → поиск на сайте.

Классическая фиксация столбцов U=долг, W=численность (старые таблицы): PARSER_CLASSIC_UW_COLUMNS=1.
Численность Synaps (AC) в последний столбец: PARSER_AC_LAST_COLUMN=1.

Режим копии между двумя листами: в .env задайте SHEET_COPY_FROM и SHEET_COPY_TO, либо PARSER_RUN_20M=1
(листы SHEET_20M_SOURCE / SHEET_20M_FINAL). Опции запуска — только переменные окружения (см. parser_sheet_env.stage2_options).
"""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Callable

import gspread
from dotenv import load_dotenv
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1

from parser_fix_columns import apply_u_w_overrides, restore_synaps_bank_column
from parser_sheet_env import sheet_final, sheet_import, sheet_unique
from parser_synaps_columns import (
    build_bootstrap_merged_headers,
    first_header_row_nonempty,
    has_inn_column_in_headers,
)
from parser_synaps_browser import (
    SHEET_JSON_KEYS,
    clean_email_as_on_page,
    email_sheet_line_key,
    resolved_dom_dumps_dir,
    scrape_inns_sequentially,
    scrape_urls_sequentially,
)

ROOT = Path(__file__).resolve().parent
# Сколько первых строк данных сверять по ИНН перед решением «копировать или только парсить»
INN_PREFIX_COMPARE_ROWS = 100
SOURCE_URL_COL = 2  # B — если столбец со ссылкой не найден по заголовку
FALLBACK_URL_COL = 1  # A
_BANK_OK_PHRASE = "действующие решения о приостановлении отсутствуют"

# Нормализованный заголовок -> логический ключ парсера.
# Важно: не маппить общее «дата регистрации» на O — у заказчика это уже своя колонка (ЕГРЮЛ),
# и строка ошибочно считалась «уже с данными парсера». Для даты с Synaps — отдельный заголовок.
HEADER_TO_KEY: dict[str, str] = {
    "дата регистрации ооо": "O",
    "дата регистрации (синапс)": "O",
    "дата регистрации синапс": "O",
    "уставной": "P",
    "доп телефон": "Q",
    "основной оквед": "R",
    "основной оквэд": "R",
    "основной okved": "R",
    "основной вид деятельности": "R",
    "оквэд": "R",
    "юр адрес": "S",
    "юридический адрес": "S",
    "доп имеил": "T",
    "доп имейл": "T",
    "доп email": "T",
    "состояние банковского счета": "U",
    "состояние банковского счёта": "U",
    "счет": "U",
    "счёт": "U",
    "23": "V",
    "24": "W",
    "25": "X",
    "тренд по выручке": "Y",
    "надежность": "Z",
    "долг по исполнительному производству": "AA",
    "долг по исполнительному производству:": "AA",
    "генеральный директор": "AB",
    "ген дир": "AB",
    "численность": "AC",
    "численность сотрудников": "AC",
    "среднесписочная численность": "AC",
}


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    return (v or "").strip().strip('"').strip("'")


def _credentials_path() -> Path:
    p = _env("GOOGLE_CREDENTIALS_JSON", _env("GOOGLE_APPLICATION_CREDENTIALS"))
    if not p:
        raise RuntimeError("В .env задайте GOOGLE_CREDENTIALS_JSON=путь к json сервисного аккаунта")
    path = Path(p)
    if not path.is_absolute():
        path = ROOT / path
    if not path.is_file():
        raise FileNotFoundError(f"Файл учётных данных не найден: {path}")
    return path


def _sheet_id() -> str:
    sid = _env("SHEET_ID")
    if not sid:
        raise RuntimeError("В .env задайте SHEET_ID (id таблицы из URL)")
    return sid


def _is_synaps_org_url(s: str) -> bool:
    t = (s or "").strip().lower()
    for prefix in ("https://", "http://"):
        if t.startswith(prefix):
            t = t[len(prefix) :]
    if t.startswith("www."):
        t = t[4:]
    t = t.replace(" ", "")
    return "synapsenet.ru/organizacii/" in t or "synapsenet.ru/searchorganization/organization/" in t


def _extract_hyperlink_formula_url(s: str) -> str:
    text = (s or "").strip()
    if not text:
        return ""
    # =HYPERLINK("https://..."; "текст") / =HYPERLINK("https://...", "text")
    m = re.match(r'^\s*=HYPERLINK\(\s*"([^"]+)"\s*[,;].*\)\s*$', text, flags=re.I)
    if not m:
        return ""
    return (m.group(1) or "").strip()


def _extract_synaps_url(primary_display: str, primary_formula: str, fallback_display: str) -> str:
    primary_display = (primary_display or "").strip()
    if _is_synaps_org_url(primary_display):
        return primary_display

    formula_url = _extract_hyperlink_formula_url(primary_formula)
    if _is_synaps_org_url(formula_url):
        return formula_url

    fallback_display = (fallback_display or "").strip()
    if _is_synaps_org_url(fallback_display):
        return fallback_display
    return ""


def _canon_header_label(h: str) -> str:
    t = (h or "").strip().lower()
    t = t.replace("ё", "е")
    t = re.sub(r"\s+", " ", t)
    return t.rstrip(":").strip()


def _normalize_inn_cell(value: str) -> str:
    return "".join(ch for ch in (value or "").strip() if ch.isdigit())


def _inn_column_1based(headers: list[str]) -> int | None:
    for i, raw in enumerate(headers):
        if _canon_header_label(raw) == "инн":
            return i + 1
    return None


def _synaps_url_column_1based(headers: list[str]) -> int:
    """Столбец со ссылкой Synaps (1-based). Если в заголовках не нашли — как раньше, столбец B."""
    for i, raw in enumerate(headers):
        c = _canon_header_label(raw)
        if not c:
            continue
        compact = c.replace(" ", "")
        if "synapsenet" in compact:
            return i + 1
        if "synaps" in compact and ("url" in compact or "ссыл" in c or "link" in compact):
            return i + 1
        if "(ссылка)" in c and any(
            x in c for x in ("поставщик", "исполнитель", "организац", "заказчик", "компани")
        ):
            return i + 1
    return SOURCE_URL_COL


def _normalize_header(h: str) -> str | None:
    t = (h or "").strip()
    if not t:
        return None
    if t in SHEET_JSON_KEYS:
        return t
    u = t.upper()
    if u in SHEET_JSON_KEYS:
        return u
    c = _canon_header_label(t)
    # Колонка из тендера «численность сотрудников (чел.) *» — не поле Synaps AC (численность в конце таблицы).
    if "(чел." in c or "(чел)" in c or "(человек" in c:
        return None
    if c in HEADER_TO_KEY:
        return HEADER_TO_KEY[c]
    if "дополнительн" not in c and "основн" in c and ("оквэд" in c or "оквед" in c or "okved" in c):
        return "R"
    return None


def _dedupe_lines_join(items: list[str], *, phone: bool) -> str:
    seen: set[str] = set()
    out: list[str] = []
    for raw in items:
        x = str(raw).strip()
        if not x:
            continue
        if phone:
            key = re.sub(r"\s+", "", x)
            line = x
        else:
            line = clean_email_as_on_page(x)
            if not line:
                continue
            key = email_sheet_line_key(line)
        if key not in seen:
            seen.add(key)
            out.append(line)
    return "\n".join(out)


def _thousands_nbsp(n: int) -> str:
    s = str(abs(int(n)))
    parts: list[str] = []
    while s:
        parts.append(s[-3:])
        s = s[:-3]
    return "\xa0".join(reversed(parts))


def format_value_for_sheet(logical_key: str, val: Any) -> str:
    if val is None:
        return "-"
    if logical_key == "O":
        return str(val).strip()
    if logical_key == "P":
        if isinstance(val, int):
            sign = "-" if val < 0 else ""
            return f"{sign}{_thousands_nbsp(val)} ₽"
        if isinstance(val, dict) and "amount_rub" in val and val["amount_rub"] is not None:
            return format_value_for_sheet("P", val["amount_rub"])
        return str(val).strip()
    if logical_key == "Q":
        if isinstance(val, list):
            s = _dedupe_lines_join([str(x) for x in val], phone=True)
            return s if s else "-"
        if isinstance(val, str) and val.strip():
            s = _dedupe_lines_join(val.splitlines(), phone=True)
            return s if s else "-"
        s = str(val).strip()
        return s if s else "-"
    if logical_key == "T":
        if isinstance(val, list):
            s = _dedupe_lines_join([str(x) for x in val], phone=False)
            return s if s else "-"
        if isinstance(val, str) and val.strip():
            s = _dedupe_lines_join(val.splitlines(), phone=False)
            return s if s else "-"
        s = str(val).strip()
        return s if s else "-"
    if logical_key == "R":
        if isinstance(val, dict):
            lines = [f"{k.strip()} {str(v).strip()}".strip() for k, v in val.items()]
            return "\n".join(x for x in lines if x)
        return str(val).strip()
    if logical_key == "S":
        return str(val).strip()
    if logical_key == "U":
        t = str(val).strip()
        low = t.lower()
        if _BANK_OK_PHRASE.lower() in low or low == _BANK_OK_PHRASE.lower():
            return "0"
        return t
    if logical_key in ("V", "W", "X"):
        if isinstance(val, dict) and not val:
            return "-"
        if isinstance(val, dict) and val:
            v0 = next(iter(val.values()))
            if v0 is None:
                return "-"
            return str(v0)
        if isinstance(val, int):
            return str(val)
        s = str(val).strip()
        return s if s else "-"
    if logical_key == "Y":
        if isinstance(val, dict) and not val:
            return "-"
        if isinstance(val, dict) and val:
            if len(val) == 1:
                k, v = next(iter(val.items()))
                return f"{str(v).strip()} {str(k).strip()}".strip()
            return json.dumps(val, ensure_ascii=False, separators=(",", ":"))
        s = str(val).strip()
        return s if s else "-"
    if logical_key == "Z":
        t = str(val).strip()
        if re.fullmatch(r"надежность\s+высокая", t, flags=re.I):
            return "высокая"
        return t
    if logical_key == "AA":
        if isinstance(val, dict):
            count = str(val.get("кол-во", "-")).strip() or "-"
            amount = str(val.get("сумма", "-")).strip() or "-"
            cap = str(val.get("задолжн. относит. устанвой капитал", "-")).strip() or "-"
            profit = str(val.get("относительно прибыли", "-")).strip() or "-"
            return "\n".join(
                [
                    f"кол-во {count}",
                    f"сумма {amount}",
                    f"задолжн. относит. устанвой капитал {cap}",
                    f"относительно прибыли {profit}",
                ],
            )
        s = str(val).strip()
        return s if s else "-"
    if logical_key == "AB":
        if isinstance(val, dict) and val:
            parts = [f"{k.strip()} {str(v).strip()}".strip() for k, v in val.items() if k or v]
            return " ".join(x for x in parts if x)
        return str(val).strip()
    if logical_key == "AC":
        s = str(val).strip()
        return s if s else "-"
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False, separators=(",", ":"))
    return str(val).strip()


def _header_to_col_index(headers: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for i, raw in enumerate(headers):
        key = _normalize_header(raw)
        if key:
            out[key] = i + 1
    return out


def _col_by_key_from_headers(headers: list[str], *, ac_last_column: bool) -> dict[str, int]:
    col_by_key = _header_to_col_index(headers)
    if (os.getenv("PARSER_CLASSIC_UW_COLUMNS") or "").strip().lower() in ("1", "true", "yes", "on"):
        apply_u_w_overrides(col_by_key)
    if ac_last_column and headers:
        col_by_key["AC"] = len(headers)
    return col_by_key


def _apply_import_by_inn(
    ws: gspread.Worksheet,
    sh: gspread.Spreadsheet,
    import_sheet_name: str,
    *,
    ac_last_column: bool = False,
) -> tuple[int, list[str]]:
    """
    Копирует поля парсера (O…AC) с листа-импортёра на ws по ИНН.
    Возвращает (число обновлённых строк, ИНН строк финала без записи в импортёре).
    """
    try:
        ws_imp = sh.worksheet(import_sheet_name)
    except gspread.WorksheetNotFound:
        print(f"Лист-импортёр «{import_sheet_name}» не найден — перенос по ИНН пропущен.")
        return 0, []

    imp_all = _sheet_call(
        lambda: ws_imp.get_all_values(),
        desc=f"чтение «{import_sheet_name}»",
    )
    if not imp_all or len(imp_all) < 2:
        print(f"Лист «{import_sheet_name}» пуст или только заголовок — импорт пропущен.")
        return 0, []

    imp_headers = imp_all[0]
    imp_inn_col = _inn_column_1based(imp_headers)
    if imp_inn_col is None:
        print(f"На листе «{import_sheet_name}» нет столбца «ИНН» — импорт пропущен.")
        return 0, []

    imp_col_by_key = _col_by_key_from_headers(imp_headers, ac_last_column=ac_last_column)
    restore_synaps_bank_column(imp_headers, imp_col_by_key)
    transfer_keys = [k for k in SHEET_JSON_KEYS if k in imp_col_by_key]
    if not transfer_keys:
        print(
            f"На «{import_sheet_name}» нет столбцов парсера (O…AC по заголовкам) — импорт пропущен.",
        )
        return 0, []

    imp_width = max(len(imp_headers), max(imp_col_by_key.values(), default=0))
    imp_by_inn: dict[str, list[str]] = {}
    for row in imp_all[1:]:
        padded = _pad_row_values(row, imp_width)
        inn = _normalize_inn_cell(padded[imp_inn_col - 1] if imp_inn_col <= len(padded) else "")
        if inn:
            imp_by_inn[inn] = padded

    final_headers = _sheet_call(lambda: ws.row_values(1), desc="заголовок финального листа")
    if not final_headers:
        print("Финальный лист без заголовка — импорт пропущен.")
        return 0, []

    final_col_by_key = _col_by_key_from_headers(final_headers, ac_last_column=ac_last_column)
    restore_synaps_bank_column(final_headers, final_col_by_key)
    keys = [k for k in transfer_keys if k in final_col_by_key]
    if not keys:
        print("Нет общих столбцов парсера между финальным листом и листом-импортёром.")
        return 0, []

    final_inn_col = _inn_column_1based(final_headers)
    if final_inn_col is None:
        print("На финальном листе нет столбца «ИНН» — импорт пропущен.")
        return 0, []

    col_a = _sheet_call(lambda: ws.col_values(1), desc="столбец A финала")
    last = len(col_a)
    if last < 2:
        return 0, []

    max_col = max(
        max(final_col_by_key.values()),
        max(imp_col_by_key.values()),
        len(final_headers),
        imp_width,
    )
    rng = f"{rowcol_to_a1(2, 1)}:{rowcol_to_a1(last, max_col)}"
    final_rows = _sheet_call(lambda: ws.get(rng), desc="данные финала") or []

    batch: list[dict] = []
    rows_updated = 0
    inns_without_import: list[str] = []

    for row in range(2, last + 1):
        idx = row - 2
        row_vals = _pad_row_values(final_rows[idx] if 0 <= idx < len(final_rows) else [], max_col)
        inn = _normalize_inn_cell(
            row_vals[final_inn_col - 1] if final_inn_col <= len(row_vals) else "",
        )
        if not inn:
            continue
        imp_row = imp_by_inn.get(inn)
        if imp_row is None:
            inns_without_import.append(inn)
            continue

        copied_keys: list[str] = []
        for key in keys:
            ic = imp_col_by_key[key]
            fc = final_col_by_key[key]
            val = imp_row[ic - 1] if ic <= len(imp_row) else ""
            if not str(val).strip():
                continue
            batch.append({"range": rowcol_to_a1(row, fc), "values": [[val]]})
            copied_keys.append(key)
        if copied_keys:
            rows_updated += 1
            print(
                f"Строка {row}: импорт с «{import_sheet_name}» (ИНН {inn}) — "
                f"{', '.join(copied_keys)}",
            )

    if batch:
        _sheet_call(
            lambda: ws.batch_update(batch, value_input_option="USER_ENTERED"),
            desc="импорт по ИНН",
        )

    return rows_updated, inns_without_import


def _sheet_call(fn: Callable[[], Any], *, desc: str = "") -> Any:
    waits = (5, 20, 65)
    for attempt in range(len(waits) + 1):
        try:
            return fn()
        except APIError as e:
            err_s = str(e)
            if "429" not in err_s and "Quota" not in err_s:
                raise
            if attempt >= len(waits):
                raise
            d = waits[attempt]
            msg = f"лимит Google Sheets API ({desc})" if desc else "лимит Google Sheets API"
            print(f"  {msg}, пауза {d} с… ({err_s[:160]})")
            time.sleep(d)


def _pad_row_values(row_vals: list[Any], width: int) -> list[str]:
    out = [("" if c is None else str(c)) for c in row_vals[:width]]
    while len(out) < width:
        out.append("")
    return out


def _final_matches_unique_by_inn(
    unique_all: list[list[str]],
    final_all: list[list[str]],
    *,
    compare_first_n: int,
) -> tuple[bool, str]:
    """
    True, если число строк данных совпадает и в первых compare_first_n строках ИНН совпадают попарно.
    ИНН нормализуются (только цифры).
    """
    if not unique_all or len(unique_all) < 2:
        return False, "«уникальные» без строк данных"

    u_headers = unique_all[0]
    u_rows = unique_all[1:]
    inn_u = _inn_column_1based(u_headers)
    if inn_u is None:
        return False, "в «уникальные» нет столбца «ИНН»"

    if not final_all or len(final_all) < 2:
        return False, "«финальные данные» пусты или только заголовок"

    f_headers = final_all[0]
    f_rows = final_all[1:]
    inn_f = _inn_column_1based(f_headers)
    if inn_f is None:
        return False, "в «финальные данные» нет столбца «ИНН»"

    if len(f_rows) != len(u_rows):
        return (
            False,
            f"разное число строк данных: финал {len(f_rows)}, уникальные {len(u_rows)}",
        )

    n = min(compare_first_n, len(u_rows))
    for i in range(n):
        ur = u_rows[i]
        fr = f_rows[i]
        iu = _normalize_inn_cell(ur[inn_u - 1] if inn_u <= len(ur) else "")
        iv = _normalize_inn_cell(fr[inn_f - 1] if inn_f <= len(fr) else "")
        if iu != iv:
            return False, f"строка листа {i + 2}: ИНН «{iv or '∅'}» ≠ «{iu or '∅'}»"
    return True, ""


def _ensure_final_worksheet(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    title = sheet_final()
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=2000, cols=32)


def _sheet_cell_is_empty_for_parser(val: Any) -> bool:
    """Только физически пустая ячейка — сюда можно писать из парсера.

    «-», «—», «–» — уже записанный результат (на Synaps нет данных), не пустота:
    строка не пропускается целиком и поле не перезаписывается.
    """
    t = str(val or "").strip()
    return not t


def _row_needs_scrape_from_prefetched(row_vals: list[str], col_by_key: dict[str, int]) -> bool:
    """
    Нужен парсинг, если хотя бы одно сопоставленное поле парсера (O…AC) физически пустое.
    «-»/тире не считаем пустым — это уже ответ с сайта, пустые ячейки ищем отдельно.
    """
    for k in SHEET_JSON_KEYS:
        if k not in col_by_key:
            continue
        i = col_by_key[k] - 1
        v = row_vals[i] if 0 <= i < len(row_vals) else ""
        if _sheet_cell_is_empty_for_parser(v):
            return True
    return False


def _fill_row_only_empty(
    ws: gspread.Worksheet,
    row: int,
    col_by_key: dict[str, int],
    data: dict,
) -> list[str]:
    updated: list[str] = []
    keys_present = [k for k in SHEET_JSON_KEYS if k in col_by_key]
    if not keys_present:
        return updated
    max_col = max(col_by_key[k] for k in keys_present)
    rng = f"{rowcol_to_a1(row, 1)}:{rowcol_to_a1(row, max_col)}"
    raw = _sheet_call(lambda: ws.get(rng), desc=f"строка {row}")
    row_vals = _pad_row_values(raw[0] if raw else [], max_col)
    batch: list[dict] = []
    for key in SHEET_JSON_KEYS:
        if key not in col_by_key or key not in data:
            continue
        col = col_by_key[key]
        cur = row_vals[col - 1] if col <= len(row_vals) else ""
        if not _sheet_cell_is_empty_for_parser(cur):
            continue
        val = format_value_for_sheet(key, data[key])
        a1 = rowcol_to_a1(row, col)
        batch.append({"range": a1, "values": [[val]]})
        updated.append(key)
    if batch:
        _sheet_call(
            lambda: ws.batch_update(batch, value_input_option="USER_ENTERED"),
            desc=f"запись строки {row}",
        )
    return updated


def _ensure_worksheet_by_title(sh: gspread.Spreadsheet, title: str, *, min_cols: int) -> gspread.Worksheet:
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=2000, cols=max(min_cols, 26))


def _synaps_enrich_worksheet(
    ws: gspread.Worksheet,
    *,
    headless: bool,
    save_dom_snapshots: bool,
    ac_last_column: bool = False,
) -> None:
    """
    Заполнение полей парсера (O…AC) на уже подготовленном листе.
    Строка: при наличии ссылки Synaps — обход по URL, иначе при наличии ИНН — поиск по ИНН (parser_synaps_browser).
    """
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("В первой строке таблицы должны быть заголовки столбцов.")

    col_by_key = _col_by_key_from_headers(headers, ac_last_column=ac_last_column)
    if not restore_synaps_bank_column(headers, col_by_key):
        print(
            "Внимание: столбец банка (логический U, «счёт») не сопоставлен по заголовку. "
            "Укажите в .env SYNAPS_BANK_COL=номер_столбца (1-based) или добавьте столбец «счёт» / «состояние банковского счёта».",
        )
    if not col_by_key:
        sample = ", ".join(sorted(HEADER_TO_KEY.keys())[:8])
        raise RuntimeError(
            "Не удалось сопоставить заголовки с полями парсера. "
            f"Примеры поддерживаемых названий: {sample}… или буквы {', '.join(SHEET_JSON_KEYS)}.",
        )

    url_col = _synaps_url_column_1based(headers)
    inn_col = _inn_column_1based(headers)
    max_col = max(
        max(col_by_key.values()),
        url_col,
        FALLBACK_URL_COL,
        len(headers),
        inn_col or 0,
    )
    col_a = _sheet_call(lambda: ws.col_values(1), desc="столбец A")
    last = len(col_a)
    if last < 2:
        print("Нет строк для обработки.")
        return

    rng = f"{rowcol_to_a1(2, 1)}:{rowcol_to_a1(last, max_col)}"
    sheet_rows = _sheet_call(lambda: ws.get(rng), desc="данные строк 2…") or []
    sheet_rows_formula = (
        _sheet_call(lambda: ws.get(rng, value_render_option="FORMULA"), desc="формулы строк 2…") or []
    )

    tasks_by_url: list[tuple[int, str]] = []
    tasks_by_inn: list[tuple[int, str]] = []
    sheet_urls_order: list[str] = []
    sheet_inns_order: list[str] = []
    skipped_no_inn_no_url = 0

    for row in range(2, last + 1):
        idx = row - 2
        row_vals = _pad_row_values(sheet_rows[idx] if 0 <= idx < len(sheet_rows) else [], max_col)
        row_formula_vals = _pad_row_values(
            sheet_rows_formula[idx] if 0 <= idx < len(sheet_rows_formula) else [],
            max_col,
        )

        url = _extract_synaps_url(
            row_vals[url_col - 1] if url_col <= len(row_vals) else "",
            row_formula_vals[url_col - 1] if url_col <= len(row_formula_vals) else "",
            row_vals[FALLBACK_URL_COL - 1] if FALLBACK_URL_COL <= len(row_vals) else "",
        )
        inn = ""
        if inn_col is not None and inn_col <= len(row_vals):
            inn = _normalize_inn_cell(row_vals[inn_col - 1])

        if url:
            sheet_urls_order.append(url)
        if inn:
            sheet_inns_order.append(inn)

        lookup: str | None = None
        mode: str | None = None
        if url:
            lookup, mode = url, "url"
        elif inn:
            lookup, mode = inn, "inn"

        if not lookup or not mode:
            skipped_no_inn_no_url += 1
            continue

        if not _row_needs_scrape_from_prefetched(row_vals, col_by_key):
            hint = lookup[:70] if mode == "url" else f"ИНН {lookup}"
            print(f"Строка {row}: пропуск (уже с данными) — {hint}")
            continue

        if mode == "url":
            tasks_by_url.append((row, lookup))
        else:
            tasks_by_inn.append((row, lookup))

    if skipped_no_inn_no_url:
        print(
            f"Строк без ИНН и без ссылки Synaps (не парсятся): {skipped_no_inn_no_url} "
            f"(номера строк в логе не выводятся).",
        )

    if not tasks_by_url and not tasks_by_inn and not save_dom_snapshots:
        print("Нет строк для парсинга (нет ИНН/ссылки Synaps или все поля парсера уже заполнены).")
        return

    if save_dom_snapshots and not sheet_urls_order and not sheet_inns_order:
        print("Нет строк для дампа DOM (в листе нет ИНН и ссылок Synaps).")
        return

    def _flush_url_row(url_key: str, data: dict[str, Any]) -> None:
        for row, u in tasks_by_url:
            if u == url_key:
                done = _fill_row_only_empty(ws, row, col_by_key, data)
                print(f"Строка {row}: записано полей: {', '.join(done) or 'ничего'}")

    def _flush_inn_row(inn_key: str, data: dict[str, Any]) -> None:
        for row, i in tasks_by_inn:
            if i == inn_key:
                done = _fill_row_only_empty(ws, row, col_by_key, data)
                print(f"Строка {row}: записано полей: {', '.join(done) or 'ничего'}")

    urls_unique = list(dict.fromkeys(u for _, u in tasks_by_url))
    inns_unique = list(dict.fromkeys(i for _, i in tasks_by_inn))

    if save_dom_snapshots:
        urls_to_fetch = list(dict.fromkeys(sheet_urls_order))
        inns_to_fetch = list(dict.fromkeys(sheet_inns_order))
    else:
        urls_to_fetch = urls_unique
        inns_to_fetch = inns_unique

    print(
        f"Строк по URL: {len(tasks_by_url)} (уникальных URL: {len(urls_to_fetch)}); "
        f"строк по ИНН: {len(tasks_by_inn)} (уникальных ИНН: {len(inns_to_fetch)})",
    )
    if tasks_by_inn or tasks_by_url:
        print(
            "Подсказка: в логе Synaps «[k/total] ИНН …» — это k-й уникальный URL/ИНН в очереди браузера; "
            "номер строки листа даётся отдельно в строке «Строка N: записано полей …».",
        )

    if not urls_to_fetch and not inns_to_fetch:
        print(
            "Нечего обходить: добавьте столбец «ИНН» или ссылку Synaps, либо очистите поля для дозаполнения.",
        )
        return

    results: dict[str, Any | BaseException] = {}

    if urls_to_fetch:
        results_url = scrape_urls_sequentially(
            urls_to_fetch,
            headless=headless,
            save_dom_snapshots=save_dom_snapshots,
            on_each_result=_flush_url_row if tasks_by_url else None,
        )
        results.update(results_url)

    if inns_to_fetch:
        if inn_col is None:
            raise RuntimeError("В таблице нет столбца «ИНН» — добавьте заголовок «ИНН» для поиска на Synaps.")
        results_inn = scrape_inns_sequentially(
            inns_to_fetch,
            headless=headless,
            save_dom_snapshots=save_dom_snapshots,
            on_each_result=_flush_inn_row if tasks_by_inn else None,
        )
        results.update(results_inn)

    if save_dom_snapshots:
        n = (len(urls_to_fetch) + len(inns_to_fetch)) * 3
        print(f"HTML-снимки DOM (до {n} файлов): {resolved_dom_dumps_dir()}")

    for row, u in tasks_by_url:
        res = results.get(u)
        if isinstance(res, BaseException):
            print(f"Строка {row}: ошибка парсинга (URL) — {res!s}")
    for row, inn in tasks_by_inn:
        res = results.get(inn)
        if isinstance(res, BaseException):
            print(f"Строка {row}: ошибка парсинга (ИНН {inn}) — {res!s}")


def run_copy_source_to_final_then_parse(
    source_sheet: str,
    final_sheet: str,
    *,
    headless: bool = True,
    save_dom_snapshots: bool = False,
    skip_copy: bool = False,
    ac_last_column: bool = False,
) -> None:
    """Полная копия листа-источника на лист-приёмник, затем тот же парсинг, что и для «финальные данные»."""
    load_dotenv(ROOT / ".env")
    cred_path = _credentials_path()
    sheet_id = _sheet_id()

    gc = gspread.service_account(filename=str(cred_path))
    sh = gc.open_by_key(sheet_id)
    try:
        ws_src = sh.worksheet(source_sheet)
    except gspread.WorksheetNotFound as e:
        raise RuntimeError(f"Нет листа «{source_sheet}».") from e

    source_all = _sheet_call(lambda: ws_src.get_all_values(), desc=f"чтение «{source_sheet}»")
    if not source_all:
        raise RuntimeError(f"Лист «{source_sheet}» пуст.")

    width = max((len(r) for r in source_all), default=1)
    ws_dst = _ensure_worksheet_by_title(sh, final_sheet, min_cols=width)

    if not skip_copy:
        _sheet_call(lambda: ws_dst.clear(), desc=f"очистка «{final_sheet}»")
        _sheet_call(
            lambda: ws_dst.update(
                range_name="A1",
                values=source_all,
                value_input_option="RAW",
            ),
            desc=f"копия «{source_sheet}» → «{final_sheet}»",
        )
        print(f"Скопировано строк: {len(source_all)} (включая заголовок).")
    else:
        print(f"Пропуск копирования: парсинг по текущему содержимому листа «{final_sheet}».")

    _synaps_enrich_worksheet(
        ws_dst,
        headless=headless,
        save_dom_snapshots=save_dom_snapshots,
        ac_last_column=ac_last_column,
    )


def run_sheet_sync_new(
    *,
    headless: bool = True,
    save_dom_snapshots: bool = False,
    force_copy_from_unique: bool = False,
    ac_last_column: bool = False,
) -> None:
    load_dotenv(ROOT / ".env")
    cred_path = _credentials_path()
    sheet_id = _sheet_id()

    gc = gspread.service_account(filename=str(cred_path))
    sh = gc.open_by_key(sheet_id)
    unique_name = sheet_unique()
    final_name = sheet_final()
    try:
        ws_unique = sh.worksheet(unique_name)
    except gspread.WorksheetNotFound:
        ws_unique = sh.add_worksheet(title=unique_name, rows=2000, cols=40)
    ws_final = _ensure_final_worksheet(sh)

    unique_all = _sheet_call(lambda: ws_unique.get_all_values(), desc=f"чтение «{unique_name}»")

    need_bootstrap = False
    if not unique_all:
        need_bootstrap = True
    elif len(unique_all) == 1:
        r0 = unique_all[0]
        if not first_header_row_nonempty(r0) or not has_inn_column_in_headers(r0):
            need_bootstrap = True
    elif not has_inn_column_in_headers(unique_all[0]):
        raise RuntimeError(
            f"На листе «{unique_name}» в строке 1 нет столбца «ИНН». Исправьте заголовок или удалите все строки — "
            f"тогда при следующем запуске будет записан шаблон.",
        )

    if need_bootstrap:
        mh = build_bootstrap_merged_headers()
        _sheet_call(lambda: ws_unique.clear(), desc=f"очистка «{unique_name}» (шаблон)")
        _sheet_call(
            lambda: ws_unique.update(range_name="A1", values=[mh], value_input_option="RAW"),
            desc=f"запись шаблона заголовков «{unique_name}»",
        )
        unique_all = [mh]
        print(
            f"Лист «{unique_name}» был пуст или без заголовка ИНН — записана строка заголовков ({len(mh)} столбцов: "
            f"исходные + Synaps). Добавьте строки с данными и запустите снова; парсинг строк пока не выполняется.",
        )

    final_all = _sheet_call(lambda: ws_final.get_all_values(), desc=f"чтение «{final_name}»")

    if force_copy_from_unique:
        need_copy = True
        copy_reason = "флаг --force-copy"
    else:
        match, mismatch_reason = _final_matches_unique_by_inn(
            unique_all,
            final_all,
            compare_first_n=INN_PREFIX_COMPARE_ROWS,
        )
        need_copy = not match
        copy_reason = mismatch_reason

    if need_copy:
        _sheet_call(lambda: ws_final.clear(), desc=f"очистка «{final_name}»")
        _sheet_call(
            lambda: ws_final.update(
                range_name="A1",
                values=unique_all,
                value_input_option="RAW",
            ),
            desc=f"копия «{unique_name}» → «{final_name}»",
        )
        print(
            f"Перезапись «{final_name}» из «{unique_name}» ({len(unique_all)} строк). Причина: {copy_reason}.",
        )
    else:
        checked = min(INN_PREFIX_COMPARE_ROWS, max(0, len(unique_all) - 1))
        print(
            f"«{final_name}» совпадает с «{unique_name}» по числу строк и по ИНН в первых {checked} "
            f"строках данных — копирование пропущено, только парсинг.",
        )

    imp_name = sheet_import()
    if imp_name:
        if imp_name == final_name:
            raise RuntimeError(
                f"SHEET_IMPORT и SHEET_FINAL не должны указывать на один лист («{imp_name}»).",
            )
        rows_imp, inns_parse = _apply_import_by_inn(
            ws_final,
            sh,
            imp_name,
            ac_last_column=ac_last_column,
        )
        print(f"Импорт с «{imp_name}»: обновлено строк {rows_imp}.")
        if inns_parse:
            preview = ", ".join(inns_parse[:12])
            more = f" и ещё {len(inns_parse) - 12}" if len(inns_parse) > 12 else ""
            print(
                f"ИНН без записи в «{imp_name}» ({len(inns_parse)}): {preview}{more} — будут обойдены парсером.",
            )

    ws = ws_final
    _synaps_enrich_worksheet(
        ws,
        headless=headless,
        save_dom_snapshots=save_dom_snapshots,
        ac_last_column=ac_last_column,
    )


if __name__ == "__main__":
    from parser_sheet_env import (
        sheet_copy_from,
        sheet_copy_to,
        sheet_20m_final,
        sheet_20m_source,
        stage2_options,
    )

    opt = stage2_options()
    headless = not opt["headed"]
    src = sheet_copy_from().strip()
    dst = sheet_copy_to().strip()
    if opt["run_20m"]:
        src = sheet_20m_source()
        dst = sheet_20m_final()
    if bool(src) ^ bool(dst):
        raise RuntimeError(
            "Задайте в .env оба имени SHEET_COPY_FROM и SHEET_COPY_TO "
            "или включите PARSER_RUN_20M=1 для листов SHEET_20M_SOURCE / SHEET_20M_FINAL.",
        )
    if opt["skip_copy"] and not (src and dst):
        raise RuntimeError("PARSER_SKIP_COPY=1 имеет смысл только вместе с копией между листами (SHEET_COPY_* или PARSER_RUN_20M).")

    if src and dst:
        run_copy_source_to_final_then_parse(
            src,
            dst,
            headless=headless,
            save_dom_snapshots=opt["dump_dom"],
            skip_copy=opt["skip_copy"],
            ac_last_column=opt["ac_last"],
        )
    else:
        if opt["force_copy"] and opt["skip_copy"]:
            raise RuntimeError("Нельзя одновременно PARSER_FORCE_COPY и PARSER_SKIP_COPY в основном режиме.")
        run_sheet_sync_new(
            headless=headless,
            save_dom_snapshots=opt["dump_dom"],
            force_copy_from_unique=opt["force_copy"],
            ac_last_column=opt["ac_last"],
        )
