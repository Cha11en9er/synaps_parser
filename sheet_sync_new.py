"""
Шаг 2 пайплайна: копия «уникальные» → «финальные данные», затем дополнение с Synaps только на «финальные данные».
Лист «уникальные» не изменяется.

Перед парсингом: если число строк в «финальных» = «уникальных» и в первых 100 строках ИНН совпадают — копирование
не делаем (только дозаполнение с Synaps). Иначе «финальные данные» полностью перезаписываются из «уникальные».
Принудительная перезапись: `--force-copy`.

Если в строке есть ссылка на Synaps — открывается напрямую (колонка по заголовку, иначе B, запасной A, HYPERLINK).
Если ссылки нет — столбец «ИНН» и поиск на сайте (.ocs-input-block).
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

from sheet_column_layout import apply_u_w_overrides, restore_synaps_bank_column
from synaps_browser import (
    SHEET_JSON_KEYS,
    clean_email_as_on_page,
    email_sheet_line_key,
    resolved_dom_dumps_dir,
    scrape_inns_sequentially,
    scrape_urls_sequentially,
)

ROOT = Path(__file__).resolve().parent
UNIQUE_SHEET = "уникальные"
FINAL_SHEET = "финальные данные"
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
    try:
        return sh.worksheet(FINAL_SHEET)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=FINAL_SHEET, rows=2000, cols=32)


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


def run_sheet_sync_new(
    *,
    headless: bool = True,
    save_dom_snapshots: bool = False,
    force_copy_from_unique: bool = False,
) -> None:
    load_dotenv(ROOT / ".env")
    cred_path = _credentials_path()
    sheet_id = _sheet_id()

    gc = gspread.service_account(filename=str(cred_path))
    sh = gc.open_by_key(sheet_id)
    ws_unique = sh.worksheet(UNIQUE_SHEET)
    ws_final = _ensure_final_worksheet(sh)

    unique_all = _sheet_call(lambda: ws_unique.get_all_values(), desc="чтение «уникальные»")
    if not unique_all:
        raise RuntimeError(f"Лист «{UNIQUE_SHEET}» пуст — сначала заполните или выполните sheet_inn_clean.py.")

    final_all = _sheet_call(lambda: ws_final.get_all_values(), desc="чтение «финальные данные»")

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
        _sheet_call(lambda: ws_final.clear(), desc="очистка «финальные данные»")
        _sheet_call(
            lambda: ws_final.update(
                range_name="A1",
                values=unique_all,
                value_input_option="RAW",
            ),
            desc="копия уникальные → финальные данные",
        )
        print(
            f"Перезапись «{FINAL_SHEET}» из «{UNIQUE_SHEET}» ({len(unique_all)} строк). Причина: {copy_reason}.",
        )
    else:
        checked = min(INN_PREFIX_COMPARE_ROWS, max(0, len(unique_all) - 1))
        print(
            f"«{FINAL_SHEET}» совпадает с «{UNIQUE_SHEET}» по числу строк и по ИНН в первых {checked} "
            f"строках данных — копирование пропущено, только парсинг.",
        )

    ws = ws_final

    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("В первой строке таблицы должны быть заголовки столбцов.")

    col_by_key = _header_to_col_index(headers)
    apply_u_w_overrides(col_by_key)
    if not restore_synaps_bank_column(headers, col_by_key):
        print(
            "Внимание: столбец банка (логический U, «счёт») не сопоставлен после фикса U=долг, W=численность Synaps. "
            "Укажите в .env SYNAPS_BANK_COL=номер_столбца (1-based, не 21 и не 23), либо добавьте отдельный столбец "
            "«счёт» вне колонок U и W.",
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
    # Формулы нужны для поддержки скрытых ссылок вида =HYPERLINK("url"; "текст")
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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Synaps → лист «финальные данные» (копия с «уникальные» при необходимости); «уникальные» не меняются",
    )
    parser.add_argument("--headed", action="store_true", help="Показать браузер (по умолчанию headless)")
    parser.add_argument(
        "--dump-dom",
        action="store_true",
        help="Сохранить HTML трёх страниц на каждую организацию в dom_dumps/",
    )
    parser.add_argument(
        "--force-copy",
        action="store_true",
        help="Всегда очистить «финальные данные» и заново скопировать из «уникальные» перед парсингом",
    )
    args = parser.parse_args()
    run_sheet_sync_new(
        headless=not args.headed,
        save_dom_snapshots=args.dump_dom,
        force_copy_from_unique=args.force_copy,
    )
