"""
Устаревший вариант синхронизации (до листов «Лист1» / «Лист1 дубликаты» и маркера «работа парсера»).

По умолчанию лист «компании», дубликаты — «дубли компании». Обрабатывается весь заполненный столбец A (без строки-маркера).

Читает URL карточек Synaps из столбца A (прямой URL или гиперссылка на названии), парсит Synaps и записывает поля.
Пустые ячейки и «-» как плейсхолдер заполняются; непустые не трогаем.

Запуск: python sheet_sync_1.py (те же флаги, что у sheet_sync.py).
"""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Callable

import gspread
import requests
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1

from synaps_browser import (
    SHEET_JSON_KEYS,
    clean_email_as_on_page,
    email_sheet_line_key,
    resolved_dom_dumps_dir,
    scrape_urls_sequentially,
)

ROOT = Path(__file__).resolve().parent

_SCOPES_SHEETS = ("https://www.googleapis.com/auth/spreadsheets",)

_BANK_OK_PHRASE = "действующие решения о приостановлении отсутствуют"

# Нормализованный заголовок (нижний регистр, ё→е, без лишних пробелов и финального :) → логический ключ
HEADER_TO_KEY: dict[str, str] = {
    "дата регистрации ооо": "O",
    "дата регистрации": "O",
    "уставной": "P",
    "доп телефон": "Q",
    "основной оквед": "R",
    "основной оквэд": "R",
    "основной okved": "R",
    "основной вид деятельности": "R",
    "оквед": "R",
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
    "генеральный директор": "AB",
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


def _worksheet_name() -> str | None:
    t = _env("SHEET_TAB", "").strip()
    return t or "компании"


def _duplicates_sheet_name() -> str:
    t = _env("SHEET_DUPLICATES_TAB", "").strip()
    return t or "дубли компании"


def _is_synaps_org_url(s: str) -> bool:
    """Допускаем http(s), опционально www — иначе ссылки из таблицы не попадают в обход DOM."""
    t = (s or "").strip().lower()
    for prefix in ("https://", "http://"):
        if t.startswith(prefix):
            t = t[len(prefix) :]
    if t.startswith("www."):
        t = t[4:]
    t = t.replace(" ", "")
    return "synapsenet.ru/organizacii/" in t or "synapsenet.ru/searchorganization/organization/" in t


def _url_from_cell_data(cell: dict | None) -> str | None:
    """URL из ячейки Sheets API (гиперссылка, формула HYPERLINK или текст = URL Synaps)."""
    if not cell:
        return None
    h = cell.get("hyperlink")
    if isinstance(h, str) and h.strip():
        return h.strip()
    uv = cell.get("userEnteredValue") or {}
    fv = uv.get("formulaValue")
    if fv:
        m = re.search(r'HYPERLINK\s*\(\s*"(https?://[^"]+)"', fv, re.I)
        if m:
            return m.group(1).strip()
        m = re.search(r"HYPERLINK\s*\(\s*'(https?://[^']+)'", fv, re.I)
        if m:
            return m.group(1).strip()
    text = str(
        cell.get("formattedValue")
        or uv.get("stringValue")
        or "",
    ).strip()
    if text and _is_synaps_org_url(text):
        return text
    return None


def _parse_grid_hyperlinks_column_a(payload: dict, sheet_title: str) -> dict[int, str]:
    """Строка 1-based → URL из первой колонки диапазона (по ответу spreadsheets.get + gridData)."""
    out: dict[int, str] = {}
    for sh in payload.get("sheets", []):
        if sh.get("properties", {}).get("title") != sheet_title:
            continue
        for grid in sh.get("data", []):
            start = int(grid.get("startRowIndex", 0))
            row_data = grid.get("rowData") or []
            for i, row in enumerate(row_data):
                if row is None:
                    continue
                row_1based = start + i + 1
                cells = row.get("values") or []
                cell = cells[0] if cells else None
                u = _url_from_cell_data(cell)
                if u:
                    out[row_1based] = u
    return out


def _fetch_column_a_hyperlinks_from_api(
    spreadsheet_id: str,
    sheet_title: str,
    cred_path: Path,
    row_start_1based: int,
    row_end_1based: int,
) -> dict[int, str]:
    """Читает реальные URL из столбца A (в т.ч. вшитые в гиперссылку)."""
    if row_end_1based < row_start_1based:
        return {}
    safe = sheet_title.replace("'", "''")
    a1 = f"'{safe}'!A{row_start_1based}:A{row_end_1based}"
    url_api = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"
    waits = (5, 20, 65)
    last_err = ""
    creds = Credentials.from_service_account_file(str(cred_path), scopes=_SCOPES_SHEETS)
    for attempt in range(len(waits) + 1):
        try:
            creds.refresh(Request())
            r = requests.get(
                url_api,
                headers={"Authorization": f"Bearer {creds.token}"},
                params=[("ranges", a1), ("includeGridData", "true")],
                timeout=120,
            )
            if r.status_code == 429 and attempt < len(waits):
                time.sleep(waits[attempt])
                continue
            r.raise_for_status()
            return _parse_grid_hyperlinks_column_a(r.json(), sheet_title)
        except Exception as e:
            last_err = str(e)
            if attempt < len(waits) and ("429" in last_err or "Quota" in last_err):
                time.sleep(waits[attempt])
                continue
            break
    print(f"  Предупреждение: не удалось загрузить гиперссылки столбца A ({last_err}); используется только видимый текст.")
    return {}


def _synaps_url_for_row(urls_by_row: dict[int, str], row_num: int, row_vals: list[str]) -> str:
    """Сначала URL из API (гиперссылка), иначе текст ячейки A (прямая ссылка в ячейке)."""
    u = (urls_by_row.get(row_num) or "").strip()
    if u:
        return u
    return (row_vals[0] or "").strip()


def _canon_header_label(h: str) -> str:
    t = (h or "").strip().lower()
    t = t.replace("ё", "е")
    t = re.sub(r"\s+", " ", t)
    return t.rstrip(":").strip()


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
    if c in HEADER_TO_KEY:
        return HEADER_TO_KEY[c]
    # Частые опечатки / варианты для столбца R (основной ОКВЭД)
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
    """Человекочитаемый текст для ячейки таблицы (логический ключ O…AB)."""
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
        s = str(val).strip()
        return s if s else "-"

    if logical_key == "AC":
        s = str(val).strip()
        return s if s else "-"

    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False, separators=(",", ":"))
    return str(val).strip()


def _header_to_col_index(headers: list[str]) -> dict[str, int]:
    """Логический ключ → номер столбца 1-based."""
    out: dict[str, int] = {}
    for i, raw in enumerate(headers):
        key = _normalize_header(raw)
        if key:
            out[key] = i + 1
    return out


def _sheet_call(fn: Callable[[], Any], *, desc: str = "") -> Any:
    """Повтор при 429 (лимит чтений/записей в минуту)."""
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


def _sheet_cell_is_empty_for_parser(val: Any) -> bool:
    """Пусто или плейсхолдер «нет данных» из format_value_for_sheet (— тоже считаем пустым)."""
    t = str(val or "").strip()
    if not t:
        return True
    if t in ("-", "—", "–"):
        return True
    return False


def _row_needs_scrape_from_prefetched(row_vals: list[str], col_by_key: dict[str, int]) -> bool:
    """
    Нужен парсинг, если хотя бы одно сопоставленное поле парсера (O…AC) пусто или «-».
    Раньше при наличии столбца O проверяли только его — тогда даты тендера в «Дата регистрации»
    ошибочно блокировали парсинг, хотя данные Synaps в других столбцах ещё не заполнены.
    """
    for k in SHEET_JSON_KEYS:
        if k not in col_by_key:
            continue
        i = col_by_key[k] - 1
        v = row_vals[i] if 0 <= i < len(row_vals) else ""
        if _sheet_cell_is_empty_for_parser(v):
            return True
    return False


def _canonical_org_key(url: str) -> str | None:
    t = (url or "").strip().lower()
    if not t:
        return None
    t = t.split("?", 1)[0].rstrip("/")
    m = re.search(r"/organizacii/([^/?#]+)$", t)
    if m:
        return m.group(1)
    m = re.search(r"/searchorganization/organization/([^/?#]+)$", t)
    if m:
        return m.group(1)
    return None


def _row_has_all_parsed_data(row_vals: list[str], col_by_key: dict[str, int]) -> bool:
    for key in SHEET_JSON_KEYS:
        if key not in col_by_key:
            continue
        i = col_by_key[key] - 1
        v = row_vals[i] if 0 <= i < len(row_vals) else ""
        if _sheet_cell_is_empty_for_parser(v):
            return False
    return True


def _ensure_duplicates_sheet(
    sh: gspread.Spreadsheet,
    headers: list[str],
    cols_count: int,
) -> gspread.Worksheet:
    name = _duplicates_sheet_name()
    try:
        ws = sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = _sheet_call(lambda: sh.add_worksheet(title=name, rows=1000, cols=max(cols_count, 26)), desc=name)
    cur_headers = _sheet_call(lambda: ws.row_values(1), desc=f"{name} заголовки")
    if not cur_headers:
        _sheet_call(
            lambda: ws.update(
                range_name="A1",
                values=[headers],
                value_input_option="USER_ENTERED",
            ),
            desc=f"{name} запись заголовков",
        )
    return ws


def _move_duplicate_rows(
    sh: gspread.Spreadsheet,
    ws: gspread.Worksheet,
    headers: list[str],
    sheet_rows: list[list[Any]],
    *,
    first_data_row: int,
    max_col: int,
    col_by_key: dict[str, int],
    urls_by_row: dict[int, str] | None = None,
) -> tuple[int, int]:
    """
    Переносит дубли на лист «дубли компании» и удаляет их из текущего листа со сдвигом вверх.
    Логика: на основном листе должна остаться ровно одна строка на компанию (по URL Synaps в столбце A).
    - если есть «полные» строки (все поля парсера заполнены) — оставляем первую полную;
    - иначе оставляем самую верхнюю строку (первую в группе, обычно пустую/новую).
    Все остальные строки группы переносим на лист дубликатов.
    """
    umap = urls_by_row or {}
    groups: dict[str, list[tuple[int, list[str], bool]]] = {}
    for idx, raw in enumerate(sheet_rows):
        row_num = first_data_row + idx
        row_vals = _pad_row_values(raw, max_col)
        url = _synaps_url_for_row(umap, row_num, row_vals).strip()
        if not _is_synaps_org_url(url):
            continue
        key = _canonical_org_key(url)
        if not key:
            continue
        full = _row_has_all_parsed_data(row_vals, col_by_key)
        groups.setdefault(key, []).append((row_num, row_vals, full))

    to_move_rows: list[tuple[int, list[str]]] = []
    for _, entries in groups.items():
        if len(entries) < 2:
            continue
        full_entries = [e for e in entries if e[2]]
        keep_row = full_entries[0][0] if full_entries else entries[0][0]
        for row_num, row_vals, _ in entries:
            if row_num != keep_row:
                to_move_rows.append((row_num, row_vals))

    if not to_move_rows:
        return (0, 0)

    to_move_rows.sort(key=lambda x: x[0])
    dup_ws = _ensure_duplicates_sheet(sh, headers, max_col)
    values = [r[:max_col] for _, r in to_move_rows]
    _sheet_call(
        lambda: dup_ws.insert_rows(values, row=2, value_input_option="USER_ENTERED"),
        desc="перенос дублей",
    )

    for row_num, _ in sorted(to_move_rows, key=lambda x: x[0], reverse=True):
        _sheet_call(lambda rn=row_num: ws.delete_rows(rn), desc=f"удаление дубля {row_num}")

    return (len(to_move_rows), len(groups))


def _dedupe_current_companies_sheet(
    sh: gspread.Spreadsheet,
    ws: gspread.Worksheet,
    headers: list[str],
    col_by_key: dict[str, int],
    *,
    cred_path: Path,
) -> int:
    """
    Прочитать актуальные строки листа и перенести дубли на лист «дубли компании».
    Возвращает количество перенесённых строк.
    """
    max_col = max(max(col_by_key.values()), len(headers), 29)
    col_a = _sheet_call(lambda: ws.col_values(1), desc="столбец A (дедуп)")
    last = len(col_a)
    urls_by_row = _sheet_call(
        lambda: _fetch_column_a_hyperlinks_from_api(sh.id, ws.title, cred_path, 1, last),
        desc="гиперссылки A (дедуп)",
    )
    sheet_rows: list[list[Any]] = []
    if last >= 2:
        rng = f"{rowcol_to_a1(2, 1)}:{rowcol_to_a1(last, max_col)}"
        sheet_rows = _sheet_call(lambda: ws.get(rng), desc="данные строк 2… (дедуп)") or []
    moved, _ = _move_duplicate_rows(
        sh,
        ws,
        headers,
        sheet_rows,
        first_data_row=2,
        max_col=max_col,
        col_by_key=col_by_key,
        urls_by_row=urls_by_row,
    )
    return moved


def _fill_row_only_empty(
    ws: gspread.Worksheet,
    row: int,
    col_by_key: dict[str, int],
    data: dict,
) -> list[str]:
    """Одно чтение строки (get), затем batch_update только по пустым ячейкам."""
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
        if key not in col_by_key:
            continue
        col = col_by_key[key]
        cur = row_vals[col - 1] if col <= len(row_vals) else ""
        if not _sheet_cell_is_empty_for_parser(cur):
            continue
        if key not in data:
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


def run_sheet_sync(*, headless: bool = True, save_dom_snapshots: bool = False) -> None:
    load_dotenv(ROOT / ".env")
    cred_path = _credentials_path()
    sheet_id = _sheet_id()
    tab = _worksheet_name()

    gc = gspread.service_account(filename=str(cred_path))
    sh = gc.open_by_key(sheet_id)
    if tab:
        try:
            ws = sh.worksheet(tab)
        except gspread.WorksheetNotFound as e:
            raise RuntimeError(
                f"Лист «{tab}» не найден. Задайте SHEET_TAB в .env или удалите переменную для первого листа.",
            ) from e
    else:
        ws = sh.sheet1

    headers = _sheet_call(lambda: ws.row_values(1), desc="заголовки")
    if not headers:
        raise RuntimeError("В первой строке таблицы должны быть заголовки столбцов.")

    col_by_key = _header_to_col_index(headers)
    if not col_by_key:
        sample = ", ".join(sorted(HEADER_TO_KEY.keys())[:8])
        raise RuntimeError(
            "Не удалось сопоставить заголовки с полями парсера. "
            f"Примеры поддерживаемых названий: {sample}… или буквы {', '.join(SHEET_JSON_KEYS)}.",
        )

    col_a = _sheet_call(lambda: ws.col_values(1), desc="столбец A")
    last = len(col_a)
    max_col = max(max(col_by_key.values()), len(headers), 29)
    sheet_rows: list[list[Any]] = []
    if last >= 2:
        rng = f"{rowcol_to_a1(2, 1)}:{rowcol_to_a1(last, max_col)}"
        sheet_rows = _sheet_call(lambda: ws.get(rng), desc="данные строк 2…") or []

    urls_by_row = _sheet_call(
        lambda: _fetch_column_a_hyperlinks_from_api(sh.id, ws.title, cred_path, 1, last),
        desc="гиперссылки столбца A",
    )

    moved, _ = _move_duplicate_rows(
        sh,
        ws,
        headers,
        sheet_rows,
        first_data_row=2,
        max_col=max_col,
        col_by_key=col_by_key,
        urls_by_row=urls_by_row,
    )
    if moved:
        dup_tab = _duplicates_sheet_name()
        print(f"Дубликаты: перенесено строк на лист «{dup_tab}»: {moved}")
        col_a = _sheet_call(lambda: ws.col_values(1), desc="столбец A после очистки дублей")
        last = len(col_a)
        sheet_rows = []
        if last >= 2:
            rng = f"{rowcol_to_a1(2, 1)}:{rowcol_to_a1(last, max_col)}"
            sheet_rows = _sheet_call(lambda: ws.get(rng), desc="данные строк 2… после очистки дублей") or []
        urls_by_row = _sheet_call(
            lambda: _fetch_column_a_hyperlinks_from_api(sh.id, ws.title, cred_path, 1, last),
            desc="гиперссылки столбца A после очистки дублей",
        )

    all_synaps_in_order: list[str] = []
    for row in range(2, last + 1):
        idx = row - 2
        row_vals = _pad_row_values(sheet_rows[idx] if 0 <= idx < len(sheet_rows) else [], max_col)
        url = _synaps_url_for_row(urls_by_row, row, row_vals).strip()
        if _is_synaps_org_url(url):
            all_synaps_in_order.append(url)
    urls_all_synaps_unique = list(dict.fromkeys(all_synaps_in_order))

    tasks: list[tuple[int, str]] = []
    for row in range(2, last + 1):
        idx = row - 2
        row_vals = _pad_row_values(sheet_rows[idx] if 0 <= idx < len(sheet_rows) else [], max_col)
        url = _synaps_url_for_row(urls_by_row, row, row_vals).strip()
        if not _is_synaps_org_url(url):
            continue
        if not _row_needs_scrape_from_prefetched(row_vals, col_by_key):
            print(f"Строка {row}: пропуск (строка уже с данными, парсинг не нужен) — {url[:70]}")
            continue
        tasks.append((row, url))

    if save_dom_snapshots:
        urls_to_fetch = urls_all_synaps_unique
    else:
        urls_to_fetch = list(dict.fromkeys(u for _, u in tasks))

    if not tasks and not save_dom_snapshots:
        print("Нет строк для парсинга (нет ссылок Synaps в гиперссылке/тексте A или все уже с данными).")
        return

    if not urls_to_fetch:
        print("В столбце A нет ссылок на карточки Synaps (в гиперссылке или тексте ячейки) — нечего открывать.")
        return

    if save_dom_snapshots and not tasks:
        print(
            "Строк для записи в таблицу нет; выполняется только обход ссылок из столбца A для сохранения DOM.",
        )

    urls_ordered = [u for _, u in tasks]
    urls_unique_tasks = list(dict.fromkeys(urls_ordered))
    print(
        f"Строк к обновлению: {len(tasks)} (уникальных URL среди них: {len(urls_unique_tasks)}); "
        f"в браузере откроется уникальных URL: {len(urls_to_fetch)}",
    )

    def _flush_scraped_to_sheet(scraped_url: str, data: dict[str, Any]) -> None:
        for row, u in tasks:
            if u != scraped_url:
                continue
            done = _fill_row_only_empty(ws, row, col_by_key, data)
            print(
                f"Строка {row}: записано полей: {', '.join(done) or 'ничего (все ячейки уже были заполнены)'}",
            )

    results = scrape_urls_sequentially(
        urls_to_fetch,
        headless=headless,
        save_dom_snapshots=save_dom_snapshots,
        on_each_result=_flush_scraped_to_sheet if tasks else None,
    )
    if save_dom_snapshots:
        n = len(urls_to_fetch) * 3
        print(f"HTML-снимки DOM ({n} файлов при {len(urls_to_fetch)} организациях): {resolved_dom_dumps_dir()}")

    for row, url in tasks:
        res = results.get(url)
        if isinstance(res, BaseException):
            print(f"Строка {row}: ошибка парсинга (в таблицу не записано) — {res!s}")

    moved_after = _dedupe_current_companies_sheet(sh, ws, headers, col_by_key, cred_path=cred_path)
    if moved_after:
        dup_tab = _duplicates_sheet_name()
        print(f"Дубликаты после парсинга: перенесено строк на лист «{dup_tab}»: {moved_after}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="(legacy) Synaps → лист «компании»: URL в столбце A (текст или гиперссылка)",
    )
    parser.add_argument("--headed", action="store_true", help="Показать браузер (по умолчанию headless)")
    parser.add_argument(
        "--dump-dom",
        action="store_true",
        help="Сохранить HTML трёх страниц на каждую организацию в dom_dumps/ (главная после кликов, ОКВЭД, ИП)",
    )
    args = parser.parse_args()
    run_sheet_sync(headless=not args.headed, save_dom_snapshots=args.dump_dom)
