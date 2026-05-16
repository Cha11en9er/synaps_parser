"""
Шаг 1: дедупликация по ИНН на лист из .env SHEET_UNIQUE.
Исходник — SHEET_SOURCE (создаётся с шаблоном заголовков, если листа ещё нет).

После последнего непустого столбца исходной строки заголовков автоматически добавляются столбцы
под поля Synaps (см. parser_synaps_columns.SYNAPS_APPEND_HEADER_LABELS), если таких заголовков ещё нет.

Дальше вручную: parser_2stage_extract.py (листы задаются в .env).
"""

from __future__ import annotations

import os
from pathlib import Path

import gspread
from dotenv import load_dotenv
from gspread.exceptions import WorksheetNotFound

from parser_sheet_env import sheet_source_stage1, sheet_unique
from parser_synaps_columns import BOOTSTRAP_SHORT_SOURCE_HEADERS, merge_source_headers_with_synaps

ROOT = Path(__file__).resolve().parent
INN_HEADER = "ИНН"


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return (value or "").strip().strip('"').strip("'")


def _credentials_path() -> Path:
    path_value = _env("GOOGLE_CREDENTIALS_JSON", _env("GOOGLE_APPLICATION_CREDENTIALS"))
    if not path_value:
        raise RuntimeError("В .env задайте GOOGLE_CREDENTIALS_JSON=путь к json сервисного аккаунта")
    path = Path(path_value)
    if not path.is_absolute():
        path = ROOT / path
    if not path.is_file():
        raise FileNotFoundError(f"Файл учётных данных не найден: {path}")
    return path


def _sheet_id() -> str:
    sheet_id = _env("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("В .env задайте SHEET_ID (id таблицы из URL)")
    return sheet_id


def _normalize_header(value: str) -> str:
    return (value or "").strip().lower().replace("ё", "е")


def _find_inn_col(headers: list[str]) -> int:
    for idx, header in enumerate(headers):
        if _normalize_header(header) == _normalize_header(INN_HEADER):
            return idx
    raise RuntimeError(f"В заголовках нет столбца «{INN_HEADER}».")


def _normalize_inn(value: str) -> str:
    return "".join(ch for ch in (value or "").strip() if ch.isdigit())


def _ensure_worksheet(sh: gspread.Spreadsheet, title: str, *, cols: int) -> gspread.Worksheet:
    try:
        return sh.worksheet(title)
    except WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=2000, cols=max(cols, 26))


def run_sheet_inn_clean() -> None:
    load_dotenv(ROOT / ".env")
    cred_path = _credentials_path()
    sheet_id = _sheet_id()

    source_name = sheet_source_stage1()
    target_name = sheet_unique()

    gc = gspread.service_account(filename=str(cred_path))
    sh = gc.open_by_key(sheet_id)

    created_source = False
    try:
        source_ws = sh.worksheet(source_name)
    except WorksheetNotFound:
        source_ws = _ensure_worksheet(sh, source_name, cols=32)
        source_ws.update(
            range_name="A1",
            values=[list(BOOTSTRAP_SHORT_SOURCE_HEADERS)],
            value_input_option="RAW",
        )
        created_source = True

    all_rows = source_ws.get_all_values()
    if created_source:
        print(
            f"Создан лист «{source_name}» с шаблоном заголовков ({len(BOOTSTRAP_SHORT_SOURCE_HEADERS)} столбцов). "
            "Заполните строки данными и снова запустите этот скрипт.",
        )
        return

    if not all_rows:
        raise RuntimeError(f"Лист «{source_name}» пуст — добавьте заголовки и данные.")

    raw_header = all_rows[0]
    data_rows = all_rows[1:]
    merged_headers, base_w = merge_source_headers_with_synaps(raw_header)
    inn_col = _find_inn_col(merged_headers)
    width = len(merged_headers)
    added_syn_cols = width - base_w

    unique_rows: list[list[str]] = [merged_headers]
    seen_inn: set[str] = set()
    skipped_empty_inn = 0

    for row in data_rows:
        pr = list(row) + [""] * (width - len(row))
        pr = pr[:width]
        inn = _normalize_inn(pr[inn_col] if inn_col < len(pr) else "")
        if not inn:
            skipped_empty_inn += 1
            continue
        if inn in seen_inn:
            continue
        seen_inn.add(inn)
        unique_rows.append(pr)

    target_ws = _ensure_worksheet(sh, target_name, cols=max(width, 32))
    target_ws.clear()
    target_ws.update(range_name="A1", values=unique_rows, value_input_option="RAW")

    print(f"Источник: {source_name}")
    print(f"Назначение: {target_name}")
    print(f"Столбцов в строке заголовков на «{target_name}»: {width} (новых под Synaps: {added_syn_cols})")
    print(f"Всего строк в источнике (без заголовка): {len(data_rows)}")
    print(f"Уникальных компаний по ИНН: {len(unique_rows) - 1}")
    print(f"Пропущено строк без ИНН: {skipped_empty_inn}")


if __name__ == "__main__":
    run_sheet_inn_clean()
