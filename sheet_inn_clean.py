"""
Шаг 1 пайплайна: дедупликация по ИНН.
Читает «исходные данные», записывает уникальные строки (первая встреча по ИНН) в «уникальные».
Дальше (шаг 2) — sheet_sync_new.py: копия на лист «финальные данные» и дополнение с Synaps (лист «уникальные» не меняется).
"""

from __future__ import annotations

import os
from pathlib import Path

import gspread
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
SOURCE_SHEET = "исходные данные"
TARGET_SHEET = "уникальные"
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
    raise RuntimeError(f"В листе «{SOURCE_SHEET}» не найден столбец «{INN_HEADER}».")


def _normalize_inn(value: str) -> str:
    return "".join(ch for ch in (value or "").strip() if ch.isdigit())


def run_sheet_inn_clean() -> None:
    load_dotenv(ROOT / ".env")
    cred_path = _credentials_path()
    sheet_id = _sheet_id()

    gc = gspread.service_account(filename=str(cred_path))
    sh = gc.open_by_key(sheet_id)

    source_ws = sh.worksheet(SOURCE_SHEET)
    try:
        target_ws = sh.worksheet(TARGET_SHEET)
    except gspread.WorksheetNotFound:
        target_ws = sh.add_worksheet(title=TARGET_SHEET, rows=2000, cols=32)

    all_rows = source_ws.get_all_values()
    if not all_rows:
        raise RuntimeError(f"Лист «{SOURCE_SHEET}» пуст.")

    headers = all_rows[0]
    data_rows = all_rows[1:]
    inn_col = _find_inn_col(headers)

    unique_rows: list[list[str]] = [headers]
    seen_inn: set[str] = set()
    skipped_empty_inn = 0

    for row in data_rows:
        padded_row = row + [""] * (len(headers) - len(row))
        inn = _normalize_inn(padded_row[inn_col] if inn_col < len(padded_row) else "")
        if not inn:
            skipped_empty_inn += 1
            continue
        if inn in seen_inn:
            continue
        seen_inn.add(inn)
        unique_rows.append(padded_row[: len(headers)])

    target_ws.clear()
    target_ws.update(range_name="A1", values=unique_rows, value_input_option="RAW")

    print(f"Источник: {SOURCE_SHEET}")
    print(f"Назначение: {TARGET_SHEET}")
    print(f"Всего строк в источнике (без заголовка): {len(data_rows)}")
    print(f"Уникальных компаний по ИНН: {len(unique_rows) - 1}")
    print(f"Пропущено строк без ИНН: {skipped_empty_inn}")


if __name__ == "__main__":
    run_sheet_inn_clean()
