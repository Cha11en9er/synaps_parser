"""
Фильтр CRM: строки из SHEET_FINAL без ИНН, уже присутствующих на SHEET_CRM → SHEET_UNIQUE_CRM.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Callable

import gspread
from dotenv import load_dotenv
from gspread.exceptions import APIError, WorksheetNotFound

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
            print(f"  {msg}, пауза {d} с...")
            time.sleep(d)


def _pad_row(row: list[str], width: int) -> list[str]:
    out = [str(c) if c is not None else "" for c in row[:width]]
    while len(out) < width:
        out.append("")
    return out


def _ensure_worksheet(sh: gspread.Spreadsheet, title: str, *, rows: int, cols: int) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(title)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=max(rows, 2000), cols=max(cols, 26))
        print(f"Создан лист «{title}».")
        return ws
    if ws.row_count < rows or ws.col_count < cols:
        ws.resize(rows=max(ws.row_count, rows), cols=max(ws.col_count, cols))
    return ws


def _read_sheet(sh: gspread.Spreadsheet, title: str) -> list[list[str]]:
    ws = sh.worksheet(title)
    data = _sheet_call(lambda: ws.get_all_values(), desc=f"чтение «{title}»") or []
    if not data:
        raise RuntimeError(f"Лист «{title}» пуст.")
    return data


def run_crm_unique_export() -> None:
    load_dotenv(ROOT / ".env")
    final_name = _env("SHEET_FINAL", "ЭКСПОРТ БАЗА ФИНАЛЬНЫЕ")
    crm_name = _env("SHEET_CRM")
    target_name = _env("SHEET_UNIQUE_CRM")
    if not crm_name:
        raise RuntimeError("В .env задайте SHEET_CRM")
    if not target_name:
        raise RuntimeError("В .env задайте SHEET_UNIQUE_CRM")

    gc = gspread.service_account(filename=str(_credentials_path()))
    sh = gc.open_by_key(_sheet_id())

    print("--- parser_crm_unique ---")
    print(f"  Источник:     «{final_name}»")
    print(f"  Исключить:    «{crm_name}» (по ИНН)")
    print(f"  Назначение:   «{target_name}»")

    final_all = _read_sheet(sh, final_name)
    crm_all = _read_sheet(sh, crm_name)

    final_header = final_all[0]
    final_data = final_all[1:]
    final_inn_col = _find_inn_col(final_header)
    final_width = max(len(final_header), max((len(r) for r in final_all), default=1))

    crm_ws = sh.worksheet(crm_name)
    crm_grid_rows = crm_ws.row_count
    if crm_grid_rows > len(crm_all) + 50:
        print(
            f"  Внимание: на «{crm_name}» в сетке {crm_grid_rows} строк, "
            f"но с данными в ячейках — {len(crm_all)} (ниже пустые строки не участвуют в исключении).",
        )

    crm_header = crm_all[0]
    crm_inn_col = _find_inn_col(crm_header)
    crm_inns: set[str] = set()
    crm_empty = 0
    for row in crm_all[1:]:
        padded = _pad_row(row, max(len(crm_header), len(row)))
        inn = _normalize_inn(padded[crm_inn_col] if crm_inn_col < len(padded) else "")
        if inn:
            crm_inns.add(inn)
        else:
            crm_empty += 1

    out_rows: list[list[str]] = [_pad_row(final_header, final_width)]
    skipped_in_crm = 0
    skipped_empty_inn = 0
    kept = 0

    for row in final_data:
        padded = _pad_row(row, final_width)
        inn = _normalize_inn(padded[final_inn_col] if final_inn_col < len(padded) else "")
        if not inn:
            skipped_empty_inn += 1
            continue
        if inn in crm_inns:
            skipped_in_crm += 1
            continue
        out_rows.append(padded)
        kept += 1

    target_ws = _ensure_worksheet(
        sh,
        target_name,
        rows=len(out_rows) + 100,
        cols=final_width + 5,
    )
    _sheet_call(lambda: target_ws.clear(), desc=f"очистка «{target_name}»")
    _sheet_call(
        lambda: target_ws.update(range_name="A1", values=out_rows, value_input_option="RAW"),
        desc=f"запись «{target_name}»",
    )

    print()
    print(f"«{final_name}» строк данных: {len(final_data)}")
    print(f"«{crm_name}» строк данных: {len(crm_all) - 1} (уник. ИНН: {len(crm_inns)}, без ИНН: {crm_empty})")
    print(f"Исключено (ИНН уже в CRM): {skipped_in_crm}")
    print(f"Пропущено без ИНН в финале: {skipped_empty_inn}")
    print(f"Записано на «{target_name}»: {kept} (+ заголовок, всего строк {len(out_rows)})")
    print(f"Проверка: {len(final_data)} - {skipped_in_crm} - {skipped_empty_inn} = {kept}")


if __name__ == "__main__":
    run_crm_unique_export()
