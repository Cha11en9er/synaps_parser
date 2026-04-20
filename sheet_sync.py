"""
Читает URL организаций из столбца A Google Таблицы, парсит Synaps и записывает поля по заголовкам строки 1.
Пустые ячейки заполняются; непустые не трогаем. Строка без парсинга, если в столбце даты (O) уже есть значение.
Заголовки — русские названия столбцов или буквенные ключи JSON.

После парсинга каждой карточки данные сразу пишутся в таблицу (не ждём конца всего списка), чтение строки — одним
запросом; при 429 — пауза и повтор.
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

from synaps_browser import (
    SHEET_JSON_KEYS,
    clean_email_as_on_page,
    email_sheet_line_key,
    resolved_dom_dumps_dir,
    scrape_urls_sequentially,
)

ROOT = Path(__file__).resolve().parent

_BANK_OK_PHRASE = "действующие решения о приостановлении отсутствуют"

# Нормализованный заголовок (нижний регистр, ё→е, без лишних пробелов и финального :) → логический ключ
HEADER_TO_KEY: dict[str, str] = {
    "дата регистрации ооо": "O",
    "уставной": "P",
    "доп телефон": "Q",
    "основной оквед": "R",
    "основной оквэд": "R",
    "основной okved": "R",
    "основной вид деятельности": "R",
    "юр адрес": "S",
    "доп имеил": "T",
    "доп имейл": "T",
    "доп email": "T",
    "состояние банковского счета": "U",
    "состояние банковского счёта": "U",
    "23": "V",
    "24": "W",
    "25": "X",
    "тренд по выручке": "Y",
    "надежность": "Z",
    "долг по исполнительному производству": "AA",
    "генеральный директор": "AB",
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
    return t or None


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
            raw = val.get("активные", val.get("Активные", ""))
            s = str(raw).strip() if raw is not None else ""
            return s if s else "0"
        return str(val).strip() if val else "0"

    if logical_key == "AB":
        if isinstance(val, dict) and val:
            parts = [f"{k.strip()} {str(v).strip()}".strip() for k, v in val.items() if k or v]
            return " ".join(x for x in parts if x)
        return str(val).strip()

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


def _row_needs_scrape_from_prefetched(row_vals: list[str], col_by_key: dict[str, int]) -> bool:
    """Те же правила, что и раньше у _row_needs_scrape, но по уже прочитанной строке (без API на каждую ячейку)."""
    if "O" in col_by_key:
        i = col_by_key["O"] - 1
        v = row_vals[i] if 0 <= i < len(row_vals) else ""
        return not (v and str(v).strip())
    for k in SHEET_JSON_KEYS:
        if k not in col_by_key:
            continue
        i = col_by_key[k] - 1
        v = row_vals[i] if 0 <= i < len(row_vals) else ""
        if not v or not str(v).strip():
            return True
    return False


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
        if cur is not None and str(cur).strip():
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

    headers = ws.row_values(1)
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
    max_col = max(col_by_key.values())
    sheet_rows: list[list[Any]] = []
    if last >= 2:
        rng = f"{rowcol_to_a1(2, 1)}:{rowcol_to_a1(last, max_col)}"
        sheet_rows = _sheet_call(lambda: ws.get(rng), desc="данные строк 2…") or []

    all_synaps_in_order: list[str] = []
    for row in range(2, last + 1):
        idx = row - 2
        row_vals = _pad_row_values(sheet_rows[idx] if 0 <= idx < len(sheet_rows) else [], max_col)
        url = (row_vals[0] or "").strip()
        if _is_synaps_org_url(url):
            all_synaps_in_order.append(url)
    urls_all_synaps_unique = list(dict.fromkeys(all_synaps_in_order))

    tasks: list[tuple[int, str]] = []
    for row in range(2, last + 1):
        idx = row - 2
        row_vals = _pad_row_values(sheet_rows[idx] if 0 <= idx < len(sheet_rows) else [], max_col)
        url = (row_vals[0] or "").strip()
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
        print("Нет строк для парсинга (пустые ссылки или все уже с данными).")
        return

    if not urls_to_fetch:
        print("В столбце A нет ссылок на карточки Synaps — нечего открывать.")
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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Synaps → Google Sheets по столбцу A")
    parser.add_argument("--headed", action="store_true", help="Показать браузер (по умолчанию headless)")
    parser.add_argument(
        "--dump-dom",
        action="store_true",
        help="Сохранить HTML трёх страниц на каждую организацию в dom_dumps/ (главная после кликов, ОКВЭД, ИП)",
    )
    args = parser.parse_args()
    run_sheet_sync(headless=not args.headed, save_dom_snapshots=args.dump_dom)
