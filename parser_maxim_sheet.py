"""
Парсер Синапс для таблицы Максима (хлебозаводы, ~172 компании).

Заполняет:
  Y (25): имя директора из ФИО
  Z (26): заметка про верификацию телефона для AI-агента
  AA (27+): телефоны по одному на колонку
  AB (28+): короткое название без ООО/АО/ИП
  29+: остальные поля Синапс (без выручки)

Запуск:
  python parser_maxim_sheet.py              # headless
  python parser_maxim_sheet.py --headed     # с окном браузера
  python parser_maxim_sheet.py --limit 5    # только 5 (тест)
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Callable

import gspread
from dotenv import load_dotenv
from gspread.exceptions import APIError
from gspread.utils import rowcol_to_a1

from parser_synaps_browser import (
    clean_email_as_on_page,
    email_sheet_line_key,
    scrape_inns_sequentially,
)

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

SHEET_ID = os.getenv("SHEET_ID", "")
SOURCE_SHEET = "Лист1"
TARGET_SHEET = "ФИНАЛЬНЫЕ ДАННЫЕ"

SYNAPS_FIELDS: list[tuple[str, str]] = [
    ("O",  "Дата регистрации (Синапс)"),
    ("P",  "Уставной капитал"),
    ("Q",  "Телефоны (сырые)"),
    ("R",  "ОКВЭД основной"),
    ("S",  "Юридический адрес"),
    ("T",  "Email"),
    ("U",  "Состояние банк. счёта"),
    ("Y",  "Тренд по выручке"),
    ("Z",  "Надёжность"),
    ("AA", "Долги (исп. производство)"),
    ("AB", "Генеральный директор (ФИО)"),
    ("AC", "Среднесписочная численность"),
]


def _credentials_path() -> Path:
    p = (os.getenv("GOOGLE_CREDENTIALS_JSON") or "").strip().strip('"').strip("'")
    path = Path(p) if Path(p).is_absolute() else ROOT / p
    if not path.is_file():
        raise FileNotFoundError(f"Нет файла: {path}")
    return path


def _sheet_call(fn: Callable, *, desc: str = "") -> Any:
    waits = (5, 20, 65)
    for attempt in range(len(waits) + 1):
        try:
            return fn()
        except APIError as e:
            if "429" not in str(e) and "500" not in str(e):
                raise
            if attempt >= len(waits):
                raise
            d = waits[attempt]
            print(f"  лимит API{f' ({desc})' if desc else ''}, пауза {d}с")
            time.sleep(d)
        except Exception as e:
            if "timeout" in str(e).lower():
                if attempt >= len(waits):
                    raise
                d = waits[attempt]
                print(f"  таймаут{f' ({desc})' if desc else ''}, пауза {d}с")
                time.sleep(d)
                continue
            raise


def _normalize_inn(raw: str) -> str:
    return "".join(ch for ch in (raw or "").strip() if ch.isdigit())


def _pad(r: list[str], w: int) -> list[str]:
    out = [str(c) if c is not None else "" for c in r[:w]]
    while len(out) < w:
        out.append("")
    return out


def _first_name(full: str) -> str:
    """Иванов Иван Иванович → Иван. Шамонова Елизавета Андреевна → Елизавета."""
    parts = full.strip().split()
    return parts[1].capitalize() if len(parts) >= 2 else (parts[0].capitalize() if parts else "")


def extract_name_from_director(raw: str) -> str:
    """Из 'Меситова Татьяна Евгеньевна руководитель 30.06.2015' → 'Татьяна'"""
    if not raw or raw == "-":
        return ""
    s = re.sub(r"\d{2}\.\d{2}\.\d{4}", "", raw)
    s = re.sub(r"(Изменение|изменение|Назначена|назначена|руководитель)", "", s)
    return _first_name(s)


def short_name(full: str) -> str:
    """ООО КФ 'ПРЕМЬЕРА' → КФ ПРЕМЬЕРА. ИП Иванов → Иванов"""
    s = full.strip()
    s = re.sub(r'^(?:ООО|АО|ЗАО|ПАО|ОАО|ИП|МУП|МКУ|ФГУП|ПК|АНО|НКО|СЗ|ТСЖ|ЖСК)\s*', '', s, flags=re.I)
    for ch in ('"', '\u00ab', '\u00bb', "'"):
        s = s.replace(ch, " ")
    return re.sub(r"\s+", " ", s).strip()


def verify_note(name: str, phones: list[str]) -> str:
    if not name:
        return "Нет данных для верификации"
    return (
        f"AI: поздороваться по имени '{name}', сверить что абонент отзывается. "
        f"Номеров: {len(phones)}. "
        f"Если не отзывается — уточнить 'это компания?'"
    )


def fmt(key: str, val: Any) -> str:
    if val is None:
        return "-"
    if key == "P" and isinstance(val, int):
        return f"{val:,} руб.".replace(",", " ")
    if key == "T" and isinstance(val, list):
        seen: set[str] = set()
        out: list[str] = []
        for v in val:
            v = clean_email_as_on_page(str(v))
            k = email_sheet_line_key(v)
            if k and k not in seen:
                seen.add(k)
                out.append(v)
        return "\n".join(out) if out else "-"
    if key == "Q" and isinstance(val, list):
        return "\n".join(str(v).strip() for v in val)
    if key == "R" and isinstance(val, dict):
        return "\n".join(f"{k} {v}".strip() for k, v in val.items() if k or v)
    if key == "AA" and isinstance(val, dict):
        return f"кол-во: {val.get('кол-во', '-')}, сумма: {val.get('сумма', '-')}"
    if key == "AB" and isinstance(val, dict):
        return " ".join(f"{k} {v}".strip() for k, v in val.items() if k or v)
    if key == "Y" and isinstance(val, dict):
        if len(val) == 1:
            k, v = next(iter(val.items()))
            return f"{v}: {k}"
        return json.dumps(val, ensure_ascii=False)
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return str(val).strip()


# ═══════════════════════════════════════════════════════════════

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dump-dom", action="store_true")
    args = ap.parse_args()

    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    gc = gspread.service_account(filename=str(_credentials_path()))
    sh = gc.open_by_key(SHEET_ID)
    print(f"Таблица: {sh.title}")

    # ── Чтение источника ──────────────────────────────────────
    ws_src = sh.worksheet(SOURCE_SHEET)
    src = _sheet_call(lambda: ws_src.get_all_values(), desc="чтение")
    if not src or len(src) < 2:
        raise SystemExit("Нет данных.")

    hdr = src[0]
    rows = src[1:]
    print(f"Источник: {len(rows)} строк, {len(hdr)} колонок")

    # ИНН
    inn_col = next((i for i, h in enumerate(hdr) if (h or "").strip().lower() == "инн"), -1)
    if inn_col == -1:
        raise SystemExit("Нет ИНН.")

    # ── Целевой лист ──────────────────────────────────────────
    try:
        ws = sh.worksheet(TARGET_SHEET)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=TARGET_SHEET, rows=2000, cols=45)

    # ── Заголовок ─────────────────────────────────────────────
    fhdr = list(hdr)
    while len(fhdr) < 28:
        fhdr.append("")
    fhdr = fhdr[:28] + [""] + [t for _, t in SYNAPS_FIELDS]

    # ── Данные ────────────────────────────────────────────────
    frows = [_pad(r, 28) + [""] * (len(fhdr) - 28) for r in rows]

    # ИНН → строки
    inn2rows: dict[str, list[int]] = {}
    names: dict[str, str] = {}
    for idx, r in enumerate(frows):
        inn = _normalize_inn(r[inn_col] if inn_col < len(r) else "")
        if inn:
            inn2rows.setdefault(inn, []).append(idx)
            names[inn] = str(r[0]) if len(r) > 0 else ""

    todo = list(inn2rows.keys())
    if args.limit and args.limit > 0:
        todo = todo[:args.limit]

    print(f"ИНН для парсинга: {len(todo)}")

    # ── Запись на лист ────────────────────────────────────────
    _sheet_call(lambda: ws.clear(), desc="очистка")
    _sheet_call(lambda: ws.update(values=[fhdr], range_name="A1", value_input_option="RAW"), desc="заголовок")
    _sheet_call(lambda: ws.update(values=frows, range_name="A2", value_input_option="RAW"), desc="данные")
    print(f"Записано {len(frows)} строк на '{TARGET_SHEET}'.")

    SYN_COL = 29  # 1-based

    def write_row(rn: int, data: dict, inn: str, cname: str) -> list[str]:
        updated: list[str] = []
        batch: list[dict] = []

        # Y(25): имя
        dr = fmt("AB", data.get("AB", "-"))
        fn = extract_name_from_director(dr)
        if not fn and cname.upper().startswith("ИП"):
            fn = extract_name_from_director(cname)
        if fn:
            batch.append({"range": rowcol_to_a1(rn, 25), "values": [[fn]]})
            updated.append(f"Y({fn})")

        # Z(26): верификация
        phones_raw = data.get("Q", [])
        if isinstance(phones_raw, list):
            phones = [str(p).strip() for p in phones_raw if str(p).strip()]
        elif isinstance(phones_raw, str):
            phones = [p.strip() for p in phones_raw.split("\n") if p.strip()]
        else:
            phones = []
        note = verify_note(fn or extract_name_from_director(dr), phones)
        batch.append({"range": rowcol_to_a1(rn, 26), "values": [[note]]})
        updated.append("Z")

        # Телефоны начиная с AA(27)
        for i, p in enumerate(phones):
            batch.append({"range": rowcol_to_a1(rn, 27 + i), "values": [[p]]})
            updated.append(f"тел.{i+1}")

        # Короткое название AB(28+)
        short = short_name(cname) or "?"
        sc = max(28, 27 + len(phones))
        batch.append({"range": rowcol_to_a1(rn, sc), "values": [[short]]})
        updated.append(f"AB({short})")

        # Synaps поля начиная с 29
        for i, (key, _) in enumerate(SYNAPS_FIELDS):
            if key not in data:
                continue
            col = SYN_COL + i
            val = fmt(key, data[key])
            batch.append({"range": rowcol_to_a1(rn, col), "values": [[val]]})
            updated.append(key)

        if batch:
            _sheet_call(lambda b=batch: ws.batch_update(b, value_input_option="USER_ENTERED"),
                        desc=f"строка {rn}")
        return updated

    def on_ok(inn: str, data: dict) -> None:
        cname = names.get(inn, "")
        for ridx in inn2rows.get(inn, []):
            try:
                rn = ridx + 2
                upd = write_row(rn, data, inn, cname)
                print(f"  [{rn}] {', '.join(upd)}")
            except Exception as e:
                print(f"  [{rn}] ERROR запись: {e}")
        time.sleep(1.3)

    def on_err(inn: str, exc: BaseException) -> None:
        print(f"  ! ИНН {inn}: {exc}")
        for ridx in inn2rows.get(inn, []):
            try:
                rn = ridx + 2
                a1 = rowcol_to_a1(rn, SYN_COL)
                _sheet_call(lambda: ws.update(values=[[f"ERR: {exc}"]],
                                              range_name=a1,
                                              value_input_option="USER_ENTERED"),
                            desc="err")
            except Exception:
                pass

    print(f"\n{'='*60}")
    print(f"Парсинг {len(todo)} ИНН...")
    print(f"{'='*60}")

    results = scrape_inns_sequentially(
        todo,
        headless=not args.headed,
        save_dom_snapshots=args.dump_dom,
        on_each_result=on_ok,
        on_each_error=on_err,
    )

    ok = sum(1 for v in results.values() if isinstance(v, dict) and not isinstance(v, BaseException))
    err = sum(1 for v in results.values() if isinstance(v, BaseException))
    print(f"\n{'='*60}")
    print(f"Готово! OK: {ok}  ERR: {err}  Всего: {len(results)}")
    print(f"Лист '{TARGET_SHEET}' обновлён.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()