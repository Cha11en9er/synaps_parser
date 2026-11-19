"""
Сборка Excel: исходные данные Максима + спарсенные поля Синапс справа.
Записи сгруппированы и разделены строкой прочерков:

  Группа 1 — с номерами (есть телефон)
  --- прочерки ---
  Группа 2 — спарсено, но без телефона
  --- прочерки ---
  Группа 3 — не спарсено (ошибка/не обработано)
  --- прочерки ---
  Группа 4 — нет на Синапсе (битый ИНН)

Столбцы 1–24 — названия как в листе Максима (не меняются).
Дальше — мои столбцы: Имя директора, номер 1..N, Короткое название, поля Синапс.

Запуск:
  python build_excel.py
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

ROOT = Path(__file__).resolve().parent
DEFAULT_STATE = ROOT / "synaps_state.json"
DEFAULT_SOURCE = ROOT / "input" / "companies.json"
DEFAULT_OUT = ROOT / "result_maxim.xlsx"

SYNAPS_FIELDS = [
    "Дата регистрации (Синапс)",
    "Уставной капитал",
    "ОКВЭД основной",
    "Юридический адрес",
    "Email",
    "Тренд по выручке",
    "Надёжность",
    "Долги (исп. производство)",
    "Генеральный директор (ФИО)",
    "Среднесписочная численность",
]

STATUS_LABELS = {
    "ok": "спарсено",
    "not_found": "нет на Синапсе",
    "error": "ошибка (не спарсено)",
    "pending": "не обработано",
}

DASH = "—"


def _inn(v) -> str:
    return "".join(c for c in str(v or "") if c.isdigit())


def count_phones(r: dict) -> int:
    n = 0
    while f"номер {n+1}" in r and str(r.get(f"номер {n+1}", "")).strip():
        n += 1
    return n


def main():
    ap = argparse.ArgumentParser(description="Сборка Excel с данными Синапс")
    ap.add_argument("--state", type=Path, default=DEFAULT_STATE)
    ap.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()

    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    # ── Источник ───────────────────────────────────────────────
    with open(args.source, encoding="utf-8") as f:
        source = json.load(f)

    all_keys = list(source[0].keys())
    skip = ["имя директора", "как проверить телефон", "номера разбить",
            "название короткое", "назвавние"]
    src_cols: list[str] = []
    for k in all_keys:
        if any(s in k.lower().strip() for s in skip):
            continue
        src_cols.append(k)
        if len(src_cols) >= 24:
            break

    src_by_inn: dict[str, dict] = {}
    order: list[str] = []
    for c in source:
        inn = _inn(c.get("ИНН", ""))
        if inn and inn not in src_by_inn:
            order.append(inn)
            src_by_inn[inn] = c

    # ── Состояние ──────────────────────────────────────────────
    with open(args.state, encoding="utf-8") as f:
        raw = json.load(f)
    state = raw.get("items", {})

    max_phones = 0
    for r in state.values():
        max_phones = max(max_phones, count_phones(r))
    max_phones = max(1, max_phones)

    # ── Заголовки ──────────────────────────────────────────────
    phone_cols = [f"номер {i+1}" for i in range(max_phones)]
    headers = src_cols + ["Имя директора"] + phone_cols + ["Короткое название"] \
        + SYNAPS_FIELDS + ["Статус парсинга"]

    # ── Группировка ────────────────────────────────────────────
    g1_with_phone: list[str] = []
    g2_no_phone: list[str] = []
    g3_not_parsed: list[str] = []
    g4_not_found: list[str] = []

    for inn in order:
        st = state.get(inn)
        status = st.get("_status") if st else "pending"
        if status == "ok":
            (g1_with_phone if count_phones(st) > 0 else g2_no_phone).append(inn)
        elif status == "not_found":
            g4_not_found.append(inn)
        else:  # error / pending
            g3_not_parsed.append(inn)

    groups = [
        ("с номерами", g1_with_phone),
        ("спарсено без номера", g2_no_phone),
        ("не спарсено", g3_not_parsed),
        ("нет на Синапсе", g4_not_found),
    ]

    # ── Excel ──────────────────────────────────────────────────
    wb = Workbook()
    ws = wb.active
    ws.title = "Данные Синапс"

    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill("solid", fgColor="4472C4")
    header_fill_syn = PatternFill("solid", fgColor="70AD47")
    header_fill_st = PatternFill("solid", fgColor="808080")
    dash_fill = PatternFill("solid", fgColor="D9D9D9")

    ws.append(headers)
    n_src = len(src_cols)
    for idx in range(1, len(headers) + 1):
        c = ws.cell(row=1, column=idx)
        c.font = header_font
        if idx <= n_src:
            c.fill = header_fill
        elif idx == len(headers):
            c.fill = header_fill_st
        else:
            c.fill = header_fill_syn
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    stats = Counter()
    row_num = 1

    def write_row(inn: str) -> None:
        nonlocal row_num
        row_num += 1
        src = src_by_inn.get(inn, {})
        st = state.get(inn)
        status = st.get("_status") if st else "pending"
        stats[status] += 1

        row = [str(src.get(k, "") or "") for k in src_cols]
        if st and status == "ok":
            row.append(st.get("Имя директора", "-"))
            for i in range(max_phones):
                row.append(st.get(f"номер {i+1}", ""))
            row.append(st.get("Короткое название", "?"))
            for fld in SYNAPS_FIELDS:
                row.append(st.get(fld, "-"))
        else:
            row.append("")
            row += [""] * max_phones
            row.append("")
            row += [""] * len(SYNAPS_FIELDS)
        row.append(STATUS_LABELS.get(status, status))
        ws.append(row)

    def write_separator() -> None:
        nonlocal row_num
        row_num += 1
        ws.append([DASH] * len(headers))
        for idx in range(1, len(headers) + 1):
            ws.cell(row=row_num, column=idx).fill = dash_fill

    for gi, (label, inns) in enumerate(groups):
        if not inns:
            continue
        for inn in inns:
            write_row(inn)
        if gi < len(groups) - 1:
            write_separator()

    ws.freeze_panes = "A2"
    widths = {1: 34, 2: 45, 3: 12, 4: 40, 5: 14}
    for i in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(i)].width = widths.get(i, 18)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(args.out)

    print("=" * 60)
    print("EXCEL СОБРАН")
    print("=" * 60)
    print(f"Файл: {args.out}  ({args.out.stat().st_size/1024/1024:.1f} MB)")
    print(f"Строк (с заголовком и разделителями): {row_num}")
    print(f"Столбцов: {len(headers)}")
    print(f"\nГруппы по порядку:")
    for label, inns in groups:
        print(f"  {label:22} {len(inns)}")
    print("=" * 60)


if __name__ == "__main__":
    main()