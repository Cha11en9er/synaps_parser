"""
Заголовки столбцов Synaps (логические O…AC), которые добавляются после исходных данных на шаге 1.
"""

from __future__ import annotations

import re

# Короткий шаблон исходных столбцов (если лист пуст или без заголовка ИНН) — шаг 1 и шаг 2.
BOOTSTRAP_SHORT_SOURCE_HEADERS: list[str] = [
    "Название компании",
    "ИНН",
    "Главный ОКВЭД (название)",
    "Главный ОКВЭД (код)",
    "Дата регистрации",
    "Тип компании",
    "Численность сотрудников (чел.) *",
    "Выручка (тыс. руб.) *",
    "Анализ выручки",
    "Город",
    "Регион",
]


def has_inn_column_in_headers(row0: list[str]) -> bool:
    return any(_canon_header(h) == "инн" for h in row0 if h is not None)


def first_header_row_nonempty(row0: list[str]) -> bool:
    return any(str(h).strip() for h in row0 if h is not None)


def build_bootstrap_merged_headers() -> list[str]:
    """Одна строка: короткий исходник + все недостающие столбцы Synaps справа."""
    merged, _ = merge_source_headers_with_synaps(list(BOOTSTRAP_SHORT_SOURCE_HEADERS))
    return merged


# Порядок — как в типовой таблице заказчика; отсутствующие в исходнике столбцы добавляются справа.
SYNAPS_APPEND_HEADER_LABELS: list[str] = [
    "Дата регистрации (Синапс)",
    "Уставной",
    "доп телефон",
    "ОКВЭД",
    "Юридический адрес",
    "доп имеил",
    "счет",
    "23",
    "24",
    "25",
    "тренд по выручке",
    "надежность",
    "Долг по исполнительному производству:",
    "Генеральный директор",
    "Среднесписочная численность",
]


def _canon_header(h: str) -> str:
    t = (h or "").strip().lower().replace("ё", "е")
    t = re.sub(r"\s+", " ", t)
    return t.rstrip(":").strip()


def _trim_trailing_empty(headers: list[str]) -> list[str]:
    row = list(headers)
    while row and not str(row[-1]).strip():
        row.pop()
    return row


def synaps_headers_to_append(existing_headers: list[str]) -> list[str]:
    """Заголовки Synaps, которых ещё нет среди исходных (сравнение по нормализованной строке)."""
    canon_existing = {_canon_header(h) for h in existing_headers if h is not None and str(h).strip()}
    out: list[str] = []
    for h in SYNAPS_APPEND_HEADER_LABELS:
        if _canon_header(h) in canon_existing:
            continue
        out.append(h)
    return out


def merge_source_headers_with_synaps(source_headers: list[str]) -> tuple[list[str], int]:
    """
    Обрезает хвост пустых ячеек в строке заголовков источника, добавляет недостающие столбцы Synaps.
    Возвращает (полная строка заголовков, число столбцов исходника до добавления).
    """
    base = _trim_trailing_empty(source_headers)
    if not base:
        base = [""]
    add = synaps_headers_to_append(base)
    merged = base + add
    return merged, len(base)
