"""Фиксированное сопоставление столбцов таблицы заказчика с логическими ключами парсера."""

from __future__ import annotations

import os
import re

# A=1 … U=21, W=23 (Google Sheets / gspread, нумерация с 1)
_COL_U = 21
_COL_W = 23

_BANK_HEADER_LABELS = frozenset(
    {"счет", "счёт", "состояние банковского счета", "состояние банковского счёта"},
)


def _canon_header_sheet(h: str) -> str:
    t = (h or "").strip().lower().replace("ё", "е")
    t = re.sub(r"\s+", " ", t)
    return t.rstrip(":").strip()


def apply_u_w_overrides(col_by_key: dict[str, int]) -> None:
    """
    Столбец U — долг по исполнительному производству (AA).
    Столбец W — численность (AC).

    Если по заголовкам в U/W оказались другие поля (например счёт или «24»), привязка с них снимается.
    """
    fixed: dict[str, int] = {"AA": _COL_U, "AC": _COL_W}
    occupied = set(fixed.values())
    for k in list(col_by_key.keys()):
        if col_by_key[k] in occupied and k not in fixed:
            del col_by_key[k]
    col_by_key.update(fixed)


def restore_synaps_bank_column(headers: list[str], col_by_key: dict[str, int]) -> bool:
    """
    Если логический U (счёт / состояние банковского счёта) не сопоставился — восстанавливаем по заголовку или .env.

    Столбцы, уже занятые AA (долг) и AC (численность), не используются для U.
    """
    if "U" in col_by_key:
        return True
    reserved: set[int] = set()
    for k in ("AA", "AC"):
        j = col_by_key.get(k)
        if isinstance(j, int) and j > 0:
            reserved.add(j)
    n = len(headers)
    raw = (os.getenv("SYNAPS_BANK_COL") or "").strip()
    if raw.isdigit():
        j = int(raw)
        if 1 <= j <= n and j not in reserved:
            col_by_key["U"] = j
            return True
    for i, raw_h in enumerate(headers):
        c = _canon_header_sheet(raw_h)
        if c not in _BANK_HEADER_LABELS:
            continue
        col = i + 1
        if col in reserved:
            continue
        col_by_key["U"] = col
        return True
    return False
