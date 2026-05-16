"""Имена листов Google Таблицы: из .env (SHEET_*) или значения по умолчанию."""

from __future__ import annotations

import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_dotenv_loaded = False


def _load_env_once() -> None:
    global _dotenv_loaded
    if _dotenv_loaded:
        return
    try:
        from dotenv import load_dotenv

        load_dotenv(_ROOT / ".env")
    except ImportError:
        pass
    _dotenv_loaded = True


def _env_or_default(key: str, default: str) -> str:
    _load_env_once()
    raw = os.getenv(key)
    if raw is None:
        return default
    s = str(raw).strip().strip('"').strip("'")
    return s if s else default


def sheet_source_stage1() -> str:
    """Шаг 1: лист с сырыми строками (по умолчанию «исходные данные»)."""
    return _env_or_default("SHEET_SOURCE", "исходные данные")


def sheet_unique() -> str:
    """Лист уникальных по ИНН (по умолчанию «уникальные»)."""
    return _env_or_default("SHEET_UNIQUE", "уникальные")


def sheet_final() -> str:
    """Лист финальных данных основного пайплайна (по умолчанию «финальные данные»)."""
    return _env_or_default("SHEET_FINAL", "финальные данные")


def sheet_import() -> str:
    """Лист с ранее спарсенными данными для переноса по ИНН. Пусто, если SHEET_IMPORT не задан."""
    _load_env_once()
    raw = os.getenv("SHEET_IMPORT")
    if raw is None:
        return ""
    s = str(raw).strip().strip('"').strip("'")
    return s


def sheet_20m_source() -> str:
    """Источник для режима PARSER_RUN_20M=1 / final_parser_links."""
    return _env_or_default("SHEET_20M_SOURCE", "исходные по ссылке (20 млн)")


def sheet_20m_final() -> str:
    """Приёмник для режима --sheets-20m / final_parser_links."""
    return _env_or_default("SHEET_20M_FINAL", "исходные по ссылке (20 млн) финальные")


def env_flag(name: str) -> bool:
    """True, если переменная задана как 1 / true / yes / on / да (без учёта регистра)."""
    _load_env_once()
    v = (os.getenv(name) or "").strip().lower()
    return v in ("1", "true", "yes", "on", "да")


def sheet_copy_from() -> str:
    """Произвольная копия лист→лист: источник (вместе с SHEET_COPY_TO)."""
    return _env_or_default("SHEET_COPY_FROM", "")


def sheet_copy_to() -> str:
    """Произвольная копия лист→лист: приёмник."""
    return _env_or_default("SHEET_COPY_TO", "")


def stage2_options() -> dict[str, bool]:
    """
    Поведение parser_2stage_extract / final_parser_links без аргументов командной строки.

    SYNAPS_HEADED=1          — браузер с окном (по умолчанию headless).
    SYNAPS_DUMP_DOM=1        — сохранять HTML в dom_dumps/.
    PARSER_FORCE_COPY=1      — всегда перезаписать «финальные» из «уникальных» перед парсингом.
    PARSER_SKIP_COPY=1       — в режиме копии: не перезаписывать приёмник из источника (только парсинг).
    PARSER_AC_LAST_COLUMN=1  — численность Synaps (AC) в последний столбец заголовка.
    PARSER_RUN_20M=1         — копия SHEET_20M_SOURCE → SHEET_20M_FINAL (имеет приоритет над SHEET_COPY_*).
    """
    _load_env_once()
    return {
        "headed": env_flag("SYNAPS_HEADED"),
        "dump_dom": env_flag("SYNAPS_DUMP_DOM"),
        "force_copy": env_flag("PARSER_FORCE_COPY"),
        "skip_copy": env_flag("PARSER_SKIP_COPY"),
        "ac_last": env_flag("PARSER_AC_LAST_COLUMN"),
        "run_20m": env_flag("PARSER_RUN_20M"),
    }
