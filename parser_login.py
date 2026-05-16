"""
Разовый вход в Synaps: открывает браузер, логин, сохраняет storage state в synaps_storage_state.json.
Дальнейший парсинг (parser_2stage_extract) подхватывает этот файл через parser_synaps_browser.

Запуск (редко):  python parser_login.py
"""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent


def main() -> None:
    load_dotenv(ROOT / ".env")
    from parser_synaps_browser import (  # noqa: WPS433 — после dotenv
        DEFAULT_STORAGE,
        _credentials,
        _ensure_logged_in,
        _ensure_utf8_stdout,
    )

    main_url, mail, password = _credentials()
    _ensure_utf8_stdout()
    DEFAULT_STORAGE.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        _ensure_logged_in(page, main_url, mail, password)
        context.storage_state(path=str(DEFAULT_STORAGE))
        print(f"Сессия сохранена: {DEFAULT_STORAGE}")
        context.close()
        browser.close()


if __name__ == "__main__":
    main()
