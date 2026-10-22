"""
Прогон Синапса по ИНН из 1_stage/inns.json → companies.json.

Запускать БЕЗ VPN (Синапс с VPN часто недоступен). Нужен .env в корне репозитория
(MAIL/EMAIL, PASS/PASSWORD, MAIN_URL) и установленный Playwright.

  cd C:\\repos\\YouDo\\18.04.26-synaps_parser
  .\\.venv\\Scripts\\Activate.ps1
  python ritual_comp/2_stage/enrich_synaps.py --dry-run
  python ritual_comp/2_stage/enrich_synaps.py --limit 5 --headed
  python ritual_comp/2_stage/enrich_synaps.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

STAGE_DIR = Path(__file__).resolve().parent
RITUAL_DIR = STAGE_DIR.parent
REPO_ROOT = RITUAL_DIR.parent
DEFAULT_INPUT = RITUAL_DIR / "1_stage" / "inns.json"
DEFAULT_OUTPUT = STAGE_DIR / "companies.json"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Логический ключ парсера → имя поля в JSON.
SYNAPS_COLUMNS: list[tuple[str, str]] = [
    ("O", "registration_date"),
    ("P", "authorized_capital"),
    ("Q", "phones"),
    ("R", "okved"),
    ("S", "legal_address"),
    ("T", "emails"),
    ("U", "bank_account_state"),
    ("V", "revenue_2023"),
    ("W", "revenue_2024"),
    ("X", "revenue_2025"),
    ("Y", "revenue_trend"),
    ("Z", "reliability"),
    ("AA", "enforcement_debt"),
    ("AB", "general_director"),
    ("AC", "headcount"),
]


def _ensure_utf8_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _normalize_inn(value: str) -> str:
    return "".join(ch for ch in (value or "").strip() if ch.isdigit())


def load_inns(path: Path) -> list[str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        values = raw
    elif isinstance(raw, dict):
        if isinstance(raw.get("inns"), list):
            values = raw["inns"]
        elif isinstance(raw.get("companies"), list):
            values = [c.get("inn") for c in raw["companies"] if isinstance(c, dict)]
        else:
            raise RuntimeError(f"В {path} нет ключа inns/companies.")
    else:
        raise RuntimeError(f"Неожиданный JSON в {path}")

    inns: list[str] = []
    seen: set[str] = set()
    for value in values:
        inn = _normalize_inn(str(value or ""))
        if not inn or inn in seen:
            continue
        seen.add(inn)
        inns.append(inn)
    return inns


def load_progress(path: Path) -> dict[str, dict[str, Any]]:
    if not path.is_file() or path.stat().st_size == 0:
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    items = raw.get("items") if isinstance(raw, dict) else None
    if not isinstance(items, list):
        return {}
    progress: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        inn = _normalize_inn(str(item.get("inn") or ""))
        if not inn:
            continue
        progress[inn] = item
    return progress


def empty_payload(source: Path, inns_total: int) -> dict[str, Any]:
    return {
        "source_inns": str(source),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "inns_total": inns_total,
        "ok_count": 0,
        "error_count": 0,
        "items": [],
    }


# Как часто сбрасывать companies.json (1 = каждый ИНН; для 100k+ по умолчанию 25).
SAVE_EVERY = max(1, int(os.environ.get("SYNAPS_SAVE_EVERY") or "25"))


def save_payload(path: Path, by_inn: dict[str, dict[str, Any]], source: Path, inns_total: int) -> None:
    """Один компактный JSON: только items с нужными полями, без indent (экономия места/IO)."""
    items = list(by_inn.values())
    ok_count = sum(1 for x in items if (x.get("status") or "").lower() == "ok")
    error_count = sum(1 for x in items if (x.get("status") or "").lower() == "error")
    payload = {
        "source_inns": str(source),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "inns_total": inns_total,
        "ok_count": ok_count,
        "error_count": error_count,
        "items": items,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    tmp.replace(path)


def serialize_data(data: dict[str, Any]) -> dict[str, str]:
    from parser_export_baza import format_value_for_sheet

    out: dict[str, str] = {}
    for key, field in SYNAPS_COLUMNS:
        if key in data:
            out[field] = format_value_for_sheet(key, data.get(key))
        else:
            out[field] = ""
    return out


def row_ok(inn: str, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "inn": inn,
        "status": "ok",
        "error": "",
        "profile_url": str(data.get("_profile_url") or "").strip(),
        "attempts": 1,
        "data": serialize_data(data),
    }


def row_error(inn: str, exc: BaseException, prev: dict[str, Any] | None) -> dict[str, Any]:
    attempts = int((prev or {}).get("attempts") or 0) + 1
    return {
        "inn": inn,
        "status": "error",
        "error": str(exc).strip(),
        "profile_url": "",
        "attempts": attempts,
        "data": {},
    }


def pending_inns(
    inns: list[str],
    progress: dict[str, dict[str, Any]],
    *,
    max_attempts: int,
) -> list[str]:
    out: list[str] = []
    for inn in inns:
        item = progress.get(inn)
        if not item:
            out.append(inn)
            continue
        status = (item.get("status") or "").strip().lower()
        if status == "ok":
            continue
        attempts = int(item.get("attempts") or 0)
        if attempts >= max_attempts:
            continue
        out.append(inn)
    return out


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Синапс: inns.json → companies.json")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="JSON с inns из 1_stage")
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Куда писать результат")
    p.add_argument("--limit", type=int, default=0, help="Сколько ещё не обработанных ИНН (0 = все)")
    p.add_argument("--batch", type=int, default=50, help="Сколько ИНН за одну сессию браузера")
    p.add_argument("--max-attempts", type=int, default=3, help="Сколько ошибок на ИНН до пропуска")
    p.add_argument("--headed", action="store_true", help="Показать окно браузера")
    p.add_argument("--dry-run", action="store_true", help="Только очередь, без браузера")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    _ensure_utf8_stdout()
    load_dotenv(REPO_ROOT / ".env")
    args = parse_args(argv if argv is not None else sys.argv[1:])

    src = args.input if args.input.is_absolute() else (REPO_ROOT / args.input)
    if not src.is_file() and (STAGE_DIR / args.input).is_file():
        src = STAGE_DIR / args.input
    if not src.is_file():
        src = DEFAULT_INPUT
    dst = args.output if args.output.is_absolute() else (STAGE_DIR / args.output)

    if not src.is_file():
        raise SystemExit(f"Нет входного файла: {src}. Сначала запустите 1_stage/scrape_inns.py")

    inns = load_inns(src)
    progress = load_progress(dst)
    if not progress and not dst.is_file():
        save_payload(dst, {}, src, len(inns))

    queue = pending_inns(inns, progress, max_attempts=max(1, args.max_attempts))
    if args.limit and args.limit > 0:
        queue = queue[: args.limit]

    done_ok = sum(1 for item in progress.values() if (item.get("status") or "").lower() == "ok")
    print(f"Вход: {src}")
    print(f"Выход: {dst}")
    print(f"ИНН во входе: {len(inns)}; уже ok: {done_ok}; в этой очереди: {len(queue)}")
    if args.dry_run or not queue:
        if not queue:
            print("Очередь пустая — парсить нечего.")
        return

    from parser_synaps_browser import scrape_inns_sequentially

    batch_size = max(1, args.batch)
    headless = not args.headed
    offset = 0
    stalls = 0

    while offset < len(queue):
        chunk = queue[offset : offset + batch_size]
        print(f"Пачка {offset + 1}–{offset + len(chunk)} из {len(queue)}")

        dirty = {"n": 0}

        def _flush(force: bool = False, _progress: dict = progress, _dst: Path = dst, _src: Path = src) -> None:
            if force or dirty["n"] >= SAVE_EVERY:
                save_payload(_dst, _progress, _src, len(inns))
                dirty["n"] = 0

        def _ok(inn: str, data: dict, _progress: dict = progress) -> None:
            _progress[inn] = row_ok(inn, data)
            dirty["n"] += 1
            _flush()
            print(f"  записан ИНН {inn}")

        def _err(inn: str, exc: BaseException, _progress: dict = progress) -> None:
            _progress[inn] = row_error(inn, exc, _progress.get(inn))
            dirty["n"] += 1
            _flush(force=True)
            print(f"  ошибка ИНН {inn}: {exc}")

        try:
            scrape_inns_sequentially(
                chunk,
                headless=headless,
                save_dom_snapshots=False,
                on_each_result=_ok,
                on_each_error=_err,
            )
            _flush(force=True)
        except KeyboardInterrupt:
            _flush(force=True)
            print("\nОстановлено. Прогресс сохранён.")
            raise
        except Exception as exc:
            _flush(force=True)
            stalls += 1
            print(f"Пачка оборвалась: {exc}")
            traceback.print_exc()
            if stalls >= 20:
                print("Синапс долго недоступен. Уже записанные строки сохранены, запустите снова.")
                break
            pause = min(180, 20 * stalls)
            print(f"Пауза {pause} с, продолжаю с ещё не записанных ИНН.")
            time.sleep(pause)
            progress = load_progress(dst)
            rest: list[str] = []
            for inn in queue[offset:]:
                item = progress.get(inn)
                if item and (item.get("status") or "").lower() == "ok":
                    continue
                if item and int(item.get("attempts") or 0) >= max(1, args.max_attempts):
                    continue
                rest.append(inn)
            queue = rest
            offset = 0
            continue

        stalls = 0
        offset += len(chunk)

    save_payload(dst, progress, src, len(inns))
    print(f"Готово. Результат: {dst}")


if __name__ == "__main__":
    main()
