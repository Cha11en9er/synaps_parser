"""
Прогон Синапса по названиям из 1_stage/names.json → companies.json.

Поиск идёт по названию (не по ИНН). В выдаче предпочитается действующая организация
с похожим названием; ИНН читается уже с карточки.

Запускать БЕЗ VPN. Нужен .env в корне репозитория.

  python developer_comp/2_stage/enrich_synaps.py --dry-run
  python developer_comp/2_stage/enrich_synaps.py --limit 5 --headed
  python developer_comp/2_stage/enrich_synaps.py
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

STAGE_DIR = Path(__file__).resolve().parent
DEV_DIR = STAGE_DIR.parent
REPO_ROOT = DEV_DIR.parent
DEFAULT_INPUT = DEV_DIR / "1_stage" / "names.json"
DEFAULT_OUTPUT = STAGE_DIR / "companies.json"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

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


def _norm_key(value: str) -> str:
    return " ".join((value or "").strip().split()).casefold()


def _normalize_inn(value: str) -> str:
    return "".join(ch for ch in (value or "").strip() if ch.isdigit())


def load_names(path: Path) -> list[str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        values = raw
    elif isinstance(raw, dict):
        if isinstance(raw.get("names"), list):
            values = raw["names"]
        elif isinstance(raw.get("companies"), list):
            values = [c.get("name") for c in raw["companies"] if isinstance(c, dict)]
        else:
            raise RuntimeError(f"В {path} нет ключа names/companies.")
    else:
        raise RuntimeError(f"Неожиданный JSON в {path}")

    names: list[str] = []
    seen: set[str] = set()
    for value in values:
        name = " ".join(str(value or "").split()).strip()
        if not name:
            continue
        key = _norm_key(name)
        if key in seen:
            continue
        seen.add(key)
        names.append(name)
    return names


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
        name = " ".join(str(item.get("name") or "").split()).strip()
        if not name:
            continue
        progress[_norm_key(name)] = item
    return progress


def save_payload(
    path: Path,
    by_key: dict[str, dict[str, Any]],
    source: Path,
    names_total: int,
) -> None:
    items = list(by_key.values())
    ok_count = sum(1 for x in items if (x.get("status") or "").lower() == "ok")
    error_count = sum(1 for x in items if (x.get("status") or "").lower() == "error")
    payload = {
        "source_names": str(source),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "names_total": names_total,
        "ok_count": ok_count,
        "error_count": error_count,
        "items": items,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
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


def row_ok(name: str, data: dict[str, Any]) -> dict[str, Any]:
    inn = _normalize_inn(str(data.get("_inn") or ""))
    return {
        "name": name,
        "inn": inn,
        "status": "ok",
        "error": "",
        "profile_url": str(data.get("_profile_url") or "").strip(),
        "attempts": 1,
        "data": serialize_data(data),
    }


def row_error(name: str, exc: BaseException, prev: dict[str, Any] | None) -> dict[str, Any]:
    attempts = int((prev or {}).get("attempts") or 0) + 1
    return {
        "name": name,
        "inn": _normalize_inn(str((prev or {}).get("inn") or "")),
        "status": "error",
        "error": str(exc).strip(),
        "profile_url": "",
        "attempts": attempts,
        "data": {},
    }


def pending_names(
    names: list[str],
    progress: dict[str, dict[str, Any]],
    *,
    max_attempts: int,
) -> list[str]:
    out: list[str] = []
    for name in names:
        item = progress.get(_norm_key(name))
        if not item:
            out.append(name)
            continue
        status = (item.get("status") or "").strip().lower()
        if status == "ok":
            continue
        attempts = int(item.get("attempts") or 0)
        if attempts >= max_attempts:
            continue
        out.append(name)
    return out


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Синапс: names.json → companies.json")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--batch", type=int, default=40)
    p.add_argument("--max-attempts", type=int, default=3)
    p.add_argument("--headed", action="store_true")
    p.add_argument("--dry-run", action="store_true")
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
        raise SystemExit(
            f"Нет входного файла: {src}. Сначала запустите 1_stage/scrape_developers.py"
        )

    names = load_names(src)
    progress = load_progress(dst)
    if not progress and not dst.is_file():
        save_payload(dst, {}, src, len(names))

    queue = pending_names(names, progress, max_attempts=max(1, args.max_attempts))
    if args.limit and args.limit > 0:
        queue = queue[: args.limit]

    done_ok = sum(1 for item in progress.values() if (item.get("status") or "").lower() == "ok")
    print(f"Вход: {src}")
    print(f"Выход: {dst}")
    print(f"Названий во входе: {len(names)}; уже ok: {done_ok}; в этой очереди: {len(queue)}")
    if args.dry_run or not queue:
        if not queue:
            print("Очередь пустая — парсить нечего.")
        return

    from parser_synaps_browser import scrape_queries_sequentially

    batch_size = max(1, args.batch)
    headless = not args.headed
    offset = 0
    stalls = 0

    while offset < len(queue):
        chunk = queue[offset : offset + batch_size]
        print(f"Пачка {offset + 1}–{offset + len(chunk)} из {len(queue)}")

        def _ok(query: str, data: dict, _progress: dict = progress, _dst: Path = dst, _src: Path = src) -> None:
            key = _norm_key(query)
            _progress[key] = row_ok(query, data)
            save_payload(_dst, _progress, _src, len(names))
            inn = _progress[key].get("inn") or "?"
            print(f"  записан {query!r} (ИНН {inn})")

        def _err(query: str, exc: BaseException, _progress: dict = progress, _dst: Path = dst, _src: Path = src) -> None:
            key = _norm_key(query)
            _progress[key] = row_error(query, exc, _progress.get(key))
            save_payload(_dst, _progress, _src, len(names))
            print(f"  ошибка {query!r}: {exc}")

        try:
            scrape_queries_sequentially(
                chunk,
                headless=headless,
                save_dom_snapshots=False,
                on_each_result=_ok,
                on_each_error=_err,
                match_name=True,
            )
        except Exception as exc:
            stalls += 1
            print(f"Пачка оборвалась: {exc}")
            traceback.print_exc()
            if stalls >= 20:
                print("Синапс долго недоступен. Уже записанные строки сохранены, запустите снова.")
                break
            pause = min(180, 20 * stalls)
            print(f"Пауза {pause} с, продолжаю с ещё не записанных названий.")
            time.sleep(pause)
            progress = load_progress(dst)
            rest: list[str] = []
            for name in queue[offset:]:
                item = progress.get(_norm_key(name))
                if item and (item.get("status") or "").lower() == "ok":
                    continue
                if item and int(item.get("attempts") or 0) >= max(1, args.max_attempts):
                    continue
                rest.append(name)
            queue = rest
            offset = 0
            continue

        stalls = 0
        offset += len(chunk)

    print(f"Готово. Результат: {dst}")


if __name__ == "__main__":
    main()
