"""
Прогон Synaps по списку ИНН из CSV. Исходный файл не меняется.
Рядом пишется отдельный CSV (ключ — ИНН), его потом можно склеить с базой.

По умолчанию:
  вход:  input/SimpleTouch_baza_INN.csv
  выход: input/SimpleTouch_baza_INN_synaps.csv

Уже успешно записанные ИНН при повторном запуске пропускаются.
Ошибка по ИНН повторяется, пока число неудач меньше --max-attempts (по умолчанию 3).
Браузер перезапускается пачками (--batch), чтобы обрыв сессии не терял уже записанные строки.

Примеры:
  python parser_csv_inns.py --dry-run
  python parser_csv_inns.py --limit 5 --headed
  python parser_csv_inns.py
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT = ROOT / "input" / "SimpleTouch_baza_INN.csv"

# Логический ключ парсера → заголовок в выходном CSV.
SYNAPS_COLUMNS: list[tuple[str, str]] = [
    ("O", "Дата регистрации (Синапс)"),
    ("P", "Уставной"),
    ("Q", "Телефон (Синапс)"),
    ("R", "ОКВЭД (Синапс)"),
    ("S", "Юридический адрес (Синапс)"),
    ("T", "Email (Синапс)"),
    ("U", "Состояние банковского счета"),
    ("V", "Выручка 2023 (Синапс)"),
    ("W", "Выручка 2024 (Синапс)"),
    ("X", "Выручка 2025 (Синапс)"),
    ("Y", "Тренд по выручке (Синапс)"),
    ("Z", "Надежность"),
    ("AA", "Долг по исполнительному производству"),
    ("AB", "Генеральный директор (Синапс)"),
    ("AC", "Численность (Синапс)"),
]

META_COLUMNS = ["ИНН", "статус", "ошибка", "ссылка Synaps"]
OUT_FIELDNAMES = META_COLUMNS + [title for _, title in SYNAPS_COLUMNS]


def _normalize_inn(value: str) -> str:
    return "".join(ch for ch in (value or "").strip() if ch.isdigit())


def _canon_header(h: str) -> str:
    return (h or "").strip().lower().replace("ё", "е")


def load_inns(path: Path) -> list[str]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError(f"В {path} нет строки заголовков.")
        inn_key = None
        for name in reader.fieldnames:
            if _canon_header(name) == "инн":
                inn_key = name
                break
        if inn_key is None:
            raise RuntimeError(f"В {path} нет столбца «ИНН». Заголовки: {reader.fieldnames}")
        inns: list[str] = []
        seen: set[str] = set()
        for row in reader:
            inn = _normalize_inn(row.get(inn_key) or "")
            if not inn or inn in seen:
                continue
            seen.add(inn)
            inns.append(inn)
    return inns


def load_progress(path: Path) -> dict[str, tuple[str, int]]:
    """ИНН → (последний статус, число ошибок)."""
    if not path.is_file():
        return {}
    progress: dict[str, tuple[str, int]] = {}
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "ИНН" not in reader.fieldnames:
            raise RuntimeError(f"В {path} нет столбца «ИНН» — это не файл результата парсера.")
        for row in reader:
            inn = _normalize_inn(row.get("ИНН") or "")
            if not inn:
                continue
            status = (row.get("статус") or "").strip().lower()
            prev_status, errors = progress.get(inn, ("", 0))
            if status == "error":
                errors += 1
            progress[inn] = (status or prev_status, errors)
    return progress


class ResultCsv:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.is_file() or self.path.stat().st_size == 0:
            with self.path.open("w", encoding="utf-8-sig", newline="") as f:
                csv.DictWriter(f, fieldnames=OUT_FIELDNAMES).writeheader()

    def append(self, row: dict[str, str]) -> None:
        with self.path.open("a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=OUT_FIELDNAMES, extrasaction="ignore")
            writer.writerow({key: row.get(key, "") for key in OUT_FIELDNAMES})
            f.flush()


def _row_ok(inn: str, data: dict) -> dict[str, str]:
    from parser_export_baza import format_value_for_sheet

    row = {title: "" for _, title in SYNAPS_COLUMNS}
    row["ИНН"] = inn
    row["статус"] = "ok"
    row["ошибка"] = ""
    row["ссылка Synaps"] = str(data.get("_profile_url") or "").strip()
    for key, title in SYNAPS_COLUMNS:
        if key in data:
            row[title] = format_value_for_sheet(key, data.get(key))
    return row


def _row_error(inn: str, exc: BaseException) -> dict[str, str]:
    row = {title: "" for title in OUT_FIELDNAMES}
    row["ИНН"] = inn
    row["статус"] = "error"
    row["ошибка"] = str(exc).strip()
    return row


def pending_inns(
    inns: list[str],
    progress: dict[str, tuple[str, int]],
    *,
    max_attempts: int,
) -> list[str]:
    out: list[str] = []
    for inn in inns:
        status, errors = progress.get(inn, ("", 0))
        if status == "ok":
            continue
        if errors >= max_attempts:
            continue
        out.append(inn)
    return out


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Synaps: CSV с ИНН → CSV с полями карточек.")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="CSV со столбцом ИНН")
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Куда писать результат. По умолчанию рядом с входом: <имя>_synaps.csv",
    )
    p.add_argument("--limit", type=int, default=0, help="Сколько ещё не обработанных ИНН взять (0 = все)")
    p.add_argument("--batch", type=int, default=50, help="Сколько ИНН за одну сессию браузера")
    p.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="После стольких ошибок ИНН больше не берётся",
    )
    p.add_argument("--headed", action="store_true", help="Показать окно браузера")
    p.add_argument("--dry-run", action="store_true", help="Только посчитать очередь, браузер не открывать")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    load_dotenv(ROOT / ".env")
    args = parse_args(argv if argv is not None else sys.argv[1:])
    src = args.input if args.input.is_absolute() else (ROOT / args.input)
    if args.output is None:
        dst = src.with_name(f"{src.stem}_synaps.csv")
    else:
        dst = args.output if args.output.is_absolute() else (ROOT / args.output)

    if not src.is_file():
        raise SystemExit(f"Нет входного файла: {src}")

    inns = load_inns(src)
    progress = load_progress(dst)
    queue = pending_inns(inns, progress, max_attempts=max(1, args.max_attempts))
    if args.limit and args.limit > 0:
        queue = queue[: args.limit]

    done_ok = sum(1 for status, _ in progress.values() if status == "ok")
    print(f"Вход: {src}")
    print(f"Выход: {dst}")
    print(f"ИНН во входе: {len(inns)}; уже ok: {done_ok}; в этой очереди: {len(queue)}")
    if args.dry_run or not queue:
        if not queue:
            print("Очередь пустая — парсить нечего.")
        return

    from parser_synaps_browser import scrape_inns_sequentially

    sink = ResultCsv(dst)
    batch_size = max(1, args.batch)
    headless = not args.headed

    offset = 0
    while offset < len(queue):
        chunk = queue[offset : offset + batch_size]
        print(f"Пачка {offset + 1}–{offset + len(chunk)} из {len(queue)}")

        def _ok(inn: str, data: dict, _sink: ResultCsv = sink) -> None:
            _sink.append(_row_ok(inn, data))
            print(f"  записан ИНН {inn}")

        def _err(inn: str, exc: BaseException, _sink: ResultCsv = sink) -> None:
            _sink.append(_row_error(inn, exc))
            print(f"  ошибка ИНН {inn}: {exc}")

        scrape_inns_sequentially(
            chunk,
            headless=headless,
            save_dom_snapshots=False,
            on_each_result=_ok,
            on_each_error=_err,
        )
        offset += len(chunk)

    print(f"Готово. Результат: {dst}")


if __name__ == "__main__":
    main()
