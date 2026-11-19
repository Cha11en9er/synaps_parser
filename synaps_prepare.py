"""
Этап 1 — подготовка очереди для парсеров Синапс.

Читает:
  input/companies.json         — полный список компаний (источник, из таблицы)
  result_part1/2/3.json        — результаты прошлых прогонов (если есть)
  synaps_state.json            — накопленное состояние (если есть)

Делает:
  1. Отсеивает уже готовые: ok (спарсено) и not_found (нет на Синапсе).
  2. Валидирует ИНН по длине (10/12 цифр) — битые помечает not_found БЕЗ браузера.
  3. Остаток (новые + ошибки сети/сессии) делит РОВНО на N частей.

Пишет:
  input/queue_part1.json … queue_partN.json   — только те, что нужно парсить
  synaps_state.json                            — накопленный статус по всем ИНН
  synaps_report.txt                            — человекочитаемая сводка

Запуск:
  python synaps_prepare.py
  python synaps_prepare.py --parts 3
  python synaps_prepare.py --retry-errors      # повторять и ошибочные (по умолчанию — да)
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEFAULT_SOURCE = ROOT / "input" / "companies.json"
DEFAULT_STATE = ROOT / "synaps_state.json"
QUEUE_DIR = ROOT / "input"


def _inn(v) -> str:
    return "".join(c for c in str(v or "") if c.isdigit())


def inn_length_ok(inn: str) -> bool:
    """ИНН физлица/ИП — 12 цифр, организации — 10."""
    return len(inn) in (10, 12)


def inn_checksum_ok(inn: str) -> bool:
    if len(inn) == 10:
        w = [2, 4, 10, 3, 5, 9, 4, 6, 8]
        return sum(int(inn[i]) * w[i] for i in range(9)) % 11 % 10 == int(inn[9])
    if len(inn) == 12:
        w1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
        w2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
        c1 = sum(int(inn[i]) * w1[i] for i in range(10)) % 11 % 10
        c2 = sum(int(inn[i]) * w2[i] for i in range(11)) % 11 % 10
        return c1 == int(inn[10]) and c2 == int(inn[11])
    return False


def load_source(path: Path) -> list[dict]:
    if not path.is_file():
        raise SystemExit(f"Нет источника: {path}")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("companies", "items", "data", "rows"):
            if k in data and isinstance(data[k], list):
                return data[k]
    raise SystemExit("Источник должен быть списком компаний")


def load_state(path: Path) -> dict[str, dict]:
    """Накопленное состояние: {ИНН: {row}}."""
    items: dict[str, dict] = {}
    if path.is_file():
        try:
            with open(path, encoding="utf-8") as f:
                raw = json.load(f)
            src = raw.get("items") if isinstance(raw, dict) else raw
            if isinstance(src, dict):
                items = {_inn(k): v for k, v in src.items() if _inn(k)}
            elif isinstance(src, list):
                for r in src:
                    i = _inn(r.get("ИНН", ""))
                    if i:
                        items[i] = r
        except Exception as e:
            print(f"  предупреждение: не смог прочитать {path.name}: {e}")
    return items


def load_shard(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, list):
            return [r for r in raw if isinstance(r, dict)]
    except Exception as e:
        print(f"  предупреждение: не смог прочитать {path.name}: {e}")
    return []


def main():
    ap = argparse.ArgumentParser(description="Подготовка очереди для парсеров Синапс")
    ap.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    ap.add_argument("--state", type=Path, default=DEFAULT_STATE)
    ap.add_argument("--parts", type=int, default=3)
    ap.add_argument("--outdir", type=Path, default=QUEUE_DIR)
    ap.add_argument("--no-retry-errors", action="store_true",
                    help="Не повторять ошибочные (сеть/сессия) — оставить как есть")
    args = ap.parse_args()

    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parts = max(1, args.parts)

    # ── 1. Источник ────────────────────────────────────────────
    source = load_source(args.source)
    print(f"Источник: {args.source} — {len(source)} записей")

    # Индекс источника по ИНН
    src_by_inn: dict[str, dict] = {}
    no_inn = 0
    for c in source:
        inn = _inn(c.get("ИНН", ""))
        if not inn:
            no_inn += 1
            continue
        src_by_inn.setdefault(inn, c)
    print(f"  уникальных ИНН: {len(src_by_inn)}  (без ИНН: {no_inn})")

    # ── 2. Накопленное состояние ───────────────────────────────
    state = load_state(args.state)
    print(f"Состояние {args.state.name}: {len(state)} записей")

    # Мержим шарды прошлых прогонов
    merged_from_shards = 0
    for i in range(1, parts + 1):
        shard = ROOT / f"result_part{i}.json"
        for r in load_shard(shard):
            inn = _inn(r.get("ИНН", ""))
            if not inn:
                continue
            prev = state.get(inn)
            # Предпочитаем ok/not_found перед error
            if prev and prev.get("_status") in ("ok", "not_found"):
                continue
            if r.get("_status") in ("ok", "not_found") or not prev:
                state[inn] = r
                merged_from_shards += 1
    if merged_from_shards:
        print(f"  подтянуто из result_part*.json: {merged_from_shards}")

    # ── 3. Определяем pending ──────────────────────────────────
    pending: list[dict] = []
    stats = Counter()
    invalid_marked = 0

    for inn, comp in src_by_inn.items():
        prev = state.get(inn)
        prev_status = (prev or {}).get("_status", "")

        # Уже готово — пропускаем
        if prev_status == "ok":
            stats["ok"] += 1
            continue
        if prev_status == "not_found":
            stats["not_found"] += 1
            continue

        # Валидация ИНН без браузера
        if not inn_length_ok(inn) or not inn_checksum_ok(inn):
            row = {k: str(v or "") for k, v in comp.items()}
            row["_status"] = "not_found"
            row["_error"] = f"некорректный ИНН ({len(inn)} цифр) — на Синапсе не ищем"
            state[inn] = row
            stats["not_found"] += 1
            invalid_marked += 1
            continue

        # Нужен парсинг: новый или error
        if prev_status == "error":
            stats["error_retry"] += 1
            if args.no_retry_errors:
                stats["error_skipped"] += 1
                continue
        else:
            stats["new"] += 1

        pending.append(comp)

    # ── 4. Сохраняем состояние ─────────────────────────────────
    ok_cnt = sum(1 for r in state.values() if r.get("_status") == "ok")
    nf_cnt = sum(1 for r in state.values() if r.get("_status") == "not_found")
    err_cnt = sum(1 for r in state.values() if r.get("_status") == "error")
    state_payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": str(args.source),
        "total": len(src_by_inn),
        "counts": {"ok": ok_cnt, "not_found": nf_cnt, "error": err_cnt,
                   "pending": len(pending)},
        "items": state,
    }
    tmp = args.state.with_suffix(args.state.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state_payload, f, ensure_ascii=False, indent=1)
    tmp.replace(args.state)

    # ── 5. Делим pending на части ──────────────────────────────
    # Равномерно: раздаём по кругу, чтобы части были максимально ровными
    buckets: list[list[dict]] = [[] for _ in range(parts)]
    for idx, comp in enumerate(pending):
        buckets[idx % parts].append(comp)

    args.outdir.mkdir(parents=True, exist_ok=True)
    for i, bucket in enumerate(buckets, start=1):
        out = args.outdir / f"queue_part{i}.json"
        tmp_q = out.with_suffix(out.suffix + ".tmp")
        with open(tmp_q, "w", encoding="utf-8") as f:
            json.dump(bucket, f, ensure_ascii=False, indent=1)
        tmp_q.replace(out)

    # ── 6. Отчёт ───────────────────────────────────────────────
    report_lines = [
        "=" * 60,
        "ПОДГОТОВКА ОЧЕРЕДИ СИНАПС",
        "=" * 60,
        f"Источник:            {args.source}",
        f"Всего компаний:      {len(src_by_inn)}",
        "",
        f"  ok (спарсено):      {stats['ok']}",
        f"  not_found (нет):    {stats['not_found']}  (в т.ч. битых ИНН: {invalid_marked})",
        f"  error (повтор):     {stats['error_retry']}",
        f"  новые:              {stats['new']}",
        "",
        f"К парсингу:          {len(pending)}",
        "",
        f"Разбито на {parts} частей:",
    ]
    for i, bucket in enumerate(buckets, start=1):
        report_lines.append(f"  queue_part{i}.json: {len(bucket)}")
    report_lines += [
        "",
        f"Состояние: {args.state}",
        f"  ok={ok_cnt}  not_found={nf_cnt}  error={err_cnt}",
        "=" * 60,
    ]
    report = "\n".join(report_lines)
    print("\n" + report)

    report_path = ROOT / "synaps_report.txt"
    report_path.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()