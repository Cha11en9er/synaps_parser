"""
Финальная выгрузка: собрать все данные из synaps_state.json в один файл.

Читает:
  synaps_state.json      — накопленное состояние (все ИНН)
  input/companies.json   — порядок компаний (как в исходной таблице)

Пишет:
  synaps_final.json      — итог: все компании + спарсенные поля + статус
  synaps_final.csv       — тот же результат в CSV (Excel-совместимый)

Запуск:
  python synaps_export.py
  python synaps_export.py --only-ok       # только успешно спарсенные
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEFAULT_STATE = ROOT / "synaps_state.json"
DEFAULT_SOURCE = ROOT / "input" / "companies.json"


def _inn(v) -> str:
    return "".join(c for c in str(v or "") if c.isdigit())


def main():
    ap = argparse.ArgumentParser(description="Сборка финального результата Синапс")
    ap.add_argument("--state", type=Path, default=DEFAULT_STATE)
    ap.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    ap.add_argument("--json-out", type=Path, default=ROOT / "synaps_final.json")
    ap.add_argument("--csv-out", type=Path, default=ROOT / "synaps_final.csv")
    ap.add_argument("--only-ok", action="store_true", help="Только ok (без not_found)")
    args = ap.parse_args()

    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    if not args.state.is_file():
        raise SystemExit(f"Нет состояния: {args.state}. Сначала запустите synaps_prepare.py")
    with open(args.state, encoding="utf-8") as f:
        raw = json.load(f)
    state = raw.get("items", {}) if isinstance(raw, dict) else {}

    # Порядок из источника
    order: list[str] = []
    if args.source.is_file():
        with open(args.source, encoding="utf-8") as f:
            src = json.load(f)
        for c in src:
            i = _inn(c.get("ИНН", ""))
            if i and i not in order:
                order.append(i)

    # Добавляем ИНН, которых нет в источнике (на всякий случай)
    for i in state:
        if i not in order:
            order.append(i)

    rows = []
    stats = Counter()
    for inn in order:
        r = state.get(inn)
        if not r:
            stats["pending"] += 1
            continue
        st = r.get("_status", "?")
        stats[st] += 1
        if args.only_ok and st != "ok":
            continue
        rows.append(r)

    # JSON
    with open(args.json_out, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=1)

    # CSV — объединяем все ключи
    all_keys: list[str] = []
    seen = set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k)
                all_keys.append(k)
    # Служебные в конец
    for k in ("_status", "_error"):
        if k in all_keys:
            all_keys.remove(k)
            all_keys.append(k)

    with open(args.csv_out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print("=" * 60)
    print("ФИНАЛЬНАЯ ВЫГРУЗКА")
    print("=" * 60)
    print(f"Всего в источнике:  {len(order)}")
    print(f"  ok:               {stats['ok']}")
    print(f"  not_found:        {stats['not_found']}")
    print(f"  error:            {stats['error']}")
    print(f"  pending:          {stats['pending']}")
    print(f"  без статуса:      {stats['?']}")
    print(f"\nЗаписано строк:     {len(rows)}")
    print(f"  JSON: {args.json_out}  ({args.json_out.stat().st_size/1024/1024:.1f} MB)")
    print(f"  CSV:  {args.csv_out}  ({args.csv_out.stat().st_size/1024/1024:.1f} MB)")
    print("=" * 60)


if __name__ == "__main__":
    main()