"""
Оставшиеся ИНН генподряда (all_inns минус уже ok в companies.json) → part1/part2.

  python genpodryad_comp/2_stage/split_inns.py
  python genpodryad_comp/2_stage/split_inns.py --parts 2
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

STAGE_DIR = Path(__file__).resolve().parent
DEFAULT_INNS = STAGE_DIR / "all_inns.json"
DEFAULT_PROGRESS = STAGE_DIR / "companies.json"


def _inn(value: object) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def load_inns(path: Path) -> list[str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        values = raw
    elif isinstance(raw, dict) and isinstance(raw.get("inns"), list):
        values = raw["inns"]
    else:
        raise SystemExit(f"Нет inns в {path}")
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        inn = _inn(value)
        if len(inn) not in (10, 12) or inn in seen:
            continue
        seen.add(inn)
        out.append(inn)
    return out


def load_ok(path: Path) -> set[str]:
    if not path.is_file() or path.stat().st_size == 0:
        return set()
    raw = json.loads(path.read_text(encoding="utf-8"))
    items = raw.get("items") if isinstance(raw, dict) else None
    if not isinstance(items, list):
        return set()
    ok: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        if (item.get("status") or "").strip().lower() != "ok":
            continue
        inn = _inn(item.get("inn"))
        if inn:
            ok.add(inn)
    return ok


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, default=DEFAULT_INNS)
    p.add_argument("--progress", type=Path, default=DEFAULT_PROGRESS)
    p.add_argument("--parts", type=int, default=2)
    p.add_argument("--outdir", type=Path, default=STAGE_DIR)
    args = p.parse_args()
    parts = max(2, int(args.parts))

    inns = load_inns(args.input)
    done = load_ok(args.progress)
    rest = [inn for inn in inns if inn not in done]
    n = len(rest)
    size = (n + parts - 1) // parts

    print(f"Всего ИНН: {len(inns)}; уже ok: {len(done)}; осталось: {n}; частей: {parts}")
    for i in range(parts):
        chunk = rest[i * size : (i + 1) * size]
        out = args.outdir / f"all_inns_part{i + 1}.json"
        payload = {
            "source": str(args.input),
            "progress": str(args.progress),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "part": i + 1,
            "parts": parts,
            "count": len(chunk),
            "inns": chunk,
        }
        out.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        print(f"  part{i + 1}: {len(chunk)} -> {out}")


if __name__ == "__main__":
    main()
