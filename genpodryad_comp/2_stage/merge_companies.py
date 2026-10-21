"""
Склеить companies.json + companies_part1.json + companies_part2.json.

  python genpodryad_comp/2_stage/merge_companies.py
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STAGE_DIR = Path(__file__).resolve().parent
DEFAULT_BASE = STAGE_DIR / "companies.json"


def _inn(value: object) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def load_items(path: Path) -> list[dict[str, Any]]:
    if not path.is_file() or path.stat().st_size == 0:
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    items = raw.get("items") if isinstance(raw, dict) else None
    if not isinstance(items, list):
        return []
    return [x for x in items if isinstance(x, dict)]


def better(old: dict[str, Any] | None, new: dict[str, Any]) -> dict[str, Any]:
    if old is None:
        return new
    old_ok = (old.get("status") or "").strip().lower() == "ok"
    new_ok = (new.get("status") or "").strip().lower() == "ok"
    if new_ok and not old_ok:
        return new
    if old_ok and not new_ok:
        return old
    return new if int(new.get("attempts") or 0) >= int(old.get("attempts") or 0) else old


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--base", type=Path, default=DEFAULT_BASE)
    p.add_argument("--parts", nargs="*", type=Path, default=[])
    p.add_argument("--out", type=Path, default=DEFAULT_BASE)
    args = p.parse_args()
    part_paths = args.parts or [STAGE_DIR / "companies_part1.json", STAGE_DIR / "companies_part2.json"]

    by_inn: dict[str, dict[str, Any]] = {}
    sources = [args.base, *part_paths]
    for path in sources:
        for item in load_items(path):
            inn = _inn(item.get("inn"))
            if not inn:
                continue
            by_inn[inn] = better(by_inn.get(inn), item)

    items = list(by_inn.values())
    ok_count = sum(1 for x in items if (x.get("status") or "").lower() == "ok")
    error_count = sum(1 for x in items if (x.get("status") or "").lower() == "error")
    payload = {
        "source_inns": str(STAGE_DIR / "all_inns.json"),
        "merged_from": [str(x) for x in sources if x.is_file()],
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "inns_total": len(items),
        "ok_count": ok_count,
        "error_count": error_count,
        "items": items,
    }
    tmp = args.out.with_suffix(args.out.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(args.out)
    print(f"OK {ok_count} / {len(items)} errors={error_count} -> {args.out}")


if __name__ == "__main__":
    main()
