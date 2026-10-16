"""
Шортлист девелоперов из search_dom_dumps → shortlist.json / shortlist_inns.json.

Критерии карточки:
  - действующая
  - ОКВЭД из ядра (41.10/41.20*/68.10*/68.20*)
На название берём лучшую карточку (по приоритету ОКВЭД), ИНН дедуплицируем.
Лимит по умолчанию 3000 (на ночной прогон Синапса).

  python developer_comp/2_stage/build_shortlist.py
  python developer_comp/2_stage/build_shortlist.py --limit 3000
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STAGE_DIR = Path(__file__).resolve().parent
DUMP_DIR = STAGE_DIR / "search_dom_dumps"
OUT_JSON = STAGE_DIR / "shortlist.json"
OUT_INNS = STAGE_DIR / "shortlist_inns.json"

# ниже = лучше
OKVED_PRIORITY: list[tuple[str, int]] = [
    (r"^41\.20(\.|$)", 10),
    (r"^41\.2$", 15),
    (r"^41\.10$", 20),
    (r"^68\.10", 30),
    (r"^68\.1$", 35),
    (r"^68\.20", 40),
    (r"^68\.2$", 45),
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def okved_rank(code: str) -> int | None:
    code = (code or "").strip()
    if not code:
        return None
    for pat, rank in OKVED_PRIORITY:
        if re.search(pat, code):
            return rank
    return None


def is_active(status: str) -> bool:
    s = (status or "").casefold().replace("ё", "е")
    if "ликвидир" in s or "исключ" in s:
        return False
    return "действующ" in s


def card_score(card: dict[str, Any], *, position: int) -> tuple[int, int, int] | None:
    if not is_active(str(card.get("status") or "")):
        return None
    rank = okved_rank(str(card.get("okved") or ""))
    if rank is None:
        return None
    inn = "".join(ch for ch in str(card.get("inn") or "") if ch.isdigit())
    if len(inn) not in (10, 12):
        return None
    # меньше tuple — лучше
    return (rank, position, -len(str(card.get("title") or "")))


def collect_best_per_query(dump_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(dump_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        query = str(data.get("name") or "").strip()
        cards = data.get("cards") or []
        if not query or not isinstance(cards, list):
            continue
        best: tuple[tuple[int, int, int], dict[str, Any], int] | None = None
        for i, card in enumerate(cards):
            if not isinstance(card, dict):
                continue
            sc = card_score(card, position=i)
            if sc is None:
                continue
            if best is None or sc < best[0]:
                best = (sc, card, i)
        if best is None:
            continue
        _, card, pos = best
        inn = "".join(ch for ch in str(card.get("inn") or "") if ch.isdigit())
        rows.append(
            {
                "query_name": query,
                "title": card.get("title") or "",
                "inn": inn,
                "ogrn": card.get("ogrn") or "",
                "okved": card.get("okved") or "",
                "okved_name": card.get("okved_name") or "",
                "status": card.get("status") or "",
                "region": card.get("region") or "",
                "url": card.get("url") or "",
                "dump_file": path.name,
                "card_position": pos,
                "okved_rank": okved_rank(str(card.get("okved") or "")),
            }
        )
    # приоритет ОКВЭД, затем позиция в выдаче
    rows.sort(key=lambda r: (int(r.get("okved_rank") or 999), int(r.get("card_position") or 999)))
    return rows


def dedupe_inns(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        inn = row["inn"]
        if inn in seen:
            continue
        seen.add(inn)
        out.append(row)
        if limit > 0 and len(out) >= limit:
            break
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dump-dir", type=Path, default=DUMP_DIR)
    p.add_argument("--limit", type=int, default=3000, help="Сколько уникальных ИНН (0=все)")
    p.add_argument("--out", type=Path, default=OUT_JSON)
    p.add_argument("--out-inns", type=Path, default=OUT_INNS)
    args = p.parse_args()

    dump_dir = args.dump_dir if args.dump_dir.is_absolute() else (STAGE_DIR / args.dump_dir)
    best = collect_best_per_query(dump_dir)
    shortlist = dedupe_inns(best, limit=max(0, args.limit))
    inns = [r["inn"] for r in shortlist]

    by_okved: dict[str, int] = {}
    for r in shortlist:
        code = str(r.get("okved") or "")
        by_okved[code] = by_okved.get(code, 0) + 1

    payload = {
        "created_at": _now(),
        "source": str(dump_dir),
        "rules": "active + OKVED 41.10/41.20*/68.10*/68.20*; best card per query; unique INN; sorted by OKVED priority",
        "candidates_before_dedupe": len(best),
        "count": len(shortlist),
        "limit": args.limit,
        "okved_breakdown": dict(sorted(by_okved.items(), key=lambda kv: (-kv[1], kv[0]))),
        "items": shortlist,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    inns_payload = {
        "source": str(args.out),
        "created_at": _now(),
        "count": len(inns),
        "inns": inns,
        "companies": [
            {"inn": r["inn"], "name": r["title"], "query_name": r["query_name"], "url": r["url"], "okved": r["okved"]}
            for r in shortlist
        ],
    }
    args.out_inns.write_text(json.dumps(inns_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Kandidatov po zaprosam: {len(best)}")
    print(f"Shortlist unique INN: {len(shortlist)} -> {args.out}")
    print(f"INN for Synaps: {args.out_inns}")
    print("OKVED breakdown:", payload["okved_breakdown"])


if __name__ == "__main__":
    main()
