"""
Все ИНН из 1_stage/members.json → all_inns.json (для полного прогона Синапса).

  python genpodryad_comp/2_stage/build_all_inns.py
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

STAGE_DIR = Path(__file__).resolve().parent
GP_DIR = STAGE_DIR.parent
MEMBERS = GP_DIR / "1_stage" / "members.json"
OUT = STAGE_DIR / "all_inns.json"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--members", type=Path, default=MEMBERS)
    p.add_argument("--out", type=Path, default=OUT)
    p.add_argument("--ul-only", action="store_true", help="Только ЮЛ (member_type id=1)")
    args = p.parse_args()

    raw = json.loads(args.members.read_text(encoding="utf-8"))
    members = raw.get("members") if isinstance(raw, dict) else raw
    if not isinstance(members, list):
        raise SystemExit(f"Нет members в {args.members}")

    inns: list[str] = []
    seen: set[str] = set()
    for m in members:
        if not isinstance(m, dict):
            continue
        if args.ul_only:
            r = m.get("raw") if isinstance(m.get("raw"), dict) else {}
            mt = r.get("member_type") if isinstance(r.get("member_type"), dict) else {}
            if mt.get("id") != 1 and str(mt.get("code") or "") != "1":
                title = str(m.get("member_type") or "").casefold()
                if not title.startswith("юл"):
                    continue
        inn = "".join(ch for ch in str(m.get("inn") or "") if ch.isdigit())
        if len(inn) not in (10, 12) or inn in seen:
            continue
        seen.add(inn)
        inns.append(inn)

    payload = {
        "source": str(args.members),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "ul_only": bool(args.ul_only),
        "count": len(inns),
        "inns": inns,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(f"INNs: {len(inns)} -> {args.out}")


if __name__ == "__main__":
    main()
