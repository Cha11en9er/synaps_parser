"""
Шортлист генподрядчиков из members.json (НОСТРОЙ).

В реестре НЕТ выручки/штата — «крупных» по деньгам не видно.
Прокси-критерии:
  1) только ЮЛ (не ИП)
  2) приоритет регионов: Москва → СПб → МО → крупные субъекты
  3) раньше дата вступления в реестр (дольше на рынке)
  4) крупнее СРО по числу членов (как слабый сигнал)

  python genpodryad_comp/2_stage/build_shortlist.py
  python genpodryad_comp/2_stage/build_shortlist.py --limit 3000
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STAGE_DIR = Path(__file__).resolve().parent
GP_DIR = STAGE_DIR.parent
MEMBERS = GP_DIR / "1_stage" / "members.json"
OUT_JSON = STAGE_DIR / "shortlist.json"
OUT_INNS = STAGE_DIR / "shortlist_inns.json"

# region_code → вес (меньше = выше приоритет)
REGION_WEIGHT: dict[str, int] = {
    "77": 1,   # Москва
    "78": 2,   # СПб
    "50": 3,   # Московская обл.
    "47": 4,   # Ленинградская
    "23": 5,   # Краснодарский
    "66": 6,   # Свердловская
    "54": 7,   # Новосибирская
    "16": 8,   # Татарстан
    "52": 9,   # Нижегородская
    "61": 10,  # Ростовская
    "74": 11,  # Челябинская
    "63": 12,  # Самарская
    "02": 13,  # Башкортостан
    "59": 14,  # Пермский
    "36": 15,  # Воронежская
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_reg_date(value: str) -> datetime | None:
    s = (value or "").strip()
    if not s:
        return None
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
        try:
            if fmt.endswith("%z") and s.endswith("+03:00"):
                return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z")
            if "T" in s and fmt == "%Y-%m-%d":
                return datetime.strptime(s[:10], fmt)
            return datetime.strptime(s[:19] if " " in s and fmt.startswith("%d") else s, fmt)
        except ValueError:
            continue
    # ISO with tz
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_ul(member: dict[str, Any]) -> bool:
    raw = member.get("raw") if isinstance(member.get("raw"), dict) else {}
    mtype = raw.get("member_type") if isinstance(raw.get("member_type"), dict) else {}
    if mtype.get("id") == 1 or str(mtype.get("code") or "") == "1":
        return True
    title = str(member.get("member_type") or mtype.get("title") or "").casefold()
    return title.startswith("юл") or title == "юл"


# Квоты на 3000 (доля крупных рынков; остальное — прочие приоритетные + остальные)
REGION_QUOTAS_3000: list[tuple[str, int]] = [
    ("77", 900),   # Москва
    ("78", 400),   # СПб
    ("50", 350),   # МО
    ("47", 150),   # ЛО
    ("23", 200),   # Краснодарский
    ("66", 150),   # Свердловская
    ("54", 120),   # Новосибирская
    ("16", 120),   # Татарстан
    ("52", 100),   # Нижегородская
    ("61", 100),   # Ростовская
    ("74", 80),    # Челябинская
    ("63", 80),    # Самарская
    ("02", 80),    # Башкортостан
    ("59", 70),    # Пермский
    ("36", 70),    # Воронежская
]


def build(members: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    sro_size = Counter(str(m.get("sro_id") or "") for m in members if m.get("sro_id") is not None)
    by_region: dict[str, list[tuple[tuple[Any, ...], dict[str, Any]]]] = {}
    seen_inn: set[str] = set()

    for m in members:
        if not _is_ul(m):
            continue
        inn = "".join(ch for ch in str(m.get("inn") or "") if ch.isdigit())
        if len(inn) not in (10, 12) or inn in seen_inn:
            continue
        seen_inn.add(inn)

        region_code = str(m.get("region_code") or "")
        if not region_code:
            raw = m.get("raw") if isinstance(m.get("raw"), dict) else {}
            rn = raw.get("region_number") if isinstance(raw.get("region_number"), dict) else {}
            region_code = str(rn.get("code") or rn.get("id") or "")
        if region_code.isdigit():
            region_code = region_code.zfill(2)

        reg_dt = _parse_reg_date(str(m.get("registry_registration_date") or ""))
        reg_key = reg_dt.timestamp() if reg_dt else 9e12
        sro_id = str(m.get("sro_id") or "")
        sro_rank = -sro_size.get(sro_id, 0)

        row = {
            "inn": inn,
            "ogrn": m.get("ogrn") or "",
            "full_name": m.get("full_name") or "",
            "short_name": m.get("short_name") or "",
            "region": m.get("region") or "",
            "region_code": region_code,
            "sro_name": m.get("sro_name") or "",
            "sro_id": m.get("sro_id"),
            "sro_members": sro_size.get(sro_id, 0),
            "registry_registration_date": m.get("registry_registration_date") or "",
            "member_type": m.get("member_type") or "",
        }
        # внутри региона: раньше в реестре, крупнее СРО
        by_region.setdefault(region_code, []).append(((reg_key, sro_rank, inn), row))

    for code in by_region:
        by_region[code].sort(key=lambda x: x[0])

    if limit <= 0:
        limit = 10**9

    # масштабируем квоты под limit
    base = sum(q for _, q in REGION_QUOTAS_3000) or 1
    quotas = [(code, max(1, int(round(q * limit / base)))) for code, q in REGION_QUOTAS_3000]

    out: list[dict[str, Any]] = []
    used: set[str] = set()
    for code, q in quotas:
        pool = by_region.get(code) or []
        take = pool[:q]
        for _, row in take:
            out.append(row)
            used.add(row["inn"])
            if len(out) >= limit:
                return out

    # добиваем из остальных регионов (по весу региона + дате)
    rest: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    for code, pool in by_region.items():
        if code in dict(REGION_QUOTAS_3000):
            # уже частично взяли — остаток пула
            already = sum(1 for r in out if r.get("region_code") == code)
            pool = pool[already:]
        w = REGION_WEIGHT.get(code, 50)
        for key, row in pool:
            rest.append(((w, key[0], key[1], row["inn"]), row))
    rest.sort(key=lambda x: x[0])
    for _, row in rest:
        if row["inn"] in used:
            continue
        out.append(row)
        used.add(row["inn"])
        if len(out) >= limit:
            break
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--members", type=Path, default=MEMBERS)
    p.add_argument("--limit", type=int, default=3000)
    p.add_argument("--out", type=Path, default=OUT_JSON)
    p.add_argument("--out-inns", type=Path, default=OUT_INNS)
    args = p.parse_args()

    members_path = args.members if args.members.is_absolute() else (GP_DIR / args.members)
    raw = json.loads(members_path.read_text(encoding="utf-8"))
    members = raw.get("members") if isinstance(raw, dict) else raw
    if not isinstance(members, list):
        raise SystemExit(f"Нет списка members в {members_path}")

    shortlist = build(members, limit=max(0, args.limit))
    inns = [r["inn"] for r in shortlist]

    by_region: Counter[str] = Counter(r.get("region") or "?" for r in shortlist)
    payload = {
        "created_at": _now(),
        "source": str(members_path),
        "rules": "ЮЛ only; region priority (MSK/SPb/MO/…); earlier registry date; larger SRO",
        "note": "НОСТРОЙ не даёт выручку — это прокси «крупности», не рейтинг по обороту",
        "members_total": len(members),
        "count": len(shortlist),
        "limit": args.limit,
        "region_breakdown": dict(by_region.most_common()),
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
            {"inn": r["inn"], "name": r.get("short_name") or r.get("full_name") or ""}
            for r in shortlist
        ],
    }
    args.out_inns.write_text(json.dumps(inns_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Members total: {len(members)}")
    print(f"Shortlist: {len(shortlist)} -> {args.out}")
    print(f"INN for Synaps: {args.out_inns}")
    print("Top regions:", by_region.most_common(8))


if __name__ == "__main__":
    main()
