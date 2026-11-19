import json
from pathlib import Path
from collections import Counter
from datetime import datetime, timezone

base = Path(__file__).resolve().parent


def load(p: Path):
    return json.loads(p.read_text(encoding="utf-8"))


all_inns = [str(x).strip() for x in load(base / "all_inns.json")["inns"]]
all1 = [str(x).strip() for x in load(base / "all_inns_part1.json")["inns"]]
all2 = [str(x).strip() for x in load(base / "all_inns_part2.json")["inns"]]
c1 = load(base / "companies_part1.json")
c2 = load(base / "companies_part2.json")
c0 = load(base / "companies.json") if (base / "companies.json").exists() else None

print("all:", len(all_inns), "part1 inns:", len(all1), "part2 inns:", len(all2))
print("part1+part2 unique:", len(set(all1) | set(all2)), "overlap:", len(set(all1) & set(all2)))


def analyze(name, payload):
    items = payload.get("items") or []
    st = Counter((i.get("status") or "").lower() for i in items)
    errs = Counter()
    for i in items:
        if (i.get("status") or "").lower() == "error":
            e = (i.get("error") or "")[:100]
            errs[e] += 1
    attempts = Counter(
        int(i.get("attempts") or 0)
        for i in items
        if (i.get("status") or "").lower() == "error"
    )
    print(
        f"\n{name}: total_items={len(items)} meta_ok={payload.get('ok_count')} "
        f"meta_err={payload.get('error_count')} updated={payload.get('updated_at')}"
    )
    print("  status:", dict(st))
    print("  error attempts:", dict(attempts))
    print("  top errors:")
    for e, n in errs.most_common(10):
        print(f"    {n}: {e!r}")
    closed_idxs = [
        idx
        for idx, i in enumerate(items)
        if "TargetClosed" in (i.get("error") or "")
        or "has been closed" in (i.get("error") or "")
    ]
    print(f"  TargetClosed count: {len(closed_idxs)}")
    if closed_idxs:
        a, b = closed_idxs[0], closed_idxs[-1]
        print(
            f"  first/last closed item index: {a}/{b} inns "
            f"{items[a]['inn']}/{items[b]['inn']}"
        )


analyze("part1", c1)
analyze("part2", c2)
if c0:
    analyze("companies.json", c0)

by: dict[str, dict] = {}
for payload in [c0, c1, c2]:
    if not payload:
        continue
    for i in payload.get("items") or []:
        inn = str(i.get("inn") or "").strip()
        if not inn:
            continue
        prev = by.get(inn)
        if not prev:
            by[inn] = i
            continue
        if (i.get("status") or "").lower() == "ok":
            by[inn] = i
        elif (prev.get("status") or "").lower() != "ok":
            if int(i.get("attempts") or 0) >= int(prev.get("attempts") or 0):
                by[inn] = i

ok = {inn for inn, i in by.items() if (i.get("status") or "").lower() == "ok"}
err = {inn for inn, i in by.items() if (i.get("status") or "").lower() == "error"}
print(f"\nMERGED unique processed: {len(by)} ok: {len(ok)} error: {len(err)}")

remaining = [inn for inn in all_inns if inn not in ok]
never = [inn for inn in all_inns if inn not in by]
print(
    f"remaining (not ok, incl errors + never tried): {len(remaining)}"
)
print(f"never tried: {len(never)} errors to retry: {len([i for i in remaining if i in err])}")

for label, inns, payload in [("part1", all1, c1), ("part2", all2, c2)]:
    items = {str(i["inn"]): i for i in payload["items"]}
    last_ok_idx = -1
    first_missing = None
    first_err = None
    for idx, inn in enumerate(inns):
        it = items.get(inn)
        if it and (it.get("status") or "").lower() == "ok":
            last_ok_idx = idx
        elif first_missing is None and not it:
            first_missing = (idx, inn)
        elif first_err is None and it and (it.get("status") or "").lower() == "error":
            first_err = (idx, inn, (it.get("error") or "")[:80])
    consec = 0
    for inn in inns:
        if inn in items:
            consec += 1
        else:
            break
    # find last stretch of ok before errors dominate near end of processed
    last_ok_in_items = None
    for i in payload["items"]:
        if (i.get("status") or "").lower() == "ok":
            last_ok_in_items = i.get("inn")
    print(
        f"\n{label} order: last_ok_idx={last_ok_idx}/{len(inns)} "
        f"first_missing={first_missing} first_err={first_err}"
    )
    print(f"  consecutive from start with any status: {consec}")
    print(f"  last ok inn in items list: {last_ok_in_items}")

# split remaining into 3 files
n = len(remaining)
parts = 3
sizes = [n // parts] * parts
for i in range(n % parts):
    sizes[i] += 1

out_files = []
offset = 0
created = datetime.now(timezone.utc).isoformat()
for part, size in enumerate(sizes, start=1):
    chunk = remaining[offset : offset + size]
    offset += size
    payload = {
        "source": str(base / "all_inns.json"),
        "created_at": created,
        "part": part,
        "parts": parts,
        "note": "remaining after companies_part1/part2: not-ok + never-tried; no duplicates across parts",
        "ok_excluded": len(ok),
        "count": len(chunk),
        "inns": chunk,
    }
    path = base / f"all_inns_remain_part{part}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    out_files.append((path, len(chunk), chunk[0] if chunk else None, chunk[-1] if chunk else None))
    print(f"\nWrote {path.name}: {len(chunk)} inns")

# verify no overlap and coverage
sets = []
for part in range(1, 4):
    inns = load(base / f"all_inns_remain_part{part}.json")["inns"]
    sets.append(set(inns))
print("\nOverlap 1&2:", len(sets[0] & sets[1]))
print("Overlap 1&3:", len(sets[0] & sets[2]))
print("Overlap 2&3:", len(sets[1] & sets[2]))
print("Union == remaining:", sets[0] | sets[1] | sets[2] == set(remaining))
print("Union size:", len(sets[0] | sets[1] | sets[2]))
print("Any remaining inn already ok?:", bool(set(remaining) & ok))
