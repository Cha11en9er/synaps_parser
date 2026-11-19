"""Split companies.json into 3 parts"""
import json, sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SRC = Path("input/companies.json")
DST = Path("input")

with open(SRC, "r", encoding="utf-8") as f:
    data = json.load(f)

total = len(data)
print(f"Total: {total}")

part_size = total // 3 + (1 if total % 3 else 0)
for i in range(3):
    start = i * part_size
    end = min(start + part_size, total)
    chunk = data[start:end]
    path = DST / f"companies_part{i+1}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(chunk, f, ensure_ascii=False, indent=2)
    print(f"  part {i+1}: {len(chunk)} rows -> {path.name}")

print("\nDone!")
for i in range(3):
    p = DST / f"companies_part{i+1}.json"
    print(f"  {p.name} ({p.stat().st_size/1024/1024:.1f} MB)")