"""Pick 10 test companies: 5 good + 5 error-prone"""
import json

with open("input/companies.json", encoding="utf-8") as f:
    data = json.load(f)

by_inn = {}
for c in data:
    inn = "".join(ch for ch in str(c.get("ИНН", "") or "") if ch.isdigit())
    if inn:
        by_inn[inn] = c

# Companies that worked before
good = ["7128007958", "7100046795", "7126017171", "7100075436", "7100027369"]
# Companies that errored before
bad = ["711504253375", "222333597515", "710601848954", "712306433160", "710307725228"]

print("=== 5 normal ===")
for inn in good:
    c = by_inn.get(inn)
    print(f"  {inn}: {c.get('Название компании', '?')[:60] if c else 'NOT FOUND'}")

print("\n=== 5 with errors ===")
for inn in bad:
    c = by_inn.get(inn)
    print(f"  {inn}: {c.get('Название компании', '?')[:60] if c else 'NOT FOUND'}")

# Save test subset
test_rows = []
for inn in good + bad:
    c = by_inn.get(inn)
    if c:
        test_rows.append(c)

with open("input/test_10.json", "w", encoding="utf-8") as f:
    json.dump(test_rows, f, ensure_ascii=False, indent=2)

print(f"\nСохранено {len(test_rows)} строк в input/test_10.json")