"""Собрать тестовую очередь: 2 реальные + 2 несуществующие (валидный формат)."""
import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent

with open(ROOT / "input/queue_part1.json", encoding="utf-8") as f:
    q = json.load(f)

real = q[:2]
fake1 = dict(real[0]); fake1["ИНН"] = "1234567894"; fake1["Название компании"] = "ФЕЙК ООО 10 цифр"
fake2 = dict(real[1]); fake2["ИНН"] = "123456789047"; fake2["Название компании"] = "ФЕЙК ИП 12 цифр"

test = real + [fake1, fake2]
with open(ROOT / "input/queue_test.json", "w", encoding="utf-8") as f:
    json.dump(test, f, ensure_ascii=False, indent=1)

print("Тестовая очередь:")
for c in test:
    print(f"  {c['ИНН']}  {c.get('Название компании','')[:45]}")