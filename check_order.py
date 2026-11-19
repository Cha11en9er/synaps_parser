"""Проверка порядка обработки в part3"""
import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(__file__).resolve().parent


def _inn(v):
    return "".join(c for c in str(v or "") if c.isdigit())


part3 = json.load(open(ROOT / "input/companies_part3.json", encoding="utf-8"))
res3 = json.load(open(ROOT / "result_part3.json", encoding="utf-8"))

in_order = [_inn(c.get("ИНН", "")) for c in part3]
res_order = [_inn(r.get("ИНН", "")) for r in res3]

# 1. Совпадают ли первые 3050 результата с первыми 3050 входа?
print(f"Первые 10 входа:   {in_order[:10]}")
print(f"Первые 10 рез-та:  {res_order[:10]}")
print(f"Первые 3050 результата == первые 3050 входа: {res_order == in_order[:len(res_order)]}")

# 2. Где находится 25602815806
for target in ["25602815806", "267023136", "22802173100"]:
    idx = in_order.index(target) if target in in_order else -1
    print(f"  {target}: позиция в файле = {idx} (1-based {idx+1})")

# 3. Сколько ok в первых 3050
res_set = set(res_order)
ok_in_first = sum(1 for r in res3 if r.get("_status") == "ok")
print(f"\nВсего строк результата: {len(res3)}, из них ok: {ok_in_first}")
print(f"Последний ИНН результата: {res_order[-1]} (позиция в файле {in_order.index(res_order[-1])+1})")

# 4. Первый не-ok в порядке входа
ok_done = {_inn(r.get("ИНН", "")) for r in res3 if r.get("_status") == "ok"}
for i, inn in enumerate(in_order):
    if inn not in ok_done:
        print(f"\nПервый НЕ-ok в порядке файла: {inn} (позиция {i+1})")
        break

# 5. Проверка: результат покрывает непрерывный блок от начала?
covered = set(res_order)
first_gap = None
for i, inn in enumerate(in_order):
    if inn not in covered:
        first_gap = i
        break
print(f"Первый пропуск в покрытии: позиция {first_gap+1 if first_gap is not None else 'нет'}")
print(f"Результат = непрерывный блок первых {len(res_order)} ИНН: {res_order == in_order[:len(res_order)]}")