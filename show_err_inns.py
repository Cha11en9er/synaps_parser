"""Show 5 example INNs that weren't found in Synaps"""
import json, sys, re
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

for p in ["result_part1", "result_part2", "result_part3"]:
    with open(f"{p}.json", encoding="utf-8") as f:
        data = json.load(f)
    errs = [r for r in data if r.get("_status") == "error"]
    print(f"--- {p} ({len(errs)} errors) ---")
    for r in errs[:5]:
        name = list(r.values())[0]
        err = r.get("_error", "")
        m = re.search(r"'(\d+)'", err)
        inn = m.group(1) if m else "?"
        print(f'  {str(name)[:48]}')
        print(f'    ИНН={inn}')
    print()