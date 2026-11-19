"""
Парсер Синапс v4 — JSON -> Synaps -> JSON.
- Отдельный файл сессии для каждого процесса (--storage) — чтобы 3 потока не портили общий.
- Дозапуск: уже спарсенные (ok) пропускаются, ошибки повторяются.
- Сохраняет результат каждые 25 компаний.

Запуск (3 терминала):
  python parser_synaps_json.py --input input/companies_part1.json --output result_part1.json --storage ss_part1.json
  python parser_synaps_json.py --input input/companies_part2.json --output result_part2.json --storage ss_part2.json
  python parser_synaps_json.py --input input/companies_part3.json --output result_part3.json --storage ss_part3.json
"""

import json, os, re, sys, time
from pathlib import Path
from typing import Any
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")


def _inn(v: Any) -> str:
    return "".join(c for c in str(v or "") if c.isdigit())


def first_name(full: str) -> str:
    parts = full.strip().split()
    return parts[1].capitalize() if len(parts) >= 2 else ""


def extract_name(raw: str) -> str:
    if not raw or raw == "-":
        return ""
    s = re.sub(r"\d{2}\.\d{2}\.\d{4}", "", raw)
    s = re.sub(r"(Изменение|изменение|Назначена|назначена|руководитель)", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    if s.upper().startswith("ИП "):
        s = s[3:].strip()
    return first_name(s)


def short_name(full: str) -> str:
    s = full.strip()
    s = re.sub(r'^(?:ООО|АО|ЗАО|ПАО|ОАО|ИП|МУП|МКУ|ФГУП|ПК|АНО|НКО|СЗ|ТСЖ|ЖСК|КФ)\s*', '', s, flags=re.I)
    for ch in ('"', '\u00ab', '\u00bb', "'"):
        s = s.replace(ch, " ")
    return re.sub(r"\s+", " ", s).strip()


def fmt_val(key: str, val: Any) -> str:
    if val is None:
        return "-"
    if key == "P" and isinstance(val, int):
        return f"{val:,} руб.".replace(",", " ")
    if key == "T" and isinstance(val, list):
        try:
            from parser_synaps_browser import clean_email_as_on_page, email_sheet_line_key
            seen, out = set(), []
            for v in val:
                v = clean_email_as_on_page(str(v)); k = email_sheet_line_key(v)
                if k and k not in seen:
                    seen.add(k); out.append(v)
            return "\n".join(out) or "-"
        except Exception:
            return "\n".join(str(v) for v in val) or "-"
    if key == "R" and isinstance(val, dict):
        return "\n".join(f"{k} {v}".strip() for k, v in val.items() if k or v)
    if key == "AA" and isinstance(val, dict):
        return f"кол-во: {val.get('кол-во', '-')}, сумма: {val.get('сумма', '-')}"
    if key == "AB" and isinstance(val, dict):
        return " ".join(f"{k} {v}".strip() for k, v in val.items() if k or v)
    if key == "Y" and isinstance(val, dict):
        if len(val) == 1:
            k, v = next(iter(val.items()))
            return f"{v}: {k}"
        return json.dumps(val, ensure_ascii=False)
    return str(val).strip() if not isinstance(val, (dict, list)) else json.dumps(val, ensure_ascii=False)


def load_json(path: Path) -> list[dict]:
    if not path.is_file():
        raise FileNotFoundError(f"Нет файла: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("companies", "items", "data", "rows"):
            if k in data and isinstance(data[k], list):
                return data[k]
    raise ValueError("JSON должен быть списком")


SYNAPS_FIELDS = [
    ("O",  "Дата регистрации (Синапс)"),
    ("P",  "Уставной капитал"),
    ("R",  "ОКВЭД основной"),
    ("S",  "Юридический адрес"),
    ("T",  "Email"),
    ("Y",  "Тренд по выручке"),
    ("Z",  "Надёжность"),
    ("AA", "Долги (исп. производство)"),
    ("AB", "Генеральный директор (ФИО)"),
    ("AC", "Среднесписочная численность"),
]


def build_row(c: dict, data: Any, src_keep: list[str], *, status: str = "") -> dict:
    is_err = isinstance(data, BaseException)
    row = {k: str(c.get(k, "") or "") for k in src_keep}

    dir_name = ""
    if not is_err:
        dr = fmt_val("AB", data.get("AB", "-"))
        dir_name = extract_name(dr)
        if not dir_name:
            raw_name = str(c.get(src_keep[0], "")) if src_keep else ""
            if "ИП" in raw_name.upper():
                dir_name = extract_name(raw_name)
    row["Имя директора"] = dir_name or "-"

    phones = []
    if not is_err and isinstance(data, dict):
        q = data.get("Q", [])
        if isinstance(q, list):
            phones = [str(p).strip() for p in q if str(p).strip()]
        elif isinstance(q, str):
            phones = [p.strip() for p in q.split("\n") if p.strip()]
    for i, p in enumerate(phones):
        row[f"номер {i+1}"] = p

    cname = str(c.get(src_keep[0], "")) if src_keep else ""
    row["Короткое название"] = short_name(cname) or "?"

    if not is_err and isinstance(data, dict):
        for key, label in SYNAPS_FIELDS:
            row[label] = fmt_val(key, data.get(key))
    else:
        for _, label in SYNAPS_FIELDS:
            row[label] = "-"

    if status:
        row["_status"] = status
    else:
        row["_status"] = "error" if is_err else "ok"
    if is_err:
        row["_error"] = str(data)
    return row


def main():
    import argparse
    ap = argparse.ArgumentParser(description="Synaps parser v4")
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--input", type=str, required=True)
    ap.add_argument("--output", type=str, required=True)
    ap.add_argument("--storage", type=str, default="", help="Файл сессии (свой для каждого потока)")
    args = ap.parse_args()

    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    src = Path(args.input)
    out_path = Path(args.output)
    companies = load_json(src)
    print(f"Загружено: {len(companies)} из {src}")

    for c in companies:
        inn = ""
        for k, v in c.items():
            if k.lower().strip() in ("инн", "инn", "inn"):
                inn = _inn(v); break
        if not inn:
            for v in c.values():
                d = _inn(v)
                if len(d) in (10, 12):
                    inn = d; break
        c["_inn"] = inn
    companies = [c for c in companies if c.get("_inn")]
    print(f"С ИНН: {len(companies)}")

    all_keys = list(companies[0].keys()) if companies else []
    src_keys = [k for k in all_keys if not k.startswith("_")]
    skip_labels = ["имя директора", "как проверить телефон", "номера разбить", "название короткое", "назвавние"]
    src_keep = []
    for k in src_keys:
        kl = k.lower().strip()
        if any(s in kl for s in skip_labels):
            continue
        src_keep.append(k)
        if len(src_keep) >= 24:
            break

    lookup = {c["_inn"]: c for c in companies}

    # ── Загрузка прогресса ─────────────────────────────────────
    existing: dict[str, dict] = {}
    if out_path.is_file():
        try:
            with open(out_path, encoding="utf-8") as f:
                prev = json.load(f)
            # Ключ — ИНН из строки
            for r in prev:
                rinn = _inn(r.get("ИНН", ""))
                if rinn:
                    existing[rinn] = r
            print(f"Прогресс: {len(existing)} строк в {out_path}")
        except Exception as e:
            print(f"Не смог прочитать прогресс ({e}), начну с нуля")

    done = {i for i, r in existing.items()
            if r.get("_status") in ("ok", "not_found")}
    todo = [i for i in lookup if i not in done]

    nf_done = sum(1 for r in existing.values() if r.get("_status") == "not_found")
    ok_done = len(done) - nf_done
    print(f"Уже готово: ok={ok_done}  not_found={nf_done}")
    print(f"К парсингу: {len(todo)}")

    if not todo:
        print("Всё уже обработано!")
        return

    # ── Парсинг ────────────────────────────────────────────────
    import parser_synaps_browser as psb
    from parser_synaps_browser import scrape_inns_sequentially, SynapsOrgNotFound

    # Отдельная сессия для процесса
    storage_path = Path(args.storage) if args.storage else psb.DEFAULT_STORAGE
    if storage_path.is_absolute():
        pass
    else:
        storage_path = ROOT / storage_path
    # Удаляем битый файл
    if storage_path.exists():
        try:
            with open(storage_path, encoding="utf-8") as f:
                json.load(f)
        except Exception:
            print(f"Битый файл сессии {storage_path.name} — удаляю")
            storage_path.unlink()
    print(f"Сессия: {storage_path.name}")

    # Monkey-patch выбора организации
    _orig_pick = psb._pick_active_org_href
    def _gentle_pick(page, *, name_hint=None):
        try:
            results = page.evaluate("""() => {
                const out = [];
                for (const g of document.querySelectorAll(".org-pc-status.org-pcs-green")) {
                    let node = g;
                    for (let d = 0; d < 8 && node; d++) {
                        const link = node.querySelector("a.org-pcr-title");
                        if (link) { const h = (link.getAttribute("href")||"").trim(); if (h) out.push({href:h}); break; }
                        node = node.parentElement;
                    }
                }
                return out;
            }""")
            if results:
                h = results[0]["href"]
                return "https://synapsenet.ru" + h if h.startswith("/") else h
        except Exception:
            pass
        return _orig_pick(page, name_hint=name_hint)
    psb._pick_active_org_href = _gentle_pick

    # Счётчики
    state = {"ok": 0, "err": 0, "nf": 0, "last_save": 0}

    def save():
        rows = list(existing.values())
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        state["last_save"] = len(rows)

    def on_result(inn, data):
        existing[inn] = build_row(lookup[inn], data, src_keep)
        state["ok"] += 1
        n = sum(1 for r in existing.values() if r.get("_status") == "ok")
        if n % 25 == 0:
            save()
            print(f"  ok={n} not_found={state['nf']} err={state['err']} (сохранено)")

    def on_error(inn, exc):
        is_nf = isinstance(exc, SynapsOrgNotFound)
        existing[inn] = build_row(lookup[inn], exc, src_keep,
                                  status="not_found" if is_nf else "")
        if is_nf:
            state["nf"] += 1
        else:
            state["err"] += 1
        n = sum(1 for r in existing.values() if r.get("_status") == "ok")
        if n % 25 == 0:
            save()
            print(f"  ok={n} not_found={state['nf']} err={state['err']} (сохранено)")

    print(f"\nПарсинг {len(todo)} ИНН...")
    print("=" * 50)

    try:
        scrape_inns_sequentially(
            todo,
            headless=not args.headed,
            save_dom_snapshots=False,
            storage_path=storage_path,
            on_each_result=on_result,
            on_each_error=on_error,
        )
    finally:
        save()

    ok_total = sum(1 for r in existing.values() if r.get("_status") == "ok")
    err_total = sum(1 for r in existing.values() if r.get("_status") == "error")
    nf_total = sum(1 for r in existing.values() if r.get("_status") == "not_found")
    print(f"\n{'='*50}")
    print(f"Сохранено: {out_path}  ({out_path.stat().st_size/1024/1024:.1f} MB)")
    print(f"OK: {ok_total}  NOT_FOUND: {nf_total}  ERR: {err_total}  Всего: {len(existing)}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()