# Допарсинг девелоперов и генподряда (Синапс)

Парсер: `ritual_comp/2_stage/enrich_synaps.py` (по ИНН). Уже `ok` пропускаются.

**Не запускать** `developer_comp/2_stage/enrich_synaps.py` — поиск по названию, сломает текущий `companies.json`.

Без VPN. В корне `.env` (`MAIL`/`EMAIL`, `PASS`/`PASSWORD`, `MAIN_URL`) + Playwright.

## Где какие файлы

| Что | Путь | В git? |
| --- | --- | --- |
| Полный реестр НОСТРОЙ (145к, ~370 MB) | `genpodryad_comp/1_stage/members.json` | **нет** (слишком большой) |
| ИНН для Синапса (134 070) | `genpodryad_comp/2_stage/all_inns.json` | **да** — это и есть «файл на 150к» |
| Половинки для 2 потоков | `all_inns_part1.json` / `all_inns_part2.json` | да |
| Прогресс генподряда (уже ~2993) | `genpodryad_comp/2_stage/companies.json` | да |
| Девелоперы ИНН | `developer_comp/2_stage/shortlist_inns.json` | да |
| Прогресс девелоперов (уже 3000) | `developer_comp/2_stage/companies.json` | да |

На ПК в `1_stage` будет только `scrape_members.py` — так и задумано. Для парсинга Синапса нужен **`2_stage/all_inns.json`**, не `members.json`.

Состояние очереди:

| База | Вход | Уже ok | Осталось |
| --- | --- | ---: | ---: |
| Девелоперы | `shortlist_inns.json` (~4066) | 3000 | ~1066 |
| Генподряд | `all_inns_part1/2` | 2993 (в `companies.json`) | ~131 077 (пополам) |

---

## 1. Ноут — запушить (если parts обновились)

```powershell
cd C:\repos\YouDo\18.04.26-synaps_parser

git add GUIDE.md
git add genpodryad_comp/2_stage/all_inns.json
git add genpodryad_comp/2_stage/all_inns_part1.json
git add genpodryad_comp/2_stage/all_inns_part2.json
git add genpodryad_comp/2_stage/companies.json
git add genpodryad_comp/2_stage/split_inns.py
git add genpodryad_comp/2_stage/merge_companies.py
git add genpodryad_comp/2_stage/build_all_inns.py
git add developer_comp/2_stage/shortlist_inns.json
git add developer_comp/2_stage/companies.json

git status
git commit -m "входы и прогресс для допарсинга девелоперов и генподряда (2 потока)"
git push origin main
```

---

## 2. ПК — принять

Путь на твоём ПК:

```powershell
cd C:\repos\YouDo\done\20.04.26-synaps_parser
git pull origin main
.\.venv\Scripts\Activate.ps1
```

Проверка, что 134к на месте:

```powershell
python -c "import json; d=json.load(open(r'genpodryad_comp/2_stage/all_inns.json',encoding='utf-8')); print(d['count'])"
```

Должно быть `134070`.

Dry-run:

```powershell
python ritual_comp/2_stage/enrich_synaps.py --input developer_comp/2_stage/shortlist_inns.json --output developer_comp/2_stage/companies.json --dry-run
python ritual_comp/2_stage/enrich_synaps.py --input genpodryad_comp/2_stage/all_inns_part1.json --output genpodryad_comp/2_stage/companies_part1.json --dry-run
python ritual_comp/2_stage/enrich_synaps.py --input genpodryad_comp/2_stage/all_inns_part2.json --output genpodryad_comp/2_stage/companies_part2.json --dry-run
```

---

## 3. Три терминала на ПК

**Терминал 1 — девелоперы (добить остаток):**

```powershell
C:\repos\YouDo\done\20.04.26-synaps_parser\.venv\Scripts\python.exe C:\repos\YouDo\done\20.04.26-synaps_parser\ritual_comp\2_stage\enrich_synaps.py --input C:\repos\YouDo\done\20.04.26-synaps_parser\developer_comp\2_stage\shortlist_inns.json --output C:\repos\YouDo\done\20.04.26-synaps_parser\developer_comp\2_stage\companies.json
```

**Терминал 2 — генподряд часть 1:**

```powershell
C:\repos\YouDo\done\20.04.26-synaps_parser\.venv\Scripts\python.exe C:\repos\YouDo\done\20.04.26-synaps_parser\ritual_comp\2_stage\enrich_synaps.py --input C:\repos\YouDo\done\20.04.26-synaps_parser\genpodryad_comp\2_stage\all_inns_part1.json --output C:\repos\YouDo\done\20.04.26-synaps_parser\genpodryad_comp\2_stage\companies_part1.json
```

**Терминал 3 — генподряд часть 2:**

```powershell
C:\repos\YouDo\done\20.04.26-synaps_parser\.venv\Scripts\python.exe C:\repos\YouDo\done\20.04.26-synaps_parser\ritual_comp\2_stage\enrich_synaps.py --input C:\repos\YouDo\done\20.04.26-synaps_parser\genpodryad_comp\2_stage\all_inns_part2.json --output C:\repos\YouDo\done\20.04.26-synaps_parser\genpodryad_comp\2_stage\companies_part2.json
```

Уже спарсенные ~2993 лежат в `companies.json`. После двух потоков склеить:

```powershell
python genpodryad_comp/2_stage/merge_companies.py
```

(склеит `companies.json` + `companies_part1.json` + `companies_part2.json`)

---

## Если parts нужно пересобрать на ноуте

```powershell
python genpodryad_comp/2_stage/split_inns.py
```

`members.json` на ПК для Синапса не нужен. Если всё же нужен полный реестр — копируй вручную (USB/облако), в git он не входит из‑за размера.
