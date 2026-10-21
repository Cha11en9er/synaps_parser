# Допарсинг девелоперов и генподряда (Синапс)

Парсер карточек: `ritual_comp/2_stage/enrich_synaps.py` (по ИНН).
Уже готовые `ok` в `companies.json` пропускаются. Перезапуск безопасен.

**Не запускать** `developer_comp/2_stage/enrich_synaps.py` — он ищет по названию и сломает текущий `companies.json` (там записи по ИНН без поля `name`).

Запускать **без VPN**. Нужен `.env` в корне репозитория (`MAIL`/`EMAIL`, `PASS`/`PASSWORD`, `MAIN_URL`) и Playwright.

Состояние на момент гайда:

| База | Вход | Уже ok | Осталось |
| --- | --- | ---: | ---: |
| Девелоперы | `shortlist_inns.json` (4066 ИНН) | 3000 | 1066 |
| Генподряд | `all_inns.json` (134070 ИНН) | 2993 | 131077 |

---

## 1. Ноут: что закоммитить и запушить

`members.json` в git не входит. На ПК нужен `all_inns.json`.

```powershell
cd C:\repos\YouDo\18.04.26-synaps_parser
git add developer_comp/2_stage/shortlist_inns.json
git add genpodryad_comp/2_stage/all_inns.json
git add genpodryad_comp/2_stage/all_inns_part1.json
git add genpodryad_comp/2_stage/all_inns_part2.json
git add genpodryad_comp/2_stage/build_all_inns.py
git add genpodryad_comp/2_stage/split_inns.py
git add genpodryad_comp/2_stage/merge_companies.py
git add ritual_comp/GUIDE.md
git commit -m "входные ИНН для допарсинга девелоперов и генподряда"
git push
```

---

## 2. ПК: git pull и окружение

```powershell
cd C:\repos\YouDo\18.04.26-synaps_parser
git pull
.\.venv\Scripts\Activate.ps1
```

Проверка очереди без браузера:

```powershell
python ritual_comp/2_stage/enrich_synaps.py --input developer_comp/2_stage/shortlist_inns.json --output developer_comp/2_stage/companies.json --dry-run
python ritual_comp/2_stage/enrich_synaps.py --input genpodryad_comp/2_stage/all_inns.json --output genpodryad_comp/2_stage/companies.json --dry-run
```

---

## 3. Девелоперы (сначала)

```powershell
python ritual_comp/2_stage/enrich_synaps.py --input developer_comp/2_stage/shortlist_inns.json --output developer_comp/2_stage/companies.json
```

Тестовый прогон (5 штук, с окном):

```powershell
python ritual_comp/2_stage/enrich_synaps.py --input developer_comp/2_stage/shortlist_inns.json --output developer_comp/2_stage/companies.json --limit 5 --headed
```

---

## 4. Генподряд (после девелоперов)

Один процесс:

```powershell
python ritual_comp/2_stage/enrich_synaps.py --input genpodryad_comp/2_stage/all_inns.json --output genpodryad_comp/2_stage/companies.json
```

Два окна (разные выходные файлы, иначе будут драться за `companies.json`):

```powershell
python ritual_comp/2_stage/enrich_synaps.py --input genpodryad_comp/2_stage/all_inns_part1.json --output genpodryad_comp/2_stage/companies_part1.json
python ritual_comp/2_stage/enrich_synaps.py --input genpodryad_comp/2_stage/all_inns_part2.json --output genpodryad_comp/2_stage/companies_part2.json
```

После двух окон склеить:

```powershell
python genpodryad_comp/2_stage/merge_companies.py
```

---

## 5. Excel (когда прогон закончен)

```powershell
python developer_comp/2_stage/json_to_excel.py
python genpodryad_comp/2_stage/json_to_excel.py
```

---

## Если нужно пересобрать входы

```powershell
python genpodryad_comp/2_stage/build_all_inns.py
python genpodryad_comp/2_stage/split_inns.py
python developer_comp/2_stage/build_shortlist.py
```

`split_inns.py` берёт `all_inns.json` минус уже `ok` в `companies.json` и пишет `all_inns_part1.json` / `all_inns_part2.json`.
