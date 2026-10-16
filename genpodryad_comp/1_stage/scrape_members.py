"""
Полная выгрузка реестра НОСТРОЙ через API → в итог только «Является членом».

Источник: https://reestr.nostroy.ru/sro/all/member/list
API: POST https://reestr.nostroy.ru/api/sro/all/member/list

- Обходит все страницы (resume через pages/ + progress.json)
- Между запросами пауза; при ошибке — несколько ретраев с backoff
- pages/page_XXXXX.json — сырой ответ страницы
- members.jsonl / members.json — только действующие члены

  python genpodryad_comp/1_stage/scrape_members.py
  python genpodryad_comp/1_stage/scrape_members.py --limit-pages 5
  python genpodryad_comp/1_stage/scrape_members.py --reset
  python genpodryad_comp/1_stage/scrape_members.py --rebuild-only
"""

from __future__ import annotations

import argparse
import json
import random
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

STAGE_DIR = Path(__file__).resolve().parent
PAGES_DIR = STAGE_DIR / "pages"
PROGRESS_PATH = STAGE_DIR / "progress.json"
MEMBERS_JSONL = STAGE_DIR / "members.jsonl"
MEMBERS_JSON = STAGE_DIR / "members.json"
SOURCE_URL = "https://reestr.nostroy.ru/sro/all/member/list"
API_URL = "https://reestr.nostroy.ru/api/sro/all/member/list"

ACTIVE_STATUS = "является членом"
DEFAULT_PAGE_SIZE = 100
REQUEST_TIMEOUT_MS = 15_000
# Пауза между успешными страницами (сек)
DELAY_MIN_SEC = 0.45
DELAY_MAX_SEC = 0.90
MAX_RETRIES = 4
RETRY_BASE_SEC = 1.5


def _ensure_utf8_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_status(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower().replace("ё", "е"))


def _is_active_member(item: dict[str, Any]) -> bool:
    status = item.get("member_status") if isinstance(item.get("member_status"), dict) else {}
    title = str(status.get("title") or "")
    sid = status.get("id")
    code = str(status.get("code") or "")
    if sid == 1 or code == "1":
        return True
    return _norm_status(title) == ACTIVE_STATUS


def _flatten_member(item: dict[str, Any]) -> dict[str, Any]:
    status = item.get("member_status") if isinstance(item.get("member_status"), dict) else {}
    mtype = item.get("member_type") if isinstance(item.get("member_type"), dict) else {}
    region = item.get("region_number") if isinstance(item.get("region_number"), dict) else {}
    sro = item.get("sro") if isinstance(item.get("sro"), dict) else {}
    return {
        "id": item.get("id"),
        "inn": str(item.get("inn") or "").strip(),
        "ogrn": str(item.get("ogrnip") or "").strip(),
        "registration_number": str(item.get("registration_number") or "").strip(),
        "inventory_number": str(item.get("inventory_number") or "").strip(),
        "full_name": str(item.get("full_description") or "").strip(),
        "short_name": str(item.get("short_description") or "").strip(),
        "director": str(item.get("director") or "").strip(),
        "registry_registration_date": str(
            item.get("registry_registration_date_time_string")
            or item.get("registry_registration_date")
            or ""
        ).strip(),
        "member_status": str(status.get("title") or "").strip(),
        "member_status_id": status.get("id"),
        "member_type": str(mtype.get("title") or "").strip(),
        "region": str(region.get("title") or "").strip(),
        "region_code": str(region.get("code") or "").strip(),
        "sro_registration_number": str(sro.get("registration_number") or "").strip(),
        "sro_name": str(sro.get("full_description") or "").strip(),
        "sro_id": sro.get("id"),
        "raw": item,
    }


def load_progress() -> dict[str, Any]:
    if not PROGRESS_PATH.is_file():
        return {
            "source_url": SOURCE_URL,
            "api_url": API_URL,
            "page_size": DEFAULT_PAGE_SIZE,
            "next_page": 1,
            "saved_pages": 0,
            "saved_members": 0,
            "seen_rows": 0,
            "total_count": None,
            "count_pages": None,
            "finished": False,
            "finish_reason": "",
            "updated_at": None,
        }
    return json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))


def save_progress(progress: dict[str, Any]) -> None:
    progress["updated_at"] = _now()
    tmp = PROGRESS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(progress, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(PROGRESS_PATH)


def page_path(page_no: int) -> Path:
    return PAGES_DIR / f"page_{page_no:05d}.json"


def polite_delay() -> None:
    time.sleep(random.uniform(DELAY_MIN_SEC, DELAY_MAX_SEC))


def rebuild_from_pages(page_size: int) -> dict[str, Any]:
    """Собрать members.* и progress из сохранённых pages/."""
    progress = load_progress()
    progress["page_size"] = page_size

    files = sorted(PAGES_DIR.glob("page_*.json")) if PAGES_DIR.is_dir() else []
    members: list[dict[str, Any]] = []
    seen_rows = 0
    saved_pages = 0
    max_page = 0
    total_count = progress.get("total_count")
    count_pages = progress.get("count_pages")

    for path in files:
        m = re.search(r"page_(\d+)\.json$", path.name)
        if not m:
            continue
        page_no = int(m.group(1))
        saved = json.loads(path.read_text(encoding="utf-8"))
        rows = saved.get("rows") or []
        seen_rows += len(rows)
        saved_pages += 1
        max_page = max(max_page, page_no)
        if saved.get("total_count") is not None:
            total_count = saved.get("total_count")
        if saved.get("count_pages") is not None:
            count_pages = saved.get("count_pages")
        for item in rows:
            if _is_active_member(item):
                members.append(_flatten_member(item))

    tmp_jsonl = MEMBERS_JSONL.with_suffix(".tmp")
    with tmp_jsonl.open("w", encoding="utf-8") as f:
        for row in members:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp_jsonl.replace(MEMBERS_JSONL)

    payload = {
        "source_url": SOURCE_URL,
        "built_at": _now(),
        "count": len(members),
        "filter": "member_status == Является членом",
        "members": members,
    }
    tmp_json = MEMBERS_JSON.with_suffix(".tmp")
    tmp_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_json.replace(MEMBERS_JSON)

    next_page = max_page + 1 if max_page else 1
    finished = False
    finish_reason = ""
    if count_pages is not None and max_page >= int(float(count_pages)):
        finished = True
        finish_reason = "Достигнут конец списка (countPages)"
    elif total_count is not None and seen_rows >= int(total_count) and max_page > 0:
        finished = True
        finish_reason = "Собраны все записи (seen_rows >= total_count)"

    progress.update(
        {
            "next_page": next_page,
            "saved_pages": saved_pages,
            "saved_members": len(members),
            "seen_rows": seen_rows,
            "total_count": total_count,
            "count_pages": count_pages,
            "finished": finished,
            "finish_reason": finish_reason,
        }
    )
    save_progress(progress)
    return progress


def fetch_page(request_context, *, page: int, page_size: int) -> dict[str, Any]:
    payload = {
        "filters": {},
        "page": page,
        "pageCount": str(page_size),
        "sortBy": {},
    }
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = request_context.post(
                API_URL,
                data=json.dumps(payload, ensure_ascii=False),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/plain, */*",
                    "Origin": "https://reestr.nostroy.ru",
                    "Referer": SOURCE_URL,
                },
                timeout=REQUEST_TIMEOUT_MS,
            )
            if resp.status in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {resp.status}")
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}: {(resp.text() or '')[:200]}")
            body = resp.json()
            if not isinstance(body, dict):
                raise RuntimeError("Ответ не JSON-объект")
            data = body.get("data") or {}
            rows = data.get("data")
            if not isinstance(rows, list):
                raise RuntimeError("Нет data.data")
            return {
                "payload": payload,
                "fetched_at": _now(),
                "page": int(data.get("page") or page),
                "count_pages": data.get("countPages"),
                "total_count": data.get("count"),
                "rows": rows,
                "raw_response_meta": {
                    "success": body.get("success"),
                    "message": body.get("message"),
                },
            }
        except Exception as exc:
            last_exc = exc
            if attempt >= MAX_RETRIES:
                break
            pause = RETRY_BASE_SEC * attempt + random.uniform(0.2, 0.8)
            print(f"  ошибка стр.{page} попытка {attempt}/{MAX_RETRIES}: {exc}; пауза {pause:.1f}с")
            time.sleep(pause)
    raise RuntimeError(f"Страница {page}: не удалось после {MAX_RETRIES} попыток ({last_exc})")


def launch_context(p, *, headless: bool):
    last_exc: Exception | None = None
    for channel in ("chrome", "msedge", None):
        kwargs: dict[str, Any] = {
            "headless": headless,
            "args": ["--disable-blink-features=AutomationControlled"],
            "ignore_default_args": ["--enable-automation"],
        }
        if channel:
            kwargs["channel"] = channel
        try:
            browser = p.chromium.launch(**kwargs)
            context = browser.new_context(locale="ru-RU", viewport={"width": 1280, "height": 900})
            return browser, context
        except Exception as exc:
            last_exc = exc
    raise RuntimeError(f"Не удалось запустить браузер: {last_exc}")


def scrape(
    *,
    page_size: int,
    limit_pages: int,
    headless: bool,
    rebuild_json_every: int,
) -> dict[str, Any]:
    PAGES_DIR.mkdir(parents=True, exist_ok=True)
    progress = rebuild_from_pages(page_size)
    print(
        f"Resume: next_page={progress.get('next_page')}, "
        f"pages={progress.get('saved_pages')}, "
        f"active_members={progress.get('saved_members')}, "
        f"seen_rows={progress.get('seen_rows')}/{progress.get('total_count')}, "
        f"finished={progress.get('finished')}"
    )
    if progress.get("finished"):
        print(f"Уже завершено: {progress.get('finish_reason')}. --reset для нового прогона.")
        return progress

    page_no = int(progress.get("next_page") or 1)
    pages_done = 0

    with Stealth().use_sync(sync_playwright()) as p:
        browser, context = launch_context(p, headless=headless)
        page = context.new_page()
        print(f"Сессия: {SOURCE_URL}")
        page.goto(SOURCE_URL, wait_until="domcontentloaded", timeout=90_000)
        polite_delay()

        try:
            while True:
                if limit_pages and pages_done >= limit_pages:
                    print(f"Достигнут --limit-pages={limit_pages}")
                    break

                out_file = page_path(page_no)
                if out_file.is_file():
                    print(f"стр. {page_no}: файл уже есть, пропускаю запрос")
                    page_no += 1
                    pages_done += 1
                    # периодически сверяем агрегаты
                    if pages_done % 50 == 0:
                        progress = rebuild_from_pages(page_size)
                        page_no = int(progress.get("next_page") or page_no)
                        if progress.get("finished"):
                            break
                    continue

                t0 = time.monotonic()
                result = fetch_page(context.request, page=page_no, page_size=page_size)
                elapsed = time.monotonic() - t0
                rows = result["rows"]
                active = [x for x in rows if _is_active_member(x)]

                page_payload = {
                    **result,
                    "active_count": len(active),
                    "elapsed_sec": round(elapsed, 3),
                }
                tmp = out_file.with_suffix(".tmp")
                tmp.write_text(json.dumps(page_payload, ensure_ascii=False), encoding="utf-8")
                tmp.replace(out_file)

                # Дописываем только активных с этой страницы
                if active:
                    with MEMBERS_JSONL.open("a", encoding="utf-8") as f:
                        for item in active:
                            f.write(json.dumps(_flatten_member(item), ensure_ascii=False) + "\n")

                progress["total_count"] = result.get("total_count")
                progress["count_pages"] = result.get("count_pages")
                progress["saved_pages"] = int(progress.get("saved_pages") or 0) + 1
                progress["saved_members"] = int(progress.get("saved_members") or 0) + len(active)
                progress["seen_rows"] = int(progress.get("seen_rows") or 0) + len(rows)
                progress["next_page"] = page_no + 1
                save_progress(progress)

                total = progress.get("total_count")
                count_pages = progress.get("count_pages")
                print(
                    f"стр. {page_no}/{int(float(count_pages)) if count_pages else '?'}: "
                    f"+{len(active)} членов / {len(rows)} строк "
                    f"(всего членов {progress['saved_members']}, "
                    f"строк {progress['seen_rows']}/{total}) "
                    f"за {elapsed:.2f}с"
                )

                if not rows:
                    progress["finished"] = True
                    progress["finish_reason"] = f"Пустая страница {page_no}"
                    save_progress(progress)
                    print("СТОП: пустая страница")
                    break

                page_no += 1
                pages_done += 1

                if count_pages is not None and page_no > int(float(count_pages)):
                    progress["finished"] = True
                    progress["finish_reason"] = "Достигнут конец списка countPages"
                    save_progress(progress)
                    print("СТОП: конец списка")
                    break

                if rebuild_json_every > 0 and pages_done % rebuild_json_every == 0:
                    # Полная пересборка из pages — без дублей при resume
                    progress = rebuild_from_pages(page_size)
                    page_no = int(progress.get("next_page") or page_no)
                    print(f"  сверка: members.json = {progress.get('saved_members')}")
                    if progress.get("finished"):
                        break

                polite_delay()

        finally:
            # Ctrl+C / закрытие браузера — не роняем уже сохранённый прогресс.
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    try:
        progress = rebuild_from_pages(page_size)
    except KeyboardInterrupt:
        print("Прервано при финальной сверке. pages/ уже сохранены — можно продолжить позже.")
        return load_progress()
    print(
        f"Итог: pages={progress.get('saved_pages')}, "
        f"active_members={progress.get('saved_members')}, "
        f"finished={progress.get('finished')} ({progress.get('finish_reason')})"
    )
    return progress


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="НОСТРОЙ API → members.json (только действующие)")
    p.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    p.add_argument("--limit-pages", type=int, default=0, help="Ограничить число новых страниц за запуск")
    p.add_argument("--headed", action="store_true")
    p.add_argument(
        "--rebuild-every",
        type=int,
        default=25,
        help="Каждые N страниц полностью пересобрать members.json из pages/",
    )
    p.add_argument("--reset", action="store_true")
    p.add_argument("--rebuild-only", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    _ensure_utf8_stdout()
    args = parse_args(argv if argv is not None else sys.argv[1:])
    page_size = max(1, min(200, int(args.page_size)))

    if args.reset:
        if PAGES_DIR.is_dir():
            shutil.rmtree(PAGES_DIR, ignore_errors=True)
        for path in (PROGRESS_PATH, MEMBERS_JSONL, MEMBERS_JSON):
            if path.is_file():
                path.unlink()
        print("Прогресс сброшен.")

    if args.rebuild_only:
        progress = rebuild_from_pages(page_size)
        print(f"members.json: {progress.get('saved_members')}")
        return

    # Если после прошлого прогона остановились на «чужом статусе» — снимаем флаг.
    if PROGRESS_PATH.is_file():
        old = load_progress()
        if old.get("stopped") or old.get("stop_reason"):
            old.pop("stopped", None)
            old.pop("stop_reason", None)
            old["finished"] = False
            old["finish_reason"] = ""
            save_progress(old)

    print(f"Каталог: {STAGE_DIR}")
    print(f"page_size={page_size}, delay={DELAY_MIN_SEC}-{DELAY_MAX_SEC}s, retries={MAX_RETRIES}")
    scrape(
        page_size=page_size,
        limit_pages=max(0, int(args.limit_pages)),
        headless=not bool(args.headed),
        rebuild_json_every=max(0, int(args.rebuild_every)),
    )


if __name__ == "__main__":
    main()
