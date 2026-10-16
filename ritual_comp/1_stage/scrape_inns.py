"""
Сбор ИНН по ОКВЭД 96.03 с Rusprofile → inns.json.

  python ritual_comp/1_stage/scrape_inns.py
  python ritual_comp/1_stage/scrape_inns.py --limit-pages 1
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests

STAGE_DIR = Path(__file__).resolve().parent
DEFAULT_OUT = STAGE_DIR / "inns.json"
SOURCE_URL = "https://www.rusprofile.ru/codes/960300"
OKVED = "96.03"
PAGE_SIZE = 1000
REQUEST_PAUSE_SEC = 0.8

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

LABEL_RE = re.compile(
    r'okved-company-tools__label[^>]*>\s*Показаны организации\s*'
    r"(\d+)\s*(?:&ndash;|–|-)\s*(\d+)\s*из\s*(\d+)",
    re.I,
)
LIST_ELEMENT_RE = re.compile(
    r'<div class="list-element">\s*'
    r'<a href="(/id/\d+)" class="list-element__title">(.*?)</a>'
    r'.*?<div class="list-element__row-info">(.*?)</div>',
    re.S,
)
INN_RE = re.compile(r"ИНН:\s*(\d{10,12})")
OGRN_RE = re.compile(r"ОГРН:\s*(\d{13,15})")
PAGING_HREF_RE = re.compile(r'href="/codes/960300/(\d+)"')


def _ensure_utf8_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def page_url(page: int) -> str:
    if page <= 1:
        return SOURCE_URL
    return f"{SOURCE_URL}/{page}"


def fetch_page(session: requests.Session, page: int) -> str:
    url = page_url(page)
    resp = session.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def parse_reported_total(html_text: str) -> int | None:
    m = LABEL_RE.search(html_text)
    if not m:
        return None
    return int(m.group(3))


def parse_max_page(html_text: str) -> int:
    nums = [int(x) for x in PAGING_HREF_RE.findall(html_text)]
    return max(nums) if nums else 1


def parse_companies(html_text: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for href, name_raw, info in LIST_ELEMENT_RE.findall(html_text):
        inn_m = INN_RE.search(info)
        if not inn_m:
            continue
        inn = inn_m.group(1)
        ogrn_m = OGRN_RE.search(info)
        name = html.unescape(re.sub(r"<[^>]+>", "", name_raw)).strip()
        out.append(
            {
                "inn": inn,
                "name": name,
                "ogrn": ogrn_m.group(1) if ogrn_m else "",
                "rusprofile_url": urljoin(SOURCE_URL, href),
            }
        )
    return out


def scrape(
    *,
    limit_pages: int = 0,
    pause_sec: float = REQUEST_PAUSE_SEC,
) -> dict[str, Any]:
    session = requests.Session()
    first_html = fetch_page(session, 1)
    reported_total = parse_reported_total(first_html)
    max_page = parse_max_page(first_html)
    if reported_total:
        max_page = max(max_page, (reported_total + PAGE_SIZE - 1) // PAGE_SIZE)

    if limit_pages and limit_pages > 0:
        max_page = min(max_page, limit_pages)

    companies: list[dict[str, str]] = []
    seen: set[str] = set()

    for page in range(1, max_page + 1):
        html_text = first_html if page == 1 else fetch_page(session, page)
        batch = parse_companies(html_text)
        if not batch:
            raise RuntimeError(f"Страница {page}: не найдено компаний ({page_url(page)})")
        added = 0
        for row in batch:
            if row["inn"] in seen:
                continue
            seen.add(row["inn"])
            companies.append(row)
            added += 1
        label = parse_reported_total(html_text)
        print(
            f"стр. {page}/{max_page}: +{added} (уник. {len(companies)}; "
            f"на странице {len(batch)}; total на сайте {label or reported_total})"
        )
        if page < max_page and pause_sec > 0:
            time.sleep(pause_sec)

    inns = [c["inn"] for c in companies]
    return {
        "source_url": SOURCE_URL,
        "okved": OKVED,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "reported_total": reported_total,
        "count": len(inns),
        "inns": inns,
        "companies": companies,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Rusprofile ОКВЭД 96.03 → inns.json")
    p.add_argument("--output", type=Path, default=DEFAULT_OUT, help="Путь к JSON")
    p.add_argument(
        "--limit-pages",
        type=int,
        default=0,
        help="Сколько страниц скачать (0 = все)",
    )
    p.add_argument(
        "--pause",
        type=float,
        default=REQUEST_PAUSE_SEC,
        help="Пауза между страницами, сек",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    _ensure_utf8_stdout()
    args = parse_args(argv if argv is not None else sys.argv[1:])
    out = args.output if args.output.is_absolute() else (STAGE_DIR / args.output)

    print(f"Источник: {SOURCE_URL}")
    print(f"Выход: {out}")
    data = scrape(limit_pages=max(0, args.limit_pages), pause_sec=max(0.0, args.pause))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Готово: {data['count']} ИНН → {out}")
    if data.get("reported_total") and data["count"] < int(data["reported_total"]):
        print(
            f"Внимание: на сайте заявлено {data['reported_total']}, "
            f"собрано {data['count']}."
        )


if __name__ == "__main__":
    main()
