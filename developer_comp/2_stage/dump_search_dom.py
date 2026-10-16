"""
Выгрузка результатов поиска Синапса по названиям девелоперов.

Берёт случайную выборку из 1_stage/names.json (по умолчанию 500),
открывает https://synapsenet.ru/organizacii?query=<название>
и сохраняет компактный JSON (карточки: ИНН/ОГРН/ОКВЭД/ссылка), без HTML.

Запускать БЕЗ VPN. Нужен .env (MAIL/EMAIL, PASS/PASSWORD, MAIN_URL).

  python developer_comp/2_stage/dump_search_dom.py --sample-only
  python developer_comp/2_stage/dump_search_dom.py --limit 5 --headed
  python developer_comp/2_stage/dump_search_dom.py --headed          # выборка 500
  python developer_comp/2_stage/dump_search_dom.py --all --headed    # все имена из 1_stage
  python developer_comp/2_stage/dump_search_dom.py --migrate-html
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

STAGE_DIR = Path(__file__).resolve().parent
DEV_DIR = STAGE_DIR.parent
REPO_ROOT = DEV_DIR.parent
DEFAULT_INPUT = DEV_DIR / "1_stage" / "names.json"
DEFAULT_SAMPLE = STAGE_DIR / "sample_500.json"
DEFAULT_DUMP_DIR = STAGE_DIR / "search_dom_dumps"
DEFAULT_PROGRESS = STAGE_DIR / "dump_search_progress.json"
DEFAULT_OKVED_FREQ = STAGE_DIR / "okved_freq.json"
QUERY_URL = "https://synapsenet.ru/organizacii?query={query}"
SYNAPSE_ORIGIN = "https://synapsenet.ru"

EXTRACT_CARDS_JS = """() => {
  const abs = (href) => {
    if (!href) return "";
    try { return new URL(href, location.origin).href; } catch (e) { return href; }
  };
  const cards = [...document.querySelectorAll(".org-partition-card")].map((card) => {
    const titleA = card.querySelector("a.org-pcr-title");
    const goA = card.querySelector("a.org-pcl-go-but");
    const ogrnBlocks = [...card.querySelectorAll(".org-pcl-ogrn")];
    const getByLabel = (label) => {
      for (const el of ogrnBlocks) {
        const t = (el.textContent || "").replace(/\\s+/g, " ");
        if (t.includes(label)) {
          const sp = el.querySelector(".copy-script");
          return sp ? sp.textContent.trim() : "";
        }
      }
      return "";
    };
    const sub = card.querySelector(".org-pcr-sutitle");
    const okvedA = sub ? sub.querySelector("a") : null;
    const okved = okvedA ? okvedA.textContent.trim() : "";
    let okved_name = "";
    if (sub) {
      okved_name = sub.textContent.replace(/\\s+/g, " ").trim();
      if (okved && okved_name.startsWith(okved)) {
        okved_name = okved_name.slice(okved.length).trim();
      }
    }
    const statusEl = card.querySelector(".org-pc-status");
    let region = "";
    let reg_date = "";
    let period = "";
    for (const d of card.querySelectorAll(".org-pcr-desc")) {
      const parts = [...d.querySelectorAll(":scope > div")]
        .map((x) => (x.textContent || "").replace(/\\s+/g, " ").trim())
        .filter(Boolean);
      // icon div often empty-text; label then value
      const label = (parts.find((p) =>
        /регион|дата|период|оквэд/i.test(p)
      ) || "").toLowerCase();
      const value = parts.length ? parts[parts.length - 1] : "";
      if (label.includes("регион")) region = value;
      else if (label.includes("дата")) reg_date = value;
      else if (label.includes("период")) period = value;
    }
    const href = abs((titleA && titleA.getAttribute("href")) || (goA && goA.getAttribute("href")) || "");
    return {
      title: titleA ? titleA.textContent.replace(/\\s+/g, " ").trim() : "",
      url: href,
      ogrn: getByLabel("ОГРН"),
      inn: getByLabel("ИНН"),
      kpp: getByLabel("КПП"),
      okved,
      okved_name,
      status: statusEl ? statusEl.textContent.replace(/\\s+/g, " ").trim() : "",
      region,
      reg_date,
      period,
    };
  });
  const countEl = document.querySelector(".orgs-count");
  return {
    total_text: countEl ? countEl.textContent.replace(/\\s+/g, " ").trim() : "",
    cards,
  };
}"""

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _ensure_utf8_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_name(value: str) -> str:
    return " ".join((value or "").strip().split())


def _slug(name: str, idx: int) -> str:
    base = re.sub(r"[^\w\-]+", "_", name, flags=re.U).strip("_")
    base = re.sub(r"_+", "_", base)[:80] or "name"
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    return f"{idx:04d}_{base}_{digest}"


def _abs_url(href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    return urljoin(SYNAPSE_ORIGIN, href)


def load_names(path: Path) -> list[str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        values = raw
    elif isinstance(raw, dict) and isinstance(raw.get("names"), list):
        values = raw["names"]
    elif isinstance(raw, dict) and isinstance(raw.get("companies"), list):
        values = [c.get("name") for c in raw["companies"] if isinstance(c, dict)]
    else:
        raise RuntimeError(f"В {path} нет списка names")
    out: list[str] = []
    seen: set[str] = set()
    for v in values:
        name = _norm_name(str(v or ""))
        if not name:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def build_or_load_sample(
    names: list[str],
    *,
    sample_path: Path,
    size: int,
    seed: int,
    rebuild: bool,
) -> list[str]:
    if sample_path.is_file() and not rebuild:
        data = json.loads(sample_path.read_text(encoding="utf-8"))
        sample = [_norm_name(x) for x in (data.get("names") or []) if _norm_name(str(x))]
        if sample:
            print(f"Выборка из файла: {sample_path} ({len(sample)})")
            return sample
    size = min(size, len(names))
    rng = random.Random(seed)
    sample = rng.sample(names, size)
    payload = {
        "source": str(DEFAULT_INPUT),
        "created_at": _now(),
        "seed": seed,
        "count": len(sample),
        "names": sample,
    }
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    sample_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Новая выборка: {len(sample)} → {sample_path} (seed={seed})")
    return sample


def load_progress(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"done": {}, "updated_at": None}
    return json.loads(path.read_text(encoding="utf-8"))


def save_progress(path: Path, progress: dict[str, Any]) -> None:
    progress["updated_at"] = _now()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(progress, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def parse_cards_from_html(html: str) -> dict[str, Any]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    cards: list[dict[str, Any]] = []
    for card in soup.select(".org-partition-card"):
        title_a = card.select_one("a.org-pcr-title")
        go_a = card.select_one("a.org-pcl-go-but")
        fields: dict[str, str] = {}
        for el in card.select(".org-pcl-ogrn"):
            text = " ".join(el.get_text(" ", strip=True).split())
            sp = el.select_one(".copy-script")
            val = sp.get_text(strip=True) if sp else ""
            if "ОГРН" in text:
                fields["ogrn"] = val
            elif "ИНН" in text:
                fields["inn"] = val
            elif "КПП" in text:
                fields["kpp"] = val
        sub = card.select_one(".org-pcr-sutitle")
        okved_a = sub.select_one("a") if sub else None
        okved = okved_a.get_text(strip=True) if okved_a else ""
        okved_name = ""
        if sub:
            okved_name = " ".join(sub.get_text(" ", strip=True).split())
            if okved and okved_name.startswith(okved):
                okved_name = okved_name[len(okved) :].strip()
        status_el = card.select_one(".org-pc-status")
        region = reg_date = period = ""
        for d in card.select(".org-pcr-desc"):
            parts = [
                " ".join(x.get_text(" ", strip=True).split())
                for x in d.find_all("div", recursive=False)
                if " ".join(x.get_text(" ", strip=True).split())
            ]
            label = next((p.lower() for p in parts if re.search(r"регион|дата|период|оквэд", p, re.I)), "")
            value = parts[-1] if parts else ""
            if "регион" in label:
                region = value
            elif "дата" in label:
                reg_date = value
            elif "период" in label:
                period = value
        href = ""
        if title_a and title_a.get("href"):
            href = title_a["href"]
        elif go_a and go_a.get("href"):
            href = go_a["href"]
        cards.append(
            {
                "title": " ".join((title_a.get_text(" ", strip=True) if title_a else "").split()),
                "url": _abs_url(href),
                "ogrn": fields.get("ogrn", ""),
                "inn": fields.get("inn", ""),
                "kpp": fields.get("kpp", ""),
                "okved": okved,
                "okved_name": okved_name,
                "status": " ".join((status_el.get_text(" ", strip=True) if status_el else "").split()),
                "region": region,
                "reg_date": reg_date,
                "period": period,
            }
        )
    count_el = soup.select_one(".orgs-count")
    return {
        "total_text": " ".join(count_el.get_text(" ", strip=True).split()) if count_el else "",
        "cards": cards,
    }


def lean_result(
    *,
    name: str,
    idx: int,
    query_url: str,
    final_url: str,
    extracted: dict[str, Any],
) -> dict[str, Any]:
    cards = extracted.get("cards") or []
    okveds = [c.get("okved") for c in cards if c.get("okved")]
    return {
        "index": idx,
        "name": name,
        "query_url": query_url,
        "final_url": final_url,
        "total_text": extracted.get("total_text") or "",
        "cards_count": len(cards),
        "cards": cards,
        "okveds": list(dict.fromkeys(okveds)),
        "saved_at": _now(),
    }


def write_lean(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def rebuild_okved_freq(dump_dir: Path, out_path: Path) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for path in sorted(dump_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        cards = data.get("cards")
        if not isinstance(cards, list):
            continue
        for card in cards:
            code = (card or {}).get("okved") or ""
            if code:
                counter[code] += 1
    ranked = dict(sorted(counter.items(), key=lambda kv: (-kv[1], kv[0])))
    payload = {
        "updated_at": _now(),
        "unique_okveds": len(ranked),
        "total_cards_with_okved": sum(ranked.values()),
        "freq": ranked,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return ranked


def dump_one(page, name: str, *, dump_dir: Path, idx: int, total: int) -> dict[str, Any]:
    from parser_synaps_browser import _goto

    url = QUERY_URL.format(query=quote(name))
    slug = _slug(name, idx)
    out_path = dump_dir / f"{slug}.json"

    print(f"[{idx}/{total}] {name!r}")
    _goto(page, url, wait_until="domcontentloaded")
    try:
        page.wait_for_selector(
            ".org-partition-card, .org-pc-status, a.org-pcr-title, .ocs-empty, .org-search-empty, .orgs-count",
            timeout=18_000,
        )
    except Exception:
        page.wait_for_timeout(1500)
    page.wait_for_timeout(800)

    try:
        extracted = page.evaluate(EXTRACT_CARDS_JS)
    except Exception:
        extracted = parse_cards_from_html(page.content())

    # абсолютные URL на всякий случай
    for card in extracted.get("cards") or []:
        card["url"] = _abs_url(card.get("url") or "")

    meta = lean_result(
        name=name,
        idx=idx,
        query_url=url,
        final_url=page.url,
        extracted=extracted,
    )
    write_lean(out_path, meta)
    print(
        f"  → {out_path.name}; карточек={meta['cards_count']}; "
        f"оквэд={len(meta['okveds'])}; urls={sum(1 for c in meta['cards'] if c.get('url'))}"
    )
    return meta


def migrate_html_dumps(dump_dir: Path, progress_path: Path, okved_path: Path) -> None:
    html_files = sorted(dump_dir.glob("*.html"))
    if not html_files:
        print("HTML для миграции нет.")
        return

    progress = load_progress(progress_path)
    done: dict[str, Any] = progress.get("done") or {}
    # name → meta_file из старого progress
    by_meta = {
        (v or {}).get("meta_file") or (v or {}).get("html_file", "").replace(".html", ".json"): k
        for k, v in done.items()
        if isinstance(v, dict)
    }

    converted = 0
    for html_path in html_files:
        old_meta_path = html_path.with_suffix(".json")
        name = ""
        idx = 0
        query_url = ""
        final_url = ""
        if old_meta_path.is_file():
            try:
                old = json.loads(old_meta_path.read_text(encoding="utf-8"))
                name = _norm_name(str(old.get("name") or ""))
                idx = int(old.get("index") or 0)
                query_url = str(old.get("query_url") or "")
                final_url = str(old.get("final_url") or "")
            except Exception:
                pass
        if not name:
            # 0001_NAME_digest.html
            m = re.match(r"^(\d{4})_(.+)_[0-9a-f]{8}$", html_path.stem)
            if m:
                idx = int(m.group(1))
                name = m.group(2).replace("_", " ")
        if not name:
            name = html_path.stem

        html = html_path.read_text(encoding="utf-8", errors="replace")
        extracted = parse_cards_from_html(html)
        for card in extracted.get("cards") or []:
            card["url"] = _abs_url(card.get("url") or "")
        if not query_url:
            query_url = QUERY_URL.format(query=quote(name))
        if not final_url:
            final_url = query_url
        if not idx:
            idx = 0

        lean = lean_result(
            name=name,
            idx=idx,
            query_url=query_url,
            final_url=final_url,
            extracted=extracted,
        )
        # пишем lean рядом; имя файла как у html
        out_path = html_path.with_suffix(".json")
        write_lean(out_path, lean)
        done[name] = {
            "status": "ok",
            "meta_file": out_path.name,
            "cards_count": lean["cards_count"],
            "okveds_count": len(lean["okveds"]),
            "urls_count": sum(1 for c in lean["cards"] if c.get("url")),
        }
        html_path.unlink(missing_ok=True)
        converted += 1
        print(f"migrate {html_path.name} → {out_path.name} (cards={lean['cards_count']})")

    # подчистить старые bloated meta без cards (уже перезаписаны)
    progress["done"] = done
    progress["dump_dir"] = str(dump_dir)
    save_progress(progress_path, progress)
    freq = rebuild_okved_freq(dump_dir, okved_path)
    print(f"Готово: конвертировано {converted}, HTML удалены. Уникальных ОКВЭД: {len(freq)} → {okved_path}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Синапс: выдача по названиям → lean JSON")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    p.add_argument("--sample", type=Path, default=DEFAULT_SAMPLE)
    p.add_argument("--dump-dir", type=Path, default=DEFAULT_DUMP_DIR)
    p.add_argument("--progress", type=Path, default=DEFAULT_PROGRESS)
    p.add_argument("--okved-freq", type=Path, default=DEFAULT_OKVED_FREQ)
    p.add_argument("--sample-size", type=int, default=500)
    p.add_argument("--seed", type=int, default=42, help="Seed для random.sample")
    p.add_argument("--rebuild-sample", action="store_true", help="Пересоздать sample_500.json")
    p.add_argument("--sample-only", action="store_true", help="Только создать выборку, без браузера")
    p.add_argument(
        "--all",
        action="store_true",
        help="Все названия из 1_stage/names.json (не sample_500)",
    )
    p.add_argument("--migrate-html", action="store_true", help="Старые HTML → lean JSON и удалить HTML")
    p.add_argument("--limit", type=int, default=0, help="Сколько ещё невыгруженных взять (0=все)")
    p.add_argument("--headed", action="store_true")
    p.add_argument("--batch-pause", type=float, default=0.4, help="Пауза между запросами, сек")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    _ensure_utf8_stdout()
    load_dotenv(REPO_ROOT / ".env")
    args = parse_args(argv if argv is not None else sys.argv[1:])

    src = args.input if args.input.is_absolute() else (REPO_ROOT / args.input)
    if not src.is_file():
        src = DEFAULT_INPUT
    sample_path = args.sample if args.sample.is_absolute() else (STAGE_DIR / args.sample)
    dump_dir = args.dump_dir if args.dump_dir.is_absolute() else (STAGE_DIR / args.dump_dir)
    progress_path = args.progress if args.progress.is_absolute() else (STAGE_DIR / args.progress)
    okved_path = args.okved_freq if args.okved_freq.is_absolute() else (STAGE_DIR / args.okved_freq)

    if args.migrate_html:
        migrate_html_dumps(dump_dir, progress_path, okved_path)
        return

    names = load_names(src)
    if args.all:
        work_list = names
        print(f"Режим --all: все имена из {src} ({len(work_list)})")
    else:
        work_list = build_or_load_sample(
            names,
            sample_path=sample_path,
            size=max(1, args.sample_size),
            seed=int(args.seed),
            rebuild=bool(args.rebuild_sample),
        )
    if args.sample_only:
        return

    # индекс в полном списке работы (стабильный slug при resume)
    work_index = {n.casefold(): i for i, n in enumerate(work_list, start=1)}

    dump_dir.mkdir(parents=True, exist_ok=True)
    progress = load_progress(progress_path)
    done: dict[str, Any] = progress.get("done") or {}

    # ok только если lean с cards уже на диске
    def _is_ok(name: str, info: Any) -> bool:
        if not isinstance(info, dict) or info.get("status") != "ok":
            return False
        meta_file = info.get("meta_file")
        if not meta_file:
            return False
        path = dump_dir / meta_file
        if not path.is_file():
            return False
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return False
        return isinstance(data.get("cards"), list)

    queue = [n for n in work_list if not _is_ok(n, done.get(n))]
    if args.limit and args.limit > 0:
        queue = queue[: args.limit]

    already_ok = sum(1 for n in work_list if _is_ok(n, done.get(n)))
    print(f"Список: {len(work_list)}; уже ok: {already_ok}; в очереди: {len(queue)}")
    print(f"JSON → {dump_dir}")
    if not queue:
        rebuild_okved_freq(dump_dir, okved_path)
        print("Очередь пустая.")
        return

    from parser_synaps_browser import DEFAULT_STORAGE, _credentials, _ensure_logged_in

    main_url, mail, password = _credentials()
    storage = DEFAULT_STORAGE
    headless = not bool(args.headed)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        ctx_args: dict[str, Any] = {}
        if storage.exists():
            ctx_args["storage_state"] = str(storage)
        context = browser.new_context(**ctx_args)
        page = context.new_page()
        try:
            _ensure_logged_in(page, main_url, mail, password)
            try:
                context.storage_state(path=str(storage))
            except BaseException:
                pass

            total = len(queue)
            for i, name in enumerate(queue, start=1):
                idx = work_index.get(name.casefold(), i)
                try:
                    meta = dump_one(page, name, dump_dir=dump_dir, idx=idx, total=total)
                    done[name] = {
                        "status": "ok",
                        "meta_file": f"{_slug(name, idx)}.json",
                        "cards_count": meta["cards_count"],
                        "okveds_count": len(meta["okveds"]),
                        "urls_count": sum(1 for c in meta["cards"] if c.get("url")),
                    }
                except Exception as exc:
                    print(f"  ошибка: {exc}")
                    done[name] = {"status": "error", "error": str(exc)}
                progress["done"] = done
                progress["mode"] = "all" if args.all else "sample"
                progress["sample_file"] = str(sample_path) if not args.all else None
                progress["source_file"] = str(src)
                progress["dump_dir"] = str(dump_dir)
                save_progress(progress_path, progress)
                if i % 10 == 0:
                    rebuild_okved_freq(dump_dir, okved_path)
                try:
                    context.storage_state(path=str(storage))
                except BaseException:
                    pass
                if args.batch_pause > 0:
                    time.sleep(args.batch_pause)
        except KeyboardInterrupt:
            print("\nОстановлено (Ctrl+C). Прогресс сохранён — можно продолжить тем же запуском.")
        finally:
            for closer in (
                lambda: context.storage_state(path=str(storage)),
                context.close,
                browser.close,
            ):
                try:
                    closer()
                except BaseException:
                    pass

    rebuild_okved_freq(dump_dir, okved_path)
    ok = sum(1 for n in work_list if _is_ok(n, done.get(n)))
    print(f"Готово. ok={ok}/{len(work_list)}. JSON: {dump_dir}; ОКВЭД: {okved_path}")


if __name__ == "__main__":
    main()
