"""
developer companies.json → девелоперы_компании.xlsx

  python developer_comp/2_stage/json_to_excel.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

STAGE_DIR = Path(__file__).resolve().parent
DEV_DIR = STAGE_DIR.parent
REPO_ROOT = DEV_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from export_companies_excel import export_excel  # noqa: E402

DEFAULT_INPUT = STAGE_DIR / "companies.json"
DEFAULT_OUTPUT = STAGE_DIR / "девелоперы_компании.xlsx"
DEFAULT_SHORTLIST = STAGE_DIR / "shortlist_inns.json"
DEFAULT_SHORTLIST_FULL = STAGE_DIR / "shortlist.json"
DEFAULT_NAMES = DEV_DIR / "1_stage" / "names.json"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    p.add_argument("--overwrite", action="store_true")
    args = p.parse_args()
    out = export_excel(
        args.input,
        args.output,
        sheet_title="Девелоперы",
        name_sources=[DEFAULT_SHORTLIST, DEFAULT_SHORTLIST_FULL, DEFAULT_NAMES],
        overwrite=bool(args.overwrite),
    )
    print(f"OK -> {out}")


if __name__ == "__main__":
    main()
