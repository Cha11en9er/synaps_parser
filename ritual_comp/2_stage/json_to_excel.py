"""
ritual companies.json → ритуальные_компании.xlsx

  python ritual_comp/2_stage/json_to_excel.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

STAGE_DIR = Path(__file__).resolve().parent
RITUAL_DIR = STAGE_DIR.parent
REPO_ROOT = RITUAL_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from export_companies_excel import export_excel  # noqa: E402

DEFAULT_INPUT = STAGE_DIR / "companies.json"
DEFAULT_OUTPUT = STAGE_DIR / "ритуальные_компании.xlsx"
DEFAULT_INNS = RITUAL_DIR / "1_stage" / "inns.json"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    p.add_argument("--overwrite", action="store_true")
    args = p.parse_args()
    out = export_excel(
        args.input,
        args.output,
        sheet_title="Ритуальные",
        name_sources=[DEFAULT_INNS],
        overwrite=bool(args.overwrite),
    )
    print(f"OK -> {out}")


if __name__ == "__main__":
    main()
