"""
Тот же сценарий, что «20 млн»: копия SHEET_20M_SOURCE → SHEET_20M_FINAL и парсинг.
Опции только через .env (см. parser_sheet_env.stage2_options); при необходимости PARSER_RUN_20M=1 не нужен — листы всегда 20M.
"""

from __future__ import annotations

from parser_2stage_extract import run_copy_source_to_final_then_parse
from parser_sheet_env import sheet_20m_final, sheet_20m_source, stage2_options


if __name__ == "__main__":
    opt = stage2_options()
    run_copy_source_to_final_then_parse(
        sheet_20m_source(),
        sheet_20m_final(),
        headless=not opt["headed"],
        save_dom_snapshots=opt["dump_dom"],
        skip_copy=opt["skip_copy"],
        ac_last_column=opt["ac_last"],
    )
