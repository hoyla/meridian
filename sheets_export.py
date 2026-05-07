"""Google Sheets export.

Pushes the latest run's observations + findings to a configured spreadsheet,
appending rather than overwriting so the sheet is a running ledger the desk
can scroll through.
"""

import logging

log = logging.getLogger(__name__)


def export_run(scrape_run_id: int) -> None:
    raise NotImplementedError
