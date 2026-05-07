"""Deterministic anomaly detection over observations.

The LLM never touches raw numbers. This module computes the actual stats —
MoM/YoY deltas, rolling z-scores against a baseline window, rank shifts among
trading partners — and writes findings rows that the LLM layer then narrates.
"""

import logging

log = logging.getLogger(__name__)


def detect_for_run(scrape_run_id: int) -> int:
    """Compute anomalies triggered by this run. Returns count of findings written."""
    raise NotImplementedError
