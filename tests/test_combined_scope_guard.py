"""Pure-logic tests for the combined (eu_27_plus_uk) scope coverage guard.

These need no database, so they run regardless of GACC_TEST_DATABASE_URL — the
DB-backed end-to-end regression lives in test_hs_groups.py
(test_yoy_combined_scope_drops_month_hmrc_has_not_published).
"""

from datetime import date

import anomalies


def _months(year: int, mos) -> set:
    return {date(year, m, 1) for m in mos}


def test_drops_month_one_source_has_not_published():
    """The trailing-lag case: Eurostat has Dec, HMRC stops at Nov -> Dec is
    one-sided and must be excluded from the summable set."""
    eur = _months(2025, range(1, 13))          # Jan..Dec 2025
    hmrc = _months(2025, range(1, 12))          # Jan..Nov 2025
    aligned = anomalies._coverage_aligned_periods({"eurostat": eur, "hmrc": hmrc})
    assert aligned == _months(2025, range(1, 12))
    assert date(2025, 12, 1) not in aligned


def test_drops_interior_hole():
    """A hole behind the frontier (a genuinely missing month in one source) is
    also one-sided and dropped, not summed as if complete."""
    eur = _months(2025, range(1, 13))
    hmrc = _months(2025, range(1, 13)) - {date(2025, 6, 1)}
    aligned = anomalies._coverage_aligned_periods({"eurostat": eur, "hmrc": hmrc})
    assert date(2025, 6, 1) not in aligned
    assert aligned == eur - {date(2025, 6, 1)}


def test_single_source_unchanged():
    """A single-source scope must pass its coverage through untouched."""
    eur = _months(2025, range(1, 13))
    assert anomalies._coverage_aligned_periods({"eurostat": eur}) == eur


def test_empty_coverage_is_empty():
    assert anomalies._coverage_aligned_periods({}) == set()
