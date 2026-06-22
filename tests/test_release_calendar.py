"""Tests for release_calendar — the publication-calendar expectation engine.

Pure module, no DB — these run regardless of GACC_TEST_DATABASE_URL.
"""
from __future__ import annotations

from datetime import date

import pytest

import release_calendar as rc


def test_period_helpers():
    assert rc.period_close(date(2026, 2, 1)) == date(2026, 2, 28)
    assert rc.period_close(date(2026, 12, 1)) == date(2026, 12, 31)
    assert rc.next_period(date(2026, 12, 1)) == date(2027, 1, 1)
    assert rc.next_period(date(2026, 3, 15)) == date(2026, 4, 1)  # day ignored


def test_exact_date_takes_precedence_over_formula():
    # 2026-03 is in the hand-entered Eurostat table → the authoritative purple
    # date, which matches our own DB row (first_seen 2026-05-19), not the
    # formula's close+46 = 2026-05-16.
    assert rc.expected_publish_date("eurostat", date(2026, 3, 1)) == date(2026, 5, 19)


def test_formula_fallback_for_uncalendared_period():
    # 2026-11 ref is past the 2026 table → formula close(Nov 30)+46 = Jan 15 2027.
    assert rc.expected_publish_date("eurostat", date(2026, 11, 1)) == date(2027, 1, 15)


def test_gacc_has_a_formula_only_calendar():
    # GACC joined the expectation axis 2026-06-22. No official forward calendar
    # exists, so it's formula-only (empty `exact`): scheduled = the 8th of the
    # following month, matching the observed cadence (Apr 2026 → 8 May, Dec
    # 2025 → 8 Jan).
    assert rc.has_calendar("gacc") is True
    assert rc.expected_publish_date("gacc", date(2026, 4, 1)) == date(2026, 5, 8)
    assert rc.expected_publish_date("gacc", date(2025, 12, 1)) == date(2026, 1, 8)


@pytest.mark.parametrize("today,want", [
    (date(2026, 6, 7), rc.NONE_EXPECTED),   # before the 8 Jun scheduled date
    (date(2026, 6, 8), rc.DUE),             # on the scheduled date
    (date(2026, 6, 12), rc.DUE),            # last day of the due-by window (~12th)
    (date(2026, 6, 13), rc.OVERDUE),        # past the cutoff → overdue
])
def test_gacc_grace_boundaries(today, want):
    # May 2026 ref → scheduled 8 Jun (close 31 May + 8d), 4-day grace → 12 Jun.
    assert rc.classify_expectation("gacc", date(2026, 5, 1), today) == want


def test_gacc_holiday_slip_reads_overdue_while_late():
    # Aug 2025 ref published 17 Sep (a China-holiday slip) vs the normal ~8 Sep.
    # The cutoff (close 31 Aug + 8 + 4 grace = 12 Sep) is deliberately tight
    # enough that the genuinely-late release reads `overdue` for the days it is
    # actually late — the signal --source-status should surface.
    assert rc.classify_expectation("gacc", date(2025, 8, 1), date(2025, 9, 12)) == rc.DUE
    assert rc.classify_expectation("gacc", date(2025, 8, 1), date(2025, 9, 13)) == rc.OVERDUE
    assert rc.classify_expectation("gacc", date(2025, 8, 1), date(2025, 9, 17)) == rc.OVERDUE


@pytest.mark.parametrize("today,want", [
    (date(2026, 6, 14), rc.NONE_EXPECTED),  # day before scheduled 15 Jun
    (date(2026, 6, 15), rc.DUE),            # on the scheduled date
    (date(2026, 6, 20), rc.DUE),            # within the 5-day grace window
    (date(2026, 6, 21), rc.OVERDUE),        # past date + grace
])
def test_eurostat_grace_boundaries(today, want):
    # April 2026 ref → scheduled 15 Jun 2026, grace 5 days.
    assert rc.classify_expectation("eurostat", date(2026, 4, 1), today) == want


def test_overdue_for_long_missing_period():
    # If the pipeline fell behind and March is still the candidate in June, it's
    # well past its 19 May date → overdue (the alert case).
    assert rc.classify_expectation("eurostat", date(2026, 3, 1), date(2026, 6, 2)) == rc.OVERDUE


def test_hmrc_uses_its_own_earlier_schedule():
    # HMRC publishes April 2026 data on 12 Jun, three days before Eurostat's 15 Jun.
    assert rc.expected_publish_date("hmrc", date(2026, 4, 1)) == date(2026, 6, 12)
    assert rc.classify_expectation("hmrc", date(2026, 4, 1), date(2026, 6, 12)) == rc.DUE
    assert rc.classify_expectation("hmrc", date(2026, 4, 1), date(2026, 6, 11)) == rc.NONE_EXPECTED


def test_valid_expectations_constant():
    assert rc.VALID_EXPECTATIONS == {"none_expected", "due", "overdue"}
