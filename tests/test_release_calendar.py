"""Tests for release_calendar — the publication-calendar expectation engine.

Pure module, no DB — these run regardless of GACC_TEST_DATABASE_URL.
"""
from __future__ import annotations

from datetime import date, timedelta

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


def test_gacc_official_2026_schedule_takes_precedence():
    # The official 2026 schedule (公告2025年第240号, discovered 2026-07-14 —
    # see dev_notes/2026-07-14-gacc-chinese-source-investigation.md) is
    # hand-entered in `exact` and overrides the formula: Apr 2026 → 9 May
    # (formula said the 8th), May 2026 → 9 Jun. Both match what GACC actually
    # did (May pages carry a 9–10 Jun date).
    assert rc.has_calendar("gacc") is True
    assert rc.expected_publish_date("gacc", date(2026, 4, 1)) == date(2026, 5, 9)
    assert rc.expected_publish_date("gacc", date(2026, 5, 1)) == date(2026, 6, 9)


def test_gacc_formula_fallback_beyond_entered_year():
    # 2027 has no hand-entered dates until GACC publishes next year's
    # announcement (each year's calendar appears Jan–Mar of that year), so the
    # formula applies: ordinary months the 8th of the following month,
    # quarter-end months the 13th.
    assert rc.expected_publish_date("gacc", date(2027, 4, 1)) == date(2027, 5, 8)
    assert rc.expected_publish_date("gacc", date(2027, 3, 1)) == date(2027, 4, 13)


@pytest.mark.parametrize("today,want", [
    (date(2026, 6, 8), rc.NONE_EXPECTED),   # before the official 9 Jun date
    (date(2026, 6, 9), rc.DUE),             # on the scheduled date
    (date(2026, 6, 13), rc.DUE),            # last day of the due-by window
    (date(2026, 6, 14), rc.OVERDUE),        # past the cutoff → overdue
])
def test_gacc_grace_boundaries(today, want):
    # May 2026 ref → official 9 Jun, 4-day grace → due-by 13 Jun.
    assert rc.classify_expectation("gacc", date(2026, 5, 1), today) == want


def test_gacc_holiday_slip_reads_overdue_while_late():
    # Aug 2025 ref published 17 Sep (a China-holiday slip) vs the normal ~8 Sep.
    # The cutoff (close 31 Aug + 8 + 4 grace = 12 Sep) is deliberately tight
    # enough that the genuinely-late release reads `overdue` for the days it is
    # actually late — the signal --source-status should surface.
    assert rc.classify_expectation("gacc", date(2025, 8, 1), date(2025, 9, 12)) == rc.DUE
    assert rc.classify_expectation("gacc", date(2025, 8, 1), date(2025, 9, 13)) == rc.OVERDUE
    assert rc.classify_expectation("gacc", date(2025, 8, 1), date(2025, 9, 17)) == rc.OVERDUE


def test_gacc_january_shares_februarys_schedule():
    # China Customs publishes no standalone January (Chinese New Year): January
    # data arrives folded into the Jan–Feb cumulative, on February's schedule.
    # So a GACC January candidate is due on February's date, not January's.
    # 2026's combined release has an official date: 10 Mar (公告 240号 note 4
    # analogue — the 2026 schedule's March 快讯 row).
    assert (
        rc.expected_publish_date("gacc", date(2026, 1, 1))
        == rc.expected_publish_date("gacc", date(2026, 2, 1))
        == date(2026, 3, 10)
    )
    # Beyond the entered year the remap still applies, on the formula date.
    assert (
        rc.expected_publish_date("gacc", date(2027, 1, 1))
        == rc.expected_publish_date("gacc", date(2027, 2, 1))
        == date(2027, 3, 8)  # close(28 Feb 2027) + 8d
    )


def test_gacc_january_not_overdue_while_waiting_for_february():
    jan = date(2026, 1, 1)
    # Mid-February, with the routine's candidate sitting on January: not yet due
    # — without the carve-out this would already read `overdue`.
    assert rc.classify_expectation("gacc", jan, date(2026, 2, 20)) == rc.NONE_EXPECTED
    # Still quiet up to the official 10 Mar; due on it; past due-by (+4 grace)
    # with nothing → genuinely overdue (the Jan–Feb combined is itself late).
    assert rc.classify_expectation("gacc", jan, date(2026, 3, 9)) == rc.NONE_EXPECTED
    assert rc.classify_expectation("gacc", jan, date(2026, 3, 10)) == rc.DUE
    assert rc.classify_expectation("gacc", jan, date(2026, 3, 20)) == rc.OVERDUE


def test_january_carve_out_is_gacc_only():
    # Must not touch other GACC months, or January for the other sources.
    assert rc.expected_publish_date("gacc", date(2026, 4, 1)) == date(2026, 5, 9)
    assert (
        rc.expected_publish_date("hmrc", date(2026, 1, 1))
        != rc.expected_publish_date("hmrc", date(2026, 2, 1))
    )
    assert (
        rc.expected_publish_date("eurostat", date(2026, 1, 1))
        != rc.expected_publish_date("eurostat", date(2026, 2, 1))
    )


def test_gacc_quarter_end_months_scheduled_mid_month():
    # Quarter-end reference months (Mar/Jun/Sep/Dec) run ~3-5 days later than
    # ordinary months — the quarterly cumulations land mid-month. The official
    # 2026 schedule confirms the pattern the formula override encoded: Apr 14 /
    # Jul 14 / Oct 14 / Jan 14 vs the 7th–10th for ordinary months.
    assert rc.expected_publish_date("gacc", date(2026, 3, 1)) == date(2026, 4, 14)
    assert rc.expected_publish_date("gacc", date(2026, 6, 1)) == date(2026, 7, 14)
    assert rc.expected_publish_date("gacc", date(2026, 9, 1)) == date(2026, 10, 14)
    assert rc.expected_publish_date("gacc", date(2025, 12, 1)) == date(2026, 1, 14)
    # Ordinary months stay early-month (official: the 9th).
    assert rc.expected_publish_date("gacc", date(2026, 4, 1)) == date(2026, 5, 9)
    assert rc.expected_publish_date("gacc", date(2026, 5, 1)) == date(2026, 6, 9)
    # The formula override carries the same quarter-end shape into years with
    # no entered schedule yet.
    assert rc.expected_publish_date("gacc", date(2027, 6, 1)) == date(2027, 7, 13)


def test_gacc_quarter_end_not_overdue_on_the_twelfth():
    # The regression that motivated the override: June 2026 (a Q2-end month)
    # must not read OVERDUE in its real mid-month window. The official date is
    # 14 Jul — which GACC met to the day (Chinese site, 09:28 Beijing,
    # 2026-07-14).
    jun = date(2026, 6, 1)
    assert rc.classify_expectation("gacc", jun, date(2026, 7, 12)) == rc.NONE_EXPECTED
    assert rc.classify_expectation("gacc", jun, date(2026, 7, 13)) == rc.NONE_EXPECTED
    assert rc.classify_expectation("gacc", jun, date(2026, 7, 14)) == rc.DUE
    # due-by = 14 Jul + 4 grace = 18 Jul; a genuine slip past that still trips
    # (e.g. the 2022 Sep-ref release that landed 24 Oct).
    assert rc.classify_expectation("gacc", jun, date(2026, 7, 18)) == rc.DUE
    assert rc.classify_expectation("gacc", jun, date(2026, 7, 19)) == rc.OVERDUE


def test_quarter_end_override_is_gacc_only():
    # The other sources have no month_lag_days, so their formula lag is uniform:
    # a quarter-end month gets no special treatment.
    assert rc.expected_publish_date("eurostat", date(2026, 11, 1)) == date(2027, 1, 15)
    assert (
        rc.expected_publish_date("hmrc", date(2027, 6, 1))
        == rc.period_close(date(2027, 6, 1)) + timedelta(days=rc._HMRC.lag_days)
    )


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


def test_next_release_forecast_orders_by_due_date_and_caps():
    # Latest published period per source. Each source's next candidate is the
    # following month, due on its calendar date:
    #   gacc 2026-05 → candidate 2026-06 → official 14 Jul (公告 240号)
    #   eurostat 2026-04 → candidate 2026-05 → exact 16 Jul
    #   hmrc 2026-04 → candidate 2026-05 → exact 16 Jul
    latest = {
        "eurostat": date(2026, 4, 1),
        "hmrc": date(2026, 4, 1),
        "gacc": date(2026, 5, 1),
    }
    # Default limit=2: soonest first, the Jul-16 tie broken by source name.
    assert rc.next_release_forecast(latest) == [
        ("gacc", date(2026, 7, 14)),
        ("eurostat", date(2026, 7, 16)),
    ]
    # limit=None returns every source, still date-sorted.
    assert rc.next_release_forecast(latest, limit=None) == [
        ("gacc", date(2026, 7, 14)),
        ("eurostat", date(2026, 7, 16)),
        ("hmrc", date(2026, 7, 16)),
    ]


def test_next_release_forecast_skips_unknown_and_empty_sources():
    latest = {
        "gacc": date(2026, 5, 1),
        "hmrc": None,            # no prior release → nothing to anchor on
        "mystery": date(2026, 5, 1),  # no calendar → skipped
    }
    assert rc.next_release_forecast(latest, limit=None) == [
        ("gacc", date(2026, 7, 14)),
    ]


def test_gacc_bulletin_is_informational_only():
    # The Monthly Bulletin (统计月报 — the verified vintage) has a cadence for
    # display but is NOT a probed source: no expectation axis, absent from
    # has_calendar, invisible to the forecast. Its dates: always the 18th of
    # the month after the reference month (official 2026 schedule), Jan+Feb
    # combined onto 18 Mar, and the formula fallback reproduces the same rule
    # for years with no entered schedule.
    assert rc.has_calendar("gacc_bulletin") is False
    assert rc.expected_publish_date("gacc_bulletin", date(2026, 5, 1)) == date(2026, 6, 18)
    assert rc.expected_publish_date("gacc_bulletin", date(2026, 6, 1)) == date(2026, 7, 18)
    assert rc.expected_publish_date("gacc_bulletin", date(2026, 1, 1)) == date(2026, 3, 18)
    assert rc.expected_publish_date("gacc_bulletin", date(2027, 5, 1)) == date(2027, 6, 18)
    assert rc.classify_expectation("gacc_bulletin", date(2026, 5, 1), date(2026, 6, 20)) is None
    assert rc.next_release_forecast(
        {"gacc_bulletin": date(2026, 5, 1)}, limit=None) == []


def test_expected_publish_date_detail_flags_official_vs_estimated():
    # Official (hand-entered from a published schedule) vs our formula
    # estimate — the portal's publication calendar renders them differently.
    assert rc.expected_publish_date_detail("gacc", date(2026, 6, 1)) == (
        date(2026, 7, 14), True)
    assert rc.expected_publish_date_detail("gacc", date(2027, 6, 1)) == (
        date(2027, 7, 13), False)
    assert rc.expected_publish_date_detail("eurostat", date(2026, 5, 1)) == (
        date(2026, 7, 16), True)
    assert rc.expected_publish_date_detail("hmrc", date(2026, 12, 1)) == (
        rc.period_close(date(2026, 12, 1)) + timedelta(days=rc._HMRC.lag_days),
        False)
    assert rc.expected_publish_date_detail("gacc_bulletin", date(2026, 4, 1)) == (
        date(2026, 5, 18), True)
    assert rc.expected_publish_date_detail("nope", date(2026, 1, 1)) is None
