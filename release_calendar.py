"""Publication-calendar engine: when is a source's data for a period *due*?

This is the expectation axis introduced 2026-06-02 (see
`dev_notes/2026-06-02-eurostat-expectation-axis-design.md`). It replaces the
old hardcoded "5 weeks past period close" fetch-gate (`not_yet_eligible`) with
a derived expectation for a candidate period:

    none_expected — today is before the scheduled publication date; a quiet
                    gap here is normal, ignore it.
    due           — today is on/just-after the scheduled date; data is
                    expected now (small grace window absorbs weekend shifts).
    overdue       — today is past the scheduled date + grace and the data
                    still hasn't shown up; this is the one a human looks at.

The expectation is orthogonal to the *result* (`new_data` / `no_change` /
`error`) the probe records — see `routine_log`. The two combine: a missing
release past its date is `no_change × overdue` (alert); an early arrival is
`new_data × none_expected` (interesting — Eurostat beat its own calendar).

Pure module — no DB, no network. Fully unit-testable. The DB-dependent
"what's the next candidate period for this source" lives in the probe
orchestration (`scrape.probe_source`); here we only answer, given a source +
period + today, what was expected.

Date sources (provenance — these are hand-entered annual constants):

- **Eurostat**: the "G.3 Trade in goods Publication Calendar" PDF, which marks
  per month the purple "Publication of the monthly news release & update of
  Comext data / Bulk download files (at 11:00 am)" date and the green "most
  recent reference month for which data are published".
  https://ec.europa.eu/eurostat/documents/6842948/10520689/Release+Calendar
  Cross-checked 2026-06-02 against our own data: the 2026-03 reference month's
  bulk file was first seen 2026-05-19, exactly the calendar's 19 May 2026
  purple date. Extra-EU detailed trade (our CN/HK/MO partners) publishes ~46
  days after the reference month ends.

- **HMRC OTS**: the uktradeinfo release calendar.
  https://www.uktradeinfo.com/trade-data/release-calendar
  ~6-week lag; HMRC publishes a few days *before* Eurostat for the same
  reference month (e.g. April 2026 ref: HMRC 12 Jun, Eurostat 15 Jun).

- **GACC**: no official forward publication calendar exists (China Customs
  does not publish one), so GACC is formula-only — `exact` is empty. The
  preliminary country/region release lands the 8th–10th of the month after
  the reference month (e.g. May 2026 → 10 Jun; Apr → 8 May; Mar → 8 Apr;
  Dec 2025 → 8 Jan), so `lag_days=8` puts the scheduled date on the 8th and a
  4-day grace pushes the due-by cutoff to the ~12th, absorbing the normal
  8th–10th wobble. Holiday slips do happen (Aug 2025 ref → 17 Sep, Jul → 12
  Aug, around the China public-holiday calendar); the cutoff deliberately lets
  a genuine slip past the 12th read `overdue` for the days it is actually
  late, which is the signal we want. See the GACC addendum in
  dev_notes/2026-06-02-eurostat-expectation-axis-design.md.

When a period isn't in the hand-entered table (e.g. a 2027 reference month
before next year's calendar is entered, or any GACC month) the formula
`period_close + lag_days` is the fallback. The grace window is set generously
enough that a source publishing on its real schedule never reads `overdue`.
"""
from __future__ import annotations

import dataclasses
from datetime import date, timedelta

# The expectation axis vocabulary. Single source of truth shared by
# routine_log (write guard) and the DB CHECK constraint.
VALID_EXPECTATIONS: frozenset[str] = frozenset({"none_expected", "due", "overdue"})

NONE_EXPECTED = "none_expected"
DUE = "due"
OVERDUE = "overdue"


def _add_months(d: date, months: int) -> date:
    """First-of-month `d` shifted by `months`. Day component is ignored."""
    base = d.year * 12 + (d.month - 1) + months
    return date(base // 12, base % 12 + 1, 1)


def next_period(period: date) -> date:
    """The month after `period` (first-of-month anchor). The candidate the
    probe tries next is `next_period(latest_period_in_db)`."""
    return _add_months(period, 1)


def period_close(period: date) -> date:
    """Last calendar day of `period`'s month — when the reference period ends."""
    return _add_months(period, 1) - timedelta(days=1)


@dataclasses.dataclass(frozen=True)
class SourceCalendar:
    """Publication schedule for one source.

    `exact` maps a reference month (first-of-month anchor) to the source's
    *scheduled* publication date for that month — the authoritative purple
    calendar date. `lag_days` + `grace_days` are the formula fallback for
    months not in `exact`.
    """

    lag_days: int
    grace_days: int
    exact: dict[date, date]


# Eurostat extra-EU detailed trade (full_v2_YYYYMM.7z). Reference month →
# scheduled bulk-file publication date. 2026 publication year, covering
# reference months 2025-11 .. 2026-10. See provenance note above.
_EUROSTAT = SourceCalendar(
    lag_days=46,
    grace_days=5,
    exact={
        date(2025, 11, 1): date(2026, 1, 15),
        date(2025, 12, 1): date(2026, 2, 13),
        date(2026, 1, 1): date(2026, 3, 20),
        date(2026, 2, 1): date(2026, 4, 17),
        date(2026, 3, 1): date(2026, 5, 19),
        date(2026, 4, 1): date(2026, 6, 15),
        date(2026, 5, 1): date(2026, 7, 16),
        date(2026, 6, 1): date(2026, 8, 14),
        date(2026, 7, 1): date(2026, 9, 15),
        date(2026, 8, 1): date(2026, 10, 16),
        date(2026, 9, 1): date(2026, 11, 13),
        date(2026, 10, 1): date(2026, 12, 16),
    },
)

# HMRC Overseas Trade Statistics. Reference month → scheduled OTS publication
# date. Authoritative for 2026-04 .. 2026-06 (uktradeinfo only listed the next
# three at fetch time); formula fallback (~44d) covers the rest.
_HMRC = SourceCalendar(
    lag_days=44,
    grace_days=7,
    exact={
        date(2026, 4, 1): date(2026, 6, 12),
        date(2026, 5, 1): date(2026, 7, 16),
        date(2026, 6, 1): date(2026, 8, 13),
    },
)

# GACC preliminary country/region release. China Customs publishes no forward
# calendar, so this is formula-only (`exact` empty): scheduled = period_close +
# 8 days ≈ the 8th of the following month, with a 4-day grace → due-by ~12th.
# That cutoff absorbs the routine 8th–10th wobble while letting the occasional
# holiday slip (Aug 2025 ref → 17 Sep) read `overdue` for the days it is late.
# See provenance note above.
_GACC = SourceCalendar(
    lag_days=8,
    grace_days=4,
    exact={},
)

# Sources with a publication calendar. GACC joined 2026-06-22: unlike eurostat/
# hmrc it has no official forward calendar, so it is formula-only, but it now
# carries the same expectation axis (none_expected / due / overdue) instead of a
# blank — a slipped GACC release surfaces on the --source-status OVERDUE line.
CALENDARS: dict[str, SourceCalendar] = {
    "eurostat": _EUROSTAT,
    "hmrc": _HMRC,
    "gacc": _GACC,
}


def has_calendar(source: str) -> bool:
    """True for sources that carry an expectation axis (eurostat, hmrc, gacc)."""
    return source in CALENDARS


def expected_publish_date(source: str, period: date) -> date | None:
    """The scheduled publication date for `source`'s `period` data.

    Exact hand-entered calendar date if known, else the formula fallback
    (`period_close + lag_days`). None for sources without a calendar. GACC is
    formula-only (no `exact` entries), so it always takes the fallback branch.
    """
    cal = CALENDARS.get(source)
    if cal is None:
        return None
    anchor = period.replace(day=1)
    if anchor in cal.exact:
        return cal.exact[anchor]
    return period_close(period) + timedelta(days=cal.lag_days)


def classify_expectation(
    source: str, period: date, today: date,
) -> str | None:
    """Derive the expectation for `source`'s `period` as of `today`.

    Returns `none_expected` / `due` / `overdue`, or None for a source with no
    calendar. The grace window after the scheduled date absorbs weekend /
    holiday shifts so an on-time release never reads `overdue`.
    """
    cal = CALENDARS.get(source)
    if cal is None:
        return None
    expected = expected_publish_date(source, period)
    assert expected is not None  # guaranteed when cal is not None
    if today < expected:
        return NONE_EXPECTED
    if today <= expected + timedelta(days=cal.grace_days):
        return DUE
    return OVERDUE
