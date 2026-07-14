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

- **GACC**: China Customs *does* publish an official annual schedule — a fact
  established 2026-07-14 (see
  dev_notes/2026-07-14-gacc-chinese-source-investigation.md, correcting this
  module's earlier "no forward calendar exists" claim). The 2026 schedule is
  海关总署公告2025年第240号 ("on the publication times of China customs
  statistics for 2026"), posted 2026-03-11 on the Chinese site
  (www.customs.gov.cn/customs/2026-03/11/article_2026031116150585435.html);
  English editions for 2023–2025 live under
  english.customs.gov.cn/statistics/Statistics?ColumnId=4. Two structural
  caveats: the calendar for year N is published **January–March of year N**
  (the 2018–2023 SDDS path dates and the 2026 announcement all confirm), so
  early-year months run on the formula fallback until it lands; and the
  English translation lags by a year or more, so recent years' dates must be
  read from the Chinese announcement. The 2026 dates are hand-entered in
  `_GACC.exact`. Beyond the entered year the formula fallback applies:
  ordinary reference months land the 7th–10th of the following month
  (`lag_days=8`), while **quarter-end reference months (Mar/Jun/Sep/Dec) are
  systematically ~3–5 days later** (the quarterly cumulations), carrying a
  `month_lag_days=13` override — visible across 2019–2026 in GACC's own
  per-page publication dates and now in the official schedules themselves
  (2026: Apr 14th / Jul 14th / Oct 14th vs May–Jun 9th / Aug 7th). A genuinely
  large slip past the override still reads `overdue`, which is the signal we
  want. See the GACC addendum in
  dev_notes/2026-06-02-eurostat-expectation-axis-design.md.

- **GACC Monthly Bulletin** (统计月报 — the *verified* vintage, revisable
  until the Yearbook): always the 18th of the month after the reference
  month, per the same official schedule; Jan+Feb combine onto 18 March. Not
  an ingested source — carried here (`_GACC_BULLETIN`, outside `CALENDARS`)
  so the portal's publication-calendar table can show when verified figures
  arrive, without giving the probe loop an expectation axis for a source we
  don't scrape.

When a period isn't in the hand-entered table (e.g. a 2027 reference month
before next year's calendar is entered) the formula `period_close + lag_days`
is the fallback. The grace window is set generously enough that a source
publishing on its real schedule never reads `overdue`.
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

    `month_lag_days` is an optional per-*reference-month* lag override (keyed by
    calendar month 1–12) for sources whose formula lag varies seasonally. GACC
    uses it for its quarter-end months (see `_GACC`); every other source leaves
    it empty and takes the uniform `lag_days`.
    """

    lag_days: int
    grace_days: int
    exact: dict[date, date]
    month_lag_days: dict[int, int] = dataclasses.field(default_factory=dict)


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

# GACC preliminary ("Statistics Express" / 统计快讯) release. `exact` carries
# the official 2026 schedule from 海关总署公告2025年第240号 (posted 2026-03-11;
# see the provenance note in the module docstring). Reference month → the
# announcement's 快讯 date. Keyed like the release anchors: the Jan+Feb
# combined release sits on the FEBRUARY anchor (January candidates are
# remapped to February in expected_publish_date), and December's data
# publishes in January of the following year. Verified live: the June-2026
# reference month published 2026-07-14, exactly the entry below.
#
# Formula fallback for years without hand-entered dates: scheduled =
# period_close + 8 days ≈ the 8th of the following month, with a 4-day grace
# → due-by ~12th, EXCEPT quarter-end reference months (Mar/Jun/Sep/Dec — the
# quarterly cumulations) which run ~3–5 days later, landing ~the 13th–15th
# (month_lag_days=13; confirmed against per-page publication dates 2019–2026
# AND the official schedules — the false-OVERDUE that bit the routine on
# 2026-07-14 is documented in reference_gacc_quarter_end_cadence). A genuinely
# large slip still trips `overdue`, which is the signal we want.
_GACC = SourceCalendar(
    lag_days=8,
    grace_days=4,
    exact={
        # 公告2025年第240号 — 快讯 column. Reference month → publish date.
        date(2025, 12, 1): date(2026, 1, 14),
        date(2026, 2, 1): date(2026, 3, 10),   # Jan+Feb combined
        date(2026, 3, 1): date(2026, 4, 14),
        date(2026, 4, 1): date(2026, 5, 9),
        date(2026, 5, 1): date(2026, 6, 9),
        date(2026, 6, 1): date(2026, 7, 14),
        date(2026, 7, 1): date(2026, 8, 7),
        date(2026, 8, 1): date(2026, 9, 8),
        date(2026, 9, 1): date(2026, 10, 14),
        date(2026, 10, 1): date(2026, 11, 10),
        date(2026, 11, 1): date(2026, 12, 8),
    },
    month_lag_days={3: 13, 6: 13, 9: 13, 12: 13},
)

# GACC Monthly Bulletin (统计月报) — the VERIFIED vintage of the same data,
# always the 18th of the month after the reference month (公告2025年第240号
# 月刊 column; the 2025 year page confirms all 12 months landed on the 18th).
# Jan+Feb combine onto 18 March, so like _GACC the combined entry sits on the
# February anchor. NOT in CALENDARS: we don't scrape the Bulletin, so it must
# not acquire a probe expectation axis — it exists for the portal's
# publication-calendar table (via expected_publish_date, which consults
# _INFORMATIONAL too). lag_days=18 makes the formula fallback (period_close +
# 18 = the 18th of the following month) exactly the official rule, so future
# years need no hand-entry unless GACC changes the cadence.
_GACC_BULLETIN = SourceCalendar(
    lag_days=18,
    grace_days=4,
    exact={
        date(2025, 12, 1): date(2026, 1, 18),
        date(2026, 2, 1): date(2026, 3, 18),   # Jan+Feb combined
        date(2026, 3, 1): date(2026, 4, 18),
        date(2026, 4, 1): date(2026, 5, 18),
        date(2026, 5, 1): date(2026, 6, 18),
        date(2026, 6, 1): date(2026, 7, 18),
        date(2026, 7, 1): date(2026, 8, 18),
        date(2026, 8, 1): date(2026, 9, 18),
        date(2026, 9, 1): date(2026, 10, 18),
        date(2026, 10, 1): date(2026, 11, 18),
        date(2026, 11, 1): date(2026, 12, 18),
    },
)

# Sources with a probe expectation axis. GACC joined 2026-06-22; its official
# 2026 dates joined 2026-07-14 (公告2025年第240号). A slipped release surfaces
# on the --source-status OVERDUE line.
CALENDARS: dict[str, SourceCalendar] = {
    "eurostat": _EUROSTAT,
    "hmrc": _HMRC,
    "gacc": _GACC,
}

# Cadences we track for display (the portal's publication calendar) but do NOT
# scrape: no probe expectations, no --source-status line, not in has_calendar.
# expected_publish_date consults these too so display callers have one entry
# point.
_INFORMATIONAL: dict[str, SourceCalendar] = {
    "gacc_bulletin": _GACC_BULLETIN,
}


def has_calendar(source: str) -> bool:
    """True for sources that carry a probe expectation axis (eurostat, hmrc,
    gacc). Deliberately excludes informational cadences (gacc_bulletin)."""
    return source in CALENDARS


def expected_publish_date(source: str, period: date) -> date | None:
    """The scheduled publication date for `source`'s `period` data.

    Exact hand-entered calendar date if known, else the formula fallback
    (`period_close + lag_days`). None for sources without a calendar. Accepts
    the informational cadences (gacc_bulletin) as well as the probed sources.
    """
    d_official = _expected_with_provenance(source, period)
    return d_official[0] if d_official else None


def expected_publish_date_detail(
    source: str, period: date,
) -> tuple[date, bool] | None:
    """(scheduled date, is_official) for `source`'s `period` data.

    `is_official` is True when the date comes from a hand-entered `exact`
    entry (an official published schedule: Eurostat's G.3 calendar, HMRC's
    uktradeinfo calendar, GACC's annual 公告) and False when it is our formula
    estimate — the portal's publication-calendar table renders the two
    differently. None for unknown sources."""
    return _expected_with_provenance(source, period)


def _expected_with_provenance(
    source: str, period: date,
) -> tuple[date, bool] | None:
    cal = CALENDARS.get(source) or _INFORMATIONAL.get(source)
    if cal is None:
        return None
    anchor = period.replace(day=1)
    # GACC publishes no standalone January (Chinese New Year): January data
    # arrives folded into the Jan–Feb cumulative, which lands on February's
    # schedule (both vintages — the 快讯 on ~10 Mar and the Bulletin on
    # 18 Mar). Treat a January candidate as due on February's date so it
    # reads `none_expected` until the combined release is genuinely due —
    # without this it reads `overdue` every February while the routine's
    # candidate sits on January (next_period after December) waiting for a
    # release that, alone, never comes.
    if source in ("gacc", "gacc_bulletin") and anchor.month == 1:
        anchor = anchor.replace(month=2)
    if anchor in cal.exact:
        return cal.exact[anchor], True
    # Seasonal lag override (GACC quarter-end months) falls back to the uniform
    # lag_days for any month not listed. Applied after the January remap, so a
    # GACC January candidate (remapped to February, month 2) correctly takes the
    # base lag, not a quarter-end one.
    lag = cal.month_lag_days.get(anchor.month, cal.lag_days)
    return period_close(anchor) + timedelta(days=lag), False


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


def next_release_forecast(
    latest_by_source: dict[str, date | None],
    *,
    limit: int | None = 2,
) -> list[tuple[str, date]]:
    """Forecast each source's *next* upcoming release, soonest first.

    For every source that has a calendar and a known latest published period,
    the next candidate is `next_period(latest)` and its scheduled publication
    date is `expected_publish_date(...)`. Returns up to `limit` (source, due)
    pairs sorted ascending by due date — the data behind the
    "Next changes expected:" report line. Ties are broken by source name so the
    order is deterministic. `limit=None` returns every source.

    A source with no prior release (`latest` is None) or no calendar is
    skipped: with no period in the DB there is nothing to anchor the next
    candidate on.

    Pure — no DB, no network, no `today`: the next candidate is the month after
    the latest period already in hand, so its due date is fixed regardless of
    when we ask. The caller supplies `latest_by_source` (MAX(period) per source
    from the releases table).
    """
    forecasts: list[tuple[str, date]] = []
    for source, latest in latest_by_source.items():
        if latest is None or not has_calendar(source):
            continue
        due = expected_publish_date(source, next_period(latest))
        assert due is not None  # has_calendar(source) guarantees a date
        forecasts.append((source, due))
    forecasts.sort(key=lambda sd: (sd[1], sd[0]))
    return forecasts if limit is None else forecasts[:limit]
