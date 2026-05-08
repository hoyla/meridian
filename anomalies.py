"""Deterministic anomaly detection over observations.

The LLM never touches raw numbers. This module computes the actual stats and
writes findings rows; the LLM layer narrates them.

v1 anomaly types:
- mirror_gap: GACC's reported export to a partner vs Eurostat's reported import
  from China by that partner, in EUR. Computed at country-pair total level
  (GACC section 4 export totals × FX → EUR, vs Eurostat sum across all HS
  codes). Each finding cites the country-alias row used, the fx_rates row used,
  and the underlying observation_ids, plus a list of caveat codes journalists
  should weigh.

Future types (not yet implemented):
- yoy: same-month year-on-year change; z-score against history
- mom: month-on-month
- mix_substitution: shift in HS-code distribution within a category
- rank_shift: changes in partner-share rankings
"""

import logging
import math
import os
import re
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import date

import psycopg2
import psycopg2.extras

import lookups

log = logging.getLogger(__name__)

ANALYSIS_SOURCE_URL = "analysis://mirror_trade/v1"
TREND_ANALYSIS_SOURCE_URL = "analysis://mirror_gap_trends/v1"
HS_GROUP_TREND_SOURCE_URL = "analysis://hs_group_trends/v1"
HS_GROUP_TRAJECTORY_SOURCE_URL = "analysis://hs_group_trajectory/v1"

# Caveats every mirror_gap finding should cite by default. Specific findings can
# add more (e.g. 'reporting_lag' if periods don't align).
DEFAULT_MIRROR_GAP_CAVEATS = [
    "cif_fob",
    "currency_timing",
    "general_vs_special_trade",
    "transshipment",
    "eurostat_stat_procedure_mix",
]

# Expected baseline gap from CIF/FOB alone: Eurostat (CIF) typically reports 5-10%
# higher than GACC (FOB) for the same flow before any other effects. Below this
# the gap is unremarkable; above is a candidate for editorial attention.
CIF_FOB_BASELINE_PCT = 0.075


def _conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


@dataclass
class _MirrorGapResult:
    period: date
    gacc_partner_label: str
    iso2: str                      # ISO-2 for single-country, 'BLOC:{kind}' for aggregates
    gacc_obs_id: int
    gacc_value_raw: float          # in unit-currency (e.g. CNY 100 Million)
    gacc_value_currency: str
    gacc_unit_scale: float         # multiplier to get raw currency (e.g. 1e8 for "CNY 100 Million")
    gacc_value_eur: float
    eurostat_total_eur: float
    eurostat_obs_ids: list[int]
    eurostat_n_hs_codes: int
    fx_rate_id: int
    fx_rate: float
    fx_rate_date: date
    alias_id: int
    gap_eur: float                 # eurostat - gacc_eur (positive = EU reports more)
    gap_pct: float                 # gap_eur / max(both)
    excess_over_cif_fob_baseline_pct: float
    # Aggregate-specific (None for single-country comparisons)
    aggregate_kind: str | None = None
    aggregate_members: list[str] | None = None
    aggregate_sources: list[str] | None = None


_UNIT_RE = re.compile(r"^([A-Z]{3})(?:\s+(\d+(?:[.,]\d+)?))?(?:\s+(Thousand|Million|Billion))?\s*$")


def parse_unit_scale(unit: str | None) -> tuple[float, str | None]:
    """Parse a release.unit string like 'CNY 100 Million' into (multiplier, currency).
    Returns (1.0, None) for missing/unrecognised units."""
    if not unit:
        return 1.0, None
    m = _UNIT_RE.match(unit.strip())
    if not m:
        log.warning("Unrecognised unit string %r — assuming raw amount", unit)
        return 1.0, None
    currency, magnitude_str, scale_word = m.groups()
    multiplier = 1.0
    if magnitude_str:
        multiplier *= float(magnitude_str.replace(",", ""))
    if scale_word == "Thousand":
        multiplier *= 1_000
    elif scale_word == "Million":
        multiplier *= 1_000_000
    elif scale_word == "Billion":
        multiplier *= 1_000_000_000
    return multiplier, currency


def detect_mirror_trade_gaps(period: date | None = None) -> dict[str, int]:
    """Compare GACC China-export-to-X to Eurostat X-import-from-China for each
    overlapping (period, partner) pair. Each comparison emits a findings row of
    kind='anomaly', subkind='mirror_gap'.

    Args:
        period: if specified, only analyse that period; otherwise all periods
                that have GACC data.

    Returns counts: {'emitted', 'skipped_no_eurostat', 'skipped_no_fx',
                     'skipped_aggregate', 'skipped_unmapped', 'skipped_no_value'}.
    """
    counts = {
        "emitted": 0,
        "skipped_no_eurostat": 0,
        "skipped_no_fx": 0,
        "skipped_aggregate_no_members": 0,
        "skipped_aggregate_no_eurostat_counterpart": 0,
        "skipped_unmapped": 0,
        "skipped_no_value": 0,
    }

    # One scrape_run per analysis call — gives the resulting findings a consistent
    # FK so journalists can group "what came out of this analysis pass".
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'running') RETURNING id",
            (ANALYSIS_SOURCE_URL,),
        )
        analysis_run_id = cur.fetchone()[0]

    try:
        gacc_rows = _select_gacc_export_rows(period)
        for gr in gacc_rows:
            result = _compute_one_gap(gr)
            if isinstance(result, str):
                counts[result] += 1
                continue
            _insert_finding(analysis_run_id, result)
            counts["emitted"] += 1
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='success', ended_at=now() WHERE id=%s",
                (analysis_run_id,),
            )
    except Exception as e:
        log.exception("Mirror-gap analysis failed")
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='failed', error_message=%s, ended_at=now() WHERE id=%s",
                (str(e), analysis_run_id),
            )
        raise
    return counts


def _select_gacc_export_rows(period: date | None) -> list[dict]:
    where = ""
    params: tuple = ()
    if period:
        where = "AND r.period = %s"
        params = (period,)
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                o.id           AS obs_id,
                o.partner_country,
                o.value_amount,
                o.value_currency,
                r.period,
                r.unit
              FROM observations o
              JOIN releases r ON r.id = o.release_id
             WHERE r.source = 'gacc'
               AND o.flow = 'export'
               AND o.period_kind = 'monthly'
               AND o.partner_country != 'Total'
               AND o.value_amount IS NOT NULL
               {where}
          ORDER BY r.period, o.partner_country
            """,
            params,
        )
        return [dict(r) for r in cur.fetchall()]


def _compute_one_gap(gr: dict) -> _MirrorGapResult | str:
    """Returns the result, OR a sentinel string naming the skip reason for counts."""
    if gr["value_amount"] is None or float(gr["value_amount"]) == 0:
        return "skipped_no_value"

    resolved = lookups.resolve_country("gacc", gr["partner_country"])
    if resolved is None:
        log.info("Unmapped GACC partner label: %r", gr["partner_country"])
        return "skipped_unmapped"

    period = gr["period"]

    # Branch: single country vs aggregate.
    aggregate_members: list[str] | None = None
    aggregate_kind: str | None = None
    aggregate_sources: list[str] | None = None

    if resolved.iso2 is None:
        # Aggregate label. We can only compare against Eurostat if the bloc has
        # an EU-side equivalent — i.e. its members are EU reporter codes. For
        # ASEAN, RCEP, Latin America, Africa, Belt&Road etc. we'd need a different
        # source (UN Comtrade or similar) and skip for now.
        membership = lookups.lookup_aggregate_members(resolved.alias_id, period=period)
        if membership is None:
            return "skipped_aggregate_no_members"
        # Only the eu_bloc has Eurostat counterparts under our current source set.
        if membership.aggregate_kind != "eu_bloc":
            return "skipped_aggregate_no_eurostat_counterpart"
        aggregate_members = membership.members_iso2
        aggregate_kind = membership.aggregate_kind
        aggregate_sources = membership.sources
        eurostat_total, eurostat_ids, n_hs = _eurostat_aggregate_for_members(period, aggregate_members)
        result_iso2 = f"BLOC:{membership.aggregate_kind}"
    else:
        eurostat_total, eurostat_ids, n_hs = _eurostat_aggregate_for(period, resolved.iso2)
        result_iso2 = resolved.iso2

    if eurostat_total is None:
        return "skipped_no_eurostat"

    unit_scale, unit_currency = parse_unit_scale(gr["unit"])
    currency_for_fx = unit_currency or gr["value_currency"]
    fx = lookups.lookup_fx(currency_for_fx, "EUR", period)
    if fx is None:
        return "skipped_no_fx"

    gacc_raw_currency = float(gr["value_amount"]) * unit_scale
    gacc_value_eur = gacc_raw_currency * fx.rate
    gap_eur = float(eurostat_total) - gacc_value_eur
    larger = max(abs(gacc_value_eur), abs(float(eurostat_total)))
    gap_pct = gap_eur / larger if larger else 0.0
    excess = abs(gap_pct) - CIF_FOB_BASELINE_PCT

    return _MirrorGapResult(
        period=period,
        gacc_partner_label=gr["partner_country"],
        iso2=result_iso2,
        gacc_obs_id=gr["obs_id"],
        gacc_value_raw=float(gr["value_amount"]),
        gacc_value_currency=currency_for_fx,
        gacc_unit_scale=unit_scale,
        gacc_value_eur=gacc_value_eur,
        eurostat_total_eur=float(eurostat_total),
        eurostat_obs_ids=eurostat_ids,
        eurostat_n_hs_codes=n_hs,
        fx_rate_id=fx.rate_id,
        fx_rate=fx.rate,
        fx_rate_date=fx.rate_date,
        alias_id=resolved.alias_id,
        gap_eur=gap_eur,
        gap_pct=gap_pct,
        excess_over_cif_fob_baseline_pct=excess,
        aggregate_kind=aggregate_kind,
        aggregate_members=aggregate_members,
        aggregate_sources=aggregate_sources,
    )


def _eurostat_aggregate_for(period: date, iso2: str) -> tuple[float | None, list[int], int]:
    return _eurostat_aggregate_for_members(period, [iso2])


def _eurostat_aggregate_for_members(
    period: date, member_iso2s: list[str]
) -> tuple[float | None, list[int], int]:
    """Sum Eurostat imports from CN across the given list of EU member ISO-2 codes
    for the given period. Returns (total_eur, obs_ids, n_obs)."""
    if not member_iso2s:
        return None, [], 0
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COALESCE(SUM(o.value_amount), 0) AS total_eur,
                COALESCE(ARRAY_AGG(o.id ORDER BY o.id), '{}') AS obs_ids,
                COUNT(*) AS n_obs
              FROM observations o
              JOIN releases r ON r.id = o.release_id
             WHERE r.source = 'eurostat'
               AND r.period = %s
               AND o.flow = 'import'
               AND o.reporter_country = ANY(%s)
               AND o.partner_country = 'CN'
            """,
            (period, member_iso2s),
        )
        total, ids, n = cur.fetchone()
    if n == 0:
        return None, [], 0
    return total, list(ids), n


def _insert_finding(analysis_run_id: int, r: _MirrorGapResult) -> None:
    direction = "Eurostat > GACC" if r.gap_eur > 0 else "GACC > Eurostat"
    is_aggregate = r.aggregate_kind is not None
    label = f"{r.aggregate_kind} ({len(r.aggregate_members or [])} members)" if is_aggregate else r.iso2
    eurostat_descriptor = (
        f"the {len(r.aggregate_members)} EU members ({', '.join(r.aggregate_members)})"
        if is_aggregate else r.iso2
    )

    title = (
        f"Mirror-trade gap, China ↔ {label}, {r.period.strftime('%Y-%m')}: "
        f"GACC reports €{r.gacc_value_eur:,.0f}, Eurostat reports €{r.eurostat_total_eur:,.0f} "
        f"({r.gap_pct*100:+.1f}%, {direction})"
    )
    body = (
        f"GACC: China's reported {r.gacc_value_raw:,.1f} ({r.gacc_value_currency} ×{r.gacc_unit_scale:,.0f}) "
        f"export to '{r.gacc_partner_label}', converted at the ECB "
        f"{r.gacc_value_currency}/EUR rate of {r.fx_rate:.6f} for {r.fx_rate_date.strftime('%Y-%m')}, "
        f"= €{r.gacc_value_eur:,.0f}.\n\n"
        f"Eurostat: imports from CN summed across {r.eurostat_n_hs_codes:,} HS-CN8 "
        f"observations from {eurostat_descriptor} = €{r.eurostat_total_eur:,.0f}.\n\n"
        f"Gap: €{r.gap_eur:,.0f} ({r.gap_pct*100:+.1f}% of larger value). "
        f"CIF/FOB baseline expects ~{CIF_FOB_BASELINE_PCT*100:.0f}% Eurostat-higher; "
        f"excess over baseline is {r.excess_over_cif_fob_baseline_pct*100:+.1f} percentage points."
    )
    if is_aggregate:
        body += (
            f"\n\nThis is an aggregate-to-aggregate comparison. GACC's '{r.gacc_partner_label}' "
            f"label is matched to Eurostat by summing the {len(r.aggregate_members)} member-state "
            f"reporters. The 'aggregate_composition' caveat applies: the GACC-side bloc "
            f"definition (per release footnote) and the Eurostat-side reporter set may differ in "
            f"composition or as-of date. Sources cited: {', '.join(r.aggregate_sources or [])}."
        )

    caveat_codes = list(DEFAULT_MIRROR_GAP_CAVEATS)
    if is_aggregate:
        caveat_codes.append("aggregate_composition")

    detail = {
        "method": "mirror_trade_v1",
        # Caveat codes — journalists should weigh these when interpreting the gap.
        # Promote to a dedicated findings.caveat_codes column when the schema
        # gets its first migration after the lookups went in.
        "caveat_codes": caveat_codes,
        "is_aggregate": is_aggregate,
        "aggregate": {
            "kind": r.aggregate_kind,
            "members_iso2": r.aggregate_members,
            "n_members": len(r.aggregate_members) if r.aggregate_members else 0,
            "sources": r.aggregate_sources,
        } if is_aggregate else None,
        "gacc": {
            "obs_id": r.gacc_obs_id,
            "partner_label_raw": r.gacc_partner_label,
            "value_raw": r.gacc_value_raw,
            "currency": r.gacc_value_currency,
            "unit_scale": r.gacc_unit_scale,
            "value_eur_converted": r.gacc_value_eur,
        },
        "eurostat": {
            "obs_ids_count": len(r.eurostat_obs_ids),
            "n_hs_codes": r.eurostat_n_hs_codes,
            "total_eur": r.eurostat_total_eur,
        },
        "fx": {
            "rate_id": r.fx_rate_id,
            "rate": r.fx_rate,
            "rate_date": r.fx_rate_date.isoformat(),
            "from_currency": r.gacc_value_currency,
            "to_currency": "EUR",
        },
        "country_alias_id": r.alias_id,
        "iso2": r.iso2,
        "gap_eur": r.gap_eur,
        "gap_pct": r.gap_pct,
        "cif_fob_baseline_pct": CIF_FOB_BASELINE_PCT,
        "excess_over_baseline_pct": r.excess_over_cif_fob_baseline_pct,
    }
    score = abs(r.gap_pct) if r.gap_pct is not None else None
    obs_ids = [r.gacc_obs_id] + r.eurostat_obs_ids

    import json
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO findings (
                scrape_run_id, kind, subkind, observation_ids, score,
                title, body, detail
            ) VALUES (
                %s, 'anomaly', 'mirror_gap', %s, %s, %s, %s, %s
            )
            """,
            (analysis_run_id, obs_ids, score, title, body, json.dumps(detail)),
        )


# =============================================================================
# Trend / time-series anomaly detection over the mirror_gap series itself.
# =============================================================================
# The structural mirror-gap (e.g. NL ~65% Eurostat-higher, IT ~70%) is a known
# fact of EU-China trade reporting and isn't itself news. The story is when
# that gap *moves*: a partner whose gap was steady and suddenly shifts is the
# kind of thing a desk wants flagged. This module computes a rolling-baseline
# z-score of each (iso2, period) gap_pct against its prior `window_months`
# of values, and emits 'mirror_gap_zscore' findings where |z| > threshold.


@dataclass
class _GapPoint:
    finding_id: int
    iso2: str
    period: date
    gap_pct: float
    observation_ids: list[int]


def _select_latest_mirror_gap_series(period_filter: date | None) -> list[_GapPoint]:
    """Return the latest mirror_gap finding's gap_pct per (iso2, period). If a
    period_filter is given, the SERIES still spans all periods (we need the
    history for the baseline) — the period_filter only restricts which periods
    we *generate trend findings for* downstream, not what we read here."""
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (f.detail->>'iso2', r.period)
                f.id              AS finding_id,
                f.detail->>'iso2' AS iso2,
                r.period          AS period,
                (f.detail->>'gap_pct')::numeric AS gap_pct,
                f.observation_ids
              FROM findings f
              JOIN observations o ON o.id = f.observation_ids[1]
              JOIN releases r     ON r.id = o.release_id
             WHERE f.subkind = 'mirror_gap'
          ORDER BY f.detail->>'iso2', r.period, f.created_at DESC
            """
        )
        rows = cur.fetchall()
    return [
        _GapPoint(
            finding_id=row["finding_id"], iso2=row["iso2"], period=row["period"],
            gap_pct=float(row["gap_pct"]),
            observation_ids=list(row["observation_ids"] or []),
        )
        for row in rows
    ]


def detect_mirror_gap_trends(
    window_months: int = 6,
    period: date | None = None,
    z_threshold: float = 1.5,
    min_baseline_n: int = 3,
) -> dict[str, int]:
    """Compute rolling z-score of each (iso2, period) gap_pct against the prior
    `window_months` for the same iso2. Emit a 'mirror_gap_zscore' finding when
    |z| >= z_threshold. Below threshold, the period-iso2 pair is silently
    skipped (so the findings table stays signal-only).

    The structural baseline gap is partner-specific (NL ~65% Eurostat-higher
    is normal; a sudden jump to 80% is the news), so we baseline per-iso2
    rather than across all partners.

    Args:
        window_months: rolling baseline length (default 6).
        period: if given, only generate trend findings for that period; the
                baseline always uses the full prior history available.
        z_threshold: minimum |z| to emit a finding (default 1.5 — generous;
                     tune up as more history accrues).
        min_baseline_n: skip if baseline has fewer than this many points.

    Returns counts: {'emitted', 'skipped_insufficient_baseline',
                     'skipped_zero_stdev', 'skipped_below_threshold'}.
    """
    counts = {
        "emitted": 0, "skipped_insufficient_baseline": 0,
        "skipped_zero_stdev": 0, "skipped_below_threshold": 0,
    }

    series_all = _select_latest_mirror_gap_series(period_filter=None)
    if not series_all:
        return counts

    by_iso2: dict[str, list[_GapPoint]] = defaultdict(list)
    for p in series_all:
        by_iso2[p.iso2].append(p)
    for points in by_iso2.values():
        points.sort(key=lambda p: p.period)

    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'running') RETURNING id",
            (TREND_ANALYSIS_SOURCE_URL,),
        )
        analysis_run_id = cur.fetchone()[0]

    try:
        for iso2, points in by_iso2.items():
            for i, point in enumerate(points):
                if period is not None and point.period != period:
                    continue
                baseline = points[max(0, i - window_months):i]
                if len(baseline) < min_baseline_n:
                    counts["skipped_insufficient_baseline"] += 1
                    continue
                baseline_pcts = [b.gap_pct for b in baseline]
                mean = statistics.mean(baseline_pcts)
                stdev = statistics.stdev(baseline_pcts) if len(baseline_pcts) > 1 else 0.0
                if stdev == 0:
                    counts["skipped_zero_stdev"] += 1
                    continue
                z = (point.gap_pct - mean) / stdev
                if abs(z) < z_threshold:
                    counts["skipped_below_threshold"] += 1
                    continue
                _insert_zscore_finding(analysis_run_id, point, baseline, mean, stdev, z, window_months)
                counts["emitted"] += 1

        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='success', ended_at=now() WHERE id=%s",
                (analysis_run_id,),
            )
    except Exception as e:
        log.exception("Mirror-gap trend analysis failed")
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='failed', error_message=%s, ended_at=now() WHERE id=%s",
                (str(e), analysis_run_id),
            )
        raise
    return counts


def _insert_zscore_finding(
    analysis_run_id: int,
    point: _GapPoint,
    baseline: list[_GapPoint],
    mean: float,
    stdev: float,
    z: float,
    window_months: int,
) -> None:
    direction = "above" if z > 0 else "below"
    title = (
        f"Mirror-gap shift, China ↔ {point.iso2}, {point.period.strftime('%Y-%m')}: "
        f"{point.gap_pct*100:+.1f}% gap is {abs(z):.1f}σ {direction} "
        f"the {len(baseline)}-month baseline mean of {mean*100:+.1f}%"
    )
    body = (
        f"The mirror-trade gap for {point.iso2} in {point.period.strftime('%Y-%m')} "
        f"({point.gap_pct*100:+.1f}%) is {abs(z):.2f} standard deviations {direction} "
        f"the rolling {window_months}-month baseline (mean {mean*100:+.2f}%, "
        f"stdev {stdev*100:.2f}%, n={len(baseline)} prior periods).\n\n"
        f"Underlying mirror_gap finding id: {point.finding_id}.\n"
        f"Baseline window periods: {baseline[0].period.strftime('%Y-%m')} → "
        f"{baseline[-1].period.strftime('%Y-%m')}.\n\n"
        f"Caveats inherited from the underlying mirror_gap finding apply; the "
        f"'currency_timing' caveat is especially relevant here because we use "
        f"the ECB monthly average rate for the period being scored."
    )
    detail = {
        "method": "mirror_gap_zscore_v1",
        "iso2": point.iso2,
        "period": point.period.isoformat(),
        "gap_pct": point.gap_pct,
        "z_score": z,
        "baseline": {
            "window_months": window_months,
            "n": len(baseline),
            "mean": mean,
            "stdev": stdev,
            "first_period": baseline[0].period.isoformat(),
            "last_period": baseline[-1].period.isoformat(),
            "values": [{"period": b.period.isoformat(), "gap_pct": b.gap_pct} for b in baseline],
        },
        "underlying_mirror_gap_finding_id": point.finding_id,
        # Caveats inherited; promote to a column when the schema gets its
        # first migration.
        "caveat_codes": DEFAULT_MIRROR_GAP_CAVEATS + ["aggregate_composition_drift"],
    }
    score = abs(z)

    import json
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO findings (
                scrape_run_id, kind, subkind, observation_ids, score,
                title, body, detail
            ) VALUES (
                %s, 'anomaly', 'mirror_gap_zscore', %s, %s, %s, %s, %s
            )
            """,
            (analysis_run_id, point.observation_ids, score, title, body, json.dumps(detail)),
        )


# =============================================================================
# HS-group component-trend analysis.
# =============================================================================
# For each (hs_group, period) we compute the rolling 12-month total of EU
# imports from CN matching the group's hs_patterns, compare to the prior
# 12-month rolling total, and emit a 'hs_group_yoy' finding when |YoY| >=
# threshold. detail.* carries the full method (which patterns, which months,
# which CN8 codes contributed most, which EU reporters imported most) so the
# finding is auditable end-to-end.
#
# This module DOES NOT bake the observation_ids[] for every contributing
# Eurostat row into each finding — chapter-wide groups (84+85, etc.) match
# millions of rows. Instead the finding records the SQL query definition
# (patterns, period window, partner, flow) that produced the totals, so a
# journalist or downstream tool can re-derive evidence on demand.


@dataclass
class _HsGroup:
    id: int
    name: str
    description: str | None
    hs_patterns: list[str]


def _list_hs_groups(group_names: list[str] | None = None) -> list[_HsGroup]:
    sql = "SELECT id, name, description, hs_patterns FROM hs_groups"
    params: tuple = ()
    if group_names:
        sql += " WHERE name = ANY(%s)"
        params = (group_names,)
    sql += " ORDER BY id"
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, params)
        return [_HsGroup(id=r["id"], name=r["name"], description=r["description"],
                         hs_patterns=list(r["hs_patterns"] or [])) for r in cur.fetchall()]


def _hs_group_per_period_totals(
    patterns: list[str],
) -> list[tuple[date, float, float, int]]:
    """Returns (period, total_eur, total_kg, n_raw_rows) per period for Eurostat
    imports from CN matching any of the given hs_patterns.

    Queries eurostat_raw_rows rather than observations because raw_rows preserves
    quantity_kg as a native column. The aggregated `observations.quantity` field
    holds either the supplementary unit (PST, kg, etc.) or kg as fallback, which
    means cross-HS kg totals from observations would silently drop the kg of
    rows whose primary unit is something else (pieces, litres, etc.).

    Periods with no matching obs are absent from the result — caller handles gaps.
    """
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT period,
                   SUM(value_eur) AS total_eur,
                   SUM(quantity_kg) AS total_kg,
                   COUNT(*) AS n_raw
              FROM eurostat_raw_rows
             WHERE flow = 1
               AND partner = 'CN'
               AND product_nc LIKE ANY(%s)
          GROUP BY period
          ORDER BY period
            """,
            (patterns,),
        )
        return [(row[0], float(row[1] or 0), float(row[2] or 0), int(row[3]))
                for row in cur.fetchall()]


def _hs_group_top_cn8s(patterns: list[str], start: date, end: date, limit: int = 10) -> list[dict]:
    """Top contributing HS-CN8 codes within a group across [start, end]."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT product_nc,
                   SUM(value_eur) AS total_eur,
                   SUM(quantity_kg) AS total_kg,
                   COUNT(*) AS n_raw
              FROM eurostat_raw_rows
             WHERE period >= %s AND period <= %s
               AND flow = 1 AND partner = 'CN'
               AND product_nc LIKE ANY(%s)
          GROUP BY product_nc
          ORDER BY SUM(value_eur) DESC NULLS LAST
             LIMIT %s
            """,
            (start, end, patterns, limit),
        )
        return [{"hs_code": r[0], "total_eur": float(r[1] or 0),
                 "total_kg": float(r[2] or 0), "n_raw": int(r[3])}
                for r in cur.fetchall()]


def _hs_group_top_reporters(patterns: list[str], start: date, end: date, limit: int = 10) -> list[dict]:
    """Top contributing EU reporters within a group across [start, end]."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT reporter,
                   SUM(value_eur) AS total_eur,
                   SUM(quantity_kg) AS total_kg,
                   COUNT(*) AS n_raw
              FROM eurostat_raw_rows
             WHERE period >= %s AND period <= %s
               AND flow = 1 AND partner = 'CN'
               AND product_nc LIKE ANY(%s)
          GROUP BY reporter
          ORDER BY SUM(value_eur) DESC NULLS LAST
             LIMIT %s
            """,
            (start, end, patterns, limit),
        )
        return [{"reporter": r[0], "total_eur": float(r[1] or 0),
                 "total_kg": float(r[2] or 0), "n_raw": int(r[3])}
                for r in cur.fetchall()]


def _months_back(period: date, n: int) -> date:
    """Subtract n months from a first-of-month date."""
    total = period.year * 12 + (period.month - 1) - n
    return date(total // 12, total % 12 + 1, 1)


def detect_hs_group_yoy(
    group_names: list[str] | None = None,
    yoy_threshold_pct: float = 0.0,
) -> dict[str, int]:
    """For each (hs_group, period_t) where 24 months of history exist, compute:
        current_12mo  = sum(value_amount) for periods [t-11 .. t]
        prior_12mo    = sum(value_amount) for periods [t-23 .. t-12]
        yoy_pct       = (current_12mo - prior_12mo) / abs(prior_12mo)
    Emit a finding when |yoy_pct| >= yoy_threshold_pct. Default 0.0 means emit
    one finding per (group, period) — useful for a 'current state' snapshot.

    Returns counts: {'emitted', 'skipped_insufficient_history', 'skipped_below_threshold', 'skipped_zero_prior'}.
    """
    counts = {
        "emitted": 0, "skipped_insufficient_history": 0,
        "skipped_below_threshold": 0, "skipped_zero_prior": 0,
    }

    groups = _list_hs_groups(group_names)
    if not groups:
        return counts

    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'running') RETURNING id",
            (HS_GROUP_TREND_SOURCE_URL,),
        )
        analysis_run_id = cur.fetchone()[0]

    try:
        for group in groups:
            series = _hs_group_per_period_totals(group.hs_patterns)
            eur_by_period: dict[date, float] = {p: e for p, e, _, _ in series}
            kg_by_period:  dict[date, float] = {p: k for p, _, k, _ in series}
            n_by_period:   dict[date, int]   = {p: n for p, _, _, n in series}
            if not series:
                counts["skipped_insufficient_history"] += 1
                continue

            periods_sorted = sorted(eur_by_period.keys())

            # Walk possible 'current month' anchors. Need 24 months ending at t.
            for t in periods_sorted:
                start_curr = _months_back(t, 11)
                end_curr   = t
                start_prior = _months_back(t, 23)
                end_prior   = _months_back(t, 12)

                # All 24 months must be present in the data for a clean window.
                want = []
                p = start_prior
                while p <= end_curr:
                    want.append(p)
                    p = _months_back(p, -1)
                if not all(p in eur_by_period for p in want):
                    counts["skipped_insufficient_history"] += 1
                    continue

                current_eur = sum(eur_by_period[p] for p in want if start_curr <= p <= end_curr)
                prior_eur   = sum(eur_by_period[p] for p in want if start_prior <= p <= end_prior)
                current_kg  = sum(kg_by_period[p]  for p in want if start_curr <= p <= end_curr)
                prior_kg    = sum(kg_by_period[p]  for p in want if start_prior <= p <= end_prior)

                if prior_eur == 0:
                    counts["skipped_zero_prior"] += 1
                    continue
                yoy_pct_eur = (current_eur - prior_eur) / abs(prior_eur)
                yoy_pct_kg  = ((current_kg - prior_kg) / abs(prior_kg)) if prior_kg else None

                # Keep the threshold gating on the EUR YoY (the editorial-relevance
                # signal). Even if kg YoY is small, big EUR moves matter.
                if abs(yoy_pct_eur) < yoy_threshold_pct:
                    counts["skipped_below_threshold"] += 1
                    continue

                top_cn8s = _hs_group_top_cn8s(group.hs_patterns, start_curr, end_curr)
                top_reporters = _hs_group_top_reporters(group.hs_patterns, start_curr, end_curr)
                _insert_hs_group_yoy_finding(
                    analysis_run_id, group, t, start_curr, end_curr,
                    start_prior, end_prior,
                    current_eur, prior_eur, yoy_pct_eur,
                    current_kg,  prior_kg,  yoy_pct_kg,
                    series, top_cn8s, top_reporters, n_by_period,
                )
                counts["emitted"] += 1

        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='success', ended_at=now() WHERE id=%s",
                (analysis_run_id,),
            )
    except Exception as e:
        log.exception("HS-group trend analysis failed")
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='failed', error_message=%s, ended_at=now() WHERE id=%s",
                (str(e), analysis_run_id),
            )
        raise

    return counts


def _insert_hs_group_yoy_finding(
    analysis_run_id: int,
    group: _HsGroup,
    anchor_period: date,
    start_curr: date,
    end_curr: date,
    start_prior: date,
    end_prior: date,
    current_eur: float,
    prior_eur: float,
    yoy_pct_eur: float,
    current_kg: float,
    prior_kg: float,
    yoy_pct_kg: float | None,
    series: list[tuple[date, float, float, int]],
    top_cn8s: list[dict],
    top_reporters: list[dict],
    n_obs_by_period: dict[date, int],
) -> None:
    direction = "up" if yoy_pct_eur > 0 else "down"
    kg_yoy_str = f"{yoy_pct_kg*100:+.1f}%" if yoy_pct_kg is not None else "n/a"
    unit_price_curr = (current_eur / current_kg) if current_kg else None
    unit_price_prior = (prior_eur / prior_kg) if prior_kg else None
    unit_price_pct = (
        ((unit_price_curr - unit_price_prior) / abs(unit_price_prior))
        if unit_price_curr is not None and unit_price_prior is not None and unit_price_prior != 0
        else None
    )

    title = (
        f"Component trend: {group.name}, rolling 12mo to {end_curr.strftime('%Y-%m')}: "
        f"€{current_eur/1e9:,.2f}B from CN ({yoy_pct_eur*100:+.1f}% {direction} value, "
        f"{kg_yoy_str} kg)"
    )

    # Decompose value change into volume × price effects so the "is it shipping
    # more or just charging more" question is answerable inline.
    body_lines = [
        f"Group: {group.name}",
        f"Definition: HS-CN8 codes matching {group.hs_patterns}",
        "",
        f"Rolling 12 months ending {end_curr.strftime('%Y-%m')}:",
        f"  Value:    €{current_eur:,.0f} ({yoy_pct_eur*100:+.2f}% YoY vs €{prior_eur:,.0f})",
        f"  Quantity: {current_kg:,.0f} kg ({kg_yoy_str} YoY vs {prior_kg:,.0f} kg)",
    ]
    if unit_price_curr is not None and unit_price_prior is not None:
        body_lines.append(
            f"  Unit price: €{unit_price_curr:,.4f}/kg current vs €{unit_price_prior:,.4f}/kg prior"
            f" ({unit_price_pct*100:+.2f}% change)"
        )
        if abs(yoy_pct_eur) > 0.01 and unit_price_pct is not None:
            # Decomposition note — this is what tells the journalist
            # whether a value rise is volume-driven or price-driven.
            volume_share = (yoy_pct_kg / yoy_pct_eur) if yoy_pct_kg is not None and yoy_pct_eur else None
            if volume_share is not None:
                body_lines.append(
                    f"  Decomposition: {'volume' if abs(volume_share) > 0.5 else 'price'}-driven "
                    f"(kg YoY contributes ~{volume_share*100:.0f}% of value YoY)."
                )
    body_lines.append("")
    body_lines.append("Top 5 contributing HS-CN8 codes in the rolling 12mo window:")
    for c in top_cn8s[:5]:
        unit_str = (
            f" (€{c['total_eur']/c['total_kg']:,.2f}/kg)" if c.get("total_kg") else ""
        )
        body_lines.append(f"  {c['hs_code']}: €{c['total_eur']:,.0f}, {c['total_kg']:,.0f} kg{unit_str}")
    body_lines.append("")
    body_lines.append("Top 5 importing EU members in the rolling 12mo window:")
    for r in top_reporters[:5]:
        body_lines.append(f"  {r['reporter']}: €{r['total_eur']:,.0f}, {r['total_kg']:,.0f} kg")
    body_lines.append("")
    body_lines.append(
        "Caveats applicable: 'cif_fob' (Eurostat reports imports CIF), 'classification_drift' "
        "(CN8 sub-headings can be ambiguous), 'eurostat_stat_procedure_mix' (totals sum across "
        "tariff regimes; see eurostat_raw_rows for the breakdown). Unit prices computed as "
        "value/kg from raw rows."
    )

    detail = {
        "method": "hs_group_yoy_v2_with_kg",
        "method_query": {
            "source": "eurostat_raw_rows", "flow": 1, "partner": "CN",
            "hs_patterns": group.hs_patterns,
            "rolling_window_months": 12,
        },
        "group": {
            "id": group.id, "name": group.name, "description": group.description,
            "hs_patterns": group.hs_patterns,
        },
        "windows": {
            "current_start": start_curr.isoformat(), "current_end": end_curr.isoformat(),
            "prior_start": start_prior.isoformat(), "prior_end": end_prior.isoformat(),
        },
        "totals": {
            "current_12mo_eur": current_eur,
            "prior_12mo_eur": prior_eur,
            "delta_eur": current_eur - prior_eur,
            "yoy_pct": yoy_pct_eur,
            "current_12mo_kg": current_kg,
            "prior_12mo_kg": prior_kg,
            "delta_kg": current_kg - prior_kg,
            "yoy_pct_kg": yoy_pct_kg,
            "current_unit_price_eur_per_kg": unit_price_curr,
            "prior_unit_price_eur_per_kg": unit_price_prior,
            "unit_price_pct_change": unit_price_pct,
        },
        "monthly_series": [
            {"period": p.isoformat(), "value_eur": e, "quantity_kg": k,
             "unit_price_eur_per_kg": (e / k) if k else None,
             "n_raw_rows": n_obs_by_period.get(p, 0)}
            for (p, e, k, _) in series
            if start_prior <= p <= end_curr
        ],
        "top_cn8_codes_in_current_12mo": top_cn8s,
        "top_reporters_in_current_12mo": top_reporters,
        "caveat_codes": [
            "cif_fob", "currency_timing", "classification_drift",
            "eurostat_stat_procedure_mix",
        ],
    }
    # Score reflects EUR YoY (the editorial-comparable signal across groups).
    score = abs(yoy_pct_eur)

    import json
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO findings (
                scrape_run_id, kind, subkind, observation_ids, hs_group_ids,
                score, title, body, detail
            ) VALUES (
                %s, 'anomaly', 'hs_group_yoy', %s, %s, %s, %s, %s, %s
            )
            """,
            (analysis_run_id, [], [group.id], score, title,
             "\n".join(body_lines), json.dumps(detail)),
        )


# =============================================================================
# Trajectory-shape classification of the HS-group YoY series.
# =============================================================================
# A single per-window YoY says "+16.9%". A trajectory says "U-shape recovery,
# decelerating from +48% peak in Jul 2025." The latter is the framing a
# journalist actually writes; this analyser reads existing hs_group_yoy
# findings, classifies the shape, and emits one 'hs_group_trajectory' finding
# per group with the supporting feature stats.

# Shape vocabulary. Each is human-readable and journalist-citable.
SHAPE_LABELS = {
    "flat":                  "flat",
    "rising":                "sustained rising",
    "rising_accelerating":   "rising, accelerating",
    "rising_decelerating":   "rising, decelerating",
    "falling":               "sustained falling",
    "falling_accelerating":  "falling, accelerating",
    "falling_decelerating":  "falling, decelerating",
    "u_recovery":            "U-shape recovery (was falling, now rising)",
    "inverse_u_peak":        "peak-and-fall (was rising, now falling)",
    "volatile":              "volatile (multiple direction changes)",
    "insufficient_data":     "insufficient data to classify",
}

# Thresholds — tuned for our scale of YoY values (typically -0.5..+1.0).
# These are conservative defaults; explained explicitly in the finding body
# so a journalist can see why a series was/wasn't classified a given way.
TRAJECTORY_FLAT_MEAN_ABS_YOY = 0.02   # mean |YoY| < 2% → flat
TRAJECTORY_FLAT_STDEV = 0.05          #   AND stdev < 5%
TRAJECTORY_SLOPE_SIGNIFICANT = 0.005  # |slope| > 0.5pp/window → meaningful direction in YoY
TRAJECTORY_MIN_WINDOWS = 6            # fewer than this can't classify reliably
TRAJECTORY_SMOOTH_WINDOW = 3          # centered moving-average window for shape detection
                                      # — suppresses 1-period zero-crossing flickers that
                                      # would otherwise trigger spurious 'volatile' labels.


def _linear_slope(xs: list[float], ys: list[float]) -> float:
    """Ordinary least-squares slope. Returns 0 if N < 2 or no variance in x."""
    n = len(xs)
    if n < 2:
        return 0.0
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    num = sum((xs[i] - x_mean) * (ys[i] - y_mean) for i in range(n))
    den = sum((xs[i] - x_mean) ** 2 for i in range(n))
    return num / den if den else 0.0


def _smooth_centered(ys: list[float], window: int) -> list[float]:
    """Centered moving average. End points use the largest centered window that
    fits — so a 3-window smooth on [a,b,c,d] yields [(a+b)/2, (a+b+c)/3, (b+c+d)/3, (c+d)/2]."""
    n = len(ys)
    if n == 0 or window <= 1:
        return list(ys)
    half = window // 2
    out = []
    for i in range(n):
        lo, hi = max(0, i - half), min(n, i + half + 1)
        out.append(sum(ys[lo:hi]) / (hi - lo))
    return out


def _count_sign_changes(signs: list[int]) -> int:
    """Sign changes ignoring zeroes (zero is ambiguous, doesn't count as a change)."""
    changes = 0
    last_nonzero = next((s for s in signs if s != 0), 0)
    for s in signs:
        if s != 0 and last_nonzero != 0 and s != last_nonzero:
            changes += 1
            last_nonzero = s
        elif s != 0:
            last_nonzero = s
    return changes


def _classify_trajectory(yoys: list[float]) -> tuple[str, dict]:
    """Returns (shape_key, features_dict). The features carry the numeric
    evidence so a finding's body can spell out *why* it got the shape it did.

    Shape detection uses a smoothed copy of the YoY series so 1-period
    zero-crossings (typical of noisy real-world trade data) don't get labelled
    as 'volatile'. Raw stats are still reported in features for transparency.
    """
    n = len(yoys)
    if n < TRAJECTORY_MIN_WINDOWS:
        return "insufficient_data", {"n": n, "min_required": TRAJECTORY_MIN_WINDOWS}

    smoothed = _smooth_centered(yoys, TRAJECTORY_SMOOTH_WINDOW)

    # Raw stats (for transparency in the finding body)
    raw_signs = [1 if y > 0 else (-1 if y < 0 else 0) for y in yoys]
    raw_sign_changes = _count_sign_changes(raw_signs)
    mean_y = sum(yoys) / n
    mean_abs = sum(abs(y) for y in yoys) / n
    var = sum((y - mean_y) ** 2 for y in yoys) / n
    stdev = var ** 0.5

    # Smoothed signs (used for shape decision)
    smoothed_signs = [1 if y > 0 else (-1 if y < 0 else 0) for y in smoothed]
    smoothed_sign_changes = _count_sign_changes(smoothed_signs)

    # Slope of the smoothed YoY series. Positive slope = YoY values rising over
    # time (growth accelerating if positive, decline easing if negative).
    overall_slope = _linear_slope(list(range(n)), smoothed)
    # Two-half slopes kept in features as additional evidence (not used in primary
    # classification — overall_slope handles the core distinction more robustly).
    half = n // 2
    earlier_slope = _linear_slope(list(range(half)), smoothed[:half])
    recent_slope = _linear_slope(list(range(n - half)), smoothed[half:])

    max_y = max(yoys); max_idx = yoys.index(max_y)
    min_y = min(yoys); min_idx = yoys.index(min_y)

    features = {
        "n": n,
        "first_yoy": yoys[0], "last_yoy": yoys[-1],
        "max_yoy": max_y, "max_idx": max_idx,
        "min_yoy": min_y, "min_idx": min_idx,
        "mean_yoy": mean_y, "stdev_yoy": stdev, "mean_abs_yoy": mean_abs,
        "sign_changes": raw_sign_changes,
        "smoothed_sign_changes": smoothed_sign_changes,
        "smoothed_first": smoothed[0], "smoothed_last": smoothed[-1],
        "earlier_slope": earlier_slope, "recent_slope": recent_slope,
        "overall_slope": overall_slope,
        "smoothing_window": TRAJECTORY_SMOOTH_WINDOW,
        "thresholds": {
            "flat_mean_abs_yoy": TRAJECTORY_FLAT_MEAN_ABS_YOY,
            "flat_stdev": TRAJECTORY_FLAT_STDEV,
            "slope_significant": TRAJECTORY_SLOPE_SIGNIFICANT,
        },
    }

    # Order matters: most specific first.
    if mean_abs < TRAJECTORY_FLAT_MEAN_ABS_YOY and stdev < TRAJECTORY_FLAT_STDEV:
        return "flat", features

    if smoothed_sign_changes >= 2:
        return "volatile", features

    if smoothed_sign_changes == 1:
        # Find the cross-zero index in the smoothed series for context.
        for i in range(1, n):
            if smoothed_signs[i] != 0 and smoothed_signs[i - 1] != 0 and smoothed_signs[i] != smoothed_signs[i - 1]:
                features["cross_zero_idx"] = i
                break
        if smoothed[-1] > 0:
            return "u_recovery", features
        return "inverse_u_peak", features

    # No sign changes (smoothed) — sustained direction. Use the overall slope
    # of the smoothed YoY series itself to distinguish accel/decel:
    #   YoY values rising over time + already positive  → growth accelerating
    #   YoY values falling over time + still positive   → growth decelerating
    #   YoY values falling over time + already negative → decline accelerating
    #   YoY values rising over time + still negative    → decline easing
    last_positive = smoothed[-1] > 0
    if last_positive:
        if overall_slope > TRAJECTORY_SLOPE_SIGNIFICANT:
            return "rising_accelerating", features
        if overall_slope < -TRAJECTORY_SLOPE_SIGNIFICANT:
            return "rising_decelerating", features
        return "rising", features
    if overall_slope < -TRAJECTORY_SLOPE_SIGNIFICANT:
        return "falling_accelerating", features
    if overall_slope > TRAJECTORY_SLOPE_SIGNIFICANT:
        return "falling_decelerating", features
    return "falling", features


def detect_hs_group_trajectories(group_names: list[str] | None = None) -> dict[str, int]:
    """For each hs_group, classify the rolling-12mo-EUR YoY series across all
    available windows into a trajectory shape. Reads existing hs_group_yoy
    findings (latest per period), emits one 'hs_group_trajectory' finding per
    group capturing the shape + supporting feature stats + supporting yoy
    finding ids for trace-back.

    Returns counts: {'emitted', 'skipped_insufficient_data', 'skipped_no_findings'}.
    """
    counts = {"emitted": 0, "skipped_insufficient_data": 0, "skipped_no_findings": 0}

    groups = _list_hs_groups(group_names)
    if not groups:
        return counts

    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'running') RETURNING id",
            (HS_GROUP_TRAJECTORY_SOURCE_URL,),
        )
        analysis_run_id = cur.fetchone()[0]

    try:
        for group in groups:
            series = _fetch_group_yoy_series(group.id)
            if not series:
                counts["skipped_no_findings"] += 1
                continue
            yoys = [s["yoy_pct"] for s in series]
            shape, features = _classify_trajectory(yoys)
            if shape == "insufficient_data":
                counts["skipped_insufficient_data"] += 1
                continue
            _insert_trajectory_finding(analysis_run_id, group, series, shape, features)
            counts["emitted"] += 1

        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='success', ended_at=now() WHERE id=%s",
                (analysis_run_id,),
            )
    except Exception as e:
        log.exception("HS-group trajectory classification failed")
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='failed', error_message=%s, ended_at=now() WHERE id=%s",
                (str(e), analysis_run_id),
            )
        raise
    return counts


def _fetch_group_yoy_series(group_id: int) -> list[dict]:
    """Return [{period, yoy_pct, finding_id, current_eur}] for the given group,
    one row per period (latest finding per period if there are duplicates)."""
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON ((detail->'windows'->>'current_end')::date)
                id AS finding_id,
                (detail->'windows'->>'current_end')::date AS period,
                (detail->'totals'->>'yoy_pct')::numeric AS yoy_pct,
                (detail->'totals'->>'current_12mo_eur')::numeric AS current_eur,
                (detail->'totals'->>'yoy_pct_kg')::numeric AS yoy_pct_kg
              FROM findings
             WHERE subkind = 'hs_group_yoy'
               AND %s = ANY(hs_group_ids)
          ORDER BY (detail->'windows'->>'current_end')::date, created_at DESC
            """,
            (group_id,),
        )
        return [
            {
                "finding_id": r["finding_id"],
                "period": r["period"],
                "yoy_pct": float(r["yoy_pct"]),
                "current_eur": float(r["current_eur"] or 0),
                "yoy_pct_kg": float(r["yoy_pct_kg"]) if r["yoy_pct_kg"] is not None else None,
            }
            for r in cur.fetchall()
        ]


def _insert_trajectory_finding(
    analysis_run_id: int,
    group: _HsGroup,
    series: list[dict],
    shape: str,
    features: dict,
) -> None:
    first = series[0]
    last  = series[-1]
    peak  = max(series, key=lambda s: s["yoy_pct"])
    trough = min(series, key=lambda s: s["yoy_pct"])

    title = (
        f"Trajectory: {group.name} — {SHAPE_LABELS.get(shape, shape)} "
        f"(latest {last['yoy_pct']*100:+.1f}% YoY, "
        f"peak {peak['yoy_pct']*100:+.1f}% in {peak['period'].strftime('%Y-%m')}, "
        f"trough {trough['yoy_pct']*100:+.1f}% in {trough['period'].strftime('%Y-%m')})"
    )

    body_lines = [
        f"Group: {group.name}",
        f"Definition: HS-CN8 codes matching {group.hs_patterns}",
        "",
        f"Trajectory shape: {shape} ({SHAPE_LABELS.get(shape, '')})",
        "",
        f"Series length: {features['n']} rolling-12mo YoY windows from "
        f"{first['period'].strftime('%Y-%m')} to {last['period'].strftime('%Y-%m')}.",
        f"  First YoY: {first['yoy_pct']*100:+.2f}%",
        f"  Last YoY:  {last['yoy_pct']*100:+.2f}%",
        f"  Peak:      {peak['yoy_pct']*100:+.2f}% in {peak['period'].strftime('%Y-%m')}",
        f"  Trough:    {trough['yoy_pct']*100:+.2f}% in {trough['period'].strftime('%Y-%m')}",
        f"  Sign changes: {features['sign_changes']}",
        f"  Mean YoY: {features['mean_yoy']*100:+.2f}%, stdev: {features['stdev_yoy']*100:.2f}%",
        f"  Earlier-half slope: {features['earlier_slope']:+.5f} per window; "
        f"recent-half slope: {features['recent_slope']:+.5f} per window.",
        "",
    ]

    # Classifier reasoning — explicit so the journalist can see WHY the shape was assigned.
    if shape == "flat":
        body_lines.append(
            f"Reasoning: mean |YoY| {features['mean_abs_yoy']*100:.2f}% < threshold "
            f"{TRAJECTORY_FLAT_MEAN_ABS_YOY*100:.0f}% AND stdev {features['stdev_yoy']*100:.2f}% < "
            f"{TRAJECTORY_FLAT_STDEV*100:.0f}%."
        )
    elif shape == "volatile":
        body_lines.append(f"Reasoning: {features['sign_changes']} sign changes across the series.")
    elif shape == "u_recovery":
        cross = series[features.get("cross_zero_idx", 0)]
        body_lines.append(
            f"Reasoning: 1 sign change. Series went from negative ({first['yoy_pct']*100:+.1f}%) "
            f"to positive ({last['yoy_pct']*100:+.1f}%), crossing zero around "
            f"{cross['period'].strftime('%Y-%m')}."
        )
    elif shape == "inverse_u_peak":
        cross = series[features.get("cross_zero_idx", 0)]
        body_lines.append(
            f"Reasoning: 1 sign change. Series went from positive ({first['yoy_pct']*100:+.1f}%) "
            f"to negative ({last['yoy_pct']*100:+.1f}%), crossing zero around "
            f"{cross['period'].strftime('%Y-%m')}."
        )
    elif shape.endswith("_accelerating"):
        direction_word = "rising" if shape.startswith("rising") else "falling"
        body_lines.append(
            f"Reasoning: no sign changes. The smoothed YoY series itself is "
            f"{direction_word} (overall slope {features['overall_slope']:+.5f} per window, "
            f"|slope| > significance threshold {TRAJECTORY_SLOPE_SIGNIFICANT})."
        )
    elif shape.endswith("_decelerating"):
        direction_word = "rising but slowing" if shape.startswith("rising") else "still falling but easing"
        body_lines.append(
            f"Reasoning: no sign changes; latest YoY is {'positive' if shape.startswith('rising') else 'negative'} "
            f"but the smoothed YoY series is moving against the direction "
            f"({direction_word}; overall slope {features['overall_slope']:+.5f} per window)."
        )
    else:
        body_lines.append("Reasoning: sustained direction with no notable acceleration/deceleration.")

    body_lines.append("")
    body_lines.append(
        "This finding rests on the underlying hs_group_yoy findings whose ids are listed in "
        "detail.underlying_yoy_finding_ids. Each of those carries kg + €/kg figures, top "
        "contributing CN8 codes, and top importing EU members."
    )

    detail = {
        "method": "hs_group_trajectory_v1",
        "group": {"id": group.id, "name": group.name, "hs_patterns": group.hs_patterns},
        "shape": shape,
        "shape_label": SHAPE_LABELS.get(shape, shape),
        "features": {
            **features,
            "first_period": first["period"].isoformat(),
            "last_period": last["period"].isoformat(),
            "peak_period": peak["period"].isoformat(),
            "trough_period": trough["period"].isoformat(),
        },
        "series": [
            {"period": s["period"].isoformat(), "yoy_pct": s["yoy_pct"],
             "current_eur": s["current_eur"], "yoy_pct_kg": s["yoy_pct_kg"]}
            for s in series
        ],
        "underlying_yoy_finding_ids": [s["finding_id"] for s in series],
        "caveat_codes": [
            "cif_fob", "currency_timing", "classification_drift",
            "eurostat_stat_procedure_mix",
        ],
    }
    # Score = absolute latest YoY; lets journalists rank by "how much movement is happening now".
    score = abs(last["yoy_pct"])

    import json
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO findings (
                scrape_run_id, kind, subkind, observation_ids, hs_group_ids,
                score, title, body, detail
            ) VALUES (
                %s, 'anomaly', 'hs_group_trajectory', %s, %s, %s, %s, %s, %s
            )
            """,
            (analysis_run_id, [], [group.id], score, title,
             "\n".join(body_lines), json.dumps(detail)),
        )
