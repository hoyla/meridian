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
) -> list[tuple[date, float, int]]:
    """Returns (period, total_eur, n_obs) per period for Eurostat imports from
    CN matching any of the given hs_patterns. Periods with no matching obs
    are absent from the result (no zero-fill); the caller handles gaps."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.period,
                   SUM(o.value_amount) AS total_eur,
                   COUNT(*) AS n_obs
              FROM observations o
              JOIN releases r ON r.id = o.release_id
             WHERE r.source = 'eurostat'
               AND o.flow = 'import'
               AND o.partner_country = 'CN'
               AND o.hs_code LIKE ANY(%s)
          GROUP BY r.period
          ORDER BY r.period
            """,
            (patterns,),
        )
        return [(row[0], float(row[1] or 0), int(row[2])) for row in cur.fetchall()]


def _hs_group_top_cn8s(patterns: list[str], start: date, end: date, limit: int = 10) -> list[dict]:
    """Top contributing HS-CN8 codes within a group across [start, end]."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT o.hs_code, SUM(o.value_amount) AS total_eur, COUNT(*) AS n_obs
              FROM observations o
              JOIN releases r ON r.id = o.release_id
             WHERE r.source = 'eurostat'
               AND r.period >= %s AND r.period <= %s
               AND o.flow = 'import' AND o.partner_country = 'CN'
               AND o.hs_code LIKE ANY(%s)
          GROUP BY o.hs_code
          ORDER BY SUM(o.value_amount) DESC NULLS LAST
             LIMIT %s
            """,
            (start, end, patterns, limit),
        )
        return [{"hs_code": r[0], "total_eur": float(r[1] or 0), "n_obs": int(r[2])}
                for r in cur.fetchall()]


def _hs_group_top_reporters(patterns: list[str], start: date, end: date, limit: int = 10) -> list[dict]:
    """Top contributing EU reporters within a group across [start, end]."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT o.reporter_country, SUM(o.value_amount) AS total_eur, COUNT(*) AS n_obs
              FROM observations o
              JOIN releases r ON r.id = o.release_id
             WHERE r.source = 'eurostat'
               AND r.period >= %s AND r.period <= %s
               AND o.flow = 'import' AND o.partner_country = 'CN'
               AND o.hs_code LIKE ANY(%s)
          GROUP BY o.reporter_country
          ORDER BY SUM(o.value_amount) DESC NULLS LAST
             LIMIT %s
            """,
            (start, end, patterns, limit),
        )
        return [{"reporter": r[0], "total_eur": float(r[1] or 0), "n_obs": int(r[2])}
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
            by_period: dict[date, float] = {p: t for p, t, _ in series}
            n_obs_by_period: dict[date, int] = {p: n for p, _, n in series}
            if not series:
                counts["skipped_insufficient_history"] += 1
                continue

            periods_sorted = sorted(by_period.keys())

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
                if not all(p in by_period for p in want):
                    counts["skipped_insufficient_history"] += 1
                    continue

                current_12 = sum(by_period[p] for p in want if start_curr <= p <= end_curr)
                prior_12 = sum(by_period[p] for p in want if start_prior <= p <= end_prior)
                if prior_12 == 0:
                    counts["skipped_zero_prior"] += 1
                    continue
                yoy_pct = (current_12 - prior_12) / abs(prior_12)
                if abs(yoy_pct) < yoy_threshold_pct:
                    counts["skipped_below_threshold"] += 1
                    continue

                top_cn8s = _hs_group_top_cn8s(group.hs_patterns, start_curr, end_curr)
                top_reporters = _hs_group_top_reporters(group.hs_patterns, start_curr, end_curr)
                _insert_hs_group_yoy_finding(
                    analysis_run_id, group, t, start_curr, end_curr,
                    start_prior, end_prior, current_12, prior_12, yoy_pct,
                    series, top_cn8s, top_reporters, n_obs_by_period,
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
    current_12: float,
    prior_12: float,
    yoy_pct: float,
    series: list[tuple[date, float, int]],
    top_cn8s: list[dict],
    top_reporters: list[dict],
    n_obs_by_period: dict[date, int],
) -> None:
    direction = "up" if yoy_pct > 0 else "down"
    title = (
        f"Component trend: {group.name}, rolling 12mo to {end_curr.strftime('%Y-%m')}: "
        f"€{current_12/1e9:,.2f}B from CN ({yoy_pct*100:+.1f}% {direction} vs prior 12mo €{prior_12/1e9:,.2f}B)"
    )
    body_lines = [
        f"Group: {group.name}",
        f"Definition: HS-CN8 codes matching {group.hs_patterns}",
        "",
        f"Rolling 12 months ending {end_curr.strftime('%Y-%m')}: €{current_12:,.0f} "
        f"({end_curr.strftime('%Y-%m')}: €{series[-1][1] if series else 0:,.0f}; "
        f"period range {start_curr.strftime('%Y-%m')} → {end_curr.strftime('%Y-%m')}).",
        f"Prior 12 months ({start_prior.strftime('%Y-%m')} → {end_prior.strftime('%Y-%m')}): €{prior_12:,.0f}.",
        f"YoY change: {yoy_pct*100:+.2f}% (€{current_12 - prior_12:+,.0f}).",
        "",
        "Top 5 contributing HS-CN8 codes in the rolling 12mo window (€):",
    ]
    for c in top_cn8s[:5]:
        body_lines.append(f"  {c['hs_code']}: €{c['total_eur']:,.0f} ({c['n_obs']} obs)")
    body_lines.append("")
    body_lines.append("Top 5 importing EU members in the rolling 12mo window (€):")
    for r in top_reporters[:5]:
        body_lines.append(f"  {r['reporter']}: €{r['total_eur']:,.0f}")
    body_lines.append("")
    body_lines.append(
        "All values in EUR. Caveats applicable: 'cif_fob' (Eurostat reports imports CIF), "
        "'classification_drift' (CN8 sub-headings can be ambiguous), 'eurostat_stat_procedure_mix' "
        "(rolling totals sum across tariff regimes — see eurostat_raw_rows for the breakdown)."
    )

    detail = {
        "method": "hs_group_yoy_v1",
        "method_query": {
            "source": "eurostat", "flow": "import", "partner_country": "CN",
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
            "current_12mo_eur": current_12,
            "prior_12mo_eur": prior_12,
            "delta_eur": current_12 - prior_12,
            "yoy_pct": yoy_pct,
        },
        "monthly_series": [
            {"period": p.isoformat(), "value_eur": v, "n_obs": n_obs_by_period.get(p, 0)}
            for (p, v, _) in series
            if start_prior <= p <= end_curr
        ],
        "top_cn8_codes_in_current_12mo": top_cn8s,
        "top_reporters_in_current_12mo": top_reporters,
        "caveat_codes": [
            "cif_fob", "currency_timing", "classification_drift",
            "eurostat_stat_procedure_mix",
        ],
    }
    score = abs(yoy_pct)

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
