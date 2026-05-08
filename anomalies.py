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
from dataclasses import dataclass
from datetime import date

import psycopg2
import psycopg2.extras

import lookups

log = logging.getLogger(__name__)

ANALYSIS_SOURCE_URL = "analysis://mirror_trade/v1"

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
    iso2: str
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
        "emitted": 0, "skipped_no_eurostat": 0, "skipped_no_fx": 0,
        "skipped_aggregate": 0, "skipped_unmapped": 0, "skipped_no_value": 0,
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
    if resolved.iso2 is None:
        # Aggregate (EU bloc, ASEAN, Latin America, etc.) — handle separately later.
        return "skipped_aggregate"

    period = gr["period"]
    eurostat_total, eurostat_ids, n_hs = _eurostat_aggregate_for(period, resolved.iso2)
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
        iso2=resolved.iso2,
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
    )


def _eurostat_aggregate_for(period: date, iso2: str) -> tuple[float | None, list[int], int]:
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
               AND o.reporter_country = %s
               AND o.partner_country = 'CN'
            """,
            (period, iso2),
        )
        total, ids, n = cur.fetchone()
    if n == 0:
        return None, [], 0
    return total, list(ids), n


def _insert_finding(analysis_run_id: int, r: _MirrorGapResult) -> None:
    direction = "Eurostat > GACC" if r.gap_eur > 0 else "GACC > Eurostat"
    title = (
        f"Mirror-trade gap, China ↔ {r.iso2}, {r.period.strftime('%Y-%m')}: "
        f"GACC reports €{r.gacc_value_eur:,.0f}, Eurostat reports €{r.eurostat_total_eur:,.0f} "
        f"({r.gap_pct*100:+.1f}%, {direction})"
    )
    body = (
        f"GACC: China's reported {r.gacc_value_raw:,.1f} ({r.gacc_value_currency} ×{r.gacc_unit_scale:,.0f}) "
        f"export to '{r.gacc_partner_label}', converted at the ECB "
        f"{r.gacc_value_currency}/EUR rate of {r.fx_rate:.6f} for {r.fx_rate_date.strftime('%Y-%m')}, "
        f"= €{r.gacc_value_eur:,.0f}.\n\n"
        f"Eurostat: {r.iso2}'s reported import from CN summed across {r.eurostat_n_hs_codes:,} HS-CN8 "
        f"codes = €{r.eurostat_total_eur:,.0f}.\n\n"
        f"Gap: €{r.gap_eur:,.0f} ({r.gap_pct*100:+.1f}% of larger value). "
        f"CIF/FOB baseline expects ~{CIF_FOB_BASELINE_PCT*100:.0f}% Eurostat-higher; "
        f"excess over baseline is {r.excess_over_cif_fob_baseline_pct*100:+.1f} percentage points."
    )
    detail = {
        "method": "mirror_trade_v1",
        # Caveat codes — journalists should weigh these when interpreting the gap.
        # Promote to a dedicated findings.caveat_codes column when the schema
        # gets its first migration after the lookups went in.
        "caveat_codes": DEFAULT_MIRROR_GAP_CAVEATS,
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
