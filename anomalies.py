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

import findings_io
import lookups

log = logging.getLogger(__name__)

ANALYSIS_SOURCE_URL = "analysis://mirror_trade/v1"


def _tally(counts: dict, action: findings_io.EmitAction) -> None:
    """Bump granular action counter + the `emitted` total.

    `emitted` is the sum of inserted_new + confirmed_existing + superseded.
    Useful as a one-number summary; the breakdown matters when the analyser
    is re-run (most actions will be confirmed_existing in steady-state)."""
    counts[action] = counts.get(action, 0) + 1
    counts["emitted"] = counts.get("emitted", 0) + 1

TREND_ANALYSIS_SOURCE_URL = "analysis://mirror_gap_trends/v1"
HS_GROUP_TREND_SOURCE_URL = "analysis://hs_group_trends/v1"
HS_GROUP_TRAJECTORY_SOURCE_URL = "analysis://hs_group_trajectory/v1"

# Universal caveats per finding-family. These are caveats that apply to EVERY
# finding of the given subkind family — they're methodological properties of
# the analyser, not facts about individual findings. They're rendered once in
# the briefing pack's methodology footer and are NOT attached to per-finding
# `caveat_codes` arrays. The per-finding array carries only caveats that vary
# between findings of the same family (low_base_effect, partial_window,
# transshipment_hub, low_baseline_n, low_kg_coverage, cross_source_sum,
# aggregate_composition).
#
# Subkind suffixes (_export, _uk, _combined) inherit the family's universal
# set; see UNIVERSAL_CAVEATS_FOR_SUBKIND below.
#
# The briefing pack imports this dict to render the universal-caveats section
# and to know which codes to exclude from per-finding caveat lines. If you add
# a caveat to an analyser, decide first: does it fire on every finding of that
# family? Add it here. Does it fire conditionally? Append it to the per-finding
# `caveat_codes` instead.
UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY: dict[str, list[str]] = {
    "mirror_gap": [
        "cif_fob", "currency_timing", "general_vs_special_trade",
        "transshipment", "eurostat_stat_procedure_mix", "multi_partner_sum",
    ],
    "mirror_gap_zscore": [
        "cif_fob", "currency_timing", "general_vs_special_trade",
        "transshipment", "eurostat_stat_procedure_mix", "multi_partner_sum",
        "aggregate_composition_drift",
    ],
    "hs_group_yoy": [
        "cif_fob", "currency_timing", "classification_drift",
        "eurostat_stat_procedure_mix", "multi_partner_sum", "cn8_revision",
    ],
    "hs_group_trajectory": [
        "cif_fob", "currency_timing", "classification_drift",
        "eurostat_stat_procedure_mix", "multi_partner_sum", "cn8_revision",
    ],
    "narrative_hs_group": ["llm_drafted"],
}


def universal_caveats_for_subkind(subkind: str) -> list[str]:
    """Map a concrete subkind (possibly with _export/_uk/_combined suffixes)
    to the universal-caveat list of its family. Returns [] for subkinds with
    no defined family (e.g. gacc_aggregate_yoy, which is GACC-only and shares
    no caveats with the Eurostat/HMRC families)."""
    # Strip flow suffix first, then scope suffix.
    base = subkind
    if base.endswith("_export"):
        base = base[: -len("_export")]
    for scope_suffix in ("_combined", "_uk"):
        if base.endswith(scope_suffix):
            base = base[: -len(scope_suffix)]
            break
    return UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY.get(base, [])

# Phase 2.2: CIF/FOB baseline now lives in the `cif_fob_baselines` table
# (lookups.lookup_cif_fob_baseline). This constant is kept as a
# last-resort fallback only — used if the table is somehow empty (e.g. a
# fresh DB before seed data has been applied). In normal operation the
# lookup returns either a per-partner row or the global default seeded
# from this same value.
CIF_FOB_BASELINE_PCT_FALLBACK = 0.075

# A 12-month total below this threshold is "low base": a percentage change on
# such a small denominator can look dramatic but doesn't carry the editorial
# weight of a comparable percentage on a larger base. €50M is a defensible
# floor for an EU-wide HS-group total; below it the absolute change might be
# under €10M which is niche-story territory, not "China shock" headline.
# Configurable per-call via the analyser's threshold parameter.
LOW_BASE_THRESHOLD_EUR = 50_000_000

# When the trajectory analyser sees this fraction or more of the windows
# tagged as low-base, the trajectory finding itself flags low_base_effect.
TRAJECTORY_LOW_BASE_FRACTION = 0.5

# Eurostat partners for "Chinese trade" — CN plus the two Special
# Administrative Regions. Editorially, China-via-Hong-Kong is still Chinese
# trade for Lisa O'Carroll's "China shock" framing; Eurostat reports it under
# partner=HK because HK is a separate trade jurisdiction, but a journalist
# investigating Chinese trade volumes wants HK and MO summed in. All
# analysers that sum on the China side use this list. The previous
# `--eurostat-partners` CLI knob has been removed: in practice we always want
# the CN+HK+MO envelope, and the rare validation against a CN-only Soapbox /
# Merics figure is a one-off comparator, not a production analyser config.
# If you need a CN-only spot check, query observations / eurostat_raw_rows
# directly rather than re-running the analyser with a narrowed partner set.
# The `multi_partner_sum` caveat is now universal for the affected families
# (see UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY above) — it no longer needs to be
# attached per-finding.
EUROSTAT_PARTNERS: tuple[str, ...] = ("CN", "HK", "MO")

# EU-27 means EU-27 at all times. Our eurostat_raw_rows table contains
# UK-reporter (GB) rows for 2017–Q1 2020 because the UK was an EU-28 reporter
# until Brexit; without filtering, hs-group analyses for those years silently
# answer "EU-28 incl. UK" while later years answer "EU-27 excl. UK", which
# breaks any cross-Brexit comparison. The hs-group helpers exclude GB so
# their "EU" sums are EU-27 across the whole range. The GB rows stay in the
# DB (audit + future "uk" scope cross-checks) — only the analyser queries
# filter them out. mirror-trade is unaffected: its single-country and EU-bloc
# (member-list) queries already scope the reporter set explicitly.
EU27_EXCLUDE_REPORTERS: tuple[str, ...] = ("GB",)

# Eurostat bulk-file aggregate row code. Eurostat ships, per
# (reporter, period, partner, flow, stat_procedure), a `product_nc='000TOTAL'`
# row that sums the per-CN8-detail rows for the same slice. Naïve
# `SUM(value_eur) FROM eurostat_raw_rows` would add detail + total = ~2x.
# All analyser queries below apply HS-pattern LIKE filters (e.g. '8507%')
# that don't match '000TOTAL' so they're unaffected, but ANY ad-hoc
# direct sum (sanity checks, validation scripts) MUST exclude it. See
# `dev_notes/forward-work-eurostat-aggregate-scale.md` for the full
# investigation.
EUROSTAT_AGGREGATE_PRODUCT_NC: str = "000TOTAL"

# Comparison scope: which reporter side(s) to sum on the China-trade
# comparison. Phase 6.1 introduced the UK as a separate ingestable
# source (HMRC OTS via OData) so analysers can answer three editorially
# distinct questions:
# - eu_27: how is EU-27 (excluding UK) trade with China moving?
# - uk: how is UK-only trade with China moving? (HMRC-side, post-Brexit
#   the only canonical UK trade source; pre-Brexit also includes UK
#   transactions reported under reporter=GB in Eurostat — Phase 6.1d
#   prefers HMRC for both periods for a consistent UK series.)
# - eu_27_plus_uk: combined view; carries a `cross_source_sum` caveat
#   because the sources have different threshold rules, suppression
#   policies, and revision cycles, so direct addition is methodologically
#   imperfect (close enough editorially when the caveat is surfaced).
COMPARISON_SCOPE_DEFAULT: str = "eu_27"
VALID_COMPARISON_SCOPES: tuple[str, ...] = ("eu_27", "uk", "eu_27_plus_uk")
COMPARISON_SCOPE_SUBKIND_SUFFIX: dict[str, str] = {
    "eu_27": "",          # backward compat: EU-27 default keeps existing subkind
    "uk": "_uk",
    "eu_27_plus_uk": "_combined",
}
COMPARISON_SCOPE_SOURCES: dict[str, tuple[str, ...]] = {
    "eu_27": ("eurostat",),
    "uk": ("hmrc",),
    "eu_27_plus_uk": ("eurostat", "hmrc"),
}


def _hs_pattern_or_clause(patterns: list[str]) -> tuple[str, list[str]]:
    """Build `(product_nc LIKE %s OR product_nc LIKE %s ...)` for a pattern list.

    Why not `product_nc LIKE ANY(%s)`: PostgreSQL doesn't push down
    multi-pattern LIKE ANY through the text_pattern_ops btree index;
    instead it falls back to a Parallel Seq Scan over the full table
    (~800ms per call vs ~3ms with the index). Rewriting the predicate
    as separate ORed LIKEs lets the planner build a Bitmap OR of index
    scans, which is what the idx_eu_raw_analyser index was designed for.

    Returns (sql_fragment, params_for_query) — params get spliced into
    the call site's parameter tuple in order. An empty pattern list
    returns FALSE so the caller's surrounding query degenerates to no
    rows rather than a SQL error."""
    if not patterns:
        return ("FALSE", [])
    fragment = "(" + " OR ".join(["product_nc LIKE %s"] * len(patterns)) + ")"
    return (fragment, list(patterns))


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
    cif_fob_baseline: lookups.CifFobBaseline | None
    transshipment_hub: lookups.TransshipmentHub | None
    eurostat_partners: list[str]   # partners summed on the EU import side (always EUROSTAT_PARTNERS = CN+HK+MO)
    # Aggregate-specific (None for single-country comparisons)
    aggregate_kind: str | None = None
    aggregate_members: list[str] | None = None
    aggregate_sources: list[str] | None = None


# Whitespace between the ISO-4217 currency code and the multiplier number is
# optional: GACC uses both "CNY 100 Million" and "USD1 Million" (no space)
# in production release pages. Without the `\s*` (vs `\s+`), every USD-side
# release silently failed the unit parse — half the GACC dataset.
_UNIT_RE = re.compile(r"^([A-Z]{3})(?:\s*(\d+(?:[.,]\d+)?))?(?:\s+(Thousand|Million|Billion))?\s*$")


def parse_unit_scale(unit: str | None) -> tuple[float | None, str | None]:
    """Parse a release.unit string like 'CNY 100 Million' into (multiplier, currency).

    Two distinct "no useful info" return shapes, on purpose:
    - `(1.0, None)` — the unit field is *missing* (None or empty). The
      caller can safely treat the value as a raw, un-scaled amount.
    - `(None, None)` — a unit string was provided but did not match any
      recognised form. The caller MUST treat this as a skip and emit an
      ERROR. Silently applying multiplier 1.0 here would produce a
      converted value off by potentially orders of magnitude (e.g. a
      release stating "USD 10,000" would be read as 1× when the real
      scale was 10⁴), which is a bug-class error rather than a hedge.
    Phase 1.2 of dev_notes/history.md.
    """
    if not unit:
        return 1.0, None
    m = _UNIT_RE.match(unit.strip())
    if not m:
        log.error(
            "Unrecognised unit string %r — skipping row. Silently applying "
            "multiplier 1.0 here would risk an order-of-magnitude error in "
            "the converted EUR value, so we refuse rather than guess. "
            "Either extend _UNIT_RE to handle this form or correct the "
            "release's unit field.",
            unit,
        )
        return None, None
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


def detect_mirror_trade_gaps(
    period: date | None = None,
) -> dict[str, int]:
    """Compare GACC China-export-to-X to Eurostat X-import-from-China for each
    overlapping (period, partner) pair. Each comparison emits a findings row of
    kind='anomaly', subkind='mirror_gap'.

    The Eurostat partner set is fixed at CN+HK+MO (see EUROSTAT_PARTNERS):
    that's the editorially-correct "Chinese trade" envelope. The
    multi_partner_sum caveat is rendered once in the briefing pack
    methodology footer, not attached per finding.

    Args:
        period: if specified, only analyse that period; otherwise all periods
                that have GACC data.

    Returns counts: {'emitted', 'skipped_no_eurostat', 'skipped_no_fx',
                     'skipped_aggregate', 'skipped_unmapped', 'skipped_no_value'}.
    """
    eurostat_partners = list(EUROSTAT_PARTNERS)
    counts = {
        "emitted": 0,
        "inserted_new": 0,
        "confirmed_existing": 0,
        "superseded": 0,
        "skipped_no_eurostat": 0,
        "skipped_no_fx": 0,
        "skipped_aggregate_no_members": 0,
        "skipped_aggregate_no_eurostat_counterpart": 0,
        "skipped_unmapped": 0,
        "skipped_unrecognised_unit": 0,
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
            action = _insert_finding(analysis_run_id, result)
            _tally(counts, action)
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


def _compute_one_gap(
    gr: dict,
) -> _MirrorGapResult | str:
    """Returns the result, OR a sentinel string naming the skip reason for counts."""
    eurostat_partners = list(EUROSTAT_PARTNERS)
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
        eurostat_total, eurostat_ids, n_hs = _eurostat_aggregate_for_members(
            period, aggregate_members, partners=eurostat_partners,
        )
        result_iso2 = f"BLOC:{membership.aggregate_kind}"
    else:
        eurostat_total, eurostat_ids, n_hs = _eurostat_aggregate_for(
            period, resolved.iso2, partners=eurostat_partners,
        )
        result_iso2 = resolved.iso2

    if eurostat_total is None:
        return "skipped_no_eurostat"

    unit_scale, unit_currency = parse_unit_scale(gr["unit"])
    if unit_scale is None:
        # Unrecognised unit format — refuse to compute a possibly off-by-10⁴
        # converted EUR value. parse_unit_scale already logged ERROR.
        return "skipped_unrecognised_unit"
    currency_for_fx = unit_currency or gr["value_currency"]
    fx = lookups.lookup_fx(currency_for_fx, "EUR", period)
    if fx is None:
        return "skipped_no_fx"

    gacc_raw_currency = float(gr["value_amount"]) * unit_scale
    gacc_value_eur = gacc_raw_currency * fx.rate
    gap_eur = float(eurostat_total) - gacc_value_eur
    larger = max(abs(gacc_value_eur), abs(float(eurostat_total)))
    gap_pct = gap_eur / larger if larger else 0.0

    # Phase 2.2: CIF/FOB baseline now comes from the lookup table — per-partner
    # row if present, else global default. Fallback to the in-code constant
    # only if the table is somehow empty (which would be a config error).
    # The lookup applies to single-country partners only; for aggregates we
    # use the global default (per-partner baselines for blocs aren't a
    # well-defined concept yet).
    lookup_partner_iso2 = resolved.iso2 if aggregate_kind is None else None
    cif_fob = lookups.lookup_cif_fob_baseline(lookup_partner_iso2)
    baseline_pct = cif_fob.baseline_pct if cif_fob else CIF_FOB_BASELINE_PCT_FALLBACK
    excess = abs(gap_pct) - baseline_pct

    # Phase 2.1: transshipment hub auto-flag. Aggregates don't have an
    # iso2; only check single-country partners.
    transshipment_hub = (
        lookups.lookup_transshipment_hub(resolved.iso2)
        if aggregate_kind is None and resolved.iso2 else None
    )

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
        cif_fob_baseline=cif_fob,
        transshipment_hub=transshipment_hub,
        eurostat_partners=list(eurostat_partners),
        aggregate_kind=aggregate_kind,
        aggregate_members=aggregate_members,
        aggregate_sources=aggregate_sources,
    )


def _eurostat_aggregate_for(
    period: date, iso2: str, partners: list[str] | None = None,
) -> tuple[float | None, list[int], int]:
    return _eurostat_aggregate_for_members(period, [iso2], partners=partners)


def _eurostat_aggregate_for_members(
    period: date, member_iso2s: list[str],
    partners: list[str] | None = None,
) -> tuple[float | None, list[int], int]:
    """Sum Eurostat imports from `partners` (default EUROSTAT_PARTNERS
    = CN+HK+MO) across the given list of EU member ISO-2 codes for the given
    period. The multi-partner sum is universal for the analyser families
    that call this — see UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY.

    Returns (total_eur, obs_ids, n_obs)."""
    if not member_iso2s:
        return None, [], 0
    if partners is None:
        partners = list(EUROSTAT_PARTNERS)
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
               AND o.partner_country = ANY(%s)
            """,
            (period, member_iso2s, partners),
        )
        total, ids, n = cur.fetchone()
    if n == 0:
        return None, [], 0
    return total, list(ids), n


def _insert_finding(analysis_run_id: int, r: _MirrorGapResult) -> findings_io.EmitAction:
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
    baseline_pct = (
        r.cif_fob_baseline.baseline_pct if r.cif_fob_baseline else CIF_FOB_BASELINE_PCT_FALLBACK
    )
    baseline_scope = (
        f"per-partner ({r.cif_fob_baseline.partner_iso2})"
        if r.cif_fob_baseline and r.cif_fob_baseline.partner_iso2 else "global default"
    )
    body = (
        f"GACC: China's reported {r.gacc_value_raw:,.1f} ({r.gacc_value_currency} ×{r.gacc_unit_scale:,.0f}) "
        f"export to '{r.gacc_partner_label}', converted at the ECB "
        f"{r.gacc_value_currency}/EUR rate of {r.fx_rate:.6f} for {r.fx_rate_date.strftime('%Y-%m')}, "
        f"= €{r.gacc_value_eur:,.0f}.\n\n"
        f"Eurostat: imports from CN summed across {r.eurostat_n_hs_codes:,} HS-CN8 "
        f"observations from {eurostat_descriptor} = €{r.eurostat_total_eur:,.0f}.\n\n"
        f"Gap: €{r.gap_eur:,.0f} ({r.gap_pct*100:+.1f}% of larger value). "
        f"CIF/FOB baseline ({baseline_scope}) expects ~{baseline_pct*100:.1f}% Eurostat-higher; "
        f"excess over baseline is {r.excess_over_cif_fob_baseline_pct*100:+.1f} percentage points."
    )
    if r.transshipment_hub is not None:
        # Phase 2.1: editorial framing for known hubs. The body annotation is
        # what a journalist sees; the caveat code is what the LLM framing
        # layer / briefing pack will surface.
        body += (
            f"\n\n⚓ TRANSSHIPMENT-HUB CONTEXT: {r.iso2} is a known transshipment "
            f"hub. Persistent gaps for hub partners primarily reflect routing "
            f"rather than direct trade: goods Chinese-in-origin may transit "
            f"through {r.iso2} before being declared by another EU member, or "
            f"vice versa. The absolute gap level is therefore not the editorial "
            f"signal — movements relative to {r.iso2}'s own baseline are. "
            f"See caveat 'transshipment_hub'. Hub note: {r.transshipment_hub.notes or '—'}"
        )
    if is_aggregate:
        body += (
            f"\n\nThis is an aggregate-to-aggregate comparison. GACC's '{r.gacc_partner_label}' "
            f"label is matched to Eurostat by summing the {len(r.aggregate_members)} member-state "
            f"reporters. The 'aggregate_composition' caveat applies: the GACC-side bloc "
            f"definition (per release footnote) and the Eurostat-side reporter set may differ in "
            f"composition or as-of date. Sources cited: {', '.join(r.aggregate_sources or [])}."
        )

    # Per-finding caveats: only the codes that vary between findings. The
    # universal ones (cif_fob, currency_timing, general_vs_special_trade,
    # transshipment, eurostat_stat_procedure_mix, multi_partner_sum) are
    # rendered once in the briefing pack methodology footer — see
    # UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY.
    caveat_codes: list[str] = []
    if is_aggregate:
        caveat_codes.append("aggregate_composition")
    if r.transshipment_hub is not None:
        caveat_codes.append("transshipment_hub")

    detail = {
        "method": "mirror_trade_v5_per_country_cif_fob_baselines",
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
            "partners_summed": r.eurostat_partners,  # Phase 2.3
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
        # Phase 2.2: record exactly which CIF/FOB baseline was used and where
        # it came from, so a journalist can audit. Falls back to the in-code
        # constant only when the lookup table is empty (config error).
        "cif_fob_baseline": (
            {
                "baseline_pct": r.cif_fob_baseline.baseline_pct,
                "scope": ("per-partner" if r.cif_fob_baseline.partner_iso2 else "global"),
                "partner_iso2": r.cif_fob_baseline.partner_iso2,
                "source": r.cif_fob_baseline.source,
                "source_url": r.cif_fob_baseline.source_url,
                "baseline_id": r.cif_fob_baseline.baseline_id,
            } if r.cif_fob_baseline else
            {"baseline_pct": CIF_FOB_BASELINE_PCT_FALLBACK,
             "scope": "fallback_constant",
             "source": "in-code fallback (cif_fob_baselines table empty)"}
        ),
        # Phase 2.1: record the hub flag with its provenance, so the editorial
        # context travels with the finding. Null when the partner isn't a hub
        # or when the comparison is aggregate-level.
        "transshipment_hub": (
            {
                "iso2": r.transshipment_hub.iso2,
                "notes": r.transshipment_hub.notes,
                "evidence_url": r.transshipment_hub.evidence_url,
            } if r.transshipment_hub else None
        ),
        # Legacy fields retained for downstream compatibility (used by
        # existing tests and the briefing pack).
        "cif_fob_baseline_pct": (
            r.cif_fob_baseline.baseline_pct if r.cif_fob_baseline else CIF_FOB_BASELINE_PCT_FALLBACK
        ),
        "excess_over_baseline_pct": r.excess_over_cif_fob_baseline_pct,
    }
    score = abs(r.gap_pct) if r.gap_pct is not None else None
    obs_ids = [r.gacc_obs_id] + r.eurostat_obs_ids
    period_yyyymm = r.period.strftime("%Y-%m")

    with _conn() as conn, conn.cursor() as cur:
        _, action = findings_io.emit_finding(
            cur,
            scrape_run_id=analysis_run_id,
            kind="anomaly",
            subkind="mirror_gap",
            natural_key=findings_io.nk_mirror_gap(r.iso2, period_yyyymm),
            # Editorially-meaningful values: if any of these move, the finding
            # is a revision and should supersede the prior row. We deliberately
            # exclude observation_ids and string descriptors that change without
            # the *story* changing. `method` is included so an analyser version
            # bump triggers supersedes even when numbers don't move (e.g. a
            # caveat-list change).
            value_fields={
                "method": detail["method"],
                "gacc_value_eur": round(r.gacc_value_eur, 2),
                "eurostat_total_eur": round(r.eurostat_total_eur, 2),
                "gap_eur": round(r.gap_eur, 2),
                "gap_pct": round(r.gap_pct, 6) if r.gap_pct is not None else None,
                "is_aggregate": is_aggregate,
                # cif_fob baseline is editorially load-bearing (the "expected"
                # gap that excess_over_baseline_pct is measured against).
                # Including it here means a baseline update — e.g. swapping the
                # global default for a per-country OECD ITIC value — propagates
                # as a supersede so the briefing pack picks up the new framing.
                "cif_fob_baseline_pct": round(detail["cif_fob_baseline_pct"], 6),
            },
            observation_ids=obs_ids,
            score=score,
            title=title,
            body=body,
            detail=detail,
        )
    return action


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


LOW_BASELINE_N_THRESHOLD = 6
"""Below this many baseline points, mirror_gap_zscore findings are flagged
with a `low_baseline_n` caveat. The mathematical floor (`min_baseline_n`,
configurable per call) stays low — you can compute *something* with 3 points
— but the editorial confidence in that z-score is limited until the baseline
has at least one full default window (6 months) behind it. Phase 1.4 of
dev_notes/history.md."""


def _log_mirror_gap_staleness() -> None:
    """Phase 2.6: log a WARNING when the latest active mirror_gap finding's
    period is older than the latest available Eurostat or GACC release.
    The trend analyser then builds on stale input — not silently dangerous,
    but the journalist should know to re-run --analyse mirror-trade first.

    No-op when fully fresh (logs INFO with period summary instead). Never
    raises; never blocks the analyser. Editorial intent: surface the state,
    let the journalist decide."""
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT
              (SELECT MAX(r.period) FROM observations o
                 JOIN releases r ON r.id = o.release_id
                WHERE o.id = ANY(
                  SELECT unnest(observation_ids) FROM findings
                   WHERE subkind = 'mirror_gap' AND superseded_at IS NULL
                )
              ) AS latest_mirror_gap_period,
              (SELECT MAX(period) FROM releases WHERE source = 'eurostat') AS latest_eurostat_period,
              (SELECT MAX(period) FROM releases WHERE source = 'gacc')     AS latest_gacc_period
            """
        )
        row = cur.fetchone()
    latest_mg = row["latest_mirror_gap_period"]
    latest_eu = row["latest_eurostat_period"]
    latest_gacc = row["latest_gacc_period"]

    if latest_mg is None:
        log.info(
            "Mirror-gap-trends staleness check: no active mirror_gap findings "
            "exist yet. The trend pass will be a no-op until you run "
            "--analyse mirror-trade first."
        )
        return

    upstream_latest = max(filter(None, [latest_eu, latest_gacc])) if (latest_eu or latest_gacc) else None
    if upstream_latest is None or latest_mg >= upstream_latest:
        log.info(
            "Mirror-gap-trends staleness check OK: latest mirror_gap is %s; "
            "latest Eurostat=%s, latest GACC=%s.",
            latest_mg, latest_eu, latest_gacc,
        )
        return

    log.warning(
        "Mirror-gap-trends staleness: latest active mirror_gap finding is %s, "
        "but Eurostat data extends to %s and GACC to %s. "
        "Re-run --analyse mirror-trade first to refresh the upstream findings, "
        "or expect this trend pass to operate on stale input.",
        latest_mg, latest_eu, latest_gacc,
    )


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
        min_baseline_n: hard floor — below this, refuse to compute a z-score
                        at all (default 3, the minimum for a meaningful
                        stdev). Findings with 3 ≤ n < LOW_BASELINE_N_THRESHOLD
                        DO emit but carry a `low_baseline_n` caveat so a
                        journalist knows the confidence is limited.

    Returns counts: {'emitted', 'inserted_new', 'confirmed_existing',
                     'superseded', 'skipped_insufficient_baseline',
                     'skipped_zero_stdev', 'skipped_below_threshold'}.
    """
    counts = {
        "emitted": 0,
        "inserted_new": 0, "confirmed_existing": 0, "superseded": 0,
        "skipped_insufficient_baseline": 0,
        "skipped_zero_stdev": 0, "skipped_below_threshold": 0,
    }

    # Phase 2.6: staleness check. The trend analyser builds on existing
    # mirror_gap findings, so if the upstream pass hasn't been re-run after
    # new Eurostat data landed, the trend pass is operating on stale input.
    # We log a WARNING with the exact periods so the journalist sees what's
    # happening — not auto-triggering, because that hides which pass
    # produced what.
    _log_mirror_gap_staleness()

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
                action = _insert_zscore_finding(
                    analysis_run_id, point, baseline, mean, stdev, z, window_months,
                )
                _tally(counts, action)

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
) -> findings_io.EmitAction:
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
    if len(baseline) < LOW_BASELINE_N_THRESHOLD:
        body += (
            f"\n\n⚠ LOW BASELINE-N FLAG: this z-score rests on only "
            f"{len(baseline)} prior periods (below the "
            f"{LOW_BASELINE_N_THRESHOLD}-point confidence threshold). The "
            f"stdev estimate is noisy at this baseline length; the |z| value "
            f"is mathematically computed but should not be quoted as if it "
            f"carried the same weight as a full-window baseline. See caveat "
            f"`low_baseline_n`."
        )
    # Per-finding caveats: only what varies. Universal mirror_gap_zscore
    # caveats (cif_fob, currency_timing, general_vs_special_trade,
    # transshipment, eurostat_stat_procedure_mix, multi_partner_sum,
    # aggregate_composition_drift) render once in the brief footer.
    caveat_codes: list[str] = []
    # Phase 1.4: flag low-confidence z-scores rather than dropping them.
    # The mathematical floor was already enforced upstream (min_baseline_n);
    # this caveat says "we computed a z-score, but the baseline is short
    # enough that a journalist should weigh it accordingly."
    if len(baseline) < LOW_BASELINE_N_THRESHOLD:
        caveat_codes.append("low_baseline_n")
    detail = {
        "method": "mirror_gap_zscore_v2_low_baseline_n_caveat",
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
            "low_n_threshold": LOW_BASELINE_N_THRESHOLD,
            "low_n_flag": len(baseline) < LOW_BASELINE_N_THRESHOLD,
        },
        "underlying_mirror_gap_finding_id": point.finding_id,
        "caveat_codes": caveat_codes,
    }
    score = abs(z)
    period_yyyymm = point.period.strftime("%Y-%m")

    with _conn() as conn, conn.cursor() as cur:
        _, action = findings_io.emit_finding(
            cur,
            scrape_run_id=analysis_run_id,
            kind="anomaly",
            subkind="mirror_gap_zscore",
            natural_key=findings_io.nk_mirror_gap_zscore(point.iso2, period_yyyymm),
            # If any of (gap_pct, baseline mean/stdev, z) moves, the finding
            # has revised — supersede. `method` included so version bumps
            # propagate even when numbers don't move.
            value_fields={
                "method": detail["method"],
                "gap_pct": round(point.gap_pct, 6),
                "z_score": round(z, 4),
                "baseline_mean": round(mean, 6),
                "baseline_stdev": round(stdev, 6),
                "baseline_n": len(baseline),
            },
            observation_ids=point.observation_ids,
            score=score,
            title=title,
            body=body,
            detail=detail,
        )
    return action


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
    patterns: list[str], flow: int = 1,
    partners: tuple[str, ...] | list[str] = EUROSTAT_PARTNERS,
    source: str = "eurostat",
) -> list[tuple[date, float, float, int, float]]:
    """Returns (period, total_eur, total_kg, n_raw_rows, eur_with_kg) per period.

    The 5th tuple element (`eur_with_kg`) is the value_eur summed only over
    rows where quantity_kg is non-null and > 0. The ratio
    `eur_with_kg / total_eur` over a window is the kg-coverage metric used
    by the unit-price decomposition (Phase 1.5 of dev_notes/history.md).

    Why this matters: groups dominated by HS codes that report a primary
    supplementary unit other than kg (machine tools by pieces, vehicles by
    units, beverages by litres) have low kg coverage. Computing a unit
    price as eur/kg over those groups is misleading — most of the
    transactions don't carry a kg value at all, so eur/kg is just (sum of
    eur over ALL transactions) / (sum of kg over the SUBSET that reported
    kg). The decomposition narrative (volume- vs. price-driven) is then
    derived from a partly-unrelated denominator.

    Queries eurostat_raw_rows rather than observations because raw_rows
    preserves quantity_kg as a native column. The aggregated
    `observations.quantity` field holds either the supplementary unit
    (PST, kg, etc.) or kg as fallback, which means cross-HS kg totals
    from observations would silently drop the kg of rows whose primary
    unit is something else.

    Periods with no matching rows are absent — caller handles gaps.
    """
    like_clause, like_params = _hs_pattern_or_clause(patterns)
    if source == "eurostat":
        sql = f"""
            SELECT period,
                   SUM(value_eur)                                            AS total_eur,
                   SUM(quantity_kg)                                          AS total_kg,
                   COUNT(*)                                                  AS n_raw,
                   SUM(value_eur) FILTER (WHERE quantity_kg IS NOT NULL
                                            AND quantity_kg > 0)             AS eur_with_kg
              FROM eurostat_raw_rows
             WHERE flow = %s
               AND partner = ANY(%s)
               AND {like_clause}
               AND reporter <> ALL(%s)   -- EU-27 excludes UK at all times
          GROUP BY period
          ORDER BY period
            """
        params = (flow, list(partners), *like_params, list(EU27_EXCLUDE_REPORTERS))
    elif source == "hmrc":
        # net_mass_kg is the HMRC equivalent of Eurostat quantity_kg.
        # SuppressionIndex != 0 means HMRC suppressed the value for
        # confidentiality; exclude those rows from the sum (already
        # excluded by the ingest aggregator but raw rows preserve them).
        sql = f"""
            SELECT period,
                   SUM(value_eur)                                            AS total_eur,
                   SUM(net_mass_kg)                                          AS total_kg,
                   COUNT(*)                                                  AS n_raw,
                   SUM(value_eur) FILTER (WHERE net_mass_kg IS NOT NULL
                                            AND net_mass_kg > 0)             AS eur_with_kg
              FROM hmrc_raw_rows
             WHERE flow = %s
               AND partner = ANY(%s)
               AND {like_clause}
               AND suppression_index = 0
          GROUP BY period
          ORDER BY period
            """
        params = (flow, list(partners), *like_params)
    else:
        raise ValueError(f"unknown source {source!r}; expected 'eurostat' or 'hmrc'")
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return [
            (row[0], float(row[1] or 0), float(row[2] or 0),
             int(row[3]), float(row[4] or 0))
            for row in cur.fetchall()
        ]


def _hs_group_top_cn8s(
    patterns: list[str], start: date, end: date, flow: int = 1, limit: int = 10,
    partners: tuple[str, ...] | list[str] = EUROSTAT_PARTNERS,
    source: str = "eurostat",
) -> list[dict]:
    """Top contributing HS-CN8 codes within a group across [start, end]."""
    like_clause, like_params = _hs_pattern_or_clause(patterns)
    if source == "eurostat":
        sql = f"""
            SELECT product_nc,
                   SUM(value_eur) AS total_eur,
                   SUM(quantity_kg) AS total_kg,
                   COUNT(*) AS n_raw
              FROM eurostat_raw_rows
             WHERE period >= %s AND period <= %s
               AND flow = %s AND partner = ANY(%s)
               AND {like_clause}
               AND reporter <> ALL(%s)
          GROUP BY product_nc
          ORDER BY SUM(value_eur) DESC NULLS LAST
             LIMIT %s
            """
        params = (start, end, flow, list(partners), *like_params, list(EU27_EXCLUDE_REPORTERS), limit)
    elif source == "hmrc":
        sql = f"""
            SELECT product_nc,
                   SUM(value_eur) AS total_eur,
                   SUM(net_mass_kg) AS total_kg,
                   COUNT(*) AS n_raw
              FROM hmrc_raw_rows
             WHERE period >= %s AND period <= %s
               AND flow = %s AND partner = ANY(%s)
               AND {like_clause}
               AND suppression_index = 0
          GROUP BY product_nc
          ORDER BY SUM(value_eur) DESC NULLS LAST
             LIMIT %s
            """
        params = (start, end, flow, list(partners), *like_params, limit)
    else:
        raise ValueError(f"unknown source {source!r}")
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return [{"hs_code": r[0], "total_eur": float(r[1] or 0),
                 "total_kg": float(r[2] or 0), "n_raw": int(r[3])}
                for r in cur.fetchall()]


def _hs_group_top_reporters(
    patterns: list[str], start: date, end: date, flow: int = 1, limit: int = 10,
    partners: tuple[str, ...] | list[str] = EUROSTAT_PARTNERS,
    source: str = "eurostat",
) -> list[dict]:
    """Top contributing reporters within a group across [start, end].
    For source='hmrc' the only reporter is GB, so this returns at most
    one row but keeps the same shape for downstream consumers."""
    like_clause, like_params = _hs_pattern_or_clause(patterns)
    if source == "eurostat":
        sql = f"""
            SELECT reporter,
                   SUM(value_eur) AS total_eur,
                   SUM(quantity_kg) AS total_kg,
                   COUNT(*) AS n_raw
              FROM eurostat_raw_rows
             WHERE period >= %s AND period <= %s
               AND flow = %s AND partner = ANY(%s)
               AND {like_clause}
               AND reporter <> ALL(%s)
          GROUP BY reporter
          ORDER BY SUM(value_eur) DESC NULLS LAST
             LIMIT %s
            """
        params = (start, end, flow, list(partners), *like_params, list(EU27_EXCLUDE_REPORTERS), limit)
    elif source == "hmrc":
        sql = f"""
            SELECT reporter,
                   SUM(value_eur) AS total_eur,
                   SUM(net_mass_kg) AS total_kg,
                   COUNT(*) AS n_raw
              FROM hmrc_raw_rows
             WHERE period >= %s AND period <= %s
               AND flow = %s AND partner = ANY(%s)
               AND {like_clause}
               AND suppression_index = 0
          GROUP BY reporter
          ORDER BY SUM(value_eur) DESC NULLS LAST
             LIMIT %s
            """
        params = (start, end, flow, list(partners), *like_params, limit)
    else:
        raise ValueError(f"unknown source {source!r}")
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
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
    flow: int = 1,
    low_base_threshold_eur: float = LOW_BASE_THRESHOLD_EUR,
    comparison_scope: str = COMPARISON_SCOPE_DEFAULT,
) -> dict[str, int]:
    """For each (hs_group, period_t) where 24 months of history exist, compute:
        current_12mo  = sum(value_eur) for periods [t-11 .. t]
        prior_12mo    = sum(value_eur) for periods [t-23 .. t-12]
        yoy_pct       = (current_12mo - prior_12mo) / abs(prior_12mo)
    Emit a finding when |yoy_pct| >= yoy_threshold_pct. Default 0.0 means emit
    one finding per (group, period) — useful for a 'current state' snapshot.

    `flow`: 1 = EU imports from China (default — the 'is China selling more X
    to Europe' question); 2 = EU exports to China (the 'is Europe selling more
    X to China' question, e.g. for the 'EU pork to China declining' angle).

    The Chinese-trade partner set is fixed at CN+HK+MO (see EUROSTAT_PARTNERS);
    the multi_partner_sum caveat is universal and lives in the methodology
    footer, not per-finding.

    Returns counts: {'emitted', 'skipped_insufficient_history', 'skipped_below_threshold', 'skipped_zero_prior'}.
    """
    if flow not in (1, 2):
        raise ValueError(f"flow must be 1 (import) or 2 (export); got {flow}")
    if comparison_scope not in VALID_COMPARISON_SCOPES:
        raise ValueError(
            f"comparison_scope must be one of {VALID_COMPARISON_SCOPES}; got {comparison_scope!r}"
        )
    partners: tuple[str, ...] = EUROSTAT_PARTNERS
    sources = COMPARISON_SCOPE_SOURCES[comparison_scope]
    counts = {
        "emitted": 0,
        "inserted_new": 0, "confirmed_existing": 0, "superseded": 0,
        "skipped_insufficient_history": 0,
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
            # Sum per-period totals across the configured comparison sources.
            # For eu_27_plus_uk we add EU-27 + UK rows per period; per-period
            # tuple shape (period, eur, kg, n_raw, eur_with_kg) matches the
            # single-source return type so downstream YoY logic is unchanged.
            per_period: dict[date, list[float | int]] = {}
            for src in sources:
                for (p, eur, kg, n, ek) in _hs_group_per_period_totals(
                    group.hs_patterns, flow=flow, partners=partners, source=src,
                ):
                    if p not in per_period:
                        per_period[p] = [0.0, 0.0, 0, 0.0]
                    per_period[p][0] += eur
                    per_period[p][1] += kg
                    per_period[p][2] += n
                    per_period[p][3] += ek
            series = [(p, *per_period[p]) for p in sorted(per_period)]
            eur_by_period: dict[date, float] = {p: e for p, e, _, _, _ in series}
            kg_by_period:  dict[date, float] = {p: k for p, _, k, _, _ in series}
            n_by_period:   dict[date, int]   = {p: n for p, _, _, n, _ in series}
            # Per-period: how much of value_eur was backed by an actual kg
            # measurement. Groups dominated by pieces/litres have low coverage.
            eur_with_kg_by_period: dict[date, float] = {
                p: ek for p, _, _, _, ek in series
            }
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

                # Phase 2.7: allow up to 1 missing month per window. The
                # most-recent Eurostat month often lags publication by 6-8
                # weeks; if we required all 24 months strictly we'd skip
                # every "close to current" window. Compromise: tolerate 1
                # gap, sum what's there, and tag the finding with a
                # `partial_window` caveat. 2+ gaps still skip.
                want = []
                p = start_prior
                while p <= end_curr:
                    want.append(p)
                    p = _months_back(p, -1)
                want_curr = [p for p in want if start_curr <= p <= end_curr]
                want_prior = [p for p in want if start_prior <= p <= end_prior]
                missing_curr = [p for p in want_curr if p not in eur_by_period]
                missing_prior = [p for p in want_prior if p not in eur_by_period]
                missing_total = len(missing_curr) + len(missing_prior)
                if missing_total > 1:
                    counts["skipped_insufficient_history"] += 1
                    continue
                partial_window = missing_total == 1

                current_eur = sum(eur_by_period[p] for p in want_curr if p in eur_by_period)
                prior_eur   = sum(eur_by_period[p] for p in want_prior if p in eur_by_period)
                current_kg  = sum(kg_by_period[p]  for p in want_curr if p in kg_by_period)
                prior_kg    = sum(kg_by_period[p]  for p in want_prior if p in kg_by_period)
                # kg coverage over the current rolling window: fraction of
                # value_eur backed by an actual kg measurement. Below the
                # threshold (default 80%) the unit-price decomposition is
                # suppressed downstream — see _insert_hs_group_yoy_finding.
                current_eur_with_kg = sum(
                    eur_with_kg_by_period[p] for p in want_curr
                    if p in eur_with_kg_by_period
                )
                kg_coverage_pct = (current_eur_with_kg / current_eur) if current_eur else 0.0

                if prior_eur == 0:
                    counts["skipped_zero_prior"] += 1
                    continue
                yoy_pct_eur = (current_eur - prior_eur) / abs(prior_eur)
                yoy_pct_kg  = ((current_kg - prior_kg) / abs(prior_kg)) if prior_kg else None

                # Single-month and 2-month-cumulative YoY (Phase 6.10).
                # Soapbox routinely quotes "Feb 2026 vs Feb 2025" or
                # "Jan-Feb 2026 vs Jan-Feb 2025" — single-period operators
                # rather than 12mo rolling. Compute them here so each
                # hs_group_yoy finding carries both views; the brief / xlsx
                # can render either depending on editorial register.
                #
                # end_prior is exactly 12 months before end_curr (set by the
                # 12mo window math above). So single-month YoY is
                # eur_by_period[end_curr] vs eur_by_period[end_prior];
                # 2-month-cumulative YoY is the last two months of each
                # window. Either becomes None if its period is missing —
                # we don't impute.
                def _yoy_or_none(c: float, p: float) -> float | None:
                    return ((c - p) / abs(p)) if (p is not None and p != 0) else None

                sm_curr_eur = eur_by_period.get(end_curr)
                sm_prior_eur = eur_by_period.get(end_prior)
                sm_curr_kg = kg_by_period.get(end_curr)
                sm_prior_kg = kg_by_period.get(end_prior)
                single_month: dict[str, Any] = {
                    "current_period": end_curr.isoformat(),
                    "prior_period": end_prior.isoformat(),
                    "current_eur": sm_curr_eur,
                    "prior_eur": sm_prior_eur,
                    "current_kg": sm_curr_kg,
                    "prior_kg": sm_prior_kg,
                    "yoy_pct": _yoy_or_none(sm_curr_eur, sm_prior_eur),
                    "yoy_pct_kg": _yoy_or_none(sm_curr_kg, sm_prior_kg),
                }

                # 2-month cumulative: end_curr + the month before, vs
                # end_prior + the month before that. want_curr[-2:] is the
                # last two months in the current window by construction;
                # likewise want_prior[-2:].
                last2_curr = want_curr[-2:] if len(want_curr) >= 2 else []
                last2_prior = want_prior[-2:] if len(want_prior) >= 2 else []
                t2_curr_eur = sum(eur_by_period.get(p, 0) for p in last2_curr) if last2_curr else None
                t2_prior_eur = sum(eur_by_period.get(p, 0) for p in last2_prior) if last2_prior else None
                t2_curr_kg = sum(kg_by_period.get(p, 0) for p in last2_curr) if last2_curr else None
                t2_prior_kg = sum(kg_by_period.get(p, 0) for p in last2_prior) if last2_prior else None
                two_month_cumulative: dict[str, Any] = {
                    "current_periods": [p.isoformat() for p in last2_curr],
                    "prior_periods": [p.isoformat() for p in last2_prior],
                    "current_eur": t2_curr_eur,
                    "prior_eur": t2_prior_eur,
                    "current_kg": t2_curr_kg,
                    "prior_kg": t2_prior_kg,
                    "yoy_pct": _yoy_or_none(t2_curr_eur, t2_prior_eur),
                    "yoy_pct_kg": _yoy_or_none(t2_curr_kg, t2_prior_kg),
                }

                # Keep the threshold gating on the EUR YoY (the editorial-relevance
                # signal). Even if kg YoY is small, big EUR moves matter.
                if abs(yoy_pct_eur) < yoy_threshold_pct:
                    counts["skipped_below_threshold"] += 1
                    continue

                # For top_cn8s and top_reporters, sum across configured
                # sources too. Top reporters across scopes: eu_27 yields
                # EU members; uk yields just GB; combined yields both.
                top_cn8s_by_code: dict[str, dict] = {}
                top_reporters_by_code: dict[str, dict] = {}
                for src in sources:
                    for c in _hs_group_top_cn8s(
                        group.hs_patterns, start_curr, end_curr,
                        flow=flow, partners=partners, source=src,
                    ):
                        existing = top_cn8s_by_code.get(c["hs_code"])
                        if existing is None:
                            top_cn8s_by_code[c["hs_code"]] = dict(c)
                        else:
                            existing["total_eur"] += c["total_eur"]
                            existing["total_kg"] += c["total_kg"]
                            existing["n_raw"] += c["n_raw"]
                    for r in _hs_group_top_reporters(
                        group.hs_patterns, start_curr, end_curr,
                        flow=flow, partners=partners, source=src,
                    ):
                        existing = top_reporters_by_code.get(r["reporter"])
                        if existing is None:
                            top_reporters_by_code[r["reporter"]] = dict(r)
                        else:
                            existing["total_eur"] += r["total_eur"]
                            existing["total_kg"] += r["total_kg"]
                            existing["n_raw"] += r["n_raw"]
                top_cn8s = sorted(top_cn8s_by_code.values(), key=lambda c: -c["total_eur"])[:10]
                top_reporters = sorted(top_reporters_by_code.values(), key=lambda r: -r["total_eur"])[:10]

                low_base = (
                    current_eur < low_base_threshold_eur
                    or prior_eur < low_base_threshold_eur
                )
                action = _insert_hs_group_yoy_finding(
                    analysis_run_id, group, t, start_curr, end_curr,
                    start_prior, end_prior,
                    current_eur, prior_eur, yoy_pct_eur,
                    current_kg,  prior_kg,  yoy_pct_kg,
                    series, top_cn8s, top_reporters, n_by_period,
                    kg_coverage_pct=kg_coverage_pct,
                    flow=flow, low_base=low_base,
                    low_base_threshold_eur=low_base_threshold_eur,
                    partial_window=partial_window,
                    missing_curr=missing_curr,
                    missing_prior=missing_prior,
                    partners=partners,
                    comparison_scope=comparison_scope,
                    single_month=single_month,
                    two_month_cumulative=two_month_cumulative,
                )
                _tally(counts, action)

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


KG_COVERAGE_DECOMPOSITION_THRESHOLD = 0.80
"""Below this fraction of value_eur backed by an actual kg value, the unit-
price decomposition is suppressed (NULL in detail.totals + body annotation
+ low_kg_coverage caveat). Phase 1.5 of dev_notes/history.md."""


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
    series: list[tuple[date, float, float, int, float]],
    top_cn8s: list[dict],
    top_reporters: list[dict],
    n_obs_by_period: dict[date, int],
    kg_coverage_pct: float = 1.0,
    flow: int = 1,
    low_base: bool = False,
    low_base_threshold_eur: float = LOW_BASE_THRESHOLD_EUR,
    partial_window: bool = False,
    missing_curr: list[date] | None = None,
    missing_prior: list[date] | None = None,
    partners: tuple[str, ...] | list[str] = EUROSTAT_PARTNERS,
    comparison_scope: str = COMPARISON_SCOPE_DEFAULT,
    single_month: dict[str, Any] | None = None,
    two_month_cumulative: dict[str, Any] | None = None,
) -> findings_io.EmitAction:
    direction = "up" if yoy_pct_eur > 0 else "down"
    kg_yoy_str = f"{yoy_pct_kg*100:+.1f}%" if yoy_pct_kg is not None else "n/a"
    # Reporter-side label depends on scope (editorial framing for the journalist).
    if comparison_scope == "eu_27":
        reporter_label = "EU imports from CN" if flow == 1 else "EU exports to CN"
    elif comparison_scope == "uk":
        reporter_label = "UK imports from CN" if flow == 1 else "UK exports to CN"
    else:  # eu_27_plus_uk
        reporter_label = "EU+UK imports from CN" if flow == 1 else "EU+UK exports to CN"
    flow_label = reporter_label
    flow_subkind_suffix = "" if flow == 1 else "_export"
    scope_subkind_suffix = COMPARISON_SCOPE_SUBKIND_SUFFIX[comparison_scope]
    low_base_marker = " ⚠ low-base" if low_base else ""
    # Phase 1.5: only compute and report unit prices when kg coverage is
    # high enough that eur/kg is editorially meaningful. For groups dominated
    # by pieces (machine tools, EV cars) or litres (beverages), kg coverage
    # is sparse and the decomposition is misleading.
    decomposition_suppressed = kg_coverage_pct < KG_COVERAGE_DECOMPOSITION_THRESHOLD
    if decomposition_suppressed:
        unit_price_curr = None
        unit_price_prior = None
        unit_price_pct = None
    else:
        unit_price_curr = (current_eur / current_kg) if current_kg else None
        unit_price_prior = (prior_eur / prior_kg) if prior_kg else None
        unit_price_pct = (
            ((unit_price_curr - unit_price_prior) / abs(unit_price_prior))
            if unit_price_curr is not None and unit_price_prior is not None and unit_price_prior != 0
            else None
        )

    title = (
        f"Component trend ({flow_label}): {group.name}, rolling 12mo to {end_curr.strftime('%Y-%m')}: "
        f"€{current_eur/1e9:,.2f}B ({yoy_pct_eur*100:+.1f}% {direction} value, "
        f"{kg_yoy_str} kg){low_base_marker}"
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
    if decomposition_suppressed:
        body_lines.append(
            f"  Unit price (€/kg): SUPPRESSED — only {kg_coverage_pct*100:.0f}% of "
            f"value_eur in this group is backed by a non-zero kg measurement "
            f"(threshold {KG_COVERAGE_DECOMPOSITION_THRESHOLD*100:.0f}%). The group is "
            f"dominated by HS codes whose primary unit is something other than kg "
            f"(pieces, litres, etc.); a unit price computed as eur/kg over the "
            f"subset that did report kg would be misleading. The volume- vs. "
            f"price-driven decomposition is therefore omitted. See caveat "
            f"'low_kg_coverage'."
        )
    elif unit_price_curr is not None and unit_price_prior is not None:
        body_lines.append(
            f"  Unit price: €{unit_price_curr:,.4f}/kg current vs €{unit_price_prior:,.4f}/kg prior"
            f" ({unit_price_pct*100:+.2f}% change)"
        )
        body_lines.append(
            f"  kg coverage in current 12mo: {kg_coverage_pct*100:.0f}% of value_eur."
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
        "Unit prices computed as value/kg from raw rows. Methodological caveats "
        "that apply to every hs_group_yoy finding (cif_fob, currency_timing, "
        "classification_drift, eurostat_stat_procedure_mix, multi_partner_sum, "
        "cn8_revision) are documented once in the briefing pack methodology footer."
    )
    if low_base:
        body_lines.append("")
        body_lines.append(
            f"⚠ LOW-BASE FLAG: prior 12mo €{prior_eur:,.0f} (or current €{current_eur:,.0f}) "
            f"is below the €{low_base_threshold_eur:,.0f} threshold. The {yoy_pct_eur*100:+.1f}% "
            f"figure rests on a small denominator — interpret alongside the absolute figures "
            f"and consider whether a single shipment or a niche reclassification could have "
            f"driven the apparent change. See caveat 'low_base_effect'."
        )

    # Per-finding caveats: only what varies. Universal hs_group_yoy caveats
    # (cif_fob, currency_timing, classification_drift,
    # eurostat_stat_procedure_mix, multi_partner_sum, cn8_revision) render
    # once in the brief footer. cn8_revision is universal because the
    # analyser's 24-month window always spans at least one CN8 revision
    # boundary (Eurostat revises annually each January).
    caveat_codes: list[str] = []
    if low_base:
        caveat_codes.append("low_base_effect")
    if decomposition_suppressed:
        caveat_codes.append("low_kg_coverage")
    if partial_window:
        # Phase 2.7: window has 1 missing month. Sums are incomplete; YoY
        # comparison is on partial data. Editorially we keep the finding
        # rather than dropping signal, but a journalist quoting the % must
        # weigh the partial-window caveat.
        caveat_codes.append("partial_window")
        body_lines.append("")
        missing_strs = ", ".join(
            d.strftime("%Y-%m") for d in (missing_curr or []) + (missing_prior or [])
        )
        body_lines.append(
            f"⚠ PARTIAL WINDOW: 1 month is missing from this 24-month window "
            f"({missing_strs}). The current/prior totals sum what's there; "
            f"the YoY comparison is therefore on partial data. The most-recent "
            f"Eurostat month often lags publication by 6-8 weeks — re-check the "
            f"finding once that month has been ingested. See caveat 'partial_window'."
        )

    partner_list = list(partners)
    if comparison_scope == "eu_27_plus_uk":
        # Cross-source sum: Eurostat (EUR-native) + HMRC (GBP→EUR via period
        # FX). The two sources have different threshold rules, suppression
        # policies, and revision cycles; direct addition is a useful editorial
        # approximation but the journalist should know.
        caveat_codes.append("cross_source_sum")

    sources_used = list(COMPARISON_SCOPE_SOURCES[comparison_scope])
    detail = {
        "method": "hs_group_yoy_v10_single_month_and_two_month_cumulative",
        "method_query": {
            "sources": sources_used,
            "comparison_scope": comparison_scope,
            "flow": flow,
            "partners": partner_list,
            "flow_label": flow_label,
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
            "kg_coverage_pct": kg_coverage_pct,
            "kg_coverage_threshold": KG_COVERAGE_DECOMPOSITION_THRESHOLD,
            "decomposition_suppressed": decomposition_suppressed,
            "low_base": low_base,
            "low_base_threshold_eur": low_base_threshold_eur,
            # Phase 2.7
            "partial_window": partial_window,
            "missing_months_current": [d.isoformat() for d in (missing_curr or [])],
            "missing_months_prior": [d.isoformat() for d in (missing_prior or [])],
            "n_months_used_current": 12 - len(missing_curr or []),
            "n_months_used_prior": 12 - len(missing_prior or []),
            # Phase 6.10: single-month and 2-month-cumulative YoY views,
            # for editorial registers that quote a single recent month
            # (Soapbox: "Feb 2026 EU-CN exports -16.2% YoY") or the latest
            # 2-month cumulative (Soapbox: "Jan-Feb 2026 -11% YoY") rather
            # than the 12mo rolling default. Either yoy_pct is None when
            # the underlying period is missing — we don't impute.
            "single_month": single_month,
            "two_month_cumulative": two_month_cumulative,
        },
        "monthly_series": [
            {"period": p.isoformat(), "value_eur": e, "quantity_kg": k,
             "unit_price_eur_per_kg": (e / k) if k else None,
             "n_raw_rows": n_obs_by_period.get(p, 0)}
            for (p, e, k, _, _) in series
            if start_prior <= p <= end_curr
        ],
        "top_cn8_codes_in_current_12mo": top_cn8s,
        "top_reporters_in_current_12mo": top_reporters,
        "caveat_codes": caveat_codes,
    }
    # Score reflects EUR YoY (the editorial-comparable signal across groups).
    score = abs(yoy_pct_eur)

    # Subkind layout (Phase 6.1d): 'hs_group_yoy{scope_suffix}{flow_suffix}'.
    # scope_suffix is '' for eu_27 (default, backward-compatible), '_uk', or
    # '_combined'. flow_suffix is '' for imports (flow=1), '_export' for
    # flow=2. Examples: hs_group_yoy / hs_group_yoy_export / hs_group_yoy_uk
    # / hs_group_yoy_uk_export / hs_group_yoy_combined / hs_group_yoy_combined_export.
    subkind = f"hs_group_yoy{scope_subkind_suffix}{flow_subkind_suffix}"
    current_end_yyyymm = end_curr.strftime("%Y-%m")
    with _conn() as conn, conn.cursor() as cur:
        _, action = findings_io.emit_finding(
            cur,
            scrape_run_id=analysis_run_id,
            kind="anomaly",
            subkind=subkind,
            natural_key=findings_io.nk_hs_group_yoy(group.id, current_end_yyyymm),
            # Editorially-meaningful values: the headline numbers a journalist
            # would notice if they shifted. Top-N CN8 / reporter lists and the
            # full monthly_series live in `detail` but aren't part of the
            # signature — they shift constantly without the *story* changing.
            # `method` included so version bumps propagate.
            value_fields={
                "method": detail["method"],
                "yoy_pct": round(yoy_pct_eur, 6) if yoy_pct_eur is not None else None,
                "current_eur": round(current_eur, 2),
                "prior_eur": round(prior_eur, 2),
                "yoy_pct_kg": round(yoy_pct_kg, 6) if yoy_pct_kg is not None else None,
                "current_kg": round(current_kg, 2) if current_kg is not None else None,
                "unit_price_pct": round(unit_price_pct, 6) if unit_price_pct is not None else None,
                "low_base": low_base,
            },
            hs_group_ids=[group.id],
            score=score,
            title=title,
            body="\n".join(body_lines),
            detail=detail,
        )
    return action


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
    "dip_recovery":          "dip-and-recovery (was rising, dipped, now rising again)",
    "failed_recovery":       "failed recovery (was falling, briefly rose, now falling again)",
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


def _theil_sen_slope(xs: list[float], ys: list[float]) -> float:
    """Theil-Sen slope estimator: median of pairwise slopes.

    Used in place of OLS for the trajectory classifier (Phase 1.3 of
    dev_notes/history.md). OLS is sensitive to outliers at
    the endpoints — a single extreme first or last window can flip the
    accelerating/decelerating classification. Theil-Sen takes the
    median of all pairwise slopes, which gives the same answer as OLS
    on clean data but is unmoved by individual outliers.

    Cost is O(n²) but at our series lengths (24–50 windows) that's
    300–1200 pairwise comparisons, well under a millisecond.

    Returns 0 if N < 2 or all xs are equal.
    """
    n = len(xs)
    if n < 2:
        return 0.0
    slopes: list[float] = []
    for i in range(n):
        for j in range(i + 1, n):
            dx = xs[j] - xs[i]
            if dx != 0:
                slopes.append((ys[j] - ys[i]) / dx)
    if not slopes:
        return 0.0
    return statistics.median(slopes)


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


def _sign_runs(signs: list[int]) -> list[tuple[int, int, int]]:
    """Collapse a sign sequence into [(sign, start_idx, end_idx_inclusive), ...].
    Zero values are absorbed into the surrounding non-zero run; if the series
    starts with zeros, they're absorbed into the first non-zero run."""
    n = len(signs)
    if n == 0:
        return []
    # Find first non-zero sign to anchor. If the whole thing is zero, return one zero run.
    first_nz = next((s for s in signs if s != 0), 0)
    if first_nz == 0:
        return [(0, 0, n - 1)]
    runs: list[tuple[int, int, int]] = []
    current = first_nz
    start = 0
    for i in range(n):
        s = signs[i]
        if s != 0 and s != current:
            runs.append((current, start, i - 1))
            current = s
            start = i
    runs.append((current, start, n - 1))
    return runs


SEASONAL_AUTOCORR_LAG = 12   # lag-12 autocorrelation captures annual seasonality
SEASONAL_SIGNAL_THRESHOLD = 0.5  # |autocorr| above this counts as a strong seasonal signal


def _autocorrelation_at_lag(ys: list[float], lag: int) -> float | None:
    """Detrended Pearson correlation between `ys[:-lag]` and `ys[lag:]` —
    used to detect *seasonality*, not generic linear trend. Returns None
    if the series is too short (< lag+2 points) or either slice has zero
    variance after detrending.

    A naive Pearson autocorrelation at lag k is high for any
    monotonically-trending series (a straight line correlates with its
    shifted self), which is not what we want here — we want to flag
    series that *oscillate* annually, distinct from series that simply
    rise or fall. The fix is to subtract a Theil-Sen linear fit before
    computing the correlation; the residual carries the cyclical
    component if any exists.

    Editorial use: lag-12 autocorrelation on the rolling-12mo YoY series
    captures whether the YoY shape itself oscillates annually — e.g. a
    group whose YoY pattern repeats year-on-year (Christmas surge,
    Lunar-New-Year dip) rather than progressing linearly. Phase 2.5 of
    dev_notes/history.md."""
    n = len(ys)
    if n < lag + 2:
        return None
    # Detrend with Theil-Sen (robust). For a linear series the residuals
    # are essentially zero and we'll return None via zero-variance.
    xs = list(range(n))
    slope = _theil_sen_slope(xs, list(ys))
    # Intercept: median of (y_i - slope * x_i), the Theil-Sen intercept.
    intercept = statistics.median([ys[i] - slope * xs[i] for i in range(n)])
    residuals = [ys[i] - (slope * xs[i] + intercept) for i in range(n)]

    a = residuals[:-lag]
    b = residuals[lag:]
    m = len(a)
    mean_a = sum(a) / m
    mean_b = sum(b) / m
    cov = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(m))
    var_a = sum((a[i] - mean_a) ** 2 for i in range(m))
    var_b = sum((b[i] - mean_b) ** 2 for i in range(m))
    if var_a == 0 or var_b == 0:
        return None
    return cov / ((var_a * var_b) ** 0.5)


def _classify_trajectory(yoys: list[float], smooth_window: int | None = None) -> tuple[str, dict]:
    """Returns (shape_key, features_dict). The features carry the numeric
    evidence so a finding's body can spell out *why* it got the shape it did.

    Shape detection uses a smoothed copy of the YoY series so 1-period
    zero-crossings (typical of noisy real-world trade data) don't get labelled
    as 'volatile'. Raw stats are still reported in features for transparency.
    """
    n = len(yoys)
    if n < TRAJECTORY_MIN_WINDOWS:
        return "insufficient_data", {"n": n, "min_required": TRAJECTORY_MIN_WINDOWS}

    # Phase 2.4: smoothing window is now configurable. Default 3 (the historic
    # behaviour). Pass smooth_window=1 to disable smoothing for analyses
    # focused on short-term policy effects (tariff pre-loading, single-month
    # spikes that 3-window smoothing would absorb).
    if smooth_window is None:
        smooth_window = TRAJECTORY_SMOOTH_WINDOW
    smoothed = _smooth_centered(yoys, smooth_window)

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
    overall_slope = _theil_sen_slope(list(range(n)), smoothed)
    # Two-half slopes kept in features as additional evidence (not used in primary
    # classification — overall_slope handles the core distinction more robustly).
    half = n // 2
    earlier_slope = _theil_sen_slope(list(range(half)), smoothed[:half])
    recent_slope = _theil_sen_slope(list(range(n - half)), smoothed[half:])

    max_y = max(yoys); max_idx = yoys.index(max_y)
    min_y = min(yoys); min_idx = yoys.index(min_y)

    # Phase 2.5: detrended lag-12 autocorrelation of the raw YoY series.
    # POSITIVE autocorrelation at lag 12 means values 12 months apart move
    # together — the editorial signature of an annual cycle (Christmas
    # surge, Lunar-New-Year dip repeating year-on-year). Negative values
    # at lag 12 are common artefacts of detrending a U-shape or
    # inverse-U-shape (the second half mirrors the first), and are NOT
    # seasonality — they're already captured by the directional shape
    # vocabulary. So the strong-signal gate is positive-only.
    seasonal_autocorr = _autocorrelation_at_lag(yoys, SEASONAL_AUTOCORR_LAG)

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
        "smoothing_window": smooth_window,
        "seasonal_signal_strength": seasonal_autocorr,
        "seasonal_signal_lag": SEASONAL_AUTOCORR_LAG,
        "seasonal_signal_threshold": SEASONAL_SIGNAL_THRESHOLD,
        "has_strong_seasonal_signal": (
            seasonal_autocorr is not None
            and seasonal_autocorr >= SEASONAL_SIGNAL_THRESHOLD
        ),
        "thresholds": {
            "flat_mean_abs_yoy": TRAJECTORY_FLAT_MEAN_ABS_YOY,
            "flat_stdev": TRAJECTORY_FLAT_STDEV,
            "slope_significant": TRAJECTORY_SLOPE_SIGNIFICANT,
        },
    }

    # Order matters: most specific first.
    if mean_abs < TRAJECTORY_FLAT_MEAN_ABS_YOY and stdev < TRAJECTORY_FLAT_STDEV:
        return "flat", features

    if smoothed_sign_changes == 2:
        # Detect dip_recovery (positive → negative → positive) and
        # failed_recovery (negative → positive → negative) patterns by looking
        # at the run sequence on the smoothed series. These are common in
        # post-COVID trade data — a clean shape that 'volatile' was hiding.
        runs = _sign_runs(smoothed_signs)
        features["smoothed_runs"] = [
            {"sign": s, "start_idx": a, "end_idx": b, "length": b - a + 1}
            for (s, a, b) in runs
        ]
        if len(runs) == 3:
            first_sign, _, _ = runs[0]
            mid_sign, _, _ = runs[1]
            last_sign, _, _ = runs[2]
            if first_sign == last_sign and mid_sign == -first_sign and first_sign != 0:
                if last_sign > 0:
                    return "dip_recovery", features
                return "failed_recovery", features
        return "volatile", features

    if smoothed_sign_changes >= 3:
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


def detect_hs_group_trajectories(
    group_names: list[str] | None = None,
    flow: int = 1,
    low_base_threshold_eur: float = LOW_BASE_THRESHOLD_EUR,
    smooth_window: int | None = None,
    comparison_scope: str = COMPARISON_SCOPE_DEFAULT,
) -> dict[str, int]:
    """For each hs_group, classify the rolling-12mo-EUR YoY series across all
    available windows into a trajectory shape. Reads the matching hs_group_yoy
    findings (subkind 'hs_group_yoy' for flow=1, 'hs_group_yoy_export' for
    flow=2), one per period, emits one trajectory finding per group capturing
    the shape + supporting feature stats + underlying yoy finding ids.

    `flow`: 1 = imports (CN→EU); 2 = exports (EU→CN). The two are analysed
    separately and emit different subkinds ('hs_group_trajectory' vs
    'hs_group_trajectory_export') so their series can't accidentally mix.

    `low_base_threshold_eur`: per-window EUR threshold below which a YoY
    window counts as "low-base" for the low_base_majority feature. Defaults
    to LOW_BASE_THRESHOLD_EUR (€50M); pass a smaller value for niche-
    commodity investigations or a larger one for macro-only analysis.
    Phase 1.6 of dev_notes/history.md.

    Returns counts: {'emitted', 'skipped_insufficient_data', 'skipped_no_findings'}.
    """
    if flow not in (1, 2):
        raise ValueError(f"flow must be 1 (import) or 2 (export); got {flow}")
    if comparison_scope not in VALID_COMPARISON_SCOPES:
        raise ValueError(
            f"comparison_scope must be one of {VALID_COMPARISON_SCOPES}; got {comparison_scope!r}"
        )
    counts = {
        "emitted": 0,
        "inserted_new": 0, "confirmed_existing": 0, "superseded": 0,
        "skipped_insufficient_data": 0, "skipped_no_findings": 0,
        "skipped_incomplete_series": 0,
    }

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
            series = _fetch_group_yoy_series(group.id, flow=flow, comparison_scope=comparison_scope)
            if not series:
                counts["skipped_no_findings"] += 1
                continue
            # Phase 6.0.7: prior behaviour was to refuse to classify on any
            # series with a gap. That was too strict — most groups have at
            # least one missing month somewhere across the 8+ year backfill
            # (a publication-lag gap, a single-month YoY skip from
            # `skipped_zero_prior`, etc.), so trajectory was producing
            # ~5/58 expected findings. Replace with longest-contiguous-run:
            # find the longest unbroken stretch and classify on that. Honest
            # because we record exactly what window the shape was fitted to,
            # in `features.effective_first_period` and
            # `features.effective_last_period`. The min-windows safeguard
            # below still rejects too-short remnants.
            full_periods = [s["period"] for s in series]
            run_start, run_end = _longest_contiguous_run(full_periods)
            effective_series = series[run_start:run_end + 1]
            n_dropped = len(series) - len(effective_series)
            if n_dropped > 0:
                log.info(
                    "Trajectory truncated for group %r (flow=%d): YoY series had %d "
                    "missing periods between %s and %s; classifying on the longest "
                    "contiguous run %s → %s (%d windows kept, %d dropped).",
                    group.name, flow, len(full_periods) - len(effective_series),
                    series[0]["period"], series[-1]["period"],
                    effective_series[0]["period"], effective_series[-1]["period"],
                    len(effective_series), n_dropped,
                )
            yoys = [s["yoy_pct"] for s in effective_series]
            shape, features = _classify_trajectory(yoys, smooth_window=smooth_window)
            if shape == "insufficient_data":
                counts["skipped_insufficient_data"] += 1
                continue
            n_low_base = sum(1 for s in effective_series if s["current_eur"] < low_base_threshold_eur)
            features["n_low_base_windows"] = n_low_base
            features["low_base_threshold_eur"] = low_base_threshold_eur
            features["low_base_majority"] = (n_low_base / len(effective_series)) >= TRAJECTORY_LOW_BASE_FRACTION
            features["effective_first_period"] = effective_series[0]["period"].isoformat()
            features["effective_last_period"] = effective_series[-1]["period"].isoformat()
            features["original_series_length"] = len(series)
            features["effective_series_length"] = len(effective_series)
            features["dropped_periods_due_to_gaps"] = n_dropped
            action = _insert_trajectory_finding(
                analysis_run_id, group, effective_series, shape, features,
                flow=flow, comparison_scope=comparison_scope,
            )
            _tally(counts, action)

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


def _detect_series_gaps(periods: list[date]) -> list[date]:
    """Return the list of months missing from `periods` (sorted ascending).
    A continuous monthly series should have no gaps; a returned list of one
    or more periods means the trajectory classifier needs to handle the gap
    (Phase 6.0.7: truncate to longest contiguous run rather than skip the
    group entirely; see _longest_contiguous_run). Originally Phase 1.7 of
    dev_notes/history.md."""
    if len(periods) < 2:
        return []
    sorted_periods = sorted(periods)
    missing: list[date] = []
    p = sorted_periods[0]
    end = sorted_periods[-1]
    present = set(sorted_periods)
    while p < end:
        # Advance one month.
        p = date(p.year + 1, 1, 1) if p.month == 12 else date(p.year, p.month + 1, 1)
        if p > end:
            break
        if p not in present:
            missing.append(p)
    return missing


def _longest_contiguous_run(periods: list[date]) -> tuple[int, int]:
    """For a sorted list of monthly `periods`, return (start_idx, end_idx)
    INCLUSIVE — the slice spanning the longest contiguous stretch of
    consecutive months. Used by the trajectory classifier so it can fit
    on the longest unbroken sub-series rather than refusing to fit when
    any gap exists. For a series with no gaps, returns (0, len-1).
    Phase 6.0.7."""
    if not periods:
        return (0, -1)
    sorted_periods = sorted(periods)
    # Walk through the series; reset the run when a gap is found; track
    # the longest run seen so far.
    best_start = 0
    best_end = 0
    cur_start = 0
    for i in range(1, len(sorted_periods)):
        prev = sorted_periods[i - 1]
        expected = date(prev.year + 1, 1, 1) if prev.month == 12 else date(prev.year, prev.month + 1, 1)
        if sorted_periods[i] != expected:
            # Gap: previous run ended at i-1; start a new one at i.
            if (i - 1) - cur_start > best_end - best_start:
                best_start, best_end = cur_start, i - 1
            cur_start = i
    # Final run.
    if (len(sorted_periods) - 1) - cur_start > best_end - best_start:
        best_start, best_end = cur_start, len(sorted_periods) - 1
    return (best_start, best_end)


def _fetch_group_yoy_series(
    group_id: int, flow: int = 1,
    comparison_scope: str = COMPARISON_SCOPE_DEFAULT,
) -> list[dict]:
    """Return [{period, yoy_pct, finding_id, current_eur}] for the given group,
    one row per period (latest finding per period if there are duplicates).
    Subkind is built from flow + comparison_scope (Phase 6.1d):
    eu_27/uk/eu_27_plus_uk × import/export → six possible subkinds.
    Filters to active (un-superseded) findings only — trajectory always
    builds on the current revision."""
    flow_suffix = "" if flow == 1 else "_export"
    scope_suffix = COMPARISON_SCOPE_SUBKIND_SUFFIX[comparison_scope]
    subkind = f"hs_group_yoy{scope_suffix}{flow_suffix}"
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
             WHERE subkind = %s
               AND %s = ANY(hs_group_ids)
               AND superseded_at IS NULL
          ORDER BY (detail->'windows'->>'current_end')::date, created_at DESC
            """,
            (subkind, group_id),
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
    flow: int = 1,
    comparison_scope: str = COMPARISON_SCOPE_DEFAULT,
) -> findings_io.EmitAction:
    first = series[0]
    last  = series[-1]
    peak  = max(series, key=lambda s: s["yoy_pct"])
    trough = min(series, key=lambda s: s["yoy_pct"])

    if comparison_scope == "eu_27":
        flow_label = "EU imports from CN" if flow == 1 else "EU exports to CN"
    elif comparison_scope == "uk":
        flow_label = "UK imports from CN" if flow == 1 else "UK exports to CN"
    else:
        flow_label = "EU+UK imports from CN" if flow == 1 else "EU+UK exports to CN"
    flow_subkind_suffix = "" if flow == 1 else "_export"
    scope_subkind_suffix = COMPARISON_SCOPE_SUBKIND_SUFFIX[comparison_scope]
    low_base_marker = " ⚠ low-base" if features.get("low_base_majority") else ""
    title = (
        f"Trajectory ({flow_label}): {group.name} — {SHAPE_LABELS.get(shape, shape)} "
        f"(latest {last['yoy_pct']*100:+.1f}% YoY, "
        f"peak {peak['yoy_pct']*100:+.1f}% in {peak['period'].strftime('%Y-%m')}, "
        f"trough {trough['yoy_pct']*100:+.1f}% in {trough['period'].strftime('%Y-%m')}){low_base_marker}"
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
    if features.get("n_low_base_windows", 0) > 0:
        n_lb = features["n_low_base_windows"]
        body_lines.append("")
        body_lines.append(
            f"⚠ Low-base context: {n_lb} of {features['n']} 12-month windows have a current "
            f"total below the €{features['low_base_threshold_eur']:,.0f} low-base threshold. "
            + ("Most windows are low-base — the trajectory shape is dominated by small "
               "denominators and any percentage figures should be interpreted alongside "
               "absolute totals."
               if features.get("low_base_majority") else
               "Some windows are low-base; spot-check the underlying findings before "
               "quoting any single percentage."
            )
        )

    if features.get("has_strong_seasonal_signal"):
        # Phase 2.5: surface seasonality as body context, not as a 13th
        # shape. The shape vocabulary stays clean for editorial sorting;
        # the seasonal note pairs with whatever directional shape was
        # assigned ("dip_recovery with strong seasonal component").
        ac = features["seasonal_signal_strength"]
        body_lines.append("")
        body_lines.append(
            f"📅 Seasonal signal: lag-12 autocorrelation = {ac:+.2f} "
            f"(threshold ±{SEASONAL_SIGNAL_THRESHOLD:.2f}). The YoY series "
            f"oscillates annually — interpret the assigned shape "
            f"('{shape}') as the *direction* of the trend, with the "
            f"seasonal pattern overlaid. Compare period-to-same-period-"
            f"prior-year rather than month-to-month."
        )

    detail = {
        "method": "hs_group_trajectory_v8_comparison_scope",
        "flow": flow,
        "flow_label": flow_label,
        "comparison_scope": comparison_scope,
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
        # Per-finding caveats: only what varies. Universal hs_group_trajectory
        # caveats (cif_fob, currency_timing, classification_drift,
        # eurostat_stat_procedure_mix, multi_partner_sum, cn8_revision)
        # render once in the brief footer.
        "caveat_codes": (
            ["low_base_effect"] if features.get("low_base_majority") else []
        ),
    }
    # Score = absolute latest YoY; lets journalists rank by "how much movement is happening now".
    score = abs(last["yoy_pct"])

    # Subkind: hs_group_trajectory{scope_suffix}{flow_suffix} — same convention
    # as hs_group_yoy. eu_27 default keeps the bare subkind; uk and combined
    # get their own subkinds so trajectory findings don't supersede across scopes.
    subkind = f"hs_group_trajectory{scope_subkind_suffix}{flow_subkind_suffix}"
    with _conn() as conn, conn.cursor() as cur:
        # Trajectory natural key is just (hs_group_id) per flow — there's only
        # one current trajectory per group per flow at any time. New data lands
        # via supersede when shape or features change.
        _, action = findings_io.emit_finding(
            cur,
            scrape_run_id=analysis_run_id,
            kind="anomaly",
            subkind=subkind,
            natural_key=findings_io.nk_hs_group_trajectory(group.id),
            value_fields={
                "method": detail["method"],
                "shape": shape,
                "last_yoy": round(last["yoy_pct"], 6) if last.get("yoy_pct") is not None else None,
                "last_period": last["period"].isoformat(),
                "first_period": first["period"].isoformat(),
                "max_yoy": round(features.get("max_yoy", 0), 6),
                "min_yoy": round(features.get("min_yoy", 0), 6),
                "n": features.get("n"),
                "low_base_majority": features.get("low_base_majority"),
                # Phase 2.5: include the seasonal flag so changes propagate
                # via supersede. The numeric strength isn't included
                # (round-to-2dp would defeat the deterministic-hash purpose
                # for borderline values); the boolean flag is the editorial
                # signal that matters.
                "has_strong_seasonal_signal": features.get("has_strong_seasonal_signal", False),
                "smoothing_window": features.get("smoothing_window"),
            },
            hs_group_ids=[group.id],
            score=score,
            title=title,
            body="\n".join(body_lines),
            detail=detail,
        )
    return action


# =============================================================================
# GACC-aggregate YoY for non-EU partner aggregates (ASEAN, RCEP, Belt&Road, ...)
# =============================================================================
# Mirror-trade compares per-partner pairs where both GACC and Eurostat have
# data — i.e. EU members only (Eurostat doesn't cover non-EU partners). This
# analyser handles the other editorial story: tracking China's reported trade
# with non-EU aggregates over time, GACC-side only. No mirror-comparison, but
# a 12mo rolling YoY answers "is China-ASEAN trade growing or shrinking?" —
# exactly the kind of pattern story Soapbox / Merics regularly cover.
#
# Pre-requisite: country_aliases must have aggregate_kind set for the labels
# we want to analyse. Schema seeds these:
#   asean, rcep, belt_road, region (Africa, LatAm), world (Total).
# eu_bloc is excluded — mirror-trade handles EU.

GACC_AGGREGATE_KINDS: tuple[str, ...] = ("asean", "rcep", "belt_road", "region", "world")
GACC_AGGREGATE_TREND_SOURCE_URL = "analysis://gacc_aggregate_yoy/v1"


def detect_gacc_aggregate_yoy(
    flow: str = "export",
    aggregate_kinds: list[str] | None = None,
    yoy_threshold_pct: float = 0.0,
) -> dict[str, int]:
    """For each (aggregate, anchor_period) where 24 months of GACC history
    exist, compute the 12mo rolling YoY in EUR-equivalent. Emits one finding
    per (aggregate, anchor) under subkind 'gacc_aggregate_yoy[_import]'.

    Editorial use: surfaces "China-ASEAN trade rose +X%", "China-Belt&Road
    contracted -Y%" stories that mirror-trade can't tell because Eurostat
    has no data for non-EU partners. GACC-only — no mirror gap.

    `flow`: 'export' (China selling to bloc) or 'import' (China buying).
    `aggregate_kinds`: subset of GACC_AGGREGATE_KINDS. Default: all five.
    `yoy_threshold_pct`: minimum |yoy_pct| to emit (0.0 = emit one per anchor).
    """
    if flow not in ("export", "import"):
        raise ValueError(f"flow must be 'export' or 'import'; got {flow!r}")
    kinds = list(aggregate_kinds or GACC_AGGREGATE_KINDS)
    counts = {
        "emitted": 0,
        "inserted_new": 0, "confirmed_existing": 0, "superseded": 0,
        "skipped_insufficient_history": 0,
        "skipped_zero_prior": 0,
        "skipped_below_threshold": 0,
        "skipped_no_aggregates": 0,
    }

    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT id AS alias_id, raw_label, aggregate_kind
              FROM country_aliases
             WHERE source = 'gacc'
               AND aggregate_kind = ANY(%s)
          ORDER BY aggregate_kind, raw_label
            """,
            (kinds,),
        )
        aggregates = [dict(r) for r in cur.fetchall()]

    if not aggregates:
        counts["skipped_no_aggregates"] += 1
        return counts

    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'running') RETURNING id",
            (GACC_AGGREGATE_TREND_SOURCE_URL,),
        )
        analysis_run_id = cur.fetchone()[0]

    try:
        for agg in aggregates:
            series = _gacc_aggregate_per_period_totals(agg["raw_label"], flow=flow)
            if not series:
                counts["skipped_insufficient_history"] += 1
                continue
            eur_by_period: dict[date, float] = {p: e for p, e, _ in series}
            obs_by_period: dict[date, list[int]] = {p: ids for p, _, ids in series}
            periods_sorted = sorted(eur_by_period.keys())

            for t in periods_sorted:
                start_curr = _months_back(t, 11)
                end_curr   = t
                start_prior = _months_back(t, 23)
                end_prior   = _months_back(t, 12)

                # Walk through the 24-month window. GACC publishes Jan + Feb
                # as a combined cumulative release (Chinese New Year
                # disruption), and our parser doesn't yet handle the
                # "January-February YYYY" title format — so for non-EU
                # aggregate labels every Jan + Feb is a structural data gap.
                # Looser tolerance than hs_group_yoy: accept up to 4 missing
                # months per 24-month window (covers 2 Jan + 2 Feb), set
                # partial_window when ANY months are missing, and require at
                # least 8 of 12 months in EACH half so we don't compute YoY
                # on a half-empty side. Editorially: still useful, but the
                # caveat carries weight.
                want = []
                p = start_prior
                while p <= end_curr:
                    want.append(p)
                    p = _months_back(p, -1)
                want_curr = [p for p in want if start_curr <= p <= end_curr]
                want_prior = [p for p in want if start_prior <= p <= end_prior]
                missing_curr = [p for p in want_curr if p not in eur_by_period]
                missing_prior = [p for p in want_prior if p not in eur_by_period]
                n_curr = 12 - len(missing_curr)
                n_prior = 12 - len(missing_prior)
                if n_curr < 8 or n_prior < 8:
                    counts["skipped_insufficient_history"] += 1
                    continue
                partial_window = (len(missing_curr) + len(missing_prior)) > 0

                current_eur = sum(eur_by_period[p] for p in want_curr if p in eur_by_period)
                prior_eur   = sum(eur_by_period[p] for p in want_prior if p in eur_by_period)
                if prior_eur == 0:
                    counts["skipped_zero_prior"] += 1
                    continue
                yoy_pct = (current_eur - prior_eur) / abs(prior_eur)
                if abs(yoy_pct) < yoy_threshold_pct:
                    counts["skipped_below_threshold"] += 1
                    continue

                # Collect underlying observation_ids across the full window
                # so the finding can be traced back to source rows.
                obs_ids: list[int] = []
                for p in want_curr + want_prior:
                    obs_ids.extend(obs_by_period.get(p, []))

                action = _insert_gacc_aggregate_yoy_finding(
                    analysis_run_id, agg, t, start_curr, end_curr,
                    start_prior, end_prior,
                    current_eur, prior_eur, yoy_pct,
                    series, obs_ids, flow=flow,
                    partial_window=partial_window,
                    missing_curr=missing_curr,
                    missing_prior=missing_prior,
                )
                _tally(counts, action)

        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='success', ended_at=now() WHERE id=%s",
                (analysis_run_id,),
            )
    except Exception as e:
        log.exception("GACC-aggregate YoY analysis failed")
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='failed', error_message=%s, ended_at=now() WHERE id=%s",
                (str(e), analysis_run_id),
            )
        raise
    return counts


def _gacc_aggregate_per_period_totals(
    aggregate_label: str, flow: str = "export",
) -> list[tuple[date, float, list[int]]]:
    """Returns (period, total_eur, [observation_ids]) per period for the given
    GACC aggregate label. Filters to canonical CNY releases (USD releases
    duplicate the same underlying transactions in a different currency); does
    FX conversion to EUR via lookups.lookup_fx. Periods with no FX rate are
    skipped silently — the YoY analyser sees a gap and applies its
    partial_window tolerance.

    Editorial subtlety: aggregating partner='ASEAN' (etc.) sums the values as
    GACC published them; we don't decompose to per-member country totals. The
    GACC bloc total may differ from sum-of-individual-members on the same
    release if GACC reports a Total cell using different rounding."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                r.period,
                o.value_amount,
                r.unit,
                o.id AS obs_id
              FROM observations o
              JOIN releases r ON r.id = o.release_id
             WHERE r.source = 'gacc'
               AND r.currency = 'CNY'
               AND o.flow = %s
               AND o.period_kind = 'monthly'
               AND o.partner_country = %s
               AND o.value_amount IS NOT NULL
          ORDER BY r.period, o.id
            """,
            (flow, aggregate_label),
        )
        rows = cur.fetchall()

    by_period: dict[date, tuple[float, list[int]]] = {}
    for period, value_amount, unit, obs_id in rows:
        scale, currency = parse_unit_scale(unit)
        if scale is None:
            log.warning(
                "Skipping GACC aggregate %r row at %s — unrecognised unit %r",
                aggregate_label, period, unit,
            )
            continue
        ccy = currency or "CNY"
        fx = lookups.lookup_fx(ccy, "EUR", period)
        if fx is None:
            log.info(
                "Skipping GACC aggregate %r row at %s — no FX rate for %s/EUR",
                aggregate_label, period, ccy,
            )
            continue
        eur = float(value_amount) * scale * fx.rate
        existing = by_period.get(period)
        if existing is None:
            by_period[period] = (eur, [obs_id])
        else:
            # Multiple observations for the same period (e.g. preliminary +
            # revised release). Sum and append — version_seen would handle
            # this more cleanly, but for aggregate-level totals the sum is
            # already small so this rarely matters editorially.
            old_eur, old_ids = existing
            by_period[period] = (old_eur + eur, old_ids + [obs_id])

    return [(p, eur, ids) for p, (eur, ids) in sorted(by_period.items())]


def _insert_gacc_aggregate_yoy_finding(
    analysis_run_id: int,
    agg: dict,
    anchor_period: date,
    start_curr: date,
    end_curr: date,
    start_prior: date,
    end_prior: date,
    current_eur: float,
    prior_eur: float,
    yoy_pct: float,
    series: list[tuple[date, float, list[int]]],
    obs_ids: list[int],
    flow: str = "export",
    partial_window: bool = False,
    missing_curr: list[date] | None = None,
    missing_prior: list[date] | None = None,
) -> findings_io.EmitAction:
    direction = "up" if yoy_pct > 0 else "down"
    flow_label = "China exports to" if flow == "export" else "China imports from"
    flow_subkind_suffix = "" if flow == "export" else "_import"

    title = (
        f"GACC aggregate ({flow_label} {agg['raw_label']}): rolling 12mo to "
        f"{end_curr.strftime('%Y-%m')}: €{current_eur/1e9:,.2f}B "
        f"({yoy_pct*100:+.1f}% {direction} YoY)"
    )

    body_lines = [
        f"GACC aggregate: {agg['raw_label']} (kind={agg['aggregate_kind']})",
        f"Direction: {flow_label} {agg['raw_label']}",
        "",
        f"Rolling 12 months ending {end_curr.strftime('%Y-%m')}:",
        f"  Value:  €{current_eur:,.0f} ({yoy_pct*100:+.2f}% YoY vs €{prior_eur:,.0f})",
        "",
        ("This is GACC-side data only; no Eurostat counterpart exists for "
         "non-EU partner aggregates. Cross-reference with UN Comtrade or "
         "destination-country customs data when corroborating."),
    ]
    caveat_codes: list[str] = []
    if partial_window:
        caveat_codes.append("partial_window")
        n_missing = len(missing_curr or []) + len(missing_prior or [])
        missing_strs = ", ".join(
            d.strftime("%Y-%m") for d in (missing_curr or []) + (missing_prior or [])
        )
        body_lines.append("")
        body_lines.append(
            f"⚠ PARTIAL WINDOW: {n_missing} of 24 months missing from this window "
            f"({missing_strs}). GACC publishes Jan + Feb as a combined "
            f"cumulative release (Chinese New Year), and our parser doesn't yet "
            f"handle that format — so every January and February is a "
            f"structural data gap for non-EU aggregates. Sums are over "
            f"available months only; treat the YoY as approximate. See "
            f"caveat 'partial_window' and `dev_notes/forward-work-gacc-2018-parser.md` "
            f"(the title-format issue is the same one that blocks 2018)."
        )

    detail = {
        "method": "gacc_aggregate_yoy_v3_per_alias_natural_key",
        "method_query": {
            "source": "observations (source=gacc)",
            "flow": flow,
            "partner_country_label": agg["raw_label"],
            "aggregate_kind": agg["aggregate_kind"],
            "rolling_window_months": 12,
        },
        "aggregate": {
            "alias_id": agg["alias_id"],
            "raw_label": agg["raw_label"],
            "kind": agg["aggregate_kind"],
        },
        "windows": {
            "current_start": start_curr.isoformat(), "current_end": end_curr.isoformat(),
            "prior_start": start_prior.isoformat(), "prior_end": end_prior.isoformat(),
        },
        "totals": {
            "current_12mo_eur": current_eur,
            "prior_12mo_eur": prior_eur,
            "delta_eur": current_eur - prior_eur,
            "yoy_pct": yoy_pct,
            "partial_window": partial_window,
            "missing_months_current": [d.isoformat() for d in (missing_curr or [])],
            "missing_months_prior": [d.isoformat() for d in (missing_prior or [])],
        },
        "monthly_series": [
            {"period": p.isoformat(), "value_eur": e}
            for (p, e, _) in series
            if start_prior <= p <= end_curr
        ],
        "caveat_codes": caveat_codes,
    }
    score = abs(yoy_pct)
    subkind = f"gacc_aggregate_yoy{flow_subkind_suffix}"
    current_end_yyyymm = end_curr.strftime("%Y-%m")

    with _conn() as conn, conn.cursor() as cur:
        _, action = findings_io.emit_finding(
            cur,
            scrape_run_id=analysis_run_id,
            kind="anomaly",
            subkind=subkind,
            natural_key=findings_io.nk_gacc_aggregate_yoy(
                agg["alias_id"], agg["aggregate_kind"], current_end_yyyymm,
            ),
            value_fields={
                "method": detail["method"],
                "yoy_pct": round(yoy_pct, 6),
                "current_eur": round(current_eur, 2),
                "prior_eur": round(prior_eur, 2),
                "partial_window": partial_window,
            },
            observation_ids=sorted(set(obs_ids)),
            score=score,
            title=title,
            body="\n".join(body_lines),
            detail=detail,
        )
    return action


# =============================================================================
# GACC-bilateral aggregate YoY: EU bloc + single-country GACC partners.
# =============================================================================
# Complementary to `gacc_aggregate_yoy`: that one covers multi-country non-EU
# aggregates (ASEAN, RCEP, Africa, Latin America, Belt & Road, world Total).
# This one covers the gap surfaced by the 2026-05-12 Soapbox A1 re-test —
# the EU bloc and individual partner countries. The article's lead claim
# ("China's exports to the EU reached US$201bn ... +19% YoY in Jan-Apr 2026")
# is a bilateral YTD-cumulative figure that nothing in the existing pipeline
# emits as a finding, though the underlying observations are in the DB.
#
# Each finding carries three YoY operators side-by-side in detail.totals:
# - rolling_12mo: 12mo to anchor vs 12mo to anchor-12 (matches the existing
#   gacc_aggregate_yoy operator)
# - ytd_cumulative: Jan-to-anchor of current year vs Jan-to-anchor of prior
#   year (the Soapbox A1 framing)
# - single_month: anchor month vs same month prior year (the Soapbox A3
#   framing, "EU exports to China Feb 2026 -16.2% YoY")
# Sharing one finding keeps the supersede chain coherent — when underlying
# data revises, all three operators move together.

GACC_BILATERAL_TREND_SOURCE_URL = "analysis://gacc_bilateral_aggregate_yoy/v1"


def _gacc_partner_ytd_by_period(
    partner_label: str, flow: str,
) -> dict[date, tuple[float, list[int]]]:
    """Return {period: (eur_total, [obs_ids])} for the partner's
    period_kind='ytd' observations. Same currency + FX convention as
    `_gacc_aggregate_per_period_totals` so values are directly comparable."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.period, o.value_amount, r.unit, o.id AS obs_id
              FROM observations o
              JOIN releases r ON r.id = o.release_id
             WHERE r.source = 'gacc'
               AND r.currency = 'CNY'
               AND o.flow = %s
               AND o.period_kind = 'ytd'
               AND o.partner_country = %s
               AND o.value_amount IS NOT NULL
          ORDER BY r.period, o.id
            """,
            (flow, partner_label),
        )
        rows = cur.fetchall()
    by_period: dict[date, tuple[float, list[int]]] = {}
    for period, value_amount, unit, obs_id in rows:
        scale, currency = parse_unit_scale(unit)
        if scale is None:
            continue
        ccy = currency or "CNY"
        fx = lookups.lookup_fx(ccy, "EUR", period)
        if fx is None:
            continue
        eur = float(value_amount) * scale * fx.rate
        existing = by_period.get(period)
        if existing is None:
            by_period[period] = (eur, [obs_id])
        else:
            old_eur, old_ids = existing
            by_period[period] = (old_eur + eur, old_ids + [obs_id])
    return by_period


def detect_gacc_bilateral_aggregate_yoy(
    flow: str = "export",
    partner_labels: list[str] | None = None,
    yoy_threshold_pct: float = 0.0,
) -> dict[str, int]:
    """For each (bilateral partner, anchor_period) where ≥1 of the three
    YoY operators can be computed, emit one finding under subkind
    'gacc_bilateral_aggregate_yoy[_import]'.

    Coverage: EU bloc (country_aliases.aggregate_kind='eu_bloc') plus
    every single-country partner (aggregate_kind IS NULL) that has at
    least one matching GACC observation. The multi-country non-EU
    aggregates remain the domain of `detect_gacc_aggregate_yoy`.

    `flow`: 'export' (China selling to partner) or 'import' (China buying).
    `partner_labels`: subset of partner raw_labels. Default: all eligible.
    `yoy_threshold_pct`: minimum |rolling_12mo_yoy_pct| to emit. Default 0.0
        emits one finding per (partner, anchor) regardless of size — useful
        for state-of-play renders that want a finding for every period.

    Returns counts: emitted, inserted_new, confirmed_existing, superseded,
    skipped_insufficient_history, skipped_zero_prior, skipped_below_threshold.
    """
    if flow not in ("export", "import"):
        raise ValueError(f"flow must be 'export' or 'import'; got {flow!r}")

    counts = {
        "emitted": 0,
        "inserted_new": 0, "confirmed_existing": 0, "superseded": 0,
        "skipped_insufficient_history": 0,
        "skipped_zero_prior": 0,
        "skipped_below_threshold": 0,
        "skipped_no_partners": 0,
    }

    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        if partner_labels:
            cur.execute(
                """
                SELECT id AS alias_id, raw_label, aggregate_kind
                  FROM country_aliases
                 WHERE source = 'gacc'
                   AND raw_label = ANY(%s)
                   AND (aggregate_kind = 'eu_bloc' OR aggregate_kind IS NULL)
              ORDER BY raw_label
                """,
                (partner_labels,),
            )
        else:
            cur.execute(
                """
                SELECT id AS alias_id, raw_label, aggregate_kind
                  FROM country_aliases
                 WHERE source = 'gacc'
                   AND (aggregate_kind = 'eu_bloc' OR aggregate_kind IS NULL)
              ORDER BY aggregate_kind NULLS LAST, raw_label
                """
            )
        partners = [dict(r) for r in cur.fetchall()]

    if not partners:
        counts["skipped_no_partners"] += 1
        return counts

    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'running') RETURNING id",
            (GACC_BILATERAL_TREND_SOURCE_URL,),
        )
        analysis_run_id = cur.fetchone()[0]

    try:
        for p in partners:
            monthly_series = _gacc_aggregate_per_period_totals(p["raw_label"], flow=flow)
            if not monthly_series:
                counts["skipped_insufficient_history"] += 1
                continue
            ytd_by_period = _gacc_partner_ytd_by_period(p["raw_label"], flow=flow)
            eur_by_period: dict[date, float] = {pd: e for pd, e, _ in monthly_series}
            obs_by_period: dict[date, list[int]] = {pd: ids for pd, _, ids in monthly_series}
            periods_sorted = sorted(eur_by_period.keys())

            for t in periods_sorted:
                # ----- 12mo rolling YoY -----
                start_curr = _months_back(t, 11)
                end_curr   = t
                start_prior = _months_back(t, 23)
                end_prior   = _months_back(t, 12)
                want = []
                pd = start_prior
                while pd <= end_curr:
                    want.append(pd)
                    pd = _months_back(pd, -1)
                want_curr = [pd for pd in want if start_curr <= pd <= end_curr]
                want_prior = [pd for pd in want if start_prior <= pd <= end_prior]
                missing_curr = [pd for pd in want_curr if pd not in eur_by_period]
                missing_prior = [pd for pd in want_prior if pd not in eur_by_period]
                n_curr = 12 - len(missing_curr)
                n_prior = 12 - len(missing_prior)
                if n_curr < 8 or n_prior < 8:
                    counts["skipped_insufficient_history"] += 1
                    continue
                partial_window = (len(missing_curr) + len(missing_prior)) > 0
                current_eur = sum(eur_by_period[pd] for pd in want_curr if pd in eur_by_period)
                prior_eur   = sum(eur_by_period[pd] for pd in want_prior if pd in eur_by_period)
                if prior_eur == 0:
                    counts["skipped_zero_prior"] += 1
                    continue
                rolling_yoy = (current_eur - prior_eur) / abs(prior_eur)
                if abs(rolling_yoy) < yoy_threshold_pct:
                    counts["skipped_below_threshold"] += 1
                    continue

                # ----- YTD cumulative YoY -----
                # YTD at T = Jan..T of T's year (read from period_kind='ytd' obs).
                # YTD prior = same range one year earlier — look up the YTD
                # observation at the equivalent month of the prior year.
                ytd_block: dict | None = None
                t_minus_12 = date(t.year - 1, t.month, 1)
                curr_ytd = ytd_by_period.get(t)
                prior_ytd = ytd_by_period.get(t_minus_12)
                if curr_ytd is not None and prior_ytd is not None:
                    curr_ytd_eur, curr_ytd_obs = curr_ytd
                    prior_ytd_eur, prior_ytd_obs = prior_ytd
                    if prior_ytd_eur != 0:
                        ytd_yoy = (curr_ytd_eur - prior_ytd_eur) / abs(prior_ytd_eur)
                        ytd_block = {
                            "current_eur": curr_ytd_eur,
                            "prior_eur": prior_ytd_eur,
                            "delta_eur": curr_ytd_eur - prior_ytd_eur,
                            "yoy_pct": ytd_yoy,
                            "months_in_ytd": t.month,
                            "prior_period": t_minus_12.isoformat(),
                        }

                # ----- Single-month YoY -----
                sm_block: dict | None = None
                curr_sm = eur_by_period.get(t)
                prior_sm = eur_by_period.get(t_minus_12)
                if curr_sm is not None and prior_sm is not None and prior_sm != 0:
                    sm_yoy = (curr_sm - prior_sm) / abs(prior_sm)
                    sm_block = {
                        "current_eur": curr_sm,
                        "prior_eur": prior_sm,
                        "delta_eur": curr_sm - prior_sm,
                        "yoy_pct": sm_yoy,
                        "current_period": t.isoformat(),
                        "prior_period": t_minus_12.isoformat(),
                    }

                obs_ids: list[int] = []
                for pd in want_curr + want_prior:
                    obs_ids.extend(obs_by_period.get(pd, []))
                if curr_ytd is not None:
                    obs_ids.extend(curr_ytd[1])
                if prior_ytd is not None:
                    obs_ids.extend(prior_ytd[1])

                action = _insert_gacc_bilateral_aggregate_yoy_finding(
                    analysis_run_id, p, t,
                    start_curr, end_curr, start_prior, end_prior,
                    current_eur, prior_eur, rolling_yoy,
                    ytd_block, sm_block,
                    monthly_series, obs_ids, flow=flow,
                    partial_window=partial_window,
                    missing_curr=missing_curr,
                    missing_prior=missing_prior,
                )
                _tally(counts, action)

        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='success', ended_at=now() WHERE id=%s",
                (analysis_run_id,),
            )
    except Exception as e:
        log.exception("GACC-bilateral-aggregate YoY analysis failed")
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='failed', error_message=%s, ended_at=now() WHERE id=%s",
                (str(e), analysis_run_id),
            )
        raise
    return counts


def _insert_gacc_bilateral_aggregate_yoy_finding(
    analysis_run_id: int,
    partner: dict,
    anchor_period: date,
    start_curr: date,
    end_curr: date,
    start_prior: date,
    end_prior: date,
    current_eur: float,
    prior_eur: float,
    rolling_yoy: float,
    ytd_block: dict | None,
    sm_block: dict | None,
    monthly_series: list[tuple[date, float, list[int]]],
    obs_ids: list[int],
    flow: str = "export",
    partial_window: bool = False,
    missing_curr: list[date] | None = None,
    missing_prior: list[date] | None = None,
) -> findings_io.EmitAction:
    direction = "up" if rolling_yoy > 0 else "down"
    flow_label = "China exports to" if flow == "export" else "China imports from"
    flow_subkind_suffix = "" if flow == "export" else "_import"

    title = (
        f"GACC bilateral ({flow_label} {partner['raw_label']}): rolling 12mo to "
        f"{end_curr.strftime('%Y-%m')}: €{current_eur/1e9:,.2f}B "
        f"({rolling_yoy*100:+.1f}% {direction} YoY)"
    )

    body_lines = [
        f"GACC bilateral partner: {partner['raw_label']} "
        f"(kind={partner['aggregate_kind'] or 'single_country'})",
        f"Direction: {flow_label} {partner['raw_label']}",
        "",
        f"Rolling 12 months ending {end_curr.strftime('%Y-%m')}:",
        f"  Value:  €{current_eur:,.0f} ({rolling_yoy*100:+.2f}% YoY vs €{prior_eur:,.0f})",
    ]
    if ytd_block is not None:
        body_lines += [
            "",
            f"Year-to-date through {end_curr.strftime('%Y-%m')}:",
            f"  Value:  €{ytd_block['current_eur']:,.0f} "
            f"({ytd_block['yoy_pct']*100:+.2f}% YoY vs €{ytd_block['prior_eur']:,.0f})",
            f"  ({ytd_block['months_in_ytd']} months cumulative)",
        ]
    if sm_block is not None:
        body_lines += [
            "",
            f"Single-month ({end_curr.strftime('%Y-%m')}) vs same month prior year:",
            f"  Value:  €{sm_block['current_eur']:,.0f} "
            f"({sm_block['yoy_pct']*100:+.2f}% YoY vs €{sm_block['prior_eur']:,.0f})",
        ]
    body_lines += [
        "",
        ("GACC-side data only; the bilateral counterpart from the partner's "
         "own customs authority (Eurostat for EU members, HMRC for UK) is "
         "tracked via the mirror_gap and hs_group_yoy families. The bilateral "
         "aggregate here is the editorial register Soapbox / Merics quote "
         "directly — \"China-X trade rose Y%\" — and the natural framing for "
         "Jan-N month-to-date headlines."),
    ]

    caveat_codes: list[str] = []
    if partial_window:
        caveat_codes.append("partial_window")
        n_missing = len(missing_curr or []) + len(missing_prior or [])
        missing_strs = ", ".join(
            d.strftime("%Y-%m") for d in (missing_curr or []) + (missing_prior or [])
        )
        body_lines.append("")
        body_lines.append(
            f"⚠ PARTIAL WINDOW: {n_missing} of 24 months missing from the rolling-12mo window "
            f"({missing_strs}). GACC publishes Jan + Feb as a combined "
            f"cumulative release (Chinese New Year), and our parser doesn't yet "
            f"handle that format — every January is a structural data gap, "
            f"and February's monthly observation often duplicates the YTD. "
            f"The YTD operator is unaffected by this gap (read directly from "
            f"period_kind='ytd' rows). See caveat 'partial_window' and "
            f"`dev_notes/forward-work-gacc-2018-parser.md`."
        )

    detail = {
        "method": "gacc_bilateral_aggregate_yoy_v1_eu_and_single_countries",
        "method_query": {
            "source": "observations (source=gacc)",
            "flow": flow,
            "partner_country_label": partner["raw_label"],
            "aggregate_kind": partner["aggregate_kind"],
            "rolling_window_months": 12,
        },
        "partner": {
            "alias_id": partner["alias_id"],
            "raw_label": partner["raw_label"],
            "kind": partner["aggregate_kind"] or "single_country",
        },
        "windows": {
            "current_start": start_curr.isoformat(), "current_end": end_curr.isoformat(),
            "prior_start": start_prior.isoformat(), "prior_end": end_prior.isoformat(),
        },
        "totals": {
            # 12mo rolling — the primary operator + the one the analyser uses
            # for scoring and supersede-chain triggering. Sub-keys mirror the
            # hs_group_yoy "totals" block so downstream renders can share
            # formatters.
            "current_12mo_eur": current_eur,
            "prior_12mo_eur": prior_eur,
            "delta_eur": current_eur - prior_eur,
            "yoy_pct": rolling_yoy,
            "partial_window": partial_window,
            "missing_months_current": [d.isoformat() for d in (missing_curr or [])],
            "missing_months_prior": [d.isoformat() for d in (missing_prior or [])],
            # YTD cumulative — Jan..anchor of current year vs same range prior
            # year. Null when either side is missing. This is the Soapbox A1
            # editorial register ("Jan-Apr exports +19% YoY").
            "ytd_cumulative": ytd_block,
            # Single-month YoY — anchor month vs same month prior year. The
            # Soapbox A3 register ("Feb 2026 -16.2% YoY"). Null when prior
            # month is missing.
            "single_month": sm_block,
        },
        "monthly_series": [
            {"period": p.isoformat(), "value_eur": e}
            for (p, e, _) in monthly_series
            if start_prior <= p <= end_curr
        ],
        "caveat_codes": caveat_codes,
    }
    score = abs(rolling_yoy)
    subkind = f"gacc_bilateral_aggregate_yoy{flow_subkind_suffix}"
    current_end_yyyymm = end_curr.strftime("%Y-%m")

    with _conn() as conn, conn.cursor() as cur:
        _, action = findings_io.emit_finding(
            cur,
            scrape_run_id=analysis_run_id,
            kind="anomaly",
            subkind=subkind,
            natural_key=findings_io.nk_gacc_bilateral_aggregate_yoy(
                partner["alias_id"], current_end_yyyymm,
            ),
            value_fields={
                "method": detail["method"],
                "yoy_pct": round(rolling_yoy, 6),
                "current_eur": round(current_eur, 2),
                "prior_eur": round(prior_eur, 2),
                "ytd_yoy_pct": (
                    round(ytd_block["yoy_pct"], 6) if ytd_block else None
                ),
                "sm_yoy_pct": (
                    round(sm_block["yoy_pct"], 6) if sm_block else None
                ),
                "partial_window": partial_window,
            },
            observation_ids=sorted(set(obs_ids)),
            score=score,
            title=title,
            body="\n".join(body_lines),
            detail=detail,
        )
    return action
