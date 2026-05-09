"""Lookup helpers for the rigor layer.

Country labels, caveat metadata, FX rates, and other normalisation steps are
stored in DB tables (see schema.sql) so the mapping logic is transparent and
auditable. Functions here read those tables; the comparator code never embeds
country dictionaries or hardcoded rates.
"""

import logging
import os
from dataclasses import dataclass
from datetime import date

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)


def _conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


@dataclass
class CountryResolution:
    """The result of resolving a raw country label to an ISO-2 code or aggregate."""
    raw_label: str
    source: str
    iso2: str | None              # populated for single-country labels
    aggregate_kind: str | None    # populated for aggregates ('eu_bloc', 'asean', etc.)
    confidence: str               # 'high' | 'probable' | 'tentative'
    method: str
    notes: str | None
    alias_id: int | None          # FK into country_aliases; None for identity resolutions


def resolve_country(source: str, raw_label: str) -> CountryResolution | None:
    """Resolve a raw country label to ISO-2 (or aggregate) for the given source.

    Behaviour:
    - For source='eurostat', if `raw_label` is a 2-letter uppercase string, return an
      identity resolution (the alias table doesn't need to enumerate every ISO-2).
    - For all other sources (or non-ISO-2 Eurostat labels), look up in the
      `country_aliases` table.
    - Returns None when the label is unknown; caller decides how to handle.
    """
    if source == "eurostat" and len(raw_label) == 2 and raw_label.isalpha() and raw_label.isupper():
        return CountryResolution(
            raw_label=raw_label, source=source, iso2=raw_label, aggregate_kind=None,
            confidence="high", method="iso2 native (Eurostat reporter/partner code)",
            notes=None, alias_id=None,
        )

    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT id, iso2, aggregate_kind, confidence, method, notes
              FROM country_aliases
             WHERE source = %s AND raw_label = %s
            """,
            (source, raw_label),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return CountryResolution(
        raw_label=raw_label, source=source,
        iso2=row["iso2"], aggregate_kind=row["aggregate_kind"],
        confidence=row["confidence"], method=row["method"], notes=row["notes"],
        alias_id=row["id"],
    )


@dataclass
class Caveat:
    code: str
    summary: str
    detail: str | None
    applies_to: list[str]


def get_caveats(codes: list[str]) -> list[Caveat]:
    """Look up caveat rows by code. Unknown codes are silently skipped — caller
    can verify by comparing the returned codes against the requested list."""
    if not codes:
        return []
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            "SELECT code, summary, detail, applies_to FROM caveats WHERE code = ANY(%s)",
            (codes,),
        )
        rows = cur.fetchall()
    return [Caveat(code=r["code"], summary=r["summary"], detail=r["detail"],
                   applies_to=list(r["applies_to"] or [])) for r in rows]


@dataclass
class AggregateMembership:
    """Members of an aggregate label (e.g. 'European Union' → 27 ISO-2 codes), with
    the alias row that owns them and the citation source so any cross-source
    comparison built on this lookup can be audited back to its origin."""
    alias_id: int
    aggregate_kind: str
    members_iso2: list[str]
    sources: list[str]


def lookup_aggregate_members(alias_id: int, period: date | None = None) -> AggregateMembership | None:
    """Return ISO-2 codes of countries that are members of this aggregate at the
    given period. Members with valid_from > period or valid_to < period are
    excluded. Returns None if the alias has no member rows."""
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT aggregate_kind FROM country_aliases WHERE id = %s", (alias_id,))
        row = cur.fetchone()
        if row is None:
            return None
        agg_kind = row["aggregate_kind"]

        cur.execute(
            """
            SELECT member_iso2, source
              FROM country_aggregate_members
             WHERE aggregate_alias_id = %s
               AND (valid_from IS NULL OR %s IS NULL OR valid_from <= %s)
               AND (valid_to   IS NULL OR %s IS NULL OR valid_to   >= %s)
          ORDER BY member_iso2
            """,
            (alias_id, period, period, period, period),
        )
        rows = cur.fetchall()
    if not rows:
        return None
    return AggregateMembership(
        alias_id=alias_id,
        aggregate_kind=agg_kind,
        members_iso2=[r["member_iso2"] for r in rows],
        sources=sorted({r["source"] for r in rows}),
    )


@dataclass
class FxRate:
    rate: float
    rate_date: date
    rate_source: str
    rate_source_url: str | None
    rate_id: int


def lookup_fx(currency_from: str, currency_to: str, period: date) -> FxRate | None:
    """Find the most recent FX rate for the given currency pair on or before
    the period anchor. Returns None if no rate is available — caller must handle
    by skipping the conversion and recording a caveat rather than guessing.
    """
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT id, rate, rate_date, rate_source, rate_source_url
              FROM fx_rates
             WHERE currency_from = %s AND currency_to = %s AND rate_date <= %s
          ORDER BY rate_date DESC
             LIMIT 1
            """,
            (currency_from, currency_to, period),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return FxRate(
        rate=float(row["rate"]), rate_date=row["rate_date"],
        rate_source=row["rate_source"], rate_source_url=row["rate_source_url"],
        rate_id=row["id"],
    )


# =============================================================================
# Phase 2.1 / 2.2 lookups: transshipment hubs + CIF/FOB baselines.
# =============================================================================


@dataclass
class TransshipmentHub:
    iso2: str
    notes: str | None
    evidence_url: str | None


def lookup_transshipment_hub(iso2: str) -> TransshipmentHub | None:
    """Return the transshipment_hubs row for `iso2` if present, else None.
    Used by the mirror-trade analyser to auto-attach a `transshipment_hub`
    caveat when the partner is in the table."""
    if not iso2:
        return None
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            "SELECT iso2, notes, evidence_url FROM transshipment_hubs WHERE iso2 = %s",
            (iso2,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return TransshipmentHub(
        iso2=row["iso2"], notes=row["notes"], evidence_url=row["evidence_url"],
    )


@dataclass
class CifFobBaseline:
    baseline_pct: float
    source: str
    source_url: str | None
    partner_iso2: str | None  # None = global default
    baseline_id: int


def lookup_cif_fob_baseline(partner_iso2: str | None) -> CifFobBaseline | None:
    """Return the CIF/FOB baseline for the given partner_iso2 (or global if
    no per-partner row). Returns None if neither is configured — caller
    should treat that as a configuration error and skip the comparison
    rather than guessing."""
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Per-partner first; fall back to global.
        if partner_iso2:
            cur.execute(
                "SELECT id, partner_iso2, baseline_pct, source, source_url "
                "  FROM cif_fob_baselines WHERE partner_iso2 = %s",
                (partner_iso2,),
            )
            row = cur.fetchone()
            if row is not None:
                return _row_to_cif_fob(row)
        cur.execute(
            "SELECT id, partner_iso2, baseline_pct, source, source_url "
            "  FROM cif_fob_baselines WHERE partner_iso2 IS NULL"
        )
        row = cur.fetchone()
    if row is None:
        return None
    return _row_to_cif_fob(row)


def _row_to_cif_fob(row) -> CifFobBaseline:
    return CifFobBaseline(
        baseline_pct=float(row["baseline_pct"]),
        source=row["source"],
        source_url=row["source_url"],
        partner_iso2=row["partner_iso2"],
        baseline_id=row["id"],
    )
