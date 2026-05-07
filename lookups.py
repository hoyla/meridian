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
