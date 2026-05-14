"""Regression guard: a GACC release row's (currency, unit) pair must be
consistent. `CNY` must pair with `'CNY 100 Million'`, `USD` with
`'USD1 Million'`.

History — release 184 (period 2025-06-01, ingested 2026-05-12) was
stored with currency='CNY' but unit='USD1 Million' after the page was
re-scraped against the USD-denominated attachment. The aggregate
analysers then applied the USD scale to the v=1 CNY observations and
summed against the v=2 USD observations, inflating every GACC bilateral
12mo headline by ~0.6-0.7%. The fix lives in:

  - migrations/2026-05-14-fix-release-184-cny-usd-unit-mismatch.sql
  - db._assert_currency_unit_consistent (refuses bad rows at insert time)

Audit query (also reprinted in the live-DB test below) — if a regression
slips through the insert-time guard, this finds it:

    SELECT id, period, currency, unit, source_url
      FROM releases
     WHERE source='gacc' AND section_number=4
       AND ((currency='CNY' AND unit NOT LIKE 'CNY%')
         OR (currency='USD' AND unit NOT LIKE 'USD%'));
"""

import os
from datetime import date

import psycopg2
import pytest

import db
from parse import ReleaseMetadata


LIVE_DB_ENV = "GACC_LIVE_DATABASE_URL"


def _meta(currency: str, unit: str | None) -> ReleaseMetadata:
    return ReleaseMetadata(
        section_number=4,
        description="Total Export & Import Values by Country (Region)",
        period=date(2025, 6, 1),
        currency=currency,
        publication_date=None,
        unit=unit,
        excel_url=None,
        source_url="http://english.customs.gov.cn/Statics/test-mismatch.html",
        title="Test title",
    )


def test_cny_release_with_usd_unit_is_rejected(clean_db, test_db_url):
    """The release 184 incident shape: currency='CNY' with unit='USD1 Million'.
    Must raise before any row is inserted."""
    bad = _meta(currency="CNY", unit="USD1 Million")
    with pytest.raises(ValueError, match="must pair with unit='CNY 100 Million'"):
        db.find_or_create_gacc_release(bad, release_kind="preliminary")

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM releases")
        assert cur.fetchone()[0] == 0


def test_usd_release_with_cny_unit_is_rejected(clean_db, test_db_url):
    """Mirror case — should also be caught."""
    bad = _meta(currency="USD", unit="CNY 100 Million")
    with pytest.raises(ValueError, match="must pair with unit='USD1 Million'"):
        db.find_or_create_gacc_release(bad, release_kind="preliminary")


def test_consistent_pairs_are_accepted(clean_db, test_db_url):
    """Both legitimate combinations persist without complaint."""
    ok_cny = _meta(currency="CNY", unit="CNY 100 Million")
    rid_cny = db.find_or_create_gacc_release(ok_cny, release_kind="preliminary")
    assert rid_cny > 0

    ok_usd = ReleaseMetadata(
        **{**ok_cny.__dict__, "currency": "USD", "unit": "USD1 Million"}
    )
    rid_usd = db.find_or_create_gacc_release(ok_usd, release_kind="preliminary")
    assert rid_usd > 0 and rid_usd != rid_cny


def test_null_unit_is_accepted(clean_db, test_db_url):
    """Early-2018 release pages omit the unit annotation; we accept NULL
    so the historical backfill can still ingest. The downstream
    parse_unit_scale call in anomalies.py already handles unit=NULL as
    multiplier=1.0 and currency=None — the analyser then falls back to
    the release row's currency for FX."""
    meta = _meta(currency="CNY", unit=None)
    rid = db.find_or_create_gacc_release(meta, release_kind="preliminary")
    assert rid > 0


# --- Live-DB guard ----------------------------------------------------------


@pytest.fixture(scope="module")
def live_db_url() -> str:
    url = os.environ.get(LIVE_DB_ENV)
    if not url:
        pytest.skip(f"{LIVE_DB_ENV} not set; skipping live currency/unit check")
    return url


@pytest.fixture(scope="module")
def live_conn(live_db_url):
    conn = psycopg2.connect(live_db_url)
    yield conn
    conn.close()


def test_no_gacc_release_has_mismatched_currency_and_unit(live_conn):
    """Every active GACC section-4 release row's (currency, unit) pair
    must be one of the recognised combinations. Currently CNY/`'CNY 100
    Million'` and USD/`'USD1 Million'` — extend
    `db._GACC_CURRENCY_UNIT_PAIRS` if GACC ever ships a third."""
    with live_conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, period, currency, unit, source_url
              FROM releases
             WHERE source='gacc' AND section_number=4
               AND ((currency='CNY' AND unit NOT LIKE 'CNY%')
                 OR (currency='USD' AND unit NOT LIKE 'USD%'))
             ORDER BY period
             LIMIT 20
            """
        )
        mismatches = cur.fetchall()
    assert mismatches == [], (
        f"{len(mismatches)} GACC release(s) have a mismatched "
        f"(currency, unit) pair (first 20 shown). Run the migration in "
        f"migrations/2026-05-14-fix-release-184-cny-usd-unit-mismatch.sql "
        f"or, if this is a new currency/unit combo, extend "
        f"db._GACC_CURRENCY_UNIT_PAIRS: {mismatches}"
    )
