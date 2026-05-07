"""Tests for the lookups module against the seeded test DB.

These tests rely on the seed data in schema.sql being present in gacc_test —
which conftest.clean_db preserves (it TRUNCATEs only the operational tables,
not country_aliases / caveats / fx_rates).
"""

from datetime import date

import psycopg2
import pytest

import lookups


@pytest.fixture(autouse=True)
def _direct_db_url(test_db_url, monkeypatch):
    """lookups.py calls _conn() which reads DATABASE_URL — point it at the test DB."""
    monkeypatch.setenv("DATABASE_URL", test_db_url)


def test_resolve_country_known_gacc_label():
    r = lookups.resolve_country("gacc", "Germany")
    assert r is not None
    assert r.iso2 == "DE"
    assert r.aggregate_kind is None
    assert r.confidence == "high"
    assert r.method == "name match"
    assert r.alias_id is not None


def test_resolve_country_aggregate_label():
    r = lookups.resolve_country("gacc", "European Union")
    assert r is not None
    assert r.iso2 is None
    assert r.aggregate_kind == "eu_bloc"
    assert "footnote" in (r.notes or "").lower()


def test_resolve_country_unknown_label_returns_none():
    assert lookups.resolve_country("gacc", "Nowhereistan") is None


def test_resolve_country_eurostat_iso2_identity():
    """Eurostat raw labels ARE ISO-2 codes — resolver should be identity, no DB hit needed."""
    r = lookups.resolve_country("eurostat", "DE")
    assert r is not None
    assert r.iso2 == "DE"
    assert r.aggregate_kind is None
    assert r.confidence == "high"
    assert r.alias_id is None  # identity resolution, no FK row
    assert "iso2 native" in r.method.lower()


def test_resolve_country_eurostat_non_iso2_falls_back_to_db(test_db_url):
    """Eurostat aggregate codes (e.g. 'EU27_2020') aren't 2-letter — they should
    look up in the alias table. We don't seed any here, so this returns None."""
    r = lookups.resolve_country("eurostat", "EU27_2020")
    assert r is None  # not seeded; correctly returns None rather than guessing


def test_get_caveats_round_trip():
    caveats = lookups.get_caveats(["cif_fob", "transshipment"])
    by_code = {c.code: c for c in caveats}
    assert "cif_fob" in by_code
    assert "transshipment" in by_code
    assert "mirror_gap" in by_code["cif_fob"].applies_to


def test_get_caveats_unknown_code_silently_skipped():
    caveats = lookups.get_caveats(["cif_fob", "definitely_not_a_caveat"])
    codes = {c.code for c in caveats}
    assert codes == {"cif_fob"}


@pytest.fixture
def empty_fx_table(test_db_url):
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE fx_rates RESTART IDENTITY")
    yield


def test_lookup_fx_returns_none_when_no_rate(empty_fx_table):
    """We haven't seeded any FX rates yet — the function should return None
    rather than guessing or interpolating."""
    assert lookups.lookup_fx("CNY", "EUR", date(2026, 2, 1)) is None


def test_lookup_fx_returns_rate_when_seeded(test_db_url, empty_fx_table):
    """The lookup picks the most recent rate_date on-or-before the period."""
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO fx_rates (currency_from, currency_to, rate_date, rate, rate_source, rate_source_url)
            VALUES
              ('CNY', 'EUR', %s, 0.131, 'ECB monthly average', 'https://data-api.ecb.europa.eu/...'),
              ('CNY', 'EUR', %s, 0.130, 'ECB monthly average', 'https://data-api.ecb.europa.eu/...')
            """,
            (date(2026, 1, 1), date(2026, 2, 1)),
        )

    r = lookups.lookup_fx("CNY", "EUR", date(2026, 2, 15))
    assert r is not None
    assert r.rate == 0.130  # the more recent on-or-before
    assert r.rate_date == date(2026, 2, 1)
    assert "ECB" in r.rate_source

    # Earlier period falls back to the earlier rate.
    r_earlier = lookups.lookup_fx("CNY", "EUR", date(2026, 1, 15))
    assert r_earlier.rate == 0.131
    assert r_earlier.rate_date == date(2026, 1, 1)
