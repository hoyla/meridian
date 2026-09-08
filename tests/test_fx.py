"""Tests for fx.py — parsing ECB SDMX-JSON and populating fx_rates.

Uses a saved ECB response fixture for parse tests (deterministic, no network).
The end-to-end populate test exercises the DB path against the saved fixture
via monkeypatching the network call out.
"""

from datetime import date
from pathlib import Path
from unittest.mock import patch

import psycopg2
import pytest

import fx

FIXTURES = Path(__file__).parent / "fixtures"
ECB_CNY_FIXTURE = FIXTURES / "ecb_cny_eur_monthly.json"
FIXTURE_URL = "https://data-api.ecb.europa.eu/service/data/EXR/M.CNY.EUR.SP00.A?format=jsondata&startPeriod=2025-10"


@pytest.fixture(autouse=True)
def _direct_db_url(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)


@pytest.fixture
def empty_fx_table(test_db_url):
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE fx_rates RESTART IDENTITY")
    yield


def _load_fixture_text() -> str:
    return ECB_CNY_FIXTURE.read_text()


def test_parse_extracts_seven_periods():
    """Fixture covers 2025-10 to 2026-04 (7 months at the time of capture)."""
    rates = fx.parse_ecb_response(_load_fixture_text(), currency_from="CNY", source_url=FIXTURE_URL)
    assert len(rates) == 7
    assert {r.rate_date for r in rates} == {
        date(2025, 10, 1), date(2025, 11, 1), date(2025, 12, 1),
        date(2026, 1, 1),  date(2026, 2, 1),  date(2026, 3, 1), date(2026, 4, 1),
    }


def test_parse_inverts_ecb_value_to_eur_per_unit():
    """ECB publishes CNY-per-EUR (e.g. 8.28 CNY = 1 EUR). We store EUR-per-CNY
    so amount_in_eur = amount_in_cny * rate. Verify the inversion is correct."""
    rates = fx.parse_ecb_response(_load_fixture_text(), currency_from="CNY", source_url=FIXTURE_URL)
    by_date = {r.rate_date: r for r in rates}

    oct_2025 = by_date[date(2025, 10, 1)]
    # ECB published 8.281034... for Oct 2025; inverse = 0.12076...
    assert abs(oct_2025.rate - (1 / 8.281034782608698)) < 1e-12
    assert oct_2025.currency_from == "CNY"
    assert oct_2025.currency_to == "EUR"
    assert "8.281034" in oct_2025.notes  # original ECB value preserved in notes for audit
    assert oct_2025.rate_source == "ECB monthly average"
    assert oct_2025.rate_source_url == FIXTURE_URL


def test_parse_skips_empty_observations():
    """If ECB returns null/0 for a period, skip it rather than guessing."""
    # Synthesise a response with a null observation.
    import json
    data = json.loads(_load_fixture_text())
    series = next(iter(data["dataSets"][0]["series"].values()))
    # Force one observation to None.
    first_key = next(iter(series["observations"]))
    series["observations"][first_key] = [None, 0, 0, None, None]
    rates = fx.parse_ecb_response(json.dumps(data), currency_from="CNY", source_url=FIXTURE_URL)
    assert len(rates) == 6  # one fewer than the original 7


def test_populate_inserts_and_skips_duplicates(empty_fx_table, test_db_url):
    """populate_fx_rates_from_ecb is idempotent thanks to the natural-key UNIQUE."""
    fixture_text = _load_fixture_text()
    with patch("fx.fetch_ecb_monthly_rates",
               return_value=fx.parse_ecb_response(fixture_text, currency_from="CNY", source_url=FIXTURE_URL)):
        first = fx.populate_fx_rates_from_ecb("CNY")
        assert first == {"inserted": 7, "skipped_existing": 0, "total_fetched": 7}

        second = fx.populate_fx_rates_from_ecb("CNY")
        assert second == {"inserted": 0, "skipped_existing": 7, "total_fetched": 7}

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), min(rate_date), max(rate_date) FROM fx_rates WHERE currency_from='CNY'")
        count, min_d, max_d = cur.fetchone()
    assert count == 7
    assert min_d == date(2025, 10, 1)
    assert max_d == date(2026, 4, 1)


def test_populate_no_op_when_fetch_returns_nothing(empty_fx_table):
    with patch("fx.fetch_ecb_monthly_rates", return_value=[]):
        result = fx.populate_fx_rates_from_ecb("CNY")
    assert result == {"inserted": 0, "skipped_existing": 0, "total_fetched": 0}


# ---------------------------------------------------------------------------
# ensure_recent_rates — the routine top-up that keeps the table from freezing.
#
# Regression cover for the Sept-2026 incident: fx_rates was populated by a
# single manual --fetch-fx in May 2026 and nothing ever refreshed it, so four
# months of GACC and HMRC data converted at April's rate.
# ---------------------------------------------------------------------------

def test_ensure_recent_rates_fetches_each_default_currency(empty_fx_table):
    """All three conversion pairs the pipeline needs are refreshed, not just one."""
    asked: list[str] = []

    def _fake(currency_from, since=None, **kw):
        asked.append(currency_from)
        return []

    with patch("fx.fetch_ecb_monthly_rates", side_effect=_fake):
        out = fx.ensure_recent_rates()

    assert asked == ["CNY", "GBP", "USD"]
    assert set(out) == {"CNY", "GBP", "USD"}


def test_ensure_recent_rates_inserts_then_is_a_no_op(empty_fx_table):
    """Idempotent: a second pass over unchanged ECB data inserts nothing."""
    parsed = fx.parse_ecb_response(
        _load_fixture_text(), currency_from="CNY", source_url=FIXTURE_URL,
    )
    with patch("fx.fetch_ecb_monthly_rates", return_value=parsed):
        first = fx.ensure_recent_rates(("CNY",))
        second = fx.ensure_recent_rates(("CNY",))

    assert first["CNY"]["inserted"] == 7
    assert second["CNY"]["inserted"] == 0
    assert second["CNY"]["skipped_existing"] == 7


def test_ensure_recent_rates_swallows_fetch_failure(empty_fx_table):
    """An ECB outage must not sink the cycle — the strict lookup downstream
    refuses the conversion, which fails loud rather than converting wrong."""
    with patch("fx.fetch_ecb_monthly_rates", side_effect=RuntimeError("ECB down")):
        out = fx.ensure_recent_rates(("CNY",))

    assert isinstance(out["CNY"], str)
    assert "ECB down" in out["CNY"]


def test_ensure_recent_rates_asks_only_for_a_trailing_window():
    """The routine top-up is a small request, not the full history."""
    seen: dict[str, object] = {}

    def _fake(currency_from, since=None, **kw):
        seen["since"] = since
        return []

    with patch("fx.fetch_ecb_monthly_rates", side_effect=_fake):
        fx.ensure_recent_rates(("CNY",), lookback_months=6, today=date(2026, 9, 8))

    assert seen["since"] == date(2026, 3, 1)
