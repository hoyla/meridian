"""Regression test for finding B2 (2026-06-25 adversarial-correctness review).

GACC publishes each monthly bulletin in a CNY and a USD edition, ingested as
two separate `releases` rows (different release_id, both section 4). The
mirror-gap export selector `_select_gacc_export_rows` lacked the
`currency = 'CNY'` pin its aggregate siblings carry, and its DISTINCT ON key
includes release_id — so BOTH editions survived and every partner's mirror gap
was computed twice down two FX paths under the same natural key (iso2, period),
making the active finding's value and provenance obs_id nondeterministic.

The pin makes the selector return exactly one (the CNY) row per partner.
"""
from datetime import date

import psycopg2
import pytest

import anomalies

_PERIOD = date(2026, 3, 1)


def _gacc_release(cur, currency: str, unit: str) -> tuple[int, int]:
    url = f"http://example/gacc-{currency}-{_PERIOD:%Y%m}"
    cur.execute(
        "INSERT INTO releases (source, section_number, currency, release_kind, "
        "                      period, source_url, unit) "
        "VALUES ('gacc', 4, %s, 'preliminary', %s, %s, %s) RETURNING id",
        (currency, _PERIOD, url, unit),
    )
    rel = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'success') RETURNING id",
        (url,),
    )
    return rel, cur.fetchone()[0]


def _export_obs(cur, rel: int, run: int, partner: str, value: float, currency: str) -> None:
    cur.execute(
        "INSERT INTO observations (release_id, scrape_run_id, period_kind, flow, "
        "                          partner_country, hs_code, value_amount, "
        "                          value_currency, source_row) "
        "VALUES (%s, %s, 'monthly', 'export', %s, NULL, %s, %s, '{}')",
        (rel, run, partner, value, currency),
    )


def test_select_gacc_export_rows_pins_cny(clean_db, test_db_url):
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cny_rel, cny_run = _gacc_release(cur, "CNY", "CNY 100 Million")
        usd_rel, usd_run = _gacc_release(cur, "USD", "USD1 Million")
        # Same partner, same period — one row per edition.
        _export_obs(cur, cny_rel, cny_run, "United States", 100.0, "CNY")
        _export_obs(cur, usd_rel, usd_run, "United States", 14.0, "USD")
        # A second partner, to confirm the pin doesn't collapse distinct rows.
        _export_obs(cur, cny_rel, cny_run, "Germany", 50.0, "CNY")
        _export_obs(cur, usd_rel, usd_run, "Germany", 7.0, "USD")
    # `with psycopg2.connect(...) as conn` commits on clean block exit.

    rows = anomalies._select_gacc_export_rows(_PERIOD)

    # Exactly one row per partner (the CNY edition), not one per currency.
    assert len(rows) == 2
    assert {r["value_currency"] for r in rows} == {"CNY"}
    by_partner = {r["partner_country"]: float(r["value_amount"]) for r in rows}
    assert by_partner == {"United States": 100.0, "Germany": 50.0}
