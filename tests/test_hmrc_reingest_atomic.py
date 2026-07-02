"""Regression: HMRC re-ingest must be atomic (2026-07-01 fresh review, F1).

The old shape ran DELETE and INSERT in two independent transactions; a crash
between the two commits destroyed the period's raw layer permanently — the
release row from the original ingest still exists, so no probe would ever
re-ingest, and observations keep dangling hmrc_raw_row_ids the whole time.
`replace_hmrc_raw_rows_for_period` runs both in one transaction: a failed
insert rolls the delete back.
"""

from datetime import date

import psycopg2
import pytest

import db

PERIOD = date(2026, 3, 1)


def _row(partner="DE", product_nc="85076000", value_gbp=1000):
    return {
        "period": PERIOD,
        "reporter": "GB",
        "partner": partner,
        "product_nc": product_nc,
        "product_hs6": product_nc[:6],
        "product_hs4": product_nc[:4],
        "product_hs2": product_nc[:2],
        "flow_type_id": 1,
        "flow": 1,
        "suppression_index": 0,
        "port_id": None,
        "value_gbp": value_gbp,
        "value_eur": value_gbp * 1.17,
        "net_mass_kg": 10,
        "suppl_unit": None,
    }


def _period_count() -> int:
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM hmrc_raw_rows WHERE period = %s", (PERIOD,)
        )
        return cur.fetchone()[0]


def test_replace_swaps_prior_rows(clean_db, test_db_url):
    run1 = db.start_run("https://api.uktradeinfo.com/test-run-1")
    deleted, ids = db.replace_hmrc_raw_rows_for_period(
        run1, PERIOD, [_row(), _row(partner="FR")]
    )
    assert deleted == 0
    assert len(ids) == 2

    run2 = db.start_run("https://api.uktradeinfo.com/test-run-2")
    deleted, ids2 = db.replace_hmrc_raw_rows_for_period(
        run2, PERIOD, [_row(value_gbp=2000)]
    )
    assert deleted == 2
    assert len(ids2) == 1
    assert _period_count() == 1


def test_failed_insert_rolls_back_the_delete(clean_db, test_db_url):
    run1 = db.start_run("https://api.uktradeinfo.com/test-run-1")
    db.replace_hmrc_raw_rows_for_period(run1, PERIOD, [_row(), _row(partner="FR")])
    assert _period_count() == 2

    # partner is NOT NULL, so the multi-row INSERT statement fails as a whole.
    run2 = db.start_run("https://api.uktradeinfo.com/test-run-2")
    bad_batch = [_row(value_gbp=2000), _row(partner=None)]
    with pytest.raises(psycopg2.Error):
        db.replace_hmrc_raw_rows_for_period(run2, PERIOD, bad_batch)

    # The prior rows must survive: the delete rolled back with the insert.
    assert _period_count() == 2


def test_refuses_an_empty_batch(clean_db, test_db_url):
    run1 = db.start_run("https://api.uktradeinfo.com/test-run-1")
    db.replace_hmrc_raw_rows_for_period(run1, PERIOD, [_row()])
    with pytest.raises(ValueError):
        db.replace_hmrc_raw_rows_for_period(run1, PERIOD, [])
    assert _period_count() == 1
