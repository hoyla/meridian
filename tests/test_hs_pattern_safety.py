"""Regression tests for finding A3 (2026-06-25 adversarial-correctness review).

hs_groups.hs_patterns are journalist-editable and spliced into SQL LIKE clauses
against product_nc with no validation. Two holes:

  1. A regex-valid but over-broad pattern ('00%', '000%') matches the Eurostat
     all-goods 000TOTAL aggregate rows (~40k in eurostat_raw_rows), sweeping the
     all-goods total into a CN8 group total and 2x-inflating it. Closed at query
     level by the product_nc <> '000TOTAL' guard now baked into
     anomalies._hs_pattern_or_clause (so all six hs-group callers inherit it).

  2. Structurally-malformed entries ('8%', a stray '%', a missing '%', an empty
     array) reach the query unchecked. Closed by the schema CHECK
     hs_groups_patterns_valid (each pattern = 2–8 digits then '%', non-empty, no
     NULL element).

The CHECK test requires gacc_test to carry the constraint — rebuild it from the
branch's schema.sql before running (dropdb/createdb/psql -f schema.sql).
"""
from datetime import date

import psycopg2
import psycopg2.errors
import pytest

import anomalies

_PERIOD = date(2026, 3, 1)


# --- Hole 1: the 000TOTAL sweep (query-level guard) --------------------------

def test_hs_group_totals_exclude_000total(clean_db, test_db_url):
    # Seed one 000TOTAL aggregate row and one CN8 detail row that BOTH match the
    # over-broad pattern '000%'. The aggregate must be excluded; only the detail
    # row may contribute to the group total.
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) "
            "VALUES ('http://x/eurostat', 'success') RETURNING id"
        )
        run = cur.fetchone()[0]
        for product_nc, value_eur in [("000TOTAL", 1000.0), ("00099999", 7.0)]:
            cur.execute(
                "INSERT INTO eurostat_raw_rows "
                "  (scrape_run_id, period, reporter, partner, product_nc, flow, value_eur) "
                "VALUES (%s, %s, 'DE', 'CN', %s, 1, %s)",
                (run, _PERIOD, product_nc, value_eur),
            )
    # `with psycopg2.connect(...) as conn` commits on clean block exit.

    rows = anomalies._hs_group_per_period_totals(
        ["000%"], flow=1, partners=("CN",), source="eurostat",
    )

    assert len(rows) == 1
    period_out, total_eur, _kg, n_raw, _eur_with_kg = rows[0]
    assert period_out == _PERIOD
    assert total_eur == 7.0, "the 000TOTAL aggregate row must be excluded"
    assert n_raw == 1


# --- Hole 2: the schema CHECK on hs_patterns ---------------------------------

@pytest.mark.parametrize(
    "patterns",
    [
        ["8%"],            # 1 digit — too short
        ["%"],             # stray wildcard, no digits
        ["85"],            # missing trailing '%'
        ["abc%"],          # non-digit
        ["850760"],        # exact code without '%'
        ["8507%", "bad"],  # one good, one malformed → whole row rejected
        [],                # empty array
    ],
)
def test_hs_groups_check_rejects_malformed(clean_db, test_db_url, patterns):
    # No autocommit + an unconditional rollback in finally: if the INSERT raises
    # CheckViolation the txn aborts (rollback resets it); if it does NOT raise
    # (constraint missing → test fails) the rollback still discards the row, so a
    # mis-provisioned test DB can't be polluted by the would-be-rejected insert.
    conn = psycopg2.connect(test_db_url)
    try:
        label = "-".join(patterns) or "empty"
        with conn.cursor() as cur, pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute(
                "INSERT INTO hs_groups (name, description, hs_patterns, created_by) "
                "VALUES (%s, 'test', %s, 'test')",
                (f"bad-{label}", patterns),
            )
    finally:
        conn.rollback()
        conn.close()


def test_hs_groups_check_allows_valid_patterns(clean_db, test_db_url):
    # 2-, 4-, 6- and 8-digit prefixes all conform; insert must not raise.
    conn = psycopg2.connect(test_db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO hs_groups (name, description, hs_patterns, created_by) "
                "VALUES ('good-grp', 'test', %s, 'test')",
                (["85%", "8507%", "850760%", "85076000%"],),
            )
    finally:
        conn.rollback()  # don't persist (hs_groups isn't truncated between tests)
        conn.close()
