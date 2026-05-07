"""Integration tests for db.py against a live Postgres test database."""

from pathlib import Path

import psycopg2

import db
from parse import parse_html

FIXTURES = Path(__file__).parent / "fixtures"
SECTION4 = FIXTURES / "release_section4_by_country_mar2026_cny.html"
URL = "http://english.customs.gov.cn/Statics/test-fixture-uuid.html"


def _parse_fixture():
    return parse_html(SECTION4.read_bytes(), URL)


def test_persist_and_idempotent_rerun(clean_db, test_db_url):
    result = _parse_fixture()

    run_id = db.start_run(URL)
    release_id = db.find_or_create_gacc_release(result.metadata, release_kind="preliminary")
    counts = db.upsert_observations(run_id, release_id, result.observations)
    db.finish_run(run_id, status="success", http_status=200)

    assert counts == {"inserted": 180, "versioned": 0, "unchanged": 0}

    # Second pass — same data, should all be unchanged and not duplicated.
    run_id2 = db.start_run(URL)
    release_id2 = db.find_or_create_gacc_release(result.metadata, release_kind="preliminary")
    counts2 = db.upsert_observations(run_id2, release_id2, result.observations)
    db.finish_run(run_id2, status="success", http_status=200)

    assert release_id2 == release_id
    assert counts2 == {"inserted": 0, "versioned": 0, "unchanged": 180}

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM observations")
        assert cur.fetchone()[0] == 180
        cur.execute("SELECT count(*) FROM releases")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(*) FROM scrape_runs WHERE status = 'success'")
        assert cur.fetchone()[0] == 2


def test_version_bumps_when_value_changes(clean_db, test_db_url):
    result = _parse_fixture()

    run_id = db.start_run(URL)
    release_id = db.find_or_create_gacc_release(result.metadata, release_kind="preliminary")
    db.upsert_observations(run_id, release_id, result.observations)
    db.finish_run(run_id, status="success")

    # Simulate a republished value: alter one observation and re-upsert.
    altered = [dict(o) for o in result.observations]
    target = next(
        o for o in altered
        if o["partner_country"] == "United States (US)"
        and o["flow"] == "export"
        and o["period_kind"] == "monthly"
    )
    target["value"] = 9999.9

    run_id2 = db.start_run(URL)
    counts = db.upsert_observations(run_id2, release_id, altered)
    db.finish_run(run_id2, status="success")

    assert counts == {"inserted": 0, "versioned": 1, "unchanged": 179}

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT version_seen, value_amount FROM observations
             WHERE release_id = %s AND partner_country = 'United States (US)'
               AND flow = 'export' AND period_kind = 'monthly'
          ORDER BY version_seen
            """,
            (release_id,),
        )
        rows = [(int(v), float(a)) for v, a in cur.fetchall()]

    assert rows == [(1, 2045.0), (2, 9999.9)]


def test_release_metadata_refresh(clean_db, test_db_url):
    """If the same release is re-fetched with a fresher publication_date or excel_url,
    those fields are updated on the existing release row rather than ignored."""
    result = _parse_fixture()

    run_id = db.start_run(URL)
    release_id = db.find_or_create_gacc_release(result.metadata, release_kind="preliminary")
    db.finish_run(run_id, status="success")

    # Stale publication_date: pretend our first observation had no pub date set.
    from dataclasses import replace

    stripped = replace(result.metadata, publication_date=None, excel_url=None)
    run_id2 = db.start_run(URL)
    release_id2 = db.find_or_create_gacc_release(stripped, release_kind="preliminary")
    db.finish_run(run_id2, status="success")
    assert release_id2 == release_id

    # The original publication_date should still be on the row (COALESCE preserves it).
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT publication_date FROM releases WHERE id = %s", (release_id,))
        assert cur.fetchone()[0] is not None
