"""Shared test fixtures.

We use a live Postgres test database (no mocking) — same approach as fuel-finder.
Set GACC_TEST_DATABASE_URL in the environment to point at a throwaway DB.
"""

import os

import psycopg2
import pytest


@pytest.fixture(scope="session")
def test_db_url() -> str:
    url = os.environ.get("GACC_TEST_DATABASE_URL")
    if not url:
        pytest.skip("GACC_TEST_DATABASE_URL not set; skipping DB-backed tests")
    return url


def _truncate_all(conn) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE TABLE findings_emit_log, llm_rejection_log, "
            "periodic_run_log, findings, observations, source_snapshots, "
            "eurostat_raw_rows, eurostat_world_aggregates, hmrc_raw_rows, "
            "brief_runs, scrape_runs, routine_check_log, releases "
            "RESTART IDENTITY CASCADE"
        )


@pytest.fixture
def db_conn(test_db_url):
    """A fresh connection to the test DB, truncated before yielding."""
    conn = psycopg2.connect(test_db_url)
    _truncate_all(conn)
    yield conn
    conn.close()


@pytest.fixture
def clean_db(test_db_url, monkeypatch):
    """For tests that call db.py functions (which read DATABASE_URL at runtime)."""
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    conn = psycopg2.connect(test_db_url)
    _truncate_all(conn)
    conn.close()
    yield
