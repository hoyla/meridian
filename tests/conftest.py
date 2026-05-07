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


@pytest.fixture(scope="function")
def db_conn(test_db_url):
    conn = psycopg2.connect(test_db_url)
    yield conn
    conn.rollback()
    conn.close()
