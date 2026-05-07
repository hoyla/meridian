"""Postgres access layer.

Thin functional wrapper around psycopg2. No ORM — keeps the SQL legible and
matches the fuel-finder pattern.
"""

import logging
import os
from contextlib import contextmanager
from typing import Any

import psycopg2

from api_client import FetchResult

log = logging.getLogger(__name__)


def _conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


@contextmanager
def transaction():
    conn = _conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def start_run(source_url: str) -> int:
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'running') RETURNING id",
            (source_url,),
        )
        return cur.fetchone()[0]


def finish_run(
    run_id: int,
    status: str,
    http_status: int | None = None,
    error_message: str | None = None,
) -> None:
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE scrape_runs
               SET status = %s, http_status = %s, error_message = %s, ended_at = now()
             WHERE id = %s
            """,
            (status, http_status, error_message, run_id),
        )


def save_snapshot(run_id: int, response: FetchResult) -> int:
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO source_snapshots
                   (scrape_run_id, url, content_type, content_sha256, content_bytes)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                run_id,
                response.url,
                response.content_type,
                response.sha256,
                psycopg2.Binary(response.content),
            ),
        )
        return cur.fetchone()[0]


def upsert_observations(run_id: int, observations: list[dict[str, Any]]) -> None:
    """Insert observations, bumping version_seen if the same (release, dims) reappears."""
    raise NotImplementedError("Implement after we settle the dimension key for GACC tables")
