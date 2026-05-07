"""Postgres access layer.

Thin functional wrapper around psycopg2. No ORM — keeps the SQL legible and
matches the fuel-finder pattern.
"""

import json
import logging
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import psycopg2

from api_client import FetchResult

if TYPE_CHECKING:
    from parse import ReleaseMetadata

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


def find_or_create_gacc_release(meta: "ReleaseMetadata", release_kind: str) -> int:
    """Resolve the GACC natural key (section_number, currency, period, release_kind) to
    a release id, creating the row if needed and refreshing display fields that may
    have changed since we last saw the page (e.g. revised excel_url)."""
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO releases (
                source, section_number, currency, period, release_kind,
                description, title, source_url, publication_date, unit, excel_url
            ) VALUES ('gacc', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (section_number, currency, period, release_kind) WHERE source = 'gacc'
            DO UPDATE SET
                last_seen_at     = now(),
                source_url       = EXCLUDED.source_url,
                publication_date = COALESCE(EXCLUDED.publication_date, releases.publication_date),
                title            = COALESCE(EXCLUDED.title,            releases.title),
                description      = COALESCE(EXCLUDED.description,      releases.description),
                unit             = COALESCE(EXCLUDED.unit,             releases.unit),
                excel_url        = COALESCE(EXCLUDED.excel_url,        releases.excel_url)
            RETURNING id
            """,
            (
                meta.section_number, meta.currency, meta.period, release_kind,
                meta.description, meta.title, meta.source_url,
                meta.publication_date, meta.unit, meta.excel_url,
            ),
        )
        return cur.fetchone()[0]


def upsert_observations(
    run_id: int,
    release_id: int,
    observations: list[dict[str, Any]],
) -> dict[str, int]:
    """Insert each observation. If an existing row with the same dimensional key
    has the same value, skip; if the value differs, insert a new row with
    version_seen bumped. Returns counts {'inserted', 'versioned', 'unchanged'}."""
    counts = {"inserted": 0, "versioned": 0, "unchanged": 0}
    with transaction() as conn, conn.cursor() as cur:
        for obs in observations:
            cur.execute(
                """
                SELECT value_amount, version_seen
                  FROM observations
                 WHERE release_id        = %s
                   AND period_kind       = %s
                   AND flow              IS NOT DISTINCT FROM %s
                   AND reporter_country  IS NOT DISTINCT FROM %s
                   AND partner_country   IS NOT DISTINCT FROM %s
                   AND hs_code           IS NOT DISTINCT FROM %s
                   AND commodity_label   IS NOT DISTINCT FROM %s
              ORDER BY version_seen DESC
                 LIMIT 1
                """,
                (
                    release_id,
                    obs.get("period_kind"),
                    obs.get("flow"),
                    obs.get("reporter_country"),
                    obs.get("partner_country"),
                    obs.get("hs_code"),
                    obs.get("commodity_label"),
                ),
            )
            existing = cur.fetchone()
            new_value = obs.get("value")

            if existing is None:
                version, action = 1, "inserted"
            else:
                existing_value, existing_version = existing
                same = (
                    (existing_value is None and new_value is None)
                    or (existing_value is not None and new_value is not None
                        and float(existing_value) == float(new_value))
                )
                if same:
                    counts["unchanged"] += 1
                    continue
                version, action = existing_version + 1, "versioned"

            cur.execute(
                """
                INSERT INTO observations (
                    release_id, scrape_run_id, period_kind,
                    flow, reporter_country, partner_country, partner_label_raw, partner_indent, partner_is_subset,
                    hs_code, commodity_label,
                    value_amount, value_currency, quantity, quantity_unit,
                    source_row, version_seen
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s
                )
                """,
                (
                    release_id, run_id, obs.get("period_kind"),
                    obs.get("flow"), obs.get("reporter_country"), obs.get("partner_country"),
                    obs.get("partner_label_raw"), obs.get("partner_indent"), obs.get("partner_is_subset"),
                    obs.get("hs_code"), obs.get("commodity_label"),
                    obs.get("value"), obs.get("currency"),
                    obs.get("quantity"), obs.get("quantity_unit"),
                    json.dumps(obs.get("source_row") or {}), version,
                ),
            )
            counts[action] += 1
    return counts
