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
import psycopg2.extras

from api_client import FetchResult

if TYPE_CHECKING:
    from datetime import date
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


_EUROSTAT_RAW_COLS = (
    "scrape_run_id", "period", "reporter", "partner", "trade_type", "product_nc",
    "product_sitc", "product_cpa21", "product_cpa22", "product_bec", "product_bec5",
    "product_section", "flow", "stat_procedure", "suppl_unit",
    "value_eur", "value_nac", "quantity_kg", "quantity_suppl_unit",
)


def bulk_insert_eurostat_raw_rows(scrape_run_id: int, raw_rows: list[dict]) -> list[int]:
    """Insert raw Eurostat rows verbatim. Returns the inserted ids in input order
    so the caller can pair them with their dicts for downstream aggregation."""
    if not raw_rows:
        return []
    cols_sql = ", ".join(_EUROSTAT_RAW_COLS)
    placeholders = "(" + ", ".join(["%s"] * len(_EUROSTAT_RAW_COLS)) + ")"
    rows_values = []
    for r in raw_rows:
        rows_values.append(tuple([scrape_run_id if c == "scrape_run_id" else r.get(c) for c in _EUROSTAT_RAW_COLS]))
    with transaction() as conn, conn.cursor() as cur:
        # execute_values would be ideal but psycopg2.extras adds dependency; manual mogrify is fine.
        args_str = b",".join(cur.mogrify(placeholders, v) for v in rows_values)
        cur.execute(
            f"INSERT INTO eurostat_raw_rows ({cols_sql}) VALUES " + args_str.decode("utf-8") + " RETURNING id"
        )
        return [r[0] for r in cur.fetchall()]


def find_or_create_eurostat_release(period: "date", source_url: str) -> int:
    """Resolve the Eurostat natural key (period) to a release id under source='eurostat'.
    GACC-only fields (section_number, currency, release_kind) stay NULL."""
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO releases (source, period, source_url)
            VALUES ('eurostat', %s, %s)
            ON CONFLICT (period) WHERE source = 'eurostat'
            DO UPDATE SET
                last_seen_at = now(),
                source_url   = EXCLUDED.source_url
            RETURNING id
            """,
            (period, source_url),
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


_OBS_INSERT_COLS = (
    "release_id", "scrape_run_id", "period_kind",
    "flow", "reporter_country", "partner_country",
    "partner_label_raw", "partner_indent", "partner_is_subset",
    "hs_code", "commodity_label",
    "value_amount", "value_currency", "quantity", "quantity_unit",
    "source_row", "eurostat_raw_row_ids", "version_seen",
)


def _obs_to_insert_tuple(release_id: int, run_id: int, obs: dict, version: int) -> tuple:
    return (
        release_id, run_id, obs.get("period_kind"),
        obs.get("flow"), obs.get("reporter_country"), obs.get("partner_country"),
        obs.get("partner_label_raw"), obs.get("partner_indent"), obs.get("partner_is_subset"),
        obs.get("hs_code"), obs.get("commodity_label"),
        obs.get("value"), obs.get("currency"),
        obs.get("quantity"), obs.get("quantity_unit"),
        json.dumps(obs.get("source_row") or {}),
        obs.get("eurostat_raw_row_ids"),
        version,
    )


def upsert_observations(
    run_id: int,
    release_id: int,
    observations: list[dict[str, Any]],
) -> dict[str, int]:
    """Insert each observation. If an existing row with the same dimensional key
    has the same value, skip; if the value differs, insert a new row with
    version_seen bumped. Returns counts {'inserted', 'versioned', 'unchanged'}.

    Fast path: when no observations exist for this release_id (first ingest),
    skip the per-row SELECT and bulk-INSERT via execute_values. This is the
    dominant case for backfill and is ~1000x faster than the per-row path.
    """
    counts = {"inserted": 0, "versioned": 0, "unchanged": 0}
    if not observations:
        return counts

    with transaction() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM observations WHERE release_id = %s LIMIT 1", (release_id,))
        is_fresh_release = cur.fetchone() is None

        if is_fresh_release:
            cols_sql = ", ".join(_OBS_INSERT_COLS)
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO observations ({cols_sql}) VALUES %s",
                [_obs_to_insert_tuple(release_id, run_id, obs, 1) for obs in observations],
                page_size=1000,
            )
            counts["inserted"] = len(observations)
            return counts

        # Slow path: re-scrape of an existing release. Per-row SELECT + version logic.
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

            placeholders = "(" + ", ".join(["%s"] * len(_OBS_INSERT_COLS)) + ")"
            cols_sql = ", ".join(_OBS_INSERT_COLS)
            cur.execute(
                f"INSERT INTO observations ({cols_sql}) VALUES {placeholders}",
                _obs_to_insert_tuple(release_id, run_id, obs, version),
            )
            counts[action] += 1
    return counts
