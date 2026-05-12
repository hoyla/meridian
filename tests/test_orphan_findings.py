"""Regression guard: no active hs_group_yoy* / hs_group_trajectory*
finding should reference an hs_groups row whose name no longer matches.

Two ways a finding can go stale:

- The hs_groups row was deleted (Phase 6.5 dropped 'Pharmaceutical APIs
  (broad)' after splitting it into three narrower groups).
- The hs_groups row was renamed in place ('Plastic waste' → 'Plastic
  waste (post-National-Sword residual)'; 'Lithium chemicals (carbonate
  + hydroxide)' → 'Lithium hydroxide (battery-grade)' with a tightened
  pattern set).

Without a guard, stale findings linger as active and pollute top-mover
queries (the Lithium chemicals broad-group findings carried YoY% in
the +2500% range and ranked above any real story in 2025-2026 mover
output, until the 2026-05-12 cleanup).

Runs against the LIVE database; skipped if `GACC_LIVE_DATABASE_URL`
isn't set. The test DB has no findings under retired groups.

Cleanup query (kept here for reference) — if this test fails, run it:

    UPDATE findings f
       SET superseded_at = now()
     WHERE f.superseded_at IS NULL
       AND (f.subkind LIKE 'hs_group_yoy%' OR f.subkind LIKE 'hs_group_trajectory%')
       AND array_length(f.hs_group_ids, 1) > 0
       AND NOT EXISTS (
           SELECT 1 FROM hs_groups g
            WHERE g.id = ANY(f.hs_group_ids)
              AND g.name = f.detail->'group'->>'name'
       );
"""

import os

import psycopg2
import pytest


LIVE_DB_ENV = "GACC_LIVE_DATABASE_URL"


@pytest.fixture(scope="module")
def live_db_url() -> str:
    url = os.environ.get(LIVE_DB_ENV)
    if not url:
        pytest.skip(f"{LIVE_DB_ENV} not set; skipping live orphan-findings check")
    return url


@pytest.fixture(scope="module")
def live_conn(live_db_url):
    conn = psycopg2.connect(live_db_url)
    yield conn
    conn.close()


def test_no_active_hs_group_findings_reference_renamed_or_deleted_groups(live_conn):
    """An active hs_group_yoy* / hs_group_trajectory* finding must have a
    matching `hs_groups` row whose `name` equals the finding's
    `detail.group.name`. A mismatch means the underlying group was
    deleted or renamed without the corresponding findings being
    superseded — the finding is editorially stale."""
    with live_conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.id, f.subkind,
                   f.detail->'group'->>'name' AS finding_group_name,
                   f.hs_group_ids[1] AS hs_group_id
              FROM findings f
             WHERE f.superseded_at IS NULL
               AND (f.subkind LIKE 'hs_group_yoy%'
                    OR f.subkind LIKE 'hs_group_trajectory%')
               AND array_length(f.hs_group_ids, 1) > 0
               AND NOT EXISTS (
                   SELECT 1 FROM hs_groups g
                    WHERE g.id = ANY(f.hs_group_ids)
                      AND g.name = f.detail->'group'->>'name'
               )
             LIMIT 20
            """
        )
        orphans = cur.fetchall()
    assert orphans == [], (
        f"{len(orphans)} active finding(s) reference a renamed/deleted "
        f"hs_groups row (first 20 shown). Run the cleanup UPDATE in this "
        f"file's module docstring: {orphans}"
    )
