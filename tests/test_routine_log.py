"""Tests for routine_log — the daily-Routine source-check telemetry."""
from __future__ import annotations

from datetime import date

import psycopg2
import pytest

import routine_log


def _seed_releases(test_db_url: str) -> None:
    """Pre-populate one release per source so latest_period_in_db reads non-None
    in the rollup. Periods chosen to mirror the live DB shape: Eurostat / HMRC
    on 2026-02 (the latest published periods), GACC on 2026-04 (preliminaries
    publish ~10 days after period close so a more recent month is plausible)."""
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO releases (source, source_url, period, section_number, currency, release_kind)
            VALUES
                ('eurostat', 'http://example/eu202602', %s, NULL, NULL, NULL),
                ('hmrc',     'http://example/hmrc202602', %s, NULL, NULL, NULL),
                ('gacc',     'http://example/gacc1', %s, 4, 'CNY', 'preliminary')
            """,
            (date(2026, 2, 1), date(2026, 2, 1), date(2026, 4, 1)),
        )
        conn.commit()


def test_log_check_rejects_unknown_result(clean_db):
    with pytest.raises(ValueError, match="result must be one of"):
        routine_log.log_check("eurostat", "definitely_not_a_result")


def test_log_check_inserts_row_and_returns_id(clean_db, test_db_url):
    rid = routine_log.log_check(
        "eurostat",
        "new_data",
        candidate_period=date(2026, 3, 1),
        notes="fetched 47k rows",
        duration_ms=12_345,
    )
    assert isinstance(rid, int) and rid > 0

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT source, result, candidate_period, notes, duration_ms "
            "FROM routine_check_log WHERE id = %s",
            (rid,),
        )
        row = cur.fetchone()
    assert row == ("eurostat", "new_data", date(2026, 3, 1), "fetched 47k rows", 12_345)


def test_compute_status_returns_all_expected_sources_when_empty(clean_db):
    """No log rows yet → one entry per expected source, all telemetry None."""
    statuses = routine_log.compute_status()
    assert [s.source for s in statuses] == list(routine_log.EXPECTED_SOURCES)
    for s in statuses:
        assert s.last_check_at is None
        assert s.last_result is None
        assert s.last_new_data_at is None
        assert s.latest_period_in_db is None


def test_compute_status_picks_most_recent_per_source(clean_db, test_db_url):
    _seed_releases(test_db_url)

    # Eurostat: earlier ineligible check, later successful fetch.
    routine_log.log_check(
        "eurostat", "not_yet_eligible",
        candidate_period=date(2026, 3, 1),
        notes="candidate not yet 5 weeks past period close",
    )
    routine_log.log_check(
        "eurostat", "new_data",
        candidate_period=date(2026, 2, 1),
        notes="fetched 47k rows",
    )

    # HMRC: last attempt errored.
    routine_log.log_check("hmrc", "error", error="psycopg2.OperationalError: timeout")

    # GACC: only ever no_change so far — never brought back new data through the routine.
    routine_log.log_check("gacc", "no_change", notes="walked indexes, no new releases")

    by_src = {s.source: s for s in routine_log.compute_status()}

    eu = by_src["eurostat"]
    assert eu.last_result == "new_data"
    assert eu.last_period_brought_back == date(2026, 2, 1)
    assert eu.latest_period_in_db == date(2026, 2, 1)
    assert eu.error is None

    hmrc = by_src["hmrc"]
    assert hmrc.last_result == "error"
    assert hmrc.last_new_data_at is None  # never a new_data row
    assert hmrc.last_period_brought_back is None
    assert "OperationalError" in (hmrc.error or "")

    gacc = by_src["gacc"]
    assert gacc.last_result == "no_change"
    assert gacc.last_new_data_at is None
    assert gacc.latest_period_in_db == date(2026, 4, 1)  # from releases, not the log


def test_compute_status_falls_back_to_releases_when_source_unlogged(clean_db, test_db_url):
    """The 'GACC has data in DB but the Routine hasn't been polling it' case —
    `latest_period_in_db` should still come back even with no log rows."""
    _seed_releases(test_db_url)
    by_src = {s.source: s for s in routine_log.compute_status()}
    assert by_src["gacc"].latest_period_in_db == date(2026, 4, 1)
    assert by_src["gacc"].last_check_at is None  # never logged


def test_render_status_table_aligns_and_surfaces_errors(clean_db, test_db_url):
    _seed_releases(test_db_url)
    routine_log.log_check("eurostat", "new_data", candidate_period=date(2026, 2, 1))
    routine_log.log_check("hmrc", "error", error="HTTP 503 from uktradeinfo.com")

    out = routine_log.render_status_table(routine_log.compute_status())
    # Header row + all three expected sources rendered, regardless of log presence.
    assert "source" in out and "last_check" in out
    for src in routine_log.EXPECTED_SOURCES:
        assert src in out
    # Errors get a trailing line in the extras block.
    assert "HTTP 503" in out
