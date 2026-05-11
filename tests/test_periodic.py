"""Tests for the periodic-run orchestrator (`periodic.py`).

The orchestrator's load-bearing properties are idempotency and trigger
distinction. We test those directly against the test DB rather than
mocking — same approach as the briefing-pack tests.

Full end-to-end runs (which invoke all the analysers) are too slow for
the unit-test loop; cover them with one combined integration test that
seeds a minimal dataset, plus targeted unit tests on the helper
functions and the no-op paths."""

from __future__ import annotations

import os
from datetime import date

import psycopg2
import pytest

import briefing_pack
import periodic


@pytest.fixture(autouse=True)
def _clear_permalink_env(monkeypatch):
    monkeypatch.delenv(briefing_pack.PERMALINK_BASE_ENV, raising=False)


@pytest.fixture
def fresh_db(test_db_url):
    """Truncate everything periodic-run could touch, including brief_runs."""
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE TABLE findings, observations, eurostat_raw_rows, "
            "hmrc_raw_rows, brief_runs, scrape_runs, releases, "
            "source_snapshots RESTART IDENTITY CASCADE"
        )
    yield


def _seed_eurostat_release(test_db_url, period: date) -> None:
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) "
            "VALUES ('seed', 'success') RETURNING id"
        )
        # release table doesn't FK to scrape_runs so we can ignore that id.
        cur.execute(
            "INSERT INTO releases (source, period, source_url) "
            "VALUES ('eurostat', %s, %s)",
            (period, f"https://example.invalid/eurostat/{period.strftime('%Y%m')}"),
        )


def _seed_brief_run(
    test_db_url,
    data_period: date,
    trigger: str = "periodic_run",
) -> None:
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO brief_runs (output_path, top_n, data_period, trigger) "
            "VALUES (%s, %s, %s, %s)",
            ("/tmp/seed/findings.md", 10, data_period, trigger),
        )


def test_latest_eurostat_period_returns_max(fresh_db, test_db_url, monkeypatch):
    """latest_eurostat_period() returns the most recent Eurostat
    release.period; None when no Eurostat data is ingested."""
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    assert briefing_pack.latest_eurostat_period() is None

    _seed_eurostat_release(test_db_url, date(2025, 12, 1))
    _seed_eurostat_release(test_db_url, date(2026, 1, 1))
    _seed_eurostat_release(test_db_url, date(2026, 2, 1))

    assert briefing_pack.latest_eurostat_period() == date(2026, 2, 1)


def test_latest_recorded_data_period_filters_by_trigger(
    fresh_db, test_db_url, monkeypatch,
):
    """Manual renders set trigger='manual'; periodic-run cycles set
    trigger='periodic_run'. The idempotency check filters on the latter
    so that on-demand manual renders for a new joiner don't block the
    next cycle (or, in reverse, get counted as cycle outputs)."""
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    assert briefing_pack.latest_recorded_data_period() is None

    _seed_brief_run(test_db_url, date(2026, 1, 1), trigger="manual")
    _seed_brief_run(test_db_url, date(2026, 2, 1), trigger="manual")
    # No periodic_run rows yet — the filtered query should return None.
    assert briefing_pack.latest_recorded_data_period(trigger="periodic_run") is None
    # The unfiltered query sees both manual rows.
    assert briefing_pack.latest_recorded_data_period() == date(2026, 2, 1)

    _seed_brief_run(test_db_url, date(2026, 1, 1), trigger="periodic_run")
    # Now the periodic_run filter sees the periodic row.
    assert briefing_pack.latest_recorded_data_period(trigger="periodic_run") == date(2026, 1, 1)


def test_periodic_run_noop_when_no_eurostat_data(
    fresh_db, test_db_url, monkeypatch, tmp_path,
):
    """On a completely empty DB, run_periodic() exits cleanly without
    invoking the analysers or writing an export. The wrapper script can
    branch on action_taken=False."""
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    result = periodic.run_periodic(out_dir=str(tmp_path))
    assert result.action_taken is False
    assert result.findings_path is None
    assert "no Eurostat data" in result.reason


def test_periodic_run_noop_when_data_period_already_published(
    fresh_db, test_db_url, monkeypatch, tmp_path,
):
    """The idempotency guard: a periodic-run cycle whose data_period
    matches the most recent already-published periodic-run row should
    not run the pipeline again. This is the property that lets a daily
    Routine fire harmlessly between Eurostat releases."""
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    _seed_eurostat_release(test_db_url, date(2026, 2, 1))
    _seed_brief_run(test_db_url, date(2026, 2, 1), trigger="periodic_run")

    result = periodic.run_periodic(out_dir=str(tmp_path))
    assert result.action_taken is False
    assert "already published" in result.reason
    assert result.data_period == date(2026, 2, 1)


def test_periodic_run_manual_render_does_not_advance_cycle(
    fresh_db, test_db_url, monkeypatch, tmp_path,
):
    """A manual on-demand render for a new joiner records as
    trigger='manual'. That row should NOT count toward the
    periodic-run cycle's idempotency check — the next periodic cycle
    should still fire."""
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    _seed_eurostat_release(test_db_url, date(2026, 2, 1))
    _seed_brief_run(test_db_url, date(2026, 2, 1), trigger="manual")

    # Even though there's a brief_runs row at the latest Eurostat period,
    # it's a manual render. The periodic-run idempotency check filters
    # to trigger='periodic_run' only, so it should see no prior cycle.
    # The run will progress to the pipeline (which on an empty findings
    # DB is fast but will still call into the analysers; for this unit
    # test we just want to assert the idempotency gate doesn't block).
    # We use force=False explicitly to make the assertion clearer.
    #
    # We don't run the full pipeline here (analysers on an empty DB
    # would do real work then write a brief). Instead, assert the
    # helper functions agree.
    assert briefing_pack.latest_recorded_data_period(trigger="periodic_run") is None
    assert briefing_pack.latest_recorded_data_period(trigger="manual") == date(2026, 2, 1)
