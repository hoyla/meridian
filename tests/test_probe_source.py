"""Tests for scrape.probe_source — the always-probe orchestration.

Mocks only the network ingest (scrape_eurostat / scrape_hmrc / run_scrape) so
the candidate-period computation, expectation classification, and the
routine_check_log write are all exercised against the real test DB.
"""
from __future__ import annotations

from datetime import date

import psycopg2
import pytest

import routine_log
import scrape


def _seed_release(test_db_url: str, source: str, period: date) -> None:
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO releases (source, source_url, period, section_number, "
            "currency, release_kind) VALUES (%s, %s, %s, NULL, NULL, NULL)",
            (source, f"http://example/{source}/{period:%Y%m}", period),
        )
        conn.commit()


def _last_row(test_db_url: str, source: str) -> dict:
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT result, expectation, candidate_period, notes, error "
            "FROM routine_check_log WHERE source = %s "
            "ORDER BY checked_at DESC, id DESC LIMIT 1",
            (source,),
        )
        r = cur.fetchone()
    return dict(zip(("result", "expectation", "candidate_period", "notes", "error"), r))


def test_eurostat_absent_before_due_date_logs_no_change_none_expected(
    clean_db, test_db_url, monkeypatch,
):
    _seed_release(test_db_url, "eurostat", date(2026, 3, 1))  # candidate → 2026-04
    monkeypatch.setattr(
        scrape, "scrape_eurostat",
        lambda *a, **k: scrape.IngestOutcome(status="absent"),
    )
    # 2026-04 publishes 15 Jun; on 2 Jun it's not expected yet.
    scrape.probe_source("eurostat", today=date(2026, 6, 2))

    row = _last_row(test_db_url, "eurostat")
    assert row["result"] == "no_change"
    assert row["expectation"] == "none_expected"
    assert row["candidate_period"] == date(2026, 4, 1)


def test_eurostat_present_logs_new_data_with_expectation(
    clean_db, test_db_url, monkeypatch,
):
    _seed_release(test_db_url, "eurostat", date(2026, 3, 1))  # candidate → 2026-04
    monkeypatch.setattr(
        scrape, "scrape_eurostat",
        lambda *a, **k: scrape.IngestOutcome(status="success", rows=47000),
    )
    # On 15 Jun the file is present and on schedule → due.
    scrape.probe_source("eurostat", today=date(2026, 6, 15))

    row = _last_row(test_db_url, "eurostat")
    assert row["result"] == "new_data"
    assert row["expectation"] == "due"
    assert row["candidate_period"] == date(2026, 4, 1)
    assert "47000" in (row["notes"] or "")


def test_eurostat_missing_past_due_logs_overdue(clean_db, test_db_url, monkeypatch):
    # Pipeline fell behind: latest is still 2026-02, so candidate is 2026-03,
    # whose 19 May date is long past by June with nothing fetched → overdue.
    _seed_release(test_db_url, "eurostat", date(2026, 2, 1))
    monkeypatch.setattr(
        scrape, "scrape_eurostat",
        lambda *a, **k: scrape.IngestOutcome(status="absent"),
    )
    scrape.probe_source("eurostat", today=date(2026, 6, 2))

    row = _last_row(test_db_url, "eurostat")
    assert row["result"] == "no_change"
    assert row["expectation"] == "overdue"
    assert row["candidate_period"] == date(2026, 3, 1)


def test_skips_probe_when_reference_month_not_closed(clean_db, test_db_url, monkeypatch):
    # Latest is 2026-05 → candidate 2026-06. On 10 Jun the June reference month
    # hasn't ended, so data for it cannot exist — the floor must skip the fetch
    # entirely rather than burn a network call on a guaranteed no-op.
    _seed_release(test_db_url, "eurostat", date(2026, 5, 1))
    called = False

    def _should_not_run(*a, **k):
        nonlocal called
        called = True
        return scrape.IngestOutcome(status="absent")

    monkeypatch.setattr(scrape, "scrape_eurostat", _should_not_run)
    scrape.probe_source("eurostat", today=date(2026, 6, 10))

    row = _last_row(test_db_url, "eurostat")
    assert called is False  # no fetch attempted before the month closed
    assert row["result"] == "no_change"
    assert row["expectation"] == "none_expected"
    assert row["candidate_period"] == date(2026, 6, 1)
    assert "not closed" in (row["notes"] or "")


def test_probes_once_month_closed_even_before_publish_date(
    clean_db, test_db_url, monkeypatch,
):
    # The floor must NOT re-introduce gating: once June has closed (1 Jul), we
    # probe even though Eurostat's scheduled date is mid-August — this is the
    # window where early arrivals get caught.
    _seed_release(test_db_url, "eurostat", date(2026, 5, 1))  # candidate 2026-06
    called = False

    def _probe(*a, **k):
        nonlocal called
        called = True
        return scrape.IngestOutcome(status="absent")

    monkeypatch.setattr(scrape, "scrape_eurostat", _probe)
    scrape.probe_source("eurostat", today=date(2026, 7, 1))

    assert called is True  # month closed → we do probe
    row = _last_row(test_db_url, "eurostat")
    assert row["result"] == "no_change"
    assert row["expectation"] == "none_expected"  # still before the 14 Aug date


def test_hmrc_missing_fx_logs_error(clean_db, test_db_url, monkeypatch):
    _seed_release(test_db_url, "hmrc", date(2026, 3, 1))
    monkeypatch.setattr(
        scrape, "scrape_hmrc",
        lambda *a, **k: scrape.IngestOutcome(status="skipped", error="no GBP/EUR FX rate"),
    )
    scrape.probe_source("hmrc", today=date(2026, 6, 2))

    row = _last_row(test_db_url, "hmrc")
    assert row["result"] == "error"
    assert "FX" in (row["error"] or "")


def test_eurostat_noop_logs_no_change_not_error(clean_db, test_db_url, monkeypatch):
    # The idempotency guard returns status='noop' when a period is already
    # ingested. That's a no-op, not a failure — it must log no_change, NOT an
    # error (which would be a message-less false alarm in the routine log).
    # Contrast test_hmrc_missing_fx_logs_error: 'skipped' still maps to error.
    _seed_release(test_db_url, "eurostat", date(2026, 3, 1))  # candidate → 2026-04
    monkeypatch.setattr(
        scrape, "scrape_eurostat",
        lambda *a, **k: scrape.IngestOutcome(status="noop"),
    )
    scrape.probe_source("eurostat", today=date(2026, 6, 15))

    row = _last_row(test_db_url, "eurostat")
    assert row["result"] == "no_change"
    assert row["error"] is None
    assert "already ingested" in (row["notes"] or "")


def test_gacc_logs_no_change_with_null_expectation(clean_db, test_db_url, monkeypatch):
    monkeypatch.setattr(scrape, "run_scrape", lambda *a, **k: None)  # no new releases
    scrape.probe_source("gacc", today=date(2026, 6, 2))

    row = _last_row(test_db_url, "gacc")
    assert row["result"] == "no_change"
    assert row["expectation"] is None
    assert row["candidate_period"] is None


def test_no_prior_releases_logs_no_change(clean_db, test_db_url, monkeypatch):
    # An empty DB can't anchor a candidate — log a no_change with a note rather
    # than guessing a starting month or crashing.
    called = False

    def _should_not_run(*a, **k):
        nonlocal called
        called = True
        return scrape.IngestOutcome(status="absent")

    monkeypatch.setattr(scrape, "scrape_eurostat", _should_not_run)
    scrape.probe_source("eurostat", today=date(2026, 6, 2))

    row = _last_row(test_db_url, "eurostat")
    assert row["result"] == "no_change"
    assert row["expectation"] is None
    assert called is False  # never attempted a fetch without an anchor
