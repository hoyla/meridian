"""Tests for scrape.probe_source — the always-probe orchestration.

Mocks only the network ingest (scrape_eurostat / scrape_hmrc / run_scrape) so
the candidate-period computation, expectation classification, and the
routine_check_log write are all exercised against the real test DB.
"""
from __future__ import annotations

from datetime import date

import psycopg2
import pytest

import gacc_cn
import routine_log
import scrape


@pytest.fixture(autouse=True)
def _quiet_cn_discovery(monkeypatch):
    """The gacc probe now runs CN-side discovery (gacc_cn.probe_cn_express)
    before the English walk. Default it to a quiet no_new here so the
    pre-existing probe tests stay hermetic — no live fetch of the tjs index
    from the test suite. Tests exercising the CN wiring override this."""
    monkeypatch.setattr(
        gacc_cn, "probe_cn_express",
        lambda *a, **k: gacc_cn.CnDiscoveryOutcome(status="no_new"))


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


def test_gacc_empty_db_logs_null_expectation(clean_db, test_db_url, monkeypatch):
    # No prior gacc releases → no anchor for a candidate period. The index walk
    # still runs (it needs no anchor), but there is nothing to classify, so the
    # expectation stays NULL.
    monkeypatch.setattr(scrape, "run_scrape", lambda *a, **k: None)  # no new releases
    scrape.probe_source("gacc", today=date(2026, 6, 2))

    row = _last_row(test_db_url, "gacc")
    assert row["result"] == "no_change"
    assert row["expectation"] is None
    assert row["candidate_period"] is None


def test_gacc_missing_past_due_logs_overdue(clean_db, test_db_url, monkeypatch):
    # Latest preliminary release is Apr 2026, so the candidate is May 2026,
    # scheduled ~8 Jun. By 22 Jun the walk still brings nothing back → the May
    # release has slipped past its due-by cutoff and must read overdue (the
    # alert that --source-status surfaces on its OVERDUE line).
    _seed_release(test_db_url, "gacc", date(2026, 4, 1))
    monkeypatch.setattr(scrape, "run_scrape", lambda *a, **k: None)  # no new releases
    scrape.probe_source("gacc", today=date(2026, 6, 22))

    row = _last_row(test_db_url, "gacc")
    assert row["result"] == "no_change"
    assert row["expectation"] == "overdue"
    assert row["candidate_period"] == date(2026, 5, 1)


def test_gacc_before_due_date_logs_none_expected(clean_db, test_db_url, monkeypatch):
    # Latest is Apr 2026 → candidate May 2026 (scheduled ~8 Jun). On 2 Jun the
    # release isn't due yet, so a quiet walk is normal → none_expected.
    _seed_release(test_db_url, "gacc", date(2026, 4, 1))
    monkeypatch.setattr(scrape, "run_scrape", lambda *a, **k: None)
    scrape.probe_source("gacc", today=date(2026, 6, 2))

    row = _last_row(test_db_url, "gacc")
    assert row["result"] == "no_change"
    assert row["expectation"] == "none_expected"
    assert row["candidate_period"] == date(2026, 5, 1)


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


def test_gacc_held_back_release_logs_error(clean_db, test_db_url, monkeypatch):
    # A parse-floor rejection during the walk writes scrape_runs 'failed' and
    # returns (run_scrape never raises, no releases row). The probe must read
    # that as 'error' — not the 'no_change' a flat release-count would imply,
    # which would let a held-back release read as a quiet day (F5, 2026-07-07).
    _seed_release(test_db_url, "gacc", date(2026, 5, 1))  # candidate → 2026-06

    def _walk_holds_one_back(*a, **k):
        with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO scrape_runs (source_url, status, error_message) "
                "VALUES (%s, 'failed', %s)",
                ("http://english.customs.gov.cn/Statics/held-back.html",
                 "GACC section 4 parse failed the plausibility floor "
                 "(CNY, 2026-06-01): only 3 top-level partners parsed"),
            )
            conn.commit()

    monkeypatch.setattr(scrape, "run_scrape", _walk_holds_one_back)
    scrape.probe_source("gacc", today=date(2026, 7, 8))

    row = _last_row(test_db_url, "gacc")
    assert row["result"] == "error"
    assert "held back" in (row["notes"] or "")
    assert "plausibility floor" in (row["error"] or "")
    assert row["candidate_period"] == date(2026, 6, 1)


def test_gacc_cn_published_awaiting_bytes_noted_on_quiet_walk(
    clean_db, test_db_url, monkeypatch,
):
    # Drop morning: the Chinese site has published the candidate month but no
    # byte route exists yet and the English walk finds nothing. The probe must
    # keep result=no_change (nothing ingested) while the notes say the release
    # is out upstream — the difference between "quiet day" and "English is
    # lagging", and the guard against a false overdue being read as a slip.
    _seed_release(test_db_url, "gacc", date(2026, 6, 1))  # candidate → 2026-07
    pending = gacc_cn.CnExpressArticle(
        url="http://tjs.customs.gov.cn/tjs/2026-08/07/article_1.html",
        title="（4）2026年7月进出口商品主要国别（地区）总值表（人民币值）",
        section=4, currency="CNY", period=date(2026, 7, 1),
        is_jan_feb_combined=False, published=date(2026, 8, 7))
    monkeypatch.setattr(
        gacc_cn, "probe_cn_express",
        lambda *a, **k: gacc_cn.CnDiscoveryOutcome(
            status="published_awaiting_bytes", pending=[pending]))
    monkeypatch.setattr(scrape, "run_scrape", lambda *a, **k: None)
    scrape.probe_source("gacc", today=date(2026, 8, 7))

    row = _last_row(test_db_url, "gacc")
    assert row["result"] == "no_change"
    assert row["expectation"] == "due"          # scheduled 7 Aug (公告 240号)
    assert "published upstream" in (row["notes"] or "")
    assert "2026-07" in (row["notes"] or "")


def test_gacc_cn_index_unavailable_never_breaks_the_walk(
    clean_db, test_db_url, monkeypatch,
):
    # CN discovery is additive: an unreachable tjs index must not turn a
    # clean English walk into an error — just a note.
    _seed_release(test_db_url, "gacc", date(2026, 5, 1))
    monkeypatch.setattr(
        gacc_cn, "probe_cn_express",
        lambda *a, **k: gacc_cn.CnDiscoveryOutcome(
            status="unavailable", error="connect timeout"))

    def _walk_ingests_one(*a, **k):
        _seed_release(test_db_url, "gacc", date(2026, 6, 1))

    monkeypatch.setattr(scrape, "run_scrape", _walk_ingests_one)
    scrape.probe_source("gacc", today=date(2026, 7, 8))

    row = _last_row(test_db_url, "gacc")
    assert row["result"] == "new_data"
    assert "CN index probe unavailable" in (row["notes"] or "")
    assert row["error"] is None


def test_gacc_clean_walk_with_new_release_still_logs_new_data(
    clean_db, test_db_url, monkeypatch,
):
    # Control: a walk that ingests a release with no failures still reads
    # new_data — the failure-detection must not disturb the happy path.
    _seed_release(test_db_url, "gacc", date(2026, 5, 1))

    def _walk_ingests_one(*a, **k):
        _seed_release(test_db_url, "gacc", date(2026, 6, 1))

    monkeypatch.setattr(scrape, "run_scrape", _walk_ingests_one)
    scrape.probe_source("gacc", today=date(2026, 7, 8))

    row = _last_row(test_db_url, "gacc")
    assert row["result"] == "new_data"
    assert "fetched 1 new releases" in (row["notes"] or "")
