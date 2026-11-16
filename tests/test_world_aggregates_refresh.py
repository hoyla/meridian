"""The share denominator (eurostat_world_aggregates) must keep pace with the
Eurostat data period. Two layers: scrape.ensure_world_aggregates_current
fills the missing months at the head of --periodic-run, and
periodic.share_denominator_warning reports a table that is still behind.

Only the network fetch (scrape_eurostat_world_totals) is mocked; the period
arithmetic and both MAX() reads run against the real test DB.
"""
from __future__ import annotations

from datetime import date

import psycopg2

import periodic
import scrape


def _seed_eurostat_release(url: str, period: date) -> None:
    with psycopg2.connect(url) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO releases (source, source_url, period, section_number, "
            "currency, release_kind) VALUES ('eurostat', %s, %s, NULL, NULL, NULL)",
            (f"http://example/eurostat/{period:%Y%m}", period),
        )
        conn.commit()


def _seed_world(url: str, period: date) -> None:
    with psycopg2.connect(url) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'success') "
            "RETURNING id", (f"http://example/world/{period:%Y%m}",))
        run_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO eurostat_world_aggregates (scrape_run_id, period, "
            "reporter, product_nc, flow, value_eur, quantity_kg, "
            "n_partners_summed, n_raw_rows) "
            "VALUES (%s, %s, 'DE', '000TOTAL', 1, 100, 10, 1, 1)",
            (run_id, period))
        conn.commit()


def _mock_fetch(monkeypatch, url: str, *, lands: bool = True) -> list[date]:
    """Replace the bulk-file fetch; record the periods asked for and (when
    `lands`) write a row so the table really advances."""
    calls: list[date] = []

    def fake(period, hs_prefixes=None, dry_run=False):
        calls.append(period)
        if lands:
            _seed_world(url, period)

    monkeypatch.setattr(scrape, "scrape_eurostat_world_totals", fake)
    monkeypatch.setattr(
        scrape, "_world_aggregate_hs_prefixes_from_hs_groups",
        lambda: ("000TOTAL",))
    return calls


def test_refresh_fills_every_missing_month_up_to_eurostat(
    clean_db, test_db_url, monkeypatch,
):
    _seed_eurostat_release(test_db_url, date(2026, 9, 1))
    _seed_world(test_db_url, date(2026, 7, 1))
    calls = _mock_fetch(monkeypatch, test_db_url)

    out = scrape.ensure_world_aggregates_current()

    assert calls == [date(2026, 8, 1), date(2026, 9, 1)]
    assert out == {"status": "refreshed", "filled": ["2026-08", "2026-09"],
                   "latest": "2026-09"}


def test_refresh_is_a_no_op_when_already_current(
    clean_db, test_db_url, monkeypatch,
):
    _seed_eurostat_release(test_db_url, date(2026, 9, 1))
    _seed_world(test_db_url, date(2026, 9, 1))
    calls = _mock_fetch(monkeypatch, test_db_url)

    out = scrape.ensure_world_aggregates_current()

    assert calls == []
    assert out["status"] == "current"


def test_refresh_leaves_an_empty_table_alone(clean_db, test_db_url, monkeypatch):
    """Seeding history is a manual act — a routine run must not start a
    multi-year backfill because the table happens to be empty."""
    _seed_eurostat_release(test_db_url, date(2026, 9, 1))
    calls = _mock_fetch(monkeypatch, test_db_url)

    out = scrape.ensure_world_aggregates_current()

    assert calls == []
    assert out["status"] == "skipped_no_anchor"


def test_refresh_caps_a_long_catch_up_at_the_most_recent_months(
    clean_db, test_db_url, monkeypatch,
):
    _seed_eurostat_release(test_db_url, date(2026, 9, 1))
    _seed_world(test_db_url, date(2024, 9, 1))  # 24 months behind
    calls = _mock_fetch(monkeypatch, test_db_url)

    scrape.ensure_world_aggregates_current()

    assert len(calls) == scrape.WORLD_AGGREGATES_MAX_CATCHUP_MONTHS
    assert calls[-1] == date(2026, 9, 1)


def test_refresh_reports_still_behind_when_the_fetch_did_not_land(
    clean_db, test_db_url, monkeypatch,
):
    """scrape_eurostat_world_totals swallows fetch failures, so the refresh
    must judge success by the table, not by having called it."""
    _seed_eurostat_release(test_db_url, date(2026, 9, 1))
    _seed_world(test_db_url, date(2026, 8, 1))
    _mock_fetch(monkeypatch, test_db_url, lands=False)

    out = scrape.ensure_world_aggregates_current()

    assert out["status"] == "still_behind"
    assert out["latest"] == "2026-08"


def test_warning_fires_when_denominator_lags_the_data_period(
    clean_db, test_db_url,
):
    _seed_world(test_db_url, date(2026, 8, 1))

    w = periodic.share_denominator_warning(date(2026, 9, 1))

    assert w is not None
    assert "reaches 2026-08" in w and "Eurostat data is at 2026-09" in w
    assert "--eurostat-world-aggregates-period 2026-09" in w


def test_warning_is_silent_when_denominator_is_current(clean_db, test_db_url):
    _seed_world(test_db_url, date(2026, 9, 1))
    assert periodic.share_denominator_warning(date(2026, 9, 1)) is None


def test_warning_fires_on_an_empty_table(clean_db, test_db_url):
    w = periodic.share_denominator_warning(date(2026, 9, 1))
    assert w is not None and "no data" in w


def test_summary_renders_warnings_on_the_action_path():
    result = periodic.PeriodicRunResult(
        action_taken=True, reason="new export written",
        data_period=date(2026, 9, 1), findings_path="/tmp/f.md",
        leads_path=None, bundle_dir="exports/x",
        warnings=["share denominator is STALE — test"],
    )
    assert "  WARNING: share denominator is STALE — test" in result.summary()
