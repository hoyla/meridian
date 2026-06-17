"""Tests for the EU–China trade-balance analyser (anomalies.detect_eu_china_trade_balance).

Seeds Eurostat 000TOTAL aggregate observations across a full 12-month
window for two EU reporters and both flows, runs the analyser, and checks:
  - the deficit arithmetic (imports − exports), single-month and rolling;
  - the €/day rendering;
  - that the 000TOTAL aggregate is used, NOT a sum over CN8 detail rows
    (the double-count guard that the mirror_gap family tripped on);
  - that GB is excluded from EU-27;
  - idempotency on re-run.
"""

from datetime import date

import psycopg2
import psycopg2.extras
import pytest

import anomalies


@pytest.fixture(autouse=True)
def _direct_db_url(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)


@pytest.fixture
def empty_op_tables(test_db_url):
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE findings, observations, source_snapshots, eurostat_raw_rows, "
            "scrape_runs, releases RESTART IDENTITY CASCADE"
        )
    yield


def _eu_release(cur, period: date) -> tuple[int, int]:
    cur.execute(
        "INSERT INTO releases (source, period, source_url) VALUES "
        "('eurostat', %s, %s) RETURNING id",
        (period, f"http://example/eurostat-{period}.7z"),
    )
    rel = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'success') RETURNING id",
        (f"http://example/eurostat-{period}.7z",),
    )
    return rel, cur.fetchone()[0]


def _obs(cur, rel, run, flow, reporter, partner, hs, val):
    cur.execute(
        "INSERT INTO observations (release_id, scrape_run_id, period_kind, flow, "
        "                          reporter_country, partner_country, hs_code, "
        "                          value_amount, value_currency, source_row) "
        "VALUES (%s, %s, 'monthly', %s, %s, %s, %s, %s, 'EUR', '{}')",
        (rel, run, flow, reporter, partner, hs, val),
    )


# 12 consecutive months ending Apr 2026 = a complete current rolling window.
_MONTHS = [date(2025, 5, 1)]
while _MONTHS[-1] < date(2026, 4, 1):
    m = _MONTHS[-1]
    _MONTHS.append(date(m.year + (m.month // 12), (m.month % 12) + 1, 1))


def _seed_full_year(conn):
    """Per month: DE+FR import 000TOTAL = 3bn, export 000TOTAL = 1.5bn
    → single-month deficit 1.5bn, 12-month deficit 18bn. Plus a GB row and
    a CN8 detail row that must both be excluded from the totals."""
    with conn, conn.cursor() as cur:
        for period in _MONTHS:
            rel, run = _eu_release(cur, period)
            # EU-27 all-goods aggregate rows (the ones that count).
            _obs(cur, rel, run, "import", "DE", "CN", "000TOTAL", 2_000_000_000)
            _obs(cur, rel, run, "import", "FR", "CN", "000TOTAL", 1_000_000_000)
            _obs(cur, rel, run, "export", "DE", "CN", "000TOTAL", 1_000_000_000)
            _obs(cur, rel, run, "export", "FR", "CN", "000TOTAL", 500_000_000)
            # GB must be excluded from EU-27.
            _obs(cur, rel, run, "import", "GB", "CN", "000TOTAL", 99_000_000_000)
            # CN8 detail rows must NOT be summed alongside 000TOTAL.
            _obs(cur, rel, run, "import", "DE", "CN", "85076000", 50_000_000_000)


def _latest(cur, subkind):
    cur.execute(
        "SELECT id, detail FROM findings WHERE subkind = %s AND superseded_at IS NULL "
        "ORDER BY (detail->'windows'->>'anchor_period')::date DESC, id DESC LIMIT 1",
        (subkind,),
    )
    return cur.fetchone()


def test_trade_balance_arithmetic_and_per_day(empty_op_tables, test_db_url):
    conn = psycopg2.connect(test_db_url)
    _seed_full_year(conn)

    counts = anomalies.detect_eu_china_trade_balance()
    assert counts["inserted_new"] > 0

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        row = _latest(cur, "trade_balance")
    assert row is not None
    detail = row["detail"]

    sm = detail["totals"]["single_month"]
    # 3bn imports − 1.5bn exports = 1.5bn deficit for Apr 2026.
    assert sm["import_eur"] == pytest.approx(3_000_000_000)
    assert sm["export_eur"] == pytest.approx(1_500_000_000)
    assert sm["deficit_eur"] == pytest.approx(1_500_000_000)
    # April has 30 days → per-day = 1.5bn / 30 = 50m.
    assert sm["deficit_per_day_eur"] == pytest.approx(1_500_000_000 / 30)

    roll = detail["totals"]["rolling_12mo"]
    assert roll["import_eur"] == pytest.approx(36_000_000_000)
    assert roll["export_eur"] == pytest.approx(18_000_000_000)
    assert roll["deficit_eur"] == pytest.approx(18_000_000_000)
    # No prior window seeded → YoY not computable.
    assert roll["yoy_pct"] is None

    conn.close()


def test_trade_balance_uses_000total_not_detail_sum(empty_op_tables, test_db_url):
    """The deficit must come from the 000TOTAL aggregate alone. If the
    analyser summed CN8 detail too, the 50bn detail row would blow the
    import total up by an order of magnitude."""
    conn = psycopg2.connect(test_db_url)
    _seed_full_year(conn)
    anomalies.detect_eu_china_trade_balance()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        row = _latest(cur, "trade_balance")
    sm = row["detail"]["totals"]["single_month"]
    # 3bn, NOT 3bn + 50bn detail, and NOT inflated by the 99bn GB row.
    assert sm["import_eur"] == pytest.approx(3_000_000_000)
    conn.close()


def test_trade_balance_idempotent(empty_op_tables, test_db_url):
    conn = psycopg2.connect(test_db_url)
    _seed_full_year(conn)
    first = anomalies.detect_eu_china_trade_balance()
    second = anomalies.detect_eu_china_trade_balance()
    assert second["inserted_new"] == 0
    assert second["confirmed_existing"] == first["inserted_new"]
    conn.close()
