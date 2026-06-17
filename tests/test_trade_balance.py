"""Tests for the Europe–China trade-balance analyser (anomalies.detect_eu_china_trade_balance).

Seeds Eurostat 000TOTAL aggregates (EU-27) and HMRC CN8 detail (UK) across
a full 12-month window, runs the analyser, and checks:
  - the deficit arithmetic (imports − exports), single-month and rolling;
  - the €/day rendering;
  - EU-27 reads 000TOTAL, NOT a sum over CN8 detail (the double-count guard
    the mirror_gap family tripped on), and excludes GB;
  - UK reads HMRC by SUMMING CN8 detail (no 000TOTAL row on that side);
  - eu27_plus_uk sums the two and carries the cross_source_sum caveat;
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


def _hmrc_release(cur, period: date) -> tuple[int, int]:
    cur.execute(
        "INSERT INTO releases (source, period, source_url) VALUES "
        "('hmrc', %s, %s) RETURNING id",
        (period, f"http://example/hmrc-{period}"),
    )
    rel = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'success') RETURNING id",
        (f"http://example/hmrc-{period}",),
    )
    return rel, cur.fetchone()[0]


def _seed_hmrc_full_year(conn):
    """HMRC has NO 000TOTAL row — the all-goods total is a SUM over CN8
    detail. Per month: UK import detail 0.6bn + 0.4bn = 1.0bn, export
    detail 0.5bn → single-month deficit 0.5bn, 12-month deficit 6.0bn."""
    with conn, conn.cursor() as cur:
        for period in _MONTHS:
            rel, run = _hmrc_release(cur, period)
            _obs(cur, rel, run, "import", "GB", "CN", "85076000", 600_000_000)
            _obs(cur, rel, run, "import", "GB", "CN", "87038010", 400_000_000)
            _obs(cur, rel, run, "export", "GB", "CN", "88024000", 500_000_000)


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


def test_trade_balance_uk_scope_sums_hmrc_detail(empty_op_tables, test_db_url):
    """The UK scope reads HMRC, which has no 000TOTAL row, so the all-goods
    total is a SUM over CN8 detail (0.6 + 0.4 = 1.0bn imports, 0.5bn
    exports → 0.5bn single-month deficit, 6.0bn over 12 months)."""
    conn = psycopg2.connect(test_db_url)
    _seed_hmrc_full_year(conn)
    anomalies.detect_eu_china_trade_balance()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        row = _latest(cur, "trade_balance_uk")
    assert row is not None
    sm = row["detail"]["totals"]["single_month"]
    assert sm["import_eur"] == pytest.approx(1_000_000_000)
    assert sm["export_eur"] == pytest.approx(500_000_000)
    assert sm["deficit_eur"] == pytest.approx(500_000_000)
    roll = row["detail"]["totals"]["rolling_12mo"]
    assert roll["deficit_eur"] == pytest.approx(6_000_000_000)
    # No cross-source caveat on a single-source UK finding.
    assert "cross_source_sum" not in row["detail"]["caveat_codes"]
    assert "hmrc_detail_sum_suppression" in row["detail"]["caveat_codes"]
    conn.close()


def test_trade_balance_combined_sums_eu_and_uk(empty_op_tables, test_db_url):
    """eu27_plus_uk = Eurostat (18bn 12mo deficit) + HMRC (6bn) = 24bn, and
    carries the cross_source_sum caveat."""
    conn = psycopg2.connect(test_db_url)
    _seed_full_year(conn)       # Eurostat: 18bn 12mo deficit
    _seed_hmrc_full_year(conn)  # HMRC: 6bn 12mo deficit
    anomalies.detect_eu_china_trade_balance()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        row = _latest(cur, "trade_balance_combined")
    assert row is not None
    roll = row["detail"]["totals"]["rolling_12mo"]
    assert roll["deficit_eur"] == pytest.approx(24_000_000_000)
    sm = row["detail"]["totals"]["single_month"]
    # EU single-month 1.5bn + UK 0.5bn = 2.0bn.
    assert sm["deficit_eur"] == pytest.approx(2_000_000_000)
    assert "cross_source_sum" in row["detail"]["caveat_codes"]
    conn.close()
