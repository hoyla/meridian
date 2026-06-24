"""Tests for the China all-goods share analyser
(anomalies.detect_china_all_goods_share) — the dependency donut + trend metric.

Seeds the numerator (Eurostat 000TOTAL observations for CN+HK+MO, EU-27
reporters) and the denominator (eurostat_world_aggregates 000TOTAL extra-EU)
across a 12-month window, runs the analyser, and checks:
  - the share arithmetic (CN+HK+MO numerator / extra-EU denominator);
  - the CN-only comparator;
  - GB is excluded from both numerator and denominator (EU-27 scope);
  - the rolling-12mo block + share_series are carried on the finding;
  - anchors before 2019-01 are skipped (pre-v2 numerator contamination guard);
  - idempotency on re-run.
"""

from datetime import date

import psycopg2
import pytest

import anomalies


@pytest.fixture(autouse=True)
def _direct_db_url(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)


@pytest.fixture
def empty_op_tables(test_db_url):
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE findings, observations, eurostat_world_aggregates, "
            "source_snapshots, eurostat_raw_rows, scrape_runs, releases "
            "RESTART IDENTITY CASCADE"
        )
    yield


def _run(cur, url="http://example/run") -> int:
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'success') RETURNING id",
        (url,),
    )
    return cur.fetchone()[0]


def _obs(cur, rel, run, flow, reporter, partner, val):
    cur.execute(
        "INSERT INTO observations (release_id, scrape_run_id, period_kind, flow, "
        "  reporter_country, partner_country, hs_code, value_amount, "
        "  value_currency, source_row) "
        "VALUES (%s, %s, 'monthly', %s, %s, %s, '000TOTAL', %s, 'EUR', '{}')",
        (rel, run, flow, reporter, partner, val),
    )


def _world(cur, run, period, reporter, flow_int, val):
    cur.execute(
        "INSERT INTO eurostat_world_aggregates (scrape_run_id, period, reporter, "
        "  product_nc, flow, value_eur, quantity_kg, quantity_suppl_unit, "
        "  n_partners_summed, n_raw_rows) "
        "VALUES (%s, %s, %s, '000TOTAL', %s, %s, 0, 0, 1, 1)",
        (run, period, reporter, flow_int, val),
    )


def _months(end: date, n: int) -> list[date]:
    out = [end]
    while len(out) < n:
        m = out[0]
        py, pm = (m.year - 1, 12) if m.month == 1 else (m.year, m.month - 1)
        out.insert(0, date(py, pm, 1))
    return out


def _seed_window(conn, anchor: date):
    """12 months ending `anchor`. Per month, import side:
      numerator CN = 1.8bn, HK = 0.2bn  -> CN+HK+MO = 2.0bn, CN-only = 1.8bn
      denominator (extra-EU) = 10bn
    -> 12mo share = 24bn/120bn = 20%; CN-only = 21.6bn/120bn = 18%.
    GB rows on both sides must be excluded. Exports seeded smaller."""
    with conn, conn.cursor() as cur:
        run = _run(cur)
        for period in _months(anchor, 12):
            cur.execute(
                "INSERT INTO releases (source, period, source_url) VALUES "
                "('eurostat', %s, %s) RETURNING id",
                (period, f"http://example/eu-{period}.7z"),
            )
            rel = cur.fetchone()[0]
            # Numerator (observations): CN + HK on DE; a GB row to be excluded.
            _obs(cur, rel, run, "import", "DE", "CN", 1_800_000_000)
            _obs(cur, rel, run, "import", "DE", "HK", 200_000_000)
            _obs(cur, rel, run, "import", "GB", "CN", 5_000_000_000)
            _obs(cur, rel, run, "export", "DE", "CN", 900_000_000)
            _obs(cur, rel, run, "export", "DE", "HK", 100_000_000)
            # Denominator (world aggregates, extra-EU): DE plus a GB row excluded.
            _world(cur, run, period, "DE", 1, 10_000_000_000)
            _world(cur, run, period, "GB", 1, 50_000_000_000)
            _world(cur, run, period, "DE", 2, 5_000_000_000)


def _latest(cur, subkind):
    cur.execute(
        "SELECT detail, title FROM findings WHERE subkind = %s AND superseded_at IS NULL "
        "ORDER BY (detail->'windows'->>'anchor_period') DESC LIMIT 1",
        (subkind,),
    )
    return cur.fetchone()


def test_share_arithmetic_and_cn_only(empty_op_tables, test_db_url):
    _seed_window(psycopg2.connect(test_db_url), date(2020, 4, 1))
    counts = anomalies.detect_china_all_goods_share()
    assert counts["emitted"] >= 2  # one import + one export finding

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        detail, title = _latest(cur, "china_all_goods_share")
        roll = detail["rolling_12mo"]
        assert roll["share"] == pytest.approx(0.20, abs=1e-6)        # 24/120
        assert roll["share_cn_only"] == pytest.approx(0.18, abs=1e-6)  # 21.6/120
        assert roll["numerator_eur"] == pytest.approx(24e9)
        assert roll["denominator_eur"] == pytest.approx(120e9)        # GB (50bn/mo) excluded
        assert "20.0%" in title
        # The series carries one point per complete-window anchor (here: one).
        assert detail["share_series"][-1]["share"] == pytest.approx(0.20, abs=1e-6)


def test_pre_2019_anchor_is_skipped(empty_op_tables, test_db_url):
    """The numerator is pre-v2-contaminated before 2019, so anchors below the
    2019-01 floor must not emit — even with a complete window."""
    _seed_window(psycopg2.connect(test_db_url), date(2018, 12, 1))
    counts = anomalies.detect_china_all_goods_share()
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM findings WHERE subkind LIKE 'china_all_goods_share%'")
        assert cur.fetchone()[0] == 0


def test_idempotent_rerun(empty_op_tables, test_db_url):
    _seed_window(psycopg2.connect(test_db_url), date(2020, 4, 1))
    anomalies.detect_china_all_goods_share()
    counts2 = anomalies.detect_china_all_goods_share()
    # Second pass: every finding re-confirmed, none superseded or newly inserted.
    assert counts2["confirmed_existing"] >= 2
    assert counts2["superseded"] == 0
    assert counts2["inserted_new"] == 0
