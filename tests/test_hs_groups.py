"""Tests for the HS-group YoY analyser."""

from datetime import date

import psycopg2
import pytest

import anomalies


@pytest.fixture(autouse=True)
def _direct_db_url(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)


@pytest.fixture
def empty_op(test_db_url):
    """Truncate operational + findings tables; preserve hs_groups + country seeds."""
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE findings, observations, source_snapshots, eurostat_raw_rows, "
            "scrape_runs, releases RESTART IDENTITY CASCADE"
        )
    yield


def _seed_eurostat_imports(conn, hs_code: str, value_per_period: list[tuple[date, float]]) -> None:
    """For each (period, value), insert a Eurostat release + a single observation
    representing DE imports from CN at that hs_code with that value."""
    cur = conn.cursor()
    for period, value in value_per_period:
        cur.execute(
            "INSERT INTO releases (source, period, source_url) VALUES ('eurostat', %s, %s) "
            "ON CONFLICT (period) WHERE source='eurostat' DO UPDATE SET last_seen_at=now() RETURNING id",
            (period, f"http://example/{period}"),
        )
        rel = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'success') RETURNING id",
            (f"http://example/{period}",),
        )
        run = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO observations (release_id, scrape_run_id, period_kind, flow,
                                      reporter_country, partner_country, hs_code,
                                      value_amount, value_currency, source_row)
            VALUES (%s, %s, 'monthly', 'import', 'DE', 'CN', %s, %s, 'EUR', '{}')
            """,
            (rel, run, hs_code, value),
        )
    conn.commit()


def _make_24_months(start: date, monthly_values: list[float]) -> list[tuple[date, float]]:
    out = []
    p = start
    for v in monthly_values:
        out.append((p, v))
        # advance one month
        if p.month == 12:
            p = date(p.year + 1, 1, 1)
        else:
            p = date(p.year, p.month + 1, 1)
    return out


def test_yoy_emits_when_growth_above_threshold(empty_op, test_db_url):
    """24 months: prior 12 averaged €100 each (€1.2B); current 12 averaged €150 each
    (€1.8B). YoY = +50%. With threshold 0.10, should emit."""
    with psycopg2.connect(test_db_url) as conn:
        # Use Solar PV cells & modules (id 2 in seed) — patterns ['854142%', '854143%']
        prior_12  = [100.0] * 12
        current_12 = [150.0] * 12
        _seed_eurostat_imports(
            conn, "85414210",
            _make_24_months(date(2024, 1, 1), prior_12 + current_12),
        )

    counts = anomalies.detect_hs_group_yoy(
        group_names=["Solar PV cells & modules"], yoy_threshold_pct=0.10,
    )
    assert counts["emitted"] >= 1, f"counts={counts}"

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT title, score, detail FROM findings "
            "WHERE subkind='hs_group_yoy' ORDER BY score DESC LIMIT 1"
        )
        title, score, detail = cur.fetchone()

    assert "Solar PV" in title
    assert "+50.0%" in title or "+50.00%" in title
    assert detail["totals"]["current_12mo_eur"] == 1800.0
    assert detail["totals"]["prior_12mo_eur"] == 1200.0
    assert abs(detail["totals"]["yoy_pct"] - 0.5) < 1e-9
    # Provenance: the method definition is queryable from the finding alone
    assert detail["method_query"]["partner_country"] == "CN"
    assert detail["method_query"]["hs_patterns"] == ["854142%", "854143%"]
    # Top contributors recorded
    assert any(c["hs_code"] == "85414210" for c in detail["top_cn8_codes_in_current_12mo"])


def test_yoy_silent_when_below_threshold(empty_op, test_db_url):
    """No real growth → no finding when threshold is set."""
    with psycopg2.connect(test_db_url) as conn:
        flat = [100.0] * 24
        _seed_eurostat_imports(
            conn, "85414210",
            _make_24_months(date(2024, 1, 1), flat),
        )

    counts = anomalies.detect_hs_group_yoy(
        group_names=["Solar PV cells & modules"], yoy_threshold_pct=0.10,
    )
    assert counts["emitted"] == 0
    assert counts["skipped_below_threshold"] >= 1


def test_yoy_skips_insufficient_history(empty_op, test_db_url):
    """Only 12 months — can't compute the prior 12-month window, must skip."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_eurostat_imports(
            conn, "85414210",
            _make_24_months(date(2025, 1, 1), [100.0] * 12),
        )

    counts = anomalies.detect_hs_group_yoy(
        group_names=["Solar PV cells & modules"], yoy_threshold_pct=0.0,
    )
    assert counts["emitted"] == 0
    assert counts["skipped_insufficient_history"] >= 1


def test_yoy_records_per_month_series_and_top_reporters(empty_op, test_db_url):
    with psycopg2.connect(test_db_url) as conn:
        _seed_eurostat_imports(
            conn, "85414210",
            _make_24_months(date(2024, 1, 1), [100.0] * 12 + [200.0] * 12),
        )

    anomalies.detect_hs_group_yoy(
        group_names=["Solar PV cells & modules"], yoy_threshold_pct=0.0,
    )
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail FROM findings WHERE subkind='hs_group_yoy' ORDER BY score DESC LIMIT 1"
        )
        detail = cur.fetchone()[0]

    series = detail["monthly_series"]
    assert len(series) == 24  # full 24-month context surfaced
    assert series[0]["value_eur"] == 100.0
    assert series[-1]["value_eur"] == 200.0

    # Top reporter is DE in our seed
    top = detail["top_reporters_in_current_12mo"]
    assert top[0]["reporter"] == "DE"
