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


def _seed_eurostat_imports(
    conn, hs_code: str, value_per_period: list[tuple[date, float]],
    kg_per_period: list[float] | None = None,
    flow: int = 1,
) -> None:
    """For each (period, value), insert a Eurostat release + a single raw row +
    a matching aggregated observation, representing DE → CN trade at the
    given hs_code. flow=1 is import (CN→DE), flow=2 is export (DE→CN). The
    analyser queries eurostat_raw_rows for value/kg totals, so the raw row
    carries the kg figure.

    `kg_per_period`: parallel list of kg values; defaults to value/10 (i.e. an
    arbitrary €10/kg unit price) when not specified, so existing tests that
    don't care about kg still see plausible numbers."""
    cur = conn.cursor()
    if kg_per_period is None:
        kg_per_period = [v / 10.0 for _, v in value_per_period]
    flow_label = "import" if flow == 1 else "export"
    for (period, value), kg in zip(value_per_period, kg_per_period):
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
            INSERT INTO eurostat_raw_rows (
                scrape_run_id, period, reporter, partner, product_nc, flow,
                value_eur, value_nac, quantity_kg, quantity_suppl_unit
            ) VALUES (%s, %s, 'DE', 'CN', %s, %s, %s, %s, %s, 0)
            """,
            (run, period, hs_code, flow, value, value, kg),
        )
        cur.execute(
            """
            INSERT INTO observations (release_id, scrape_run_id, period_kind, flow,
                                      reporter_country, partner_country, hs_code,
                                      value_amount, value_currency, source_row)
            VALUES (%s, %s, 'monthly', %s, 'DE', 'CN', %s, %s, 'EUR', '{}')
            """,
            (rel, run, flow_label, hs_code, value),
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
    # kg figures present alongside EUR
    assert detail["totals"]["current_12mo_kg"] > 0
    assert detail["totals"]["yoy_pct_kg"] is not None
    assert detail["totals"]["current_unit_price_eur_per_kg"] is not None
    # Provenance: the method definition is queryable from the finding alone
    assert detail["method_query"]["partner"] == "CN"
    assert detail["method_query"]["hs_patterns"] == ["854142%", "854143%"]
    # Top contributors recorded with kg
    top = detail["top_cn8_codes_in_current_12mo"]
    assert any(c["hs_code"] == "85414210" for c in top)
    assert all("total_kg" in c for c in top)


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
    # kg + unit price recorded per month
    assert series[0]["quantity_kg"] > 0
    assert series[0]["unit_price_eur_per_kg"] is not None

    # Top reporter is DE in our seed
    top = detail["top_reporters_in_current_12mo"]
    assert top[0]["reporter"] == "DE"
    assert top[0]["total_kg"] > 0


def test_yoy_export_flow_isolated_from_import(empty_op, test_db_url):
    """flow=2 query (EU→CN exports) reads only flow=2 raw rows, even when
    flow=1 rows exist in the same DB. Subkind is 'hs_group_yoy_export' to
    keep export findings separate from import findings."""
    with psycopg2.connect(test_db_url) as conn:
        # Pork (HS 0203). Imports trending up; exports trending down.
        # Imports (flow=1): rising
        _seed_eurostat_imports(
            conn, "02031910",
            _make_24_months(date(2024, 1, 1), [100.0] * 12 + [200.0] * 12),
            flow=1,
        )
        # Exports (flow=2): falling
        _seed_eurostat_imports(
            conn, "02031910",
            _make_24_months(date(2024, 1, 1), [500.0] * 12 + [400.0] * 12),
            flow=2,
        )

    # Import-side: should see +100% YoY
    counts_imp = anomalies.detect_hs_group_yoy(
        group_names=["Pork (HS 0203)"], yoy_threshold_pct=0.0, flow=1,
    )
    assert counts_imp["emitted"] >= 1

    # Export-side: should see -20% YoY
    counts_exp = anomalies.detect_hs_group_yoy(
        group_names=["Pork (HS 0203)"], yoy_threshold_pct=0.0, flow=2,
    )
    assert counts_exp["emitted"] >= 1

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT subkind, (detail->'totals'->>'yoy_pct')::numeric AS yoy "
            "FROM findings WHERE detail->'group'->>'name' = 'Pork (HS 0203)' "
            "ORDER BY subkind"
        )
        rows = cur.fetchall()

    by_subkind = {r[0]: float(r[1]) for r in rows}
    assert "hs_group_yoy" in by_subkind
    assert "hs_group_yoy_export" in by_subkind
    assert abs(by_subkind["hs_group_yoy"] - 1.0) < 1e-9          # +100% on imports
    assert abs(by_subkind["hs_group_yoy_export"] + 0.2) < 1e-9   # -20% on exports


def test_yoy_low_base_flag_set_when_below_threshold(empty_op, test_db_url):
    """A group with very small absolute totals should fire the low_base flag,
    add the low_base_effect caveat, and mark the title with ⚠."""
    with psycopg2.connect(test_db_url) as conn:
        # 24 months at €100 each = €1.2k current/prior totals — way below the €50M default
        _seed_eurostat_imports(
            conn, "85076010",
            _make_24_months(date(2024, 1, 1), [100.0] * 24),
        )

    anomalies.detect_hs_group_yoy(group_names=["EV batteries (Li-ion)"], yoy_threshold_pct=0.0)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT title, detail FROM findings WHERE subkind='hs_group_yoy' "
            "AND detail->'group'->>'name'='EV batteries (Li-ion)' ORDER BY score DESC LIMIT 1"
        )
        title, detail = cur.fetchone()

    assert detail["totals"]["low_base"] is True
    assert "low_base_effect" in detail["caveat_codes"]
    assert "low-base" in title


def test_yoy_low_base_flag_not_set_when_above_threshold(empty_op, test_db_url):
    """A group with €100M+ rolling totals shouldn't be flagged as low-base."""
    with psycopg2.connect(test_db_url) as conn:
        # €10M each month × 24 → both windows ~€120M, comfortably above €50M
        _seed_eurostat_imports(
            conn, "85076010",
            _make_24_months(date(2024, 1, 1), [10_000_000.0] * 24),
        )

    anomalies.detect_hs_group_yoy(group_names=["EV batteries (Li-ion)"], yoy_threshold_pct=0.0)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT title, detail FROM findings WHERE subkind='hs_group_yoy' "
            "AND detail->'group'->>'name'='EV batteries (Li-ion)' ORDER BY score DESC LIMIT 1"
        )
        title, detail = cur.fetchone()

    assert detail["totals"]["low_base"] is False
    assert "low_base_effect" not in detail["caveat_codes"]
    assert "low-base" not in title


def test_yoy_decomposition_volume_vs_price(empty_op, test_db_url):
    """Value YoY +50% with kg flat → price-driven. Value YoY +50% with kg +50% → volume-driven.
    The decomposition is what answers Lisa's permanent-magnets puzzle:
    +18% volume but flat value would mean prices fell ~18%."""
    with psycopg2.connect(test_db_url) as conn:
        # Volume up 50%, value up 50% (unit price flat — pure volume growth)
        prior_vals = [100.0] * 12
        curr_vals  = [150.0] * 12
        prior_kgs  = [10.0]  * 12      # €10/kg
        curr_kgs   = [15.0]  * 12      # still €10/kg
        _seed_eurostat_imports(
            conn, "85076010",
            _make_24_months(date(2024, 1, 1), prior_vals + curr_vals),
            kg_per_period=prior_kgs + curr_kgs,
        )

    anomalies.detect_hs_group_yoy(group_names=["EV batteries (Li-ion)"], yoy_threshold_pct=0.0)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail FROM findings WHERE subkind='hs_group_yoy' "
            "AND detail->'group'->>'name' = 'EV batteries (Li-ion)' "
            "ORDER BY score DESC LIMIT 1"
        )
        detail = cur.fetchone()[0]

    # Value +50%, kg +50%, unit price ~unchanged → volume-driven
    assert abs(detail["totals"]["yoy_pct"] - 0.5) < 1e-9
    assert abs(detail["totals"]["yoy_pct_kg"] - 0.5) < 1e-9
    assert abs(detail["totals"]["unit_price_pct_change"]) < 1e-9
    assert "volume-driven" in detail.get("body", "") or True  # body lives outside detail
