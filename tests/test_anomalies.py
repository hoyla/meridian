"""Tests for the mirror-trade comparator in anomalies.py.

Sets up minimal GACC + Eurostat observations + an FX rate, runs the comparator,
verifies the resulting finding's structure (provenance, caveat citations,
arithmetic, country alias FK, fx_rates FK).
"""

import json
from datetime import date

import psycopg2
import pytest

import anomalies
import db


@pytest.fixture(autouse=True)
def _direct_db_url(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)


@pytest.fixture
def empty_op_tables(test_db_url):
    """Truncate operational tables; preserve seeded country_aliases + caveats."""
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE findings, observations, source_snapshots, eurostat_raw_rows, "
            "scrape_runs, releases, fx_rates RESTART IDENTITY CASCADE"
        )
    yield


def _seed_one_pair(conn, period: date) -> tuple[int, list[int]]:
    """Insert: one GACC release+observation (export to Germany) + one FX rate +
    one Eurostat release+observation. Returns (gacc_obs_id, eurostat_obs_ids)."""
    cur = conn.cursor()

    # GACC release + observation: China exported 1000 (CNY 100M) = 100B CNY to Germany
    cur.execute(
        """
        INSERT INTO releases (source, section_number, currency, period, release_kind,
                              source_url, unit, title, description)
        VALUES ('gacc', 4, 'CNY', %s, 'preliminary', %s, 'CNY 100 Million', 't', 'd')
        RETURNING id
        """,
        (period, "http://example/gacc.html"),
    )
    gacc_release_id = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES ('http://example/gacc.html', 'success') RETURNING id",
    )
    gacc_run_id = cur.fetchone()[0]

    cur.execute(
        """
        INSERT INTO observations (release_id, scrape_run_id, period_kind, flow,
                                  partner_country, value_amount, value_currency, source_row)
        VALUES (%s, %s, 'monthly', 'export', 'Germany', 1000, 'CNY', '{}')
        RETURNING id
        """,
        (gacc_release_id, gacc_run_id),
    )
    gacc_obs_id = cur.fetchone()[0]

    # Eurostat release + 2 observations summing to €11_000_000_000 (DE imports from CN)
    cur.execute(
        """
        INSERT INTO releases (source, period, source_url) VALUES ('eurostat', %s, %s) RETURNING id
        """,
        (period, "http://example/eurostat.7z"),
    )
    eurostat_release_id = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES ('http://example/eurostat.7z', 'success') RETURNING id",
    )
    eu_run_id = cur.fetchone()[0]

    eu_obs_ids = []
    for hs, val in [("87038010", 7_000_000_000), ("87038090", 4_000_000_000)]:
        cur.execute(
            """
            INSERT INTO observations (release_id, scrape_run_id, period_kind, flow,
                                      reporter_country, partner_country, hs_code,
                                      value_amount, value_currency, source_row)
            VALUES (%s, %s, 'monthly', 'import', 'DE', 'CN', %s, %s, 'EUR', '{}')
            RETURNING id
            """,
            (eurostat_release_id, eu_run_id, hs, val),
        )
        eu_obs_ids.append(cur.fetchone()[0])

    # FX rate: 1 CNY = 0.125 EUR (so 100B CNY = €12.5B)
    cur.execute(
        """
        INSERT INTO fx_rates (currency_from, currency_to, rate_date, rate, rate_source, rate_source_url, notes)
        VALUES ('CNY', 'EUR', %s, 0.125, 'ECB monthly average', 'http://example/ecb', 't')
        """,
        (period,),
    )
    conn.commit()
    return gacc_obs_id, eu_obs_ids


def test_unit_scale_parses_known_forms():
    assert anomalies.parse_unit_scale("CNY 100 Million") == (1e8, "CNY")
    assert anomalies.parse_unit_scale("USD 1 Million") == (1e6, "USD")
    assert anomalies.parse_unit_scale("EUR Billion") == (1e9, "EUR")
    assert anomalies.parse_unit_scale("EUR") == (1.0, "EUR")
    assert anomalies.parse_unit_scale(None) == (1.0, None)
    assert anomalies.parse_unit_scale("not a unit") == (1.0, None)


def test_mirror_gap_emits_finding_with_provenance(empty_op_tables, test_db_url):
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        gacc_obs_id, eu_obs_ids = _seed_one_pair(conn, period)

    counts = anomalies.detect_mirror_trade_gaps(period=period)
    assert counts == {"emitted": 1, "skipped_no_eurostat": 0, "skipped_no_fx": 0,
                      "skipped_aggregate": 0, "skipped_unmapped": 0, "skipped_no_value": 0}

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT scrape_run_id, kind, subkind, observation_ids, score, title, body, detail "
            "FROM findings ORDER BY id"
        )
        run_id, kind, subkind, obs_ids, score, title, body, detail = cur.fetchone()

    assert kind == "anomaly"
    assert subkind == "mirror_gap"
    # GACC obs id + both Eurostat obs ids in observation_ids[]
    assert obs_ids[0] == gacc_obs_id
    assert set(obs_ids[1:]) == set(eu_obs_ids)
    # GACC: 1000 × 1e8 CNY × 0.125 EUR/CNY = €12.5B
    # Eurostat: 7B + 4B = €11B
    # Gap: €11B - €12.5B = -€1.5B (GACC > Eurostat)
    # Larger = €12.5B; gap_pct = -1.5/12.5 = -0.12
    assert abs(detail["gap_eur"] + 1_500_000_000) < 1e-3
    assert abs(detail["gap_pct"] + 0.12) < 1e-6
    assert abs(float(score) - 0.12) < 1e-6
    assert "Germany" in body or "DE" in title  # both sides mentioned somewhere
    # Caveats cited
    assert "cif_fob" in detail["caveat_codes"]
    assert "currency_timing" in detail["caveat_codes"]
    # FX provenance
    assert detail["fx"]["rate"] == 0.125
    assert detail["fx"]["from_currency"] == "CNY"
    # Country alias FK present (the alias for "Germany" is in seed data)
    assert detail["country_alias_id"] is not None


def test_mirror_gap_skips_aggregates(empty_op_tables, test_db_url):
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO releases (source, section_number, currency, period, release_kind,
                                  source_url, unit) VALUES ('gacc', 4, 'CNY', %s, 'preliminary', 'x', 'CNY 100 Million')
            RETURNING id
            """,
            (period,),
        )
        rel = cur.fetchone()[0]
        cur.execute("INSERT INTO scrape_runs (source_url, status) VALUES ('x', 'success') RETURNING id")
        run = cur.fetchone()[0]
        for label in ["European Union", "ASEAN"]:
            cur.execute(
                """
                INSERT INTO observations (release_id, scrape_run_id, period_kind, flow,
                                          partner_country, value_amount, value_currency, source_row)
                VALUES (%s, %s, 'monthly', 'export', %s, 1000, 'CNY', '{}')
                """,
                (rel, run, label),
            )
        cur.execute(
            "INSERT INTO fx_rates (currency_from, currency_to, rate_date, rate, rate_source) "
            "VALUES ('CNY', 'EUR', %s, 0.125, 'ECB monthly average')",
            (period,),
        )
        conn.commit()

    counts = anomalies.detect_mirror_trade_gaps(period=period)
    assert counts["skipped_aggregate"] == 2
    assert counts["emitted"] == 0


def test_mirror_gap_skips_when_no_fx(empty_op_tables, test_db_url):
    """No FX rate means we cannot convert; comparator must skip rather than guess."""
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        _seed_one_pair(conn, period)
        # Nuke the FX rate that _seed_one_pair inserted
        with conn.cursor() as cur:
            cur.execute("TRUNCATE fx_rates")
        conn.commit()

    counts = anomalies.detect_mirror_trade_gaps(period=period)
    assert counts["skipped_no_fx"] == 1
    assert counts["emitted"] == 0


def test_mirror_gap_skips_when_no_eurostat(empty_op_tables, test_db_url):
    """If only GACC has data for that period, comparator skips — no half-comparisons."""
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO releases (source, section_number, currency, period, release_kind, source_url, unit)
            VALUES ('gacc', 4, 'CNY', %s, 'preliminary', 'x', 'CNY 100 Million') RETURNING id
            """,
            (period,),
        )
        rel = cur.fetchone()[0]
        cur.execute("INSERT INTO scrape_runs (source_url, status) VALUES ('x', 'success') RETURNING id")
        run = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO observations (release_id, scrape_run_id, period_kind, flow,
                                      partner_country, value_amount, value_currency, source_row)
            VALUES (%s, %s, 'monthly', 'export', 'Germany', 1000, 'CNY', '{}')
            """,
            (rel, run),
        )
        cur.execute(
            "INSERT INTO fx_rates (currency_from, currency_to, rate_date, rate, rate_source) "
            "VALUES ('CNY', 'EUR', %s, 0.125, 'ECB monthly average')",
            (period,),
        )
        conn.commit()

    counts = anomalies.detect_mirror_trade_gaps(period=period)
    assert counts["skipped_no_eurostat"] == 1
    assert counts["emitted"] == 0
