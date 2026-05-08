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
    assert counts["emitted"] == 1
    assert all(v == 0 for k, v in counts.items() if k != "emitted")

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


def test_aggregate_eu_skips_when_no_members_seeded(empty_op_tables, test_db_url):
    """A bloc that has no member rows in country_aggregate_members can't be
    expanded, so the comparator skips with the dedicated counter rather than
    guessing a definition."""
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        # Drop EU member seed for this test only
        cur.execute("TRUNCATE country_aggregate_members RESTART IDENTITY")
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
    assert counts["skipped_aggregate_no_members"] == 2
    assert counts["emitted"] == 0


def test_aggregate_eu_emits_finding_with_member_provenance(empty_op_tables, test_db_url, monkeypatch):
    """With EU members seeded (default state) and Eurostat data for those members,
    an EU-aggregate GACC observation produces a finding with the member list and
    the aggregate_composition caveat in detail."""
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        # Re-seed EU 27 in case a prior test cleared it (tests aren't strictly ordered)
        cur.execute(
            """
            INSERT INTO country_aggregate_members (aggregate_alias_id, member_iso2, source)
            SELECT ca.id, m, 'test seed'
              FROM country_aliases ca,
                   unnest(ARRAY['DE','FR','IT','NL']) m
             WHERE ca.source='gacc' AND ca.raw_label='European Union'
            ON CONFLICT DO NOTHING
            """
        )
        # GACC: China exported 5000 (CNY 100M) = 500B CNY to EU
        cur.execute(
            """
            INSERT INTO releases (source, section_number, currency, period, release_kind, source_url, unit)
            VALUES ('gacc', 4, 'CNY', %s, 'preliminary', 'g', 'CNY 100 Million') RETURNING id
            """,
            (period,),
        )
        gacc_rel = cur.fetchone()[0]
        cur.execute("INSERT INTO scrape_runs (source_url, status) VALUES ('g', 'success') RETURNING id")
        run = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO observations (release_id, scrape_run_id, period_kind, flow,
                                      partner_country, value_amount, value_currency, source_row)
            VALUES (%s, %s, 'monthly', 'export', 'European Union', 5000, 'CNY', '{}')
            """,
            (gacc_rel, run),
        )

        # Eurostat: imports from CN by DE/FR/IT/NL — totals to €70B
        cur.execute(
            "INSERT INTO releases (source, period, source_url) VALUES ('eurostat', %s, 'e') RETURNING id",
            (period,),
        )
        eu_rel = cur.fetchone()[0]
        for reporter, val in [("DE", 20e9), ("FR", 15e9), ("IT", 13e9), ("NL", 22e9)]:
            cur.execute(
                """
                INSERT INTO observations (release_id, scrape_run_id, period_kind, flow,
                                          reporter_country, partner_country, hs_code,
                                          value_amount, value_currency, source_row)
                VALUES (%s, %s, 'monthly', 'import', %s, 'CN', '99999999', %s, 'EUR', '{}')
                """,
                (eu_rel, run, reporter, val),
            )

        cur.execute(
            "INSERT INTO fx_rates (currency_from, currency_to, rate_date, rate, rate_source) "
            "VALUES ('CNY', 'EUR', %s, 0.125, 'ECB monthly average')",
            (period,),
        )
        conn.commit()

    counts = anomalies.detect_mirror_trade_gaps(period=period)
    # With only the EU obs in this test, we should get 1 emitted finding
    assert counts["emitted"] == 1, f"counts={counts}"

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT title, body, detail FROM findings ORDER BY id DESC LIMIT 1")
        title, body, detail = cur.fetchone()

    assert "eu_bloc" in title or "EU" in title or "BLOC" in title
    assert detail["is_aggregate"] is True
    assert detail["aggregate"]["kind"] == "eu_bloc"
    assert "DE" in detail["aggregate"]["members_iso2"]
    assert "aggregate_composition" in detail["caveat_codes"]
    # GACC: 5000 × 1e8 × 0.125 = €62.5B; Eurostat: 70B; gap = +7.5B / 70B = +10.71%
    assert abs(detail["gap_pct"] - 0.10714) < 1e-3


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


def _seed_mirror_gap_findings(conn, iso2: str, gap_pcts: list[tuple[date, float]]) -> list[int]:
    """Insert a series of mirror_gap findings for the given iso2 across periods.
    Each finding gets a stub GACC observation+release so the trend detector can
    join through observation_ids[1] to recover the period."""
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES ('seed', 'success') RETURNING id",
    )
    run_id = cur.fetchone()[0]

    finding_ids = []
    for period, gap_pct in gap_pcts:
        cur.execute(
            """
            INSERT INTO releases (source, section_number, currency, period, release_kind, source_url, unit)
            VALUES ('gacc', 4, 'CNY', %s, 'preliminary', 'x', 'CNY 100 Million')
            RETURNING id
            """,
            (period,),
        )
        rel_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO observations (release_id, scrape_run_id, period_kind, flow,
                                      partner_country, value_amount, value_currency, source_row)
            VALUES (%s, %s, 'monthly', 'export', %s, 1000, 'CNY', '{}')
            RETURNING id
            """,
            (rel_id, run_id, iso2),
        )
        gacc_obs_id = cur.fetchone()[0]
        detail = {
            "method": "mirror_trade_v1",
            "iso2": iso2,
            "gap_pct": gap_pct,
        }
        cur.execute(
            """
            INSERT INTO findings (scrape_run_id, kind, subkind, observation_ids, score,
                                  title, body, detail)
            VALUES (%s, 'anomaly', 'mirror_gap', %s, %s, %s, %s, %s::jsonb)
            RETURNING id
            """,
            (run_id, [gacc_obs_id], abs(gap_pct), f"seed {iso2} {period}", "seed body",
             '{"method":"mirror_trade_v1","iso2":"' + iso2 + '","gap_pct":' + str(gap_pct) + '}'),
        )
        finding_ids.append(cur.fetchone()[0])
    conn.commit()
    return finding_ids


def test_trend_emits_finding_when_gap_jumps(empty_op_tables, test_db_url):
    """A steady ~50% baseline followed by a sudden jump to 80% should fire a
    z-score finding for that period."""
    with psycopg2.connect(test_db_url) as conn:
        # 6 months of baseline at 50%, 51%, 49%, 50%, 50%, 51%, then 80% (the jump)
        series = [
            (date(2025, 7, 1), 0.50),
            (date(2025, 8, 1), 0.51),
            (date(2025, 9, 1), 0.49),
            (date(2025, 10, 1), 0.50),
            (date(2025, 11, 1), 0.50),
            (date(2025, 12, 1), 0.51),
            (date(2026, 1, 1), 0.80),  # the news
        ]
        _seed_mirror_gap_findings(conn, "NL", series)

    counts = anomalies.detect_mirror_gap_trends(window_months=6, z_threshold=1.5, min_baseline_n=3)
    assert counts["emitted"] == 1, f"expected 1 finding, got counts={counts}"
    # i=0,1,2 lack a 3-point baseline; i=3,4,5 are within-baseline noise (below threshold);
    # i=6 is the jump and fires.
    assert counts["skipped_insufficient_baseline"] == 3
    assert counts["skipped_below_threshold"] == 3

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT subkind, score, title, detail FROM findings WHERE subkind='mirror_gap_zscore'"
        )
        subkind, score, title, detail = cur.fetchone()
    assert subkind == "mirror_gap_zscore"
    assert "NL" in title
    assert "2026-01" in title
    # Baseline mean ≈ 0.5017; stdev ≈ 0.008; jump to 0.80 → z ≈ 38; well above threshold
    assert detail["z_score"] > 5
    assert float(score) == abs(detail["z_score"])
    assert detail["baseline"]["n"] == 6
    assert detail["underlying_mirror_gap_finding_id"] is not None


def test_trend_silent_when_below_threshold(empty_op_tables, test_db_url):
    """A noisy series with no real shift should produce no findings."""
    import random
    random.seed(42)
    with psycopg2.connect(test_db_url) as conn:
        series = [(date(2025, m, 1), 0.50 + random.uniform(-0.02, 0.02)) for m in range(3, 13)]
        _seed_mirror_gap_findings(conn, "DE", series)

    counts = anomalies.detect_mirror_gap_trends(window_months=6, z_threshold=2.0, min_baseline_n=3)
    assert counts["emitted"] == 0


def test_trend_skips_when_baseline_too_short(empty_op_tables, test_db_url):
    """Only 2 periods of history → can't form a baseline of n>=3."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_mirror_gap_findings(conn, "FR", [
            (date(2025, 11, 1), 0.50),
            (date(2025, 12, 1), 0.80),
        ])

    counts = anomalies.detect_mirror_gap_trends(window_months=6, z_threshold=1.0, min_baseline_n=3)
    assert counts["emitted"] == 0
    # 2 points: i=0 has 0 prior, i=1 has 1 prior. Both < 3.
    assert counts["skipped_insufficient_baseline"] == 2


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
