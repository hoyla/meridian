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
    assert anomalies.parse_unit_scale("") == (1.0, None)
    # Numeric-only multipliers are still recognised (currency required, but no
    # scale-word needed).
    assert anomalies.parse_unit_scale("USD 10000") == (10000.0, "USD")


def test_unit_scale_unrecognised_returns_none_not_silent_fallback(caplog):
    """Phase 1.2: a non-empty unit string we can't parse must signal failure
    via (None, None), not silently fall back to (1.0, None) which can produce
    a converted EUR value off by orders of magnitude. Logs at ERROR."""
    import logging as _logging
    with caplog.at_level(_logging.ERROR, logger="anomalies"):
        result = anomalies.parse_unit_scale("CNY 万")  # Chinese-style scale not in regex
    assert result == (None, None)
    assert any("Unrecognised unit string" in rec.message for rec in caplog.records)
    assert any(rec.levelno == _logging.ERROR for rec in caplog.records)


def _seed_pair_for_partner(conn, period: date, gacc_label: str,
                            eurostat_reporter: str) -> tuple[int, list[int]]:
    """Variant of _seed_one_pair for a specific partner. Used for the
    transshipment-hub tests in Phase 2.1."""
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO releases (source, section_number, currency, period, release_kind, "
        "                      source_url, unit, title, description) "
        "VALUES ('gacc', 4, 'CNY', %s, 'preliminary', %s, 'CNY 100 Million', 't', 'd') "
        "RETURNING id",
        (period, f"http://example/gacc-{gacc_label}.html"),
    )
    gacc_release_id = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES "
        "(%s, 'success') RETURNING id",
        (f"http://example/gacc-{gacc_label}.html",),
    )
    gacc_run_id = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO observations (release_id, scrape_run_id, period_kind, flow, "
        "                          partner_country, value_amount, value_currency, source_row) "
        "VALUES (%s, %s, 'monthly', 'export', %s, 1000, 'CNY', '{}') RETURNING id",
        (gacc_release_id, gacc_run_id, gacc_label),
    )
    gacc_obs_id = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO releases (source, period, source_url) VALUES "
        "('eurostat', %s, %s) RETURNING id",
        (period, f"http://example/eurostat-{eurostat_reporter}.7z"),
    )
    eu_release = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES "
        "(%s, 'success') RETURNING id",
        (f"http://example/eurostat-{eurostat_reporter}.7z",),
    )
    eu_run = cur.fetchone()[0]
    eu_obs_ids = []
    for hs, val in [("87038010", 7_000_000_000), ("87038090", 4_000_000_000)]:
        cur.execute(
            "INSERT INTO observations (release_id, scrape_run_id, period_kind, flow, "
            "                          reporter_country, partner_country, hs_code, "
            "                          value_amount, value_currency, source_row) "
            "VALUES (%s, %s, 'monthly', 'import', %s, 'CN', %s, %s, 'EUR', '{}') RETURNING id",
            (eu_release, eu_run, eurostat_reporter, hs, val),
        )
        eu_obs_ids.append(cur.fetchone()[0])
    cur.execute(
        "INSERT INTO fx_rates (currency_from, currency_to, rate_date, rate, rate_source, "
        "                      rate_source_url, notes) "
        "VALUES ('CNY', 'EUR', %s, 0.125, 'ECB monthly average', 'http://example/ecb', 't')",
        (period,),
    )
    conn.commit()
    return gacc_obs_id, eu_obs_ids


def test_mirror_gap_attaches_transshipment_caveat_for_known_hub(
    empty_op_tables, test_db_url,
):
    """Phase 2.1: when the partner iso2 is in transshipment_hubs (NL is
    seeded), the mirror_gap finding gets the `transshipment_hub` caveat
    and a hub annotation in the body. This is editorial framing — the
    gap level for hubs reflects routing, not direct trade."""
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        _seed_pair_for_partner(conn, period, "Netherlands", "NL")

    counts = anomalies.detect_mirror_trade_gaps(period=period)
    assert counts["emitted"] == 1, f"counts={counts}"

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT body, detail FROM findings WHERE subkind = 'mirror_gap'"
        )
        body, detail = cur.fetchone()

    assert detail["transshipment_hub"] is not None
    assert detail["transshipment_hub"]["iso2"] == "NL"
    assert "transshipment_hub" in detail["caveat_codes"]
    assert "TRANSSHIPMENT-HUB CONTEXT" in body
    # Evidence URL travels with the finding so a journalist can audit.
    assert detail["transshipment_hub"]["evidence_url"]


def test_mirror_gap_no_transshipment_caveat_for_non_hub(empty_op_tables, test_db_url):
    """Germany is not in transshipment_hubs — finding emits without the caveat."""
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        _seed_pair_for_partner(conn, period, "Germany", "DE")

    anomalies.detect_mirror_trade_gaps(period=period)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT body, detail FROM findings WHERE subkind = 'mirror_gap'"
        )
        body, detail = cur.fetchone()

    assert detail["transshipment_hub"] is None
    assert "transshipment_hub" not in detail["caveat_codes"]
    assert "TRANSSHIPMENT-HUB CONTEXT" not in body


def _seed_extra_eurostat_partner(conn, period: date, reporter: str,
                                  partner_iso2: str, value: float) -> None:
    """Add one extra Eurostat import row with a non-CN partner_country (e.g.
    'HK') for an existing reporter+period. Used by Phase 2.3 multi-partner
    tests."""
    cur = conn.cursor()
    # Find the existing eurostat release for this period.
    cur.execute(
        "SELECT id FROM releases WHERE source = 'eurostat' AND period = %s",
        (period,),
    )
    rel = cur.fetchone()
    assert rel is not None, "expected a Eurostat release seeded for the period"
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES "
        "('http://example/extra-partner', 'success') RETURNING id"
    )
    run = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO observations (release_id, scrape_run_id, period_kind, flow, "
        "                          reporter_country, partner_country, hs_code, "
        "                          value_amount, value_currency, source_row) "
        "VALUES (%s, %s, 'monthly', 'import', %s, %s, '85076010', %s, 'EUR', '{}')",
        (rel[0], run, reporter, partner_iso2, value),
    )
    conn.commit()


def test_mirror_gap_default_partners_excludes_hk(empty_op_tables, test_db_url):
    """Phase 2.3 baseline: with the default eurostat_partners=['CN'], a
    Eurostat partner=HK row is NOT counted in the mirror-gap sum. The
    finding has no `multi_partner_sum` caveat."""
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        _seed_pair_for_partner(conn, period, "Germany", "DE")
        # Add a HK-routed €1B import for DE — this should NOT show up.
        _seed_extra_eurostat_partner(conn, period, "DE", "HK", 1_000_000_000)

    anomalies.detect_mirror_trade_gaps(period=period)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail FROM findings WHERE subkind = 'mirror_gap'"
        )
        detail = cur.fetchone()[0]
    assert detail["eurostat"]["partners_summed"] == ["CN"]
    assert "multi_partner_sum" not in detail["caveat_codes"]
    # Eurostat total = 11B (CN-only), not 12B (CN + HK).
    assert abs(detail["eurostat"]["total_eur"] - 11_000_000_000) < 1


def test_mirror_gap_with_cn_hk_sums_both_partners(empty_op_tables, test_db_url):
    """Phase 2.3: when --eurostat-partners CN,HK is used, the analyser sums
    both partner_country rows. The multi_partner_sum caveat is attached
    and the body annotation explains the methodological shift."""
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        _seed_pair_for_partner(conn, period, "Germany", "DE")
        _seed_extra_eurostat_partner(conn, period, "DE", "HK", 1_000_000_000)

    anomalies.detect_mirror_trade_gaps(
        period=period, eurostat_partners=["CN", "HK"],
    )
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT body, detail FROM findings WHERE subkind = 'mirror_gap'"
        )
        body, detail = cur.fetchone()
    assert detail["eurostat"]["partners_summed"] == ["CN", "HK"]
    assert "multi_partner_sum" in detail["caveat_codes"]
    # Eurostat total = 12B (CN + HK).
    assert abs(detail["eurostat"]["total_eur"] - 12_000_000_000) < 1
    assert "Multi-partner Eurostat sum" in body


def test_mirror_gap_records_cif_fob_baseline_provenance(empty_op_tables, test_db_url):
    """Phase 2.2: each finding records exactly which CIF/FOB baseline row
    drove the comparison — including a `source` string and `source_url`
    for editorial audit. Default is the global row seeded at 7.5%."""
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        _seed_pair_for_partner(conn, period, "Germany", "DE")

    anomalies.detect_mirror_trade_gaps(period=period)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail FROM findings WHERE subkind = 'mirror_gap'"
        )
        detail = cur.fetchone()[0]

    cf = detail["cif_fob_baseline"]
    assert cf["scope"] == "global"
    assert cf["partner_iso2"] is None
    assert abs(cf["baseline_pct"] - 0.075) < 1e-9
    assert "UNCTAD" in cf["source"]
    assert cf["source_url"]


def test_mirror_gap_uses_per_partner_cif_fob_override(empty_op_tables, test_db_url):
    """Phase 2.2: when a per-partner row exists in cif_fob_baselines, the
    analyser prefers it over the global default. Editorial use: a
    journalist sourcing a UNCTAD per-route figure can override one
    partner without touching the global.

    The test mutates cif_fob_baselines (a lookup table not covered by the
    empty_op_tables fixture, which truncates only operational tables),
    so we wrap in try/finally to delete the override after the assertions
    — otherwise it leaks into subsequent tests."""
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        _seed_pair_for_partner(conn, period, "Germany", "DE")
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cif_fob_baselines (partner_iso2, baseline_pct, source, source_url, notes) "
                "VALUES ('DE', 0.12, 'Test override', 'http://example/de-cif', 't')"
            )
            conn.commit()

    try:
        anomalies.detect_mirror_trade_gaps(period=period)
        with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT detail FROM findings WHERE subkind = 'mirror_gap'"
            )
            detail = cur.fetchone()[0]

        cf = detail["cif_fob_baseline"]
        assert cf["scope"] == "per-partner"
        assert cf["partner_iso2"] == "DE"
        assert abs(cf["baseline_pct"] - 0.12) < 1e-9
        # Excess is recomputed using the new baseline.
        expected_excess = abs(detail["gap_pct"]) - 0.12
        assert abs(detail["excess_over_baseline_pct"] - expected_excess) < 1e-9
    finally:
        with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM cif_fob_baselines WHERE partner_iso2 = 'DE'")
            conn.commit()


def test_mirror_gap_skips_unrecognised_unit(empty_op_tables, test_db_url):
    """End-to-end: a GACC release with an unrecognised unit string must NOT
    produce a mirror_gap finding. The skip is tallied under
    skipped_unrecognised_unit so journalists notice the gap."""
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        gacc_obs_id, _ = _seed_one_pair(conn, period)
        # Mutate the GACC release's unit to something the parser won't handle.
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE releases SET unit = 'CNY 万' "  # Chinese ten-thousand — not in regex
                "WHERE source = 'gacc' AND period = %s",
                (period,),
            )
            conn.commit()

    counts = anomalies.detect_mirror_trade_gaps(period=period)
    assert counts["skipped_unrecognised_unit"] == 1
    assert counts["emitted"] == 0
    assert counts["inserted_new"] == 0

    # No mirror_gap finding should have been written.
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM findings WHERE subkind = 'mirror_gap'")
        assert cur.fetchone()[0] == 0


def test_mirror_gap_emits_finding_with_provenance(empty_op_tables, test_db_url):
    period = date(2025, 12, 1)
    with psycopg2.connect(test_db_url) as conn:
        gacc_obs_id, eu_obs_ids = _seed_one_pair(conn, period)

    counts = anomalies.detect_mirror_trade_gaps(period=period)
    assert counts["emitted"] == 1
    assert counts["inserted_new"] == 1  # empty DB → first emission inserts new
    # No skip categories should fire on this clean seeded data.
    assert all(v == 0 for k, v in counts.items() if k.startswith("skipped_"))

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
    # Phase 1.4: with n=6, the baseline is right at the confidence threshold,
    # so no low_baseline_n caveat should fire here.
    assert "low_baseline_n" not in detail["caveat_codes"]


def test_trend_logs_staleness_warning_when_upstream_lags(
    empty_op_tables, test_db_url, caplog,
):
    """Phase 2.6: when the latest active mirror_gap finding's period is
    older than the latest Eurostat/GACC release, a WARNING fires before
    the trend analyser runs. The journalist sees the staleness instead
    of receiving findings built silently on stale input."""
    import logging as _logging
    # Seed: an old mirror_gap finding (Sep 2025) but a newer Eurostat
    # release (Feb 2026) — so the upstream pass clearly hasn't been
    # re-run after newer data landed.
    with psycopg2.connect(test_db_url) as conn:
        _seed_mirror_gap_findings(conn, "DE", [
            (date(2025, 9, 1), 0.50),
        ])
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO releases (source, period, source_url) "
                "VALUES ('eurostat', '2026-02-01', 'http://example/eu-feb')"
            )
            cur.execute(
                "INSERT INTO releases (source, period, source_url) "
                "VALUES ('gacc', '2026-02-01', 'http://example/gacc-feb')"
            )
            conn.commit()

    with caplog.at_level(_logging.WARNING, logger="anomalies"):
        anomalies.detect_mirror_gap_trends(
            window_months=6, z_threshold=1.5, min_baseline_n=3,
        )

    staleness = [r for r in caplog.records
                 if "staleness" in r.message and r.levelno == _logging.WARNING]
    assert len(staleness) >= 1, (
        f"expected a staleness WARNING; got records: "
        f"{[(r.levelname, r.message) for r in caplog.records]}"
    )


def test_trend_attaches_low_baseline_n_caveat_below_threshold(empty_op_tables, test_db_url):
    """Phase 1.4: with only 4 baseline points (≥ min_baseline_n=3 but
    < LOW_BASELINE_N_THRESHOLD=6), the z-score finding still emits but
    must carry the `low_baseline_n` caveat. Editorial honesty over silent
    drops."""
    with psycopg2.connect(test_db_url) as conn:
        # 4 months of low-noise baseline, then a jump.
        series = [
            (date(2025, 9, 1), 0.50),
            (date(2025, 10, 1), 0.51),
            (date(2025, 11, 1), 0.49),
            (date(2025, 12, 1), 0.50),
            (date(2026, 1, 1), 0.80),  # jump
        ]
        _seed_mirror_gap_findings(conn, "BE", series)

    counts = anomalies.detect_mirror_gap_trends(
        window_months=6, z_threshold=1.5, min_baseline_n=3,
    )
    assert counts["emitted"] == 1, f"counts={counts}"

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail, body FROM findings WHERE subkind='mirror_gap_zscore'"
        )
        detail, body = cur.fetchone()
    assert detail["baseline"]["n"] == 4
    assert detail["baseline"]["low_n_flag"] is True
    assert detail["baseline"]["low_n_threshold"] == anomalies.LOW_BASELINE_N_THRESHOLD
    assert "low_baseline_n" in detail["caveat_codes"]
    assert "LOW BASELINE-N FLAG" in body


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
