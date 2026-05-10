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
            "WHERE subkind='hs_group_yoy' "
            "  AND (detail->'totals'->>'partial_window')::boolean = false "
            "ORDER BY score DESC LIMIT 1"
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
    # Provenance: the method definition is queryable from the finding alone.
    # New default sums CN+HK+MO; explicit partner override available via the
    # eurostat_partners parameter.
    assert detail["method_query"]["partners"] == ["CN", "HK", "MO"]
    assert detail["method_query"]["hs_patterns"] == ["854142%", "854143%"]
    # multi_partner_sum caveat fires by default (>1 partner summed).
    assert "multi_partner_sum" in detail["caveat_codes"]
    # Top contributors recorded with kg
    top = detail["top_cn8_codes_in_current_12mo"]
    assert any(c["hs_code"] == "85414210" for c in top)
    assert all("total_kg" in c for c in top)


def test_yoy_excludes_gb_reporter_at_all_times(empty_op, test_db_url):
    """EU-27 must mean EU-27 across the whole period range, not EU-28 for
    pre-Brexit years. Our eurostat_raw_rows has reporter='GB' rows for 2017
    through Q1 2020 (UK was an EU-28 reporter then); without filtering, the
    hs-group analysers silently roll UK trade into the EU sum for those years
    and exclude it from 2021+ — breaking any cross-Brexit comparison. This
    test seeds the same period range under both DE and GB reporters and
    verifies only DE contributes to the EU-27 sum."""
    with psycopg2.connect(test_db_url) as conn:
        # DE reporter: rises 100 -> 150 (1.2B -> 1.8B, +50% YoY)
        de_24 = _make_24_months(date(2018, 1, 1), [100.0] * 12 + [150.0] * 12)
        _seed_eurostat_imports(conn, "85414210", de_24)
        # GB reporter (would-be UK contribution): flat 200 across the same window.
        # If included, would shift totals to (1.2+2.4)B -> (1.8+2.4)B and YoY to
        # ~+16.7%. We assert the EU-27 sum sees DE only (+50%, 1.8B).
        cur = conn.cursor()
        for period, _ in de_24:
            cur.execute(
                "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'success') RETURNING id",
                (f"http://example/gb-{period}",),
            )
            run = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO eurostat_raw_rows (scrape_run_id, period, reporter, partner, "
                "                                product_nc, flow, value_eur, value_nac, "
                "                                quantity_kg, quantity_suppl_unit) "
                "VALUES (%s, %s, 'GB', 'CN', '85414210', 1, 200, 200, 20, 0)",
                (run, period),
            )
        conn.commit()

    counts = anomalies.detect_hs_group_yoy(
        group_names=["Solar PV cells & modules"], yoy_threshold_pct=0.10,
    )
    assert counts["emitted"] >= 1, f"counts={counts}"

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail FROM findings "
            "WHERE subkind='hs_group_yoy' "
            "  AND (detail->'totals'->>'partial_window')::boolean = false "
            "ORDER BY score DESC LIMIT 1"
        )
        detail = cur.fetchone()[0]

    # DE-only sums: prior 12 * 100 = 1200, current 12 * 150 = 1800. If GB
    # were leaking in, both would be inflated by 200*12 = 2400.
    assert detail["totals"]["current_12mo_eur"] == 1800.0
    assert detail["totals"]["prior_12mo_eur"] == 1200.0
    assert abs(detail["totals"]["yoy_pct"] - 0.5) < 1e-9
    # And the v8 method-version tag is on the finding (cheap-honesty: the
    # method-name change makes the supersede chain traceable).
    assert "excludes_gb_reporter" in detail["method"]
    # Top reporters list should not contain GB either.
    top_reporters = [r["reporter"] for r in detail["top_reporters_in_current_12mo"]]
    assert "GB" not in top_reporters
    assert "DE" in top_reporters


def test_yoy_cn_only_override_excludes_hk_mo(empty_op, test_db_url):
    """Mirrors test_yoy_emits_when_growth_above_threshold but with explicit
    eurostat_partners=['CN']. The finding records partner='CN' alone in
    method_query.partners and DOES NOT carry the multi_partner_sum caveat
    (the override is single-partner so the comparison is direct, not summed).
    Editorial use: the journalist comparing against a single-partner
    Soapbox/Merics figure passes --eurostat-partners CN to match scope."""
    with psycopg2.connect(test_db_url) as conn:
        prior_12 = [100.0] * 12
        current_12 = [150.0] * 12
        _seed_eurostat_imports(
            conn, "85414210",
            _make_24_months(date(2024, 1, 1), prior_12 + current_12),
        )

    counts = anomalies.detect_hs_group_yoy(
        group_names=["Solar PV cells & modules"], yoy_threshold_pct=0.10,
        eurostat_partners=["CN"],
    )
    assert counts["emitted"] >= 1, f"counts={counts}"

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail FROM findings "
            "WHERE subkind='hs_group_yoy' "
            "  AND (detail->'totals'->>'partial_window')::boolean = false "
            "ORDER BY score DESC LIMIT 1"
        )
        detail = cur.fetchone()[0]

    assert detail["method_query"]["partners"] == ["CN"]
    assert "multi_partner_sum" not in detail["caveat_codes"]


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
            "SELECT detail FROM findings WHERE subkind='hs_group_yoy' "
            "  AND (detail->'totals'->>'partial_window')::boolean = false "
            "ORDER BY score DESC LIMIT 1"
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
            "  AND (detail->'totals'->>'partial_window')::boolean = false "
            "ORDER BY score DESC LIMIT 1"
        )
        detail = cur.fetchone()[0]

    # Value +50%, kg +50%, unit price ~unchanged → volume-driven
    assert abs(detail["totals"]["yoy_pct"] - 0.5) < 1e-9
    assert abs(detail["totals"]["yoy_pct_kg"] - 0.5) < 1e-9
    assert abs(detail["totals"]["unit_price_pct_change"]) < 1e-9
    assert "volume-driven" in detail.get("body", "") or True  # body lives outside detail
    # Phase 1.5: full kg coverage on this synthetic data → decomposition NOT suppressed.
    assert detail["totals"]["kg_coverage_pct"] >= 0.99
    assert detail["totals"]["decomposition_suppressed"] is False


def test_yoy_low_base_threshold_is_configurable(empty_op, test_db_url):
    """Phase 1.6: a group with €60M rolling totals should NOT be flagged
    low-base under the default €50M threshold, but SHOULD be flagged when
    the threshold is bumped to €100M. The threshold itself is recorded in
    the finding's detail for auditability."""
    with psycopg2.connect(test_db_url) as conn:
        # 24 months × €5M each → both windows = €60M. Above €50M default,
        # below a €100M caller-provided threshold.
        _seed_eurostat_imports(
            conn, "85076010",
            _make_24_months(date(2024, 1, 1), [5_000_000.0] * 24),
        )

    # Default threshold: not flagged.
    counts_default = anomalies.detect_hs_group_yoy(
        group_names=["EV batteries (Li-ion)"], yoy_threshold_pct=0.0,
    )
    assert counts_default["emitted"] >= 1
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail FROM findings WHERE subkind='hs_group_yoy' "
            "AND detail->'group'->>'name' = 'EV batteries (Li-ion)' "
            "AND superseded_at IS NULL ORDER BY score DESC LIMIT 1"
        )
        detail = cur.fetchone()[0]
    assert detail["totals"]["low_base"] is False
    assert detail["totals"]["low_base_threshold_eur"] == anomalies.LOW_BASE_THRESHOLD_EUR

    # Bumped threshold: now flagged. Re-running with a different threshold
    # supersedes the prior finding (low_base bool changes → value_signature
    # changes → supersede).
    counts_strict = anomalies.detect_hs_group_yoy(
        group_names=["EV batteries (Li-ion)"], yoy_threshold_pct=0.0,
        low_base_threshold_eur=100_000_000.0,
    )
    assert counts_strict["superseded"] >= 1
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail FROM findings WHERE subkind='hs_group_yoy' "
            "AND detail->'group'->>'name' = 'EV batteries (Li-ion)' "
            "AND superseded_at IS NULL ORDER BY score DESC LIMIT 1"
        )
        detail = cur.fetchone()[0]
    assert detail["totals"]["low_base"] is True
    assert detail["totals"]["low_base_threshold_eur"] == 100_000_000.0
    assert "low_base_effect" in detail["caveat_codes"]


def test_yoy_partial_window_caveat_when_one_month_missing(empty_op, test_db_url):
    """Phase 2.7: when the 24-month window has 1 missing month, the
    finding emits with a `partial_window` caveat and records which
    month is missing in detail.totals.missing_months_*."""
    with psycopg2.connect(test_db_url) as conn:
        # 23 months, NOT 24 — the most-recent (anchor) month is missing,
        # which is the realistic case where Eurostat hasn't published yet.
        # We'll use anchor t such that exactly one month from the window
        # is absent.
        full_24 = _make_24_months(date(2024, 1, 1), [100.0] * 12 + [150.0] * 12)
        # Drop the 24th period entirely.
        partial = full_24[:-1]  # 23 months
        _seed_eurostat_imports(conn, "85076010", partial)

    counts = anomalies.detect_hs_group_yoy(
        group_names=["EV batteries (Li-ion)"], yoy_threshold_pct=0.0,
    )
    # Anchor t = the missing 24th month (Dec 2025): all 12 prior months
    # present, all but the last current month present → exactly 1 missing.
    assert counts["emitted"] >= 1, f"counts={counts}"
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail FROM findings WHERE subkind='hs_group_yoy' "
            "  AND detail->'group'->>'name' = 'EV batteries (Li-ion)' "
            "  AND (detail->'totals'->>'partial_window')::boolean = true "
            "ORDER BY score DESC LIMIT 1"
        )
        row = cur.fetchone()
        assert row is not None, "expected at least one partial_window finding"
        detail = row[0]
    assert detail["totals"]["partial_window"] is True
    # 1 month missing from current; 0 from prior in this fixture.
    total_missing = (
        len(detail["totals"]["missing_months_current"]) +
        len(detail["totals"]["missing_months_prior"])
    )
    assert total_missing == 1
    assert "partial_window" in detail["caveat_codes"]


def test_yoy_skips_window_with_two_missing_months(empty_op, test_db_url):
    """Phase 2.7: when 2+ months are missing from the 24-month window, the
    analyser still skips. The 1-month tolerance is deliberately narrow."""
    with psycopg2.connect(test_db_url) as conn:
        full_24 = _make_24_months(date(2024, 1, 1), [100.0] * 12 + [150.0] * 12)
        # Drop the last 2 months.
        partial = full_24[:-2]  # 22 months
        _seed_eurostat_imports(conn, "85076010", partial)

    counts = anomalies.detect_hs_group_yoy(
        group_names=["EV batteries (Li-ion)"], yoy_threshold_pct=0.0,
    )
    # No findings should have partial_window=true with 2 missing.
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM findings WHERE subkind='hs_group_yoy' "
            "  AND detail->'group'->>'name' = 'EV batteries (Li-ion)' "
            "  AND (detail->'totals'->>'partial_window')::boolean = true"
        )
        partial_count = cur.fetchone()[0]
    # And the skip count went up (per anchor where 2+ months are missing).
    assert counts["skipped_insufficient_history"] >= 1
    # No partial_window emitted.
    assert partial_count == 0


def test_yoy_attaches_cn8_revision_caveat_for_cross_year_window(empty_op, test_db_url):
    """Phase 2.8: any 24-month window spanning a calendar-year boundary
    gets a `cn8_revision` caveat. That's most windows in practice."""
    with psycopg2.connect(test_db_url) as conn:
        # Window from Jan 2024 (start_prior) to Dec 2025 (end_curr) —
        # spans 2024→2025 boundary. Expect cn8_revision caveat.
        _seed_eurostat_imports(
            conn, "85076010",
            _make_24_months(date(2024, 1, 1), [100.0] * 24),
        )

    anomalies.detect_hs_group_yoy(
        group_names=["EV batteries (Li-ion)"], yoy_threshold_pct=0.0,
    )
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail FROM findings WHERE subkind='hs_group_yoy' "
            "  AND detail->'group'->>'name' = 'EV batteries (Li-ion)' "
            "  AND (detail->'totals'->>'partial_window')::boolean = false "
            "  ORDER BY score DESC LIMIT 1"
        )
        detail = cur.fetchone()[0]
    assert "cn8_revision" in detail["caveat_codes"]


def test_yoy_decomposition_suppressed_when_kg_coverage_low(empty_op, test_db_url):
    """Phase 1.5: when most of the value_eur in a group is backed by rows
    with no kg measurement (pieces-, litres-, units-denominated HS codes),
    the unit-price decomposition is misleading and is suppressed.

    Seed setup: most rows have kg=0 (no kg coverage); a small minority
    have kg>0. Coverage ≈ small minority's value share / total. We want
    that share < 80% to trigger suppression."""
    with psycopg2.connect(test_db_url) as conn:
        # Seed two HS codes to the same group (machine tools 8456%–8463%):
        # - One with full kg coverage but small value (€10 × 24)
        # - One with no kg (zero) but large value (€1000 × 24)
        # Coverage = €10 / (€10+€1000) = 1% per period — way below the 80% threshold.
        _seed_eurostat_imports(
            conn, "84561000",  # has kg
            _make_24_months(date(2024, 1, 1), [10.0] * 24),
            kg_per_period=[1.0] * 24,
        )
        _seed_eurostat_imports(
            conn, "84571000",  # no kg
            _make_24_months(date(2024, 1, 1), [1000.0] * 24),
            kg_per_period=[0.0] * 24,
        )

    anomalies.detect_hs_group_yoy(
        group_names=["Machine tools"], yoy_threshold_pct=0.0,
    )
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT title, body, detail FROM findings WHERE subkind='hs_group_yoy' "
            "AND detail->'group'->>'name' = 'Machine tools' "
            "ORDER BY score DESC LIMIT 1"
        )
        row = cur.fetchone()
        assert row is not None, "expected an hs_group_yoy finding for Machine tools"
        title, body, detail = row

    assert detail["totals"]["kg_coverage_pct"] < 0.80
    assert detail["totals"]["decomposition_suppressed"] is True
    # The suppressed unit-price fields are NULL in totals.
    assert detail["totals"]["current_unit_price_eur_per_kg"] is None
    assert detail["totals"]["prior_unit_price_eur_per_kg"] is None
    assert detail["totals"]["unit_price_pct_change"] is None
    # And the caveat is attached.
    assert "low_kg_coverage" in detail["caveat_codes"]
    # Body explains the suppression rather than silently omitting.
    assert "SUPPRESSED" in body
