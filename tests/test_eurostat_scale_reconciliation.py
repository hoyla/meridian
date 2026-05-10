"""Reconciliation smoke test: CN8-detail sum matches '000TOTAL'-row sum.

Resolved 2026-05-10 in
`dev_notes/forward-work-eurostat-aggregate-scale.md`. Eurostat's bulk
file ships both per-CN8-detail rows and a `product_nc='000TOTAL'`
aggregate row per (reporter, period, partner, flow, stat_procedure).
A naïve `SUM(value_eur)` includes both and double-counts.

This test guards against regression from any future change that:
- introduces a new aggregate-shape `product_nc` value we don't filter, OR
- accidentally drops the implicit HS-pattern LIKE filter from an
  analyser query, OR
- changes the bulk-file format such that the relationship between the
  aggregate row and the detail rows shifts.

Runs against the LIVE Eurostat database (the test DB has no Eurostat
raw rows). Skipped if `GACC_LIVE_DATABASE_URL` isn't set or the named
DB has no 2024 EU-27 imports from CN.

The assertion: per (reporter, period), the sum of CN8-detail
`value_eur` should equal the `'000TOTAL'`-row `value_eur` within the
suppression-rate tolerance of 15% (DK runs 11.4% under, the highest
seen). Larger gaps indicate either a new aggregate row class we're
missing or a per-reporter format change.
"""

import os

import psycopg2
import pytest


LIVE_DB_ENV = "GACC_LIVE_DATABASE_URL"
SUPPRESSION_TOLERANCE = 0.15   # 15% — DK is the highest-seen at 11.4%


@pytest.fixture(scope="module")
def live_db_url() -> str:
    url = os.environ.get(LIVE_DB_ENV)
    if not url:
        pytest.skip(f"{LIVE_DB_ENV} not set; skipping live reconciliation")
    return url


@pytest.fixture(scope="module")
def live_conn(live_db_url):
    conn = psycopg2.connect(live_db_url)
    yield conn
    conn.close()


def _has_2024_eu27_cn_data(conn) -> bool:
    """Cheap probe — skips the assertion when the live DB hasn't been
    backfilled (e.g. on a fresh schema-applied DB)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM eurostat_raw_rows "
            "WHERE period >= '2024-01-01' AND period < '2025-01-01' "
            "AND partner = 'CN' AND flow = 1 LIMIT 1)"
        )
        return cur.fetchone()[0]


def test_cn8_detail_sum_matches_000total_per_reporter(live_conn):
    """For each EU-27 reporter, the sum of CN8-detail `value_eur` for
    2024 imports from CN (stat_procedure=1) should match the
    `'000TOTAL'`-row `value_eur` within the suppression tolerance."""
    if not _has_2024_eu27_cn_data(live_conn):
        pytest.skip("Live DB has no 2024 EU-27 imports from CN")
    with live_conn.cursor() as cur:
        cur.execute(
            """
            WITH per_reporter AS (
              SELECT reporter,
                     SUM(value_eur) FILTER (WHERE product_nc ~ '^[0-9]{8}$') AS detail_eur,
                     SUM(value_eur) FILTER (WHERE product_nc = '000TOTAL')   AS total_eur
                FROM eurostat_raw_rows
               WHERE period >= '2024-01-01' AND period < '2025-01-01'
                 AND partner = 'CN' AND flow = 1 AND stat_procedure = '1'
                 AND reporter <> 'GB'
            GROUP BY reporter
            )
            SELECT reporter, detail_eur, total_eur,
                   (detail_eur - total_eur) / NULLIF(total_eur, 0) AS rel_diff
              FROM per_reporter
             WHERE total_eur IS NOT NULL AND total_eur > 0
          ORDER BY reporter
            """
        )
        rows = cur.fetchall()
    assert rows, "Expected at least one EU-27 reporter with data"

    failures = []
    for reporter, detail_eur, total_eur, rel_diff in rows:
        rel_diff = float(rel_diff)
        if abs(rel_diff) > SUPPRESSION_TOLERANCE:
            failures.append(
                f"{reporter}: CN8 detail €{float(detail_eur)/1e9:.2f}B vs "
                f"'000TOTAL' €{float(total_eur)/1e9:.2f}B "
                f"(rel diff {rel_diff*100:+.1f}%)"
            )
    assert not failures, (
        "Per-reporter CN8-detail vs '000TOTAL' divergence exceeds "
        f"{SUPPRESSION_TOLERANCE*100:.0f}% tolerance — bulk-file format "
        "may have changed:\n  " + "\n  ".join(failures)
    )


def test_eu27_total_matches_published_headline(live_conn):
    """The CN8-detail sum across all EU-27 reporters and all
    stat_procedures for 2024 imports from CN should match Eurostat's
    published headline of ~€517B within ±5%."""
    if not _has_2024_eu27_cn_data(live_conn):
        pytest.skip("Live DB has no 2024 EU-27 imports from CN")
    with live_conn.cursor() as cur:
        cur.execute(
            """
            SELECT SUM(value_eur)/1e9
              FROM eurostat_raw_rows
             WHERE period >= '2024-01-01' AND period < '2025-01-01'
               AND partner = 'CN' AND flow = 1
               AND product_nc ~ '^[0-9]{8}$'
               AND reporter <> 'GB'
            """
        )
        total_b = float(cur.fetchone()[0])

    expected_b = 517.0
    rel_diff = abs(total_b - expected_b) / expected_b
    assert rel_diff < 0.05, (
        f"2024 EU-27 imports from CN: our CN8-detail sum is €{total_b:.1f}B; "
        f"Eurostat published ~€{expected_b}B (relative diff {rel_diff*100:.1f}%). "
        "More than 5% off — investigation needed."
    )


def test_no_hs_group_pattern_matches_aggregate_total_row(live_conn):
    """No hs_groups.hs_patterns LIKE pattern should match the
    `'000TOTAL'` aggregate row. Defence against someone adding an
    over-broad pattern like '0%' or '%' that would drag the
    grand-total row into HS-group sums and create a 2x double-count.
    """
    import anomalies

    code = anomalies.EUROSTAT_AGGREGATE_PRODUCT_NC
    with live_conn.cursor() as cur:
        cur.execute(
            "SELECT name, hs_patterns FROM hs_groups WHERE hs_patterns IS NOT NULL"
        )
        rows = cur.fetchall()
    assert rows, "Expected at least one hs_groups row to test against"

    offenders = []
    for name, patterns in rows:
        for pat in patterns:
            # Translate SQL LIKE % to fnmatch * for a Python-side check
            import fnmatch
            fn_pat = pat.replace("%", "*").replace("_", "?")
            if fnmatch.fnmatch(code, fn_pat):
                offenders.append((name, pat))
    assert not offenders, (
        f"hs_groups patterns that would match the aggregate row {code!r} "
        f"(would cause 2x double-counting): {offenders}"
    )
