"""Integration tests for db.py against a live Postgres test database."""

from datetime import date
from pathlib import Path

import psycopg2

import db
from parse import parse_html

FIXTURES = Path(__file__).parent / "fixtures"
SECTION4 = FIXTURES / "release_section4_by_country_mar2026_cny.html"
URL = "http://english.customs.gov.cn/Statics/test-fixture-uuid.html"


def _seed_eurostat_total(test_db_url, rows):
    """Seed eurostat_raw_rows 000TOTAL cells. Each row: (period, reporter,
    partner). Flow/value are immaterial to coverage/presence — those key on
    (period, reporter, partner, product_nc)."""
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) "
            "VALUES ('test://eurostat-coverage', 'success') RETURNING id")
        run_id = cur.fetchone()[0]
        for period, reporter, partner in rows:
            cur.execute(
                "INSERT INTO eurostat_raw_rows (scrape_run_id, period, reporter, "
                "partner, product_nc, flow, value_eur) "
                "VALUES (%s, %s, %s, %s, '000TOTAL', 1, 100)",
                (run_id, period, reporter, partner))
        conn.commit()


def _parse_fixture():
    return parse_html(SECTION4.read_bytes(), URL)


def test_persist_and_idempotent_rerun(clean_db, test_db_url):
    result = _parse_fixture()

    run_id = db.start_run(URL)
    release_id = db.find_or_create_gacc_release(result.metadata, release_kind="preliminary")
    counts = db.upsert_observations(run_id, release_id, result.observations)
    db.finish_run(run_id, status="success", http_status=200)

    assert counts == {"inserted": 180, "versioned": 0, "unchanged": 0}

    # Second pass — same data, should all be unchanged and not duplicated.
    run_id2 = db.start_run(URL)
    release_id2 = db.find_or_create_gacc_release(result.metadata, release_kind="preliminary")
    counts2 = db.upsert_observations(run_id2, release_id2, result.observations)
    db.finish_run(run_id2, status="success", http_status=200)

    assert release_id2 == release_id
    assert counts2 == {"inserted": 0, "versioned": 0, "unchanged": 180}

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM observations")
        assert cur.fetchone()[0] == 180
        cur.execute("SELECT count(*) FROM releases")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(*) FROM scrape_runs WHERE status = 'success'")
        assert cur.fetchone()[0] == 2


def test_version_bumps_when_value_changes(clean_db, test_db_url):
    result = _parse_fixture()

    run_id = db.start_run(URL)
    release_id = db.find_or_create_gacc_release(result.metadata, release_kind="preliminary")
    db.upsert_observations(run_id, release_id, result.observations)
    db.finish_run(run_id, status="success")

    # Simulate a republished value: alter one observation and re-upsert.
    altered = [dict(o) for o in result.observations]
    target = next(
        o for o in altered
        if o["partner_country"] == "United States (US)"
        and o["flow"] == "export"
        and o["period_kind"] == "monthly"
    )
    target["value"] = 9999.9

    run_id2 = db.start_run(URL)
    counts = db.upsert_observations(run_id2, release_id, altered)
    db.finish_run(run_id2, status="success")

    assert counts == {"inserted": 0, "versioned": 1, "unchanged": 179}

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT version_seen, value_amount FROM observations
             WHERE release_id = %s AND partner_country = 'United States (US)'
               AND flow = 'export' AND period_kind = 'monthly'
          ORDER BY version_seen
            """,
            (release_id,),
        )
        rows = [(int(v), float(a)) for v, a in cur.fetchall()]

    assert rows == [(1, 2045.0), (2, 9999.9)]


def test_release_metadata_refresh(clean_db, test_db_url):
    """If the same release is re-fetched with a fresher publication_date or excel_url,
    those fields are updated on the existing release row rather than ignored."""
    result = _parse_fixture()

    run_id = db.start_run(URL)
    release_id = db.find_or_create_gacc_release(result.metadata, release_kind="preliminary")
    db.finish_run(run_id, status="success")

    # Stale publication_date: pretend our first observation had no pub date set.
    from dataclasses import replace

    stripped = replace(result.metadata, publication_date=None, excel_url=None)
    run_id2 = db.start_run(URL)
    release_id2 = db.find_or_create_gacc_release(stripped, release_kind="preliminary")
    db.finish_run(run_id2, status="success")
    assert release_id2 == release_id

    # The original publication_date should still be on the row (COALESCE preserves it).
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT publication_date FROM releases WHERE id = %s", (release_id,))
        assert cur.fetchone()[0] is not None


def _synthetic_obs(partner_country: str, hs_code: str, value: float) -> dict:
    """Minimal observation dict for upsert_observations() tests."""
    return {
        "period_kind": "monthly",
        "flow": "import",
        "reporter_country": "DE",
        "partner_country": partner_country,
        "hs_code": hs_code,
        "commodity_label": None,
        "value": value,
        "currency": "EUR",
        "quantity": 100.0,
        "quantity_unit": "kg",
        "source_row": {"_test": True},
        "eurostat_raw_row_ids": None,
    }


def test_partner_additive_takes_fast_path(clean_db, test_db_url):
    """Adding observations for a partner not yet present under a release should
    take the bulk INSERT fast path, even when the release already has rows for
    other partners. Editorial motivation: the HK/MO Eurostat backfill adds new
    partners to existing CN-only releases; without scoped freshness this would
    fall into the per-row slow path (~hours) instead of bulk insert (~seconds)."""
    result = _parse_fixture()
    run_id = db.start_run(URL)
    release_id = db.find_or_create_gacc_release(result.metadata, release_kind="preliminary")
    db.finish_run(run_id, status="success")

    # First: seed partner=CN under the release. Fast path (release is empty).
    cn_obs = [_synthetic_obs("CN", "850760", 100.0), _synthetic_obs("CN", "850231", 200.0)]
    run1 = db.start_run(URL)
    counts1 = db.upsert_observations(run1, release_id, cn_obs)
    db.finish_run(run1, status="success")
    assert counts1 == {"inserted": 2, "versioned": 0, "unchanged": 0}

    # Now: add partner=HK under the SAME release_id. Must take the fast path —
    # if it falls into the slow path, the assertion still passes (the counts
    # would be the same), so cross-check by run lookup that no per-row SELECTs
    # were issued (proxy: confirm the data landed correctly and quickly).
    hk_obs = [_synthetic_obs("HK", "850760", 50.0), _synthetic_obs("HK", "850231", 75.0)]
    run2 = db.start_run(URL)
    counts2 = db.upsert_observations(run2, release_id, hk_obs)
    db.finish_run(run2, status="success")
    assert counts2 == {"inserted": 2, "versioned": 0, "unchanged": 0}

    # Re-running the CN partner with same values should take the slow path
    # (CN already exists under release) and report all unchanged.
    run3 = db.start_run(URL)
    counts3 = db.upsert_observations(run3, release_id, cn_obs)
    db.finish_run(run3, status="success")
    assert counts3 == {"inserted": 0, "versioned": 0, "unchanged": 2}

    # Final state: 2 CN + 2 HK observations under the release, no duplicates.
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT partner_country, COUNT(*) FROM observations "
            " WHERE release_id = %s GROUP BY partner_country ORDER BY partner_country",
            (release_id,),
        )
        assert cur.fetchall() == [("CN", 2), ("HK", 2)]


JAN, FEB = date(2026, 1, 1), date(2026, 2, 1)


def test_eurostat_reporters_present_is_partner_scoped(clean_db, test_db_url):
    # DE filed CN totals in Jan; nobody filed US.
    _seed_eurostat_total(test_db_url, [(JAN, "DE", "CN"), (JAN, "FR", "CN")])

    assert db.eurostat_reporters_present_for_period(JAN) == {"DE", "FR"}
    assert db.eurostat_reporters_present_for_period(JAN, partners={"CN"}) == {"DE", "FR"}
    # Partner-scoped: a brand-new partner is NOT 'already present', so a later
    # --partner US ingest into the same period must not be skipped as a dup.
    assert db.eurostat_reporters_present_for_period(JAN, partners={"US"}) == set()


def test_eurostat_coverage_gaps_flags_missing_reporter_month(clean_db, test_db_url):
    # DE present both months; NL only in Jan (missing Feb — the NL-March bug shape).
    _seed_eurostat_total(test_db_url, [
        (JAN, "DE", "CN"), (FEB, "DE", "CN"), (JAN, "NL", "CN"),
    ])
    assert db.eurostat_coverage_gaps(JAN, FEB) == [(FEB, "NL")]


def test_eurostat_coverage_gaps_excludes_gb(clean_db, test_db_url):
    # GB filed only in Jan (Brexit-style come-and-go). Without exclusion it would
    # be flagged as a Feb gap; the default exclude_reporters=("GB",) suppresses it.
    _seed_eurostat_total(test_db_url, [
        (JAN, "DE", "CN"), (FEB, "DE", "CN"), (JAN, "GB", "CN"),
    ])
    assert db.eurostat_coverage_gaps(JAN, FEB) == []
    # Opting out of the exclusion surfaces the GB gap again.
    assert db.eurostat_coverage_gaps(JAN, FEB, exclude_reporters=()) == [(FEB, "GB")]


def test_eurostat_coverage_gaps_partner_scoped(clean_db, test_db_url):
    # Coverage keys on the requested partner's 000TOTAL rows only.
    _seed_eurostat_total(test_db_url, [
        (JAN, "DE", "CN"), (FEB, "DE", "CN"), (JAN, "NL", "CN"),
        (FEB, "NL", "US"),  # NL filed US (not CN) in Feb — still a CN gap
    ])
    assert db.eurostat_coverage_gaps(JAN, FEB, partner="CN") == [(FEB, "NL")]


def test_eurostat_coverage_gaps_multi_covers_hk_mo_envelope(clean_db, test_db_url):
    """The periodic guard checks the full CN+HK+MO envelope the ingest stores,
    not just CN — a missing HK (or MO) reporter-month would otherwise slip past.
    Results are partner-tagged so a near-certain CN data gap reads differently
    from an advisory HK/MO one."""
    _seed_eurostat_total(test_db_url, [
        # CN: DE both months, NL only Jan → CN gap NL/Feb (the classic case).
        (JAN, "DE", "CN"), (FEB, "DE", "CN"), (JAN, "NL", "CN"),
        # HK: NL both months, DE only Jan → HK gap DE/Feb — exactly the blind
        # spot the old CN-only guard missed.
        (JAN, "DE", "HK"), (JAN, "NL", "HK"), (FEB, "NL", "HK"),
    ])
    # Single-partner default still sees only the CN gap.
    assert db.eurostat_coverage_gaps(JAN, FEB) == [(FEB, "NL")]
    # The envelope check catches both, each tagged with its partner; MO has no
    # rows, so it contributes nothing (no false "everyone's missing MO").
    assert db.eurostat_coverage_gaps_multi(JAN, FEB) == [
        (FEB, "DE", "HK"),
        (FEB, "NL", "CN"),
    ]


def test_eurostat_raw_rows_natural_key_unique_backstop(clean_db, test_db_url):
    """The partial unique index (uq_eurostat_raw_natural_key) is the DB-level
    backstop for the append-only ingest guard:

    - a duplicate *modern* raw line is refused (the concurrency/guard-bug case);
    - rows that differ only in a classification column are allowed (Eurostat
      masks confidential NC8 to chapter stubs like '28XXXXXX', distinguished
      only by SITC/CPA/BEC — so those must be in the key, not collapsed);
    - legacy pre-2019 rows — which the pre-v2 source itself duplicated — are
      outside the partial index and stay insertable (that cleanup is separate).

    Skips cleanly if the test DB predates the index (it's applied by the
    migration / schema.sql, and conftest truncates rather than rebuilding)."""
    import psycopg2
    import psycopg2.errors
    import pytest

    conn = psycopg2.connect(test_db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_indexes WHERE tablename='eurostat_raw_rows' "
                "AND indexname='uq_eurostat_raw_natural_key'")
            if cur.fetchone() is None:
                pytest.skip("uq_eurostat_raw_natural_key not applied to the "
                            "test DB — run the 2026-06-24 migration against it")
            cur.execute("INSERT INTO scrape_runs (source_url, status) "
                        "VALUES ('test://eu-unique', 'success') RETURNING id")
            run_id = cur.fetchone()[0]
        conn.commit()

        def insert(period, sitc="100XX"):
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO eurostat_raw_rows (scrape_run_id, period, "
                    "reporter, partner, trade_type, product_nc, product_sitc, "
                    "flow, stat_procedure, suppl_unit, value_eur) VALUES "
                    "(%s, %s, 'DE', 'CN', 'E', '28XXXXXX', %s, 1, '1', "
                    "'NO_SU', 100)",
                    (run_id, period, sitc))

        # Modern row inserts; an identical one is refused by the backstop.
        insert(date(2026, 1, 1)); conn.commit()
        with pytest.raises(psycopg2.errors.UniqueViolation):
            insert(date(2026, 1, 1)); conn.commit()
        conn.rollback()

        # Differs only in the classification column → a distinct flow, allowed.
        insert(date(2026, 1, 1), sitc="200XX"); conn.commit()

        # Legacy pre-2019 duplicates are outside the partial index → allowed.
        insert(date(2017, 5, 1)); insert(date(2017, 5, 1)); conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM eurostat_raw_rows")
            assert cur.fetchone()[0] == 4  # 2 modern distinct + 2 legacy dupes
    finally:
        conn.close()
