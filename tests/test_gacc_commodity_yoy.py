"""Tests for detect_gacc_commodity_yoy — the section-5/6 headline-commodity
analyser (dev_notes/2026-07-14-gacc-commodity-highlights.md).

Covered: the single-month prior-YTD-differencing arithmetic (value + quantity),
the January direct-prior special case, the same-page cumulative YoY with its
published-pct cross-check (hard gate on structural divergence, tolerance
propagated from printed-cell rounding for small values), the Jan-Feb combined
anchor skip, the aggregate/leaf label-collision split (the 2021 "Machine
tools*" incident), and the emission threshold.

DB-backed — skips unless GACC_TEST_DATABASE_URL is set (see conftest)."""
from __future__ import annotations

import json
from datetime import date

import psycopg2
import pytest

import anomalies


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


def _seed_commodity_page(conn, period: date, rows: list[dict],
                         section: int = 5, jan_feb: bool = False):
    """Insert one canonical-CNY section-5/6 release + its commodity
    observations, mirroring what the parser stores. Each row dict:
      label, monthly=(qty,val)|None, ytd=(qty,val), prior=(qty,val),
      published=(qty_pct,val_pct), is_aggregate=False, quantity_unit='Ton'
    jan_feb=True stores the ytd pair under period_kind='cumulative_jan_feb'
    with the combined release_kind (no monthly pair), as the parser does."""
    flow = "export" if section == 5 else "import"
    cur = conn.cursor()
    kind = "preliminary_jan_feb" if jan_feb else "preliminary"
    cur.execute(
        """
        INSERT INTO releases (source, section_number, currency, period, release_kind,
                              source_url, unit, title, description)
        VALUES ('gacc', %s, 'CNY', %s, %s, %s, 'CNY 100 Million', 't', 'd')
        RETURNING id
        """,
        (section, period, kind,
         f"http://example/gacc-s{section}-{period.isoformat()}.html"),
    )
    rel_id = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'success') RETURNING id",
        (f"http://example/gacc-s{section}-{period.isoformat()}.html",),
    )
    run_id = cur.fetchone()[0]

    for row in rows:
        prior_q, prior_v = row.get("prior", (None, None))
        pub_q, pub_v = row.get("published", (None, None))
        qunit = row.get("quantity_unit", "Ton")
        base_src = {
            "raw_label": row["label"],
            "is_aggregate": row.get("is_aggregate", False),
            "quantity_unit": qunit,
            "published_yoy_quantity_pct": pub_q,
            "published_yoy_value_pct": pub_v,
        }
        if jan_feb:
            q, v = row["ytd"]
            src = dict(base_src,
                       cumulative_quantity=q, cumulative_value=v,
                       prior_year_cumulative_quantity=prior_q,
                       prior_year_cumulative_value=prior_v)
            cur.execute(
                "INSERT INTO observations (release_id, scrape_run_id, period_kind, flow, "
                "commodity_label, quantity, quantity_unit, value_amount, value_currency, source_row) "
                "VALUES (%s, %s, 'cumulative_jan_feb', %s, %s, %s, %s, %s, 'CNY', %s)",
                (rel_id, run_id, flow, row["label"], q, qunit, v, json.dumps(src)),
            )
        else:
            y_q, y_v = row["ytd"]
            src = dict(base_src,
                       ytd_quantity=y_q, ytd_value=y_v,
                       prior_year_ytd_quantity=prior_q,
                       prior_year_ytd_value=prior_v)
            if row.get("monthly"):
                m_q, m_v = row["monthly"]
                src.update(monthly_quantity=m_q, monthly_value=m_v)
                cur.execute(
                    "INSERT INTO observations (release_id, scrape_run_id, period_kind, flow, "
                    "commodity_label, quantity, quantity_unit, value_amount, value_currency, source_row) "
                    "VALUES (%s, %s, 'monthly', %s, %s, %s, %s, %s, 'CNY', %s)",
                    (rel_id, run_id, flow, row["label"], m_q, qunit, m_v, json.dumps(src)),
                )
            cur.execute(
                "INSERT INTO observations (release_id, scrape_run_id, period_kind, flow, "
                "commodity_label, quantity, quantity_unit, value_amount, value_currency, source_row) "
                "VALUES (%s, %s, 'ytd', %s, %s, %s, %s, %s, 'CNY', %s)",
                (rel_id, run_id, flow, row["label"], y_q, qunit, y_v, json.dumps(src)),
            )
    cur.execute(
        "INSERT INTO fx_rates (currency_from, currency_to, rate_date, rate, rate_source, "
        "rate_source_url, notes) VALUES ('CNY', 'EUR', %s, 0.125, 'test', 'http://example/fx', 't') "
        "ON CONFLICT (currency_from, currency_to, rate_date, rate_source) DO NOTHING",
        (period,),
    )
    conn.commit()


def _live_findings(test_db_url, subkind="gacc_commodity_yoy"):
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT title, detail FROM findings "
            "WHERE subkind=%s AND superseded_at IS NULL ORDER BY id",
            (subkind,),
        )
        return cur.fetchall()


def test_single_month_yoy_via_adjacent_page_differencing(empty_op_tables, test_db_url):
    """The cars register: May monthly value vs (prior 1-5) − (prior 1-4),
    both prior cumulatives read off current-era pages. Quantity analogous.
    Cumulative YoY cross-checks against the published pct (here consistent:
    500 vs 400 = +25.0%)."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_commodity_page(conn, date(2026, 4, 1), [
            {"label": "Widgets", "monthly": (10.0, 100.0), "ytd": (40.0, 400.0),
             "prior": (30.0, 300.0), "published": (33.3, 33.3)},
        ])
        _seed_commodity_page(conn, date(2026, 5, 1), [
            {"label": "Widgets", "monthly": (14.0, 130.0), "ytd": (54.0, 530.0),
             "prior": (40.0, 400.0), "published": (35.0, 32.5)},
        ])

    counts = anomalies.detect_gacc_commodity_yoy(flow="export")
    assert counts["skipped_crosscheck_divergent"] == 0, counts
    assert counts["emitted"] == 2, counts  # April + May anchors

    may = [d for _, d in _live_findings(test_db_url)
           if d["windows"]["current_end"] == "2026-05-01"]
    assert len(may) == 1
    sm = may[0]["totals"]["single_month"]
    # prior May value = 400 - 300 = 100; monthly 130 → +30%
    assert sm["prior_value_cny"] == pytest.approx(100.0)
    assert sm["value_yoy_pct"] == pytest.approx(0.30)
    # prior May qty = 40 - 30 = 10; monthly 14 → +40%
    assert sm["prior_quantity"] == pytest.approx(10.0)
    assert sm["quantity_yoy_pct"] == pytest.approx(0.40)
    assert sm["prior_derivation"] == "prior_ytd_adjacent_page_difference"
    ytd = may[0]["totals"]["ytd_cumulative"]
    # 530 vs 400 = +32.5%, agreeing with published 32.5
    assert ytd["value_yoy_pct"] == pytest.approx(0.325)
    assert ytd["crosscheck"] == "agreed"
    # EUR display level: 130 CNY-100M × 1e8 × 0.125 = 1.625bn
    assert may[0]["totals"]["eur_month"] == pytest.approx(130e8 * 0.125)
    assert "cny_denominated" in may[0]["caveat_codes"]


def test_january_anchor_uses_prior_cumulative_directly(empty_op_tables, test_db_url):
    """A January page's prior-year 1-1 cumulative IS prior January — no
    differencing, no dependence on a December page existing."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_commodity_page(conn, date(2020, 1, 1), [
            {"label": "Widgets", "monthly": (5.0, 50.0), "ytd": (5.0, 50.0),
             "prior": (4.0, 40.0), "published": (25.0, 25.0)},
        ])
    counts = anomalies.detect_gacc_commodity_yoy(flow="export")
    assert counts["emitted"] == 1, counts
    (_, detail), = _live_findings(test_db_url)
    sm = detail["totals"]["single_month"]
    assert sm["prior_derivation"] == "prior_ytd_january_direct"
    assert sm["prior_value_cny"] == pytest.approx(40.0)
    assert sm["value_yoy_pct"] == pytest.approx(0.25)


def test_crosscheck_divergence_blocks_emission(empty_op_tables, test_db_url):
    """A published pct that our recomputation can't reproduce (beyond the
    rounding-propagation tolerance) means a misread column or scale shift —
    the finding must not emit."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_commodity_page(conn, date(2026, 5, 1), [
            # 530 vs 400 = +32.5% but page claims +90% → structural mismatch
            {"label": "Widgets", "monthly": (14.0, 130.0), "ytd": (54.0, 530.0),
             "prior": (40.0, 400.0), "published": (35.0, 90.0)},
        ])
    counts = anomalies.detect_gacc_commodity_yoy(flow="export")
    assert counts["skipped_crosscheck_divergent"] == 1, counts
    assert counts["emitted"] == 0
    assert _live_findings(test_db_url) == []


def test_crosscheck_tolerance_scales_with_printed_rounding(empty_op_tables, test_db_url):
    """Small commodities legitimately diverge by tenths of a pp because GACC
    computes its pct on unrounded internals while the cells print at 0.1
    precision (the Rice-2019 shape). Recomputed (15.6 vs 15.2) = +2.63%;
    published 2.2% differs by 0.43pp yet must emit — the propagated
    tolerance for these magnitudes (~0.7pp) absorbs it."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_commodity_page(conn, date(2025, 3, 1), [
            {"label": "Grain", "monthly": (5.0, 5.2), "ytd": (15.0, 15.6),
             "prior": (14.0, 15.2), "published": (7.1, 2.2)},
        ])
    counts = anomalies.detect_gacc_commodity_yoy(flow="export")
    assert counts["skipped_crosscheck_divergent"] == 0, counts
    assert counts["emitted"] == 1, counts


def test_jan_feb_combined_anchor_skipped(empty_op_tables, test_db_url):
    """v1 emits no finding at a Jan-Feb combined anchor (the cumulative rows
    are stored, just not analysed — matching the build note)."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_commodity_page(conn, date(2025, 2, 1), [
            {"label": "Widgets", "ytd": (9.0, 90.0), "prior": (8.0, 80.0),
             "published": (12.5, 12.5)},
        ], jan_feb=True)
    counts = anomalies.detect_gacc_commodity_yoy(flow="export")
    assert counts["skipped_jan_feb_anchor"] == 1, counts
    assert counts["emitted"] == 0
    assert _live_findings(test_db_url) == []


def test_aggregate_and_leaf_sharing_a_label_stay_distinct(empty_op_tables, test_db_url):
    """The 2021 'Machine tools*' incident: a starred aggregate and a leaf
    with the same post-strip label must produce two findings with distinct
    natural keys (not a mixed series, not mutual supersession), the
    aggregate one carrying the catalogue_aggregate caveat."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_commodity_page(conn, date(2021, 3, 1), [
            {"label": "Machine tools", "is_aggregate": True,
             "monthly": (None, 5000.0), "ytd": (None, 16658.2),
             "prior": (None, 13646.0), "published": (None, 22.1)},
            {"label": "Machine tools", "is_aggregate": False,
             "monthly": (8000.0, 40.0), "ytd": (24431.0, 121.1),
             "prior": (14646.0, 100.1), "published": (66.8, 20.9),
             "quantity_unit": "Set"},
        ], section=6)
    counts = anomalies.detect_gacc_commodity_yoy(flow="import")
    assert counts["skipped_crosscheck_divergent"] == 0, counts
    assert counts["emitted"] == 2, counts

    found = _live_findings(test_db_url, subkind="gacc_commodity_yoy_import")
    assert len(found) == 2
    flags = sorted(d["commodity"]["is_aggregate"] for _, d in found)
    assert flags == [False, True]
    agg = next(d for _, d in found if d["commodity"]["is_aggregate"])
    leaf = next(d for _, d in found if not d["commodity"]["is_aggregate"])
    assert "catalogue_aggregate" in agg["caveat_codes"]
    assert "catalogue_aggregate" not in leaf["caveat_codes"]
    # each recomputes cleanly against its own published pct
    assert agg["totals"]["ytd_cumulative"]["crosscheck"] == "agreed"
    assert leaf["totals"]["ytd_cumulative"]["crosscheck"] == "agreed"

    # Re-run: both confirm in place; neither supersedes the other.
    counts2 = anomalies.detect_gacc_commodity_yoy(flow="import")
    assert counts2["confirmed_existing"] == 2, counts2
    assert counts2["superseded"] == 0, counts2


def test_threshold_filters_small_single_month_moves(empty_op_tables, test_db_url):
    with psycopg2.connect(test_db_url) as conn:
        _seed_commodity_page(conn, date(2026, 4, 1), [
            {"label": "Widgets", "monthly": (10.0, 100.0), "ytd": (40.0, 400.0),
             "prior": (30.0, 300.0), "published": (33.3, 33.3)},
        ])
        _seed_commodity_page(conn, date(2026, 5, 1), [
            # prior May = 400-300 = 100; monthly 102 → +2%
            {"label": "Widgets", "monthly": (10.0, 102.0), "ytd": (50.0, 502.0),
             "prior": (40.0, 400.0), "published": (25.0, 25.5)},
        ])
    counts = anomalies.detect_gacc_commodity_yoy(flow="export", yoy_threshold_pct=0.10)
    # April (+33% ytd-only, sm unavailable → always emitted) + May filtered
    assert counts["skipped_below_threshold"] == 1, counts


# ---------------------------------------------------------------------------
# Catalogue-rename aliases (2026-08-07). GACC ran "Integrated circuits" and
# "Electronic integrated circuits" in parallel on the English section-5/6
# pages for 2026-02/-03/-04 with byte-identical figures, then retired the old
# label from 2026-05. Without an alias the family's safe default (a rename
# starts a fresh series) splits one real series in two, losing the 12-month
# trend and any run-rate spanning the change.
# ---------------------------------------------------------------------------

def test_verified_rename_collapses_to_one_canonical_label():
    canon = anomalies._canonical_gacc_commodity_label
    assert canon("Integrated circuits") == "Electronic integrated circuits"
    # Section 6 capitalises differently; keys are casefolded.
    assert canon("Integrated Circuits") == "Electronic integrated circuits"
    # Already-canonical passes through unchanged (idempotent).
    assert (canon("Electronic integrated circuits")
            == "Electronic integrated circuits")


def test_unverified_labels_pass_through_untouched():
    # The map is an allow-list: a rename we haven't checked must get the safe
    # default (fresh series), never a guessed merge. And near-matching names
    # are exactly the collision the (label, is_aggregate) keying guards
    # against — they must not be collapsed.
    canon = anomalies._canonical_gacc_commodity_label
    for label in ("Machine tools", "Machine tools*", "Agricultural",
                  "Agriculture products", "Fertilizers", "Integrated"):
        assert canon(label) == label


def test_alias_conflict_is_refused_not_silently_merged(monkeypatch, caplog):
    """If the two aliased labels ever disagree for the same period, the alias
    premise is false and merging would fabricate a series. The loader must
    keep what it has and say so, rather than overwrite."""
    import logging
    from datetime import date as _date

    period = _date(2026, 3, 1)
    rows = [
        # canonical label first, then the aliased one with a DIFFERENT value
        (period, "monthly", "Electronic integrated circuits", 2023.8, 325.3,
         {"is_aggregate": False}, "CNY 100 Million", 1),
        (period, "monthly", "Integrated circuits", 9999.9, 111.1,
         {"is_aggregate": False}, "CNY 100 Million", 2),
    ]

    class _Cur:
        def execute(self, *a, **k): pass
        def fetchall(self): return rows
        def __enter__(self): return self
        def __exit__(self, *a): return False

    class _Conn:
        def cursor(self): return _Cur()
        def __enter__(self): return self
        def __exit__(self, *a): return False

    monkeypatch.setattr(anomalies, "_conn", lambda: _Conn())
    with caplog.at_level(logging.ERROR):
        hist = anomalies._gacc_commodity_history("export")

    key = ("Electronic integrated circuits", False)
    assert key in hist
    # The first (canonical) row survives; the conflicting one is refused.
    assert hist[key][period]["monthly"]["value"] == 2023.8
    assert "alias conflict" in caplog.text


def test_matching_aliased_rows_merge_without_complaint(monkeypatch, caplog):
    # The real-world case: both labels carry identical figures, so the merge
    # is a no-op on values and must stay silent.
    import logging
    from datetime import date as _date

    period = _date(2026, 3, 1)
    rows = [
        (period, "monthly", "Electronic integrated circuits", 2023.8, 325.3,
         {"is_aggregate": False}, "CNY 100 Million", 1),
        (period, "monthly", "Integrated circuits", 2023.8, 325.3,
         {"is_aggregate": False}, "CNY 100 Million", 2),
    ]

    class _Cur:
        def execute(self, *a, **k): pass
        def fetchall(self): return rows
        def __enter__(self): return self
        def __exit__(self, *a): return False

    class _Conn:
        def cursor(self): return _Cur()
        def __enter__(self): return self
        def __exit__(self, *a): return False

    monkeypatch.setattr(anomalies, "_conn", lambda: _Conn())
    with caplog.at_level(logging.ERROR):
        hist = anomalies._gacc_commodity_history("export")

    assert list(hist) == [("Electronic integrated circuits", False)]
    assert "alias conflict" not in caplog.text
