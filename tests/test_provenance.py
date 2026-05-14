"""Smoke tests for `provenance.generate_for_finding`.

The deep correctness check is in `tests/test_release_currency_unit_consistency.py`
(arithmetic from raw observations → EUR matches the finding). These tests
exercise the file-emission shape: idempotency, supported_only filtering,
the stub fallback, and that the file actually contains the expected
finding id and source URLs.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import psycopg2
import pytest

import provenance


@pytest.fixture
def provenance_dir(tmp_path, monkeypatch) -> Path:
    """Redirect provenance writes to a tmp dir so tests don't pollute the repo."""
    target = tmp_path / "provenance"
    monkeypatch.setattr(provenance, "PROVENANCE_DIR", target)
    return target


def _seed_bilateral_aggregate(conn, finding_id: int = 999_001) -> int:
    """Insert a minimal release/observation/finding triplet — enough for the
    bilateral_aggregate_yoy renderer to produce a file. Returns the finding id.

    The renderer queries `monthly_series` from the finding's `detail` JSON to
    label EUR contributions, and the `observation_ids` array to look up the
    source release URLs. So we need a release, 1-2 observations on it, and a
    finding whose detail/observation_ids point at those obs."""
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scrape_runs (source_url, status)
            VALUES ('test://seed', 'success') RETURNING id
            """
        )
        run_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO releases (source, section_number, currency, period,
                                  release_kind, source_url, title, unit)
            VALUES ('gacc', 4, 'CNY', '2026-04-01', 'preliminary',
                    'http://english.customs.gov.cn/Statics/test-april.html',
                    '(4) China''s Total Export & Import Values by Country/Region, Apr 2026 (in CNY)',
                    'CNY 100 Million') RETURNING id
            """
        )
        release_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO observations (release_id, scrape_run_id, period_kind,
                                      flow, partner_country, partner_label_raw,
                                      value_amount, value_currency, source_row,
                                      version_seen)
            VALUES (%s, %s, 'monthly', 'export', 'Germany',
                    'of which: Germany', 770.5, 'CNY', '{}'::jsonb, 1) RETURNING id
            """,
            (release_id, run_id),
        )
        obs_id = cur.fetchone()[0]

        # ECB rate so the renderer's FX appendix has at least one row.
        cur.execute(
            """
            INSERT INTO fx_rates (currency_from, currency_to, rate_date, rate,
                                  rate_source, rate_source_url)
            VALUES ('CNY', 'EUR', '2026-04-01', 0.124993, 'ECB monthly average',
                    'https://data-api.ecb.europa.eu/test')
            ON CONFLICT DO NOTHING
            """
        )

        detail = {
            "method": "gacc_bilateral_aggregate_yoy_v1_eu_and_single_countries",
            "method_query": {"flow": "export", "partner_country_label": "Germany"},
            "partner": {"raw_label": "of which: Germany", "kind": "single_country"},
            "windows": {
                "current_start": "2025-05-01", "current_end": "2026-04-01",
                "prior_start": "2024-05-01", "prior_end": "2025-04-01",
            },
            "totals": {
                "current_12mo_eur": 9_630_684_197.30,
                "prior_12mo_eur": 9_092_214_134.96,
                "yoy_pct": 0.059,
                "partial_window": False,
                "missing_months_current": [],
                "missing_months_prior": [],
            },
            "monthly_series": [
                {"period": "2026-04-01", "value_eur": 9_630_684_197.30},
            ],
            "caveat_codes": ["partial_window"],
        }
        cur.execute(
            """
            INSERT INTO findings (id, scrape_run_id, kind, subkind, title, body,
                                  detail, observation_ids)
            VALUES (%s, %s, 'anomaly', 'gacc_bilateral_aggregate_yoy',
                    'GACC bilateral test finding', 'test body', %s, %s)
            """,
            (finding_id, run_id, json.dumps(detail), [obs_id]),
        )
    return finding_id


def test_bilateral_generator_writes_a_file_with_source_urls(
    clean_db, db_conn, provenance_dir,
):
    finding_id = _seed_bilateral_aggregate(db_conn)
    path = provenance.generate_for_finding(finding_id)
    assert path is not None
    assert path.exists()
    text = path.read_text()
    assert f"finding/{finding_id}" in text
    assert "http://english.customs.gov.cn/Statics/test-april.html" in text
    assert "ECB" in text  # the FX appendix preamble
    assert "Germany imports from China" in text  # partner-side direction translation


def test_generator_is_idempotent(clean_db, db_conn, provenance_dir):
    finding_id = _seed_bilateral_aggregate(db_conn)
    p1 = provenance.generate_for_finding(finding_id)
    mtime_before = p1.stat().st_mtime_ns
    # Second call must not rewrite the file (the journalist-reading-an-old-
    # export contract: regenerating would un-freeze the snapshot).
    p2 = provenance.generate_for_finding(finding_id)
    assert p2 == p1
    assert p1.stat().st_mtime_ns == mtime_before


def test_force_rewrites_existing_file(clean_db, db_conn, provenance_dir):
    finding_id = _seed_bilateral_aggregate(db_conn)
    p1 = provenance.generate_for_finding(finding_id)
    mtime_before = p1.stat().st_mtime_ns
    p2 = provenance.generate_for_finding(finding_id, force=True)
    assert p2 == p1
    assert p2.stat().st_mtime_ns >= mtime_before


def test_supported_only_skips_unsupported_subkinds(clean_db, db_conn, provenance_dir):
    """A subkind without a dedicated renderer should return None under
    supported_only=True, leaving the bundle directory clean."""
    with db_conn, db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) "
            "VALUES ('test://seed-unsupported', 'success') RETURNING id"
        )
        run_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO findings (id, scrape_run_id, kind, subkind, title, detail)
            VALUES (888888, %s, 'anomaly', 'mirror_gap',
                    'Mirror-gap test finding', '{}'::jsonb)
            """,
            (run_id,),
        )
    result = provenance.generate_for_finding(888888, supported_only=True)
    assert result is None
    assert not (provenance_dir / "finding-888888.md").exists()


def test_supported_only_false_still_writes_a_stub(clean_db, db_conn, provenance_dir):
    """Without supported_only, an unknown subkind gets a stub file so the
    CLI flag (which a journalist invokes ad-hoc) always produces *something*."""
    with db_conn, db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) "
            "VALUES ('test://seed-stub', 'success') RETURNING id"
        )
        run_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO findings (id, scrape_run_id, kind, subkind, title, detail)
            VALUES (777777, %s, 'anomaly', 'mirror_gap',
                    'Stub-only finding', '{}'::jsonb)
            """,
            (run_id,),
        )
    path = provenance.generate_for_finding(777777)
    assert path is not None
    assert path.exists()
    text = path.read_text()
    assert "Detailed provenance generator pending" in text
    assert "mirror_gap" in text


def test_unknown_finding_id_raises(clean_db, provenance_dir):
    with pytest.raises(ValueError, match="No finding with id"):
        provenance.generate_for_finding(404_404)


def _seed_hs_group_yoy(conn, finding_id: int = 999_002) -> int:
    """Minimal hs_group_yoy finding (Eurostat scope, flow=1). The renderer
    queries `releases` for source URLs by source+period range, so we also
    insert one Eurostat release inside the window."""
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) "
            "VALUES ('test://seed-hs', 'success') RETURNING id"
        )
        run_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO releases (source, period, source_url)
            VALUES ('eurostat', '2025-06-01',
                    'https://ec.europa.eu/eurostat/api/dissemination/files?downfile=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_202506.7z')
            """
        )

        detail = {
            "method": "hs_group_yoy_v11_per_reporter_breakdown",
            "method_query": {
                "flow": 1, "sources": ["eurostat"],
                "partners": ["CN", "HK", "MO"],
                "comparison_scope": "eu_27", "rolling_window_months": 12,
                "hs_patterns": ["85044086%"],
            },
            "group": {
                "id": 49,
                "name": "Photovoltaic inverters (CN8 85044086)",
                "description": "Test description of PV inverters group.",
                "hs_patterns": ["85044086%"],
            },
            "windows": {
                "prior_start": "2024-01-01", "prior_end": "2024-12-01",
                "current_start": "2025-01-01", "current_end": "2025-12-01",
            },
            "totals": {
                "yoy_pct": 0.06, "yoy_pct_kg": 0.097,
                "current_12mo_eur": 2_028_652_255, "prior_12mo_eur": 1_913_700_366,
                "current_12mo_kg": 75_896_256, "prior_12mo_kg": 69_184_730,
                "kg_coverage_pct": 1.0, "kg_coverage_threshold": 0.8,
                "current_unit_price_eur_per_kg": 26.73,
                "prior_unit_price_eur_per_kg": 27.66,
                "unit_price_pct_change": -0.034,
                "missing_months_current": [], "missing_months_prior": [],
                "n_months_used_current": 12, "n_months_used_prior": 12,
                "low_base": False, "partial_window": False,
            },
            "monthly_series": [],
            "caveat_codes": [],
            "per_reporter_breakdown": [
                {"reporter": "NL", "current_eur": 1_226_290_742,
                 "prior_eur": 1_056_888_513, "yoy_pct": 0.16,
                 "share_of_group_delta_pct": 1.47},
            ],
            "top_cn8_codes_in_current_12mo": [
                {"product_nc": "85044086", "total_eur": 2_028_652_255,
                 "total_kg": 75_896_256, "n_raw": 350},
            ],
        }
        cur.execute(
            """
            INSERT INTO findings (id, scrape_run_id, kind, subkind, title, body,
                                  detail, observation_ids)
            VALUES (%s, %s, 'anomaly', 'hs_group_yoy',
                    'PV inverters test finding', 'test body', %s, '{}')
            """,
            (finding_id, run_id, json.dumps(detail)),
        )
    return finding_id


def test_hs_group_yoy_renderer_emits_group_definition_and_source_urls(
    clean_db, db_conn, provenance_dir,
):
    """The HS-group-yoy renderer must (a) surface the group definition
    prominently — Luke's 2026-05-13 pack-review note explicitly flagged
    that journalists need this — and (b) list per-period Eurostat bulk
    file URLs as the source attribution chain."""
    finding_id = _seed_hs_group_yoy(db_conn)
    path = provenance.generate_for_finding(finding_id)
    assert path is not None
    text = path.read_text()
    # Group definition is in the "What's in this HS group" section.
    assert "What's in this HS group" in text
    assert "Photovoltaic inverters" in text
    assert "Test description of PV inverters group." in text
    assert "`85044086%`" in text
    # Per-period Eurostat bulk URL appears via the friendly filename label.
    assert "full_v2_202506.7z" in text
    # Directionality plain-English present.
    assert "EU-27 (Eurostat) imports from China" in text
