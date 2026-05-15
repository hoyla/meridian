"""Tests for findings_emit_log — Layer 1 audit table for analyser invocations."""
from __future__ import annotations

import psycopg2

import findings_emit_log


def test_log_run_inserts_row_with_jsonb_counts(clean_db, test_db_url):
    counts = {
        "emitted": 240, "inserted_new": 0, "confirmed_existing": 230,
        "superseded": 10, "skipped_insufficient_history": 5,
    }
    rid = findings_emit_log.log_run(
        scrape_run_id=None,
        analyser_method="hs_group_yoy_v11_per_reporter_breakdown",
        subkind="hs_group_yoy",
        counts=counts,
        comparison_scope="eu_27",
        flow=1,
        duration_ms=15_000,
    )
    assert rid > 0
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT subkind, comparison_scope, flow, counts, duration_ms "
            "FROM findings_emit_log WHERE id = %s",
            (rid,),
        )
        row = cur.fetchone()
    assert row[0] == "hs_group_yoy"
    assert row[1] == "eu_27"
    assert row[2] == 1
    assert row[3]["superseded"] == 10
    assert row[4] == 15_000


def test_recent_runs_newest_first(clean_db):
    findings_emit_log.log_run(
        scrape_run_id=None, analyser_method="mirror_trade_v5",
        subkind="mirror_gap", counts={"emitted": 5},
    )
    findings_emit_log.log_run(
        scrape_run_id=None, analyser_method="partner_share_v1",
        subkind="partner_share", counts={"emitted": 12},
    )
    rows = findings_emit_log.recent_runs(limit=5)
    assert rows[0].subkind == "partner_share"
    assert rows[1].subkind == "mirror_gap"


def test_render_runs_shows_nonzero_counts_only(clean_db):
    findings_emit_log.log_run(
        scrape_run_id=None, analyser_method="x", subkind="hs_group_yoy",
        counts={"emitted": 10, "superseded": 0, "skipped_below_threshold": 5},
        comparison_scope="eu_27", flow=2,
    )
    out = findings_emit_log.render_runs(findings_emit_log.recent_runs())
    assert "emitted=10" in out
    assert "skipped_below_threshold=5" in out
    # Zero-valued keys are suppressed.
    assert "superseded=0" not in out


def test_render_runs_handles_empty(clean_db):
    assert "no analyser invocations" in findings_emit_log.render_runs([])
