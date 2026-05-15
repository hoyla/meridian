"""Tests for periodic_run_log — Layer 1 audit table for `--periodic-run`."""
from __future__ import annotations

from datetime import date

import psycopg2

import periodic_run_log


def test_log_run_inserts_noop_row(clean_db, test_db_url):
    rid = periodic_run_log.log_run(
        action_taken=False,
        reason="data_period 2026-02-01 already published",
        data_period=date(2026, 2, 1),
        findings_path=None,
        analyser_counts=None,
        duration_ms=42,
    )
    assert rid > 0
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT action_taken, reason, data_period, findings_path, "
            "analyser_counts, duration_ms FROM periodic_run_log WHERE id = %s",
            (rid,),
        )
        row = cur.fetchone()
    assert row[0] is False
    assert "already published" in row[1]
    assert row[2] == date(2026, 2, 1)
    assert row[3] is None
    assert row[4] is None
    assert row[5] == 42


def test_log_run_persists_analyser_counts_as_jsonb(clean_db, test_db_url):
    counts = {
        "mirror_trade": {"emitted": 5, "superseded": 1},
        "hs_group_yoy_eu_27_flow1": {"emitted": 240},
        "llm_framing": {"emitted": 31, "skipped_unverified": 2},
    }
    rid = periodic_run_log.log_run(
        action_taken=True,
        reason="new export written for data_period 2026-03-01",
        data_period=date(2026, 3, 1),
        findings_path="/exports/2026-05-15-1811/03_Findings.md",
        analyser_counts=counts,
        duration_ms=120_000,
    )
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT analyser_counts FROM periodic_run_log WHERE id = %s",
            (rid,),
        )
        stored = cur.fetchone()[0]
    # psycopg2 returns JSONB as a Python dict
    assert stored["mirror_trade"]["emitted"] == 5
    assert stored["llm_framing"]["skipped_unverified"] == 2


def test_recent_cycles_newest_first(clean_db):
    periodic_run_log.log_run(
        action_taken=False, reason="no Eurostat data", data_period=None,
        findings_path=None,
    )
    periodic_run_log.log_run(
        action_taken=True, reason="new export", data_period=date(2026, 2, 1),
        findings_path="/exports/.../03_Findings.md",
    )
    rows = periodic_run_log.recent_cycles(limit=5)
    assert rows[0].action_taken is True
    assert rows[1].action_taken is False


def test_render_cycles_marks_noop_and_export(clean_db):
    periodic_run_log.log_run(
        action_taken=False, reason="no fresher Eurostat", data_period=date(2026, 2, 1),
        findings_path=None,
    )
    periodic_run_log.log_run(
        action_taken=True, reason="wrote export", data_period=date(2026, 3, 1),
        findings_path="/exports/X/03_Findings.md",
    )
    out = periodic_run_log.render_cycles(periodic_run_log.recent_cycles())
    assert "WROTE EXPORT" in out
    assert "no-op" in out


def test_render_cycles_handles_error(clean_db):
    periodic_run_log.log_run(
        action_taken=False, reason="orchestrator crashed", data_period=None,
        findings_path=None, error="psycopg2.OperationalError",
    )
    out = periodic_run_log.render_cycles(periodic_run_log.recent_cycles())
    assert "ERROR" in out
    assert "OperationalError" in out


def test_render_cycles_handles_empty(clean_db):
    assert "no periodic-run cycles" in periodic_run_log.render_cycles([])
