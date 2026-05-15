"""Tests for llm_rejection_log — Layer 1 audit table for verifier hits."""
from __future__ import annotations

import psycopg2
import pytest

import llm_rejection_log


def test_log_rejection_rejects_invalid_stage(clean_db):
    with pytest.raises(ValueError, match="stage must be one of"):
        llm_rejection_log.log_rejection(
            scrape_run_id=None,
            cluster_name="Some group",
            model="qwen3.6:latest",
            stage="not_a_real_stage",  # type: ignore[arg-type]
            reason="whatever",
        )


def test_log_rejection_inserts_full_row(clean_db, test_db_url):
    rid = llm_rejection_log.log_rejection(
        scrape_run_id=None,
        cluster_name="Natural graphite (HS 250410)",
        model="qwen3.6:latest",
        stage="validate",
        reason="rationale_failed_verification",
        detail="base_effect: 12 (parsed 12.0000)",
        raw_output='{"anomaly_summary": "...", "hypotheses": [...]}',
    )
    assert rid > 0

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT cluster_name, stage, reason, detail, raw_output "
            "FROM llm_rejection_log WHERE id = %s",
            (rid,),
        )
        row = cur.fetchone()
    assert row[0] == "Natural graphite (HS 250410)"
    assert row[1] == "validate"
    assert row[2] == "rationale_failed_verification"
    assert "base_effect" in row[3]
    assert "anomaly_summary" in row[4]


def test_recent_rejections_returns_newest_first(clean_db):
    llm_rejection_log.log_rejection(
        scrape_run_id=None, cluster_name="first", model="m",
        stage="parse", reason="json_parse_error",
    )
    llm_rejection_log.log_rejection(
        scrape_run_id=None, cluster_name="second", model="m",
        stage="validate", reason="rationale_failed_verification",
    )
    rows = llm_rejection_log.recent_rejections(limit=5)
    assert [r.cluster_name for r in rows] == ["second", "first"]


def test_render_rejections_truncates_long_output(clean_db):
    long_text = "x" * 500
    llm_rejection_log.log_rejection(
        scrape_run_id=None, cluster_name="Group A", model="qwen3.6",
        stage="validate", reason="rationale_failed_verification",
        detail="base_effect: 12", raw_output=long_text,
    )
    rendered = llm_rejection_log.render_rejections(
        llm_rejection_log.recent_rejections()
    )
    assert "Group A" in rendered
    assert "..." in rendered  # truncated
    assert "x" * 500 not in rendered  # the full long string isn't displayed


def test_render_rejections_handles_empty(clean_db):
    assert "no LLM rejections" in llm_rejection_log.render_rejections([])
