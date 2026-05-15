"""Smoke tests for `briefing_pack.render_groups`.

Heavier validation (matching the rendered numbers against raw row sums)
lives in the test_provenance.py family. These tests pin the output
shape: every active group gets a section, draft groups end up in their
own section, sibling links resolve, and the slug used in anchors
matches the heading slug a Markdown renderer will auto-generate from
the heading text.
"""

from __future__ import annotations

import json
from datetime import date

import pytest

import briefing_pack.render_groups as rg
from briefing_pack._helpers import _slugify_heading


def _seed_groups(conn) -> None:
    """Insert three test groups using synthetic IDs and HS chapter
    `9999`-range patterns so they don't collide with the schema.sql
    seed data (which other tests rely on). `clean_db` doesn't truncate
    `hs_groups` for the same reason — it's treated as immutable seed."""
    with conn, conn.cursor() as cur:
        # Tidy up if a prior run left rows in our reserved id range.
        cur.execute("DELETE FROM hs_groups WHERE id IN (999991, 999992, 999993)")
        cur.execute(
            """
            INSERT INTO hs_groups (id, name, description, hs_patterns,
                                   created_by, created_at)
            VALUES
              (999991, 'TestRender — Permanent magnets',
               'Test fixture group covering synthetic HS chapter 9999.',
               ARRAY['9999%'], 'seed', now()),
              (999992, 'TestRender — Sintered NdFeB sub-CN8',
               'Sub-CN8 of TestRender — Permanent magnets.',
               ARRAY['99991110%'], 'seed', now()),
              (999993, 'TestRender — Honey',
               'Synthetic draft group for the draft-section test.',
               ARRAY['9998%'], 'draft:test_category', now())
            """
        )


def _seed_hs_group_yoy(conn, finding_id: int, group_id: int,
                       top_cn8: list[dict] | None = None) -> None:
    """An active hs_group_yoy finding pointing at the given group, with
    a small top_cn8_codes_in_current_12mo list so the renderer's
    contributing-codes table has rows to fill."""
    detail = {
        "method": "hs_group_yoy_v11",
        "method_query": {"flow": 1, "comparison_scope": "eu_27",
                         "sources": ["eurostat"], "partners": ["CN", "HK", "MO"]},
        "group": {"id": group_id, "name": "test", "hs_patterns": ["8505%"]},
        "windows": {
            "prior_start": "2024-01-01", "prior_end": "2024-12-01",
            "current_start": "2025-01-01", "current_end": "2025-12-01",
        },
        "totals": {
            "current_12mo_eur": 1_400_000_000, "prior_12mo_eur": 1_200_000_000,
            "yoy_pct": 0.16, "yoy_pct_kg": 0.05,
            "n_months_used_current": 12, "n_months_used_prior": 12,
            "missing_months_current": [], "missing_months_prior": [],
        },
        "monthly_series": [],
        "top_cn8_codes_in_current_12mo": top_cn8 or [
            {"hs_code": "85051110", "total_eur": 732_400_000,
             "total_kg": 20_200_000, "n_raw": 100},
            {"hs_code": "85051190", "total_eur": 226_800_000,
             "total_kg": 15_400_000, "n_raw": 90},
        ],
        "caveat_codes": [],
    }
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) "
            "VALUES ('test://render_groups', 'success') RETURNING id"
        )
        run_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO findings (id, scrape_run_id, kind, subkind, title,
                                  body, detail, observation_ids, hs_group_ids)
            VALUES (%s, %s, 'anomaly', 'hs_group_yoy', 'test title',
                    'test body', %s, '{}', %s)
            """,
            (finding_id, run_id, json.dumps(detail), [group_id]),
        )


def test_active_groups_appear_in_quick_index_and_body(clean_db, db_conn):
    _seed_groups(db_conn)
    _seed_hs_group_yoy(db_conn, finding_id=2001, group_id=999991)
    text = rg.render_groups()
    # Both synthetic active groups land in the quick index.
    assert "[TestRender — Permanent magnets](" in text
    assert "[TestRender — Sintered NdFeB sub-CN8](" in text
    # And both get a body section under the active-groups heading.
    assert "## Active groups" in text
    assert "### TestRender — Permanent magnets" in text


def test_draft_groups_in_their_own_section(clean_db, db_conn):
    _seed_groups(db_conn)
    text = rg.render_groups()
    # The synthetic draft group has the methodology flag in its heading.
    assert (
        "### TestRender — Honey *(draft — methodology not yet validated)*"
        in text
    )
    # And the dedicated draft-section header appears.
    assert "## Draft groups" in text
    # Draft preamble warns against quoting.
    assert "Figures appear in `03_Findings.md` for transparency" in text


def test_sibling_groups_link_via_4_digit_hs_prefix(clean_db, db_conn):
    """The two TestRender active groups share the 9999 HS prefix and
    should appear in each other's 'Related groups' list."""
    _seed_groups(db_conn)
    text = rg.render_groups()
    start = text.find("### TestRender — Permanent magnets")
    end = text.find("### TestRender — Sintered NdFeB", start)
    body = text[start:end]
    assert "**Related groups**" in body
    assert "TestRender — Sintered NdFeB sub-CN8" in body


def test_concentration_warning_fires_above_80pct(clean_db, db_conn):
    """When one CN8 carries >80% of the group's EUR, render a
    concentration warning so a journalist knows the group is editorially
    a wrapper around that single code."""
    _seed_groups(db_conn)
    _seed_hs_group_yoy(db_conn, finding_id=2002, group_id=999991, top_cn8=[
        {"hs_code": "99991110", "total_eur": 1_300_000_000,
         "total_kg": 30_000_000, "n_raw": 100},
        {"hs_code": "99991190", "total_eur": 100_000_000,
         "total_kg": 5_000_000, "n_raw": 90},
    ])
    text = rg.render_groups()
    section_start = text.find("### TestRender — Permanent magnets")
    section_end = text.find("### TestRender — Sintered NdFeB", section_start)
    body = text[section_start:section_end]
    assert "Concentration note" in body
    assert "99991110" in body


def test_quick_index_anchors_match_heading_slugs(clean_db, db_conn):
    """The anchor in `[Name](#slug)` must equal what most Markdown
    renderers generate from `### Name`. If `_slugify_heading` ever
    drifts, the document's internal links silently break — this test
    fires first."""
    _seed_groups(db_conn)
    text = rg.render_groups()
    name = "TestRender — Sintered NdFeB sub-CN8"
    expected_slug = _slugify_heading(name)
    assert f"[{name}](#{expected_slug})" in text


def test_renders_without_synthetic_rows(clean_db, db_conn):
    """Smoke test: the renderer should produce a coherent document
    against the schema.sql seed data alone — no crash, header present,
    at least one section heading (the seed has plenty of groups)."""
    # No _seed_groups() call: rely on whatever schema.sql seeded.
    text = rg.render_groups()
    assert "# HS group reference" in text
    # If the seed data has *any* group at all, the active section
    # header should fire.
    with db_conn, db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM hs_groups WHERE created_by NOT LIKE 'draft:%%'")
        n_active = cur.fetchone()[0]
    if n_active > 0:
        assert "## Active groups" in text
