"""Held-back groups — the "analyse but hold back" staging tool.

A group whose `created_by` carries a hold prefix (`hidden:` = valid but staged,
`draft:` = methodology not yet validated) is still analysed and still listed —
flagged — in Sector detail, but kept OUT of the published rankings (the Standout
movers list and the Biggest-mover KPI). Promotion is just dropping the prefix.

The predicate test runs everywhere; the gate tests are DB-backed and skip
without GACC_TEST_DATABASE_URL.
"""
import json

import psycopg2
import psycopg2.extras
import pytest

import db


# ---- pure-Python predicate (always runs) --------------------------------

@pytest.mark.parametrize("created_by,expected", [
    ("hidden:reporter_2026_06", True),
    ("draft:methodology-pending", True),
    ("seed:reporter_request_2026_06", False),
    ("seed", False),
    ("", False),
    (None, False),
])
def test_is_held_created_by(created_by, expected):
    assert db.is_held_created_by(created_by) is expected


# ---- DB-backed gate (skips without a test DB) ---------------------------

_NORMAL = "ZZ test normal group"
_HIDDEN = "ZZ test hidden group"


def _seed_group(cur, name, created_by, patterns):
    cur.execute(
        "INSERT INTO hs_groups (name, description, hs_patterns, created_by) "
        "VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (name) DO UPDATE SET created_by = EXCLUDED.created_by",
        (name, "test fixture", patterns, created_by),
    )


def _seed_mover(cur, run, name, nk):
    # A big, clean import mover that clears the top-movers filters
    # (|yoy| over threshold, well over the min € base, not low-base).
    detail = {"group": {"name": name},
              "totals": {"yoy_pct": 0.80, "current_12mo_eur": 5e9,
                         "prior_12mo_eur": 2.78e9, "low_base": False},
              "windows": {"current_end": "2026-04-01"}}
    cur.execute(
        "INSERT INTO findings (scrape_run_id, kind, subkind, detail, "
        "natural_key_hash) VALUES (%s,'anomaly','hs_group_yoy',%s::jsonb,%s)",
        (run, json.dumps(detail), nk),
    )


def test_hidden_excluded_from_top_movers_but_flagged_in_sector_detail(
        clean_db, test_db_url):
    """A hidden group with a qualifying mover is (1) absent from the Standout
    movers, yet (2) still present in Sector detail, title-flagged + metrics.held."""
    from briefing_pack._helpers import _compute_top_movers
    from report_builder import build_report
    try:
        with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
            _seed_group(cur, _NORMAL, "seed:test", ["999991%"])
            _seed_group(cur, _HIDDEN, "hidden:test", ["999992%"])
            cur.execute("INSERT INTO scrape_runs (status, source_url) "
                        "VALUES ('success', 'test://seed') RETURNING id")
            run = cur.fetchone()[0]
            _seed_mover(cur, run, _NORMAL, "nk_norm")
            _seed_mover(cur, run, _HIDDEN, "nk_hidden")

        # (1) Standout movers: the normal group rides; the hidden one is gated out.
        with psycopg2.connect(test_db_url) as conn, conn.cursor(
                cursor_factory=psycopg2.extras.DictCursor) as cur:
            names = {m["group_name"] for m in _compute_top_movers(cur)}
        assert _NORMAL in names
        assert _HIDDEN not in names

        # (2) Sector detail still lists the hidden group, flagged.
        r = build_report(source_trigger="eurostat")
        sd = [s for s in r.sections if s.kind == "sector_detail"]
        titles = [g.title for g in sd[0].sections]
        assert any(t.startswith(_NORMAL) for t in titles)
        hidden_sec = next(g for g in sd[0].sections if g.title.startswith(_HIDDEN))
        assert "held back" in hidden_sec.title
        assert hidden_sec.metrics.get("held") is True
    finally:
        with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM hs_groups WHERE name IN (%s, %s)",
                        (_NORMAL, _HIDDEN))


def test_biggest_mover_excludes_cn8_whose_only_parent_is_held(
        clean_db, test_db_url):
    """A CN8 mover watched ONLY because a held group widened the prefixes must
    not win the Biggest-mover card (its sole parent is held → no candidate)."""
    from report_builder import _biggest_mover_indicator
    try:
        with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
            _seed_group(cur, _HIDDEN, "hidden:test", ["999992%"])
            cur.execute("INSERT INTO scrape_runs (status, source_url) "
                        "VALUES ('success', 'test://seed') RETURNING id")
            run = cur.fetchone()[0]
            detail = {"product": {"cn8": "99999201", "label_short": "held widget"},
                      "parent_groups": [_HIDDEN],
                      "totals": {"yoy_pct": 5.0, "current_12mo_eur": 9e9},
                      "windows": {"current_end": "2026-04-01"}}
            cur.execute(
                "INSERT INTO findings (scrape_run_id, kind, subkind, detail, "
                "natural_key_hash) VALUES (%s,'anomaly','cn8_yoy_mover',%s::jsonb,'nkc_h')",
                (run, json.dumps(detail)),
            )
        with psycopg2.connect(test_db_url) as conn, conn.cursor(
                cursor_factory=psycopg2.extras.DictCursor) as cur:
            indicator = _biggest_mover_indicator(cur, surfaced_groups=set())
        assert indicator is None
    finally:
        with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM hs_groups WHERE name = %s", (_HIDDEN,))
