"""Tier 1's historic-correction collapse (2026-07-05): a source backfill
can materially re-state hundreds of OLD-window findings in one supersede
pass (first hit: the Jan–Feb 2020 GACC backfill rippling through 720
findings). Those are preserved revisions, not this cycle's news — so
`_compute_diff` lists only recent-window shifts as line items and collapses
the older ones into a counted, explained line, on every surface (Tier 1
markdown, front-page digest, portal What's-changed)."""

from __future__ import annotations

from datetime import date, datetime

import psycopg2
import psycopg2.extras
import pytest

import report_model as rm
from briefing_pack.sections.diff import _compute_diff
from briefing_pack.sections.front_page import _since_last_pack_lines
from report_render_html import _what_changed as _what_changed_html


@pytest.fixture
def fresh_db(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE TABLE findings, brief_runs, scrape_runs, releases "
            "RESTART IDENTITY CASCADE"
        )
    yield


def _seed_pair(cur, run_id, *, window_end: date, old_yoy: float,
               new_yoy: float, seq: int, subkind="gacc_bilateral_aggregate_yoy",
               group="Testland") -> None:
    """One superseded→superseding finding pair, superseded AFTER the
    brief_runs baseline."""
    detail_old = {"partner": {"raw_label": group},
                  "windows": {"current_end": window_end.isoformat()},
                  "totals": {"yoy_pct": old_yoy},
                  "method": "m_v1"}
    detail_new = dict(detail_old, totals={"yoy_pct": new_yoy})
    cur.execute(
        "INSERT INTO findings (scrape_run_id, kind, subkind, title, detail, "
        "natural_key_hash, created_at) VALUES (%s,'anomaly',%s,%s,%s,%s, "
        "now() - interval '30 days') RETURNING id",
        (run_id, subkind, f"old {seq}", psycopg2.extras.Json(detail_old),
         f"nk-old-{seq}"))
    old_id = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO findings (scrape_run_id, kind, subkind, title, detail, "
        "natural_key_hash, created_at) VALUES (%s,'anomaly',%s,%s,%s,%s, now()) "
        "RETURNING id",
        (run_id, subkind, f"new {seq}", psycopg2.extras.Json(detail_new),
         f"nk-old-{seq}-v2"))
    new_id = cur.fetchone()[0]
    cur.execute(
        "UPDATE findings SET superseded_at = now(), "
        "superseded_by_finding_id = %s WHERE id = %s", (new_id, old_id))


def _diff(test_db_url):
    conn = psycopg2.connect(test_db_url)
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return _compute_diff(cur)
    finally:
        conn.close()


@pytest.fixture
def seeded(fresh_db, test_db_url):
    """A baseline brief a week ago, then one correction batch: two material
    shifts at the current window (May 2026) and three on historic windows
    (2021)."""
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO scrape_runs (source_url, status) "
                    "VALUES ('seed','success') RETURNING id")
        run_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO brief_runs (output_path, top_n, data_period, trigger, "
            "generated_at) VALUES ('/tmp/x', 10, '2026-04-01', 'periodic_run', "
            "now() - interval '7 days')")
        _seed_pair(cur, run_id, window_end=date(2026, 5, 1),
                   old_yoy=0.10, new_yoy=0.30, seq=1, group="Currentland")
        _seed_pair(cur, run_id, window_end=date(2026, 4, 1),
                   old_yoy=-0.02, new_yoy=0.09, seq=2, group="Recentland")
        _seed_pair(cur, run_id, window_end=date(2021, 7, 1),
                   old_yoy=1.04, new_yoy=0.64, seq=3, group="Historia")
        _seed_pair(cur, run_id, window_end=date(2021, 10, 1),
                   old_yoy=0.73, new_yoy=0.38, seq=4, group="Historia")
        _seed_pair(cur, run_id, window_end=date(2022, 3, 1),
                   old_yoy=0.20, new_yoy=0.55, seq=5, group="Oldshire")
    return None


def test_recent_shifts_listed_historic_collapsed(seeded, test_db_url):
    d = _diff(test_db_url)
    assert d.regime == "movement"
    listed = {s["group_name"] for s in d.significant}
    assert listed == {"Currentland", "Recentland"}
    assert d.restated_count == 3
    assert d.restated_range == "Jul 2021 – Mar 2022"
    assert d.restated_max_pp == pytest.approx(40.0, abs=0.1)


def test_pure_historic_correction_is_movement_not_no_change(
        fresh_db, test_db_url):
    """A correction touching ONLY old windows must still read as movement
    (with the collapsed line), never as a silent 'no_change'."""
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO scrape_runs (source_url, status) "
                    "VALUES ('seed','success') RETURNING id")
        run_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO brief_runs (output_path, top_n, data_period, trigger, "
            "generated_at) VALUES ('/tmp/x', 10, '2026-04-01', 'periodic_run', "
            "now() - interval '7 days')")
        # All pairs share ONE old window: the recency cutoff hangs off the
        # batch's own newest window, so these stay listed (a small targeted
        # correction is legible either way); a MIXED batch is what collapses.
        _seed_pair(cur, run_id, window_end=date(2021, 7, 1),
                   old_yoy=1.04, new_yoy=0.64, seq=1, group="Historia")
    d = _diff(test_db_url)
    assert d.regime == "movement"
    assert len(d.significant) + d.restated_count == 1


def test_all_surfaces_carry_the_collapsed_line(seeded, test_db_url):
    d = _diff(test_db_url)
    # Front-page digest.
    digest = " ".join(_since_last_pack_lines(d))
    assert "3 older-window findings" in digest
    assert "not this cycle's news" in digest
    # Portal What's-changed (model + HTML).
    wc = rm.WhatChanged(
        regime=d.regime, summary="s",
        significant=[rm.Shift(group_name=s["group_name"], subkind=s["subkind"],
                              old_yoy=s["old_yoy"], new_yoy=s["new_yoy"],
                              direction_flipped=s["direction_flipped"])
                     for s in d.significant],
        new_count=0, restated_count=d.restated_count,
        restated_range=d.restated_range)
    h = _what_changed_html(wc)
    assert "3 older-window findings" in h
    assert "Jul 2021 – Mar 2022" in h
    assert "supersede chain" in h
    # And the restated groups are NOT line items.
    assert "Historia" not in h and "Oldshire" not in h


def test_restated_only_html_renders_quietly(fresh_db):
    wc = rm.WhatChanged(regime="movement", summary="s", significant=[],
                        new_count=0, restated_count=720,
                        restated_range="Jul 2019 – Dec 2025")
    h = _what_changed_html(wc)
    assert "no current-window figure moved materially" in h
    assert "720 older-window findings" in h
