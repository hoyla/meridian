"""Baseline selection for "since the last briefing" (2026-07-16).

The portal snapshot is built AFTER its own cycle's brief_runs row exists
(periodic-run records the row at export and builds the snapshot in a later
step; a republish amends a cycle recorded long before). A most-recent-row
baseline therefore self-references and reports an empty cycle — the first
main-track periodic run after the snapshot was wired into the cycle (the
2026-07-16 May briefing) published "nothing moved materially" over ~3,000
new findings. `_compute_diff(baseline_before_period=...)` anchors on the
report's data_period instead: the baseline is the latest MAIN-track row for
a strictly earlier period — the previous briefing the reader actually saw —
and the diff is idempotent across rebuilds of the same period.
"""

from __future__ import annotations

from datetime import date

import psycopg2.extras
import pytest

from briefing_pack.sections.diff import _compute_diff


def _seed_run(cur) -> int:
    cur.execute("INSERT INTO scrape_runs (source_url, status) "
                "VALUES ('seed','success') RETURNING id")
    return cur.fetchone()[0]


def _seed_brief_run(cur, *, data_period: str, days_ago: int,
                    trigger: str = "periodic_run") -> int:
    cur.execute(
        "INSERT INTO brief_runs (output_path, top_n, data_period, trigger, "
        "generated_at) VALUES ('/tmp/x', 10, %s, %s, "
        "now() - make_interval(days => %s)) RETURNING id",
        (data_period, trigger, days_ago))
    return cur.fetchone()[0]


def _seed_new_finding(cur, run_id: int, *, days_ago: int, seq: int) -> None:
    """One active finding whose natural key first appeared `days_ago`."""
    detail = {"group": {"name": f"Testland {seq}"},
              "windows": {"current_end": "2026-05-01"},
              "totals": {"yoy_pct": 0.12},
              "method": "m_v1"}
    cur.execute(
        "INSERT INTO findings (scrape_run_id, kind, subkind, title, detail, "
        "natural_key_hash, created_at) VALUES (%s,'anomaly','hs_group_yoy',"
        "%s,%s,%s, now() - make_interval(days => %s))",
        (run_id, f"new {seq}", psycopg2.extras.Json(detail),
         f"nk-baseline-{seq}", days_ago))


def _diff(conn, **kwargs):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        return _compute_diff(cur, **kwargs)
    finally:
        cur.close()


def test_period_baseline_skips_own_cycle_row(db_conn):
    """The regression: April briefing (7d ago), this cycle's findings (3d
    ago), then this cycle's own May row (now). A most-recent-row baseline
    sees its own row and reports an empty cycle; the period-anchored
    baseline reads against the April briefing and reports the movement."""
    with db_conn, db_conn.cursor() as cur:
        run = _seed_run(cur)
        _seed_brief_run(cur, data_period="2026-04-01", days_ago=7)
        _seed_new_finding(cur, run, days_ago=3, seq=1)
        _seed_brief_run(cur, data_period="2026-05-01", days_ago=0)

    # The failure mode this fix removes: self-baseline → empty cycle.
    d = _diff(db_conn)
    assert d.regime == "no_change"
    assert d.total_new == 0

    d = _diff(db_conn, baseline_before_period=date(2026, 5, 1))
    assert d.regime == "movement"
    assert d.total_new == 1


def test_period_baseline_is_idempotent_across_rebuilds(db_conn):
    """Rebuilding the same period's snapshot (republish, take amendments)
    must reproduce the same diff — the baseline hangs off the period, not
    off whichever row happens to be newest at build time."""
    with db_conn, db_conn.cursor() as cur:
        run = _seed_run(cur)
        _seed_brief_run(cur, data_period="2026-04-01", days_ago=7)
        _seed_new_finding(cur, run, days_ago=3, seq=1)
        # The cycle publishes, then republishes the same period twice more.
        for days in (1, 0, 0):
            _seed_brief_run(cur, data_period="2026-05-01", days_ago=days)
    first = _diff(db_conn, baseline_before_period=date(2026, 5, 1))
    again = _diff(db_conn, baseline_before_period=date(2026, 5, 1))
    assert (first.regime, first.total_new) == ("movement", 1)
    assert (again.regime, again.total_new) == ("movement", 1)
    assert first.prev_ref == again.prev_ref


def test_period_baseline_first_briefing(db_conn):
    """No earlier-period briefing exists → first_export, not a self-baselined
    no_change against the cycle's own row."""
    with db_conn, db_conn.cursor() as cur:
        _seed_brief_run(cur, data_period="2026-05-01", days_ago=0)
    d = _diff(db_conn, baseline_before_period=date(2026, 5, 1))
    assert d.regime == "first_export"


def test_explicit_baseline_id_wins_over_period(db_conn):
    """The re-issue path names its row directly; the period anchor must not
    override it. The finding lands between the March and April rows, so the
    two baselines disagree about whether it is new."""
    with db_conn, db_conn.cursor() as cur:
        run = _seed_run(cur)
        march_id = _seed_brief_run(cur, data_period="2026-03-01", days_ago=14)
        _seed_new_finding(cur, run, days_ago=10, seq=1)
        _seed_brief_run(cur, data_period="2026-04-01", days_ago=7)
    d = _diff(db_conn, baseline_brief_run_id=march_id,
              baseline_before_period=date(2026, 5, 1))
    assert d.total_new == 1  # diffed against March, not April


def test_gacc_track_rows_never_anchor_the_period_baseline(db_conn):
    """A newer gacc_update row for an earlier period must not become the
    baseline — the period-anchored query is main-track-scoped like the
    default one. With the April MAIN row as baseline, the 5-days-ago
    finding is new; against the 2-days-ago GACC row it would not be."""
    with db_conn, db_conn.cursor() as cur:
        run = _seed_run(cur)
        _seed_brief_run(cur, data_period="2026-04-01", days_ago=10)
        _seed_new_finding(cur, run, days_ago=5, seq=1)
        _seed_brief_run(cur, data_period="2026-04-01", days_ago=2,
                        trigger="gacc_update")
    d = _diff(db_conn, baseline_before_period=date(2026, 5, 1))
    assert d.regime == "movement"
    assert d.total_new == 1
