"""Tests for the GACC-track update cycle (`periodic.run_gacc_update`) and
the cross-track isolation it depends on.

Load-bearing properties (dev_notes/2026-07-05-gacc-update-page-design.md
§ brief_runs mechanics):

- fires once per new GACC *period*, not per release — GACC publishes each
  month twice (CNY then USD);
- the dual-currency second release for an already-published period takes
  the quiet-refresh path (analysers re-run, refresh row recorded, no
  new-period event);
- gacc_update rows never advance the MAIN track's baselines — idempotency,
  the Tier 1 diff baseline, the "new source data since the last brief"
  phrase — and main rows never advance the GACC track's.

Same live-Postgres approach as test_periodic.py."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import psycopg2
import pytest

import briefing_pack
import periodic


@pytest.fixture
def fresh_db(test_db_url, monkeypatch, tmp_path):
    """Truncate everything the gacc-update cycle could touch and point
    DATABASE_URL at the test DB (db.py + briefing_pack read it at runtime).

    Also stubs `periodic.write_portal_snapshot`: since PR 2 the cycle
    rebuilds the whole portal snapshot on every action, which is a full
    build_report — far too heavy for this unit loop, and it would write
    export dirs into the working tree. The stub records the call and
    returns a fake 04_Portal path; the snapshot build itself is covered by
    test_gacc_page.py + test_portal.py."""
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    calls: list[dict] = []

    def _stub_snapshot(bundle_dir, data_period, **kwargs):
        calls.append({"bundle_dir": bundle_dir, "data_period": data_period,
                      **kwargs})
        return str(tmp_path / "04_Portal")

    monkeypatch.setattr(periodic, "write_portal_snapshot", _stub_snapshot)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE TABLE findings, observations, eurostat_raw_rows, "
            "hmrc_raw_rows, brief_runs, scrape_runs, releases, "
            "source_snapshots, periodic_run_log, findings_emit_log "
            "RESTART IDENTITY CASCADE"
        )
    yield calls


def _seed_gacc_release(
    test_db_url,
    period: date,
    currency: str = "CNY",
    first_seen_at: datetime | None = None,
) -> None:
    """One GACC release row. currency distinguishes the dual-currency pair
    (the gacc natural key is (section_number, currency, period,
    release_kind)). first_seen_at defaults to now() like the live ingest."""
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO releases (source, period, source_url, currency, "
            "section_number, release_kind, first_seen_at) "
            "VALUES ('gacc', %s, %s, %s, 4, 'preliminary', COALESCE(%s, now()))",
            (
                period,
                f"https://example.invalid/gacc/{period:%Y%m}/{currency}",
                currency,
                first_seen_at,
            ),
        )


def _seed_main_brief_run(
    test_db_url,
    data_period: date,
    trigger: str = "periodic_run",
    generated_at: datetime | None = None,
) -> None:
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO brief_runs "
            "(output_path, top_n, data_period, trigger, generated_at) "
            "VALUES (%s, %s, %s, %s, COALESCE(%s, now()))",
            ("/tmp/seed/findings.md", 10, data_period, trigger, generated_at),
        )


def _gacc_brief_rows(test_db_url) -> list[tuple]:
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT data_period, output_path, notes FROM brief_runs "
            "WHERE trigger = 'gacc_update' ORDER BY generated_at",
        )
        return cur.fetchall()


def test_noop_when_no_gacc_data(fresh_db):
    result = periodic.run_gacc_update()
    assert result.action_taken is False
    assert "no GACC data" in result.reason
    assert result.data_period is None


def test_new_period_fires_once_and_is_invisible_to_main_track(
    fresh_db, test_db_url,
):
    """A fresh GACC period fires the track exactly once; the recorded row
    carries the GACC reference month and stays invisible to the main
    track's idempotency baseline."""
    _seed_gacc_release(test_db_url, date(2026, 6, 1), currency="CNY")

    result = periodic.run_gacc_update()
    assert result.action_taken is True
    assert result.refresh is False
    assert result.data_period == date(2026, 6, 1)
    assert "new GACC period" in result.reason

    rows = _gacc_brief_rows(test_db_url)
    assert len(rows) == 1
    data_period, output_path, notes = rows[0]
    assert data_period == date(2026, 6, 1)
    assert output_path is not None and output_path.endswith("04_Portal")
    assert notes is None
    # The snapshot step ran once, publish-ready; MAIN takes stay ungented
    # (grafted instead), the GACC page's own slots generate on the
    # new-period path (~2 paid calls a month — Luke-blessed).
    assert len(fresh_db) == 1
    assert fresh_db[0]["generate_takes"] is False
    assert fresh_db[0]["generate_gacc_takes"] is True
    assert fresh_db[0]["write_workbook"] is True
    assert result.portal_dir == output_path

    # Cross-track isolation: the GACC row (2026-06, a month ahead of any
    # Eurostat period) must not register on the main track.
    assert briefing_pack.latest_recorded_data_period(trigger="periodic_run") is None

    # Second invocation, nothing new → clean no-op.
    again = periodic.run_gacc_update()
    assert again.action_taken is False
    assert "already published" in again.reason
    assert len(_gacc_brief_rows(test_db_url)) == 1


def test_second_currency_release_takes_quiet_refresh_path(
    fresh_db, test_db_url,
):
    """CNY lands, the track fires; the USD release for the SAME period
    lands days later → quiet refresh (refresh row, refresh=True), not a
    second new-period event; then a further call is a clean no-op."""
    _seed_gacc_release(test_db_url, date(2026, 6, 1), currency="CNY")
    first = periodic.run_gacc_update()
    assert first.action_taken is True and first.refresh is False

    # The USD release arrives after the first run (first_seen_at = now()).
    _seed_gacc_release(test_db_url, date(2026, 6, 1), currency="USD")

    second = periodic.run_gacc_update()
    assert second.action_taken is True
    assert second.refresh is True
    assert second.data_period == date(2026, 6, 1)
    # The quiet-refresh path re-grafts the page's LLM slots rather than
    # re-paying for generation (same GACC month).
    assert fresh_db[-1]["generate_gacc_takes"] is False

    rows = _gacc_brief_rows(test_db_url)
    assert len(rows) == 2
    assert rows[1][2] is not None and rows[1][2].startswith("refresh:")

    third = periodic.run_gacc_update()
    assert third.action_taken is False
    assert len(_gacc_brief_rows(test_db_url)) == 2


def test_main_track_rows_do_not_block_gacc_track(fresh_db, test_db_url):
    """A main-track row at the SAME data_period as the GACC month must not
    make the GACC track think it already published (the reverse of the
    footgun: tracks are scoped both ways)."""
    _seed_main_brief_run(test_db_url, date(2026, 6, 1), trigger="periodic_run")
    _seed_gacc_release(test_db_url, date(2026, 6, 1))

    result = periodic.run_gacc_update()
    assert result.action_taken is True
    assert result.data_period == date(2026, 6, 1)


def test_tier1_diff_baseline_ignores_gacc_rows(fresh_db, test_db_url):
    """With ONLY a gacc_update row in brief_runs, the main briefing's
    Tier 1 diff must still read 'first export' — a GACC-track run is not a
    previous brief. (Unscoped, the gacc row would become the baseline and
    every finding created before it would vanish from 'new since the last
    brief'.)"""
    from briefing_pack.sections.diff import _compute_diff

    _seed_gacc_release(test_db_url, date(2026, 6, 1))
    periodic.run_gacc_update()
    assert len(_gacc_brief_rows(test_db_url)) == 1

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        diff = _compute_diff(cur)
    assert diff.regime == "first_export"


def test_new_data_phrase_baselines_on_main_track_not_gacc(
    fresh_db, test_db_url,
):
    """The 'new source data since the last brief' phrase must baseline on
    the last MAIN brief. Scenario: main brief at T0; a GACC release lands
    at T0+1h; the GACC track runs (brief_runs row at now()). The phrase
    must still NAME the GACC release — baselining on the gacc row's
    timestamp would silently drop it."""
    from briefing_pack._helpers import _new_data_phrase_since_last_brief

    t0 = datetime.now(timezone.utc) - timedelta(hours=2)
    _seed_main_brief_run(
        test_db_url, date(2026, 4, 1), trigger="periodic_run", generated_at=t0,
    )
    _seed_gacc_release(
        test_db_url, date(2026, 6, 1), first_seen_at=t0 + timedelta(hours=1),
    )
    periodic.run_gacc_update()

    phrase = _new_data_phrase_since_last_brief()
    assert "GACC" in phrase

    # And with ONLY gacc rows in brief_runs there is no prior MAIN brief,
    # so the phrase is empty rather than baselined on the gacc row.
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM brief_runs WHERE trigger <> 'gacc_update'")
    assert _new_data_phrase_since_last_brief() == ""


def test_gacc_cycle_crash_writes_gacc_track_log_row(
    fresh_db, test_db_url, monkeypatch,
):
    """F4 parity for the GACC track: a mid-cycle crash leaves a
    periodic_run_log error row tagged track='gacc'."""
    _seed_gacc_release(test_db_url, date(2026, 6, 1))

    import anomalies

    def _boom(**kwargs):
        raise RuntimeError("gacc analyser exploded")

    monkeypatch.setattr(anomalies, "detect_gacc_aggregate_yoy", _boom)

    with pytest.raises(RuntimeError, match="gacc analyser exploded"):
        periodic.run_gacc_update()

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT track, action_taken, error FROM periodic_run_log"
        )
        rows = cur.fetchall()
    assert len(rows) == 1
    track, action_taken, error = rows[0]
    assert track == "gacc"
    assert action_taken is False
    assert "gacc analyser exploded" in error


def test_gacc_track_logs_noop_cycles(fresh_db, test_db_url):
    """Both the action and the no-op paths leave periodic_run_log rows
    tagged track='gacc' — the 'did it fire and no-op, or never fire?'
    signal works for this track too."""
    _seed_gacc_release(test_db_url, date(2026, 6, 1))
    periodic.run_gacc_update()   # action
    periodic.run_gacc_update()   # no-op

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT action_taken FROM periodic_run_log WHERE track = 'gacc' "
            "ORDER BY id"
        )
        rows = [r[0] for r in cur.fetchall()]
    assert rows == [True, False]


# ---------------------------------------------------------------------------
# The published marker must describe what the page RENDERED, not what was
# ingested. Regression cover for 2026-09-08: GACC published section 4 in USD
# only, so 2026-08 landed in `releases` while the by-country analyser (which
# pins to CNY) left the page rendering 2026-07. The track recorded 2026-08 as
# published, and because `latest_recorded_data_period` is a MAX, that marker
# could never be outvoted by a later correct row — the real August drop would
# have been treated as an already-published month, skipping fresh LLM takes.
# ---------------------------------------------------------------------------

def _seed_gacc_anchor(test_db_url, current_end: date) -> None:
    """A live gacc_aggregate_yoy finding — what the page anchors its rendered
    month on (report_builder._gacc_latest_period)."""
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) "
            "VALUES ('https://example.invalid/anchor', 'success') RETURNING id"
        )
        run_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO findings (scrape_run_id, kind, subkind, title, "
            "natural_key_hash, value_signature, detail) "
            "VALUES (%s, 'anomaly', 'gacc_aggregate_yoy', 'anchor', %s, 'v', "
            "%s::jsonb)",
            (run_id, f"nk-{current_end}",
             '{"windows": {"current_end": "%s"}}' % current_end.isoformat()),
        )


def test_renderable_period_matches_the_page_anchor(fresh_db, test_db_url):
    """briefing_pack's helper and the report builder must agree on which
    month the page shows, or the track's bookkeeping drifts from reality."""
    import report_builder

    _seed_gacc_anchor(test_db_url, date(2026, 7, 1))
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        from_builder = report_builder._gacc_latest_period(cur)

    assert briefing_pack.latest_gacc_renderable_period() == from_builder
    assert from_builder == date(2026, 7, 1)


def test_partial_release_records_the_rendered_month_not_the_ingested_one(
    fresh_db, test_db_url,
):
    """August ingested, page still on July: record July."""
    _seed_gacc_release(test_db_url, date(2026, 8, 1), currency="USD")
    _seed_gacc_anchor(test_db_url, date(2026, 7, 1))

    result = periodic.run_gacc_update()

    assert result.action_taken is True
    rows = _gacc_brief_rows(test_db_url)
    assert [r[0] for r in rows] == [date(2026, 7, 1)], (
        "the marker must record the month readers were shown"
    )
    assert briefing_pack.latest_recorded_data_period(
        trigger="gacc_update") == date(2026, 7, 1)
    # And the operator is told, rather than reading "new GACC period 2026-08".
    assert "still renders 2026-07-01" in result.reason
    assert "2026-08-01" in result.reason


def test_a_lagging_page_does_not_burn_fresh_takes(fresh_db, test_db_url):
    """No new month reached readers, so no paid regeneration."""
    _seed_gacc_release(test_db_url, date(2026, 8, 1), currency="USD")
    _seed_gacc_anchor(test_db_url, date(2026, 7, 1))
    _seed_main_brief_run(test_db_url, date(2026, 7, 1), trigger="gacc_update")

    calls = fresh_db
    periodic.run_gacc_update()

    assert calls, "the snapshot should still rebuild"
    assert calls[-1]["generate_gacc_takes"] is False


def test_takes_regenerate_when_the_anchor_finally_advances(
    fresh_db, test_db_url,
):
    """The drop-day path: once section 4 CNY lands and the anchor moves to
    August, the cycle must treat it as a new month and generate fresh takes —
    the graft deliberately refuses to carry July's takes onto an August page,
    so without this the page would publish with empty slots."""
    _seed_gacc_release(test_db_url, date(2026, 8, 1), currency="USD")
    _seed_gacc_anchor(test_db_url, date(2026, 7, 1))
    periodic.run_gacc_update()          # partial release: records July

    # Section 4 CNY arrives; the analysers move the anchor to August.
    _seed_gacc_release(test_db_url, date(2026, 8, 1), currency="CNY")
    _seed_gacc_anchor(test_db_url, date(2026, 8, 1))

    calls = fresh_db
    result = periodic.run_gacc_update()

    assert result.action_taken is True
    assert calls[-1]["generate_gacc_takes"] is True
    assert briefing_pack.latest_recorded_data_period(
        trigger="gacc_update") == date(2026, 8, 1)
