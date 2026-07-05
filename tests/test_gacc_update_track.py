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
    # The snapshot step ran once, publish-ready but without fresh LLM spend.
    assert len(fresh_db) == 1
    assert fresh_db[0]["generate_takes"] is False
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
