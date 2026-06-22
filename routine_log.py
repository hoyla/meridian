"""Routine source-check telemetry.

The daily Routine (see `.claude/scheduled-tasks/meridian-daily-periodic-run/`)
polls three upstream sources — Eurostat, HMRC, GACC — and then runs the
`--periodic-run` orchestrator. Each per-source poll attempt writes one row
to `routine_check_log` so `python scrape.py --source-status` can show:

- when each source was last checked
- when each source last brought back new data
- the most recent attempt's two orthogonal axes:
    result      — the objective outcome: new_data / no_change / error
    expectation — derived from the source's publication calendar
                  (release_calendar.py): none_expected / due / overdue
                  for every checked source (gacc joined 2026-06-22 with a
                  formula-only calendar); NULL only when there is no candidate
                  period to classify (empty DB, or the _routine bookends)

The two axes combine: a quiet expected gap is no_change × none_expected
(ignore); a release missing past its scheduled date is no_change × overdue
(the one row a human should look at); an early arrival is new_data ×
none_expected. This replaced the old "5 weeks past period close" fetch-gate
(result='not_yet_eligible') — we always probe now, because fetching is
idempotent and harmless, and the calendar gives the "is data expected yet?"
signal more precisely. See dev_notes/2026-06-02-eurostat-expectation-axis-design.md.

Debug-only — no journalist-facing artefact reads from here. The Routine
SKILL.md is the only writer in production; tests + ad-hoc CLI invocations
write too.
"""
from __future__ import annotations

import dataclasses
import logging
from datetime import date, datetime
from typing import Iterable, Sequence

import db
from release_calendar import VALID_EXPECTATIONS

log = logging.getLogger(__name__)


# Sources the Routine is expected to check. The status rollup surfaces a
# "never checked" row for any of these missing from the log — making
# Routine drift (e.g. GACC silently dropped) immediately visible.
EXPECTED_SOURCES: tuple[str, ...] = ("eurostat", "hmrc", "gacc")

# Reserved source name for whole-Routine lifecycle events. Logged once at
# the start of a fire (result='started') and once at the end
# (result='completed' on success, result='error' on an orchestrator-level
# failure). A 'started' row with no matching 'completed' or 'error' = the
# Routine died mid-run; rely on the source rows to see how far it got.
ROUTINE_LIFECYCLE_SOURCE: str = "_routine"

# Result values the application writes. 'not_yet_eligible' is intentionally
# absent — the 5-week fetch-gate was replaced by the expectation axis
# (2026-06-02); we always probe now. Historical rows may still carry it, and
# the DB CHECK still permits it, but log_check no longer accepts it.
VALID_RESULTS: frozenset[str] = frozenset(
    {
        "new_data", "no_change", "error",
        "started", "completed",
    }
)


def log_check(
    source: str,
    result: str,
    *,
    expectation: str | None = None,
    candidate_period: date | None = None,
    notes: str | None = None,
    error: str | None = None,
    duration_ms: int | None = None,
) -> int:
    """Insert one row into routine_check_log; returns the new id.

    `expectation` is the publication-calendar axis (none_expected / due /
    overdue) — None for the _routine lifecycle bookends and any check with no
    candidate period to classify (e.g. an empty DB). Compute it via
    release_calendar.classify_expectation.
    """
    if result not in VALID_RESULTS:
        raise ValueError(
            f"result must be one of {sorted(VALID_RESULTS)}, got {result!r}"
        )
    if expectation is not None and expectation not in VALID_EXPECTATIONS:
        raise ValueError(
            f"expectation must be one of {sorted(VALID_EXPECTATIONS)} or None, "
            f"got {expectation!r}"
        )
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO routine_check_log
                (source, result, expectation, candidate_period,
                 notes, error, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (source, result, expectation, candidate_period,
             notes, error, duration_ms),
        )
        return cur.fetchone()[0]


@dataclasses.dataclass(frozen=True)
class SourceStatus:
    source: str
    last_check_at: datetime | None
    last_result: str | None
    last_expectation: str | None
    last_new_data_at: datetime | None
    last_period_brought_back: date | None
    latest_period_in_db: date | None
    notes: str | None
    error: str | None


@dataclasses.dataclass(frozen=True)
class RoutineLifecycle:
    """Whole-Routine fire-level state. Derived from `_routine` rows.

    A `started` row whose `checked_at` is later than the matching
    `completed` / `error` row (or has no matching pair) → the Routine
    died mid-run, before reaching the final bookend.
    """

    last_started_at: datetime | None
    last_finished_at: datetime | None      # 'completed' or 'error', whichever is more recent
    last_finished_result: str | None        # 'completed' | 'error' | None
    last_finished_error: str | None
    in_flight: bool                          # last started has no later finished


def compute_lifecycle() -> RoutineLifecycle:
    """Roll up the most recent Routine fire's lifecycle bookends."""
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(checked_at) FROM routine_check_log
            WHERE source = %s AND result = 'started'
            """,
            (ROUTINE_LIFECYCLE_SOURCE,),
        )
        last_started_at = cur.fetchone()[0]

        cur.execute(
            """
            SELECT checked_at, result, error
            FROM routine_check_log
            WHERE source = %s AND result IN ('completed', 'error')
            ORDER BY checked_at DESC
            LIMIT 1
            """,
            (ROUTINE_LIFECYCLE_SOURCE,),
        )
        finished = cur.fetchone()

    last_finished_at = finished[0] if finished else None
    last_finished_result = finished[1] if finished else None
    last_finished_error = finished[2] if finished else None

    in_flight = bool(
        last_started_at is not None
        and (last_finished_at is None or last_started_at > last_finished_at)
    )

    return RoutineLifecycle(
        last_started_at=last_started_at,
        last_finished_at=last_finished_at,
        last_finished_result=last_finished_result,
        last_finished_error=last_finished_error,
        in_flight=in_flight,
    )


def compute_status(
    sources: Sequence[str] = EXPECTED_SOURCES,
) -> list[SourceStatus]:
    """Roll up the most-recent state per source.

    Returns one entry per source in `sources` (preserving order). Sources
    that have never been logged appear with all-None telemetry fields but
    `latest_period_in_db` is still populated from `releases` — so a state
    like "GACC has 149 releases in the DB but the Routine has never
    checked it" reads at a glance.
    """
    sources = tuple(sources)
    if not sources:
        return []
    source_list = list(sources)

    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (source)
                source, checked_at, result, expectation, notes, error
            FROM routine_check_log
            WHERE source = ANY(%s)
            ORDER BY source, checked_at DESC
            """,
            (source_list,),
        )
        last_check = {row[0]: row[1:] for row in cur.fetchall()}

        cur.execute(
            """
            SELECT DISTINCT ON (source)
                source, checked_at, candidate_period
            FROM routine_check_log
            WHERE source = ANY(%s) AND result = 'new_data'
            ORDER BY source, checked_at DESC
            """,
            (source_list,),
        )
        last_new = {row[0]: row[1:] for row in cur.fetchall()}

        cur.execute(
            """
            SELECT source, MAX(period)
            FROM releases
            WHERE source = ANY(%s)
            GROUP BY source
            """,
            (source_list,),
        )
        latest_release = {row[0]: row[1] for row in cur.fetchall()}

    out: list[SourceStatus] = []
    for src in sources:
        check = last_check.get(src)
        new = last_new.get(src)
        out.append(SourceStatus(
            source=src,
            last_check_at=check[0] if check else None,
            last_result=check[1] if check else None,
            last_expectation=check[2] if check else None,
            notes=check[3] if check else None,
            error=check[4] if check else None,
            last_new_data_at=new[0] if new else None,
            last_period_brought_back=new[1] if new else None,
            latest_period_in_db=latest_release.get(src),
        ))
    return out


def render_status_table(
    statuses: Iterable[SourceStatus],
    lifecycle: RoutineLifecycle | None = None,
) -> str:
    """Plain-text aligned table for terminal output. No colour, no unicode
    box-drawing — designed to be readable in a Routine chat reply too.

    Optional `lifecycle` prepends a one-block header surfacing the most
    recent Routine fire's bookend state — so a stuck / silently failed
    run reads immediately above the per-source view."""
    rows = list(statuses)
    if not rows:
        return "(no sources to report)\n"

    def fmt_ts(ts: datetime | None) -> str:
        return ts.strftime("%Y-%m-%d %H:%M") if ts else "—"

    def fmt_period(d: date | None) -> str:
        return d.strftime("%Y-%m") if d else "—"

    headers = [
        "source", "last_check", "last_result", "expectation",
        "last_new_data", "period_brought_back", "latest_in_db",
    ]
    body = [[
        row.source,
        fmt_ts(row.last_check_at),
        row.last_result or "—",
        row.last_expectation or "—",
        fmt_ts(row.last_new_data_at),
        fmt_period(row.last_period_brought_back),
        fmt_period(row.latest_period_in_db),
    ] for row in rows]

    widths = [max(len(c) for c in col) for col in zip(*([headers] + body))]
    table_lines = [
        "  ".join(h.ljust(w) for h, w in zip(headers, widths)),
        "  ".join("-" * w for w in widths),
    ]
    for row in body:
        table_lines.append("  ".join(c.ljust(w) for c, w in zip(row, widths)))

    lines: list[str] = []
    if lifecycle is not None:
        lines.extend(_render_lifecycle_header(lifecycle))
        lines.append("")
    lines.extend(table_lines)

    extras: list[str] = []
    # Surface "anything overdue?" prominently and independently of whether
    # anything landed — a source past its scheduled publication date with no
    # data yet is the one state worth a human glance.
    overdue = [row.source for row in rows if row.last_expectation == "overdue"]
    if overdue:
        extras.append(
            f"  OVERDUE: {', '.join(overdue)} — scheduled release date "
            "passed, data not yet seen"
        )
    for row in rows:
        if row.error:
            extras.append(f"  {row.source}: last error → {row.error}")
        elif row.notes:
            extras.append(f"  {row.source}: last notes → {row.notes}")
    if extras:
        lines.append("")
        lines.extend(extras)

    return "\n".join(lines) + "\n"


def _render_lifecycle_header(lifecycle: RoutineLifecycle) -> list[str]:
    """Lead-in lines describing the last Routine fire's bookend state."""

    def fmt_ts(ts: datetime | None) -> str:
        return ts.strftime("%Y-%m-%d %H:%M") if ts else "—"

    if lifecycle.last_started_at is None:
        return ["routine fire: never started"]

    if lifecycle.in_flight:
        return [
            f"routine fire: STARTED {fmt_ts(lifecycle.last_started_at)} — no completion event",
            "  (either still running, or died mid-run before logging completion)",
        ]

    finished = lifecycle.last_finished_result or "—"
    line = (
        f"routine fire: started {fmt_ts(lifecycle.last_started_at)}, "
        f"finished {fmt_ts(lifecycle.last_finished_at)} ({finished})"
    )
    out = [line]
    if lifecycle.last_finished_error:
        out.append(f"  last error → {lifecycle.last_finished_error}")
    return out
