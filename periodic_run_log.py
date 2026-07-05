"""Periodic-run cycle log — Layer 1 audit table for `--periodic-run` /
`--gacc-update-run` invocations.

One row per orchestrator cycle (`periodic.run_periodic()` on the main
track, `periodic.run_gacc_update()` on the gacc track), whether the cycle
acted or no-op'd. Pairs with `brief_runs` (which only has rows for cycles
that wrote) — most rows here will be no-ops, which is exactly the signal
we want when debugging "did the Routine fire today and silently no-op, or
did it not fire at all?" The `track` column keeps the two cycles' history
distinguishable in `--periodic-history`.

See `dev_notes/2026-05-15-logging-policy.md`.
"""
from __future__ import annotations

import dataclasses
import json
import logging
from datetime import date, datetime
from typing import Any

import db

log = logging.getLogger(__name__)

# Track vocabulary. Single source of truth shared by the write guard below
# and the DB CHECK constraint (same pattern as release_calendar's
# VALID_EXPECTATIONS ↔ routine_check_log).
VALID_TRACKS: frozenset[str] = frozenset({"main", "gacc"})


def log_run(
    *,
    action_taken: bool,
    reason: str,
    data_period: date | None,
    findings_path: str | None,
    analyser_counts: dict[str, Any] | None = None,
    duration_ms: int | None = None,
    forced: bool = False,
    skip_llm: bool = False,
    error: str | None = None,
    track: str = "main",
) -> int:
    """Insert one row into periodic_run_log; returns the new id."""
    if track not in VALID_TRACKS:
        raise ValueError(
            f"track must be one of {sorted(VALID_TRACKS)}; got {track!r}"
        )
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO periodic_run_log
                (action_taken, reason, data_period, findings_path,
                 analyser_counts, duration_ms, forced, skip_llm, error, track)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                action_taken, reason, data_period, findings_path,
                json.dumps(analyser_counts) if analyser_counts is not None else None,
                duration_ms, forced, skip_llm, error, track,
            ),
        )
        return cur.fetchone()[0]


@dataclasses.dataclass(frozen=True)
class CycleRow:
    id: int
    invoked_at: datetime
    action_taken: bool
    reason: str
    data_period: date | None
    findings_path: str | None
    duration_ms: int | None
    forced: bool
    skip_llm: bool
    error: str | None
    track: str = "main"


def recent_cycles(limit: int = 20) -> list[CycleRow]:
    """Return the most recent cycle invocations (both tracks interleaved),
    newest first."""
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, invoked_at, action_taken, reason, data_period,
                   findings_path, duration_ms, forced, skip_llm, error, track
            FROM periodic_run_log
            ORDER BY invoked_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [
        CycleRow(
            id=r[0], invoked_at=r[1], action_taken=r[2], reason=r[3],
            data_period=r[4], findings_path=r[5], duration_ms=r[6],
            forced=r[7], skip_llm=r[8], error=r[9], track=r[10],
        )
        for r in rows
    ]


def render_cycles(rows: list[CycleRow]) -> str:
    """Human-readable rendering for terminal output."""
    if not rows:
        return "(no periodic-run cycles logged)\n"
    out: list[str] = []
    for r in rows:
        ts = r.invoked_at.strftime("%Y-%m-%d %H:%M")
        outcome = "WROTE EXPORT" if r.action_taken else "no-op"
        if r.error:
            outcome = "ERROR"
        flags = []
        if r.track != "main":
            flags.append(r.track)
        if r.forced:
            flags.append("forced")
        if r.skip_llm:
            flags.append("skip_llm")
        flag_str = f" [{', '.join(flags)}]" if flags else ""
        duration = f" ({r.duration_ms}ms)" if r.duration_ms is not None else ""
        out.append(f"[{r.id}] {ts}  {outcome}{flag_str}{duration}")
        out.append(f"      reason: {r.reason}")
        if r.data_period:
            out.append(f"      data_period: {r.data_period.isoformat()}")
        if r.findings_path:
            out.append(f"      output: {r.findings_path}")
        if r.error:
            out.append(f"      error: {r.error}")
        out.append("")
    return "\n".join(out)
