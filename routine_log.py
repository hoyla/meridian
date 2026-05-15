"""Routine source-check telemetry.

The daily Routine (see `.claude/scheduled-tasks/meridian-daily-periodic-run/`)
polls three upstream sources — Eurostat, HMRC, GACC — and then runs the
`--periodic-run` orchestrator. Each per-source poll attempt writes one row
to `routine_check_log` so `python scrape.py --source-status` can show:

- when each source was last checked
- when each source last brought back new data
- whether the most recent attempt was a successful fetch, a "nothing new
  yet" no-op, an explicit skip (the next candidate period isn't 5 weeks
  past period-close yet, so the Routine doesn't bother fetching) or an
  error

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

log = logging.getLogger(__name__)


# Sources the Routine is expected to check. The status rollup surfaces a
# "never checked" row for any of these missing from the log — making
# Routine drift (e.g. GACC silently dropped) immediately visible.
EXPECTED_SOURCES: tuple[str, ...] = ("eurostat", "hmrc", "gacc")

VALID_RESULTS: frozenset[str] = frozenset(
    {"new_data", "no_change", "not_yet_eligible", "error"}
)


def log_check(
    source: str,
    result: str,
    *,
    candidate_period: date | None = None,
    notes: str | None = None,
    error: str | None = None,
    duration_ms: int | None = None,
) -> int:
    """Insert one row into routine_check_log; returns the new id."""
    if result not in VALID_RESULTS:
        raise ValueError(
            f"result must be one of {sorted(VALID_RESULTS)}, got {result!r}"
        )
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO routine_check_log
                (source, result, candidate_period, notes, error, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (source, result, candidate_period, notes, error, duration_ms),
        )
        return cur.fetchone()[0]


@dataclasses.dataclass(frozen=True)
class SourceStatus:
    source: str
    last_check_at: datetime | None
    last_result: str | None
    last_new_data_at: datetime | None
    last_period_brought_back: date | None
    latest_period_in_db: date | None
    notes: str | None
    error: str | None


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
                source, checked_at, result, notes, error
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
            notes=check[2] if check else None,
            error=check[3] if check else None,
            last_new_data_at=new[0] if new else None,
            last_period_brought_back=new[1] if new else None,
            latest_period_in_db=latest_release.get(src),
        ))
    return out


def render_status_table(statuses: Iterable[SourceStatus]) -> str:
    """Plain-text aligned table for terminal output. No colour, no unicode
    box-drawing — designed to be readable in a Routine chat reply too."""
    rows = list(statuses)
    if not rows:
        return "(no sources to report)\n"

    def fmt_ts(ts: datetime | None) -> str:
        return ts.strftime("%Y-%m-%d %H:%M") if ts else "—"

    def fmt_period(d: date | None) -> str:
        return d.strftime("%Y-%m") if d else "—"

    headers = [
        "source", "last_check", "last_result",
        "last_new_data", "period_brought_back", "latest_in_db",
    ]
    body = [[
        row.source,
        fmt_ts(row.last_check_at),
        row.last_result or "—",
        fmt_ts(row.last_new_data_at),
        fmt_period(row.last_period_brought_back),
        fmt_period(row.latest_period_in_db),
    ] for row in rows]

    widths = [max(len(c) for c in col) for col in zip(*([headers] + body))]
    lines = [
        "  ".join(h.ljust(w) for h, w in zip(headers, widths)),
        "  ".join("-" * w for w in widths),
    ]
    for row in body:
        lines.append("  ".join(c.ljust(w) for c, w in zip(row, widths)))

    extras: list[str] = []
    for row in rows:
        if row.error:
            extras.append(f"  {row.source}: last error → {row.error}")
        elif row.notes:
            extras.append(f"  {row.source}: last notes → {row.notes}")
    if extras:
        lines.append("")
        lines.extend(extras)

    return "\n".join(lines) + "\n"
