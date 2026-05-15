"""findings_emit_log — Layer 1 audit table for analyser invocations.

One row per `detect_X()` analyser call, capturing the method version,
comparison scope, flow direction, and the emit-counts dict (`new`,
`confirmed`, `superseded`, various `skipped_*` keys). Covers both
periodic-run cycles AND ad-hoc CLI invocations.

The supersede chain in `findings` is the per-row audit trail. This
table is the per-invocation audit trail, complementing it:
- The chain shows individual finding revisions.
- This table shows what an analyser run produced in aggregate.

See `dev_notes/logging-policy.md`.
"""
from __future__ import annotations

import dataclasses
import json
import logging
from datetime import datetime
from typing import Any

import db

log = logging.getLogger(__name__)


def log_run(
    *,
    scrape_run_id: int | None,
    analyser_method: str,
    subkind: str,
    counts: dict[str, Any],
    comparison_scope: str | None = None,
    flow: int | None = None,
    duration_ms: int | None = None,
) -> int:
    """Insert one row into findings_emit_log; returns the new id.

    `counts` is the dict the analyser computed (and typically already
    returns). Stored verbatim as JSONB so the schema doesn't need to
    grow new columns when an analyser adds a new `skipped_*` reason."""
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO findings_emit_log
                (scrape_run_id, analyser_method, subkind, comparison_scope,
                 flow, counts, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
            RETURNING id
            """,
            (
                scrape_run_id, analyser_method, subkind, comparison_scope,
                flow, json.dumps(counts), duration_ms,
            ),
        )
        return cur.fetchone()[0]


@dataclasses.dataclass(frozen=True)
class EmitRunRow:
    id: int
    logged_at: datetime
    scrape_run_id: int | None
    analyser_method: str
    subkind: str
    comparison_scope: str | None
    flow: int | None
    counts: dict[str, Any]
    duration_ms: int | None


def recent_runs(limit: int = 20) -> list[EmitRunRow]:
    """Return the most recent analyser invocations, newest first."""
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, logged_at, scrape_run_id, analyser_method, subkind,
                   comparison_scope, flow, counts, duration_ms
            FROM findings_emit_log
            ORDER BY logged_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [
        EmitRunRow(
            id=r[0], logged_at=r[1], scrape_run_id=r[2],
            analyser_method=r[3], subkind=r[4], comparison_scope=r[5],
            flow=r[6], counts=r[7] or {}, duration_ms=r[8],
        )
        for r in rows
    ]


def render_runs(rows: list[EmitRunRow]) -> str:
    """Human-readable rendering for terminal output."""
    if not rows:
        return "(no analyser invocations logged)\n"
    out: list[str] = []
    for r in rows:
        ts = r.logged_at.strftime("%Y-%m-%d %H:%M")
        scope = f" scope={r.comparison_scope}" if r.comparison_scope else ""
        flow_str = f" flow={r.flow}" if r.flow is not None else ""
        duration = f" ({r.duration_ms}ms)" if r.duration_ms is not None else ""
        out.append(f"[{r.id}] {ts}  {r.subkind}{scope}{flow_str}{duration}")
        out.append(f"      method: {r.analyser_method}")
        # Show non-zero counts; full dict is in the DB.
        nonzero = {k: v for k, v in r.counts.items() if v}
        if nonzero:
            counts_str = ", ".join(f"{k}={v}" for k, v in sorted(nonzero.items()))
            out.append(f"      counts: {counts_str}")
        out.append("")
    return "\n".join(out)
