"""LLM-framing rejection log — Layer 1 audit table for verifier hits.

Every output `llm_framing.detect_llm_framings()` rejects (parse failure
or numeric verification failure) writes a row here. The verifier's
warning log line is preserved as Layer 2 diagnostics; this is the
queryable persistent record so the rejected prose stays inspectable
once terminal scrollback rolls off.

See `dev_notes/logging-policy.md` for the policy this fits into.
"""
from __future__ import annotations

import dataclasses
import logging
from datetime import datetime
from typing import Literal

import db

log = logging.getLogger(__name__)

Stage = Literal["parse", "validate"]
VALID_STAGES: frozenset[str] = frozenset({"parse", "validate"})


def log_rejection(
    *,
    scrape_run_id: int | None,
    cluster_name: str,
    model: str | None,
    stage: Stage,
    reason: str,
    detail: str | None = None,
    raw_output: str | None = None,
    closest_fact_path: str | None = None,
    closest_fact_value: float | None = None,
) -> int:
    """Insert one row into llm_rejection_log; returns the new id.

    Called by `llm_framing.detect_llm_framings()` immediately after the
    `log.warning("Lead-scaffold ... rejected ...")` line at the parse
    or validation rejection sites."""
    if stage not in VALID_STAGES:
        raise ValueError(
            f"stage must be one of {sorted(VALID_STAGES)}, got {stage!r}"
        )
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO llm_rejection_log
                (scrape_run_id, cluster_name, model, stage, reason, detail,
                 raw_output, closest_fact_path, closest_fact_value)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                scrape_run_id, cluster_name, model, stage, reason, detail,
                raw_output, closest_fact_path, closest_fact_value,
            ),
        )
        return cur.fetchone()[0]


@dataclasses.dataclass(frozen=True)
class RejectionRow:
    id: int
    rejected_at: datetime
    cluster_name: str
    model: str | None
    stage: str
    reason: str
    detail: str | None
    raw_output: str | None


def recent_rejections(limit: int = 20) -> list[RejectionRow]:
    """Return the most recent rejections, newest first."""
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, rejected_at, cluster_name, model, stage, reason,
                   detail, raw_output
            FROM llm_rejection_log
            ORDER BY rejected_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [
        RejectionRow(
            id=r[0], rejected_at=r[1], cluster_name=r[2], model=r[3],
            stage=r[4], reason=r[5], detail=r[6], raw_output=r[7],
        )
        for r in rows
    ]


def render_rejections(rows: list[RejectionRow]) -> str:
    """Human-readable rendering. Truncates raw_output to keep terminal
    output skim-able; full prose is in the DB."""
    if not rows:
        return "(no LLM rejections logged)\n"
    out: list[str] = []
    for r in rows:
        ts = r.rejected_at.strftime("%Y-%m-%d %H:%M")
        out.append(f"[{r.id}] {ts}  {r.cluster_name}  ({r.stage} / {r.reason})")
        if r.model:
            out.append(f"      model: {r.model}")
        if r.detail:
            out.append(f"      detail: {r.detail}")
        if r.raw_output:
            preview = r.raw_output.strip().replace("\n", " ")
            if len(preview) > 220:
                preview = preview[:217] + "..."
            out.append(f"      output: {preview}")
        out.append("")
    return "\n".join(out)
