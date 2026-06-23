"""Graft prior LLM takes onto a freshly-built (LLM-less) report — the
"sticky takes" / reuse path (roadmap.md "retain prior LLM content on an
LLM-less rebuild").

Why this exists: a portal rebuild *without* `--portal-takes` rebuilds the
deterministic report but leaves the LLM takes empty — per-finding `item.take`
is `None`, the release-level general slot is a `status="placeholder"` stub — and
the renderer shows nothing for either. Re-running the paid LLM is the right
DEFAULT (if the content moved, the prior interpretation may be stale), so reuse
is a deliberate opt-in: it's for *amending an existing release* — a cosmetic /
layout fix, or a low-impact data correction — where the prior takes still hold
and re-paying the API is waste. This module carries those takes forward.

Two safety layers, both from the roadmap design:
  1. **data_period gate** — carry over ONLY when the prior snapshot's
     `data_period` equals the new report's. A new cycle (period advanced) wants
     fresh takes; every prior take is dropped.
  2. **finding-id match** — a per-finding take is grafted only onto the SAME
     finding id it was grounded in. Findings are append-only with supersede
     chains, so any content change that moves a finding's numbers supersedes it
     to a NEW id; the stale take then fails to match and is dropped (left as a
     placeholder), never mis-attached to changed numbers.

Pure: it mutates the new `Report` in place and reads the prior snapshot as a
plain dict (the parsed `report.json`), so it needs no DB, GCS, or LLM and is
unit-testable in isolation. The caller (`periodic.write_portal_snapshot`)
fetches the prior — from the live bucket via `portal_publish.read_latest_report`.
"""

from __future__ import annotations

import logging

from report_model import LLMSlot, Report

log = logging.getLogger(__name__)


def _prior_period(prior: dict) -> str | None:
    return (prior.get("meta") or {}).get("data_period")


def _new_period(report: Report) -> str | None:
    """The new report's data_period as an ISO string, to compare against the
    prior snapshot's JSON value (always a string)."""
    d = report.meta.data_period if report.meta else None
    if d is None:
        return None
    return d.isoformat() if hasattr(d, "isoformat") else str(d)


def graft_prior_takes(report: Report, prior: dict) -> int:
    """Carry forward generated LLM takes from a prior snapshot onto `report`, in
    place. Returns the number of takes grafted (per-finding + the general slot).

    No-op (returns 0) unless the prior's `data_period` matches the report's —
    the period gate. Within a matching period each per-finding take is grafted
    only onto the headline item grounded in the same finding id; the general
    slot is carried over wholesale (same period ⇒ same finding set). A take
    already generated on `report` (e.g. from a partial paid run) is never
    overwritten."""
    if report.headline is None:
        return 0

    new_period = _new_period(report)
    if new_period is None or _prior_period(prior) != new_period:
        if prior:  # a prior existed, but for a different period — note the drop
            log.info(
                "reuse-takes: prior data_period %r != %r; not grafting "
                "(fresh takes expected on a new cycle)",
                _prior_period(prior), new_period,
            )
        return 0

    prior_headline = prior.get("headline") or {}

    # Per-finding takes: finding id -> generated questions, from the prior items.
    by_finding: dict[int, list[dict]] = {}
    for it in prior_headline.get("items") or []:
        take = it.get("take") or {}
        if take.get("status") == "generated" and take.get("questions"):
            for fid in take.get("grounded_in") or []:
                by_finding[fid] = take["questions"]

    grafted = 0
    for item in report.headline.items:
        if item.take is not None and item.take.status == "generated":
            continue  # never clobber a live take
        fids = item.provenance.finding_ids or []
        qs = next((by_finding[f] for f in fids if f in by_finding), None)
        if qs:
            item.take = LLMSlot(
                slot_type="specific",
                grounded_in=[fids[0]],
                status="generated",
                questions=qs,
            )
            grafted += 1

    # General "one other thing worth a look" slot — carried over wholesale.
    prior_general = next(
        (
            s for s in (prior_headline.get("llm_slots") or [])
            if s.get("slot_type") == "general"
            and s.get("status") == "generated"
            and s.get("content")
        ),
        None,
    )
    if prior_general:
        cur = report.headline.llm_slots
        live = bool(cur and cur[0].status == "generated" and cur[0].content)
        if not live:
            report.headline.llm_slots = [LLMSlot(
                slot_type="general",
                grounded_in=prior_general.get("grounded_in") or [],
                status="generated",
                content=prior_general.get("content"),
            )]
            grafted += 1

    return grafted
