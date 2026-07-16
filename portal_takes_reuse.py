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

    # Per-finding takes: ANCHOR finding id -> the prior take, keyed on
    # grounded_in[0] only. grounded_in is now the take's full citation list
    # (anchor first, then cross-flow findings its questions cite) — keying on
    # every id would let an export slot's take graft onto the import slot it
    # merely cited for contrast.
    by_finding: dict[int, dict] = {}
    for it in prior_headline.get("items") or []:
        take = it.get("take") or {}
        grounded = take.get("grounded_in") or []
        if take.get("status") == "generated" and take.get("questions") and grounded:
            by_finding[grounded[0]] = take

    grafted = 0
    for item in report.headline.items:
        if item.take is not None and item.take.status == "generated":
            continue  # never clobber a live take
        fids = item.provenance.finding_ids or []
        prior_take = next((by_finding[f] for f in fids if f in by_finding), None)
        if prior_take:
            item.take = LLMSlot(
                slot_type="specific",
                grounded_in=prior_take.get("grounded_in") or [fids[0]],
                status="generated",
                questions=prior_take["questions"],
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


def graft_gacc_slots(report: Report, prior: dict) -> int:
    """Carry the GACC page's LLM slots (synthesis + questions) forward from a
    prior snapshot, in place. Returns the number of slots grafted.

    Deliberately SEPARATE from graft_prior_takes and gated on the GACC
    page's OWN data_period, not the report's: the two tracks advance a month
    apart, so a main-track rebuild (meta.data_period moved, GACC month
    unchanged) must still carry the GACC slots, and a GACC-track new-period
    rebuild (GACC month moved) must drop them for fresh generation even
    though meta.data_period is unchanged. Freshly-generated slots are never
    clobbered."""
    gp = report.gacc_page
    prior_gp = prior.get("gacc_page") or {}
    if gp is None or gp.data_period is None or not prior_gp:
        return 0
    new_period = (gp.data_period.isoformat()
                  if hasattr(gp.data_period, "isoformat")
                  else str(gp.data_period))
    if prior_gp.get("data_period") != new_period:
        log.info(
            "reuse-takes: prior gacc_page data_period %r != %r; not grafting "
            "(fresh gacc slots expected on a new GACC month)",
            prior_gp.get("data_period"), new_period,
        )
        return 0
    grafted = 0
    if gp.synthesis is None and prior_gp.get("synthesis"):
        gp.synthesis = prior_gp["synthesis"]
        grafted += 1
    if gp.commodity_take is None and prior_gp.get("commodity_take"):
        gp.commodity_take = prior_gp["commodity_take"]
        grafted += 1
    prior_q = prior_gp.get("questions") or {}
    if (gp.questions is None and prior_q.get("status") == "generated"
            and prior_q.get("questions")):
        gp.questions = LLMSlot(
            slot_type="general",
            grounded_in=prior_q.get("grounded_in") or [],
            status="generated",
            questions=prior_q["questions"],
        )
        grafted += 1
    return grafted
