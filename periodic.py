"""Periodic-run orchestration — the deployment-agnostic pipeline that
wraps ingest → analyse → render into a single idempotent CLI invocation.

Design philosophy (see dev_notes/periodic-runs-design-2026-05-11.md for
the full discussion):

- This module knows nothing about *where* it runs (Claude Code routine,
  GHA cron, manual invocation, hosted cron) or *how* the output is
  delivered (manual email, Slack, S3, etc.). It writes a findings-export
  bundle to a folder; the surrounding scheduler/delivery layer is the
  caller's problem.
- Idempotency is the load-bearing property. If the latest Eurostat data
  in the DB is no fresher than what the most recent periodic-run cycle
  already published, the function exits cleanly without re-running. The
  caller can detect this via the return value.
- On-demand renders (trigger='manual') and cycle renders
  (trigger='periodic_run') are tracked separately. Only the latter advance
  the subscriber-facing sequence. A new user pulling an on-demand bundle
  does NOT alter the cycle for everyone else.

This module is the only place that orchestrates the multi-step pipeline.
Each individual analyser remains independently invocable via scrape.py
(--analyse <kind>) for ad-hoc work.
"""
from __future__ import annotations

import dataclasses
import logging
from datetime import date
from typing import Any

import anomalies
import briefing_pack
import llm_framing

log = logging.getLogger(__name__)


@dataclasses.dataclass
class PeriodicRunResult:
    """Return value of run_periodic(). Contains everything a scheduler
    layer needs to decide what to do next: whether anything ran, what
    period it covers, where the output landed."""

    action_taken: bool
    """True if a new findings export was generated; False if the call
    was a no-op because nothing fresher was available."""

    reason: str
    """Human-readable explanation of action_taken — printable to stdout
    so a routine wrapper can log it. Examples: 'data_period 2026-02 '
    'already published in periodic-run cycle 47'; 'new export written'."""

    data_period: date | None
    """The Eurostat period this run reflects (latest period in the DB at
    the time of the run). None if no Eurostat data is ingested yet."""

    findings_path: str | None
    """Absolute path to the new findings.md if action_taken, else None."""

    leads_path: str | None
    """Absolute path to the new leads.md if action_taken, else None."""

    analyser_counts: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Per-analyser-step result dicts, keyed by step name. Useful for
    surfacing in routine logs without re-reading the export bundle."""


def run_periodic(
    *,
    force: bool = False,
    out_dir: str | None = None,
    top_n: int = briefing_pack.DEFAULT_TOP_N,
    llm_model: str | None = None,
    skip_llm: bool = False,
) -> PeriodicRunResult:
    """Run the full periodic cycle end-to-end.

    Sequence:
      1. Idempotency check — return no-op if the latest Eurostat period
         is no newer than what the previous periodic-run cycle published.
         `force=True` skips this and always runs.
      2. Re-run every active analyser kind across all scope/flow combos.
         Each analyser is independently idempotent at the per-finding
         level: rows that haven't changed are confirmed, rows that have
         shifted produce supersede chains. No double-publishes.
      3. Re-run llm-framing for any HS group whose underlying YoY findings
         have shifted (or for all groups on first pass). `skip_llm=True`
         omits this step; useful for fast iterations or when Ollama is
         unavailable.
      4. Generate the bundled findings export (findings.md + leads.md +
         data.xlsx) with trigger='periodic_run' so the row in brief_runs
         is recognised as part of the global subscriber cycle.

    The function is deliberately non-fetching: it works against whatever
    Eurostat/HMRC/GACC data is already in the DB. Trigger the fetch
    upstream (e.g. via `python scrape.py --eurostat-period YYYY-MM` in
    the same routine, before this call) — keeping fetch and pipeline as
    separate concerns means a network failure during fetch doesn't leave
    the analyser pipeline in an in-flight state.
    """
    latest_data = briefing_pack.latest_eurostat_period()
    latest_published = briefing_pack.latest_recorded_data_period(
        trigger="periodic_run"
    )

    log.info(
        "periodic-run: latest_eurostat=%s latest_published_periodic=%s force=%s",
        latest_data, latest_published, force,
    )

    if latest_data is None:
        return PeriodicRunResult(
            action_taken=False,
            reason="no Eurostat data ingested yet; ingest a period first",
            data_period=None,
            findings_path=None,
            leads_path=None,
        )

    if not force and latest_published is not None and latest_published >= latest_data:
        return PeriodicRunResult(
            action_taken=False,
            reason=(
                f"data_period {latest_data} already published by a previous "
                f"periodic-run cycle (latest_published={latest_published}); "
                f"pass --force to re-run anyway"
            ),
            data_period=latest_data,
            findings_path=None,
            leads_path=None,
        )

    # --- Step 2: run all analyser kinds across all scope/flow combos. ---
    counts: dict[str, Any] = {}

    log.info("periodic-run: running mirror-trade and mirror-gap-trends")
    counts["mirror_trade"] = anomalies.detect_mirror_trade_gaps()
    counts["mirror_gap_trends"] = anomalies.detect_mirror_gap_trends()

    for scope in ("eu_27", "uk", "eu_27_plus_uk"):
        for flow in (1, 2):
            key = f"hs_group_yoy_{scope}_flow{flow}"
            log.info("periodic-run: running %s", key)
            counts[key] = anomalies.detect_hs_group_yoy(
                flow=flow, comparison_scope=scope,
            )
            traj_key = f"hs_group_trajectory_{scope}_flow{flow}"
            log.info("periodic-run: running %s", traj_key)
            counts[traj_key] = anomalies.detect_hs_group_trajectories(
                flow=flow, comparison_scope=scope,
            )

    for flow_str in ("export", "import"):
        key = f"gacc_aggregate_yoy_{flow_str}"
        log.info("periodic-run: running %s", key)
        counts[key] = anomalies.detect_gacc_aggregate_yoy(flow=flow_str)

    for flow_str in ("export", "import"):
        key = f"gacc_bilateral_aggregate_yoy_{flow_str}"
        log.info("periodic-run: running %s", key)
        counts[key] = anomalies.detect_gacc_bilateral_aggregate_yoy(flow=flow_str)

    if not skip_llm:
        log.info("periodic-run: running llm-framing")
        try:
            counts["llm_framing"] = llm_framing.detect_llm_framings(
                model=llm_model,
            )
        except Exception as exc:
            # LLM step failures shouldn't kill the cycle — the
            # deterministic findings document is what subscribers depend
            # on; leads.md is best-effort. Log and continue.
            log.warning(
                "periodic-run: llm-framing failed (%s); continuing without "
                "fresh leads", exc,
            )
            counts["llm_framing"] = {"error": str(exc)}
    else:
        counts["llm_framing"] = {"skipped": True}

    # --- Step 3: write the findings-export bundle. ---
    log.info("periodic-run: writing findings export")
    findings_path, leads_path = briefing_pack.export(
        out_dir=out_dir,
        top_n=top_n,
        trigger="periodic_run",
    )

    return PeriodicRunResult(
        action_taken=True,
        reason=f"new export written for data_period {latest_data}",
        data_period=latest_data,
        findings_path=findings_path,
        leads_path=leads_path,
        analyser_counts=counts,
    )
