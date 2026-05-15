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
import time
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
    started_monotonic = time.monotonic()

    def _persist_log(result: "PeriodicRunResult", error: str | None = None) -> None:
        """Best-effort persistence to periodic_run_log. A failure here
        never escalates — the caller's PeriodicRunResult is the source
        of truth for the cycle's outcome."""
        try:
            import periodic_run_log
            periodic_run_log.log_run(
                action_taken=result.action_taken,
                reason=result.reason,
                data_period=result.data_period,
                findings_path=result.findings_path,
                analyser_counts=result.analyser_counts or None,
                duration_ms=int((time.monotonic() - started_monotonic) * 1000),
                forced=force,
                skip_llm=skip_llm,
                error=error,
            )
        except Exception:
            log.exception("Failed to write periodic_run_log row")

    latest_data = briefing_pack.latest_eurostat_period()
    latest_published = briefing_pack.latest_recorded_data_period(
        trigger="periodic_run"
    )

    log.info(
        "periodic-run: latest_eurostat=%s latest_published_periodic=%s force=%s",
        latest_data, latest_published, force,
    )

    if latest_data is None:
        result = PeriodicRunResult(
            action_taken=False,
            reason="no Eurostat data ingested yet; ingest a period first",
            data_period=None,
            findings_path=None,
            leads_path=None,
        )
        _persist_log(result)
        return result

    if not force and latest_published is not None and latest_published >= latest_data:
        result = PeriodicRunResult(
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
        _persist_log(result)
        return result

    # --- Step 2: run all analyser kinds across all scope/flow combos. ---
    counts: dict[str, Any] = {}

    def _run_analyser(
        key: str, subkind: str, fn, *,
        scope: str | None = None, flow_label: int | str | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Invoke an analyser, time it, and persist a findings_emit_log row.
        `flow_label` is the value to record in findings_emit_log.flow (the
        analyser's own flow kwarg is in **kwargs, which may be int or str).
        Best-effort: log failures don't escalate."""
        log.info("periodic-run: running %s", key)
        t_start = time.monotonic()
        result = fn(**kwargs)
        duration_ms = int((time.monotonic() - t_start) * 1000)
        try:
            import findings_emit_log
            # Coerce flow_label: schema column is INT, but the gacc-aggregate
            # analysers take flow='export'/'import'. Map those to 2/1 for
            # storage so the column stays numeric-comparable.
            flow_int: int | None
            if flow_label is None:
                flow_int = None
            elif isinstance(flow_label, int):
                flow_int = flow_label
            else:
                flow_int = 2 if flow_label == "export" else 1
            findings_emit_log.log_run(
                scrape_run_id=None,
                analyser_method=subkind,
                subkind=subkind,
                comparison_scope=scope,
                flow=flow_int,
                counts=dict(result) if isinstance(result, dict) else {"raw": str(result)},
                duration_ms=duration_ms,
            )
        except Exception:
            log.exception("Failed to write findings_emit_log row for %s", key)
        return result

    counts["mirror_trade"] = _run_analyser(
        "mirror_trade", "mirror_gap", anomalies.detect_mirror_trade_gaps,
    )
    counts["mirror_gap_trends"] = _run_analyser(
        "mirror_gap_trends", "mirror_gap_zscore", anomalies.detect_mirror_gap_trends,
    )

    for scope in ("eu_27", "uk", "eu_27_plus_uk"):
        for flow in (1, 2):
            key = f"hs_group_yoy_{scope}_flow{flow}"
            counts[key] = _run_analyser(
                key, "hs_group_yoy", anomalies.detect_hs_group_yoy,
                scope=scope, flow_label=flow,
                flow=flow, comparison_scope=scope,
            )
            traj_key = f"hs_group_trajectory_{scope}_flow{flow}"
            counts[traj_key] = _run_analyser(
                traj_key, "hs_group_trajectory",
                anomalies.detect_hs_group_trajectories,
                scope=scope, flow_label=flow,
                flow=flow, comparison_scope=scope,
            )

    for flow_str in ("export", "import"):
        key = f"gacc_aggregate_yoy_{flow_str}"
        counts[key] = _run_analyser(
            key, "gacc_aggregate_yoy", anomalies.detect_gacc_aggregate_yoy,
            flow_label=flow_str,
            flow=flow_str,
        )

    for flow_str in ("export", "import"):
        key = f"gacc_bilateral_aggregate_yoy_{flow_str}"
        counts[key] = _run_analyser(
            key, "gacc_bilateral_aggregate_yoy",
            anomalies.detect_gacc_bilateral_aggregate_yoy,
            flow_label=flow_str,
            flow=flow_str,
        )

    # Partner-share runs ONLY if the eurostat_world_aggregates table has
    # data for the latest 12 months — otherwise the analyser skips with
    # `skipped_no_world_data`. The periodic step is light when there's
    # no fresh denominator, heavy when there is.
    for flow_int_ in (1, 2):
        key = f"partner_share_flow{flow_int_}"
        counts[key] = _run_analyser(
            key, "partner_share", anomalies.detect_partner_share,
            flow_label=flow_int_,
            flow=flow_int_,
        )

    if not skip_llm:
        log.info("periodic-run: running llm-framing")
        t_start_llm = time.monotonic()
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
        try:
            import findings_emit_log
            llm_counts = counts["llm_framing"]
            findings_emit_log.log_run(
                scrape_run_id=None,
                analyser_method="llm_topline_v2_lead_scaffold",
                subkind="narrative_hs_group",
                counts=llm_counts if isinstance(llm_counts, dict) else {"raw": str(llm_counts)},
                duration_ms=int((time.monotonic() - t_start_llm) * 1000),
            )
        except Exception:
            log.exception("Failed to write findings_emit_log row for llm_framing")
    else:
        counts["llm_framing"] = {"skipped": True}

    # --- Step 3: write the findings-export bundle. ---
    log.info("periodic-run: writing findings export")
    findings_path, leads_path = briefing_pack.export(
        out_dir=out_dir,
        top_n=top_n,
        trigger="periodic_run",
    )

    result = PeriodicRunResult(
        action_taken=True,
        reason=f"new export written for data_period {latest_data}",
        data_period=latest_data,
        findings_path=findings_path,
        leads_path=leads_path,
        analyser_counts=counts,
    )
    _persist_log(result)
    return result
