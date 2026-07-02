"""Periodic-run orchestration — the deployment-agnostic pipeline that
wraps ingest → analyse → render into a single idempotent CLI invocation.

Design philosophy (see dev_notes/2026-05-11-periodic-runs-design.md for
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
import portal_publish  # cheap — its GCS deps are lazy-imported inside functions

from briefing_pack._helpers import (
    _bundle_root,
    _new_data_phrase_since_last_brief,
)

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

    bundle_dir: str | None = None
    """Top-level export-folder path when action_taken (the dir to hand to
    `--upload-to-drive`), else None."""

    new_data: str | None = None
    """One-line phrase naming the source releases (GACC / Eurostat / HMRC)
    first seen since the previous cycle, e.g. 'GACC March 2026
    (preliminary); Eurostat March 2026'. Empty string when nothing new has
    arrived (a forced rerun). None when not computed."""

    portal_dir: str | None = None
    """Path to the 04_Portal/ snapshot folder (report.json + index.html) when
    written, else None. Additive and best-effort: a portal failure leaves this
    None but the findings/docx bundle still ships."""

    next_releases: list[tuple[str, date]] = dataclasses.field(default_factory=list)
    """Forecast of the next upcoming source releases — (source, scheduled-date)
    pairs sorted soonest-first — rendered as the 'Next changes expected:' line
    in summary(). Empty when no source has both a calendar and prior data."""

    def summary(self) -> str:
        """Human-readable per-run report for the scheduled Routine to
        surface. When a briefing was generated it includes which sources
        brought new data and the exact manual command to publish it to
        Drive — we deliberately do NOT auto-upload (the shared folder is
        journalist-facing and the format is still under review). The closing
        'Next changes expected:' line forecasts the next upcoming source
        releases and is appended on both the action and no-op paths (it is most
        useful precisely when nothing landed this cycle)."""
        if not self.action_taken:
            lines = [f"Periodic run: no new briefing this cycle — {self.reason}"]
        else:
            lines = [
                f"Periodic run: new briefing generated for data period "
                f"{self.data_period}."
            ]
            if self.new_data:
                lines.append(f"  New source data since the last cycle: {self.new_data}.")
            elif self.new_data == "":
                lines.append(
                    "  No new source releases since the last cycle "
                    "(forced rerun against the same data)."
                )
            if self.bundle_dir:
                lines.append(f"  Bundle written to: {self.bundle_dir}")
                if self.portal_dir:
                    lines.append(f"  Portal snapshot: {self.portal_dir}")
                lines.append("  To publish it to Google Drive, run:")
                lines.append(
                    f"    python scrape.py --upload-to-drive {self.bundle_dir}"
                )
        next_line = _format_next_releases(self.next_releases)
        if next_line:
            lines.append(next_line)
        return "\n".join(lines)


# Display names for the "Next changes expected:" forecast line — the source
# keys (eurostat/hmrc/gacc) rendered as the journalist sees them.
_SOURCE_DISPLAY = {"eurostat": "Eurostat", "hmrc": "HMRC", "gacc": "GACC"}


def _format_next_releases(forecast: list[tuple[str, date]]) -> str | None:
    """Render the forecast as the 'Next changes expected:' line, or None when
    the forecast is empty. e.g.
    'Next changes expected: GACC (due July 8); HMRC (due July 16)'."""
    if not forecast:
        return None
    parts = [
        f"{_SOURCE_DISPLAY.get(src, src)} (due {due:%B} {due.day})"
        for src, due in forecast
    ]
    return "Next changes expected: " + "; ".join(parts)


def _next_releases_forecast(limit: int | None = 2) -> list[tuple[str, date]]:
    """Forecast the next `limit` upcoming source releases for the run summary.

    Reads MAX(period) per source from the releases table and hands it to the
    pure `release_calendar.next_release_forecast`. Best-effort: any DB hiccup
    yields an empty forecast (the summary simply omits the line) rather than
    sinking the run."""
    import release_calendar
    try:
        import db
        with db.transaction() as conn, conn.cursor() as cur:
            cur.execute("SELECT source, MAX(period) FROM releases GROUP BY source")
            latest_by_source = {src: mx for src, mx in cur.fetchall()}
    except Exception:
        log.exception("periodic-run: failed to compute next-release forecast")
        return []
    return release_calendar.next_release_forecast(latest_by_source, limit=limit)


def write_portal_snapshot(
    bundle_dir: str, data_period, *, generate_takes: bool,
    write_workbook: bool = False,
    reuse_takes: bool = False, portal_bucket: str | None = None,
    prior_report: dict | None = None, publishing: bool = False,
) -> str | None:
    """Write the portal snapshot into `<bundle_dir>/04_Portal/`: report.json
    (the canonical published snapshot the web portal serves) + index.html (a
    rendered preview). Best-effort — returns the dir on success, None on any
    failure, so a portal problem never disturbs the findings/docx bundle.
    Eurostat-triggered cycle → the eurostat variant. `generate_takes` runs the
    per-finding LLM takes (needs a backend); default off.

    Read-only: it builds from existing findings and writes two files — it
    records NO brief_runs row. That's load-bearing for the standalone
    `--portal-snapshot` caller, which must refresh the portal on demand
    without advancing the subscriber cycle or moving the 'since last brief'
    baseline.

    `write_workbook=True` also builds `<bundle_dir>/04_Data.xlsx` (the workbook
    the Tables-tab "Download Excel" button links to) so a standalone snapshot
    publish actually has it; periodic-run leaves it False because export()
    already wrote that file beside the bundle.

    `reuse_takes=True` (opt-in, and only when NOT generating fresh takes) carries
    the PREVIOUS LLM takes forward instead of leaving them empty — for amending
    an existing release without re-paying the API. It reads the live snapshot
    (`prior_report` if injected, else `portal_bucket`'s latest/report.json) and
    grafts takes whose data_period + finding id still match (see
    `portal_takes_reuse`).

    `publishing=True` says this snapshot is about to go live (not a
    `--portal-no-publish` preview). It makes the prior-snapshot read STRICT: a
    genuine read error (GCS/auth/parse — not "no prior yet") raises
    `portal_publish.PriorSnapshotUnreadable` instead of falling back to empty
    takes, so we refuse to publish a takes-less portal while reporting success.
    With `publishing=False` (preview, or the periodic-run path) reuse stays
    best-effort: any failure leaves empty takes and never sinks the snapshot.
    The graft itself is always best-effort either way."""
    try:
        from pathlib import Path
        import report_model
        from report_builder import build_report
        from report_render_html import render_html
        report = build_report(
            source_trigger="eurostat", data_period=data_period,
            generate_takes=generate_takes,
        )
        # Sticky takes: carry prior LLM takes onto this LLM-less rebuild. Only
        # when reuse is asked for AND we didn't just generate fresh ones.
        if reuse_takes and not generate_takes:
            import portal_takes_reuse
            # Read the prior snapshot STRICTLY when publishing: a genuine read
            # error must not be read as "no prior" and silently empty the takes.
            # PriorSnapshotUnreadable propagates (caught below + re-raised past
            # the best-effort wrapper) so the publish is refused, not faked.
            prior = prior_report
            if prior is None:
                prior = portal_publish.read_latest_report(
                    portal_bucket, required=publishing)
            if prior is None:
                log.warning(
                    "portal snapshot: --portal-reuse-takes set but no prior "
                    "snapshot available (need --portal-bucket / PORTAL_BUCKET "
                    "with a readable latest/report.json); takes will be empty"
                )
            else:
                # The graft itself stays best-effort — a shape mismatch in the
                # prior blob shouldn't sink the snapshot.
                try:
                    n = portal_takes_reuse.graft_prior_takes(report, prior)
                    log.info("portal snapshot: grafted %d prior take(s) "
                             "(reuse — no LLM spend)", n)
                except Exception:
                    log.exception("portal snapshot: take graft failed; "
                                  "continuing with empty takes")
        pdir = Path(bundle_dir) / "04_Portal"
        pdir.mkdir(parents=True, exist_ok=True)
        (pdir / "report.json").write_text(report_model.to_json(report))
        (pdir / "index.html").write_text(render_html(report))
        if write_workbook:
            # The Tables tab's "Download Excel workbook" button links to
            # /data.xlsx, which publish_snapshot serves from
            # `<bundle_dir>/04_Data.xlsx`. A standalone --portal-snapshot has no
            # briefing-pack run to produce that workbook, so build it here —
            # otherwise the download 404s. Isolated + best-effort: a workbook
            # failure logs but never sinks the publish (report.json + index.html
            # are already written; the button would just 404 as before).
            try:
                import sheets_export
                xlsx_path = Path(bundle_dir) / "04_Data.xlsx"
                sheets_export.XlsxWriter().write(
                    sheets_export.assemble_sheets(), str(xlsx_path),
                )
                log.info("portal snapshot: wrote workbook to %s", xlsx_path)
            except Exception:
                log.exception(
                    "portal snapshot: workbook build failed; portal published "
                    "without the Tables-tab download"
                )
        log.info("periodic-run: wrote portal snapshot to %s", pdir)
        return str(pdir)
    except portal_publish.PriorSnapshotUnreadable:
        # Deliberate fail-loud: a publish asked to carry prior takes forward but
        # the prior snapshot couldn't be read. Refuse rather than ship empty
        # takes while reporting success. The caller surfaces it actionably.
        raise
    except Exception:
        log.exception(
            "periodic-run: portal snapshot failed; continuing "
            "(findings bundle shipped)"
        )
        return None


def run_periodic(
    *,
    force: bool = False,
    out_dir: str | None = None,
    top_n: int = briefing_pack.DEFAULT_TOP_N,
    llm_model: str | None = None,
    skip_llm: bool = False,
    docx: bool = False,  # docx→Drive is legacy (see Step 3 comment); .md + .xlsx + portal snapshot are the live surfaces
    generate_takes: bool = False,
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
      5. Write the portal snapshot (04_Portal/report.json + index.html) into
         the same bundle — additive and best-effort. `generate_takes=True`
         also runs the per-finding LLM takes (needs an LLM backend); off by
         default.

    The function is deliberately non-fetching: it works against whatever
    Eurostat/HMRC/GACC data is already in the DB. Trigger the fetch
    upstream (e.g. via `python scrape.py --eurostat-period YYYY-MM` in
    the same routine, before this call) — keeping fetch and pipeline as
    separate concerns means a network failure during fetch doesn't leave
    the analyser pipeline in an in-flight state.

    A crash anywhere mid-cycle still writes a periodic_run_log row
    (action_taken=False, the exception in `error`) before propagating,
    so `--periodic-history` and the Chat notifier can distinguish
    "the cycle broke" from "the cycle never ran".
    """
    started_monotonic = time.monotonic()
    try:
        return _run_periodic_cycle(
            started_monotonic,
            force=force, out_dir=out_dir, top_n=top_n, llm_model=llm_model,
            skip_llm=skip_llm, docx=docx, generate_takes=generate_takes,
        )
    except Exception as exc:
        try:
            import periodic_run_log
            periodic_run_log.log_run(
                action_taken=False,
                reason=f"cycle crashed: {type(exc).__name__}",
                data_period=None,
                findings_path=None,
                analyser_counts=None,
                duration_ms=int((time.monotonic() - started_monotonic) * 1000),
                forced=force,
                skip_llm=skip_llm,
                error=str(exc) or type(exc).__name__,
            )
        except Exception:
            log.exception("Failed to write periodic_run_log row for crashed cycle")
        raise


def _run_periodic_cycle(
    started_monotonic: float,
    *,
    force: bool,
    out_dir: str | None,
    top_n: int,
    llm_model: str | None,
    skip_llm: bool,
    docx: bool,
    generate_takes: bool,
) -> PeriodicRunResult:
    """Body of run_periodic — see its docstring."""

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

    # Forecast the next upcoming source releases once, up front — attached to
    # every PeriodicRunResult below so the 'Next changes expected:' line shows
    # on the action and no-op paths alike. limit=None → every calendar source
    # (so a co-due source is never silently dropped).
    next_releases = _next_releases_forecast(limit=None)

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
            next_releases=next_releases,
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
            next_releases=next_releases,
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

    # Biggest single-product (CN8) mover within the watched HS prefixes — the
    # finer-grained companion to the hs_group_yoy movers (roadmap "Biggest mover
    # KPI", Option A). Imports only (flow=1) in v1; reads eurostat_raw_rows, so
    # it's a no-op cost on cycles without fresh Eurostat detail.
    counts["cn8_biggest_mover"] = _run_analyser(
        "cn8_biggest_mover", "cn8_yoy_mover", anomalies.detect_cn8_biggest_mover,
        flow_label=1, flow=1,
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

    # EU–China trade balance (the all-goods bilateral deficit, framed per
    # day). No flow axis — the balance IS imports minus exports — so it
    # runs once and emits both partner scopes (CN+HK+MO and CN-only)
    # internally. Eurostat-only today; cheap (reads the 000TOTAL aggregate
    # rows, not the CN8 detail).
    counts["trade_balance"] = _run_analyser(
        "trade_balance", "trade_balance",
        anomalies.detect_eu_china_trade_balance,
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

    # China's share of EU all-goods trade — the dependency donut + trend.
    # All-goods generalisation of partner_share; same denominator dependency
    # (eurostat_world_aggregates 000TOTAL), so it sits alongside it and is a
    # light no-op when the denominator isn't fresh.
    counts["china_all_goods_share"] = _run_analyser(
        "china_all_goods_share", "china_all_goods_share",
        anomalies.detect_china_all_goods_share,
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
    # The live Lisa-facing surface is the web portal (snapshot written in
    # Step 5), NOT the .docx → Google Drive bundle, which is legacy: the
    # docx→Drive pipeline predates the portal and is unlikely to be used again
    # (Luke, 2026-06). So `docx` defaults to False — the cycle emits the .md
    # (deterministic findings/leads) and .xlsx (data) surfaces plus the portal
    # snapshot, and skips the per-cycle .docx render. Pass docx=True (or run
    # `--briefing-pack --docx` by hand) for a one-off .docx bundle if ever
    # needed; the rendering code is retained, just not run every cycle.
    # Which sources brought new data this cycle, for the run summary.
    # Compute BEFORE export() records this run's brief_runs row, so the
    # latest row is still the previous cycle. None (not "") on failure, so
    # the summary stays silent rather than falsely reporting a forced rerun.
    try:
        new_data_phrase = _new_data_phrase_since_last_brief()
    except Exception:
        log.exception("periodic-run: failed to compute new-data phrase")
        new_data_phrase = None

    log.info("periodic-run: writing findings export")
    findings_path, leads_path = briefing_pack.export(
        out_dir=out_dir,
        top_n=top_n,
        trigger="periodic_run",
        docx=docx,
    )

    # The top-level export folder is what `--upload-to-drive` takes.
    bundle_dir = str(_bundle_root(findings_path))

    # --- Step 5: portal snapshot into the same bundle (additive, best-effort). ---
    log.info("periodic-run: building portal snapshot (takes=%s)", generate_takes)
    portal_dir = write_portal_snapshot(
        bundle_dir, latest_data, generate_takes=generate_takes,
    )

    result = PeriodicRunResult(
        action_taken=True,
        reason=f"new export written for data_period {latest_data}",
        data_period=latest_data,
        findings_path=findings_path,
        leads_path=leads_path,
        analyser_counts=counts,
        bundle_dir=bundle_dir,
        new_data=new_data_phrase,
        portal_dir=portal_dir,
        next_releases=next_releases,
    )
    _persist_log(result)
    return result
