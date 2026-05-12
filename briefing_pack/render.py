"""Top-level orchestrator for the briefing-pack package.

`render()` assembles the findings.md document by calling each section module
in document order. `render_leads()` builds the leads.md companion. `export()`
writes both to disk (plus the data.xlsx via sheets_export) and records the
brief_runs audit row.

The periodic-run pipeline (periodic.py) uses `latest_eurostat_period()` and
`latest_recorded_data_period()` for idempotency checks: if the latest
Eurostat data we hold is no newer than the data_period stamped on the most
recent periodic-run output, there is nothing new to publish."""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import psycopg2.extras

from briefing_pack._helpers import (
    DEFAULT_TOP_N,
    _Section,
    _compute_predictability_per_group,
    _conn,
    _slugify_scope,
)
from briefing_pack.sections.about_findings import _section_about_findings
from briefing_pack.sections.detail_opener import _section_detail_opener
from briefing_pack.sections.diff import _section_diff_since_last_brief
from briefing_pack.sections.headline import _section_headline
from briefing_pack.sections.hs_yoy_movers import _section_hs_yoy_movers
from briefing_pack.sections.llm_narratives import _section_llm_narratives
from briefing_pack.sections.low_base import _section_low_base
from briefing_pack.sections.methodology_footer import _section_methodology_footer
from briefing_pack.sections.mirror_gaps import _section_mirror_gaps
from briefing_pack.sections.reader_guide import _section_reader_guide
from briefing_pack.sections.sources_appendix import _section_sources_appendix
from briefing_pack.sections.state_of_play import _section_state_of_play
from briefing_pack.sections.state_of_play_aggregates import (
    _section_state_of_play_aggregates,
)
from briefing_pack.sections.state_of_play_bilaterals import (
    _section_state_of_play_bilaterals,
)
from briefing_pack.sections.trajectories import _section_trajectories

log = logging.getLogger(__name__)


def latest_eurostat_period() -> date | None:
    """Return the most recent Eurostat release period in the DB, or None
    if no Eurostat data is ingested. Used by the periodic-run orchestrator
    to decide whether a new findings export is warranted and to stamp the
    new brief_runs row with the data freshness it reflects."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(period) FROM releases WHERE source = 'eurostat'"
        )
        row = cur.fetchone()
        return row[0] if row else None


def latest_recorded_data_period(trigger: str | None = None) -> date | None:
    """Return the data_period of the most recently-recorded findings export,
    optionally filtered to a specific trigger ('manual' or 'periodic_run').
    Returns None if there are no recorded exports with a populated
    data_period. Used by the periodic-run orchestrator for idempotency:
    if the latest Eurostat data is no fresher than the most recent
    periodic-run output, there is nothing new to publish."""
    with _conn() as conn, conn.cursor() as cur:
        if trigger is None:
            cur.execute(
                "SELECT MAX(data_period) FROM brief_runs "
                "WHERE data_period IS NOT NULL"
            )
        else:
            cur.execute(
                "SELECT MAX(data_period) FROM brief_runs "
                "WHERE data_period IS NOT NULL AND trigger = %s",
                (trigger,),
            )
        row = cur.fetchone()
        return row[0] if row else None


def _record_brief_run(
    out_path: str | None,
    top_n: int,
    data_period: date | None = None,
    trigger: str = "manual",
) -> None:
    """Insert a row into brief_runs after a successful findings export.
    Called by export() — render() doesn't write since callers may render
    for non-archival purposes (preview, test).

    `data_period` stamps the export with the freshness of the underlying
    Eurostat data; the periodic-run orchestrator uses it for idempotency
    checks. `trigger` distinguishes manual ad-hoc renders from periodic-run
    cycle outputs — only the latter advance the global subscriber cycle.
    """
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO brief_runs (output_path, top_n, data_period, trigger) "
            "VALUES (%s, %s, %s, %s)",
            (out_path, top_n, data_period, trigger),
        )


def render(
    top_n: int = DEFAULT_TOP_N,
    companion_filename: str | None = None,
    scope_label: str | None = None,
) -> str:
    """Render the full briefing pack as a single Markdown string.

    `companion_filename` (when provided): the basename of the paired
    leads document. The headline paragraph cites it directly so a reader
    can find the LLM-scaffolded leads alongside. Set automatically by
    `export()`; pass None for ad-hoc renders that have no paired file.

    `scope_label` (when provided): a human-readable scope description
    surfaced in the headline so a brief shared standalone still
    announces what slice of the data it covers (None = full brief).
    """
    sections: list[_Section] = []
    release_ids: set[int] = set()
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # ----- Front matter: title, reader's guide, scope/caveat setup -----
        sections.append(_section_headline(
            cur, companion_filename=companion_filename, scope_label=scope_label,
        ))
        sections.append(_section_reader_guide())

        # Phase: per-group YoY-predictability badges. Computed once and
        # passed into the state-of-play section and each per-scope mover
        # section. Empty dict on a fresh DB with no T-6 history; the
        # section renderer falls back to no badge in that case.
        predictability = _compute_predictability_per_group(cur)

        # ----- Tier 1: what's new this cycle (the diff) -----
        # Excludes narrative_hs_group findings — those live in the companion
        # leads file, not the brief. The function emits its own `## Tier 1`
        # heading + the `---` separator above it. Empty case (first-ever brief
        # or nothing material changed) still emits the heading with a
        # baseline-explainer paragraph.
        sections.append(_section_diff_since_last_brief(cur))

        # ----- Tier 2: current state of play (compact summary) -----
        # Per-HS-group block first (the EU-CN deep-dive view), then the
        # bilateral block (EU + single-country GACC partners, the Soapbox
        # editorial register), then the non-EU partner-aggregate block
        # (ASEAN / RCEP / Belt&Road / Africa / Latin America / world Total).
        # A reader scanning Tier 2 sees per-group detail → bilateral
        # bloc-and-country → multi-country aggregates, narrowing scope
        # rather than widening.
        sections.append(_section_state_of_play(cur, predictability))
        sections.append(_section_state_of_play_bilaterals(cur))
        sections.append(_section_state_of_play_aggregates(cur))

        # ----- Tier 3: full per-finding detail by HS group -----
        # The opener emits the `## Tier 3` heading + `---`. The detail
        # sections below all use `###` headings so they sit under the
        # Tier 3 parent. Per-scope blocks (Phase 6.1e): each scope renders
        # its own YoY top-movers + trajectory sections so EU-27 / UK /
        # combined views are distinct sub-blocks. Scopes with no findings
        # return empty markdown and are dropped by the join filter.
        sections.append(_section_detail_opener())
        for scope in ("eu_27", "uk", "eu_27_plus_uk"):
            for flow in (1, 2):
                sec = _section_hs_yoy_movers(
                    cur, flow=flow, top_n=top_n, comparison_scope=scope,
                    predictability=predictability,
                )
                sections.append(sec)
                release_ids |= sec.release_ids
            sec = _section_trajectories(cur, comparison_scope=scope)
            sections.append(sec)
            release_ids |= sec.release_ids

        sec = _section_mirror_gaps(cur)
        sections.append(sec)
        release_ids |= sec.release_ids

        sec = _section_low_base(cur)
        sections.append(sec)
        release_ids |= sec.release_ids

        # ----- Endmatter: methodology footer, source citations, endnote.
        # Universal caveats live here (not at the top) so a journalist
        # scanning the document hits findings first; the methodology block
        # is reference material they consult on demand, not the lead.
        sections.append(_section_methodology_footer(cur))
        sections.append(_section_sources_appendix(cur, release_ids))
        sections.append(_section_about_findings())

    return "\n".join(s.markdown for s in sections if s.markdown).rstrip() + "\n"


def render_leads(
    companion_filename: str | None = None,
    scope_label: str | None = None,
) -> str:
    """Render the LLM lead-scaffold companion document. Standalone — does
    not depend on the brief — but cross-references finding IDs that the
    brief also surfaces. Lives in its own document so a downstream LLM
    tool (NotebookLM, etc.) can choose to consume the deterministic
    brief, the leads, both, or neither, without one being baked into the
    other.

    `companion_filename` (when provided): the basename of the paired
    brief document, cited near the top so a reader can find the
    deterministic context. Set automatically by `export()`; pass None
    for ad-hoc renders.

    `scope_label` (when provided): a human-readable scope description
    surfaced in the header so a leads doc shared standalone still
    announces what slice of the data it covers.
    """
    lines: list[str] = []
    lines.append("# GACC × Eurostat trade — investigation leads")
    lines.append(
        f"*Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} from "
        "active `narrative_hs_group` findings.*"
    )
    if scope_label:
        lines.append(f"*Scope: **{scope_label}**.*")
    lines.append("")
    lines.append(
        "Each lead below is an LLM-scaffolded starting position for one HS "
        "group: a one-sentence anomaly summary, 2–3 hypotheses picked from "
        "a curated catalog of standard causes for China-EU/UK trade "
        "movements, and concrete corroboration steps to test them. The LLM "
        "does NOT compute, draft prose, or invent hypotheses outside the "
        "catalog. Every number cited is verified against the underlying "
        "findings before storage; failures are silently rejected rather "
        "than published. Use the leads as starting positions for "
        "investigation; verify against the deterministic findings or the "
        "underlying database."
    )
    lines.append("")
    findings_ref = f"`{companion_filename}`" if companion_filename else "`findings.md`"
    lines.append("## In this export folder")
    lines.append("")
    lines.append(
        "This is one of three artefacts generated together from the same DB "
        "snapshot. All three share the same finding IDs; switch between them "
        "depending on what you need."
    )
    lines.append("")
    lines.append(
        f"- **{findings_ref}** — deterministic Markdown findings. "
        "NotebookLM-ready, no LLM in the loop. Cite this for the "
        "underlying numbers any lead below references."
    )
    lines.append(
        "- **`leads.md`** — LLM-scaffolded investigation leads (this "
        "document). Kept separate from the findings so a downstream LLM "
        "tool reasoning over them sees raw data, not another LLM's "
        "interpretation."
    )
    lines.append(
        "- **`data.xlsx`** — 8-tab spreadsheet for data journalists. Same "
        "findings, long-format with filterable scope/flow columns, "
        "predictability badges, CIF/FOB baseline expansion. Also LLM-free."
    )
    lines.append("")
    lines.append(
        "Each lead carries an `llm_drafted` caveat plus the union of "
        "caveats on its underlying findings; underlying caveats "
        "(low_base, partial_window, transshipment_hub, cn8_revision, "
        "low_kg_coverage, etc.) propagate from the source findings. "
        "Trace ids point to the lead finding itself; the underlying "
        "deterministic findings are listed alongside so you can walk the "
        "chain."
    )
    lines.append("")

    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        section = _section_llm_narratives(cur)

    if not section.markdown:
        lines.append(
            "_No active `narrative_hs_group` findings — run "
            "`scrape.py --analyse llm-framing` to generate leads._"
        )
        lines.append("")
    else:
        # _section_llm_narratives starts with its own "## Investigation
        # leads" header + intro paragraph; strip those (we provide the
        # framing here) and keep the per-group blocks.
        body = section.markdown
        marker = "### "
        idx = body.find(marker)
        if idx > 0:
            body = body[idx:]
        lines.append(body)

    # Endnote on what `finding/N` citations mean — same text as the brief's
    # endnote so a journalist coming to either doc gets the same orientation.
    lines.append("")
    lines.append(_section_about_findings().markdown)

    return "\n".join(lines).rstrip() + "\n"


def export(
    out_dir: str | None = None,
    scope_label: str | None = None,
    top_n: int = DEFAULT_TOP_N,
    out_path: str | None = None,
    leads_path: str | None = None,
    spreadsheet: bool | None = None,
    trigger: str = "manual",
    record: bool = True,
) -> tuple[str, str]:
    """Write the findings document AND the companion leads file to disk.
    Returns (findings_path, leads_path).

    Default behaviour: create `./exports/YYYY-MM-DD-HHMM[-slug]/` and
    write `findings.md` + `leads.md` inside it. Pairs are self-evident
    from the folder; consumers find the pair by convention.

    `scope_label` (optional, human-readable): when set, slugified into a
    folder suffix (e.g. "EV batteries (Li-ion)" → `-ev-batteries-li-ion`)
    AND surfaced inside both docs' headers so a brief shared standalone
    still announces its scope. Note: the scope_label is currently
    metadata only; the brief/leads still render the full finding set.
    Scoped *filtering* (only emit findings for one HS group, only one
    comparison scope) is a separate future change — having the naming
    convention in place now means scoped exports can land cleanly.

    `out_dir` (optional): override the default folder path.

    `out_path` / `leads_path` (legacy escape hatch): explicit per-file
    paths, both required if either is given. Skips folder creation.
    Use the folder approach by default — these are kept only for
    callers (e.g. tests) that want explicit control.

    `spreadsheet`: also write `data.xlsx` into the export folder so
    all three artefacts (findings / leads / spreadsheet) share a
    single DB snapshot. A data journalist opens data.xlsx; an editorial
    journalist opens findings.md; everyone is working from the same
    point in time. Default depends on mode: folder mode → True
    (spreadsheet is part of the user-facing bundle); legacy explicit-
    paths mode → False (callers using explicit paths are typically
    tests / preview / programmatic use that don't need the bundle).
    Pass explicitly to override either default.

    Records the brief run in `brief_runs` so the next brief can compute
    "what changed since" (Phase 6.8). render() is called for the
    markdown but doesn't record — record only on disk-writing exports
    so test/preview renders don't pollute the run log.

    `record=False` produces the bundle without inserting a `brief_runs`
    row — useful for test / preview / on-demand exports that should not
    advance any cycle and should not appear in the "Tier 1 — what's new
    since the previous export" baseline. The bundle itself is still
    written normally; only the audit row is skipped.
    """
    if out_path is not None or leads_path is not None:
        # Legacy explicit-paths mode. Both must be given.
        if out_path is None or leads_path is None:
            raise ValueError(
                "If using explicit out_path / leads_path, pass both."
            )
        p = Path(out_path)
        lp = Path(leads_path)
        if spreadsheet is None:
            spreadsheet = False  # legacy callers opt in if they want it
    else:
        if out_dir is None:
            ts = datetime.now().strftime("%Y-%m-%d-%H%M")
            slug = f"-{_slugify_scope(scope_label)}" if scope_label else ""
            out_dir = f"./exports/{ts}{slug}"
        d = Path(out_dir)
        p = d / "findings.md"
        lp = d / "leads.md"
        if spreadsheet is None:
            spreadsheet = True  # bundle is the default user-facing mode

    # Each render gets the OTHER doc's basename as its companion citation.
    brief_basename = p.name
    leads_basename = lp.name

    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(render(
        top_n=top_n, companion_filename=leads_basename,
        scope_label=scope_label,
    ))
    if record:
        _record_brief_run(
            out_path=str(p),
            top_n=top_n,
            data_period=latest_eurostat_period(),
            trigger=trigger,
        )
    else:
        log.info(
            "Skipping brief_runs insert (record=False) — this export is "
            "unsequenced and will not appear in the cycle history."
        )
    log.info("Wrote briefing pack to %s", p)

    lp.parent.mkdir(parents=True, exist_ok=True)
    lp.write_text(render_leads(
        companion_filename=brief_basename, scope_label=scope_label,
    ))
    log.info("Wrote investigation leads to %s", lp)

    if spreadsheet:
        # Lazy import — sheets_export imports from this module, so a
        # top-level import would create a cycle. The sheet always lives
        # next to the brief in the same folder; filename is `data.xlsx`.
        import sheets_export
        xlsx_path = p.parent / "data.xlsx"
        sheets_export.XlsxWriter().write(
            sheets_export.assemble_sheets(), str(xlsx_path),
        )
        log.info("Wrote spreadsheet to %s", xlsx_path)

    return str(p), str(lp)
