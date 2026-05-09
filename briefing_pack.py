"""Markdown briefing-pack export for findings.

Companion to sheets_export.py. Where the spreadsheet is for editorial
scanning, the briefing pack is for narrative reading — and, by design,
for upload to NotebookLM as a one-shot exploration corpus.

Design principles:

1. Deterministic. No LLM. The pack is a structured render of what's in
   the `findings` table, grouped and sorted but otherwise untransformed.
   The LLM framing layer is a separate later step that operates over the
   same finding set.
2. Provenance-first. Every finding line ends with a canonical
   `[finding/{id}]` token (NotebookLM citation handle, future web-UI
   permalink) and a one-line method tag. A `## Sources` appendix at the
   end of the pack lists every release URL underlying the brief, grouped
   by source, with fetch timestamps. A journalist clicking through has
   third-party links one tap away.
3. Same data layer as the Sheets exporter. We re-read findings from
   Postgres, not the rendered XLSX — so the two surfaces are independent
   and any one of them can be wrong without contaminating the other.

CLI: see scrape.py `--briefing-pack`.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

import eurostat

log = logging.getLogger(__name__)

PERMALINK_BASE_ENV = "GACC_PERMALINK_BASE"
DEFAULT_TOP_N = 10


@dataclass
class _Section:
    """Rendered section + the set of release_ids it touched (for the appendix)."""
    markdown: str
    release_ids: set[int] = field(default_factory=set)


# =============================================================================
# DB helpers
# =============================================================================


def _conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _trace_token(finding_id: int) -> str:
    """Stable citation token for NotebookLM. If GACC_PERMALINK_BASE is
    set, render as a Markdown link; otherwise emit the bare token. The
    bare token still works as a citation handle — NotebookLM picks up
    `finding/123` strings as searchable references."""
    base = os.environ.get(PERMALINK_BASE_ENV, "").rstrip("/")
    if base:
        return f"[finding/{finding_id}]({base}/finding/{finding_id})"
    return f"`finding/{finding_id}`"


def _fmt_eur(v: Any) -> str:
    if v is None:
        return "—"
    n = float(v)
    if abs(n) >= 1e9:
        return f"€{n / 1e9:.2f}B"
    if abs(n) >= 1e6:
        return f"€{n / 1e6:.1f}M"
    if abs(n) >= 1e3:
        return f"€{n / 1e3:.1f}k"
    return f"€{n:.0f}"


def _fmt_pct(v: Any) -> str:
    if v is None:
        return "—"
    return f"{float(v) * 100:+.1f}%"


def _fmt_kg(v: Any) -> str:
    if v is None:
        return "—"
    n = float(v)
    if abs(n) >= 1e9:
        return f"{n / 1e9:.2f}B kg"
    if abs(n) >= 1e6:
        return f"{n / 1e6:.1f}M kg"
    if abs(n) >= 1e3:
        return f"{n / 1e3:.1f}k kg"
    return f"{n:.0f} kg"


def _release_ids_for_window(cur, start: date, end: date) -> set[int]:
    """Eurostat releases whose period falls in [start, end]. Used to
    populate the sources appendix for window-traced findings (hs_group_yoy
    and trajectories) — these don't have observation_ids[] so we go via
    the period range that fed them."""
    cur.execute(
        "SELECT id FROM releases WHERE source = 'eurostat' "
        "AND period BETWEEN %s AND %s",
        (start, end),
    )
    return {r[0] for r in cur.fetchall()}


def _release_ids_for_observations(cur, obs_ids: list[int]) -> set[int]:
    if not obs_ids:
        return set()
    cur.execute(
        "SELECT DISTINCT release_id FROM observations WHERE id = ANY(%s)",
        (obs_ids,),
    )
    return {r[0] for r in cur.fetchall()}


# =============================================================================
# Section builders
# =============================================================================


def _section_headline(cur) -> _Section:
    """Top-of-pack scene-setting: schema version, period coverage, finding counts."""
    cur.execute(
        "SELECT source, MIN(period) AS lo, MAX(period) AS hi, COUNT(*) AS n "
        "FROM releases GROUP BY source ORDER BY source"
    )
    sources = cur.fetchall()
    cur.execute(
        "SELECT subkind, COUNT(*) FROM findings WHERE kind = 'anomaly' "
        "GROUP BY subkind ORDER BY subkind"
    )
    counts = cur.fetchall()

    lines: list[str] = []
    lines.append(f"# GACC × Eurostat trade briefing")
    lines.append(f"*Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} from the `findings` table.*")
    lines.append("")
    lines.append("This pack is a deterministic render of the underlying findings — no LLM in the loop. ")
    lines.append("Each finding line ends with a citation token (e.g. `finding/123`) which is a stable handle ")
    lines.append("into the project's database. A **Sources** appendix at the end lists every third-party ")
    lines.append("URL the brief rests on, with fetch timestamps.")
    lines.append("")
    lines.append("## Period coverage")
    for s in sources:
        lines.append(f"- **{s['source']}**: {s['lo']} → {s['hi']} ({s['n']} releases)")
    lines.append("")
    lines.append("## Findings included")
    for k, n in counts:
        lines.append(f"- {k}: {n}")
    lines.append("")
    return _Section(markdown="\n".join(lines))


def _section_hs_yoy_movers(cur, flow: int, top_n: int) -> _Section:
    """Top-N movers by |yoy_pct| for the latest period per group."""
    subkind = "hs_group_yoy" if flow == 1 else "hs_group_yoy_export"
    flow_label = "Imports (CN→EU)" if flow == 1 else "Exports (EU→CN)"
    flow_short = "imports" if flow == 1 else "exports"
    cur.execute(
        """
        WITH latest AS (
          SELECT DISTINCT ON (detail->'group'->>'name')
                 id,
                 detail->'group'->>'name' AS group_name,
                 (detail->'windows'->>'current_start')::date AS current_start,
                 (detail->'windows'->>'current_end')::date AS current_end,
                 (detail->'windows'->>'prior_start')::date AS prior_start,
                 (detail->'totals'->>'current_12mo_eur')::numeric AS current_eur,
                 (detail->'totals'->>'prior_12mo_eur')::numeric AS prior_eur,
                 (detail->'totals'->>'yoy_pct')::numeric AS yoy_pct,
                 (detail->'totals'->>'current_12mo_kg')::numeric AS current_kg,
                 (detail->'totals'->>'yoy_pct_kg')::numeric AS yoy_pct_kg,
                 (detail->'totals'->>'unit_price_pct_change')::numeric AS unit_price_pct,
                 (detail->'totals'->>'low_base')::boolean AS low_base,
                 detail->'method_query'->'hs_patterns' AS hs_patterns
            FROM findings
           WHERE subkind = %s
        ORDER BY detail->'group'->>'name', (detail->'windows'->>'current_end')::date DESC, id DESC
        )
        SELECT * FROM latest ORDER BY abs(yoy_pct) DESC NULLS LAST LIMIT %s
        """,
        (subkind, top_n),
    )
    rows = cur.fetchall()

    release_ids: set[int] = set()
    lines: list[str] = []
    lines.append(f"## {flow_label} — top {len(rows)} movers (latest 12mo YoY)")
    lines.append("")
    if not rows:
        lines.append("*No findings of this kind yet.*")
        lines.append("")
        return _Section(markdown="\n".join(lines))

    for r in rows:
        lines.append(f"### {r['group_name']}")
        # Surface the period the finding actually refers to. For groups where
        # the analyser has stopped emitting findings (e.g. low-base failure),
        # this prevents the brief from claiming a stale period is "latest".
        lines.append(
            f"- **Period (12mo ending)**: {r['current_end'].strftime('%Y-%m')}"
        )
        lines.append(
            f"- **Value**: {_fmt_pct(r['yoy_pct'])} "
            f"({_fmt_eur(r['prior_eur'])} → {_fmt_eur(r['current_eur'])})"
        )
        if r['yoy_pct_kg'] is not None:
            lines.append(
                f"- **Volume**: {_fmt_pct(r['yoy_pct_kg'])} "
                f"(12mo total: {_fmt_kg(r['current_kg'])})"
            )
        if r['unit_price_pct'] is not None:
            decomp = _decomposition_label(r['yoy_pct'], r['yoy_pct_kg'])
            lines.append(
                f"- **Unit price (€/kg)**: {_fmt_pct(r['unit_price_pct'])}"
                + (f" — *{decomp}*" if decomp else "")
            )
        if r['low_base']:
            lines.append(
                "- ⚠️ **Low-base flag**: prior or current 12mo total below the €50M "
                "threshold. Verify absolute figures before quoting the percentage."
            )
        lines.append(
            f"- *Method*: 12mo rolling, partner=CN, flow={flow_short}, "
            f"hs_patterns=`{r['hs_patterns']}`"
        )
        # Window-traced source span
        period_start = r['prior_start']
        period_end = r['current_end']
        ids = _release_ids_for_window(cur, period_start, period_end)
        release_ids |= ids
        lines.append(
            f"- *Sources*: {len(ids)} Eurostat monthly bulk files, "
            f"{period_start.strftime('%Y-%m')} → {period_end.strftime('%Y-%m')}"
        )
        lines.append(f"- *Trace*: {_trace_token(r['id'])}")
        lines.append("")

    return _Section(markdown="\n".join(lines), release_ids=release_ids)


def _decomposition_label(yoy_eur: Any, yoy_kg: Any) -> str:
    """Mirrors the volume-vs-price decomposition in anomalies.py."""
    if yoy_eur is None or yoy_kg is None or float(yoy_eur) == 0:
        return ""
    share = float(yoy_kg) / float(yoy_eur)
    return "volume-driven" if abs(share) > 0.5 else "price-driven"


def _section_trajectories(cur) -> _Section:
    """Trajectory findings grouped by shape — narrative-rich pattern bucket."""
    cur.execute(
        """
        SELECT id, subkind,
               detail->'group'->>'name' AS group_name,
               detail->>'shape' AS shape,
               detail->>'shape_label' AS shape_label,
               (detail->'features'->>'last_yoy')::numeric AS last_yoy,
               (detail->'features'->>'max_yoy')::numeric AS peak,
               (detail->'features'->>'min_yoy')::numeric AS trough,
               (detail->'features'->>'first_period')::date AS first_period,
               (detail->'features'->>'last_period')::date AS last_period,
               (detail->'features'->>'low_base_majority')::boolean AS low_base_majority
          FROM findings
         WHERE subkind IN ('hs_group_trajectory', 'hs_group_trajectory_export')
      ORDER BY detail->>'shape', subkind, detail->'group'->>'name'
        """
    )
    rows = cur.fetchall()

    release_ids: set[int] = set()
    lines: list[str] = []
    lines.append("## Trajectory shapes")
    lines.append("")
    lines.append(
        "Each HS group's rolling-12mo YoY series classified by shape. "
        "Editorially the shape vocabulary matters: `dip_recovery` and `inverse_u_peak` "
        "are narrative-rich (a comeback or a peak-and-fall); `falling`/`rising` are "
        "directional; `volatile` flags series the classifier didn't fit confidently."
    )
    lines.append("")
    if not rows:
        lines.append("*No trajectory findings yet.*")
        lines.append("")
        return _Section(markdown="\n".join(lines))

    by_shape: dict[str, list[psycopg2.extras.DictRow]] = {}
    for r in rows:
        by_shape.setdefault(r['shape'], []).append(r)

    # Order shapes editorially: narrative-rich first, then directional, then volatile/flat.
    shape_order = [
        "dip_recovery", "failed_recovery", "inverse_u_peak", "u_recovery",
        "rising_accelerating", "rising_decelerating", "rising",
        "falling_decelerating", "falling_accelerating", "falling",
        "volatile", "flat",
    ]
    seen_shapes = set()
    for shape in shape_order + sorted(by_shape.keys()):
        if shape in seen_shapes or shape not in by_shape:
            continue
        seen_shapes.add(shape)
        shape_label = by_shape[shape][0]['shape_label'] or shape
        lines.append(f"### {shape} — *{shape_label}*")
        for r in by_shape[shape]:
            flow = "imports" if r['subkind'] == 'hs_group_trajectory' else "exports"
            low_base_marker = " ⚠️ low-base" if r['low_base_majority'] else ""
            lines.append(
                f"- **{r['group_name']}** ({flow}): "
                f"latest YoY {_fmt_pct(r['last_yoy'])}, "
                f"peak {_fmt_pct(r['peak'])}, trough {_fmt_pct(r['trough'])}"
                f"{low_base_marker} — {_trace_token(r['id'])}"
            )
            # Window: features.first_period (first 12mo-window end) — 12mo back covers
            # the earliest observation period that fed this trajectory.
            if r['first_period'] and r['last_period']:
                window_start = (r['first_period'].replace(day=1) - timedelta(days=1)).replace(day=1)
                # Step back 11 more months to cover the full 12mo prior window for the first point.
                ws = r['first_period']
                for _ in range(12):
                    ws = (ws.replace(day=1) - timedelta(days=1)).replace(day=1)
                ids = _release_ids_for_window(cur, ws, r['last_period'])
                release_ids |= ids
        lines.append("")

    return _Section(markdown="\n".join(lines), release_ids=release_ids)


def _section_mirror_gaps(cur) -> _Section:
    """Latest mirror_gap finding per partner, plus z-score movers."""
    cur.execute(
        """
        SELECT DISTINCT ON (detail->>'iso2')
            f.id, f.observation_ids,
            detail->>'iso2' AS iso2,
            detail->'gacc'->>'partner_label_raw' AS gacc_label,
            (detail->'gacc'->>'value_eur_converted')::numeric AS gacc_eur,
            (detail->'eurostat'->>'total_eur')::numeric AS eurostat_eur,
            (detail->>'gap_eur')::numeric AS gap_eur,
            (detail->>'gap_pct')::numeric AS gap_pct,
            (detail->>'is_aggregate')::boolean AS is_aggregate,
            (SELECT to_char(r.period, 'YYYY-MM')
               FROM observations o JOIN releases r ON r.id = o.release_id
              WHERE o.id = f.observation_ids[1]) AS period
          FROM findings f
         WHERE subkind = 'mirror_gap'
      ORDER BY detail->>'iso2',
               (SELECT r.period FROM observations o JOIN releases r ON r.id = o.release_id
                 WHERE o.id = f.observation_ids[1]) DESC,
               f.id DESC
        """
    )
    gap_rows = cur.fetchall()

    release_ids: set[int] = set()
    lines: list[str] = []
    lines.append("## Mirror-trade gaps (latest per partner)")
    lines.append("")
    lines.append(
        "Mirror-gap = (Eurostat — GACC_EUR_converted) / Eurostat. The *expected* "
        "baseline is +5–10% (CIF vs FOB pricing — caveat `cif_fob`). Persistent gaps "
        "well above that — Netherlands and Italy notably — sit in the structural "
        "transshipment territory; sudden movements are flagged separately as movers."
    )
    lines.append("")
    if not gap_rows:
        lines.append("*No mirror-gap findings yet.*")
        lines.append("")
    else:
        # Sort: real countries first (iso2 not null), then aggregates.
        gap_rows_sorted = sorted(
            gap_rows,
            key=lambda r: (r['is_aggregate'] or False, r['iso2'] or '~'),
        )
        for r in gap_rows_sorted:
            label = r['gacc_label'] or r['iso2']
            agg = " *(aggregate)*" if r['is_aggregate'] else ""
            lines.append(f"### {r['iso2']} — {label}{agg}")
            lines.append(
                f"- Period: **{r['period']}** | GACC (EUR-converted): {_fmt_eur(r['gacc_eur'])} "
                f"| Eurostat: {_fmt_eur(r['eurostat_eur'])} | Gap: **{_fmt_pct(r['gap_pct'])}**"
            )
            lines.append("- *Caveats*: cif_fob, classification_drift, currency_timing")
            ids = _release_ids_for_observations(cur, list(r['observation_ids'] or []))
            release_ids |= ids
            lines.append(
                f"- *Sources*: {len(ids)} releases (one GACC + one Eurostat per period)"
            )
            lines.append(f"- *Trace*: {_trace_token(r['id'])}")
            lines.append("")

    # z-score movers
    cur.execute(
        """
        SELECT id, detail->>'iso2' AS iso2,
               to_char((detail->>'period')::date, 'YYYY-MM') AS period,
               (detail->>'gap_pct')::numeric AS gap_pct,
               (detail->'baseline'->>'mean')::numeric AS baseline_mean,
               (detail->>'z_score')::numeric AS z
          FROM findings
         WHERE subkind = 'mirror_gap_zscore'
      ORDER BY abs((detail->>'z_score')::numeric) DESC NULLS LAST
         LIMIT 10
        """
    )
    movers = cur.fetchall()
    lines.append("### Mirror-gap movers (top 10 by |z|)")
    lines.append("")
    lines.append(
        "Each row: a partner whose gap shifted notably vs that partner's own rolling "
        "baseline. High |z| = the gap moved unusually for *this* country, regardless "
        "of where the gap level sits structurally."
    )
    lines.append("")
    if not movers:
        lines.append("*No mover findings yet.*")
        lines.append("")
    else:
        for m in movers:
            lines.append(
                f"- **{m['iso2']} {m['period']}**: gap {_fmt_pct(m['gap_pct'])} vs "
                f"baseline mean {_fmt_pct(m['baseline_mean'])} — "
                f"z = **{float(m['z']):+.2f}** — {_trace_token(m['id'])}"
            )
        lines.append("")

    return _Section(markdown="\n".join(lines), release_ids=release_ids)


def _section_low_base(cur) -> _Section:
    """Editorial review queue: every hs_group_yoy*-flavoured finding flagged low_base."""
    cur.execute(
        """
        SELECT id, subkind,
               detail->'group'->>'name' AS group_name,
               to_char((detail->'windows'->>'current_end')::date, 'YYYY-MM') AS period,
               (detail->'totals'->>'current_12mo_eur')::numeric AS current_eur,
               (detail->'totals'->>'prior_12mo_eur')::numeric AS prior_eur,
               (detail->'totals'->>'yoy_pct')::numeric AS yoy_pct,
               (detail->'totals'->>'low_base_threshold_eur')::numeric AS threshold
          FROM findings
         WHERE subkind IN ('hs_group_yoy', 'hs_group_yoy_export')
           AND (detail->'totals'->>'low_base')::boolean = true
      ORDER BY abs((detail->'totals'->>'yoy_pct')::numeric) DESC NULLS LAST
        """
    )
    rows = cur.fetchall()

    lines: list[str] = []
    if not rows:
        # Suppress the section entirely when there's nothing to review.
        return _Section(markdown="")

    lines.append("## Low-base review queue")
    lines.append("")
    lines.append(
        f"{len(rows)} findings rest on a denominator below the low-base threshold "
        f"(€50M for either current or prior 12mo total). Verify the absolute figures "
        f"before quoting any percentage from these — small bases can exaggerate."
    )
    lines.append("")
    for r in rows:
        flow = "imports" if r['subkind'] == 'hs_group_yoy' else "exports"
        lines.append(
            f"- **{r['group_name']}** ({flow}, {r['period']}): "
            f"{_fmt_pct(r['yoy_pct'])}, "
            f"prior {_fmt_eur(r['prior_eur'])} → current {_fmt_eur(r['current_eur'])} — "
            f"{_trace_token(r['id'])}"
        )
    lines.append("")
    return _Section(markdown="\n".join(lines))


def _section_sources_appendix(cur, release_ids: set[int]) -> _Section:
    """Final appendix listing every release URL underlying the brief.

    Eurostat: synthesises the bulk-file URL via eurostat.bulk_file_url, since
    the canonical URL is deterministic per period (and we deliberately don't
    store the 44 MB 7z bytes). GACC: the actual source_url from the release
    row, plus the fetched_at from source_snapshots so a journalist knows
    the page state we read."""
    lines: list[str] = []
    lines.append("## Sources")
    lines.append("")
    lines.append(
        "Every release whose data fed any finding above. Eurostat URLs are "
        "the deterministic monthly bulk-file URLs; the raw CSV rows we extracted "
        "from each are preserved verbatim in the project DB (`eurostat_raw_rows`). "
        "GACC URLs are the actual customs.gov.cn pages we scraped — the page "
        "bytes are stored in `source_snapshots` so the read is reproducible "
        "even if the page is later revised or removed."
    )
    lines.append("")
    if not release_ids:
        lines.append("*No releases referenced.*")
        lines.append("")
        return _Section(markdown="\n".join(lines))

    cur.execute(
        """
        SELECT r.id, r.source, r.source_url, r.period, r.first_seen_at, r.last_seen_at,
               r.section_number, r.currency, r.release_kind,
               (SELECT MAX(s.fetched_at) FROM source_snapshots s
                  JOIN scrape_runs sr ON sr.id = s.scrape_run_id
                 WHERE s.url = r.source_url) AS snapshot_fetched_at
          FROM releases r
         WHERE r.id = ANY(%s)
      ORDER BY r.source, r.period DESC, r.id
        """,
        (sorted(release_ids),),
    )
    rels = cur.fetchall()

    by_source: dict[str, list[psycopg2.extras.DictRow]] = {}
    for r in rels:
        by_source.setdefault(r['source'], []).append(r)

    if 'eurostat' in by_source:
        lines.append("### Eurostat monthly bulk files")
        lines.append("")
        lines.append(
            "*Eurostat occasionally re-publishes corrected files at the same URL. "
            "The `as_of` timestamp is when we fetched and parsed the file into "
            "`eurostat_raw_rows` — that is the ground truth we used.*"
        )
        lines.append("")
        for r in by_source['eurostat']:
            url = eurostat.bulk_file_url(r['period'])
            as_of = r['first_seen_at'].strftime('%Y-%m-%d') if r['first_seen_at'] else '—'
            lines.append(
                f"- **{r['period'].strftime('%Y-%m')}** — as_of {as_of} — <{url}>"
            )
        lines.append("")

    if 'gacc' in by_source:
        lines.append("### GACC release pages")
        lines.append("")
        lines.append(
            "*Page bytes preserved in `source_snapshots`. The `fetched_at` "
            "timestamp is when we last successfully read the page; the link "
            "below points to the live page.*"
        )
        lines.append("")
        for r in by_source['gacc']:
            ts = r['snapshot_fetched_at'] or r['last_seen_at']
            ts_str = ts.strftime('%Y-%m-%d') if ts else '—'
            kind_bits = " ".join(filter(None, [
                f"section {r['section_number']}" if r['section_number'] else None,
                r['currency'],
                r['release_kind'],
            ]))
            lines.append(
                f"- **{r['period'].strftime('%Y-%m')}** "
                f"({kind_bits}) — fetched {ts_str} — <{r['source_url']}>"
            )
        lines.append("")

    lines.append("### Known gaps in source coverage")
    lines.append("")
    lines.append(
        "- We scrape the GACC English-language pages. The underlying "
        "Chinese-language release at `customs.gov.cn` is a separate URL "
        "we don't currently track — a journalist triangulating in Chinese "
        "would need to navigate there independently."
    )
    lines.append(
        "- Caveat codes referenced inline (e.g. `cif_fob`, `low_base_effect`) "
        "have full definitions in the project's `caveats` table."
    )
    lines.append("")
    return _Section(markdown="\n".join(lines))


# =============================================================================
# Top-level orchestrator
# =============================================================================


def render(top_n: int = DEFAULT_TOP_N) -> str:
    """Render the full briefing pack as a single Markdown string."""
    sections: list[_Section] = []
    release_ids: set[int] = set()
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sections.append(_section_headline(cur))

        for flow in (1, 2):
            sec = _section_hs_yoy_movers(cur, flow=flow, top_n=top_n)
            sections.append(sec)
            release_ids |= sec.release_ids

        sec = _section_trajectories(cur)
        sections.append(sec)
        release_ids |= sec.release_ids

        sec = _section_mirror_gaps(cur)
        sections.append(sec)
        release_ids |= sec.release_ids

        sec = _section_low_base(cur)
        sections.append(sec)
        release_ids |= sec.release_ids

        sections.append(_section_sources_appendix(cur, release_ids))

    return "\n".join(s.markdown for s in sections if s.markdown).rstrip() + "\n"


def export(out_path: str | None = None, top_n: int = DEFAULT_TOP_N) -> str:
    """Write the briefing pack to disk. Returns the final path."""
    if out_path is None:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        out_path = f"./exports/briefing-{ts}.md"
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(render(top_n=top_n))
    log.info("Wrote briefing pack to %s", p)
    return str(p)
