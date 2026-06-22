"""Docx output for the briefing pack — parallel surface to `02_Findings.md`.

Lisa-facing surface. The markdown stays canonical (NotebookLM-feed,
per `memory/architecture_journalist_surfaces.md` — keep LLM output /
interpretation OUTSIDE documents downstream LLM tools will read).
Verified Drive → Google Docs round-trip fidelity 2026-05-16; see
`dev_notes/2026-05-16-docx-drive-spike.md`.

Architecture (v4, 2026-05-16 evening — full markdown-content parity):

1. Render the canonical `findings.md` via `briefing_pack.render.render()`.
2. Compute the top-N movers and build a PNG chart for each — same
   24-month rolling-window line chart shape as v1.
3. Hand both into `MarkdownToDocxTranslator` from `briefing_pack.md_to_docx`,
   which walks the markdown AST and emits the equivalent docx blocks.
   Charts inject after the top-N list items via a finding/{id} → PNG
   lookup callable.
4. Apply A4 portrait + 10mm margins page setup (spike-verified).
5. Save.

The output is therefore *the same content as the .md* (Tier 1 diff,
Tier 2 state-of-play, mirror gaps, partner share, trajectories,
methodology footer, sources appendix) *plus charts at top-N movers*.

v1 (this module's earlier shape) shipped a chart-bearing top-N
extract; v4 replaces it with full content parity. The single
`render_findings_docx` entry point is what callers use.
"""

from __future__ import annotations

import io
import logging
import math
from datetime import date
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # non-interactive backend; deterministic on a
                       # given host. See dev_notes design doc step 5
                       # for the known cross-host determinism caveat.

# Silence matplotlib's "Using categorical units to plot a list of
# strings..." INFO logs. We deliberately plot month labels (strings)
# on a categorical x-axis; matplotlib's heuristic flags this as
# "did you mean to parse them as dates?" every time. Harmless but
# spammy — once per chart × 10 charts per run × every run.
logging.getLogger("matplotlib.category").setLevel(logging.WARNING)

import matplotlib.pyplot as plt
import psycopg2.extras
from docx import Document
from docx.shared import Mm, Pt

import db
from briefing_pack._helpers import (
    DEFAULT_TOP_N,
    _compute_predictability_per_group,
    _compute_top_movers,
    _conn,
)
from briefing_pack.md_to_docx import MarkdownToDocxTranslator

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Page-setup constants — values verified by the 2026-05-16 docx fidelity
# spike to round-trip cleanly through Drive → Google Docs.
# ---------------------------------------------------------------------------

_PAGE_WIDTH_MM = 210     # A4 portrait
_PAGE_HEIGHT_MM = 297    # A4 portrait
_MARGIN_MM = 10          # all four sides
_BODY_FONT_PT = 11
_CHART_WIDTH_MM = 190    # 210 - 2×10 usable width

# Heading point sizes for the docx styles. Google Docs honours the docx
# Heading style's run properties on import (verified by the bold fix), so
# setting size on the style carries through to the converted Doc.
_HEADING_SIZES_PT = {1: 18, 2: 15, 3: 13, 4: 13}

# "Metadata" sections — orientation / how-to-read material, functionally
# distinct from the briefing's actual findings. The docx renders them with
# a tinted background so a reader can tell them apart from content. Matched
# against each heading's plain text; codespans collapse to their raw text
# (e.g. "About the `finding/N` citations" → "About the finding/N citations").
_METADATA_SECTION_HEADINGS = {
    "In this export folder",
    "Scope notes",
    "Period coverage",
    "Findings included",
    "How to read this findings document",
    "Reading the numbers",
    "About the finding/N citations",
}
# Per-file metadata sets for the other house-styled Docs. Leads mirrors the
# Findings treatment (orientation block + key + citation endnote); Groups
# tints only its navigation index; Read_Me_First gets none (it is all
# orientation).
_LEADS_METADATA_HEADINGS = {
    "In this export folder",
    "Reading the numbers",
    "About the finding/N citations",
}
_GROUPS_METADATA_HEADINGS = {
    "Quick index",
}
_METADATA_SHADE_FILL = "EEF2F7"  # light blue-grey callout tint

# EU-27 reporter exclusion (matches anomalies.EU27_EXCLUDE_REPORTERS).
_EU27_EXCLUDE_REPORTERS = ("GB",)

_MONTH_LABEL = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _months_back(d: date, n: int) -> date:
    """Return `date(d - n months, day=1)`. 23 months back from 2026-02-01
    → 2024-03-01."""
    total = d.year * 12 + (d.month - 1) - n
    return date(total // 12, (total % 12) + 1, 1)


def _month_iter(start: date, end: date):
    """Yield consecutive month-anchored dates from `start` to `end` inclusive."""
    cur = date(start.year, start.month, 1)
    end_anchor = date(end.year, end.month, 1)
    while cur <= end_anchor:
        yield cur
        cur = (
            date(cur.year + 1, 1, 1) if cur.month == 12
            else date(cur.year, cur.month + 1, 1)
        )


# ---------------------------------------------------------------------------
# Data fetch — finding detail + monthly chart series
# ---------------------------------------------------------------------------

def _fetch_finding_detail(cur, finding_id: int) -> dict | None:
    """Load the full `detail` JSONB for a finding by id."""
    cur.execute(
        "SELECT detail FROM findings WHERE id = %s",
        (finding_id,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return row[0] if not isinstance(row, dict) else row["detail"]


def _fetch_monthly_eur_series(
    cur,
    *,
    hs_patterns: list[str],
    flow: int,
    partners: list[str],
    start: date,
    end: date,
) -> dict[date, float]:
    """Return {month_date: value_eur} summing `eurostat_raw_rows`.

    Matches the filter shape of `_hs_group_top_cn8s` in `anomalies.py`:
    flow ∈ {1=import, 2=export}, partners filter, HS pattern OR-LIKE,
    EU-27 reporter exclusion."""
    if not hs_patterns:
        return {}
    like_clause = "(" + " OR ".join(
        ["product_nc LIKE %s"] * len(hs_patterns)
    ) + ")"
    sql = f"""
        SELECT date_trunc('month', period)::date AS month,
               SUM(value_eur)::float8 AS value_eur
          FROM eurostat_raw_rows
         WHERE period >= %s AND period <= %s
           AND flow = %s
           AND partner = ANY(%s)
           AND {like_clause}
           AND reporter <> ALL(%s)
      GROUP BY 1
      ORDER BY 1
    """
    params = (
        start, end, flow, list(partners),
        *hs_patterns,
        list(_EU27_EXCLUDE_REPORTERS),
    )
    cur.execute(sql, params)
    return {r[0]: float(r[1] or 0.0) for r in cur.fetchall()}


# ---------------------------------------------------------------------------
# Chart rendering
# ---------------------------------------------------------------------------

def _pick_eur_scale(max_value: float) -> tuple[float, str]:
    """Pick a divisor + label for a y-axis based on the series' max."""
    if max_value >= 1e9:
        return 1e9, "€ billions"
    if max_value >= 1e6:
        return 1e6, "€ millions"
    if max_value >= 1e3:
        return 1e3, "€ thousands"
    return 1.0, "€"


def _flow_label_for_subkind(subkind: str) -> str:
    if subkind.endswith("_export"):
        return "EU-27 exports to China"
    return "EU-27 imports from China"


def _build_chart_png(
    *,
    current_end: date,
    monthly_eur: dict[date, float],
    group_name: str,
    flow_label: str,
) -> bytes:
    """Render a 24-month line chart, prior-12mo grey vs current-12mo red."""
    start = _months_back(current_end, 23)
    months = list(_month_iter(start, current_end))
    values = [monthly_eur.get(m, float("nan")) for m in months]

    prior_vals = values[:12]
    current_vals = [float("nan")] * 11 + values[11:]
    labels = [
        f"{_MONTH_LABEL[m.month]} {m.year % 100:02d}" for m in months
    ]

    scale, unit_label = _pick_eur_scale(
        max((v for v in values if not math.isnan(v)), default=0.0)
    )
    prior_scaled = [v / scale if not math.isnan(v) else float("nan")
                    for v in prior_vals]
    current_scaled = [v / scale if not math.isnan(v) else float("nan")
                      for v in current_vals]

    fig, ax = plt.subplots(figsize=(7.5, 3.2), dpi=150)
    ax.plot(
        labels[:12], prior_scaled,
        label="Prior 12mo", linewidth=2, color="#888888",
    )
    ax.plot(
        labels, current_scaled,
        label="Current 12mo", linewidth=2.5, color="#cc3333",
    )
    ax.set_title(
        f"{group_name} — {flow_label}", fontsize=11, loc="left",
    )
    ax.set_ylabel(unit_label, fontsize=9)
    ax.tick_params(axis="x", labelsize=7, rotation=45)
    ax.tick_params(axis="y", labelsize=8)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=8, frameon=False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _build_bilateral_summary_bar_png(
    *,
    partner_label: str,
    subkind: str,
    current_end: date,
    current_eur: float,
    prior_eur: float,
    yoy_pct: float,
) -> bytes:
    """Render a two-bar prior-vs-current 12mo chart for a
    `gacc_bilateral_aggregate_yoy*` finding.

    Shape: two grouped bars (Prior 12mo in grey, Current 12mo in red),
    with the YoY % in the title and the partner label as the main
    label. Editorial purpose: when a partner's bilateral flow flips
    direction or moves materially, this gives Lisa the magnitude in
    one glance alongside the headline percentage.

    Simpler than the hs_group_yoy line chart because:
    - Bilateral findings are headline-figure findings (one number);
      a trajectory chart would require new SQL against GACC
      observations.
    - The diff section already says "+22% → -2.8%"; this chart turns
      those abstract percentages into € magnitudes.
    """
    flow_human = (
        "imports from China"
        if subkind.endswith("_import")
        else "exports to China"
    )
    direction = "↑" if (current_eur or 0) > (prior_eur or 0) else "↓"

    max_val = max(prior_eur or 0.0, current_eur or 0.0, 0.0)
    scale, unit_label = _pick_eur_scale(max_val)
    prior_scaled = (prior_eur or 0.0) / scale
    current_scaled = (current_eur or 0.0) / scale

    fig, ax = plt.subplots(figsize=(5.5, 2.8), dpi=150)
    bars = ax.bar(
        ["Prior 12mo", "Current 12mo"],
        [prior_scaled, current_scaled],
        color=["#888888", "#cc3333"],
        width=0.5,
    )
    # Annotate each bar with its value
    for bar, value in zip(bars, [prior_scaled, current_scaled]):
        ax.annotate(
            f"{value:.2f}",
            xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
            xytext=(0, 3),
            textcoords="offset points",
            ha="center", va="bottom",
            fontsize=9,
        )
    yoy_str = f"{yoy_pct * 100:+.1f}%"
    ax.set_title(
        f"{partner_label} {flow_human} — "
        f"YoY {yoy_str} {direction} (12mo to {current_end:%Y-%m})",
        fontsize=10, loc="left",
    )
    ax.set_ylabel(unit_label, fontsize=9)
    ax.tick_params(axis="y", labelsize=8)
    ax.grid(True, axis="y", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _build_per_reporter_bar_png(
    *,
    breakdown: list[dict],
    group_name: str,
    flow_label: str,
    top_k: int = 5,
) -> bytes | None:
    """Render a grouped-bar chart of the top-K reporters by absolute
    YoY delta (current_eur - prior_eur), prior 12mo vs current 12mo.

    Answers Lisa's likely "which country is driving the move?"
    follow-up to the trajectory chart. Data source: the finding's
    `per_reporter_breakdown` field (populated by Phase 6.11 in
    `anomalies._build_per_reporter_breakdown`).

    Returns None if no reporter rows have usable values; caller skips
    the chart in that case.
    """
    # Filter and rank by absolute delta
    rows = []
    for r in breakdown:
        cur_eur = r.get("current_eur")
        prior_eur = r.get("prior_eur")
        if cur_eur is None and prior_eur is None:
            continue
        cur_eur = float(cur_eur or 0.0)
        prior_eur = float(prior_eur or 0.0)
        delta = abs(cur_eur - prior_eur)
        rows.append({
            "reporter": r.get("reporter") or "?",
            "current_eur": cur_eur,
            "prior_eur": prior_eur,
            "delta": delta,
        })
    if not rows:
        return None
    rows.sort(key=lambda r: -r["delta"])
    rows = rows[:top_k]

    labels = [r["reporter"] for r in rows]
    prior_vals = [r["prior_eur"] for r in rows]
    current_vals = [r["current_eur"] for r in rows]

    max_val = max(prior_vals + current_vals + [0.0])
    scale, unit_label = _pick_eur_scale(max_val)
    prior_scaled = [v / scale for v in prior_vals]
    current_scaled = [v / scale for v in current_vals]

    fig, ax = plt.subplots(figsize=(7.5, 3.2), dpi=150)
    x = list(range(len(labels)))
    width = 0.36
    ax.bar(
        [i - width / 2 for i in x], prior_scaled, width,
        label="Prior 12mo", color="#888888",
    )
    ax.bar(
        [i + width / 2 for i in x], current_scaled, width,
        label="Current 12mo", color="#cc3333",
    )
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_title(
        f"{group_name} — top reporter contributions ({flow_label})",
        fontsize=11, loc="left",
    )
    ax.set_ylabel(unit_label, fontsize=9)
    ax.tick_params(axis="y", labelsize=8)
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend(loc="best", fontsize=8, frameon=False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# Top-movers chart lookup — pre-compute PNGs keyed by finding_id
# ---------------------------------------------------------------------------

def _build_top_mover_charts(
    cur, top_n: int,
) -> dict[int, list[bytes]]:
    """Compute the top-N movers and build a *set* of charts for each,
    keyed by finding id. Each value is a list of PNG bytes — the
    translator inserts each chart in order after the first occurrence
    of the finding's list item.

    Currently each top mover gets up to two charts:
    1. Rolling-12mo line chart (prior vs current 12mo monthly series).
    2. Per-reporter grouped bar chart (top reporter contributions to
       the YoY delta) — only if the finding's
       `detail.per_reporter_breakdown` has data.

    Movers with missing detail / no underlying raw rows have an empty
    list entry; the translator simply doesn't inject anything for
    those.
    """
    predictability = _compute_predictability_per_group(cur)
    movers = _compute_top_movers(
        cur, predictability=predictability, limit=top_n,
    )
    disp = db.group_display_names(cur)  # reader-facing chart labels

    charts: dict[int, list[bytes]] = {}
    for m in movers:
        current_end = m["current_end"]
        group_disp = disp.get(m["group_name"], m["group_name"])
        flow_label = _flow_label_for_subkind(m["subkind"])
        detail = _fetch_finding_detail(cur, m["id"])
        if not detail:
            continue

        per_finding: list[bytes] = []

        # Chart 1 — rolling-12mo line chart
        method_q = detail.get("method_query", {})
        hs_patterns = (
            method_q.get("hs_patterns")
            or detail.get("group", {}).get("hs_patterns")
            or []
        )
        flow = int(method_q.get("flow") or 0)
        partners = method_q.get("partners") or []
        if hs_patterns and flow and partners:
            series = _fetch_monthly_eur_series(
                cur,
                hs_patterns=hs_patterns,
                flow=flow,
                partners=partners,
                start=_months_back(current_end, 23),
                end=current_end,
            )
            if series:
                per_finding.append(_build_chart_png(
                    current_end=current_end,
                    monthly_eur=series,
                    group_name=group_disp,
                    flow_label=flow_label,
                ))

        # Chart 2 — per-reporter grouped bar chart
        breakdown = detail.get("per_reporter_breakdown") or []
        if breakdown:
            bar_png = _build_per_reporter_bar_png(
                breakdown=breakdown,
                group_name=group_disp,
                flow_label=flow_label,
            )
            if bar_png is not None:
                per_finding.append(bar_png)

        if per_finding:
            charts[m["id"]] = per_finding

    # Bilateral findings (gacc_bilateral_aggregate_yoy*) get a simple
    # two-bar prior-vs-current chart each. They're the other family
    # that turns up frequently in Lisa's Tier 1 diff reading.
    bilaterals = _compute_top_bilateral_movers(cur, limit=top_n)
    for b in bilaterals:
        try:
            png = _build_bilateral_summary_bar_png(
                partner_label=b["partner_label"] or "(unlabelled)",
                subkind=b["subkind"],
                current_end=b["current_end"],
                current_eur=float(b["current_eur"] or 0.0),
                prior_eur=float(b["prior_eur"] or 0.0),
                yoy_pct=float(b["yoy_pct"] or 0.0),
            )
        except (TypeError, ValueError) as exc:
            log.warning(
                "Skipping bilateral chart for finding %s: %s", b["id"], exc,
            )
            continue
        charts.setdefault(b["id"], []).append(png)

    return charts


def _compute_top_bilateral_movers(cur, *, limit: int) -> list[dict]:
    """Top-N `gacc_bilateral_aggregate_yoy*` findings at the latest
    anchor across the family, ranked by absolute YoY %.

    Filter mirrors the briefing pack's Tier 1 diff material-shift
    threshold (≥5pp move). Both flow directions (export from China,
    import to China) included; ranking is by |yoy_pct| so a strong
    move in either direction surfaces.

    Returns dicts with the fields needed to render the summary bar:
    id, subkind, partner_label, yoy_pct, current_eur, prior_eur,
    current_end.
    """
    cur.execute(
        """
        SELECT MAX((detail->'windows'->>'current_end')::date)
          FROM findings
         WHERE subkind IN (
                 'gacc_bilateral_aggregate_yoy',
                 'gacc_bilateral_aggregate_yoy_import'
             )
           AND superseded_at IS NULL
        """
    )
    latest = cur.fetchone()[0]
    if latest is None:
        return []

    cur.execute(
        """
        SELECT id,
               subkind,
               detail->'partner'->>'raw_label'                  AS partner_label,
               (detail->'totals'->>'yoy_pct')::numeric          AS yoy_pct,
               (detail->'totals'->>'current_12mo_eur')::numeric AS current_eur,
               (detail->'totals'->>'prior_12mo_eur')::numeric   AS prior_eur,
               (detail->'windows'->>'current_end')::date        AS current_end
          FROM findings
         WHERE subkind IN (
                 'gacc_bilateral_aggregate_yoy',
                 'gacc_bilateral_aggregate_yoy_import'
             )
           AND superseded_at IS NULL
           AND (detail->'windows'->>'current_end')::date = %s
           AND abs((detail->'totals'->>'yoy_pct')::numeric) >= 0.05
         ORDER BY abs((detail->'totals'->>'yoy_pct')::numeric) DESC
         LIMIT %s
        """,
        (latest, limit),
    )
    return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Docx assembly
# ---------------------------------------------------------------------------

def _apply_page_setup(doc: Document) -> None:
    """Apply A4 + 10mm margins + 11pt body to a fresh Document."""
    section = doc.sections[0]
    section.page_height = Mm(_PAGE_HEIGHT_MM)
    section.page_width = Mm(_PAGE_WIDTH_MM)
    section.top_margin = Mm(_MARGIN_MM)
    section.bottom_margin = Mm(_MARGIN_MM)
    section.left_margin = Mm(_MARGIN_MM)
    section.right_margin = Mm(_MARGIN_MM)
    doc.styles["Normal"].font.size = Pt(_BODY_FONT_PT)


def _apply_heading_styles(doc: Document) -> None:
    """Bump heading sizes per the reporting team's request; keep Heading 4
    italic (its template default, restated so it survives the size set)."""
    for level, size in _HEADING_SIZES_PT.items():
        doc.styles[f"Heading {level}"].font.size = Pt(size)
    doc.styles["Heading 4"].font.italic = True


def render_markdown_to_docx(
    markdown: str,
    out_path: str | Path,
    *,
    metadata_section_headings: set[str] | None = None,
    chart_for_finding=None,
    chart_width_mm: int = _CHART_WIDTH_MM,
) -> Path:
    """Translate a rendered markdown string into a house-styled `.docx`:
    A4 + 10mm margins, the larger heading sizes, and tinted metadata
    sections. Shared by every briefing-pack Doc so they all carry the same
    styling. `chart_for_finding` is Findings-only; the others pass none."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    doc = Document()
    _apply_page_setup(doc)
    _apply_heading_styles(doc)

    translator = MarkdownToDocxTranslator(
        doc,
        chart_for_finding=chart_for_finding,
        chart_width_mm=chart_width_mm,
        shaded_section_headings=metadata_section_headings or set(),
        shade_fill=_METADATA_SHADE_FILL,
    )
    translator.translate(markdown)
    doc.save(str(out_path))
    return out_path


def render_findings_docx(
    out_path: str | Path,
    *,
    top_n: int = DEFAULT_TOP_N,
    scope_label: str | None = None,
    companion_filename: str | None = None,
    groups_filename: str | None = None,
) -> Path:
    """Render the full findings.md content into a house-styled docx at
    `out_path`, with charts inserted after each top-N mover's list item.
    Same content as the .md (Tier 1 diff, Tier 2 state-of-play, mirror
    gaps, partner share, trajectories, methodology footer, sources
    appendix) plus charts at the top-N movers."""
    # Lazy import to avoid the briefing_pack → docx → md_to_docx →
    # briefing_pack import cycle that would otherwise trip on module
    # initialisation.
    from briefing_pack.render import render

    markdown = render(
        top_n=top_n,
        companion_filename=companion_filename,
        groups_filename=groups_filename,
        scope_label=scope_label,
    )

    # Pre-compute chart PNGs for the top movers, keyed by finding_id.
    with _conn() as conn, conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor,
    ) as cur:
        charts_by_id = _build_top_mover_charts(cur, top_n=top_n)

    path = render_markdown_to_docx(
        markdown, out_path,
        metadata_section_headings=_METADATA_SECTION_HEADINGS,
        chart_for_finding=charts_by_id.get,
    )
    total_charts = sum(len(v) for v in charts_by_id.values())
    log.info(
        "Wrote findings docx to %s (%d findings charted, "
        "%d total charts, %d markdown chars)",
        path, len(charts_by_id), total_charts, len(markdown),
    )
    return path


def render_leads_docx(
    out_path: str | Path,
    *,
    scope_label: str | None = None,
    companion_filename: str | None = None,
) -> Path:
    """House-styled docx of the investigation-leads document."""
    from briefing_pack.render import render_leads

    markdown = render_leads(
        companion_filename=companion_filename, scope_label=scope_label,
    )
    return render_markdown_to_docx(
        markdown, out_path,
        metadata_section_headings=_LEADS_METADATA_HEADINGS,
    )


def render_groups_docx(
    out_path: str | Path,
    *,
    companion_filename: str | None = None,
    leads_filename: str | None = None,
) -> Path:
    """House-styled docx of the HS-group reference document."""
    from briefing_pack.render_groups import render_groups

    markdown = render_groups(
        companion_filename=companion_filename, leads_filename=leads_filename,
    )
    return render_markdown_to_docx(
        markdown, out_path,
        metadata_section_headings=_GROUPS_METADATA_HEADINGS,
    )


def render_readme_docx(out_path: str | Path, readme_md_path: str | Path) -> Path:
    """House-styled docx of the static Read-Me-First template. No metadata
    tint — the whole document is orientation."""
    markdown = Path(readme_md_path).read_text()
    return render_markdown_to_docx(
        markdown, out_path, metadata_section_headings=set(),
    )


