"""Docx output for the briefing pack — parallel surface to `03_Findings.md`.

Lisa-facing surface that carries charts; the markdown stays canonical
(NotebookLM-feed, per `memory/architecture_journalist_surfaces.md` —
keep LLM output / images / interpretation OUTSIDE documents downstream
LLM tools will read). Verified Drive → Google Docs round-trip fidelity
2026-05-16; see `dev_notes/2026-05-16_docx-drive-spike.md`.

v1 (this slice): top-5 movers as a heading + numbered list, mirroring
the markdown's Top 5 section. Charts land in a follow-up commit (see
`dev_notes/2026-05-16_docx-production-module-design.md` § "Concrete
first slice", step 2).

Caller contract: `render_top_movers_docx(out_path)` opens its own DB
connection, fetches the top movers via the same `_compute_top_movers`
helper the markdown renderer uses, and writes the .docx atomically to
the given path. No global state.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import psycopg2.extras
from docx import Document
from docx.shared import Mm, Pt

from briefing_pack._helpers import (
    DEFAULT_TOP_N,
    _compute_predictability_per_group,
    _compute_top_movers,
    _conn,
    _fmt_eur,
    _fmt_pct,
)

log = logging.getLogger(__name__)


# Page-setup constants — values verified by the 2026-05-16 docx
# fidelity spike to round-trip cleanly through Drive → Google Docs.
# Change with care; the spike doc captures what each was checked
# against.
_PAGE_WIDTH_MM = 210     # A4 portrait
_PAGE_HEIGHT_MM = 297    # A4 portrait
_MARGIN_MM = 10          # all four sides
_BODY_FONT_PT = 11
_CHART_WIDTH_MM = 190    # leaves a small breathing margin inside
                         # the 190mm usable width (210 - 2×10)


def _apply_page_setup(doc: Document) -> None:
    """Apply A4 + 10mm margins + 11pt body to a fresh Document.

    Lives in this module rather than as inline calls in
    `render_top_movers_docx` so future renderers (per-finding cards,
    per-section breakdowns) can reuse the same defaults without
    re-typing constants.
    """
    section = doc.sections[0]
    section.page_height = Mm(_PAGE_HEIGHT_MM)
    section.page_width = Mm(_PAGE_WIDTH_MM)
    section.top_margin = Mm(_MARGIN_MM)
    section.bottom_margin = Mm(_MARGIN_MM)
    section.left_margin = Mm(_MARGIN_MM)
    section.right_margin = Mm(_MARGIN_MM)

    style = doc.styles["Normal"]
    style.font.size = Pt(_BODY_FONT_PT)


def _flow_label_for_subkind(subkind: str) -> str:
    """Human-readable flow label matching the markdown renderer's
    convention (see `briefing_pack/sections/top_movers.py`)."""
    if subkind.endswith("_export"):
        return "EU-27 exports (reporter→CN)"
    return "EU-27 imports (CN→reporter)"


def render_top_movers_docx(
    out_path: str | Path,
    *,
    top_n: int = DEFAULT_TOP_N,
    scope_label: str | None = None,
) -> Path:
    """Render the top-N movers section of the briefing pack to a docx.

    v1 produces a single-section docx with title, generation timestamp,
    "Top {N} movers this cycle" heading, an italic editorial preamble,
    and a numbered list — one mover per line, format mirrors the
    markdown:

        1. **Group name [🟡|🔴|🟢]** — EU-27 imports (CN→reporter):
           +34.5% (kg +69.4%) to €27.25B (12mo to 2026-02). finding/N

    Returns the resolved Path of the written file.

    Empty-movers case (fresh DB, restrictive filters): writes a docx
    with just the title and a single italic "no movers passed the
    filter" paragraph rather than an empty document, so the file is
    always a valid bundle artefact.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # DictCursor (matching `briefing_pack/render.py`) — `_compute_top_movers`
    # builds dicts via `dict(r)` which requires a dict-style row factory.
    with _conn() as conn, conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor,
    ) as cur:
        predictability = _compute_predictability_per_group(cur)
        movers = _compute_top_movers(
            cur, predictability=predictability, limit=top_n,
        )

    doc = Document()
    _apply_page_setup(doc)

    title_text = "Meridian — Findings"
    if scope_label:
        title_text += f" ({scope_label})"
    doc.add_heading(title_text, level=0)

    # Cycle context line — mirrors what the markdown carries in its
    # header. Generation timestamp is useful for Lisa to disambiguate
    # multiple cycles that might land in the same Drive folder.
    p = doc.add_paragraph()
    p.add_run("Generated: ").bold = True
    p.add_run(datetime.now().strftime("%Y-%m-%d %H:%M"))

    if not movers:
        empty_p = doc.add_paragraph()
        empty_p.add_run(
            "No top-mover findings passed the editorial filter for this "
            "cycle. See 03_Findings.md (Tier 2) for the full state of play."
        ).italic = True
        doc.save(str(out_path))
        log.info("Wrote findings docx to %s (no movers)", out_path)
        return out_path

    doc.add_heading(f"Top {len(movers)} movers this cycle", level=1)

    preamble = doc.add_paragraph()
    preamble.add_run(
        "Editorially-quotable shifts ranked by a composite of "
        "|YoY| × log(€). Filters: ≥10pp move, ≥€100M current 12mo total, "
        "not low-base, predictability badge ≠ 🔴. Figures match those in "
        "03_Findings.md; refer there for full context, caveats, and the "
        "Tier 1/2/3 detail."
    ).italic = True

    for m in movers:
        flow_label = _flow_label_for_subkind(m["subkind"])
        pred = m.get("predictability")
        badge = f" {pred[0]}" if pred is not None else ""
        yoy_kg = m.get("yoy_pct_kg")
        kg_str = f" (kg {_fmt_pct(yoy_kg)})" if yoy_kg is not None else ""
        period = m["current_end"]

        p = doc.add_paragraph(style="List Number")
        p.add_run(f"{m['group_name']}{badge}").bold = True
        p.add_run(
            f" — {flow_label}: {_fmt_pct(m['yoy_pct'])}{kg_str} to "
            f"{_fmt_eur(m['current_eur'])} "
            f"(12mo to {period.strftime('%Y-%m')}). "
        )
        p.add_run(f"finding/{m['id']}").italic = True

    doc.save(str(out_path))
    log.info("Wrote findings docx to %s", out_path)
    return out_path
