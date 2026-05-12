"""Three-tier reader's guide. Sits right after the headline."""

from __future__ import annotations

from briefing_pack._helpers import _Section


def _section_reader_guide() -> _Section:
    """Three-tier reader's guide. Sits right after the headline so a reader
    knows what they're looking at and where to dive in before any findings.

    The three tiers are also reflected in the headings below so the document
    is navigable: Tier 1 (lead), Tier 2 (compact summary), Tier 3 (full
    per-finding detail). The supersede chain in the DB powers tier 1; the
    `data.xlsx` companion is the same data as tier 3 in sortable form. The
    Methodology footer at the end documents caveats that apply to every
    finding of a given family (cf. _section_methodology_footer)."""
    lines: list[str] = []
    lines.append("## How to read this findings document")
    lines.append("")
    lines.append(
        "Three sections, descending in newness and ascending in completeness. "
        "Read them in order if this is your first findings export; skip "
        "straight to **Tier 1** if you read the previous one."
    )
    lines.append("")
    lines.append(
        "1. **Tier 1 — What's new this cycle.** Additions, revisions, "
        "direction-flips, and threshold-crossings since the previous findings "
        "export. The actually-news part. A regular subscriber reads this and "
        "probably stops here unless something needs drilling into."
    )
    lines.append(
        "2. **Tier 2 — Current state of play.** A compact summary of where "
        "every active finding stands today — one block per HS group for "
        "the EU-CN deep-dive view, then one block per GACC partner aggregate "
        "(ASEAN / Africa / Latin America / world Total) for the bloc-level "
        "context view. Each row carries **two YoY operators**: the 12-month "
        "rolling figure (stable, smooths seasonality) and, where the data "
        "supports it, the **latest-month** figure (acceleration signal — "
        "matches Soapbox / Lisa register). The persistent picture between "
        "cycles. Skim to orient yourself; most of this re-renders "
        "identically cycle to cycle."
    )
    lines.append(
        "3. **Tier 3 — Full detail by HS group.** Per-finding breakdown with "
        "caveats, top reporters, top CN8 contributors, monthly trajectory series. "
        "Drill in when you need to source a specific number or quote with "
        "confidence. The same data is available in sortable/filterable form "
        "in the `data.xlsx` companion."
    )
    lines.append("")
    lines.append(
        "*Each tier is delimited by a horizontal rule (`---`) and a `## Tier N` "
        "heading so it's easy to scan to where you want to land.*"
    )
    lines.append("")
    return _Section(markdown="\n".join(lines))
