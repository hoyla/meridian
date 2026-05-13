"""Top-5 movers — the editorial digest at the top of findings.md.

A composite-ranked shortlist of the most quotable shifts across the
active hs_group_yoy* family at the current anchor. Sits above Tier 1
so a journalist opening the brief sees "if you read nothing else, read
these five lines."

The scoring rule (see `_compute_top_movers` in `_helpers.py`):
- |yoy_pct| ≥ 10pp
- current_12mo_eur ≥ €100M
- low_base = False
- predictability badge ≠ 🔴 (no badge is fine — absence of T-6 data
  doesn't imply volatility, just lack of confidence cue yet)
- Score = |yoy_pct| × log10(current_12mo_eur)

Rendered as a numbered list with the same headline-figure format Tier 2
uses; each entry carries its `finding/{id}` trace token and an anchor
link to its full Tier 3 detail.
"""

from __future__ import annotations

from briefing_pack._helpers import (
    _Section,
    _fmt_eur,
    _fmt_pct,
    _slugify_heading,
    _trace_token,
)


def _section_top_movers(top_movers: list[dict]) -> _Section:
    """Render the Top-5 section for findings.md.

    `top_movers` comes from `_compute_top_movers` — already filtered,
    scored, and capped. Empty case (no group passes the filters at the
    current anchor) returns empty markdown so the caller drops the
    section entirely. This is correct behaviour on a fresh DB or any
    cycle where every candidate sits in low-base / 🔴 / <€100M
    territory.
    """
    if not top_movers:
        return _Section(markdown="")

    lines: list[str] = []
    lines.append(f"## Top {len(top_movers)} movers this cycle")
    lines.append("")
    lines.append(
        "*Editorially-quotable shifts ranked by a composite of "
        "|YoY| × log(€). Filters: ≥10pp move, ≥€100M current 12mo total, "
        "not low-base, predictability badge ≠ 🔴 (no badge is fine — "
        "groups without enough T-6 history yet are still eligible). "
        "Drill into each via its Tier 2 anchor or `finding/{id}` token; "
        "the full state-of-play picture is in Tier 2 below.*"
    )
    lines.append("")

    for i, m in enumerate(top_movers, start=1):
        group_name = m["group_name"]
        is_export = m["subkind"].endswith("_export")
        flow_label = (
            "EU-27 exports (reporter→CN)" if is_export
            else "EU-27 imports (CN→reporter)"
        )
        # Badge if present.
        pred = m.get("predictability")
        badge = f" {pred[0]}" if pred is not None else ""
        # Kg YoY in parens when available (matches Tier 2 format).
        yoy_kg = m.get("yoy_pct_kg")
        kg_str = (
            f" (kg {_fmt_pct(yoy_kg)})" if yoy_kg is not None else ""
        )
        period = m["current_end"]
        anchor = _slugify_heading(group_name)

        lines.append(
            f"{i}. **[{group_name}](#{anchor}){badge}** — "
            f"{flow_label}: {_fmt_pct(m['yoy_pct'])}{kg_str} to "
            f"{_fmt_eur(m['current_eur'])} "
            f"(12mo to {period.strftime('%Y-%m')}). "
            f"{_trace_token(m['id'])}"
        )
    lines.append("")

    return _Section(markdown="\n".join(lines))
