"""Tier 3 — opener heading for the per-finding detail blocks."""

from __future__ import annotations

from briefing_pack._helpers import _Section


def _section_detail_opener() -> _Section:
    """Tier 3 — opener heading for the per-finding detail blocks. The
    detail itself is rendered by the existing _section_hs_yoy_movers /
    _section_trajectories / _section_mirror_gaps / _section_low_base
    functions, all of which use ### headings so they sit naturally under
    this ## opener."""
    lines: list[str] = []
    lines.append("---")
    lines.append("")
    lines.append("## Tier 3 — Full detail by HS group")
    lines.append("")
    lines.append(
        "Per-finding detail for the top movers in each scope-and-flow "
        "combination, plus trajectory shape buckets, mirror gaps, and "
        "low-base reviews. Drill in here when you need a citable number, "
        "per-reporter contribution detail, or the per-finding caveat list. "
        "The same content is in `04_Data.xlsx` if you'd rather sort and filter."
    )
    lines.append("")
    return _Section(markdown="\n".join(lines))
