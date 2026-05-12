"""End-of-brief methodology block: caveats that apply to every finding
of a given subkind family."""

from __future__ import annotations

import anomalies

from briefing_pack._helpers import _Section

_SUBKIND_FAMILY_LABELS: dict[str, str] = {
    "mirror_gap": "Mirror-trade gap findings (per-partner, per-period)",
    "mirror_gap_zscore": "Mirror-trade gap z-score movers",
    "hs_group_yoy": "HS-group YoY findings (rolling 12-month windows)",
    "hs_group_trajectory": "HS-group trajectories (multi-period shape)",
    "narrative_hs_group": "LLM-scaffolded investigation leads",
}


def _section_methodology_footer(cur) -> _Section:
    """End-of-brief methodology block: caveats that apply to every finding
    in their subkind family. Defined once in
    `anomalies.UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY`; rendered here from the
    canonical summary + detail text in the `caveats` table so a reader can
    look up any code's definition without digging into the schema.

    Grouped by family so a reader reading a Tier-3 hs_group_yoy section
    knows exactly which family-universal caveats apply to those findings
    (without those caveats cluttering each finding's own caveat line)."""
    universal = anomalies.UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY
    all_codes = sorted({c for codes in universal.values() for c in codes})
    cur.execute(
        "SELECT code, summary, detail FROM caveats WHERE code = ANY(%s)",
        (all_codes,),
    )
    by_code = {r["code"]: r for r in cur.fetchall()}

    lines: list[str] = []
    lines.append("## Methodology — universal caveats")
    lines.append("")
    lines.append(
        "Each block below lists caveats that fire on *every* finding of that "
        "family. They are real limitations on the underlying data — not "
        "per-finding signal — so they are documented once here rather than "
        "repeated on every finding's caveat line. Per-finding caveat lines "
        "above carry only what varies between findings (low_base_effect, "
        "partial_window, transshipment_hub, low_baseline_n, low_kg_coverage, "
        "cross_source_sum, aggregate_composition)."
    )
    lines.append("")
    for family, codes in universal.items():
        label = _SUBKIND_FAMILY_LABELS.get(family, family)
        lines.append(f"### {label} (`subkind={family}` and `_uk` / `_combined` / `_export` variants)")
        lines.append("")
        for code in codes:
            row = by_code.get(code)
            if row is None:
                lines.append(f"- **`{code}`** — *Note: missing `caveats` table definition.*")
                lines.append("")
                continue
            lines.append(f"- **`{code}` — {row['summary']}**")
            if row["detail"]:
                lines.append("")
                lines.append(f"  {row['detail']}")
            lines.append("")
    return _Section(markdown="\n".join(lines))
