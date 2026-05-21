"""Top-of-pack scene-setting: schema version, period coverage, finding counts."""

from __future__ import annotations

from datetime import datetime

from briefing_pack._helpers import _Section, _in_this_export_folder_md


def _section_headline(
    cur,
    companion_filename: str | None = None,
    scope_label: str | None = None,
    groups_filename: str | None = None,
) -> _Section:
    """Top-of-pack scene-setting: schema version, period coverage, finding counts."""
    cur.execute(
        "SELECT source, MIN(period) AS lo, MAX(period) AS hi, COUNT(*) AS n "
        "FROM releases GROUP BY source ORDER BY source"
    )
    sources = cur.fetchall()
    # Active (un-superseded) findings only — superseded rows are revision
    # history, queryable but not part of the current picture.
    cur.execute(
        "SELECT subkind, COUNT(*) FROM findings "
        "WHERE kind = 'anomaly' AND superseded_at IS NULL "
        "GROUP BY subkind ORDER BY subkind"
    )
    counts = cur.fetchall()

    lines: list[str] = []
    lines.append(f"# GACC × Eurostat trade findings")
    lines.append(f"*Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} from the `findings` table.*")
    if scope_label:
        lines.append(f"*Scope: **{scope_label}**.*")
    lines.append("")
    lines.append("This document is a deterministic render of the underlying findings — no LLM in the loop. ")
    lines.append("Each finding line ends with a citation token (e.g. `finding/123`) which is a stable handle ")
    lines.append("into the project's database. A **Sources** appendix at the end lists every third-party ")
    lines.append("URL the findings rest on, with fetch timestamps.")
    lines.append("")
    lines.append(_in_this_export_folder_md(current="03_Findings"))
    lines.append("")
    lines.append("## Scope notes")
    lines.append("")
    lines.append("- **Eurostat partners summed**: CN + HK + MO (the editorially-correct \"Chinese trade\" ")
    lines.append("  envelope including the two Special Administrative Regions). The HMRC side mirrors this ")
    lines.append("  partner envelope. For a CN-only spot-check against a Soapbox / Merics figure, query ")
    lines.append("  `eurostat_raw_rows` directly with `partner = 'CN'`.")
    lines.append("- **EU-27 = EU-27.** Eurostat reporter rows from GB (pre-2021) are excluded at all times so ")
    lines.append("  EU-27 totals are consistent through the Brexit transition. UK trade is captured ")
    lines.append("  separately via HMRC ingest (Phase 6.1) and surfaced under the **UK** comparison scope.")
    lines.append("- **Comparison scopes**: each hs-group section renders three views — EU-27 (Eurostat), UK ")
    lines.append("  (HMRC), and EU-27 + UK combined. The combined view carries a `cross_source_sum` caveat ")
    lines.append("  reflecting the methodological non-comparability of summing across two statistical agencies.")
    lines.append("")
    lines.append("Methodological caveats that apply to *every* finding of a given family (e.g. `cif_fob`, ")
    lines.append("`currency_timing`, `multi_partner_sum`) are documented once in the **Methodology — universal ")
    lines.append("caveats** block at the end of this document. Per-finding caveat lines above carry only what ")
    lines.append("varies between findings of the same family.")
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
