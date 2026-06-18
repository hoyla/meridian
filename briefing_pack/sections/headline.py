"""Top-of-pack scene-setting: schema version, period coverage, finding counts."""

from __future__ import annotations

from datetime import datetime

from briefing_pack._helpers import (
    _Section,
    _in_this_export_folder_md,
    _subkind_plain_label,
)


def _section_headline(
    cur,
    companion_filename: str | None = None,
    scope_label: str | None = None,
    groups_filename: str | None = None,
    reissue_note: str | None = None,
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
    lines.append("# China–EU/UK trade — findings")
    lines.append(f"*Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} from the `findings` table.*")
    if scope_label:
        lines.append(f"*Scope: **{scope_label}**.*")
    # Corrected-re-issue banner sits immediately under the title — the first
    # thing a reader sees, before the orientation copy — so a withdrawal /
    # correction notice can't be missed.
    if reissue_note:
        lines.append("")
        lines.append(f"> **⚠ {reissue_note}**")
    lines.append("")
    lines.append("This document is a deterministic render of the underlying findings — no LLM in the loop. ")
    lines.append("Each finding line ends with a citation token (e.g. `finding/123`) which is a stable handle ")
    lines.append("into the project's database. A **Sources** appendix at the end lists every third-party ")
    lines.append("URL the findings rest on, with fetch timestamps.")
    lines.append("")
    lines.append(_in_this_export_folder_md(current="02_Findings"))
    lines.append("")
    lines.append("## Scope notes")
    lines.append("")
    lines.append("- **\"China\" includes Hong Kong and Macau.** European statistics report trade routed via ")
    lines.append("  Hong Kong and Macau under separate codes because they are separate customs territories; ")
    lines.append("  editorially they are still Chinese trade, so every figure here sums all three (CN + HK + MO) ")
    lines.append("  on both the Eurostat and HMRC side. (Technical readers: for a CN-only spot-check against a ")
    lines.append("  Soapbox / Merics figure, query `eurostat_raw_rows` with `partner = 'CN'`.)")
    lines.append("- **EU-27 means EU-27.** UK rows are excluded from EU totals at all times — including ")
    lines.append("  pre-Brexit years — so EU-27 figures are comparable across the whole period. UK trade is ")
    lines.append("  covered separately from HMRC data under the **UK** view.")
    lines.append("- **Three views of each category**: EU-27 (from Eurostat), UK (from HMRC), and EU-27 + UK ")
    lines.append("  combined. The combined view adds together two different statistical agencies' figures — a ")
    lines.append("  useful approximation, but not a like-for-like number from a single source (the ")
    lines.append("  `cross_source_sum` caveat in the methodology footer explains the differences).")
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
        lines.append(f"- {n} — {_subkind_plain_label(k)} (`{k}`)")
    lines.append("")
    return _Section(markdown="\n".join(lines))
