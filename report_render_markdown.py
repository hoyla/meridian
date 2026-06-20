"""Report → Markdown. One renderer over the rendering-agnostic content
model (`report_model.Report`).

This is the proof that the spine works: it consumes only the model, no DB
and no analysis, and emits the LLM-facing markdown surface. The HTML
portal and any docx fallback are sibling renderers over the same model —
none canonical. Demonstrates that `briefing_pack/sections/headlines.py`
(which built markdown by hand) is now redundant: the structure lives in
the model, the prose in its `prose` fields, and this file only formats.
"""

from __future__ import annotations

from report_model import Headline, Indicator, Report, WhatChanged

_COMPANIONS = (
    ("State of play", "where each group and partner currently stands"),
    ("Sector detail", "the full per-HS-group YoY breakdown"),
    ("Data", "the underlying spreadsheet, one row per finding"),
    ("Glossary", "HS-group definitions and methodology"),
)


def _indicator_line(ind: Indicator) -> str:
    bits = [f"**{ind.label}:** {ind.formatted}"]
    if ind.delta:
        bits.append(f"({ind.delta['formatted']})")
    spark = ""
    if ind.chart_data and ind.chart_data.series:
        spark = f" · sparkline: {len(ind.chart_data.series)} pts"
    cite = ""
    if ind.provenance.finding_ids:
        cite = f" `finding/{ind.provenance.finding_ids[0]}`"
    return " ".join(bits) + spark + cite


def _llm_block(slot) -> list[str]:
    scope = "on the lead finding above" if slot.slot_type == "specific" else "once per release"
    grounded = (
        f"grounded in finding/{slot.grounded_in[0]}"
        if slot.grounded_in else "grounded in the findings above"
    )
    return [
        f"> 🔶 **LLM · {slot.slot_type}** — _{scope}_  ",
        f"> _Awaiting generation. Will interpret only the cited findings "
        f"({grounded}); no new facts._",
        "",
    ]


def _render_headline(h: Headline) -> list[str]:
    lines = [f"## {h.lead_title}", ""]
    if h.items:
        lines.append(
            "*The most quotable shifts this cycle — each a 12-month total "
            "vs the prior 12 months, ending in its citation token, ready "
            "to lift into copy.*"
        )
        lines.append("")
        for i, item in enumerate(h.items, start=1):
            lines.append(f"{i}. {item.prose}")
        lines.append("")
    else:
        lines.append(
            "_Macro/geographic lead not yet wired into the model "
            "(GACC partner/bloc totals) — next increment._"
        )
        lines.append("")
    for slot in h.llm_slots:
        if slot.slot_type == "specific":
            lines.extend(_llm_block(slot))
    if h.items:
        lines.append(
            "*The smaller and shakier moves are in the **Sector detail** "
            "tab — not dropped, just not headlined.*"
        )
        lines.append("")
    return lines


def _render_what_changed(wc: WhatChanged) -> list[str]:
    return [
        "## What changed since the last pack",
        "",
        f"**Since the last pack:** {wc.summary}",
        "",
        "*This tab answers \"what changed?\". Where each group and partner "
        "currently stands is in the **State of play** tab.*",
        "",
    ]


def _fmt_eur_md(v) -> str:
    if v is None:
        return "—"
    v = float(v)
    if abs(v) >= 1e9:
        return f"€{v / 1e9:,.1f}B"
    if abs(v) >= 1e6:
        return f"€{v / 1e6:,.0f}M"
    return f"€{v:,.0f}"


def _sector_flow_line(f) -> str:
    flow = f.metrics.get("flow")
    label = ("EU-27 exports to China" if flow == "export"
             else "EU-27 imports from China")
    yoy = f.metrics.get("yoy_pct")
    if yoy is None:
        val = "—"
    else:
        yoy = float(yoy)
        val = f"{'+' if yoy >= 0 else '−'}{abs(yoy) * 100:.1f}% · {_fmt_eur_md(f.metrics.get('current_eur'))}"
    lb = " _(low base)_" if f.metrics.get("low_base") else ""
    cite = (f" `finding/{f.provenance.finding_ids[0]}`"
            if f.provenance.finding_ids else "")
    return f"- {label}: **{val}**{lb}{cite}"


def _render_sections(sections) -> list[str]:
    """Render the content tree (currently sector-detail) — parity with the
    HTML portal so the LLM-facing surface carries the granularity too. The
    `### {group}` heading auto-slugs to match the headline drill-down links."""
    out: list[str] = []
    for sec in sections:
        if sec.kind != "sector_detail" or not sec.sections:
            continue
        out.append("## Sector detail")
        out.append("")
        if sec.intro:
            out.append(f"*{sec.intro} {len(sec.sections)} groups, "
                       "ordered by size.*")
            out.append("")
        for grp in sec.sections:
            out.append(f"### {grp.title}")
            out.append("")
            for f in grp.findings:
                out.append(_sector_flow_line(f))
            out.append("")
    return out


def render_markdown(report: Report) -> str:
    from report_model import Report as _R  # local, keeps signature obvious
    assert isinstance(report, _R)
    m = report.meta
    lines: list[str] = []
    lines.append("# Headlines — China–Europe trade")
    lines.append("")
    if report.headline:
        lines.append(f"_Data to {_fmt_period(m.data_period)}. {report.headline.note}_")
        lines.append("")

    if report.key_indicators:
        lines.append("## Key indicators")
        lines.append("")
        lines.append("*Standing vital signs — shown every release, change or not.*")
        lines.append("")
        for ind in report.key_indicators:
            lines.append(f"- {_indicator_line(ind)}")
        lines.append("")

    lines.append("---")
    lines.append("")

    if report.headline:
        lines.extend(_render_headline(report.headline))
    if report.what_changed:
        lines.extend(_render_what_changed(report.what_changed))

    # The general LLM slot sits at the end (once per release).
    if report.headline:
        lines.append("## What the model flags across this release")
        lines.append("")
        for slot in report.headline.llm_slots:
            if slot.slot_type == "general":
                lines.extend(_llm_block(slot))

    lines.extend(_render_sections(report.sections))

    lines.append("---")
    lines.append("")
    lines.append("**Where to go deeper**")
    lines.append("")
    for name, what in _COMPANIONS:
        lines.append(f"- **{name}** — {what}.")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _fmt_period(d) -> str:
    if d is None:
        return "—"
    # d may be a date or an ISO string (from a deserialised snapshot).
    if isinstance(d, str):
        from datetime import date as _date
        try:
            d = _date.fromisoformat(d)
        except ValueError:
            return d
    return d.strftime("%b %Y")
