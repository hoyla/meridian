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
        lines.append("_No headline items this cycle._")
        lines.append("")
    for slot in h.llm_slots:
        if slot.slot_type == "specific":
            lines.extend(_llm_block(slot))
    if h.items and h.variant == "eurostat":
        lines.append(
            "*The smaller and shakier moves are in the **Sector detail** "
            "tab — not dropped, just not headlined.*"
        )
        lines.append("")
    elif h.items and h.variant == "gacc":
        lines.append(
            "*China's per-country detail (24 partners each way) is the "
            "deeper layer — not yet surfaced.*"
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
    scope = f.metrics.get("scope", "EU-27")
    label = (f"{scope} exports to China" if flow == "export"
             else f"{scope} imports from China")
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


def _deficit_line_md(f) -> str:
    m = f.metrics
    per_day = m.get("per_day_eur")
    pd = f" · €{per_day / 1e6:,.0f}M/day" if per_day else ""
    yoy = m.get("yoy_pct")
    delta = ""
    if yoy is not None:
        yoy = float(yoy)
        delta = f" · {'+' if yoy >= 0 else '−'}{abs(yoy) * 100:.1f}% YoY"
    cite = (f" `finding/{f.provenance.finding_ids[0]}`"
            if f.provenance.finding_ids else "")
    return f"- {m.get('scope', '')}: **{_fmt_eur_md(m.get('deficit_eur'))}**{pd}{delta}{cite}"


def _render_sections(sections) -> list[str]:
    """Render the content tree — parity with the HTML portal so the
    LLM-facing surface carries it too. `### {heading}` auto-slugs to match
    the headline drill-down links."""
    out: list[str] = []
    for sec in sections:
        if not (sec.sections or sec.findings or sec.metrics):
            continue
        if sec.kind == "state_of_play":
            out.append("## State of play")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro}*")
                out.append("")
            for sub in sec.sections:
                out.append(f"### {sub.title}")
                out.append("")
                if sub.intro:
                    out.append(f"*{sub.intro}*")
                    out.append("")
                for f in sub.findings:
                    out.append(_deficit_line_md(f))
                out.append("")
        elif sec.kind == "reference":
            out.append("## Methodology, sources & caveats")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro}*")
                out.append("")
            m = sec.metrics or {}
            if m.get("sources"):
                out.append("**Sources**")
                out.append("")
                for s in m["sources"]:
                    out.append(f"- **{s['source']}** — {s['note']}")
                out.append("")
            if m.get("caveats"):
                out.append("**Caveats**")
                out.append("")
                for c in m["caveats"]:
                    detail = f" {c['detail']}" if c.get("detail") else ""
                    out.append(f"- **{c['summary']}** (`{c['code']}`){detail}")
                out.append("")
        elif sec.kind == "mirror_gap":
            out.append("## Mirror-trade gaps")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro}*")
                out.append("")
            for f in sec.findings:
                m = f.metrics
                gp = (m.get("gap_pct") or 0) * 100
                ex = m.get("excess_pct")
                exc = (f" · {'+' if (ex or 0) >= 0 else '−'}{abs(ex) * 100:.1f}% "
                       "beyond CIF/FOB baseline") if ex is not None else ""
                hub = (f" ⚓ {m['hub']}: {m['hub_notes'][:160]}"
                       if m.get("hub") and m.get("hub_notes") else "")
                cite = (f" `finding/{f.provenance.finding_ids[0]}`"
                        if f.provenance.finding_ids else "")
                out.append(
                    f"- **China ↔ {m.get('partner', '')}**: China reports "
                    f"{_fmt_eur_md(m.get('gacc_eur'))}, partner reports "
                    f"{_fmt_eur_md(m.get('eurostat_eur'))} — gap "
                    f"{_fmt_eur_md(m.get('gap_eur'))} ({gp:+.1f}%){exc}.{cite}{hub}")
            out.append("")
        elif sec.kind == "structural":
            out.append("## Trade map (SITC divisions)")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro}*")
                out.append("")
            for d in sec.sections:
                m = d.metrics
                groups = m.get("groups", [])
                if groups:
                    cov = (f"{m.get('covered_share', 0) * 100:.0f}% in groups: "
                           + ", ".join(g["name"] for g in groups[:6])
                           + (f" +{len(groups) - 6} more" if len(groups) > 6 else ""))
                else:
                    cov = "— not in any editorial group"
                out.append(f"- **{d.title}** — {m.get('value_share', 0) * 100:.1f}% "
                           f"of import value · {m.get('code_count', 0)} codes · {cov}")
            out.append("")
        elif sec.kind == "sector_detail":
            out.append("## Sector detail")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro} {len(sec.sections)} groups, "
                           "ordered by size.*")
                out.append("")
            for grp in sec.sections:
                out.append(f"### {grp.title}")
                out.append("")
                if grp.intro:
                    out.append(grp.intro)
                    out.append("")
                meta = []
                if grp.facets and grp.facets.theme:
                    meta.append("themes: " + ", ".join(grp.facets.theme))
                if grp.facets and grp.facets.sector:
                    from classifications import division_title
                    meta.append("SITC: " + ", ".join(
                        division_title(c) for c in grp.facets.sector[:3]))
                if grp.facets and grp.facets.end_use:
                    meta.append("end-use: " + ", ".join(grp.facets.end_use))
                ms = grp.metrics or {}
                if ms.get("china_share_value") is not None or ms.get("china_share_kg") is not None:
                    sp = []
                    if ms.get("china_share_value") is not None:
                        sp.append(f"{ms['china_share_value'] * 100:.0f}% value")
                    if ms.get("china_share_kg") is not None:
                        sp.append(f"{ms['china_share_kg'] * 100:.0f}% volume")
                    meta.append("China share of EU imports: " + ", ".join(sp))
                if meta:
                    out.append("*" + " · ".join(meta) + "*")
                    out.append("")
                for f in grp.findings:
                    out.append(_sector_flow_line(f))
                ms = grp.metrics or {}
                if ms.get("top_cn8"):
                    out.append("- _Top products: " + " · ".join(
                        f"{t['code']} {_fmt_eur_md(t['eur'])}"
                        for t in ms["top_cn8"]) + "_")
                if ms.get("reporters"):
                    rp = []
                    for r in ms["reporters"]:
                        sh = f" ({r['share'] * 100:.0f}% of the move)" if r.get("share") is not None else ""
                        rp.append((r.get("reporter") or "") + sh)
                    out.append("- _Driven by: " + " · ".join(rp) + "_")
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
