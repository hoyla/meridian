"""Report → Markdown. One renderer over the rendering-agnostic content
model (`report_model.Report`).

This is the proof that the spine works: it consumes only the model, no DB
and no analysis, and emits the LLM-facing markdown surface. The HTML
portal and any docx fallback are sibling renderers over the same model —
none canonical. It supersedes the earlier hand-built markdown prototype:
the structure now lives in the model, the prose in its `prose` fields, and
this file only formats.
"""

from __future__ import annotations

from briefing_pack._helpers import _fmt_eur
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


def _take_block_md(take) -> list[str]:
    """The per-finding LLM take under a mover — a segregated blockquote of
    leading questions. They are leads to explore, never findings; the label and
    the blockquote carry that hedge so it survives copy-paste. A placeholder
    (rejected/ungenerated) renders nothing — the deterministic mover stands."""
    if take is None or take.status != "generated" or not take.questions:
        return []
    out = ["   > 🔶 **Machine hypotheses** — unverified leads to explore, "
           "not findings:"]
    out.extend(f"   > - {q['q']}" for q in take.questions)
    return out


def _general_take_md(slot) -> list[str]:
    """The across-release 'One other thing worth a look' — one machine
    hypothesis (a short paragraph ending in a leading question) pointing at a
    buried, non-headline finding, with its citations. Empty unless generated."""
    if slot is None or slot.status != "generated" or not slot.content:
        return []
    cites = " ".join(f"`finding/{int(fid)}`" for fid in (slot.grounded_in or []))
    out = [
        "## One other thing worth a look",
        "",
        "> 🔶 **Machine hypothesis** — one unverified lead from beyond the "
        "headlines, not a finding:",
        ">",
        f"> {slot.content}",
    ]
    if cites:
        out += [">", f"> _Sources: {cites}_"]
    out.append("")
    return out


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
            lines.extend(_take_block_md(item.take))
        lines.append("")
    else:
        lines.append("_No headline items this cycle._")
        lines.append("")
    if h.items and h.variant == "eurostat":
        lines.append(
            "*The smaller and shakier moves are in the **Sector detail** "
            "tab — not dropped, just not headlined.*"
        )
        lines.append("")
    elif h.items and h.variant == "gacc":
        lines.append(
            "*China's per-country detail (24 partners each way) is below.*"
        )
        lines.append("")
    return lines


def _render_what_changed(wc: WhatChanged) -> list[str]:
    # The per-type new-findings tally lives in Sources & coverage now.
    return [
        "## What changed since the last pack",
        "",
        f"**Since the last pack:** {wc.summary}",
        "",
        "*This answers \"what changed?\". Where each group and partner currently "
        "stands is in **State of play**; the per-type count of new findings is "
        "in **Sources & coverage**.*",
        "",
    ]


def _sector_flow_line(f) -> str:
    flow = f.metrics.get("flow")
    scope = f.metrics.get("scope", "EU-27")
    if scope == "China":
        label = "China's exports" if flow == "export" else "China's imports"
    else:
        label = (f"{scope} exports to China" if flow == "export"
                 else f"{scope} imports from China")
    yoy = f.metrics.get("yoy_pct")
    if yoy is None:
        val = "—"
    else:
        yoy = float(yoy)
        val = f"{'+' if yoy >= 0 else '−'}{abs(yoy) * 100:.1f}% · {_fmt_eur(f.metrics.get('current_eur'))}"
    sm = f.metrics.get("sm_yoy_pct")
    sm_str = ""
    if sm is not None and yoy is not None:
        sm = float(sm)
        sm_str = f" · latest mo {'+' if sm >= 0 else '−'}{abs(sm) * 100:.0f}%"
    lb = " _(low base)_" if f.metrics.get("low_base") else ""
    cav = "".join(f" _({c.replace('_', ' ')})_" for c in (f.metrics.get("caveats") or []))
    cite = (f" `finding/{f.provenance.finding_ids[0]}`"
            if f.provenance.finding_ids else "")
    return f"- {label}: **{val}**{sm_str}{lb}{cav}{cite}"


def _deficit_line_md(f) -> str:
    m = f.metrics
    per_day = m.get("per_day_eur")
    pd = f" · €{per_day / 1e6:,.0f}M/day" if per_day else ""
    cn = m.get("cn_per_day_eur")
    cn_note = f" (China reports €{cn / 1e6:,.0f}M/day)" if cn else ""
    yoy = m.get("yoy_pct")
    delta = ""
    if yoy is not None:
        yoy = float(yoy)
        delta = f" · {'+' if yoy >= 0 else '−'}{abs(yoy) * 100:.1f}% YoY"
    cite = (f" `finding/{f.provenance.finding_ids[0]}`"
            if f.provenance.finding_ids else "")
    return f"- {m.get('scope', '')}: **{_fmt_eur(m.get('deficit_eur'))}**{pd}{delta}{cn_note}{cite}"


def _prov_note(sec) -> str | None:
    """A source/as-of line for an aggregate section with no per-leaf finding
    to cite (the trade map) — keeps its numbers provenance-bearing."""
    p = getattr(sec, "provenance", None)
    if not p or not (p.source or p.as_of):
        return None
    bits = []
    if p.source:
        bits.append(f"Source: {p.source}")
    if p.as_of:
        bits.append(f"as of {p.as_of}")
    tot = (sec.metrics or {}).get("total_eur")
    if tot:
        bits.append(f"total {_fmt_eur(tot)}")
    return "*" + " · ".join(bits) + " · live aggregate, no per-code finding.*"


def _about_md(sec) -> list[str]:
    """The section's 'More about this section' copy, for the LLM-facing surface
    (rendered inline, not collapsed — markdown has no panels)."""
    about = getattr(sec, "about", None)
    return ["_More about this section:_", "", about, ""] if about else []


def _flatten_md(text: str) -> str:
    """Collapse a multi-line markdown body to a single line (glossary/table
    lists in markdown stay compact)."""
    return " ".join(ln.strip() for ln in (text or "").splitlines() if ln.strip())


def _render_sections(sections) -> list[str]:
    """Render the content tree — parity with the HTML portal so the
    LLM-facing surface carries it too. Each linkable heading carries an
    explicit `<a id>` anchor (the model's slug, the same id the HTML portal
    uses) so headline drill-downs resolve without depending on the host
    markdown engine's auto-slug rule."""
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
            out.extend(_about_md(sec))
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
            out.append("## Methodology & caveats")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro}*")
                out.append("")
            m = sec.metrics or {}
            if getattr(sec, "about", None):
                out.append(sec.about)
                out.append("")
            for g in m.get("guides", []):
                out.append(f"### {g['title']}")
                out.append("")
                out.append(g["body"])
                out.append("")
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
        elif sec.kind == "gacc_bilateral":
            out.append("## China's trade by partner (GACC)")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro} {len(sec.sections)} partners.*")
                out.append("")
            for p in sec.sections:
                out.append(f"### {p.title}")
                out.append("")
                for f in p.findings:
                    out.append(_sector_flow_line(f))
                out.append("")
        elif sec.kind == "mirror_gap":
            out.append("## Mirror-trade gaps")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro}*")
                out.append("")
            out.extend(_about_md(sec))
            for f in sec.findings:
                m = f.metrics
                gp = (m.get("gap_pct") or 0) * 100
                ex = m.get("excess_pct")
                exc = (f" · {'+' if (ex or 0) >= 0 else '−'}{abs(ex) * 100:.1f}% "
                       "beyond CIF/FOB baseline") if ex is not None else ""
                z = m.get("zscore")
                zn = (f" · last flagged unusual {m.get('zscore_period') or ''}: {z:.1f}σ"
                      if z is not None else "")
                hub = (f" ⚓ {m['hub']}: {m['hub_notes'][:160]}"
                       if m.get("hub") and m.get("hub_notes") else "")
                cite = (f" `finding/{f.provenance.finding_ids[0]}`"
                        if f.provenance.finding_ids else "")
                out.append(
                    f"- **China ↔ {m.get('partner', '')}**: China reports "
                    f"{_fmt_eur(m.get('gacc_eur'))}, partner reports "
                    f"{_fmt_eur(m.get('eurostat_eur'))} — gap "
                    f"{_fmt_eur(m.get('gap_eur'))} ({gp:+.1f}%){exc}{zn}.{cite}{hub}")
            out.append("")
        elif sec.kind == "structural":
            out.append("## Trade map (SITC divisions)")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro}*")
                out.append("")
            out.extend(_about_md(sec))
            note = _prov_note(sec)
            if note:
                out.append(note)
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
                           f"of import value · {m.get('code_count', 0):,} codes · {cov}")
            out.append("")
        elif sec.kind == "sector_detail":
            out.append("## Sector detail")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro} {len(sec.sections)} groups, "
                           "grouped by SITC section (largest category first).*")
                out.append("")
            out.extend(_about_md(sec))
            cur_sec = object()
            for grp in sec.sections:
                gsec = ((grp.metrics or {}).get("section") or {}).get("code")
                if gsec != cur_sec:
                    cur_sec = gsec
                    st = ((grp.metrics or {}).get("section") or {}).get("title", "")
                    out.append(f"### {st}")
                    out.append("")
                out.append(f'<a id="{grp.id}"></a>')
                badge = ((grp.metrics or {}).get("predictability") or {}).get("badge")
                out.append(f"#### {grp.title}" + (f" {badge}" if badge else ""))
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
                    sct = (f" `finding/{ms['china_share_finding']}`"
                           if ms.get("china_share_finding") else "")
                    meta.append("China share of EU imports: " + ", ".join(sp) + sct)
                if meta:
                    out.append("*" + " · ".join(meta) + "*")
                    out.append("")
                for f in grp.findings:
                    out.append(_sector_flow_line(f))
                ms = grp.metrics or {}
                if ms.get("top_cn8"):
                    out.append("- _Top products: " + " · ".join(
                        f"{t['code']} {_fmt_eur(t['eur'])}"
                        for t in ms["top_cn8"]) + "_")
                if ms.get("reporters"):
                    rp = []
                    for r in ms["reporters"]:
                        sh = f" ({r['share'] * 100:.0f}% of the move)" if r.get("share") is not None else ""
                        rp.append((r.get("reporter") or "") + sh)
                    out.append("- _Driven by: " + " · ".join(rp) + "_")
                tr = ms.get("trajectory") or {}
                if tr:
                    parts_t = []
                    for scope in ("EU-27", "UK", "EU-27+UK"):
                        fl = tr.get(scope)
                        if not fl:
                            continue
                        sub = ", ".join(f"{flow}s {fl[flow]}"
                                        for flow in ("import", "export") if fl.get(flow))
                        parts_t.append(f"{scope}: {sub}")
                    tt = "".join(f" `finding/{i}`"
                                 for i in (ms.get("trajectory_findings") or []))
                    out.append("- _Trajectory — " + " · ".join(parts_t) + "_" + tt)
                if ms.get("china_export_share_value") is not None:
                    et = (f" `finding/{ms['china_export_share_finding']}`"
                          if ms.get("china_export_share_finding") else "")
                    out.append(f"- _China takes {ms['china_export_share_value'] * 100:.1f}% "
                               f"of EU-27 exports of this group_{et}")
                out.append("")
        elif sec.kind == "glossary":
            m = sec.metrics or {}
            if not m.get("groups"):
                continue
            out.append("## Glossary")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro}*")
                out.append("")
            for g in m["groups"]:
                out.append(f"### {g['title']}")
                out.append("")
                for t in g["terms"]:
                    out.append(f"- **{t['term']}** — {_flatten_md(t.get('body', ''))}")
                out.append("")
        elif sec.kind == "data":
            m = sec.metrics or {}
            if not m.get("tables"):
                continue
            out.append("## Tables")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro}*")
                out.append("")
            out.append("_The full workbook (every tab, every row) is the Excel "
                       "download; this surface lists the tabs rather than dumping "
                       "thousands of rows._")
            out.append("")
            for t in m["tables"]:
                out.append(f"- **{t['name']}** — {t.get('description', '')} "
                           f"({t.get('total_rows', 0):,} rows)")
            out.append("")
        elif sec.kind == "sources":
            m = sec.metrics or {}
            out.append("## Sources & coverage")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro}*")
                out.append("")
            if m.get("sources"):
                out.append("**Data sources**")
                out.append("")
                for s in m["sources"]:
                    out.append(f"- **{s['source']}** — {s['note']}")
                out.append("")
            if m.get("coverage"):
                out.append("**Period coverage**")
                out.append("")
                for c in m["coverage"]:
                    out.append(f"- **{c['source']}**: {c.get('start') or '—'} → "
                               f"{c.get('end') or '—'} "
                               f"({c.get('releases', 0):,} releases)")
                out.append("")
            if m.get("new_findings"):
                out.append(f"**New this cycle** — "
                           f"{m.get('new_findings_total', 0):,} findings added "
                           "since the last pack, by type:")
                out.append("")
                for nf in m["new_findings"]:
                    out.append(f"- {nf['count']:,} new — {nf['label']} "
                               f"(`{nf['subkind']}`)")
                out.append("")
            if m.get("manifest"):
                out.append(f"**Findings included** — "
                           f"{m.get('manifest_total', 0):,} live, by type:")
                out.append("")
                for f in m["manifest"]:
                    out.append(f"- {f['count']:,} — {f['family']}")
                out.append("")
            if m.get("appendix"):
                out.append("**Release appendix** — recent releases per source "
                           "(URL · fetch date):")
                out.append("")
                for a in m["appendix"]:
                    out.append(f"- **{a['source']}** "
                               f"({a.get('total', 0):,} releases)")
                    for rel in a.get("recent", [])[:6]:
                        out.append(f"    - {rel.get('period') or ''} "
                                   f"{rel.get('url') or ''} "
                                   f"(fetched {rel.get('fetched') or ''})")
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
        # 'One other thing worth a look' — directly under the headline, the
        # natural "...and one other thing" coda (the reader has just seen the
        # take boxes on the movers). Empty unless a take was generated.
        for slot in report.headline.llm_slots:
            if slot.slot_type == "general":
                lines.extend(_general_take_md(slot))
    if report.what_changed:
        lines.extend(_render_what_changed(report.what_changed))

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
