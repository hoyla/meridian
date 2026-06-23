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

from briefing_pack._helpers import _fmt_eur, _fmt_month, _source_label
from briefing_pack.sections.diff import _shift_flow_phrase, _fmt_window_end
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
    if ind.note:
        bits.append(f"— _{ind.note}_")
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


def _yoy_arc_md(old, new) -> str:
    if old is None or new is None:
        return "—"
    def f(v):
        return f"{'+' if v >= 0 else '−'}{abs(v) * 100:.1f}%"
    return f"{f(old)} → {f(new)}"


def _render_what_changed(wc: WhatChanged) -> list[str]:
    """What *moved* since the last pack — the material YoY shifts, not a count of
    new findings (that's bookkeeping, in Sources & coverage). Shift list when
    something moved; a slim honest line otherwise."""
    shifts = wc.significant or []
    if not shifts:
        if wc.regime == "method_bump":
            msg = ("a methodology update re-stamped findings without changing "
                   "any numbers — nothing editorial moved.")
        elif wc.regime == "first_export":
            msg = ("this is the first pack from the database — everything below "
                   "is a baseline, not a change.")
        else:
            msg = ("nothing moved materially — no finding's 12-month change "
                   "shifted by more than 5 percentage points, and nothing "
                   "flipped direction.")
        return ["## What changed since the last pack", "",
                f"**Since the last pack:** {msg}", ""]
    flips = sum(1 for s in shifts if s.direction_flipped)
    lead = (f"{len(shifts)} findings moved materially (12-month change shifted "
            "by more than 5 percentage points)")
    if flips:
        lead += f", {flips} of them flipping direction"
    lead += "."
    out = ["## What changed since the last pack", "",
           f"**Since the last pack:** {lead}", ""]
    for s in shifts:
        flip = " 🔄 **flipped**" if s.direction_flipped else ""
        pp = ""
        if s.old_yoy is not None and s.new_yoy is not None:
            d = (s.new_yoy - s.old_yoy) * 100
            pp = f" ({'+' if d >= 0 else '−'}{abs(d):.1f}pp)"
        out.append(f"- **{s.group_name}** ({_shift_flow_phrase(s.subkind)}, "
                   f"12 months to {_fmt_window_end(s.window_end)}): "
                   f"{_yoy_arc_md(s.old_yoy, s.new_yoy)}{pp}{flip}")
    out += ["", "*Each figure here was already reported in an earlier pack and "
            "has since been revised — most often because a recent month's data "
            "has filled in as Eurostat's figures mature, which shifts the rolling "
            "12-month rate. These are corrections to previously-published numbers, "
            "not new findings. Where each group and partner currently stands is in "
            "**State of play**; the count of newly-added findings is in **Sources "
            "& coverage**.*", ""]
    return out


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


def _bilateral_ctx_line_md(f) -> str:
    """Nested sub-bullet under a partner flow line: YTD and latest-month value —
    the registers the 12-month headline drops. Empty when absent."""
    m = f.metrics
    bits: list[str] = []
    yp, ye, ym = m.get("ytd_pct"), m.get("ytd_eur"), m.get("ytd_months")
    if ye is not None:
        mo = f" ({ym}-mo)" if ym else ""
        pct = (f"{'+' if float(yp) >= 0 else '−'}{abs(float(yp)) * 100:.1f}% · "
               if yp is not None else "")
        bits.append(f"YTD{mo}: {pct}{_fmt_eur(ye)}")
    se = m.get("sm_eur")
    if se is not None:
        bits.append(f"latest month: {_fmt_eur(se)}")
    return f"  - _{' · '.join(bits)}_" if bits else ""


def _bilateral_balance_line_md(p) -> str:
    """The partner-level net balance (China's exports − imports) for the LLM
    surface, mirroring the portal's balance row: sign-aware label, magnitude-
    based YoY (a € swing when it flips sign / near-zero prior), plus a nested
    YTD-net sub-bullet. Both flows' tokens keep it drillable. Empty when the
    section carries no balance metrics (a single-flow partner)."""
    m = getattr(p, "metrics", None) or {}
    be = m.get("bal_eur")
    if be is None:
        return ""
    label = "China's surplus" if be >= 0 else "China's deficit"
    gloss = f"{p.title}'s {'deficit' if be >= 0 else 'surplus'}"
    pct = m.get("bal_yoy_pct")
    if m.get("bal_low_base") or pct is None:
        d = m.get("bal_delta_eur")
        val = (f"{_fmt_eur(abs(be))} ({'+' if d >= 0 else '−'}{_fmt_eur(abs(d))} YoY)"
               if d is not None else _fmt_eur(abs(be)))
    else:
        pct = float(pct)
        val = f"{'+' if pct >= 0 else '−'}{abs(pct) * 100:.1f}% · {_fmt_eur(abs(be))}"
    cite = "".join(f" `finding/{f.provenance.finding_ids[0]}`"
                   for f in p.findings if f.provenance.finding_ids)
    line = f"- {label} ({gloss}): **{val}**{cite}"
    ye = m.get("bal_ytd_eur")
    if ye is not None:
        ym = m.get("bal_ytd_months")
        mo = f" ({ym}-mo)" if ym else ""
        ylabel = "surplus" if ye >= 0 else "deficit"
        ypct = m.get("bal_ytd_pct")
        if m.get("bal_ytd_low_base") or ypct is None:
            yd = m.get("bal_ytd_delta_eur")
            ypart = (f"{'+' if yd >= 0 else '−'}{_fmt_eur(abs(yd))} YoY · "
                     if yd is not None else "")
        else:
            ypart = (f"{'+' if float(ypct) >= 0 else '−'}"
                     f"{abs(float(ypct)) * 100:.1f}% · ")
        line += f"\n  - _YTD{mo} {ylabel}: {ypart}{_fmt_eur(abs(ye))}_"
    return line


def _partner_charts_md(charts: list[dict]) -> list[str]:
    """Compact annual-by-region tables for the LLM corpus — the numbers behind
    the portal's HTML-only multi-line charts (SVG doesn't travel into the `.md`
    that the model ingests). One small table per metric (exports / imports /
    balance), regions × the latest ~3 years (including the partial current
    year), so the figures are in the text without reproducing the full history.
    The partial year's header is flagged 'YTD'."""
    if not charts:
        return []
    out: list[str] = ["**Annual trade by region (China, EUR)**", ""]
    for ch in charts:
        years = ch.get("years") or []
        if not years:
            continue
        recent = years[-3:]                       # latest ~3 incl. the partial
        partial = ch.get("partial_last_year")
        hdr = [f"{y} YTD" if y == partial else str(y) for y in recent]
        out.append(f"_{ch.get('title', ch.get('metric', ''))}_")
        out.append("")
        out.append("| Region | " + " | ".join(hdr) + " |")
        out.append("|" + "---|" * (len(recent) + 1))
        idx = [years.index(y) for y in recent]
        for s in ch.get("series") or []:
            vals = s.get("values") or []
            cells = []
            for i in idx:
                v = vals[i] if i < len(vals) else None
                cells.append(_fmt_eur(v) if v is not None else "—")
            out.append(f"| {s.get('name', '')} | " + " | ".join(cells) + " |")
        out.append("")
    if any(ch.get("partial_last_year") for ch in charts):
        out.append("_YTD = year-to-date (the current year is incomplete — not a "
                   "full-year figure)._")
        out.append("")
    return out


def _deficit_line_md(f) -> str:
    m = f.metrics
    per_day = m.get("per_day_eur")
    pd = f" · €{per_day / 1e6:,.0f}M/day" if per_day else ""
    cn = m.get("cn_per_day_eur")
    # CN-only (excl. HK/Macao) Eurostat counterpart — the published EU-China
    # basis, NOT GACC. ("China reports" stays reserved for the mirror-gap
    # section; see _deficit_row in report_render_html for the full rationale.)
    cn_note = f" (China only, excl. HK/Macao: €{cn / 1e6:,.0f}M/day)" if cn else ""
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
        bits.append(f"Source: {_source_label(p.source)}")
    if p.as_of:
        bits.append(f"as of {_fmt_month(p.as_of)}")
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
            out.append(f"## {sec.title}")
            out.append("")
            if sec.intro:
                out.append(f"*{sec.intro} {len(sec.sections)} partners.*")
                out.append("")
            out.extend(_partner_charts_md(
                (sec.metrics or {}).get("partner_charts") or []))
            for p in sec.sections:
                out.append(f"### {p.title}")
                out.append("")
                win = next((f.metrics.get("window_label") for f in p.findings
                            if f.metrics.get("window_label")), None)
                if win:
                    out.append(f"*{win}*")
                    out.append("")
                for f in p.findings:
                    out.append(_sector_flow_line(f))
                    ctx = _bilateral_ctx_line_md(f)
                    if ctx:
                        out.append(ctx)
                bal = _bilateral_balance_line_md(p)
                if bal:
                    out.append(bal)
                notes: list[str] = []
                for f in p.findings:
                    nt = f.metrics.get("note")
                    if nt and nt not in notes:
                        notes.append(nt)
                for nt in notes:
                    out.append(f"- _⚠ {nt}_")
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
                zp = _fmt_period(m.get("zscore_period")) if m.get("zscore_period") else ""
                zn = (f" · last flagged unusual {zp}: {z:.1f}σ"
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
                               f"({c.get('releases', 0):,} releases, "
                               f"updated {c.get('last_updated') or '—'})")
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
                        out.append(f"    - {_fmt_period(rel.get('period'))} "
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
