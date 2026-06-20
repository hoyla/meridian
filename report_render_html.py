"""Report -> HTML. The web-portal renderer — a sibling over the same
rendering-agnostic content model (`report_model.Report`) as
`report_render_markdown`.

Self-contained single document: inline CSS, inline-SVG sparklines built
straight from the model's chart `series` (no external chart lib, no
image-by-URL — the model carries data, this renderer chooses SVG). This
is what Cloud Run serves (or a static file); it needs no DB.

Guardian-flavoured styling — palette and a serif-headline identity that
evoke the Guardian. Fonts here are system stand-ins (Georgia for the slab
headline feel); the real Guardian webfonts and design tokens swap in when
wired to the Guardian design system.

The deterministic-vs-model trust boundary is rendered explicitly: LLM
slots get a visually distinct amber block, so a reader can never mistake a
(future) model interpretation for a deterministic finding — the provenance
concern made visible.

Fork-A note: `HeadlineItem.prose` carries light markdown; `_inline_md`
converts it to HTML here. When prose is decoupled into plain text +
structured emphasis, this converter goes away.
"""

from __future__ import annotations

import html
import re

from report_model import Headline, Indicator, Report, WhatChanged
from classifications import division_title  # static SITC title lookup

# Guardian Source tokens — resolved hexes. See
# ~/Code/guardian-source/CONVENTIONS.md — a shared local design-system
# reference (kept outside this repo so all local projects can use it).
_GUARDIAN_BLUE = "#052962"  # --brand-400, masthead
_NEWS = "#c70000"           # --news-400, the editorial pillar rule
_UP = "#22874d"             # --text-success, positive delta
_DOWN = "#c70000"           # --text-error, negative delta
_LINK = "#0077b6"           # --brand-500 / --text-link
_MUTED = "#707070"          # --neutral-46

_COMPANIONS = (
    ("State of play", "where each group and partner currently stands"),
    ("Sector detail", "the full per-HS-group YoY breakdown"),
    ("Data", "the underlying spreadsheet, one row per finding"),
    ("Glossary", "HS-group definitions and methodology"),
)


def _inline_md(s: str) -> str:
    """Minimal inline markdown -> HTML for prose fields (Fork-A wrinkle).
    Handles **bold**, [text](#anchor), `code`. Escapes the rest."""
    # Protect the three constructs, escape, then restore as HTML.
    tokens: list[str] = []

    def stash(repl: str) -> str:
        tokens.append(repl)
        return f"\x00{len(tokens) - 1}\x00"

    s = re.sub(r"\[([^\]]+)\]\((#[^)]+)\)",
               lambda m: stash(f'<a href="{html.escape(m.group(2))}">{html.escape(m.group(1))}</a>'),
               s)
    s = re.sub(r"\*\*([^*]+)\*\*",
               lambda m: stash(f"<strong>{html.escape(m.group(1))}</strong>"), s)
    s = re.sub(r"`([^`]+)`",
               lambda m: stash(f'<span class="token">{html.escape(m.group(1))}</span>'), s)
    s = html.escape(s)
    # Restore iteratively: a link nested inside bold means a stashed token
    # contains another placeholder, so a single pass leaves it dangling.
    while "\x00" in s:
        s = re.sub(r"\x00(\d+)\x00", lambda m: tokens[int(m.group(1))], s)
    return s


def _sparkline_svg(chart_data, w: int = 150, h: int = 36) -> str:
    """Inline-SVG sparkline from any ChartData series. No axes — a
    glanceable vital sign. Last point marked."""
    if not chart_data or len(chart_data.series) < 2:
        return ""
    vals = [p.value for p in chart_data.series]
    lo, hi = min(vals), max(vals)
    span = (hi - lo) or 1.0
    n = len(vals)
    pad = 2
    def x(i): return pad + i * (w - 2 * pad) / (n - 1)
    def y(v): return pad + (1 - (v - lo) / span) * (h - 2 * pad)
    pts = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in enumerate(vals))
    lx, ly = x(n - 1), y(vals[-1])
    return (
        f'<svg class="spark" viewBox="0 0 {w} {h}" width="{w}" height="{h}" '
        f'preserveAspectRatio="none" aria-hidden="true">'
        f'<polyline fill="none" stroke="{_GUARDIAN_BLUE}" stroke-width="1.5" points="{pts}"/>'
        f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="2.2" fill="{_NEWS}"/>'
        f"</svg>"
    )


def _indicator_card(ind: Indicator) -> str:
    delta = ""
    if ind.delta:
        col = _DOWN if ind.delta.get("direction") in ("wider", "down") else _UP
        delta = f'<div class="delta" style="color:{col}">{html.escape(ind.delta["formatted"])}</div>'
    cite = ""
    if ind.provenance.finding_ids:
        cite = f'<span class="token">finding/{ind.provenance.finding_ids[0]}</span>'
    asof = f" · as of {ind.provenance.as_of}" if ind.provenance.as_of else ""
    return (
        '<div class="kpi">'
        f'<div class="kpi-label">{html.escape(ind.label)}</div>'
        f'<div class="kpi-value">{html.escape(ind.formatted)}</div>'
        f"{delta}"
        f'<div class="kpi-spark">{_sparkline_svg(ind.chart_data)}</div>'
        f'<div class="kpi-prov">{cite}{html.escape(asof)}</div>'
        "</div>"
    )


def _llm_block(slot) -> str:
    scope = "on the lead finding" if slot.slot_type == "specific" else "across this release"
    grounded = (f"grounded in finding/{slot.grounded_in[0]}"
                if slot.grounded_in else "grounded in the findings above")
    return (
        '<div class="llm">'
        f'<div class="llm-tag">◆ AI interpretation · {html.escape(slot.slot_type)} · {scope}</div>'
        f'<div class="llm-body">Awaiting generation. Will interpret only the cited '
        f'findings ({html.escape(grounded)}) — no new facts.</div>'
        "</div>"
    )


def _headline(h: Headline) -> str:
    out = [f'<h2 class="lead">{html.escape(h.lead_title)}</h2>']
    if h.items:
        out.append('<p class="kicker">The most quotable shifts this cycle — '
                   'each ready to lift into copy, with its citation token.</p>')
        out.append('<ol class="movers">')
        for item in h.items:
            dd = (f'<a class="drill" href="#{html.escape(item.drill_down)}">detail ›</a>'
                  if item.drill_down else "")
            out.append(f'<li>{_inline_md(item.prose)} {dd}</li>')
        out.append("</ol>")
        for slot in h.llm_slots:
            if slot.slot_type == "specific":
                out.append(_llm_block(slot))
        if h.variant == "eurostat":
            out.append('<p class="note">The smaller and shakier moves are in '
                       '<strong>Sector detail</strong> — not dropped, just not '
                       'headlined.</p>')
        elif h.variant == "gacc":
            out.append('<p class="note">China&#39;s per-country detail (24 '
                       'partners each way) is below.</p>')
    else:
        out.append('<p class="note">No headline items this cycle.</p>')
    return "\n".join(out)


def _fmt_eur(v) -> str:
    if v is None:
        return "—"
    v = float(v)
    if abs(v) >= 1e9:
        return f"€{v / 1e9:,.1f}B"
    if abs(v) >= 1e6:
        return f"€{v / 1e6:,.0f}M"
    return f"€{v:,.0f}"


def _sector_flow_row(f) -> str:
    flow = f.metrics.get("flow")
    scope = f.metrics.get("scope", "EU-27")
    if scope == "China":  # GACC bilateral — the partner is the heading
        label = "China's exports" if flow == "export" else "China's imports"
    else:
        label = (f"{scope} exports to China" if flow == "export"
                 else f"{scope} imports from China")
    yoy = f.metrics.get("yoy_pct")
    val = _fmt_eur(f.metrics.get("current_eur"))
    if yoy is None:
        valstr, col = "—", _MUTED
    else:
        yoy = float(yoy)
        col = _UP if yoy > 0 else _DOWN
        valstr = f"{'+' if yoy >= 0 else '−'}{abs(yoy) * 100:.1f}% · {val}"
    cap = ' <span class="flow-cap">low base</span>' if f.metrics.get("low_base") else ""
    cite = (f'<span class="token">finding/{f.provenance.finding_ids[0]}</span>'
            if f.provenance.finding_ids else "")
    return (
        '<div class="flow">'
        f'<span class="flow-label">{html.escape(label)}{cap}</span>'
        f'<span class="flow-val" style="color:{col}">{valstr}</span>'
        f'{_sparkline_svg(f.chart_data, w=90, h=24)}'
        f'<span class="flow-cite">{cite}</span>'
        "</div>"
    )


def _deficit_row(f) -> str:
    m = f.metrics
    per_day = m.get("per_day_eur")
    pd = f" · €{per_day / 1e6:,.0f}M/day" if per_day else ""
    cn = m.get("cn_per_day_eur")
    cn_note = (f' <span style="color:{_MUTED}">(China reports €{cn / 1e6:,.0f}M/day)</span>'
               if cn else "")
    yoy = m.get("yoy_pct")
    delta = ""
    if yoy is not None:
        yoy = float(yoy)
        col = _DOWN if yoy > 0 else _UP  # a widening deficit is the "bad" direction
        delta = (f' · <span style="color:{col}">{"+" if yoy >= 0 else "−"}'
                 f'{abs(yoy) * 100:.1f}% YoY</span>')
    cite = (f'<span class="token">finding/{f.provenance.finding_ids[0]}</span>'
            if f.provenance.finding_ids else "")
    return (
        '<div class="flow">'
        f'<span class="flow-label">{html.escape(m.get("scope", ""))}</span>'
        f'<span class="flow-val">{_fmt_eur(m.get("deficit_eur"))}{pd}{delta}{cn_note}</span>'
        f'{_sparkline_svg(f.chart_data, w=90, h=24)}'
        f'<span class="flow-cite">{cite}</span>'
        "</div>"
    )


def _state_of_play_section(section) -> str:
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{html.escape(section.intro)}</p>')
    for sub in section.sections:
        out.append(f'<div class="sector" id="{html.escape(sub.id)}">')
        out.append(f'<h3 class="sector-h">{html.escape(sub.title)}</h3>')
        if sub.intro:
            out.append(f'<p class="note">{html.escape(sub.intro)}</p>')
        for f in sub.findings:
            out.append(_deficit_row(f))
        out.append("</div>")
    return "\n".join(out)


def _reference_html(section) -> str:
    m = section.metrics or {}
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{html.escape(section.intro)}</p>')
    sources = m.get("sources", [])
    if sources:
        out.append('<h3 class="ref-h">Sources</h3><ul class="ref">')
        for s in sources:
            out.append(f'<li><strong>{html.escape(s["source"])}</strong> — '
                       f'{html.escape(s["note"])}</li>')
        out.append("</ul>")
    caveats = m.get("caveats", [])
    if caveats:
        out.append('<h3 class="ref-h">Caveats</h3><ul class="ref">')
        for c in caveats:
            detail = f' {html.escape(c["detail"])}' if c.get("detail") else ""
            out.append(f'<li><strong>{html.escape(c["summary"])}</strong>'
                       f'<span class="ref-code">{html.escape(c["code"])}</span>{detail}</li>')
        out.append("</ul>")
    return "\n".join(out)


def _mirror_gap_html(section) -> str:
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{html.escape(section.intro)}</p>')
    for f in section.findings:
        m = f.metrics
        gap = m.get("gap_eur") or 0
        gp = (m.get("gap_pct") or 0) * 100
        ex = m.get("excess_pct")
        col = _DOWN if gap > 0 else _UP
        excess = ""
        if ex is not None:
            exc_col = _DOWN if ex > 0 else _MUTED
            sign = "+" if ex >= 0 else "−"
            excess = (f' · <span style="color:{exc_col}">{sign}{abs(ex) * 100:.1f}% '
                      f'beyond CIF/FOB baseline</span>')
        z = m.get("zscore")
        znote = (f' · <span class="hub">last flagged unusual {html.escape(str(m.get("zscore_period") or ""))}: '
                 f'{z:.1f}σ</span>' if z is not None else "")
        hub = ""
        if m.get("hub") and m.get("hub_notes"):
            hub = (f'<div class="hub">⚓ {html.escape(m["hub"])} — '
                   f'{html.escape(m["hub_notes"][:200])}</div>')
        cite = (f'<span class="token">finding/{f.provenance.finding_ids[0]}</span>'
                if f.provenance.finding_ids else "")
        out.append(
            '<div class="mg">'
            f'<div class="mg-h"><span class="mg-p">China ↔ {html.escape(m.get("partner", ""))}</span>'
            f'<span class="mg-g" style="color:{col}">gap {_fmt_eur(gap)} ({gp:+.1f}%){excess}{znote}</span></div>'
            f'<div class="mg-v">China reports {_fmt_eur(m.get("gacc_eur"))} · '
            f'partner reports {_fmt_eur(m.get("eurostat_eur"))} {cite}</div>'
            f"{hub}"
            "</div>"
        )
    return "\n".join(out)


def _gacc_bilateral_html(section) -> str:
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{html.escape(section.intro)} '
                   f'{len(section.sections)} partners, biggest first.</p>')
    for p in section.sections:
        out.append(f'<div class="sector" id="{html.escape(p.id)}">')
        out.append(f'<h3 class="sector-h">{html.escape(p.title)}</h3>')
        for f in p.findings:
            out.append(_sector_flow_row(f))
        out.append("</div>")
    return "\n".join(out)


def _structural_section_html(section) -> str:
    """The trade-map browse: SITC divisions, value-weighted, each showing its
    share, code count, coverage by editorial groups, and the groups within —
    so the ~43% of value in no group is visible (a 'no editorial group' tail)."""
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{html.escape(section.intro)}</p>')
    divs = section.sections
    maxshare = max((d.metrics.get("value_share", 0) for d in divs), default=1) or 1
    for d in divs:
        m = d.metrics
        share = m.get("value_share", 0)
        cov = m.get("covered_share", 0)
        n = m.get("code_count", 0)
        groups = m.get("groups", [])
        w = max(1, round(share / maxshare * 100))
        if groups:
            links = " · ".join(
                f'<a href="#{html.escape(g["slug"])}">{html.escape(g["name"])}</a>'
                for g in groups[:6]
            )
            extra = f" +{len(groups) - 6} more" if len(groups) > 6 else ""
            cover = (f'<span class="cov">{cov * 100:.0f}% in groups</span> '
                     + links + html.escape(extra))
        else:
            cover = '<span class="cov dark">— not in any editorial group</span>'
        out.append(
            f'<div class="tmrow" id="{html.escape(d.id)}">'
            f'<div class="tmhead"><span class="tmname">{html.escape(d.title)}</span>'
            f'<span class="tmval">{share * 100:.1f}% · {n} codes</span></div>'
            f'<div class="tmbar"><div class="tmfill" style="width:{w}%"></div></div>'
            f'<div class="tmgroups">{cover}</div>'
            "</div>"
        )
    return "\n".join(out)


def _sector_section(section) -> str:
    """The sector-detail tree — one anchored block per HS group. Each
    block's id is the group slug, so headline drill-down links land here."""
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{html.escape(section.intro)} '
                   "Ordered by size — filter to find a sector.</p>")
    n = len(section.sections)
    if n:
        out.append(
            '<div class="filter-bar">'
            '<input id="sector-filter" class="filter" type="text" '
            'placeholder="Filter by sector, SITC bucket or theme…" '
            'aria-label="Filter sectors" autocomplete="off">'
            f'<span id="sector-count" class="filter-count">{n} groups</span>'
            "</div>"
        )
        # Theme chips — clickable cross-cutting labels that drive the filter.
        themes = sorted({t for g in section.sections
                         if g.facets for t in g.facets.theme})
        if themes:
            chips = "".join(
                f'<button class="chip" data-q="{html.escape(t.lower())}">'
                f'{html.escape(t)}</button>' for t in themes
            )
            out.append('<div class="chips"><span class="chips-l">Themes:</span> '
                       + chips + "</div>")
    for grp in section.sections:
        f = grp.facets
        secs = f.sector if f else []
        titles = [division_title(c) for c in secs]
        themes = f.theme if f else []
        end_use = f.end_use if f else []
        # SITC division names + theme names + end-use join the filter index,
        # so "machinery", "xinjiang" or "capital" all find groups.
        data_name = (grp.title + " " + " ".join(titles) + " "
                     + " ".join(themes) + " " + " ".join(end_use)).lower()
        out.append(f'<div class="sector" id="{html.escape(grp.id)}" '
                   f'data-name="{html.escape(data_name)}">')
        out.append(f'<h3 class="sector-h">{html.escape(grp.title)}</h3>')
        if grp.intro:
            out.append(f'<p class="gdesc">{html.escape(grp.intro)}</p>')
        if themes:
            out.append('<div class="themes">'
                       + "".join(f'<span class="theme">{html.escape(t)}</span>'
                                 for t in themes) + "</div>")
        if titles or end_use:
            bits = []
            if titles:
                shown = titles[:3]
                extra = f" +{len(titles) - 3} more" if len(titles) > 3 else ""
                bits.append("SITC · " + " · ".join(html.escape(t) for t in shown)
                            + html.escape(extra))
            if end_use:
                bits.append("end-use · " + ", ".join(html.escape(e) for e in end_use))
            out.append('<div class="sitc">' + "  |  ".join(bits) + "</div>")
        ms = grp.metrics or {}
        sv, sk = ms.get("china_share_value"), ms.get("china_share_kg")
        if sv is not None or sk is not None:
            sp = []
            if sv is not None:
                sp.append(f"{sv * 100:.0f}% by value")
            if sk is not None:
                sp.append(f"{sk * 100:.0f}% by volume")
            sct = (f' <span class="token">finding/{ms["china_share_finding"]}</span>'
                   if ms.get("china_share_finding") else "")
            out.append('<div class="cshare">China = '
                       + " · ".join(sp) + " of EU-27 imports" + sct + "</div>")
        for fi in grp.findings:
            out.append(_sector_flow_row(fi))
        top = ms.get("top_cn8") or []
        if top:
            out.append('<div class="detail">Top products: '
                       + " · ".join(f'{html.escape(t["code"])} {_fmt_eur(t["eur"])}'
                                    for t in top) + "</div>")
        reps = ms.get("reporters") or []
        if reps:
            parts_r = []
            for r in reps:
                sh = f' ({r["share"] * 100:.0f}% of the move)' if r.get("share") is not None else ""
                parts_r.append(html.escape(r["reporter"] or "") + sh)
            out.append('<div class="detail">Driven by: ' + " · ".join(parts_r) + "</div>")
        tr = ms.get("trajectory") or {}
        if tr:
            parts_t = []
            for scope in ("EU-27", "UK", "EU-27+UK"):
                fl = tr.get(scope)
                if not fl:
                    continue
                sub = ", ".join(f"{flow}s {html.escape(fl[flow])}"
                                for flow in ("import", "export") if fl.get(flow))
                parts_t.append(f"<em>{scope}</em>: {sub}")
            tt = "".join(f' <span class="token">finding/{i}</span>'
                         for i in (ms.get("trajectory_findings") or []))
            out.append('<div class="detail">Trajectory — ' + " · ".join(parts_t)
                       + tt + "</div>")
        ev = ms.get("china_export_share_value")
        if ev is not None:
            et = (f' <span class="token">finding/{ms["china_export_share_finding"]}</span>'
                  if ms.get("china_export_share_finding") else "")
            out.append(f'<div class="detail">China takes {ev * 100:.1f}% '
                       f"of EU-27 exports of this group{et}</div>")
        out.append("</div>")
    out.append('<p id="sector-empty" class="note" style="display:none">'
               "No sector matches that filter.</p>")
    return "\n".join(out)


def _what_changed(wc: WhatChanged) -> str:
    return (
        '<h2 class="lead">What changed since the last pack</h2>'
        f'<p class="since"><strong>Since the last pack:</strong> {html.escape(wc.summary)}</p>'
        '<p class="note">This answers <em>what changed?</em> — where each group '
        'and partner currently stands is in <strong>State of play</strong>.</p>'
    )


_CSS = """
:root{
--masthead:#052962;--ink:#121212;--muted:#707070;--line:#dcdcdc;
--surface:#ffffff;--surface-alt:#f6f6f6;--link:#0077b6;--news:#c70000;--highlight:#ffe500;
--font-headline:'Source Serif 4','GH Guardian Headline',Georgia,'Times New Roman',serif;
--font-body:'Noto Serif','Guardian Text Egyptian',Georgia,serif;
--font-sans:'Source Sans 3','Guardian Text Sans',system-ui,-apple-system,'Helvetica Neue',Arial,sans-serif}
*{box-sizing:border-box}
body{margin:0;background:var(--surface-alt);color:var(--ink);font:16px/1.4 var(--font-sans)}
.wrap{max-width:860px;margin:0 auto;background:var(--surface)}
.masthead{background:var(--masthead);color:#fff;padding:18px 28px 16px}
.mast{font-family:var(--font-headline);font-weight:700;font-size:34px;line-height:1.05;letter-spacing:-.4px}
.sub{font-family:var(--font-headline);font-weight:400;font-size:19px;color:#cdddf6;margin-top:2px}
.subbar{padding:10px 28px;border-bottom:1px solid var(--line);display:flex;align-items:baseline;flex-wrap:wrap;gap:8px}
.subbar .meta{font-size:13px;color:var(--muted)}
.tag{background:var(--masthead);color:#fff;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;padding:2px 10px;border-radius:62.5rem}
.note-line{flex-basis:100%;font-size:13px;color:var(--muted);font-style:italic}
section{padding:18px 28px}
.kpis{display:flex;flex-wrap:wrap;gap:16px;border-bottom:1px solid var(--line)}
.kpi{flex:1 1 230px;background:var(--surface);border:1px solid var(--line);border-top:4px solid var(--news);padding:14px 16px}
.kpi-label{font-size:13px;color:var(--muted)}
.kpi-value{font-family:var(--font-headline);font-size:28px;font-weight:700;line-height:1.15;margin-top:4px}
.delta{font-size:13px;font-weight:700;margin-top:2px}
.kpi-spark{margin-top:10px}.spark{width:100%;height:36px;display:block}
.kpi-prov{margin-top:8px;font-size:12px;color:var(--muted)}
h2.lead{font-family:var(--font-headline);font-size:26px;line-height:1.15;color:var(--ink);margin:4px 0 6px;font-weight:700}
.kicker{color:var(--muted);font-size:14px;margin:0 0 12px}
ol.movers{margin:0;padding-left:24px}
ol.movers li{font-family:var(--font-body);font-size:17px;line-height:1.4;margin:0 0 14px}
ol.movers li strong{color:var(--ink);font-weight:700}
.token{font-family:ui-monospace,Menlo,monospace;font-size:12px;color:var(--muted);background:var(--surface-alt);padding:1px 5px;border-radius:4px}
a{color:var(--link);text-decoration:none;border-bottom:1px solid var(--line)}
a:hover{border-bottom-color:var(--link)}
.drill{font-size:13px;font-weight:700;white-space:nowrap;border-bottom:none}
.note{font-size:13px;color:var(--muted);font-style:italic}
.since{font-family:var(--font-body);font-size:17px;line-height:1.4}
.filter-bar{display:flex;align-items:center;gap:10px;margin:0 0 12px;padding-bottom:12px;border-bottom:1px solid var(--line)}
.filter{font-family:var(--font-sans);font-size:14px;padding:7px 10px;border:1px solid var(--line);border-radius:4px;width:280px;max-width:60%;color:var(--ink);background:var(--surface)}
.filter:focus{outline:none;border-color:var(--link);box-shadow:0 0 0 3px rgba(0,119,182,.15)}
.filter-count{font-size:13px;color:var(--muted)}
.sector{padding:12px 0;border-bottom:1px solid var(--line)}
.sector:target{background:#fffdf0;scroll-margin-top:12px}
.sector-h{font-family:var(--font-headline);font-size:18px;font-weight:700;color:var(--ink);margin:0 0 2px}
.sitc{font-size:12px;color:var(--muted);margin:0 0 4px;letter-spacing:.2px}
.cshare{font-size:12.5px;color:var(--news);font-weight:700;margin:0 0 8px}
.gdesc{font-family:var(--font-body);font-size:14px;line-height:1.45;color:var(--muted);margin:2px 0 8px}
.detail{font-size:12.5px;color:var(--muted);margin:4px 0 0}
.chips{margin:0 0 14px;font-size:13px}
.chips-l{color:var(--muted);font-weight:700;margin-right:6px}
.chip{font-family:var(--font-sans);font-size:12.5px;color:var(--masthead);background:var(--surface);border:1px solid var(--line);border-radius:62.5rem;padding:3px 11px;margin:0 6px 6px 0;cursor:pointer}
.chip:hover{border-color:var(--link)}
.chip.on{background:var(--masthead);color:#fff;border-color:var(--masthead)}
.themes{margin:0 0 6px}
.theme{display:inline-block;font-size:11px;font-weight:700;color:#7a5c00;background:#fff4d6;border-radius:62.5rem;padding:2px 9px;margin:0 5px 4px 0}
.flow{display:flex;align-items:center;gap:12px;font-size:14px;margin:4px 0}
.flow-label{flex:1 1 auto;color:var(--ink)}
.flow-cap{color:var(--news);font-weight:700;font-size:12px}
.flow-val{font-weight:700;white-space:nowrap;font-variant-numeric:tabular-nums}
.flow .spark{width:90px;height:24px;flex:0 0 auto}
.flow-cite{flex:0 0 auto}
.tmrow{padding:10px 0;border-bottom:1px solid var(--line)}
.tmhead{display:flex;justify-content:space-between;align-items:baseline;gap:10px}
.tmname{font-family:var(--font-headline);font-size:16px;font-weight:700;color:var(--ink)}
.tmval{font-size:13px;font-weight:700;color:var(--ink);white-space:nowrap;font-variant-numeric:tabular-nums}
.tmbar{height:6px;background:var(--surface-alt);margin:5px 0 5px;overflow:hidden}
.tmfill{height:6px;background:var(--masthead)}
.tmgroups{font-size:12.5px;color:var(--muted)}
.mg{padding:11px 0;border-bottom:1px solid var(--line)}
.mg-h{display:flex;justify-content:space-between;align-items:baseline;gap:10px;flex-wrap:wrap}
.mg-p{font-family:var(--font-headline);font-size:16px;font-weight:700;color:var(--ink)}
.mg-g{font-size:13.5px;font-weight:700;white-space:nowrap}
.mg-v{font-size:13.5px;color:var(--ink);margin-top:3px}
.hub{font-size:12.5px;color:var(--muted);font-style:italic;margin-top:4px}
.ref-h{font-family:var(--font-sans);font-size:13px;font-weight:700;color:var(--muted);margin:14px 0 6px}
ul.ref{margin:0 0 8px;padding-left:18px}
ul.ref li{font-size:13.5px;line-height:1.5;margin:0 0 7px;color:var(--ink)}
.ref-code{font-family:ui-monospace,Menlo,monospace;font-size:11px;color:var(--muted);background:var(--surface-alt);padding:1px 5px;border-radius:4px;margin:0 6px}
.cov{font-weight:700;color:var(--ink);margin-right:6px}
.cov.dark{color:var(--muted);font-weight:400;font-style:italic}
.llm{background:#fffbe6;border:1px solid #f3c100;border-left:4px solid #f3c100;padding:10px 14px;margin:12px 0}
.llm-tag{font-size:12px;font-weight:700;color:#7a5c00}
.llm-body{font-size:13px;color:#7a5c00;font-style:italic;margin-top:3px}
footer{padding:18px 28px 28px;border-top:1px solid var(--line)}
footer h3{font-size:13px;font-weight:700;color:var(--muted);margin:0 0 10px}
.comp{display:flex;flex-wrap:wrap;gap:12px}
.comp a{flex:1 1 180px;background:var(--surface);border:1px solid var(--line);padding:10px 12px}
.comp b{display:block;color:var(--link)}.comp span{font-size:13px;color:var(--muted)}
@media(max-width:560px){.mast{font-size:27px}.sub{font-size:16px}section{padding:14px 18px}.masthead{padding:16px 18px}.subbar{padding:10px 18px}}
"""


_FILTER_JS = """<script>
(function(){
  var f=document.getElementById('sector-filter');if(!f)return;
  var blocks=[].slice.call(document.querySelectorAll('.sector[data-name]'));
  var count=document.getElementById('sector-count');
  var empty=document.getElementById('sector-empty');
  function apply(){
    var q=f.value.trim().toLowerCase(),shown=0;
    blocks.forEach(function(b){
      var m=!q||b.getAttribute('data-name').indexOf(q)!==-1;
      b.style.display=m?'':'none';if(m)shown++;
    });
    if(count)count.textContent=q?('showing '+shown+' of '+blocks.length):(blocks.length+' groups');
    if(empty)empty.style.display=shown?'none':'block';
    [].forEach.call(document.querySelectorAll('.chip'),function(c){
      c.classList.toggle('on', c.getAttribute('data-q')===q);
    });
  }
  f.addEventListener('input',apply);
  [].forEach.call(document.querySelectorAll('.chip'),function(c){
    c.addEventListener('click',function(){
      var q=c.getAttribute('data-q');
      f.value=(f.value.trim().toLowerCase()===q)?'':q;  // toggle
      apply();
    });
  });
})();
</script>"""


def render_html(report: Report) -> str:
    m = report.meta
    period = m.data_period
    period_str = period.strftime("%B %Y") if hasattr(period, "strftime") else str(period)
    note = report.headline.note if report.headline else ""

    parts = [
        "<!doctype html><html lang=en><head><meta charset=utf-8>",
        '<meta name=viewport content="width=device-width,initial-scale=1">',
        f"<title>Meridian — China–Europe trade, {html.escape(period_str)}</title>",
        '<link rel=preconnect href="https://fonts.googleapis.com">',
        '<link rel=preconnect href="https://fonts.gstatic.com" crossorigin>',
        '<link rel=stylesheet href="https://fonts.googleapis.com/css2?'
        'family=Noto+Serif:wght@400;700&family=Source+Sans+3:wght@400;600;700&'
        'family=Source+Serif+4:wght@600;700&display=swap">',
        f"<style>{_CSS}</style></head><body><div class=wrap>",
        '<header class="masthead">',
        '<div class="mast">Meridian</div>',
        '<div class="sub">China–Europe trade</div>',
        "</header>",
        '<div class="subbar">',
        f'<span class="meta">Data to {html.escape(period_str)}</span>'
        f'<span class="tag">{html.escape(m.variant)}</span>',
        f'<div class="note-line">{html.escape(note)}</div>',
        "</div>",
    ]

    if report.key_indicators:
        parts.append('<section class="kpis">')
        for ind in report.key_indicators:
            parts.append(_indicator_card(ind))
        parts.append("</section>")

    if report.headline:
        parts.append("<section>" + _headline(report.headline) + "</section>")
    if report.what_changed:
        parts.append("<section>" + _what_changed(report.what_changed) + "</section>")
    if report.headline:
        for slot in report.headline.llm_slots:
            if slot.slot_type == "general":
                parts.append("<section><h2 class='lead'>What the model flags "
                             "across this release</h2>" + _llm_block(slot) + "</section>")

    for sec in report.sections:
        if sec.kind == "state_of_play" and sec.sections:
            parts.append("<section>" + _state_of_play_section(sec) + "</section>")
        elif sec.kind == "sector_detail" and sec.sections:
            parts.append("<section>" + _sector_section(sec) + "</section>")
        elif sec.kind == "mirror_gap" and sec.findings:
            parts.append("<section>" + _mirror_gap_html(sec) + "</section>")
        elif sec.kind == "structural" and sec.sections:
            parts.append("<section>" + _structural_section_html(sec) + "</section>")
        elif sec.kind == "gacc_bilateral" and sec.sections:
            parts.append("<section>" + _gacc_bilateral_html(sec) + "</section>")
        elif sec.kind == "reference":
            parts.append("<section>" + _reference_html(sec) + "</section>")

    parts.append("<footer><h3>Where to go deeper</h3><div class='comp'>")
    for name, what in _COMPANIONS:
        parts.append(f'<a href="#">{name and f"<b>{html.escape(name)}</b>"}'
                     f'<span>{html.escape(what)}</span></a>')
    parts.append("</div></footer></div>")
    parts.append(_FILTER_JS)
    parts.append("</body></html>")
    return "".join(parts)
