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

from briefing_pack._helpers import _fmt_eur
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
_LINE = "#dcdcdc"           # --border-primary, hairline
_SHIP_BASE = "#c3cbd4"      # muted blue-grey, the container-pictograph base

def _inline_md(s: str) -> str:
    """Minimal inline markdown -> HTML for prose fields (Fork-A wrinkle).
    Handles **bold**, [text](#anchor), `code`. Escapes the rest."""
    # Protect the three constructs, escape, then restore as HTML.
    tokens: list[str] = []

    def stash(repl: str) -> str:
        tokens.append(repl)
        return f"\x00{len(tokens) - 1}\x00"

    def _link(m):
        text, href = m.group(1), m.group(2)
        # Real links only for in-page anchors and http(s); any other target
        # (e.g. a `methodology.md#…` cross-doc ref from the source glossary) is
        # dead in the portal, so render just the text — no broken link, no raw
        # `[x](y)` litter.
        if href.startswith("#") or href.startswith("http"):
            attrs = ' target="_blank" rel="noopener"' if href.startswith("http") else ""
            return stash(f'<a href="{html.escape(href)}"{attrs}>{html.escape(text)}</a>')
        return stash(html.escape(text))

    s = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _link, s)
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


def _md_blocks_to_html(text: str) -> str:
    """Minimal block-markdown → HTML for the explanatory copy (section `about`,
    glossary definitions, methodology guides). Handles blank-line-separated
    paragraphs, `- ` bullet lists, and `### ` sub-headings; inline emphasis via
    `_inline_md`. Deliberately small — the content is authored to this subset, so
    a full markdown dependency isn't warranted (and a 47KB methodology dump was
    explicitly out of scope)."""
    if not text:
        return ""
    out: list[str] = []
    bullets: list[str] = []

    def flush_bullets():
        if bullets:
            out.append("<ul>" + "".join(f"<li>{b}</li>" for b in bullets) + "</ul>")
            bullets.clear()

    for block in re.split(r"\n\s*\n", text.strip()):
        lines = [ln.rstrip() for ln in block.splitlines() if ln.strip()]
        if not lines:
            continue
        # A block that is entirely bullet lines → one list.
        if all(ln.lstrip().startswith("- ") for ln in lines):
            for ln in lines:
                bullets.append(_inline_md(ln.lstrip()[2:]))
            flush_bullets()
            continue
        flush_bullets()
        if lines[0].startswith("### "):
            out.append(f"<h4>{_inline_md(lines[0][4:])}</h4>")
            rest = " ".join(lines[1:])
            if rest:
                out.append(f"<p>{_inline_md(rest)}</p>")
        else:
            out.append(f"<p>{_inline_md(' '.join(lines))}</p>")
    flush_bullets()
    return "".join(out)


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


def _period_label(p) -> str:
    """A short 'Mon YY' label from a SeriesPoint period (date, or ISO string
    after a snapshot round-trip)."""
    if isinstance(p, str):
        from datetime import date as _d
        try:
            p = _d.fromisoformat(p)
        except ValueError:
            return p
    return p.strftime("%b %y") if hasattr(p, "strftime") else str(p)


# Chart geometry (viewBox units; the SVG scales to its column width). Left gutter
# holds the y-axis value labels; bottom gutter the x-axis labels.
_CW, _CH = 360, 196
_GL, _GB, _PT, _PR = 48, 22, 12, 10  # gutters: y-labels / x-labels / top / right
_BAR_ALT = "#7aa6d6"  # lighter blue for the second bar (exports)
_LINE_LEGEND = ('<span class="sw sw-prior"></span> earlier · '
                '<span class="sw sw-curr"></span> latest 12 months · auto-scaled')


def _y_axis(lo: float, hi: float, zero_based: bool) -> str:
    """3 horizontal gridlines (top/mid/bottom) with €-labels in the left gutter.
    Bars are zero-based (honest scale comparison); the auto-scaled line is not."""
    x0, x1 = _GL, _CW - _PR
    y0, y1 = _PT, _CH - _GB
    out = []
    for frac, val in ((0.0, hi), (0.5, (hi + lo) / 2), (1.0, lo)):
        yy = y0 + frac * (y1 - y0)
        out.append(f'<line x1="{x0}" y1="{yy:.1f}" x2="{x1}" y2="{yy:.1f}" '
                   f'stroke="{_LINE}" stroke-width="1"/>')
        out.append(f'<text x="{x0 - 4}" y="{yy + 3:.1f}" text-anchor="end" '
                   f'class="ct">{html.escape(_fmt_eur(val))}</text>')
    return "".join(out)


def _x_tick_indices(n: int) -> list[int]:
    """Evenly spaced x-tick indices including both ends, ~4-5 intervals on a
    'nice' month step (1/2/3/6/12/24/36/60), so the time span is legible — a
    9-year deficit gets year ticks, a 2-year sector gets 6-month ticks. Without
    intermediates, two end labels can't tell 4 months from 9 years."""
    if n <= 2:
        return list(range(n))
    step = 1
    for s in (1, 2, 3, 6, 12, 24, 36, 60, 120):
        step = s
        if (n - 1) / s <= 5:
            break
    idxs = list(range(0, n - 1, step))
    if len(idxs) >= 2 and (n - 1 - idxs[-1]) < step * 0.7:
        idxs.pop()  # drop a near-end intermediate so it can't crowd the end label
    idxs.append(n - 1)
    return idxs


def _line_chart_svg(chart_data, *, split_last: int = 12) -> str:
    """Inline-SVG line chart with real axes — the docx trajectory graph, restored
    and made legible. The last `split_last` points (current 12 months) are red
    over the earlier period in grey, with a divider. Y-axis (3 €-gridlines) and
    x-axis (start / divider / end months) are drawn; auto-scaled (not zero-based)
    — fine for a trend, and the legend says so. No chart lib."""
    if not chart_data or len(chart_data.series) < 2:
        return ""
    pts = chart_data.series
    vals = [p.value for p in pts]
    lo, hi = min(vals), max(vals)
    if lo == hi:
        lo, hi = (lo * 0.95, hi * 1.05) if hi else (0.0, 1.0)
    span = (hi - lo) or 1.0
    n = len(vals)
    x0, x1, y0, y1 = _GL, _CW - _PR, _PT, _CH - _GB
    def x(i): return x0 + i * (x1 - x0) / (n - 1)
    def y(v): return y0 + (1 - (v - lo) / span) * (y1 - y0)
    split = max(1, n - split_last) if n > split_last else None

    def poly(a, b, color, wdt):
        seg = " ".join(f"{x(i):.1f},{y(vals[i]):.1f}" for i in range(a, b))
        return (f'<polyline fill="none" stroke="{color}" stroke-width="{wdt}" '
                f'points="{seg}"/>') if seg else ""

    body = [_y_axis(lo, hi, zero_based=False)]
    # Vertical gridlines + intermediate date labels at 'nice' steps, so the time
    # span reads at a glance (the divider is a separate dashed line, unlabelled —
    # on a long series its label would crowd the end).
    xticks = _x_tick_indices(n)
    body.append("".join(
        f'<line x1="{x(i):.1f}" y1="{y0}" x2="{x(i):.1f}" y2="{y1}" '
        f'stroke="{_LINE}" stroke-width="1"/>' for i in xticks))
    if split is not None:
        body.append(poly(0, split + 1, _MUTED, 1.4))
        body.append(poly(split, n, _NEWS, 2.0))
        body.append(f'<line x1="{x(split):.1f}" y1="{y0}" x2="{x(split):.1f}" '
                    f'y2="{y1}" stroke="{_MUTED}" stroke-width="1" '
                    'stroke-dasharray="2 2"/>')
    else:
        body.append(poly(0, n, _NEWS, 2.0))
    lx, ly = x(n - 1), y(vals[-1])
    body.append(f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="2.6" fill="{_NEWS}"/>')
    for i in xticks:
        anchor = "start" if i == 0 else ("end" if i == n - 1 else "middle")
        body.append(f'<text x="{x(i):.1f}" y="{_CH - 6}" text-anchor="{anchor}" '
                    f'class="ct">{html.escape(_period_label(pts[i].period))}</text>')
    return (f'<svg class="chart" viewBox="0 0 {_CW} {_CH}" width="100%" '
            'preserveAspectRatio="xMidYMid meet" role="img" '
            'aria-label="monthly trajectory">' + "".join(body) + "</svg>")


def _bar_chart_svg(bars: list[dict]) -> str:
    """Inline-SVG vertical bar chart for relative-scale comparisons (e.g. imports
    vs exports) — what a single line can't show. **Zero-based** y-axis so bar
    heights compare honestly. bars = [{label, value, color?}]."""
    bars = [b for b in bars if b.get("value") is not None]
    if not bars:
        return ""
    vals = [float(b["value"]) for b in bars]
    hi = max(vals) or 1.0
    x0, x1, y0, y1 = _GL, _CW - _PR, _PT, _CH - _GB
    n = len(bars)
    slot = (x1 - x0) / n
    bw = min(slot * 0.5, 64)
    def y(v): return y0 + (1 - v / hi) * (y1 - y0)
    out = [_y_axis(0.0, hi, zero_based=True)]
    for i, b in enumerate(bars):
        cx = x0 + slot * (i + 0.5)
        v = float(b["value"])
        by = y(v)
        out.append(f'<rect x="{cx - bw / 2:.1f}" y="{by:.1f}" width="{bw:.1f}" '
                   f'height="{y1 - by:.1f}" fill="{b.get("color", _GUARDIAN_BLUE)}"/>')
        out.append(f'<text x="{cx:.1f}" y="{by - 4:.1f}" text-anchor="middle" '
                   f'class="ct">{html.escape(_fmt_eur(v))}</text>')
        out.append(f'<text x="{cx:.1f}" y="{_CH - 6}" text-anchor="middle" '
                   f'class="ct">{html.escape(str(b["label"]))}</text>')
    return (f'<svg class="chart" viewBox="0 0 {_CW} {_CH}" width="100%" '
            'preserveAspectRatio="xMidYMid meet" role="img" '
            'aria-label="bar comparison">' + "".join(out) + "</svg>")


def _chart_card(title: str, value: str, legend: str, svg: str, *,
                sub: str = "") -> str:
    """A chart with its number / title / key in a meta column to the LEFT of the
    plot (so the plot isn't stretched and has room for axes). `sub` is a small
    caption under the value (e.g. "12-month total" — the headline figure is an
    annual total while the plot is a monthly series). The card self-wraps: meta
    left on a wide card, on top when narrow (mobile, or two side by side in a
    .chart-row) — no media query needed."""
    if not svg:
        return ""
    val = f'<div class="cc-value">{html.escape(value)}</div>' if value else ""
    sub_html = f'<div class="cc-sub">{html.escape(sub)}</div>' if (value and sub) else ""
    leg = f'<div class="cc-legend">{legend}</div>' if legend else ""
    return (
        '<figure class="chartcard"><div class="cc-meta">'
        f'<div class="cc-title">{html.escape(title)}</div>{val}{sub_html}{leg}</div>'
        f'<div class="cc-plot">{svg}</div></figure>'
    )


def _donut_svg(share: float, *, size: int = 116, label: str = "") -> str:
    """A part-of-whole donut (one share of a whole). Stroke-dasharray on a ring,
    centre percentage. Ready for the China-import-share indicator once an
    all-goods denominator is ingested."""
    share = max(0.0, min(1.0, float(share)))
    r = size / 2 - 9
    import math
    circ = 2 * math.pi * r
    on = circ * share
    cx = cy = size / 2
    return (
        f'<svg class="donut" viewBox="0 0 {size} {size}" width="{size}" '
        f'height="{size}" role="img" aria-label="{html.escape(label)} '
        f'{share * 100:.0f}%">'
        f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="{_LINE}" '
        'stroke-width="11"/>'
        f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="{_GUARDIAN_BLUE}" '
        f'stroke-width="11" stroke-dasharray="{on:.1f} {circ - on:.1f}" '
        f'stroke-dashoffset="{circ / 4:.1f}" transform="rotate(-90 {cx} {cy})" '
        'stroke-linecap="butt"/>'
        f'<text x="{cx}" y="{cy + 1}" text-anchor="middle" dominant-baseline="middle" '
        f'class="donut-pct">{share * 100:.0f}%</text>'
        "</svg>"
    )


def _container_gauge_svg(frac: float, *, n: int = 24) -> str:
    """A small, muted container-ship pictograph: n deck containers on a hull, the
    last round(frac·n) highlighted. It illustrates ONE ratio — the mirror-gap
    excess over the CIF/FOB freight baseline — as a proportion of a fixed,
    illustrative stack; it is NOT a real container count. Two fills only (muted
    base + brand highlight) so it informs without shouting."""
    frac = max(0.0, min(1.0, float(frac)))
    k = max(1, round(frac * n))              # highlighted containers, filled from the bow
    W, H = 300, 64
    top, deck, bottom = 16, 42, 56           # stack top, deck line, hull bottom
    cstart, cend = 36, 282                    # container tier (stern area kept clear at left)
    rows = 2
    cols = max(1, n // rows)
    cw = (cend - cstart) / cols
    ch = (deck - top) / rows
    gap = 1.4
    cells = []
    for c in range(cols):
        for r in range(rows):
            rank = (cols - 1 - c) * rows + r  # 0 = bow (rightmost); fill the band from there
            cells.append(
                f'<rect x="{cstart + c * cw + gap:.1f}" y="{top + r * ch + gap:.1f}" '
                f'width="{cw - 2 * gap:.1f}" height="{ch - 2 * gap:.1f}" rx="1" '
                f'fill="{_GUARDIAN_BLUE if rank < k else _SHIP_BASE}"/>')
    boxes = "".join(cells)
    # Squared (slim, slightly-raked) stern at the left; raked pointed bow at the
    # right — and a slim aft funnel + bridge so it reads as a ship, not a barge.
    hull = (f'<path d="M10,{deck} L286,{deck} L296,{deck + 7} L282,{bottom} '
            f'L16,{bottom} Z" fill="{_SHIP_BASE}"/>')
    bridge = f'<rect x="14" y="24" width="15" height="{deck - 24}" rx="1" fill="{_SHIP_BASE}"/>'
    funnel = f'<rect x="18.5" y="6" width="6" height="18" rx="1.5" fill="{_SHIP_BASE}"/>'
    return (f'<svg class="ship" viewBox="0 0 {W} {H}" width="240" role="img" '
            f'aria-label="about {frac * 100:.0f}% beyond the freight baseline">'
            + hull + bridge + funnel + boxes + "</svg>")


def _more_about(section) -> str:
    """The collapsed 'More about this section' disclosure carrying the section's
    longer explanatory `about` copy (ported from the Findings preamble)."""
    about = getattr(section, "about", None)
    if not about:
        return ""
    return (
        '<details class="more"><summary>More about this section</summary>'
        f'<div class="more-body">{_md_blocks_to_html(about)}</div>'
        "</details>"
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
    prov = f'<div class="kpi-prov">{cite}{html.escape(asof)}</div>'

    if ind.chart == "donut":
        share = ind.value if 0 <= ind.value <= 1 else 0.0
        if (not share) and ind.chart_data:
            share = ind.chart_data.extra.get("share", 0.0)
        return (
            '<div class="kpi kpi-donut">'
            f'<div class="kpi-label">{html.escape(ind.label)}</div>'
            f'<div class="kpi-donut-wrap">{_donut_svg(share, label=ind.label)}</div>'
            f"{delta}{prov}"
            "</div>"
        )

    # bignumber (a level, no series) shows no sparkline; sparkline indicators do.
    spark = ""
    if ind.chart_data and ind.chart_data.series:
        spark = f'<div class="kpi-spark">{_sparkline_svg(ind.chart_data)}</div>'
    return (
        '<div class="kpi">'
        f'<div class="kpi-label">{html.escape(ind.label)}</div>'
        f'<div class="kpi-value">{html.escape(ind.formatted)}</div>'
        f"{delta}{spark}{prov}"
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


def _take_block_html(take) -> str:
    """The per-finding LLM take — leading questions in a visually segregated
    block (the reliable 'unverified, leads only' hedge). A placeholder
    (rejected/ungenerated) renders nothing; the deterministic mover stands."""
    if take is None or take.status != "generated" or not take.questions:
        return ""
    qs = "".join(f"<li>{html.escape(q.get('q', ''))}</li>" for q in take.questions)
    return (
        '<div class="take">'
        '<div class="take-tag">◆ Machine hypotheses — unverified leads to '
        'explore, not findings</div>'
        f'<ul class="take-qs">{qs}</ul>'
        "</div>"
    )


def _general_take_html(slot) -> str:
    """The across-release 'One other thing worth a look' — one machine
    hypothesis (a short paragraph ending in a leading question) pointing at a
    buried, non-headline finding, with per-fact citations. Renders nothing
    unless generated (a quiet release / abstention shows no box)."""
    if slot is None or slot.status != "generated" or not slot.content:
        return ""
    cites = " ".join(
        f'<span class="token">finding/{int(fid)}</span>'
        for fid in (slot.grounded_in or [])
    )
    return (
        '<h2 class="lead">One other thing worth a look</h2>'
        '<div class="take">'
        '<div class="take-tag">◆ Machine hypothesis — one unverified lead from '
        'beyond the headlines, not a finding</div>'
        f'<p class="take-prose">{html.escape(slot.content)}</p>'
        + (f'<p class="take-cite">{cites}</p>' if cites else "")
        + "</div>"
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
            # Cross-cutting theme chips for the mover's group — clickable: they
            # filter the Sector detail list to that theme (the `mover-chip` marker
            # tells the JS to scroll the list into view).
            themes = item.facets.theme if item.facets else []
            chips = ""
            if themes:
                chips = '<div class="mover-chips">' + "".join(
                    f'<button class="chip mover-chip" data-q="{html.escape(t.lower())}">'
                    f'{html.escape(t)}</button>' for t in themes) + "</div>"
            out.append(f'<li>{_inline_md(item.prose)} {dd}{chips}'
                       f'{_take_block_html(item.take)}</li>')
        out.append("</ol>")
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
    # Per-row caveat flags (partial window, low kg coverage…) — what's unusual
    # about this row; full definitions in the Methodology tab.
    cap += "".join(
        f' <span class="flow-cav" title="Methodology caveat: {html.escape(c)}">'
        f'{html.escape(c.replace("_", " "))}</span>'
        for c in (f.metrics.get("caveats") or []))
    cite = (f'<span class="token">finding/{f.provenance.finding_ids[0]}</span>'
            if f.provenance.finding_ids else "")
    # Latest-month register beside the 12-month figure (an acceleration hint —
    # the Findings doc shows both; muted because it swings on lumpy categories).
    sm = f.metrics.get("sm_yoy_pct")
    sm_str = ""
    if sm is not None and yoy is not None:
        sm = float(sm)
        sm_str = (f'<span class="flow-sm">latest mo '
                  f'{"+" if sm >= 0 else "−"}{abs(sm) * 100:.0f}%</span>')
    return (
        '<div class="flow">'
        f'<span class="flow-label">{html.escape(label)}{cap}</span>'
        f'<span class="flow-val" style="color:{col}">{valstr}</span>'
        f'{sm_str}'
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
    out.append(_more_about(section))
    for sub in section.sections:
        out.append(f'<div class="sector" id="{html.escape(sub.id)}">')
        out.append(f'<h3 class="sector-h">{html.escape(sub.title)}</h3>')
        if sub.intro:
            out.append(f'<p class="note">{html.escape(sub.intro)}</p>')
        for f in sub.findings:
            out.append(_deficit_row(f))
        # The headline (EU-27, the first scope) deficit's monthly trajectory —
        # the standing level made visible as a chart (meta-left, real axes).
        charted = next((f for f in sub.findings
                        if f.chart_data and f.chart_data.series), None)
        if charted:
            out.append(_chart_card(
                f"{charted.metrics.get('scope', '')} deficit with China",
                _fmt_eur(charted.metrics.get("deficit_eur")),
                _LINE_LEGEND,
                _line_chart_svg(charted.chart_data),
                sub="12-month total · monthly series"))
        out.append("</div>")
    return "\n".join(out)


def _reference_html(section) -> str:
    m = section.metrics or {}
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{html.escape(section.intro)}</p>')
    # In the Methodology tab the explanatory copy is the main content, so it's
    # shown expanded here (not behind a 'More about' disclosure as on the main
    # page).
    about = getattr(section, "about", None)
    if about:
        out.append(f'<div class="prose">{_md_blocks_to_html(about)}</div>')
    for g in m.get("guides", []):
        out.append(f'<h3 class="ref-h2">{html.escape(g["title"])}</h3>')
        out.append(f'<div class="prose">{_md_blocks_to_html(g["body"])}</div>')
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


def _sources_html(section) -> str:
    """The Sources & coverage tab: data sources, period coverage (a small
    table), and a readable findings manifest. The Trade Map renders after this
    in the same tab (assembled in render_html)."""
    m = section.metrics or {}
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{html.escape(section.intro)}</p>')
    sources = m.get("sources", [])
    if sources:
        out.append('<h3 class="ref-h2">Data sources</h3><ul class="ref">')
        for s in sources:
            out.append(f'<li><strong>{html.escape(s["source"])}</strong> — '
                       f'{html.escape(s["note"])}</li>')
        out.append("</ul>")
    cov = m.get("coverage", [])
    if cov:
        out.append('<h3 class="ref-h2">Period coverage</h3>'
                   '<div class="dt-scroll"><table class="dtable"><thead><tr>'
                   "<th>Source</th><th>From</th><th>To</th><th>Releases</th>"
                   "</tr></thead><tbody>")
        for c in cov:
            out.append(
                f'<tr><td>{html.escape(c["source"])}</td>'
                f'<td>{html.escape(str(c.get("start") or "—"))}</td>'
                f'<td>{html.escape(str(c.get("end") or "—"))}</td>'
                f'<td>{c.get("releases", 0):,}</td></tr>')
        out.append("</tbody></table></div>")
    # New findings this cycle, by type (moved here from 'What changed' — it's a
    # coverage tally, not substance). Sits with Period coverage.
    nf = m.get("new_findings", [])
    if nf:
        nft = m.get("new_findings_total", 0)
        out.append('<h3 class="ref-h2">New this cycle</h3>'
                   f'<p class="kicker">{nft:,} findings added since the last pack, '
                   "by type:</p><ul class=\"ref\">")
        for f in nf:
            out.append(f'<li><strong>{f["count"]:,}</strong> new — '
                       f'{html.escape(f["label"])} '
                       f'<span class="ref-code">{html.escape(f["subkind"])}</span></li>')
        out.append("</ul>")
    man = m.get("manifest", [])
    if man:
        total = m.get("manifest_total", 0)
        out.append('<h3 class="ref-h2">Findings included</h3>'
                   f'<p class="kicker">{total:,} live findings in this pack, '
                   "by type:</p><ul class=\"ref\">")
        for f in man:
            out.append(f'<li><strong>{f["count"]:,}</strong> — '
                       f'{html.escape(f["family"])}</li>')
        out.append("</ul>")
    # Per-source release appendix (the drill-back trail) behind a per-source
    # expander — the recent releases with their URL + fetch date.
    apx = m.get("appendix", [])
    if apx:
        out.append('<h3 class="ref-h2">Release appendix</h3>'
                   '<p class="kicker">Recent releases per source, with the '
                   "third-party URL and the date we fetched it — click a source "
                   "to expand.</p>")
        for a in apx:
            out.append(
                '<details class="partner"><summary>'
                f'<span class="pp-name">{html.escape(a["source"])}</span>'
                f'<span class="pp-fig">{a.get("total", 0):,} releases</span>'
                '</summary><div class="pp-body"><ul class="ref">')
            for rel in a.get("recent", []):
                url = rel.get("url") or ""
                link = (f' <a href="{html.escape(url)}" target="_blank" '
                        'rel="noopener">source ›</a>') if url else ""
                title = (rel.get("title") or "").strip()
                title = (title[:80] + "…") if len(title) > 80 else title
                fetched = rel.get("fetched") or ""
                ft = (f' <span class="note">fetched {html.escape(fetched)}</span>'
                      if fetched else "")
                out.append(f'<li>{html.escape(rel.get("period") or "")} — '
                           f'{html.escape(title)}{link}{ft}</li>')
            out.append("</ul></div></details>")
    return "\n".join(out)


def _glossary_html(section) -> str:
    """The Glossary tab — definitions grouped by category, with a live filter.
    Each item carries a `data-name` so the filter (and group auto-hide) works
    with no server round-trip."""
    m = section.metrics or {}
    groups = m.get("groups", [])
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{html.escape(section.intro)}</p>')
    if groups:
        out.append(
            '<div class="filter-bar">'
            '<input id="glossary-filter" class="filter" type="text" '
            'placeholder="Filter terms…" aria-label="Filter glossary" '
            'autocomplete="off">'
            '<span id="glossary-count" class="filter-count"></span></div>'
        )
    for g in groups:
        out.append(f'<section class="gloss-group"><h3 class="ref-h2">'
                   f'{html.escape(g["title"])}</h3>')
        for t in g["terms"]:
            term = t.get("term", "")
            out.append(
                f'<div class="gloss-item" data-name="{html.escape(term.lower())}">'
                f'<div class="gterm">{html.escape(term)}</div>'
                f'<div class="gdef">{_md_blocks_to_html(t.get("body", ""))}</div>'
                "</div>"
            )
        out.append("</section>")
    out.append('<p id="glossary-empty" class="note" style="display:none">'
               "No term matches that filter.</p>")
    return "\n".join(out)


# Columns whose integers are identifiers / codes / years, NOT quantities — never
# comma-group these (a finding id 71942 must not read as "71,942"; a year 2018
# must not read as "2,018"). Everything else that's numeric gets thousands commas
# so a count is unmistakably a count.
_NOCOMMA_COL = re.compile(
    r"(^|_)(id|ids|code|codes|cn8|nc|iso2?|year|years|rank)($|_)", re.I)


def _fmt_cell(c, header: str = "") -> str:
    """A data-table cell → display string. Genuine quantities are comma-grouped
    at full precision (no rounding — a rounded share would misread); identifier /
    code / year columns are left ungrouped. The .xlsx download carries the
    properly-typed version."""
    if c is None:
        return ""
    if isinstance(c, bool):
        return "yes" if c else ""
    if isinstance(c, (int, float)):
        return str(c) if (header and _NOCOMMA_COL.search(header)) else f"{c:,}"
    return str(c)


def _one_table_html(t: dict, *, hidden: bool, xlsx: bool = True) -> str:
    name = t["name"]
    headers = t.get("headers", [])
    th = "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
    trs = []
    for r in t.get("rows", []):
        cells = []
        for i, c in enumerate(r):
            hdr = headers[i] if i < len(headers) else ""
            disp = html.escape(_fmt_cell(c, hdr))
            # data-raw carries the ungrouped value so Copy-as-TSV pastes a clean
            # number into Sheets/Excel (commas in the display would break paste).
            if isinstance(c, (int, float)) and not isinstance(c, bool):
                cells.append(f'<td data-raw="{html.escape(str(c))}">{disp}</td>')
            else:
                cells.append(f"<td>{disp}</td>")
        trs.append(f"<tr>{''.join(cells)}</tr>")
    trunc = ""
    if t.get("shown_rows", 0) < t.get("total_rows", 0):
        trunc = (f'<p class="note">Showing {t["shown_rows"]:,} of '
                 f'{t["total_rows"]:,} rows — the full set is in the Excel '
                 "download.</p>")
    style = ' style="display:none"' if hidden else ""
    # Download (whole workbook) + Copy (this table) are the same-size buttons,
    # download first, tip beside Copy — so the per-table pills aren't upstaged by
    # a big CTA. Only one table is visible at a time, so the download shows once.
    dl = ('<a class="btn btn-sm" href="data.xlsx" download>⤓ Download Excel '
          "workbook</a>" if xlsx else "")
    return (
        f'<div class="dt-wrap" id="dt-{html.escape(name)}"{style}>'
        f'<div class="dt-head"><span class="dt-desc">{html.escape(t.get("description", ""))}</span>'
        '<div class="dt-actions">'
        f"{dl}"
        f'<button class="btn btn-sm copy-tsv" data-table="dt-{html.escape(name)}">'
        "⧉ Copy as TSV</button>"
        '<span class="dt-tip">pastes into Sheets / Excel</span>'
        "</div></div>"
        f'<div class="dt-scroll"><table class="dtable"><thead><tr>{th}</tr></thead>'
        f'<tbody>{"".join(trs)}</tbody></table></div>'
        f"{trunc}</div>"
    )


def _data_tables_html(section, *, xlsx: bool = True) -> str:
    """The Tables tab — embedded digestible spreadsheet tabs (pill-switched),
    a Copy-as-TSV button per table, and a full-workbook .xlsx download. The
    heavy tabs are listed as download-only (no thousands of rows inline)."""
    m = section.metrics or {}
    tables = m.get("tables", [])
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{html.escape(section.intro)}</p>')
    # The pills lead (table selector); the workbook download + Copy-as-TSV are
    # same-size buttons in each table's header (no big CTA upstaging the pills).
    inline = [t for t in tables if t.get("inline") and t.get("rows")]
    others = [t for t in tables if not (t.get("inline") and t.get("rows"))]
    if inline:
        pills = "".join(
            f'<button class="dtab{" on" if i == 0 else ""}" '
            f'data-target="dt-{html.escape(t["name"])}">{html.escape(t["name"])} '
            f'<span class="dtab-n">{t["total_rows"]:,}</span></button>'
            for i, t in enumerate(inline))
        out.append(f'<div class="dtabs">{pills}</div>')
        for i, t in enumerate(inline):
            out.append(_one_table_html(t, hidden=(i != 0), xlsx=xlsx))
    if others:
        out.append('<div class="data-more"><h3 class="ref-h2">Also in the '
                   "workbook</h3><ul class=\"ref\">")
        for t in others:
            out.append(f'<li><strong>{html.escape(t["name"])}</strong> — '
                       f'{html.escape(t.get("description", ""))} '
                       f'<span class="note">({t.get("total_rows", 0):,} rows — '
                       "in the download)</span></li>")
        out.append("</ul></div>")
    if not tables:
        out.append('<p class="note">No tables in this snapshot.</p>')
    return "\n".join(out)


def _mirror_gap_html(section) -> str:
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{html.escape(section.intro)}</p>')
    out.append(_more_about(section))
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
        # Container-ship pictograph beside the text — only where the excess over
        # the freight baseline is materially positive (≈1+ container); the bloc
        # and the net-negative partners get none, which is the honest read.
        ship_svg = ship_cap = ""
        if ex is not None and ex >= 0.03:
            ship_svg = f'<div class="mg-ship">{_container_gauge_svg(ex)}</div>'
            cap = (f"Highlighted: the {ex * 100:.1f}% of "
                   f"{m.get('partner', '')}'s reported imports from China beyond "
                   "what China's own export figures + normal freight explain "
                   "(each block ≈ 4%).")
            ship_cap = f'<div class="ship-cap">{html.escape(cap)}</div>'
        # Text block (heading → gap stats below it → reports + finding token),
        # with the ship beside it; the small captions go full width below both.
        gapline = (f'<div class="mg-g" style="color:{col}">gap {_fmt_eur(gap)} '
                   f'({gp:+.1f}%){excess}{znote}</div>')
        out.append(
            '<div class="mg"><div class="mg-main"><div class="mg-text">'
            f'<div class="mg-p">China ↔ {html.escape(m.get("partner", ""))}</div>'
            f'{gapline}'
            f'<div class="mg-v">China reports {_fmt_eur(m.get("gacc_eur"))} · '
            f'partner reports {_fmt_eur(m.get("eurostat_eur"))} {cite}</div>'
            f'</div>{ship_svg}</div>'
            f"{hub}{ship_cap}"
            "</div>"
        )
    return "\n".join(out)


def _gacc_bilateral_html(section) -> str:
    """Progressive disclosure: one collapsed button per partner (name + a
    headline figure), expanding on click to that partner's flows. Keeps ~24
    partners compact while offering full per-country granularity on demand."""
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{html.escape(section.intro)} '
                   f'{len(section.sections)} partners, biggest first — '
                   "click a partner to expand.</p>")
    out.append(_more_about(section))
    for p in section.sections:
        # Collapsed summary headline: China's exports (the primary read), else
        # whatever the first finding is — so the button is useful before opening.
        hdr = next((f for f in p.findings if f.metrics.get("flow") == "export"),
                   p.findings[0] if p.findings else None)
        summ = ""
        if hdr:
            lab = ("China's exports" if hdr.metrics.get("flow") == "export"
                   else "China's imports")
            val = _fmt_eur(hdr.metrics.get("current_eur"))
            yoy = hdr.metrics.get("yoy_pct")
            if yoy is not None:
                yoy = float(yoy)
                col = _UP if yoy > 0 else _DOWN
                summ = (f'<span class="pp-fig">{lab} {val} '
                        f'<span style="color:{col}">{"+" if yoy >= 0 else "−"}'
                        f'{abs(yoy) * 100:.1f}%</span></span>')
            else:
                summ = f'<span class="pp-fig">{lab} {val}</span>'
        out.append(f'<details class="partner" id="{html.escape(p.id)}">')
        out.append(f'<summary><span class="pp-name">{html.escape(p.title)}</span>'
                   f'{summ}</summary><div class="pp-body">')
        for f in p.findings:
            out.append(_sector_flow_row(f))
        out.append("</div></details>")
    return "\n".join(out)


def _structural_section_html(section) -> str:
    """The trade-map browse: SITC divisions, value-weighted, each showing its
    share, code count, coverage by editorial groups, and the groups within —
    so the ~43% of value in no group is visible (a 'no editorial group' tail)."""
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{html.escape(section.intro)}</p>')
    out.append(_more_about(section))
    # These are live aggregates with no per-code finding, so they carry the
    # section's own provenance (source + as-of + total) rather than a finding
    # token — the trade-map numbers stay attributable.
    p = getattr(section, "provenance", None)
    if p and (p.source or p.as_of):
        bits = []
        if p.source:
            bits.append(f"Source: {html.escape(p.source)}")
        if p.as_of:
            bits.append(f"as of {html.escape(str(p.as_of))}")
        tot = (section.metrics or {}).get("total_eur")
        if tot:
            bits.append(f"total {html.escape(_fmt_eur(tot))}")
        out.append(f'<p class="source">{" · ".join(bits)} · live aggregate, '
                   "no per-code finding.</p>")
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
            f'<span class="tmval">{share * 100:.1f}% · {n:,} codes</span></div>'
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
    out.append(_more_about(section))
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
        pb = (grp.metrics or {}).get("predictability") or {}
        pbadge = pb.get("badge")
        plabel = {"🟢": "reliable", "🟡": "mixed", "🔴": "volatile"}.get(pbadge, "")
        # 'reliable'/'mixed'/'volatile' join the filter index, so you can filter
        # to e.g. only the volatile groups.
        data_name = (grp.title + " " + " ".join(titles) + " "
                     + " ".join(themes) + " " + " ".join(end_use) + " "
                     + plabel).lower()
        out.append(f'<div class="sector" id="{html.escape(grp.id)}" '
                   f'data-name="{html.escape(data_name)}">')
        badge_html = ""
        if pbadge:
            pct = pb.get("persistence_pct")
            tip = (f"{plabel.capitalize()} — {pct * 100:.0f}% of this group's "
                   "year-on-year views held over the past 6 months"
                   if pct is not None else plabel.capitalize())
            badge_html = (f' <span class="pred" title="{html.escape(tip)}">'
                          f"{pbadge}</span>")
        out.append(f'<h3 class="sector-h">{html.escape(grp.title)}{badge_html}</h3>')
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
        # Deeper detail (charts, top products, drivers, trajectory, export share)
        # goes behind a per-group expander: the list stays scannable (just the
        # flow rows), full Tier-3 granularity on demand — and the Briefing is no
        # longer a wall of charts.
        deep: list[str] = []
        imp = next((fi for fi in grp.findings if fi.metrics.get("scope") == "EU-27"
                    and fi.metrics.get("flow") == "import"), None)
        exp = next((fi for fi in grp.findings if fi.metrics.get("scope") == "EU-27"
                    and fi.metrics.get("flow") == "export"), None)
        cards = []
        if imp and imp.chart_data and len(imp.chart_data.series) >= 2:
            cards.append(_chart_card(
                f"{grp.title}: EU-27 imports from China",
                _fmt_eur(imp.metrics.get("current_eur")),
                _LINE_LEGEND, _line_chart_svg(imp.chart_data),
                sub="12-month total · monthly series"))
        bars = []
        if imp and imp.metrics.get("current_eur") is not None:
            bars.append({"label": "Imports", "value": imp.metrics["current_eur"],
                         "color": _GUARDIAN_BLUE})
        if exp and exp.metrics.get("current_eur") is not None:
            bars.append({"label": "Exports", "value": exp.metrics["current_eur"],
                         "color": _BAR_ALT})
        if len(bars) == 2:
            cards.append(_chart_card(
                f"{grp.title}: imports vs exports (EU-27)", "",
                "current 12-month totals · zero-based", _bar_chart_svg(bars)))
        if cards:
            deep.append('<div class="chart-row">' + "".join(cards) + "</div>")
        top = ms.get("top_cn8") or []
        if top:
            deep.append('<div class="detail">Top products: '
                        + " · ".join(f'{html.escape(t["code"])} {_fmt_eur(t["eur"])}'
                                     for t in top) + "</div>")
        reps = ms.get("reporters") or []
        if reps:
            parts_r = []
            for r in reps:
                sh = f' ({r["share"] * 100:.0f}% of the move)' if r.get("share") is not None else ""
                parts_r.append(html.escape(r["reporter"] or "") + sh)
            deep.append('<div class="detail">Driven by: ' + " · ".join(parts_r) + "</div>")
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
            deep.append('<div class="detail">Trajectory — ' + " · ".join(parts_t)
                        + tt + "</div>")
        ev = ms.get("china_export_share_value")
        if ev is not None:
            et = (f' <span class="token">finding/{ms["china_export_share_finding"]}</span>'
                  if ms.get("china_export_share_finding") else "")
            deep.append(f'<div class="detail">China takes {ev * 100:.1f}% '
                        f"of EU-27 exports of this group{et}</div>")
        if deep:
            out.append('<details class="gdetail"><summary>Show detail &amp; '
                       'charts</summary><div class="gdetail-body">'
                       + "".join(deep) + "</div></details>")
        out.append("</div>")
    out.append('<p id="sector-empty" class="note" style="display:none">'
               "No sector matches that filter.</p>")
    return "\n".join(out)


def _what_changed(wc: WhatChanged) -> str:
    # The per-type new-findings breakdown lives in Sources & coverage (it's
    # bookkeeping); What changed keeps the substantive 'since the last pack'
    # digest.
    return (
        '<h2 class="lead">What changed since the last pack</h2>'
        f'<p class="since"><strong>Since the last pack:</strong> '
        f'{html.escape(wc.summary)}</p>'
        '<p class="note">This answers <em>what changed?</em> — where each group '
        'and partner currently stands is in <strong>State of play</strong>; the '
        'per-type count of new findings is in <strong>Sources &amp; coverage'
        '</strong>.</p>'
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
.source{color:var(--muted);font-size:12px;margin:0 0 12px;font-style:italic}
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
.sector:target{background:#dcebfa;scroll-margin-top:12px}
.sector-h{font-family:var(--font-headline);font-size:18px;font-weight:700;color:var(--ink);margin:0 0 2px}
.pred{cursor:help;font-size:15px;vertical-align:baseline}
.sitc{font-size:12px;color:var(--muted);margin:0 0 4px;letter-spacing:.2px}
.cshare{font-size:12.5px;color:var(--news);font-weight:700;margin:0 0 8px}
.gdesc{font-family:var(--font-body);font-size:14px;line-height:1.45;color:var(--muted);margin:2px 0 8px}
.detail{font-size:12.5px;color:var(--muted);margin:4px 0 0}
details.gdetail{margin:6px 0 2px}
details.gdetail>summary{cursor:pointer;list-style:none;font-family:var(--font-sans);font-weight:700;font-size:12.5px;color:var(--masthead);padding:3px 0}
details.gdetail>summary::-webkit-details-marker{display:none}
details.gdetail>summary::before{content:"▸ "}
details.gdetail[open]>summary::before{content:"▾ "}
.chips{margin:0 0 14px;font-size:13px}
.mover-chips{margin:5px 0 2px}
.chips-l{color:var(--muted);font-weight:700;margin-right:6px}
.chip{font-family:var(--font-sans);font-size:12.5px;color:var(--masthead);background:var(--surface);border:1px solid var(--line);border-radius:62.5rem;padding:3px 11px;margin:0 6px 6px 0;cursor:pointer}
.chip:hover{border-color:var(--link)}
.chip.on{background:var(--masthead);color:#fff;border-color:var(--masthead)}
.themes{margin:0 0 6px}
.theme{display:inline-block;font-size:11px;font-weight:700;color:#7a5c00;background:#fff4d6;border-radius:62.5rem;padding:2px 9px;margin:0 5px 4px 0}
.flow{display:flex;align-items:center;gap:12px;font-size:14px;margin:4px 0}
.flow-label{flex:1 1 auto;color:var(--ink)}
.flow-cap{color:var(--news);font-weight:700;font-size:12px}
.flow-cav{display:inline-block;font-size:10.5px;color:var(--muted);background:var(--surface-alt);border:1px solid var(--line);border-radius:62.5rem;padding:0 6px;margin-left:4px;vertical-align:middle;white-space:nowrap}
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
.mg-main{display:flex;align-items:center;gap:16px;flex-wrap:wrap}
.mg-text{flex:1 1 260px;min-width:0}
.mg-ship{flex:0 0 auto}
.mg-p{font-family:var(--font-headline);font-size:16px;font-weight:700;color:var(--ink)}
.mg-g{font-size:13.5px;font-weight:700;margin-top:2px}
.mg-v{font-size:13.5px;color:var(--ink);margin-top:3px}
.hub{font-size:12.5px;color:var(--muted);font-style:italic;margin-top:6px}
.ship{display:block;max-width:240px}
.ship-cap{font-size:11.5px;color:var(--muted);font-style:italic;margin-top:2px}
.ref-h{font-family:var(--font-sans);font-size:13px;font-weight:700;color:var(--muted);margin:14px 0 6px}
ul.ref{margin:0 0 8px;padding-left:18px}
ul.ref li{font-size:13.5px;line-height:1.5;margin:0 0 7px;color:var(--ink)}
.ref-code{font-family:ui-monospace,Menlo,monospace;font-size:11px;color:var(--muted);background:var(--surface-alt);padding:1px 5px;border-radius:4px;margin:0 6px}
.cov{font-weight:700;color:var(--ink);margin-right:6px}
.cov.dark{color:var(--muted);font-weight:400;font-style:italic}
.llm{background:#fffdf0;border:1px solid #f3c100;border-left:4px solid #f3c100;padding:10px 14px;margin:12px 0}
.llm-tag{font-size:12px;font-weight:700;color:#7a5c00}
.llm-body{font-size:13px;color:#7a5c00;font-style:italic;margin-top:3px}
.take{background:#fffdf0;border:1px solid #f3c100;border-left:4px solid #f3c100;padding:8px 12px;margin:8px 0 4px}
.take-tag{font-family:var(--font-sans);font-size:14px;font-weight:700;letter-spacing:.02em;color:#7a5c00;text-transform:uppercase}
.take-qs{margin:6px 0 0;padding-left:18px}
.take .take-qs li{font-family:var(--font-sans);font-size:14px;line-height:1.45;color:#7a5c00;margin:0 0 5px}
.take-prose{font-family:var(--font-sans);font-size:14.5px;line-height:1.5;color:#7a5c00;margin:6px 0 0}
.take-cite{font-size:12px;color:#7a5c00;margin:7px 0 0;opacity:.85}
/* tabs (Guardian Source — thick brand-blue underline over the hairline) */
.tabs{display:flex;gap:4px;background:var(--surface);padding:0 28px;border-bottom:1px solid var(--line);flex-wrap:wrap;position:sticky;top:0;z-index:5}
.tab{padding:12px 16px;color:var(--muted);font-family:var(--font-sans);font-weight:600;font-size:15px;border-bottom:4px solid transparent;margin-bottom:-1px;cursor:pointer;display:inline-flex;align-items:center;gap:8px}
.tab:hover{color:var(--ink);border-bottom-color:transparent}
.tab.active{color:var(--masthead);border-bottom-color:var(--masthead)}
.tabpanel.hide{display:none}
.sector:target{scroll-margin-top:64px}
/* "More about this section" disclosure */
details.more{border:1px solid var(--line);border-left:4px solid var(--masthead);background:var(--surface-alt);border-radius:3px;margin:0 0 14px}
details.more>summary{cursor:pointer;list-style:none;padding:9px 14px;font-family:var(--font-sans);font-weight:700;font-size:13.5px;color:var(--masthead)}
details.more>summary::-webkit-details-marker{display:none}
details.more>summary::before{content:"▸ "}
details.more[open]>summary::before{content:"▾ "}
.more-body{padding:0 16px 12px;font-family:var(--font-body);font-size:14.5px;line-height:1.5;color:var(--ink)}
.more-body p{margin:8px 0}.more-body ul{margin:8px 0;padding-left:20px}.more-body li{margin:3px 0}
.more-body h4,.prose h4{font-family:var(--font-sans);font-size:13.5px;margin:12px 0 4px}
/* GACC bilateral — per-partner expand buttons (progressive disclosure) */
details.partner{border:1px solid var(--line);border-radius:3px;margin:0 0 6px;background:var(--surface)}
details.partner>summary{cursor:pointer;list-style:none;display:flex;flex-wrap:wrap;align-items:baseline;gap:8px;padding:9px 14px}
details.partner>summary::-webkit-details-marker{display:none}
details.partner>summary::before{content:"▸";color:var(--masthead);font-weight:700}
details.partner[open]>summary::before{content:"▾"}
details.partner>summary:hover{background:var(--surface-alt)}
details.partner[open]>summary{border-bottom:1px solid var(--line)}
.pp-name{font-family:var(--font-sans);font-weight:700;font-size:14.5px;color:var(--ink)}
.pp-fig{margin-left:auto;font-size:13px;color:var(--muted);font-variant-numeric:tabular-nums}
.pp-body{padding:8px 14px 10px}
@media(max-width:560px){.pp-fig{margin-left:0;flex-basis:100%}}
/* prose (methodology guides, glossary defs) */
.prose{font-family:var(--font-body);font-size:15px;line-height:1.55;color:var(--ink)}
.prose p{margin:8px 0}.prose ul{margin:8px 0;padding-left:20px}.prose li{margin:3px 0}
.ref-h2{font-family:var(--font-headline);font-size:18px;font-weight:700;color:var(--ink);margin:18px 0 6px}
/* charts (restored docx graphs, inline SVG with axes) — meta column left of the
   plot; the card self-wraps (meta on top) when narrow or two-up in a chart-row */
.chartcard{display:flex;flex-wrap:wrap;gap:12px;align-items:center;background:var(--surface-alt);border:1px solid var(--line);border-radius:3px;padding:10px 12px;margin:10px 0}
.cc-meta{flex:1 1 160px;min-width:148px}
.cc-plot{flex:6 1 300px;min-width:250px}
.cc-plot svg{display:block}
.cc-title{font-family:var(--font-sans);font-weight:700;font-size:13.5px;color:var(--ink);line-height:1.25}
.cc-value{font-family:var(--font-headline);font-weight:700;font-size:21px;margin:3px 0 0;color:var(--ink)}
.cc-sub{font-size:11px;color:var(--muted);margin:0 0 5px}
.cc-legend{font-size:11.5px;color:var(--muted);line-height:1.75}
.sw{display:inline-block;width:12px;height:3px;vertical-align:middle;margin-right:1px}
.sw-prior{background:var(--muted)}.sw-curr{background:var(--news)}
.ct{font-family:var(--font-sans);font-size:9px;fill:var(--muted)}
.chart-row{display:grid;grid-template-columns:1fr;gap:10px;margin:10px 0}
.chart-row .chartcard{margin:0}
@media(min-width:720px){.chart-row{grid-template-columns:1fr 1fr}}
.flow-sm{font-size:12px;color:var(--muted);white-space:nowrap;flex:0 0 auto}
/* donut indicator */
.kpi-donut{align-items:center;text-align:center}
.kpi-donut-wrap{margin:6px auto 2px}
.donut-pct{font-family:var(--font-headline);font-weight:700;font-size:20px;fill:var(--ink)}
/* glossary */
.gloss-group{margin:0 0 8px}
.gloss-item{padding:10px 0;border-bottom:1px solid var(--line)}
.gterm{font-family:var(--font-sans);font-weight:700;font-size:15px;color:var(--ink);margin-bottom:2px}
.gdef{font-family:var(--font-body);font-size:14px;line-height:1.5;color:var(--ink)}
.gdef p{margin:4px 0}.gdef ul{margin:4px 0;padding-left:18px}
/* data tables */
.dt-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.dt-tip{font-size:12px;color:var(--muted);font-style:italic}
.btn{display:inline-block;background:var(--masthead);color:#fff;font-family:var(--font-sans);font-weight:700;font-size:13.5px;padding:8px 14px;border-radius:4px;border:none;cursor:pointer;text-decoration:none;border-bottom:none}
.btn:hover{background:#063a82}
.btn-sm{font-size:12px;padding:5px 10px;background:var(--surface);color:var(--masthead);border:1px solid var(--line)}
.btn-sm:hover{background:var(--surface-alt);border-color:var(--link)}
.dtabs{display:flex;gap:6px;flex-wrap:wrap;margin:0 0 12px}
.dtab{font-family:var(--font-sans);font-size:13px;color:var(--masthead);background:var(--surface);border:1px solid var(--line);border-radius:62.5rem;padding:5px 12px;cursor:pointer;display:inline-flex;gap:6px;align-items:center}
.dtab.on{background:var(--masthead);color:#fff;border-color:var(--masthead)}
.dtab-n{font-size:11px;opacity:.7}
.dt-head{display:flex;justify-content:space-between;align-items:center;gap:10px;margin:0 0 6px;flex-wrap:wrap}
.dt-desc{font-size:13px;color:var(--muted);font-style:italic}
.dt-scroll{overflow-x:auto;border:1px solid var(--line);border-radius:3px;max-height:70vh}
table.dtable{border-collapse:collapse;font-family:var(--font-sans);font-size:12.5px;width:100%;white-space:nowrap}
table.dtable th{background:var(--surface-alt);text-align:left;font-weight:700;padding:7px 10px;border-bottom:2px solid var(--line);position:sticky;top:0}
table.dtable td{padding:6px 10px;border-bottom:1px solid var(--line);color:var(--ink)}
table.dtable tbody tr:hover{background:var(--surface-alt)}
.data-more{margin-top:18px}
footer{padding:18px 28px 28px;border-top:1px solid var(--line);font-size:12px;color:var(--muted);line-height:1.6}
@media(max-width:560px){.mast{font-size:27px}.sub{font-size:16px}section{padding:14px 18px}.masthead{padding:16px 18px}.subbar{padding:10px 18px}.tabs{padding:0 10px}.tab{padding:10px 11px;font-size:14px}}
"""


_PORTAL_JS = """<script>
(function(){
  // ---- tab router: panels show/hide; deep-links (#tab-x) and in-page anchors
  // (a drill-down into a panel) both resolve; degrades to anchored sections
  // with no JS (panels are just divs, all visible).
  var tabs=[].slice.call(document.querySelectorAll('.tab'));
  var panels=[].slice.call(document.querySelectorAll('.tabpanel'));
  function panelOf(el){while(el&&el.classList&&!el.classList.contains('tabpanel'))el=el.parentElement;return el;}
  function expandDetail(el){ // open a drilled-to sector's collapsed charts/detail
    if(!el)return;
    if(el.tagName==='DETAILS')el.open=true;
    var d=el.querySelector&&el.querySelector('details.gdetail');
    if(d)d.open=true;
  }
  function show(id){
    if(!document.getElementById(id))id='tab-briefing';
    panels.forEach(function(p){p.classList.toggle('hide',p.id!==id);});
    tabs.forEach(function(t){t.classList.toggle('active',t.getAttribute('href')==='#'+id);});
  }
  function go(hash){
    var id=(hash||'').replace(/^#/,'');
    var el=id&&document.getElementById(id);
    if(el&&el.classList.contains('tabpanel')){show(id);window.scrollTo(0,0);return;}
    if(el){var p=panelOf(el);if(p){show(p.id);expandDetail(el);el.scrollIntoView();return;}}
    show('tab-briefing');
  }
  tabs.forEach(function(t){t.addEventListener('click',function(e){
    e.preventDefault();var id=t.getAttribute('href').slice(1);
    show(id);if(history.replaceState)history.replaceState(null,'','#'+id);window.scrollTo(0,0);
  });});
  document.addEventListener('click',function(e){
    var a=e.target.closest?e.target.closest('a[href^="#"]'):null;
    if(!a||a.classList.contains('tab'))return;
    var id=a.getAttribute('href').slice(1);var el=document.getElementById(id);if(!el)return;
    var p=el.classList.contains('tabpanel')?el:panelOf(el);
    if(p){show(p.id);if(el!==p){e.preventDefault();expandDetail(el);el.scrollIntoView();
      if(history.replaceState)history.replaceState(null,'','#'+id);}}
  });
  window.addEventListener('hashchange',function(){go(location.hash);});
  if(tabs.length)go(location.hash);

  // ---- sector filter (name / SITC / theme / end-use) + theme chips
  var f=document.getElementById('sector-filter');
  if(f){
    var blocks=[].slice.call(document.querySelectorAll('.sector[data-name]'));
    var count=document.getElementById('sector-count');
    var empty=document.getElementById('sector-empty');
    var apply=function(){
      var q=f.value.trim().toLowerCase(),shown=0;
      blocks.forEach(function(b){
        var m=!q||b.getAttribute('data-name').indexOf(q)!==-1;
        b.style.display=m?'':'none';if(m)shown++;
      });
      if(count)count.textContent=q?('showing '+shown+' of '+blocks.length):(blocks.length+' groups');
      if(empty)empty.style.display=shown?'none':'block';
      [].forEach.call(document.querySelectorAll('.chip'),function(c){
        c.classList.toggle('on',c.getAttribute('data-q')===q);
      });
    };
    f.addEventListener('input',apply);
    [].forEach.call(document.querySelectorAll('.chip'),function(c){
      c.addEventListener('click',function(){
        var q=c.getAttribute('data-q');
        f.value=(f.value.trim().toLowerCase()===q)?'':q;apply();
        // a chip up in the headline movers: bring the filtered Sector list into view
        if(c.classList.contains('mover-chip')&&f.value)f.scrollIntoView({block:'start'});
      });
    });
  }

  // ---- glossary filter (term name or definition text); hides empty groups
  var gf=document.getElementById('glossary-filter');
  if(gf){
    var items=[].slice.call(document.querySelectorAll('.gloss-item'));
    var groups=[].slice.call(document.querySelectorAll('.gloss-group'));
    var gc=document.getElementById('glossary-count');
    var ge=document.getElementById('glossary-empty');
    var gapply=function(){
      var q=gf.value.trim().toLowerCase(),shown=0;
      items.forEach(function(it){
        var m=!q||it.getAttribute('data-name').indexOf(q)!==-1||it.textContent.toLowerCase().indexOf(q)!==-1;
        it.style.display=m?'':'none';if(m)shown++;
      });
      groups.forEach(function(g){
        var any=[].slice.call(g.querySelectorAll('.gloss-item')).some(function(it){return it.style.display!=='none';});
        g.style.display=any?'':'none';
      });
      if(gc)gc.textContent=q?('showing '+shown):(items.length+' terms');
      if(ge)ge.style.display=shown?'none':'block';
    };
    gf.addEventListener('input',gapply);gapply();
  }

  // ---- data-table pills (switch which table shows) + Copy as TSV
  [].forEach.call(document.querySelectorAll('.dtabs'),function(bar){
    var pills=[].slice.call(bar.querySelectorAll('.dtab'));
    var scope=panelOf(bar)||document;
    pills.forEach(function(p){p.addEventListener('click',function(){
      var target=p.getAttribute('data-target');
      pills.forEach(function(q){q.classList.toggle('on',q===p);});
      [].forEach.call(scope.querySelectorAll('.dt-wrap'),function(w){
        w.style.display=(w.id===target)?'':'none';
      });
    });});
  });
  [].forEach.call(document.querySelectorAll('.copy-tsv'),function(btn){
    btn.addEventListener('click',function(){
      var wrap=document.getElementById(btn.getAttribute('data-table'));if(!wrap)return;
      var tsv=[].slice.call(wrap.querySelectorAll('tr')).map(function(tr){
        return [].slice.call(tr.querySelectorAll('th,td')).map(function(c){
          var raw=c.getAttribute('data-raw');   // ungrouped number for clean paste
          return ((raw!==null?raw:c.textContent)||'').replace(/[\\t\\n]/g,' ');
        }).join('\\t');
      }).join('\\n');
      var done=function(){var o=btn.textContent;btn.textContent='\\u2713 Copied';
        setTimeout(function(){btn.textContent=o;},1500);};
      var fb=function(){var ta=document.createElement('textarea');ta.value=tsv;
        document.body.appendChild(ta);ta.select();
        try{document.execCommand('copy');done();}catch(e){}document.body.removeChild(ta);};
      if(navigator.clipboard&&navigator.clipboard.writeText)
        navigator.clipboard.writeText(tsv).then(done,fb);
      else fb();
    });
  });
})();
</script>"""


def render_html(report: Report) -> str:
    """Render the whole report as a single self-contained, tabbed HTML page.
    Sections are routed to tabs by kind: data → Tables, reference →
    Methodology, glossary → Glossary; everything else → the Briefing main
    page. Tabs are client-side (panels show/hide) so the snapshot stays one
    static blob the portal serves without per-route rendering."""
    m = report.meta
    period = m.data_period
    period_str = period.strftime("%B %Y") if hasattr(period, "strftime") else str(period)
    note = report.headline.note if report.headline else ""

    data_sec = next((s for s in report.sections if s.kind == "data"), None)
    ref_sec = next((s for s in report.sections if s.kind == "reference"), None)
    gloss_sec = next((s for s in report.sections if s.kind == "glossary"), None)
    sources_sec = next((s for s in report.sections if s.kind == "sources"), None)
    structural_sec = next((s for s in report.sections if s.kind == "structural"), None)

    # --- Briefing panel: indicators, headline, general take, what-changed, then
    # the main-page sections (everything that isn't a tab of its own).
    brief: list[str] = []
    if report.key_indicators:
        brief.append('<section class="kpis">'
                     + "".join(_indicator_card(i) for i in report.key_indicators)
                     + "</section>")
    if report.headline:
        brief.append("<section>" + _headline(report.headline) + "</section>")
        for slot in report.headline.llm_slots:
            if slot.slot_type == "general":
                block = _general_take_html(slot)
                if block:
                    brief.append("<section>" + block + "</section>")
    if report.what_changed:
        brief.append("<section>" + _what_changed(report.what_changed) + "</section>")
    for sec in report.sections:
        if sec.kind == "state_of_play" and sec.sections:
            brief.append("<section>" + _state_of_play_section(sec) + "</section>")
        elif sec.kind == "sector_detail" and sec.sections:
            brief.append("<section>" + _sector_section(sec) + "</section>")
        elif sec.kind == "mirror_gap" and sec.findings:
            brief.append("<section>" + _mirror_gap_html(sec) + "</section>")
        elif sec.kind == "gacc_bilateral" and sec.sections:
            brief.append("<section>" + _gacc_bilateral_html(sec) + "</section>")
        # 'structural' (the Trade Map) is NOT here — it moved to the Sources &
        # coverage tab below.

    # --- tabs: (key, label, panel-html). Only built when they have content, so a
    # GACC variant with no data tab simply shows fewer tabs.
    tabdefs: list[tuple[str, str, str]] = [
        ("briefing", "Briefing", "".join(brief)),
    ]
    if data_sec is not None and (data_sec.metrics or {}).get("tables"):
        tabdefs.append(("tables", "Tables",
                        "<section>" + _data_tables_html(data_sec) + "</section>"))
    # Sources & coverage = provenance/coverage (sources, period coverage,
    # findings manifest) + the Trade Map (moved off Briefing), one tab.
    src_parts = []
    if sources_sec is not None and (sources_sec.metrics or {}).get("sources"):
        src_parts.append("<section>" + _sources_html(sources_sec) + "</section>")
    if structural_sec is not None and (structural_sec.sections or structural_sec.metrics):
        src_parts.append("<section>" + _structural_section_html(structural_sec) + "</section>")
    if src_parts:
        tabdefs.append(("sources", "Sources & coverage", "".join(src_parts)))
    if ref_sec is not None:
        tabdefs.append(("methodology", "Methodology",
                        "<section>" + _reference_html(ref_sec) + "</section>"))
    if gloss_sec is not None and (gloss_sec.metrics or {}).get("groups"):
        tabdefs.append(("glossary", "Glossary",
                        "<section>" + _glossary_html(gloss_sec) + "</section>"))

    nav = '<nav class="tabs" role="tablist">' + "".join(
        f'<a class="tab{" active" if i == 0 else ""}" href="#tab-{key}">'
        f'{html.escape(label)}</a>'
        for i, (key, label, _h) in enumerate(tabdefs)
    ) + "</nav>"
    panels = "".join(
        f'<div class="tabpanel{" hide" if i else ""}" id="tab-{key}">{panel}</div>'
        for i, (key, _l, panel) in enumerate(tabdefs)
    )

    gen = m.generated_at
    gen_str = (gen.strftime("%Y-%m-%d %H:%M") if hasattr(gen, "strftime")
               else str(gen or ""))
    footer = (
        "<footer>Meridian · China–Europe trade · snapshot "
        f'<span class="token">{html.escape(m.snapshot_id)}</span>'
        + (f" · generated {html.escape(gen_str)}" if gen_str else "")
        + f" · schema {html.escape(m.schema_version)} · every figure is "
        "drillable to source via its <span class=\"token\">finding/N</span> "
        "token.</footer>"
    )

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
        nav,
        panels,
        footer,
        "</div>",
        _PORTAL_JS,
        "</body></html>",
    ]
    return "".join(parts)
