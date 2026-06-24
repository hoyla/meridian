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

from briefing_pack._helpers import _fmt_eur, _fmt_month, _source_label
from briefing_pack.sections.diff import _shift_flow_phrase, _fmt_window_end
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

def _top_product_item(t: dict) -> str:
    """One "Top products" entry: "Citric acid (29181400) €168.1M", with the full
    self-explanatory CN text on hover. Falls back to the bare code + value when no
    description is baked in (the optional cn8_descriptions.csv was missing at build
    time). Tooltip-only by design — no external link (see the CN-descriptions
    design note); the inline short label carries the gist on every device."""
    code = html.escape(t["code"])
    eur = _fmt_eur(t["eur"])
    label = (t.get("label") or "").strip()
    full = (t.get("desc") or "").strip()
    if not label:
        return f"{code} {eur}"
    code_el = (f'<span class="cn8" title="{html.escape(full)}">{code}</span>'
               if full else code)
    return f"{html.escape(label)} ({code_el}) {eur}"


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


def _gloss_anchor(term: str) -> str:
    """Stable in-page anchor id for a glossary term, so standing copy can
    deep-link straight to its definition — the tab router resolves the id,
    switches to the Glossary tab, scrolls to it and highlights it."""
    slug = re.sub(r"[^a-z0-9]+", "-", term.lower()).strip("-")
    return f"gloss-{slug}"


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
    # Optional caption → a native hover tooltip (SVG <title>) naming the window +
    # cadence, e.g. "Monthly figures for the 36 months to Apr 2026". Keeps the
    # explanation off the card face. When present the SVG is labelled (role/aria)
    # rather than aria-hidden so the caption is exposed to assistive tech too.
    caption = (getattr(chart_data, "extra", None) or {}).get("caption")
    title_el = f"<title>{html.escape(caption)}</title>" if caption else ""
    a11y = (f'role="img" aria-label="{html.escape(caption)}"'
            if caption else 'aria-hidden="true"')
    return (
        f'<svg class="spark" viewBox="0 0 {w} {h}" width="{w}" height="{h}" '
        f'preserveAspectRatio="none" {a11y}>{title_el}'
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


def _y_axis(lo: float, hi: float, zero_based: bool, fmt=None) -> str:
    """3 horizontal gridlines (top/mid/bottom) with labels in the left gutter.
    Bars are zero-based (honest scale comparison); the auto-scaled line is not.
    `fmt` formats the gridline value (default €); pass e.g. a percent formatter
    for a share trend."""
    fmt = fmt or _fmt_eur
    x0, x1 = _GL, _CW - _PR
    y0, y1 = _PT, _CH - _GB
    out = []
    for frac, val in ((0.0, hi), (0.5, (hi + lo) / 2), (1.0, lo)):
        yy = y0 + frac * (y1 - y0)
        out.append(f'<line x1="{x0}" y1="{yy:.1f}" x2="{x1}" y2="{yy:.1f}" '
                   f'stroke="{_LINE}" stroke-width="1"/>')
        out.append(f'<text x="{x0 - 4}" y="{yy + 3:.1f}" text-anchor="end" '
                   f'class="ct">{html.escape(fmt(val))}</text>')
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


def _line_chart_svg(chart_data, *, split_last: int = 12, fmt=None) -> str:
    """Inline-SVG line chart with real axes — the docx trajectory graph, restored
    and made legible. The last `split_last` points (current 12 months) are red
    over the earlier period in grey, with a divider. Y-axis (3 gridlines, €
    by default — pass `fmt` for e.g. a percent share trend) and x-axis (start /
    divider / end months) are drawn; auto-scaled (not zero-based) — fine for a
    trend, and the legend says so. No chart lib."""
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

    body = [_y_axis(lo, hi, zero_based=False, fmt=fmt)]
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


def _signed_y_axis(lo: float, hi: float) -> str:
    """3 horizontal gridlines (top/mid/bottom) with €-labels — like `_y_axis`
    but spanning a [lo, hi] that may straddle zero (the balance chart's
    deficits). The visible zero *baseline* is drawn separately by the caller so
    it reads as a reference line, not just one more gridline."""
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


def _multiline_chart_svg(chart: dict) -> str:
    """Inline-SVG multi-line chart — one solid-colour polyline per region, a
    shared y-scale across all series, x-axis of years, and an inline legend
    (swatch + name). Mirrors `_line_chart_svg`'s geometry/axes; differs in that
    every series is one flat colour (no current-12mo red/grey split) and the
    scale is shared so the regions compare honestly.

    `chart` is the plain dict built in report_builder: {metric, title, years,
    partial_last_year, series:[{name, values:[float|None]}], colors:[hex]}.

    - exports / imports are zero-based positive (honest magnitudes).
    - balance carries negatives (deficits) → a SIGNED scale with a visible zero
      baseline line, so a deficit reads as below the line, not as a small bar.
    - The partial last year (latest period not December) is drawn with a dashed
      final segment and a "(YYYY YTD)" x-label, so it can't be misread as a real
      full-year fall."""
    years = chart.get("years") or []
    series = chart.get("series") or []
    colors = chart.get("colors") or []
    if len(years) < 2 or not series:
        return ""
    metric = chart.get("metric")
    signed = metric == "balance"
    partial = chart.get("partial_last_year")

    flat = [v for s in series for v in (s.get("values") or []) if v is not None]
    if not flat:
        return ""
    vmax = max(flat)
    if signed:
        vmin = min(flat)
        lo = min(0.0, vmin)
        hi = max(0.0, vmax)
        if lo == hi:                       # all-zero series — give it room
            lo, hi = -1.0, 1.0
    else:
        lo = 0.0
        hi = vmax if vmax > 0 else 1.0
    span = (hi - lo) or 1.0

    n = len(years)
    x0, x1, y0, y1 = _GL, _CW - _PR, _PT, _CH - _GB
    def x(i): return x0 + i * (x1 - x0) / (n - 1)
    def y(v): return y0 + (1 - (v - lo) / span) * (y1 - y0)

    body = [_signed_y_axis(lo, hi) if signed else _y_axis(lo, hi, zero_based=True)]
    # Vertical gridlines at every year (the span is short — ~7 years — so one
    # tick per year is legible without crowding).
    body.append("".join(
        f'<line x1="{x(i):.1f}" y1="{y0}" x2="{x(i):.1f}" y2="{y1}" '
        f'stroke="{_LINE}" stroke-width="1"/>' for i in range(n)))
    # Visible zero baseline for the signed (balance) chart, drawn over the
    # gridlines so the surplus/deficit boundary is unmistakable.
    if signed and lo < 0 < hi:
        yz = y(0.0)
        body.append(f'<line x1="{x0}" y1="{yz:.1f}" x2="{x1}" y2="{yz:.1f}" '
                    f'stroke="{_MUTED}" stroke-width="1.2"/>')

    partial_idx = years.index(partial) if partial in years else None
    for si, s in enumerate(series):
        vals = s.get("values") or []
        color = colors[si % len(colors)] if colors else _GUARDIAN_BLUE
        # One polyline per region (a region missing a year just omits that point,
        # so the line bridges the gap — acceptable on a sparse annual series).
        pts = [(i, v) for i, v in enumerate(vals) if v is not None]
        if not pts:
            continue
        # Solid polyline up to the last full-year point; the final segment into
        # the partial year is overdrawn as a dashed line so a YTD value can't be
        # misread as a real full-year fall.
        solid_end = len(pts)
        dashed_seg = None
        if (partial_idx is not None and len(pts) >= 2
                and pts[-1][0] == partial_idx):
            solid_end = len(pts) - 1            # polyline stops at the prior year
            dashed_seg = (pts[-2], pts[-1])
        seg = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, v in pts[:solid_end])
        if seg:
            body.append(f'<polyline fill="none" stroke="{color}" '
                        f'stroke-width="2" points="{seg}"/>')
        if dashed_seg is not None:
            (i0, v0), (i1, v1) = dashed_seg
            body.append(
                f'<line x1="{x(i0):.1f}" y1="{y(v0):.1f}" '
                f'x2="{x(i1):.1f}" y2="{y(v1):.1f}" fill="none" '
                f'stroke="{color}" stroke-width="2" stroke-dasharray="4 3"/>')
        # A single-point region (only the partial year, no prior) still needs a
        # mark so its legend colour is anchored on the plot.
        if len(pts) == 1:
            i0, v0 = pts[0]
            body.append(f'<circle cx="{x(i0):.1f}" cy="{y(v0):.1f}" r="2.2" '
                        f'fill="{color}"/>')

    for i in range(n):
        anchor = "start" if i == 0 else ("end" if i == n - 1 else "middle")
        lbl = str(years[i])  # uniform years; the YTD/dashed note lives in the key
        body.append(f'<text x="{x(i):.1f}" y="{_CH - 6}" text-anchor="{anchor}" '
                    f'class="ct">{html.escape(lbl)}</text>')

    svg = (f'<svg class="chart" viewBox="0 0 {_CW} {_CH}" width="100%" '
           'preserveAspectRatio="xMidYMid meet" role="img" '
           f'aria-label="{html.escape(str(chart.get("title", "annual by region")))}">'
           + "".join(body) + "</svg>")
    return svg


def _multiline_legend_html(chart: dict) -> str:
    """The region key for a multi-line chart — a colour swatch + name per region
    — for the chart card's LEFT meta column, under the headline (so the plot
    keeps its full width and height instead of losing a row to a bottom legend).
    A trailing note explains the dashed partial-year segment."""
    series = chart.get("series") or []
    colors = chart.get("colors") or []
    chips = "".join(
        f'<span class="ml-key"><span class="ml-sw" '
        f'style="background:{colors[si % len(colors)] if colors else _GUARDIAN_BLUE}">'
        f'</span>{html.escape(str(s.get("name", "")))}</span>'
        for si, s in enumerate(series))
    partial = chart.get("partial_last_year")
    note = (f'<span class="ml-ytd">┄ {partial} = year-to-date</span>'
            if partial else "")
    return chips + note


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


def _donut_svg(share: float, *, size: int = 116, label: str = "",
               pct_label: str | None = None) -> str:
    """A part-of-whole donut (one share of a whole). Stroke-dasharray on a ring,
    centre percentage. Ready for the China-import-share indicator once an
    all-goods denominator is ingested."""
    share = max(0.0, min(1.0, float(share)))
    # Centre label: the caller's formatted figure when given (so the donut and the
    # KPI headline agree to the same precision — e.g. 22.5%, not a re-rounded 23%);
    # otherwise a whole-percent fallback.
    centre = pct_label if pct_label else f"{share * 100:.0f}%"
    r = size / 2 - 9
    import math
    circ = 2 * math.pi * r
    on = circ * share
    cx = cy = size / 2
    return (
        f'<svg class="donut" viewBox="0 0 {size} {size}" width="{size}" '
        f'height="{size}" role="img" aria-label="{html.escape(label)} '
        f'{html.escape(centre)}">'
        f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="{_LINE}" '
        'stroke-width="11"/>'
        f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="{_GUARDIAN_BLUE}" '
        f'stroke-width="11" stroke-dasharray="{on:.1f} {circ - on:.1f}" '
        f'stroke-dashoffset="{circ / 4:.1f}" transform="rotate(-90 {cx} {cy})" '
        'stroke-linecap="butt"/>'
        f'<text x="{cx}" y="{cy + 1}" text-anchor="middle" dominant-baseline="middle" '
        f'class="donut-pct">{html.escape(centre)}</text>'
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


def _more_about_html(about: str) -> str:
    """The collapsed 'More about this section' disclosure markup for a block of
    explanatory copy. Shared by `_more_about` (Section-driven) and the sections
    whose copy is built inline in the renderer (e.g. What's changed)."""
    if not about:
        return ""
    return (
        '<details class="more"><summary>More about this section</summary>'
        f'<div class="more-body">{_md_blocks_to_html(about)}</div>'
        "</details>"
    )


def _more_about(section) -> str:
    """The collapsed 'More about this section' disclosure carrying the section's
    longer explanatory `about` copy (ported from the Findings preamble)."""
    return _more_about_html(getattr(section, "about", None) or "")


_ABOUT_SITE = (
    "**Meridian** surfaces findings from China–Europe trade data, drawn from "
    "three official sources: **GACC** (China's customs administration), "
    "**Eurostat** (EU-27) and **HMRC** (UK). Each release is triggered when our "
    "scraper finds fresh data from one of these — the source and the month it "
    "covers are shown by the badge, top right.\n\n"
    "**What's in a briefing.** Which sections appear depends on the data "
    "release that triggered it; you may see:\n\n"
    "- **Standout moves** — the largest year-on-year shifts in the freshest "
    "figures, each with leading questions for a reporter to chase.\n"
    "- **What's changed** — what materially moved since the previous briefing: "
    "revisions to existing findings, not newly-discovered ones.\n"
    "- **Europe's deficit with China** — the standing goods-trade deficit "
    "(the ~€1bn/day level) across EU-27, UK and combined scopes; a level, not a "
    "change.\n"
    "- **Mirror-trade gaps** — China's reported exports to each partner vs that "
    "partner's reported imports, and how much of the gap exceeds the normal "
    "CIF/FOB accounting wedge (a possible transshipment signal).\n"
    "- **Sector detail** — the full per-HS-group year-on-year breakdown, with "
    "value, volume and predictability badges.\n"
    "- **China's trade by country (GACC)** — China's own reported trade with "
    "each of its ~24 named partner countries, both flows, rolling 12 months.\n\n"
    "Unless a figure is labelled **China-only**, **“China” includes Hong "
    "Kong and Macao**: a large share of China's exports route through Hong Kong, "
    "so the combined envelope reflects the trade flow more completely. Where a "
    "figure is China-only it says so, and names the comparator.\n\n"
    "The analysis covers a configurable set of **Harmonised System (HS)** "
    "product categories, not all traded goods. The list is editorially "
    "maintained and can be widened — tell the team if there's a category worth "
    "adding.\n\n"
    "Every figure drills back to its source release via its `finding/N` token. "
    "The **Sources & coverage** and **Methodology** tabs carry the full "
    "provenance, definitions and caveats."
)


def _about_site_html() -> str:
    """A collapsed 'About this site' box for the whole briefing — same disclosure
    pattern as the per-section 'More about this section', but page-level."""
    return (
        '<details class="more about-site"><summary>About this site</summary>'
        f'<div class="more-body">{_md_blocks_to_html(_ABOUT_SITE)}</div>'
        "</details>"
    )


def _prov_body(payload: dict | None) -> str:
    """The inner HTML of a provenance drawer (iteration 3) — source-URL trail
    first (the primary 'where did this come from'), then the arithmetic, the
    caveats, and a collapsed replay-SQL 'for the record'. Returns '' when there
    is nothing to show (the caller then renders a plain, non-expandable cite)."""
    if not payload:
        return ""
    parts: list[str] = []
    srcs = [s for s in (payload.get("sources") or []) if s.get("url")]
    if srcs:
        items = "".join(
            f'<li><a href="{html.escape(s["url"])}" target="_blank" rel="noopener">'
            f'{html.escape(s.get("label") or s["url"])}</a>'
            f'<span class="prov-meta">{html.escape(s.get("source", ""))}'
            + (f' · {html.escape(s["coverage"])}' if s.get("coverage") else "")
            + "</span></li>"
            for s in srcs)
        parts.append('<div class="prov-grp"><div class="prov-h">Sources — every '
                     'release this figure draws on</div>'
                     f'<ul class="prov-src">{items}</ul></div>')
    arith = payload.get("arithmetic") or []
    if arith:
        lines = "".join(f"<li>{html.escape(a)}</li>" for a in arith)
        parts.append('<div class="prov-grp"><div class="prov-h">How it’s '
                     f'computed</div><ul class="prov-arith">{lines}</ul></div>')
    cavs = payload.get("caveats") or []
    if cavs:
        lines = "".join(
            f'<li><code>{html.escape(c["code"])}</code>'
            + (f' — {html.escape(c["gloss"])}' if c.get("gloss") else "")
            + "</li>" for c in cavs)
        parts.append('<div class="prov-grp"><div class="prov-h">Caveats</div>'
                     f'<ul class="prov-cav">{lines}</ul></div>')
    sql = payload.get("replay_sql")
    if sql:
        parts.append('<details class="prov-sql"><summary>Replay SQL (for the '
                     f'record)</summary><pre>{html.escape(sql)}</pre></details>')
    return "".join(parts)


def _prov_details(payload: dict | None, summary_inner: str,
                  *, summary_class: str) -> str:
    """A no-JS `<details>` provenance drawer: the citation line is the clickable
    summary; the panel expands inline beneath it. Returns '' when there's no
    payload to show, so callers fall back to a plain citation line."""
    body = _prov_body(payload)
    if not body:
        return ""
    return (
        f'<details class="prov"><summary class="{summary_class}">{summary_inner}'
        '<span class="prov-cue"> · where this came from ▸</span></summary>'
        f'<div class="prov-body">{body}</div></details>'
    )


def _indicator_card(ind: Indicator, payloads: dict | None = None) -> str:
    delta = ""
    if ind.delta:
        col = _DOWN if ind.delta.get("direction") in ("wider", "down") else _UP
        delta = f'<div class="delta" style="color:{col}">{html.escape(ind.delta["formatted"])}</div>'
    # Provenance line: finding token · source · month. The source names the
    # origin (Eurostat / HMRC / GACC) so single-source figures are legible on
    # the card; the as-of is the data month, not a raw ISO day (which misreads
    # as the 1st — Lisa, 2026-06-22).
    parts = []
    if ind.provenance.finding_ids:
        parts.append(f'<span class="token">finding/{ind.provenance.finding_ids[0]}</span>')
    src = _source_label(ind.provenance.source)
    if src:
        parts.append(html.escape(src))
    if ind.provenance.as_of:
        parts.append(f"as of {html.escape(_fmt_month(ind.provenance.as_of))}")
    prov_inner = " · ".join(parts)
    # If this indicator's finding carries a provenance payload, the citation line
    # becomes a no-JS drawer (source trail + workings); otherwise a plain line.
    fid = ind.provenance.finding_ids[0] if ind.provenance.finding_ids else None
    drawer = _prov_details((payloads or {}).get(str(fid)) if fid is not None else None,
                           prov_inner, summary_class="kpi-prov")
    prov = drawer or f'<div class="kpi-prov">{prov_inner}</div>'

    if ind.chart == "donut":
        share = ind.value if 0 <= ind.value <= 1 else 0.0
        if (not share) and ind.chart_data:
            share = ind.chart_data.extra.get("share", 0.0)
        dnote = (f'<div class="kpi-note">{html.escape(ind.note)}</div>'
                 if ind.note else "")
        return (
            '<div class="kpi kpi-donut">'
            f'<div class="kpi-label">{html.escape(ind.label)}</div>'
            '<div class="kpi-donut-wrap">'
            f'{_donut_svg(share, label=ind.label, pct_label=ind.formatted)}</div>'
            f"{dnote}{delta}{prov}"
            "</div>"
        )

    # bignumber (a level, no series) shows no sparkline; sparkline indicators do.
    spark = ""
    if ind.chart_data and ind.chart_data.series:
        spark = f'<div class="kpi-spark">{_sparkline_svg(ind.chart_data)}</div>'
    note = (f'<div class="kpi-note">{html.escape(ind.note)}</div>'
            if ind.note else "")
    # Sparkline cards span 2 of the 3 KPI columns (they need width for the plot);
    # the level + donut cards take 1 — so four cards land as two even rows rather
    # than a stretched lone card.
    wide = " kpi-wide" if spark else ""
    # Order: value, the headline figure's YoY (delta), then the China-only
    # comparator (note, with its own YoY) — so the delta can't be read as the
    # comparator's.
    return (
        f'<div class="kpi{wide}">'
        f'<div class="kpi-label">{html.escape(ind.label)}</div>'
        f'<div class="kpi-value">{html.escape(ind.formatted)}</div>'
        f"{delta}{note}{spark}{prov}"
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


def _headline(h: Headline, payloads: dict | None = None) -> str:
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
            # Provenance drawer for the mover (iteration 3): the finding/N in the
            # prose is a visual cite; this is the click-to-verify panel beneath.
            fid = item.provenance.finding_ids[0] if item.provenance.finding_ids else None
            prov = _prov_details(
                (payloads or {}).get(str(fid)) if fid is not None else None,
                f'<span class="token">finding/{fid}</span>' if fid is not None else "source",
                summary_class="mover-prov")
            out.append(f'<li>{_inline_md(item.prose)} {dd}{chips}'
                       f'{_take_block_html(item.take)}{prov}</li>')
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
    # The CN-only counterpart is still Eurostat / EU-27 — partner = mainland
    # China only (excl. the HK & Macao SARs), the slice the EU's *published*
    # EU-China headline uses. Labelling it "China reports" wrongly implied GACC
    # (China's own customs); "China reports" is reserved for the mirror-gap
    # section, which genuinely compares GACC against Eurostat.
    cn_note = (f' <span style="color:{_MUTED}" title="Same source (Eurostat, EU-27'
               f' reporters); partner = China only, excluding the Hong Kong and Macao'
               f' SARs. This narrower scope is the basis the EU uses for its published'
               f' EU-China balance; the headline figure above adds HK and Macao.">'
               f'(China only, excl. HK/Macao: €{cn / 1e6:,.0f}M/day)</span>'
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
        out.append(f'<p class="kicker">{_inline_md(section.intro)}</p>')
    out.append(_more_about(section))
    # The section carries a single "deficit" child holding the per-scope rows;
    # a sub-heading would just restate the section title, so the rows render
    # directly under the section h2 (the wrapper is a vestigial "first cut").
    for sub in section.sections:
        out.append(f'<div class="sector" id="{html.escape(sub.id)}">')
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
    # China-dependency trend — share of extra-EU goods imports over time, the
    # companion to the donut KPI. Carried as a plain dict on the section metrics
    # (like the GACC partner_charts); rendered with a percent y-axis.
    trend = (getattr(section, "metrics", None) or {}).get("china_share_trend")
    if trend and len(trend.get("series") or []) >= 2:
        from types import SimpleNamespace
        pts = [SimpleNamespace(period=p["period"], value=p["share"])
               for p in trend["series"]]
        now = trend.get("share_now")
        out.append(
            '<div class="sector" id="china-share-trend">'
            + _chart_card(
                trend.get("title", "China's share of EU imports"),
                f"{now * 100:.1f}%" if now is not None else "",
                _LINE_LEGEND,
                _line_chart_svg(SimpleNamespace(series=pts),
                                fmt=lambda v: f"{v * 100:.0f}%"),
                sub="share of extra-EU goods imports · 12-month rolling")
            + "</div>")
    return "\n".join(out)


def _reference_html(section) -> str:
    m = section.metrics or {}
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{_inline_md(section.intro)}</p>')
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
        out.append(f'<p class="kicker">{_inline_md(section.intro)}</p>')
    sources = m.get("sources", [])
    if sources:
        out.append('<h3 class="ref-h2">Data sources</h3><ul class="ref">')
        for s in sources:
            out.append(f'<li><strong>{html.escape(s["source"])}</strong> — '
                       f'{html.escape(s["note"])}</li>')
        out.append("</ul>")
    refs = m.get("reference_sources", [])
    if refs:
        out.append('<h3 class="ref-h2">Reference &amp; classification data</h3>'
                   '<p class="kicker">Static reference lookups the briefing draws '
                   "on, separate from the trade-data releases above.</p>"
                   '<ul class="ref">')
        for r in refs:
            url = r.get("url") or ""
            link = (f' <a href="{html.escape(url)}" target="_blank" '
                    'rel="noopener">source ›</a>') if url else ""
            out.append(f'<li><strong>{html.escape(r["name"])}</strong> — '
                       f'{html.escape(r["note"])}{link}</li>')
        out.append("</ul>")
    cov = m.get("coverage", [])
    if cov:
        out.append('<h3 class="ref-h2">Period coverage</h3>'
                   '<div class="dt-scroll"><table class="dtable"><thead><tr>'
                   "<th>Source</th><th>From</th><th>To</th><th>Releases</th>"
                   "<th>Last updated</th>"
                   "</tr></thead><tbody>")
        for c in cov:
            out.append(
                f'<tr><td>{html.escape(c["source"])}</td>'
                f'<td>{html.escape(_fmt_month(c.get("start")))}</td>'
                f'<td>{html.escape(_fmt_month(c.get("end")))}</td>'
                f'<td>{c.get("releases", 0):,}</td>'
                f'<td>{html.escape(c.get("last_updated") or "—")}</td></tr>')
        out.append("</tbody></table></div>")
    # New findings this cycle, by type (moved here from 'What changed' — it's a
    # coverage tally, not substance). Sits with Period coverage.
    nf = m.get("new_findings", [])
    if nf:
        nft = m.get("new_findings_total", 0)
        out.append('<h3 class="ref-h2">New this cycle</h3>'
                   f'<p class="kicker">{nft:,} findings added since the last briefing, '
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
                out.append(f'<li>{html.escape(_fmt_month(rel.get("period")))} — '
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
        out.append(f'<p class="kicker">{_inline_md(section.intro)}</p>')
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
                f'<div class="gloss-item" id="{_gloss_anchor(term)}" '
                f'data-name="{html.escape(term.lower())}">'
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
        out.append(f'<p class="kicker">{_inline_md(section.intro)}</p>')
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
        out.append(f'<p class="kicker">{_inline_md(section.intro)}</p>')
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
        zp = _fmt_month(m.get("zscore_period")) if m.get("zscore_period") else ""
        znote = (f' · <span class="hub">last flagged unusual {html.escape(zp)}: '
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
        out.append(f'<p class="kicker">{_inline_md(section.intro)} '
                   f'{len(section.sections)} partners, biggest first — '
                   "click a partner to expand.</p>")
    out.append(_more_about(section))
    # Three annual per-region trend charts (exports / imports / balance) above
    # the per-partner expanders — the macro shape before the country detail.
    charts = (getattr(section, "metrics", None) or {}).get("partner_charts") or []
    cards = [_chart_card(c.get("title", ""), "", _multiline_legend_html(c),
                         _multiline_chart_svg(c))
             for c in charts]
    cards = [c for c in cards if c]
    if cards:
        out.append('<div class="chart-row chart-row-1">' + "".join(cards) + "</div>")
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
        # Window orientation, once per partner (same period for both flows).
        win = next((f.metrics.get("window_label") for f in p.findings
                    if f.metrics.get("window_label")), None)
        if win:
            out.append(f'<p class="pp-window">{html.escape(win)}</p>')
        for f in p.findings:
            out.append(_sector_flow_row(f))
            out.append(_bilateral_ctx_row(f))
        out.append(_bilateral_balance_row(p))
        # The incomplete-window prose, once per partner (deduped — both flows
        # usually carry the same note).
        notes: list[str] = []
        for f in p.findings:
            nt = f.metrics.get("note")
            if nt and nt not in notes:
                notes.append(nt)
        for nt in notes:
            out.append(f'<p class="pp-caveat">⚠ {html.escape(nt)}</p>')
        out.append("</div></details>")
    return "\n".join(out)


def _bilateral_ctx_row(f) -> str:
    """The muted secondary register under a partner flow row: year-to-date and
    the latest-month value — the substance the 12-month headline alone drops.
    Restored from the finding's own totals; empty string when absent."""
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
    if not bits:
        return ""
    return f'<div class="pp-ctx">{html.escape(" · ".join(bits))}</div>'


def _bilateral_balance_row(p) -> str:
    """The partner-level net balance: China's exports − imports on the same
    rolling-12-month window as the two flow rows above, with a muted YTD-net
    line beneath. Sign-aware label — GACC reports China-as-reporter, so a
    positive net is China's surplus (the partner's deficit) and a negative net
    China's deficit (the partner's surplus). Empty when either flow is absent
    (nothing to net). Drillable via both flows' finding tokens."""
    m = getattr(p, "metrics", None) or {}
    be = m.get("bal_eur")
    if be is None:
        return ""
    surplus = be >= 0
    label = "China's surplus" if surplus else "China's deficit"
    gloss = f"{p.title}'s {'deficit' if surplus else 'surplus'}"
    val = _fmt_eur(abs(be))
    pct = m.get("bal_yoy_pct")
    if m.get("bal_low_base") or pct is None:
        d = m.get("bal_delta_eur")
        valstr = (f'{val} <span class="flow-sm">{"+" if d >= 0 else "−"}'
                  f'{_fmt_eur(abs(d))} YoY</span>') if d is not None else val
        col = _MUTED
    else:
        pct = float(pct)
        col = _UP if pct > 0 else _DOWN
        valstr = f"{'+' if pct >= 0 else '−'}{abs(pct) * 100:.1f}% · {val}"
    toks = "".join(
        f'<span class="token">finding/{f.provenance.finding_ids[0]}</span>'
        for f in p.findings if f.provenance.finding_ids)
    row = (
        '<div class="flow flow-balance">'
        f'<span class="flow-label">{html.escape(label)} '
        f'<span class="flow-gloss">{html.escape(gloss)}</span></span>'
        f'<span class="flow-val" style="color:{col}">{valstr}</span>'
        f'<span class="flow-cite">{toks}</span>'
        "</div>"
    )
    # YTD net register beneath, mirroring the per-flow ctx line.
    ye = m.get("bal_ytd_eur")
    if ye is not None:
        ylabel = "surplus" if ye >= 0 else "deficit"
        ym = m.get("bal_ytd_months")
        mo = f" ({ym}-mo)" if ym else ""
        ypct = m.get("bal_ytd_pct")
        if m.get("bal_ytd_low_base") or ypct is None:
            yd = m.get("bal_ytd_delta_eur")
            ypart = (f"{'+' if yd >= 0 else '−'}{_fmt_eur(abs(yd))} YoY · "
                     if yd is not None else "")
        else:
            ypart = (f"{'+' if float(ypct) >= 0 else '−'}"
                     f"{abs(float(ypct)) * 100:.1f}% · ")
        ctx = f"YTD{mo} {ylabel}: {ypart}{_fmt_eur(abs(ye))}"
        row += f'<div class="pp-ctx">{html.escape(ctx)}</div>'
    return row


def _structural_section_html(section) -> str:
    """The trade-map browse: SITC divisions, value-weighted, each showing its
    share, code count, coverage by editorial groups, and the groups within —
    so the ~43% of value in no group is visible (a 'no editorial group' tail)."""
    out = [f'<h2 class="lead">{html.escape(section.title)}</h2>']
    if section.intro:
        out.append(f'<p class="kicker">{_inline_md(section.intro)}</p>')
    out.append(_more_about(section))
    # These are live aggregates with no per-code finding, so they carry the
    # section's own provenance (source + as-of + total) rather than a finding
    # token — the trade-map numbers stay attributable.
    p = getattr(section, "provenance", None)
    if p and (p.source or p.as_of):
        bits = []
        if p.source:
            bits.append(f"Source: {html.escape(_source_label(p.source))}")
        if p.as_of:
            bits.append(f"as of {html.escape(_fmt_month(p.as_of))}")
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
        out.append(f'<p class="kicker">{_inline_md(section.intro)} '
                   "Grouped by SITC section, biggest category first — "
                   "filter to find a sector.</p>")
    out.append(_more_about(section))
    n = len(section.sections)
    if n:
        out.append(
            '<div class="filter-bar">'
            '<input id="sector-filter" class="filter" type="text" '
            'placeholder="Filter by sector, SITC bucket or theme…" '
            'aria-label="Filter sectors" autocomplete="off">'
            f'<span id="sector-count" class="filter-count">{n} groups</span>'
            '<span class="filter-note">Contact Luke if you want more added</span>'
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
    # Groups arrive grouped by SITC section (value-ordered); emit a subhead at
    # each section boundary. Subheads carry data-section so the filter can hide
    # them when all their groups are filtered out.
    sec_idx = {s["code"]: s for s in (section.metrics or {}).get("section_index", [])}
    cur_sec = object()
    for grp in section.sections:
        gsec = ((grp.metrics or {}).get("section") or {}).get("code")
        if gsec != cur_sec:
            cur_sec = gsec
            si = sec_idx.get(gsec, {})
            cnt = si.get("count", 0)
            out.append(
                f'<div class="sec-head" data-section="{html.escape(str(gsec or ""))}">'
                f'<span class="sec-h-title">{html.escape(si.get("title", ""))}</span>'
                f'<span class="sec-h-meta">{cnt} '
                f'{"group" if cnt == 1 else "groups"} · '
                f'{_fmt_eur(si.get("value"))} 12-mo EU-27 imports</span></div>')
        f = grp.facets
        secs = f.sector if f else []
        titles = [division_title(c) for c in secs]
        themes = f.theme if f else []
        end_use = f.end_use if f else []
        # SITC division + section names, theme names and end-use join the filter
        # index, so "machinery", "chemicals", "xinjiang" or "capital" all find.
        pb = (grp.metrics or {}).get("predictability") or {}
        pbadge = pb.get("badge")
        plabel = {"🟢": "reliable", "🟡": "mixed", "🔴": "volatile"}.get(pbadge, "")
        sec_title = ((grp.metrics or {}).get("section") or {}).get("title", "")
        data_name = (grp.title + " " + " ".join(titles) + " "
                     + " ".join(themes) + " " + " ".join(end_use) + " "
                     + plabel + " " + sec_title).lower()
        out.append(f'<div class="sector" id="{html.escape(grp.id)}" '
                   f'data-section="{html.escape(str(gsec or ""))}" '
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
                        + " · ".join(_top_product_item(t) for t in top) + "</div>")
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


def _yoy_arc(old, new) -> str:
    """'+12.0% → −4.0%' with the portal's typographic minus, or '—' if either
    side is missing."""
    if old is None or new is None:
        return "—"
    def f(v):
        return f"{'+' if v >= 0 else '−'}{abs(v) * 100:.1f}%"
    return f"{f(old)} → {f(new)}"


def _shift_line_html(s) -> str:
    """One material shift: the group, its flow/window context, the old→new YoY
    arc, the pp delta, and a flip marker — the substance of what *moved*."""
    flip = ' <span class="flip">🔄 flipped</span>' if s.direction_flipped else ""
    pp = ""
    if s.old_yoy is not None and s.new_yoy is not None:
        d = (s.new_yoy - s.old_yoy) * 100
        pp = f' <span class="muted">({"+" if d >= 0 else "−"}{abs(d):.1f}pp)</span>'
    return (
        f'<li><strong>{html.escape(s.group_name)}</strong> '
        f'<span class="muted">({html.escape(_shift_flow_phrase(s.subkind))}, '
        f'12 months to {html.escape(_fmt_window_end(s.window_end))})</span>: '
        f'{_yoy_arc(s.old_yoy, s.new_yoy)}{pp}{flip}</li>'
    )


_WHAT_CHANGED_ABOUT = (
    "Each figure here was already reported in an earlier briefing and has "
    "since been revised — most often because a recent month's data has filled "
    "in as Eurostat's figures mature, which shifts the rolling 12-month rate. "
    "These are corrections to previously-published numbers, not new findings.\n"
    "\n"
    "Where each group currently stands is in **Sector detail** (partners in "
    "**Mirror-trade gaps**); the count of newly-added findings is in "
    "**Sources & coverage**."
)


def _what_changed(wc: WhatChanged) -> str:
    """The 'what moved since the last briefing' register — the material YoY shifts
    (change of ≥5 percentage points, direction flips), NOT a count of new
    findings (that's bookkeeping, in Sources & coverage). Renders the full
    section with the shift list when something moved; a slim, honest one-liner
    when nothing did, so an empty cycle never claims an H2's weight."""
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
        return (f'<p class="quiet-change"><strong>Since the last briefing:</strong> '
                f'{html.escape(msg)}</p>')
    flips = sum(1 for s in shifts if s.direction_flipped)
    lead = (f"{len(shifts)} finding{'s' if len(shifts) != 1 else ''} moved "
            "materially (12-month change shifted by more than 5 percentage "
            "points)")
    if flips:
        lead += f", {flips} of them flipping direction"
    lead += "."
    rows = "".join(_shift_line_html(s) for s in shifts)
    return (
        '<h2 class="lead">What\'s changed since the last briefing</h2>'
        f'<p class="since"><strong>Since the last briefing:</strong> '
        f'{html.escape(lead)}</p>'
        + _more_about_html(_WHAT_CHANGED_ABOUT)
        + f'<ul class="changed">{rows}</ul>'
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
.masthead{background:var(--masthead);color:#fff;padding:18px 28px 16px;display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap}
.mast{font-family:var(--font-headline);font-weight:700;font-size:34px;line-height:1.05;letter-spacing:-.4px}
.sub{font-family:var(--font-headline);font-weight:400;font-size:19px;color:#cdddf6;margin-top:2px}
.mast-meta{display:flex;flex-direction:column;align-items:flex-end;gap:6px;text-align:right;padding-top:5px}
.mast-period{font-size:12.5px;color:#cdddf6}
/* source badge sits on the dark masthead: a white pill, blue text; carries the
   'triggered by' note as its tooltip. */
.tag{background:#fff;color:var(--masthead);font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;padding:2px 10px;border-radius:62.5rem;cursor:help}
section{padding:18px 28px}
.kpis{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;border-bottom:1px solid var(--line)}
.kpi{background:var(--surface);border:1px solid var(--line);border-top:4px solid var(--news);padding:14px 16px}
.kpi-wide{grid-column:span 2}
@media(max-width:640px){.kpis{grid-template-columns:1fr}.kpi-wide{grid-column:auto}}
.kpi-label{font-size:13px;color:var(--muted)}
.kpi-note{font-size:11px;color:var(--muted);margin-top:3px}
.kpi-value{font-family:var(--font-headline);font-size:28px;font-weight:700;line-height:1.15;margin-top:4px}
.delta{font-size:13px;font-weight:700;margin-top:2px}
.kpi-spark{margin-top:10px}.spark{width:100%;height:36px;display:block}
.kpi-prov{margin-top:8px;font-size:12px;color:var(--muted)}
/* Provenance drawer (iteration 3) — a no-JS <details>: the citation line is the
   clickable summary, the panel expands inline beneath. */
details.prov{margin-top:8px}
details.prov>summary{cursor:pointer;list-style:none}
details.prov>summary::-webkit-details-marker{display:none}
details.prov>summary.mover-prov{font-size:12px;color:var(--muted);margin-top:6px}
.prov-cue{color:var(--news);font-weight:600}
details.prov[open]>summary .prov-cue{opacity:.7}
.prov-body{margin-top:8px;padding:10px 12px;background:var(--surface);border:1px solid var(--line);border-left:3px solid var(--news);font-size:12.5px;line-height:1.45}
.prov-grp{margin-bottom:8px}.prov-grp:last-child{margin-bottom:0}
.prov-h{font-weight:700;color:var(--ink);font-size:11px;text-transform:uppercase;letter-spacing:.3px;margin-bottom:3px}
.prov-body ul{margin:0;padding-left:16px}
.prov-src li{margin:2px 0}.prov-src .prov-meta{color:var(--muted);margin-left:6px;font-size:11px}
.prov-arith li,.prov-cav li{margin:2px 0;color:var(--ink)}
.prov-cav code{background:#f3f3f3;padding:0 3px;border-radius:3px}
details.prov-sql{margin-top:6px}details.prov-sql>summary{cursor:pointer;color:var(--muted);font-size:11px}
details.prov-sql pre{overflow-x:auto;background:#f6f6f6;padding:8px;font-size:11px;border:1px solid var(--line);margin:6px 0 0}
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
.changed{margin:8px 0 10px;padding-left:18px;font-variant-numeric:tabular-nums}
.changed li{margin:4px 0;font-size:14.5px;line-height:1.4}
.changed .muted{color:var(--muted);font-size:12.5px}
.flip{color:var(--news);font-weight:700;font-size:12px;white-space:nowrap}
.quiet-change{font-size:13.5px;color:var(--muted);margin:2px 0 0;line-height:1.4}
.quiet-change strong{color:var(--ink);font-weight:600}
.filter-bar{display:flex;align-items:center;gap:10px;margin:0 0 12px;padding-bottom:12px;border-bottom:1px solid var(--line)}
.filter{font-family:var(--font-sans);font-size:14px;padding:7px 10px;border:1px solid var(--line);border-radius:4px;width:280px;max-width:60%;color:var(--ink);background:var(--surface)}
.filter:focus{outline:none;border-color:var(--link);box-shadow:0 0 0 3px rgba(0,119,182,.15)}
.filter-count{font-size:13px;color:var(--muted)}
.filter-note{font-size:12px;color:var(--muted);font-style:italic}
.sector{padding:12px 0;border-bottom:1px solid var(--line)}
/* Jump targets clear the sticky tab bar; offset is unconditional so it applies
   to JS scrollIntoView too (not only native :target jumps). */
.brief-sec,.sector,.partner,.tmrow,.filter,.sec-head{scroll-margin-top:52px}
/* Drilled-to highlight: .jumped is set by JS (the click handler preventDefaults,
   so the native :target never fires); :target is the no-JS fallback. */
.sector.jumped,.partner.jumped,.tmrow.jumped,.gloss-item.jumped,
.sector:target,.partner:target,.tmrow:target,.gloss-item:target{background:#dcebfa}
.sec-head{display:flex;align-items:baseline;gap:8px;flex-wrap:wrap;margin:20px 0 6px;padding:5px 0 4px;border-top:2px solid var(--masthead)}
.sec-h-title{font-family:var(--font-sans);font-size:13px;font-weight:700;color:var(--masthead);text-transform:uppercase;letter-spacing:.4px}
.sec-h-meta{font-size:12px;color:var(--muted)}
.sector-h{font-family:var(--font-headline);font-size:18px;font-weight:700;color:var(--ink);margin:0 0 2px}
.pred{cursor:help;font-size:15px;vertical-align:baseline}
.sitc{font-size:12px;color:var(--muted);margin:0 0 4px;letter-spacing:.2px}
.cshare{font-size:12.5px;color:var(--news);font-weight:700;margin:0 0 8px}
.gdesc{font-family:var(--font-body);font-size:14px;line-height:1.45;color:var(--muted);margin:2px 0 8px}
.detail{font-size:12.5px;color:var(--muted);margin:4px 0 0}
.cn8{font-family:ui-monospace,Menlo,monospace;border-bottom:1px dotted var(--muted);cursor:help}
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
.flow-balance{border-top:1px solid var(--line);margin-top:8px;padding-top:8px}
.flow-gloss{color:var(--muted);font-weight:400;font-size:12px}
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
/* Main tabs are NOT sticky — the Briefing's in-page sub-nav is the sticky
   element instead, so only one bar ever occupies the top (see .subnav). */
.tabs{display:flex;gap:4px;background:var(--surface);padding:0 28px;border-bottom:1px solid var(--line);flex-wrap:wrap}
.subnav{position:sticky;top:0;z-index:6;display:flex;flex-wrap:wrap;align-items:center;gap:4px 16px;background:var(--surface);border-bottom:1px solid var(--line);padding:8px 28px;font-family:var(--font-sans);font-size:13.5px}
/* "See also" cross-link footing a section (e.g. Europe's deficit → the GACC by-country
   section far below) — a quiet, top-ruled pointer, not a call to action. */
.see-also{margin:16px 0 2px;padding-top:10px;border-top:1px solid var(--line);font-size:13px;color:var(--muted)}
.subnav a{color:var(--muted);text-decoration:none;font-weight:600;white-space:nowrap;padding:2px 0;border-bottom:2px solid transparent}
.subnav a:hover{color:var(--ink)}
.subnav a.active{color:var(--masthead);border-bottom-color:var(--masthead)}
.subnav-top{color:var(--masthead) !important;margin-right:6px}
.tab{padding:12px 16px;color:var(--muted);font-family:var(--font-sans);font-weight:600;font-size:15px;border-bottom:4px solid transparent;margin-bottom:-1px;cursor:pointer;display:inline-flex;align-items:center;gap:8px}
.tab:hover{color:var(--ink);border-bottom-color:transparent}
.tab.active{color:var(--masthead);border-bottom-color:var(--masthead)}
.tabpanel.hide{display:none}
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
.pp-window{font-size:12px;color:var(--muted);margin:0 0 6px;font-variant-numeric:tabular-nums}
.pp-ctx{font-size:12px;color:var(--muted);margin:-2px 0 8px;padding-left:2px;font-variant-numeric:tabular-nums}
.pp-caveat{font-size:12px;color:var(--muted);margin:8px 0 0;line-height:1.45;border-top:1px solid var(--line);padding-top:7px}
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
/* The annual per-region charts stay one-up (full width): each carries a 6-item
   legend, so two side-by-side would crowd. */
@media(min-width:720px){.chart-row-1{grid-template-columns:1fr}}
/* Region key for a multi-line chart — a colour swatch (a short bar matching the
   line stroke) + name. Rendered in the card's left meta column, under the
   headline, by _multiline_legend_html. */
.ml-key{white-space:nowrap}
.ml-sw{display:inline-block;width:14px;height:3px;vertical-align:middle;margin-right:4px;border-radius:1px}
/* Multi-line regional charts: the key sits in the LEFT meta column under the
   headline (not a bottom row), and that column is narrow — the title may run to
   2–3 decks — so the plot reclaims both width and height. */
.chart-row-1 .chartcard{align-items:flex-start}
.chart-row-1 .cc-meta{flex:0 1 132px;min-width:112px}
.chart-row-1 .cc-legend{display:flex;flex-direction:column;gap:3px;margin-top:8px}
.chart-row-1 .cc-legend .ml-key{display:flex;align-items:center;white-space:normal}
.ml-ytd{display:block;margin-top:6px;font-style:italic;opacity:.8}
.flow-sm{font-size:12px;color:var(--muted);white-space:nowrap;flex:0 0 auto}
/* donut indicator */
.kpi-donut{align-items:center;text-align:center}
.kpi-donut-wrap{margin:6px auto 2px}
.donut-pct{font-family:var(--font-headline);font-weight:700;font-size:20px;fill:var(--ink)}
/* glossary — groups are nested <section>s inside the tab's own <section>, so
   strip the inherited section padding (it would otherwise double up). */
.gloss-group{margin:0 0 8px;padding:0}
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
@media(max-width:560px){.mast{font-size:27px}.sub{font-size:16px}section{padding:14px 18px}.masthead{padding:16px 18px}.tabs{padding:0 10px}.tab{padding:10px 11px;font-size:14px}.subnav{padding:8px 18px;flex-wrap:nowrap;overflow-x:auto;-webkit-overflow-scrolling:touch}}
"""


_PORTAL_JS = """<script>
(function(){
  // ---- tab router: panels show/hide; deep-links (#tab-x) and in-page anchors
  // (a drill-down into a panel) both resolve; degrades to anchored sections
  // with no JS (panels are just divs, all visible).
  //
  // Browser back/forward is honoured. Switching tab pushes a history entry
  // carrying {tab, anchor, y}; on popstate we restore both the tab AND the exact
  // scroll position you were at, so back from (say) the Glossary returns you to
  // your place in the Briefing. In-tab jumps (subnav, sector drill-downs) use
  // replaceState instead, so they don't pile up entries -- back steps between
  // tabs, not through every jump. Scroll restore is manual because panels
  // hide/show rather than truly navigate, so page height shifts under the
  // browser's own restore. We route off popstate (not hashchange) to stay the
  // single source of truth for back/forward; deep links still resolve on load.
  var tabs=[].slice.call(document.querySelectorAll('.tab'));
  var panels=[].slice.call(document.querySelectorAll('.tabpanel'));
  var cur='tab-briefing';
  function panelOf(el){while(el&&el.classList&&!el.classList.contains('tabpanel'))el=el.parentElement;return el;}
  function expandDetail(el){ // open a drilled-to sector's collapsed charts/detail
    if(!el||(el.classList&&el.classList.contains('brief-sec')))return; // not whole-section jumps
    if(el.tagName==='DETAILS')el.open=true;
    var d=el.querySelector&&el.querySelector('details.gdetail');
    if(d)d.open=true;
  }
  function mark(el){ // the drilled-to highlight (JS stands in for native :target)
    var prev=document.querySelector('.jumped');if(prev)prev.classList.remove('jumped');
    if(el&&el.classList)el.classList.add('jumped');
  }
  function show(id){
    if(!document.getElementById(id))id='tab-briefing';
    cur=id;
    panels.forEach(function(p){p.classList.toggle('hide',p.id!==id);});
    tabs.forEach(function(t){t.classList.toggle('active',t.getAttribute('href')==='#'+id);});
  }
  var hist=!!(window.history&&history.pushState);
  if(hist&&'scrollRestoration' in history)history.scrollRestoration='manual';
  function stamp(){ // record where we are in the entry we're about to leave
    if(hist)try{history.replaceState(Object.assign({},history.state||{},{y:window.scrollY}),'');}catch(e){}
  }
  // Navigate to panel `id`, optionally drilling to element `el` within it.
  // A tab change pushes a back-stop; an in-tab jump only replaces.
  function nav(id,el,hash){
    var panelEl=document.getElementById(id);
    var drill=el&&el!==panelEl;
    var changing=id!==cur;
    if(changing)stamp();
    show(id);
    if(drill){expandDetail(el);el.scrollIntoView();mark(el);}
    else{mark(null);if(changing)window.scrollTo(0,0);}
    if(!hist)return;
    var st={tab:id,y:window.scrollY,anchor:drill?el.id:null};
    if(changing)history.pushState(st,'','#'+(hash||id));
    else history.replaceState(Object.assign({},history.state||{},st),'','#'+(hash||id));
  }
  function seed(id,anchor){ // give the first (load) entry a state for back/forward
    if(hist)try{history.replaceState({tab:id,y:window.scrollY,anchor:anchor||null},'');}catch(e){}
  }
  function go(hash){ // initial deep-link resolution (load only)
    var id=(hash||'').replace(/^#/,'');
    var el=id&&document.getElementById(id);
    if(el&&el.classList.contains('tabpanel')){show(id);window.scrollTo(0,0);seed(id,null);return;}
    if(el){var p=panelOf(el);if(p){show(p.id);expandDetail(el);el.scrollIntoView();mark(el);seed(p.id,el.id);return;}}
    show('tab-briefing');seed('tab-briefing',null);
  }
  tabs.forEach(function(t){t.addEventListener('click',function(e){
    e.preventDefault();nav(t.getAttribute('href').slice(1),null,null);
  });});
  document.addEventListener('click',function(e){
    var a=e.target.closest?e.target.closest('a[href^="#"]'):null;
    if(!a||a.classList.contains('tab'))return;
    var id=a.getAttribute('href').slice(1);var el=document.getElementById(id);if(!el)return;
    var p=el.classList.contains('tabpanel')?el:panelOf(el);if(!p)return;
    e.preventDefault();nav(p.id,el===p?null:el,id);
  });
  window.addEventListener('popstate',function(e){
    var st=e.state||{};var id=st.tab;
    if(!id||!document.getElementById(id)){ // no state (e.g. landed via raw hash) -> resolve hash
      var h=(location.hash||'').replace(/^#/,'');var hel=h&&document.getElementById(h);
      if(hel&&hel.classList.contains('tabpanel'))id=h;
      else if(hel){var pp=panelOf(hel);id=pp?pp.id:'tab-briefing';st.anchor=st.anchor||h;}
      else id='tab-briefing';
    }
    show(id);
    var aEl=st.anchor&&document.getElementById(st.anchor);
    if(aEl){expandDetail(aEl);mark(aEl);}else mark(null);
    var y=(typeof st.y==='number')?st.y:0;
    requestAnimationFrame(function(){window.scrollTo(0,y);}); // after panel layout settles
  });
  if(tabs.length)go(location.hash);

  // ---- briefing sub-nav: immediate active-state on click (works everywhere),
  // plus scroll-spy that highlights the link for the section in view (picks the
  // topmost intersecting section). The IO half no-ops where unsupported; links
  // still jump and the clicked one still highlights.
  var spy=[].slice.call(document.querySelectorAll('.subnav a[data-spy]'));
  spy.forEach(function(a){a.addEventListener('click',function(){
    spy.forEach(function(x){x.classList.remove('active');});a.classList.add('active');});});
  if(spy.length&&'IntersectionObserver' in window){
    var vis={};
    var io=new IntersectionObserver(function(entries){
      entries.forEach(function(en){vis[en.target.id]=en.isIntersecting;});
      var pick=null;
      spy.forEach(function(a){var id=a.getAttribute('data-spy');if(!pick&&vis[id])pick=id;});
      spy.forEach(function(a){a.classList.toggle('active',a.getAttribute('data-spy')===pick);});
    },{rootMargin:'-52px 0px -55% 0px',threshold:0});
    spy.forEach(function(a){var el=document.getElementById(a.getAttribute('data-spy'));if(el)io.observe(el);});
  }

  // ---- sector filter (name / SITC / theme / end-use) + theme chips
  var f=document.getElementById('sector-filter');
  if(f){
    var blocks=[].slice.call(document.querySelectorAll('.sector[data-name]'));
    var heads=[].slice.call(document.querySelectorAll('.sec-head'));
    var count=document.getElementById('sector-count');
    var empty=document.getElementById('sector-empty');
    var apply=function(){
      var q=f.value.trim().toLowerCase(),shown=0;
      blocks.forEach(function(b){
        var m=!q||b.getAttribute('data-name').indexOf(q)!==-1;
        b.style.display=m?'':'none';if(m)shown++;
      });
      // hide a section subhead when none of its groups are showing
      heads.forEach(function(hd){
        var sc=hd.getAttribute('data-section');
        var any=blocks.some(function(b){
          return b.getAttribute('data-section')===sc && b.style.display!=='none';});
        hd.style.display=any?'':'none';
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


def _source_received_date(sources_sec, variant) -> str | None:
    """When we last received the active source's data: the latest release's
    fetch date from the sources appendix, formatted '16 Jun 2026'. None if the
    section, the matching source, or the date is unavailable."""
    from datetime import date as _d
    for entry in ((sources_sec.metrics or {}).get("appendix", []) if sources_sec else []):
        if entry.get("source") == variant:
            recent = entry.get("recent") or []
            iso = recent[0].get("fetched") if recent else None
            if not iso:
                return None
            try:
                d = _d.fromisoformat(iso)
            except (ValueError, TypeError):
                return iso
            return f"{d.day} {d:%b %Y}"
    return None


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
    # The note's first sentence ("Triggered by new Eurostat data.") becomes the
    # source badge's tooltip; the rest was boilerplate and is dropped.
    tip = note.split(". ", 1)[0].strip() if note else ""
    if tip and not tip.endswith("."):
        tip += "."

    data_sec = next((s for s in report.sections if s.kind == "data"), None)
    ref_sec = next((s for s in report.sections if s.kind == "reference"), None)
    gloss_sec = next((s for s in report.sections if s.kind == "glossary"), None)
    sources_sec = next((s for s in report.sections if s.kind == "sources"), None)
    structural_sec = next((s for s in report.sections if s.kind == "structural"), None)

    # Append when we received this source's latest data to the badge tooltip —
    # answers "how fresh is this?" on hover.
    recv = _source_received_date(sources_sec, m.variant)
    if recv:
        tip = f"{tip} Received {recv}.".strip()
    tip_attr = f' title="{html.escape(tip)}"' if tip else ""

    # --- Briefing panel: indicators, headline, general take, what-changed, then
    # the main-page sections (everything that isn't a tab of its own).
    brief: list[str] = []
    subnav: list[tuple[str, str]] = []   # (anchor id, short label) for the sub-nav
    if report.key_indicators:
        brief.append('<section class="kpis">'
                     + "".join(_indicator_card(i, report.provenance_payloads)
                               for i in report.key_indicators)
                     + "</section>")
    # Page-level "About this site" box, just above the Standout-moves lead.
    brief.append("<section>" + _about_site_html() + "</section>")
    if report.headline:
        brief.append("<section>" + _headline(report.headline, report.provenance_payloads) + "</section>")
        for slot in report.headline.llm_slots:
            if slot.slot_type == "general":
                block = _general_take_html(slot)
                if block:
                    brief.append("<section>" + block + "</section>")
    if report.what_changed:
        wc = report.what_changed
        if wc.significant:                   # something actually moved → full section
            subnav.append(("brief-changed", "What's changed"))
            brief.append('<section class="brief-sec" id="brief-changed">'
                         + _what_changed(wc) + "</section>")
        else:                                # nothing moved → slim one-liner, no nav
            brief.append("<section>" + _what_changed(wc) + "</section>")
    _BRIEF_NAV = {"state_of_play": "The deficit", "mirror_gap": "Mirror gaps",
                  "sector_detail": "Sector detail", "gacc_bilateral": "GACC by country"}
    has_gacc = any(
        s.kind == "gacc_bilateral"
        and (s.sections or (s.metrics or {}).get("partner_charts"))
        for s in report.sections)
    for sec in report.sections:
        inner = None
        if sec.kind == "state_of_play" and sec.sections:
            inner = _state_of_play_section(sec)
            if has_gacc:
                # Bridge to the GACC "by country" section far below: the two do
                # different jobs (EU↔China detail here; China's own-customs view
                # of its whole world there) and sit far apart in this tab, so a
                # reader sees the relationship and can jump straight to it.
                inner += (
                    '<p class="see-also">→ See also '
                    '<a href="#brief-gacc_bilateral">China’s trade by country '
                    '(GACC)</a> — China’s own customs view of its trade with the '
                    'world, the global counterpart to the EU-focused picture '
                    'above.</p>')
        elif sec.kind == "sector_detail" and sec.sections:
            inner = _sector_section(sec)
        elif sec.kind == "mirror_gap" and sec.findings:
            inner = _mirror_gap_html(sec)
        elif sec.kind == "gacc_bilateral" and (
                sec.sections or (sec.metrics or {}).get("partner_charts")):
            # Render when there are per-partner subsections OR the annual
            # per-region charts — the charts live on the root and shouldn't be
            # suppressed just because the per-country findings are absent.
            inner = _gacc_bilateral_html(sec)
        # 'structural' (the Trade Map) is NOT here — it moved to the Sources &
        # coverage tab below.
        if inner is not None:
            anchor = "brief-" + sec.kind
            subnav.append((anchor, _BRIEF_NAV[sec.kind]))
            brief.append(f'<section class="brief-sec" id="{anchor}">{inner}</section>')

    # Sticky in-page nav for the Briefing (its only long, multi-section tab) —
    # so a landing reader sees what's below and can jump. "Top" returns to the
    # masthead + main tabs (which are not sticky, to keep one bar at a time).
    if subnav:
        links = '<a class="subnav-top" href="#top">↑&nbsp;Top</a>' + "".join(
            f'<a href="#{a}" data-spy="{a}">{html.escape(lbl)}</a>'
            for a, lbl in subnav)
        brief.insert(0, f'<nav class="subnav" aria-label="On this page">{links}</nav>')

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
        '<header class="masthead" id="top">',
        '<div class="mast-brand">',
        '<div class="mast">Meridian</div>',
        '<div class="sub">China–Europe trade</div>',
        "</div>",
        '<div class="mast-meta">',
        f'<span class="tag"{tip_attr}>{html.escape(m.variant)}</span>',
        f'<span class="mast-period">Data to {html.escape(period_str)}</span>',
        "</div>",
        "</header>",
        nav,
        panels,
        footer,
        "</div>",
        _PORTAL_JS,
        "</body></html>",
    ]
    return "".join(parts)
