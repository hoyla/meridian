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

# Guardian Source tokens — resolved hexes. See
# dev_notes/guardian-source/CONVENTIONS.md (exported from the design system).
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


def _sparkline(ind: Indicator, w: int = 150, h: int = 36) -> str:
    """Inline-SVG sparkline from the indicator's series. No axes — a
    glanceable vital sign. Last point marked."""
    if not ind.chart_data or len(ind.chart_data.series) < 2:
        return ""
    vals = [p.value for p in ind.chart_data.series]
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
        f'<div class="kpi-spark">{_sparkline(ind)}</div>'
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
        out.append('<p class="note">The smaller and shakier moves are in '
                   '<strong>Sector detail</strong> — not dropped, just not headlined.</p>')
    else:
        out.append('<p class="note">Macro/geographic lead (GACC partner/bloc '
                   'totals) not yet wired — next increment.</p>')
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

    parts.append("<footer><h3>Where to go deeper</h3><div class='comp'>")
    for name, what in _COMPANIONS:
        parts.append(f'<a href="#">{name and f"<b>{html.escape(name)}</b>"}'
                     f'<span>{html.escape(what)}</span></a>')
    parts.append("</div></footer></div></body></html>")
    return "".join(parts)
