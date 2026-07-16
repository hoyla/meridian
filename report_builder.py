"""DB → Report. Builds the rendering-agnostic content model
(`report_model.Report`) from live findings.

Reuses the existing analytical + prose helpers (`_compute_top_movers`,
`_compute_diff`, `front_page._mover_sentence`) so the model is populated
from the same deterministic source the current docs use — the schema is a
*restructuring* of trusted output, not a reanalysis.

Scope of this first cut: `meta`, `key_indicators` (the EU–China deficit,
real), `headline` (variant-shaped, real movers + placeholder LLM slots),
and `what_changed` (real diff). The `sections` content tree — the
navigable granularity layer — is the next increment and is left empty
here.

Known v0 wrinkle (Fork A): `HeadlineItem.prose` currently reuses
`_mover_sentence`, whose string carries light markdown (bold subject, a
`[group](#slug)` anchor, a backtick citation token). That’s fine for the
markdown renderer but leaks doc-specific nav into the model; the clean
path is the structured `subject`/`metrics`/`stability`/`drill_down`
fields, and prose gets decoupled into plain text + structured emphasis
when the HTML renderer needs it. `facets` are likewise minimally stubbed
(commodity only) pending hs_groups facet metadata.
"""

from __future__ import annotations

import logging
import math
import pathlib
import re
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal

import psycopg2.extras

log = logging.getLogger(__name__)

from briefing_pack._helpers import (
    _ALL_UNIVERSAL_CAVEATS,
    _compute_predictability_per_group,
    _compute_top_movers,
    _conn,
    _construct_chinese_source_url,
    _fmt_eur,
    _fmt_eur_per_day,
    _fmt_month,
    _fmt_pct_kpi,
    _slugify_heading,
    _subkind_plain_label,
)
from briefing_pack.sections.diff import _compute_diff
from briefing_pack.sections.front_page import _mover_sentence
import anomalies
from anomalies import (
    EU27_EXCLUDE_REPORTERS,
    EUROSTAT_PARTNERS,
    _months_back,
)
import classifications
import db
import eurostat
import labels
import release_calendar
from report_model import (
    ChartData,
    Facets,
    Finding,
    GaccPage,
    Headline,
    HeadlineItem,
    Indicator,
    LLMSlot,
    Provenance,
    Report,
    ReportMeta,
    Section,
    SeriesPoint,
    Shift,
    WhatChanged,
)


def _f(v):
    return None if v is None else float(v)


# Caveats hidden from the per-row chips: `low_base_effect` is already shown as
# its own "low base" flag; `cross_source_sum` is invariant on every EU-27+UK
# combined row (structural, not a per-row signal) and is explained in the
# section explainer + Methodology. A row chip should mark what's *unusual*.
_ROW_HIDDEN_CAVEATS = {"low_base_effect", "cross_source_sum"}


def _visible_caveats(codes) -> list[str]:
    """Per-finding-variable caveats only — drop the family-universal ones (shown
    once in Methodology) and the structural/already-shown ones, so a row carries
    only what’s unusual *about it* (partial window, low kg coverage…)."""
    return [c for c in (codes or [])
            if c not in _ALL_UNIVERSAL_CAVEATS and c not in _ROW_HIDDEN_CAVEATS]


def _month_label(period) -> str | None:
    """A 'YYYY-MM-01' window-end into 'Month YYYY' (e.g. 'May 2026')."""
    if not period:
        return None
    try:
        d = period if isinstance(period, date) else date.fromisoformat(str(period))
    except (ValueError, TypeError):
        return None
    return d.strftime("%B %Y")


def _bilateral_context(detail) -> dict:
    """The richer registers behind a partner flow, restored from the finding’s
    own `totals`/`windows` — the YTD and latest-month figures plus a plain-prose
    incomplete-window note. All already computed upstream; this only reshapes
    them for the expanded panel (the collapsed summary stays terse)."""
    tot = (detail or {}).get("totals") or {}
    win = (detail or {}).get("windows") or {}
    out: dict = {}

    ytd = tot.get("ytd_cumulative") or {}
    if ytd.get("current_eur") is not None:
        out["ytd_pct"] = _f(ytd.get("yoy_pct"))
        out["ytd_eur"] = _f(ytd.get("current_eur"))
        out["ytd_months"] = ytd.get("months_in_ytd")

    sm = tot.get("single_month") or {}
    if sm.get("current_eur") is not None:
        out["sm_yoy_pct"] = _f(sm.get("yoy_pct"))  # latest-month register (row)
        out["sm_eur"] = _f(sm.get("current_eur"))  # latest-month value (ctx line)

    out["window_label"] = (f"12 months to {_month_label(win.get('current_end'))}"
                           if win.get("current_end") else None)

    # Plain-prose incomplete-window note, built from the structured fields so it
    # names the actual months (more honest than the cryptic "jan feb combined"
    # chip). Two independent clauses: genuinely-missing months, and GACC's
    # merged Jan+Feb releases counted as one combined figure.
    clauses: list[str] = []
    missing = [m for m in (tot.get("missing_months_current") or []) if m]
    if missing:
        names = ", ".join(_month_label(m) or str(m) for m in missing)
        clauses.append(f"missing {names} from the current 12-month window")
    jf = [y for y in (tot.get("jan_feb_combined_years") or []) if y]
    if jf:
        yrs = ", ".join(str(y) for y in jf)
        clauses.append(f"Jan+Feb {yrs} counted as a single combined figure "
                       "(GACC publishes them merged)")
    out["note"] = "Incomplete window — " + "; ".join(clauses) if clauses else None
    return out


def _primary_section(divisions) -> tuple[str, str]:
    """A group’s primary SITC section (1-digit) — the coarse bucket the sector
    list groups by. Mode of its divisions' sections (ties → lowest code); section
    '9' (Other / unclassified) when it maps to no division. A heuristic: single-
    division groups (most) are exact; the few multi-division groups land in their
    most-common section."""
    secs = [d[0] for d in (divisions or []) if d]
    if not secs:
        return ("9", classifications.section_title("9"))
    code = sorted(Counter(secs).items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
    return (code, classifications.section_title(code))

# Variant content (Q1). The lead title and note are *content* (they live
# in the Headline node), so they belong here in the builder, not in a
# renderer. `has_sector_movers` decides whether the lead is HS-sector
# movers (Eurostat/HMRC) or macro/geographic partner-bloc totals (GACC).
_VARIANTS: dict[str, dict] = {
    "eurostat": {
        "lead": "Standout moves in {month}’s EU figures",
        "note": (
            "Triggered by new Eurostat data. The China-vs-Europe "
            "mirror-trade discrepancy and the HS-sector shifts live here, "
            "at their freshest month."
        ),
        "has_sector_movers": True,
        "general_slot": "surface what connects {month}'s findings — and what’s notably absent",
    },
    "gacc": {
        "lead": "Standout moves in China’s own {month} figures",
        "note": (
            "Triggered by new GACC data, a month ahead of Europe’s. No "
            "mirror-gap or HS-sector detail at this altitude — GACC "
            "preliminary is partner/bloc totals, so the headline is "
            "macro/geographic."
        ),
        "has_sector_movers": False,
        "general_slot": "read what China’s {month} geography shift implies — grounded in the totals above",
    },
    "hmrc": {
        "lead": "Standout moves in the UK’s {month} figures",
        "note": (
            "Triggered by new HMRC data, with no fresher Eurostat month — "
            "the UK cut of the China trade picture."
        ),
        "has_sector_movers": True,
        "general_slot": "read what the UK-China {month} move implies — grounded in the findings above",
    },
}


def _latest_trade_balance(cur, subkind: str):
    """The latest (by anchor period) live trade-balance finding for a subkind —
    (id, detail) or (None, None). Ordered by anchor period, not id, so a
    back-filled revision can’t masquerade as the newest month."""
    cur.execute(
        """SELECT id, detail FROM findings
            WHERE superseded_at IS NULL AND subkind = %s
            ORDER BY (detail->'windows'->>'anchor_period')::date DESC NULLS LAST,
                     id DESC
            LIMIT 1""",
        (subkind,),
    )
    row = cur.fetchone()
    return (row["id"], row["detail"]) if row else (None, None)


# "More about this section" copy — the explanatory matter ported from the long
# Findings-doc preamble, attached to each section's `about` field (collapsed by
# default in the portal). Light markdown: **bold**, `code`, - bullets, blank-line
# paragraphs. Kept here (content lives in the model, not a renderer) and in one
# place so the editorial voice stays consistent.
_ABOUT: dict[str, str] = {
    "the-deficit": (
        "Unlike the year-on-year moves elsewhere in the briefing, this is a "
        "**standing level, not a change** — Europe’s all-goods trade balance "
        "with China (imports minus exports), straight from Eurostat and HMRC. "
        "It barely moves cycle to cycle, so it never trips the \"what’s new\" "
        "thresholds — but its size, per day, is usually the most quotable single "
        "number in the pack.\n"
        "\n"
        "**Before quoting:** Eurostat values imports CIF (freight + insurance) "
        "and exports FOB, which widens the deficit — but it is the basis "
        "Eurostat publishes on. **\"China\" includes Hong Kong and Macao** "
        "(CN+HK+MO) on both sides. The EU-27 + UK line adds two statistical "
        "agencies’ figures: a close approximation, not a single-source number."
    ),
    "mirror-gaps": (
        "**Mirror trade** compares the two sides’ own books: China’s reported "
        "exports to a partner against that partner’s reported imports from "
        "China. They never match exactly — but a gap *beyond* the normal "
        "accounting wedge (China reports exports FOB; the partner reports "
        "imports CIF, ~5–10% higher) can signal **transshipment**: goods routed "
        "through a hub (often Rotterdam or Hong Kong) and cleared into the EU "
        "elsewhere.\n"
        "\n"
        "**Scope:** the partner’s-imports (Eurostat) side sums the same "
        "**CN+HK+MO** envelope used across the pack — **\"China\" includes Hong "
        "Kong and Macao** — while the other side is China’s own reported exports "
        "to that partner (GACC). The two agencies define and value trade "
        "differently, which is why a residual gap beyond the CIF/FOB wedge is "
        "the signal.\n"
        "\n"
        "Each row shows both reported totals, the gap, how much of it exceeds "
        "the CIF/FOB baseline, and — where flagged — the named hub and a "
        "z-score for how unusual the gap is against the partner’s own six-month "
        "history."
    ),
    "sector-detail": (
        "**How to read each group.** Every figure is a **rolling 12-month "
        "total** compared with the prior 12 months, unless a latest-month "
        "figure is shown beside it. The 12-month figure smooths seasonal swings "
        "and is the one to quote; the **latest month** is a direction hint that "
        "swings wildly on lumpy categories (aircraft, ships) — don’t headline "
        "it.\n"
        "\n"
        "- **Value vs volume** — *value* is what the goods cost (€); *volume* is "
        "their weight (kg). Value falling faster than volume means the same "
        "goods got cheaper.\n"
        "- **low base** — when flagged, the percentage rests on a small total "
        "(under €50M); quote the absolute € amount, not the %.\n"
        "- **🟢 🟡 🔴 predictability** — whether the group’s year-on-year signal "
        "held over the past six months: 🟢 held (reliable) / 🟡 mixed / 🔴 "
        "didn’t hold (verify before quoting). No badge means too little history "
        "to score.\n"
        "- **`finding/N`** — the citation token: a permanent handle to the exact "
        "database row behind the number, with its full audit trail.\n"
        "\n"
        "**Three views** of each group sit side by side: **EU-27** (Eurostat), "
        "**UK** (HMRC), and **EU-27 + UK** combined (two agencies summed — a "
        "useful approximation, not a like-for-like single source).\n"
        "\n"
        "**A curated selection.** These groups are a hand-picked set of HS "
        "product categories — journalist-defined clusters of customs codes, "
        "not the full product universe — sorted into coarse **SITC buckets**. "
        "See the **[Glossary](#tab-glossary)** for how “SITC bucket” and “HS "
        "group” are defined."
    ),
    "trade-map": (
        "The trade map places **every** CN8 product code by its **SITC "
        "division**, weighted by 12-month import value — the whole structure, "
        "not just the curated groups above. SITC divisions **partition** the "
        "data (they sum to the total), so the shares are exhaustive; the "
        "explicit *unclassified* remainder and the *not-in-any-editorial-group* "
        "tail are shown rather than hidden, so you can see what the curated "
        "categories leave out.\n"
        "\n"
        "Value is EU-27 imports from China (CN+HK+MO), summed from the canonical "
        "Eurostat CN8 detail — never from the aggregate `000TOTAL` row, which "
        "would double-count."
    ),
    "gacc-bilateral": (
        "China’s own customs figures (GACC) for its trade with each major "
        "partner and bloc — released about a month ahead of Europe’s data, so "
        "the **earliest read** on where China’s trade is shifting. These are "
        "**China-reported** totals (exports valued FOB), not the EU mirror, and "
        "cover China’s whole world, not just Europe.\n"
        "\n"
        "**Scope note:** here China is the *reporter*, so — unlike the Eurostat "
        "sections above, where \"China\" means the **CN+HK+MO** envelope — **Hong "
        "Kong and Macao appear as China’s own partners** here, not folded into "
        "\"China\". It’s the one place that envelope doesn’t apply, by "
        "construction.\n"
        "\n"
        "Partners are ordered biggest first; **click one to expand** its rolling "
        "12-month exports and imports. Each line keeps its `finding/N` token."
    ),
    "methodology": (
        "Every value is a rolling 12-month total compared with the prior 12 "
        "months unless stated, and every line ends in its **`finding/N`** "
        "citation token — drillable back to the underlying source rows, FX "
        "rates and arithmetic. The **caveats** below are the methodological "
        "wrinkles that apply across whole families of findings (per-finding "
        "caveats are noted in place); the data **sources** and how much of "
        "each we hold live in the **Sources & coverage** tab."
    ),
}


def _deficit_indicator(
    cur,
    *,
    subkind: str = "trade_balance",
    key: str = "eu_china_deficit_per_day",
    kicker: str = "EU-27 DEFICIT",
    label: str = "EU-27 goods-trade deficit with China, HK & Macao",
    source: str = "eurostat",
    chart: str = "sparkline",
) -> Indicator | None:
    """A goods-trade deficit with China as a vital sign — the standing
    €/day level + YoY delta + a monthly sparkline. Its home is Key
    indicators (amended Q3): a level the delta sections can’t carry.
    Parameterised by `subkind` so EU-27 (`trade_balance`) and the UK
    (`trade_balance_uk`) reuse one builder. `chart="bignumber"` drops the
    sparkline so the card occupies a single KPI column (used for the UK
    card, freeing a slot — the headline EU-27 card is the only wide one)."""
    fid, detail = _latest_trade_balance(cur, subkind)
    if fid is None:
        return None
    roll = (detail or {}).get("totals", {}).get("rolling_12mo", {})
    per_day = roll.get("deficit_per_day_eur")
    if per_day is None:
        return None
    yoy = roll.get("yoy_pct")
    anchor = (detail or {}).get("windows", {}).get("anchor_period")
    as_of = date.fromisoformat(anchor) if isinstance(anchor, str) else None

    series = []
    for p in (detail or {}).get("monthly_deficit_series", []) or []:
        per = p.get("period")
        val = p.get("deficit_eur")
        if per is not None and val is not None:
            series.append(SeriesPoint(
                period=date.fromisoformat(per) if isinstance(per, str) else per,
                value=float(val),
            ))
    # The sparkline is a glanceable vital sign, not the full record — window it to
    # the most recent 36 months so the recent trajectory is legible rather than 9
    # years of dense wiggle. The window + cadence go in a hover tooltip (the
    # `<title>`) so the card itself stays uncluttered.
    series = series[-36:]
    spark_caption = (
        f"Monthly figures for the {len(series)} months to "
        f"{_fmt_month(series[-1].period)}" if series else None
    )

    delta = None
    if yoy is not None:
        delta = {
            "value": float(yoy),
            "direction": "wider" if float(yoy) > 0 else "narrower",
            "formatted": f"{'+' if float(yoy) >= 0 else ''}{float(yoy) * 100:.1f}% YoY",
        }

    # Scope: the headline is the CN+HK+MO envelope (our editorial standard —
    # ~15% of China's exports route via Hong Kong), named in the label. The note
    # carries the China-only counterpart external sources cite — with its OWN
    # YoY, since the headline's delta belongs to the CN+HK+MO figure and the two
    # must not read as one. (Full multi-surface scope pass tracked separately.)
    note = None
    _, cn_detail = _latest_trade_balance(cur, subkind + "_cn_only")
    cn_roll = (cn_detail or {}).get("totals", {}).get("rolling_12mo", {})
    cn_pd, cn_yoy = cn_roll.get("deficit_per_day_eur"), cn_roll.get("yoy_pct")
    if cn_pd is not None:
        cn_yoy_str = (
            f" ({'+' if float(cn_yoy) >= 0 else ''}{float(cn_yoy) * 100:.1f}% YoY)"
            if cn_yoy is not None else "")
        note = f"China-only: {_fmt_eur_per_day(cn_pd)}{cn_yoy_str}"

    return Indicator(
        key=key,
        kicker=kicker,
        label=label,
        value=float(per_day),
        unit="eur_per_day",
        formatted=_fmt_eur_per_day(per_day),
        chart=chart,
        delta=delta,
        note=note,
        chart_data=(ChartData(chart_type="sparkline", series=series,
                              extra={"caption": spark_caption} if spark_caption else {})
                    if chart == "sparkline" else None),
        provenance=Provenance(finding_ids=[fid], source=source, as_of=as_of),
    )


def _import_level_indicator(cur) -> Indicator | None:
    """EU-27 goods imports from China, rolling-12-month level (the size of
    the inflow, not its change). A clean all-goods figure straight off the
    same `trade_balance` finding as the deficit (`rolling_12mo.import_eur`,
    CN+HK+MO, 000TOTAL basis) — so it carries that finding’s citation."""
    fid, detail = _latest_trade_balance(cur, "trade_balance")
    if fid is None:
        return None
    roll = (detail or {}).get("totals", {}).get("rolling_12mo", {})
    imp = roll.get("import_eur")
    if imp is None:
        return None
    anchor = (detail or {}).get("windows", {}).get("anchor_period")
    as_of = date.fromisoformat(anchor) if isinstance(anchor, str) else None
    _, cn_detail = _latest_trade_balance(cur, "trade_balance_cn_only")
    cn_imp = (cn_detail or {}).get("totals", {}).get("rolling_12mo", {}).get("import_eur")
    note = f"China-only: {_fmt_eur(cn_imp)}" if cn_imp is not None else None
    return Indicator(
        key="eu_china_imports_12mo",
        kicker="EU-27 IMPORTS",
        label="EU-27 goods imports from China, HK & Macao (12-month)",
        value=float(imp),
        unit="eur",
        formatted=_fmt_eur(imp),
        chart="bignumber",
        note=note,
        provenance=Provenance(finding_ids=[fid], source="eurostat", as_of=as_of),
    )


def _latest_china_share(cur, subkind: str = "china_all_goods_share"):
    """The latest (by anchor) live china_all_goods_share finding — (id, detail)
    or (None, None). Ordered by anchor period so a back-filled revision can’t
    masquerade as the newest month."""
    cur.execute(
        """SELECT id, detail FROM findings
            WHERE superseded_at IS NULL AND subkind = %s
            ORDER BY (detail->'windows'->>'anchor_period')::date DESC NULLS LAST,
                     id DESC
            LIMIT 1""",
        (subkind,),
    )
    row = cur.fetchone()
    return (row["id"], row["detail"]) if row else (None, None)


def _china_share_indicator(cur) -> Indicator | None:
    """China’s share of EU-27 extra-EU all-goods imports — the dependency
    headline, as a donut. CN+HK+MO share (our editorial standard) with the
    CN-only comparator the press cites in the note. Reads the latest
    china_all_goods_share finding (the all-goods generalisation of
    partner_share; denominator is extra-EU, so this is China’s slice of the
    EU’s trade with the wider world, not 'share of EU consumption')."""
    fid, detail = _latest_china_share(cur, "china_all_goods_share")
    if fid is None:
        return None
    roll = (detail or {}).get("rolling_12mo", {})
    share = roll.get("share")
    if share is None:
        return None
    anchor = (detail or {}).get("windows", {}).get("anchor_period")
    as_of = date.fromisoformat(anchor) if isinstance(anchor, str) else None
    cn = roll.get("share_cn_only")
    note = f"China-only: {cn * 100:.1f}%" if cn is not None else None
    return Indicator(
        key="china_share_eu_imports",
        kicker="SHARE OF TRADE",
        # Name the CN+HK+MO envelope in the label, as the deficit/imports KPIs do
        # — the headline figure is the envelope; the note carries the China-only
        # comparator. (Convention: never report a "China" figure without saying
        # whether Hong Kong & Macao are in it.)
        label="China, HK & Macao share of EU-27 goods imports "
              "from outside the EU (12-month)",
        value=float(share),
        unit="share",
        formatted=f"{share * 100:.1f}%",
        chart="donut",
        note=note,
        provenance=Provenance(finding_ids=[fid], source="eurostat", as_of=as_of),
    )


def _biggest_mover_indicator(cur, surfaced_groups: set[str]) -> Indicator | None:
    """The biggest *single-product* (CN8) mover within the watched HS prefixes
    — a 'worth a look' provocation surfacing a product the ~46 displayed groups
    aggregate away (Option A of the roadmap 'Biggest mover KPI'; the stepping
    stone to the Option-B blind-spot radar). Reads `cn8_yoy_mover` findings at
    the latest anchor, ranks by composite |YoY|×log(value) (material, not just a
    high %), and *prefers a code whose parent group isn't already a Standout
    mover* — so the card adds something rather than restating the headline.

    Self-contained by design: there's no per-CN8 detail section to link to, so
    the card carries the product, the move, and its citation, and the framing
    flags whether it sits outside the headline movers."""
    cur.execute(
        """SELECT id, detail FROM findings
            WHERE superseded_at IS NULL AND subkind = 'cn8_yoy_mover'
            ORDER BY (detail->'windows'->>'current_end')::date DESC NULLS LAST""",
    )
    rows = cur.fetchall()
    if not rows:
        return None
    # Restrict to the latest anchor so a back-filled older month can't win.
    latest_anchor = (rows[0]["detail"].get("windows") or {}).get("current_end")
    cands = [
        (r["id"], r["detail"]) for r in rows
        if (r["detail"].get("windows") or {}).get("current_end") == latest_anchor
    ]
    # Drop CN8s that are watched only because a held-back group (hidden:/draft:)
    # widened the prefix set — keep a product if any non-held parent claims it.
    held = db.held_group_names(cur)
    cands = [(fid, d) for (fid, d) in cands
             if set(d.get("parent_groups") or []) - held]
    if not cands:
        return None

    def rank_key(item):
        _, d = item
        t = d.get("totals", {})
        yoy = abs(float(t.get("yoy_pct") or 0.0))
        eur = max(float(t.get("current_12mo_eur") or 0.0), 10.0)
        already = bool(set(d.get("parent_groups") or []) & surfaced_groups)
        # Un-surfaced parents first (False < True), then composite descending.
        return (already, -(yoy * math.log10(eur)))

    fid, d = min(cands, key=rank_key)
    t = d.get("totals", {})
    prod = d.get("product", {})
    yoy = float(t.get("yoy_pct"))
    cur_eur = float(t.get("current_12mo_eur"))
    anchor = (d.get("windows") or {}).get("current_end")
    as_of = date.fromisoformat(anchor) if isinstance(anchor, str) else None
    parents = d.get("parent_groups") or []
    off_watch = not (set(parents) & surfaced_groups)
    # The product is the card's prominent line now (not a truncated tail), so
    # allow a longer label, word-boundary-trimmed; the full denomination is in
    # the provenance drawer.
    product = (prod.get("label_short") or prod.get("cn8") or "").strip()
    if len(product) > 58:
        product = (product[:58].rsplit(" ", 1)[0] or product[:57]).rstrip() + "…"
    frame = ("outside the headline movers" if off_watch
             else f"within {parents[0]}" if parents else "")
    # Product is the prominent label (card line 1); value + framing become the
    # note so the HTML card can drop them to their own line beneath it
    # (.kpi-mover). Markdown rejoins "label · note" with the same separator, so
    # its rendered line is unchanged.
    detail = f"{_fmt_eur(cur_eur)} imports, 12mo"
    if frame:
        detail += f" · {frame}"
    # 'How this is calculated' rollover; the provenance drawer carries the full
    # source trail + arithmetic.
    tooltip = (
        "Biggest single product (8-digit CN8 code) by change in its 12-month "
        "import value (EU-27 from China, HK & Macao). Filtered for size "
        "(≥ €25M in both the latest and prior year), a move that held across the "
        "last 3 months, and robustness to any single shipment. Open the citation "
        "below for the full workings."
    )
    return Indicator(
        key="cn8_biggest_mover",
        kicker="BIGGEST MOVER",
        label=product,
        value=float(yoy),
        unit="yoy_pct",   # signals a signed change → the value is coloured
        formatted=_fmt_pct_kpi(yoy),
        chart="bignumber",
        note=detail,
        tooltip=tooltip,
        provenance=Provenance(finding_ids=[fid], source="eurostat", as_of=as_of),
    )


def _headline_item(m: dict, disp: dict[str, str]) -> HeadlineItem:
    """One restated quotable mover → a HeadlineItem (Q2)."""
    is_export = m["subkind"].endswith("_export")
    yoy = float(m["yoy_pct"])
    group = m["group_name"]  # stable internal key (themes lookup, slug source)
    group_disp = disp.get(group, group)  # reader-facing label
    pred = m.get("predictability")
    badge = pred[0] if pred is not None else None
    metrics = {
        "direction": "rose" if yoy > 0 else "fell",
        "pct": abs(yoy),
        "value_eur": float(m["current_eur"]),
    }
    if m.get("yoy_pct_kg") is not None:
        metrics["volume_pct"] = float(m["yoy_pct_kg"])
    return HeadlineItem(
        subject={
            "scope": "eu_27",
            "flow": "export" if is_export else "import",
            "group_name": group_disp,
        },
        metrics=metrics,
        stability={"badge": badge, "hedge_phrase": None},
        prose=_mover_sentence(m, disp),  # v0: carries markdown — see module note
        drill_down=_slugify_heading(group_disp),
        provenance=Provenance(
            finding_ids=[m["id"]], source="eurostat",
            as_of=m.get("current_end"),
        ),
        facets=Facets(commodity=[group_disp],
                      theme=labels.themes_for_group(group)),
    )


def _what_changed(diff, disp: dict[str, str]) -> WhatChanged:
    from briefing_pack.sections.front_page import _since_last_pack_lines
    import dataclasses as _dc
    # Track-scope the portal tally (2026-07-15): the Full briefing's "since
    # the last briefing" numbers must not move on a GACC-track calendar
    # arrival — supersession (and news) follows source-track. GACC-track
    # findings all carry gacc_* subkinds; mirror-gap findings (Eurostat×GACC
    # crosses) are main-track and don't. The bundle's findings.md keeps the
    # unscoped diff (it documents the whole cycle); this scoping is
    # portal-model only.
    main_new = sum(n for sk, n in (diff.new_by_subkind or [])
                   if not str(sk).startswith("gacc_"))
    if main_new != diff.total_new:
        diff = _dc.replace(diff, total_new=main_new)
    # `disp` maps the stable internal group key → reader-facing display name.
    # Shift.group_name is rendered verbatim by both renderers (no slug, no
    # lookup), so resolve the display string here at the model-build site.
    significant = [
        Shift(
            group_name=disp.get(s["group_name"], s["group_name"]),
            subkind=s["subkind"],
            window_end=s.get("window_end"),
            old_yoy=s.get("old_yoy"),
            new_yoy=s.get("new_yoy"),
            direction_flipped=s.get("direction_flipped", False),
        )
        for s in diff.significant
    ]
    # The digest prose is authored once (editorial substance); strip the
    # leading markdown label so the field is closer to plain content.
    summary = " ".join(_since_last_pack_lines(diff, disp)).replace(
        "**Since the last pack:** ", ""
    )
    return WhatChanged(
        regime=diff.regime,
        summary=summary,
        significant=significant,
        new_count=diff.total_new,
        restated_count=diff.restated_count,
        restated_range=diff.restated_range or None,
    )


def _series_chart(monthly_series) -> ChartData | None:
    if not monthly_series:
        return None
    pts = []
    for p in monthly_series:
        per, val = p.get("period"), p.get("value_eur")
        if per is None or val is None:
            continue
        pts.append(SeriesPoint(
            period=date.fromisoformat(per) if isinstance(per, str) else per,
            value=float(val),
        ))
    return ChartData(chart_type="line", series=pts) if len(pts) >= 2 else None


def _sector_detail_section(cur, predictability: dict | None = None) -> Section:
    """The navigable granularity layer: one child Section per HS group,
    each carrying its import + export Finding. The group’s Section id is
    `_slugify_heading(name)` — the SAME slug the headline movers point
    their drill-downs at, so those links resolve here. Groups ordered by
    12-month value (biggest sectors first); navigation/search by `facets`
    is the later refinement.
    """
    root = Section(
        id="sector-detail", title="Sector detail", kind="sector_detail",
        intro="Every [HS group](#gloss-hs-group)'s rolling 12-month value vs "
              "the prior 12 months, China ↔ EU-27, both flows.",
        about=_ABOUT["sector-detail"],
    )
    cur.execute(
        """SELECT max((detail->'windows'->>'current_end')::date)
             FROM findings
            WHERE subkind IN ('hs_group_yoy','hs_group_yoy_export')
              AND superseded_at IS NULL"""
    )
    anchor = cur.fetchone()[0]
    if anchor is None:
        return root
    # Group HS patterns → SITC division facet (the structural spine);
    # descriptions for the inline glossary.
    cur.execute("SELECT name, hs_patterns, description, created_by FROM hs_groups")
    _g = cur.fetchall()
    patterns_by_name = {n: (p or []) for n, p, d, cb in _g}
    desc_by_name = {n: d for n, p, d, cb in _g}
    held_by_name = {n: db.is_held_created_by(cb) for n, p, d, cb in _g}
    # Reader-facing group labels. `name` stays the lookup key for every dict
    # above (patterns_by_name, desc_by_name, share_by_name, traj_by_name,
    # by_group, predictability…); disp is used only for displayed titles and
    # the slug/commodity-facet that anchor cross-references.
    disp = db.group_display_names(cur)
    # Latest China-share-of-EU-imports per group (partner_share).
    cur.execute(
        """SELECT DISTINCT ON (detail->'group'->>'name')
                  detail->'group'->>'name', id,
                  (detail->'totals'->>'share_value')::numeric,
                  (detail->'totals'->>'share_kg')::numeric,
                  detail->'windows'->>'current_end'
             FROM findings
            WHERE superseded_at IS NULL AND subkind='partner_share'
            ORDER BY detail->'group'->>'name',
                     (detail->'windows'->>'current_end')::date DESC"""
    )
    share_by_name = {n: (fid, _f(sv), _f(sk), end)
                     for n, fid, sv, sk, end in cur.fetchall()}
    # China's share of EU-27 *exports* of the group (China as a destination).
    cur.execute(
        """SELECT DISTINCT ON (detail->'group'->>'name')
                  detail->'group'->>'name', id,
                  (detail->'totals'->>'share_value')::numeric
             FROM findings
            WHERE superseded_at IS NULL AND subkind='partner_share_export'
            ORDER BY detail->'group'->>'name',
                     (detail->'windows'->>'current_end')::date DESC"""
    )
    export_share_by_name = {n: (fid, _f(sv)) for n, fid, sv in cur.fetchall()}
    # Trajectory shape (volatile / accelerating / declining …) per group, all
    # three scopes × flow.
    _SCOPE_LABEL = {"eu_27": "EU-27", "uk": "UK", "eu_27_plus_uk": "EU-27+UK"}
    cur.execute(
        """SELECT detail->'group'->>'name', id, subkind, detail->>'shape_label',
                  detail->>'comparison_scope'
             FROM findings WHERE superseded_at IS NULL
              AND subkind LIKE 'hs_group_trajectory%'"""
    )
    traj_by_name: dict[str, dict] = {}
    traj_ids_by_name: dict[str, list] = {}
    for n, fid, sk, shape, scope in cur.fetchall():
        sl = _SCOPE_LABEL.get(scope, scope)
        flow = "export" if sk.endswith("_export") else "import"
        traj_by_name.setdefault(n, {}).setdefault(sl, {})[flow] = shape
        traj_ids_by_name.setdefault(n, []).append(fid)
    # All three reporter scopes (EU-27 / UK / EU-27+UK) per group, each at its
    # own latest anchor (HMRC may lag Eurostat).
    scopes = [
        ("EU-27", "eurostat", ("hs_group_yoy", "hs_group_yoy_export")),
        ("UK", "hmrc", ("hs_group_yoy_uk", "hs_group_yoy_uk_export")),
        ("EU-27+UK", "cross_source",
         ("hs_group_yoy_combined", "hs_group_yoy_combined_export")),
    ]
    scope_order = {"EU-27": 0, "UK": 1, "EU-27+UK": 2}
    by_group: dict[str, dict] = {}
    for scope_label, src, subks in scopes:
        cur.execute(
            """SELECT max((detail->'windows'->>'current_end')::date) FROM findings
                WHERE subkind = ANY(%s) AND superseded_at IS NULL""",
            (list(subks),),
        )
        sa = cur.fetchone()[0]
        if sa is None:
            continue
        cur.execute(
            """SELECT id, subkind, detail FROM findings WHERE superseded_at IS NULL
                AND subkind = ANY(%s)
                AND (detail->'windows'->>'current_end')::date = %s""",
            (list(subks), sa),
        )
        for fid, subkind, detail in cur.fetchall():
            grp = ((detail or {}).get("group") or {}).get("name") or "Unknown"
            grp_disp = disp.get(grp, grp)  # reader-facing label; grp = key
            tot = (detail or {}).get("totals", {})
            sm = tot.get("single_month") or {}
            is_export = subkind.endswith("_export")
            finding = Finding(
                finding_id=fid, subkind=subkind,
                title=(f"{scope_label} {'exports' if is_export else 'imports'} "
                       f"of {grp_disp} {'to' if is_export else 'from'} China"),
                metrics={
                    "scope": scope_label,
                    "flow": "export" if is_export else "import",
                    "yoy_pct": _f(tot.get("yoy_pct")),
                    "current_eur": _f(tot.get("current_12mo_eur")),
                    "yoy_pct_kg": _f(tot.get("yoy_pct_kg")),
                    "low_base": bool(tot.get("low_base")),
                    # Latest-month register (acceleration signal) alongside the
                    # 12-month rolling figure — both, as the Findings doc shows.
                    "sm_yoy_pct": _f(sm.get("yoy_pct")),
                    "sm_yoy_pct_kg": _f(sm.get("yoy_pct_kg")),
                    "sm_period": sm.get("current_period"),
                    "caveats": _visible_caveats((detail or {}).get("caveat_codes")),
                },
                chart_data=_series_chart((detail or {}).get("monthly_series")),
                provenance=Provenance(
                    finding_ids=[fid], source=src, as_of=sa,
                    caveat="low base" if tot.get("low_base") else None,
                ),
                facets=Facets(commodity=[grp_disp]),
            )
            g = by_group.setdefault(grp, {"max_eur": 0.0, "findings": []})
            g["findings"].append(finding)
            if scope_label == "EU-27":
                g["max_eur"] = max(g["max_eur"],
                                   finding.metrics["current_eur"] or 0.0)
                if not is_export:  # rich detail lives on the EU-27 import row
                    g["import_eur"] = finding.metrics["current_eur"] or 0.0
                    g["top_cn8"] = (detail or {}).get(
                        "top_cn8_codes_in_current_12mo") or []
                    g["reporters"] = (detail or {}).get(
                        "per_reporter_breakdown") or []

    built: list[tuple] = []  # (import_value, section_code, Section)
    for name, g in by_group.items():
        # ordered scope (EU-27, UK, combined), then export-then-import
        fs = sorted(g["findings"], key=lambda f: (
            scope_order.get(f.metrics.get("scope"), 9), f.metrics["flow"]))
        sectors = classifications.sitc_divisions_for_patterns(
            patterns_by_name.get(name, [])
        )
        themes = labels.themes_for_group(name)
        end_use = classifications.enduse_for_patterns(patterns_by_name.get(name, []))
        sfid, sv, sk, send = share_by_name.get(name, (None, None, None, None))
        metrics = {}
        if sv is not None or sk is not None:
            metrics = {"china_share_value": sv, "china_share_kg": sk,
                       "china_share_period": send, "china_share_finding": sfid}
        top = (g.get("top_cn8") or [])[:3]
        if top:
            # Bake the product description into the snapshot so the static portal
            # can show "Citric acid (29181400)" with the full self-explanatory
            # text on hover — no runtime lookup. Empty strings if the (optional)
            # cn8_descriptions.csv is absent: the render degrades to bare codes.
            _desc = classifications.cn8_description_lookup()
            metrics["top_cn8"] = [
                {"code": t.get("hs_code"),
                 "eur": _f(t.get("total_eur")),
                 "label": (_desc.get(t.get("hs_code") or "") or {}).get("short", ""),
                 "desc": (_desc.get(t.get("hs_code") or "") or {}).get("full", "")}
                for t in top]
        reps = sorted(g.get("reporters") or [],
                      key=lambda r: -(r.get("share_of_group_delta_pct") or 0))[:3]
        if reps:
            # yoy is suppressed (None, plus a low_base flag) when the
            # reporter's prior-window base is under the render-time floor —
            # report.json is a data surface, so the misleading % must not
            # ship in it even though the HTML only renders `share`.
            metrics["reporters"] = []
            for r in reps:
                lb = anomalies.reporter_yoy_is_low_base(r.get("prior_eur"))
                metrics["reporters"].append(
                    {"reporter": r.get("reporter"),
                     "share": _f(r.get("share_of_group_delta_pct")),
                     "yoy": None if lb else _f(r.get("yoy_pct")),
                     "low_base": lb})
        if traj_by_name.get(name):
            metrics["trajectory"] = traj_by_name[name]
            metrics["trajectory_findings"] = traj_ids_by_name.get(name, [])
        efid, esv = export_share_by_name.get(name, (None, None))
        if esv is not None:
            metrics["china_export_share_value"] = esv
            metrics["china_export_share_finding"] = efid
        # Predictability badge (🟢/🟡/🔴) per group — the same stability signal the
        # Findings doc shows beside each group name. Absent for groups with too
        # little history (correctly = no badge).
        pred = (predictability or {}).get(name)
        if pred:
            metrics["predictability"] = {
                "badge": pred[0], "persistence_pct": _f(pred[1]), "n": pred[2]}
        sec_code, sec_title = _primary_section(sectors)
        metrics["section"] = {"code": sec_code, "title": sec_title}
        value = g.get("import_eur", g["max_eur"]) or 0.0
        name_disp = disp.get(name, name)  # reader-facing title/slug; name = key
        # Held-back groups (hidden:/draft:) are listed here but flagged and kept
        # out of the rankings (Standout movers + Biggest-mover KPI). The marker
        # rides the title so it shows in any renderer; metrics['held'] lets the
        # HTML style it. Slug stays name-based so existing links still resolve.
        held = held_by_name.get(name, False)
        if held:
            metrics["held"] = True
        title_disp = (f"{name_disp} (held back — not yet in rankings)"
                      if held else name_disp)
        built.append((value, sec_code, Section(
            id=_slugify_heading(name_disp), title=title_disp, kind="sector_detail",
            findings=fs, metrics=metrics, intro=desc_by_name.get(name),
            facets=Facets(commodity=[name_disp], sector=sectors, theme=themes,
                          end_use=end_use),
        )))
    # Group by SITC section: sections ordered by their groups' combined 12-month
    # EU-27 import value (biggest category leads — keeps "meaty first"), section
    # '9' (Other) last; groups within a section by value. The renderer inserts a
    # subhead at each section boundary; `section_index` carries the subhead data.
    sec_val: dict[str, float] = defaultdict(float)
    sec_cnt: dict[str, int] = defaultdict(int)
    sec_title_by: dict[str, str] = {}
    for value, sc, secobj in built:
        sec_val[sc] += value
        sec_cnt[sc] += 1
        sec_title_by[sc] = secobj.metrics["section"]["title"]
    section_order = sorted(sec_val, key=lambda sc: (sc == "9", -sec_val[sc], sc))
    rank = {sc: i for i, sc in enumerate(section_order)}
    built.sort(key=lambda t: (rank[t[1]], -t[0], t[2].title))
    root.sections = [secobj for _v, _sc, secobj in built]
    root.metrics = {"section_index": [
        {"code": sc, "title": sec_title_by[sc], "value": sec_val[sc],
         "count": sec_cnt[sc]} for sc in section_order]}
    return root


def _gacc_latest_period(cur) -> date | None:
    cur.execute(
        """SELECT max((detail->'windows'->>'current_end')::date)
             FROM findings
            WHERE subkind = 'gacc_aggregate_yoy' AND superseded_at IS NULL"""
    )
    return cur.fetchone()[0]


def _gacc_macro_items(cur, period) -> list[HeadlineItem]:
    """The GACC variant’s macro/geographic lead: China’s own reported
    exports and imports by partner *bloc* (ASEAN / Africa / Latin America /
    Total), both flows. The bilateral (per-country) detail is the deeper
    layer, a future GACC sections tree."""
    if period is None:
        return []
    cur.execute(
        """SELECT id, subkind, detail FROM findings
            WHERE superseded_at IS NULL
              AND subkind IN ('gacc_aggregate_yoy','gacc_aggregate_yoy_import')
              AND (detail->'windows'->>'current_end')::date = %s""",
        (period,),
    )
    items: list[HeadlineItem] = []
    for fid, subkind, detail in cur.fetchall():
        is_export = not subkind.endswith("_import")
        bloc = ((detail or {}).get("aggregate") or {}).get("raw_label") or "partners"
        tot = (detail or {}).get("totals", {})
        yoy = _f(tot.get("yoy_pct"))
        eur = _f(tot.get("current_12mo_eur"))
        subj = (f"China’s exports to {bloc}" if is_export
                else f"China’s imports from {bloc}")
        # No YoY → state the level, never a direction. Asserting "fell" for a
        # missing change (e.g. a first-period bloc) publishes a move the data
        # doesn't support; the magnitude was already dashed out, so the verb
        # must be too.
        if yoy is None:
            verb = None
            prose = (f"**{subj}** stood at {_fmt_eur(eur)} in the 12 months "
                     f"to {_fmt_month(period)} (year-on-year change "
                     f"unavailable). `finding/{fid}`")
        else:
            verb = "rose" if yoy > 0 else "fell"
            prose = (f"**{subj}** {verb} {abs(yoy) * 100:.1f}% by value in "
                     f"the 12 months to {_fmt_month(period)}, to "
                     f"{_fmt_eur(eur)}. `finding/{fid}`")
        items.append(HeadlineItem(
            subject={"scope": "china",
                     "flow": "export" if is_export else "import",
                     "group_name": bloc},
            metrics={"direction": verb,
                     "pct": abs(yoy) if yoy is not None else None,
                     "value_eur": eur},
            stability={"badge": None, "hedge_phrase": None},
            prose=prose,
            provenance=Provenance(finding_ids=[fid], source="gacc", as_of=period),
            facets=Facets(partner=[bloc]),
        ))
    # Total first, then by magnitude of move.
    items.sort(key=lambda it: (
        0 if it.subject["group_name"].lower() == "total" else 1,
        -(it.metrics["pct"] or 0),
    ))
    return items


_TB_SCOPES = [
    ("trade_balance", "trade_balance_cn_only", "EU-27 (Eurostat)", "eurostat"),
    ("trade_balance_uk", "trade_balance_uk_cn_only", "UK (HMRC)", "hmrc"),
    ("trade_balance_combined", "trade_balance_combined_cn_only",
     "EU-27 + UK", "cross_source"),
]


def _latest_deficit_per_day(cur, subkind):
    """(finding_id, deficit_per_day_eur) for the latest finding of a
    trade-balance subkind — used for the CN-only (excl. HK/Macao) counterpart,
    which is still Eurostat, not GACC."""
    cur.execute(
        """SELECT id, (detail->'totals'->'rolling_12mo'->>'deficit_per_day_eur')::numeric
             FROM findings WHERE superseded_at IS NULL AND subkind=%s
             ORDER BY (detail->'windows'->>'anchor_period')::date DESC LIMIT 1""",
        (subkind,),
    )
    r = cur.fetchone()
    return (r[0], _f(r[1])) if r else (None, None)


def _state_of_play_section(cur) -> Section:
    """The 'where things stand' companion (Q3). First cut: Europe’s
    standing goods-trade deficit with China across the three reporter
    scopes — the canonical standing level (the ~€1bn/day figure). A level,
    not a change, so it lives here rather than in 'what changed'."""
    root = Section(
        id="state-of-play", title="Europe’s trade position with China",
        kind="state_of_play",
        intro="The standing picture from Europe’s side of the ledger — how big "
              "the deficit is, and how much of its trade from outside the bloc "
              "China accounts for.",
        about=_ABOUT["the-deficit"],
    )
    deficit = Section(
        id="the-deficit",
        title="Europe’s deficit with China", kind="state_of_play",
        intro="The standing level by reporter scope, on the "
              "[CN+HK+MO](#gloss-cn-hk-mo) envelope — a level, not this "
              "cycle’s change.",
    )
    for subkind, cn_subkind, label, source in _TB_SCOPES:
        cur.execute(
            """SELECT id, detail FROM findings
                WHERE superseded_at IS NULL AND subkind = %s
                ORDER BY (detail->'windows'->>'anchor_period')::date DESC
                LIMIT 1""",
            (subkind,),
        )
        row = cur.fetchone()
        if row is None:
            continue
        fid, d = row["id"], row["detail"]
        cn_fid, cn_per_day = _latest_deficit_per_day(cur, cn_subkind)
        roll = (d or {}).get("totals", {}).get("rolling_12mo", {})
        anchor = (d or {}).get("windows", {}).get("anchor_period")
        as_of = date.fromisoformat(anchor) if isinstance(anchor, str) else None
        series = []
        for p in (d or {}).get("monthly_deficit_series", []) or []:
            per, val = p.get("period"), p.get("deficit_eur")
            if per is not None and val is not None:
                pd = date.fromisoformat(per) if isinstance(per, str) else per
                # Start at 2019 so this chart shares an x-axis range with the
                # China-dependency trend below (which can't be extended earlier).
                if pd is not None and pd.year < 2019:
                    continue
                series.append(SeriesPoint(period=pd, value=float(val)))
        deficit.findings.append(Finding(
            finding_id=fid, subkind=subkind,
            title=f"{label} deficit with China",
            metrics={
                "scope": label,
                "deficit_eur": _f(roll.get("deficit_eur")),
                "per_day_eur": _f(roll.get("deficit_per_day_eur")),
                "yoy_pct": _f(roll.get("yoy_pct")),
                "cn_per_day_eur": cn_per_day,  # CN-only (excl. HK/Macao) Eurostat counterpart — the published EU-China basis, NOT GACC
                "cn_finding": cn_fid,
            },
            chart_data=(ChartData(chart_type="line", series=series)
                        if len(series) >= 2 else None),
            provenance=Provenance(
                finding_ids=[fid] + ([cn_fid] if cn_fid else []),
                source=source, as_of=as_of),
        ))
    if deficit.findings:
        root.sections.append(deficit)
    # China-dependency trend — the share-of-EU-imports line over time, the
    # companion to the donut KPI. From the latest china_all_goods_share (import)
    # finding's share_series. Kept as a plain dict (like the GACC partner_charts)
    # so it JSON-serialises into the snapshot.
    cs_fid, cs_detail = _latest_china_share(cur, "china_all_goods_share")
    if cs_fid is not None:
        ser = [
            {"period": p["period"], "share": float(p["share"])}
            for p in ((cs_detail or {}).get("share_series") or [])
            if p.get("period") and p.get("share") is not None
        ]
        if len(ser) >= 2:
            roll = (cs_detail or {}).get("rolling_12mo", {})
            root.metrics["china_share_trend"] = {
                "heading": "China vs the rest of the world",
                "title": "China’s share of EU-27 goods imports from outside the EU",
                "series": ser,
                "share_now": roll.get("share"),
                "finding_id": cs_fid,
            }
    return root


def _mirror_gap_section(cur) -> Section:
    """The China↔EU mirror-trade discrepancy — the signature distinctive
    analysis. Per partner: China’s reported exports vs the partner’s reported
    imports, the gap, and how much exceeds the CIF/FOB accounting baseline
    (the transshipment signal), with the named hub where relevant."""
    root = Section(
        id="mirror-gaps", title="Mirror-trade gaps", kind="mirror_gap",
        intro="China’s reported exports to each partner vs the partner’s "
              "reported imports from China — the discrepancy, and how much of "
              "it exceeds the CIF/FOB accounting baseline (a transshipment "
              "signal).",
        about=_ABOUT["mirror-gaps"],
    )
    cur.execute(
        "SELECT id, title, detail FROM findings "
        "WHERE superseded_at IS NULL AND subkind='mirror_gap'"
    )
    rows = []
    for fid, title, d in cur.fetchall():
        m = re.search(r"(\d{4}-\d{2})", title or "")
        rows.append((m.group(1) if m else "", fid, title, d))
    if not rows:
        return root
    latest = max(r[0] for r in rows)
    as_of = date.fromisoformat(latest + "-01") if latest else None
    # Latest z-score per partner (how unusual the gap is vs its 6-mo baseline).
    cur.execute(
        """SELECT DISTINCT ON (detail->>'iso2') detail->>'iso2', id,
                  (detail->>'z_score')::numeric, detail->>'period'
             FROM findings WHERE superseded_at IS NULL
              AND subkind='mirror_gap_zscore'
            ORDER BY detail->>'iso2', (detail->>'period')::date DESC"""
    )
    zby = {iso: (zid, _f(z), per2) for iso, zid, z, per2 in cur.fetchall()}
    for per, fid, title, d in rows:
        if per != latest:
            continue
        d = d or {}
        hub = d.get("transshipment_hub") or {}
        pm = re.search(r"China ↔ ([^,]+),", title or "")
        zid, zval, zper = zby.get(d.get("iso2"), (None, None, None))
        root.findings.append(Finding(
            finding_id=fid, subkind="mirror_gap",
            title=title,
            metrics={
                "partner": pm.group(1) if pm else "?",
                "is_aggregate": bool(d.get("is_aggregate"))
                or str(d.get("iso2") or "").upper().startswith("BLOC"),
                "gacc_eur": _f((d.get("gacc") or {}).get("value_eur_converted")),
                "eurostat_eur": _f((d.get("eurostat") or {}).get("total_eur")),
                "gap_eur": _f(d.get("gap_eur")),
                "gap_pct": _f(d.get("gap_pct")),
                "excess_pct": _f(d.get("excess_over_baseline_pct")),
                "baseline_pct": _f(d.get("cif_fob_baseline_pct")),
                "hub": hub.get("iso2"),
                "hub_notes": hub.get("notes"),
                "zscore": zval, "zscore_period": zper,
            },
            provenance=Provenance(
                finding_ids=[fid] + ([zid] if zid else []),
                source="cross_source", as_of=as_of,
                caveat=", ".join(d.get("caveat_codes") or []) or None,
            ),
        ))
    # The EU-bloc aggregate leads (it's the whole-bloc picture, not one member);
    # then members by biggest excess-over-baseline (the strongest transshipment
    # signal).
    root.findings.sort(key=lambda f: (
        not f.metrics.get("is_aggregate"),
        -(f.metrics.get("excess_pct") or -9)))
    return root


_UNCLASSIFIED = "unclassified"


def _structural_section(cur, anchor: date | None) -> Section:
    """The structural SITC-division browse — the full trade map by value,
    surfacing the value / codes that sit in no editorial group. Each division
    is a *summary* node (Section.metrics): its value share, how much is covered
    by editorial groups, code count, and the groups within it. No per-code
    findings (those don’t exist for the tail); this is the spine made visible.

    Value is the rolling 12-month EU-27 import total to `anchor`, summed from
    the canonical Eurostat CN8 detail (`eurostat_raw_rows`) with the *same*
    partner envelope (CN+HK+MO), EU-27 reporter set, flow and window the
    sector-detail findings use — so the trade map reconciles with them by
    construction. It must NOT re-aggregate `observations`: that table mixes
    period_kinds, every release snapshot, and the `000TOTAL` aggregate row, so
    a naive sum there multiply-counts (~8× on real data).

    Real 8-digit CN8 codes partition the total (they sum to ~99% of the
    `000TOTAL` all-goods figure); the tiny remainder with no SITC mapping is
    kept as an explicit `unclassified` bucket rather than silently dropped, so
    the division shares sum to the true total."""
    cn8div = classifications.cn8_division_map()
    if anchor is None:
        cur.execute("SELECT max(period) FROM eurostat_raw_rows WHERE flow = 1")
        anchor = cur.fetchone()[0]
    if not cn8div or anchor is None:
        return Section(id="trade-map", title="Trade map", kind="structural")

    root = Section(
        id="trade-map", title="Trade map", kind="structural",
        intro="Every [CN8](#gloss-cn8-combined-nomenclature-8-digit) code by "
              "[SITC division](#gloss-sitc-bucket), value-weighted — the whole "
              "structure, including the value outside the editorial groups. "
              f"EU-27 imports from China (CN+HK+MO), rolling 12 months to "
              f"{_fmt_month(anchor)}.",
        about=_ABOUT["trade-map"],
        provenance=Provenance(source="eurostat", as_of=anchor),
    )
    start = _months_back(anchor, 11)

    cur.execute("SELECT hs_patterns FROM hs_groups")
    pats = [p.replace("%", "") for (pp,) in cur.fetchall() for p in (pp or [])]

    def covered(c):
        return any(c.startswith(p) for p in pats)

    # Per-CN8 value over the rolling 12 months, EU-27 imports from CN+HK+MO —
    # mirrors anomalies._hs_group_top_cn8s, minus the pattern filter (all codes).
    cur.execute(
        """SELECT product_nc, sum(value_eur) FROM eurostat_raw_rows
            WHERE flow = 1
              AND partner = ANY(%s)
              AND reporter <> ALL(%s)
              AND period >= %s AND period <= %s
            GROUP BY product_nc""",
        (list(EUROSTAT_PARTNERS), list(EU27_EXCLUDE_REPORTERS), start, anchor),
    )
    dval: dict = defaultdict(float)
    dcov: dict = defaultdict(float)
    dn: dict = defaultdict(int)
    total = 0.0
    for code, val in cur.fetchall():
        # Real 8-digit CN8 only — excludes 000TOTAL and the NNXXXXXX
        # confidential-aggregate codes, which would double-count the detail.
        if not (code and code.isdigit() and len(code) == 8):
            continue
        v = float(val or 0)
        total += v
        d = cn8div.get(code) or _UNCLASSIFIED
        dval[d] += v
        dn[d] += 1
        if covered(code):
            dcov[d] += v

    cur.execute("SELECT name, hs_patterns FROM hs_groups")
    gdiv: dict = defaultdict(list)
    for name, pp in cur.fetchall():
        for d in classifications.sitc_divisions_for_patterns(pp or []):
            gdiv[d].append(name)
    # Reader-facing labels. The per-division group list below both displays the
    # name and links it to the sector-detail heading, so name + slug use disp
    # (keeping them consistent with that heading's title + anchor).
    disp = db.group_display_names(cur)

    # Real divisions by value desc; the unclassified remainder always last.
    order = sorted((d for d in dval if d != _UNCLASSIFIED), key=lambda x: -dval[x])
    if _UNCLASSIFIED in dval:
        order.append(_UNCLASSIFIED)
    for d in order:
        is_uncl = d == _UNCLASSIFIED
        root.sections.append(Section(
            id="sitc-" + d,
            title=("Unclassified (no SITC division)" if is_uncl
                   else classifications.division_title(d)),
            kind="structural",
            facets=Facets(sector=[] if is_uncl else [d]),
            metrics={
                "value_share": dval[d] / total if total else 0.0,
                "covered_share": dcov[d] / dval[d] if dval[d] else 0.0,
                "value_eur": dval[d],
                "code_count": dn[d],
                "groups": [{"name": disp.get(g, g),
                            "slug": _slugify_heading(disp.get(g, g))}
                           for g in sorted(set(gdiv.get(d, [])))],
            },
            provenance=Provenance(source="eurostat", as_of=anchor),
        ))
    root.metrics = {"total_codes": sum(dn.values()), "divisions": len(dval),
                    "total_eur": total}
    return root


# Curated methodology explainers for the Methodology tab — drawn from the long
# Findings preamble + docs/methodology.md, kept short. Each renders as a titled
# block (light markdown). Reference-grade, so they live with the report content.
_METHOD_GUIDES: list[dict] = [
    {
        "title": "The three comparison scopes",
        "body": (
            "Each category is shown three ways. **EU-27** is Eurostat data with "
            "UK rows excluded at all times (so it’s comparable across the whole "
            "period, including pre-Brexit years). **UK** is HMRC data, converted "
            "to EUR at the period’s reference rate. **EU-27 + UK** adds the two "
            "together — a useful combined view, but it sums two different "
            "statistical agencies’ figures, so it is not a like-for-like number "
            "from a single source.\n"
            "\n"
            "**\"China\" always includes Hong Kong and Macao** (CN+HK+MO): "
            "European statistics report trade routed via Hong Kong and Macao "
            "under separate customs codes, but editorially it is still Chinese "
            "trade, so every figure sums all three."
        ),
    },
    {
        "title": "Predictability badges (🟢 🟡 🔴)",
        "body": (
            "Some HS-group findings carry a badge scoring how *stable* the "
            "group’s year-on-year signal has been. Each (scope, flow) view is "
            "compared with its reading six months earlier and counts as "
            "**persistent** if the direction didn’t flip and the rate moved by "
            "less than 5 percentage points:\n"
            "\n"
            "- 🟢 **Reliable** — at least 67% of views persistent; the move has "
            "held up.\n"
            "- 🟡 **Mixed** — 33–67% persistent; treat with some caution.\n"
            "- 🔴 **Volatile** — under 33% persistent; noise-dominated at the "
            "year-on-year level.\n"
            "\n"
            "**No badge** means too few comparable views (fewer than three) to "
            "score — not the same as red; just not enough history yet."
        ),
    },
    {
        "title": "What to quote, what to hedge",
        "body": (
            "Quote the **12-month rolling** figure and the **absolute € "
            "amount**. Treat the **latest-month** figure as a direction hint, "
            "not a headline — it swings wildly on lumpy categories. Where a "
            "**low base** is flagged, quote the € amount, not the %. Where a "
            "**🔴 volatile** badge sits, verify independently before quoting. "
            "Every number is drillable to source via its **`finding/N`** token "
            "— ask for the audit trail before publishing anything contestable."
        ),
    },
]


_SOURCE_NOTES = {
    "eurostat": "Eurostat Comext — EU-27 extra-EU imports/exports with China "
                "(CN+HK+MO), CN8 8-digit, monthly. Imports valued CIF.",
    "gacc": "China General Administration of Customs — China’s own reported "
            "trade, preliminary monthly releases (CNY and USD), exports FOB.",
    "hmrc": "HMRC Overseas Trade Statistics — UK trade with China, converted "
            "to EUR at the period’s reference rate.",
}


def _reference_section(cur) -> Section:
    """The endmatter — methodology orientation, the caveats that apply, and the
    sources. The defensibility scaffolding ported from the old Findings doc
    (reader’s guide + methodology footer + sources appendix), minus the LLM
    narrative which stays deferred."""
    cur.execute("SELECT code, summary, detail FROM caveats ORDER BY code")
    caveats = [{"code": c, "summary": s, "detail": d}
               for c, s, d in cur.fetchall()]
    # Sources moved to the Sources & coverage tab (_sources_section); Methodology
    # keeps how-to-read, the guides, and the caveats.
    return Section(
        id="methodology", title="Methodology & caveats",
        kind="reference",
        intro="How to read these figures: every value is a rolling 12-month "
              "total compared with the prior 12 months unless stated, and "
              "every line ends in its finding/N citation token — drillable "
              "back to the underlying source rows.",
        about=_ABOUT["methodology"],
        metrics={"caveats": caveats, "guides": _METHOD_GUIDES},
    )


_FINDING_FAMILIES = [
    ("hs_group_trajectory", "Trajectory shapes"),
    ("hs_group_yoy", "HS-group year-on-year (price & volume)"),
    ("mirror_gap", "Mirror-trade gaps"),
    ("partner_share", "China import/export shares"),
    ("trade_balance", "Trade-balance (deficit) series"),
    ("gacc_bilateral", "GACC bilateral (per-country)"),
    ("gacc_aggregate", "GACC bloc aggregates"),
]


def _finding_family(subkind: str) -> str:
    """Map a raw subkind to a readable family — so the manifest reads as
    journalism, not a dump of `hs_group_yoy_combined_export` strings."""
    for prefix, label in _FINDING_FAMILIES:
        if subkind.startswith(prefix):
            return label
    return "Other"


# The sources whose drops appear in the publication-calendar table, in
# display order. `track_landed` distinguishes sources we actually ingest
# (their cells can show "✓ published") from informational cadences
# (gacc_bulletin — we know when it publishes but don't fetch it, so its past
# cells just show the scheduled date, never a checkmark or an "awaited").
_PUBCAL_SOURCES: tuple[tuple[str, bool], ...] = (
    ("gacc", True),
    ("gacc_bulletin", False),
    ("eurostat", True),
    ("hmrc", True),
)


def _publication_calendar(cur, today: date | None = None) -> dict:
    """The 'estimated calendar' table for the Sources & coverage tab (Luke,
    2026-07-14): per reference month, when each source's figures are expected
    — official schedule dates where a published calendar covers the month
    (GACC 公告2025年第240号, Eurostat's G.3 calendar, uktradeinfo), our
    formula estimate otherwise — and whether they have already landed here.

    Window: 3 reference months back through 2 forward of the build date —
    wide enough that the row journalists care about (the month mid-drop) is
    always flanked by the just-confirmed and the upcoming ones.

    Cell statuses: 'landed' (release row exists; shows the actual date),
    'scheduled' (official date, in the future), 'estimated' (formula date,
    in the future), 'awaited' (expected date has passed with no release —
    only for ingested sources), 'folded' (GACC January cells: the data
    arrives inside the Jan–Feb combined release on February's row). GACC
    February cells carry combined=True so the renderer can label them
    'Jan–Feb'."""
    today = today or date.today()
    anchor = today.replace(day=1)
    refs = [release_calendar._add_months(anchor, k) for k in range(-3, 3)]

    cur.execute(
        """SELECT source, period,
                  MIN(COALESCE(publication_date, first_seen_at::date))
             FROM releases
            WHERE period = ANY(%s)
         GROUP BY source, period""",
        (refs,),
    )
    landed = {(s, p): d for s, p, d in cur.fetchall()}

    rows = []
    for ref in refs:
        cells: dict[str, dict] = {}
        for src, track_landed in _PUBCAL_SOURCES:
            detail = release_calendar.expected_publish_date_detail(src, ref)
            if detail is None:
                continue
            expected, official = detail
            is_gacc_kind = src in ("gacc", "gacc_bulletin")
            landed_date = landed.get((src, ref)) if track_landed else None
            if is_gacc_kind and ref.month == 1:
                status = "folded"
            elif landed_date is not None:
                status = "landed"
            elif expected > today:
                status = "scheduled" if official else "estimated"
            else:
                status = "awaited" if track_landed else (
                    "scheduled" if official else "estimated")
            cells[src] = {
                "expected": expected.isoformat(),
                "official": official,
                "landed": landed_date.isoformat() if landed_date else None,
                "status": status,
                "combined": is_gacc_kind and ref.month == 2,
            }
        rows.append({"period": ref.isoformat(), "cells": cells})
    return {"as_of": today.isoformat(), "rows": rows}


def _sources_section(cur, diff=None) -> Section:
    """The Sources & coverage tab: the data sources, how much of each we hold
    (period coverage), the publication calendar (when each source's next
    figures are expected and what they update), the per-type count of what’s
    new this cycle, and a readable manifest of what the pack contains. The
    Trade Map (structural) renders in the same tab — together they answer
    'what the briefing rests on and how completely it covers the ground'
    (principle 7, given its own home)."""
    cur.execute("SELECT DISTINCT source FROM releases ORDER BY source")
    sources = [{"source": s, "note": _SOURCE_NOTES.get(s, "")}
               for (s,) in cur.fetchall()]
    # `last_updated` = the most recent fetch (max first_seen_at) per source — the
    # freshness signal beside the period range ("covered through X, last pulled Y").
    cur.execute("""SELECT source, min(period), max(period), count(*), max(first_seen_at)
                     FROM releases GROUP BY source ORDER BY source""")
    coverage = [{"source": s,
                 "start": a.isoformat() if a else None,
                 "end": b.isoformat() if b else None,
                 "releases": n,
                 "last_updated": u.date().isoformat() if hasattr(u, "date") else None}
                for s, a, b, n, u in cur.fetchall()]
    cur.execute("""SELECT subkind, count(*) FROM findings
                    WHERE superseded_at IS NULL GROUP BY subkind""")
    fam: dict[str, int] = defaultdict(int)
    total = 0
    for sk, n in cur.fetchall():
        total += n
        fam[_finding_family(sk)] += n
    manifest = sorted(({"family": k, "count": v} for k, v in fam.items()),
                      key=lambda x: -x["count"])
    # Per-source provenance appendix: the most recent releases with their
    # third-party URL + fetch date — the drill-back trail, behind a per-source
    # expander (capped; the full set is in the DB).
    cur.execute("""SELECT source, period, title, source_url, first_seen_at
                     FROM releases ORDER BY source, period DESC, id DESC""")
    recent: dict[str, list] = defaultdict(list)
    for src, per, title, url, seen in cur.fetchall():
        if len(recent[src]) >= 12:
            continue
        recent[src].append({
            "period": per.isoformat() if per else None,
            "title": title, "url": url,
            "fetched": seen.date().isoformat() if hasattr(seen, "date") else None,
        })
    cov_total = {c["source"]: c["releases"] for c in coverage}
    appendix = [{"source": s, "total": cov_total.get(s, len(rs)), "recent": rs}
                for s, rs in recent.items()]
    # New findings this cycle, by type (the Findings-doc "N new — <type>" list) —
    # a coverage fact, so it lives here rather than in 'What changed'.
    new_findings = [
        {"subkind": sk, "label": _subkind_plain_label(sk), "count": n}
        for sk, n in ((diff.new_by_subkind or []) if diff is not None else [])
    ]
    # Reference & classification lookups — externally-sourced static data the
    # briefing leans on, distinct from the trade-data releases above. Disclosed
    # for full provenance (principle 7). The CN product-description row only
    # appears when those descriptions are actually loaded.
    reference_sources = [
        {"name": "CN → SITC sector classification",
         "note": "UN SITC Rev. 4 correspondence (UNSD) mapping each 8-digit "
                 "Combined Nomenclature code to a sector division — the "
                 "structural spine for the Trade Map and sector navigation.",
         "url": "https://unstats.un.org/unsd/classifications/Econ/"},
        {"name": "CN → BEC end-use classification",
         "note": "UN BEC Rev. 4 correspondence (UNSD) classifying codes by "
                 "broad economic end-use (capital / intermediate / consumption "
                 "/ fuel).",
         "url": "https://unstats.un.org/unsd/classifications/Econ/"},
    ]
    if classifications.cn8_description_lookup():
        reference_sources.insert(0, {
            "name": f"CN product descriptions (CN {classifications.CN_DESC_YEAR})",
            "note": "Combined Nomenclature self-explanatory texts (Eurostat; "
                    "Commission Implementing Regulation (EU) 2024/2522), taken "
                    "via the Hungarian KSH tabular mirror and cross-validated "
                    "against the EU primary SKOS/RDF. Source of the plain-language "
                    "product labels and the full hover definitions on 'Top "
                    "products'.",
            "url": "https://data.europa.eu/data/datasets/"
                   "combined-nomenclature-2025"})
    return Section(
        id="sources", title="Sources & coverage", kind="sources",
        intro="What this briefing rests on — the data sources, how much of each "
              "we hold, and what the pack contains.",
        metrics={"sources": sources, "coverage": coverage,
                 "publication_calendar": _publication_calendar(cur),
                 "reference_sources": reference_sources,
                 "new_findings": new_findings,
                 "new_findings_total": sum(f["count"] for f in new_findings),
                 "manifest": manifest, "manifest_total": total,
                 "appendix": appendix},
    )


def _bal_yoy(cur, prior):
    """YoY change of a *balance* (a signed net), returned as (pct, low_base).
    A ratio on a net is unstable two ways the portal’s normal low-base guard
    doesn’t cover: a near-zero prior makes it explode, and a sign flip
    (surplus→deficit) makes the % meaningless. In both cases we suppress the %
    and let the renderer quote the € swing instead — same philosophy as the
    low-base flag on the flow rows."""
    if cur is None or prior is None or prior == 0:
        return None, True
    if (cur >= 0) != (prior >= 0):          # surplus⇄deficit flip — % is nonsense
        return None, True
    # On the *magnitude* shown, so +% always means the stated surplus/deficit
    # widened (whichever side); a signed-net % would render a growing deficit as
    # a negative — the opposite of how a reader parses it.
    pct = (abs(cur) - abs(prior)) / abs(prior)
    if abs(pct) >= 5:                        # >500% swing — prior was tiny
        return None, True
    return pct, False


def _partner_balance(flows: dict) -> dict:
    """China’s net balance with a partner (exports − imports; positive = China
    surplus, i.e. the partner’s deficit), on the same rolling-12-month and YTD
    windows as the per-flow rows — so the balance never drifts onto a different
    clock from the lines it’s derived from. Empty dict when either flow is
    missing (can’t net one side)."""
    exp, imp = flows.get("export"), flows.get("import")
    if not exp or not imp:
        return {}

    def _net(a, b):
        return (a - b) if a is not None and b is not None else None

    out: dict = {}
    c12, p12 = _net(exp.get("cur12"), imp.get("cur12")), \
        _net(exp.get("prior12"), imp.get("prior12"))
    if c12 is None:
        return {}
    pct, low = _bal_yoy(c12, p12)
    out.update(bal_eur=c12, bal_yoy_pct=pct, bal_low_base=low,
               bal_delta_eur=(c12 - p12) if p12 is not None else None)

    yc, yp = _net(exp.get("ytd_cur"), imp.get("ytd_cur")), \
        _net(exp.get("ytd_prior"), imp.get("ytd_prior"))
    if yc is not None:
        ypct, ylow = _bal_yoy(yc, yp)
        out.update(bal_ytd_eur=yc, bal_ytd_pct=ypct, bal_ytd_low_base=ylow,
                   bal_ytd_delta_eur=(yc - yp) if yp is not None else None,
                   bal_ytd_months=exp.get("ytd_months") or imp.get("ytd_months"))
    return out


# The six regions plotted on the per-partner annual charts, in plot order, and
# the Guardian-ish line palette (one colour per region, index-aligned). The US
# is one logical line but two GACC labels over time — "United States" (2019) and
# "United States (US)" (2020-03 on) — so its entry carries BOTH labels to union.
_PARTNER_CHART_REGIONS: list[tuple[str, list[str]]] = [
    ("ASEAN", ["ASEAN"]),
    ("European Union", ["European Union"]),
    ("United States", ["United States", "United States (US)"]),
    ("Africa", ["Africa"]),
    ("Latin America", ["Latin America"]),
    ("Russian Federation", ["Russian Federation"]),
]
_PARTNER_CHART_COLORS = [
    "#052962", "#c70000", "#22874d", "#0077b6", "#b08800", "#7d4cdb",
]


def _gacc_annual_by_region(labels: list[str], flow: str) -> dict[int, float]:
    """{year: annual_eur} for one region (one logical line) and one flow,
    from GACC’s *year-to-date cumulative* observations.

    Each GACC release carries the running total of the calendar year so far,
    so the LATEST period within a year is that year’s annual EUR — December for
    a complete year, the latest published month for the current (partial) one.
    `labels` is usually one GACC partner label; the US passes two (the label
    changed in 2020) and we union them — the year ranges don’t overlap, but if
    they ever did we keep the value from the later period (a fuller cumulative).

    Reuses anomalies._gacc_aggregate_per_period_totals (canonical CNY releases,
    EUR conversion via parse_unit_scale + lookup_fx) — no currency logic here.
    That helper opens its own connection per call; a handful of calls per
    snapshot build is acceptable."""
    # year -> (latest_period_seen, eur_at_that_period)
    best: dict[int, tuple[date, float]] = {}
    for label in labels:
        for period, eur, _ids in anomalies._gacc_aggregate_per_period_totals(
            label, flow=flow, period_kind="ytd",
        ):
            yr = period.year
            cur_best = best.get(yr)
            if cur_best is None or period > cur_best[0]:
                best[yr] = (period, eur)
    return {yr: eur for yr, (_p, eur) in best.items()}


def _gacc_annual_latest_period_by_year(labels: list[str], flow: str) -> dict[int, date]:
    """{year: latest_period} companion to `_gacc_annual_by_region` — the period
    each year’s annual figure was taken at, so the caller can tell which year is
    partial (latest period not December)."""
    out: dict[int, date] = {}
    for label in labels:
        for period, _eur, _ids in anomalies._gacc_aggregate_per_period_totals(
            label, flow=flow, period_kind="ytd",
        ):
            cur_best = out.get(period.year)
            if cur_best is None or period > cur_best:
                out[period.year] = period
    return out


def _gacc_partner_charts() -> list[dict]:
    """Three multi-line annual charts — China’s exports, imports and balance per
    region (one line per region) — as plain JSON-serialisable dicts attached to
    the gacc_bilateral root Section’s `metrics` (so they travel in the portal
    snapshot). Annual EUR per region per flow comes from GACC’s YTD-cumulative
    observations (latest period in each calendar year = that year’s total).

    Balance = annual_exports − annual_imports per year (only where both exist;
    it can be negative — a deficit). `partial_last_year` flags the latest year
    whose latest period isn’t December, so the renderer can mark it as not a
    full year (and a reader can’t misread a partial year as a real drop)."""
    # Per region: {flow: {year: eur}} plus the latest period per year (to detect
    # the partial year — same across flows in practice, but we union to be safe).
    exports: dict[str, dict[int, float]] = {}
    imports: dict[str, dict[int, float]] = {}
    latest_period_by_year: dict[int, date] = {}
    for name, lbls in _PARTNER_CHART_REGIONS:
        exports[name] = _gacc_annual_by_region(lbls, "export")
        imports[name] = _gacc_annual_by_region(lbls, "import")
        for src in (
            _gacc_annual_latest_period_by_year(lbls, "export"),
            _gacc_annual_latest_period_by_year(lbls, "import"),
        ):
            for yr, p in src.items():
                cur_best = latest_period_by_year.get(yr)
                if cur_best is None or p > cur_best:
                    latest_period_by_year[yr] = p

    years = sorted(latest_period_by_year)
    if not years:
        return []
    # The latest year whose latest period isn't December is partial. (Earlier
    # years missing December would be data gaps, not the live partial year; the
    # live partial is by definition the most recent.)
    partial_last_year = None
    for yr in reversed(years):
        if latest_period_by_year[yr].month != 12:
            partial_last_year = yr
            break

    def _series(per_region: dict[str, dict[int, float]]) -> list[dict]:
        return [
            {"name": name,
             "values": [per_region[name].get(yr) for yr in years]}
            for name, _lbls in _PARTNER_CHART_REGIONS
        ]

    balance: dict[str, dict[int, float]] = {}
    for name, _lbls in _PARTNER_CHART_REGIONS:
        exp_y, imp_y = exports[name], imports[name]
        balance[name] = {
            yr: exp_y[yr] - imp_y[yr]
            for yr in years if yr in exp_y and yr in imp_y
        }

    return [
        {"metric": "exports",
         "title": "China’s exports by region (annual)",
         "years": years, "partial_last_year": partial_last_year,
         "series": _series(exports), "colors": _PARTNER_CHART_COLORS},
        {"metric": "imports",
         "title": "China’s imports by region (annual)",
         "years": years, "partial_last_year": partial_last_year,
         "series": _series(imports), "colors": _PARTNER_CHART_COLORS},
        {"metric": "balance",
         "title": "China’s balance by region (annual, exports − imports)",
         "years": years, "partial_last_year": partial_last_year,
         "series": _series(balance), "colors": _PARTNER_CHART_COLORS},
    ]


def _gacc_bilateral_section(cur, period) -> Section:
    """The GACC variant’s deeper layer: China’s own reported trade with each
    of its ~24 named partner countries (both flows), under the bloc-level
    macro lead."""
    root = Section(
        id="gacc-bilateral", title="China’s trade by country (GACC)",
        kind="gacc_bilateral",
        intro="China’s own reported exports and imports by country, "
              "rolling 12 months — the per-country detail under the bloc lead.",
        about=_ABOUT["gacc-bilateral"],
    )
    if period is None:
        return root
    # Three annual per-region trend charts (exports / imports / balance), carried
    # as plain dicts on the root section's metrics so they JSON-serialise into the
    # portal snapshot and render at the top of this section.
    charts = _gacc_partner_charts()
    if charts:
        root.metrics["partner_charts"] = charts
    cur.execute(
        """SELECT id, subkind, detail FROM findings WHERE superseded_at IS NULL
            AND subkind IN ('gacc_bilateral_aggregate_yoy',
                            'gacc_bilateral_aggregate_yoy_import')
            AND (detail->'windows'->>'current_end')::date = %s""",
        (period,),
    )
    by_partner: dict[str, dict] = {}
    for fid, subkind, detail in cur.fetchall():
        pmeta = (detail or {}).get("partner") or {}
        partner = pmeta.get("raw_label") or "?"
        is_export = not subkind.endswith("_import")
        tot = (detail or {}).get("totals", {})
        ctx = _bilateral_context(detail)
        finding = Finding(
            finding_id=fid, subkind=subkind,
            title=(f"China {'exports to' if is_export else 'imports from'} {partner}"),
            metrics={"scope": "China", "flow": "export" if is_export else "import",
                     "yoy_pct": _f(tot.get("yoy_pct")),
                     "current_eur": _f(tot.get("current_12mo_eur")),
                     "caveats": _visible_caveats((detail or {}).get("caveat_codes")),
                     **ctx},
            chart_data=_series_chart((detail or {}).get("monthly_series")),
            provenance=Provenance(finding_ids=[fid], source="gacc", as_of=period),
        )
        p = by_partner.setdefault(partner, {"max_eur": 0.0, "findings": [],
                                            "flows": {},
                                            # The EU bloc's expander defaults
                                            # OPEN (Luke, 2026-07-05): one
                                            # unfolded profile signals what
                                            # every collapsed row contains.
                                            # Structural marker (kind), never
                                            # the label spelling.
                                            "default_open":
                                                pmeta.get("kind") == "eu_bloc"})
        p["findings"].append(finding)
        p["max_eur"] = max(p["max_eur"], finding.metrics["current_eur"] or 0.0)
        # Stash the raw totals both flows need for a netted balance — the priors
        # the per-flow finding metrics drop. Keyed by flow so export−import nets.
        ytd = (tot.get("ytd_cumulative") or {})
        p["flows"]["export" if is_export else "import"] = {
            "cur12": _f(tot.get("current_12mo_eur")),
            "prior12": _f(tot.get("prior_12mo_eur")),
            "ytd_cur": _f(ytd.get("current_eur")),
            "ytd_prior": _f(ytd.get("prior_eur")),
            "ytd_months": ytd.get("months_in_ytd"),
        }
    for name, p in sorted(by_partner.items(), key=lambda kv: -kv[1]["max_eur"]):
        fs = sorted(p["findings"], key=lambda f: f.metrics["flow"])
        metrics = _partner_balance(p["flows"])
        if p.get("default_open"):
            metrics["default_open"] = True
        root.sections.append(Section(
            id="gacc-" + _slugify_heading(name), title=name,
            kind="gacc_bilateral", findings=fs,
            facets=Facets(partner=[name]),
            metrics=metrics,
        ))
    return root


# =============================================================================
# The GACC-only update page (dev_notes/2026-07-05-gacc-update-page-design.md)
# =============================================================================
# The second release track's tab. Everything below derives from live GACC
# findings, so BOTH cycles rebuild it identically — a main-track rebuild
# carries the GACC tab forward and vice versa; only LLM takes need the
# reuse-graft. China-perspective throughout; change-shaped (single-month YoY
# leads); mainland-customs-territory scope (no CN+HK+MO envelope — HK/MO are
# partners here).


def _fmt_month_abbr(d: date | None) -> str:
    """'May 2026' — the portal's abbreviated month convention, used for the
    period-explicit tab labels. Year always present (Dec/Jan ambiguity)."""
    return f"{d:%b %Y}" if d is not None else ""


def _fmt_signed_pct(yoy: float | None) -> str:
    return f"{yoy * 100:+.1f}%" if yoy is not None else "—"


class _GaccRow:
    """One live GACC finding at the page's anchor, flattened for selection."""
    __slots__ = ("fid", "family", "label", "kind", "iso2", "flow", "detail",
                 "totals", "monthly_series", "caveat_codes")

    def __init__(self, fid, family, label, kind, iso2, flow, detail):
        self.fid = fid
        self.family = family        # 'aggregate' | 'bilateral'
        self.label = label          # partner / bloc raw_label
        self.kind = kind            # eu_bloc | single_country | asean | ... | world
        self.iso2 = iso2            # single-country partners only
        self.flow = flow            # 'export' | 'import'
        self.detail = detail or {}  # full finding detail (windows etc.)
        self.totals = (detail or {}).get("totals", {}) or {}
        self.monthly_series = (detail or {}).get("monthly_series")
        self.caveat_codes = (detail or {}).get("caveat_codes") or []

    @property
    def sm_yoy(self) -> float | None:
        sm = self.totals.get("single_month") or {}
        return _f(sm.get("yoy_pct"))

    @property
    def sm_eur(self) -> float | None:
        sm = self.totals.get("single_month") or {}
        return _f(sm.get("current_eur"))

    @property
    def ytd_yoy(self) -> float | None:
        ytd = self.totals.get("ytd_cumulative") or {}
        return _f(ytd.get("yoy_pct"))

    @property
    def rolling_yoy(self) -> float | None:
        return _f(self.totals.get("yoy_pct"))

    @property
    def rolling_eur(self) -> float | None:
        return _f(self.totals.get("current_12mo_eur"))


_GACC_PAGE_SUBKINDS = (
    "gacc_aggregate_yoy", "gacc_aggregate_yoy_import",
    "gacc_bilateral_aggregate_yoy", "gacc_bilateral_aggregate_yoy_import",
)


def _gacc_rows_at(cur, period: date) -> list[_GaccRow]:
    """Every live GACC finding anchored at `period`, with the partner's iso2
    joined in (single-country bilaterals only — how the page tells EU members
    and the US/UK/HK apart without hardcoding GACC's label spellings)."""
    cur.execute(
        """SELECT f.id, f.subkind, f.detail, ca.iso2
             FROM findings f
        LEFT JOIN country_aliases ca
               ON ca.id = COALESCE(f.detail->'partner'->>'alias_id',
                                   f.detail->'aggregate'->>'alias_id')::bigint
            WHERE f.superseded_at IS NULL
              AND f.subkind = ANY(%s)
              AND (f.detail->'windows'->>'current_end')::date = %s""",
        (list(_GACC_PAGE_SUBKINDS), period),
    )
    rows: list[_GaccRow] = []
    for fid, subkind, detail, iso2 in cur.fetchall():
        detail = detail or {}
        family = "bilateral" if subkind.startswith("gacc_bilateral") else "aggregate"
        ent = detail.get("partner") if family == "bilateral" else detail.get("aggregate")
        ent = ent or {}
        rows.append(_GaccRow(
            fid=fid, family=family,
            label=ent.get("raw_label") or "?",
            kind=ent.get("kind") or "single_country",
            iso2=iso2,
            flow="import" if subkind.endswith("_import") else "export",
            detail=detail,
        ))
    return rows


# The context strip: China's exports to EU · US · ASEAN · World — the
# "general surge or EU-specific re-routing?" instrument (the page's endorsed
# framing made into furniture). Selection is structural (kind/iso2), never by
# GACC's label spellings.
_GACC_STRIP_SLOTS: list[tuple[str, str, str]] = [
    # (key, kicker, display name used in the label line)
    ("gacc_strip_eu", "CHINA → EU", "the EU"),
    ("gacc_strip_us", "CHINA → US", "the US"),
    ("gacc_strip_asean", "CHINA → ASEAN", "ASEAN"),
    ("gacc_strip_world", "CHINA → WORLD", "the world"),
]


def _strip_pick(rows: list[_GaccRow], slot_key: str) -> _GaccRow | None:
    exports = [r for r in rows if r.flow == "export"]
    if slot_key == "gacc_strip_eu":
        cands = [r for r in exports if r.family == "bilateral" and r.kind == "eu_bloc"]
    elif slot_key == "gacc_strip_us":
        cands = [r for r in exports if r.family == "bilateral" and r.iso2 == "US"]
    elif slot_key == "gacc_strip_asean":
        cands = [r for r in exports if r.family == "aggregate" and r.kind == "asean"]
    else:  # world
        cands = [r for r in exports if r.family == "aggregate" and r.kind == "world"]
    return cands[0] if cands else None


def _gacc_strip_cards(rows: list[_GaccRow], period: date) -> list[Indicator]:
    cards: list[Indicator] = []
    for key, kicker, display in _GACC_STRIP_SLOTS:
        r = _strip_pick(rows, key)
        if r is None:
            continue
        # Single-month YoY is the face (this page is about the fresh month);
        # fall back to the rolling figure — clearly labelled — when the
        # comparable prior month is missing (GACC's Jan–Feb combined gap).
        face = r.sm_yoy if r.sm_yoy is not None else r.rolling_yoy
        if face is None:
            continue
        basis_note = (
            f"{_fmt_month_abbr(period)} vs same month last year"
            if r.sm_yoy is not None else
            "12-month basis (no comparable single month)"
        )
        detail_bits = []
        if r.sm_eur is not None:
            detail_bits.append(f"{_fmt_eur(r.sm_eur)} in the month")
        if r.ytd_yoy is not None:
            detail_bits.append(f"YTD {_fmt_signed_pct(r.ytd_yoy)}")
        if r.rolling_yoy is not None:
            detail_bits.append(f"12mo {_fmt_signed_pct(r.rolling_yoy)}")
        cards.append(Indicator(
            key=key, kicker=kicker,
            label=f"China’s exports to {display}",
            value=face, unit="yoy_pct",
            formatted=_fmt_signed_pct(face),
            note=" · ".join([basis_note] + detail_bits) or None,
            chart="bignumber",
            provenance=Provenance(finding_ids=[r.fid], source="gacc", as_of=period),
        ))
    return cards


def _gacc_standout(rows: list[_GaccRow], period: date) -> HeadlineItem | None:
    """The sharpest single-month move anywhere in the release — partner-
    agnostic (anti-fixation), but the world Total is excluded (that's the
    wire headline, not our differentiation) and small bilateral partners are
    size-floored so a tiny denominator can't take the standout slot."""
    SIZE_FLOOR_EUR = 10e9  # 12-month value; keeps micro-partners out
    cands = [
        r for r in rows
        if r.kind != "world"
        and (r.family == "aggregate"
             or (r.rolling_eur is not None and r.rolling_eur >= SIZE_FLOOR_EUR))
    ]
    with_sm = [r for r in cands if r.sm_yoy is not None]
    pool = with_sm or [r for r in cands if r.rolling_yoy is not None]
    if not pool:
        return None
    best = max(pool, key=lambda r: abs(r.sm_yoy if r.sm_yoy is not None
                                       else r.rolling_yoy))
    yoy = best.sm_yoy if best.sm_yoy is not None else best.rolling_yoy
    basis = ("in " + _fmt_month(period) if best.sm_yoy is not None
             else f"in the 12 months to {_fmt_month(period)}")
    direction = "rose" if yoy > 0 else "fell"
    flow_phrase = ("exports to" if best.flow == "export" else "imports from")
    prose = (
        f"**China’s {flow_phrase} {best.label}** {direction} "
        f"{abs(yoy) * 100:.1f}% year-on-year {basis} — the sharpest move in "
        f"this GACC release."
    )
    if best.sm_yoy is not None and best.rolling_yoy is not None:
        prose += f" 12-month trend: {_fmt_signed_pct(best.rolling_yoy)}."
    prose += f" `finding/{best.fid}`"
    return HeadlineItem(
        subject={"scope": "china", "flow": best.flow, "group_name": best.label},
        metrics={"direction": direction, "pct": abs(yoy),
                 "value_eur": best.sm_eur if best.sm_yoy is not None else best.rolling_eur},
        stability={"badge": None, "hedge_phrase": None},
        prose=prose,
        provenance=Provenance(finding_ids=[best.fid], source="gacc", as_of=period),
        facets=Facets(partner=[best.label]),
    )


def _gacc_partner_section(label: str, slug_prefix: str,
                          flows_rows: list[_GaccRow], period: date,
                          default_open: bool = False,
                          hub: bool = False) -> Section:
    """One partner subsection in the _gacc_bilateral_section shape (same
    Finding metrics + netted-balance metrics), so the existing renderer
    handles it unchanged. `default_open` renders the expander unfolded —
    the EU bloc leads that way (Luke, 2026-07-05: one open profile signals
    what every collapsed row contains)."""
    findings: list[Finding] = []
    flows: dict[str, dict] = {}
    for r in sorted(flows_rows, key=lambda r: r.flow):
        ytd = r.totals.get("ytd_cumulative") or {}
        findings.append(Finding(
            finding_id=r.fid, subkind=f"gacc_bilateral_aggregate_yoy"
                                      f"{'' if r.flow == 'export' else '_import'}",
            title=(f"China {'exports to' if r.flow == 'export' else 'imports from'} "
                   f"{label}"),
            metrics={"scope": "China", "flow": r.flow,
                     "yoy_pct": r.rolling_yoy,
                     "current_eur": r.rolling_eur,
                     "caveats": _visible_caveats(r.caveat_codes),
                     **_bilateral_context(r.detail)},
            chart_data=_series_chart(r.monthly_series),
            provenance=Provenance(finding_ids=[r.fid], source="gacc", as_of=period),
        ))
        flows[r.flow] = {
            "cur12": r.rolling_eur,
            "prior12": _f(r.totals.get("prior_12mo_eur")),
            "ytd_cur": _f(ytd.get("current_eur")),
            "ytd_prior": _f(ytd.get("prior_eur")),
            "ytd_months": ytd.get("months_in_ytd"),
        }
    metrics = _partner_balance(flows)
    if default_open:
        metrics["default_open"] = True
    if hub:
        # Entrepôt routing hub (HK/MO — separate customs regimes): the row
        # renders a visible flag + an explainer, so a routing spike is
        # never read as that partner's own demand. Structural (iso2), set
        # by the caller — never GACC's label spellings.
        metrics["is_hub"] = True
    return Section(
        id=slug_prefix + _slugify_heading(label), title=label,
        kind="gacc_bilateral", findings=findings,
        facets=Facets(partner=[label]),
        metrics=metrics,
    )


def _gacc_by_country_section(rows: list[_GaccRow], period: date) -> Section | None:
    """By country: every named GACC partner, both flows, in two groups —
    Europe first (GACC's EU aggregate, then each member state it names,
    plus the UK), then the rest of the world; biggest first within each
    group, the EU bloc's expander open by default.

    Consolidates the old Europe-up-close section with the Full briefing's
    former China-by-country section (2026-07-15 ruling: surfaces are
    single-period — the briefing carries no floating GACC content, and ALL
    per-country GACC detail lives here at this page's period)."""
    eu_iso = eurostat.EU27_PARTNER_CODES | {"GB"}
    eu_bloc: list[_GaccRow] = []
    europe: dict[str, list[_GaccRow]] = {}
    world: dict[str, list[_GaccRow]] = {}
    for r in rows:
        if r.family != "bilateral":
            continue
        if r.kind == "eu_bloc":
            eu_bloc.append(r)
        elif r.iso2 in eu_iso:
            europe.setdefault(r.label, []).append(r)
        else:
            world.setdefault(r.label, []).append(r)
    if not (eu_bloc or europe or world):
        return None

    def scale(pair: list[_GaccRow]) -> float:
        # Biggest-first = the larger flow's rolling-12mo value (a roster
        # ordering, predictable across editions; the change-shaped reads
        # live in the strip / standout / since-last above).
        return max((r.rolling_eur or 0.0) for r in pair)

    root = Section(
        id="gacc-by-country", title="By country",
        kind="gacc_bilateral",
        intro="China’s own reported trade with every partner it names, "
              "both flows — Europe first, then the rest of the world.",
        about=_GACC_PAGE_ABOUT_BY_COUNTRY,
    )
    root.metrics["grouped"] = True
    grp_eu = Section(
        id="gacc-bycountry-europe", title="Europe",
        kind="gacc_partner_group",
        intro="The EU aggregate, each member state GACC names, and the UK. "
              "“Imports from” here is the China-side read on European "
              "exporters’ China demand.",
    )
    if eu_bloc:
        grp_eu.sections.append(_gacc_partner_section(
            eu_bloc[0].label, "gaccpage-", eu_bloc, period,
            default_open=True))
    for label, pair in sorted(europe.items(), key=lambda kv: -scale(kv[1])):
        grp_eu.sections.append(_gacc_partner_section(label, "gaccpage-", pair, period))
    grp_row = Section(
        id="gacc-bycountry-world", title="Rest of the world",
        kind="gacc_partner_group",
    )
    for label, pair in sorted(world.items(), key=lambda kv: -scale(kv[1])):
        grp_row.sections.append(_gacc_partner_section(
            label, "gaccpage-", pair, period,
            hub=any(r.iso2 in ("HK", "MO") for r in pair)))
    root.sections = [g for g in (grp_eu, grp_row) if g.sections]
    return root


_GACC_PAGE_ABOUT_WORLD = (
    "**Why these regions.** The table and charts carry every region-level "
    "aggregate GACC’s preliminary release publishes, plus two named-country "
    "majors (the US and Russia). The cast is GACC’s, not an editorial "
    "selection — nothing is filtered out.\n\n"
    "**Why there is no Middle East line.** GACC’s preliminary release "
    "publishes no Middle East aggregate and names no Middle East partner — "
    "no Saudi Arabia, no UAE — so China–Gulf trade sits unseparated inside "
    "the Belt & Road row and the world total. That is China’s reporting "
    "choice, not a gap on our side. Splitting it out needs a source that "
    "itemises those partners — GACC’s fuller monthly publications or "
    "partner-side data such as UN Comtrade — which is on the roadmap, not "
    "in this release."
)

# The single-country majors shown in the world view alongside the blocs:
# US (indispensable to the re-routing frame the section serves) and Russia
# (sanctions-era China–Russia trade is a standing story). With these two,
# the section's cast equals the region charts' cast exactly — one coherent
# region tier. The UK and the HK entrepôt line were removed 2026-07-15
# (Luke): both now live one scroll down in the By-country roster, where
# HK carries the entrepôt treatment. iso2 → compact glyph label; extend
# here when another major earns a line. NB: no Middle East entry is
# POSSIBLE from this source — GACC's preliminary release carries no such
# aggregate and names no Middle East partners; that trade sits
# unseparated inside Belt & Road and the world Total (the "more partner
# countries" breadth item on the roadmap is the route to it).
_GACC_WORLD_MAJORS: dict[str, str] = {
    "US": "US",
    "RU": "Russia",
}


def _gacc_world_section(rows: list[_GaccRow], period: date) -> Section | None:
    """China and the world: the named blocs + the world total, both flows —
    the interpretive context around the Europe read (re-routing shows up
    here) — plus the EU bloc, the US and Russia (_GACC_WORLD_MAJORS), so
    the cast equals the region charts' cast exactly. The UK and the Hong
    Kong entrepôt line moved to the By-country roster below (2026-07-15):
    single countries with no region-tier job belong in the country tier,
    and HK's entrepôt flag rides its row there."""
    ents: dict[str, dict] = {}
    for r in rows:
        is_major = (r.family == "bilateral"
                    and (r.kind == "eu_bloc" or r.iso2 in _GACC_WORLD_MAJORS))
        if r.family != "aggregate" and not is_major:
            continue
        # Compact label for the scale glyphs (the table keeps the full
        # name; the glyph tooltips too). Mapped structurally — iso2 /
        # kind, never GACC's label spellings.
        if r.kind == "eu_bloc":
            short = "EU"
        elif r.iso2 in _GACC_WORLD_MAJORS:
            short = _GACC_WORLD_MAJORS[r.iso2]
        else:
            short = r.label
        e = ents.setdefault(r.label, {
            "label": r.label, "short_label": short, "kind": r.kind,
            "flows": {},
        })
        e["flows"][r.flow] = {
            "sm_yoy": r.sm_yoy, "ytd_yoy": r.ytd_yoy,
            "rolling_yoy": r.rolling_yoy, "rolling_eur": r.rolling_eur,
            "finding_id": r.fid,
        }
    if not ents:
        return None

    def order(e: dict):
        # World total first, then blocs + majors by size.
        exp = (e["flows"].get("export") or {}).get("rolling_eur") or 0.0
        return (0 if e["kind"] == "world" else 1, -exp)

    root = Section(
        id="gacc-world", title="China and the world",
        kind="gacc_world",
        intro="The context that makes the Europe numbers readable: is an EU "
              "move part of a general surge in China’s trade, or "
              "specific to Europe? The EU appears here at bloc level, "
              "alongside the US and Russia — the per-country detail, "
              "including the UK and the Hong Kong entrepôt line, follows "
              "in By country below.",
        about=_GACC_PAGE_ABOUT_WORLD,
    )
    root.metrics["rows"] = sorted(ents.values(), key=order)
    root.metrics["period"] = period.isoformat()
    # The annual per-region trend charts (exports / imports / balance) —
    # region-tier data, so they live with the region-tier table (moved here
    # 2026-07-15 from the Full briefing's retired China-by-country section).
    charts = _gacc_partner_charts()
    if charts:
        root.metrics["region_charts"] = charts
    return root


def _gacc_since_last(cur, rows: list[_GaccRow], period: date) -> dict:
    """The within-track delta: which year-on-year readings swung most vs the
    previous GACC month. Single-month basis where both months carry it,
    12-month basis otherwise. Empty rows (with prev_period=None) when the
    previous anchor has no findings — e.g. across GACC’s Jan–Feb gap."""
    prev_period = _months_back(period, 1)
    prev = {(r.label, r.flow): r for r in _gacc_rows_at(cur, prev_period)}
    if not prev:
        return {"prev_period": None, "rows": []}
    swings = []
    for r in rows:
        p = prev.get((r.label, r.flow))
        if p is None:
            continue
        if r.sm_yoy is not None and p.sm_yoy is not None:
            delta, basis, cur_v, prev_v = (
                r.sm_yoy - p.sm_yoy, "single-month", r.sm_yoy, p.sm_yoy)
        elif r.rolling_yoy is not None and p.rolling_yoy is not None:
            delta, basis, cur_v, prev_v = (
                r.rolling_yoy - p.rolling_yoy, "12-month",
                r.rolling_yoy, p.rolling_yoy)
        else:
            continue
        swings.append({
            "label": r.label, "flow": r.flow, "basis": basis,
            "prev_yoy": prev_v, "cur_yoy": cur_v, "delta": delta,
            "finding_id": r.fid,
        })
    swings.sort(key=lambda s: -abs(s["delta"]))
    return {"prev_period": prev_period.isoformat(), "rows": swings[:6]}


# --- The commodity-highlights block (sections 5/6 catalogue) ----------------
# dev_notes/2026-07-14-gacc-commodity-highlights.md. China↔world totals from
# GACC's own ~30-commodity headline catalogue — the sector lens this page
# lacked (its country table has none). Positioned between "Since the last
# read" and the region/country sections (Luke, 2026-07-14): the movers are
# part of the month's momentum and set up the partner reads that follow.


class _GaccCommodityRow:
    """One live gacc_commodity_yoy finding at the page anchor, flattened."""
    __slots__ = ("fid", "label", "is_aggregate", "flow", "detail",
                 "sm", "ytd", "eur_month", "quantity_unit", "caveat_codes",
                 "monthly_series")

    def __init__(self, fid, subkind, detail):
        self.fid = fid
        self.detail = detail or {}
        com = self.detail.get("commodity") or {}
        totals = self.detail.get("totals") or {}
        self.label = com.get("label") or "?"
        self.is_aggregate = bool(com.get("is_aggregate"))
        self.flow = "import" if subkind.endswith("_import") else "export"
        self.sm = totals.get("single_month") or {}
        self.ytd = totals.get("ytd_cumulative") or {}
        self.eur_month = _f(totals.get("eur_month"))
        self.quantity_unit = com.get("quantity_unit")
        self.caveat_codes = self.detail.get("caveat_codes") or []
        self.monthly_series = self.detail.get("monthly_series")

    @property
    def sm_val_yoy(self) -> float | None:
        return _f(self.sm.get("value_yoy_pct"))

    @property
    def sm_qty_yoy(self) -> float | None:
        return _f(self.sm.get("quantity_yoy_pct"))


def _gacc_commodity_rows_at(cur, period: date) -> list[_GaccCommodityRow]:
    cur.execute(
        """SELECT id, subkind, detail FROM findings
            WHERE superseded_at IS NULL
              AND subkind IN ('gacc_commodity_yoy', 'gacc_commodity_yoy_import')
              AND (detail->'windows'->>'current_end')::date = %s""",
        (period,),
    )
    return [_GaccCommodityRow(fid, subkind, detail)
            for fid, subkind, detail in cur.fetchall()]


# Quantity units on the commodity pages carry their scale in the label
# ("10,000 Autos", "100 Million PCS"); absolute unit counts need it parsed
# out. Unmatched units scale 1 (Ton, Set, Ship, …).
_QTY_UNIT_SCALE_RE = re.compile(r"^\s*(?P<scale>10,000|100 Million)\s+(?P<noun>.+)$")
_QTY_UNIT_SCALES = {"10,000": 1e4, "100 Million": 1e8}


def _qty_units_absolute(qty: float, unit: str | None) -> tuple[float, str]:
    """(absolute unit count, noun) — e.g. (988_000, 'Autos') from
    (98.8, '10,000 Autos')."""
    if not unit:
        return qty, ""
    m = _QTY_UNIT_SCALE_RE.match(unit)
    if not m:
        return qty, unit
    return qty * _QTY_UNIT_SCALES[m.group("scale")], m.group("noun")


def _fmt_units(n: float) -> str:
    if n >= 1e9:
        return f"{n / 1e9:,.1f}bn"
    if n >= 1e6:
        return f"{n / 1e6:,.2f}mn"
    if n >= 1e3:
        return f"{n / 1e3:,.0f}k"
    return f"{n:,.0f}"


# The round-number ladder for milestone detection, in ABSOLUTE units:
# 1/2/5 × 10^k. A milestone fires when the anchor month crosses a rung no
# earlier month of the same catalogue line reached.
def _milestone_rungs(value: float):
    rungs = []
    k = 1.0
    while k <= value * 10:
        for m in (1.0, 2.0, 5.0):
            if m * k <= value:
                rungs.append(m * k)
        k *= 10
    return rungs


def _gacc_commodity_quantity_series(cur, flow: str, label: str,
                                    is_aggregate: bool) -> list[tuple[date, float]]:
    """Monthly quantity history for one catalogue line (canonical CNY,
    same label + aggregate flag — era-local by construction, so a milestone
    reads 'first in our records for this line', not 'first ever')."""
    section = 5 if flow == "export" else 6
    cur.execute(
        """SELECT r.period, o.quantity FROM observations o
             JOIN releases r ON r.id = o.release_id
            WHERE r.source='gacc' AND r.section_number=%s AND r.currency='CNY'
              AND o.flow=%s AND o.period_kind='monthly'
              AND o.commodity_label=%s AND o.quantity IS NOT NULL
              AND COALESCE((o.source_row->>'is_aggregate')::boolean, false) = %s
         ORDER BY r.period""",
        (section, flow, label, is_aggregate),
    )
    return [(p, float(q)) for p, q in cur.fetchall()]


def _gacc_commodity_facts(cur, r: _GaccCommodityRow, period: date) -> dict:
    """Code-computed milestone + run-rate facts for one shown commodity row —
    the deterministic layer of the 'takes' Luke asked for (2026-07-14): the
    1mn-cars-a-month class of milestone and the on-pace-for-10mn class of
    projection are arithmetic, so they are computed, never LLM-asserted.
    Returns {} when nothing fires; every fact carries its method note."""
    facts: dict = {}
    cur_qty = _f(r.sm.get("current_quantity"))
    if cur_qty is None or not r.quantity_unit:
        return facts
    series = _gacc_commodity_quantity_series(cur, r.flow, r.label,
                                             r.is_aggregate)
    prior = [q for p, q in series if p < period]
    # A "first month above N" claim needs a record worth being first in:
    # require ≥24 prior months of this catalogue line. Guards against
    # newly-added / recently-renamed lines (era-local series) where a
    # "milestone" would really mean "the line is four months old".
    if len(prior) >= 24:
        abs_cur, noun = _qty_units_absolute(cur_qty, r.quantity_unit)
        abs_prior_max = _qty_units_absolute(max(prior), r.quantity_unit)[0]
        crossed = [g for g in _milestone_rungs(abs_cur) if g > abs_prior_max]
        if crossed:
            rung = max(crossed)
            facts["milestone"] = {
                "text": (f"First month above {_fmt_units(rung)} "
                         f"{noun.lower() or 'units'} in our records "
                         f"(catalogue line tracked from "
                         f"{series[0][0].year})"),
                "threshold_units": rung,
                "month_units": abs_cur,
                "method": "round-number crossing vs all prior months of "
                          "this catalogue line (same label), monthly "
                          "quantity, canonical CNY releases",
            }
    # Run-rate: YTD × 12/m vs the prior year's full-year (Dec cumulative).
    # Month ≥ 3 only — a Jan/Feb-based extrapolation is noise.
    ytd_qty = _f(r.ytd.get("current_quantity"))
    if ytd_qty is not None and period.month >= 3:
        cur.execute(
            """SELECT o.quantity FROM observations o
                 JOIN releases r2 ON r2.id = o.release_id
                WHERE r2.source='gacc' AND r2.section_number=%s
                  AND r2.currency='CNY' AND r2.period=%s
                  AND o.flow=%s AND o.period_kind='ytd'
                  AND o.commodity_label=%s AND o.quantity IS NOT NULL""",
            (5 if r.flow == "export" else 6, date(period.year - 1, 12, 1),
             r.flow, r.label),
        )
        row = cur.fetchone()
        if row and row[0]:
            pace = ytd_qty * 12.0 / period.month
            abs_pace, noun = _qty_units_absolute(pace, r.quantity_unit)
            abs_full, _ = _qty_units_absolute(float(row[0]), r.quantity_unit)
            # Only a story when the pace is meaningfully different from
            # last year's total (±5% — inside that it's "flat").
            if abs_full > 0 and abs(abs_pace / abs_full - 1) >= 0.05:
                facts["run_rate"] = {
                    "text": (f"On pace for ~{_fmt_units(abs_pace)} "
                             f"{noun.lower() or 'units'} in {period.year} "
                             f"vs {_fmt_units(abs_full)} in "
                             f"{period.year - 1}"),
                    "pace_units": abs_pace,
                    "prior_full_year_units": abs_full,
                    "method": (f"linear run-rate: Jan–{period:%b} cumulative "
                               f"× 12/{period.month}, vs the prior year's "
                               f"December cumulative (same catalogue line)"),
                }
    return facts


# The KPI-card floor: cards are genuinely scarce space (2 per flow), so a
# selection criterion earns its keep there — biggest single-month movers
# among big lines (a €1bn month keeps small-line volatility off the cards;
# small lines are fully visible in the tables below).
_COMMODITY_CARD_FLOOR_EUR = 1e9
_COMMODITY_CARDS_PER_FLOW = 2


def _gacc_commodities_section(cur, period: date) -> Section | None:
    """The commodity block — the FULL catalogue (Luke, 2026-07-14: GACC
    already curated it to ~30 lines per flow; re-curating a curation hid
    cars at +33% because five lines beat it on percentage). Two per-flow
    tables, every leaf line, sorted by |single-month value YoY| so the
    block still reads movers-first; the three starred aggregates ride at
    the foot of each table as a set-off context group (never summed —
    non-adjacency membership). Rows whose single-month operator is
    unavailable (fresh catalogue lines, post-Jan-Feb anchors) sort after
    the rated rows rather than vanishing."""
    rows = _gacc_commodity_rows_at(cur, period)
    if not rows:
        return None

    def sort_key(r: _GaccCommodityRow):
        return (0, -abs(r.sm_val_yoy)) if r.sm_val_yoy is not None else (1, 0.0)

    def row_payload(r: _GaccCommodityRow) -> dict:
        facts = _gacc_commodity_facts(cur, r, period)
        out = {
            "label": r.label,
            "flow": r.flow,
            "finding_id": r.fid,
            "sm_value_yoy": r.sm_val_yoy,
            "sm_quantity_yoy": r.sm_qty_yoy,
            "eur_month": r.eur_month,
            "quantity_unit": r.quantity_unit,
            "ytd_value_yoy": _f(r.ytd.get("value_yoy_pct")),
            "published_ytd_yoy_pct": r.ytd.get("published_yoy_value_pct"),
            # cny_denominated is family-universal here — the table header
            # ("CNY terms") and the about-box carry it; repeating it as a
            # per-row chip is noise. Row chips are for row-varying caveats.
            "caveats": [c for c in _visible_caveats(r.caveat_codes)
                        if c != "cny_denominated"],
        }
        qty = _f(r.sm.get("current_quantity"))
        if qty is not None and r.quantity_unit:
            abs_qty, noun = _qty_units_absolute(qty, r.quantity_unit)
            out["quantity_display"] = f"{_fmt_units(abs_qty)} {noun.lower()}".strip()
        if facts:
            out["facts"] = facts
        return out

    root = Section(
        id="gacc-commodities",
        title="What’s moving — GACC’s headline commodities",
        kind="gacc_commodities",
        intro="China’s trade by product — GACC’s own headline catalogue, "
              "complete: every line it publishes, world totals, no country "
              "split (the EU-specific product read stays on the Full "
              "briefing). Sharpest single-month moves first; growth rates "
              "in CNY terms, GACC’s own comparison basis.",
        about=_GACC_COMMODITIES_ABOUT_MD,
    )
    for flow in ("export", "import"):
        leaves = sorted((r for r in rows
                         if r.flow == flow and not r.is_aggregate),
                        key=sort_key)
        aggs = sorted((r for r in rows if r.flow == flow and r.is_aggregate),
                      key=sort_key)
        root.metrics[f"{flow}_rows"] = [row_payload(r) for r in leaves]
        root.metrics[f"{flow}_aggregates"] = [row_payload(r) for r in aggs]
    root.metrics["period"] = period.isoformat()
    if not (root.metrics["export_rows"] or root.metrics["import_rows"]):
        return None
    return root


def _gacc_commodity_strip(cur, period: date) -> list[Indicator]:
    """The commodity KPI row (Luke, 2026-07-14): two key exports + two key
    imports as cards under the partner strip. Cards are scarce space, so
    the selection criterion the full-catalogue tables dropped earns its
    keep here: sharpest |single-month value YoY| among LEAF lines with a
    ≥€1bn month (small-line volatility stays in the tables, where nothing
    is hidden). Card face = the CNY-basis rate (marked); note carries the
    EUR level and the volume rate."""
    rows = _gacc_commodity_rows_at(cur, period)
    cards: list[Indicator] = []
    for flow in ("export", "import"):
        picks = sorted(
            (r for r in rows
             if r.flow == flow and not r.is_aggregate
             and r.sm_val_yoy is not None
             and r.eur_month is not None
             and r.eur_month >= _COMMODITY_CARD_FLOOR_EUR),
            key=lambda r: -abs(r.sm_val_yoy),
        )[:_COMMODITY_CARDS_PER_FLOW]
        for r in picks:
            note_bits = [f"{_fmt_month_abbr(period)}, CNY terms",
                         f"{_fmt_eur(r.eur_month)} in the month"]
            if r.sm_qty_yoy is not None:
                note_bits.append(f"{_fmt_signed_pct(r.sm_qty_yoy)} by volume")
            cards.append(Indicator(
                key=f"gacc_commodity_{flow}_{_slugify_heading(r.label)}",
                kicker=("EXPORT MOVER" if flow == "export" else "IMPORT MOVER"),
                label=r.label,
                value=r.sm_val_yoy, unit="yoy_pct",
                formatted=_fmt_signed_pct(r.sm_val_yoy),
                note=" · ".join(note_bits),
                chart="bignumber",
                provenance=Provenance(finding_ids=[r.fid], source="gacc",
                                      as_of=period),
            ))
    return cards


_GACC_COMMODITIES_ABOUT_MD = (
    "**What this is.** GACC’s preliminary release includes two "
    "commodity tables — “Major Exports/Imports by Quantity and Value” — "
    "a curated catalogue of ~30 headline lines (cars, integrated "
    "circuits, rare earths, steel …) with physical quantities as well as "
    "values. These are China↔world totals: there is no commodity-by-"
    "country breakdown in the preliminary release, so this section and "
    "the country sections cannot be cross-filtered.\n\n"
    "**Two sector lenses, two pages.** The Full briefing’s product "
    "sections are EU↔China specific, built from Europe’s CN8 detail with "
    "journalist-curated groups. This section is China’s own world-market "
    "cut — broader scope, coarser grain, five weeks earlier.\n\n"
    "**Growth rates are CNY-denominated** — computed in yuan, matching "
    "GACC’s own published comparisons, and every cumulative rate is "
    "cross-checked against the percentage GACC printed on the page "
    "(figures that fail that check are withheld). This differs from the "
    "EUR-denominated rates elsewhere on this page — the drawers carry "
    "both bases.\n\n"
    "**Quantities are the quotable half.** “+71% by volume to 1.06mn "
    "cars” survives every currency argument; where GACC publishes "
    "physical units this section shows the volume rate beside the value "
    "rate.\n\n"
    "**Milestones and pace lines are computed, not editorialised** — "
    "round-number crossings against every prior month of the same "
    "catalogue line, and linear year-pace vs last year’s total. Each "
    "carries its method in the drawer."
)


def _gacc_identity(cur, period: date, rows: list[_GaccRow]) -> dict:
    """The identity header: when China published, when Europe's harmonised
    figures for the same month are due (release_calendar), and links to
    both language versions of the release. The page's epistemic framing
    lives in ONE place — the "About this page" disclosure (`understanding`)
    — not here: two about-boxes with overlapping content read as
    functionally different when they weren't (Luke, 2026-07-05)."""
    cur.execute(
        """SELECT source_url, publication_date, first_seen_at
             FROM releases WHERE source = 'gacc' AND period = %s
         ORDER BY (currency = 'CNY') DESC, first_seen_at ASC LIMIT 1""",
        (period,),
    )
    row = cur.fetchone()
    url = row[0] if row else None
    published = None
    if row:
        published = row[1] or (row[2].date() if row[2] else None)
    due = release_calendar.expected_publish_date("eurostat", period)
    zh = _construct_chinese_source_url(url) if url else None
    return {
        "published": published.isoformat() if published else None,
        "confirmation_due": due.isoformat() if due else None,
        "source_url": url,
        "source_url_zh": zh,
    }


_GACC_PAGE_ABOUT_BY_COUNTRY = (
    "**Which countries appear.** GACC’s preliminary country/region "
    "table names its major partners, so smaller countries (including "
    "smaller EU member states) may be absent — this is China’s reporting "
    "choice, not a data gap on our side. Notably, GACC names no Middle "
    "East partner in this release: China–Gulf trade sits unseparated in "
    "the world total (and the Belt & Road row in China and the world "
    "above). Partners are matched to EU membership by ISO code, never by "
    "label spelling.\n\n"
    "**No product detail here.** The GACC country table has no commodity "
    "dimension — these country sections are partners-only by data "
    "availability. GACC’s product view (world totals, no country split) is "
    "the commodities section above; the EU-specific product detail arrives "
    "with the European confirmation in ~5 weeks.\n\n"
    "**Hong Kong (and Macao).** Separate customs regimes, so they appear "
    "here as partners and are flagged as **entrepôt** rows: a large share "
    "of mainland exports to Hong Kong are re-exported onward, so their "
    "figures read as routing, not local demand — and they are never summed "
    "into any China or EU total on this site."
)

# The single "About this page" disclosure (under the KPI strip, mirroring
# the Briefing's "About this site"). The page's WHOLE epistemic framing,
# consolidated 2026-07-05: it previously split across a caveat list here
# and an "Understanding these figures" section at the bottom — with three
# of five blocks duplicated between them and no functional difference.
_GACC_ABOUT_PAGE_MD = (
    "**An early read, not the confirmed record.** China’s own customs "
    "figures (GACC), published ~5 weeks before Eurostat covers the same "
    "month. Levels typically read ~20% above the EU’s mirror figures "
    "(valuation, routing and scope differences — see Methodology); "
    "year-on-year changes travel across the mirror far better than levels "
    "do. Quote changes; attribute levels to “China’s customs "
    "administration”.\n\n"
    "**Reading the direction.** “China’s exports to the EU” ≈ the "
    "EU’s imports from China, measured from the Chinese side. This page "
    "speaks China-perspective throughout — GACC’s own frame and the wire "
    "convention — where the Full briefing speaks Europe-perspective.\n\n"
    "**Scope.** “China” here means the mainland customs territory "
    "(GACC’s remit); goods routed via Hong Kong appear as trade with "
    "Hong Kong. The Full briefing’s Eurostat figures use the wider "
    "China+Hong Kong+Macao envelope, so the two tabs’ “China” "
    "differ by construction.\n\n"
    "**Products and countries don’t cross.** GACC’s country table is "
    "partner-level only; its commodity tables (the “What’s moving” "
    "section) are world-level only. The two dimensions can’t be combined "
    "from this source — EU-specific product detail arrives with the "
    "European confirmation.\n\n"
    "**Currency.** Partner figures are converted to EUR at per-period "
    "exchange rates, so their growth rates won’t exactly match GACC’s "
    "published CNY / USD figures — which matters doubly on a page that "
    "leads with growth rates. The commodity section is the exception: its "
    "rates are computed in CNY on GACC’s own basis, cross-checked against "
    "the page’s printed percentages."
)

_GACC_ABOUT_JAN_FEB_MD = (
    "**January–February windows.** Some windows include GACC’s combined "
    "January–February release (Chinese New Year); affected figures carry "
    "the jan_feb_combined caveat in their drawers."
)


def _gacc_about_page_md(rows: list[_GaccRow]) -> str:
    """The consolidated About-this-page copy, with the Jan–Feb note
    appended only when a shown window actually includes the combined
    release."""
    if any(r.totals.get("jan_feb_combined_years") for r in rows):
        return _GACC_ABOUT_PAGE_MD + "\n\n" + _GACC_ABOUT_JAN_FEB_MD
    return _GACC_ABOUT_PAGE_MD


def _gacc_llm_facts(page: GaccPage, rows: list[_GaccRow], period: date) -> dict:
    """The fact set for the page's LLM slots — display lines (the exact
    formatting the model must cite verbatim), the numeric facts for
    verify_numbers (fractions / EUR floats), and per-fact finding
    provenance. Deterministic; the model sees nothing else."""
    lines: list[str] = []
    numbers: dict[str, float] = {}
    prov: dict[str, int] = {}
    strip_fids: list[int] = []

    def add(key: str, val, fid) -> None:
        if val is None:
            return
        numbers[key] = float(val)
        if fid is not None:
            prov[key] = fid

    for key, kicker, display in _GACC_STRIP_SLOTS:
        r = _strip_pick(rows, key)
        if r is None:
            continue
        strip_fids.append(r.fid)
        bits = []
        if r.sm_yoy is not None:
            bits.append(f"{_fmt_signed_pct(r.sm_yoy)} YoY (single month)")
            add(f"{key}_sm", r.sm_yoy, r.fid)
        if r.ytd_yoy is not None:
            bits.append(f"YTD {_fmt_signed_pct(r.ytd_yoy)}")
            add(f"{key}_ytd", r.ytd_yoy, r.fid)
        if r.rolling_yoy is not None:
            # "12-month", not the page's "12mo" shorthand: the model echoes
            # the notation it is shown, and the verifier's time-period strip
            # recognises the former (llm_framing._TIME_PERIOD_RE).
            bits.append(f"12-month {_fmt_signed_pct(r.rolling_yoy)}")
            add(f"{key}_12mo", r.rolling_yoy, r.fid)
        lines.append(
            f"China’s exports to {display}, {_fmt_month_abbr(period)}: "
            + "; ".join(bits))
    st = page.standout
    if st is not None and st.metrics.get("pct") is not None:
        flow_phrase = ("exports to" if st.subject.get("flow") == "export"
                       else "imports from")
        direction = st.metrics.get("direction") or "moved"
        signed = (st.metrics["pct"] if direction != "fell"
                  else -st.metrics["pct"])
        lines.append(
            f"Sharpest move: China’s {flow_phrase} "
            f"{st.subject.get('group_name')} {direction} "
            f"{abs(signed) * 100:.1f}% YoY")
        add("standout_pct", signed,
            st.provenance.finding_ids[0] if st.provenance.finding_ids else None)
    for i, s in enumerate((page.since_last or {}).get("rows", [])[:3]):
        flow_phrase = ("exports to" if s["flow"] == "export"
                       else "imports from")
        lines.append(
            f"Swing since last month: China’s {flow_phrase} {s['label']} "
            f"was {s['prev_yoy'] * 100:+.1f}%, now {s['cur_yoy'] * 100:+.1f}% "
            f"({s['basis']})")
        add(f"swing{i}_prev", s["prev_yoy"], s.get("finding_id"))
        add(f"swing{i}_cur", s["cur_yoy"], s.get("finding_id"))
    return {"lines": lines, "numbers": numbers, "prov": prov,
            "strip_fids": strip_fids}


def _gacc_commodity_llm_facts(page: GaccPage, period: date) -> dict:
    """The fact set for the commodity take — every displayed catalogue row
    (both flows, leaves AND aggregates) plus the computed milestone / pace
    facts, in the exact formats the page shows. The input set EQUALS the
    displayed set by construction, so any number the take cites has a
    drawer on the page (the finding/N dead-end stays dead). Display-scale
    numerals for milestone/pace figures ride in the numbers dict alongside
    the raw magnitudes: verify_numbers reads the "10.17" in "10.17mn" as a
    bare count, so the count-scale value must be a fact too."""
    cm = (page.commodities.metrics if page.commodities is not None else {}) or {}
    lines: list[str] = []
    numbers: dict[str, float] = {}
    prov: dict[str, int] = {}

    def add(key: str, val, fid) -> None:
        if val is None:
            return
        numbers[key] = float(val)
        if fid is not None:
            prov[key] = fid

    n = 0
    for flow in ("export", "import"):
        flow_phrase = ("China's exports of" if flow == "export"
                       else "China's imports of")
        for group in ("rows", "aggregates"):
            for row in cm.get(f"{flow}_{group}", []):
                n += 1
                key = f"cm{n}"
                fid = row.get("finding_id")
                label = row["label"] + (
                    " [catalogue aggregate — includes other lines]"
                    if group == "aggregates" else "")
                bits: list[str] = []
                if row.get("sm_value_yoy") is not None:
                    bits.append(f"{_fmt_signed_pct(row['sm_value_yoy'])} YoY "
                                f"by value (single month, CNY terms)")
                    add(f"{key}_smval", row["sm_value_yoy"], fid)
                if row.get("sm_quantity_yoy") is not None:
                    q = f"{_fmt_signed_pct(row['sm_quantity_yoy'])} by volume"
                    if row.get("quantity_display"):
                        q += f" ({row['quantity_display']})"
                    bits.append(q)
                    add(f"{key}_smqty", row["sm_quantity_yoy"], fid)
                if row.get("eur_month") is not None:
                    bits.append(f"{_fmt_eur(row['eur_month'])} in the month")
                    add(f"{key}_eur", row["eur_month"], fid)
                if row.get("ytd_value_yoy") is not None:
                    bits.append(f"YTD {_fmt_signed_pct(row['ytd_value_yoy'])}")
                    add(f"{key}_ytd", row["ytd_value_yoy"], fid)
                if not bits:
                    continue
                lines.append(f"{flow_phrase} {label}, "
                             f"{_fmt_month_abbr(period)}: " + "; ".join(bits))
                for fk, fact in (row.get("facts") or {}).items():
                    lines.append(f"  Computed fact ({row['label']}): "
                                 f"{fact['text']}")
                    for vk in ("threshold_units", "month_units",
                               "pace_units", "prior_full_year_units"):
                        v = fact.get(vk)
                        if v is None:
                            continue
                        add(f"{key}_{fk}_{vk}", v, fid)
                        # Display-scale twin (the "10.17" in "10.17mn").
                        for scale in (1e9, 1e6, 1e3):
                            if v >= scale:
                                add(f"{key}_{fk}_{vk}_disp", v / scale, fid)
                                break
    return {"lines": lines, "numbers": numbers, "prov": prov,
            "strip_fids": [i.provenance.finding_ids[0]
                           for i in page.commodity_strip
                           if i.provenance.finding_ids]}


def _generate_gacc_page_slots(page: GaccPage, rows: list[_GaccRow],
                              period: date) -> None:
    """Populate the page's LLM slots in place (paid backend calls — the
    caller opts in per the cost discipline; three calls per new GACC month
    since the commodity take joined: synthesis + questions + commodity).
    Best-effort: any backend or generation failure leaves the deterministic
    page standing alone, same posture as every other LLM surface."""
    import llm_gacc_page
    facts = _gacc_llm_facts(page, rows, period)
    if not facts["lines"]:
        return
    try:
        from llm_framing import make_backend
        backend = make_backend(role="takes")
    except Exception:
        log.exception("gacc page: no LLM backend; slots left empty")
        return
    try:
        page.synthesis = llm_gacc_page.generate_synthesis(facts, backend)
    except Exception:
        log.exception("gacc page: synthesis generation failed; slot empty")
    try:
        qs = llm_gacc_page.generate_questions(facts, backend)
        if qs:
            page.questions = LLMSlot(
                slot_type="general",
                grounded_in=[f for f in facts["strip_fids"] if f is not None],
                status="generated",
                questions=qs,
            )
    except Exception:
        log.exception("gacc page: questions generation failed; slot empty")
    try:
        cfacts = _gacc_commodity_llm_facts(page, period)
        if cfacts["lines"]:
            page.commodity_take = llm_gacc_page.generate_commodity_take(
                cfacts, backend)
    except Exception:
        log.exception("gacc page: commodity take generation failed; slot empty")


def _build_gacc_page(cur, generate_takes: bool = False) -> GaccPage | None:
    """Assemble the GACC-only tab from live findings. None when no GACC
    findings exist yet — the renderer then simply omits the tab.
    `generate_takes` also runs the page's LLM slots (synthesis + questions)
    via the configured backend — paid calls, so opt-in: the gacc-update
    cycle passes True on new-period actions only; refreshes and main-track
    rebuilds carry the slots forward via the reuse graft instead."""
    period = _gacc_latest_period(cur)
    if period is None:
        return None
    rows = _gacc_rows_at(cur, period)
    if not rows:
        return None
    page = GaccPage(
        data_period=period,
        tab_label=f"GACC-only ({_fmt_month_abbr(period)})",
        identity=_gacc_identity(cur, period, rows),
        strip=_gacc_strip_cards(rows, period),
        standout=_gacc_standout(rows, period),
        by_country=_gacc_by_country_section(rows, period),
        world=_gacc_world_section(rows, period),
        since_last=_gacc_since_last(cur, rows, period),
        commodities=_gacc_commodities_section(cur, period),
        commodity_strip=_gacc_commodity_strip(cur, period),
        understanding=_gacc_about_page_md(rows),
    )
    if generate_takes:
        _generate_gacc_page_slots(page, rows, period)
    return page


_GLOSSARY_PATH = pathlib.Path(__file__).resolve().parent / "docs" / "glossary.md"


_GLOSSARY_WEB_HIDE = "<!--web-hide-->"


def _parse_glossary_md(text: str) -> list[dict]:
    """Parse the regular `## category` / `### term` / body structure of
    docs/glossary.md into [{title, terms: [{term, body}]}]. The H1 and any
    preamble before the first category are ignored (the section supplies its
    own intro). A bespoke parser for one well-structured file — far simpler,
    and safer, than a general markdown dependency.

    A `### Term <!--web-hide-->` marker drops that term from the PORTAL
    glossary: the entry documents a docx-bundle artifact a web-only reader has
    no access to (Tier 1/2/3, `02_Findings.md`, the provenance files, etc.).
    The term stays in the source file, so the bundle / GitHub glossary — where
    those references are meaningful — is unchanged. The comment is invisible in
    rendered Markdown, so it costs nothing on those surfaces."""
    groups: list[dict] = []
    cur_group: dict | None = None
    cur_term: str | None = None
    body: list[str] = []

    def flush():
        nonlocal cur_term, body
        if cur_term is not None and cur_group is not None:
            cur_group["terms"].append(
                {"term": cur_term, "body": "\n".join(body).strip()})
        cur_term, body = None, []

    for line in text.splitlines():
        if line.startswith("## "):
            flush()
            cur_group = {"title": line[3:].strip(), "terms": []}
            groups.append(cur_group)
        elif line.startswith("### "):
            flush()
            term = line[4:].strip()
            # Web-hidden terms set cur_term=None, so the body lines that follow
            # are skipped and the next flush() is a no-op until the next term.
            cur_term = None if _GLOSSARY_WEB_HIDE in term else term
        elif line.startswith("# "):
            continue
        elif cur_term is not None:
            body.append(line)
    flush()
    return [g for g in groups if g["terms"]]


def _glossary_section() -> Section:
    """The Glossary tab — definitions parsed from docs/glossary.md, baked into
    the snapshot (the portal serves a static blob, so the content must travel
    with it). Build-time file read; degrades to an empty section if absent."""
    root = Section(
        id="glossary", title="Glossary", kind="glossary",
        intro="Plain-language definitions of the terms, sources and methods "
              "used across the briefing.",
    )
    try:
        text = _GLOSSARY_PATH.read_text(encoding="utf-8")
    except OSError as e:
        log.warning("glossary: could not read %s (%s); empty section",
                    _GLOSSARY_PATH, e)
        return root
    groups = _parse_glossary_md(text)
    if groups:
        root.metrics = {"groups": groups}
    return root


# Tables tab: which spreadsheet tabs to embed inline (name -> max rows, None =
# all). The rest are listed as download-only — the full workbook (every tab,
# every row) is the .xlsx download, so embedding the heavy hs_yoy tabs (3000+
# rows each) inline would only bloat the snapshot. Inline = the digestible,
# scannable tabs a reporter actually reads on screen.
_DATA_TABLES_INLINE: dict[str, int | None] = {
    "summary": None,
    "trade_balance": 120,
    "mirror_gaps": None,
    "gacc_bilateral_yoy": None,
    "predictability_index": None,
}


def _jsonable_cell(v):
    """Coerce a spreadsheet cell to a JSON-serialisable scalar for the snapshot
    (Decimal → float, date/datetime → ISO, else str). Numbers stay numbers so
    the portal can right-align and the TSV copy round-trips."""
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return str(v)


def _data_section() -> Section:
    """The Tables tab — the journalist spreadsheet (`assemble_sheets()`) as
    embedded, filterable tables plus a full-workbook download. Best-effort: any
    failure leaves an empty section so the rest of the report still builds."""
    root = Section(
        id="tables", title="Tables", kind="data",
        intro="The findings as filterable tables. The digestible tabs are "
              "shown here; download the complete workbook (all tabs, every "
              "row) or copy any table straight into a spreadsheet.",
    )
    try:
        from sheets_export import assemble_sheets
        sheets = assemble_sheets()
    except Exception:
        log.exception("tables: assemble_sheets failed; empty Tables section")
        return root
    tables = []
    for sd in sheets:
        total = len(sd.rows)
        inline = sd.name in _DATA_TABLES_INLINE
        cap = _DATA_TABLES_INLINE.get(sd.name)
        src_rows = sd.rows if (cap is None) else sd.rows[:cap]
        rows = ([[_jsonable_cell(c) for c in r] for r in src_rows]
                if inline else [])
        if inline and len(rows) < total:
            log.info("tables: %s embedded %d of %d rows (rest in the download)",
                     sd.name, len(rows), total)
        tables.append({
            "name": sd.name,
            "description": sd.description,
            "headers": list(sd.headers),
            "rows": rows,
            "total_rows": total,
            "shown_rows": len(rows),
            "inline": inline,
            # Track grouping (2026-07-15): the Tables tab is the one
            # deliberately mixed-vintage surface, so every table declares
            # its release track and the renderer groups by it.
            "track": getattr(sd, "track", "main"),
        })
    if tables:
        root.metrics = {"tables": tables}
    return root


def build_report(
    source_trigger: str = "eurostat",
    data_period: date | None = None,
    diff_baseline_brief_run_id: int | None = None,
    *,
    generate_takes: bool = False,
    generate_gacc_takes: bool = False,
) -> Report:
    """Build the content model for one cycle. `source_trigger` selects the
    variant (Q1); defaults to 'eurostat' (every periodic export today is
    Eurostat-triggered).

    `generate_takes` (opt-in) runs the LLM per-finding take on the top movers
    (eurostat/hmrc only) via the configured backend — slow and backend-
    dependent, so it’s off by default. The deterministic report is complete
    without it; a rejected or failed take just leaves a placeholder.
    `generate_gacc_takes` (opt-in, independently) runs the GACC page's LLM
    slots — the gacc-update cycle passes it on new-period actions; every
    other rebuild carries those slots forward via the reuse graft."""
    # Hard publication dependency: without the SITC/BEC lookups every group
    # collapses into "Other / unclassified". Fail loud rather than silently
    # ship that (see classifications.assert_classifications_available).
    classifications.assert_classifications_available()
    variant_cfg = _VARIANTS.get(source_trigger, _VARIANTS["eurostat"])
    with _conn() as conn, conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor
    ) as cur:
        predictability = _compute_predictability_per_group(cur)
        top_movers = _compute_top_movers(cur, predictability=predictability)
        diff = _compute_diff(cur, baseline_brief_run_id=diff_baseline_brief_run_id)
        # Reader-facing group labels (db.group_display_names) — substituted at
        # every display/slug site; the stable internal key stays for lookups.
        disp = db.group_display_names(cur)
        # Key indicators (vital signs) — a small fixed set, each carrying its
        # figure + citation + as-of (the glyph never travels without its number).
        # Row 1: the headline EU-27 deficit/day (wide sparkline) + the EU import
        # level. Row 2: the China-dependency donut, the UK deficit/day (rendered
        # compact so it's 1 column, not wide-by-accident), and the biggest
        # single-product (CN8) mover. The donut (China's share of extra-EU goods
        # imports) carries its 000TOTAL extra-EU all-goods denominator.
        indicators = [
            ind for ind in (
                _deficit_indicator(cur),
                _import_level_indicator(cur),
                _china_share_indicator(cur),
                _deficit_indicator(
                    cur, subkind="trade_balance_uk", key="uk_china_deficit_per_day",
                    kicker="UK DEFICIT",
                    label="UK goods-trade deficit with China, HK & Macao",
                    source="hmrc", chart="bignumber",
                ),
                _biggest_mover_indicator(
                    cur, {m["group_name"] for m in top_movers}),
            ) if ind is not None
        ]

        # Variant-shaped lead + period (Q1).
        items: list[HeadlineItem] = []
        sections = []
        if source_trigger == "gacc":
            if data_period is None:
                data_period = _gacc_latest_period(cur)
            items = _gacc_macro_items(cur, data_period)
            sections = [_gacc_bilateral_section(cur, data_period),
                        _sources_section(cur, diff),
                        _data_section(),
                        _reference_section(cur),
                        _glossary_section()]
        else:  # eurostat / hmrc: HS-sector movers + the EU-27 sector tree
            if data_period is None and top_movers:
                data_period = top_movers[0].get("current_end")
            items = [_headline_item(m, disp) for m in top_movers]
            if generate_takes:
                # Per-mover LLM take (leading questions), verify-or-reject; a
                # failed/rejected take leaves a placeholder, never blocks.
                # grounded_in is the take's honest citation list — the slot's
                # anchor finding first, then every other finding whose numbers
                # the questions actually cite (cross-flow contrast questions
                # draw on the sibling finding's numbers).
                from llm_takes import generate_take_for_finding
                for m, item in zip(top_movers, items):
                    fid = m.get("id")
                    take = generate_take_for_finding(fid) if fid else None
                    item.take = LLMSlot(
                        slot_type="specific",
                        grounded_in=(take.grounded_in if take
                                     else ([fid] if fid else [])),
                        status="generated" if take else "placeholder",
                        questions=take.questions if take else [],
                    )
            if source_trigger == "eurostat":
                # No GACC section here (2026-07-15 ruling): the Full briefing
                # is single-period — Eurostat/HMRC at the anchor month, every
                # claim. ALL per-country/world GACC content lives on the
                # GACC-only tab at ITS period; the identity strip carries a
                # signpost, not a floating section.
                sections = [_state_of_play_section(cur),
                            _mirror_gap_section(cur),
                            _sector_detail_section(cur, predictability),
                            _structural_section(cur, data_period),
                            _sources_section(cur, diff),
                            _data_section(),
                            _reference_section(cur),
                            _glossary_section()]

        # The GACC-only tab — the second release track's page, built from
        # live findings on EVERY variant's rebuild so whichever cycle runs
        # carries the other track's tab forward (design doc § two-track
        # model). None (tab omitted) until GACC findings exist.
        gacc_page = _build_gacc_page(cur, generate_takes=generate_gacc_takes)

        # Per-source data vintage for the Briefing tab's identity strip (the
        # masthead no longer claims a single "Data to X" — see render).
        cur.execute(
            "SELECT source, MAX(period) FROM releases GROUP BY source")
        _latest_by_source = dict(cur.fetchall())
        source_vintages = {
            "eurostat": _latest_by_source.get("eurostat"),
            "hmrc": _latest_by_source.get("hmrc"),
            "gacc": gacc_page.data_period if gacc_page else
                    _latest_by_source.get("gacc"),
        }

        # Iteration 3 — bake provenance drawers for the Quotability-gated set:
        # the KPI standing levels + the headline movers + the GACC page's
        # strip and standout, i.e. the numbers a reporter actually quotes.
        # Built here (with DB access) and carried in the snapshot so the
        # static portal can show "where this came from" with no database.
        # Source-trail-first; best-effort per finding.
        import provenance_payload
        _gated = {f for ind in indicators for f in ind.provenance.finding_ids}
        _gated |= {f for it in items for f in it.provenance.finding_ids}
        if gacc_page is not None:
            _gated |= {f for ind in gacc_page.strip
                       for f in ind.provenance.finding_ids}
            if gacc_page.standout is not None:
                _gated |= set(gacc_page.standout.provenance.finding_ids)
            _gated |= {f for ind in gacc_page.commodity_strip
                       for f in ind.provenance.finding_ids}
            if gacc_page.commodities is not None:
                cm = gacc_page.commodities.metrics
                _gated |= {row["finding_id"]
                           for key in ("export_rows", "import_rows",
                                       "export_aggregates", "import_aggregates")
                           for row in cm.get(key, [])
                           if row.get("finding_id")}
        prov_payloads = provenance_payload.build_payloads_for(cur, _gated)

    month = _fmt_month(data_period)
    # Per-finding takes live on each HeadlineItem (the 'specific' interpretation);
    # only the across-release 'general' slot sits at the headline level.
    llm_slots: list[LLMSlot] = [LLMSlot(
        slot_type="general",
        grounded_in=[i.provenance.finding_ids[0] for i in items if i.provenance.finding_ids],
    )]

    headline = Headline(
        variant=source_trigger,
        lead_title=variant_cfg["lead"].format(month=month),
        note=variant_cfg["note"],
        items=items,
        llm_slots=llm_slots,
    )

    generated_at = datetime.now()
    meta = ReportMeta(
        data_period=data_period,
        variant=source_trigger,
        snapshot_id=f"{source_trigger}-{data_period}-{generated_at:%Y%m%dT%H%M%S}",
        generated_at=generated_at,
    )
    report = Report(
        meta=meta,
        key_indicators=indicators,
        headline=headline,
        what_changed=_what_changed(diff, disp),
        sections=sections,  # the navigable content tree (Eurostat variant)
        provenance_payloads=prov_payloads,
        gacc_page=gacc_page,
        source_vintages=source_vintages,
    )
    # Across-release 'general' take — "One other thing worth a look". It selects
    # from a shortlist of NON-headline findings, so it needs the finished report
    # and runs last. Best-effort: a failure or abstention leaves the stub slot
    # (no content), and the render simply shows nothing.
    if generate_takes and source_trigger == "eurostat":
        try:
            from llm_general_take import generate_general_take
            gt = generate_general_take(report)
            if gt:
                report.headline.llm_slots = [LLMSlot(
                    slot_type="general",
                    grounded_in=gt["citations"],
                    status="generated",
                    content=gt["take"],
                )]
        except Exception:
            import logging
            logging.getLogger(__name__).exception(
                "general take failed; leaving the placeholder slot"
            )
    return report
