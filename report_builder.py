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
`[group](#slug)` anchor, a backtick citation token). That's fine for the
markdown renderer but leaks doc-specific nav into the model; the clean
path is the structured `subject`/`metrics`/`stability`/`drill_down`
fields, and prose gets decoupled into plain text + structured emphasis
when the HTML renderer needs it. `facets` are likewise minimally stubbed
(commodity only) pending hs_groups facet metadata.
"""

from __future__ import annotations

import logging
import pathlib
import re
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal

import psycopg2.extras

log = logging.getLogger(__name__)

from briefing_pack._helpers import (
    _ALL_UNIVERSAL_CAVEATS,
    _compute_predictability_per_group,
    _compute_top_movers,
    _conn,
    _fmt_eur,
    _fmt_month,
    _slugify_heading,
    _subkind_plain_label,
)
from briefing_pack.sections.diff import _compute_diff
from briefing_pack.sections.front_page import _mover_sentence
from anomalies import (
    EU27_EXCLUDE_REPORTERS,
    EUROSTAT_PARTNERS,
    _months_back,
)
import classifications
import labels
from report_model import (
    ChartData,
    Facets,
    Finding,
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
    only what's unusual *about it* (partial window, low kg coverage…)."""
    return [c for c in (codes or [])
            if c not in _ALL_UNIVERSAL_CAVEATS and c not in _ROW_HIDDEN_CAVEATS]

# Variant content (Q1). The lead title and note are *content* (they live
# in the Headline node), so they belong here in the builder, not in a
# renderer. `has_sector_movers` decides whether the lead is HS-sector
# movers (Eurostat/HMRC) or macro/geographic partner-bloc totals (GACC).
_VARIANTS: dict[str, dict] = {
    "eurostat": {
        "lead": "What {month}'s EU figures changed",
        "note": (
            "Triggered by new Eurostat data. The China-vs-Europe "
            "mirror-trade discrepancy and the HS-sector shifts live here, "
            "at their freshest month."
        ),
        "has_sector_movers": True,
        "general_slot": "surface what connects {month}'s findings — and what's notably absent",
    },
    "gacc": {
        "lead": "What China's own {month} figures changed",
        "note": (
            "Triggered by new GACC data, a month ahead of Europe's. No "
            "mirror-gap or HS-sector detail at this altitude — GACC "
            "preliminary is partner/bloc totals, so the headline is "
            "macro/geographic."
        ),
        "has_sector_movers": False,
        "general_slot": "read what China's {month} geography shift implies — grounded in the totals above",
    },
    "hmrc": {
        "lead": "What the UK's {month} figures changed",
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
    back-filled revision can't masquerade as the newest month."""
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
        "**standing level, not a change** — Europe's all-goods trade balance "
        "with China (imports minus exports), straight from Eurostat and HMRC. "
        "It barely moves cycle to cycle, so it never trips the \"what's new\" "
        "thresholds — but its size, per day, is usually the most quotable single "
        "number in the pack.\n"
        "\n"
        "**Before quoting:** Eurostat values imports CIF (freight + insurance) "
        "and exports FOB, which widens the deficit — but it is the basis "
        "Eurostat publishes on. **\"China\" includes Hong Kong and Macau** "
        "(CN+HK+MO) on both sides. The EU-27 + UK line adds two statistical "
        "agencies' figures: a close approximation, not a single-source number."
    ),
    "mirror-gaps": (
        "**Mirror trade** compares the two sides' own books: China's reported "
        "exports to a partner against that partner's reported imports from "
        "China. They never match exactly — but a gap *beyond* the normal "
        "accounting wedge (China reports exports FOB; the partner reports "
        "imports CIF, ~5–10% higher) can signal **transshipment**: goods routed "
        "through a hub (often Rotterdam or Hong Kong) and cleared into the EU "
        "elsewhere.\n"
        "\n"
        "Each row shows both reported totals, the gap, how much of it exceeds "
        "the CIF/FOB baseline, and — where flagged — the named hub and a "
        "z-score for how unusual the gap is against the partner's own six-month "
        "history."
    ),
    "sector-detail": (
        "**How to read each group.** Every figure is a **rolling 12-month "
        "total** compared with the prior 12 months, unless a latest-month "
        "figure is shown beside it. The 12-month figure smooths seasonal swings "
        "and is the one to quote; the **latest month** is a direction hint that "
        "swings wildly on lumpy categories (aircraft, ships) — don't headline "
        "it.\n"
        "\n"
        "- **Value vs volume** — *value* is what the goods cost (€); *volume* is "
        "their weight (kg). Value falling faster than volume means the same "
        "goods got cheaper.\n"
        "- **low base** — when flagged, the percentage rests on a small total "
        "(under €50M); quote the absolute € amount, not the %.\n"
        "- **🟢 🟡 🔴 predictability** — whether the group's year-on-year signal "
        "held over the past six months: 🟢 held (reliable) / 🟡 mixed / 🔴 "
        "didn't hold (verify before quoting). No badge means too little history "
        "to score.\n"
        "- **`finding/N`** — the citation token: a permanent handle to the exact "
        "database row behind the number, with its full audit trail.\n"
        "\n"
        "**Three views** of each group sit side by side: **EU-27** (Eurostat), "
        "**UK** (HMRC), and **EU-27 + UK** combined (two agencies summed — a "
        "useful approximation, not a like-for-like single source)."
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
        "China's own customs figures (GACC) for its trade with each major "
        "partner and bloc — released about a month ahead of Europe's data, so "
        "the **earliest read** on where China's trade is shifting. These are "
        "**China-reported** totals (exports valued FOB), not the EU mirror, and "
        "cover China's whole world, not just Europe.\n"
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
    label: str = "EU-27 goods-trade deficit with China",
    source: str = "eurostat",
) -> Indicator | None:
    """A goods-trade deficit with China as a vital sign — the standing
    €/day level + YoY delta + a monthly sparkline. Its home is Key
    indicators (amended Q3): a level the delta sections can't carry.
    Parameterised by `subkind` so EU-27 (`trade_balance`) and the UK
    (`trade_balance_uk`) reuse one builder."""
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

    delta = None
    if yoy is not None:
        delta = {
            "value": float(yoy),
            "direction": "wider" if float(yoy) > 0 else "narrower",
            "formatted": f"{'+' if float(yoy) >= 0 else ''}{float(yoy) * 100:.1f}% YoY",
        }

    return Indicator(
        key=key,
        label=label,
        value=float(per_day),
        unit="eur_per_day",
        formatted=f"€{float(per_day) / 1e6:,.0f}M/day",
        chart="sparkline",
        delta=delta,
        chart_data=ChartData(chart_type="sparkline", series=series),
        provenance=Provenance(finding_ids=[fid], source=source, as_of=as_of),
    )


def _import_level_indicator(cur) -> Indicator | None:
    """EU-27 goods imports from China, rolling-12-month level (the size of
    the inflow, not its change). A clean all-goods figure straight off the
    same `trade_balance` finding as the deficit (`rolling_12mo.import_eur`,
    CN+HK+MO, 000TOTAL basis) — so it carries that finding's citation."""
    fid, detail = _latest_trade_balance(cur, "trade_balance")
    if fid is None:
        return None
    roll = (detail or {}).get("totals", {}).get("rolling_12mo", {})
    imp = roll.get("import_eur")
    if imp is None:
        return None
    anchor = (detail or {}).get("windows", {}).get("anchor_period")
    as_of = date.fromisoformat(anchor) if isinstance(anchor, str) else None
    return Indicator(
        key="eu_china_imports_12mo",
        label="EU-27 goods imports from China (12-month)",
        value=float(imp),
        unit="eur",
        formatted=_fmt_eur(imp),
        chart="bignumber",
        provenance=Provenance(finding_ids=[fid], source="eurostat", as_of=as_of),
    )


def _headline_item(m: dict) -> HeadlineItem:
    """One restated quotable mover → a HeadlineItem (Q2)."""
    is_export = m["subkind"].endswith("_export")
    yoy = float(m["yoy_pct"])
    group = m["group_name"]
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
            "group_name": group,
        },
        metrics=metrics,
        stability={"badge": badge, "hedge_phrase": None},
        prose=_mover_sentence(m),  # v0: carries markdown — see module note
        drill_down=_slugify_heading(group),
        provenance=Provenance(
            finding_ids=[m["id"]], source="eurostat",
            as_of=m.get("current_end"),
        ),
        facets=Facets(commodity=[group]),  # v0: minimal — see module note
    )


def _what_changed(diff) -> WhatChanged:
    from briefing_pack.sections.front_page import _since_last_pack_lines
    significant = [
        Shift(
            group_name=s["group_name"],
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
    summary = " ".join(_since_last_pack_lines(diff)).replace(
        "**Since the last pack:** ", ""
    )
    # Per-subkind new-findings breakdown (Tier-1 "N new — <type>"), behind an
    # expander in the renderers.
    new_by_subkind = [
        {"subkind": sk, "label": _subkind_plain_label(sk), "count": n}
        for sk, n in (diff.new_by_subkind or [])
    ]
    return WhatChanged(
        regime=diff.regime,
        summary=summary,
        significant=significant,
        new_count=diff.total_new,
        new_by_subkind=new_by_subkind,
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


def _sector_detail_section(cur) -> Section:
    """The navigable granularity layer: one child Section per HS group,
    each carrying its import + export Finding. The group's Section id is
    `_slugify_heading(name)` — the SAME slug the headline movers point
    their drill-downs at, so those links resolve here. Groups ordered by
    12-month value (biggest sectors first); navigation/search by `facets`
    is the later refinement.
    """
    root = Section(
        id="sector-detail", title="Sector detail", kind="sector_detail",
        intro="Every HS group's rolling 12-month value vs the prior 12 "
              "months, China ↔ EU-27, both flows.",
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
    cur.execute("SELECT name, hs_patterns, description FROM hs_groups")
    _g = cur.fetchall()
    patterns_by_name = {n: (p or []) for n, p, d in _g}
    desc_by_name = {n: d for n, p, d in _g}
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
            tot = (detail or {}).get("totals", {})
            sm = tot.get("single_month") or {}
            is_export = subkind.endswith("_export")
            finding = Finding(
                finding_id=fid, subkind=subkind,
                title=(f"{scope_label} {'exports' if is_export else 'imports'} "
                       f"of {grp} {'to' if is_export else 'from'} China"),
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
                facets=Facets(commodity=[grp]),
            )
            g = by_group.setdefault(grp, {"max_eur": 0.0, "findings": []})
            g["findings"].append(finding)
            if scope_label == "EU-27":
                g["max_eur"] = max(g["max_eur"],
                                   finding.metrics["current_eur"] or 0.0)
                if not is_export:  # rich detail lives on the EU-27 import row
                    g["top_cn8"] = (detail or {}).get(
                        "top_cn8_codes_in_current_12mo") or []
                    g["reporters"] = (detail or {}).get(
                        "per_reporter_breakdown") or []

    for name, g in sorted(by_group.items(), key=lambda kv: -kv[1]["max_eur"]):
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
            metrics["top_cn8"] = [{"code": t.get("hs_code"),
                                   "eur": _f(t.get("total_eur"))} for t in top]
        reps = sorted(g.get("reporters") or [],
                      key=lambda r: -(r.get("share_of_group_delta_pct") or 0))[:3]
        if reps:
            metrics["reporters"] = [
                {"reporter": r.get("reporter"),
                 "share": _f(r.get("share_of_group_delta_pct")),
                 "yoy": _f(r.get("yoy_pct"))} for r in reps]
        if traj_by_name.get(name):
            metrics["trajectory"] = traj_by_name[name]
            metrics["trajectory_findings"] = traj_ids_by_name.get(name, [])
        efid, esv = export_share_by_name.get(name, (None, None))
        if esv is not None:
            metrics["china_export_share_value"] = esv
            metrics["china_export_share_finding"] = efid
        root.sections.append(Section(
            id=_slugify_heading(name), title=name, kind="sector_detail",
            findings=fs, metrics=metrics, intro=desc_by_name.get(name),
            facets=Facets(commodity=[name], sector=sectors, theme=themes,
                          end_use=end_use),
        ))
    return root


def _gacc_latest_period(cur) -> date | None:
    cur.execute(
        """SELECT max((detail->'windows'->>'current_end')::date)
             FROM findings
            WHERE subkind = 'gacc_aggregate_yoy' AND superseded_at IS NULL"""
    )
    return cur.fetchone()[0]


def _gacc_macro_items(cur, period) -> list[HeadlineItem]:
    """The GACC variant's macro/geographic lead: China's own reported
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
        subj = (f"China's exports to {bloc}" if is_export
                else f"China's imports from {bloc}")
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
    trade-balance subkind — used for the China-reported counterpart."""
    cur.execute(
        """SELECT id, (detail->'totals'->'rolling_12mo'->>'deficit_per_day_eur')::numeric
             FROM findings WHERE superseded_at IS NULL AND subkind=%s
             ORDER BY (detail->'windows'->>'anchor_period')::date DESC LIMIT 1""",
        (subkind,),
    )
    r = cur.fetchone()
    return (r[0], _f(r[1])) if r else (None, None)


def _state_of_play_section(cur) -> Section:
    """The 'where things stand' companion (Q3). First cut: Europe's
    standing goods-trade deficit with China across the three reporter
    scopes — the canonical standing level (the ~€1bn/day figure). A level,
    not a change, so it lives here rather than in 'what changed'."""
    root = Section(
        id="state-of-play", title="State of play", kind="state_of_play",
        intro="Where things stand — standing levels, not this cycle's change.",
        about=_ABOUT["the-deficit"],
    )
    deficit = Section(
        id="the-deficit",
        title="Europe's goods-trade deficit with China", kind="state_of_play",
        intro="The standing level by reporter scope, on the CN+HK+MO envelope.",
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
                series.append(SeriesPoint(
                    period=date.fromisoformat(per) if isinstance(per, str) else per,
                    value=float(val),
                ))
        deficit.findings.append(Finding(
            finding_id=fid, subkind=subkind,
            title=f"{label} deficit with China",
            metrics={
                "scope": label,
                "deficit_eur": _f(roll.get("deficit_eur")),
                "per_day_eur": _f(roll.get("deficit_per_day_eur")),
                "yoy_pct": _f(roll.get("yoy_pct")),
                "cn_per_day_eur": cn_per_day,  # China-reported counterpart
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
    return root


def _mirror_gap_section(cur) -> Section:
    """The China↔EU mirror-trade discrepancy — the signature distinctive
    analysis. Per partner: China's reported exports vs the partner's reported
    imports, the gap, and how much exceeds the CIF/FOB accounting baseline
    (the transshipment signal), with the named hub where relevant."""
    root = Section(
        id="mirror-gaps", title="Mirror-trade gaps", kind="mirror_gap",
        intro="China's reported exports to each partner vs the partner's "
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
    # biggest excess-over-baseline first (the strongest transshipment signal)
    root.findings.sort(key=lambda f: -(f.metrics.get("excess_pct") or -9))
    return root


_UNCLASSIFIED = "unclassified"


def _structural_section(cur, anchor: date | None) -> Section:
    """The structural SITC-division browse — the full trade map by value,
    surfacing the value / codes that sit in no editorial group. Each division
    is a *summary* node (Section.metrics): its value share, how much is covered
    by editorial groups, code count, and the groups within it. No per-code
    findings (those don't exist for the tail); this is the spine made visible.

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
        intro="Every CN8 code by SITC division, value-weighted — the whole "
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
                "groups": [{"name": g, "slug": _slugify_heading(g)}
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
            "UK rows excluded at all times (so it's comparable across the whole "
            "period, including pre-Brexit years). **UK** is HMRC data, converted "
            "to EUR at the period's reference rate. **EU-27 + UK** adds the two "
            "together — a useful combined view, but it sums two different "
            "statistical agencies' figures, so it is not a like-for-like number "
            "from a single source.\n"
            "\n"
            "**\"China\" always includes Hong Kong and Macau** (CN+HK+MO): "
            "European statistics report trade routed via Hong Kong and Macau "
            "under separate customs codes, but editorially it is still Chinese "
            "trade, so every figure sums all three."
        ),
    },
    {
        "title": "Predictability badges (🟢 🟡 🔴)",
        "body": (
            "Some HS-group findings carry a badge scoring how *stable* the "
            "group's year-on-year signal has been. Each (scope, flow) view is "
            "compared with its reading six months earlier and counts as "
            "**persistent** if the direction didn't flip and the rate moved by "
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
    "gacc": "China General Administration of Customs — China's own reported "
            "trade, preliminary monthly releases (CNY and USD), exports FOB.",
    "hmrc": "HMRC Overseas Trade Statistics — UK trade with China, converted "
            "to EUR at the period's reference rate.",
}


def _reference_section(cur) -> Section:
    """The endmatter — methodology orientation, the caveats that apply, and the
    sources. The defensibility scaffolding ported from the old Findings doc
    (reader's guide + methodology footer + sources appendix), minus the LLM
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


def _sources_section(cur) -> Section:
    """The Sources & coverage tab: the data sources, how much of each we hold
    (period coverage), and a readable manifest of what the pack contains. The
    Trade Map (structural) renders in the same tab — together they answer 'what
    the briefing rests on and how completely it covers the ground' (principle 7,
    given its own home)."""
    cur.execute("SELECT DISTINCT source FROM releases ORDER BY source")
    sources = [{"source": s, "note": _SOURCE_NOTES.get(s, "")}
               for (s,) in cur.fetchall()]
    cur.execute("""SELECT source, min(period), max(period), count(*)
                     FROM releases GROUP BY source ORDER BY source""")
    coverage = [{"source": s,
                 "start": a.isoformat() if a else None,
                 "end": b.isoformat() if b else None,
                 "releases": n}
                for s, a, b, n in cur.fetchall()]
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
    return Section(
        id="sources", title="Sources & coverage", kind="sources",
        intro="What this briefing rests on — the data sources, how much of each "
              "we hold, and what the pack contains.",
        metrics={"sources": sources, "coverage": coverage,
                 "manifest": manifest, "manifest_total": total,
                 "appendix": appendix},
    )


def _gacc_bilateral_section(cur, period) -> Section:
    """The GACC variant's deeper layer: China's own reported trade with each
    of its ~24 named partner countries (both flows), under the bloc-level
    macro lead."""
    root = Section(
        id="gacc-bilateral", title="China's trade by partner (GACC)",
        kind="gacc_bilateral",
        intro="China's own reported exports and imports by partner country, "
              "rolling 12 months — the per-country detail under the bloc lead.",
        about=_ABOUT["gacc-bilateral"],
    )
    if period is None:
        return root
    cur.execute(
        """SELECT id, subkind, detail FROM findings WHERE superseded_at IS NULL
            AND subkind IN ('gacc_bilateral_aggregate_yoy',
                            'gacc_bilateral_aggregate_yoy_import')
            AND (detail->'windows'->>'current_end')::date = %s""",
        (period,),
    )
    by_partner: dict[str, dict] = {}
    for fid, subkind, detail in cur.fetchall():
        partner = ((detail or {}).get("partner") or {}).get("raw_label") or "?"
        is_export = not subkind.endswith("_import")
        tot = (detail or {}).get("totals", {})
        finding = Finding(
            finding_id=fid, subkind=subkind,
            title=(f"China {'exports to' if is_export else 'imports from'} {partner}"),
            metrics={"scope": "China", "flow": "export" if is_export else "import",
                     "yoy_pct": _f(tot.get("yoy_pct")),
                     "current_eur": _f(tot.get("current_12mo_eur")),
                     "caveats": _visible_caveats((detail or {}).get("caveat_codes"))},
            chart_data=_series_chart((detail or {}).get("monthly_series")),
            provenance=Provenance(finding_ids=[fid], source="gacc", as_of=period),
        )
        p = by_partner.setdefault(partner, {"max_eur": 0.0, "findings": []})
        p["findings"].append(finding)
        p["max_eur"] = max(p["max_eur"], finding.metrics["current_eur"] or 0.0)
    for name, p in sorted(by_partner.items(), key=lambda kv: -kv[1]["max_eur"]):
        fs = sorted(p["findings"], key=lambda f: f.metrics["flow"])
        root.sections.append(Section(
            id="gacc-" + _slugify_heading(name), title=name,
            kind="gacc_bilateral", findings=fs,
            facets=Facets(partner=[name]),
        ))
    return root


_GLOSSARY_PATH = pathlib.Path(__file__).resolve().parent / "docs" / "glossary.md"


def _parse_glossary_md(text: str) -> list[dict]:
    """Parse the regular `## category` / `### term` / body structure of
    docs/glossary.md into [{title, terms: [{term, body}]}]. The H1 and any
    preamble before the first category are ignored (the section supplies its
    own intro). A bespoke parser for one well-structured file — far simpler,
    and safer, than a general markdown dependency."""
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
            cur_term = line[4:].strip()
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
) -> Report:
    """Build the content model for one cycle. `source_trigger` selects the
    variant (Q1); defaults to 'eurostat' (every periodic export today is
    Eurostat-triggered).

    `generate_takes` (opt-in) runs the LLM per-finding take on the top movers
    (eurostat/hmrc only) via the configured backend — slow and backend-
    dependent, so it's off by default. The deterministic report is complete
    without it; a rejected or failed take just leaves a placeholder."""
    variant_cfg = _VARIANTS.get(source_trigger, _VARIANTS["eurostat"])
    with _conn() as conn, conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor
    ) as cur:
        predictability = _compute_predictability_per_group(cur)
        top_movers = _compute_top_movers(cur, predictability=predictability)
        diff = _compute_diff(cur, baseline_brief_run_id=diff_baseline_brief_run_id)
        # Key indicators (vital signs) — a small fixed set, each carrying its
        # figure + citation + as-of (the glyph never travels without its number).
        # EU-27 deficit/day, the EU import level, the UK deficit/day. (China's
        # share-of-EU-imports donut is deferred: it needs an all-goods extra-EU
        # world denominator we don't yet ingest — eurostat_world_aggregates
        # covers only the tracked codes, so any share off it would mislabel.)
        indicators = [
            ind for ind in (
                _deficit_indicator(cur),
                _import_level_indicator(cur),
                _deficit_indicator(
                    cur, subkind="trade_balance_uk", key="uk_china_deficit_per_day",
                    label="UK goods-trade deficit with China", source="hmrc",
                ),
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
                        _sources_section(cur),
                        _data_section(),
                        _reference_section(cur),
                        _glossary_section()]
        else:  # eurostat / hmrc: HS-sector movers + the EU-27 sector tree
            if data_period is None and top_movers:
                data_period = top_movers[0].get("current_end")
            items = [_headline_item(m) for m in top_movers]
            if generate_takes:
                # Per-mover LLM take (leading questions), verify-or-reject; a
                # failed/rejected take leaves a placeholder, never blocks.
                from llm_takes import generate_take_for_finding
                for m, item in zip(top_movers, items):
                    fid = m.get("id")
                    qs = generate_take_for_finding(fid) if fid else None
                    item.take = LLMSlot(
                        slot_type="specific",
                        grounded_in=[fid] if fid else [],
                        status="generated" if qs else "placeholder",
                        questions=qs or [],
                    )
            if source_trigger == "eurostat":
                sections = [_state_of_play_section(cur),
                            _mirror_gap_section(cur),
                            _sector_detail_section(cur),
                            # China's own (GACC) bilateral context — in the
                            # Findings doc's state-of-play, missing from the
                            # portal till now. Uses GACC's latest period (a month
                            # ahead); empty-safe if no GACC data.
                            _gacc_bilateral_section(cur, _gacc_latest_period(cur)),
                            _structural_section(cur, data_period),
                            _sources_section(cur),
                            _data_section(),
                            _reference_section(cur),
                            _glossary_section()]

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
        what_changed=_what_changed(diff),
        sections=sections,  # the navigable content tree (Eurostat variant)
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
