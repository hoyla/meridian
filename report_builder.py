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

import re
from collections import defaultdict
from datetime import date, datetime

import psycopg2.extras

from briefing_pack._helpers import (
    _compute_predictability_per_group,
    _compute_top_movers,
    _conn,
    _fmt_eur,
    _fmt_month,
    _slugify_heading,
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


def _deficit_indicator(cur) -> Indicator | None:
    """The EU-27 goods-trade deficit with China — the standing €1bn/day
    level. Big-number + YoY delta + a monthly sparkline series. Its home
    is Key indicators (amended Q3): a level the delta sections can't
    carry."""
    cur.execute(
        """
        SELECT id, detail FROM findings
         WHERE superseded_at IS NULL AND subkind = 'trade_balance'
         ORDER BY id DESC LIMIT 1
        """
    )
    row = cur.fetchone()
    if row is None:
        return None
    fid, detail = row["id"], row["detail"]
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
        key="eu_china_deficit_per_day",
        label="EU-27 goods-trade deficit with China",
        value=float(per_day),
        unit="eur_per_day",
        formatted=f"€{float(per_day) / 1e6:,.0f}M/day",
        chart="sparkline",
        delta=delta,
        chart_data=ChartData(chart_type="sparkline", series=series),
        provenance=Provenance(
            finding_ids=[fid], source="eurostat", as_of=as_of,
        ),
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
    return WhatChanged(
        regime=diff.regime,
        summary=summary,
        significant=significant,
        new_count=diff.total_new,
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
    cur.execute("SELECT DISTINCT source FROM releases ORDER BY source")
    sources = [{"source": s, "note": _SOURCE_NOTES.get(s, "")}
               for (s,) in cur.fetchall()]
    return Section(
        id="methodology", title="Methodology, sources & caveats",
        kind="reference",
        intro="How to read these figures: every value is a rolling 12-month "
              "total compared with the prior 12 months unless stated, and "
              "every line ends in its finding/N citation token — drillable "
              "back to the underlying source rows.",
        metrics={"caveats": caveats, "sources": sources},
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
                     "current_eur": _f(tot.get("current_12mo_eur"))},
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


def build_report(
    source_trigger: str = "eurostat",
    data_period: date | None = None,
    diff_baseline_brief_run_id: int | None = None,
) -> Report:
    """Build the content model for one cycle. `source_trigger` selects the
    variant (Q1); defaults to 'eurostat' (every periodic export today is
    Eurostat-triggered)."""
    variant_cfg = _VARIANTS.get(source_trigger, _VARIANTS["eurostat"])
    with _conn() as conn, conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor
    ) as cur:
        predictability = _compute_predictability_per_group(cur)
        top_movers = _compute_top_movers(cur, predictability=predictability)
        diff = _compute_diff(cur, baseline_brief_run_id=diff_baseline_brief_run_id)
        indicators = []
        deficit = _deficit_indicator(cur)
        if deficit is not None:
            indicators.append(deficit)

        # Variant-shaped lead + period (Q1).
        items: list[HeadlineItem] = []
        sections = []
        if source_trigger == "gacc":
            if data_period is None:
                data_period = _gacc_latest_period(cur)
            items = _gacc_macro_items(cur, data_period)
            sections = [_gacc_bilateral_section(cur, data_period)]
        else:  # eurostat / hmrc: HS-sector movers + the EU-27 sector tree
            if data_period is None and top_movers:
                data_period = top_movers[0].get("current_end")
            items = [_headline_item(m) for m in top_movers]
            if source_trigger == "eurostat":
                sections = [_state_of_play_section(cur),
                            _mirror_gap_section(cur),
                            _sector_detail_section(cur),
                            _structural_section(cur, data_period),
                            _reference_section(cur)]

    month = _fmt_month(data_period)
    llm_slots: list[LLMSlot] = []
    if items:
        llm_slots.append(LLMSlot(
            slot_type="specific",
            grounded_in=items[0].provenance.finding_ids,
        ))
    llm_slots.append(LLMSlot(
        slot_type="general",
        grounded_in=[i.provenance.finding_ids[0] for i in items if i.provenance.finding_ids],
    ))

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
    return Report(
        meta=meta,
        key_indicators=indicators,
        headline=headline,
        what_changed=_what_changed(diff),
        sections=sections,  # the navigable content tree (Eurostat variant)
    )
