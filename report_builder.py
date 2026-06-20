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

from datetime import date, datetime

import psycopg2.extras

from briefing_pack._helpers import (
    _compute_predictability_per_group,
    _compute_top_movers,
    _conn,
    _fmt_month,
    _slugify_heading,
)
from briefing_pack.sections.diff import _compute_diff
from briefing_pack.sections.front_page import _mover_sentence
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
    cur.execute(
        """SELECT id, subkind, detail FROM findings
            WHERE superseded_at IS NULL
              AND subkind IN ('hs_group_yoy','hs_group_yoy_export')
              AND (detail->'windows'->>'current_end')::date = %s""",
        (anchor,),
    )
    by_group: dict[str, dict] = {}
    for fid, subkind, detail in cur.fetchall():
        grp = ((detail or {}).get("group") or {}).get("name") or "Unknown"
        tot = (detail or {}).get("totals", {})
        is_export = subkind.endswith("_export")
        finding = Finding(
            finding_id=fid, subkind=subkind,
            title=(f"EU-27 {'exports' if is_export else 'imports'} of "
                   f"{grp} {'to' if is_export else 'from'} China"),
            metrics={
                "flow": "export" if is_export else "import",
                "yoy_pct": _f(tot.get("yoy_pct")),
                "current_eur": _f(tot.get("current_12mo_eur")),
                "yoy_pct_kg": _f(tot.get("yoy_pct_kg")),
                "low_base": bool(tot.get("low_base")),
            },
            chart_data=_series_chart((detail or {}).get("monthly_series")),
            provenance=Provenance(
                finding_ids=[fid], source="eurostat", as_of=anchor,
                caveat="low base" if tot.get("low_base") else None,
            ),
            facets=Facets(commodity=[grp]),
        )
        g = by_group.setdefault(grp, {"max_eur": 0.0, "findings": []})
        g["findings"].append(finding)
        g["max_eur"] = max(g["max_eur"], finding.metrics["current_eur"] or 0.0)

    for name, g in sorted(by_group.items(), key=lambda kv: -kv[1]["max_eur"]):
        # findings sorted export-then-import (alpha: 'export' < 'import')
        fs = sorted(g["findings"], key=lambda f: f.metrics["flow"])
        root.sections.append(Section(
            id=_slugify_heading(name), title=name, kind="sector_detail",
            findings=fs, facets=Facets(commodity=[name]),
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
        if data_period is None and top_movers:
            data_period = top_movers[0].get("current_end")
        indicators = []
        deficit = _deficit_indicator(cur)
        if deficit is not None:
            indicators.append(deficit)
        sector_detail = _sector_detail_section(cur)

    month = _fmt_month(data_period)
    items: list[HeadlineItem] = []
    llm_slots: list[LLMSlot] = []
    if variant_cfg["has_sector_movers"]:
        items = [_headline_item(m) for m in top_movers]
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
        sections=[sector_detail],  # the navigable content tree
    )
