"""Tests for the rendering-agnostic content model + portal renderers + the
taxonomy layers (classifications / labels).

Most tests are pure-logic and need no database — they build a Report by hand
and exercise the model, both renderers, and the taxonomy helpers. A few
DB-backed smoke tests (guarded by the standard test-DB fixtures) confirm
build_report executes against the real schema.
"""

import json

import psycopg2
import pytest

import classifications
import labels
import report_model as rm
from report_render_html import render_html, _inline_md
from report_render_markdown import render_markdown


# --------------------------------------------------------------------------
# A hand-built Report exercising every section kind / facet / metric the
# renderers branch on. No DB.
# --------------------------------------------------------------------------

def _sample_report() -> rm.Report:
    from datetime import date, datetime
    series = [rm.SeriesPoint(period=date(2025, 1, 1), value=1.0),
              rm.SeriesPoint(period=date(2025, 2, 1), value=2.0)]
    deficit_ind = rm.Indicator(
        key="eu_china_deficit_per_day", label="EU-27 deficit", value=9.4e8,
        unit="eur_per_day", formatted="€939M/day", chart="sparkline",
        delta={"value": 0.065, "direction": "wider", "formatted": "+6.5% YoY"},
        chart_data=rm.ChartData(chart_type="sparkline", series=series),
        provenance=rm.Provenance(finding_ids=[1], source="eurostat", as_of=date(2026, 4, 1)),
    )
    level_ind = rm.Indicator(
        key="eu_china_imports_12mo", label="EU-27 imports (12mo)", value=5.6e11,
        unit="eur", formatted="€561.37B", chart="bignumber",
        provenance=rm.Provenance(finding_ids=[1], source="eurostat", as_of=date(2026, 4, 1)),
    )
    donut_ind = rm.Indicator(
        key="china_import_share", label="China share of EU imports", value=0.23,
        unit="share", formatted="23%", chart="donut",
        provenance=rm.Provenance(source="eurostat", as_of=date(2026, 4, 1)),
    )
    headline = rm.Headline(
        variant="eurostat", lead_title="What April changed", note="note.",
        items=[rm.HeadlineItem(
            subject={"scope": "eu_27", "flow": "export", "group_name": "Cars"},
            metrics={"direction": "fell", "pct": 0.4, "value_eur": 7.5e9},
            stability={"badge": "🟡", "hedge_phrase": None},
            prose="**EU-27 exports of [Cars](#cars) to China** fell 40% `finding/2`",
            drill_down="cars",
            provenance=rm.Provenance(finding_ids=[2], source="eurostat"),
            facets=rm.Facets(commodity=["Cars"], theme=["EV supply chain"]),
            take=rm.LLMSlot(
                slot_type="specific", grounded_in=[2], status="generated",
                questions=[
                    {"q": "Is the 40% fall volume-driven? (NB: hypothesis, not a finding)",
                     "axis": "volume-vs-value"},
                    {"q": "Is one member state behind the drop?",
                     "axis": "concentration"},
                ]))],
        llm_slots=[rm.LLMSlot(slot_type="general", grounded_in=[2])],
    )
    what_changed = rm.WhatChanged(
        regime="movement", summary="3 findings shifted.", new_count=49,
        significant=[
            rm.Shift(group_name="Steel", subkind="hs_group_yoy_export",
                     window_end="2026-04-01", old_yoy=0.12, new_yoy=-0.04,
                     direction_flipped=True),
            rm.Shift(group_name="EV batteries", subkind="hs_group_yoy",
                     window_end="2026-04-01", old_yoy=0.30, new_yoy=0.52,
                     direction_flipped=False),
        ])
    state = rm.Section(
        id="state-of-play", title="State of play", kind="state_of_play",
        sections=[rm.Section(
            id="the-deficit", title="The deficit", kind="state_of_play",
            findings=[rm.Finding(
                finding_id=1, subkind="trade_balance", title="EU-27 deficit",
                metrics={"scope": "EU-27", "deficit_eur": 3.4e11, "per_day_eur": 9.4e8,
                         "yoy_pct": 0.065, "cn_per_day_eur": 9.9e8, "cn_finding": 9},
                provenance=rm.Provenance(finding_ids=[1, 9]))])])
    mirror = rm.Section(
        id="mirror-gaps", title="Mirror-trade gaps", kind="mirror_gap",
        findings=[rm.Finding(
            finding_id=3, subkind="mirror_gap", title="China ↔ NL",
            metrics={"partner": "NL", "gacc_eur": 7.3e9, "eurostat_eur": 9.2e9,
                     "gap_eur": 1.8e9, "gap_pct": 0.20, "excess_pct": 0.135,
                     "hub": "NL", "hub_notes": "Rotterdam.",
                     "zscore": 2.1, "zscore_period": "2025-11"},
            provenance=rm.Provenance(finding_ids=[3, 8], source="cross_source"))])
    group = rm.Section(
        id="cars", title="Cars", kind="sector_detail", intro="Passenger cars.",
        about="**How to read each group.** Every figure is a 12-month total.\n"
              "\n- value vs volume\n- low base means quote the € amount",
        facets=rm.Facets(commodity=["Cars"], sector=["78"], theme=["EV supply chain"],
                         end_use=["Consumption"]),
        metrics={"china_share_value": 0.19, "china_share_kg": 0.24,
                 "china_share_finding": 4,
                 "top_cn8": [{"code": "87038010", "eur": 5.0e9}],
                 "reporters": [{"reporter": "DE", "share": 0.66, "yoy": -0.1}],
                 "trajectory": {"EU-27": {"import": "volatile", "export": "peak-and-fall"},
                                "UK": {"import": "volatile"}},
                 "trajectory_findings": [5, 6],
                 "china_export_share_value": 0.015, "china_export_share_finding": 7,
                 "predictability": {"badge": "🟡", "persistence_pct": 0.5, "n": 4},
                 "section": {"code": "7", "title": "Machinery & transport"}},
        findings=[rm.Finding(
            finding_id=2, subkind="hs_group_yoy_export", title="EU-27 exports of Cars",
            metrics={"scope": "EU-27", "flow": "export", "yoy_pct": -0.4,
                     "current_eur": 7.5e9, "low_base": False,
                     "sm_yoy_pct": -0.62, "sm_yoy_pct_kg": -0.5,
                     "sm_period": "2026-04-01"},
            chart_data=rm.ChartData(chart_type="line", series=series),
            provenance=rm.Provenance(finding_ids=[2])),
            rm.Finding(
            finding_id=12, subkind="hs_group_yoy", title="EU-27 imports of Cars",
            metrics={"scope": "EU-27", "flow": "import", "yoy_pct": 0.23,
                     "current_eur": 1.6e10, "low_base": False,
                     "sm_yoy_pct": 0.82, "sm_period": "2026-04-01",
                     "caveats": ["partial_window"]},
            chart_data=rm.ChartData(chart_type="line", series=series),
            provenance=rm.Provenance(finding_ids=[12]))])
    sector = rm.Section(id="sector-detail", title="Sector detail", kind="sector_detail",
                        intro="Every group.",
                        about="Reading the numbers: value vs volume; **low base** "
                              "means quote the € amount.\n\n- 12-month vs latest month",
                        metrics={"section_index": [
                            {"code": "7", "title": "Machinery & transport",
                             "value": 1.6e10, "count": 1}]},
                        sections=[group])
    structural = rm.Section(
        id="trade-map", title="Trade map", kind="structural", intro="By division.",
        provenance=rm.Provenance(source="eurostat", as_of=date(2026, 4, 1)),
        metrics={"total_codes": 199, "divisions": 2, "total_eur": 5.5e11},
        sections=[rm.Section(
            id="sitc-78", title="Road vehicles", kind="structural",
            facets=rm.Facets(sector=["78"]),
            provenance=rm.Provenance(source="eurostat", as_of=date(2026, 4, 1)),
            metrics={"value_share": 0.9999, "covered_share": 1.0, "value_eur": 5.5e11,
                     "code_count": 197, "groups": [{"name": "Cars", "slug": "cars"}]}),
            rm.Section(
            id="sitc-unclassified", title="Unclassified (no SITC division)",
            kind="structural", facets=rm.Facets(sector=[]),
            provenance=rm.Provenance(source="eurostat", as_of=date(2026, 4, 1)),
            metrics={"value_share": 0.0001, "covered_share": 0.0, "value_eur": 5.5e7,
                     "code_count": 2, "groups": []})])
    reference = rm.Section(
        id="methodology", title="Methodology & caveats", kind="reference",
        intro="How to read.",
        about="Every value is a rolling 12-month total. See [methodology.md §2]"
              "(methodology.md#scopes) for detail.",
        metrics={"caveats": [{"code": "cif_fob", "summary": "CIF vs FOB", "detail": "…"}],
                 "guides": [{"title": "The three comparison scopes",
                             "body": "EU-27 is **Eurostat**.\n\n- UK is HMRC"}]})
    sources = rm.Section(
        id="sources", title="Sources & coverage", kind="sources",
        intro="What this rests on.",
        metrics={"sources": [{"source": "eurostat", "note": "Comext."}],
                 "coverage": [{"source": "eurostat", "start": "2017-01-01",
                               "end": "2026-04-01", "releases": 111,
                               "last_updated": "2026-06-15"}],
                 "new_findings": [
                     {"subkind": "hs_group_yoy",
                      "label": "year-on-year change for an HS group", "count": 44},
                     {"subkind": "mirror_gap", "label": "mirror-trade gap",
                      "count": 5}],
                 "new_findings_total": 49,
                 "manifest": [{"family": "HS-group year-on-year (price & volume)",
                               "count": 3509}],
                 "manifest_total": 3509,
                 "appendix": [{"source": "eurostat", "total": 111, "recent": [
                     {"period": "2026-04-01", "title": "April 2026",
                      "url": "https://ec.europa.eu/eurostat/x",
                      "fetched": "2026-06-01"}]}]})
    glossary = rm.Section(
        id="glossary", title="Glossary", kind="glossary",
        intro="Definitions.",
        metrics={"groups": [{"title": "Economic & data terms", "terms": [
            {"term": "CIF / FOB", "body": "CIF includes freight.\n\n- FOB does not"},
            {"term": "Mirror gap", "body": "The two sides' books differ."}]}]})
    data = rm.Section(
        id="tables", title="Tables", kind="data", intro="The findings as tables.",
        metrics={"tables": [
            {"name": "summary", "description": "one row per group",
             "headers": ["group", "yoy"], "rows": [["Cars", 0.23], ["Steel", -0.1]],
             "total_rows": 2, "shown_rows": 2, "inline": True},
            {"name": "hs_yoy_imports", "description": "full detail",
             "headers": ["group"], "rows": [], "total_rows": 3509,
             "shown_rows": 0, "inline": False}]})
    gacc_bi = rm.Section(
        id="gacc-bilateral", title="China’s trade by partner (GACC)",
        kind="gacc_bilateral", intro="By partner.",
        sections=[rm.Section(
            id="gacc-united-states", title="United States", kind="gacc_bilateral",
            findings=[rm.Finding(
                finding_id=10, subkind="gacc_bilateral_aggregate_yoy",
                title="China exports to US",
                metrics={"scope": "China", "flow": "export", "yoy_pct": -0.297,
                         "current_eur": 4.6e11, "sm_yoy_pct": 0.162,
                         "sm_eur": 7.3e9, "ytd_pct": 0.125, "ytd_eur": 3.32e10,
                         "ytd_months": 5, "window_label": "12 months to May 2026",
                         "note": ("Incomplete window — missing January 2026 from "
                                  "the current 12-month window"),
                         "caveats": ["partial_window"]},
                provenance=rm.Provenance(finding_ids=[10], source="gacc"))])])
    meta = rm.ReportMeta(data_period=date(2026, 4, 1), variant="eurostat",
                         snapshot_id="t", generated_at=datetime(2026, 6, 20, 12, 0))
    return rm.Report(meta=meta,
                     key_indicators=[deficit_ind, level_ind, donut_ind],
                     headline=headline, what_changed=what_changed,
                     sections=[state, mirror, sector, structural, gacc_bi,
                               sources, data, reference, glossary])


# ---- model serialisation ----

def test_serialisation_roundtrips_and_dates_are_iso():
    r = _sample_report()
    js = rm.to_json(r)
    d = json.loads(js)
    assert d["meta"]["data_period"] == "2026-04-01"   # date -> ISO
    assert d["meta"]["schema_version"] == rm.SCHEMA_VERSION
    assert d["key_indicators"][0]["chart_data"]["series"][0]["period"] == "2025-01-01"
    assert rm.to_dict(r)["headline"]["variant"] == "eurostat"


# ---- renderers exercise every branch without crashing ----

def test_markdown_renders_all_sections():
    md = render_markdown(_sample_report())
    for marker in ("# Headlines", "## Key indicators", "## State of play",
                   "## Mirror-trade gaps", "## Sector detail", "## Trade map",
                   "## Methodology & caveats", "## Sources & coverage",
                   "## China’s trade by partner (GACC)"):
        assert marker in md, marker
    assert "China only, excl. HK/Macao" in md  # cn-only deficit = Eurostat CN-only, NOT GACC
    assert "China reports" in md          # mirror-gap (GACC vs Eurostat) — the one place "China reports" is right
    assert "2025-11" in md and "σ" in md  # mirror-gap z-score (period + sigma)
    assert "China takes 1.5%" in md       # export share
    assert "Trajectory —" in md           # multi-scope trajectory
    assert "finding/" in md               # citations


def test_html_renders_all_sections_and_is_self_contained():
    h = render_html(_sample_report())
    assert h.startswith("<!doctype html")
    assert 'id="cars"' in h               # drill-down anchor
    assert 'class="mg"' in h              # mirror gaps
    assert 'class="tmrow"' in h           # trade map
    assert 'id="sector-filter"' in h     # filter input
    assert "addEventListener('input'" in h  # filter JS embedded
    assert "Rotterdam" in h               # transshipment hub note
    assert "China only, excl. HK/Macao" in h   # cn-only deficit = Eurostat CN-only, NOT GACC
    assert "China reports" in h           # mirror-gap (GACC vs Eurostat) — the one place "China reports" is right
    assert 'class="see-also"' in h and 'href="#brief-gacc_bilateral"' in h  # State-of-play → Trading-partners bridge
    assert "EV supply chain" in h         # theme pill/chip


def test_inline_md_handles_link_nested_in_bold():
    # the bug: a [link](#x) inside **bold** must not strand the link text
    out = _inline_md("**EU exports of [Cars](#cars) to China** `finding/2`")
    assert '<a href="#cars">Cars</a>' in out
    assert "<strong>" in out
    assert "\x00" not in out              # no unrestored placeholder


def test_structural_section_is_attributed_in_both_renderers():
    """The trade-map aggregates have no per-code finding, so the section must
    carry its own source/as-of and the renderers must surface it — numbers stay
    attributable (global principle 7). The unclassified remainder is shown, not
    silently dropped."""
    r = _sample_report()
    md = render_markdown(r)
    html = render_html(r)
    assert "Source: Eurostat" in md           # display name, not the raw code
    assert "as of Apr 2026" in md              # data month, not a raw ISO day
    assert "live aggregate, no per-code finding" in md
    assert "Unclassified (no SITC division)" in md   # partition remainder
    assert 'class="source"' in html
    # the section-level provenance survives serialisation
    d = rm.to_dict(r)
    tm = next(s for s in d["sections"] if s["kind"] == "structural")
    assert tm["provenance"]["source"] == "eurostat"
    assert tm["provenance"]["as_of"] == "2026-04-01"


def test_markdown_sector_headings_carry_explicit_anchors():
    """Headline drill-downs target the group slug; the markdown heading carries
    an explicit <a id> (the model's slug) so the link resolves without relying
    on the host engine's auto-slug rule — the failure class fixed for docx."""
    md = render_markdown(_sample_report())
    assert '<a id="cars"></a>' in md


def test_fmt_eur_shared_handles_trillions():
    """The single shared formatter (no per-renderer copies) covers the €T tier
    the GACC macro needs, so the same value reads identically everywhere."""
    from briefing_pack._helpers import _fmt_eur
    assert _fmt_eur(1.2e12) == "€1.20T"
    assert _fmt_eur(4.6e11) == "€460.00B"
    assert _fmt_eur(None) == "—"


# ---- tabbed portal + the restored Findings-doc surfaces (no DB) ----

def test_html_is_tabbed_and_routes_sections():
    """The page is tabbed; data → Tables, reference → Methodology, glossary →
    Glossary; everything else → Briefing. Tab router JS is embedded; degrades to
    plain anchored panels with no JS."""
    h = render_html(_sample_report())
    assert 'class="tabs"' in h
    for href in ("#tab-briefing", "#tab-tables", "#tab-sources",
                 "#tab-methodology", "#tab-glossary"):
        assert f'href="{href}"' in h, href
    for pid in ("tab-briefing", "tab-tables", "tab-sources",
                "tab-methodology", "tab-glossary"):
        assert f'id="{pid}"' in h, pid
    assert "hashchange" in h               # tab router present
    assert 'class="badge"' not in h        # no count badges in tab names


def test_more_about_is_a_collapsed_disclosure():
    h = render_html(_sample_report())
    assert '<details class="more">' in h
    assert "More about this section" in h
    assert "Reading the numbers" in h      # the sector about copy
    md = render_markdown(_sample_report())
    assert "More about this section" in md


def test_key_indicators_level_and_donut_render():
    h = render_html(_sample_report())
    assert "€561.37B" in h                 # bignumber level
    assert 'class="donut"' in h            # part-of-whole donut
    assert ">23%</text>" in h              # donut centre percentage
    assert "€561.37B" in render_markdown(_sample_report())


def test_latest_month_register_in_sector_rows():
    assert "latest mo" in render_html(_sample_report())
    assert "latest mo" in render_markdown(_sample_report())


def test_sector_group_charts_line_and_bar_side_by_side():
    h = render_html(_sample_report())
    assert 'class="chart-row"' in h        # side-by-side container (wide viewports)
    assert 'class="chartcard"' in h and 'class="cc-meta"' in h  # meta-left card
    assert "latest 12 months" in h         # line legend
    assert ">Imports<" in h and ">Exports<" in h  # the imports-vs-exports bar
    # charts are labelled by their group, not a generic repeated headline
    assert "Cars: EU-27 imports from China" in h
    assert "Cars: imports vs exports" in h


def test_headline_movers_carry_theme_chips():
    """Each mover shows its group's theme chips, clickable to filter Sector
    detail (the mover-chip marker drives the scroll-into-view)."""
    h = render_html(_sample_report())
    hi = h.index('class="movers"')
    nxt = h.index("</ol>", hi)
    movers = h[hi:nxt]
    assert 'class="chip mover-chip"' in movers and "EV supply chain" in movers
    assert 'data-q="ev supply chain"' in movers      # wired to the sector filter
    assert "mover-chip" in h and "scrollIntoView" in h  # JS scroll on mover-chip


def test_drilldown_expands_target_sector_detail():
    """A mover's 'detail ›' drill-down auto-opens the target group's collapsed
    charts/detail (router JS wired to expand on navigation)."""
    h = render_html(_sample_report())
    assert "function expandDetail" in h
    assert "details.gdetail" in h and "expandDetail(el)" in h


def test_sector_group_deep_detail_behind_expander():
    """Charts + top products + drivers + trajectory collapse behind a per-group
    'Show detail & charts' expander; the flow rows stay visible."""
    h = render_html(_sample_report())
    assert 'class="gdetail"' in h and "Show detail" in h
    assert 'class="chart-row"' in h and "Top products" in h  # inside the expander


def test_sector_group_predictability_badge():
    """The 🟢/🟡/🔴 badge the explainer describes actually renders beside each
    group heading (with a tooltip), and the label joins the filter index."""
    h = render_html(_sample_report())
    assert 'class="pred"' in h and "🟡" in h
    assert 'data-name="cars' in h and "mixed" in h   # 'mixed' filterable
    assert "#### Cars 🟡" in render_markdown(_sample_report())


def test_sector_detail_grouped_by_sitc_section_with_subheads():
    """Groups carry a section subhead (data-section for filter auto-hide); the
    subhead names the SITC section and its combined value."""
    h = render_html(_sample_report())
    assert 'class="sec-head"' in h and 'data-section="7"' in h
    assert "Machinery &amp; transport" in h          # section title in the subhead
    assert 'class="sector"' in h and 'data-section="7"' in h  # group tagged too
    md = render_markdown(_sample_report())
    assert "### Machinery & transport" in md


def test_primary_section_heuristic():
    from report_builder import _primary_section
    assert _primary_section(["78"])[0] == "7"          # single division → its section
    assert _primary_section(["73", "77", "51"])[0] == "7"  # mode of sections
    assert _primary_section([])[0] == "9"              # none → Other/unclassified


def test_per_row_caveat_flags():
    h = render_html(_sample_report())
    assert 'class="flow-cav"' in h and "partial window" in h   # humanised code
    assert "(partial window)" in render_markdown(_sample_report())


def test_new_findings_breakdown_lives_in_sources_not_what_changed():
    """The per-type new-findings tally is bookkeeping, so it sits in Sources &
    coverage (by Period coverage), not in What changed."""
    h = render_html(_sample_report())
    # in the Sources tab
    si = h.index('id="tab-sources"')
    assert "New this cycle" in h and h.index("New this cycle") > si
    assert "year-on-year change for an HS group" in h and ">44</strong> new" in h
    # NOT in the What-changed block (which keeps only the digest)
    wi = h.index("What changed since the last pack")
    assert "New this cycle" not in h[wi:si]
    md = render_markdown(_sample_report())
    assert "**New this cycle**" in md and "44 new — year-on-year change" in md


def test_masthead_carries_badge_period_and_first_sentence_tooltip():
    """Period + source badge live in the masthead (no separate subbar); the
    badge's tooltip is the note's first sentence and the boilerplate second
    sentence is dropped."""
    import dataclasses
    r = _sample_report()
    r = dataclasses.replace(r, headline=dataclasses.replace(
        r.headline,
        note="Triggered by new Eurostat data. Boilerplate second sentence here."))
    h = render_html(r)
    assert '<div class="subbar">' not in h and "note-line" not in h
    mast = h[h.index('class="masthead"'):h.index("</header>")]
    assert "Data to April 2026" in mast
    # tooltip = first sentence + when we received this source's latest data
    # (sample appendix fetched 2026-06-01); boilerplate second sentence dropped
    assert ('class="tag" title="Triggered by new Eurostat data. '
            'Received 1 Jun 2026.">eurostat</span>') in mast
    assert "Boilerplate second sentence" not in h


def test_about_this_site_box_sits_above_standout_moves():
    """A page-level 'About this site' disclosure renders in the Briefing, between
    the KPI band and the Standout-moves lead."""
    h = render_html(_sample_report())
    assert "About this site</summary>" in h and "about-site" in h
    assert "Harmonised System (HS)" in h          # the HS-scope copy
    i_kpi, i_about = h.find('class="kpis"'), h.find("about-site")
    i_moves = h.find('class="lead"')              # the headline lead H2
    assert i_kpi < i_about < i_moves


def test_source_received_date_falls_back_gracefully():
    """The badge tooltip omits the received-date (rather than erroring) when the
    sources section or its fetch date is missing."""
    from report_render_html import _source_received_date
    assert _source_received_date(None, "eurostat") is None
    r = _sample_report()
    src = next(s for s in r.sections if s.kind == "sources")
    assert _source_received_date(src, "eurostat") == "1 Jun 2026"
    assert _source_received_date(src, "nonesuch") is None


def test_what_changed_demotes_to_one_liner_on_quiet_cycle():
    """No material change (no new findings, no significant shifts) → What changed
    renders as a slim one-liner: no H2 section, no sub-nav entry, so it doesn't
    claim vertical weight near the top of the Briefing. The 'nothing changed'
    note is still said."""
    import dataclasses
    r = dataclasses.replace(_sample_report(), what_changed=rm.WhatChanged(
        regime="no_change", summary="(unused on the web)", new_count=0,
        significant=[]))
    h = render_html(r)
    assert 'class="quiet-change"' in h and "nothing moved materially" in h
    assert "What changed since the last pack" not in h     # no H2 section
    assert 'data-spy="brief-changed"' not in h             # no sub-nav entry
    assert 'id="brief-changed"' not in h
    # the material case (sample carries significant shifts) gets the full section + nav
    full = render_html(_sample_report())
    assert "What changed since the last pack" in full and 'data-spy="brief-changed"' in full


def test_what_changed_renders_the_material_shifts():
    """B: 'What changed' surfaces the actual shift list — group, old→new YoY,
    pp delta, flip marker — not a bare count of new findings, and with no stray
    'Tier 1' reference (a docx-only concept)."""
    h = render_html(_sample_report())
    assert "moved materially" in h and "1 of them flipping direction" in h
    assert "Steel" in h and "EV batteries" in h
    assert "+12.0% → −4.0%" in h          # old → new YoY arc (typographic minus)
    assert "🔄 flipped" in h               # direction-flip marker
    assert "Tier 1" not in h               # the docx leftover is gone
    md = render_markdown(_sample_report())
    assert "Steel" in md and "+12.0% → −4.0%" in md and "🔄 **flipped**" in md
    assert "Tier 1" not in md


def test_sources_release_appendix():
    r = _sample_report()
    h = render_html(r)
    assert "Release appendix" in h
    assert "ec.europa.eu/eurostat/x" in h and "fetched 2026-06-01" in h
    md = render_markdown(r)
    assert "Release appendix" in md and "ec.europa.eu/eurostat/x" in md


def test_glossary_renders_in_both_surfaces():
    r = _sample_report()
    h = render_html(r)
    assert 'class="gloss-item"' in h and 'id="glossary-filter"' in h
    assert "CIF / FOB" in h and "CIF includes freight" in h
    # groups are nested <section>s; their padding is stripped so glossary text
    # doesn't inset twice as far as every other tab.
    assert ".gloss-group{margin:0 0 8px;padding:0}" in h
    md = render_markdown(r)
    assert "## Glossary" in md and "**CIF / FOB**" in md


def test_glossary_web_hides_docx_bundle_terms():
    """The portal Glossary is parsed from the shared docs/glossary.md, which
    also serves the docx bundle. Terms marked `<!--web-hide-->` (the bundle's
    Tier 1/2/3, 02_Findings.md, provenance files, etc.) must NOT reach the web
    surface — a web reader can't open those artifacts — while web-relevant
    terms stay and no surviving body references a bundle filename."""
    import report_builder as rb
    groups = rb._parse_glossary_md(rb._GLOSSARY_PATH.read_text(encoding="utf-8"))
    terms = {t["term"] for g in groups for t in g["terms"]}
    for hidden in ("Brief / findings document", "Tier 1 / 2 / 3", "Front page",
                   "Lead scaffold", "Provenance file", "Groups glossary"):
        assert hidden not in terms, f"web glossary leaks docx-bundle term: {hidden}"
    assert {"CIF / FOB", "Mirror trade / mirror gap",
            "Finding ID / trace token"} <= terms          # web-relevant terms survive
    for g in groups:
        for t in g["terms"]:
            assert "web-hide" not in t["body"]             # marker never leaks into copy
            for fname in ("02_Findings.md", "03_Leads.md", "04_Data.xlsx",
                          "05_Groups.md", ".docx"):
                assert fname not in t["body"], f"{t['term']} still cites {fname}"

    # The marker is purely a parser directive — verify it drops only the marked
    # term and nothing around it.
    parsed = rb._parse_glossary_md(
        "## Cat\n### Shown\nbody A\n### Hidden <!--web-hide-->\nbody B\n"
        "### Also shown\nbody C\n"
    )
    assert {t["term"] for t in parsed[0]["terms"]} == {"Shown", "Also shown"}


def test_tables_tab_inline_and_download_only():
    r = _sample_report()
    h = render_html(r)
    assert "Download Excel workbook" in h and 'href="data.xlsx"' in h
    assert "Copy as TSV" in h and 'class="dtable"' in h
    # download + copy are same-size buttons grouped per-table (no big top CTA)
    assert 'class="dt-actions"' in h and 'class="data-toolbar"' not in h
    assert h.count("btn-sm") >= 2          # both buttons are the small size
    assert ">Cars<" in h                   # an inline cell
    assert "hs_yoy_imports" in h and "3,509 rows" in h  # download-only, count shown
    md = render_markdown(r)
    assert "## Tables" in md and "hs_yoy_imports" in md


def test_gacc_bilateral_per_partner_expanders():
    """Progressive disclosure: each partner is a collapsed <details> button with
    a headline figure, expanding to its flows on click."""
    h = render_html(_sample_report())
    assert 'class="partner"' in h and "<summary>" in h
    assert "United States" in h
    assert "China's exports" in h and "€460.00B" in h   # headline in the summary
    md = render_markdown(_sample_report())               # LLM surface keeps it flat
    assert "## China’s trade by partner (GACC)" in md


def test_gacc_bilateral_expanded_panel_restores_ytd_window_and_caveat_prose():
    """The expanded partner panel carries the richer registers the 12-month
    headline drops: window orientation (once), a YTD + latest-month-value
    sub-line per flow, the latest-month register on the row, and one plain-prose
    incomplete-window note (not just the cryptic chip). Parity in markdown."""
    h = render_html(_sample_report())
    assert "12 months to May 2026" in h            # window orientation, once
    assert "latest mo +16%" in h                    # latest-month register on row
    assert "YTD (5-mo): +12.5% · €33.20B" in h      # YTD sub-line
    assert "latest month: €7.30B" in h              # latest-month value
    assert "Incomplete window — missing January 2026" in h   # prose, not chip
    # window + note appear once, not duplicated per flow
    assert h.count("12 months to May 2026") == 1
    md = render_markdown(_sample_report())
    assert "*12 months to May 2026*" in md
    assert "YTD (5-mo): +12.5% · €33.20B" in md
    assert "Incomplete window — missing January 2026" in md


def test_gacc_bilateral_partner_balance_row():
    """Per-partner net balance (China's exports − imports) on the same
    12-month/YTD windows as the flow rows: sign-aware label, magnitude-based
    YoY (so a widening deficit reads as +%, not a misleading −%), and a € swing
    in place of % when the prior balance flips sign or is near zero."""
    import report_builder as rb
    from report_render_html import _bilateral_balance_row

    def _partner(name, exp, imp):
        flows, fs = {}, []
        for flow, (c, p, yc, yp, ym) in (("export", exp), ("import", imp)):
            flows[flow] = {"cur12": c, "prior12": p, "ytd_cur": yc,
                           "ytd_prior": yp, "ytd_months": ym}
            fs.append(rm.Finding(
                finding_id=len(fs) + 1, subkind="gacc_bilateral_aggregate_yoy",
                title="t", metrics={"scope": "China", "flow": flow},
                provenance=rm.Provenance(finding_ids=[len(fs) + 10], source="gacc")))
        return rm.Section(id="s" + name, title=name, kind="gacc_bilateral",
                          findings=fs, metrics=rb._partner_balance(flows))

    # China surplus, both sides same sign — straightforward magnitude %.
    de = _bilateral_balance_row(
        _partner("Germany", (110e9, 100e9, 40e9, 38e9, 4),
                 (90e9, 88e9, 33e9, 32e9, 4)))
    assert "s surplus" in de and "Germany" in de and "s deficit" in de
    assert "+66.7% · €20.00B" in de                       # (110−90) vs (100−88)
    assert de.count('class="token">finding/') == 2        # drillable to both flows

    # China deficit (commodity exporter): a widening deficit must read +%, green.
    br = _bilateral_balance_row(
        _partner("Brazil", (60e9, 58e9, 22e9, 21e9, 4),
                 (95e9, 90e9, 35e9, 33e9, 4)))
    assert "s deficit" in br and "Brazil" in br and "s surplus" in br
    assert "+9.4% · €35.00B" in br                         # |−35| vs |−32|, widened
    assert "#22874d" in br                                 # green: the figure rose

    # Sign flip across a near-zero prior — % suppressed, € swing shown instead.
    nl = _bilateral_balance_row(
        _partner("Netherlands", (120e9, 80e9, 45e9, 30e9, 4),
                 (118e9, 82e9, 44e9, 31e9, 4)))
    assert "+€4.00B YoY" in nl and "€2.00B" in nl          # +2 net, swung from −2
    assert "%" not in nl.split("flow-val")[1].split("</span>")[0]

    # No balance when a partner has only one flow (nothing to net).
    solo = rm.Section(id="x", title="X", kind="gacc_bilateral",
                      findings=[], metrics={})
    assert _bilateral_balance_row(solo) == ""


def test_jump_targets_clear_sticky_bar_and_get_highlight():
    """Drill-down/Trade-Map jumps must clear the sticky tab bar (unconditional
    scroll-margin, not scoped to :target, since the JS preventDefaults) and the
    landed-on block must get the highlight via a JS-applied .jumped class."""
    h = render_html(_sample_report())
    # offset applies to the elements themselves, not only :target (clears the
    # sticky sub-nav now that the main tabs are no longer sticky)
    assert "scroll-margin-top:52px" in h
    assert ".sector.jumped" in h and "background:#dcebfa" in h
    # the JS stand-in for native :target is wired into both jump paths
    assert "function mark(" in h
    assert "mark(el)" in h


def test_briefing_subnav_is_the_sticky_element_not_the_tabs():
    """The Briefing gets a sticky in-page sub-nav (Top + its sections); the main
    tabs are NOT sticky, so only one bar occupies the top at a time."""
    h = render_html(_sample_report())
    assert '<nav class="subnav"' in h
    assert 'class="subnav-top" href="#top"' in h          # Top → masthead
    assert 'id="top"' in h                                  # the masthead anchor
    # the section anchors + their sub-nav links
    for anchor, label in (("brief-state_of_play", "State of play"),
                          ("brief-mirror_gap", "Mirror gaps"),
                          ("brief-sector_detail", "Sector detail")):
        assert f'id="{anchor}"' in h
        assert f'data-spy="{anchor}"' in h
    # the sub-nav is the sticky one; the tab bar is not
    assert ".subnav{position:sticky;top:0" in h
    assert "position:sticky;top:0;z-index:5" not in h      # old sticky .tabs gone
    # immediate active-on-click + scroll-spy wiring
    assert "new IntersectionObserver" in h
    assert "a.classList.add('active')" in h


def test_methodology_tab_shows_about_and_guides():
    r = _sample_report()
    h = render_html(r)
    assert "The three comparison scopes" in h    # a guide
    assert "Caveats" in h                          # caveats stay in Methodology
    assert "The three comparison scopes" in render_markdown(r)


def test_sources_tab_groups_provenance_and_trade_map():
    """Sources & coverage = data sources + period coverage + findings manifest,
    plus the Trade Map (moved off Briefing) in the SAME tab."""
    r = _sample_report()
    h = render_html(r)
    assert 'id="tab-sources"' in h
    assert "Data sources" in h and "Period coverage" in h
    assert "Last updated" in h and "2026-06-15" in h     # coverage-table freshness column
    assert "Findings included" in h and "3,509" in h     # humanised manifest
    assert "Apr 2026" in h                                # coverage end as a month, not raw ISO
    # the Trade Map renders inside the Sources tab, not Briefing
    si = h.index('id="tab-sources"')
    assert 'class="tmrow"' in h
    assert h.index('class="tmrow"') > si                  # tmrow sits in the sources panel
    assert "Road vehicles" in h                            # a division title
    md = render_markdown(r)
    assert "## Sources & coverage" in md and "111 releases" in md


def test_md_blocks_to_html_paragraphs_bullets_inline():
    from report_render_html import _md_blocks_to_html
    out = _md_blocks_to_html("A **bold** line.\n\n- one\n- two")
    assert "<p>A <strong>bold</strong> line.</p>" in out
    assert "<ul><li>one</li><li>two</li></ul>" in out


def test_inline_md_drops_dead_internal_link_to_text():
    out = _inline_md("see [methodology.md §2](methodology.md#scopes) here")
    assert "methodology.md §2" in out      # text kept
    assert "href=" not in out              # dead cross-doc target dropped


def test_line_chart_two_tone_split_with_axes():
    from report_render_html import _line_chart_svg
    from datetime import date as d
    pts = [rm.SeriesPoint(period=d(2024, m % 12 + 1, 1), value=float(m))
           for m in range(24)]
    svg = _line_chart_svg(rm.ChartData(chart_type="line", series=pts))
    assert "<svg" in svg
    assert svg.count("polyline") == 2      # prior (grey) + current (red) segments
    # axes: ≥3 y + ≥3 x labels, and gridlines (horizontal + intermediate vertical)
    assert svg.count("<text") >= 6
    assert svg.count("<line") >= 6


def test_x_tick_indices_show_intermediate_dates():
    from report_render_html import _x_tick_indices
    t = _x_tick_indices(111)               # ~9-year deficit → year-ish steps
    assert t[0] == 0 and t[-1] == 110 and 4 <= len(t) <= 7  # not just two ends
    t2 = _x_tick_indices(24)               # 2-year sector → 6-month steps
    assert t2[0] == 0 and t2[-1] == 23 and len(t2) >= 4
    assert _x_tick_indices(2) == [0, 1]


def test_bar_chart_zero_based_and_labelled():
    from report_render_html import _bar_chart_svg
    svg = _bar_chart_svg([{"label": "Imports", "value": 1.6e10},
                          {"label": "Exports", "value": 7.5e9}])
    assert svg.count("<rect") == 2
    assert ">Imports<" in svg and ">Exports<" in svg
    assert "€16.00B" in svg                 # value label on the bar
    assert _bar_chart_svg([]) == ""


def test_chart_card_puts_meta_left_of_plot():
    from report_render_html import _chart_card
    out = _chart_card("Title", "€5B", "legend", "<svg></svg>")
    assert 'class="cc-meta"' in out and 'class="cc-plot"' in out
    assert "Title" in out and "€5B" in out and "legend" in out
    assert _chart_card("T", "", "", "") == ""   # no svg → nothing


def test_container_gauge_two_fills_and_highlight_band():
    from report_render_html import _container_gauge_svg, _GUARDIAN_BLUE, _SHIP_BASE
    svg = _container_gauge_svg(0.135, n=24)
    assert "<svg" in svg and svg.count("<rect") >= 24      # 24 containers (+ funnel/bridge)
    assert _GUARDIAN_BLUE in svg and _SHIP_BASE in svg     # exactly two fills
    assert svg.count(_GUARDIAN_BLUE) == 3                  # round(0.135*24) highlighted


def test_mirror_gap_pictograph_only_when_excess_material():
    h = render_html(_sample_report())                      # NL excess 13.5% → shown
    assert 'class="ship"' in h
    assert "beyond what China" in h and "own export figures" in h  # honest caption


def test_donut_svg_clamps_and_labels():
    from report_render_html import _donut_svg
    assert "50%" in _donut_svg(0.5)
    assert "100%" in _donut_svg(1.7)       # clamped to 1.0
    assert "0%" in _donut_svg(-1)          # clamped to 0.0


def test_parse_glossary_md_groups_and_terms():
    from report_builder import _parse_glossary_md
    text = ("# Glossary\nintro\n## Cat A\n### Term1\nbody1 line\n### Term2\n"
            "body2\n## Cat B\n### T3\nb3")
    groups = _parse_glossary_md(text)
    assert [g["title"] for g in groups] == ["Cat A", "Cat B"]
    assert groups[0]["terms"][0] == {"term": "Term1", "body": "body1 line"}
    assert len(groups[0]["terms"]) == 2


def test_jsonable_cell_coerces_for_snapshot():
    from report_builder import _jsonable_cell
    from decimal import Decimal
    from datetime import date as d
    assert _jsonable_cell(Decimal("1.5")) == 1.5
    assert _jsonable_cell(d(2026, 4, 1)) == "2026-04-01"
    assert _jsonable_cell(None) is None
    assert _jsonable_cell(3) == 3 and _jsonable_cell("x") == "x"


def test_data_cells_comma_quantities_not_identifiers():
    """Thousands commas so a count reads as a count (2,018) not a year (2018) —
    but NOT on identifier / code / year columns (a finding id 71942 must not
    become 71,942)."""
    from report_render_html import _fmt_cell
    assert _fmt_cell(2018, "ytd_months") == "2,018"          # a count → commas
    assert _fmt_cell(28326997748.3, "r12_import_eur") == "28,326,997,748.3"
    assert _fmt_cell(71942, "finding_id") == "71942"         # id → no commas
    assert _fmt_cell(2018, "anchor_year") == "2018"          # year → no commas
    assert _fmt_cell(85076000, "product_nc") == "85076000"   # code → no commas


def test_data_table_cells_carry_raw_for_clean_paste():
    h = render_html(_sample_report())
    assert "data-raw=" in h                       # ungrouped value on numeric cells
    assert "getAttribute('data-raw')" in h        # TSV copy prefers it over commas


# ---- LLM per-finding take (the v1 layer) ----

def test_headline_take_renders_segregated_in_both_surfaces():
    """A generated take renders as a 'machine hypotheses — unverified' block
    under its mover, in both renderers, with the hedge in the text so it
    survives copy-paste; and it round-trips through serialisation."""
    r = _sample_report()
    md = render_markdown(r)
    html = render_html(r)
    assert "Machine hypotheses" in md
    assert "leads to explore, not findings" in md
    assert "Is the 40% fall volume-driven?" in md
    assert 'class="take"' in html
    assert "Is the 40% fall volume-driven?" in html
    take = rm.to_dict(r)["headline"]["items"][0]["take"]
    assert take["status"] == "generated"
    assert take["questions"][0]["axis"] == "volume-vs-value"


def test_take_parse_questions():
    from llm_takes import _parse_questions
    assert _parse_questions('{"questions":[{"q":"Is X up?","axis":"a"}]}') == [
        {"q": "Is X up?", "axis": "a"}]
    assert _parse_questions('```json\n{"questions":[{"q":"Is X up?"}]}\n```') == [
        {"q": "Is X up?", "axis": ""}]          # tolerates a code fence
    assert _parse_questions("not json at all") is None
    assert _parse_questions('{"questions":[]}') is None
    assert _parse_questions('{"nope":1}') is None


def test_take_validate_is_the_safety_contract():
    """The guard: every question interrogative, no number absent from the facts
    (reusing verify_numbers). This is what rejected the live Permanent-magnets
    take that cited a 93% not in its facts."""
    from llm_takes import _validate_questions
    facts = {"scopes": {"eu_27": {"imports": {"yoy_pct": 0.355}}}}
    assert _validate_questions(
        [{"q": "Is the +35.5% rise volume-driven?", "axis": "x"}], facts) is None
    rej = _validate_questions([{"q": "Is the 93% share large?", "axis": "x"}], facts)
    assert rej and rej["reason"] == "number_not_in_facts"
    rej = _validate_questions([{"q": "This rise is volume-driven.", "axis": "x"}], facts)
    assert rej and rej["reason"] == "not_interrogative"


# ---- labels (theme layer) ----

def test_labels_themes_many_to_many():
    assert labels.themes_for_group("EV batteries (Li-ion)") == ["EV supply chain"]
    # Permanent magnets sits in three overlapping labels — the many-to-many point
    mags = labels.themes_for_group("Permanent magnets")
    assert {"EV supply chain", "Rare earths & magnets",
            "China export-control regime"} <= set(mags)
    assert labels.themes_for_group("No such group") == []


def test_seed_labels_well_formed():
    for lab in labels.SEED_LABELS:
        assert lab.name and lab.definition and lab.kind
        assert lab.member_groups          # composed from named groups
    kinds = {l.kind for l in labels.SEED_LABELS}
    assert "origin_risk" in kinds         # Xinjiang-style lens exists


def test_wind_power_theme_replaces_retired_group():
    """The retired 'Wind turbine components' group is replaced by an
    overlapping 'Wind power' lens: the precise turbine flow plus the NdFeB
    magnets feeding direct-drive generators — and deliberately NOT generic
    steel/motors (the patterns that made the old group conflate)."""
    by_name = {l.name: l for l in labels.SEED_LABELS}
    assert "Wind power" in by_name
    assert set(by_name["Wind power"].member_groups) == {
        "Wind generating sets only",
        "Sintered NdFeB magnets (CN8 85051110)",
    }
    # 'Wind generating sets only' belongs to exactly this lens.
    assert labels.themes_for_group("Wind generating sets only") == ["Wind power"]


def test_q2_expansion_groups_carry_expected_themes():
    """The Q2 expansion (refined critical minerals + pharma APIs + engine
    parts/engines) wires each material-named group to the right cross-cutting
    themes — locking the labels.py reconciliation, incl. realising the pharma
    theme's previously-aspirational antibiotic/ibuprofen/paracetamol members."""
    tf = labels.themes_for_group
    # Battery-mineral feedstocks ride both the EV chain and the export-control lens.
    assert tf("Lithium chemicals (carbonate + hydroxide)") == [
        "EV supply chain", "China export-control regime"]
    assert tf("Cobalt (oxides, hydroxides & unwrought)") == [
        "EV supply chain", "China export-control regime"]
    assert tf("Manganese oxides") == ["EV supply chain"]
    # The China-export-controlled minor metals.
    for g in ("Tungsten (HS 8101)",
              "Gallium, germanium & other minor metals (HS 8112)",
              "Antimony (HS 8110)"):
        assert tf(g) == ["China export-control regime"], g
    # Pharma APIs — antibiotics/ibuprofen/paracetamol were aspirational theme
    # members; naming the groups to match realises them. Vitamins added.
    for g in ("Antibiotics (HS 2941)",
              "Ibuprofen-class monocarboxylic acids (HS 2916)",
              "Paracetamol-class amides (HS 2924)",
              "Vitamins & provitamins (HS 2936)"):
        assert tf(g) == ["Pharma & fine chemicals"], g
    # Engine parts + engines complete the Automotive powertrain.
    for g in ("Engine parts (CN8 84099100 + 84099900)",
              "Internal-combustion engines (HS 8407 + 8408)"):
        assert tf(g) == ["Automotive"], g
    # Round 2 — cosmetics + paint groups; TiO2 now bridges both new themes.
    for g in ("Essential oils & fragrance mixtures (HS 3301 + 3302)",
              "Beauty, make-up & skin-care preparations (HS 3304)"):
        assert tf(g) == ["Cosmetics & personal care"], g
    assert tf("Paints & varnishes (HS 3208-3210)") == ["Paint & coatings"]
    assert tf("Titanium dioxide (CN8 320611)") == [
        "Cosmetics & personal care", "Paint & coatings"]


def test_scope_labelling_present_on_mirror_and_gacc_sections():
    """Scope-clarity (deploy-batch #1): the mirror-gap and GACC-bilateral
    section explainers must state the partner scope — the Eurostat side's
    CN+HK+MO envelope, and that GACC is China-as-reporter with HK/Macao as
    partners — so "China" isn't silently two different things across sections.
    Load-bearing copy: this guards against a refactor dropping the label."""
    import report_builder as rb
    mg = rb._ABOUT["mirror-gaps"]
    assert "CN+HK+MO" in mg and "Hong Kong and Macau" in mg
    gacc = rb._ABOUT["gacc-bilateral"]
    assert "CN+HK+MO" in gacc and "partners" in gacc


# ---- classifications (BEC end-use mapping is pure) ----

@pytest.mark.parametrize("bec4,expected", [
    ("41", "Capital"), ("521", "Capital"),
    ("21", "Intermediate"), ("42", "Intermediate"),
    ("61", "Consumption"), ("122", "Consumption"),
    ("7", "Other"), ("999", "Other"),
])
def test_bec4_enduse_mapping(bec4, expected):
    assert classifications.bec4_enduse(bec4) == expected


def test_division_title_falls_back_gracefully():
    assert classifications.division_title("78") == "Road vehicles"
    assert classifications.division_title("zz").startswith("div ")


def test_sitc_divisions_present_or_graceful():
    # If the shared UNSD files are available, batteries map to electrical
    # machinery (77); if absent the helper degrades to [] (never crashes).
    divs = classifications.sitc_divisions_for_patterns(["850760%"])
    assert divs == [] or "77" in divs


# --------------------------------------------------------------------------
# DB-backed smoke tests (skip without GACC_TEST_DATABASE_URL).
# --------------------------------------------------------------------------

def test_build_report_runs_against_real_schema(clean_db):
    """build_report executes against the live schema for both variants and the
    output renders, even with an empty findings table (empty-safe)."""
    from report_builder import build_report
    for trigger in ("eurostat", "gacc"):
        r = build_report(source_trigger=trigger)
        assert isinstance(r, rm.Report)
        assert r.meta.variant == trigger
        # both renderers must not crash on whatever the model holds
        assert render_markdown(r).startswith("# Headlines")
        assert render_html(r).startswith("<!doctype html")


def test_build_report_surfaces_a_seeded_group(clean_db, test_db_url):
    """A seeded EU-27 hs_group_yoy finding for a real hs_group surfaces as a
    sector-detail group carrying its citation."""
    from report_builder import build_report
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT name FROM hs_groups LIMIT 1")
        row = cur.fetchone()
        if row is None:
            pytest.skip("no hs_groups seeded in test DB")
        name = row[0]
        cur.execute("INSERT INTO scrape_runs (status, source_url) "
                    "VALUES ('success', 'test://seed') RETURNING id")
        run = cur.fetchone()[0]
        detail = {"group": {"name": name},
                  "totals": {"yoy_pct": -0.4, "current_12mo_eur": 7.5e9,
                             "low_base": False},
                  "windows": {"current_end": "2026-04-01"}}
        cur.execute(
            "INSERT INTO findings (scrape_run_id, kind, subkind, detail, "
            "natural_key_hash) VALUES (%s,'anomaly','hs_group_yoy',%s::jsonb,'nk1')",
            (run, json.dumps(detail)),
        )
    r = build_report(source_trigger="eurostat")
    sd = [s for s in r.sections if s.kind == "sector_detail"]
    assert sd and any(g.title == name for g in sd[0].sections)


def test_display_name_substituted_and_slug_is_consistent(clean_db, test_db_url):
    """A group with a `display_name` set must (a) surface its reader-facing
    label on a rendered surface, and (b) keep its cross-reference slug
    consistent — the headline mover's drill-down slug, the sector-detail
    Section's id (anchor), and the rendered HTML link must ALL be the slug of
    the *display* name, not the stored key. This is the invariant that breaks
    silently if a heading is renamed without its slug/links.

    The finding still snapshots the stable internal key in detail.group.name;
    only the rendered title/slug change. EV batteries (Li-ion) gets the display
    name 'Lithium-ion accumulators (HS 850760)' in schema.sql / the 2026-06-22c
    migration (and in the test DB)."""
    from briefing_pack._helpers import _slugify_heading
    from report_builder import build_report

    KEY = "EV batteries (Li-ion)"
    DISPLAY = "Lithium-ion accumulators (HS 850760)"
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT display_name FROM hs_groups WHERE name = %s", (KEY,))
        row = cur.fetchone()
        if row is None or row[0] != DISPLAY:
            pytest.skip(
                "EV batteries display_name not set in test DB "
                "(run the 2026-06-22c migration / schema UPDATE)"
            )
        cur.execute("INSERT INTO scrape_runs (status, source_url) "
                    "VALUES ('success', 'test://seed') RETURNING id")
        run = cur.fetchone()[0]
        # Big, clean import mover so it clears the top-movers filters
        # (≥10pp, ≥€100M, not low-base, no 🔴) and becomes a headline item
        # with a drill-down link into sector detail. detail.group.name keeps
        # the RAW key — display substitution happens only at render time.
        detail = {"group": {"name": KEY},
                  "totals": {"yoy_pct": 0.35, "current_12mo_eur": 27e9,
                             "prior_12mo_eur": 20e9, "low_base": False},
                  "windows": {"current_end": "2026-04-01"}}
        cur.execute(
            "INSERT INTO findings (scrape_run_id, kind, subkind, detail, "
            "natural_key_hash) VALUES (%s,'anomaly','hs_group_yoy',%s::jsonb,'nkdisp1')",
            (run, json.dumps(detail)),
        )

    r = build_report(source_trigger="eurostat")

    # (a) Rendered surface shows the display name, not the stored key.
    sd = [s for s in r.sections if s.kind == "sector_detail"]
    ev_section = next(g for g in sd[0].sections if g.title == DISPLAY)
    assert not any(g.title == KEY for g in sd[0].sections)

    # The headline mover for this group carries the display name in its subject.
    ev_item = next(i for i in r.headline.items
                   if i.subject.get("group_name") == DISPLAY)

    # (b) Slug consistency: the drill-down slug == the section anchor ==
    # the slugified DISPLAY name (NOT the stored key's slug).
    expected_slug = _slugify_heading(DISPLAY)
    assert expected_slug == "lithium-ion-accumulators-hs-850760"
    assert ev_item.drill_down == expected_slug
    assert ev_section.id == expected_slug
    assert _slugify_heading(KEY) != expected_slug  # the rename actually moved the slug

    # And the rendered HTML wires the drill-down link (href="#slug") to the
    # sector-detail heading anchor (id="slug") — same display-derived slug on
    # both ends, so the in-page link actually resolves.
    html = render_html(r)
    assert DISPLAY in html
    assert f'id="{expected_slug}"' in html        # the target heading anchor
    assert f'href="#{expected_slug}"' in html     # a link pointing at it


def test_gacc_macro_item_with_null_yoy_states_level_not_direction(clean_db, test_db_url):
    """A GACC bloc finding with no YoY must read as a level ('stood at'), never
    assert a direction ('fell') for a change the data doesn't carry."""
    from datetime import date
    from report_builder import build_report
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO scrape_runs (status, source_url) "
                    "VALUES ('success', 'test://seed') RETURNING id")
        run = cur.fetchone()[0]
        detail = {"aggregate": {"raw_label": "ASEAN"},
                  "totals": {"current_12mo_eur": 4.5e9},   # deliberately no yoy_pct
                  "windows": {"current_end": "2026-04-01"}}
        cur.execute(
            "INSERT INTO findings (scrape_run_id, kind, subkind, detail, "
            "natural_key_hash) VALUES (%s,'anomaly','gacc_aggregate_yoy',%s::jsonb,'nkg1')",
            (run, json.dumps(detail)),
        )
    r = build_report(source_trigger="gacc", data_period=date(2026, 4, 1))
    prose = " ".join(i.prose for i in r.headline.items)
    assert "ASEAN" in prose
    assert "stood at" in prose
    assert "fell" not in prose          # never a fabricated direction


def test_structural_section_partitions_and_is_attributed(clean_db):
    """If the trade map has divisions, their value shares sum to ~1 (a true
    partition — the unclassified remainder is included, nothing dropped) and the
    section carries its source. Skips gracefully when the test DB has no
    eurostat_raw_rows data."""
    from report_builder import build_report
    r = build_report(source_trigger="eurostat")
    tm = [s for s in r.sections if s.kind == "structural"]
    if not tm or not tm[0].sections:
        pytest.skip("no eurostat_raw_rows data in test DB")
    shares = [d.metrics.get("value_share", 0) for d in tm[0].sections]
    assert abs(sum(shares) - 1.0) < 1e-6
    assert tm[0].provenance.source == "eurostat"
    assert tm[0].metrics.get("total_eur", 0) > 0


def test_periodic_writes_portal_snapshot(clean_db, tmp_path):
    """The periodic-run portal step writes report.json (the published snapshot)
    + index.html into 04_Portal/. Exercised via the helper (no full cycle, no
    LLM); build_report runs against the clean schema."""
    import periodic
    pdir = periodic.write_portal_snapshot(str(tmp_path), None, generate_takes=False)
    assert pdir is not None
    p = tmp_path / "04_Portal"
    assert (p / "report.json").exists() and (p / "index.html").exists()
    snap = json.loads((p / "report.json").read_text())
    assert snap["meta"]["variant"] == "eurostat"
    assert (p / "index.html").read_text().startswith("<!doctype html")


def test_portal_snapshot_builds_workbook_when_requested(clean_db, tmp_path):
    """The Tables-tab "Download Excel workbook" button links to /data.xlsx,
    which publish_snapshot serves from `<bundle_dir>/04_Data.xlsx`. A standalone
    --portal-snapshot has no briefing-pack run, so write_portal_snapshot must
    build that workbook when write_workbook=True — otherwise the download 404s.
    Default stays off: periodic-run already wrote it via export()."""
    import periodic
    # Default (periodic-run path): no workbook built here — export() did it.
    periodic.write_portal_snapshot(str(tmp_path), None, generate_takes=False)
    assert not (tmp_path / "04_Data.xlsx").exists()
    # Snapshot-only path: build the workbook so publish_snapshot can serve it.
    snap = tmp_path / "snap"
    pdir = periodic.write_portal_snapshot(
        str(snap), None, generate_takes=False, write_workbook=True)
    assert pdir is not None
    assert (snap / "04_Data.xlsx").is_file()              # the download resolves
    assert (snap / "04_Portal" / "report.json").exists()  # portal still written


def test_portal_snapshot_records_no_brief_run(clean_db, test_db_url, tmp_path):
    """The standalone snapshot must NOT insert a brief_runs row — that's the
    whole reason --portal-snapshot exists apart from --periodic-run: an
    on-demand render that never advances the subscriber cycle or moves the
    'since last brief' baseline."""
    import periodic

    def _brief_runs() -> int:
        with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM brief_runs")
            return cur.fetchone()[0]

    before = _brief_runs()
    pdir = periodic.write_portal_snapshot(str(tmp_path), None, generate_takes=False)
    assert pdir is not None
    assert _brief_runs() == before  # no cycle advanced by snapshotting


def test_publish_snapshot_validates_before_touching_gcs(tmp_path, monkeypatch):
    """The publish step fails cheap and clear: no bucket → ValueError, no
    04_Portal snapshot → FileNotFoundError (both before any GCS call)."""
    import portal_publish
    monkeypatch.delenv("PORTAL_BUCKET", raising=False)
    with pytest.raises(ValueError):
        portal_publish.publish_snapshot(str(tmp_path))            # no bucket
    with pytest.raises(FileNotFoundError):
        portal_publish.publish_snapshot(str(tmp_path), bucket="b")  # no 04_Portal/


def test_publish_period_read_from_snapshot(tmp_path):
    """The per-period archive path comes from the snapshot's own meta; a missing
    or unreadable snapshot yields None (latest/ still publishes)."""
    import portal_publish
    pd = tmp_path / "04_Portal"
    pd.mkdir()
    (pd / "report.json").write_text(json.dumps({"meta": {"data_period": "2026-04-01"}}))
    assert portal_publish._period_from_snapshot(pd) == "2026-04-01"
    assert portal_publish._period_from_snapshot(tmp_path) is None


# --------------------------------------------------------------------------
# General "one other thing worth a look" take (release-level LLMSlot)
# --------------------------------------------------------------------------

def test_general_take_validate():
    """The general take's guards: finding_id on the shortlist, a question
    present, a word cap, and every number round-trips to the candidate's facts."""
    import llm_general_take as gt
    shortlist = [{
        "finding_id": 1, "subject": "Magnets", "kinds": ["china_dependency"],
        "facts": {"yoy_pct": -0.23, "china_import_share": 0.92},
        "prov": {"yoy_pct": 71, "china_import_share": 1},
    }]
    assert gt._validate({"finding_id": 1, "take": "Magnets fell 23% with China at 92% — worth a look?"}, shortlist)[0]
    assert not gt._validate({"finding_id": 999, "take": "anything?"}, shortlist)[0]        # not shortlisted
    assert not gt._validate({"finding_id": 1, "take": "Magnets fell 23%."}, shortlist)[0]  # no question
    assert not gt._validate({"finding_id": 1, "take": "Magnets fell 50%?"}, shortlist)[0]  # unverified number
    assert not gt._validate({"finding_id": 1, "take": "word " * 80 + "?"}, shortlist)[0]   # too long


def test_general_take_render_html_and_md():
    """Renders the 'One other thing worth a look' box with the paragraph + the
    citation tokens when generated; nothing when placeholder/empty."""
    import report_render_html as rh
    import report_render_markdown as rmd
    gen = rm.LLMSlot(slot_type="general", status="generated",
                     content="China supplied 92% — worth a look?", grounded_in=[5, 9])
    html = rh._general_take_html(gen)
    assert "One other thing worth a look" in html
    assert "92%" in html and "finding/5" in html and "finding/9" in html
    md = rmd._general_take_md(gen)
    assert any("One other thing worth a look" in line for line in md)
    assert any("finding/5" in line for line in md)
    empty = rm.LLMSlot(slot_type="general", status="placeholder", grounded_in=[5])
    assert rh._general_take_html(empty) == ""
    assert rmd._general_take_md(empty) == []


def test_general_take_shortlist_excludes_headline_and_keeps_provenance():
    """The shortlist drops headline subjects, and each numeric fact carries its
    own source finding (a candidate's numbers can come from different findings)."""
    import llm_general_take as gt
    sl = gt.build_general_shortlist(_sample_report())   # headline subject = "Cars"
    subjects = {c["subject"] for c in sl}
    assert "Cars" not in subjects
    mg = next((c for c in sl if "mirror" in c["subject"].lower()), None)
    assert mg is not None and all(isinstance(fid, int) for fid in mg["prov"].values())


# --------------------------------------------------------------------------
# Annual per-region trade charts (GACC "China's trade by partner" section)
# --------------------------------------------------------------------------

def test_gacc_partner_annual_charts_aggregate_ytd_union_and_balance(
    clean_db, test_db_url,
):
    """The annual-per-region chart data: GACC YTD-cumulative observations are
    read at the LATEST period per calendar year (= that year's annual EUR),
    converted to EUR via the shared helper; the US unions its two GACC labels
    (the name changed in 2020); balance = exports − imports; and the partial
    current year (latest month < December) is flagged, not dropped."""
    from datetime import date
    import report_builder as rb

    # Annual figures are taken at the latest period of each year. CNY→EUR is
    # 0.125 flat and the unit is 'CNY 100 Million' (×1e8), so the EUR value is
    # value_amount × 1.25e7 (e.g. 1000 → €12.5B).
    SCALE_FX = 1e8 * 0.125
    # (partner_label, flow, period, value_amount)
    rows = [
        # ASEAN exports: 2019 + 2020 full (Nov AND Dec in 2020 — Dec must win),
        # 2021 partial (April only).
        ("ASEAN", "export", date(2019, 12, 1), 1000),
        ("ASEAN", "export", date(2020, 11, 1), 1500),   # earlier in-year cumulative
        ("ASEAN", "export", date(2020, 12, 1), 1600),   # the annual (latest) figure
        ("ASEAN", "export", date(2021, 4, 1), 900),     # partial year
        ("ASEAN", "import", date(2019, 12, 1), 800),
        ("ASEAN", "import", date(2020, 12, 1), 900),
        ("ASEAN", "import", date(2021, 4, 1), 500),
        # Africa, both flows.
        ("Africa", "export", date(2019, 12, 1), 400),
        ("Africa", "export", date(2020, 12, 1), 500),
        ("Africa", "export", date(2021, 4, 1), 300),
        ("Africa", "import", date(2019, 12, 1), 600),
        ("Africa", "import", date(2020, 12, 1), 700),
        ("Africa", "import", date(2021, 4, 1), 350),
        # United States: 2019 under "United States", 2020-on under
        # "United States (US)" — one logical line, two labels to union.
        ("United States", "export", date(2019, 12, 1), 2000),
        ("United States (US)", "export", date(2020, 12, 1), 2200),
        ("United States (US)", "export", date(2021, 4, 1), 1100),
        ("United States", "import", date(2019, 12, 1), 1000),
        ("United States (US)", "import", date(2020, 12, 1), 1100),
        ("United States (US)", "import", date(2021, 4, 1), 600),
    ]
    periods = {p for _l, _f, p, _v in rows}
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO scrape_runs (source_url, status) "
                    "VALUES ('ytd://seed', 'success') RETURNING id")
        run = cur.fetchone()[0]
        # One CNY release per period (release.unit carries the scale word).
        rel_by_period = {}
        for p in periods:
            cur.execute(
                "INSERT INTO releases (source, section_number, currency, period, "
                "    release_kind, source_url, unit) "
                "VALUES ('gacc', 4, 'CNY', %s, 'preliminary', 'ytd://r', "
                "        'CNY 100 Million') RETURNING id",
                (p,),
            )
            rel_by_period[p] = cur.fetchone()[0]
        for label, flow, p, val in rows:
            cur.execute(
                "INSERT INTO observations (release_id, scrape_run_id, period_kind, "
                "    flow, partner_country, value_amount, value_currency, source_row) "
                "VALUES (%s, %s, 'ytd', %s, %s, %s, 'CNY', '{}')",
                (rel_by_period[p], run, flow, label, val),
            )
        for p in periods:
            cur.execute(
                "INSERT INTO fx_rates (currency_from, currency_to, rate_date, "
                "    rate, rate_source) "
                "VALUES ('CNY', 'EUR', %s, 0.125, 'test') "
                "ON CONFLICT (currency_from, currency_to, rate_date, rate_source) "
                "DO NOTHING",
                (p,),
            )
        conn.commit()

    # ASEAN exports: Dec 2020 (1600), not Nov (1500), is the 2020 annual figure.
    asean_exp = rb._gacc_annual_by_region(["ASEAN"], "export")
    assert asean_exp == {
        2019: 1000 * SCALE_FX, 2020: 1600 * SCALE_FX, 2021: 900 * SCALE_FX,
    }
    # The US line unions both labels across the (non-overlapping) year ranges.
    us_exp = rb._gacc_annual_by_region(
        ["United States", "United States (US)"], "export")
    assert us_exp == {
        2019: 2000 * SCALE_FX, 2020: 2200 * SCALE_FX, 2021: 1100 * SCALE_FX,
    }

    charts = rb._gacc_partner_charts()
    assert [c["metric"] for c in charts] == ["exports", "imports", "balance"]
    assert all(c["years"] == [2019, 2020, 2021] for c in charts)
    # 2021 is partial (latest period April, not December).
    assert all(c["partial_last_year"] == 2021 for c in charts)

    def series(chart, name):
        return next(s["values"] for s in chart["series"] if s["name"] == name)

    exp_chart, imp_chart, bal_chart = charts
    # US series (unioned) appears as one line on every chart.
    assert series(exp_chart, "United States") == [
        2000 * SCALE_FX, 2200 * SCALE_FX, 1100 * SCALE_FX]
    # Balance = exports − imports per year (ASEAN: 1000−800, 1600−900, 900−500).
    assert series(bal_chart, "ASEAN") == [
        (1000 - 800) * SCALE_FX, (1600 - 900) * SCALE_FX, (900 - 500) * SCALE_FX]
    # Africa is China-deficit on imports>exports? here exports<imports in 2019
    # (400 vs 600) → a negative balance, which the signed chart must carry.
    assert series(bal_chart, "Africa")[0] == (400 - 600) * SCALE_FX < 0
    # An unseeded region still gets a (None-filled) line, so all six plot.
    assert {s["name"] for s in exp_chart["series"]} == {
        "ASEAN", "European Union", "United States", "Africa",
        "Latin America", "Russian Federation"}
    assert series(exp_chart, "Latin America") == [None, None, None]


def test_multiline_chart_svg_renders_six_lines_and_balance_zero_baseline():
    """The renderer: one polyline per region (six), all region names in the
    legend, and the balance variant draws a visible zero baseline (its values
    straddle zero) — what stops a deficit reading as a small positive bar."""
    from report_render_html import _multiline_chart_svg, _multiline_legend_html

    regions = ["ASEAN", "European Union", "United States", "Africa",
               "Latin America", "Russian Federation"]
    colors = ["#052962", "#c70000", "#22874d", "#0077b6", "#b08800", "#7d4cdb"]
    years = [2022, 2023, 2024, 2025, 2026]

    def chart(metric, signed):
        ser = []
        for k, name in enumerate(regions):
            base = (k + 1) * 20e9
            vals = [base + y * 1e9 for y in range(len(years))]
            if signed and k % 2:                      # half the lines negative
                vals = [-v for v in vals]
            ser.append({"name": name, "values": vals})
        return {"metric": metric, "title": metric, "years": years,
                "partial_last_year": 2026, "series": ser, "colors": colors}

    exp = _multiline_chart_svg(chart("exports", signed=False))
    assert exp.count("<polyline") == 6                  # one solid line per region
    assert exp.count("stroke-dasharray") == 6            # partial-year final seg
    assert "YTD" not in exp                               # YTD note lives in the key, not the axis
    # exports are zero-based positive → no zero *baseline* reference line.
    assert 'stroke-width="1.2"' not in exp

    bal = _multiline_chart_svg(chart("balance", signed=True))
    assert bal.count("<polyline") == 6
    # The signed balance chart straddles zero → a visible zero baseline line.
    assert 'stroke-width="1.2"' in bal

    # The key (rendered in the card's left meta column, under the headline)
    # carries the region names + the dashed partial-year (YTD) note.
    leg = _multiline_legend_html(chart("exports", signed=False))
    assert all(name in leg for name in regions)
    assert "year-to-date" in leg
