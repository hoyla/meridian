"""Tests for the GACC-only tab (dev_notes/2026-07-05-gacc-update-page-design.md):
the `_build_gacc_page` builder (DB-backed, live-Postgres approach) and the
renderer's two-track surface — period-explicit tab labels, the masthead
descent, the identity strips, the context strip, the By-country entrepôt
line, and the since-last-read delta."""

from __future__ import annotations

import dataclasses
from datetime import date, datetime

import psycopg2
import psycopg2.extras
import pytest

import release_calendar
import report_builder as rb
import report_model as rm
from report_render_html import render_html


# ---------------------------------------------------------------------------
# Model-level render tests (no DB) — the fast surface checks.
# ---------------------------------------------------------------------------

def _gacc_page(**over) -> rm.GaccPage:
    base = dict(
        data_period=date(2026, 5, 1),
        tab_label="GACC-only (May 2026)",
        identity={
            "published": "2026-06-09",
            "confirmation_due": "2026-07-15",
            "source_url": "https://english.customs.gov.cn/Statics/x.html",
            "source_url_zh": (
                "http://www.customs.gov.cn/customs/302249/zfxxgk/fdzdgknr/"
                "302274/302275/index.html"
            ),
        },
        strip=[rm.Indicator(
            key="gacc_strip_eu", kicker="CHINA → EU",
            label="China’s exports to the EU",
            value=0.124, unit="yoy_pct", formatted="+12.4%",
            note="May 2026 vs same month last year · €45.20B in the month",
            provenance=rm.Provenance(finding_ids=[42], source="gacc",
                                     as_of=date(2026, 5, 1)))],
        standout=rm.HeadlineItem(
            subject={"scope": "china", "flow": "export", "group_name": "Vietnam"},
            metrics={"direction": "rose", "pct": 0.312, "value_eur": 1.2e10},
            stability={"badge": None, "hedge_phrase": None},
            prose="**China’s exports to Vietnam** rose 31.2% year-on-year "
                  "in May 2026 — the sharpest move in this GACC release. "
                  "`finding/43`",
            provenance=rm.Provenance(finding_ids=[43], source="gacc",
                                     as_of=date(2026, 5, 1))),
        by_country=rm.Section(
            id="gacc-by-country", title="By country",
            kind="gacc_bilateral", intro="China’s own numbers.",
            metrics={"grouped": True},
            sections=[
                rm.Section(id="gacc-bycountry-europe", title="Europe",
                           kind="gacc_partner_group"),
                rm.Section(
                    id="gacc-bycountry-world", title="Rest of the world",
                    kind="gacc_partner_group",
                    sections=[rm.Section(
                        id="gaccpage-hong-kong-china",
                        title="Hong Kong, China", kind="gacc_bilateral",
                        metrics={"is_hub": True},
                        findings=[rm.Finding(
                            finding_id=45,
                            subkind="gacc_bilateral_aggregate_yoy",
                            title="China exports to Hong Kong, China",
                            metrics={"scope": "China", "flow": "export",
                                     "yoy_pct": 0.02,
                                     "current_eur": 2.4e11},
                            provenance=rm.Provenance(
                                finding_ids=[45], source="gacc"))])])]),
        world=rm.Section(id="gacc-world", title="China and the world",
                         kind="gacc_world", intro="Context.",
                         about=rb._GACC_PAGE_ABOUT_WORLD, metrics={"rows": [
                             {"label": "Total", "short_label": "Total", "kind": "world", "is_hub": False,
                              "flows": {"export": {
                                  "sm_yoy": 0.04, "ytd_yoy": 0.05,
                                  "rolling_yoy": 0.06, "rolling_eur": 3.1e12,
                                  "finding_id": 44}}},
                             {"label": "European Union", "short_label": "EU", "kind": "eu_bloc",
                              "is_hub": False,
                              "flows": {"export": {
                                  "sm_yoy": 0.045, "ytd_yoy": 0.062,
                                  "rolling_yoy": 0.074, "rolling_eur": 5.0e11,
                                  "finding_id": 42},
                                  "import": {
                                  "sm_yoy": -0.02, "ytd_yoy": -0.01,
                                  "rolling_yoy": -0.03, "rolling_eur": 2.5e11,
                                  "finding_id": 47}}},
                             {"label": "ASEAN", "short_label": "ASEAN", "kind": "asean", "is_hub": False,
                              "flows": {"export": {
                                  "sm_yoy": 0.21, "ytd_yoy": 0.15,
                                  "rolling_yoy": 0.12, "rolling_eur": 6.5e11,
                                  "finding_id": 46}}},
                             {"label": "RCEP", "short_label": "RCEP", "kind": "rcep", "is_hub": False,
                              "flows": {"export": {
                                  "sm_yoy": 0.18, "ytd_yoy": 0.12,
                                  "rolling_yoy": 0.10, "rolling_eur": 9.0e11,
                                  "finding_id": 48}}},
                         ], "period": "2026-05-01"}),
        since_last={"prev_period": "2026-04-01", "rows": [
            {"label": "the EU", "flow": "export", "basis": "single-month",
             "prev_yoy": 0.031, "cur_yoy": 0.124, "delta": 0.093,
             "finding_id": 42}]},
        understanding=("**An early read.** China’s own customs figures.\n\n"
               "**Reading the direction.** Test copy."),
    )
    base.update(over)
    return rm.GaccPage(**base)


def _report(gacc_page=None, vintages=None) -> rm.Report:
    return rm.Report(
        meta=rm.ReportMeta(
            data_period=date(2026, 4, 1), variant="eurostat", snapshot_id="t",
            generated_at=datetime(2026, 7, 5, 22, 0)),
        gacc_page=gacc_page,
        source_vintages=vintages if vintages is not None else {
            "eurostat": date(2026, 4, 1), "hmrc": date(2026, 4, 1),
            "gacc": date(2026, 5, 1)},
    )


def test_two_track_tab_labels_are_period_explicit():
    h = render_html(_report(gacc_page=_gacc_page()))
    assert ">Full briefing (Apr 2026)</a>" in h
    assert ">GACC-only (May 2026)</a>" in h


def test_no_gacc_page_means_no_gacc_tab():
    h = render_html(_report(gacc_page=None))
    # No tab, no period-labelled tab text, no cross-link target. (The
    # About-this-site copy legitimately *mentions* the GACC-only tab by
    # name, so the needles are the structural forms, not the bare phrase.)
    assert "GACC-only (" not in h and 'href="#tab-gacc"' not in h
    assert ">Full briefing (Apr 2026)</a>" in h


def test_masthead_descends_to_updated_stamp_only():
    """The masthead makes only the portal-wide claim (Updated …); the source
    badge and 'Data to X' moved down into per-tab identity strips, and the
    page <title> drops the period (two tracks, two vintages — design doc
    § masthead)."""
    h = render_html(_report(gacc_page=_gacc_page()))
    mast = h[h.index('class="masthead"'):h.index("</header>")]
    assert "Updated 2026-07-05 22:00" in mast
    assert "Data to " not in h
    assert "<title>Meridian — China–Europe trade</title>" in h


def test_briefing_identity_strip_carries_source_vintages():
    h = render_html(_report(gacc_page=_gacc_page()))
    assert "Eurostat · to Apr 2026" in h
    assert "HMRC · to Apr 2026" in h
    # The briefing strip carries no GACC VINTAGE chip (2026-07-15 ruling) —
    # a signpost to the other track's tab replaces it.
    assert "GACC context" not in h
    assert "China has already reported May 2026" in h
    assert 'href="#tab-gacc"' in h


def test_gacc_identity_strip_dates_links_and_about_page():
    h = render_html(_report(gacc_page=_gacc_page()))
    assert "GACC · May 2026" in h
    assert "Published 9 Jun 2026" in h
    assert "European confirmation due ~15 Jul 2026" in h
    assert "中文" in h and "www.customs.gov.cn" in h
    # The page's whole epistemic framing lives in ONE collapsed "About
    # this page" disclosure (consolidated 2026-07-05 — previously split
    # across a caveat list and a bottom Understanding section with
    # duplicated content).
    assert "About this page</summary>" in h
    assert "China’s own customs figures." in h
    i_summary = h.index("About this page</summary>")
    i_caveat = h.index("China’s own customs figures.")
    assert i_summary < i_caveat  # the framing sits inside the disclosure
    # Placement mirrors the Briefing's About-this-site: under the KPI band,
    # above the Standout lead (Luke, 2026-07-05).
    panel = h[h.index('id="tab-gacc"'):]
    i_kpis = panel.index('class="kpis kpis-4"')
    i_about = panel.index("About this page</summary>")
    i_standout = panel.index('id="gacc-standout"')
    assert i_kpis < i_about < i_standout


def test_world_table_sheds_hub_line_and_by_country_carries_it():
    """2026-07-15: the world section is the region tier — no HK entrepôt
    line, no hub labelling. The entrepôt treatment (summary chip +
    panel-leading explainer, incl. the mirror-gaps cross-reference) rides
    the By-country HK row instead."""
    h = render_html(_report(gacc_page=_gacc_page()))
    world = h[h.index('id="gacc-world"'):h.index('id="gacc-by-country"')]
    assert "Hong Kong" not in world and "entrepôt" not in world
    # The cast rationale + the no-Middle-East explanation ride the section's
    # More-about expander (2026-07-15: an absence a reader would otherwise
    # read as our omission is named as GACC's reporting choice).
    assert "More about this section" in world
    assert "no Middle East aggregate" in world
    assert "Belt &amp; Road row and the world total" in world
    # World total first, bloc next.
    assert world.index(">Total<") < world.index(">ASEAN<")
    # A missing operator renders an em-dash cell, never a fabricated figure.
    assert '<td class="num">—</td>' in world
    bc = h[h.index('id="gacc-by-country"'):]
    assert "Hong Kong, China" in bc
    assert "entrepôt ⓘ" in bc                      # collapsed-row chip
    assert "Entrepôt signal.</strong>" in bc        # panel explainer leads
    assert "Mirror-trade gaps" in bc                # cross-track pointer


def test_world_scale_glyphs_render_with_honest_exclusions():
    """The semicircle scale view under the change table (Luke, 2026-07-05):
    left half = exports, right half = imports, area ∝ 12-month value. The
    world TOTAL (the sum of its own components) and overlapping blocs
    (RCEP ⊃ ASEAN) stay table-only so the visual field can't double-count;
    tooltips repeat the table's figures. (No hub glyph since 2026-07-15 —
    the HK entrepôt line lives in By country.)"""
    h = render_html(_report(gacc_page=_gacc_page()))
    bub = h[h.index('class="gtable-wrap gworld-bubbles"'):
            h.index('id="gacc-by-country"')]
    assert "China’s exports to European Union: €500.00B" in bub
    assert "China’s imports from European Union: €250.00B" in bub
    assert "12 months to May 2026" in bub
    # Honest exclusions: no Total, no RCEP glyph — but both stay in the table.
    assert "exports to Total" not in bub and "exports to RCEP" not in bub
    assert ">RCEP<" in h  # the table row survives
    assert "left-heavy = China’s surplus" in bub
    # Under-glyph labels are the compact form (full names overlapped on
    # small glyphs — Luke, 2026-07-05); tooltips keep the full names.
    assert ">EU</text>" in bub
    assert ">European Union</text>" not in bub
    # No hub styling anywhere in the strip.
    assert "Hong Kong" not in bub and "entrepôt" not in bub
    assert 'stroke-dasharray' not in bub


def test_world_scale_glyphs_absent_below_two_entities():
    gp = _gacc_page()
    gp.world.metrics["rows"] = [r for r in gp.world.metrics["rows"]
                                if r["label"] in ("Total", "ASEAN")]
    h = render_html(_report(gacc_page=gp))
    assert "gworld-bubbles" not in h[h.index('id="tab-gacc"'):]


def test_since_last_read_renders_swings_and_gap_case():
    h = render_html(_report(gacc_page=_gacc_page()))
    assert "Since the last read" in h
    assert "+9.3pp" in h  # 0.031 → 0.124
    # The Jan–Feb-gap case: no comparable previous month → honest one-liner.
    gp = _gacc_page(since_last={"prev_period": None, "rows": []})
    h2 = render_html(_report(gacc_page=gp))
    assert "No directly comparable previous GACC month" in h2


def _swing(label, flow, prev, cur, basis="single-month", fid=99):
    return {"label": label, "flow": flow, "basis": basis, "prev_yoy": prev,
            "cur_yoy": cur, "delta": cur - prev, "finding_id": fid}


def test_since_last_dumbbell_renders_for_same_basis_rows():
    """≥2 same-basis swings → the dumbbell chart renders above the table:
    open prev-month dot, filled current-month dot, signed pp labels, and the
    caption naming the basis."""
    gp = _gacc_page(since_last={"prev_period": "2026-04-01", "rows": [
        _swing("the EU", "export", 0.031, 0.124),
        _swing("Canada", "import", 0.128, 0.924),
        _swing("Brazil", "export", 0.05, -0.198),
    ]})
    h = render_html(_report(gacc_page=gp))
    assert '<div class="gdumbbell">' in h
    assert 'fill="#fff"' in h          # the open previous-month dot
    assert "+9.3pp" in h and "+79.6pp" in h and "-24.8pp" in h
    assert "single-month basis" in h
    # Sign-flip row (Brazil +5% → −19.8%) crosses the marked zero line.
    assert 'aria-label="Year-on-year swings' in h


def test_since_last_dumbbell_plots_dominant_basis_only():
    """Mixed bases must not share one axis: the chart keeps the dominant
    basis and the caption names what it excluded (the table keeps all)."""
    gp = _gacc_page(since_last={"prev_period": "2026-04-01", "rows": [
        _swing("the EU", "export", 0.031, 0.124),
        _swing("Canada", "import", 0.128, 0.924),
        _swing("ASEAN", "export", 0.05, 0.08, basis="12-month"),
    ]})
    h = render_html(_report(gacc_page=gp))
    assert "1 reading on another basis shown in the table only" in h
    # The excluded 12-month row still appears in the table beneath.
    assert ">12-month<" in h


def test_since_last_dumbbell_absent_below_two_rows():
    gp = _gacc_page()  # the base fixture carries a single swing row
    h = render_html(_report(gacc_page=gp))
    # The markup (not the stylesheet, which always carries the class) —
    # a one-dot dumbbell isn't a chart.
    assert '<div class="gdumbbell">' not in h
    assert "Since the last read" in h  # the table/section still renders


def test_strip_and_standout_render_with_drawer_hooks():
    h = render_html(_report(gacc_page=_gacc_page()))
    assert "CHINA → EU" in h and "+12.4%" in h
    assert "Standout move" in h and "sharpest single-month shift" in h
    # The by-country roster orders biggest-first (2026-07-15: the
    # change-shaped reads live in strip/standout/since-last, so the roster
    # keeps a predictable size sort).
    assert "biggest first" in h


def test_no_separate_understanding_section():
    """One about-box, not two (Luke, 2026-07-05): the bottom "Understanding
    these figures" section is gone; its content lives in the top
    About-this-page disclosure."""
    h = render_html(_report(gacc_page=_gacc_page()))
    assert "Understanding these figures" not in h
    assert 'id="gacc-understanding"' not in h
    assert "Reading the direction." in h  # inside About this page


def test_gacc_tab_has_its_own_sticky_subnav():
    """UI consistency with the Briefing tab (Luke, 2026-07-05): the GACC
    panel carries the same sticky .subnav pattern, with data-spy anchors
    for its sections. The global scroll-spy scopes itself to the visible
    panel, so two subnavs coexist."""
    h = render_html(_report(gacc_page=_gacc_page()))
    panel = h[h.index('id="tab-gacc"'):h.index('id="tab-methodology"')
              if 'id="tab-methodology"' in h else len(h)]
    assert '<nav class="subnav"' in panel
    for anchor in ("gacc-standout", "gacc-sincelast", "gacc-world",
                   "gacc-by-country"):
        assert f'data-spy="{anchor}"' in panel
    assert 'data-spy="gacc-understanding"' not in panel  # consolidated away
    # Sections use the shared brief-sec class (sticky-bar scroll offset).
    assert 'class="brief-sec" id="gacc-standout"' in panel


def test_world_table_headers_align_with_values_and_tokens_fold_in():
    """The numeric column headers right-align with their values, the two
    flow groups get a divider, and the finding tokens ride beneath the
    partner name instead of costing a column."""
    h = render_html(_report(gacc_page=_gacc_page()))
    panel = h[h.index('id="gacc-world"'):]
    assert '<th class="num">Month YoY</th>' in panel
    assert '<th class="num grp">Month YoY</th>' in panel  # import group divider
    assert '<div class="gtable-toks">' in panel
    assert ">Findings</th>" not in panel  # the token column is gone


def test_methodology_tab_renamed_method():
    """'Method', not 'Methodology' — the nav row needs every character with
    two period-labelled track tabs. The tab KEY stays 'methodology' so
    existing #tab-methodology deep-links keep working."""
    r = _report(gacc_page=_gacc_page())
    r.sections = [rm.Section(id="ref", title="Methodology", kind="reference")]
    h = render_html(r)
    assert 'href="#tab-methodology">Method</a>' in h
    assert ">Methodology</a>" not in h


# ---------------------------------------------------------------------------
# DB-backed builder tests — seed real finding rows, build the page.
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_db(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE TABLE findings, observations, brief_runs, scrape_runs, "
            "releases, source_snapshots RESTART IDENTITY CASCADE"
        )
    yield


def _alias(cur, *, kind=None, iso2=None):
    """(id, raw_label) of a schema-seeded GACC country_alias, looked up
    structurally (aggregate_kind / iso2), never by label spelling."""
    if kind is not None:
        cur.execute(
            "SELECT id, raw_label FROM country_aliases "
            "WHERE source='gacc' AND aggregate_kind=%s LIMIT 1", (kind,))
    else:
        cur.execute(
            "SELECT id, raw_label FROM country_aliases "
            "WHERE source='gacc' AND iso2=%s AND aggregate_kind IS NULL "
            "LIMIT 1", (iso2,))
    row = cur.fetchone()
    assert row is not None, f"no gacc alias for kind={kind} iso2={iso2}"
    return row


def _totals(sm_yoy, rolling_eur, *, rolling_yoy=0.05, ytd_yoy=0.04,
            sm_eur=1e9):
    t = {
        "current_12mo_eur": rolling_eur,
        "prior_12mo_eur": rolling_eur / (1 + rolling_yoy),
        "yoy_pct": rolling_yoy,
        "ytd_cumulative": {"current_eur": rolling_eur / 3,
                           "prior_eur": rolling_eur / 3 / (1 + ytd_yoy),
                           "yoy_pct": ytd_yoy, "months_in_ytd": 5},
        "single_month": None,
        "jan_feb_combined_years": [],
    }
    if sm_yoy is not None:
        t["single_month"] = {"current_eur": sm_eur,
                             "prior_eur": sm_eur / (1 + sm_yoy),
                             "yoy_pct": sm_yoy}
    return t


def _seed_finding(cur, scrape_run_id, *, family, alias_id, label, kind,
                  flow, period, totals, seq):
    subkind = ("gacc_bilateral_aggregate_yoy" if family == "bilateral"
               else "gacc_aggregate_yoy")
    if flow == "import":
        subkind += "_import"
    ent_key = "partner" if family == "bilateral" else "aggregate"
    detail = {
        ent_key: {"alias_id": alias_id, "raw_label": label, "kind": kind},
        "windows": {"current_end": period.isoformat(),
                    "current_start": period.replace(year=period.year - 1).isoformat()},
        "totals": totals,
        "monthly_series": [],
        "caveat_codes": [],
    }
    cur.execute(
        "INSERT INTO findings (scrape_run_id, kind, subkind, title, detail, "
        "natural_key_hash) VALUES (%s, 'anomaly', %s, %s, %s, %s) RETURNING id",
        (scrape_run_id, subkind, f"{label} {flow} {period}",
         psycopg2.extras.Json(detail), f"test-nk-{seq}"),
    )
    return cur.fetchone()[0]


@pytest.fixture
def seeded(fresh_db, test_db_url):
    """A representative GACC finding set at 2026-05 (+ a 2026-04 prior for the
    since-last delta): EU bloc, Germany, US, a tiny partner, HK, ASEAN and the
    world total — enough to exercise every selector."""
    period, prev = date(2026, 5, 1), date(2026, 4, 1)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO scrape_runs (source_url, status) "
                    "VALUES ('seed', 'success') RETURNING id")
        run_id = cur.fetchone()[0]
        eu = _alias(cur, kind="eu_bloc")
        asean = _alias(cur, kind="asean")
        world = _alias(cur, kind="world")
        us = _alias(cur, iso2="US")
        ru = _alias(cur, iso2="RU")
        de = _alias(cur, iso2="DE")
        hk = _alias(cur, iso2="HK")
        seq = 0

        def seed(family, alias, kind, flow, p, totals):
            nonlocal seq
            seq += 1
            return _seed_finding(cur, run_id, family=family, alias_id=alias[0],
                                 label=alias[1], kind=kind, flow=flow,
                                 period=p, totals=totals, seq=seq)

        ids = {}
        # Current month — exports.
        ids["eu"] = seed("bilateral", eu, "eu_bloc", "export", period,
                         _totals(0.045, 5.0e11))
        ids["us"] = seed("bilateral", us, "single_country", "export", period,
                         _totals(0.312, 4.0e11))
        ids["ru"] = seed("bilateral", ru, "single_country", "export", period,
                         _totals(0.06, 1.1e11))
        ids["de"] = seed("bilateral", de, "single_country", "export", period,
                         _totals(-0.08, 1.0e11))
        ids["hk"] = seed("bilateral", hk, "single_country", "export", period,
                         _totals(0.11, 2.4e11))
        ids["asean"] = seed("aggregate", asean, "asean", "export", period,
                            _totals(0.205, 6.5e11))
        ids["world"] = seed("aggregate", world, "world", "export", period,
                            _totals(0.9, 3.1e12))  # huge |sm| but excluded from standout
        # One import-flow row so both flows appear for the EU.
        ids["eu_imp"] = seed("bilateral", eu, "eu_bloc", "import", period,
                             _totals(-0.02, 2.5e11))
        # A tiny NON-EU partner with an extreme swing — must NOT take the
        # standout (size floor) nor appear in the Europe section.
        cur.execute(
            "SELECT id, raw_label FROM country_aliases WHERE source='gacc' "
            "AND aggregate_kind IS NULL AND iso2 IN ('JP','KR','CA','AU','BR') "
            "LIMIT 1")
        tiny = cur.fetchone()
        ids["tiny"] = seed("bilateral", tiny, "single_country", "export",
                           period, _totals(2.5, 4.0e9))  # +250%, 12mo €4B < floor
        # Previous month (for since-last): EU export at a lower sm.
        ids["eu_prev"] = seed("bilateral", eu, "eu_bloc", "export", prev,
                              _totals(0.010, 4.9e11))
        # A GACC release row for the identity header.
        cur.execute(
            "INSERT INTO releases (source, period, source_url, currency, "
            "section_number, release_kind, publication_date) "
            "VALUES ('gacc', %s, %s, 'CNY', 4, 'preliminary', %s)",
            (period, "https://english.customs.gov.cn/Statics/test-uuid.html",
             date(2026, 6, 10)),
        )
    return {"period": period, "ids": ids, "us_label": us[1], "ru_label": ru[1],
            "de_label": de[1], "tiny_label": tiny[1]}


def _build(test_db_url):
    import report_builder as rb
    conn = psycopg2.connect(test_db_url)
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return rb._build_gacc_page(cur)
    finally:
        conn.close()


def test_build_gacc_page_none_on_empty_db(fresh_db, test_db_url):
    assert _build(test_db_url) is None


def test_strip_selects_structurally_and_in_slot_order(seeded, test_db_url):
    gp = _build(test_db_url)
    assert gp is not None
    assert gp.tab_label == "GACC-only (May 2026)"
    kickers = [i.kicker for i in gp.strip]
    assert kickers == ["CHINA → EU", "CHINA → US", "CHINA → ASEAN",
                       "CHINA → WORLD"]
    faces = {i.kicker: i.formatted for i in gp.strip}
    assert faces["CHINA → EU"] == "+4.5%"
    assert faces["CHINA → US"] == "+31.2%"


def test_standout_excludes_world_total_and_size_floors(seeded, test_db_url):
    """The world total (+90% here) is the wire headline, not our standout;
    the tiny partner (+250% on a €4B base) is size-floored. The US move
    (+31.2%, biggest surviving |single-month|) wins."""
    gp = _build(test_db_url)
    assert seeded["us_label"] in gp.standout.prose
    assert "31.2%" in gp.standout.prose
    assert seeded["tiny_label"] not in gp.standout.prose


def test_by_country_groups_europe_first_then_rest_of_world(
        seeded, test_db_url):
    """The 2026-07-15 consolidation: ONE by-country roster, grouped — the
    Europe group (EU bloc first, then member states + UK) then Rest of the
    world (everything else, US and tiny partners included: the roster has
    no size floor — small partners are data, not noise)."""
    gp = _build(test_db_url)
    assert gp.by_country.metrics.get("grouped") is True
    groups = {g.title: g for g in gp.by_country.sections}
    assert list(groups) == ["Europe", "Rest of the world"]
    eu_titles = [s.title for s in groups["Europe"].sections]
    row_titles = [s.title for s in groups["Rest of the world"].sections]
    # EU bloc leads Europe; Germany (EU member) is in the Europe group; the
    # US and the tiny non-EU partner land in Rest of the world.
    assert seeded["de_label"] in eu_titles
    assert seeded["us_label"] in row_titles
    assert seeded["tiny_label"] in row_titles
    assert seeded["us_label"] not in eu_titles
    # Both flows for the bloc.
    bloc = groups["Europe"].sections[0]
    assert {f.metrics["flow"] for f in bloc.findings} == {"export", "import"}
    # Rest of world orders biggest-first by the larger flow's 12mo value.
    def _scale(sec):
        return max((f.metrics.get("current_eur") or 0.0) for f in sec.findings)
    scales = [_scale(s) for s in groups["Rest of the world"].sections]
    assert scales == sorted(scales, reverse=True)


def test_eu_bloc_expander_defaults_open_everywhere(seeded, test_db_url):
    """The EU bloc's twisty ships unfolded (Luke, 2026-07-05) — one open
    profile signals what every collapsed country row contains. Marker is
    the structural eu_bloc kind, set at build time; every other partner
    (both groups) stays collapsed. `_gacc_bilateral_section` — the gacc
    VARIANT's flat section, no longer on the Briefing since the 2026-07-15
    consolidation — keeps the same contract."""
    import report_builder as rb

    # GACC page: the By-country Europe group leads with the open bloc.
    gp = _build(test_db_url)
    eu_group = gp.by_country.sections[0]
    eu_sec = eu_group.sections[0]
    assert eu_sec.metrics.get("default_open") is True
    others = (eu_group.sections[1:]
              + [s for g in gp.by_country.sections[1:] for s in g.sections])
    assert all(not s.metrics.get("default_open") for s in others)

    # The gacc variant's flat section: same single-open contract.
    conn = psycopg2.connect(test_db_url)
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        variant_sec = rb._gacc_bilateral_section(cur, seeded["period"])
    finally:
        conn.close()
    flags = {s.title: bool(s.metrics.get("default_open"))
             for s in variant_sec.sections}
    assert sum(flags.values()) == 1  # exactly one open: the bloc
    assert flags[eu_sec.title] is True

    # And the renderer honours it in the grouped roster: the open attribute
    # lands on exactly one details element.
    from report_render_html import _gacc_bilateral_html
    h = _gacc_bilateral_html(gp.by_country)
    assert '<details class="partner" open id=' in h
    assert h.count('<details class="partner" open') == 1


def test_world_rows_order_and_hub_moved_to_by_country(seeded, test_db_url):
    gp = _build(test_db_url)
    rows = gp.world.metrics["rows"]
    labels = [r["label"] for r in rows]
    # World total first, then blocs + the named majors by size.
    assert rows[0]["kind"] == "world"
    assert "ASEAN" in " ".join(labels)
    # The majors are the US and Russia (2026-07-15: the cast equals the
    # region charts' cast); the EU bloc joins them; the ordinary EU member
    # (Germany) does NOT — it stays in the By-country Europe group. The HK
    # entrepôt line left this section entirely. (No Middle East entity is
    # possible: GACC's release carries no such aggregate and names no
    # Middle East partners.)
    kinds = {r["label"]: r["kind"] for r in rows}
    assert "eu_bloc" in kinds.values()
    assert seeded["us_label"] in labels
    assert seeded["ru_label"] in labels
    shorts = {r["label"]: r.get("short_label") for r in rows}
    assert shorts[seeded["ru_label"]] == "Russia"
    assert seeded["de_label"] not in labels
    assert not any("Hong Kong" in lbl for lbl in labels)
    assert not any(r.get("is_hub") for r in rows)
    # The hub flag rides the By-country row instead — structurally (iso2).
    row_group = {g.title: g for g in gp.by_country.sections}["Rest of the world"]
    hk = [s for s in row_group.sections if "Hong Kong" in s.title]
    assert hk and hk[0].metrics.get("is_hub") is True
    # No other partner inherits the flag.
    assert sum(1 for g in gp.by_country.sections for s in g.sections
               if s.metrics.get("is_hub")) == 1


def test_since_last_computes_single_month_swing(seeded, test_db_url):
    gp = _build(test_db_url)
    assert gp.since_last["prev_period"] == "2026-04-01"
    eu_rows = [r for r in gp.since_last["rows"]
               if r["flow"] == "export" and r["basis"] == "single-month"
               and abs(r["delta"] - 0.035) < 1e-9]
    assert eu_rows, gp.since_last["rows"]


def test_identity_reads_release_and_calendar(seeded, test_db_url):
    gp = _build(test_db_url)
    ident = gp.identity
    assert ident["published"] == "2026-06-10"
    from briefing_pack._helpers import GACC_CN_STATS_INDEX_URL
    assert ident["source_url_zh"] == GACC_CN_STATS_INDEX_URL
    expected_due = release_calendar.expected_publish_date(
        "eurostat", seeded["period"])
    assert ident["confirmation_due"] == (
        expected_due.isoformat() if expected_due else None)
    # The epistemic framing consolidated onto `understanding` (2026-07-05);
    # identity carries dates + links only.
    assert "caveats" not in ident
    assert "mainland customs territory" in gp.understanding
