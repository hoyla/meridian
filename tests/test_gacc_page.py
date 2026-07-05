"""Tests for the GACC-only tab (dev_notes/2026-07-05-gacc-update-page-design.md):
the `_build_gacc_page` builder (DB-backed, live-Postgres approach) and the
renderer's two-track surface — period-explicit tab labels, the masthead
descent, the identity strips, the context strip, the world table's entrepôt
line, and the since-last-read delta."""

from __future__ import annotations

import dataclasses
from datetime import date, datetime

import psycopg2
import psycopg2.extras
import pytest

import release_calendar
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
            "source_url_zh": "https://www.customs.gov.cn/Statics/x.html",
            "caveats": ["China’s own customs figures.", "FX note."],
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
        europe=rm.Section(id="gacc-europe", title="Europe up close",
                          kind="gacc_bilateral", intro="China’s own numbers.",
                          metrics={"order_note": "sharpest move first"}),
        world=rm.Section(id="gacc-world", title="China and the world",
                         kind="gacc_world", intro="Context.", metrics={"rows": [
                             {"label": "Total", "kind": "world", "is_hub": False,
                              "flows": {"export": {
                                  "sm_yoy": 0.04, "ytd_yoy": 0.05,
                                  "rolling_yoy": 0.06, "rolling_eur": 3.1e12,
                                  "finding_id": 44}}},
                             {"label": "ASEAN", "kind": "asean", "is_hub": False,
                              "flows": {"export": {
                                  "sm_yoy": 0.21, "ytd_yoy": 0.15,
                                  "rolling_yoy": 0.12, "rolling_eur": 6.5e11,
                                  "finding_id": 46}}},
                             {"label": "Hong Kong, China", "kind": "single_country",
                              "is_hub": True,
                              "flows": {"export": {
                                  "sm_yoy": 0.11, "ytd_yoy": None,
                                  "rolling_yoy": 0.02, "rolling_eur": 2.4e11,
                                  "finding_id": 45}}},
                         ], "period": "2026-05-01"}),
        since_last={"prev_period": "2026-04-01", "rows": [
            {"label": "the EU", "flow": "export", "basis": "single-month",
             "prev_yoy": 0.031, "cur_yoy": 0.124, "delta": 0.093,
             "finding_id": 42}]},
        understanding="**Reading the direction.** Test copy.",
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
    assert "GACC context · to May 2026" in h
    # The GACC-context chip bridges to the GACC-only tab.
    assert 'href="#tab-gacc"' in h


def test_gacc_identity_strip_dates_links_and_about_page():
    h = render_html(_report(gacc_page=_gacc_page()))
    assert "GACC · May 2026" in h
    assert "Published 9 Jun 2026" in h
    assert "European confirmation due ~15 Jul 2026" in h
    assert "中文" in h and "www.customs.gov.cn" in h
    # The standing caveats live in a collapsed "About this page" disclosure
    # (Luke, 2026-07-05) — present in the markup, but behind a summary, not
    # always-visible bullets.
    assert "About this page</summary>" in h
    assert "China’s own customs figures." in h
    i_summary = h.index("About this page</summary>")
    i_caveat = h.index("China’s own customs figures.")
    assert i_summary < i_caveat  # caveat body sits inside the disclosure


def test_world_table_orders_and_labels_the_entrepot_line():
    h = render_html(_report(gacc_page=_gacc_page()))
    assert "entrepôt signal" in h
    # World total first, bloc next, the hub line last.
    i_total, i_asean, i_hk = (h.index(">Total<"), h.index(">ASEAN<"),
                              h.index("Hong Kong, China"))
    assert i_total < i_asean < i_hk
    # A missing operator renders an em-dash cell, never a fabricated figure.
    assert '<td class="num">—</td>' in h


def test_since_last_read_renders_swings_and_gap_case():
    h = render_html(_report(gacc_page=_gacc_page()))
    assert "Since the last read" in h
    assert "+9.3pp" in h  # 0.031 → 0.124
    # The Jan–Feb-gap case: no comparable previous month → honest one-liner.
    gp = _gacc_page(since_last={"prev_period": None, "rows": []})
    h2 = render_html(_report(gacc_page=gp))
    assert "No directly comparable previous GACC month" in h2


def test_strip_and_standout_render_with_drawer_hooks():
    h = render_html(_report(gacc_page=_gacc_page()))
    assert "CHINA → EU" in h and "+12.4%" in h
    assert "Standout move" in h and "sharpest single-month shift" in h
    # Europe section carries its own ordering phrase, not the main tab's.
    assert "sharpest move first" in h


def test_understanding_expander_is_collapsed_disclosure():
    h = render_html(_report(gacc_page=_gacc_page()))
    assert "Understanding these figures" in h
    assert "Reading the direction." in h


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
    return {"period": period, "ids": ids, "us_label": us[1],
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


def test_europe_includes_members_and_bloc_but_not_us_or_tiny(
        seeded, test_db_url):
    gp = _build(test_db_url)
    titles = [s.title for s in gp.europe.sections]
    # EU bloc leads; Germany (EU member) follows; the US and the tiny
    # non-EU partner never appear in the Europe section.
    assert titles[0] == gp.europe.sections[0].title  # bloc first by construction
    assert seeded["de_label"] in titles
    assert seeded["us_label"] not in titles
    assert seeded["tiny_label"] not in titles
    # Both flows for the bloc.
    bloc = gp.europe.sections[0]
    assert {f.metrics["flow"] for f in bloc.findings} == {"export", "import"}


def test_world_rows_order_and_hub_flag(seeded, test_db_url):
    gp = _build(test_db_url)
    rows = gp.world.metrics["rows"]
    labels = [r["label"] for r in rows]
    # World total first, then blocs, HK hub last.
    assert rows[0]["kind"] == "world"
    assert rows[-1]["is_hub"] is True
    assert "ASEAN" in " ".join(labels)


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
    assert ident["source_url_zh"].endswith(
        "www.customs.gov.cn/Statics/test-uuid.html")
    expected_due = release_calendar.expected_publish_date(
        "eurostat", seeded["period"])
    assert ident["confirmation_due"] == (
        expected_due.isoformat() if expected_due else None)
    assert any("mainland customs territory" in c for c in ident["caveats"])
