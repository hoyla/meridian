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
    headline = rm.Headline(
        variant="eurostat", lead_title="What April changed", note="note.",
        items=[rm.HeadlineItem(
            subject={"scope": "eu_27", "flow": "export", "group_name": "Cars"},
            metrics={"direction": "fell", "pct": 0.4, "value_eur": 7.5e9},
            stability={"badge": "🟡", "hedge_phrase": None},
            prose="**EU-27 exports of [Cars](#cars) to China** fell 40% `finding/2`",
            drill_down="cars",
            provenance=rm.Provenance(finding_ids=[2], source="eurostat"),
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
    what_changed = rm.WhatChanged(regime="movement", summary="3 findings shifted.",
                                  new_count=1)
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
        facets=rm.Facets(commodity=["Cars"], sector=["78"], theme=["EV supply chain"],
                         end_use=["Consumption"]),
        metrics={"china_share_value": 0.19, "china_share_kg": 0.24,
                 "china_share_finding": 4,
                 "top_cn8": [{"code": "87038010", "eur": 5.0e9}],
                 "reporters": [{"reporter": "DE", "share": 0.66, "yoy": -0.1}],
                 "trajectory": {"EU-27": {"import": "volatile", "export": "peak-and-fall"},
                                "UK": {"import": "volatile"}},
                 "trajectory_findings": [5, 6],
                 "china_export_share_value": 0.015, "china_export_share_finding": 7},
        findings=[rm.Finding(
            finding_id=2, subkind="hs_group_yoy_export", title="EU-27 exports of Cars",
            metrics={"scope": "EU-27", "flow": "export", "yoy_pct": -0.4,
                     "current_eur": 7.5e9, "low_base": False},
            chart_data=rm.ChartData(chart_type="line", series=series),
            provenance=rm.Provenance(finding_ids=[2]))])
    sector = rm.Section(id="sector-detail", title="Sector detail", kind="sector_detail",
                        intro="Every group.", sections=[group])
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
        id="methodology", title="Methodology, sources & caveats", kind="reference",
        intro="How to read.",
        metrics={"caveats": [{"code": "cif_fob", "summary": "CIF vs FOB", "detail": "…"}],
                 "sources": [{"source": "eurostat", "note": "Comext."}]})
    gacc_bi = rm.Section(
        id="gacc-bilateral", title="China's trade by partner (GACC)",
        kind="gacc_bilateral", intro="By partner.",
        sections=[rm.Section(
            id="gacc-united-states", title="United States", kind="gacc_bilateral",
            findings=[rm.Finding(
                finding_id=10, subkind="gacc_bilateral_aggregate_yoy",
                title="China exports to US",
                metrics={"scope": "China", "flow": "export", "yoy_pct": -0.297,
                         "current_eur": 4.6e11},
                provenance=rm.Provenance(finding_ids=[10], source="gacc"))])])
    meta = rm.ReportMeta(data_period=date(2026, 4, 1), variant="eurostat",
                         snapshot_id="t", generated_at=datetime(2026, 6, 20, 12, 0))
    return rm.Report(meta=meta, key_indicators=[deficit_ind], headline=headline,
                     what_changed=what_changed,
                     sections=[state, mirror, sector, structural, reference, gacc_bi])


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
                   "## Methodology, sources & caveats",
                   "## China's trade by partner (GACC)"):
        assert marker in md, marker
    assert "China reports" in md          # cn-only deficit
    assert "z" in md and "2025-11" in md  # mirror-gap z-score
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
    assert "China reports" in h           # cn-only deficit
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
    assert "Source: eurostat" in md
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
