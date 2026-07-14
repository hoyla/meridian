"""Tests for the commodity take (PR D of
dev_notes/2026-07-14-gacc-commodity-highlights.md § takes): the two new
catalog entries, the verify-or-reject generation path, the fact-set assembly
(input set == displayed set, display-scale numeric twins), the reuse-graft,
and the render inside the commodities section."""

from __future__ import annotations

from datetime import date

import pytest

import hypothesis_catalog
import llm_gacc_page
import portal_takes_reuse
import report_builder as rb
import report_model as rm
from report_render_html import render_html


class FakeBackend:
    def __init__(self, *responses: str):
        self.responses = list(responses)
        self.calls: list[tuple[str, str]] = []

    def generate(self, system: str, user: str) -> str:
        self.calls.append((system, user))
        return self.responses.pop(0)


@pytest.fixture(autouse=True)
def _capture_rejections(monkeypatch):
    captured: list[dict] = []

    def _fake_log(**kwargs):
        captured.append(kwargs)
        return 1

    monkeypatch.setattr(llm_gacc_page.llm_rejection_log, "log_rejection",
                        _fake_log)
    yield captured


CFACTS = {
    "lines": [
        "China's exports of Rare-earth ore, metals, compounds, Jun 2026: "
        "-34.0% YoY by value (single month, CNY terms); -38.0% by volume "
        "(4k ton); €55.0M in the month",
        "China's exports of Motor vehicles, Jun 2026: +69.0% YoY by value "
        "(single month, CNY terms); +71.2% by volume (1.06mn autos); "
        "€15.80B in the month",
        "  Computed fact (Motor vehicles): First month above 1.00mn autos "
        "in our records (catalogue line tracked from 2019)",
    ],
    "numbers": {
        "cm1_smval": -0.34, "cm1_smqty": -0.38, "cm1_eur": 5.5e7,
        "cm2_smval": 0.69, "cm2_smqty": 0.712, "cm2_eur": 1.58e10,
        "cm2_milestone_threshold_units": 1e6,
        "cm2_milestone_threshold_units_disp": 1.0,
        "cm2_milestone_month_units": 1.06e6,
        "cm2_milestone_month_units_disp": 1.06,
    },
    "prov": {"cm1_smval": 71, "cm1_smqty": 71, "cm2_smval": 72,
             "cm2_smqty": 72},
    "strip_fids": [72],
}


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

def test_catalog_carries_the_two_commodity_hypotheses():
    for hid in ("export_controls_china", "domestic_demand_pivot"):
        entry = hypothesis_catalog.CATALOG_BY_ID[hid]
        assert entry["description"]
        assert len(entry["corroboration_steps"]) >= 3
        assert hid in llm_gacc_page.COMMODITY_CATALOG_IDS
        # deliberately NOT offered to the partner-level page synthesis
        assert hid not in llm_gacc_page.SYNTHESIS_CATALOG_IDS


# ---------------------------------------------------------------------------
# Generation (verify-or-reject)
# ---------------------------------------------------------------------------

def test_commodity_take_happy_path(_capture_rejections):
    fake = FakeBackend(
        '{"summary": "China\'s exports of motor vehicles rose 71.2% by '
        'volume to a first month above 1.06mn autos, while China\'s exports '
        'of rare earths fell 34.0% by value and 38.0% by volume.", '
        '"hypotheses": [{"id": "export_controls_china", "rationale": '
        '"Rare-earth volume (-38.0%) fell harder than value (-34.0%)."}, '
        '{"id": "domestic_demand_pivot", "rationale": '
        '"Vehicle volume growth (71.2%) outpaced value growth (69.0%)."}]}'
    )
    out = llm_gacc_page.generate_commodity_take(CFACTS, backend=fake)
    assert out is not None, _capture_rejections
    assert "1.06mn autos" in out["summary"]
    labels = [h["label"] for h in out["hypotheses"]]
    assert labels == ["Chinese export controls", "Domestic-demand pivot to exports"]
    # catalog steps attach deterministically
    assert all(h["steps"] for h in out["hypotheses"])
    # the system prompt is the commodity one; the user prompt carries the
    # commodity catalog, not the partner-level subset
    system, user = fake.calls[0]
    assert "headline commodity catalogue" in system
    assert "export_controls_china" in user
    assert not _capture_rejections


def test_commodity_take_rejects_partner_level_hypothesis(_capture_rejections):
    # eu_demand_pull is in the SYNTHESIS subset but not the commodity one —
    # the take must reject it even though it's a real catalog id.
    fake = FakeBackend(
        '{"summary": "China\'s exports of motor vehicles rose 69.0% by '
        'value.", "hypotheses": [{"id": "eu_demand_pull", '
        '"rationale": "Vehicles rose 69.0%."}]}'
    )
    out = llm_gacc_page.generate_commodity_take(CFACTS, backend=fake)
    assert out is None
    assert any("not in the offered catalog" in r["reason"]
               for r in _capture_rejections)


def test_commodity_take_rejects_unverified_number(_capture_rejections):
    fake = FakeBackend(
        '{"summary": "China\'s exports of motor vehicles rose 84.0% by '
        'value.", "hypotheses": []}'
    )
    out = llm_gacc_page.generate_commodity_take(CFACTS, backend=fake)
    assert out is None
    assert any("unverified" in r["reason"] for r in _capture_rejections)


def test_commodity_take_abstention_is_silent(_capture_rejections):
    fake = FakeBackend('{"summary": null}')
    assert llm_gacc_page.generate_commodity_take(CFACTS, backend=fake) is None
    assert not _capture_rejections


def test_citations_are_tight_on_a_large_fact_set(_capture_rejections):
    """Regression for the 2026-07-14 debut over-citation: with ~200 facts,
    the old substring heuristic cited 65 findings for a four-number summary
    (short numerals like '2' matched everything). Citations now come from
    the verifier's own matcher: only findings whose numbers the text
    actually used."""
    # A commodity-scale fact pool: the two discussed rows plus 60 bystander
    # rows with distinctive rates and small integer-ish values that the old
    # heuristic would have swept in.
    numbers = dict(CFACTS["numbers"])
    prov = dict(CFACTS["prov"])
    for i in range(60):
        numbers[f"bg{i}_smval"] = 0.01 + i * 0.013   # +1.0% … +77.7%
        numbers[f"bg{i}_eur"] = 1e8 * (i + 1)
        prov[f"bg{i}_smval"] = 1000 + i
        prov[f"bg{i}_eur"] = 1000 + i
    facts = dict(CFACTS, numbers=numbers, prov=prov)
    fake = FakeBackend(
        '{"summary": "China\'s exports of rare earths fell 34.0% by value '
        'while China\'s exports of motor vehicles rose 71.2% by volume.", '
        '"hypotheses": []}'
    )
    out = llm_gacc_page.generate_commodity_take(facts, backend=fake)
    assert out is not None, _capture_rejections
    # exactly the two discussed findings — none of the 60 bystanders
    assert sorted(out["citations"]) == [71, 72]


# ---------------------------------------------------------------------------
# Fact-set assembly (input set == displayed set)
# ---------------------------------------------------------------------------

def _page_with_commodities() -> rm.GaccPage:
    sec = rm.Section(id="gacc-commodities", title="What’s moving",
                     kind="gacc_commodities", intro="i", about="a")
    sec.metrics = {
        "period": "2026-06-01",
        "export_rows": [{
            "label": "Motor vehicles", "flow": "export", "finding_id": 72,
            "sm_value_yoy": 0.69, "sm_quantity_yoy": 0.712,
            "eur_month": 1.58e10, "quantity_unit": "10,000 Autos",
            "quantity_display": "1.06mn autos", "ytd_value_yoy": 0.50,
            "published_ytd_yoy_pct": 50.4, "caveats": [],
            "facts": {"milestone": {
                "text": "First month above 1.00mn autos in our records "
                        "(catalogue line tracked from 2019)",
                "threshold_units": 1e6, "month_units": 1.06e6,
                "method": "m"}},
        }],
        "export_aggregates": [{
            "label": "Hi-tech products", "flow": "export", "finding_id": 73,
            "sm_value_yoy": 0.35, "sm_quantity_yoy": None,
            "eur_month": 9.0e10, "quantity_unit": None,
            "ytd_value_yoy": 0.354, "published_ytd_yoy_pct": 35.4,
            "caveats": ["catalogue_aggregate"],
        }],
        "import_rows": [], "import_aggregates": [],
    }
    return rm.GaccPage(
        data_period=date(2026, 6, 1), tab_label="GACC-only (Jun 2026)",
        commodities=sec,
        commodity_strip=[rm.Indicator(
            key="k", kicker="EXPORT MOVER", label="Motor vehicles",
            value=0.69, unit="yoy_pct", formatted="+69.0%",
            provenance=rm.Provenance(finding_ids=[72], source="gacc",
                                     as_of=date(2026, 6, 1)))],
    )


def test_commodity_fact_assembly_covers_all_rows_and_scales():
    page = _page_with_commodities()
    facts = rb._gacc_commodity_llm_facts(page, date(2026, 6, 1))
    text = "\n".join(facts["lines"])
    # every displayed row present, aggregates marked
    assert "China's exports of Motor vehicles" in text
    assert "Hi-tech products [catalogue aggregate" in text
    # computed fact travels as a line
    assert "First month above 1.00mn autos" in text
    # rates + EUR levels + published-basis marker in the display formats
    assert "+69.0% YoY by value (single month, CNY terms)" in text
    assert "+71.2% by volume (1.06mn autos)" in text
    assert "€15.80B in the month" in text
    # numbers: fractions, EUR raw, and the display-scale milestone twins
    nums = facts["numbers"]
    assert nums["cm1_smval"] == pytest.approx(0.69)
    assert nums["cm1_eur"] == pytest.approx(1.58e10)
    assert nums["cm1_milestone_month_units"] == pytest.approx(1.06e6)
    assert nums["cm1_milestone_month_units_disp"] == pytest.approx(1.06)
    # provenance maps to the row findings; strip fids present
    assert facts["prov"]["cm1_smval"] == 72
    assert facts["strip_fids"] == [72]


def test_commodity_facts_empty_without_section():
    page = rm.GaccPage(data_period=date(2026, 6, 1), tab_label="t")
    facts = rb._gacc_commodity_llm_facts(page, date(2026, 6, 1))
    assert facts["lines"] == []


# ---------------------------------------------------------------------------
# Reuse-graft
# ---------------------------------------------------------------------------

_TAKE = {"summary": "s", "citations": [72], "hypotheses": []}


def _graft_report(gp_period="2026-06-01") -> rm.Report:
    return rm.Report(
        meta=rm.ReportMeta(data_period=date(2026, 4, 1), variant="eurostat",
                           snapshot_id="t", generated_at=None),
        headline=rm.Headline(variant="eurostat", lead_title="t", note="",
                             items=[]),
        gacc_page=rm.GaccPage(data_period=date.fromisoformat(gp_period),
                              tab_label="t"),
    )


def test_graft_carries_commodity_take_on_same_gacc_period():
    report = _graft_report()
    prior = {"gacc_page": {"data_period": "2026-06-01",
                           "commodity_take": _TAKE}}
    n = portal_takes_reuse.graft_gacc_slots(report, prior)
    assert n == 1
    assert report.gacc_page.commodity_take == _TAKE


def test_graft_drops_commodity_take_on_new_gacc_month():
    report = _graft_report("2026-07-01")
    prior = {"gacc_page": {"data_period": "2026-06-01",
                           "commodity_take": _TAKE}}
    assert portal_takes_reuse.graft_gacc_slots(report, prior) == 0
    assert report.gacc_page.commodity_take is None


def test_graft_never_clobbers_fresh_commodity_take():
    report = _graft_report()
    fresh = {"summary": "fresh", "citations": [], "hypotheses": []}
    report.gacc_page.commodity_take = fresh
    prior = {"gacc_page": {"data_period": "2026-06-01",
                           "commodity_take": _TAKE}}
    portal_takes_reuse.graft_gacc_slots(report, prior)
    assert report.gacc_page.commodity_take == fresh


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

def test_take_box_renders_inside_commodities_section():
    page = _page_with_commodities()
    page.commodity_take = {
        "summary": "Vehicle exports led the month.",
        "citations": [72],
        "hypotheses": [{
            "id": "domestic_demand_pivot",
            "label": "Domestic-demand pivot to exports",
            "rationale": "Volume outpaced value.",
            "steps": ["Check CAAM domestic sales data"],
        }],
    }
    html_out = render_html(rm.Report(
        meta=rm.ReportMeta(data_period=date(2026, 4, 1), variant="eurostat",
                           snapshot_id="t", generated_at=None),
        headline=rm.Headline(variant="eurostat", lead_title="t", note="",
                             items=[]),
        gacc_page=page,
    ))
    sec = html_out[html_out.index('id="gacc-commodities"'):]
    sec = sec[:sec.index("</section>")]
    assert "Machine reading of the product tables" in sec
    assert "Vehicle exports led the month." in sec
    assert "Domestic-demand pivot to exports" in sec
    assert "Check CAAM domestic sales data" in sec
    assert "finding/72" in sec
    # the box sits ABOVE the tables it reads
    assert sec.index("Machine reading") < sec.index("China’s exports by product")
