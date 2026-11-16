"""Headline movers are reported per COMMODITY, not per finding.

When both directions of one commodity qualify, they used to take two of the
five slots and read as unrelated observations, each with its own take written
from identical facts (Sep 2026: finished cars at 1 and 3, lithium-ion
batteries at 2 and 4). They are now one item: lead flow, the other direction,
the commodity's net position, one take for the pair.

Pure-function tests — no DB, no model.
"""
from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace as NS

import llm_takes
import report_builder
from briefing_pack._helpers import _net_position, _pair_movers_by_subject
from briefing_pack.sections.front_page import _mover_sentence
from report_model import Headline
from report_render_html import _headline


def _row(fid, group, *, export=False, yoy, cur, prior, score, kg=None):
    return {
        "id": fid, "group_name": group,
        "subkind": "hs_group_yoy_export" if export else "hs_group_yoy",
        "yoy_pct": yoy, "yoy_pct_kg": kg, "current_eur": cur,
        "prior_eur": prior, "current_end": date(2026, 9, 1), "score": score,
        "predictability": ("🟡", 0.5, 6),
    }


def _cars_and_batteries():
    """The Sep-2026 shape: five qualifying findings, three commodities."""
    return [
        _row(1, "Cars", yoy=0.625, cur=22.45e9, prior=13.82e9, score=6.5, kg=0.739),
        _row(2, "Batteries", yoy=0.394, cur=32.97e9, prior=23.65e9, score=4.1, kg=0.706),
        _row(3, "Cars", export=True, yoy=-0.391, cur=6.03e9, prior=9.90e9, score=3.8, kg=-0.359),
        _row(4, "Batteries", export=True, yoy=-0.317, cur=354.1e6, prior=518.4e6, score=2.7, kg=0.21),
        _row(5, "Wind", yoy=0.303, cur=627.6e6, prior=481.7e6, score=2.6, kg=0.396),
    ]


# --- selection -----------------------------------------------------------

def test_two_flows_of_one_commodity_become_one_row():
    rows = _pair_movers_by_subject(_cars_and_batteries())
    assert [r["group_name"] for r in rows] == ["Cars", "Batteries", "Wind"]
    assert rows[0]["id"] == 1 and rows[0]["counterpart"]["id"] == 3
    assert rows[1]["id"] == 2 and rows[1]["counterpart"]["id"] == 4


def test_a_single_flow_mover_is_unchanged():
    wind = _pair_movers_by_subject(_cars_and_batteries())[2]
    assert wind["counterpart"] is None and wind["net"] is None


def test_the_larger_flow_leads_the_pair_in_the_rank_its_best_score_earned():
    """Sep 2026, semiconductor equipment: the score fronted a €172M import
    rise over an €8B export fall. The bigger flow leads the sentence; the
    subject keeps the rank (and score) of whichever flow scored best."""
    rows = _pair_movers_by_subject([
        _row(9, "Other", yoy=0.9, cur=5e9, prior=2.6e9, score=8.7),
        _row(1, "Chips kit", yoy=0.235, cur=172.5e6, prior=139.7e6, score=1.94),
        _row(3, "Chips kit", export=True, yoy=-0.165, cur=8.04e9, prior=9.63e9, score=1.63),
        _row(7, "Third", yoy=0.15, cur=1e9, prior=0.87e9, score=1.7),
    ])
    assert [r["id"] for r in rows] == [9, 3, 7]
    chips = rows[1]
    assert chips["counterpart"]["id"] == 1 and chips["score"] == 1.94
    assert "counterpart" not in chips["counterpart"]
    assert chips["net"]["kind"] == "surplus"
    s = _mover_sentence(chips)
    assert s.startswith("**EU-27 exports of [Chips kit](#chips-kit) to China** fell 16.5%")
    assert "the much smaller flow of EU-27 imports from China (€172.5M) rose 23.5%" in s


def test_a_flow_that_did_not_qualify_is_not_attached():
    """Only rows that cleared the mover filters reach the pairing step, so a
    low-base or sub-threshold other side is simply absent — never mentioned."""
    rows = _pair_movers_by_subject([_cars_and_batteries()[0]])
    assert rows[0]["counterpart"] is None and rows[0]["net"] is None


# --- net position --------------------------------------------------------

def test_net_position_deficit_widened():
    cars = _pair_movers_by_subject(_cars_and_batteries())[0]
    net = cars["net"]
    assert (net["kind"], net["direction"]) == ("deficit", "widened")
    assert round(net["current_eur"] / 1e9, 2) == 16.42   # 22.45 − 6.03
    assert round(net["prior_eur"] / 1e9, 2) == 3.92      # 13.82 − 9.90
    assert round(net["change_eur"] / 1e9, 2) == 12.50


def test_net_position_is_the_same_whichever_flow_leads():
    a, b = _cars_and_batteries()[0], _cars_and_batteries()[2]
    assert _net_position(a, b) == _net_position(b, a)


def test_net_position_surplus_narrowed_and_swing():
    imp = _row(1, "G", yoy=0.5, cur=3e9, prior=2e9, score=1)
    exp = _row(2, "G", export=True, yoy=-0.2, cur=4e9, prior=5e9, score=1)
    net = _net_position(imp, exp)
    assert (net["kind"], net["direction"]) == ("surplus", "narrowed")
    assert net["change_eur"] == 2e9

    exp2 = _row(2, "G", export=True, yoy=-0.5, cur=2.5e9, prior=5e9, score=1)
    swing = _net_position(imp, exp2)
    assert swing["kind"] == "swing_to_deficit" and swing["direction"] is None
    assert (swing["prior_eur"], swing["current_eur"]) == (3e9, 0.5e9)


def test_net_position_needs_both_prior_totals():
    imp = _row(1, "G", yoy=0.5, cur=3e9, prior=None, score=1)
    exp = _row(2, "G", export=True, yoy=-0.2, cur=4e9, prior=5e9, score=1)
    assert _net_position(imp, exp) is None


# --- the sentence --------------------------------------------------------

def test_paired_sentence_reports_both_directions_and_the_net():
    cars = _pair_movers_by_subject(_cars_and_batteries())[0]
    s = _mover_sentence(cars)
    assert s.startswith("**EU-27 imports of [Cars](#cars) from China** rose "
                        "62.5% by value in the 12 months to Sep 2026, to "
                        "€22.45B; volume up 73.9%.")
    assert ("In the other direction, EU-27 exports to China fell 39.1%, to "
            "€6.03B; volume down 35.9%.") in s
    assert ("Net, the EU-27’s deficit with China in this category widened "
            "by €12.50B, to €16.42B.") in s
    assert s.endswith("double-check before headlining. `finding/1` `finding/3`")


def test_paired_sentence_states_a_large_size_gap_in_words():
    """€33B of imports against €354M of exports are not mirror images."""
    batteries = _pair_movers_by_subject(_cars_and_batteries())[1]
    s = _mover_sentence(batteries)
    assert ("In the other direction, the much smaller flow of EU-27 exports "
            "to China (€354.1M) fell 31.7%; volume up 21.0%.") in s


def test_paired_sentence_says_also_when_both_flows_move_the_same_way():
    rows = _pair_movers_by_subject([
        _row(1, "G", yoy=0.5, cur=3e9, prior=2e9, score=2),
        _row(2, "G", export=True, yoy=0.25, cur=2.5e9, prior=2e9, score=1),
    ])
    assert "EU-27 exports to China also rose 25.0%" in _mover_sentence(rows[0])


def test_swing_sentence():
    rows = _pair_movers_by_subject([
        _row(1, "G", yoy=0.5, cur=3e9, prior=2e9, score=2),
        _row(2, "G", export=True, yoy=-0.5, cur=2.5e9, prior=5e9, score=1),
    ])
    assert ("swung from a €3.00B surplus to a €500.0M deficit"
            in _mover_sentence(rows[0]))


# --- report model + HTML -------------------------------------------------

def test_headline_item_carries_both_findings_and_the_structured_pair(monkeypatch):
    monkeypatch.setattr(report_builder.labels, "themes_for_group", lambda g: [])
    cars = _pair_movers_by_subject(_cars_and_batteries())[0]
    item = report_builder._headline_item(cars, {})
    assert item.provenance.finding_ids == [1, 3]      # lead first
    assert item.subject["flow"] == "import"
    assert item.subject["counterpart_flow"] == "export"
    assert item.metrics["counterpart"]["finding_id"] == 3
    assert item.metrics["counterpart"]["direction"] == "fell"
    assert item.metrics["net"]["kind"] == "deficit"


def test_single_flow_headline_item_has_no_pair_fields(monkeypatch):
    monkeypatch.setattr(report_builder.labels, "themes_for_group", lambda g: [])
    wind = _pair_movers_by_subject(_cars_and_batteries())[2]
    item = report_builder._headline_item(wind, {})
    assert item.provenance.finding_ids == [5]
    assert "counterpart" not in item.metrics and "net" not in item.metrics


def _payload(fid):
    return {"sources": [{"url": f"https://example.invalid/{fid}",
                         "label": f"release {fid}", "source": "eurostat"}]}


def test_html_gives_a_paired_item_a_drawer_per_finding(monkeypatch):
    monkeypatch.setattr(report_builder.labels, "themes_for_group", lambda g: [])
    cars = _pair_movers_by_subject(_cars_and_batteries())[0]
    item = report_builder._headline_item(cars, {})
    h = Headline(variant="eurostat", lead_title="t", note="", items=[item])

    out = _headline(h, {"1": _payload(1), "3": _payload(3)})

    assert out.count('class="token">finding/1<') == 1
    assert out.count('class="token">finding/3<') == 1
    assert "`finding/" not in out and "<code>finding/" not in out


def test_html_keeps_in_prose_tokens_when_there_is_no_drawer(monkeypatch):
    monkeypatch.setattr(report_builder.labels, "themes_for_group", lambda g: [])
    cars = _pair_movers_by_subject(_cars_and_batteries())[0]
    item = report_builder._headline_item(cars, {})
    h = Headline(variant="eurostat", lead_title="t", note="", items=[item])
    out = _headline(h, {})
    assert "finding/1" in out and "finding/3" in out


# --- the take ------------------------------------------------------------

def _stub_cluster(monkeypatch):
    facts = {"scopes": {"eu_27": {
        "imports": {"yoy_pct": 0.625, "current_12mo_eur": 22.45e9},
        "exports": {"yoy_pct": -0.391, "current_12mo_eur": 6.03e9}}}}
    cluster = NS(group_name="Cars", group_description="cars",
                 finding_id_by_scope_attr={"eu_27.yoy_import": 1,
                                           "eu_27.yoy_export": 3})
    monkeypatch.setattr(llm_takes, "_load_hs_group_clusters", lambda names: [cluster])
    monkeypatch.setattr(llm_takes, "_build_facts", lambda c: json.loads(json.dumps(facts)))


def _paired_anchor():
    cars = _pair_movers_by_subject(_cars_and_batteries())[0]
    return llm_takes.TakeAnchor(finding_id=1, scope="eu_27", flow="imports",
                                counterpart_finding_id=3, net=cars["net"])


def test_paired_prompt_names_both_directions_and_offers_the_net(monkeypatch):
    _stub_cluster(monkeypatch)
    facts, _system, user, _ids = llm_takes._assemble("Cars", _paired_anchor())
    assert "reports BOTH directions of this commodity together" in user
    assert "Sharing a commodity is NOT evidence of a shared cause" in user
    net = facts[llm_takes.NET_FACTS_KEY]
    assert net["position"] == "deficit, widened"
    assert round(net["net_change_eur"] / 1e9, 2) == 12.50
    # formatted for the model (json.dumps escapes the € sign)
    assert '"net_change_eur": "\\u20ac12.50B"' in user


def test_single_flow_prompt_is_unchanged(monkeypatch):
    _stub_cluster(monkeypatch)
    anchor = llm_takes.TakeAnchor(finding_id=1, scope="eu_27", flow="imports")
    facts, _system, user, _ids = llm_takes._assemble("Cars", anchor)
    assert "this take sits under ONE specific headline" in user
    assert llm_takes.NET_FACTS_KEY not in facts


def test_paired_take_may_cite_the_net_and_is_grounded_in_both_findings(monkeypatch):
    _stub_cluster(monkeypatch)
    monkeypatch.setattr(llm_takes, "log_rejection", lambda **kw: 1)
    backend = NS(model="fake", generate=lambda s, u: json.dumps({"questions": [
        {"q": "With EU-27 imports of cars from China up 62.5% while the "
              "deficit in cars widened by €12.50B, is one pressure driving "
              "both sides, or are they unrelated?", "axis": "pair"}]}))
    got = llm_takes.generate_take("Cars", backend, anchor=_paired_anchor())
    assert got is not None
    assert got.grounded_in == [1, 3]


def test_paired_take_must_still_open_on_the_lead_flow(monkeypatch):
    _stub_cluster(monkeypatch)
    monkeypatch.setattr(llm_takes, "log_rejection", lambda **kw: 1)
    backend = NS(model="fake", generate=lambda s, u: json.dumps({"questions": [
        {"q": "Are EU-27 exports of cars to China collapsing?", "axis": "x"}]}))
    assert llm_takes.generate_take("Cars", backend, anchor=_paired_anchor()) is None
