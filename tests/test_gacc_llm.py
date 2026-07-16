"""Tests for the GACC page's LLM layer (llm_gacc_page + its wiring):
the release-synthesis lead-scaffold, the questions-take with answerability
tags, the verify-or-reject guards, the reuse graft's independent period
gate, and the renderer's machine corner. Backend always faked — no paid
calls in the unit loop."""

from __future__ import annotations

import json
from datetime import date, datetime

import pytest

import hypothesis_catalog
import llm_gacc_page
import portal_takes_reuse
import report_model as rm
from report_render_html import render_html


class FakeBackend:
    def __init__(self, *responses: str):
        self.responses = list(responses)
        self.calls: list[tuple[str, str]] = []

    def generate(self, system: str, user: str) -> str:
        self.calls.append((system, user))
        return self.responses.pop(0)


FACTS = {
    "lines": [
        "China’s exports to the EU, May 2026: +4.5% YoY (single month); "
        "YTD +6.2%; 12mo +7.4%",
        "China’s exports to the US, May 2026: +31.2% YoY (single month)",
        "China’s exports to ASEAN, May 2026: +20.5% YoY (single month)",
        "China’s exports to the world, May 2026: +15.6% YoY (single month)",
    ],
    "numbers": {
        "gacc_strip_eu_sm": 0.045, "gacc_strip_eu_ytd": 0.062,
        "gacc_strip_eu_12mo": 0.074,
        "gacc_strip_us_sm": 0.312, "gacc_strip_asean_sm": 0.205,
        "gacc_strip_world_sm": 0.156,
    },
    "prov": {
        "gacc_strip_eu_sm": 42, "gacc_strip_eu_ytd": 42,
        "gacc_strip_eu_12mo": 42,
        "gacc_strip_us_sm": 43, "gacc_strip_asean_sm": 44,
        "gacc_strip_world_sm": 45,
    },
    "strip_fids": [42, 43, 44, 45],
}


@pytest.fixture(autouse=True)
def _capture_rejections(monkeypatch):
    """Rejections go to a list, not the DB — the unit loop asserts on them."""
    captured: list[dict] = []

    def _fake_log(**kwargs):
        captured.append(kwargs)
        return 1

    monkeypatch.setattr(llm_gacc_page.llm_rejection_log, "log_rejection",
                        _fake_log)
    yield captured


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

def test_catalog_carries_us_tariff_diversion():
    """The China-side diversion entry exists with deterministic steps, and
    every id the synthesis offers is a real catalog entry."""
    entry = hypothesis_catalog.CATALOG_BY_ID["us_tariff_diversion"]
    assert entry["label"] == "US-tariff diversion"
    assert entry["corroboration_steps"]
    for hid in llm_gacc_page.SYNTHESIS_CATALOG_IDS:
        assert hid in hypothesis_catalog.CATALOG_BY_ID, hid


# ---------------------------------------------------------------------------
# Synthesis: verify-or-reject
# ---------------------------------------------------------------------------

def _synthesis_json(summary, hypotheses=None):
    return json.dumps({"summary": summary, "hypotheses": hypotheses or []})


def test_synthesis_happy_path_attaches_catalog_steps(_capture_rejections):
    raw = _synthesis_json(
        "China’s exports to the EU rose 4.5% in the month, well below the "
        "20.5% ASEAN rise and the 15.6% world average, while US shipments "
        "rose 31.2%.",
        [{"id": "us_tariff_diversion",
          "rationale": "The US line at 31.2% against the world’s 15.6% fits "
                       "a diversion pattern."}],
    )
    out = llm_gacc_page.generate_synthesis(FACTS, FakeBackend(raw))
    assert out is not None
    assert "4.5%" in out["summary"]
    assert out["hypotheses"][0]["label"] == "US-tariff diversion"
    # Steps come from the catalog, never the model.
    assert out["hypotheses"][0]["steps"] == (
        hypothesis_catalog.get_corroboration_steps(["us_tariff_diversion"]))
    assert 42 in out["citations"]  # the EU reading is cited
    assert not _capture_rejections


def test_synthesis_rejects_unverified_number(_capture_rejections):
    raw = _synthesis_json(
        "China’s exports to the EU rose 9.9% in the month.")  # not a fact
    out = llm_gacc_page.generate_synthesis(FACTS, FakeBackend(raw))
    assert out is None
    assert _capture_rejections[0]["cluster_name"] == "gacc_page_synthesis"
    assert "unverified" in _capture_rejections[0]["reason"]


def test_synthesis_rejects_off_catalog_hypothesis(_capture_rejections):
    raw = _synthesis_json(
        "China’s exports to the EU rose 4.5% in the month.",
        [{"id": "made_up_cause", "rationale": "sounds plausible"}],
    )
    out = llm_gacc_page.generate_synthesis(FACTS, FakeBackend(raw))
    assert out is None
    assert "not in the offered catalog" in _capture_rejections[0]["reason"]


def test_synthesis_abstention_is_silent(_capture_rejections):
    out = llm_gacc_page.generate_synthesis(
        FACTS, FakeBackend(json.dumps({"summary": None})))
    assert out is None
    assert not _capture_rejections  # abstaining is not a rejection


def test_synthesis_rejects_unparseable_and_logs_raw(_capture_rejections):
    out = llm_gacc_page.generate_synthesis(FACTS, FakeBackend("sorry, prose"))
    assert out is None
    assert _capture_rejections[0]["stage"] == "parse"
    assert _capture_rejections[0]["raw_output"] == "sorry, prose"


# ---------------------------------------------------------------------------
# Questions: interrogative + answerability enum
# ---------------------------------------------------------------------------

def _questions_json(qs):
    return json.dumps({"questions": qs})


def test_questions_happy_path(_capture_rejections):
    raw = _questions_json([
        {"q": "Is the US rise of 31.2% concentrated in a few partner "
              "routes?", "axis": "concentration", "answerable": "world_table"},
        {"q": "Will Eurostat’s mirror confirm the EU’s 4.5% rise?",
         "axis": "mirror check", "answerable": "eurostat_confirmation"},
    ])
    out = llm_gacc_page.generate_questions(FACTS, FakeBackend(raw))
    assert out is not None and len(out) == 2
    assert out[0]["answerable"] == "world_table"
    assert not _capture_rejections


def test_questions_reject_bad_tag_and_missing_question_mark(
        _capture_rejections):
    bad_tag = _questions_json(
        [{"q": "Is this rerouting?", "axis": "x", "answerable": "google_it"}])
    assert llm_gacc_page.generate_questions(FACTS, FakeBackend(bad_tag)) is None
    assert "not in the enum" in _capture_rejections[0]["reason"]

    flat = _questions_json(
        [{"q": "This is rerouting.", "axis": "x", "answerable": "world_table"}])
    assert llm_gacc_page.generate_questions(FACTS, FakeBackend(flat)) is None
    assert "question mark" in _capture_rejections[1]["reason"]


def test_time_period_shorthand_not_a_false_positive(_capture_rejections):
    """Regression (2026-07-05 live run): '12mo' — the portal's own compact
    register, echoed by the model — must strip as a time period, not parse
    as an unverifiable count of 12."""
    from llm_framing import verify_numbers
    ok, failures = verify_numbers(
        "set against its 12mo -29.7% and the 24-month window",
        {"us_12mo": -0.297})
    assert ok, failures
    raw = _questions_json(
        [{"q": "Does the +31.2% surge against the 12mo -29.7% reading fit "
              "any tariff round?", "axis": "base vs trend",
          "answerable": "drawers"}])
    out = llm_gacc_page.generate_questions(
        {**FACTS, "numbers": {**FACTS["numbers"], "us_12mo": -0.297}},
        FakeBackend(raw))
    assert out is not None
    assert not _capture_rejections


def test_bare_group_hs_code_not_a_false_positive():
    """Regression (2026-07-16 live run): both EV+hybrid headline takes were
    rejected on '870360' — the group's own subheading (patterns
    870360%/870370%/870380%), written by the model without an HS/CN8 prefix
    for _HS_CODE_RE to catch. The group's own defining codes (and their HS4
    truncation) must strip, not read as an invented count — while a number
    that is NOT one of the group's codes still fails."""
    from llm_framing import verify_numbers, _group_code_strings
    facts = {"hs_patterns": ["870380%", "870370%", "870360%"],
             "scopes": {"yoy_import": {"yoy_pct": 0.386}}}
    assert _group_code_strings(facts) == {"8703", "870360", "870370", "870380"}
    # bare subheading + HS4 heading both pass, alongside a verified pct
    ok, failures = verify_numbers(
        "Is the +38.6% rise in imports under 870360 (within the 8703 heading) "
        "volume-driven?", facts)
    assert ok, failures
    # an invented count that isn't one of the group's codes still fails
    ok, failures = verify_numbers("Did all 55000 units ship in May?", facts)
    assert not ok and failures[0].kind == "count"
    # a group with no embedded codes yields no strip set (no over-reach)
    assert _group_code_strings({"scopes": {}}) == set()


def test_questions_reject_unverified_number(_capture_rejections):
    raw = _questions_json(
        [{"q": "Why did EU exports rise 44.4%?", "axis": "x",
          "answerable": "drawers"}])
    assert llm_gacc_page.generate_questions(FACTS, FakeBackend(raw)) is None
    assert "unverified" in _capture_rejections[0]["reason"]


# ---------------------------------------------------------------------------
# Reuse graft: the GACC slots' independent period gate
# ---------------------------------------------------------------------------

def _report_with_gacc(period=date(2026, 5, 1), synthesis=None, questions=None):
    gp = rm.GaccPage(data_period=period, tab_label=f"GACC-only ({period:%b %Y})",
                     synthesis=synthesis, questions=questions)
    return rm.Report(
        meta=rm.ReportMeta(data_period=date(2026, 4, 1), variant="eurostat",
                           snapshot_id="t",
                           generated_at=datetime(2026, 7, 5, 23, 0)),
        gacc_page=gp,
    )


_PRIOR_GACC = {
    "meta": {"data_period": "2026-03-01"},   # main track a month behind — irrelevant
    "gacc_page": {
        "data_period": "2026-05-01",
        "synthesis": {"summary": "Prior synthesis.", "citations": [42],
                      "hypotheses": []},
        "questions": {"slot_type": "general", "grounded_in": [42],
                      "status": "generated",
                      "questions": [{"q": "Prior question?", "axis": "x",
                                     "answerable": "drawers"}]},
    },
}


def test_gacc_graft_carries_slots_on_same_gacc_period():
    """The gate is the GACC page's own period — the main track's
    data_period differing (as it does mid-cycle) must not block it."""
    r = _report_with_gacc()
    n = portal_takes_reuse.graft_gacc_slots(r, _PRIOR_GACC)
    assert n == 2
    assert r.gacc_page.synthesis["summary"] == "Prior synthesis."
    assert r.gacc_page.questions.questions[0]["q"] == "Prior question?"


def test_gacc_graft_drops_on_new_gacc_month():
    r = _report_with_gacc(period=date(2026, 6, 1))
    assert portal_takes_reuse.graft_gacc_slots(r, _PRIOR_GACC) == 0
    assert r.gacc_page.synthesis is None


def test_gacc_graft_never_clobbers_fresh_slots():
    fresh = {"summary": "Fresh.", "citations": [], "hypotheses": []}
    r = _report_with_gacc(synthesis=fresh)
    n = portal_takes_reuse.graft_gacc_slots(r, _PRIOR_GACC)
    assert r.gacc_page.synthesis["summary"] == "Fresh."
    assert n == 1  # only the questions slot grafted


# ---------------------------------------------------------------------------
# Renderer: the machine corner
# ---------------------------------------------------------------------------

def _rendered(synthesis=None, questions=None):
    gp = rm.GaccPage(
        data_period=date(2026, 5, 1), tab_label="GACC-only (May 2026)",
        identity={"confirmation_due": "2026-07-16", "caveats": ["Caveat."]},
        synthesis=synthesis, questions=questions,
    )
    r = rm.Report(
        meta=rm.ReportMeta(data_period=date(2026, 4, 1), variant="eurostat",
                           snapshot_id="t",
                           generated_at=datetime(2026, 7, 5, 23, 0)),
        gacc_page=gp,
        source_vintages={"eurostat": date(2026, 4, 1)},
    )
    return render_html(r)


def test_synthesis_box_renders_with_hypotheses_and_steps():
    h = _rendered(synthesis={
        "summary": "The EU rise sits below the world average.",
        "citations": [42, 44],
        "hypotheses": [{"id": "us_tariff_diversion",
                        "label": "US-tariff diversion",
                        "rationale": "US down while EU up.",
                        "steps": ["Compare the US, EU and ASEAN lines"]}],
    })
    assert "The story of this release" in h
    assert "Machine synthesis" in h
    assert "US-tariff diversion." in h
    assert "How to corroborate" in h
    assert "finding/42" in h
    assert 'data-spy="gacc-synthesis"' in h  # subnav entry


def test_questions_box_renders_answerability_copy():
    h = _rendered(questions=rm.LLMSlot(
        slot_type="general", grounded_in=[42], status="generated",
        questions=[
            {"q": "Concentrated in a few routes?", "axis": "concentration",
             "answerable": "world_table"},
            {"q": "Will the mirror confirm it?", "axis": "mirror",
             "answerable": "eurostat_confirmation"},
        ]))
    assert "Questions this release raises" in h
    assert 'href="#gacc-world">China and the world</a>' in h
    assert "answerable when Eurostat confirms this month (due ~16 Jul 2026)" in h


def test_machine_corner_absent_when_slots_empty():
    h = _rendered()
    assert "The story of this release" not in h
    assert 'data-spy="gacc-synthesis"' not in h
