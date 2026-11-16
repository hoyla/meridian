"""The general ("one other thing of note") take must never headline a
low-base year-on-year percentage. The page's own rule is "when low base is
flagged, quote the € amount, not the %"; the Sep-2026 release (2026-11-16)
led with lithium hydroxide "+1348%" off a €5.7M base because the shortlist
ranked on |yoy| with no low-base gate.

Pure-function tests over a hand-built report shape — no DB, no model.
"""
from __future__ import annotations

import json
from types import SimpleNamespace as NS

import llm_general_take as gt


def _group(title, fid, yoy, *, low_base=False, share=None, share_fid=None,
           trajectory=None):
    metrics = {"china_share_value": share, "china_share_finding": share_fid}
    if trajectory:
        metrics["trajectory"] = {"EU-27": {"import": trajectory}}
    return NS(
        kind="sector_detail", title=title, intro="", facets=None, sections=[],
        metrics=metrics,
        findings=[NS(finding_id=fid, metrics={
            "yoy_pct": yoy, "flow": "import", "low_base": low_base})],
    )


def _report(*groups):
    return NS(headline=NS(items=[]), sections=list(groups))


class _Backend:
    def __init__(self, pick):
        self._pick = pick

    def generate(self, system, user):
        return json.dumps({"pick": self._pick})


def test_low_base_mover_is_not_shortlisted_as_a_big_mover():
    report = _report(
        _group("Lithium hydroxide", 1, 13.48, low_base=True, share=0.51, share_fid=11),
        _group("Finished cars", 2, 0.40),
    )
    shortlist = gt.build_general_shortlist(report)
    assert [c["subject"] for c in shortlist] == ["Finished cars"]


def test_low_base_percentage_is_withheld_when_group_is_shortlisted_otherwise():
    """A low-base group can still earn its place on China dependency — but
    then only the share is offered, never the percentage."""
    report = _report(
        _group("Gallium", 3, 9.0, low_base=True, share=0.93, share_fid=33),
    )
    (cand,) = gt.build_general_shortlist(report)
    assert cand["kinds"] == ["china_dependency"]
    assert cand["facts"] == {"china_import_share": 0.93}
    assert "YoY" not in gt.assemble_general_prompt([cand])[1]


def test_low_base_percentage_is_withheld_on_a_trajectory_flip():
    report = _report(
        _group("Cobalt", 4, 7.5, low_base=True, share=0.30, share_fid=44,
               trajectory="peak-and-fall (2023)"),
    )
    (cand,) = gt.build_general_shortlist(report)
    assert cand["kinds"] == ["trajectory_flip"]
    assert "yoy_pct" not in cand["facts"]


def test_a_take_citing_the_withheld_low_base_percentage_is_rejected():
    """Belt and braces: even if the model produced the percentage from
    somewhere, it is not in the facts, so the number verifier refuses it."""
    report = _report(
        _group("Gallium", 3, 9.0, low_base=True, share=0.93, share_fid=33),
    )
    take = ("EU-27 imports of gallium from China are up 900% year on year, "
            "with China at 93% of the import share. Is this stockpiling?")
    out = gt.generate_general_take(
        report, backend=_Backend({"finding_id": 33, "take": take}))
    assert out is None


def test_a_normal_base_mover_still_gets_its_percentage():
    report = _report(_group("Finished cars", 2, 0.40, share=0.20, share_fid=22))
    (cand,) = gt.build_general_shortlist(report)
    assert cand["kinds"] == ["big_mover"]
    assert cand["facts"]["yoy_pct"] == 0.40
    take = ("EU-27 imports of finished cars from China rose 40% year on year, "
            "with China at 20% of the import share. Is this price or volume?")
    out = gt.generate_general_take(
        report, backend=_Backend({"finding_id": 2, "take": take}))
    assert out is not None and out["finding_id"] == 2
