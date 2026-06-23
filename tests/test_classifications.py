"""Classification lookups must come from the committed CSVs, and a missing
lookup must fail loud — never silently collapse every group into SITC section
9 ("Other / unclassified").

Regression guard for the 2026-06-23 incident: the raw UNSD workbooks were kept
outside the repo, a folder move emptied the live lookup, and a published
briefing bucketed all 60 groups into one "Other / unclassified" section. The
fix moved the publication source of truth onto the committed derived CSVs and
added `assert_classifications_available()`. These tests are deliberately
DB-free so they run in any checkout.
"""

import pytest

import classifications
from report_builder import _primary_section


def test_committed_lookups_load_nonempty():
    # The derived CSVs are committed, so these are populated in any checkout —
    # no dependency on a machine-local reference folder.
    assert classifications._hs6_division(), "cn8_sitc.csv should populate the division map"
    assert classifications._hs6_enduse(), "cn8_bec.csv should populate the end-use map"


def test_sitc_division_resolves_from_committed_csv():
    # Li-ion batteries (HS 850760) → SITC division 77, electrical machinery.
    assert "77" in classifications.sitc_divisions_for_patterns(["850760%"])
    assert classifications.sitc_divisions_for_patterns(["850760"]) == ["77"]


def test_enduse_resolves_from_committed_csv():
    # The end-use facet was the second silent casualty of the same bug.
    assert classifications.enduse_for_patterns(["850760%"]), "batteries should map to an end-use"


def test_group_is_not_collapsed_to_other_unclassified():
    # The exact symptom: a mappable group must land in its real section, not 9.
    sec = _primary_section(classifications.sitc_divisions_for_patterns(["850760%"]))
    assert sec == ("7", "Machinery & transport")
    assert sec[0] != "9"


def test_assert_classifications_available_passes_with_committed_csvs():
    classifications.assert_classifications_available()  # must not raise


def test_assert_classifications_available_fails_loud_when_empty(monkeypatch):
    # Simulate a missing/moved lookup: the preflight must raise, not degrade.
    monkeypatch.setattr(classifications, "_HS6_DIV", {})
    monkeypatch.setattr(classifications, "_HS6_ENDUSE", {})
    with pytest.raises(RuntimeError, match="Other / unclassified"):
        classifications.assert_classifications_available()
