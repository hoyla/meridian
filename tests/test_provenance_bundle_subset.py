"""Unit tests for the export-bundle's "editorially-fresh subset" filter.

The bundling-into-export flow used to copy every finding/N token cited
anywhere in the brief — ~2,000 files per export, most of which repeat
across exports because the long tail of state-of-play findings doesn't
move between cycles. The filter narrows this to Tier 1 changes +
Top-N movers + Top-N leads (typically ~40-60 files), with everything
else served on-demand via the --finding-provenance CLI flag.

These tests pin the regex boundaries the filter uses — if a section
heading changes shape in `briefing_pack/sections/{diff,top_movers,llm_narratives}.py`,
the filter will silently start dropping a section, and these will
break first.
"""

from briefing_pack.render import _editorially_fresh_finding_ids


BRIEF_SAMPLE = """\
# Findings

intro paragraph mentioning `finding/1` which should NOT be bundled.

## Top 3 movers this cycle

*Editorially-quotable shifts ranked by ...*

1. **[EV batteries](#ev-batteries)** — +34% to €27B. `finding/100`
2. **[Solar PV](#solar-pv)** — +18% to €8B. `finding/101`
3. **[Permanent magnets](#permanent-magnets)** — +12% to €1.5B. `finding/102`

---

## Tier 1 — What's new this cycle

- New: `finding/200`
- Revised: `finding/201`
- Direction flip: `finding/202`

---

## Tier 2 — Current state of play

This section cites many findings — `finding/300`, `finding/301`, etc. —
that should NOT be bundled because they're the long tail of
state-of-play, not the editorially-fresh subset.

### EV batteries

Some detail block citing `finding/400`.

## Tier 3 — Full detail by HS group

More long-tail findings: `finding/500`, `finding/501`.

## Methodology footer

(no findings here)
"""


LEADS_SAMPLE = """\
# Leads

## In this export folder

Sees `finding/999` should NOT be bundled — wrong section.

## Top 5 leads to investigate

1. **EV batteries** — `finding/100` (rationale: ...)
2. **Solar PV** — `finding/101`
3. **Permanent magnets** — `finding/102`
4. **Wind turbines** — `finding/103`
5. **Lithium chemicals** — `finding/104`

## Full lead detail by HS group

Long tail again: `finding/600`, `finding/601`, `finding/602`.
"""


def test_extracts_top_movers_findings():
    ids = _editorially_fresh_finding_ids(BRIEF_SAMPLE, leads_text="")
    assert 100 in ids and 101 in ids and 102 in ids


def test_extracts_tier_1_findings():
    ids = _editorially_fresh_finding_ids(BRIEF_SAMPLE, leads_text="")
    assert {200, 201, 202}.issubset(ids)


def test_extracts_top_leads_findings():
    ids = _editorially_fresh_finding_ids(brief_text="", leads_text=LEADS_SAMPLE)
    assert {100, 101, 102, 103, 104} == ids


def test_excludes_long_tail_state_of_play():
    """Findings in Tier 2 / Tier 3 / unrelated leads.md sections must
    NOT be in the fresh subset. They're served on-demand instead."""
    ids = _editorially_fresh_finding_ids(BRIEF_SAMPLE, LEADS_SAMPLE)
    assert 300 not in ids   # Tier 2 prose
    assert 301 not in ids   # Tier 2 prose
    assert 400 not in ids   # under a Tier 2 ### sub-heading
    assert 500 not in ids   # Tier 3
    assert 501 not in ids   # Tier 3
    assert 600 not in ids   # leads.md "Full lead detail" section
    assert 601 not in ids
    assert 999 not in ids   # leads.md "In this export folder"
    assert 1 not in ids     # brief intro paragraph


def test_empty_inputs_return_empty_set():
    assert _editorially_fresh_finding_ids("", "") == set()


def test_method_bump_suppressed_tier_1_contributes_nothing():
    """When Tier 1 is suppressed (the cycle is a method-version bump
    with no real editorial movement), the section renders but contains
    no finding/N tokens — fine, that just means zero contribution."""
    method_bump_brief = """\
## Top 5 movers this cycle

(no movers — empty cycle)

---

## Tier 1 — What's new this cycle

*This cycle is a method-version bump (v10 → v11), not editorial
movement. The full state-of-play below reflects the same numbers as
last cycle with the new method tag.*

---

## Tier 2 — Current state of play
"""
    ids = _editorially_fresh_finding_ids(method_bump_brief, leads_text="")
    assert ids == set()
