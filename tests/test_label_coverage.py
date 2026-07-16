"""Label ↔ hs_groups reconciliation (the theme layer's coverage).

The editorial themes live in code (labels.py) but name groups that live in the
DB (journalist-editable, added via migrations). Three drift risks follow, and
the pure helpers in labels.py detect each given a group set:

- **Dead references** — a `member_groups` entry with no hs_groups row, so the
  label silently expands to fewer codes than it claims. This is how
  "Oil & gas: origin watch" ended up pointing at 2710/2711 groups on a DB that
  never applied the migration seeding them.
- **Unthemed groups** — live groups carrying no theme. Informational, not an
  error: broad catch-alls are theme-less by design.
- **Subset pattern collisions** — one group's codes wholly inside another's.
  Parent/child is normal; a same-level pair (the two lithium groups that both
  claimed 282520) is usually accidental duplication.

The pure tests below need no DB. The live guard reconciles against
`GACC_LIVE_DATABASE_URL` and is skipped when unset — same convention as
tests/test_orphan_findings.py. See dev_notes/2026-07-16-label-coverage-audit.md.
"""

import os

import psycopg2
import pytest

import labels


# --- pure helpers (no DB) ---------------------------------------------------

def test_conventional_hybrids_now_in_automotive():
    """The non-plug-in HEV body type sits in HS 8703 beside the other car
    groups; it was the one clear omission the coverage audit surfaced. It
    belongs to Automotive ONLY — a mild/full hybrid has no EV battery/charging
    supply chain, so it is deliberately NOT in 'EV supply chain'."""
    assert labels.themes_for_group("Conventional hybrids (HEV, non-plug-in)") == [
        "Automotive"]


def test_dead_member_refs_flags_missing_group():
    known = {"Finished cars (broad)"}  # deliberately tiny
    dead = labels.dead_member_refs(known)
    # Every member of every label that isn't "Finished cars (broad)" is dead.
    assert dead == sorted(dead)                    # stable ordering
    assert ("Automotive", "Motor-vehicle parts") in dead
    # A member that DOES exist is never reported.
    assert all(missing != "Finished cars (broad)" for _, missing in dead)


def test_dead_member_refs_empty_when_all_present():
    all_members = {g for lab in labels.SEED_LABELS for g in lab.member_groups}
    assert labels.dead_member_refs(all_members) == []


def test_unthemed_groups_reports_only_the_themeless():
    known = {"Finished cars (broad)",          # themed (Automotive)
             "Steel (broad)"}                  # not in any label
    assert labels.unthemed_groups(known) == ["Steel (broad)"]


def test_subset_pattern_collisions_catches_duplicate_not_disjoint():
    patterns = {
        # the lithium anomaly: 282520-only is a strict subset of carbonate+hydroxide
        "Lithium hydroxide (battery-grade)": ["282520%"],
        "Lithium chemicals (carbonate + hydroxide)": ["283691%", "282520%"],
        # a legitimate parent/child
        "Rare-earth materials": ["284690%"],
        "Lanthanum compounds (CN8 28469040)": ["28469040%"],
        # disjoint — never a collision
        "Honey": ["0409%"],
    }
    collisions = labels.subset_pattern_collisions(patterns)
    assert ("Lithium hydroxide (battery-grade)",
            "Lithium chemicals (carbonate + hydroxide)") in collisions
    assert ("Lanthanum compounds (CN8 28469040)",
            "Rare-earth materials") in collisions
    assert all("Honey" not in pair for pair in collisions)


def test_subset_collisions_reports_equal_sets_once():
    # Two groups with identical code-sets: reported a single time, name-ordered.
    collisions = labels.subset_pattern_collisions({
        "B group": ["9999%"],
        "A group": ["9999%"],
    })
    assert collisions == [("A group", "B group")]


# --- live DB guard (skipped without GACC_LIVE_DATABASE_URL) ------------------

LIVE_DB_ENV = "GACC_LIVE_DATABASE_URL"


@pytest.fixture(scope="module")
def live_group_names() -> set[str]:
    url = os.environ.get(LIVE_DB_ENV)
    if not url:
        pytest.skip(f"{LIVE_DB_ENV} not set; skipping live label-coverage check")
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM hs_groups")
            return {r[0] for r in cur.fetchall()}
    finally:
        conn.close()


def test_no_dead_label_refs_against_live_db(live_group_names):
    """Every `member_groups` entry in every seed label must correspond to a
    live hs_groups row. A failure means a theme claims codes it doesn't cover —
    fix by creating the group (or applying the pending migration that seeds it)
    or by correcting the member name in labels.py."""
    dead = labels.dead_member_refs(live_group_names)
    assert not dead, (
        "Labels reference groups absent from the live DB "
        "(theme under-expands): "
        + "; ".join(f"{lab} -> {missing}" for lab, missing in dead)
    )
