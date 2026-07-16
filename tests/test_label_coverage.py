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


def test_semiconductors_theme_membership():
    """The Semiconductors lens (2026-07-16, Luke-approved): fab equipment +
    the two-way chip trade + wafer feedstock + the Ga/Ge inputs. Locks two
    editorial decisions: (1) the 8541 group is the excl-solar-PV cut — PV
    cells stay with the Solar theme, so 'Solar PV cells & modules' must NOT
    appear here; (2) polysilicon stays Solar-only (280461 can't distinguish
    solar- from semiconductor-grade, and the tonnage is overwhelmingly
    solar)."""
    by_name = {l.name: l for l in labels.SEED_LABELS}
    assert "Semiconductors" in by_name
    lab = by_name["Semiconductors"]
    assert lab.kind == "narrative"
    assert set(lab.member_groups) == {
        "Semiconductor manufacturing equipment",
        "Integrated circuits (HS 8542)",
        "Semiconductor devices excl. solar PV (HS 8541)",
        "Doped wafers (HS 3818)",
        "Gallium, germanium & other minor metals (HS 8112)",
    }
    assert "Solar PV cells & modules" not in lab.member_groups
    assert "Polysilicon (solar PV upstream — Xinjiang exposure)" \
        not in lab.member_groups
    # The GPU disclaimer is load-bearing copy: HS can't isolate GPUs and the
    # definition must keep saying so (defensibility — a reader will ask).
    assert "GPU" in lab.definition
    # 8486 gains its first badge; it was theme-less before this lens existed.
    assert labels.themes_for_group("Semiconductor manufacturing equipment") \
        == ["Semiconductors"]


def test_micromobility_theme_membership():
    """The Micromobility lens (2026-07-16, Luke: 'three groups, connected by
    micromobility theme'). Locks the editorial design: (1) the two 8711 groups
    mirror the CN8 split because the 2019 anti-dumping boundary runs exactly
    along it — merging them would average the duty cliff away; (2) pedal
    bicycles are IN (trade-policy baseline; duties since 1993) even though not
    electric; (3) none of the three joins 'EV supply chain' or 'Automotive' —
    micromobility is its own story, not a car-trade or battery-chain
    sub-plot."""
    by_name = {l.name: l for l in labels.SEED_LABELS}
    assert "Micromobility" in by_name
    lab = by_name["Micromobility"]
    assert lab.kind == "narrative"
    assert set(lab.member_groups) == {
        "Electric bicycles, pedal-assist (CN8 87116010)",
        "Electric motorcycles, scooters & mopeds (CN8 87116090)",
        "Bicycles, non-motorised (HS 8712)",
    }
    for g in lab.member_groups:
        assert labels.themes_for_group(g) == ["Micromobility"], g
    # The can't-split-scooters-from-motorbikes caveat is load-bearing copy.
    assert "cannot be separated" in lab.definition


def test_penicillin_child_group_theming_and_caveat():
    """The amoxicillin-proxy child (CN8 29411000, inside the Antibiotics
    parent — Soapbox amoxicillin piece, 2026-07-16). Same theme as the parent,
    Pharma only; and the seed description must keep the categorisation
    disclaimer — amoxicillin has no EU customs line, so the code can never be
    presented as an amoxicillin-only number (load-bearing copy, like the
    Semiconductors GPU caveat)."""
    assert labels.themes_for_group("Penicillin-family APIs (CN8 29411000)") \
        == ["Pharma & fine chemicals"]
    assert _seeded_patterns("Penicillin-family APIs (CN8 29411000)") \
        == ["29411000%"]
    import pathlib
    sql = (pathlib.Path(__file__).parent.parent / "schema.sql").read_text()
    seed_start = sql.index("('Penicillin-family APIs (CN8 29411000)'")
    seed = sql[seed_start:seed_start + 2000]
    assert "amoxicillin has no EU customs line" in seed


def test_micromobility_8711_seeds_partition_the_electric_code():
    """The two 8711 seeds must stay an exact partition of CN8 871160 — one
    group per side of the anti-dumping boundary, no overlap, no broad pattern
    (871160% or 8711%) that would re-merge them. Reads the schema.sql seeds."""
    ebike = _seeded_patterns("Electric bicycles, pedal-assist (CN8 87116010)")
    scoot = _seeded_patterns(
        "Electric motorcycles, scooters & mopeds (CN8 87116090)")
    assert ebike == ["87116010%"]
    assert scoot == ["87116090%"]


def _seeded_patterns(group_name: str) -> list[str]:
    """The hs_patterns ARRAY seeded for `group_name`, read from schema.sql —
    the canonical in-repo artifact (the migration carries an identical copy).
    Regex is anchored to the quoted group name then the next ARRAY[...]."""
    import pathlib
    import re
    sql = (pathlib.Path(__file__).parent.parent / "schema.sql").read_text()
    m = re.search(
        re.escape(f"('{group_name}',") + r".*?ARRAY\[(.*?)\]", sql, re.S)
    assert m, f"group {group_name!r} not seeded in schema.sql"
    return re.findall(r"'([^']+)'", m.group(1))


def test_semiconductor_8541_seed_excludes_all_pv_codes():
    """The excl-PV 8541 pattern set must never include the PV codes — neither
    the current 854142/854143 nor the pre-HS2022 mixed bucket 854140, which
    bundled PV cells with LEDs/photosensitive devices (2017–2021 in our data;
    including it would contaminate the pre-2022 series with ~EUR 25bn of
    solar panels). Reads the actual schema.sql seed so pattern edits are
    caught, not a re-declared copy."""
    patterns = _seeded_patterns("Semiconductor devices excl. solar PV (HS 8541)")
    assert patterns, "empty pattern seed"
    prefixes = {p.rstrip("%") for p in patterns}
    banned = {"854140", "854142", "854143"}
    assert not banned & prefixes
    # No pattern broad enough to sweep a banned code back in (e.g. '8541%'
    # or '85414%' would).
    assert all(
        not b.startswith(p) for b in banned for p in prefixes
    )
    # And every pattern stays inside heading 8541.
    assert all(p.startswith("8541") for p in prefixes)


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
