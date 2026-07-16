"""The editorial THEME layer — many-to-many labels over the trade data.

The taxonomy direction (see dev_notes/2026-06-20-taxonomy-sitc-spine-and-labels.md)
splits two jobs the editorial `hs_groups` used to conflate:

- **Structural spine** = SITC division (a partition; `classifications.py`).
- **Editorial themes = labels** (this module): named, drillable, *overlapping*
  code-sets that may span any number of SITC divisions. They are additive, not
  exclusive — a code can carry several — which is what makes them less
  prescriptive than a single story-partition, and they isolate the editorial
  judgment so it stays explicit and auditable.

Two kinds of label exist:

1. **Commodity labels** = the existing `hs_groups` themselves (each group IS a
   label; it's already the heading in the portal). Not re-declared here.
2. **Cross-cutting labels** (below) = the value-add: themes that span several
   groups/codes — "EV supply chain", "Xinjiang exposure". These are seeded
   illustratively and are **journalist-editable**: a label is just a name + a
   definition + the groups/codes it unions. Composing them from *named, vetted
   groups* (rather than hand-typed codes) keeps them transparent and defensible.

INVARIANT: labels OVERLAP, so a per-label value rollup is fine but labels must
NEVER be summed to a "total" — that double-counts.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field


@dataclass
class Label:
    name: str
    definition: str          # the auditable editorial rationale
    kind: str                # "narrative" | "origin_risk" | "commodity"
    member_groups: list[str] = field(default_factory=list)  # hs_group names it unions
    extra_patterns: list[str] = field(default_factory=list)  # codes beyond the groups
    created_by: str = "seed"


# Cross-cutting seed labels. Composed from vetted group names so the membership
# is transparent; edit freely (add a label = add an entry; add a member = add a
# name). Definitions are the editorial rationale a reader can audit.
SEED_LABELS: list[Label] = [
    Label(
        "EV supply chain",
        "Battery cells, EV/hybrid vehicles, traction magnets and key battery "
        "inputs (lithium, graphite).",
        "narrative",
        member_groups=[
            "EV batteries (Li-ion)",
            "EV + hybrid passenger cars",
            "Permanent magnets",
            "Sintered NdFeB magnets (CN8 85051110)",
            "Lithium chemicals (carbonate + hydroxide)",
            "Cobalt (oxides, hydroxides & unwrought)",
            "Manganese oxides",
            "Natural graphite (HS 250410)",
        ],
    ),
    Label(
        "Solar supply chain",
        "Polysilicon through to PV cells/modules and the inverters that pair "
        "with them.",
        "narrative",
        member_groups=[
            "Solar PV cells & modules",
            "Polysilicon (solar PV upstream — Xinjiang exposure)",
            "Solar/grid inverters (broad)",
            "MPPT solar inverters (CN8 85044084)",
            "Photovoltaic inverters (CN8 85044086)",
        ],
    ),
    Label(
        "Wind power",
        "Wind-generation supply chain — the finished-turbine flow (wind-powered "
        "generating sets) plus the NdFeB magnets critical to direct-drive "
        "turbine generators. A lens, not a clean category: the magnet codes "
        "overlap with EV and electronics, and wind also draws on generic "
        "structural steel and electrical machinery that aren't wind-specific "
        "and so aren't claimed here. Replaces the retired 'Wind turbine "
        "components' group, whose patterns (generator parts 850300, steel "
        "towers 730820) weren't wind-specific.",
        "narrative",
        member_groups=[
            "Wind generating sets only",
            "Sintered NdFeB magnets (CN8 85051110)",
        ],
    ),
    Label(
        "Xinjiang exposure",
        "Commodities with documented Xinjiang production concentration — an "
        "origin-risk lens, not a commodity category.",
        "origin_risk",
        member_groups=[
            "Polysilicon (solar PV upstream — Xinjiang exposure)",
            "Cotton (raw + woven fabrics)",
            "Tomato paste / preserved tomatoes",
        ],
    ),
    Label(
        "Rare earths & magnets",
        "Rare-earth compounds and the permanent magnets they feed.",
        "narrative",
        member_groups=[
            "Rare-earth materials",
            "Permanent magnets",
            "Sintered NdFeB magnets (CN8 85051110)",
            "Praseodymium/neodymium/samarium compounds (CN8 28469050)",
            "Gadolinium/terbium/dysprosium compounds (CN8 28469060)",
            "Lanthanum compounds (CN8 28469040)",
            "Europium/holmium/erbium/thulium/ytterbium/lutetium/yttrium compounds (CN8 28469070)",
        ],
    ),
    Label(
        "China export-control regime",
        "Materials China has placed under export licensing — gallium/germanium/"
        "antimony et al., graphite, rare earths and the magnets downstream.",
        "narrative",
        member_groups=[
            "Critical minerals (export-controlled by China)",
            "Rare-earth materials",
            "Natural graphite (HS 250410)",
            "Permanent magnets",
            "Sintered NdFeB magnets (CN8 85051110)",
            "Lithium chemicals (carbonate + hydroxide)",
            "Cobalt (oxides, hydroxides & unwrought)",
            "Tungsten (HS 8101)",
            "Gallium, germanium & other minor metals (HS 8112)",
            "Antimony (HS 8110)",
        ],
    ),
    Label(
        "Semiconductors",
        "The chip supply chain: EU-made fab equipment flowing east (the ASML "
        "story — ~50:1 export/import ratio on HS 8486) and the two-way chip "
        "trade, where Europe is a net EXPORTER to China at heading level "
        "(automotive/industrial silicon out, legacy chips in). Includes the "
        "wafer feedstock stage and the gallium/germanium inputs China has "
        "placed under export licence. A lens with a stated limit: HS/CN8 "
        "cannot isolate GPUs or AI accelerators — bare chips sit inside "
        "854231 mixed with CPUs and MCUs, assembled cards inside 8473 "
        "computer parts — so this theme carries the supply chain, never a "
        "'GPU imports' number.",
        "narrative",
        member_groups=[
            "Semiconductor manufacturing equipment",
            "Integrated circuits (HS 8542)",
            "Semiconductor devices excl. solar PV (HS 8541)",
            "Doped wafers (HS 3818)",
            "Gallium, germanium & other minor metals (HS 8112)",
        ],
        created_by="seed:semiconductors_2026_07",
    ),
    Label(
        "Automotive",
        "Finished vehicles, EV/hybrid cars and the parts that feed them — the "
        "China–Europe car-trade story.",
        "narrative",
        member_groups=[
            "Finished cars (broad)",
            "EV + hybrid passenger cars",
            "Conventional hybrids (HEV, non-plug-in)",
            "Motor-vehicle parts",
            "Engine parts (CN8 84099100 + 84099900)",
            "Internal-combustion engines (HS 8407 + 8408)",
        ],
    ),
    Label(
        "Micromobility",
        "Two-wheeler trade with China — electric and pedal. Two live stories: "
        "the 2019 anti-dumping cliff (EU duties of up to ~79% on Chinese "
        "pedal-assist e-bikes cut that flow ~90% in a year; conventional "
        "bicycles have carried EU anti-dumping duties since 1993, the bloc's "
        "longest-running measure), and the ~EUR 1bn/yr e-scooter/moped boom "
        "in the code the duties don't cover. Includes non-motorised bicycles "
        "as the substitution/trade-policy baseline rather than for the "
        "'electric' angle. Code-split caveat: CN8 87116090 bundles e-scooters, "
        "e-mopeds and e-motorcycles in one code — they cannot be separated.",
        "narrative",
        member_groups=[
            "Electric bicycles, pedal-assist (CN8 87116010)",
            "Electric motorcycles, scooters & mopeds (CN8 87116090)",
            "Bicycles, non-motorised (HS 8712)",
        ],
        created_by="seed:micromobility_2026_07",
    ),
    Label(
        "Food & agriculture",
        "Agri-food trade with China — meat, produce, sweeteners and animal-feed "
        "inputs, often the subject of dumping or food-safety disputes. Includes "
        "feed-grade fine chemicals (the lysine/methionine/threonine amino acids, "
        "choline) that ALSO appear under 'Pharma & fine chemicals' — a worked "
        "example of the many-to-many design: the group stays material-named and "
        "carries every application it genuinely serves, rather than being bound "
        "to one.",
        "narrative",
        member_groups=[
            "Honey",
            "Pork (HS 0203)",
            "Pork offal (HS 0206 swine)",
            "Tomato paste / preserved tomatoes",
            "Feed premixes (HS 230990)",
            "Amino acids (HS 2922)",
            "Choline (HS 292310)",
        ],
    ),
    Label(
        "Pharma & fine chemicals",
        "Active-ingredient and precursor chemistry — antibiotics, analgesic "
        "precursors and other fine/speciality organics where Europe leans on "
        "Chinese supply.",
        "narrative",
        member_groups=[
            "Antibiotics (HS 2941)",
            "Ibuprofen-class monocarboxylic acids (HS 2916)",
            "Paracetamol-class amides (HS 2924)",
            "Vitamins & provitamins (HS 2936)",
            "Amino acids (HS 2922)",
            "Choline (HS 292310)",
            "Aldehyde/ketone acids (HS 2918)",
            "Vanillin and ethylvanillin (HS 29124100 + 29124200)",
        ],
    ),
    Label(
        "Cosmetics & personal care",
        "Cosmetics, perfumery and personal-care inputs and finished goods — "
        "fragrance bases and the beauty/skin-care products built from them, "
        "plus the titanium-dioxide pigment used as a filler/whitener.",
        "narrative",
        member_groups=[
            "Essential oils & fragrance mixtures (HS 3301 + 3302)",
            "Beauty, make-up & skin-care preparations (HS 3304)",
            "Titanium dioxide (CN8 320611)",
        ],
    ),
    Label(
        "Paint & coatings",
        "Paints, varnishes and the pigments that go into them — finished "
        "coatings plus titanium dioxide, the dominant white pigment.",
        "narrative",
        member_groups=[
            "Paints & varnishes (HS 3208-3210)",
            "Titanium dioxide (CN8 320611)",
        ],
    ),
    Label(
        "Oil & gas: origin watch",
        "Refined products and gas as a re-export / origin-laundering lens "
        "rather than a commodity category: the EU/UK refine and re-ship more "
        "than they extract, so member-state EXPORTS to China here can flag "
        "product made elsewhere rather than domestic output. The live signal is "
        "EU refined-petroleum (HS 2710) exports to China — order ~EUR 1bn/12mo, "
        "led by Greece and Hungary (Hungary a new post-2023 surge worth a look). "
        "The trade-flow footprint of the 'shadow fleet' story — the cargo side, "
        "not the vessels. Crude (HS 2709) was dropped: zero EU-China trade. "
        "Caveat: the counterparty is always China (+HK/MO), so this catches "
        "reselling to China only, and shows the export leg, not the origin.",
        "origin_risk",
        member_groups=[
            "Refined petroleum products (HS 2710)",
            "Natural gas & other petroleum gases (HS 2711)",
        ],
        created_by="seed:reporter_request_2026_06",
    ),
]


def themes_for_group(group_name: str) -> list[str]:
    """The cross-cutting label(s) an editorial group belongs to (its `theme`
    facet). Direct membership — exact and transparent, no fuzzy matching."""
    return [lab.name for lab in SEED_LABELS if group_name in lab.member_groups]


def label_patterns(label: Label, patterns_by_group: dict[str, list[str]]) -> list[str]:
    """The HS code-set a label expands to: the union of its member groups'
    patterns plus any extra patterns. (For value rollups / drill-down.)"""
    pats = list(label.extra_patterns)
    for g in label.member_groups:
        pats += patterns_by_group.get(g, [])
    return sorted(set(pats))


# --- Coverage reconciliation ------------------------------------------------
# labels.py is code; the groups it names live in the DB (journalist-editable,
# added via migrations). So these three drift risks can only be checked against
# an actual group set, not against the module alone. The helpers below are pure
# — the caller supplies the live facts (names / patterns) — so both the
# --audit-labels CLI and the DB-backed test in tests/test_label_coverage.py can
# share one implementation. See dev_notes/2026-07-16-label-coverage-audit.md.


def dead_member_refs(known_group_names: Iterable[str]) -> list[tuple[str, str]]:
    """(label_name, missing_group_name) for every `member_groups` entry that
    has no matching hs_groups row. A non-empty result means the label silently
    expands to fewer codes than it claims — the class of drift that left
    "Oil & gas: origin watch" pointing at 2710/2711 groups a DB never got.
    Sorted for stable diagnostics/test output."""
    known = set(known_group_names)
    out = [
        (lab.name, g)
        for lab in SEED_LABELS
        for g in lab.member_groups
        if g not in known
    ]
    return sorted(out)


def unthemed_groups(known_group_names: Iterable[str]) -> list[str]:
    """Live group names carrying no cross-cutting theme. INFORMATIONAL, not an
    error: the broad catch-alls ("Steel (broad)", "Electrical equipment &
    machinery (chapters 84-85, broad)") are deliberately theme-less, as are
    groups no existing lens fits. Surfaced so a genuine omission (a car body
    type missing from Automotive) is visible rather than silent."""
    return sorted(g for g in set(known_group_names) if not themes_for_group(g))


def _prefixes(patterns: Iterable[str]) -> frozenset[str]:
    return frozenset(p.rstrip("%") for p in patterns)


def _covers(broad: frozenset[str], narrow: frozenset[str]) -> bool:
    """True if every prefix in `narrow` is at or below some prefix in `broad`
    (i.e. narrow's code-set ⊆ broad's)."""
    return all(any(n.startswith(b) for b in broad) for n in narrow)


def subset_pattern_collisions(
    patterns_by_group: Mapping[str, Iterable[str]],
) -> list[tuple[str, str]]:
    """(subset_group, superset_group) pairs where the first's HS code-set is
    wholly contained in the second's. Most are legitimate — a curated CN8 leaf
    inside its broad parent (e.g. the rare-earth CN8 compounds inside
    "Rare-earth materials"). But a same-intent pair like the two lithium groups
    that both claimed 282520 is usually accidental duplication worth
    reconciling. INFORMATIONAL: annotate with themes at the call site so
    parent/child (shared theme) reads differently from a true duplicate."""
    prefs = {name: _prefixes(pats) for name, pats in patterns_by_group.items()}
    out: list[tuple[str, str]] = []
    for a, pa in prefs.items():
        for b, pb in prefs.items():
            if a == b or not pa or not pb:
                continue
            # a ⊆ b, and not the trivial equal-set double-count (report each
            # unordered equal pair once, keyed by name order).
            if _covers(pb, pa) and (pa != pb or a < b):
                out.append((a, b))
    return sorted(out)
