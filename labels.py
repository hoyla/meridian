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
            "Lithium hydroxide (battery-grade)",
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
        ],
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
