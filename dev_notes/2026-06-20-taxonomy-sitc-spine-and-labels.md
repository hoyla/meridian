# Taxonomy: a UN structural spine + an editorial label layer

**Date:** 2026-06-20  **Status:** Design agreed in principle, lookup built,
not yet wired into the schema/pipeline.

## The problem this solves

The editorial `hs_groups` (EV batteries, solar, critical minerals…) currently
double as both the *definition* of a commodity unit and the *primary
categorisation*. As the tool reaches more reporters with different beats, a
single story-driven partition is too **prescriptive** — and the data shows why:

- The dataset is the full China–EU nomenclature: **~10,100 real CN8 codes**
  across 98 HS chapters.
- The 48 editorial groups cover **25% of codes / 57% of value**. So **75% of the
  product space (43% of value) is ingested but in no group — invisible.**
- One group, "Electrical equipment & machinery (84-85, broad)", spans **10 SITC
  divisions** on its own — proof it's an editorial *lens*, not a structural unit.

## The model: two layers, different jobs

**1. Structural spine = SITC division (a partition).**
Every CN8 code maps to exactly one SITC Rev 4 division via `reference/cn8_sitc.csv`
(built by `classifications.py` from the UNSD correspondence; HS2022→SITC4 with
HS2017 fallback). 65 divisions are populated; they're authoritative, defensible,
reporter-legible ("Road vehicles", "Iron & steel", "Pharmaceuticals"), and —
critically — they **partition** the data: every code sits in one division, so
**SITC-division aggregates DO sum to the total**. This is the safe place for
totals and the default browse structure.

**2. Editorial layer = themes as many-to-many LABELS (an overlay).**
The current `hs_groups` become **labels**: named, drillable code-sets that may
overlap and may span any number of divisions. New cross-cutting ones join them
(`EV supply chain`, `Xinjiang exposure`). Labels are *additive*, not exclusive —
that's what makes them feel less prescriptive — and the editorial judgment is
isolated, explicit, and auditable (you can see "this €X is tagged EV-supply-chain
because of these 7 codes"). **Labels must NEVER be summed to a total** (they
overlap — a code can carry several).

The two layers map onto the existing `Facets` (in `report_model.py`):

```python
@dataclass
class Facets:
    sector: list[str]    # SITC division code(s) — the STRUCTURAL spine
                         #   (1 per code; an editorial label spans the union)
    theme: list[str]     # editorial LABEL names — the many-to-many overlay
    partner: list[str]   # origin/partner (e.g. for Xinjiang-style origin facets)
    commodity: list[str] # free commodity keywords (search aid)
```

## The label registry (new — journalist-editable)

```python
@dataclass
class Label:
    name: str            # "EV supply chain", "Critical minerals", "Xinjiang exposure"
    definition: str      # one-line editorial definition — the auditable rationale
    hs_patterns: list[str]  # the code-set it expands to (same wildcard syntax as hs_groups)
    kind: str            # "commodity" | "narrative" | "origin_risk"
    created_by: str      # who applied the judgment (provenance)
```

- **`kind` captures that labels are heterogeneous.** `solar` is a *commodity*
  label (codes). `Xinjiang exposure` is an *origin_risk* label — it cuts across
  tomato paste, polysilicon and cotton, things with nothing commodity-wise in
  common; it could never be a clean taxonomic category but is a natural label.
  The fact that the most journalistically valuable cut only works as a label is
  the strongest vote for this design.
- **Migration:** today's 48 `hs_groups` → `Label(kind="commodity")` unchanged
  (same `hs_patterns`). Nothing is lost; the partition assumption is just
  relaxed (sets may now overlap).
- **Editable by reporters** (per the journalist-editable-groups principle): add a
  label = add a row + its code-set. No re-architecting, no re-analysis.

## Aggregation rules (the one thing to get right)

- **SITC division** is a partition → division totals sum to the grand total. Safe.
- **Labels** overlap → a per-label rollup is `sum(value over the label's codes)`,
  but **labels never sum to a total** (double-counts). Any UI that lists labels
  with values must not show a "total of labels" row.
- A code's `sector` is its single SITC division; a *label's* `sector` set is the
  **union** of its codes' divisions (so "Electrical equipment, broad" legitimately
  carries 10 sector tags).

## How it renders (reuses what's built)

- **Structural browse** = the SITC-division tree (default "where in the trade map"
  navigation; the spine that makes all 10,100 codes reachable).
- **Theme filter** = the label layer. The sector-detail **filter box already built**
  becomes a label filter — typing matches label names/definitions, not just group
  titles. A reporter's beat is one saved label filter.
- The `Finding`/`Section` nodes carry `facets.sector` + `facets.theme`, so both
  navigations are data-driven from the same model — no renderer-specific taxonomy.

## Open / next

- Wire `classifications.build()` output into the pipeline so findings carry their
  `sector` facet automatically.
- Decide the unit of analysis (per-code rollups vs per-named-set) — labels want
  fine-grain code-level aggregation so a code can roll into many labels.
- Surface threshold: whether the structural spine shows *all* 10,100 codes or a
  value-cut tail (product call).
- BEC Rev 5 (`hs2022_bec5.xlsx`) is downloaded for an optional **end-use** second
  facet (capital/intermediate/consumption) when wanted.
