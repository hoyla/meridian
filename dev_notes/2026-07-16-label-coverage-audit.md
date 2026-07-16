# Label coverage audit + lithium group de-dup

**Date:** 2026-07-16
**Trigger:** Luke noticed HS 282520's Sector-detail text describes it as an
EV-supply-chain feedstock, yet the group carries no EV supply-chain theme badge
— and asked whether that was a deliberate parent/child call or a bug.

## What it turned out to be

Not a deliberate rule — drift. Two live groups both cover HS 282520:

| group | patterns | themes |
|---|---|---|
| `Lithium hydroxide (battery-grade)` (id 21) | `282520%` | *none* |
| `Lithium chemicals (carbonate + hydroxide)` (id 52) | `283691%`, `282520%` | EV supply chain · China export-control regime |

The quoted "cell-grade lithium… NMC and NCA" text is group 21's live
description. How the pair arose: the original broad "Lithium chemicals" group
was **renamed in place** to "Lithium hydroxide (battery-grade)" and narrowed to
282520 only (Phase 5, the cleaner EV story — see `history.md`). The Q2 expansion
(`migrations/2026-06-22d`) then **re-inserted** "Lithium chemicals (carbonate +
hydroxide)" as a new row; `ON CONFLICT (name) DO NOTHING` didn't catch it
because the old name was gone. Both emit active findings; 282520 is
double-listed, one badged and one not.

On the "would badging both levels confuse readers?" question: no — labelling
parent and child is the **established** pattern (Rare-earth materials + its four
CN8 children; Solar inverters broad + CN8 children; Permanent magnets + Sintered
NdFeB). The `labels.py` invariant (labels overlap, never sum to a total) makes a
badge on both levels analytically free. What's confusing is the *current* state:
two near-identical lithium rows, one badged, one not.

## Decisions

1. **Retire group 21, keep group 52.** Carbonate+hydroxide is the wider, more
   correct story — LFP (lithium-carbonate) chemistry now dominates Chinese
   battery output, so a hydroxide-only group understates the flow. This reverses
   the Phase 5 narrowing deliberately. `schema.sql` already seeds only the
   survivor, so this is a live-DB cleanup that brings prod back in line with it.
   → `migrations/2026-07-16-retire-lithium-hydroxide-dup.sql` (supersede active
   findings, then delete the row — same shape as the 2026-06-22b Wind
   retirement; guarded on name + the 2026-05-09 `created_by` tag).

2. **Add "Conventional hybrids (HEV, non-plug-in)" to the Automotive theme.**
   The one clear omission among 15 theme-less groups: a car body type in HS 8703
   beside the other car groups, and currently a top mover. Automotive **only** —
   a non-plug-in HEV has no EV battery/charging supply chain, so deliberately
   not in "EV supply chain". The other theme-less groups are either legitimate
   broad catch-alls (Steel, Aluminium, Electrical machinery 84-85) or have no
   existing lens that fits (Machine tools, Semicon mfg eqpt, Drones, Telecoms
   base stations, Civil aircraft, PPE, adipic/inorganic acids) — left for an
   editorial pass, not invented here.

## Reusable guard (the "audit" deliverable)

The groups a theme names live in the DB; the theme definitions live in code. So
reconciliation can only be checked against a real group set. Three drift risks,
all now surfaced by pure helpers in `labels.py`
(`dead_member_refs`, `unthemed_groups`, `subset_pattern_collisions`), shared by:

- **`scrape.py --audit-labels`** — a diagnostic (mirrors `--eurostat-coverage`)
  that reconciles labels ↔ hs_groups in the connected DB: dead references,
  theme-less groups, and subset pattern collisions (‡ marks a same-granularity
  shared prefix — the likely-duplicate signal, vs an expected CN8-leaf-in-broad-
  parent). Point it at local or prod.
- **`tests/test_label_coverage.py`** — pure tests for the helpers + the
  Conventional-hybrids fix, plus a **live-DB guard** (`GACC_LIVE_DATABASE_URL`,
  skipped when unset, same convention as `test_orphan_findings.py`) that
  hard-fails on any dead label reference.

### A caveat this exposed

Running `--audit-labels` against the dev **local** DB reports the
"Oil & gas: origin watch" theme's two members
(`Refined petroleum products (HS 2710)`, `Natural gas & other petroleum gases
(HS 2711)`) as dead. That is a **stale local DB**, not a prod bug: `schema.sql`
and `migrations/2026-06-24b` both seed those groups; the local DB predates
2026-06-24b and never got it (no migration-tracking table here). The guard is
doing its job — it flags a DB that's behind. Whether prod is also behind on
2026-06-24b should be checked by running the guard against
`GACC_LIVE_DATABASE_URL`; if it is, the fix is to apply the pending migration,
not to touch `labels.py`.
