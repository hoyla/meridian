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

## Semiconductors theme (same day, Luke-approved)

Reviewing the theme-less groups prompted "should we have a theme for chips
(GPUs etc) and the components needed to make them?" — answered with data
(rolling 12mo EU↔CN, local DB): 8486 fab equipment **€7.56bn out / €0.15bn in**
(the ASML story, ~50:1) and 8542 ICs **€6.81bn out / €4.78bn in** — Europe is a
net chip *exporter* to China at heading level, against the intuitive narrative.
Raw CN8 data for all headings was already ingested (broad-ingest paying off).

New theme **"Semiconductors"** = existing `Semiconductor manufacturing
equipment` (8486, gains its first badge) + `Gallium, germanium & other minor
metals (HS 8112)` (chip inputs; second badge beside export-control) + three new
groups (migration `2026-07-16b`, seeded in schema.sql too):

- `Integrated circuits (HS 8542)`
- `Semiconductor devices excl. solar PV (HS 8541)` — enumerated patterns, NOT
  `8541%`: excludes PV cells 854142/43 (Solar's group) **and** pre-HS2022
  `854140`, which mixed LEDs/photosensitive with PV (~€25bn of mostly panels,
  2017–2021). Cost: LEDs/photosensitive enter the series only from 2022-01;
  the 854150 lineage has continuity. YoY/rolling-12mo anchored 2023+ unaffected.
- `Doped wafers (HS 3818)` — small (~€0.2bn each way) but upstream-complete.

Deliberate exclusions: **photoresists (3707)** — €20m each way, noise;
**polysilicon (280461)** stays Solar-only (code can't split solar- from
semiconductor-grade; tonnage is overwhelmingly solar). **GPU honesty caveat**
baked into the label definition and the 8542 description, and pinned by a test:
HS/CN8 cannot isolate GPUs/AI accelerators (bare chips → 854231 mixed with
CPUs; assembled cards → 8473), so the theme never yields a "GPU imports"
number.

Post-merge ops: `eurostat_world_aggregates` backfill for **3818 only**
(8542/8541/8486 already swept by the ch-84/85 broad group's aggregates), then
findings appear on the next periodic `--analyse`.

## Micromobility theme (same day, Luke-approved)

"Do we have coverage for electric bikes, electric scooters and electric
motorbikes?" — no: heading 8711 (where ALL electric two-wheelers classify) and
8712 (pedal bicycles) were tracked by no group, and chapter 87 has no catch-all
(cars 8703 / parts 8708 only). Raw data ingested back to 2017. The yearly read
surfaced two ready-made stories:

- **The anti-dumping cliff** — pedal-assist e-bike imports (87116010) fell
  €302m (2018) → €33m (2019), the year the EU's combined ~79% duties on the
  Chinese product took effect; still depressed. Conventional bicycles carry the
  EU's longest-running anti-dumping measure of all (since 1993).
- **The ~€1bn/yr e-scooter/moped boom** (87116090): €225m (2017) → €1.37bn
  (2022), ~€0.9–1bn/12mo since — and NOT covered by the duties, which also
  makes it where reclassified flows would surface.

New theme **"Micromobility"** (migration `2026-07-16c` + schema.sql seeds) =
three new groups: `Electric bicycles, pedal-assist (CN8 87116010)`,
`Electric motorcycles, scooters & mopeds (CN8 87116090)`,
`Bicycles, non-motorised (HS 8712)`. Design decisions, all test-pinned:

- The two 8711 groups **mirror the CN8 split** because the anti-dumping
  boundary runs exactly along it; one merged group would average the cliff
  away. A schema-parsing test keeps them an exact partition (no `871160%`).
- **Honesty caveat in the 87116090 description**: e-scooters, e-mopeds and
  e-motorcycles share one code and cannot be separated — never present it as
  an e-scooter-only number (same pattern as the Semiconductors GPU caveat).
- Pedal bicycles are in as the **substitution/trade-policy baseline**, not for
  an electric angle; deliberately NOT in "EV supply chain" or "Automotive".

Post-merge ops: `eurostat_world_aggregates` backfill for 87116010 / 87116090 /
8712 (nothing in ch-87 beyond 8703/8708 has aggregates), then the next
periodic `--analyse`.

## Soapbox amoxicillin/dependency-loop follow-up (same day, Luke-approved)

Luke flagged the second Soapbox piece
(https://soapboxtrade.substack.com/p/chinese-exports-to-the-eu-head-for) —
amoxicillin categorisation + the "solar dependency loop". Both headline claims
**reproduce** through our `detect_partner_share` helpers (CN+HK+MO numerator,
EU-27 reporters, extra-EU denominator, 2025 full year):

| claim | Soapbox | ours |
|---|---|---|
| China share, extra-EU penicillin-family (29411000) imports, by qty | 86% | **87.2%** (73.3% by value) |
| China share, extra-EU solar module (85414300) imports | 99% | **99.3%** qty / 98.3% value |

(Gap on penicillins ≈ their CN-only vs our CN+HK+MO envelope. First naive
attempt summed `observations` directly and got an impossible 103% on solar —
the analyser helpers exist precisely because the raw tables need the
subset/envelope filters. Bonus finding: all-of-2941 reads 76.7% by qty but
only 34.1% by value — the starkest tonnes-vs-euros gap we've seen.)

Shipped from this: **`Penicillin-family APIs (CN8 29411000)`** child group
(migration `2026-07-16d` + schema seed; Pharma & fine chemicals theme beside
its parent). Categorisation caveat is load-bearing, test-pinned copy:
amoxicillin has no EU customs line — the code also holds ampicillin,
piperacillin etc., never present it as amoxicillin-only (China's customs DOES
split it at 8 digits; that's the detailed-query-platform roadmap item). Policy
hook baked into the description: Sandoz anti-dumping complaint (May 2026) —
this series is positioned to record any pre-duty surge/post-duty cliff like
the e-bike codes did. No world-aggregates backfill needed (parent's 2941%
covers it).

Recorded in roadmap.md: the **mutual-dependency pairing** analyser (all-goods
buildable now from GACC section 4; division-level with Track 2; product-level
needs the detailed-query source) and the **GACC detailed-query platform**
(stats.customs.gov.cn) as a deferred targeted-query source.

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
