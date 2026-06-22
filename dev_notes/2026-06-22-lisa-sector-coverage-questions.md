# Lisa O'Carroll sector-coverage questions — investigation + actions (2026-06-22)

**Trigger.** Two questions from Lisa about what HS categories the tool covers:

1. Do we cover EV plug-in hybrids and hybrid batteries? Context: the 2024 EU
   EV tariffs may have driven a pivot toward critical EV parts (one such part
   reportedly up ~300% since), which she suspects we may be missing.
2. Do we have good coverage of the main chemicals used to manufacture
   cosmetics, pharma and paint, and of products that use refined critical
   minerals?

---

## Framing: "coverage" is two layers

This distinction governs both answers.

- **Data layer.** The EU↔China bilateral ingest stores the *entire* CN8
  universe — ~10,100 distinct 8-digit codes (`reference/cn8_sitc.csv`),
  including ~511 organic-chemical, 271 inorganic, 116 pharma, 78
  cosmetics/essential-oil codes, plus paint pigments, plastics, etc. All
  navigable in the portal via the SITC spine. **Nothing in chapters 28–38 is
  missing from the data.**
- **Editorial-group layer.** Only the ~35 curated `hs_groups`
  (`schema.sql`, the `INSERT INTO hs_groups` seed) get the proactive treatment:
  YoY findings, volume-vs-price decomposition, narratives, takes, briefing
  entries. This is what Lisa actually sees surfaced.

So "do we cover X" almost always means "have we made a *group* for it" — the
data is already ingested. Adding a group is a seed row plus (for the
share-of-extra-EU-imports metric only) a `eurostat_world_aggregates` backfill
for the new prefix. The YoY half works immediately; nothing is re-fetched.

The analyser picks up **every** group with no draft filter
(`anomalies._list_hs_groups`); the `draft:` prefix on `created_by` only changes
how the renderer flags a group (a "(draft — methodology not yet validated)"
suffix and its own section). So seeding a group — even as a draft — starts
generating findings on the next run. That is why the Q2 proposal below is
deliberately **not** seeded yet (see "Decision" there).

---

## Q1 — EV plug-in hybrids and hybrid batteries

The tariff framing checks out. The Oct 2024 EU definitive countervailing duties
apply to **BEVs (and EREVs) only**; plug-in hybrids, conventional hybrids,
**car parts and battery packs are all out of scope**. Chinese PHEV sales in
Europe rose ~6× (≈27k→160k) 2024→2025, and parts/batteries kept surging because
they were never tariffed — so a pivot toward plug-ins and components is exactly
what the policy design produces.

What we surfaced against that, before this change:

| Item | Covered? | Group / HS |
|---|---|---|
| Plug-in hybrid cars (PHEV) | ✅ yes | `EV + hybrid passenger cars` — patterns include 870360 + 870370 |
| Battery-electric cars (BEV) | ✅ yes | same group, 870380 |
| EV/PHEV traction batteries (Li-ion) | ✅ yes | `EV batteries (Li-ion)` — 850760 |
| Generic motor-vehicle parts | ✅ yes (excludes motors & batteries) | `Motor-vehicle parts` — 8708 |
| Electric traction motors / drive units | ❌ no | HS 8501 — ungrouped |
| Conventional (non-plug-in) hybrids | ❌ no | 870340/870350 — ungrouped |
| NiMH "hybrid batteries" (older HEVs) | ❌ no | 850750 — ungrouped (only Li-ion) |

### Bug found

`EV + hybrid passenger cars` described its codes as "870380 (electric only),
870370 (PHEV), 870360 (HEV non-plug-in)". Per HS 2022 that is wrong: **870360 is
plug-in petrol hybrid and 870370 is plug-in diesel hybrid — both PHEVs.** The
non-plug-in hybrids are 870340/870350, which were *not* in the patterns despite
the prose claiming 870360 covered them.

Net effect: the patterns were always right (BEV + both PHEV = NEV ex-FCEV, so
the PHEV surge *was* being measured), but a reporter quoting a rendered finding
could have called a plug-in hybrid a non-plug-in one — a defensibility risk.

### Shipped (branch `ljh-ev-coverage-lisa-sector-q`)

`schema.sql` seed + idempotent migration
`migrations/2026-06-22-ev-coverage-lisa-sector-q.sql`:

1. **Corrected** the `EV + hybrid passenger cars` description (codes relabelled
   accurately + tariff-scope note). Patterns unchanged → no findings
   superseded, no orphan-finding risk (`tests/test_orphan_findings.py` keys off
   `name`, which is unchanged).
2. **Added** `Conventional hybrids (HEV, non-plug-in)` = 870340/870350.
3. **Added** `Electric motors & generators (HS 8501, broad)` = 8501. Broad on
   purpose (8501 spans sub-watt motors to alternators); EV drive motors
   concentrate in CN8 85015350–85015399 (AC > 75 kW) — both code families
   confirmed present in the data before committing. Description points at that
   refinement, matching the existing broad/narrow idiom (cf. Permanent magnets
   8505 → sintered NdFeB 85051110).

### On the ~300% figure

Not validated from our side — no public source I checked pins a clean 300% to a
single code (figures around: Li-ion accumulator imports +~29% latest period,
~90% China share; PHEV units ~6×). The codes that *test* it are now all in
place: 870360/870370 (plug-ins), 850760 (batteries), and the new 8501 motors
group. One caveat: 850760 is the **whole** Li-ion family (EV packs, cells,
stationary storage, consumer electronics) — refine to CN8 for an EV-pack-only
signal.

### Possible follow-ups (not done)

- EV-traction-motor-specific group at CN8 85015350–99 if Lisa wants a tight
  signal rather than the broad 8501 bucket.
- NiMH `850750` if classic-hybrid battery chemistry ever matters (small flow).

---

## Q2 — chemicals for cosmetics / pharma / paint, and refined critical minerals

**Short answer: no, not as surfaced groups — with one exception.** We have a
targeted specialty-chemicals set and good rare-earth/graphite coverage, but
cosmetics, pharma and paint feedstocks are not grouped, and refined critical
minerals are grouped only for rare earths + graphite (not lithium, cobalt,
titanium, etc.).

What we *do* surface (all from the Soapbox-validation work):

- **Specialty/intermediate chemicals:** amino acids (2922), adipic acid
  (291712), choline (292310), vanillin/ethylvanillin (2912 41/42), feed
  premixes (230990), inorganic acids (2811), aldehyde/ketone acids (2918).
- **Refined critical minerals — REE & graphite, well covered:** REE metals
  (280530), cerium (284610), the four post-2023 REE sub-buckets
  (28469040/50/60/70), sintered NdFeB magnets (85051110), natural graphite
  (250410).

Gaps against Lisa's three sectors:

- **Cosmetics** — nothing. No HS 33 (essential oils / perfumery / cosmetic
  preparations), no surfactants (3402), no titanium-dioxide pigment (320611).
- **Pharma** — almost nothing. No HS 30 (formulated medicaments 3003/3004), and
  only a handful of chapter-29 sub-codes — the API families (antibiotics 2941,
  vitamins 2936, hormones 2937, alkaloids 2939) aren't grouped. (Note: a
  "Pharmaceutical APIs" group existed once and was dropped in Phase 6.5 — see
  `tests/test_orphan_findings.py` header. Worth understanding *why* before
  naively re-adding.)
- **Paint** — nothing. No HS 32 (pigments, paints, varnishes 3208–3210), no
  titanium dioxide (320611).
- **Refined critical minerals beyond REE/graphite** — gaps: lithium carbonate
  (283691), lithium oxide/hydroxide (282520), cobalt (810520 / oxides 282200),
  manganese, tungsten, gallium/germanium (8112), antimony (8110), TiO₂ again.

### Decision

Hold on seeding these. The selection of *which* feedstocks to surface is an
editorial call for Lisa (journalism principle 1 — don't bake the hypothesis
into extraction), and because the analyser surfaces any seeded group
immediately, seeding a speculative set would start emitting findings she hasn't
asked for. So this stays a proposal until she prioritises.

### Proposed groups for Lisa to prioritise — material-typed, application via themes

**Correction to an earlier draft of this note.** The first cut tiered the
proposal by *application* (A: critical minerals, B: pharma, C: cosmetics,
D: paint) — and titanium dioxide landed in both the minerals and paint tiers,
which is precisely the application-binding smell. Per the 2026-06-20 taxonomy
design (`dev_notes/2026-06-20-taxonomy-sitc-spine-and-labels.md`) and Lisa's
Jun-2026 point: **groups are named by material; application is carried by the
many-to-many theme layer (`labels.py`), never by partitioning the group.** A
multi-industry chemical gets one material group and as many themes as it
genuinely serves.

So the proposal is a flat list of **material groups**, each tagged with the
**themes** it would join. Themes marked *(new)* don't exist yet and would be
added as `labels.py` entries; the rest already exist.

| Material group (HS) | Themes |
|---|---|
| Lithium carbonate `283691` + lithium oxide/hydroxide `282520` | EV supply chain · China export-control regime |
| Cobalt oxides/hydroxides `282200` + cobalt `8105` | EV supply chain · China export-control regime |
| Manganese oxides `282010` | EV supply chain |
| Tungsten ores/oxides/carbide `2841` / `8101` | China export-control regime |
| Gallium/germanium/indium `8112`; antimony `8110` | China export-control regime |
| **Titanium dioxide `320611`** | Paint & coatings *(new)* · Cosmetics & personal care *(new)* |
| Antibiotics `2941` | Pharma & fine chemicals |
| Vitamins/provitamins `2936`; hormones `2937`; alkaloids `2939` | Pharma & fine chemicals |
| Formulated medicaments `3003`/`3004` (HS 30) | Pharma & fine chemicals |
| Essential oils `3301` + odoriferous mixtures `3302` | Cosmetics & personal care *(new)* |
| Surfactants `3402`; beauty/make-up preparations `3304` | Cosmetics & personal care *(new)* |
| Paints/varnishes `3208`/`3209`/`3210`; pigments `3206` | Paint & coatings *(new)* |

**Titanium dioxide is the worked argument for the model**: one material group
(`320611`), three story angles (paint pigment, cosmetics filler/whitener,
refined-mineral product) expressed as three theme memberships — not three
groups, and not one group arbitrarily filed under "paint".

Notes:
- `labels.py` already seeds a **Pharma & fine chemicals** theme whose
  member_groups list anticipates `Antibiotics (HS 2941)`, an ibuprofen-class
  and a paracetamol-class group — i.e. the theme is waiting for the groups. The
  pharma rows above would slot straight in.
- **Cosmetics & personal care** and **Paint & coatings** would be two new
  labels (a name + definition + member groups).
- The refined-mineral rows lean on the existing **China export-control regime**
  and **EV supply chain** themes — close to work we already do.

**Worked example already shipped on this branch (Task 1).** Choline (`292310`)
and the feed amino-acids group (`2922`) were Pharma-themed only, despite being
core animal-nutrition inputs. They now also carry **Food & agriculture** — the
same group surfacing under both its applications via overlapping themes, with
the overlap spelled out in the label's definition for auditability. This is the
template every dual-use row above follows.

Each chosen group is still a seed row + migration (+ `eurostat_world_aggregates`
backfill for the share metric) — mechanically identical to the EV additions —
plus its theme membership(s) in `labels.py`.

### BUILT — first tranche + engines (branch `ljh-q2-expansion-minerals-pharma-engines`, 2026-06-22)

Lisa liked the list and is "very open to suggestions", so we built the first
tranche directly (schema.sql + `migrations/2026-06-22d-q2-expansion-groups.sql`;
theme memberships in `labels.py`). Material-named; themes in brackets.

**Refined critical minerals** (`seed:lisa_q2_2026_06`)
- Lithium chemicals (carbonate + hydroxide) — `283691`,`282520` [EV supply chain · China export-control]
- Cobalt (oxides, hydroxides & unwrought) — `282200`,`810520` [EV supply chain · China export-control]
- Manganese oxides — `282010` [EV supply chain]
- Tungsten (HS 8101) — `8101` [China export-control]
- Gallium, germanium & other minor metals (HS 8112) — `8112` [China export-control]
- Antimony (HS 8110) — `8110` [China export-control]
- Titanium dioxide (CN8 320611) — `320611` [no theme yet — Paint/Cosmetics arrive in round 2]

**Pharma APIs** (`seed:lisa_q2_2026_06`) — all [Pharma & fine chemicals]; the first three
realise members the theme already anticipated
- Antibiotics (HS 2941) · Ibuprofen-class monocarboxylic acids (HS 2916) ·
  Paracetamol-class amides (HS 2924) · Vitamins & provitamins (HS 2936)

**Engine parts + engines** (`seed:lisa_engine_parts_2026_06`, Lisa's "more engine parts"
request) — both [Automotive]; the engine side 8708 excludes
- Engine parts (CN8 84099100 + 84099900) · Internal-combustion engines (HS 8407 + 8408)

The broad-by-design groups (8112 minor metals; 2916/2924 acid/amide families;
8407/8408 engines) carry a "refine to CN8 / subheadings" steer in their
descriptions. YoY findings land on the next `--analyse hs-groups` run; the
China-share metric needs the `eurostat_world_aggregates` backfill for the new
prefixes (follows).

**Round 2 — BUILT 2026-06-22** (branch `ljh-q2-round2-cosmetics-paint`;
`migrations/2026-06-22e`). Cosmetics + paint, all `seed:lisa_q2_round2_2026_06`:
- Essential oils & fragrance mixtures (HS 3301 + 3302) [Cosmetics & personal care]
- Beauty, make-up & skin-care preparations (HS 3304) [Cosmetics & personal care]
- Paints & varnishes (HS 3208-3210) [Paint & coatings]

Two new themes added to `labels.py` — **Cosmetics & personal care** and
**Paint & coatings** — and the existing **Titanium dioxide (320611)** group now
joins both (the pigment bridge, so it's no longer themeless). Deferred again:
surfactants (HS 3402 — dominantly cleaning, would want a "cleaning products"
theme) and a broad pigments group (HS 3206 — overlaps the TiO₂ group).

### Done — legacy application-bound groups (branch `ljh-legacy-group-taxonomy-retrofit`, 2026-06-22)

Retrofitted via a new `hs_groups.display_name` column + `db.group_display_names`
resolver, rather than renaming the `name` key (which is snapshotted into
findings and hardcoded in ~100 tests — a rename would orphan findings and force
a sweep). `EV batteries (Li-ion)` now displays as
`Lithium-ion accumulators (HS 850760)` everywhere a reader sees it (portal,
briefing, sheets, glossary), with heading/slug/cross-link consistency; the key
is unchanged so it stays in the `EV supply chain` theme and no findings orphan.
`Wind turbine components` was retired (its `850300`/`730820` patterns weren't
wind-specific) — `Wind generating sets only` survives and a new overlapping
`Wind power` theme (labels.py) gathers the wind-relevant groups. `Solar/grid
inverters (broad)` left as-is by decision (already hedged; has PV-specific
siblings). Slug consistency is locked by a regression test
(`test_portal.test_display_name_substituted_and_slug_is_consistent`).
