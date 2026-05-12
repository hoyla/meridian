# Roadmap — outstanding work

What's still open. For history of what shipped, see
[`history.md`](history.md). For the design rationale that drove
the original Phase 1–6 plan, look at the git log around
`8f18e68`–`5d0e23e` (2026-05-09 to 2026-05-10).

## Proposed work order (post-2026-05-12 A1 re-test)

The 2026-05-12 re-test of Soapbox A1 ("China's export surge puts EU
trade defence in the spotlight") against the live DB
([soapbox-validation-2026-05-11.md § A1](soapbox-validation-2026-05-11.md#a1-2026-05-11--chinas-export-surge-puts-eu-trade-defence-in-the-spotlight))
confirmed every numerical claim the tool *could* check (no contradictions),
and surfaced the **shape** of the gap precisely: their reporting is
share-and-bilateral-aggregate-heavy; ours is growth-and-per-HS-group-heavy.
Closing that gap is a re-ingest + new-analyser job, not a methodology
rework.

Recommended order, cheapest-to-most-impactful-per-hour:

1. ~~**Tier 1 hs_group additions**~~ — **DONE 2026-05-12**. Thirteen
   new `hs_groups` rows shipped (`seed:soapbox_a1_2026_05_12`):
   seven chemicals/feed/pharma-adjacent groups, four rare-earth
   sub-buckets matching the 2023 EU CN8 split of HS 284690, plus
   MPPT solar inverters (CN8 85044084, limited history — will catch
   when 24mo accumulates) and Civil aircraft (HS 8802). Analysers
   re-run; 3,995 new YoY findings + 60 new trajectory findings
   emitted across the three comparison scopes × both flows. Crude
   oil (HS 2709) and the Central Asia alias dropped from this round
   — both need ingest-scope expansion to surface anything (China is
   not a meaningful crude-oil exporter to the EU, and GACC section
   4 doesn't break out Central Asian states individually). See
   "Tier 1 hs_group additions" below.
2. ~~**briefing_pack.py modularisation**~~ — **DONE 2026-05-12**.
   Split into a `briefing_pack/` package: `_helpers.py` (constants
   + cross-section utilities), `sections/*.py` (one file per
   `_section_*` builder), `render.py` (orchestrator), `__init__.py`
   (re-export shim). Zero behaviour change — test suite passes
   200/200 identical to pre-refactor. Largest section module is 172
   lines (`hs_yoy_movers.py`); smallest is 27 (`detail_opener.py`).
   New sections for steps 3 + 4 below land as new files in
   `sections/` rather than appends to a 2,000-line monolith.
3. ~~**`gacc_bilateral_aggregate_yoy` analyser**~~ — **DONE 2026-05-12**.
   New analyser covers the EU bloc + every single-country GACC
   partner. Each finding carries three YoY operators in
   `detail.totals` side-by-side: `current_12mo_eur`/`yoy_pct` (12mo
   rolling), `ytd_cumulative` (Jan-to-anchor vs same range prior
   year — the Soapbox A1 register), and `single_month` (anchor
   month vs same month prior year — the Soapbox A3 register).
   Subkinds `gacc_bilateral_aggregate_yoy[_import]`. Wired into
   the periodic pipeline. Brief renders a new "GACC bilateral
   partners" Tier-2 block between the per-HS-group view and the
   non-EU aggregate view. Spot-check 2026-04 EU export YTD vs
   article: ours +18.2% to €175.04B (≈$200B), article +19% to
   $201bn — within rounding + FX-source noise.
4. ~~**Selective Eurostat re-ingest + `partner_share` analyser**~~ —
   **DONE 2026-05-12**. Discovered that Eurostat bulk files contain
   no pre-computed aggregates (only 246 ISO-2 partner codes), so the
   denominator is computed at ingest time: `aggregate_to_world_totals`
   sums across all partners except the 27 EU-27 ISO codes
   (`eurostat.EU27_PARTNER_CODES`), producing an extra-EU total. New
   table `eurostat_world_aggregates` stores it. New analyser
   `detect_partner_share` divides our CN+HK+MO sum (from
   `eurostat_raw_rows`) into that extra-EU sum, emitting one finding
   per (hs_group, anchor) with `share_value`, `share_kg`, and the
   `qty_minus_value_pp` gap (the Soapbox "bigger in tonnes than in
   euros" signal). Backfilled 26 months (2024-01 → 2026-02). Spot-
   check vs Soapbox A1: new narrow group `Photovoltaic inverters
   (CN8 85044086)` shows **80% value / 91% qty** for 12mo to 2025-12;
   article claims 75% / 87%. Within ±5pp validation band; gap shape
   (+10.7pp qty-over-value) matches the editorial register exactly.

Tier 2 hs_groups (MPPT-only inverters, crude oil, aircraft, Central
Asia) can be bundled into step 1 or done independently anytime.

## Near-term (likely next session)

### Watch the first 2-3 real cycles + decide delivery vector

Periodic-run **pipeline + Routine** shipped 2026-05-11 (Phase 6.9 /
6.10 — see `history.md` and
[`periodic-runs-design-2026-05-11.md`](periodic-runs-design-2026-05-11.md)).
Routine fires daily at 09:01 local time. What remains is observation
and Layer-3 design:

- **Click "Run now" once from the Scheduled sidebar** to pre-approve
  the tools the Routine uses (`psql`, `python scrape.py ...`).
  Otherwise the first real scheduled run will pause on permission
  prompts.
- **Watch the first 2–3 real cycles land** (whenever the next
  Eurostat release publishes — typically 6-8 weeks after period
  close). Tier 1 currently shows same-day method-bump churn
  (everything created today); after the first real Eurostat-release
  cycle, it'll show the actual data diff. Validate that the diff
  reads usefully editorially.
- **Decide on delivery vector** (Layer 3) once we've seen what a
  real cycle looks like in Lisa's hands. Don't pre-pick
  email / Slack / Drive — pick after the first usable export
  has been delivered manually a few times.
- **Migrate Luke's environment** from laptop to desktop. Steps in
  the design doc § "Migration: laptop → desktop". Routines are
  account-bound; the pipeline is portable via `git clone` +
  `pg_dump | pg_restore`.

## Coverage extension (surfaced by the 2026-05-11 Soapbox validation pass)

Items the Soapbox validation surfaced as real gaps but not on
the periodic-runs critical path. Each is small-to-medium and
self-contained. See
[`soapbox-validation-2026-05-11.md`](soapbox-validation-2026-05-11.md)
for the per-claim test that motivates each.

### Tier 1 hs_group additions — shipped 2026-05-12

Pork offal (HS 0206 swine), Sintered NdFeB magnets (CN8 85051110)
and Natural graphite (HS 250410) shipped 2026-05-11. The 2026-05-12
A1 re-test added thirteen more, all tagged
`seed:soapbox_a1_2026_05_12`. Spot-check of latest hs_group_yoy
findings at current_end=2026-02, scope=eu_27, flow=1 (CN→EU
imports), `partial_window=false` (numbers from the live DB):

**Chemicals / feed / pharma-adjacent inputs** (the article's "less
visible inputs" cluster):

- **Amino acids (HS 2922)** — id=36. 12mo to 2026-02: €968M,
  -28.4% YoY value / -22.9% kg.
- **Adipic acid (HS 291712)** — id=37. €74M, -43.4% / -31.9%.
  (Roadmap originally proposed 291713 which is sebacic/azelaic;
  corrected to 291712 during the per-code data verification.)
- **Choline (HS 292310)** — id=38. €8M, -50.8% / -70.8% (low_base).
- **Vanillin and ethylvanillin (HS 29124100 + 29124200)** —
  id=39. €42M, -46.9% / -42.5% (low_base).
- **Feed premixes (HS 230990)** — id=40. €210M, -36.4% / -30.1%.
- **Inorganic acids (HS 2811)** — id=41. €171M, +5.0% / +28.3%.
- **Aldehyde/ketone acids (HS 2918)** — id=42. €663M, +0.4% / +12.1%.

**Rare-earth sub-buckets** (post-2023 EU CN8 split of HS 284690;
element labels per Eurostat 2024 CN8 nomenclature):

- **Lanthanum compounds (CN8 28469040)** — id=43. €5M, +5.2% /
  +7.4% (low_base). Aligns with Soapbox's "dark-red bucket"
  framing (bulk light-REE volume).
- **Praseodymium/neodymium/samarium compounds (CN8 28469050)** —
  id=44. €2M, +25.6% / +12.8% (low_base). Nd is the magnet element.
- **Gadolinium/terbium/dysprosium compounds (CN8 28469060)** —
  id=45. €11M, **+149.6% value** / -21.9% kg (low_base). The
  article's "blue bucket"; price-per-kg surging as Dy/Tb usage in
  high-end magnets accelerates.
- **Europium/holmium/erbium/thulium/ytterbium/lutetium/yttrium
  compounds (CN8 28469070)** — id=46. €16M, +85.0% / +67.5%
  (low_base). Yttrium dominates by volume; heavier elements small
  but strategically tracked.

**Tier 2 article-exposed gaps**:

- **MPPT solar inverters (CN8 85044084)** — id=47. **Skipped with
  insufficient_history** (code separated by the EU only from 1 Jan
  2026; 24mo window needs ~mid-2027). Included now so the next
  valid window picks it up automatically.
- **Civil aircraft (HS 8802)** — id=48. €1.16B, **+57.7%** /
  +26.7%. Notable mover.

**Dropped from Tier 1**:

- **Crude oil (HS 2709)** — CN→EU 2025 was effectively zero
  (€0.0M / 0.34 t — China is not a meaningful crude exporter to
  the EU). The article's Libya story is about China-as-importer,
  which our `partner ∈ {CN, HK, MO}` ingest can't reach. Logged as
  a data-source-expansion item, not a schema addition.
- **Central Asia `country_aliases` row** — GACC section 4
  doesn't break out KZ/UZ/KG/TJ/TM individually (they roll into
  the existing Belt & Road aggregate). Adding an alias without
  matching observation rows would emit empty findings. Logged as
  a data-source-expansion item.

### EU/bilateral aggregate analyser (`gacc_bilateral_aggregate_yoy`) — shipped 2026-05-12

`gacc_aggregate_yoy` deliberately excludes `eu_bloc` per
[`anomalies.py`](../anomalies.py) ("mirror-trade handles EU").
The 2026-05-12 A1 re-test confirmed this left a real editorial
gap: Soapbox's USD top-lines ("$201B in Jan-Apr 2026, +19% YoY")
aren't the same finding as a bilateral mirror gap, and Lisa quotes
them directly. The new analyser closes the gap.

**Shape**: rather than separate subkinds per (period_kind, flow),
one finding per (partner, anchor_period, flow) carries three YoY
operators side-by-side in `detail.totals`. This mirrors the
Phase 6.10 design on `hs_group_yoy` (where `single_month` and
`two_month_cumulative` are sub-fields, not separate findings):

- `yoy_pct` — 12mo rolling (the primary, drives `score` + supersede)
- `ytd_cumulative.yoy_pct` — Jan-to-anchor of current year vs prior
  year. Null when prior-year YTD is missing.
- `single_month.yoy_pct` — anchor month vs same month prior year.
  Null when prior month is missing.

Sharing one finding keeps the supersede chain coherent — when
underlying data revises, all three operators move together — and
matches the rendering pattern in the new
`briefing_pack/sections/state_of_play_bilaterals.py`.

**Coverage**: EU bloc + all single-country GACC partners
(`country_aliases.aggregate_kind = 'eu_bloc' OR IS NULL`). The
existing `gacc_aggregate_yoy` keeps its non-EU-multi-country scope
unchanged. Subkinds: `gacc_bilateral_aggregate_yoy` (export) /
`gacc_bilateral_aggregate_yoy_import` (import). Method version:
`gacc_bilateral_aggregate_yoy_v1_eu_and_single_countries`.

**First run** (2026-05-12): 2,664 findings emitted across 22
partners × 2 flows × ~30 valid anchor periods. EU export YTD
through 2026-04 = +18.2% to €175.04B (≈$200B at period FX) vs
Soapbox's +19% to $201bn — within rounding + FX-rate-source noise.

### Partner-share analyser + extra-EU world aggregates — shipped 2026-05-12

Soapbox's most distinctive analytical move is the "China share of
EU imports by qty vs value" pattern. The 2026-05-12 A1 re-test
made the gap concrete: every claim of the form "China supplied
X% of EU imports of Y" was unverifiable from our DB because
`eurostat.py` filters at ingest to `partner ∈ {CN, HK, MO}`. We
had the China-side numerator but no rest-of-world denominator.

**Implementation**. New table `eurostat_world_aggregates` stores
pre-summed extra-EU partner totals per (period, reporter, product_nc,
flow) for the HS prefixes our hs_groups track. The aggregator
(`eurostat.aggregate_to_world_totals`) excludes the 27 EU-27
ISO-2 partner codes (`eurostat.EU27_PARTNER_CODES`) so the
denominator matches the editorial register "share of imports from
*outside* the EU". Including intra-EU would conflate "share of EU
consumption" with "share of non-EU imports" — Soapbox always means
the latter.

The discovery during validation: the bulk files contain only ISO-2
country codes, no pre-computed `EXTRA_EU27_2020` or `WORLD`
aggregates. So "rest of world" has to be computed by summing across
all 246 partner codes Eurostat publishes, minus the 27 EU-27 codes.
The aggregator does this in a single streaming pass over the bulk
file (~11s per period). Backfill: 26 months (2024-01 → 2026-02),
77k–94k aggregate rows per period, ~2.2M total rows.

**Analyser** (`detect_partner_share`): emits one finding per
(hs_group, anchor_period, flow) under subkind
`partner_share[_export]`. `detail.totals` carries `share_value`,
`share_kg`, and `qty_minus_value_pp` — the Soapbox "bigger in
tonnes than in euros" signal. Natural key
`(hs_group_id, current_end_yyyymm)`. Method version
`partner_share_v1_eurostat_world_aggregates`. New universal caveat
`extra_eu_definitional_drift` documents that the 246-partner
denominator sums customs authorities with slight definitional
drift across CN8 revisions.

**Brief section**: `briefing_pack/sections/partner_share.py` —
Tier-3 block listing groups by share descending, with both shares
+ the gap + a brief framing note (>+5pp = unit-price pressure;
>-5pp = premium pricing; between = noise band).

**Validation** vs Soapbox A1. The article cites "China supplied
87% of EU solar inverter imports by quantity in 2025, compared
with 75% by value" — for the narrow PV-specific CN8 sub-code
85044086 (separated from broader HS 850440 as a new hs_group
during this work) our finding at 12mo-to-2025-12 shows **80%
value / 91% qty**, gap +10.7pp. Within ±5pp validation band on
the absolute figures; the qty-over-value gap shape matches
exactly. Direction and editorial framing fully reproduce
Soapbox's analytical move.

**Coverage limitation**: EU-27 scope only. UK and combined scopes
would need an HMRC-side world aggregate (HMRC ingest stores
GB+CN/HK/MO only) — forward work.

### ~~Single-month / 2-month YoY operator~~ DONE 2026-05-11

Single-month and 2-month-cumulative YoY are now sub-fields on
every hs_group_yoy finding (`detail.totals.single_month`,
`detail.totals.two_month_cumulative`). Method bumped v9 → v10.
Tier 2 render shows the latest-month figure alongside the 12mo
rolling. **Remaining work**: aggregate-level single-month YoY
(EU-CN deficit single month) is still derivable only from raw
rows — extending `gacc_aggregate_yoy` to carry the same
sub-fields would unblock A3.1 / A3.2 / A3.3 at native cadence
as named findings rather than raw-row queries.

### Per-reporter hs_group rollup

`hs_group_yoy` aggregates across all EU-27 reporters. Per-reporter
breakdowns are one query filter away; useful for the
"Germany-as-bellwether" story shape (e.g. Soapbox A4.5 / A5.6:
"Germany alone accounts for 66% of the EU-wide drop in car-part
exports"). Touches `hs_group_yoy` emission logic (method-version
bump territory) so heavier than the sub-group additions above.

### Eurostat-side HS-level mirror for "China's exports to EU"

Soapbox routinely quotes GACC-side HS-level figures ("China's
EV+hybrid exports to EU +87% in Q1 2026 at $20.6B"). GACC
sections 5/6 in our DB have only ~30 hand-curated commodity
names (no HS codes), so the GACC-side HS-level test is blocked
on parser work for sections 5 and 6 specifically. The cleaner
path is to rely on Eurostat for HS-level and accept the CIF/FOB
caveat — but the editorial register ("China reported $20.6B...")
isn't substitutable.

### 2017 pre-v2 COMEXT format duplicate `000TOTAL` rows

Surfaced by the §5.4 snapshot refresh on 2026-05-11. The pre-v2
bulk-file format produces duplicate `000TOTAL` rows per
(reporter, period, partner, flow, stat_procedure) — 2017 sp=1
has 648 rows vs 2018 sp=1 has 351. Analyser output is unaffected
(HS LIKE filters skip aggregate rows) but any 2017 raw-row
aggregate rollup is 2x inflated. Forward work to dedupe or
re-ingest 2017 with the v2 parser. Independent of the 000TOTAL
filter rule resolution from 2026-05-10.

## Methodology depth (pick up if a story warrants it)

### CN8 concordance table (Phase 4 carry-over)

Full mapping of old→new codes across Eurostat's annual revisions.
Currently we apply a blanket `cn8_revision` caveat to any YoY
window spanning a year boundary; a real concordance would let us
strip the caveat where the relevant codes didn't change. The
historical Eurostat backfill (2017–2026) spans 9 CN8 revisions so
the blanket caveat is on most findings. Pick up when a story rests
on a precise YoY for a specific HS-CN8 code.

### Per-(country, commodity) CIF/FOB granularity

Phase 4 carry-over partially addressed in
`cif-fob-baselines-2026-05-10.md`. Per-(EU member state, China)
margins are now in `cif_fob_baselines`. The OECD ITIC SDMX endpoint
also supports HS-4 splits (1224 commodities × 28 EU countries × CN
≈ 34k rows) for per-(country, commodity) precision. Schema-extend
`cif_fob_baselines` and pull when a story needs it.

### Structural-break detection (Chow / CUSUM)

Statistically right but unstable on the 11 periods we had at
Phase 1. With the historical Eurostat backfill we now have 110
periods; this is ready to move from parked to scheduled if a
journalist's question warrants it.

### Sector breadth review (round 2)

The 6.5 promote/drop pass shipped 2026-05-10. A year from now a
similar pass should re-evaluate what's editorially live. Three
groups stayed draft (Honey, Polysilicon, Tropical timber) and
might warrant a second look.

## Data sources (deferred until needed)

### 2018 GACC mirror-trade

See [`forward-work-gacc-2018-parser.md`](forward-work-gacc-2018-parser.md).
Title parser handles all 2018 quirks but the section-4 release
pages embed PNG screenshots, not HTML tables. Body parse fails
and the data is in pixels. Options: OCR (~half-day with editorial
risk), hunt for source xlsx, accept gap, lean on Eurostat+HMRC
which already cover 2018.

### Aggregate-label handling for non-EU blocs (ASEAN, RCEP)

Original project requirement. GACC reports trade with these blocs
as labelled aggregates; we don't currently split them into
member-country flows. Pick up if a non-EU-bloc story emerges.

### Chinese-language source URL backfill on `releases`

Most GACC releases have a Chinese-language equivalent at
`www.customs.gov.cn` (vs the English `english.customs.gov.cn`).
The brief constructs the link via `_construct_chinese_source_url`
but we don't store it. Backfill if useful for downstream consumers.

## Future-platform items

### Web UI / hosted deployment

Required for any non-luke-laptop use. AWS-side (Fargate, RDS,
Cognito) per the fuel-finder precedent. Triggers: when a desk
journalist actually wants to use the tool independently.

### Custom Q&A bot (Phase 7+)

See [`forward-work-qa-bot.md`](forward-work-qa-bot.md). Two-tier
scope (ask the findings, ask the underlying data). Tier 1 is ~3-5
days of build. Triggers: web UI exists AND a journalist has a
recurrent question pattern the brief can't answer.

### GoogleSheetsWriter implementation

Pending service-account credentials (was due "next week" as of
2026-05-09). Once available, the sheets-export module wires up
trivially.

## Closed forward-work, kept for reference

These are real options that may be picked up later but aren't on
any near-term path:

- [`forward-work-gacc-2018-parser.md`](forward-work-gacc-2018-parser.md)
  — 2018 image-only blocker (above).
- [`forward-work-qa-bot.md`](forward-work-qa-bot.md) — Phase 7+
  Q&A bot (above).
- [`shock-validation-2026-05-09.md`](shock-validation-2026-05-09.md)
  + companion `.sql` — pre-registered shock validation
  methodology. Living methodology doc, not just a one-off; re-run
  after any major analyser change. §5.4 refreshed 2026-05-11
  using the canonical `product_nc='000TOTAL'` row.
- [`soapbox-validation-2026-05-11.md`](soapbox-validation-2026-05-11.md)
  — peer-comparison validation against Soapbox Trade
  (substack). 50 testable claims pre-registered, ~60% clean concur,
  ~80% directional. Stage B follow-ups #3 (pork+offal) and #6
  (§5.4 refresh) completed same day; surfaced the open items in
  "Coverage extension" above. Re-run after any major analyser
  change, same pattern as shock-validation.
- [`cif-fob-baselines-2026-05-10.md`](cif-fob-baselines-2026-05-10.md)
  — sourced reference for the OECD ITIC backfill. Reproducibility
  notes for refreshing in a future year.
