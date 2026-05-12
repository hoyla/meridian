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

1. **Tier 1 hs_group additions** — chemicals / feed / pharma-adjacent
   inputs + rare-earth sub-buckets + MPPT inverters / crude oil /
   aircraft / Central Asia alias. Pure `INSERT INTO hs_groups` rows
   picked up automatically by `hs_group_yoy` /
   `hs_group_trajectory` / `gacc_aggregate_yoy`. Zero briefing-pack
   code change. See "Tier 1 hs_group additions" below.
2. **briefing_pack.py modularisation** — split the 2,001-line file
   into a `briefing_pack/` package. Done *before* the structural
   analyser additions so each new analyser's section lands in its
   natural module from day one. See "Refactor backlog" below.
3. **`gacc_bilateral_aggregate_yoy` analyser** — emit per-(GACC
   partner, period, period_kind) YoY findings, including the EU
   bloc (currently excluded from `gacc_aggregate_yoy`) and using
   the `period_kind='ytd'` observations we already store. Surfaces
   the article's lead claim ($201bn / +19% etc.) as a finding on
   the next periodic run. See "EU/bilateral aggregate analyser"
   below — expansion of the existing entry.
4. **Selective Eurostat re-ingest with `partner='EXTRA_EU27_2020'`
   + `partner_share` analyser** — adds rest-of-world totals for the
   HS codes we already track so we can compute *share of EU imports
   by partner*. Unlocks an entire metric class (every "X% from China"
   claim Soapbox makes). Bigger lift (ingest expansion + new
   analyser + new section). See "Share-of-EU-imports analyser" below.

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

### Tier 1 hs_group additions (proposed step 1 above)

Pork offal (HS 0206 swine), Sintered NdFeB magnets (CN8 85051110)
and Natural graphite (HS 250410) shipped 2026-05-11. The 2026-05-12
A1 re-test added the following candidates, all with CN-side data
already in `eurostat_raw_rows` (verified per code, 2025 totals
shown for sanity):

**Chemicals / feed / pharma-adjacent inputs** (the article's "less
visible inputs" cluster — Soapbox A1 makes the qty-vs-value share
their headline analytical move):

- **Amino acids (HS 2922)** — 2025 CN→EU value €1.02B / 1,829 rows.
  Soapbox: 88% qty / 52% value.
- **Adipic acid (HS 291713)** — 2025 CN→EU €78M / 191 rows.
- **Choline (HS 292310)** — 2025 CN→EU €10M / 178 rows.
- **Vanillin + ethylvanillin (HS 29124100 + 29124200)** — 2025 CN→EU
  €40M + €12M. Soapbox ethylvanillin: 68% qty / 62% value.
- **Feed premixes (HS 230990)** — 2025 CN→EU €228M / 599 rows.
  Soapbox: 50% qty / 37% value.
- **Inorganic acids (HS 2811)** — 2025 CN→EU €167M / 736 rows.
  Soapbox "other inorganic acids": 60% qty / 47% value.
- **Aldehyde/ketone acids (HS 2918)** — separate group (Soapbox
  groups it with the above).

**Rare-earth sub-buckets** (the existing `Rare-earth materials`
group bulks the pre- and post-2023 CN8 splits together; cn8_revision
caveat suppresses the seam but the editorial story is in the
sub-codes):

- **Light rare-earth compounds (CN8 28469040)** — the "dark red
  bucket"; 2025 CN→EU 3,740 t / €5.3M. Soapbox: ~90% China share
  each year 2023-25 (share unverifiable from our data — see step 4).
- **Heavy rare-earth compounds (CN8 28469060 + 28469070)** —
  contains Dy/Tb-bearing compounds. 2025 CN→EU 67 t / €11.3M for
  28469060; 490 t / €13.6M for 28469070 (value rising sharply).

**Tier 2 — additions to fill obvious gaps the article exposed:**

- **MPPT inverters (CN8 85044084)** — separate code only from 1
  Jan 2026. Article quotes "more than €220M Jan-Feb 2026"; our
  CN-side sum is €209.3M (HK adds €23k). Limited history but
  add now so the next 24mo window catches it.
- **Crude oil (HS 2709)** — no oil coverage at present. Covers
  the article's Libya claim and broader China-energy partner
  picture.
- **Civil aircraft (HS 8802)** — covers the Boeing/Airbus story.
  GACC reports section-4 country aggregates so we can answer
  "China imports more aircraft from US than EU" only at country-
  aggregate level (which we already have).

**Central Asia alias** — add `country_aliases` row covering
KZ+UZ+KG+TJ+TM so `gacc_aggregate_yoy` picks it up. Soapbox A1
claim "$70bn+, tripled since 2020" becomes directly testable.

Each = one schema row + re-run analysers. Estimated <60 minutes
total including CN8 sanity-checks per code.

### EU/bilateral aggregate analyser (`gacc_bilateral_aggregate_yoy`) — proposed step 3 above

`gacc_aggregate_yoy` deliberately excludes `eu_bloc` per
[`anomalies.py`](../anomalies.py) ("mirror-trade handles EU").
The 2026-05-12 A1 re-test confirmed this leaves a real editorial
gap: Soapbox's USD top-lines ("$201B in Jan-Apr 2026, +19% YoY")
aren't the same finding as a bilateral mirror gap, and Lisa quotes
them directly. Our DB has the underlying observation — `releases`
join `observations` for partner='European Union', period='2026-04',
period_kind='ytd' returns USD 200,727M — but no analyser promotes
it to a finding.

**Scope of the new analyser:**

- One subkind per (period_kind, flow) combination —
  `gacc_bilateral_aggregate_yoy_{ytd,monthly}_{export,import,total}`.
  YTD subkinds answer the "Jan-Apr 2026 vs Jan-Apr 2025" framing
  Soapbox uses heavily; monthly answers the same-month YoY framing
  (Soapbox A3 "EU exports to China Feb 2026 -16.2%").
- Apply to all aggregate `country_aliases` rows including
  `eu_bloc`, plus single-country GACC partners (United Kingdom (US),
  Japan, etc.) so per-partner bilateral YoY is also surfaced.
- Method-version: bump to a new `gacc_bilateral_aggregate_yoy_v1_...`
  string — the existing `gacc_aggregate_yoy` keeps its current
  scope (non-EU aggregates) and natural keys; this is an additive
  analyser, not a refactor.

Covers ~7 currently-blocked Soapbox claims (every "China-X
bilateral aggregate YoY" claim across the validation doc).

### Share-of-EU-imports analyser + extra-EU re-ingest — proposed step 4 above

Soapbox's most distinctive analytical move is the "China share of
EU imports by qty vs value" pattern. The 2026-05-12 A1 re-test
made the gap concrete: every claim of the form "China supplied
X% of EU imports of Y" was unverifiable from our DB because
`eurostat.py` filters at ingest to `partner ∈ {CN, HK, MO}`
([eurostat.py:98](../eurostat.py)). We have the China-side
numerator but no rest-of-world denominator.

**Approach** (incremental, not a full re-ingest):

- Extend the Eurostat ingest CLI to accept `--partner EXTRA_EU27_2020`
  (or equivalent code for the rest-of-world bucket).
- Re-ingest historical periods for that one partner code across the
  HS codes we already track + the Tier 1 additions above. Smaller
  than ingesting every partner individually; bigger than the current
  CN+HK+MO slice.
- Add `partner_share_v1_qty_value` analyser. Subkind:
  `partner_share`. Natural key: `(hs_group_id, period, flow)`.
  Emits: `cn_share_qty_pct`, `cn_share_value_pct`, plus the
  qty-vs-value delta (the Soapbox-style flag). Universal caveat:
  `extra_eu_definitional_drift` (rest-of-world composition
  changes year-on-year as Brexit-era reclassifications settle).
- Briefing-pack section: new module under the post-modularisation
  `briefing_pack/sections/` directory.

**Cost / benefit:** highest analytical impact (whole new metric
class), highest cost (ingest expansion + new analyser + new
section). Lowest urgency in the sense that findings without it
remain defensible; high urgency in the sense that *most* future
Soapbox articles will lean on this metric.

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

## Refactor backlog

### briefing_pack.py modularisation — proposed step 2 above

[`briefing_pack.py`](../briefing_pack.py) is 2,001 lines: section
renderers (`_section_*`), DB helpers, formatters, trace-token
logic, and the `render()` orchestrator all in one file. Adding a
new section (which the bilateral aggregate analyser and the share
analyser both need) means appending to the bottom of an
already-long file.

**Target shape**: split into a `briefing_pack/` package:

- `briefing_pack/__init__.py` — re-exports `render`,
  `render_leads`, `export`, `is_threshold_fragile`,
  `_compute_predictability_per_group`, `_ALL_UNIVERSAL_CAVEATS`,
  `_SCOPE_LABEL`, `_SCOPE_SUBKIND_SUFFIX` (the symbols
  `sheets_export` and tests currently import).
- `briefing_pack/_helpers.py` — DB connection, trace tokens,
  formatters (`_fmt_eur`, `_fmt_pct`, `_fmt_kg`), shared
  predicates.
- `briefing_pack/sections/headline.py`, `reader_guide.py`,
  `diff.py`, `state_of_play.py`, `state_of_play_aggregates.py`,
  `hs_yoy_movers.py`, `trajectories.py`, `mirror_gaps.py`,
  `low_base.py`, `llm_narratives.py`, `methodology_footer.py`,
  `sources_appendix.py`, `about_findings.py` — one section per
  module.
- `briefing_pack/render.py` — the orchestrator (current
  `render()` + `render_leads()` + `export()`).

**Migration plan**: zero behaviour change — the diff is purely
moves + an `__init__.py` re-export shim so external callers (CLI,
sheets_export.py, tests, llm_framing.py) don't change. After the
move, the bilateral-aggregate analyser and the share analyser
each land as a new file in `sections/` rather than another
500-line append.

**Doing it now** rather than after the new analysers means: (a)
each new section lives in its natural module from day one; (b)
the migration diff stays clean (just moves, no new logic riding
along); (c) tests written against the new structure stay valid.

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
