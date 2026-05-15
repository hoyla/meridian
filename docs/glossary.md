# Glossary

For when a term in the docs / `03_Findings.md` / `04_Data.xlsx` /
`02_Leads.md` looks unfamiliar. Each entry is a sentence or two;
cross-links lead to where the methodology or architecture goes
deeper.

- [Economic & data terms](#economic--data-terms) — trade
  economics, customs/HS codes, comparison cadences.
- [Sources](#sources) — the customs feeds, FX rates, and the
  analytical publications this tool serves alongside.
- [System & methodology terms](#system--methodology-terms) — how
  this project names its pipeline parts and editorial decisions.

The three docs that reference these terms in depth:
- [architecture.md](architecture.md) — system overview.
- [methodology.md](methodology.md) — what each finding means and
  when to quote it.
- [editorial-sources.md](editorial-sources.md) — the journalism
  the tool exists to support.

---

## Economic & data terms

### CIF / FOB
**CIF** (Cost, Insurance, Freight) is the import-side valuation
including freight and insurance to the destination port. **FOB**
(Free On Board) is the export-side valuation at the port of
loading. Eurostat and HMRC report CIF on imports; GACC reports
FOB on exports. The structural gap (typically 3–8% of value for
EU-CN trade) is captured per-partner in `cif_fob_baselines`; the
per-finding **excess over baseline** is the editorial signal.
See [methodology.md §0](methodology.md#why-the-same-trade-flow-has-different-numbers).

### CN8 (Combined Nomenclature, 8-digit)
The EU's 8-digit version of the HS classification system. Each
annual revision (every January) can add, split, or retire codes.
The universal `cn8_revision` caveat fires on any 24-month YoY
window because those always span at least one revision. Distinct
from **CHS8** — China's parallel 8-digit version, which diverges
from CN8 even though both align on HS-2 / HS-4 / HS-6.

### CN+HK+MO
The default partner set the tool sums on the Eurostat side,
treating Hong Kong (HK) and Macau (MO) as part of "Chinese
trade" since ~15% of China's exports to the EU route via HK.
For a CN-only spot check against a Soapbox or Merics figure,
query `eurostat_raw_rows` directly with `partner = 'CN'`.

### EU-27 vs EU-27 + UK (comparison scopes)
The three **scope** options on hs-group findings:
- `eu_27` = the 27 current EU member states, Eurostat reporters,
  GB excluded at all times (including pre-Brexit, for
  consistency).
- `uk` = UK-only, HMRC reporter.
- `eu_27_plus_uk` = sum of both; carries the `cross_source_sum`
  caveat reflecting methodological non-comparability.
See [methodology.md §2](methodology.md#2-the-three-comparison-scopes).

### Extra-EU
Trade with countries outside the EU-27. The `partner_share`
analyser uses extra-EU as its denominator (not all-partners)
because intra-EU trade dwarfs extra-EU trade for most products;
Soapbox-style "China's share of EU imports of Y" colloquially
means "of *extra-EU* imports of Y".

### HS-code (Harmonized System)
The international tariff classification standard, hierarchical:
HS-2 (chapter, e.g. 87 = vehicles), HS-4 (heading, 8703 = motor
cars), HS-6 (subheading, internationally aligned), HS-8
(national: **CN8** in EU, **CHS8** in China). Aggregating to
HS-6 minimises cross-source divergence for HS-level
comparisons.

### kg coverage
The fraction of a finding's `value_eur` backed by an actual
`quantity_kg` measurement. Groups dominated by HS codes whose
primary unit is pieces, litres, or supplementary units have low
kg coverage; unit-price decomposition (€/kg) is suppressed below
the 80% default and the `low_kg_coverage` caveat fires.

### Low base
A YoY finding whose smaller-of-(current_12mo, prior_12mo) sits
below the `low_base_threshold_eur` (default €50M). The
percentage rests on a small denominator and can swing widely;
quote alongside the absolute figures. Carries the
`low_base_effect` caveat.

### Mirror trade / mirror gap
**Mirror trade** is the cross-source comparison: what China's
customs says it exported to country X vs what X's customs says
it imported from China. **Mirror gap** is the (signed)
difference, normalised by the larger side. The structural gap
reflects CIF/FOB pricing, transshipment routing, classification
drift, etc.; the `mirror_gap_zscore` family flags partners whose
gap deviates from their own multi-period baseline.
See [methodology.md §1 → `mirror_gap`](methodology.md#mirror_gap-mirror-trade).

### Partial window
A YoY finding where 1 of the 24 months in the comparison is
missing (usually the most recent — Eurostat lags publication by
~6–8 weeks). Sums use what's there and the finding carries the
`partial_window` caveat.

### Partner share
The fraction of EU-27 *extra-EU* imports (or exports) of an HS
group that comes from / goes to China (CN+HK+MO summed).
Surfaces the Soapbox-style "qty share > value share" pattern
when China is shipping cheaper-per-kg than the rest-of-world
average.

### Single-month / 2-month / 12mo-rolling / YTD YoY
Four operators the tool emits per hs_group_yoy finding:
- **12mo rolling**: SUM(last 12 months) vs SUM(prior 12 months).
  Smooths seasonality; the editorial default.
- **Single-month**: latest month vs same month one year earlier.
- **2-month cumulative**: last two months vs same two months a
  year earlier.
- **YTD cumulative** (on `gacc_aggregate_yoy` /
  `gacc_bilateral_aggregate_yoy`): Jan-to-anchor of current year
  vs same range prior year.

Soapbox / Lisa usually quote single-month or YTD; 12mo rolling
is the "where does this trend stand" anchor.

### Stat procedure (Eurostat)
A reporter's tariff-regime breakdown per (period, partner, flow,
product): preferential tariff, MFN, special-regime, inward
processing, etc. The `observations` table sums across procedures;
the `eurostat_stat_procedure_mix` caveat fires universally because
a shift in the mix over a YoY window can itself be a story we're
not surfacing.

### Transshipment / transshipment hub
Goods physically Chinese in origin that an importer attributes to
a third country (Rotterdam in NL is the canonical hub for
EU-bound trade). The `transshipment_hubs` lookup seeds NL, BE,
HK, SG, AE, MX with citable `evidence_url`s; the mirror-trade
analyser attaches the `transshipment_hub` caveat when the
partner matches.

### YoY (Year-over-Year)
Percentage change between a window and the same-shape window
one year earlier. The tool emits four YoY operators — see
[Single-month / 2-month / 12mo-rolling / YTD YoY](#single-month--2-month--12mo-rolling--ytd-yoy).

### YTD (Year-to-Date)
SUM(Jan through anchor month) of the current year vs the same
range in the prior year. Emitted on `gacc_aggregate_yoy` and
`gacc_bilateral_aggregate_yoy` findings; captures the Soapbox
register "Jan-Apr trade +N%".

---

## Sources

### GACC (General Administration of Customs of the PRC)
China's customs agency. The English-side site
`english.customs.gov.cn` publishes monthly bulletins as HTML
release pages with section-numbered tables. The tool ingests
section 4 (by country/region). Currency CNY or USD per release;
valuation FOB.

### Eurostat Comext
The EU's external-trade statistics. Bulk monthly `.7z` files at
`ec.europa.eu/eurostat`. One CN8 row per `(reporter, partner,
flow, stat_procedure, product)`. EUR-native; CIF on imports.

### HMRC OTS (Overseas Trade Statistics)
UK customs declarations of trade with all partners, via the
`api.uktradeinfo.com` OData REST API. Post-Brexit canonical
source for UK-China trade. GBP-native; converted to EUR at
ingest using the ECB monthly-average rate.

### ECB (European Central Bank)
Daily and monthly-average FX rates from the SDMX endpoint
`data-api.ecb.europa.eu`. CNY/EUR for GACC, USD/EUR for the
USD-side GACC rows, GBP/EUR for HMRC.

### OECD ITIC
The OECD's International Transport and Insurance Costs of
merchandise trade dataset. Source of the per-(EU member, China)
CIF/FOB baselines in `cif_fob_baselines` (2022 values).
Reproducibility notes in
[`dev_notes/cif-fob-baselines-2026-05-10.md`](../dev_notes/cif-fob-baselines-2026-05-10.md).

### Soapbox Trade
The Substack at <https://soapboxtrade.substack.com>. The model
for the kind of analysis this tool helps Guardian journalists
do on their own schedule. Lisa O'Carroll relies on Soapbox for
headline figures. Range covers EVs, pharma APIs, honey, gold,
photovoltaics, wine, semiconductors, etc.
See [editorial-sources.md](editorial-sources.md#soapbox-trade).

### Merics (Mercator Institute for China Studies)
Quantitative analytical work on Chinese trade and policy. Lisa
cites them frequently. Useful cross-check before publication.
<https://merics.org/>

### Bruegel
Brussels economic think-tank. Mostly qualitative — useful for
"what does this mean for EU policy" framing alongside the raw
numbers. <https://www.bruegel.org/>

---

## System & methodology terms

### Anomaly subkind
The specific classification of a finding. Currently emitted:
- `mirror_gap` / `mirror_gap_zscore` — cross-source comparisons
- `hs_group_yoy{,_export,_uk,_uk_export,_combined,_combined_export}` — YoY by HS group, six scope×flow combos
- `hs_group_trajectory{,_*scopes...}` — shape of a YoY series
- `gacc_aggregate_yoy{,_import}` — non-EU bloc partner aggregates
- `gacc_bilateral_aggregate_yoy{,_import}` — EU bloc + single-country GACC partners
- `partner_share{,_export}` — China's share of EU-27 extra-EU imports/exports per HS group
- `narrative_hs_group` — LLM lead scaffold (in `02_Leads.md` only)

### Brief / findings document
The deterministic Markdown rendering at `03_Findings.md`. Three
tiers ([Tier 1 / 2 / 3](#tier-1--2--3)). Pure SQL → text; no LLM
in the loop. Pairs with `04_Data.xlsx` and `02_Leads.md` per export
folder.

### Brief run
A row in `brief_runs` recording a brief generation. Stamps the
Eurostat `data_period` (latest release at render time) and a
`trigger` (`'periodic_run'` vs `'manual'`). The periodic-run
pipeline's idempotency check reads these.

### Caveat
A named limitation on a finding. **Family-universal** caveats
fire on every finding of a subkind family (`cif_fob`,
`multi_partner_sum`, `cn8_revision`, etc.) and render once in
the brief's methodology footer. **Per-finding-variable** caveats
(`low_base_effect`, `partial_window`, `transshipment_hub`,
`low_kg_coverage`, etc.) ride on each finding's `caveat_codes`
array and surface inline.
See [methodology.md §3](methodology.md#3-caveats-reference).

### Finding
One row in the `findings` table. Has a `kind` (always `anomaly`
or `llm_topline`), a `subkind` (see [Anomaly subkind](#anomaly-subkind)),
a `natural_key_hash` + `value_signature` for idempotency, a
`detail` JSONB blob with all editorial context, and a stable
integer `id` that `finding/{id}` citation tokens point at.

### Finding ID / trace token
Every quotable number in `03_Findings.md` or `04_Data.xlsx` carries a
`finding/{id}` citation token (e.g. `finding/41349`). The integer
is the row's primary key. Stable across re-runs of the analyser —
when a finding revises, the new value gets a fresh id and the
old one is superseded but remains queryable.

### HS group
A journalist-editable cluster of HS-code patterns that the
analyser treats as a single editorial group. Defined in the
`hs_groups` table; the `created_by` column tags which editorial
input prompted each addition (e.g. `seed:lisa_article`,
`seed:soapbox_a1_2026_05_12`).

### Lead scaffold
The LLM-produced output for each HS group: anomaly summary +
2–3 hypotheses picked from a curated catalog of standard causes
+ deterministic corroboration steps. Rendered in `02_Leads.md`,
NOT in `03_Findings.md`, to keep deterministic output
downstream-LLM-safe. Every number cited must round-trip to a fact
in the prompt or the whole scaffold is rejected.

### Method version
A version string in every finding's `detail.method` field, e.g.
`hs_group_yoy_v11_per_reporter_breakdown`. Bumping the version
in the analyser code triggers supersedes across all existing
findings of that family on next run. Tracks methodology
evolution at the row level rather than via DB migration.

### Natural key
The identity tuple that says "this is the same finding in
editorial terms" — e.g. `(hs_group_id, current_end_yyyymm)` for
hs_group_yoy. The DB enforces "at most one active finding per
natural key" via a partial unique index on `natural_key_hash
WHERE superseded_at IS NULL`.

### Observation
One row in the `observations` table — a normalised per-cell
trade flow (flow × reporter × partner × period × HS-code), in
EUR, with an array of `eurostat_raw_row_ids` /
`hmrc_raw_row_ids` linking back to the verbatim source rows.

### Periodic run
The deployment-agnostic orchestrator: `python scrape.py
--periodic-run`. Idempotency-checks against
`brief_runs.data_period`, re-runs every analyser kind across
scope/flow combos, regenerates the findings export bundle.
Wrapped in a Claude Code Routine that fires daily.
See [architecture.md §Periodic-run orchestrator](architecture.md#periodic-run-orchestrator-phase-69).

### Predictability badge
🟢 / 🟡 / 🔴 next to each HS group heading in the brief.
Computed across all available (scope, flow) permutations at T
vs T-6: 🟢 = ≥67% of permutations stayed on-direction with
shift <5pp, 🟡 = 33–67%, 🔴 = <33%. Suppressed when fewer than
3 permutations have T-6 data.
See [methodology.md §10](methodology.md#10-known-editorial-output-limitations).

### Supersede chain
How the `findings` table tracks revisions. When an analyser
re-emits a finding with a different value signature (e.g.
underlying data revised, method version bumped), the prior row
gets `superseded_at = now()` and `superseded_by_finding_id`
pointing at the new row. Active findings are those with
`superseded_at IS NULL`.
See [architecture.md §Append-plus-supersede chain](architecture.md#append-plus-supersede-chain).

### Tier 1 / 2 / 3
The three sections of `03_Findings.md`:
- **Tier 1 — What's new this cycle.** Diff against previous brief.
  Auto-suppressed on method-bump cycles where ≥95% of supersede
  pairs are value-identical (renders a one-line "this cycle is
  plumbing" notice instead of a long churn list).
- **Tier 2 — Current state of play.** Compact per-HS-group summary.
  Inline `Trajectory: …` annotations are dropped when shape is
  `volatile` — absence signals "no useful narrative shape; lean on
  the headline %".
- **Tier 3 — Full detail.** Per-finding mover sections.

The brief opens with [**Top 5 movers this cycle**](#top-movers)
above Tier 1. A regular subscriber reads Top 5 → Tier 1; a new
joiner reads Top 5 → Tier 2 → Tier 3.

### Top movers
The composite-ranked editorial digest at the top of `03_Findings.md`
and `02_Leads.md`. Filter rules: |yoy_pct| ≥ 10pp, current_12mo_eur
≥ €100M, not low-base, predictability badge ≠ 🔴, current_end =
latest anchor across the family (recency filter). Score is
|yoy_pct| × log10(current_12mo_eur) — rewards "big move on a
meaningful base" without favouring either dimension alone. Same
scoring drives the `top_movers_rank` / `top_movers_score`
columns in `04_Data.xlsx`, so a journalist sorting the spreadsheet
by score lands on the same picks as the brief's Top 5.

### Threshold fragility
A finding whose smaller-of-(current_12mo_eur, prior_12mo_eur)
sits within 1.5× of `low_base_threshold_eur` (above OR below) —
the classification is sensitive to a small threshold movement.
Surfaced as ⚖️ in the Markdown, `near_low_base_threshold = TRUE`
in the spreadsheet.

### Trajectory shape
One of 12 vocabulary entries classifying a 24-month YoY series:
rising / falling (+ accel/decel), `inverse_u_peak`
(peak-and-fall), `u_recovery` (trough-and-recovery),
`dip_recovery`, `failed_recovery`, `volatile`, `flat`. Editorial
care: `volatile` over-fires at HS-group granularity (~68% of
trajectories at the current anchor).
See [methodology.md §4](methodology.md#4-trajectory-shape-vocabulary).

### Value signature
A deterministic hash of the editorially-meaningful values of a
finding (the YoY, the totals, the method tag, etc.). When two
re-runs produce the same signature, the existing finding is just
re-confirmed; when they produce a different signature, the old
row is superseded and a new one is inserted.

### Provenance file
A per-finding audit-trail Markdown file at `provenance/finding-N.md`
(at repo root) listing the source URLs, FX rates, plain-English
caveats, cross-source check, headline arithmetic, and replay SQL
for one specific finding. Generated on-demand via
`python scrape.py --finding-provenance N`, or bundled with the
editorially-fresh subset of findings in an export by
`--briefing-pack --with-provenance`. Frozen at generation time —
re-running on the same finding is a no-op unless `--force` is
passed. Detailed templates currently cover
`gacc_bilateral_aggregate_yoy{,_import}`, `hs_group_yoy*` (six
scope/flow variants), and `hs_group_trajectory*` (six scope/flow
variants); other subkinds emit a stub.

### Groups glossary
The `05_Groups.md` sister document in every export bundle, plus
its standalone form at `exports/groups-glossary-YYYY-MM-DD.md`
(via `--groups-glossary`). One section per HS group: editorial
description, HS LIKE patterns, top contributing CN8 codes from
the latest active `hs_group_yoy*` finding, and sibling groups
auto-discovered by 4-digit HS prefix overlap. Read once before
quoting any category figure — it makes explicit what each named
group does and does not contain.
