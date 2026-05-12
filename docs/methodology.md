# Methodology

What each finding actually means, what its limits are, and when it's
quotable. Aimed at a journalist asking "can I use this in copy?".

For the system that produces the findings, see
[architecture.md](architecture.md). For the journalism the tool
serves, see [editorial-sources.md](editorial-sources.md).

## 0. Sources, and why they don't agree

The tool ingests three customs sources. They report — nominally —
the same trade. They never agree exactly. The divergences are
themselves the editorial story: a "mirror gap" between what GACC
says China exported to country X and what X's customs says it
imported from China is the headline product of this tool.

But the divergences come from real causes, not bookkeeping
mistakes. Before quoting any cross-source comparison, you need to
know what each source IS and what it's NOT.

### GACC — General Administration of Customs of the People's Republic of China

- **What:** monthly bulletins of Chinese customs declarations of
  trade with named countries / regions.
- **Reported on:** `english.customs.gov.cn` (English) and
  `customs.gov.cn` (Chinese).
- **Currency:** CNY or USD per release.
- **Valuation:** **FOB** (Free On Board) — value at the port of
  loading in China; freight + insurance to the destination NOT
  included.
- **Lag:** preliminary release ~10 days after period close;
  monthly bulletin follows.
- **Coverage:** China's perspective only. So GACC reports
  "exports to NL = X" but doesn't tell you what NL declared as
  imports from China.
- **Format:** HTML release pages (parsed) + per-format URL
  conventions; some 2018 pages embed PNG screenshots instead of
  tables and aren't ingestable.

### Eurostat — Statistical Office of the European Union

- **What:** bulk dumps of EU member states' customs declarations of
  trade with non-EU partners. We use the monthly `full_v2_YYYYMM.7z`
  files, one CN8 (8-digit Combined Nomenclature) row per
  `(reporter, partner, flow, stat_procedure, product_nc)`.
- **Reported on:** `ec.europa.eu/eurostat`.
- **Currency:** EUR-native.
- **Valuation:** **CIF** (Cost, Insurance, Freight) on the import
  side — value at the point of entry into the importing country,
  including freight + insurance from origin.
- **Lag:** ~6-8 weeks behind the period being reported.
- **Coverage:** EU-27 only. UK reporter rows exist pre-2021 (we
  exclude them — see "EU-27 means EU-27" in §3 below). HK / MO
  appear as separate `partner_country` codes, not as part of CN.
- **Quirks:** the bulk file ships a `product_nc='000TOTAL'`
  per-(reporter, period, partner, flow, stat_procedure) row that
  sums all CN8 detail rows — naïve `SUM(value_eur)` would
  double-count by ~2x. The `'85XXXXXX'` and `'850610XX'` style
  codes are confidentiality residuals, NOT chapter aggregates.

### HMRC OTS — UK HM Revenue & Customs, Overseas Trade Statistics

- **What:** UK customs declarations of trade with all partners
  (EU and non-EU).
- **Reported on:** `api.uktradeinfo.com` (OData REST).
- **Currency:** GBP-native; we convert to EUR at ingest time using
  the period's ECB monthly-average rate.
- **Valuation:** CIF on imports.
- **Coverage:** UK only — sole post-Brexit source for UK trade
  with China. Carries its own `SuppressionIndex` flag where small
  flows from few traders would breach confidentiality.

### Why the same trade flow has different numbers

A non-exhaustive list, in rough order of magnitude:

1. **CIF vs FOB pricing baseline.** Eurostat (CIF imports) is
   structurally higher than GACC (FOB exports) for the same flow
   by roughly the freight + insurance cost from China to the
   destination port. The OECD ITIC dataset gives per-(country)
   margins: typically **3–8%** for EU-27 + UK destinations
   (e.g. NL 6.55%, DE 6.50%, FR 7.22%). We populate
   `cif_fob_baselines` from ITIC; mirror-gap findings expose the
   excess **over** this expected baseline. (See §3 caveat
   `cif_fob`.)

2. **Partner attribution.** Goods physically Chinese in origin can
   be reported by Eurostat under partner=HK / partner=MO if the
   shipping documentation routes them via Hong Kong or Macau.
   ~15% of China's reported exports route via HK. We sum CN + HK + MO
   on the Eurostat side (`anomalies.EUROSTAT_PARTNERS`). For a CN-only
   spot check against a Soapbox / Merics single-partner figure, query
   `eurostat_raw_rows` directly with `partner = 'CN'` — the analyser no
   longer accepts a partner-set override since CN+HK+MO is the
   editorially-correct production envelope. (See §3 caveat
   `multi_partner_sum`, which is family-universal.)

3. **Transshipment via third countries.** Goods might leave China
   FOB-declared to NL, transit Rotterdam, then be declared by DE
   as imported from CN under DE's mirror flow — or under partner=NL
   if the importer is Dutch. The known transshipment hubs (NL, BE,
   HK, SG, AE, MX) get a `transshipment_hub` caveat
   automatically. The classic NL ~65% Eurostat-higher mirror gap
   is mostly this, NOT a smuggling story. (See §3 caveat
   `transshipment_hub`.)

4. **Different HS classifications at HS-8.** GACC uses CHS8
   (Chinese 8-digit harmonised); Eurostat uses CN8 (Combined
   Nomenclature). Both align with the HS-2 / HS-4 / HS-6
   international standard, but HS-8 codes diverge. So a
   GACC-vs-Eurostat HS-8 comparison may compare different
   commodity definitions. Aggregate to HS-6 to minimise.
   (See §3 caveat `classification_drift`.)

5. **Stat-procedure mix.** Eurostat splits by `STAT_PROCEDURE` —
   preferential tariff, MFN, special-regime imports, inward
   processing. Our `observations` row sums across all procedures;
   the per-procedure detail lives in `eurostat_raw_rows`. A surge
   in inward-processing imports may indicate a re-export pattern
   that the totals would obscure. (See §3 caveat
   `eurostat_stat_procedure_mix`.)

6. **Trade-definition differences.** GACC and Eurostat differ on
   what counts as "trade" — bonded zones, transit goods,
   processing trade are treated differently. Effects vary by HS
   chapter. (See §3 caveat `general_vs_special_trade`.)

7. **CN8 nomenclature revisions.** Eurostat revises CN8 annually
   each January. A 24-month YoY window spanning a year boundary
   may capture a subtly different commodity scope pre- and post-
   revision. Most revisions are minor; the `cn8_revision` caveat
   fires automatically when relevant. (See §3.)

8. **FX-conversion timing.** Cross-source comparisons in EUR rest
   on a chosen day's FX rate. We use the ECB monthly-average for
   each period; differences from end-of-period or trade-weighted
   rates can be 1-3%. (See §3 caveat `currency_timing`.)

9. **Reporting lags + revision cycles.** GACC publishes ~10 days
   after period close; Eurostat lags ~6-8 weeks; HMRC publishes
   monthly. Comparing the same period across sources requires both
   to have reported. Revisions also happen on different schedules.

10. **Confidentiality suppression.** Eurostat aggregates flows that
    would identify a single trader into HS-X-suffix residual codes;
    HMRC carries its own `SuppressionIndex`. Per-reporter CN8
    detail is typically 0-5% lower than the corresponding `'000TOTAL'`
    row — that gap is the suppression rate.

The net effect: a "mirror gap" of, say, +65% for NL is mostly the
transshipment effect (≈65%) plus the CIF/FOB baseline (≈6.5%)
minus some reporting noise; the *change* in the gap, when the gap
itself is normally stable for a partner, is what `mirror_gap_zscore`
flags.

## 1. Anomaly subkinds catalogue

Every finding lives in the `findings` table with `kind` (always
`anomaly` or `llm_topline`) and `subkind` (the specific
classification). All subkinds carry `detail` JSONB with the values
the findings document renders.

### `mirror_gap` (mirror-trade)

For a (period, country) pair where both GACC and Eurostat have
data, computes `gap_pct = (eurostat_eur - gacc_eur) / max(...)`.
Stores `excess_over_baseline_pct = |gap_pct| - cif_fob_baseline_pct`.

What's in `detail`: GACC value (raw + EUR-converted), Eurostat
total + how many CN8 codes contributed, FX rate used, transshipment
flag if applicable, the CIF/FOB baseline that was applied (and its
source). Where in the findings document: "Mirror-trade gaps" section.

### `mirror_gap_zscore` (mirror-gap-trends)

For each partner with a multi-period mirror_gap series, computes
z-score against the rolling baseline. Emits when |z| ≥ 1.5
(default). Carries `low_baseline_n` caveat if fewer than 6 prior
periods.

What's in `detail`: z-score, rolling mean + stdev, current gap_pct,
baseline composition. Where in the findings document: same "Mirror-trade
gaps" section, headed by z-score magnitude.

### `hs_group_yoy` / `hs_group_yoy_export` / `_uk` / `_uk_export` / `_combined` / `_combined_export`

For each HS group, computes the rolling 12-month total (current
window vs prior window) and the YoY %. The subkind suffix encodes
flow + scope:

- bare `hs_group_yoy` = imports, EU-27 (Eurostat)
- `_export` = exports
- `_uk` = UK (HMRC)
- `_uk_export` = UK exports
- `_combined` = EU-27 + UK summed (carries `cross_source_sum`)
- `_combined_export` = same, exports

What's in `detail`: window dates, current + prior 12mo EUR + KG
totals, yoy_pct, yoy_pct_kg, unit_price_pct_change, low_base flag,
kg_coverage_pct, top contributing CN8 codes, top contributing
reporter countries. Where in the findings document: per-scope "Top movers"
sections.

**Phase 6.10 — single-month + 2-month-cumulative YoY**: every
`hs_group_yoy*` finding under method `v10` also carries two
sub-blocks: `detail.totals.single_month` (latest month vs same
month a year earlier) and `detail.totals.two_month_cumulative`
(last two months vs corresponding pair a year earlier). Both
include current/prior eur+kg and yoy_pct/yoy_pct_kg; either
yoy_pct is None when the underlying period is missing (we don't
impute). Editorial register: Soapbox / Lisa routinely quote single-
period operators ("Feb 2026 vs Feb 2025") rather than 12mo rolling,
so the Tier 2 render shows BOTH the 12mo and the latest-month YoY
inline. Method version bump propagates supersedes as usual; older
v9 findings still render — the brief just omits the "Latest month"
suffix where the field isn't populated.

### `hs_group_trajectory` (+ same suffixes as yoy)

Reads `hs_group_yoy*` findings as a time series (one yoy% per
anchor period) and classifies the shape into one of 12 vocabulary
entries (see §4). Uses Theil-Sen slope (not OLS) to be robust to
endpoint outliers; tolerates gaps via longest-contiguous-run.

What's in `detail`: shape + shape_label, last/max/min YoY,
n_windows, smoothing_window, seasonal_signal_strength, the
effective_first/last_period actually used. Where in the findings
document: per-scope "Trajectories" sections, grouped by shape.

### `narrative_hs_group` (LLM lead-scaffold)

Per HS group with active findings, the LLM produces a structured
JSON: anomaly_summary (one sentence), 2-3 hypotheses picked from
the curated catalog (each with a one-line rationale), and
deterministic corroboration steps (looked up from the catalog).

What's in `detail`: lead_scaffold (the structured payload),
underlying_finding_ids, model used, full prompt facts. Where:
in the **companion leads document** (`leads-<timestamp>.md`), NOT
in the findings document itself. It is deterministic-only so a
downstream LLM tool (NotebookLM, etc.) is reasoning over the raw
findings, not over another LLM's interpretation of them.

### `gacc_aggregate_yoy` / `gacc_aggregate_yoy_import`

Year-on-year movement on GACC's own partner-aggregate trade totals
(not HS-group-level). Covers ASEAN, RCEP, Belt & Road, Africa, Latin
America, world Total. The EU bloc and single-country partners are
covered by the bilateral analyser below — this one's scope is the
multi-country non-EU aggregates. See `anomalies.py:GACC_AGGREGATE_KINDS`.
Findings have the aggregate label under `detail.aggregate.raw_label`
and the bucket under `detail.aggregate.kind` (NOT `detail.group.name`
like hs_group_yoy).

**Natural-key fix 2026-05-11**: the key is `(alias_id,
aggregate_kind, current_end_yyyymm)`. Before the alias_id was added,
Africa and Latin America (both kind=`region`) collided and silently
overwrote each other on every analyser run — Africa was completely
invisible in active findings. Method bumped
`v2_loose_partial_window` → `v3_per_alias_natural_key`.

**YTD + single-month extension 2026-05-12**: each finding now carries
`ytd_cumulative` and `single_month` sub-fields in `detail.totals`
alongside the 12mo rolling — mirrors the Phase 6.10 design on
`hs_group_yoy` and the design on `gacc_bilateral_aggregate_yoy`. Method
bumped `v3_per_alias_natural_key` →
`v4_ytd_and_single_month_operators`. The brief's partner-aggregate
block surfaces all three operators at once so Soapbox-style "China-X
Jan-N trade +Y%" or single-month claims have direct finding citations.

Where in the findings document: Tier 2 has a dedicated per-aggregate
state-of-play block alongside the per-HS-group blocks (Phase 6.10).

### `partner_share` / `partner_share_export`

China's share of EU-27 *extra-EU* imports/exports per HS group, by
value AND by quantity_kg. Added 2026-05-12 after the Soapbox A1 re-
test confirmed the metric gap: every claim of the form "China
supplied X% of EU imports of Y" was unverifiable from our DB
because `eurostat.py` filters at ingest to partner ∈ {CN, HK, MO}
— we had the numerator but no denominator.

**Numerator**: SUM(value_eur), SUM(quantity_kg) from
`eurostat_raw_rows` for partner ∈ {CN, HK, MO}, EU-27 reporters
only, 12mo rolling window.

**Denominator**: SUM(value_eur), SUM(quantity_kg) from
`eurostat_world_aggregates` — pre-summed at ingest across all
Eurostat partner codes EXCEPT the 27 EU-27 ISO-2 codes (so the
denominator is "imports from outside the EU"). Same reporter
scope, same window.

Why extra-EU and not all-partner: intra-EU trade dwarfs extra-EU
trade for most HS chapters (German imports from the Netherlands
exceed German imports from China by an order of magnitude for
many products). Including intra-EU in the denominator would
conflate "share of EU consumption" (a very different metric)
with "share of non-EU imports" (the editorial point). Soapbox
always means the latter; this analyser matches that convention.

**Why the bulk-file aggregation route**: Eurostat's Comext bulk
files contain only per-country (ISO-2) rows — no pre-computed
`EXTRA_EU27` or `WORLD` aggregate codes. The share denominator
therefore has to be computed by summing across partner codes at
ingest. The work runs in a single streaming pass through the
bulk file (~11s per period); the resulting table is filtered to
just the HS prefixes our hs_groups care about so storage stays
bounded.

**Findings** carry `share_value`, `share_kg`, and
`qty_minus_value_pp` (qty share minus value share, in percentage
points). The gap is the Soapbox-signature "bigger in tonnes than
in euros" signal: positive gap = unit-price pressure (China is
shipping more cheaply per kg than the rest-of-world average);
negative gap = premium pricing.

**Coverage limitation**: EU-27 scope only — there is no HMRC-side
world-aggregate equivalent yet.

### `gacc_bilateral_aggregate_yoy` / `gacc_bilateral_aggregate_yoy_import`

Bilateral counterpart to `gacc_aggregate_yoy`. Same GACC-side data,
but covers the EU bloc + every single-country GACC partner (those
that `gacc_aggregate_yoy` deliberately omits). Added 2026-05-12
after the Soapbox A1 re-test confirmed the gap: Soapbox's lead claim
("China's exports to the EU reached US\$201bn ... +19% YoY in Jan-Apr
2026") was sitting in our `observations` table as a `period_kind='ytd'`
row but no analyser promoted it to a finding.

**Each finding carries three YoY operators side-by-side** in
`detail.totals`:

- `yoy_pct` (with `current_12mo_eur` / `prior_12mo_eur`) — 12mo
  rolling. Same operator as `gacc_aggregate_yoy`. Drives `score`
  and the supersede-chain trigger.
- `ytd_cumulative.yoy_pct` — Jan-to-anchor of current year vs same
  range prior year. The Soapbox A1 register: "Jan-Apr exports +19%".
  Null when the prior-year YTD observation is absent (e.g. anchors
  in early 2025 where the equivalent prior-year month is missing
  due to GACC's Jan-Feb-combined Chinese New Year format).
- `single_month.yoy_pct` — anchor month vs same month prior year.
  The Soapbox A3 register: "EU exports to China Feb 2026 -16.2%".
  Null when prior month is missing.

Sharing one finding keeps the supersede chain coherent — when
underlying data revises, all three operators move together. The
brief renders all three in `state_of_play_bilaterals.py`; a
journalist picks whichever cadence matches the story.

**Natural key**: `(alias_id, current_end_yyyymm)`. Flow direction
encoded in subkind. Method version
`gacc_bilateral_aggregate_yoy_v1_eu_and_single_countries`.

Where in the findings document: Tier 2 has a dedicated per-bilateral
state-of-play block, between the per-HS-group view (narrower) and
the non-EU aggregate view (wider). Reader narrows scope rather than
widens.

## 2. The three comparison scopes

Each hs-group analyser supports `--comparison-scope`:

| Scope | Source | What it answers | When to use |
|---|---|---|---|
| `eu_27` (default) | Eurostat, partners CN+HK+MO | EU-27 trade with China (excluding UK at all times) | Headline EU-side stories |
| `uk` | HMRC, partner CN | UK-only post-Brexit trade with China | Guardian-direct UK angles |
| `eu_27_plus_uk` | both, summed in EUR | "British Isles" envelope | Cross-source sum; carries `cross_source_sum` caveat warning that the two sources differ in methodology |

The three scopes render as three distinct sections in the findings document.
For most stories, pick one scope and stay with it; the combined
view is for headline framing only.

`eu_27` excludes UK reporter rows from Eurostat at all times
(pre-Brexit GB rows in 2017–2020 are dropped) so the EU-27 series
is consistent through the Brexit transition. UK-CN trade is then
captured separately under the `uk` scope from HMRC.

## 3. Caveats reference

Caveat codes split two ways. *Family-universal* codes fire on every
finding of a given analyser family — they are not attached to
individual findings; they are documented once in the brief's
Methodology footer, sourced from
`anomalies.UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY`. *Per-finding-variable*
codes ride on each finding's `caveat_codes` array and surface inline.
Canonical text for each code lives in the `caveats` schema table.

### Family-universal (rendered once in Methodology footer)

These fire on every active finding within the relevant subkind family.
They reflect inherent methodology, not anything unusual about a
specific finding.

| Code | Applies to | What it means |
|---|---|---|
| `cif_fob` | mirror_gap, mirror_gap_zscore, hs_group_yoy*, hs_group_trajectory* | CIF (imports) vs FOB (exports) baseline gap |
| `classification_drift` | hs_group_yoy*, hs_group_trajectory* | CHS8 vs CN8 divergence at HS-8 |
| `cn8_revision` | hs_group_yoy*, hs_group_trajectory* | 24-month window always spans Eurostat's annual CN8 revision |
| `currency_timing` | mirror_gap*, hs_group_yoy*, hs_group_trajectory* | FX rate sensitive to which day's used |
| `eurostat_stat_procedure_mix` | mirror_gap*, hs_group_yoy*, hs_group_trajectory* | Sum across tariff regimes hides the mix |
| `multi_partner_sum` | all Eurostat-side | EU side sums across CN+HK+MO partners |
| `general_vs_special_trade` | mirror_gap* | Different definitions of "trade" |
| `transshipment` | mirror_gap* | Generic transshipment caveat (not the per-hub one below) |
| `aggregate_composition_drift` | mirror_gap_zscore | Z-score baseline's underlying composition may shift |
| `llm_drafted` | narrative_hs_group | Editorial framing produced by LLM (with verification) |

### Per-finding-variable (rendered inline; signal something specific)

| Code | Applies to | What it means |
|---|---|---|
| `partial_window` | hs_group_yoy*, gacc_aggregate_yoy | 1 missing month in either current or prior 12mo window |
| `low_base_effect` | hs_group_yoy*, hs_group_trajectory* | Smaller of current/prior 12mo EUR is below €50M threshold; percentage rests on small base |
| `low_baseline_n` | mirror_gap_zscore | Z-score baseline has fewer than 6 prior periods; stdev estimate is noisy |
| `low_kg_coverage` | hs_group_yoy* | Less than 80% of value_eur is backed by an actual quantity_kg measurement; unit-price decomposition suppressed |
| `transshipment_hub` | mirror_gap | Partner is in `transshipment_hubs` table (NL, BE, HK, SG, AE, MX) |
| `aggregate_composition` | mirror_gap | Comparison is aggregate-to-aggregate (e.g. EU bloc); member definitions may differ across sources |
| `cross_source_sum` | hs_group_yoy_combined* | Combined-scope finding sums Eurostat (EUR-native) + HMRC (GBP→EUR via period FX) |

### CIF/FOB baselines: per-country detail

`cif_fob_baselines` ships 28 per-(EU member state + UK) values from
the OECD ITIC dataset (2022, all commodities, China as origin), plus
one global default (7.5%, UNCTAD/WTO derived) for any partner
without a specific row. Range across EU-27+GB: 3.15% (SK) to 7.79%
(BG); unweighted mean 6.65%. Northwest-European core (DE, NL, FR,
IT, BE) clusters around 6.5–7.2%.

To refresh in a future year, the OECD ITIC SDMX endpoint at
`sdmx.oecd.org/sti-public/rest/data/OECD.SDD.TPS,DSD_ITIC@DF_ITIC,1.1/`
supports the same query shape. See
[`dev_notes/cif-fob-baselines-2026-05-10.md`](../dev_notes/cif-fob-baselines-2026-05-10.md)
for the reproducibility notes.

## 4. Trajectory shape vocabulary

The hs_group_trajectory analyser classifies each 24-month YoY series
into one of 12 shapes. Editorial framing matters: some shapes are
narrative-rich (a comeback, a peak-and-fall); others are flat or
volatile.

| Shape | Editorial label | What it looks like |
|---|---|---|
| `rising` | Rising | Sustained positive YoY |
| `rising_accelerating` | Rising, accelerating | Positive YoY with a positive slope (gaining speed) |
| `rising_decelerating` | Rising, decelerating | Positive YoY but slope flattening |
| `falling` | Falling | Sustained negative YoY |
| `falling_accelerating` | Falling, accelerating | Negative YoY getting more negative |
| `falling_decelerating` | Falling, decelerating | Negative YoY but slope flattening |
| `inverse_u_peak` | Peak-and-fall (was rising, now falling) | Classic policy-bite shape |
| `u_recovery` | Trough-and-recovery (was falling, now rising) | Post-crisis bounce |
| `dip_recovery` | Dip-and-recovery (was rising, dipped, now rising again) | Brief disruption then resumption |
| `failed_recovery` | Failed recovery (was falling, briefly rose, now falling again) | Dead-cat bounce |
| `flat` | Flat / stable | No meaningful directional movement |
| `volatile` | Volatile (multiple direction changes) | Multiple inflections; story is the noise itself |

Seasonality is handled as a feature, not a shape: any series with
strong lag-12 autocorrelation gets `has_strong_seasonal_signal=true`
and a 📅 annotation in the body, regardless of the shape that gets
classified.

## 5. The hypothesis catalog

The LLM lead-scaffold layer (Phase 6.4) does NOT draft prose. It
picks 2-3 hypothesis ids from a curated catalog
([`hypothesis_catalog.py`](../hypothesis_catalog.py)) and writes a
one-line rationale per pick. Corroboration steps come
deterministically from the catalog (the LLM doesn't invent them).

Twelve hypotheses currently in the catalog, each with a description
(visible to the LLM in the prompt) and a list of corroboration
steps (attached to the lead post-pick):

`tariff_preloading`, `capacity_expansion_china`, `eu_demand_pull`,
`transshipment_reroute`, `russia_substitution`, `currency_effect`,
`friend_shoring_decline`, `trade_defence_outcome`,
`cn8_reclassification`, `base_effect`, `energy_transition`,
`post_pandemic_normalisation`.

To add a hypothesis: append to `CAUSAL_HYPOTHESES` with a unique
snake_case id, a label, a description, and a list of corroboration
steps. The next LLM run can pick it. Removing entries is similarly
trivial — the LLM's vocabulary is exactly the catalog.

## 6. Numeric verification rules

Before a `narrative_hs_group` finding is persisted, every number
in the LLM's output (anomaly_summary OR any rationale) must
round-trip to a fact within tolerance. Tolerances:

| Kind | Tolerance | Example |
|---|---|---|
| Percentage | ±0.5 pp absolute | fact +34.2% accepts "34%" or "34.2%" but not "35%" |
| Currency | ±5% relative | fact €26.9B accepts "€27B" |
| Count | ±0.5 absolute | exact integer match |

Sign inference: percentages get their sign from the prose's
movement verbs ("a 36.8% drop" parses as -36.8%) so a stock-vs-flow
hallucination ("rose 36%" when fact is -36%) gets caught.
Currencies are stocks and don't sign-flip.

Pre-extraction strips: calendar years (`19xx`, `20xx`), time
periods ("12 months", "24-month"), and HS-code references
("HS 2941", "HS 850760") — these are editorial scaffolding, not
facts to verify.

Rejection: if any number doesn't round-trip OR any picked hypothesis
id isn't in the catalog, the entire lead is rejected. Tally goes to
`skipped_unverified`. Editorial cost: silence on that group.
Editorial benefit: never confidently wrong.

## 7. Known fragility

Three known sensitivities, with pointers to the artefact reports
that quantify each:

### Threshold sensitivity (Phase 6.3)

See [`dev_notes/sensitivity-sweep-2026-05-10.md`](../dev_notes/sensitivity-sweep-2026-05-10.md)
for the full numbers.

- `low_base_threshold_eur` (default €50M) is **highly sensitive**:
  ~7,100 findings (49% of all hs_group_yoy*) flip classification
  across the €5M–€500M sweep range. The single largest methodology
  choice driving editorial framing.
- `kg_coverage_threshold` (default 0.80) is **insensitive** in
  production: 84% of findings sit at 0.90–1.00 coverage; only 7
  findings near the threshold.
- `z_threshold` (default 1.5) is **moderately sensitive**: 18 of 74
  mirror_gap_zscore findings sit within ±0.3 of the default. NL
  Rotterdam-transshipment z-scores cluster in the marginal band.

### YoY rolling-window stability (Phase 6.6)

See [`dev_notes/out-of-sample-backtest-2026-05-10.md`](../dev_notes/out-of-sample-backtest-2026-05-10.md).

Comparing each `hs_group_yoy*` finding at the latest period T
against the same (group, subkind) at T-6: 31% of YoY signals
sign-flip across 6 months; 43% shift by ≥5pp same-sign; only 26%
are persistent. Per-group predictability ranges 100% (broad chapter
groups like Electrical 84-85) to 0% (Telecoms / Pharma niche
groups, Industrial fasteners, EV+hybrid passenger cars,
Motor-vehicle parts, Semicon mfg eqpt).

### EU-27 absolute-total reconciliation

See history.md (resolved 2026-05-10).

If summing `eurostat_raw_rows.value_eur` directly, MUST filter
`product_nc != '000TOTAL'` — otherwise you double-count by ~2x.
HS-pattern LIKE filters in the analysers exclude `'000TOTAL'`
naturally. Tests in `tests/test_eurostat_scale_reconciliation.py`
guard against regression.

## 8. Transparency annotations in the findings document and spreadsheet

The Phase 6.3 sensitivity sweep and Phase 6.6 backtest produced
editorial signal that's surfaced *in the findings document and
spreadsheet themselves*, not just in dev_notes reports. Three
annotations to look for when scanning:

- **Per-group YoY-predictability badge** (🟢 / 🟡 / 🔴) next to
  each HS group heading in the findings document, and as a
  `predictability_badge` + `predictability_pct` column in the
  spreadsheet. 🟢 = ≥67% of recent (scope, flow) permutations
  stayed on-direction with shift <5pp at T-6 vs T (quote with
  confidence); 🟡 = noisy; 🔴 = volatile (lean on the trajectory
  shape, not the headline percentage). Empty if no T-6 history
  exists yet (fresh DB).
- **Threshold-fragility flag** (⚖️ Near low-base threshold in the
  Markdown, `near_low_base_threshold = TRUE` in the spreadsheet)
  for findings whose smaller-of-(curr, prior) sits within 1.5×
  the low_base threshold (above OR below). A finding at €48M
  (low_base) and one at €52M (not low_base) are equally fragile
  to a small threshold move; this annotation surfaces that without
  making editorial claims about which way the classification
  "should" go.
- **Per-finding CIF/FOB baseline display** in the mirror-gap
  section (Markdown: a "**CIF/FOB baseline**: 6.55% (per-partner);
  excess over baseline = +52.8 pp" line; spreadsheet:
  `cif_fob_baseline_pct`, `cif_fob_baseline_scope`,
  `excess_over_baseline_pp` columns). Surfaces the per-country
  OECD ITIC baseline plus the excess-over-baseline split, so a
  journalist can see what the structural CIF/FOB component is and
  what the residual editorial signal is.

The spreadsheet additionally exposes a `predictability_index` tab
listing every group's badge + persistence-rate, so a data
journalist can sort/filter on robustness directly.

## 9. What to quote vs hedge vs not quote

A practical rubric:

### Quote with confidence
- A YoY finding for a group with a 🟢 predictability badge and
  meaningful base (>€100M).
- A trajectory shape that's `inverse_u_peak`, `u_recovery`, or
  `dip_recovery` — these are narrative-rich and reflect real
  inflections.
- A mirror_gap_zscore finding with |z| ≥ 2.5.
- An LLM-scaffolded lead's anomaly summary (it's been numerically
  verified).

### Quote with hedging
- Any group with a 🟡 (noisy) predictability badge — quote the
  trajectory shape rather than the headline %.
- Any finding carrying `low_base_effect`, `low_baseline_n`,
  `low_kg_coverage`, or `partial_window`. Mention the caveat in
  the copy.
- Any finding flagged ⚖️ near low-base threshold (or
  `near_low_base_threshold = TRUE` in the spreadsheet) — the
  classification is fragile to small methodology choices.
- Any mirror_gap finding for a transshipment-hub partner — the
  gap mostly reflects routing, not direct trade.
- A YoY % from a niche group with low Phase 6.6 predictability —
  rely on the trajectory shape rather than the headline percentage.
- A `_combined` (EU-27 + UK) figure — it sums two methodologically
  distinct sources.

### Don't quote without further investigation
- Any group with a 🔴 (volatile) predictability badge —
  the headline percentage hasn't held over 6 months; rely on the
  trajectory shape or skip the group.
- A YoY % over 100% on a low-base group — almost always a base-effect
  artefact, not a real surge.
- An LLM-scaffolded hypothesis as the cause — the catalog is
  scaffolding for investigation, not a verdict. Use the
  corroboration steps.
- An absolute EU-27 total without filtering `'000TOTAL'` from the
  source query — see §7.

When in doubt, click through the trace token (`finding/12345`) to
the underlying detail. Every claim has a citation chain back to the
raw rows; quote only the parts that hold up under that walk.
