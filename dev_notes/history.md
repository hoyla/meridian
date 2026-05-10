# History — addressed items

A single chronological record of significant design decisions and
problem resolutions. New entries go at the top. Each entry: what
the issue was, what shipped, where to find the work in git.

The companion `roadmap.md` lists only what's still open. This file
exists so you don't need to read through closed forward-work docs
to understand how the project got here.

---

## 2026-05-10 (evening) — output-shape refactor and transparency annotations

A focused session on what the journalist actually opens. No new
analysers; no methodology change; the data layer is unchanged. The
work re-shapes the output bundle and surfaces methodology safeguards
where readers can see them.

### Transparency annotations in the findings document and spreadsheet

Three editorial signals that previously sat only in dev_notes
reports now appear inline next to the findings they qualify:

- **Per-group YoY-predictability badge** (🟢 / 🟡 / 🔴) next to
  each HS group heading, computed via the same logic as the Phase
  6.6 backtest (T vs T-6 across all (scope, flow) permutations).
  ≥67% persistent → 🟢; 33–67% → 🟡; <33% → 🔴. Includes a one-line
  rationale for 🔴 ("Lean on trajectory shape; hedge any % quoted
  from this group").
- **Threshold-fragility annotation** (⚖️) for findings whose
  smaller-of-(curr, prior) sits within 1.5× the low_base threshold,
  above OR below. A finding at €48M (low_base) and one at €52M
  (not low_base) are equally fragile to a small threshold move; the
  annotation surfaces that without making editorial claims.
- **Per-finding CIF/FOB baseline display** in the mirror-gap
  section: the per-(partner) OECD ITIC baseline plus the
  excess-over-baseline-pp split. Was sitting in `detail` since the
  ITIC backfill but not surfaced.

Commit [`314962f`](https://github.com/hoyla/gacc/commit/314962f).
Helpers `is_threshold_fragile()` and `_compute_predictability_per_group()`
shared between briefing_pack and sheets_export so both render
paths use the same definition.

### LLM leads split out of the findings document

The "no LLM in the loop" framing on the original brief had become
inaccurate once the Phase 6.4 lead-scaffold layer landed: leads
were rendering inside the brief alongside deterministic findings.
For a NotebookLM-style downstream LLM tool that mixed bundle
created a telephone-game effect (the tool ends up reasoning over
another LLM's interpretation, not over the data).

Split into two paired files: brief stays fully deterministic; LLM
lead scaffolds move into a separate companion document. Both share
the same finding IDs; cross-references explicit.

Commit [`acb8697`](https://github.com/hoyla/gacc/commit/acb8697).
Diff section ("Changes since previous brief" → "Changes since
previous export") now also excludes `narrative_hs_group` since
those don't appear in the brief.

### Per-export folder convention + scope label

Replaced timestamped flat files (`brief-YYYYMMDD-HHMMSS.md`) with
per-export folders containing stable filenames:

```
exports/2026-05-10-1747[-slug]/
  findings.md
  leads.md
  data.xlsx
```

Pairs are self-evident from the folder; consumers find the bundle
by convention. Optional `scope_label` parameter (default None)
slugifies into the folder suffix and surfaces in both docs' headers
as a "*Scope: …*" line, so a doc shared standalone still announces
what slice of the data it covers. Currently metadata only — the
filtering logic is forward work; the naming convention is in place
so scoped exports can land cleanly when needed.

Commit [`4c3da25`](https://github.com/hoyla/gacc/commit/4c3da25). New
CLI flags: `--export-dir PATH` and `--export-scope LABEL`.

### Spreadsheet refresh — three-artefact bundle

The spreadsheet had drifted on multiple axes (UK / combined scopes
absent, no per-country CIF/FOB column, no predictability badge, no
threshold-fragility flag). Refreshed all eight tabs to match the
current methodology and added a NEW `predictability_index` tab.
`briefing_pack.export()` now drops `data.xlsx` into the per-export
folder by default so all three artefacts share a single DB
snapshot.

Commit [`c1ed375`](https://github.com/hoyla/gacc/commit/c1ed375).
Tab roster (8): summary, hs_yoy_imports, hs_yoy_exports,
trajectories, mirror_gaps, mirror_gap_movers, low_base_review,
predictability_index. The narrative_hs_group findings are
intentionally NOT in any tab (same telephone-game argument as the
findings document).

### Endnote on `finding/N` citations

Both docs now end with the same shared endnote explaining what
`finding/N` citations mean — what a finding is, the supersede chain
(so a citation is reproducible even after numbers later move), how
to look one up today (direct DB query), and pointers to
`docs/methodology.md` + `docs/architecture.md` for deeper context.

Commit [`4c3da25`](https://github.com/hoyla/gacc/commit/4c3da25).

### "In this export folder" block

Replaced the prose cross-references between findings.md and
leads.md with a structured block in each, listing all three
artefacts (with the current one marked "(this document)"). The
spreadsheet is now visible from the Markdown side too.

Commit [`49e9c64`](https://github.com/hoyla/gacc/commit/49e9c64).

### `brief.md` → `findings.md` rename

The output filename was misleading: the file is comprehensive, not
brief. Renamed to `findings.md` to match what's actually in it (a
render of the `findings` table) and to pair cleanly with
`data.xlsx`. H1 changed from "GACC × Eurostat trade briefing" to
"GACC × Eurostat trade findings". Module name `briefing_pack.py`
and CLI flag `--briefing-pack` kept (the *bundle* is still a
briefing pack — the rename is just the deterministic document
inside it).

Commit [`73a7f71`](https://github.com/hoyla/gacc/commit/73a7f71).

---

## 2026-05-10 — Phase 6 closeouts and autonomous methodology block

A long working session that closed Phase 6 except for the
infrastructure-track item (periodic analyser runs).

### 6.1 — HMRC ingest + comparison_scope abstraction

UK trade data was structurally missing from the brief because
Eurostat dropped UK reporting after Brexit. Shipped HMRC OTS
ingest via OData REST API at `https://api.uktradeinfo.com`, plus a
`comparison_scope` parameter (eu_27 / uk / eu_27_plus_uk) on the
hs-group analysers. Briefing pack restructured for per-scope
sections. 3.9M HMRC raw rows backfilled 2017–2026; UK numbers
cross-checked against HMRC published headlines.

Commits `9489970` → `0cb91bf`. (Closes the original
`forward-work-uk-data-gap.md`.)

### 6.2 — Universal-caveat suppression in the brief

Eleven caveats fired on essentially every active finding
(`cif_fob`, `classification_drift`, `cn8_revision`,
`currency_timing`, `eurostat_stat_procedure_mix`, `multi_partner_sum`,
`general_vs_special_trade`, `transshipment`, `cross_source_sum`,
`aggregate_composition_drift`, `llm_drafted`). They cluttered
per-finding caveat lists and obscured the *unusual* caveats
(`partial_window`, `low_base_effect`, `low_baseline_n`,
`low_kg_coverage`, `transshipment_hub`). Now suppressed inline and
explained once in a top-of-brief "Universal caveats" section that
reads canonical text from the `caveats` schema table.

Side-effect: surfaced two missing schema definitions
(`aggregate_composition_drift`, `cross_source_sum`) which had been
emitted by analysers but never had `caveats` table entries; both
seeded.

Commit [`6765afa`](https://github.com/hoyla/gacc/commit/6765afa).

### 6.3 — Methodology sensitivity sweep

Pure-compute pass over active findings that replays
classifications under variant thresholds. Three findings:

- **`low_base_threshold_eur` (default €50M)** is HIGHLY sensitive:
  ~7,100 findings (49%) flip classification across the €5M–€500M
  range. The single largest methodology-choice driver of editorial
  framing.
- **`kg_coverage_threshold` (default 0.80)** is INSENSITIVE in
  production: 84% of findings sit at 0.90–1.00 coverage, 15.7% at
  exactly 1.0; only 7 findings in the 0.80–0.90 band. The gate is
  defensive against a failure mode that doesn't currently trigger.
- **`z_threshold` (default 1.5)** is MODERATELY sensitive: 18 of
  74 mirror_gap_zscore findings sit within ±0.3 of the default.

Script: `scripts/sensitivity_sweep.py`.
Report: `dev_notes/sensitivity-sweep-2026-05-10.md` (kept as
dated artefact). Commit [`85d6cf7`](https://github.com/hoyla/gacc/commit/85d6cf7).

### 6.4 — Lead-scaffold restructure of LLM framing

Replaced the v1 narrative-drafting prompt ("write a 2-3 sentence
top-line") with a structured lead-scaffolding shape: per HS group
the LLM produces (a) one-line anomaly summary, (b) 2-3 hypothesis
ids picked from a curated catalog with one-line rationales, (c)
deterministic corroboration steps drawn from the picked catalog
entries.

The catalog (`hypothesis_catalog.py`) seeds 12 standard causes for
China-EU/UK trade movements — tariff_preloading,
capacity_expansion_china, eu_demand_pull, transshipment_reroute,
russia_substitution, currency_effect, friend_shoring_decline,
trade_defence_outcome, cn8_reclassification, base_effect,
energy_transition, post_pandemic_normalisation. Verifier
discipline carries through unchanged. Method: `llm_topline_v2_lead_scaffold`.

Follow-up [`f301342`](https://github.com/hoyla/gacc/commit/f301342) adds an HS-code regex strip
to the verifier so groups whose names embed HS codes (e.g.
"Antibiotics (HS 2941)") don't trigger false-positive failures
when the LLM cites the code in a rationale.

Commits [`f624108`](https://github.com/hoyla/gacc/commit/f624108) + [`f301342`](https://github.com/hoyla/gacc/commit/f301342).

### 6.5 — Sector breadth review

Thirteen draft HS groups were proposed in Phase 5; a review pass
classified each as promote / keep-draft / drop-or-rework. After
user approval:

- **Promoted (7)**: Critical minerals, Drones, PPE, Semicon mfg
  eqpt, Telecoms base stations, Cotton, Tomato paste.
- **Stayed draft (3)**: Honey, Polysilicon, Tropical timber.
- **Lithium chemicals → Lithium hydroxide (battery-grade)**:
  scope shrunk to HS 282520 only (the cell-grade chemical with
  the cleaner EV-supply-chain story).
- **Pharmaceutical APIs (broad) dropped + replaced** by three
  narrower groups: Paracetamol-class amides (HS 2924),
  Ibuprofen-class monocarboxylic acids (HS 2916), Antibiotics (HS
  2941). The broad group's +215.6% YoY at €8.11B base was
  unmistakably an artefact of HS 2942 being a catch-all that
  includes non-APIs.
- **Plastic waste renamed** "Plastic waste (post-National-Sword
  residual)" so the historical-only intent is explicit.

Commits [`1b3cdf8`](https://github.com/hoyla/gacc/commit/1b3cdf8) (proposal) + the user-approved
group revisions in the live DB.

### 6.6 — Out-of-sample backtest of YoY signal stability

Pure-compute script that compares each `hs_group_yoy*` finding at
T (2026-02) against the same (group, subkind) at T-6 (2025-08).
Headlines: 31% of YoY signals sign-flip across 6 months; 43%
shift by ≥5pp same-sign; only 26% are persistent. Per-group
predictability ranges 100% (broad chapter groups like Electrical
84-85) to 0% (Telecoms / Pharma niche groups, Industrial
fasteners, etc.).

Crucial caveat captured prominently in the report: this is NOT a
clean out-of-sample forecast test (rolling windows share data);
it IS a YoY-framing-stability test. The result is that
YoY-on-rolling-windows is genuinely less stable than it looks —
groups with low persistence should rely on the trajectory shape,
not the headline percentage.

Trajectory-shape backtest sketched but flagged as forward work:
all current findings have `created_at = today` from the Phase 5
clean-state rebuild, so the supersede chain isn't a historical
record yet. Becomes measurable once analyser runs are scheduled
periodically — see `roadmap.md`.

Script: `scripts/out_of_sample_backtest.py`.
Report: `dev_notes/out-of-sample-backtest-2026-05-10.md` (kept as
dated artefact). Commit [`5d0e23e`](https://github.com/hoyla/gacc/commit/5d0e23e).

### 6.7 — GACC 2018 parser (partial)

Title parser fixed to handle four 2018-format quirks:
alternative wording ("by Major Country (Region)"), trailing
period after month abbreviation ("Jan." not "Jan"), missing
`(in CCY)` suffix, and missing date entirely. Fix plumbs
`expected_currency` and `expected_period` from the discovery side
through to the parser.

But: 2018 section-4 release pages embed PNG screenshots
(`<img src='Excel/4-RMB.png'>`) instead of HTML tables, so the
body parse still fails. The data is in pixels, not numbers.
Editorial cost is bounded (only 2018 mirror-trade is missing;
hs-group analyses use Eurostat which extends to 2017).

Forward work options (OCR, hunt for source xlsx, accept gap, lean
on Eurostat+HMRC) captured in
`dev_notes/forward-work-gacc-2018-parser.md` — kept open because
this is genuinely deferred, not closed.

Commit [`3f115b4`](https://github.com/hoyla/gacc/commit/3f115b4).

### 6.8 — Brief versioning ("Changes since previous brief")

New `brief_runs` table tracks brief generation timestamps. The
brief now opens with a "Changes since previous brief" section
listing findings with `created_at > prev_at` (new) or
`superseded_at > prev_at` (revised). Material YoY shifts (>5pp)
highlighted; direction flips (sign change) marked 🔄.

Foundation for the journalist workflow piece — they want to know
what's changed since they last looked, not re-read the whole
brief.

Commit [`1267362`](https://github.com/hoyla/gacc/commit/1267362).

### Eurostat aggregate-scale 2x mystery — RESOLVED

Original symptom: direct sums over `eurostat_raw_rows` for
sanity-checking ran ~2x Eurostat's published EU-27 totals (€998B
vs published ~€517B for 2024 imports from CN). Per-country
numbers were roughly right; the factor only inflated as we summed
across reporters.

Cause: Eurostat's bulk file ships, per (reporter, period, partner,
flow, stat_procedure), a `product_nc='000TOTAL'` aggregate row
that sums the per-CN8-detail rows for the same slice. Naïve
`SUM(value_eur)` includes both = ~2x. CN8-only sum across all
EU-27 reporters and all stat_procedures for 2024 = **€517.1B**,
matches Eurostat's published headline exactly.

**HS-group analysers were never affected** because they all apply
HS-pattern LIKE filters (`'8507%'`, `'85%'`, etc.) that don't
match `'000TOTAL'`. Editorial impact: zero. The "X-suffix" codes
(`'85XXXXXX'`, `'850610XX'`) in the bulk file are confidentiality
residuals, not aggregates — including them in HS-pattern LIKE-
matched sums is correct.

Code change: new `EUROSTAT_AGGREGATE_PRODUCT_NC` constant in
`anomalies.py` documenting the convention; new
`tests/test_eurostat_scale_reconciliation.py` (3 tests, opt-in to
live DB) guards against regression.

Commit [`50f8dbd`](https://github.com/hoyla/gacc/commit/50f8dbd).

### Per-country CIF/FOB baselines from OECD ITIC

Replaced the 7.5% global default in `cif_fob_baselines` with 28
per-(EU member state, China) values sourced from OECD's
International Transport and Insurance Costs of merchandise trade
(ITIC) dataset, 2022. Range: 3.15% (SK) → 7.79% (BG); unweighted
mean 6.65%. Northwest-European core (DE 6.50%, NL 6.55%, FR
7.22%, IT 7.00%, BE 7.01%) clusters around 6.5–7.2%. The 7.5%
global default is preserved as fallback for non-EU partners.

Method bumped: `mirror_trade_v4_multi_partner_default` →
`mirror_trade_v5_per_country_cif_fob_baselines`. The mirror_gap
value_signature now includes `cif_fob_baseline_pct` so future
baseline updates propagate via the supersede chain without
needing a method-version bump. 351 mirror_gap findings re-emitted.

Sourced reference kept at
`dev_notes/cif-fob-baselines-2026-05-10.md` for reproducibility.
Commit [`4d4f7cc`](https://github.com/hoyla/gacc/commit/4d4f7cc).

---

## 2026-05-09 — Phase 5: methodology audit + clean-state rebuild

Phase 5 was triggered by a strategic review of whether the tool
was genuinely surfacing newsworthy insights vs converging on a
self-consistent loop with confirmation bias. Six concerns
surfaced; this phase fully addressed three (HK/MO routing,
historical baseline depth, validation methodology) and partially
addressed two (sector breadth, threshold robustness).

### 5.1 — HK/MO partner inclusion

Eurostat reports goods routed via Hong Kong / Macau under
partner=HK / partner=MO rather than partner=CN (~15% of China's
exports to EU). New constant
`EUROSTAT_PARTNERS_DEFAULT = ('CN', 'HK', 'MO')` adopted by all
four analysers as the default. CLI override `--eurostat-partners CN`
available for the narrower direct-China view. The
`multi_partner_sum` caveat fires by default as honest annotation.

### 5.2 — `upsert_observations` partner-scoped fast path

The bulk-insert fast path keyed off "release_id has any rows" —
adding HK/MO observations to existing CN-only releases fell into
the per-row slow path, taking ~6 minutes per period instead of
~1 second. Scoped the freshness check by `partner_country` for a
~280x speedup on partner-additive ingest.

### 5.3 — GACC parser: historical title formats

GACC release titles in 2018 and earlier had divergences from the
2025/2026-tuned regex (no `(N)` prefix, "RMB" synonym for CNY,
"Only August" parenthetical). Fixed for the formats seen in the
2018 monthly summary releases.

### 5.4 — Historical Eurostat + GACC backfill

- **Eurostat**: 2017-01 → 2026-02 (110 periods, partners CN+HK+MO).
- **GACC**: walks 9 yearly indexes (preliminary.html for current
  year + preliminaryYYYY.html for 2018–2025); section 4 parses
  cleanly across all years after the parser fix. (2018 still
  blocked at the body level — see Phase 6.7 above.)

### 5.5 — Clean-state rebuild

Wiped the live DB, re-applied `schema.sql` from scratch, archived
`migrations/` (folded into `schema.sql`; preserved as
`migrations.archived-2026-05-09/` for the dev history), and
re-ingested everything with the new defaults.

### 5.6 — Pre-registered shock validation

Document `dev_notes/shock-validation-2026-05-09.md` —
pre-registered expectations for what the analysers should
surface across four known historical shocks (2018 Section 232
tariffs, Q1 2020 COVID lockdown, Feb 2022 Russia invasion → renewables
substitution, Oct 2023 EU EV anti-subsidy probe). Written **before**
running the analysers; Results sections filled in afterwards.
The discipline is the structural defence against the
confirmation-bias risk. **This document is kept** in dev_notes/
because the methodology has ongoing value.

### 6.0.5 — EU-27 means EU-27 at all times

Pre-Brexit UK reporter rows (2017–Q1 2020) were inflating EU-27
sums. Three SQL helpers in `anomalies.py` now filter
`reporter <> ALL(EU27_EXCLUDE_REPORTERS)` where
`EU27_EXCLUDE_REPORTERS = ('GB',)`. Method bumps:
`hs_group_yoy v7→v8_excludes_gb_reporter_pre_brexit`,
`hs_group_trajectory v5→v6_inherits_eu27_yoy`. 4596 supersedes
triggered; 1144 findings (25%) had YoY shifts > 5pp; **337 had
the YoY direction flip** ("growth" ↔ "decline"). Worst examples
in 2018-2019 aluminium and electrical machinery — old EU-28 sums
showed +25–30% growth, new EU-27 shows -20–30% decline. Commit
[`388be73`](https://github.com/hoyla/gacc/commit/388be73).

### 6.0.6 — ~10000x analyser speedup

A re-run of all hs-group-yoy findings was projected to take ~3
hours because each per-anchor query was doing a Parallel Seq Scan
on the 17.5M-row `eurostat_raw_rows` table. Two changes dropped
the planner's estimated cost from ~476725 to ~41:

1. New covering index `idx_eu_raw_analyser` on
   `(flow, partner, product_nc text_pattern_ops, period)` INCLUDE
   `(value_eur, quantity_kg, reporter)`.
2. New helper `_hs_pattern_or_clause(patterns)` rewrites
   `product_nc LIKE ANY(%s)` (which the planner refuses to push
   down through the text_pattern_ops btree) into separate ORed
   LIKEs that the planner happily turns into a BitmapOr.

Wall time dropped from ~40 minutes (mid-run, ~40%) to ~7 minutes
full chain. Commit [`70d7bc5`](https://github.com/hoyla/gacc/commit/70d7bc5).

### 6.0.7 — Trajectory tolerates gaps

Phase 1.7's all-or-nothing gap rejection was producing only
~5/58 expected trajectory findings on real data. Replaced with
longest-contiguous-run: find the longest unbroken sub-series and
classify on that. The chosen window is recorded in
`features.{effective_first_period, effective_last_period,
original_series_length, effective_series_length,
dropped_periods_due_to_gaps}`. The `TRAJECTORY_MIN_WINDOWS = 6`
safeguard still rejects too-short remnants. Coverage went from 5
to 57 trajectory findings. The EV trajectory now classifies as
`dip_recovery` with trough at 2024-08, exactly when EU duties bit
hardest. Commit [`13f5ea1`](https://github.com/hoyla/gacc/commit/13f5ea1).

---

## 2026-05-09 — Phases 1-3: roadmap delivery

Triggered by an analysis-assumptions review identifying eight
per-pass concerns and two cross-cutting issues across the four
anomaly passes (`mirror-trade`, `mirror-gap-trends`,
`hs-group-yoy`, `hs-group-trajectory`). The review document is
preserved as the planning record (now consolidated below); each
phase shipped its scope.

### Phase 1 — Rigour fixes (7 items)

1. **Idempotent findings with revision history** (cross-cut).
   Append-plus-supersede chain on findings, mirroring how
   observations are versioned. New columns: `superseded_at`,
   `superseded_by_finding_id`, `last_confirmed_at`,
   `natural_key_hash`, `value_signature`. Per-subkind natural keys
   in `findings_io.py`. Default queries filter
   `WHERE superseded_at IS NULL`.
2. **Unit-scale parse failure → hard skip** (mirror-trade). The
   fallback multiplier 1.0 with WARNING was risking 10⁴-off EUR
   values for unrecognised unit strings. Now treated as a skip
   with ERROR log.
3. **Theil-Sen slope replaces OLS** (hs-group-trajectory). Robust
   to endpoint outliers. Live impact: Solar PV cells & modules
   trajectory flipped from `falling` → `falling_decelerating` (the
   decline is slowing, not just continuing).
4. **`min_baseline_n=6` confidence threshold + `low_baseline_n`
   caveat** (mirror-gap-trends). Hard floor stays 3
   (mathematical minimum); confidence threshold 6 triggers the
   caveat. "Make the noise honest" rather than dropping early-
   period signal.
5. **kg-coverage metric → conditional decomposition**
   (hs-group-yoy). `kg_coverage_pct` computed; below 80% the
   volume/price decomposition is suppressed and a
   `low_kg_coverage` caveat fires.
6. **Configurable `--low-base-threshold`** (hs-group-yoy). CLI
   flag accepting EUR, default unchanged at €50M.
7. **Trajectory gap detection** (hs-group-trajectory). Before
   classifying, check the YoY series for period gaps (later
   refined in 6.0.7 to longest-contiguous-run rather than skip).

Commits `8f18e68` → `3dc4c72`. Tests 75 → 101.

### Phase 2 — Editorial framing (8 items)

1. **Transshipment-hub flag** (mirror-trade). New
   `transshipment_hubs` table seeded with NL, BE, HK, SG, AE, MX,
   each with a citable `evidence_url`. The mirror-trade analyser
   auto-attaches a `transshipment_hub` caveat when the partner is
   in the table.
2. **CIF/FOB baselines table — lighter version** (mirror-trade).
   New `cif_fob_baselines` table with global default 7.5% (later
   superseded per-country by the 2026-05-10 OECD ITIC backfill).
3. **Multi-partner Eurostat support** (cross-cut). Default
   unchanged at the time of Phase 2 (`['CN']`); fully promoted to
   default in Phase 5.1.
4. **Configurable trajectory smoothing** (hs-group-trajectory).
   `--smooth-window N` flag, default 3.
5. **Seasonality as a feature, not a shape**
   (hs-group-trajectory). `_autocorrelation_at_lag()` helper
   computes a detrended Pearson correlation; surfaces as
   `features.seasonal_signal_strength`.
6. **Staleness log line** (mirror-gap-trends). Before running,
   warns if the latest mirror_gap finding period is older than
   the latest available Eurostat or GACC release.
7. **Single-missing-month tolerance** (hs-group-yoy). Allow up to
   1 missing month across both 12mo windows; sum what's present;
   never interpolate. `partial_window` caveat attached when
   triggered.
8. **Blanket `cn8_revision` caveat for cross-year-boundary
   windows** (hs-group-yoy). Auto-applied to any YoY window
   spanning a calendar-year boundary.

Commits `c0aa48c` → `26b2c94`. Tests 101 → 114.

### Phase 3 — LLM framing layer v1

`llm_framing.py` v1: per-HS-group narratives with strict numeric
verification. Default backend Ollama, default model
`qwen3.6:latest`. Every number cited had to round-trip to a fact
within tolerance, or the narrative was rejected. v1 shipped 15/16
hs_groups producing verified narratives on first pass; one
hallucination (qwen3.6 cited "93%" for permanent magnets,
recalled from training data) correctly rejected.

**Subsequently restructured** into the lead-scaffold shape in
Phase 6.4 (above) — replaces narrative drafting with structured
hypothesis selection from a curated catalog.

Commit `e3766c7`. Tests 114 → 129.
