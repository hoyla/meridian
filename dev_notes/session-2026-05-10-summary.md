# Session summary — 2026-05-10

What shipped today, in order. Use as a quick orientation when picking
up where the work left off.

## Phase 6 closeouts (interactive, with user approval)

- **6.1** HMRC ingest + comparison_scope (a-f). Commits `9489970` →
  `0cb91bf`. Three-mode comparison scope (eu_27 / uk / eu_27_plus_uk)
  plumbed through hs-group analysers; briefing pack restructured for
  per-scope sections.
- **6.8** Brief versioning. Commit [`1267362`](https://github.com/hoyla/gacc/commit/1267362).
  New `brief_runs` table; "Changes since previous brief" section
  highlights material YoY shifts (>5pp) and direction flips (🔄).
- **6.4** Lead-scaffold restructure of LLM framing. Commit [`f624108`](https://github.com/hoyla/gacc/commit/f624108).
  New `hypothesis_catalog.py` with 12 standard causes; LLM picks 2-3
  from the catalog with one-line rationales; corroboration steps attached
  deterministically. Method bumped to `llm_topline_v2_lead_scaffold`.
- **Q&A bot forward-work doc.** Commit [`fb7cace`](https://github.com/hoyla/gacc/commit/fb7cace).
  Tier-1/Tier-2 scope, AWS architecture sketch, trigger conditions.
- **6.2** Universal-caveat suppression. Commit [`6765afa`](https://github.com/hoyla/gacc/commit/6765afa).
  11 universally-fired caveats now suppressed inline; new "Universal
  caveats" section reads canonical text from the `caveats` schema table.
  Surfaced + seeded two missing definitions.
- **6.7** GACC 2018 parser. Commit [`3f115b4`](https://github.com/hoyla/gacc/commit/3f115b4).
  Parser handles all 4 title-format quirks (alternative wording,
  trailing-period months, missing currency suffix, missing date).
  But: 2018 section-4 release pages embed PNG screenshots not HTML
  tables — body parse still fails. Forward-work doc updated.
- **6.5 prep** review doc. Commit [`1b3cdf8`](https://github.com/hoyla/gacc/commit/1b3cdf8).
  Per-group activity stats + recommendations for the 13 draft HS groups.
- **6.5 execution.** User approved promote/keep-draft/drop split:
  7 promoted (Critical minerals, Drones, PPE, Semicon mfg eqpt,
  Telecoms base stations, Cotton, Tomato paste); Lithium chemicals
  shrunk to lithium hydroxide only (HS 282520); Pharmaceutical APIs
  (broad) dropped + replaced by 3 narrower groups (HS 2924
  Paracetamol-class, HS 2916 Ibuprofen-class, HS 2941 Antibiotics);
  Plastic waste renamed "(post-National-Sword residual)"; 3 stay
  draft (Honey, Polysilicon, Tropical timber).
- **6.5 follow-through.** Commit [`f301342`](https://github.com/hoyla/gacc/commit/f301342).
  LLM verifier strips `HS NNNN` references (otherwise the new pharma
  groups whose names embed HS codes failed verification).

## Autonomous-work block (Bypass permissions, ~3 hours)

User went away with Bypass on; pre-commitments: no destructive prod
ops, no breaking-test pushes, write up editorial judgment calls as
docs rather than choosing.

- **Eurostat aggregate-scale 2x mystery.** ✓ RESOLVED. Commit
  [`50f8dbd`](https://github.com/hoyla/gacc/commit/50f8dbd). Cause: Eurostat's bulk file ships a
  `product_nc='000TOTAL'` aggregate row alongside per-CN8 detail.
  Naïve `SUM(value_eur)` doubles. CN8-only sum = €517.1B, matches
  Eurostat's published 2024 EU-27 imports headline exactly. **HS-
  group analysers were never affected** because their LIKE patterns
  (`'8507%'`) don't match `'000TOTAL'`. Added 3 reconciliation tests
  (live-DB-conditional). Forward-work doc updated to mark resolution
  + capture lessons.
- **6.3 Sensitivity sweep.** ✓ DONE. Commit [`85d6cf7`](https://github.com/hoyla/gacc/commit/85d6cf7). Pure-compute
  script + dev_notes report. Headline: low_base threshold is HIGHLY
  sensitive (49% of findings flip across €5M-€500M); kg_coverage is
  insensitive in production; z_threshold is moderately sensitive
  (18 of 74 findings within ±0.3 of default).
- **CIF/FOB UNCTAD/WTO research.** ✓ DONE. Commit [`4d4f7cc`](https://github.com/hoyla/gacc/commit/4d4f7cc). Backfilled
  28 per-(EU member state, China) CIF/FOB margins from OECD ITIC
  dataset 2022. Range 3.15% (SK) → 7.79% (BG); unweighted mean 6.65%;
  northwest-European core ~6.5-7.2%. Method bumped to
  `mirror_trade_v5_per_country_cif_fob_baselines`; 351 findings
  superseded with new baselines; value_signature now includes
  cif_fob_baseline_pct so future updates propagate cleanly.
- **6.6 Out-of-sample backtest.** ✓ DONE. Commit [`5d0e23e`](https://github.com/hoyla/gacc/commit/5d0e23e). Pure-compute
  script. Headline: 31% of YoY signals sign-flip across 6 months;
  only 26% are persistent. Per-group predictability ranges 100%
  (broad chapter groups) → 0% (Telecoms / Pharma niche). Crucial
  caveat captured prominently: this is NOT a clean OOS forecast
  (rolling windows share data); it IS a YoY-framing-stability test.
  Trajectory-shape backtest sketched but flagged forward work — needs
  periodic analyser runs (monthly cron) to be measurable.

## Tests

- Started session at 151 passing.
- Ended at **175 passing** (+ 3 live-DB-conditional skipped without
  GACC_LIVE_DATABASE_URL).
- New test files: `tests/test_eurostat_scale_reconciliation.py`
  (3 tests, opt-in to live DB).

## Code state

Live `gacc` DB:
- Eurostat: 2017-01 → 2026-02 (110 periods, partners CN+HK+MO).
- HMRC: 2017-01 → 2026-02 (110 periods, partner CN — main partner).
- GACC: 2019-01 → 2026-04 (149 releases; 2018 unreachable due to
  PNG-only release pages — see forward-work-gacc-2018-parser.md).
- 31 hs_groups (was 32, 3 new pharma + 1 renamed lithium replaced 1
  broad pharma).
- ~14,558 active hs_group_yoy* findings; 351 mirror_gap; 74
  mirror_gap_zscore; ~179 hs_group_trajectory*; 30 narrative_hs_group.
- Latest brief: `exports/briefing-20260510-163605.md`.

## What's still open

In rough priority order if/when you pick up:

1. **Periodic analyser runs.** A monthly cron (or GitHub Action) that
   re-runs the analyser pipeline after each Eurostat / HMRC release
   would turn the supersede chain into a real historical record —
   then the trajectory-shape backtest becomes measurable, the brief
   versioning catches actual data revisions (not method-bumps), and
   journalists get genuine "what changed since last month" framing.
2. **Per-(country, commodity) CIF/FOB granularity.** If a story
   emerges that rests on a specific HS bracket's CIF/FOB sensitivity,
   the OECD ITIC SDMX endpoint supports it directly.
3. **Sector breadth review (round 2).** The 6.5 promote/drop pass
   is shipped; a year from now a similar pass should re-evaluate
   what's editorially live.
4. **Q&A bot (Phase 7+).** Forward-work doc captured what would
   trigger picking it up.
5. **Eurostat aggregate-scale: editorial framing.** The 2x mystery
   is resolved at the methodology level; if a brief ever quotes
   absolute EU-27 totals as a headline, double-check the underlying
   query filters out '000TOTAL'. The new tests guard against it.
