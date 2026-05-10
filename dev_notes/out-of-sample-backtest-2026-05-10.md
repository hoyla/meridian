# Out-of-sample backtest — 2026-05-10

Phase 6.6 of dev_notes/history.md. For each `hs_group_yoy*` finding active at the latest anchor period **T = 2026-02-01**, compare against the same (group, subkind) finding 6 months earlier at **T-6 = 2025-08-01**. Measures persistence vs mean-reversion in the analyser's output.


Editorial value: a finding whose T-6 YoY signal aged well (same direction, similar magnitude) is one a journalist can quote with confidence. A finding whose T-6 signal mean-reverted (sign flip, magnitude collapse) was either picking up noise OR the underlying flow has substantively changed. Either way the brief should hedge.


## Interpretive note: this is NOT a clean out-of-sample test

The hs_group_yoy comparison is a rolling 12-month window. At T = 2026-02-01 we compare the 12 months ending T against the 12 months ending T-12. At T-6 we compared the 12 months ending T-6 against the 12 months ending T-18. So the T and T-6 windows **share 6 months of underlying data** in their *current* leg, and 6 months in their *prior* leg (different 6 months). A genuine out-of-sample test would compare a model trained ONLY on data ≤ T-6 against the actual values in T-5, T-4, ..., T. We're instead comparing two adjacent rolling windows.


What this measure DOES capture:
- **YoY framing stability**: if a journalist quoted a +50%   YoY at T-6 and then quoted YoY again at T, would the story   still be 'rising'? Sign-flip rate of 31% is the answer   ('about a third of the time, no'). That's editorially   important — YoY-on-rolling-windows is less stable than it   looks because the window itself moves.
- **Group-level reliability**: groups whose multiple (scope,   flow) permutations all stayed persistent (e.g. broad chapter   groups) are giving robust signals; groups where every   permutation flipped (e.g. Telecoms, Pharma niche groups)   are noise-dominated at the YoY-level — the story has to   come from the trajectory, not the headline percentage.

What this measure DOES NOT capture:
- True forecast accuracy (would need a held-out test on the   T-5..T monthly raw data, with the analyser blind to it).
- Trajectory-shape persistence (a separate backtest could   ask 'did rising_accelerating at T-6 stay rising_accelerating   at T?').


## Top-line

- Paired findings examined: **171** (28 groups × 6 scope/flow combinations)
- **Persistent** (same sign + |shift| < 5.0pp): **45** (26%)
- **Sign flips** (yoy direction reversed): **53** (31%) — clear mean-reversion / inflection
- **Material magnitude shift** (|shift| ≥ 5.0pp, same sign): **73** (43%) — trend strengthened or weakened
- **Low-base flip OUT** (T-6 was low-base, T is not): **1** — the base grew through the window
- **Low-base flip IN** (T-6 wasn't, T is): **2** — the base shrank through the window


## Predictability per group

How often did the T-6 signal age well across the (scope, flow) permutations for each group? Sorted by predictability%.


| Group | n permutations | persistent | predictability % |
|---|---:|---:|---:|
| Electrical equipment & machinery (chapters 84-85, broad) | 6 | 6 | 100% |
| Critical minerals (export-controlled by China) | 6 | 4 | 67% |
| Finished cars (broad) | 6 | 4 | 67% |
| Steel (broad) | 6 | 3 | 50% |
| Aluminium (broad) | 6 | 3 | 50% |
| Machine tools | 6 | 3 | 50% |
| Permanent magnets | 6 | 3 | 50% |
| EV batteries (Li-ion) | 6 | 3 | 50% |
| Tropical timber (rough + sawn) | 5 | 2 | 40% |
| Antibiotics (HS 2941) | 5 | 2 | 40% |
| Drones and unmanned aircraft | 5 | 2 | 40% |
| Pork (HS 0203) | 3 | 1 | 33% |
| Wind turbine components | 6 | 2 | 33% |
| Cotton (raw + woven fabrics) | 6 | 2 | 33% |
| Solar/grid inverters (broad) | 6 | 2 | 33% |
| Solar PV cells & modules | 5 | 1 | 20% |
| Plastic waste | 5 | 1 | 20% |
| Honey | 6 | 1 | 17% |
| Telecoms base stations | 5 | 0 | 0% |
| Industrial fasteners | 6 | 0 | 0% |
| Pharmaceutical APIs (broad) | 6 | 0 | 0% |
| EV + hybrid passenger cars | 6 | 0 | 0% |
| Motor-vehicle parts | 6 | 0 | 0% |
| Semiconductor manufacturing equipment | 6 | 0 | 0% |
| Polysilicon (solar PV upstream — Xinjiang exposure) | 4 | 0 | 0% |
| Lithium hydroxide (battery-grade) | 2 | 0 | 0% |
| Paracetamol-class amides (HS 2924) | 6 | 0 | 0% |
| Tomato paste / preserved tomatoes | 5 | 0 | 0% |
| Ibuprofen-class monocarboxylic acids (HS 2916) | 6 | 0 | 0% |
| PPE — surgical gloves and masks | 6 | 0 | 0% |
| Rare-earth materials | 4 | 0 | 0% |
| Wind generating sets only | 2 | 0 | 0% |
| Lithium chemicals (carbonate + hydroxide) | 1 | 0 | 0% |


## Sign flips (top 25 by |shift|)

Findings whose YoY direction reversed between T-6 and T. These are mean-reversion signals OR genuine inflections (e.g. a tariff bite). The brief should hedge any reference to the older signal.


| Group | Subkind | yoy@T-6 | yoy@T | shift |
|---|---|---:|---:|---:|
| Tropical timber (rough + sawn) | `hs_group_yoy_uk` | +94.6% | -14.4% | -109.0pp |
| Cotton (raw + woven fabrics) | `hs_group_yoy_uk_export` | +49.3% | -48.3% | -97.6pp |
| Antibiotics (HS 2941) | `hs_group_yoy_export` | +91.1% | -1.1% | -92.1pp |
| Antibiotics (HS 2941) | `hs_group_yoy_combined_export` | +90.9% | -1.0% | -91.9pp |
| Solar PV cells & modules | `hs_group_yoy_export` | -34.2% | +41.1% | +75.3pp |
| Solar PV cells & modules | `hs_group_yoy_combined_export` | -35.0% | +40.0% | +75.0pp |
| Machine tools | `hs_group_yoy_uk` | +55.7% | -19.2% | -74.9pp |
| Paracetamol-class amides (HS 2924) | `hs_group_yoy_uk_export` | +27.1% | -38.1% | -65.2pp |
| Telecoms base stations | `hs_group_yoy_export` | +18.7% | -33.6% | -52.3pp |
| Critical minerals (export-controlled by China) | `hs_group_yoy_uk_export` | +39.9% | -12.0% | -51.8pp |
| Rare-earth materials | `hs_group_yoy_export` | -21.7% | +26.8% | +48.5pp |
| Rare-earth materials | `hs_group_yoy_combined_export` | -20.3% | +26.9% | +47.3pp |
| Tropical timber (rough + sawn) | `hs_group_yoy_combined` | +37.2% | -9.9% | -47.1pp |
| EV + hybrid passenger cars | `hs_group_yoy_uk` | -17.2% | +26.7% | +43.9pp |
| Tropical timber (rough + sawn) | `hs_group_yoy` | +34.1% | -9.6% | -43.7pp |
| Telecoms base stations | `hs_group_yoy_combined_export` | +5.4% | -38.0% | -43.5pp |
| Cotton (raw + woven fabrics) | `hs_group_yoy_uk` | +20.6% | -19.7% | -40.3pp |
| Critical minerals (export-controlled by China) | `hs_group_yoy_uk` | -14.7% | +23.9% | +38.6pp |
| PPE — surgical gloves and masks | `hs_group_yoy_uk_export` | -19.8% | +18.2% | +38.0pp |
| Finished cars (broad) | `hs_group_yoy_uk` | -11.6% | +26.1% | +37.7pp |
| Ibuprofen-class monocarboxylic acids (HS 2916) | `hs_group_yoy_combined` | +5.7% | -29.4% | -35.1pp |
| Ibuprofen-class monocarboxylic acids (HS 2916) | `hs_group_yoy` | +3.2% | -31.3% | -34.6pp |
| Paracetamol-class amides (HS 2924) | `hs_group_yoy_combined_export` | +25.2% | -6.8% | -32.0pp |
| Drones and unmanned aircraft | `hs_group_yoy_export` | +0.9% | -30.9% | -31.8pp |
| Ibuprofen-class monocarboxylic acids (HS 2916) | `hs_group_yoy_combined_export` | +7.5% | -22.8% | -30.3pp |


## Magnitude shifts (same sign, top 25)

Findings whose YoY direction held but magnitude moved by ≥ 5.0pp. The story is the same; the headline number isn't.


| Group | Subkind | yoy@T-6 | yoy@T | shift |
|---|---|---:|---:|---:|
| Wind generating sets only | `hs_group_yoy` | +501.3% | +34.5% | -466.8pp |
| Wind generating sets only | `hs_group_yoy_combined` | +488.6% | +38.3% | -450.3pp |
| EV + hybrid passenger cars | `hs_group_yoy_uk_export` | +67.6% | +216.0% | +148.4pp |
| Pharmaceutical APIs (broad) | `hs_group_yoy` | +297.6% | +215.6% | -82.1pp |
| Pharmaceutical APIs (broad) | `hs_group_yoy_combined` | +284.4% | +207.4% | -77.0pp |
| Plastic waste | `hs_group_yoy_uk` | +118.7% | +187.4% | +68.8pp |
| Plastic waste | `hs_group_yoy_export` | -69.9% | -10.4% | +59.6pp |
| Lithium chemicals (carbonate + hydroxide) | `hs_group_yoy_combined_export` | -62.7% | -5.3% | +57.4pp |
| Rare-earth materials | `hs_group_yoy_combined` | +10.7% | +67.6% | +56.9pp |
| Rare-earth materials | `hs_group_yoy` | +9.9% | +66.0% | +56.1pp |
| Polysilicon (solar PV upstream — Xinjiang exposure) | `hs_group_yoy` | +103.9% | +49.7% | -54.2pp |
| Plastic waste | `hs_group_yoy_combined_export` | -66.2% | -13.6% | +52.6pp |
| Polysilicon (solar PV upstream — Xinjiang exposure) | `hs_group_yoy_combined` | +96.4% | +47.6% | -48.8pp |
| Ibuprofen-class monocarboxylic acids (HS 2916) | `hs_group_yoy_uk` | +55.2% | +6.4% | -48.8pp |
| Ibuprofen-class monocarboxylic acids (HS 2916) | `hs_group_yoy_uk_export` | -34.6% | -68.4% | -33.8pp |
| Paracetamol-class amides (HS 2924) | `hs_group_yoy` | +38.5% | +5.4% | -33.2pp |
| Lithium hydroxide (battery-grade) | `hs_group_yoy_combined` | -93.5% | -60.8% | +32.8pp |
| Paracetamol-class amides (HS 2924) | `hs_group_yoy_combined` | +37.2% | +4.6% | -32.6pp |
| Lithium hydroxide (battery-grade) | `hs_group_yoy` | -93.5% | -61.3% | +32.3pp |
| Pharmaceutical APIs (broad) | `hs_group_yoy_uk_export` | -12.8% | -44.5% | -31.7pp |
| Wind turbine components | `hs_group_yoy` | +45.3% | +16.9% | -28.4pp |
| Wind turbine components | `hs_group_yoy_combined` | +43.6% | +16.5% | -27.0pp |
| Semiconductor manufacturing equipment | `hs_group_yoy_uk_export` | -12.9% | -38.0% | -25.1pp |
| Pharmaceutical APIs (broad) | `hs_group_yoy_export` | +27.1% | +4.2% | -22.9pp |
| Pharmaceutical APIs (broad) | `hs_group_yoy_combined_export` | +25.8% | +3.0% | -22.9pp |


## Low-base flips OUT — base grew across the window (1)

These are HS groups that crossed from low-base to well-quoted-base in the last 6 months. Editorially: small markets becoming materially significant.


| Group | Subkind | EUR@T-6 | EUR@T | yoy@T |
|---|---|---:|---:|---:|
| Paracetamol-class amides (HS 2924) | `hs_group_yoy_combined_export` | €58M | €58M | -6.8% |

## Low-base flips IN — base shrank across the window (2)

HS groups that crossed from significant-base to low-base. Editorially: previously quotable flows now too small to anchor a story.


| Group | Subkind | EUR@T-6 | EUR@T | yoy@T |
|---|---|---:|---:|---:|
| Steel (broad) | `hs_group_yoy_uk_export` | €61M | €48M | -43.5% |
| Ibuprofen-class monocarboxylic acids (HS 2916) | `hs_group_yoy_export` | €57M | €49M | -19.1% |


## Trajectory-shape persistence — forward work

A proper backtest of trajectory-shape persistence (did the analyser's `rising_accelerating` classification at T-6 still read `rising_accelerating` at T?) requires analyser runs spread over real calendar time. Today's findings are all stamped today's date — the result of the Phase 5 clean-state rebuild plus the iterative method-version bumps in Phases 6.x. So the supersede chain reflects same-day re-runs, not a historical record of what we said in August.


This only becomes a measurable question once analyser runs have been periodically scheduled (e.g. monthly via cron after each Eurostat release). At that point the trajectory chain captures a real T-6 → T comparison and this section can be filled in. Suggested next-step: a trivial GitHub Action that runs the full analyser pipeline on the 1st of each month and re-runs this backtest script.


In the meantime, the YoY-rolling-window measure above is the best proxy: 26% of YoY signals stayed within 5pp of their T-6 value with the same direction.
