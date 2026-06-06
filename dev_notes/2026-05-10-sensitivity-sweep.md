# Sensitivity sweep — 2026-05-10

Phase 6.3 of dev_notes/history.md. For each methodology threshold, replay the classification under variant values and report which findings are fragile. Pure compute over the existing active findings — does not touch the findings table.


Editorial value: a finding that flips classification under a small threshold change rests on a methodology choice as much as on the data. Journalists should know which numbers are robust and which are sensitive.


## Top-line: which thresholds matter?

1. **`low_base_threshold_eur` (default €50M) — HIGHLY SENSITIVE.** About a third of all `hs_group_yoy*` findings flag low_base at the default; the count would nearly double at €100M and roughly halve at €25M. ~7,100 findings (49% of all) sit in the flip zone between €5M and €500M. The threshold is the single largest methodology choice driving editorial framing — the per-finding table below identifies which groups are most threshold-fragile.

2. **`kg_coverage_threshold` (default 0.80) — INSENSITIVE in production.** Real-data kg coverage is bimodal at 100% (rich, ~84%) or exactly 1.0 (15.7%); only 7 findings sit in the 0.80–0.90 band. Lowering the threshold to 0.60 changes nothing; raising it to 0.90 catches just 7 findings. The gate is doing essentially no work in production — it's defensive against a failure mode (HS groups dominated by pieces or litres) that doesn't currently trigger. Worth keeping as a guard, but not a knob to tune.

3. **`z_threshold` (default 1.5) — MODERATELY SENSITIVE.** All 74 active mirror_gap_zscore findings already pass |z| ≥ 1.5; raising to 2.0 cuts 30 findings (-41%); raising to 2.5 cuts 44 (-59%). 18 findings sit within ±0.3 of the default — these are the marginal cases. NL Rotterdam-transshipment z-scores cluster in this band, as do French/Italian/Bloc-level findings. Editorial guidance: a z=1.6 mirror-gap reads as 'just above the threshold' rather than 'a clean anomaly'.


## 1. `low_base_threshold_eur` (hs_group_yoy*)

Each `hs_group_yoy*` finding flags `low_base = true` when the smaller of (current_12mo_eur, prior_12mo_eur) is below the threshold (default €50M). Editorial impact: low_base findings carry a `low_base_effect` caveat that warns journalists not to quote the percentage without context.


Variants swept: ['€5M', '€25M', '€50M', '€100M', '€500M'].

Active findings examined: 14,558.

| Threshold | low_base count | % of total | Δ vs default |
|---|---:|---:|---:|
| €5M | 1,932 | 13.3% | +0 |
| €25M | 3,606 | 24.8% | +0 |
| €50M (default) | 4,834 | 33.2% | — |
| €100M | 6,365 | 43.7% | +1531 |
| €500M | 9,037 | 62.1% | +4203 |

### Findings flipping classification under sweep range

Findings that switch low_base status between the most aggressive (€5M) and most permissive (€500M) variants:


Total flip-zone findings: **7,105** across **116** distinct (group, subkind) pairs.


Top 20 (group, subkind) pairs in the flip zone, by minimum-window EUR:

| Group | Subkind | Latest period | min(curr, prior) EUR | yoy% |
|---|---|---|---:|---:|
| Wind turbine components | `hs_group_yoy_uk_export` | 2025-08-01 | €5M | -9.5% |
| Plastic waste | `hs_group_yoy` | 2025-11-01 | €5M | -30.4% |
| Pharmaceutical APIs (broad) | `hs_group_yoy_uk_export` | 2026-01-01 | €5M | -41.1% |
| Honey | `hs_group_yoy_combined_export` | 2022-09-01 | €5M | -14.6% |
| Rare-earth materials | `hs_group_yoy_export` | 2025-10-01 | €5M | +12.9% |
| Honey | `hs_group_yoy_export` | 2018-12-01 | €5M | -67.2% |
| Pork (HS 0203) | `hs_group_yoy` | 2022-07-01 | €5M | -9.4% |
| Pork (HS 0203) | `hs_group_yoy_combined` | 2022-07-01 | €5M | -9.4% |
| Lithium chemicals (carbonate + hydroxide) | `hs_group_yoy_export` | 2026-02-01 | €5M | +90.8% |
| Telecoms base stations | `hs_group_yoy_uk` | 2025-08-01 | €5M | +2.9% |
| Rare-earth materials | `hs_group_yoy_combined_export` | 2025-10-01 | €5M | +12.8% |
| Plastic waste | `hs_group_yoy_combined_export` | 2021-08-01 | €6M | -86.1% |
| Plastic waste | `hs_group_yoy_export` | 2021-07-01 | €6M | -83.9% |
| Plastic waste | `hs_group_yoy_uk_export` | 2020-10-01 | €6M | -57.8% |
| PPE — surgical gloves and masks | `hs_group_yoy_uk_export` | 2023-07-01 | €6M | -15.5% |
| EV batteries (Li-ion) | `hs_group_yoy_uk_export` | 2026-02-01 | €6M | +16.6% |
| Telecoms base stations | `hs_group_yoy_export` | 2026-02-01 | €6M | -33.6% |
| Telecoms base stations | `hs_group_yoy_combined_export` | 2026-02-01 | €7M | -38.0% |
| Critical minerals (export-controlled by China) | `hs_group_yoy_uk_export` | 2023-02-01 | €7M | +12.5% |
| Cotton (raw + woven fabrics) | `hs_group_yoy_uk` | 2026-02-01 | €7M | -19.7% |

## 2. `kg_coverage_threshold` (hs_group_yoy*)

Each `hs_group_yoy*` finding records the kg_coverage_pct (the fraction of value_eur in the rolling 12-month window backed by an actual quantity_kg measurement). Below the threshold (default 80%) the unit-price decomposition is suppressed and a `low_kg_coverage` caveat fires. Editorial impact: changes which findings carry a unit-price story.


Active findings with kg_coverage_pct recorded: 14,558.

kg_coverage_pct distribution:

| Range | Count | % |
|---|---:|---:|
| 0.80–0.90 | 7 | 0.0% |
| 0.90–1.00 | 12,268 | 84.3% |
| 1.00 (exact) | 2,283 | 15.7% |

Findings with decomposition suppressed at each threshold:

| Threshold | Suppressed count | % | Δ vs default |
|---|---:|---:|---:|
| 0.60 | 0 | 0.0% | +0 |
| 0.70 | 0 | 0.0% | +0 |
| 0.80 (default) | 0 | 0.0% | — |
| 0.90 | 7 | 0.0% | +7 |

## 3. `z_threshold` (mirror_gap_zscore)

Each `mirror_gap_zscore` finding records its computed |z| score. The analyser only emits findings with |z| ≥ z_threshold (default 1.5). Lowering the threshold surfaces more findings (smaller signal); raising it focuses on stronger anomalies.


Active findings examined: 74.

Active |z| distribution:

| |z| range | Count |
|---|---:|
| 0.0 ≤ |z| < 1.0 | 0 |
| 1.0 ≤ |z| < 1.5 | 0 |
| 1.5 ≤ |z| < 2.0 | 30 |
| 2.0 ≤ |z| < 2.5 | 14 |
| 2.5 ≤ |z| < 3.0 | 10 |
| 3.0 ≤ |z| < 5.0 | 17 |
| 5.0 ≤ |z| < ∞ | 3 |

Findings retained at each threshold:

| Threshold | Retained | % | Δ vs default |
|---|---:|---:|---:|
| 1.0 | 74 | 100.0% | +0 |
| 1.5 | 74 | 100.0% | — |
| 2.0 | 44 | 59.5% | -30 |
| 2.5 | 30 | 40.5% | -44 |


### Findings within |z| ±0.3 of default 1.5 (most fragile)


Total: **18**. These are the findings whose presence in the brief is most sensitive to small methodology choices.


| ISO2 | Period | |z| | gap% |
|---|---|---:|---:|
| NL | 2021-03-01 | 1.52 | +65.1% |
| DE | 2024-06-01 | 1.53 | +39.8% |
| IT | 2025-03-01 | 1.55 | +70.5% |
| NL | 2025-04-01 | 1.56 | +58.4% |
| DE | 2020-09-01 | 1.56 | +56.1% |
| BLOC:eu_bloc | 2020-09-01 | 1.57 | +56.4% |
| BLOC:eu_bloc | 2020-10-01 | 1.59 | +57.5% |
| IT | 2019-07-01 | 1.60 | +64.0% |
| FR | 2020-10-01 | 1.63 | +58.0% |
| NL | 2020-10-01 | 1.63 | +63.8% |
| NL | 2024-06-01 | 1.69 | +53.5% |
| FR | 2022-07-01 | 1.70 | +48.2% |
| NL | 2020-04-01 | 1.70 | +56.0% |
| IT | 2025-11-01 | 1.73 | +50.9% |
| IT | 2025-08-01 | 1.74 | +49.8% |
| IT | 2019-08-01 | 1.74 | +42.4% |
| NL | 2021-06-01 | 1.74 | +58.8% |
| BLOC:eu_bloc | 2024-10-01 | 1.78 | +61.7% |
