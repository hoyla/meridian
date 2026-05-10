# Per-country CIF/FOB baselines from OECD ITIC

Phase 6 follow-on (originally Phase 4 in the roadmap), shipped
2026-05-10. Replaces the 7.5% global default with per-(EU member
state, China) values sourced from the OECD's International
Transport and Insurance Costs of merchandise trade (ITIC) dataset.
Editorial impact: 351 active mirror_gap findings now carry the
country-specific baseline, and `excess_over_baseline_pct`
recalculates accordingly.

## What changed

Before: every mirror_gap finding compared its observed gap to a
single 7.5% global default — a ballpark from UNCTAD's headline
freight-cost estimate.

After: 28 per-country rows in `cif_fob_baselines` cover every EU-27
member + GB, sourced from OECD ITIC 2022 (the most recent year in
the published dataset). The 7.5% global default is preserved for
non-EU partners we haven't covered, and as a fallback if a future
country isn't in the table.

## Source

OECD International Transport and Insurance Costs of merchandise
trade (ITIC), version 1.1.

- Dataset URL: <https://www.oecd.org/en/data/datasets/international-transport-and-insurance-costs-of-merchandise-trade-itic.html>
- Methodology paper: [CIF/FOB margins: Insights on global transport and insurance costs of merchandise trade (OECD, 2024)](https://www.oecd.org/content/dam/oecd/en/publications/reports/2024/06/cif-fob-margins_daa81e46/469123ab-en.pdf)
- SDMX query (per-country, China as origin, all commodities, 2022):
  ```
  https://sdmx.oecd.org/sti-public/rest/data/OECD.SDD.TPS,DSD_ITIC@DF_ITIC,1.1/{REPORTER}.CHN....A.?format=csvfilewithlabels&startPeriod=2022&endPeriod=2022
  ```

ITIC combines official national statistics on observed CIF-FOB
margins with model-based estimates (gravity model). The figures
below carry methodology=`Aggregation` (computed from underlying
HS-4 commodity-level rows for that country pair). Coverage is
1995-2022; 2022 is the most recent.

## Per-country values

Imports from China, all commodities, 2022:

| ISO2 | Country | CIF/FOB margin | vs old 7.5% default |
|---|---|---:|---:|
| BG | Bulgaria | 7.79% | +0.29 pp |
| PT | Portugal | 7.41% | -0.09 pp |
| HR | Croatia | 7.40% | -0.10 pp |
| ES | Spain | 7.37% | -0.13 pp |
| GR | Greece | 7.36% | -0.14 pp |
| PL | Poland | 7.29% | -0.21 pp |
| MT | Malta | 7.29% | -0.21 pp |
| LT | Lithuania | 7.27% | -0.23 pp |
| FR | France | 7.22% | -0.28 pp |
| CY | Cyprus | 7.18% | -0.32 pp |
| HU | Hungary | 7.18% | -0.32 pp |
| LV | Latvia | 7.14% | -0.36 pp |
| BE | Belgium | 7.01% | -0.49 pp |
| IT | Italy | 7.00% | -0.50 pp |
| SE | Sweden | 6.97% | -0.53 pp |
| GB | United Kingdom | 6.91% | -0.59 pp |
| DK | Denmark | 6.88% | -0.62 pp |
| EE | Estonia | 6.73% | -0.77 pp |
| AT | Austria | 6.63% | -0.87 pp |
| NL | Netherlands | 6.55% | -0.95 pp |
| DE | Germany | 6.50% | -1.00 pp |
| FI | Finland | 6.50% | -1.00 pp |
| SI | Slovenia | 6.38% | -1.12 pp |
| IE | Ireland | 5.81% | -1.69 pp |
| LU | Luxembourg | 5.67% | -1.83 pp |
| CZ | Czechia | 5.04% | -2.46 pp |
| RO | Romania | 4.95% | -2.55 pp |
| SK | Slovak Republic | 3.15% | -4.35 pp |

Unweighted mean: **6.65%** (vs the 7.50% global default we'd been
using). The northwest-European core (DE, NL, FR, IT, BE) clusters
around 6.5–7.2%; landlocked / Eastern European countries vary more
widely. Slovakia at 3.15% is the outlier — worth flagging if any
investigation specifically rests on Slovak imports from China.

## Editorial implication

For a typical NL mirror-gap finding (~65% Eurostat-higher, the
Rotterdam-transshipment classic), the excess_over_baseline_pct
under the old default was ~57.5% (65% - 7.5%); under the new
NL-specific 6.55% it's ~58.5%. So the structural transshipment
narrative gets ~1pp stronger, but qualitatively unchanged.

For SK (the most divergent), the old default would understate the
true excess by ~4pp — meaningful if Slovakia ever featured in a
specific mirror-gap story. (It currently doesn't, so this is
defensive accuracy rather than editorial unlock.)

## What's NOT in this update

- **Per-(country, commodity) granularity.** ITIC supports HS-4 splits
  (e.g. NL imports of HS17_8517 — telecoms equipment — from CN may
  have a different margin than NL all-commodities). The 1224 HS-4
  codes × 28 EU countries × 1 partner (CN) = ~34k rows. We currently
  use the all-commodities `_T` row per country. If a story rests on
  a specific HS group's CIF/FOB sensitivity, the SDMX query above
  can pull the per-commodity rows directly.
- **Time-varying baselines.** ITIC has 1995-2022 data; we use the
  2022 snapshot. Margins shift slowly but the COVID/post-COVID period
  saw freight cost spikes (peaked 2021). For trajectory-level work
  this might matter; for headline mirror-gap framing it doesn't.
- **Non-EU partners.** Other GACC trading partners (US, JP, KR, etc.)
  still use the 7.5% global default. ITIC has data for them too —
  add a row per partner if a story needs it.
- **HK/MO transshipment effects.** ITIC covers CN as the origin; the
  Hong Kong / Macau routing question (Phase 2.3) is orthogonal — this
  is about freight cost from origin to destination, not about which
  partner code captures the trade.

## Code change

- 28 INSERTs into `cif_fob_baselines` (idempotent — the table has
  `UNIQUE(COALESCE(partner_iso2, '_GLOBAL_'))`).
- Method bump: `mirror_trade_v4_multi_partner_default` →
  `mirror_trade_v5_per_country_cif_fob_baselines`.
- `value_fields` for mirror_gap findings now includes
  `cif_fob_baseline_pct` so future baseline updates propagate via
  the supersede chain (no method-bump dance required).
- 351 mirror_gap findings re-emitted; 351 superseded.

## Reproducibility

The `cif_fob_baselines` rows include `source_url` pointing at the
OECD ITIC dataset page. The exact SDMX query for Germany/China 2022
is:

```bash
curl -sL "https://sdmx.oecd.org/sti-public/rest/data/OECD.SDD.TPS,DSD_ITIC@DF_ITIC,1.1/DEU.CHN....A.?format=csvfilewithlabels&startPeriod=2022&endPeriod=2022&dimensionAtObservation=AllDimensions" \
  | grep ',DEU,Germany,CHN,.*,_T,'
```

Filter the resulting CSV to `COMM_HS2017 = '_T'` and `MEASURE =
'C_F'` for the all-commodities CIF/FOB margin. To refresh in a
future year (when ITIC publishes 2023+ data), re-run the bulk
download script in this commit message and re-INSERT.

## Forward work

If editorial questions emerge that depend on per-commodity CIF/FOB:
- Schema-extend `cif_fob_baselines` to add `(commodity_hs2, baseline_pct)`
- Pull HS-4 ITIC data via the same SDMX endpoint (no `_T` filter)
- Update mirror-trade analyser to look up per-(partner, commodity)
  before falling back to per-partner before falling back to global

For the IMF/World Bank Direction-of-Trade convention (10% CIF/FOB
default that some institutions use for cross-checks), keep the
ITIC values. They're per-country observed/modeled; the 10% IMF DOTS
default is a placeholder for the absence of country-specific data.
