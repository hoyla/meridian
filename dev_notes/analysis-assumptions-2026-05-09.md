# Analysis features: assumptions & proposals

Reviewed: 2026-05-09

## 1. `mirror-trade` (mirror gap)

### Assumptions

- **FX timing**: GACC values (CNY) are converted to EUR using the ECB monthly average rate for the same period. If GACC uses a different rate date, settlement rate, or averaging convention, the gap is partly an FX artefact. Acknowledged via the `currency_timing` caveat.
- **CIF vs FOB baseline**: A single global constant (`CIF_FOB_BASELINE_PCT = 7.5%`) is used as the expected Eurostat-higher margin. In practice CIF/FOB margins vary significantly by route and commodity (bulk sea freight from China to a landlocked EU member will differ from containerised goods to Rotterdam).
- **Transshipment**: GACC's "export to Germany" may include goods that transit through the Netherlands. The country-level breakdown won't match even if the global total does.
- **Unit-scale parsing fallback**: If a release uses an unexpected unit format, `parse_unit_scale` silently falls back to multiplier `1.0` — the converted EUR value could be off by orders of magnitude, with only a warning log.
- **Aggregation granularity**: GACC totals are section-4 country-level exports; Eurostat totals are the sum of all HS-CN8 import rows for `partner=CN`. These are not necessarily the same scope.

### Proposals

1. **Per-route CIF/FOB baselines**: Replace the single 7.5% constant with a lookup table keyed by (partner_iso2) or (partner_iso2, commodity_group). Seed it with UNCTAD/WTO estimates and allow manual overrides. Store the baseline used in each finding's `detail` for auditability.
2. **Unit-scale parse failure → hard skip**: When `parse_unit_scale` returns the `(1.0, None)` fallback, treat it as a skip (`skipped_unrecognised_unit`) rather than proceeding with a likely-wrong multiplier. Log at ERROR, not WARNING.
3. **Transshipment flag**: Where the GACC partner is a known transshipment hub (NL, BE, HK, SG), add a `transshipment_hub` caveat automatically and note it in the finding body.

---

## 2. `mirror-gap-trends` (z-score on gap series)

### Assumptions

- **Stationarity**: The rolling z-score assumes the per-country gap percentage is roughly stationary within the baseline window. A structural shift (new trade-defence measure, classification change) within the window inflates stdev and suppresses detection — or sits inside the baseline making the new normal look anomalous.
- **Small-sample baseline**: With `min_baseline_n=3` and a default 6-month window, the stdev estimate is noisy. A z-score computed from 3 points is not statistically robust.
- **Dependency on prior findings**: This pass reads `findings` rows written by `mirror-trade`. If that pass hasn't been run, or was run with different filters, the trend analysis operates on stale/incomplete data with no guard.

### Proposals

1. **Raise `min_baseline_n`**: Default to at least 6 (a full window), or emit a `low_baseline_n` caveat when firing on fewer than 6 points so journalists can weigh accordingly.
2. **Staleness check**: Before running, query the most recent `mirror-trade` analysis run. If it's older than N days (configurable), warn or abort. Optionally auto-trigger `mirror-trade` as a prerequisite.
3. **Structural-break detection**: Before computing the z-score, run a simple Chow test or CUSUM on the baseline. If there's evidence of a break, flag the finding with a `baseline_regime_change` caveat and report the break point.

---

## 3. `hs-group-yoy` (rolling 12-month YoY)

### Assumptions

- **Complete monthly coverage required**: All 24 months must be present in `eurostat_raw_rows`. A missing month means the window is skipped entirely rather than treating absence as zero trade. Conservative, but late-reporting periods silently disappear.
- **`quantity_kg` reliability**: The volume decomposition ("volume-driven vs price-driven") relies on `quantity_kg`, which is not always populated. If the primary supplementary unit is pieces, litres, etc., the kg field may be zero/null, making the decomposition unreliable.
- **Low-base threshold is editorial judgement**: The €50M floor (`LOW_BASE_THRESHOLD_EUR`) is hardcoded. Valid for macro stories, too high for niche-commodity coverage.
- **CN8 nomenclature drift**: HS patterns are matched via `LIKE ANY(patterns)` on CN8 codes. Eurostat revises CN8 nomenclature annually — the same pattern may capture a different product scope in the prior-year window, making the YoY partly a classification artefact.

### Proposals

1. **Gap-tolerant windows**: Instead of requiring all 24 months, allow up to N missing months (e.g. 2) and prorate or interpolate. Add a `missing_months` field to `detail` so consumers know the window isn't clean.
2. **kg coverage metric**: Compute and store the fraction of value_eur that has a non-null, non-zero `quantity_kg`. When coverage is below a threshold (e.g. 80%), suppress the volume decomposition and note it in the finding body rather than reporting potentially misleading unit prices.
3. **Configurable low-base threshold**: Accept `--low-base-threshold` on the CLI. Keep €50M as default but allow override for niche analyses.
4. **CN8 concordance table**: Maintain a `cn8_concordances` table mapping old→new codes across nomenclature revisions. When a YoY window spans a revision boundary, join through the concordance so like is compared with like. Flag findings that cross a revision boundary with a `cn8_revision` caveat.

---

## 4. `hs-group-trajectory` (shape classification)

### Assumptions

- **Reads its own findings**: Trajectory classification has no independent data access — it reads `hs_group_yoy` findings from the DB. If the YoY pass was run with a threshold that filtered out periods, the trajectory sees a partial series with no indication of gaps.
- **Smoothing hides real signals**: A 3-period centered moving average suppresses 1-month reversals by design, but also smooths away genuine 1-month spikes (e.g. tariff pre-loading).
- **OLS slope sensitivity**: The accelerating/decelerating distinction uses OLS on the smoothed series. OLS is sensitive to outliers at the endpoints — a single extreme first or last window can flip the classification.
- **Fixed shape vocabulary**: The classifier maps to 12 shapes. Patterns that don't fit (e.g. seasonal oscillation) get labelled `volatile`, which is accurate but uninformative.

### Proposals

1. **Gap detection in input series**: Before classifying, check for period gaps in the YoY series. If gaps exist, either interpolate (and flag) or refuse to classify (and emit `skipped_incomplete_series`).
2. **Configurable smoothing**: Accept `--smooth-window` on the CLI (default 3). For analyses focused on short-term policy effects, allow `--smooth-window 1` (no smoothing).
3. **Robust slope**: Replace OLS with Theil-Sen slope (median of pairwise slopes) — resistant to endpoint outliers. Negligible compute cost at these series lengths.
4. **Seasonal shape**: Add a `seasonal` shape to the vocabulary. Detect via autocorrelation at lag 12 on the raw (unsmoothed) YoY series. If significant, label as seasonal and report the amplitude.

---

## Cross-cutting issues

### No idempotency

All four passes `INSERT` findings without checking for existing rows for the same (period, partner/group). Re-running produces duplicates.

**Proposal**: Add a `UNIQUE` constraint or upsert logic on `(subkind, detail->>'iso2', period)` for mirror-trade findings and `(subkind, hs_group_id, detail->'windows'->>'current_end')` for HS-group findings. Alternatively, delete prior findings from the same analysis source URL at the start of each run (within the same transaction).

### Eurostat partner hardcoded to `'CN'`

All Eurostat-side queries filter `partner = 'CN'`. Goods reported by China via Hong Kong or Macau won't appear in the comparison.

**Proposal**: Make the partner list configurable (default `['CN']`). For mirror-trade specifically, allow a `--eurostat-partners CN,HK` flag. When multiple partners are used, note it in the finding detail and add a `multi_partner_sum` caveat.
