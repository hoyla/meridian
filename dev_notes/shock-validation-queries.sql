-- Validation queries for shock-validation-2026-05-09.md
-- Run AFTER analysers complete; outputs feed the Results sections.

-- ========================================================================
-- 0. Coverage / sanity
-- ========================================================================

-- 0.1 Eurostat period range + per-partner row counts
SELECT
    'Eurostat raw rows by partner' AS metric,
    partner,
    COUNT(*) AS rows,
    MIN(period) AS earliest,
    MAX(period) AS latest
FROM eurostat_raw_rows
GROUP BY partner
ORDER BY rows DESC;

-- 0.2 GACC release count per year (section 4 only, since that's what we parse)
SELECT
    EXTRACT(YEAR FROM period)::int AS year,
    COUNT(*) AS releases,
    MIN(period) AS earliest,
    MAX(period) AS latest
FROM releases
WHERE source = 'gacc' AND section_number = 4
GROUP BY 1
ORDER BY 1;

-- 0.3 Active findings count by subkind
SELECT
    kind, subkind,
    COUNT(*) AS active_findings
FROM findings
WHERE superseded_at IS NULL
GROUP BY 1, 2
ORDER BY 1, 2;

-- 0.4 Method versions in use (confirms v7/v5/v4 are live)
SELECT
    DISTINCT detail->>'method' AS method,
    COUNT(*) AS findings
FROM findings
WHERE superseded_at IS NULL
GROUP BY 1
ORDER BY 1;

-- ========================================================================
-- 1. Shock 1: 2018 Section 232 steel & aluminium tariffs (March 2018)
-- ========================================================================
-- Expected: positive YoY for Steel (HS 72%) and Aluminium (HS 76%) in
-- mid-to-late 2018 (windows ending 2018-06 through 2018-12).

-- 1.1 Steel YoY across 2018
SELECT
    detail->'group'->>'name' AS group_name,
    (detail->'windows'->>'current_end')::date AS window_end,
    ROUND((detail->'totals'->>'yoy_pct')::numeric * 100, 2) AS yoy_pct,
    ROUND((detail->'totals'->>'yoy_pct_kg')::numeric * 100, 2) AS yoy_kg_pct,
    ROUND((detail->'totals'->>'current_12mo_eur')::numeric / 1e9, 2) AS current_eur_bn,
    detail->'totals'->>'low_base' AS low_base
FROM findings
WHERE subkind = 'hs_group_yoy'
  AND superseded_at IS NULL
  AND detail->'group'->>'name' IN ('Steel (broad)', 'Aluminium (broad)')
  AND (detail->'windows'->>'current_end')::date BETWEEN '2018-01-01' AND '2018-12-31'
ORDER BY group_name, window_end;

-- 1.2 Steel + Aluminium trajectory classification covering 2017-2019 era
-- (trajectory uses the full series, so any post-backfill trajectory finding
-- now reflects the full 2017+ history)
SELECT
    detail->'group'->>'name' AS group_name,
    detail->>'shape' AS shape,
    detail->>'shape_label' AS shape_label,
    detail->'features'->>'first_period' AS first_period,
    detail->'features'->>'peak_period' AS peak_period,
    detail->'features'->>'trough_period' AS trough_period
FROM findings
WHERE subkind = 'hs_group_trajectory'
  AND superseded_at IS NULL
  AND detail->'group'->>'name' IN ('Steel (broad)', 'Aluminium (broad)')
ORDER BY group_name;

-- ========================================================================
-- 2. Shock 2: Q1 2020 COVID lockdown
-- ========================================================================
-- Expected: most flow=1 groups show negative YoY for windows ending
-- Feb/Mar/Apr 2020. Trajectory mostly classifies as dip_recovery.

-- 2.1 All groups: YoY for window ending around 2020-04 (Apr 2020 window
-- captures the full Q1 2020 collapse vs Apr 2019).
SELECT
    detail->'group'->>'name' AS group_name,
    (detail->'windows'->>'current_end')::date AS window_end,
    ROUND((detail->'totals'->>'yoy_pct')::numeric * 100, 1) AS yoy_pct,
    ROUND((detail->'totals'->>'current_12mo_eur')::numeric / 1e9, 2) AS current_eur_bn
FROM findings
WHERE subkind = 'hs_group_yoy'
  AND superseded_at IS NULL
  AND (detail->'windows'->>'current_end')::date IN ('2020-04-01', '2020-06-01', '2020-12-01')
ORDER BY window_end, group_name;

-- 2.2 Trajectory shape distribution (full data)
SELECT
    detail->>'shape' AS shape,
    COUNT(*) AS n_groups,
    string_agg(detail->'group'->>'name', ', ' ORDER BY detail->'group'->>'name') AS groups
FROM findings
WHERE subkind = 'hs_group_trajectory'
  AND superseded_at IS NULL
GROUP BY 1
ORDER BY 2 DESC;

-- ========================================================================
-- 3. Shock 3: Feb 2022 Russia invasion (renewables substitution)
-- ========================================================================
-- Expected: positive YoY for Solar PV and Wind groups through 2022-2023.

-- 3.1 Solar PV + Wind YoY across 2022
SELECT
    detail->'group'->>'name' AS group_name,
    (detail->'windows'->>'current_end')::date AS window_end,
    ROUND((detail->'totals'->>'yoy_pct')::numeric * 100, 1) AS yoy_pct,
    ROUND((detail->'totals'->>'current_12mo_eur')::numeric / 1e9, 2) AS current_eur_bn
FROM findings
WHERE subkind = 'hs_group_yoy'
  AND superseded_at IS NULL
  AND detail->'group'->>'name' IN (
        'Solar PV cells & modules',
        'Wind generating sets only',
        'Wind turbine components',
        'Solar/grid inverters (broad)'
      )
  AND (detail->'windows'->>'current_end')::date BETWEEN '2022-03-01' AND '2023-12-01'
ORDER BY group_name, window_end;

-- ========================================================================
-- 4. Shock 4: Oct 2023 EU EV anti-subsidy probe (and Oct 2024 duties)
-- ========================================================================
-- Expected: EV cars positive YoY through 2023-mid-2024, peak Q3-Q4 2024
-- (pre-tariff stockpiling), sharp negative from Q4 2024+. Trajectory
-- should be inverse_u_peak.

-- 4.1 EV + hybrid passenger cars YoY across 2023-2026
SELECT
    (detail->'windows'->>'current_end')::date AS window_end,
    ROUND((detail->'totals'->>'yoy_pct')::numeric * 100, 1) AS yoy_pct,
    ROUND((detail->'totals'->>'yoy_pct_kg')::numeric * 100, 1) AS yoy_kg_pct,
    ROUND((detail->'totals'->>'current_12mo_eur')::numeric / 1e9, 2) AS current_eur_bn
FROM findings
WHERE subkind = 'hs_group_yoy'
  AND superseded_at IS NULL
  AND detail->'group'->>'name' = 'EV + hybrid passenger cars'
ORDER BY window_end;

-- 4.2 EV trajectory classification
SELECT
    detail->'group'->>'name' AS group_name,
    detail->>'shape' AS shape,
    detail->>'shape_label' AS shape_label,
    detail->'features'->>'first_period' AS first_period,
    detail->'features'->>'peak_period' AS peak_period,
    detail->'features'->>'trough_period' AS trough_period
FROM findings
WHERE subkind = 'hs_group_trajectory'
  AND superseded_at IS NULL
  AND detail->'group'->>'name' IN ('EV + hybrid passenger cars', 'Finished cars (broad)')
ORDER BY group_name;

-- ========================================================================
-- 5. Cross-shock sanity
-- ========================================================================

-- 5.1 Caveat distribution across active YoY findings
SELECT
    caveat,
    COUNT(*) AS n_findings
FROM findings,
     LATERAL jsonb_array_elements_text(detail->'caveat_codes') AS caveat
WHERE subkind = 'hs_group_yoy'
  AND superseded_at IS NULL
GROUP BY caveat
ORDER BY n_findings DESC;

-- 5.2 Mirror-gap baselines: how the NL gap shifts now HK/MO are summed in
SELECT
    detail->>'iso2' AS iso2,
    COUNT(*) AS n_findings,
    ROUND(AVG((detail->>'gap_pct')::numeric * 100), 1) AS avg_gap_pct,
    ROUND(MIN((detail->>'gap_pct')::numeric * 100), 1) AS min_gap_pct,
    ROUND(MAX((detail->>'gap_pct')::numeric * 100), 1) AS max_gap_pct
FROM findings
WHERE subkind = 'mirror_gap'
  AND superseded_at IS NULL
  AND detail->>'iso2' IS NOT NULL
  AND detail->>'iso2' NOT LIKE 'BLOC:%'
GROUP BY detail->>'iso2'
HAVING COUNT(*) >= 3
ORDER BY avg_gap_pct DESC;

-- 5.3 Total Chinese EU imports/exports trend across the full backfill
-- (a basic eyeball check: do the Lisa O'Carroll Q1 2026 numbers reproduce?)
SELECT
    EXTRACT(YEAR FROM period)::int AS year,
    SUM(CASE WHEN flow = 1 THEN value_eur ELSE 0 END) / 1e9 AS imports_from_cn_bn,
    SUM(CASE WHEN flow = 2 THEN value_eur ELSE 0 END) / 1e9 AS exports_to_cn_bn,
    SUM(CASE WHEN flow = 1 THEN value_eur ELSE -value_eur END) / 1e9 AS eu_deficit_bn
FROM eurostat_raw_rows
WHERE partner IN ('CN', 'HK', 'MO')
GROUP BY 1
ORDER BY 1;
