-- 2026-09-09 — register the `jan_derived_from_feb` caveat row so the brief +
-- per-finding provenance pages can say plainly when a window's January was
-- computed rather than published. Idempotent via ON CONFLICT DO NOTHING.
--
-- Companion to the analyser change that derives January from a standalone
-- February release. GACC bundled Jan+Feb into one cumulative release every
-- Chinese New Year from 2020 to 2025; 2026 broke that pattern and published
-- February as an ordinary release. The 2026-05-15 work folded the bundled
-- years into the window but left the 2026 shape unhandled, so a rolling-12mo
-- window ending after Feb 2026 summed 11 months against a 12-month prior
-- half. On the 2026-09-09 August page that reported China's 12-month exports
-- to the EU at -0.4% when the true figure was about +9.1%.

INSERT INTO caveats (code, summary, detail, applies_to) VALUES
  ('jan_derived_from_feb',
   'January is computed as (February-release YTD − February-release Monthly), not published as its own release',
   'From 2026 GACC publishes February as an ordinary release carrying both a Monthly column (February alone) and a YTD column (January + February), instead of the single Jan+Feb cumulative it issued each Chinese New Year from 2020 to 2025. January therefore has no release of its own, but it is not missing: it is the YTD minus the Monthly, an exact identity on two figures GACC itself published. That is arithmetic, not interpolation, which is why it is safe where splitting a combined Jan+Feb cumulative 50/50 would not be — the analyser still refuses to do the latter. The derived month fills rolling-12mo windows but never anchors a finding, because no January release exists to date one against. Both source observations are carried in the finding''s observation_ids so the provenance drawer shows the whole chain. detail.totals.jan_derived_years lists which years a window relied on. Editorial implication: the 12-month total and YoY cover all 12 months and are directly comparable with a prior half that used a bundled Jan+Feb chunk; without the derivation they would not be.',
   ARRAY['gacc_bilateral_aggregate_yoy', 'gacc_bilateral_aggregate_yoy_import',
         'gacc_aggregate_yoy', 'gacc_aggregate_yoy_import'])
ON CONFLICT (code) DO NOTHING;
