-- 2026-05-15 — register the `jan_feb_combined` caveat row so the brief +
-- per-finding provenance pages can render its plain-English text when a
-- finding's rolling-12mo window was filled in part by a GACC Jan+Feb
-- combined release. Same row as `schema.sql` (kept in sync at the
-- moment of writing); idempotent via `ON CONFLICT DO NOTHING` so re-
-- runs are safe.
--
-- Companion to commit 908b1f3 ("parser: handle GACC's January-February
-- combined release") and the analyser changes that tag affected
-- findings with `'jan_feb_combined'` in detail.caveat_codes.

INSERT INTO caveats (code, summary, detail, applies_to) VALUES
  ('jan_feb_combined',
   'Rolling-12mo sum includes a Jan+Feb cumulative chunk rather than separate monthly observations',
   'GACC bundles January and February into a single cumulative release each Chinese New Year (the publication pattern across 2020-2025; 2026 was the recent exception). When the rolling-12mo window covers such a year''s Jan + Feb, the analyser uses the Jan+Feb cumulative value as a single 2-month chunk in the sum. The cumulative is NOT split 50/50 between Jan and Feb — interpolation would invent per-month figures the source never asserted. Editorial implication: the rolling-12mo total and YoY are based on honest accounting (12 months of data, even though some of those months arrive as a 2-month sum), but a per-month series for those years is unavailable from GACC; questions like "what was the Feb 2024 single-month figure?" can''t be answered from the China side without a separate Eurostat / HMRC cross-check. detail.totals.jan_feb_combined_years lists which years contributed a cumulative chunk to this finding''s window.',
   ARRAY['gacc_bilateral_aggregate_yoy', 'gacc_bilateral_aggregate_yoy_import',
         'gacc_aggregate_yoy', 'gacc_aggregate_yoy_import'])
ON CONFLICT (code) DO NOTHING;
