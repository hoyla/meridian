-- Migration: add `partial_window` + `cn8_revision` caveat rows.
-- Phase 2.7 + 2.8 of dev_notes/history.md.
--
-- Apply with:
--   psql $DATABASE_URL < migrations/2026-05-09-partial-window-and-cn8-revision-caveats.sql
--   psql $GACC_TEST_DATABASE_URL < migrations/2026-05-09-partial-window-and-cn8-revision-caveats.sql

INSERT INTO caveats (code, summary, detail, applies_to) VALUES
  ('partial_window',
   'YoY computed on a 24-month window with 1 missing month',
   'This hs_group_yoy finding rests on a 24-month window where 1 month is missing from Eurostat (most commonly the most-recent month, which lags publication by 6-8 weeks). The current/prior totals sum what was available; the YoY percentage is computed against partial-window denominators. detail.totals.missing_months_current and missing_months_prior list which months were absent. Re-check the finding once the missing month has been ingested.',
   ARRAY['hs_group_yoy', 'hs_group_yoy_export']),
  ('cn8_revision',
   'YoY window spans a Eurostat CN8 nomenclature revision boundary',
   'Eurostat revises the Combined Nomenclature (CN8) annually, effective each January. When a 24-month YoY window spans a calendar-year boundary (which is true for most of them) the LIKE patterns this analyser uses may capture a subtly different commodity scope pre- and post-revision. Most revisions are minor — a code split, a new sub-heading, a description change — but for stories that rest on a precise YoY figure for a specific HS-CN8 code, verify the code definition didn''t change at the year boundary. A full concordance table is roadmap Phase 4 work; this caveat is the cheap-honesty interim flag.',
   ARRAY['hs_group_yoy', 'hs_group_yoy_export'])
ON CONFLICT (code) DO NOTHING;
