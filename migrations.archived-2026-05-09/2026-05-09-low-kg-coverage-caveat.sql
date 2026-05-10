-- Migration: add the `low_kg_coverage` caveat row.
-- Phase 1.5 of dev_notes/history.md.
--
-- Apply with:
--   psql $DATABASE_URL < migrations/2026-05-09-low-kg-coverage-caveat.sql
--   psql $GACC_TEST_DATABASE_URL < migrations/2026-05-09-low-kg-coverage-caveat.sql

INSERT INTO caveats (code, summary, detail, applies_to) VALUES
  ('low_kg_coverage',
   'Volume / unit-price decomposition suppressed (kg coverage too low)',
   'Less than 80% of the value_eur in the rolling 12-month window for this group is backed by an actual quantity_kg measurement — the rest is rows whose primary supplementary unit was something other than kg (pieces, litres, etc.). A unit price computed as eur/kg over only the kg-reporting subset, then compared against value YoY, would mislead. The decomposition (volume- vs. price-driven) and the per-kg unit price are therefore omitted from this finding. The value YoY itself remains valid.',
   ARRAY['hs_group_yoy', 'hs_group_yoy_export'])
ON CONFLICT (code) DO NOTHING;
