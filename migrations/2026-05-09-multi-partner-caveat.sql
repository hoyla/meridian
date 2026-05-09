-- Migration: add the `multi_partner_sum` caveat row.
-- Phase 2.3 of dev_notes/roadmap-2026-05-09.md.
--
-- Apply with:
--   psql $DATABASE_URL < migrations/2026-05-09-multi-partner-caveat.sql
--   psql $GACC_TEST_DATABASE_URL < migrations/2026-05-09-multi-partner-caveat.sql

INSERT INTO caveats (code, summary, detail, applies_to) VALUES
  ('multi_partner_sum',
   'Eurostat side sums across multiple partner_country codes (e.g. CN + HK)',
   'The EU import side of this finding sums across multiple Eurostat partner_country codes — typically CN + HK to capture Hong-Kong-routed Chinese trade (~15% of China''s exports). The aggregate view is more inclusive of de-facto Chinese trade than a CN-only view, but is not directly comparable to single-partner findings. If you compare findings emitted with different partner lists, you are comparing different methodological choices, not different data realities. detail.eurostat.partners_summed records the exact list used.',
   ARRAY['mirror_gap'])
ON CONFLICT (code) DO NOTHING;
