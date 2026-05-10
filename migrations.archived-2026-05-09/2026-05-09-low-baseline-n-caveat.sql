-- Migration: add the `low_baseline_n` caveat row.
-- Phase 1.4 of dev_notes/roadmap-2026-05-09.md.
--
-- Apply with:
--   psql $DATABASE_URL < migrations/2026-05-09-low-baseline-n-caveat.sql
--   psql $GACC_TEST_DATABASE_URL < migrations/2026-05-09-low-baseline-n-caveat.sql
--
-- Idempotent via ON CONFLICT DO NOTHING.

INSERT INTO caveats (code, summary, detail, applies_to) VALUES
  ('low_baseline_n',
   'Z-score baseline is below the confidence threshold',
   'The z-score for this finding rests on fewer than 6 prior periods (the threshold for one full default rolling window). Stdev estimates are noisy at this baseline length: a z=2.0 from a 4-point baseline does not carry the same weight as a z=2.0 from a 12-point baseline. The |z| value is mathematically correct but the editorial confidence is limited until the partner has accumulated more history. Re-evaluate when the baseline grows past the threshold.',
   ARRAY['mirror_gap_zscore'])
ON CONFLICT (code) DO NOTHING;
