-- 2026-05-14 — fix release 184 (June 2025 GACC) unit corruption + add
-- structural constraint preventing recurrence.
--
-- Symptom (uncovered while writing provenance file for Lisa O'Carroll on
-- 2026-05-14): release 184 (June 2025 GACC section-4 CNY edition) has
-- `currency='CNY'` but `unit='USD1 Million'`, and 180 observations on
-- the release are duplicated — a v=1 row holding the CNY value and a
-- v=2 row holding the USD value. The downstream analyser
-- `_gacc_aggregate_per_period_totals` reads release.unit to decide how
-- to interpret each observation, then sums all matching observations
-- per period — so the v=1 (CNY value, mis-interpreted as USD-Million)
-- and v=2 (USD value, correctly interpreted as USD-Million) get summed
-- into a number that's ~6-7% inflated for June 2025 across every
-- bilateral and aggregate finding.
--
-- Root cause: GACC's June 2025 release page apparently had a
-- self-inconsistency where the title said "(in CNY)" but the page's
-- "Unit:" annotation said "USD1 Million" (and the excel_url linked to
-- the USD edition). Two issues compounded:
--   1. parse.py reads "Unit:" verbatim from the page without
--      cross-checking it against the title-derived currency.
--   2. db.find_or_create_gacc_release uses `unit = COALESCE(EXCLUDED.unit,
--      releases.unit)` on conflict, so a re-scrape with bad unit silently
--      overwrites a previously-correct value.
--
-- Fix:
--   1. Correct release 184's unit to the canonical 'CNY 100 Million'.
--   2. Delete the 180 spurious v=2 observations. These are not editorially
--      a revision (the v=1 values are the canonical CNY readings of the
--      same release); they're a parser-confusion artefact and shouldn't
--      have been inserted. Findings referencing them via the obs_ids
--      array will have stale pointers until re-run; after re-running the
--      analysers the new findings supersede with clean obs_ids.
--   3. Leave excel_url alone — it points to a wrong file but is an audit
--      artefact, not data the analyser uses, and reconstructing the
--      correct URL needs a fresh scrape.
--   4. Add CHECK constraint preventing any future GACC release row from
--      committing with an inconsistent (currency, unit) pair.
--
-- After applying this migration, re-run the analysers to regenerate the
-- bilateral / aggregate / hs_group findings affected by the inflated
-- June 2025 contributions:
--   python scrape.py --analyse gacc-bilateral-aggregate-yoy --flow 1
--   python scrape.py --analyse gacc-bilateral-aggregate-yoy --flow 2
--   python scrape.py --analyse gacc-aggregate-yoy --flow 1
--   python scrape.py --analyse gacc-aggregate-yoy --flow 2

BEGIN;

-- 1. Correct the release row.
UPDATE releases
   SET unit = 'CNY 100 Million'
 WHERE id = 184
   AND source = 'gacc'
   AND currency = 'CNY'
   AND unit = 'USD1 Million';

-- 2. Delete spurious v=2 observations on release 184.
DELETE FROM observations
 WHERE release_id = 184
   AND version_seen > 1;

-- 3. Add structural constraint. For GACC releases, the unit string must
--    be the canonical form for the currency, or NULL. Other sources
--    (Eurostat, HMRC) keep both currency and unit NULL and are
--    unaffected. Constraint is NOT VALID first so this migration can
--    run idempotently; then VALIDATE separately so the validation pass
--    confirms every existing row is clean.
ALTER TABLE releases
  DROP CONSTRAINT IF EXISTS releases_gacc_unit_consistent;

ALTER TABLE releases
  ADD CONSTRAINT releases_gacc_unit_consistent CHECK (
        source <> 'gacc'
     OR unit IS NULL
     OR (currency = 'CNY' AND unit = 'CNY 100 Million')
     OR (currency = 'USD' AND unit = 'USD1 Million')
  ) NOT VALID;

ALTER TABLE releases
  VALIDATE CONSTRAINT releases_gacc_unit_consistent;

COMMIT;
