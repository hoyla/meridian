-- 2026-05-19 — recurrence of the release 184 (June 2025 GACC) unit-
-- mismatch incident, plus the parser-level fix that prevents it
-- coming back.
--
-- Recurrence context: the 2026-05-14 migration cleaned the spurious
-- v=2 observations and added the CHECK constraint
-- `releases_gacc_unit_consistent`. But parse.py at the time still
-- coerced the bad page's "Unit:" annotation to the title-derived
-- canonical before insert (warn-and-continue), which let the daily
-- walker re-fetch the bad URL on 2026-05-17 and re-insert another
-- batch of corrupt cell values as v=2. Today's walk added v=3 (from
-- the good CNY URL) AND v=4 (from the bad USD-content URL). The
-- 2026-05-19 periodic-run picked the highest version_seen per
-- observation, which was v=4 — driving 30 GACC-bilateral findings to
-- show flips from ~-3% to ~+260% YoY on every 12-month window that
-- includes June 2025. See dev_notes (or the spawn-task record) for
-- the diagnostic trail.
--
-- Fix:
--   1. parse.py now raises ValueError when the title-derived
--      currency disagrees with the page's "Unit:" annotation, so the
--      bad URL's scrape lands as status='failed' and no observations
--      get inserted from it going forward. The dedup guard added
--      2026-05-19 retries failed URLs on each walk by design, so the
--      bad URL will keep failing-cheaply (~1 fetch/day) until GACC
--      fixes the page or we add a per-URL kill-list.
--   2. This migration deletes the recurrent v=2 / v=4 observations
--      on release 184 — the v=1 and v=3 rows carry the canonical
--      CNY-100-Million values (358.4 etc.) and remain authoritative.
--   3. Re-run the analysers after applying:
--        python scrape.py --analyse gacc-bilateral-aggregate-yoy --flow 1
--        python scrape.py --analyse gacc-bilateral-aggregate-yoy --flow 2
--        python scrape.py --analyse gacc-aggregate-yoy --flow 1
--        python scrape.py --analyse gacc-aggregate-yoy --flow 2
--      Then `python scrape.py --periodic-run --force` for a clean
--      export.

BEGIN;

DELETE FROM observations
 WHERE release_id = 184
   AND version_seen IN (2, 4);

COMMIT;
