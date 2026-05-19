-- 2026-05-19 — final piece of the release-184 recurrence cleanup:
-- repair the GACC aggregate / bilateral findings supersede chain so
-- the cycle's editorial Tier 1 diff doesn't dredge up the
-- corrupt-and-back round-trip as a wall of direction flips.
--
-- Context: the data was corrected by prior migrations (parser fix +
-- v=2/v=4 obs delete + v=3 obs delete + analyser DISTINCT-ON dedup).
-- The findings table now carries, for every affected (subkind,
-- partner, window) natural key, a chain like:
--
--   pre_incident_finding (was live at start of 2026-05-19)
--     → corrupt_finding (analyser run at 11:49 with v=4 USD page)
--     → intermediate_finding (analyser run at 12:16 with v=1+v=3
--                             double-count)
--     → clean_finding (analyser run at 12:19+ after full cleanup)
--     ... and any further analyser passes that confirmed-or-bumped
--         the same value
--     → live (current)
--
-- briefing_pack/sections/diff.py iterates `superseded_at > prev_at`
-- supersede PAIRS, not start-vs-end values, so the chain produces
-- three significant Tier 1 entries per partner-window: a corrupt-in
-- flip (-0.3% → +283%), a partial-cleanup (+283% → +9.5%), and a
-- final cleanup (+9.5% → -0.3%) — the editorial round-trip from the
-- incident. ~1,120 such entries across the four subkinds.
--
-- The corrupt intermediate findings reference observations that no
-- longer exist (v=2 / v=4 were deleted by the prior migrations) and
-- have no editorial standing — they're operational noise from the
-- incident, not part of the canonical supersede ledger. The audit
-- trail of the incident lives in the migration history
-- (2026-05-14 / 2026-05-19 series) plus the preserved corrupt
-- export folder (`exports/2026-05-19-1149-corrupt-pre-release184-fix/`).
--
-- Repair: for every live finding in the four affected GACC subkinds
-- whose chain crosses 2026-05-19, relink its most-recent
-- pre-2026-05-19 ancestor directly to the live finding (leapfrog),
-- then DELETE all today-created intermediate findings in the chain.
-- After this migration, the Tier 1 comparator sees one supersede
-- pair per partner-window: pre_incident → live, with delta close to
-- zero (the underlying value is unchanged). Genuine editorial
-- movements unrelated to the incident remain untouched (they emit
-- via different subkinds: hs_group_yoy*, mirror_gap*, partner_share).
--
-- This is the one place we step outside strict append-only on the
-- findings table. Justified narrowly: the rows being deleted reference
-- deleted observations and have no readers other than the diff
-- comparator that would surface them as false editorial signal.

BEGIN;

-- Materialise the chain walk so we can run two separate operations
-- (UPDATE then DELETE) against the same set of ancestors.
CREATE TEMP TABLE _chain_walk ON COMMIT DROP AS
WITH RECURSIVE chains AS (
  SELECT id, superseded_by_finding_id, created_at, subkind,
         id AS live_id, 1 AS depth
    FROM findings
   WHERE superseded_at IS NULL
     AND subkind IN (
        'gacc_aggregate_yoy', 'gacc_aggregate_yoy_import',
        'gacc_bilateral_aggregate_yoy', 'gacc_bilateral_aggregate_yoy_import'
     )
  UNION ALL
  SELECT f.id, f.superseded_by_finding_id, f.created_at, f.subkind,
         c.live_id, c.depth + 1
    FROM findings f
    JOIN chains c ON f.superseded_by_finding_id = c.id
   WHERE c.depth < 100  -- defensive; chains are typically depth 2-6
)
SELECT * FROM chains;

-- 1. Repair: each pre-incident ancestor (latest pre-2026-05-19 row in
--    its chain) now points directly to the live finding.
WITH pre_incidents AS (
  SELECT DISTINCT ON (live_id)
         id AS pre_id, live_id
    FROM _chain_walk
   WHERE id != live_id
     AND created_at < '2026-05-19'
   ORDER BY live_id, created_at DESC
)
UPDATE findings f
   SET superseded_by_finding_id = pi.live_id
  FROM pre_incidents pi
 WHERE f.id = pi.pre_id;

-- 2. Delete today-created intermediates. (Pre-2026-05-19 ancestors
--    older than the pre_incident row are left untouched — their
--    chain is still consistent, they continue pointing to their
--    original successor which now points forward to live.)
DELETE FROM findings
 WHERE id IN (
   SELECT id FROM _chain_walk
    WHERE id != live_id
      AND created_at >= '2026-05-19'
 );

COMMIT;
