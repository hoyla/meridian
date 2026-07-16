-- 2026-07-16 — retire the duplicate "Lithium hydroxide (battery-grade)" group.
--
-- Two live groups both cover HS 282520:
--   * "Lithium hydroxide (battery-grade)"          ['282520%']            (dup)
--   * "Lithium chemicals (carbonate + hydroxide)"  ['283691%','282520%']  (keep)
--
-- How the duplicate arose: the original broad "Lithium chemicals (carbonate +
-- hydroxide)" group was renamed IN PLACE to "Lithium hydroxide (battery-grade)"
-- and its pattern set tightened to 282520 only (Phase 5 — the cell-grade cut
-- with the cleaner EV-supply-chain story; see dev_notes/history.md). Later the
-- Q2 expansion (migration 2026-06-22d) RE-INSERTED "Lithium chemicals
-- (carbonate + hydroxide)" as a fresh row — ON CONFLICT (name) DO NOTHING
-- didn't catch it, because the old name no longer existed. So the DB ended up
-- with both: the narrower 282520-only survivor of the rename, plus the
-- re-added broad carbonate+hydroxide group. Both emit active hs_group findings,
-- and 282520 is double-listed in Sector detail — one badged with the EV
-- supply-chain / export-control themes, one not.
--
-- Which one wins: keep the BROADER "Lithium chemicals (carbonate + hydroxide)"
-- (283691 carbonate + 282520 oxide/hydroxide). It is the one schema.sql seeds,
-- the one wired into the "EV supply chain" and "China export-control regime"
-- themes (labels.py), and the fuller story — LFP (lithium-carbonate) chemistry
-- now dominates Chinese battery output, so a hydroxide-only group understates
-- the flow. This reverses the Phase 5 narrowing deliberately.
--
-- Findings under the retiring group are superseded first (history preserved —
-- journalism principle 4; and so they neither orphan per
-- tests/test_orphan_findings.py nor linger in mover queries), then the row is
-- deleted. Same shape as the 2026-06-22b "Wind turbine components" retirement.
--
-- schema.sql needs NO change: it already seeds only the survivor. This migration
-- exists solely to bring an already-live DB (which carries the rename artefact)
-- back in line with schema.sql.
--
-- Idempotent: the supersede UPDATE only matches not-yet-superseded rows; the
-- DELETE only matches a row that still exists. The DELETE is pinned to the
-- exact name AND the created_by tag from the original 2026-05-09 seed, so a
-- journalist's later same-named group can't be removed by surprise. Safe to
-- re-run, and a no-op on a fresh DB built from schema.sql (which never had the
-- duplicate).

UPDATE findings
   SET superseded_at = now()
 WHERE superseded_at IS NULL
   AND (subkind LIKE 'hs_group_yoy%' OR subkind LIKE 'hs_group_trajectory%')
   AND detail->'group'->>'name' = 'Lithium hydroxide (battery-grade)';

DELETE FROM hs_groups
 WHERE name = 'Lithium hydroxide (battery-grade)'
   AND created_by = 'claude_2026_05_09_current_affairs_revised';
