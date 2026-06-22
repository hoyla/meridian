-- 2026-06-22 (b) — add hs_groups.display_name + retire the conflating
-- "Wind turbine components" group.
--
-- Companion to dev_notes/2026-06-22-lisa-sector-coverage-questions.md and the
-- material-vs-application taxonomy audit. Brings a live DB in line with the
-- schema.sql edits made in the same change.
--
-- 1. display_name column. `name` is the stable internal key (findings snapshot
--    it into detail.group.name; analysers + ~100 tests key off it).
--    display_name is a journalist-editable, reader-facing label, resolved as
--    COALESCE(display_name, name) at render time (db.group_display_names). NO
--    values are set here — this only adds the column. The editorial display
--    renames (e.g. EV batteries -> Lithium-ion accumulators) land in a later
--    change, once the render layer reads display_name.
--
-- 2. Retire "Wind turbine components". Two of its three HS patterns (850300
--    generator/motor parts, 730820 iron/steel towers & masts) are NOT
--    wind-specific, so the group conflated wind with general electrical/steel
--    trade. The precise "Wind generating sets only" (850231) stays; a new
--    "Wind power" theme (labels.py) gathers the wind-relevant groups as an
--    overlapping lens. Active findings under the group are superseded (history
--    preserved — journalism principle 4), then the row is deleted, following
--    the Phase 6.5 group-retirement precedent.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS; the supersede UPDATE only matches
-- rows not already superseded; the DELETE only matches a row that still
-- exists. Safe to re-run.

ALTER TABLE hs_groups ADD COLUMN IF NOT EXISTS display_name TEXT;

-- Supersede any active hs_group findings under the retiring group so they
-- neither orphan (cf. tests/test_orphan_findings.py) nor linger in mover
-- queries. Re-run-safe: superseded rows are excluded by the WHERE.
UPDATE findings
   SET superseded_at = now()
 WHERE superseded_at IS NULL
   AND (subkind LIKE 'hs_group_yoy%' OR subkind LIKE 'hs_group_trajectory%')
   AND detail->'group'->>'name' = 'Wind turbine components';

DELETE FROM hs_groups WHERE name = 'Wind turbine components';
