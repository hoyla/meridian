-- 2026-06-22 (c) — set the EV-battery group's reader-facing display_name.
--
-- Follows migrations/2026-06-22b (which added the display_name column but set
-- no values). `name` = 'EV batteries (Li-ion)' is the stable internal key:
-- findings snapshot it into detail.group.name and analysers + ~100 tests key
-- off it, so it stays unchanged. display_name is the journalist-editable,
-- reader-facing label, resolved COALESCE(display_name, name) at render time
-- (db.group_display_names) so the displayed string can change without touching
-- the key. Mirrors the schema.sql seed UPDATE so a fresh DB and a migrated DB
-- agree.
--
-- Idempotent: the UPDATE only matches the one named row; safe to re-run.

UPDATE hs_groups
   SET display_name = 'Lithium-ion accumulators (HS 850760)'
 WHERE name = 'EV batteries (Li-ion)';
