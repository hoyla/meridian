-- 2026-05-19 — final step of the release-184 recurrence cleanup.
--
-- After the 11:49 corrupt export was identified and the data layer
-- was cleaned (see migrations/2026-05-19-reject-mismatched-gacc-
-- currency-unit-pages.sql and migrations/2026-05-19-cleanup-duplicate-
-- good-version-on-release-184.sql), --periodic-run --force was re-run
-- at 13:00 to produce a clean export. The 13:00 export's Tier 1
-- ("what's new this cycle") compared against the 11:49 export as its
-- predecessor (`SELECT MAX(generated_at) FROM brief_runs` at
-- briefing_pack/sections/diff.py:40) and dutifully reported the
-- cleanup as ~1,120 "material shifts" — the inverse of the morning's
-- wall of corruption-driven flips. The deterministic findings and
-- 04_Data.xlsx were correct; only the Tier 1 diff surface was
-- showing the artefact.
--
-- Treatment, consistent with the spirit of the append-only audit
-- trail: both export folders are preserved on disk under explicit
-- "-corrupt-pre-release184-fix" / "-tier1-vs-corrupt-predecessor"
-- suffixes so the incident is auditable. But the corresponding
-- brief_runs rows are removed so that the NEXT --periodic-run reaches
-- past these two non-canonical cycles to the 2026-05-15 export as
-- its editorial predecessor and produces a Tier 1 that diffs against
-- a clean baseline.
--
-- This is the one place we step away from strict append-only on
-- brief_runs — there's no `superseded_at` column on this table to
-- mark the rows non-canonical structurally, and a single-incident
-- cleanup doesn't justify a schema change. If the pattern recurs, the
-- right follow-up is to add brief_runs.superseded_at and switch the
-- diff comparator to filter on it.

BEGIN;

DELETE FROM brief_runs
 WHERE id IN (19, 20)
   AND output_path IN (
       'exports/2026-05-19-1149/03_Findings.md',
       'exports/2026-05-19-1300/03_Findings.md'
   );

COMMIT;
