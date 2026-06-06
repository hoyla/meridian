-- 2026-06-02 — add the expectation axis to routine_check_log.
--
-- Splits the old single "result" signal into two orthogonal axes:
--   result      — the objective outcome (new_data / no_change / error)
--   expectation — derived from the source's publication calendar
--                 (none_expected / due / overdue)
-- so a missing release past its scheduled date reads as no_change × overdue
-- (the one row a human should look at), distinct from a quiet expected gap
-- (no_change × none_expected) and an early arrival (new_data × none_expected).
-- See dev_notes/2026-06-02-eurostat-expectation-axis-design.md and
-- release_calendar.py.
--
-- This replaces the old hardcoded "5 weeks past period close" fetch-gate that
-- logged result='not_yet_eligible'. The application no longer writes that
-- value (we always probe now — fetching is idempotent and harmless), but the
-- result CHECK below deliberately STILL permits it: this migration is purely
-- additive and must not invalidate historical rows (journalism principle 4 —
-- append-only, never rewrite the audit trail).
--
-- expectation is NULL for: gacc rows (index walk, no candidate-period
-- concept), the _routine lifecycle bookends, and any pre-migration row.

ALTER TABLE routine_check_log
    ADD COLUMN IF NOT EXISTS expectation TEXT;

ALTER TABLE routine_check_log
    DROP CONSTRAINT IF EXISTS routine_check_log_expectation_check;
ALTER TABLE routine_check_log
    ADD CONSTRAINT routine_check_log_expectation_check
    CHECK (expectation IS NULL OR expectation IN ('none_expected', 'due', 'overdue'));
