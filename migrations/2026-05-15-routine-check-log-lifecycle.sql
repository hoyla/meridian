-- 2026-05-15 — extend routine_check_log's result enum with two run-level
-- lifecycle values: 'started' (first thing the Routine logs, after venv
-- activate) and 'completed' (last thing the Routine logs, after the
-- orchestrator step). Used with source='_routine' by convention.
--
-- Motivation: without bookend events, the log can't distinguish "Routine
-- fired successfully but everything was a no-op" from "Routine errored
-- mid-run, missed later steps" from "Routine never even reached step 1".
-- Per-source rows are absent in all three cases; only a started-without-
-- completed pair makes the mid-run failure unambiguous.
--
-- An 'error' result on the _routine source still means an explicit
-- orchestrator failure (caught and reported by the prompt).

ALTER TABLE routine_check_log
    DROP CONSTRAINT routine_check_log_result_check;

ALTER TABLE routine_check_log
    ADD CONSTRAINT routine_check_log_result_check
    CHECK (result IN (
        'new_data', 'no_change', 'not_yet_eligible', 'error',
        'started', 'completed'
    ));
