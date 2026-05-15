-- 2026-05-15 — routine_check_log table for the daily Routine's per-source
-- check telemetry. Captures the "no new data today" and "not yet eligible"
-- cases as well as successful fetches, so `python scrape.py --source-status`
-- can show what the Routine has been doing without scrolling through chat
-- history.
--
-- Population is via `python scrape.py --log-check ...` — one call per source
-- per Routine fire. The `.claude/scheduled-tasks/meridian-daily-periodic-run/`
-- SKILL.md is the only writer. The pipeline (`periodic.run_periodic`) does
-- not write rows itself — its idempotency check is on Eurostat alone and
-- happens regardless of how the Routine got there.
--
-- Debug-only surface: no journalist-facing artefact reads from this table.

CREATE TABLE IF NOT EXISTS routine_check_log (
    id               BIGSERIAL PRIMARY KEY,
    source           TEXT        NOT NULL,            -- 'eurostat' | 'hmrc' | 'gacc' (extensible)
    checked_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    result           TEXT        NOT NULL,
    candidate_period DATE,                            -- the period the Routine attempted to fetch (Eurostat/HMRC); NULL for GACC index walk
    notes            TEXT,                            -- short human note, e.g. 'walked 9 indexes, no new releases'
    error            TEXT,                            -- error text when result='error'
    duration_ms      INT,
    CHECK (result IN ('new_data', 'no_change', 'not_yet_eligible', 'error'))
);
CREATE INDEX IF NOT EXISTS idx_routine_check_log_source_checked
    ON routine_check_log (source, checked_at DESC);
