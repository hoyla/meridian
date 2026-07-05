-- 2026-07-05 — add the track axis to periodic_run_log.
--
-- The GACC update page (dev_notes/2026-07-05-gacc-update-page-design.md)
-- introduces a second release track: `periodic.run_gacc_update()` fires on a
-- new GACC reference month, independently of the main Eurostat-gated cycle
-- (`periodic.run_periodic()`). Both orchestrators log every invocation here —
-- action, no-op, or crash (the F4 contract) — so the history needs to say
-- WHICH cycle each row belongs to:
--   track = 'main' — the Full-briefing cycle (--periodic-run)
--   track = 'gacc' — the GACC-only update cycle (--gacc-update-run)
--
-- Purely additive: DEFAULT 'main' back-labels every historical row, which is
-- correct — the gacc track did not exist before this migration (journalism
-- principle 4 — append-only, never rewrite the audit trail).
--
-- Vocabulary is mirrored in periodic_run_log.VALID_TRACKS (write guard);
-- keep the two in sync.

ALTER TABLE periodic_run_log
    ADD COLUMN IF NOT EXISTS track TEXT NOT NULL DEFAULT 'main';

ALTER TABLE periodic_run_log
    DROP CONSTRAINT IF EXISTS periodic_run_log_track_check;
ALTER TABLE periodic_run_log
    ADD CONSTRAINT periodic_run_log_track_check
    CHECK (track IN ('main', 'gacc'));
