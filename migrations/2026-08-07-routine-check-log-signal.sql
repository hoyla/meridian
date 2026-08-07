-- 2026-08-07 — add a machine-readable `signal` to routine_check_log.
--
-- The GACC probe can end a run in a state that is not an error and not new
-- data, but IS actionable: the Chinese site has published the month and only
-- the byte route is missing (published-awaiting-bytes), or the JS-challenge
-- WAF is blocking discovery outright so we cannot even see whether it is out.
-- Both currently log `result = 'no_change'` with an explanatory note, so the
-- chat notifier — which fires on new_data, newly-overdue and newly-errored —
-- stays silent through exactly the window where someone needs to act
-- (2026-08-07: July's release was live on the Chinese site ~14h before the
-- English one, and only a hand-driven browser step got the bytes).
--
-- `notes` already carries this in prose, but an alerting path must not key on
-- display copy: a copy edit would silently disable the alert. This column is
-- the stable, testable handle notify.py matches on.
--
-- Purely additive and nullable: every historical row keeps signal = NULL,
-- which is correct — the state was not distinguished before this migration
-- (journalism principle 4 — append-only, never rewrite the audit trail).
--
-- Vocabulary is mirrored in routine_log.VALID_SIGNALS (write guard); keep the
-- two in sync. Deliberately NOT a CHECK constraint on the table: probe
-- signals are expected to grow, and a new one must never be able to make an
-- audit-trail INSERT fail — the write guard rejects typos at the app layer
-- where the failure is loud and recoverable.

ALTER TABLE routine_check_log
    ADD COLUMN IF NOT EXISTS signal TEXT;

COMMENT ON COLUMN routine_check_log.signal IS
    'Machine-readable probe state for alerting, orthogonal to `result`. '
    'NULL = nothing beyond result/expectation. See routine_log.VALID_SIGNALS.';

-- No new index needed: the notifier's "latest two rows per source" lookup is
-- already served by idx_routine_check_log_source_checked (source, checked_at
-- DESC) in schema.sql.
