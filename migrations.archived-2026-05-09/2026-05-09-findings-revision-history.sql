-- Migration: idempotency + revision history for findings.
-- Phase 1.1 of dev_notes/history.md.
--
-- Apply with:
--   psql $DATABASE_URL < migrations/2026-05-09-findings-revision-history.sql
--   psql $GACC_TEST_DATABASE_URL < migrations/2026-05-09-findings-revision-history.sql
--
-- Idempotent: re-runnable. Skips column / index creation if already present.

BEGIN;

-- 1. Add columns (skip if already present).
ALTER TABLE findings
    ADD COLUMN IF NOT EXISTS natural_key_hash         TEXT,
    ADD COLUMN IF NOT EXISTS value_signature          TEXT,
    ADD COLUMN IF NOT EXISTS superseded_at            TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS superseded_by_finding_id BIGINT,
    ADD COLUMN IF NOT EXISTS last_confirmed_at        TIMESTAMPTZ NOT NULL DEFAULT now();

-- 2. FK on the supersede pointer (separate from ADD COLUMN so we can guard it).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
         WHERE constraint_name = 'findings_superseded_by_finding_id_fkey'
           AND table_name = 'findings'
    ) THEN
        ALTER TABLE findings
            ADD CONSTRAINT findings_superseded_by_finding_id_fkey
            FOREIGN KEY (superseded_by_finding_id) REFERENCES findings(id);
    END IF;
END
$$;

-- 3. Indexes (CREATE IF NOT EXISTS — Postgres 9.5+).
CREATE UNIQUE INDEX IF NOT EXISTS uq_findings_active_natural_key
    ON findings (natural_key_hash) WHERE superseded_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_findings_supersede_chain
    ON findings (superseded_by_finding_id) WHERE superseded_by_finding_id IS NOT NULL;

COMMIT;
