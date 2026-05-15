-- 2026-05-15 — three audit-log tables per the new logging policy
-- (dev_notes/logging-policy.md, agreed 2026-05-15). Each captures a
-- silent system decision the project was previously losing to terminal
-- scrollback.

-- llm_rejection_log: every LLM-framing output that fails parse or
-- numeric verification. Today the verifier silently rejects and logs a
-- warning; without persistence, the rejected prose is gone once
-- scrollback rolls off.
CREATE TABLE IF NOT EXISTS llm_rejection_log (
    id              BIGSERIAL PRIMARY KEY,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    scrape_run_id   BIGINT REFERENCES scrape_runs(id),
    cluster_name    TEXT NOT NULL,
    model           TEXT,
    stage           TEXT NOT NULL,
    reason          TEXT NOT NULL,
    detail          TEXT,
    raw_output      TEXT,
    closest_fact_path  TEXT,
    closest_fact_value DOUBLE PRECISION,
    CHECK (stage IN ('parse', 'validate'))
);
CREATE INDEX IF NOT EXISTS idx_llm_rejection_log_rejected_at
    ON llm_rejection_log (rejected_at DESC);
CREATE INDEX IF NOT EXISTS idx_llm_rejection_log_cluster
    ON llm_rejection_log (cluster_name);

-- periodic_run_log: one row per `--periodic-run` invocation, regardless
-- of whether it wrote a new export or no-op'd. Captures the decision
-- shape (action_taken, reason, data_period) and the per-analyser counts
-- so post-hoc "what did the Routine actually do across the last N
-- cycles" is queryable. Pairs with brief_runs (which only has rows for
-- cycles that wrote an export) — most periodic_run_log rows will be
-- no-ops.
CREATE TABLE IF NOT EXISTS periodic_run_log (
    id              BIGSERIAL PRIMARY KEY,
    invoked_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    action_taken    BOOLEAN NOT NULL,
    reason          TEXT NOT NULL,
    data_period     DATE,
    findings_path   TEXT,
    analyser_counts JSONB,
    duration_ms     INT,
    forced          BOOLEAN NOT NULL DEFAULT FALSE,
    skip_llm        BOOLEAN NOT NULL DEFAULT FALSE,
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_periodic_run_log_invoked_at
    ON periodic_run_log (invoked_at DESC);

-- findings_emit_log: one row per `detect_X()` analyser invocation, with
-- the emit-counts dict the analyser computed (`new`, `confirmed`,
-- `superseded`, plus various `skipped_*` keys). Covers both periodic-
-- run cycles AND ad-hoc CLI runs — the scrape_runs row is the natural
-- key linking the emit log back to whichever analyser_run context fired
-- it.
CREATE TABLE IF NOT EXISTS findings_emit_log (
    id               BIGSERIAL PRIMARY KEY,
    logged_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    scrape_run_id    BIGINT REFERENCES scrape_runs(id),
    analyser_method  TEXT NOT NULL,
    subkind          TEXT NOT NULL,
    comparison_scope TEXT,
    flow             INT,
    counts           JSONB NOT NULL,
    duration_ms      INT
);
CREATE INDEX IF NOT EXISTS idx_findings_emit_log_logged_at
    ON findings_emit_log (logged_at DESC);
CREATE INDEX IF NOT EXISTS idx_findings_emit_log_subkind
    ON findings_emit_log (subkind, logged_at DESC);
