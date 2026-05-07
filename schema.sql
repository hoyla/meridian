-- Initial schema for the gacc project.
-- Apply with: psql $DATABASE_URL < schema.sql
-- Move to Alembic on the first schema change.

CREATE TABLE scrape_runs (
    id              BIGSERIAL PRIMARY KEY,
    source_url      TEXT        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at        TIMESTAMPTZ,
    status          TEXT        NOT NULL CHECK (status IN ('running', 'success', 'failed', 'no_change')),
    http_status     INT,
    error_message   TEXT,
    notes           JSONB
);
CREATE INDEX idx_scrape_runs_url_started ON scrape_runs (source_url, started_at DESC);

CREATE TABLE source_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    scrape_run_id   BIGINT      NOT NULL REFERENCES scrape_runs(id),
    url             TEXT        NOT NULL,
    content_type    TEXT,
    content_sha256  TEXT        NOT NULL,
    content_bytes   BYTEA       NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_snapshots_url ON source_snapshots (url, fetched_at DESC);
CREATE INDEX idx_snapshots_sha ON source_snapshots (content_sha256);

CREATE TABLE releases (
    id              BIGSERIAL PRIMARY KEY,
    period          DATE        NOT NULL,        -- e.g. 2026-04-01 for April 2026
    release_kind    TEXT        NOT NULL,        -- 'preliminary' | 'monthly' | 'revised'
    source_url      TEXT        NOT NULL,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (period, release_kind, source_url)
);

CREATE TABLE observations (
    id              BIGSERIAL PRIMARY KEY,
    release_id      BIGINT      NOT NULL REFERENCES releases(id),
    scrape_run_id   BIGINT      NOT NULL REFERENCES scrape_runs(id),
    -- Dimensions (nullable so we can model different table shapes)
    flow            TEXT,                        -- 'import' | 'export' | 'total'
    partner_country TEXT,
    hs_code         TEXT,
    commodity_label TEXT,
    -- Measures
    value_usd       NUMERIC,
    quantity        NUMERIC,
    quantity_unit   TEXT,
    -- Provenance + versioning
    source_row      JSONB       NOT NULL,        -- raw parsed row, for audit
    version_seen    INT         NOT NULL DEFAULT 1,
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_obs_release ON observations (release_id);
CREATE INDEX idx_obs_dims ON observations (flow, partner_country, hs_code, release_id);

CREATE TABLE findings (
    id              BIGSERIAL PRIMARY KEY,
    scrape_run_id   BIGINT      NOT NULL REFERENCES scrape_runs(id),
    kind            TEXT        NOT NULL,        -- 'anomaly' | 'llm_topline'
    subkind         TEXT,                        -- 'zscore' | 'yoy' | 'mom' | 'rank_shift' | 'narrative'
    observation_ids BIGINT[],                    -- supporting rows for provenance
    score           NUMERIC,
    title           TEXT,
    body            TEXT,
    detail          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_findings_run ON findings (scrape_run_id);
CREATE INDEX idx_findings_kind ON findings (kind, subkind);
