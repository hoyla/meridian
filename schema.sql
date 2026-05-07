-- Initial schema for the gacc project.
-- Apply with: psql $DATABASE_URL < schema.sql
-- Move to Alembic on the first schema change after we have data we care about.

CREATE TABLE scrape_runs (
    id              BIGSERIAL PRIMARY KEY,
    source_url      TEXT        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at        TIMESTAMPTZ,
    status          TEXT        NOT NULL CHECK (status IN ('running', 'success', 'failed', 'no_change', 'no_parser')),
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
    id                BIGSERIAL PRIMARY KEY,
    section_number    INT         NOT NULL,
    currency          TEXT        NOT NULL,        -- 'CNY' | 'USD'
    period            DATE        NOT NULL,        -- anchor date, 1st of month
    release_kind      TEXT        NOT NULL,        -- 'preliminary' | 'monthly' | 'revised'
    -- Display + provenance
    description       TEXT,                        -- e.g. "China's Total Export & Import Values by Country/Region"
    title             TEXT,                        -- full bulletin title from .atcl-ttl
    source_url        TEXT        NOT NULL,
    publication_date  DATE,                        -- when GACC published this page (.atcl-date)
    unit              TEXT,                        -- e.g. "CNY 100 Million"
    excel_url         TEXT,
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (section_number, currency, period, release_kind)
);
CREATE INDEX idx_releases_period ON releases (period DESC);

CREATE TABLE observations (
    id                  BIGSERIAL PRIMARY KEY,
    release_id          BIGINT      NOT NULL REFERENCES releases(id),
    scrape_run_id       BIGINT      NOT NULL REFERENCES scrape_runs(id),
    -- Period window for this observation
    period_kind         TEXT        NOT NULL,      -- 'monthly' | 'ytd'
    -- Dimensions
    flow                TEXT,                      -- 'export' | 'import' | 'total'
    partner_country     TEXT,
    partner_label_raw   TEXT,
    partner_indent      INT,
    partner_is_subset   BOOLEAN,
    hs_code             TEXT,
    commodity_label     TEXT,
    -- Measures
    value_amount        NUMERIC,
    value_currency      TEXT,                      -- 'CNY' | 'USD'
    quantity            NUMERIC,
    quantity_unit       TEXT,
    -- Provenance + versioning (within a single release)
    source_row          JSONB       NOT NULL,
    version_seen        INT         NOT NULL DEFAULT 1,
    inserted_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_obs_release ON observations (release_id);
CREATE INDEX idx_obs_dims ON observations (release_id, flow, period_kind, partner_country, hs_code);

CREATE TABLE findings (
    id              BIGSERIAL PRIMARY KEY,
    scrape_run_id   BIGINT      NOT NULL REFERENCES scrape_runs(id),
    kind            TEXT        NOT NULL,          -- 'anomaly' | 'llm_topline'
    subkind         TEXT,                          -- 'zscore' | 'yoy' | 'mom' | 'rank_shift' | 'narrative'
    observation_ids BIGINT[],                      -- supporting rows for provenance
    score           NUMERIC,
    title           TEXT,
    body            TEXT,
    detail          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_findings_run ON findings (scrape_run_id);
CREATE INDEX idx_findings_kind ON findings (kind, subkind);
