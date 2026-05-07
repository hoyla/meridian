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
    source            TEXT        NOT NULL CHECK (source IN ('gacc', 'eurostat')),
    -- GACC-specific (NULL for Eurostat)
    section_number    INT,
    currency          TEXT,                        -- 'CNY' | 'USD' for GACC
    release_kind      TEXT,                        -- 'preliminary' | 'monthly' | 'revised' for GACC
    -- Common
    period            DATE        NOT NULL,        -- anchor date, 1st of month
    description       TEXT,
    title             TEXT,
    source_url        TEXT        NOT NULL,
    publication_date  DATE,
    unit              TEXT,
    excel_url         TEXT,
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Per-source natural identity, expressed as partial unique indexes.
CREATE UNIQUE INDEX uq_releases_gacc
    ON releases (section_number, currency, period, release_kind)
    WHERE source = 'gacc';
CREATE UNIQUE INDEX uq_releases_eurostat
    ON releases (period)
    WHERE source = 'eurostat';
CREATE INDEX idx_releases_period ON releases (period DESC);
CREATE INDEX idx_releases_source ON releases (source, period DESC);

CREATE TABLE observations (
    id                  BIGSERIAL PRIMARY KEY,
    release_id          BIGINT      NOT NULL REFERENCES releases(id),
    scrape_run_id       BIGINT      NOT NULL REFERENCES scrape_runs(id),
    -- Period window
    period_kind         TEXT        NOT NULL,      -- 'monthly' | 'ytd'
    -- Dimensions
    flow                TEXT,                      -- 'export' | 'import' | 'total'
    reporter_country    TEXT,                      -- NULL for GACC (China implicit); ISO-2 EU member state for Eurostat
    partner_country     TEXT,                      -- ISO-2 for Eurostat; free-text label for GACC
    partner_label_raw   TEXT,
    partner_indent      INT,
    partner_is_subset   BOOLEAN,
    hs_code             TEXT,                      -- HS-CN8 from Eurostat (zero-padded 8 chars); NULL for GACC headline rows
    commodity_label     TEXT,                      -- e.g. 'Plastic products' for GACC; NULL for Eurostat (use hs_code)
    -- Measures
    value_amount        NUMERIC,
    value_currency      TEXT,                      -- 'CNY' | 'USD' | 'EUR'
    quantity            NUMERIC,
    quantity_unit       TEXT,
    -- Provenance + versioning (within a single release)
    source_row          JSONB       NOT NULL,
    version_seen        INT         NOT NULL DEFAULT 1,
    inserted_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_obs_release ON observations (release_id);
CREATE INDEX idx_obs_dims ON observations (release_id, flow, period_kind, reporter_country, partner_country, hs_code);
CREATE INDEX idx_obs_hs ON observations (hs_code) WHERE hs_code IS NOT NULL;

-- Journalist-curated product groupings. Empty by default; populated per-investigation.
-- hs_patterns are SQL LIKE patterns matched against observations.hs_code, e.g. {'870380%', '870370%'}
-- for plug-in hybrid passenger cars.
CREATE TABLE hs_groups (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT        NOT NULL UNIQUE,
    description     TEXT,
    hs_patterns     TEXT[]      NOT NULL,
    created_by      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE findings (
    id                  BIGSERIAL PRIMARY KEY,
    scrape_run_id       BIGINT      NOT NULL REFERENCES scrape_runs(id),
    kind                TEXT        NOT NULL,    -- 'anomaly' | 'llm_topline'
    subkind             TEXT,                    -- 'zscore' | 'yoy' | 'mom' | 'rank_shift' | 'mirror_gap' | 'mix_substitution' | 'narrative'
    observation_ids     BIGINT[],                -- supporting rows for provenance
    hs_group_ids        BIGINT[],                -- which group(s) this finding pertains to
    score               NUMERIC,
    title               TEXT,
    body                TEXT,
    detail              JSONB,
    editorial_status    TEXT        NOT NULL DEFAULT 'open'
                        CHECK (editorial_status IN ('open', 'noted', 'investigating', 'published', 'dismissed')),
    editorial_notes     TEXT,
    editorial_updated_at TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_findings_run ON findings (scrape_run_id);
CREATE INDEX idx_findings_kind ON findings (kind, subkind);
CREATE INDEX idx_findings_status ON findings (editorial_status, created_at DESC);
