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

-- Eurostat raw rows — one row per CSV line in the bulk file, preserved exactly.
-- The aggregated `observations` rows derived from these reference back via
-- observations.eurostat_raw_row_ids (BIGINT[]). The aggregation method is therefore
-- inspectable: for any observation, you can SELECT the raw rows and re-derive.
CREATE TABLE eurostat_raw_rows (
    id                    BIGSERIAL PRIMARY KEY,
    scrape_run_id         BIGINT      NOT NULL REFERENCES scrape_runs(id),
    period                DATE        NOT NULL,
    reporter              TEXT        NOT NULL,
    partner               TEXT        NOT NULL,
    trade_type            TEXT,
    product_nc            TEXT        NOT NULL,    -- HS-CN8, zero-padded 8 chars
    product_sitc          TEXT,
    product_cpa21         TEXT,
    product_cpa22         TEXT,
    product_bec           TEXT,
    product_bec5          TEXT,
    product_section       TEXT,
    flow                  INT         NOT NULL,    -- 1 = import, 2 = export (Eurostat native code)
    stat_procedure        TEXT,
    suppl_unit            TEXT,
    value_eur             NUMERIC,
    value_nac             NUMERIC,
    quantity_kg           NUMERIC,
    quantity_suppl_unit   NUMERIC,
    inserted_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_eu_raw_period_partner ON eurostat_raw_rows (period, partner, reporter, product_nc);
CREATE INDEX idx_eu_raw_run ON eurostat_raw_rows (scrape_run_id);

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
    source_row          JSONB       NOT NULL,      -- aggregation metadata for Eurostat; raw parsed row for GACC
    eurostat_raw_row_ids BIGINT[],                 -- FK array into eurostat_raw_rows for Eurostat-derived observations
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

-- =============================================================================
-- Lookup tables for cross-source work.
-- These exist to keep normalisation logic transparent (principle: don't bury
-- mappings in code). Each derived/linked record FKs back to rows here.
-- =============================================================================

-- Country-name → ISO-2 mapping per source. Aggregate labels (e.g. "European Union")
-- have iso2 = NULL and aggregate_kind populated. Confidence allows journalists to
-- weigh how sure we are about a mapping when interpreting findings.
CREATE TABLE country_aliases (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT        NOT NULL,        -- 'gacc' | 'eurostat' | etc.
    raw_label       TEXT        NOT NULL,
    iso2            TEXT,                        -- NULL for aggregate labels
    aggregate_kind  TEXT,                        -- 'eu_bloc' | 'asean' | 'rcep' | 'belt_road' | 'region' | 'world' | NULL
    confidence      TEXT        NOT NULL CHECK (confidence IN ('high', 'probable', 'tentative')),
    method          TEXT        NOT NULL,        -- e.g. 'name match', 'iso2 native', 'aggregate'
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, raw_label)
);
CREATE INDEX idx_country_aliases_lookup ON country_aliases (source, raw_label);

-- Composition of aggregate labels (EU bloc, ASEAN, etc.). Every member is recorded
-- as a row with provenance + optional valid_from/valid_to so historical bloc
-- changes (e.g. Brexit) are queryable rather than implicit.
CREATE TABLE country_aggregate_members (
    id                  BIGSERIAL PRIMARY KEY,
    aggregate_alias_id  BIGINT      NOT NULL REFERENCES country_aliases(id) ON DELETE CASCADE,
    member_iso2         TEXT        NOT NULL,
    valid_from          DATE,                        -- NULL = no lower bound
    valid_to            DATE,                        -- NULL = current
    source              TEXT        NOT NULL,        -- citation for the membership claim
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (aggregate_alias_id, member_iso2, valid_from)
);
CREATE INDEX idx_agg_members_alias ON country_aggregate_members (aggregate_alias_id);
CREATE INDEX idx_agg_members_iso2  ON country_aggregate_members (member_iso2);

-- Known caveats journalists should be aware of when interpreting cross-source comparisons.
-- Findings reference caveats by code so we don't duplicate the explanation each time.
CREATE TABLE caveats (
    code            TEXT        PRIMARY KEY,
    summary         TEXT        NOT NULL,
    detail          TEXT,
    applies_to      TEXT[],                      -- finding subkinds this caveat applies to, e.g. {mirror_gap}
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- FX rates with explicit provenance. We never mutate observation values; conversions
-- to a common currency (e.g. EUR) are derived at query time using these rates.
CREATE TABLE fx_rates (
    id              BIGSERIAL PRIMARY KEY,
    currency_from   TEXT        NOT NULL,        -- ISO-4217 e.g. 'CNY', 'USD'
    currency_to     TEXT        NOT NULL,        -- usually 'EUR'
    rate_date       DATE        NOT NULL,
    rate            NUMERIC     NOT NULL,        -- amount_in_to = amount_in_from * rate
    rate_source     TEXT        NOT NULL,        -- e.g. 'ECB monthly average'
    rate_source_url TEXT,
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (currency_from, currency_to, rate_date, rate_source)
);
CREATE INDEX idx_fx_lookup ON fx_rates (currency_from, currency_to, rate_date DESC);

-- Cross-source links between observations. Confidence is editorial: never assert
-- universal identity — surface that this PAIR is a candidate / probable /
-- corroborated linkage, with the method spelled out.
CREATE TABLE cross_source_links (
    id              BIGSERIAL PRIMARY KEY,
    obs_a_id        BIGINT      NOT NULL REFERENCES observations(id),
    obs_b_id        BIGINT      NOT NULL REFERENCES observations(id),
    link_kind       TEXT        NOT NULL,        -- 'mirror_trade' | 'aggregate_member' | 'hs_group' | etc.
    confidence      TEXT        NOT NULL CHECK (confidence IN ('candidate', 'probable', 'corroborated')),
    method_notes    JSONB       NOT NULL,        -- the comparison method, normalisation steps used, etc.
    caveat_codes    TEXT[],                      -- FK into caveats.code
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_csl_obs_a ON cross_source_links (obs_a_id);
CREATE INDEX idx_csl_obs_b ON cross_source_links (obs_b_id);
CREATE INDEX idx_csl_kind ON cross_source_links (link_kind, confidence);

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

-- =============================================================================
-- Seed data
-- =============================================================================

-- GACC partner labels observed in section-4 release pages, mapped to ISO-2.
-- Aggregate labels (EU, ASEAN, regions) have iso2 = NULL; their aggregate_kind
-- tells the comparator how to handle them.
INSERT INTO country_aliases (source, raw_label, iso2, aggregate_kind, confidence, method, notes) VALUES
  ('gacc', 'Germany',                                                  'DE',  NULL,         'high', 'name match', NULL),
  ('gacc', 'France',                                                   'FR',  NULL,         'high', 'name match', NULL),
  ('gacc', 'Italy',                                                    'IT',  NULL,         'high', 'name match', NULL),
  ('gacc', 'Netherlands',                                              'NL',  NULL,         'high', 'name match', NULL),
  ('gacc', 'United States (US)',                                       'US',  NULL,         'high', 'name match', NULL),
  ('gacc', 'United Kingdom (UK)',                                      'GB',  NULL,         'high', 'name match', NULL),
  ('gacc', 'Japan',                                                    'JP',  NULL,         'high', 'name match', NULL),
  ('gacc', 'R. O. Korea',                                              'KR',  NULL,         'high', 'name match', 'Republic of Korea (South Korea)'),
  ('gacc', 'Russian Federation',                                       'RU',  NULL,         'high', 'name match', NULL),
  ('gacc', 'Australia',                                                'AU',  NULL,         'high', 'name match', NULL),
  ('gacc', 'Canada',                                                   'CA',  NULL,         'high', 'name match', NULL),
  ('gacc', 'New Zealand',                                              'NZ',  NULL,         'high', 'name match', NULL),
  ('gacc', 'India',                                                    'IN',  NULL,         'high', 'name match', NULL),
  ('gacc', 'Brazil',                                                   'BR',  NULL,         'high', 'name match', NULL),
  ('gacc', 'South Africa',                                             'ZA',  NULL,         'high', 'name match', NULL),
  ('gacc', 'Vietnam',                                                  'VN',  NULL,         'high', 'name match', NULL),
  ('gacc', 'Malaysia',                                                 'MY',  NULL,         'high', 'name match', NULL),
  ('gacc', 'Thailand',                                                 'TH',  NULL,         'high', 'name match', NULL),
  ('gacc', 'Singapore',                                                'SG',  NULL,         'high', 'name match', NULL),
  ('gacc', 'Indonesia',                                                'ID',  NULL,         'high', 'name match', NULL),
  ('gacc', 'Philippines',                                              'PH',  NULL,         'high', 'name match', NULL),
  ('gacc', 'Hong Kong, China',                                         'HK',  NULL,         'high', 'name match', 'SAR of China — Eurostat may not match composition'),
  ('gacc', 'Taiwan, China',                                            'TW',  NULL,         'high', 'name match', 'Eurostat reporter convention may differ'),
  ('gacc', 'European Union',                                           NULL,  'eu_bloc',    'high', 'aggregate',  'Composition per release footnote (27 countries as of 2026)'),
  ('gacc', 'ASEAN',                                                    NULL,  'asean',      'high', 'aggregate',  'Brunei, Myanmar, Cambodia, Indonesia, Laos, Malaysia, Philippines, Singapore, Thailand, Vietnam'),
  ('gacc', 'Latin America',                                            NULL,  'region',     'high', 'aggregate',  'Region; GACC does not enumerate composition'),
  ('gacc', 'Africa',                                                   NULL,  'region',     'high', 'aggregate',  'Region'),
  ('gacc', 'Regional Comprehensive Economic Partnership',              NULL,  'rcep',       'high', 'aggregate',  'RCEP — Brunei, Myanmar, Cambodia, Indonesia, Laos, Malaysia, Philippines, Singapore, Thailand, Vietnam, Japan, South Korea, Australia, New Zealand'),
  ('gacc', 'Jointly build the countries along Belt and Road Routes',   NULL,  'belt_road',  'high', 'aggregate',  'Per https://www.yidaiyilu.gov.cn — composition varies'),
  ('gacc', 'Total',                                                    NULL,  'world',      'high', 'aggregate',  'World total — China''s reported total trade');

-- EU 27 membership for the 'European Union' aggregate (per the GACC release
-- footnote, which lists 27 countries as of 2026). Note Eurostat uses GR (not EL)
-- for Greece so the iso2 codes here align with our `eurostat_raw_rows.reporter`.
INSERT INTO country_aggregate_members (aggregate_alias_id, member_iso2, source, notes)
SELECT ca.id, m, 'GACC release footnote, Mar 2026', 'EU 27 as of 2026; Brexit reflected (UK absent)'
  FROM country_aliases ca,
       unnest(ARRAY[
           'AT','BE','BG','CY','CZ','DE','DK','EE','ES','FI','FR','GR','HR','HU',
           'IE','IT','LT','LU','LV','MT','NL','PL','PT','RO','SE','SI','SK'
       ]) m
 WHERE ca.source = 'gacc' AND ca.raw_label = 'European Union';

-- HS-CN8 patterns for component-trend analysis. Patterns use SQL LIKE
-- semantics; '850760%' matches every 8-digit code starting 850760.
-- This list is journalist-editable: refine via SQL UPDATE (the patterns are
-- a TEXT[] column) or add new groups via INSERT. New findings will be
-- generated against the new definitions on the next analysis run.
INSERT INTO hs_groups (name, description, hs_patterns, created_by) VALUES
  ('EV batteries (Li-ion)',
   'Lithium-ion accumulators (HS 850760) — the dominant battery type for electric vehicles and stationary storage.',
   ARRAY['850760%'], 'seed'),
  ('Solar PV cells & modules',
   'Photovoltaic cells assembled (HS 854142) and PV cells in modules/panels (HS 854143). Captures Chinese-manufactured solar panels imported into the EU.',
   ARRAY['854142%', '854143%'], 'seed'),
  ('Solar/grid inverters (broad)',
   'Static converters HS 850440 — includes solar inverters but also non-solar grid inverters and other DC-DC converters. Broad bucket; refine to specific CN8 codes (e.g. 85044020) if a tighter solar-only signal is needed.',
   ARRAY['850440%'], 'seed'),
  ('Wind turbine components',
   'Wind-powered generating sets (850231), parts for generators/motors (850300), iron/steel towers and lattice masts (730820). Wind-component coverage is necessarily approximate — components spread across many HS chapters.',
   ARRAY['850231%', '850300%', '730820%'], 'seed'),
  ('Rare-earth materials',
   'Rare-earth metals (280530), cerium compounds (284610), other rare-earth compounds (284690).',
   ARRAY['280530%', '284610%', '284690%'], 'seed'),
  ('Steel (broad)',
   'HS chapter 72 — iron and steel. Broad chapter; finer subgroups can be added (e.g. just flat-rolled finished steel) for specific stories.',
   ARRAY['72%'], 'seed'),
  ('Aluminium (broad)',
   'HS chapter 76 — aluminium and articles thereof.',
   ARRAY['76%'], 'seed'),
  ('Motor-vehicle parts',
   'HS 8708 — parts and accessories of motor vehicles. Includes the components flagged in editorial brief (axles, brakes, clutches, etc.) but not engines or batteries.',
   ARRAY['8708%'], 'seed'),
  ('Machine tools',
   'HS 8456-8463 — metalworking machine tools (numerically-controlled, conventional, presses, lathes, etc.).',
   ARRAY['8456%', '8457%', '8458%', '8459%', '8460%', '8461%', '8462%', '8463%'], 'seed'),
  ('Industrial fasteners',
   'Threaded fasteners — screws and bolts (731815), nuts (731816), other threaded articles (731819).',
   ARRAY['731815%', '731816%', '731819%'], 'seed'),
  ('Electrical equipment & machinery (chapters 84-85, broad)',
   'HS chapters 84 (machinery and mechanical appliances) + 85 (electrical machinery and equipment). Broad — useful for the headline component-trade total often cited in trade press. Refine to specific HS-4 sub-headings for narrower investigations.',
   ARRAY['84%', '85%'], 'seed'),
  -- Added after Lisa O'Carroll's Apr 2026 piece — these groups produce findings
  -- that map directly onto sentences in her article ("Chinese EV sales doubled",
  -- "China still accounts for 93% of permanent magnets", etc.).
  ('Permanent magnets',
   'HS 8505 — electromagnets and permanent magnets. Lisa O''Carroll (Apr 2026): China supplies 93% of EU permanent magnet imports, volumes up 18% YoY.',
   ARRAY['8505%'], 'seed:lisa_article'),
  ('Finished cars (broad)',
   'HS 8703 — motor cars and other motor vehicles principally designed for the transport of persons. Headline "China shock" category.',
   ARRAY['8703%'], 'seed:lisa_article'),
  ('EV + hybrid passenger cars',
   'HS 870380 (electric only), 870370 (PHEV), 870360 (HEV non-plug-in) — the new-energy-vehicle (NEV) export category whose surge from $11bn to $20.6bn Q1 YoY drove the Apr 2026 Guardian story.',
   ARRAY['870380%', '870370%', '870360%'], 'seed:lisa_article'),
  ('Pork (HS 0203)',
   'HS 0203 — meat of swine, fresh, chilled or frozen. EU exports to China declined notably in Feb 2026 (Soapbox/Lisa).',
   ARRAY['0203%'], 'seed:lisa_article'),
  -- Added after Tan's May 2026 The Conversation piece on wind turbines —
  -- isolates finished generating sets from the broader components group
  -- so 'is China selling more turbines' can be answered without confusion
  -- with 'is China selling more wind-related metalwork'.
  ('Wind generating sets only',
   'HS 850231 — wind-powered electric generating sets. Narrower than "Wind turbine components"; isolates the finished-turbine question from generator parts and steel towers.',
   ARRAY['850231%'], 'seed:tan_article');

-- Caveats journalists should weigh when reading cross-source findings.
INSERT INTO caveats (code, summary, detail, applies_to) VALUES
  ('cif_fob',
   'CIF (imports) vs FOB (exports) pricing',
   'Eurostat imports are reported CIF (cost+insurance+freight included); GACC exports are reported FOB (free-on-board). The expected baseline gap is therefore EU-import value > GACC-export value by ~5-10% (the freight & insurance component). Only deviations from that baseline should be treated as anomalous.',
   ARRAY['mirror_gap']),
  ('reporting_lag',
   'Different publication lags across sources',
   'GACC preliminary releases come out ~10 days after period close; Eurostat data lags 6-8 weeks. When comparing the same period, ensure both sources have published. Also: GACC may revise figures between preliminary and monthly bulletin before Eurostat ever sees them.',
   ARRAY['mirror_gap']),
  ('general_vs_special_trade',
   'Trade-definition differences',
   'GACC and Eurostat may differ on what counts as "trade" (general vs special trade) — bonded zones, transit goods, processing trade are treated differently. Effects vary by HS chapter.',
   ARRAY['mirror_gap']),
  ('transshipment',
   'Transshipment via third countries',
   'Goods exported from China may be reported by Eurostat under a non-China origin if they pass through (and are partially transformed in) a third country. Mirror gaps may reflect routing rather than direct trade. Watch country-of-origin shifts in particular.',
   ARRAY['mirror_gap']),
  ('classification_drift',
   'HS classification at 8-digit can diverge',
   'GACC uses CHS8 (Chinese 8-digit harmonised classification); Eurostat uses CN8. The two systems agree at HS-2/4/6 by international standard but diverge at HS-8. Comparisons at HS-8 may compare different commodity definitions; aggregate to HS-6 to minimise.',
   ARRAY['mirror_gap', 'mix_substitution']),
  ('currency_timing',
   'FX conversion is sensitive to which day''s rate is used',
   'When converting CNY/USD to EUR (or vice versa), the choice of day''s rate matters. We use the ECB monthly-average reference rate per period; differences from end-of-period or trade-weighted rates can be 1-3%.',
   ARRAY['mirror_gap']),
  ('aggregate_composition',
   'Aggregate-region composition may differ between sources',
   'Regional aggregates ("EU", "ASEAN", "Latin America") may have slightly different country lists or as-of dates between GACC and Eurostat (e.g. Brexit timing for EU). Always check composition before comparing aggregates directly.',
   ARRAY['mirror_gap']),
  ('eurostat_stat_procedure_mix',
   'Eurostat trade splits across tariff regimes (STAT_PROCEDURE)',
   'Eurostat reports trade by STAT_PROCEDURE — preferential, MFN, special-regime imports etc. Our `observations` row is the sum across regimes; the breakdown is in `eurostat_raw_rows`. Some stories live in the regime mix itself (e.g. surge in inward-processing imports may indicate a re-export pattern).',
   ARRAY['mirror_gap', 'mix_substitution']),
  ('low_base_effect',
   'Percentage change rests on a tiny denominator',
   'YoY percentages for HS groups whose prior or current 12-month total is small in absolute terms can look dramatic without being journalistically significant. A "+750%" rise on a €5M base is a €40M absolute move — meaningful for a niche subgroup but not "China shock" headline material. Always interpret a flagged finding alongside its absolute EUR totals before quoting the percentage.',
   ARRAY['hs_group_yoy', 'hs_group_trajectory']);

