-- 2026-06-24 — eurostat_raw_rows natural-key UNIQUE backstop (partial, 2019+).
--
-- Enforce in the DB the natural-key uniqueness the append-only ingest guard
-- (db.eurostat_reporters_present_for_period) enforces in application code alone.
-- The guard is a check-then-insert across two transactions, so it isn't
-- concurrency-safe; this index makes the database refuse a duplicate raw line
-- outright, so a race or a guard bug can't silently duplicate a row and inflate
-- the aggregates derived from it. The guard stays the primary, graceful path
-- (it returns a clean noop); this index is the backstop that converts the
-- impossible-today race from *silent duplication* into a *loud abort*. The
-- insert path is unchanged (no ON CONFLICT) — bulk_insert_eurostat_raw_rows
-- relies on RETURNING every input row's id to build the observation FK arrays,
-- which ON CONFLICT DO NOTHING would break.
--
-- The key is every DIMENSION column (never the measures). Eurostat masks
-- confidential NC8 codes to chapter stubs like '28XXXXXX' that are distinguished
-- only by their classification columns (SITC/CPA/BEC/section), so those MUST be
-- in the key or legitimately-distinct flows would be rejected as "duplicates".
-- COALESCE on the nullable dims because a plain unique index counts NULL != NULL
-- and would let NULL-bearing rows slip past dedup (modern data has none today,
-- but the COALESCE future-proofs it).
--
-- PARTIAL — period >= 2019-01. The pre-v2 2017–2018 COMEXT bulk format emitted
-- genuine duplicate lines (~1.96M rows; up to 50 per cell) — a separate
-- forward-work item (dedupe / re-ingest 2017–2018 with the v2 parser). Scoping
-- to 2019+ enforces uniqueness where the data is ALREADY clean without forcing
-- that legacy cleanup first. Verified on live data 2026-06-24: the modern era
-- (88 periods, 13.4M rows) is unique on this exact key with zero violations.
--
-- Apply with (CONCURRENTLY → no write-lock on the 13M-row table; it cannot run
-- inside a transaction block, so run it on its own / under psql autocommit):
--   psql "$DATABASE_URL" -f migrations/2026-06-24-eurostat-raw-rows-unique-natural-key.sql
-- Reversible: DROP INDEX uq_eurostat_raw_natural_key;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_eurostat_raw_natural_key
ON eurostat_raw_rows (
    period, reporter, partner,
    COALESCE(trade_type, ''),
    product_nc,
    COALESCE(product_sitc, ''),
    COALESCE(product_cpa21, ''),
    COALESCE(product_cpa22, ''),
    COALESCE(product_bec, ''),
    COALESCE(product_bec5, ''),
    COALESCE(product_section, ''),
    flow,
    COALESCE(stat_procedure, ''),
    COALESCE(suppl_unit, '')
)
WHERE period >= DATE '2019-01-01';
