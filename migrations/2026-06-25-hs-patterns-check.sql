-- 2026-06-25 — validate hs_groups.hs_patterns at the schema level.
--
-- Companion to the schema.sql CHECK added in the same change and to finding A3
-- of dev_notes/2026-06-25-adversarial-correctness-review.md. hs_patterns are
-- journalist-editable and spliced into SQL LIKE clauses against product_nc, so
-- a malformed entry ('8%', a stray '%', a missing '%') silently yields a wrong
-- or over-broad group total under a clean "success". This constrains each
-- pattern to 2–8 digits followed by '%', a non-empty array, and no NULL element.
--
-- Pre-verified safe: at time of writing all 62 live hs_groups rows already
-- satisfy this, so ADD CONSTRAINT validates without rewriting or rejecting any
-- existing row. (The query-level 000TOTAL guard in
-- anomalies._hs_pattern_or_clause ships in the same PR and is what actually
-- closes the '00%'/'000%' double-count against the ~40k 000TOTAL rows in
-- eurostat_raw_rows; this constraint blocks the structurally-malformed inputs
-- the regex can express.)
--
-- Idempotent: guarded on pg_constraint, so re-running is a no-op.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'hs_groups_patterns_valid'
    ) THEN
        ALTER TABLE hs_groups
            ADD CONSTRAINT hs_groups_patterns_valid CHECK (
                array_position(hs_patterns, NULL) IS NULL
                AND array_to_string(hs_patterns, ',') ~ '^[0-9]{2,8}%(,[0-9]{2,8}%)*$'
            );
    END IF;
END $$;
