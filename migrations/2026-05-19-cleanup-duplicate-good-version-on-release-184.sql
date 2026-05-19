-- 2026-05-19 follow-up to
-- migrations/2026-05-19-reject-mismatched-gacc-currency-unit-pages.sql.
--
-- After deleting the corrupt v=2 / v=4 observations on release 184,
-- a spot-check showed the rolling-12mo Thailand finding rose from
-- €43.88B (pre-incident, 2026-05-15) to €48.21B — a delta of
-- exactly €4.33B, the canonical 2025-06 single-month value, being
-- counted twice. Root cause: the GACC daily walker re-fetched URL A
-- (the good CNY page) on 2026-05-19 11:29 while v=2 (corrupt) was
-- still the latest version. db.upsert_observations compared the new
-- parse to v=2 only, saw it differed, and inserted v=3 — even
-- though its content was identical to v=1. So release 184 now has
-- v=1 and v=3 carrying the same canonical value.
--
-- The aggregate analyser at anomalies.py:2913-2918 intentionally
-- sums all observations per (period, partner) — designed for
-- preliminary+revised releases where two distinct readings should
-- both contribute. It does not deduplicate equal-content rows. So
-- v=1 + v=3 = double-counted 2025-06 for every partner.
--
-- Three layers of fix in flight:
--   1. parse.py now rejects the mismatched URL B, so v=2-style
--      corruption won't recur. (Shipped in the prior migration.)
--   2. db.upsert_observations should skip the insert if the new
--      parse matches ANY prior version's content, not just the
--      latest. (Open — needs investigation; the existing
--      "unchanged" path covers latest-match, not any-match.)
--   3. anomalies.py should take MAX(version_seen) per natural key
--      rather than summing all versions. (Open — spawn task.)
--
-- This migration is the interim cleanup: delete v=3 on release 184
-- so the analyser sees a single canonical row per period. v=1
-- alone matches the data state the pre-incident 2026-05-15 export
-- ran against.

BEGIN;

DELETE FROM observations
 WHERE release_id = 184
   AND version_seen = 3;

COMMIT;
