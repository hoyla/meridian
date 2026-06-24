-- 2026-06-24 (c) — drop "Crude oil (HS 2709)".
--
-- 2709 was seeded in 2026-06-24b to test the petrochemicals-reselling
-- hypothesis. Reading the data (gacc mirror, rolling 12 months to 2026-04)
-- shows ZERO EU<->China crude trade in either direction — a clean null. The
-- reselling signal lives in refined products (2710 — ~EUR 1bn of EU->China
-- exports, led by Greece and Hungary), not crude. So 2709 is removed rather
-- than shipped as a permanently-empty group. Refined (2710) and gases (2711)
-- are kept; the "Oil & gas: origin watch" theme drops 2709 in labels.py.
--
-- Forward-only and idempotent: a no-op on any DB that never applied 2026-06-24b
-- (e.g. live, which had not yet applied it). Match is exact on name + the seed
-- tag so a journalist's later same-named group can't be deleted by surprise.

DELETE FROM hs_groups
 WHERE name = 'Crude oil (HS 2709)'
   AND created_by = 'seed:reporter_request_2026_06';
