-- 2026-07-16 (d) — penicillin-family child group (the amoxicillin proxy).
--
-- Prompted by Soapbox's amoxicillin piece
-- (https://soapboxtrade.substack.com/p/chinese-exports-to-the-eu-head-for),
-- which is explicit about the categorisation problem: amoxicillin has NO EU
-- customs line of its own — it disappears into CN8 29411000 (penicillins with
-- a penicillanic-acid structure). Our partner_share helpers reproduce their
-- headline on that code: China (CN+HK+MO) at 87.2% of extra-EU EU-27 import
-- QUANTITY in 2025 but 73.3% by value (Soapbox: 86% by quantity, CN-only).
--
-- The existing "Antibiotics (HS 2941)" parent keeps the whole clean heading;
-- this child gives the penicillin cut its own findings — most usefully
-- partner_share at the granularity the amoxicillin story actually lives at,
-- and a per-group series positioned to record any import surge/cliff around
-- the pending EU anti-dumping decision (Sandoz complaint, May 2026) the way
-- the e-bike codes recorded theirs. Parent/child overlap is the established
-- pattern (rare-earth CN8 children, Sintered NdFeB in Permanent magnets);
-- labels overlap by design and are never summed.
--
-- Idempotent: ON CONFLICT (name) DO NOTHING. schema.sql gains the same row in
-- this change, so a fresh DB needs no migration. No world-aggregates backfill
-- needed: the parent's 2941% aggregates already cover 29411000.

INSERT INTO hs_groups (name, description, hs_patterns, created_by) VALUES
  ('Penicillin-family APIs (CN8 29411000)',
   'CN8 29411000 — penicillins and derivatives with a penicillanic-acid structure: the amoxicillin code, and the categorisation caveat comes first: amoxicillin has no EU customs line of its own, so this bucket ALSO contains ampicillin, piperacillin and the rest of the penicillin family — never present it as an amoxicillin-only number (China''s own customs does split amoxicillin at 8 digits; EU data cannot). China supplied ~87% of extra-EU import quantity in 2025 but ~73% by value. Live policy hook: Sandoz''s May 2026 anti-dumping complaint (its Kundl, Austria site is the EU''s only vertically integrated amoxicillin producer) — watch this series for a pre-duty surge or post-duty cliff, cf. the 2019 e-bike collapse under 87116010. Child of "Antibiotics (HS 2941)".',
   ARRAY['29411000%'], 'seed:soapbox_amoxicillin_2026_07')
ON CONFLICT (name) DO NOTHING;
