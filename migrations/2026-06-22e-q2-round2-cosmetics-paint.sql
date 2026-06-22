-- 2026-06-22 (e) — Q2 expansion round 2: cosmetics + paint (Lisa, 2026-06-22).
--
-- Completes the Q2 coverage expansion (round 1 = 2026-06-22d). Material-named
-- groups; the two new themes (Cosmetics & personal care, Paint & coatings) live
-- in labels.py — code, not DB, so no migration for the theme memberships. Those
-- themes also pick up the existing Titanium dioxide (320611) group as the
-- pigment that bridges paint and cosmetics.
--
-- Deferred (not in this migration): surfactants (HS 3402 — dominantly cleaning,
-- not cosmetics; wants a "cleaning products" theme) and a broad pigments group
-- (HS 3206 — overlaps the TiO2 320611 group).
--
-- YoY findings land on the next periodic --analyse run (raw CN8 data already
-- ingested). Idempotent: ON CONFLICT (name) DO NOTHING. Safe to re-run.

INSERT INTO hs_groups (name, description, hs_patterns, created_by) VALUES
  ('Essential oils & fragrance mixtures (HS 3301 + 3302)',
   'Essential oils (3301) and the odoriferous fragrance mixtures (3302) blended from them — the scent base for perfumery and cosmetics (3302 also serves food/drink flavouring).',
   ARRAY['3301%', '3302%'], 'seed:lisa_q2_round2_2026_06'),
  ('Beauty, make-up & skin-care preparations (HS 3304)',
   'HS 3304 — finished beauty, make-up and skin-care preparations (incl. sunscreen, manicure/pedicure). The consumer cosmetics end-product.',
   ARRAY['3304%'], 'seed:lisa_q2_round2_2026_06'),
  ('Paints & varnishes (HS 3208-3210)',
   'Paints and varnishes — synthetic-polymer-based (3208), other (3209), and other paints/varnishes incl. prepared water pigments (3210). Finished coatings; the titanium-dioxide pigment that whitens most of them is its own group (CN8 320611).',
   ARRAY['3208%', '3209%', '3210%'], 'seed:lisa_q2_round2_2026_06')
ON CONFLICT (name) DO NOTHING;
