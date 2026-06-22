-- 2026-06-22 (d) — Q2 coverage expansion: refined critical minerals + pharma
-- APIs (first tranche), plus engine parts + engines (Lisa, 2026-06-22).
--
-- See dev_notes/2026-06-22-lisa-sector-coverage-questions.md. Material-named
-- groups; applications are carried by the themes in labels.py (which is code,
-- not DB — no migration needed for the theme memberships). Cosmetics/paint are
-- deferred to a round 2.
--
-- New groups produce hs_group_yoy/trajectory findings on the next periodic
-- --analyse run (the raw CN8 data for these prefixes is already ingested). The
-- China-share-of-extra-EU-imports metric additionally needs an
-- eurostat_world_aggregates backfill for the new prefixes — a separate step,
-- done once the keepers are settled.
--
-- Idempotent: ON CONFLICT (name) DO NOTHING (hs_groups.name is UNIQUE). Safe to
-- re-run.

INSERT INTO hs_groups (name, description, hs_patterns, created_by) VALUES
  ('Lithium chemicals (carbonate + hydroxide)',
   'Refined battery-grade lithium — lithium carbonate (283691) and lithium oxide/hydroxide (282520). The cathode-precursor feedstock; refining is heavily China-concentrated.',
   ARRAY['283691%', '282520%'], 'seed:lisa_q2_2026_06'),
  ('Cobalt (oxides, hydroxides & unwrought)',
   'Cobalt oxides/hydroxides (282200) and unwrought cobalt (810520). Battery-cathode input — DRC-mined, largely China-refined.',
   ARRAY['282200%', '810520%'], 'seed:lisa_q2_2026_06'),
  ('Manganese oxides',
   'Manganese oxides (282010) — battery-cathode (NMC) and steel input.',
   ARRAY['282010%'], 'seed:lisa_q2_2026_06'),
  ('Tungsten (HS 8101)',
   'Tungsten and articles, incl. powders and carbide (HS 8101). Under China export licensing from 2025; critical for cutting tools and defence.',
   ARRAY['8101%'], 'seed:lisa_q2_2026_06'),
  ('Gallium, germanium & other minor metals (HS 8112)',
   'HS 8112 — gallium, germanium, indium and other minor base metals (BROAD: also beryllium, chromium, hafnium, etc.). Gallium and germanium are the headline China export controls (2023); refine to CN8 (e.g. 811292) for a tighter Ga/Ge-only signal.',
   ARRAY['8112%'], 'seed:lisa_q2_2026_06'),
  ('Antimony (HS 8110)',
   'Antimony and articles (HS 8110). Under China export licensing from 2024; flame retardants, defence, PV glass.',
   ARRAY['8110%'], 'seed:lisa_q2_2026_06'),
  ('Titanium dioxide (CN8 320611)',
   'Titanium dioxide pigment (CN8 320611) — the dominant white pigment for paint, plastics and cosmetics, and a refined-mineral product. Its Paint and Cosmetics themes arrive with the round-2 expansion; until then it sits in its SITC division only.',
   ARRAY['320611%'], 'seed:lisa_q2_2026_06'),
  ('Antibiotics (HS 2941)',
   'HS 2941 — antibiotics. A core "Europe leans on Chinese active ingredients" category.',
   ARRAY['2941%'], 'seed:lisa_q2_2026_06'),
  ('Ibuprofen-class monocarboxylic acids (HS 2916)',
   'HS 2916 — unsaturated acyclic and cyclic monocarboxylic acids (BROAD; ibuprofen sits here among many others). Frame findings as "the HS 2916 acid family", not ibuprofen alone; refine to CN8 for a single API.',
   ARRAY['2916%'], 'seed:lisa_q2_2026_06'),
  ('Paracetamol-class amides (HS 2924)',
   'HS 2924 — carboxyamide-function compounds (BROAD; paracetamol/acetaminophen sits here). Frame findings as "the HS 2924 amide family"; refine to CN8 for a single API.',
   ARRAY['2924%'], 'seed:lisa_q2_2026_06'),
  ('Vitamins & provitamins (HS 2936)',
   'HS 2936 — provitamins and vitamins, unmixed or mixed. Feed/food/pharma input with China-concentrated supply.',
   ARRAY['2936%'], 'seed:lisa_q2_2026_06'),
  ('Engine parts (CN8 84099100 + 84099900)',
   'Parts of spark-ignition (84099100) and compression-ignition (84099900) piston engines. One bucket covers both passenger and industrial vehicle engines — HS does not split engine parts by vehicle class. Excludes aircraft engine parts (84091000). Complements "Motor-vehicle parts" (HS 8708), which excludes engines and motors.',
   ARRAY['84099100%', '84099900%'], 'seed:lisa_engine_parts_2026_06'),
  ('Internal-combustion engines (HS 8407 + 8408)',
   'Spark-ignition petrol (8407) and compression-ignition diesel (8408) piston engines. BROAD: road-vehicle engines sit in 8407.3x/8408.20, industrial/other in 8408.90, and marine/aircraft in 8407.1-2x/8408.10 — refine to those subheadings to separate domestic-vehicle from industrial engines if a story needs it.',
   ARRAY['8407%', '8408%'], 'seed:lisa_engine_parts_2026_06')
ON CONFLICT (name) DO NOTHING;
