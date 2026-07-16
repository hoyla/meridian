-- 2026-07-16 (c) — Micromobility theme: three new groups.
--
-- Companion to the "Micromobility" label added to labels.py in the same
-- change (Luke, 2026-07-16: "three groups, connected by micromobility theme").
-- See dev_notes/2026-07-16-label-coverage-audit.md §Micromobility. Chapter 87
-- had no coverage beyond cars (8703) and parts (8708): heading 8711 — where
-- ALL electric two-wheelers classify — and 8712 (pedal bicycles) were tracked
-- by no group. Raw CN8 data is already ingested back to 2017; findings appear
-- on the next periodic --analyse run. The China-share-of-extra-EU metric needs
-- an eurostat_world_aggregates backfill for 87116010/87116090/8712 (no
-- existing group's aggregates cover these prefixes).
--
-- The two 8711 groups deliberately mirror the CN8 split, because an EU
-- trade-policy boundary runs exactly along it: the anti-dumping +
-- countervailing duties imposed in January 2019 apply to pedal-assist
-- e-bikes (87116010) but NOT to the rest of the electric two-wheeler code
-- (87116090). Keeping them separate keeps the duty cliff — imports EUR 302m
-- (2018) -> EUR 33m (2019) — and any reclassification/circumvention drift
-- between the two codes visible. A single merged group would average the
-- story away.
--
-- Idempotent: ON CONFLICT (name) DO NOTHING (hs_groups.name is UNIQUE). Safe
-- to re-run. schema.sql gains the same three rows in this change, so a fresh
-- DB needs no migration.

INSERT INTO hs_groups (name, description, hs_patterns, created_by) VALUES
  ('Electric bicycles, pedal-assist (CN8 87116010)',
   'CN8 87116010 — bicycles, tricycles and quadricycles with pedal assistance, auxiliary electric motor <= 250 W: the classic European commuter e-bike. The trade-policy story: EU anti-dumping + countervailing duties on the Chinese product (combined up to ~79%) took effect January 2019 and imports collapsed ~90% in a year — EUR 313m (2017) / EUR 302m (2018) -> EUR 33m (2019); still depressed (~EUR 60-140m/yr since). Watch for circumvention: production shifting to third countries our China-partner lens cannot see, or drift into the un-dutied 87116090. E-bikes above 250 W classify there, not here.',
   ARRAY['87116010%'], 'seed:micromobility_2026_07'),
  ('Electric motorcycles, scooters & mopeds (CN8 87116090)',
   'CN8 87116090 — electrically-propelled motorcycles, mopeds and cycles EXCEPT pedal-assist <= 250 W e-bikes (87116010): stand-up e-scooters, e-mopeds, e-motorcycles and higher-powered e-bikes in ONE code — the constituents cannot be separated at CN8 granularity, so never present this as an e-scooter-only (or e-motorbike-only) number. The boom code: EU imports from China EUR 225m (2017) -> EUR 1.37bn (2022), ~EUR 0.9-1bn/12mo since. NOT covered by the 2019 e-bike anti-dumping duties — which also makes it the place reclassified flows would surface.',
   ARRAY['87116090%'], 'seed:micromobility_2026_07'),
  ('Bicycles, non-motorised (HS 8712)',
   'HS 8712 — pedal bicycles and other non-motorised cycles (CN8 87120030 ball-bearing bicycles, 87120070 other incl. delivery tricycles). In the Micromobility theme as the substitution/trade-policy baseline rather than for an electric angle: Chinese bicycles have carried EU anti-dumping duties since 1993 — the bloc''s longest-running measure, repeatedly extended and covering circumvention via third countries. EU imports from China ~EUR 120-210m/yr across our window.',
   ARRAY['8712%'], 'seed:micromobility_2026_07')
ON CONFLICT (name) DO NOTHING;
