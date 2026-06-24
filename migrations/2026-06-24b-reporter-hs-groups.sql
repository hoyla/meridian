-- 2026-06-24 (b) — reporter-requested HS groups (2026-06).
--
-- Two clusters, both reporter-requested:
--
-- (A) China→EU consumer/industrial exports — vapes/nicotine (2404), nuclear
--     reactors (8401), arms parts (9305), munitions (9306) and coin-/token-
--     operated gaming machines (9504.30).
--
-- (B) Energy / petrochemicals — crude oil (2709), refined products (2710) and
--     natural gas & other petroleum gases (2711). These test a reselling
--     hypothesis: the EU/UK produce little of these, so member-state EXPORTS to
--     China (flow 2) would flag entrepot re-export of product made elsewhere
--     rather than domestic output. The analyser already sweeps both flows
--     (periodic.py: scope x flow 1|2) with a per-reporter breakdown, and the
--     GACC mirror cross-checks. Flows are expected small/lumpy; a near-null is
--     itself a finding. Caveat: the bilateral counterparty is always China, so
--     this catches reselling TO China only, not to other destinations.
--
-- New groups produce hs_group_yoy / hs_group_yoy_export + trajectory findings on
-- the next periodic --analyse run (the raw CN8 data for these prefixes is
-- already ingested). The China-share-of-extra-EU-imports metric additionally
-- needs an eurostat_world_aggregates backfill for the new prefixes — a separate
-- step, done once the keepers are settled.
--
-- Idempotent: ON CONFLICT (name) DO NOTHING (hs_groups.name is UNIQUE). Safe to
-- re-run.

INSERT INTO hs_groups (name, description, hs_patterns, created_by) VALUES
  -- (A) China→EU consumer/industrial exports
  ('Vapes, heated tobacco & nicotine products (HS 2404)',
   'HS 2404 — products for inhalation without combustion and other nicotine-intake products: e-cigarette/vape liquids and devices (240412/240419), heated-tobacco sticks (240411) and oral nicotine pouches (240491). A heading created in the 2022 HS revision, so the China→EU series starts 2022. China (Shenzhen especially) is the dominant global manufacturer of vaping hardware, making this a live consumer-health and disposable-vape-ban story.',
   ARRAY['2404%'], 'seed:reporter_request_2026_06'),
  ('Nuclear reactors & fuel elements (HS 8401)',
   'HS 8401 — nuclear reactors (840110), isotopic-separation machinery (840120), non-irradiated fuel elements/cartridges (840130) and parts (840140). Low-volume and lumpy — a single reactor component can spike a month — so read levels and multi-month windows rather than single-period YoY.',
   ARRAY['8401%'], 'seed:reporter_request_2026_06'),
  ('Arms parts & accessories (HS 9305)',
   'HS 9305 — parts and accessories of the weapons in headings 9301–9304 (military weapons, revolvers/pistols, other firearms, other arms). Pairs with "Munitions & ammunition (HS 9306)". Small and sensitive trade; dual-use and export-control angles make even modest flows newsworthy.',
   ARRAY['9305%'], 'seed:reporter_request_2026_06'),
  ('Munitions & ammunition (HS 9306)',
   'HS 9306 — bombs, grenades, missiles and other munitions of war (930690), plus shotgun cartridges (930621) and other cartridges/ammunition and parts (930630). Sibling to "Arms parts & accessories (HS 9305)". Low-volume and politically sensitive; treat single-month spikes with care.',
   ARRAY['9306%'], 'seed:reporter_request_2026_06'),
  ('Coin- & token-operated games (CN8 9504.30)',
   'CN8 9504.30 — games operated by coins, banknotes, cards or tokens (arcade and amusement machines), excluding automatic bowling equipment. Deliberately the 9504.30 subheading only, NOT all of HS 9504: it excludes video-game consoles (950450) and playing cards (950440), which are far larger and a different story.',
   ARRAY['950430%'], 'seed:reporter_request_2026_06'),
  -- (B) Energy / petrochemicals — reselling hypothesis (export-leg + GACC mirror)
  ('Crude oil (HS 2709)',
   'HS 2709 — crude petroleum and oils from bituminous minerals (incl. natural-gas condensates). Added to test the reselling hypothesis: the EU/UK extract little crude, so member-state EXPORTS to China here (flow 2) point to entrepot re-export — Rotterdam/Antwerp the obvious watch — rather than domestic output. Imports from China are ~nil. Expect small, lumpy flows; the GACC mirror (Chinese crude imports by partner) cross-checks.',
   ARRAY['2709%'], 'seed:reporter_request_2026_06'),
  ('Refined petroleum products (HS 2710)',
   'HS 2710 — petroleum oils other than crude: gas oil/diesel and light oils/motor spirit (271019/271012), kerosene/jet, fuel oils, lubricants and waste oils. The likeliest re-export bucket for the reselling hypothesis — EU refining/blending hubs re-sell product they did not extract. BROAD heading; refine to CN8 for a specific cut. Same lens: member-state exports to China (flow 2).',
   ARRAY['2710%'], 'seed:reporter_request_2026_06'),
  ('Natural gas & other petroleum gases (HS 2711)',
   'HS 2711 — petroleum gases and other gaseous hydrocarbons: LNG (271111) and gaseous natural gas (271121), plus LPG — propane (271112), butanes (271113) — and petrochemical feedstock gases (271114). Broader than LNG alone, per the reporter request; for an LNG-only cut, narrow to CN8 271111. Same reselling lens: member-state exports to China (flow 2), expected small.',
   ARRAY['2711%'], 'seed:reporter_request_2026_06')
ON CONFLICT (name) DO NOTHING;
