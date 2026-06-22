-- 2026-06-22 — EV-coverage corrections + additions from Lisa O'Carroll's
-- Jun 2026 sector-coverage questions.
-- See dev_notes/2026-06-22-lisa-sector-coverage-questions.md for the full
-- investigation. This migration brings a live DB in line with the schema.sql
-- seed edits made in the same change.
--
-- THREE changes, all in the hs_groups lookup. No findings data is touched:
-- the next periodic `--analyse hs-groups` run append-emits findings for the
-- two new groups (journalism principle 4 — history preserved, nothing
-- destroyed). Correcting a description does not change any group's HS
-- patterns, so existing findings remain valid and are not superseded.
--
--   1. Correct the "EV + hybrid passenger cars" description. The prior prose
--      mislabelled CN8 870360 as "HEV non-plug-in" and 870370 as plain
--      "PHEV". Per HS 2022 both 870360 (petrol) and 870370 (diesel) are
--      PLUG-IN hybrids. The group's HS patterns were always correct
--      (BEV + both PHEV codes = NEV ex-FCEV); only the prose was wrong, so a
--      reporter quoting a finding could have called a plug-in hybrid a
--      non-plug-in one. Defensibility fix, not a data fix.
--
--   2. Add "Conventional hybrids (HEV, non-plug-in)" — CN8 870340/870350.
--      The old "EV + hybrid" prose claimed to cover non-plug-in hybrids but
--      no group actually held those codes. Now a real, separate group.
--
--   3. Add "Electric motors & generators (HS 8501, broad)" — the principal
--      "critical EV part" captured by neither Motor-vehicle parts (8708,
--      which excludes motors/batteries) nor EV batteries (850760).
--
-- Idempotent:
--   * The UPDATE re-sets the same text on re-run (no-op once applied).
--   * The INSERTs use ON CONFLICT (name) DO NOTHING — hs_groups.name is UNIQUE
--     — so re-running adds nothing. Safe to re-run.

-- 1. Correct the mislabelled EV+hybrid description.
UPDATE hs_groups
   SET description = 'HS 870380 (battery-electric, BEV), 870360 (plug-in hybrid, petrol) and 870370 (plug-in hybrid, diesel) — the battery-electric + plug-in-hybrid set that makes up China''s new-energy-vehicle (NEV) export category (ex-FCEV), whose surge from $11bn to $20.6bn Q1 YoY drove the Apr 2026 Guardian story. CORRECTION (2026-06-22): the prior description mislabelled 870360 as "HEV non-plug-in" and 870370 as plain "PHEV" — per HS 2022 both 870360 (petrol) and 870370 (diesel) are PLUG-IN hybrids; the non-plug-in hybrids are 870340/870350 and live in the sibling "Conventional hybrids (HEV, non-plug-in)" group. The HS patterns were always correct (BEV + both PHEV codes); only the prose was wrong. NB the EU''s Oct 2024 countervailing duties hit BEVs (870380) only — the two plug-in-hybrid codes here are outside that tariff scope, which is what makes a BEV-vs-PHEV split editorially live.'
 WHERE name = 'EV + hybrid passenger cars';

-- 2. Conventional (non-plug-in) hybrids.
INSERT INTO hs_groups (name, description, hs_patterns, created_by) VALUES
  ('Conventional hybrids (HEV, non-plug-in)',
   'HS 870340 (petrol + electric) and 870350 (diesel + electric) — full/mild hybrid cars that CANNOT be charged from an external source. Sibling to "EV + hybrid passenger cars" (which holds the battery-electric 870380 and the two plug-in-hybrid codes 870360/870370). Like PHEVs, conventional hybrids sit outside the EU''s Oct 2024 BEV-only countervailing duties, so this group lets the non-plug-in slice of the post-tariff import mix be tracked separately from BEVs and PHEVs.',
   ARRAY['870340%', '870350%'], 'seed:lisa_sector_q_2026_06')
ON CONFLICT (name) DO NOTHING;

-- 3. Electric motors & generators (broad).
INSERT INTO hs_groups (name, description, hs_patterns, created_by) VALUES
  ('Electric motors & generators (HS 8501, broad)',
   'HS 8501 — electric motors and generators (excluding generating sets). BROAD chapter: spans sub-watt motors, large industrial machines and alternators alike, so it is NOT an EV-traction-motor signal on its own. EV drive motors concentrate in the higher-power multiphase AC codes — chiefly CN8 85015350-85015399 (AC motors > 75 kW) — refine to those if a tighter EV signal is needed. Added for Lisa''s Jun 2026 question on the post-tariff pivot toward critical EV components: traction motors are the principal EV part captured by neither "Motor-vehicle parts" (HS 8708, which excludes motors and batteries) nor "EV batteries (Li-ion)" (HS 850760).',
   ARRAY['8501%'], 'seed:lisa_sector_q_2026_06')
ON CONFLICT (name) DO NOTHING;
