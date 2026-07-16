-- 2026-07-16 (b) — Semiconductors theme: three new groups.
--
-- Companion to the "Semiconductors" label added to labels.py in the same
-- change (approved by Luke 2026-07-16; see
-- dev_notes/2026-07-16-label-coverage-audit.md §Semiconductors). The theme
-- unions these three with the existing "Semiconductor manufacturing
-- equipment" (8486) and "Gallium, germanium & other minor metals (HS 8112)"
-- groups. Raw CN8 data for all three prefixes is already ingested
-- (observations carry 8542/8541/3818 back to 2017); findings appear on the
-- next periodic --analyse run. The China-share-of-extra-EU metric needs an
-- eurostat_world_aggregates backfill for 3818 only — 8542/8541 are already
-- swept in by the chapters-84-85 broad group's aggregates.
--
-- The HS 8541 group enumerates its patterns instead of claiming '8541%'
-- because heading 8541 contains solar photovoltaic cells, which would
-- otherwise swamp the group (854143 alone is ~EUR 64bn across the ingested
-- window vs ~EUR 2bn/12mo for everything else here). Two exclusions:
--   * 854142/854143 — PV cells, the existing "Solar PV cells & modules" group;
--   * 854140 — the PRE-HS2022 code (2017-2021 in our data) that mixed LEDs,
--     photosensitive devices AND PV cells in one bucket. Including it would
--     contaminate the pre-2022 series with solar panels. The cost is a step
--     in 2022: LEDs (854141) and photosensitive devices (854149) enter the
--     series only from 2022-01, when the HS2022 restructure gave them PV-free
--     codes. 854150 (pre-2022 "other semiconductor devices") is PV-free and
--     kept, giving that slice continuity across the boundary. Rolling-12mo
--     and YoY analysers anchored in 2023+ are unaffected; only a
--     trend-from-2019 chart would show the 2022 step.
--
-- Idempotent: ON CONFLICT (name) DO NOTHING (hs_groups.name is UNIQUE). Safe
-- to re-run. schema.sql gains the same three rows in this change, so a fresh
-- DB needs no migration.

INSERT INTO hs_groups (name, description, hs_patterns, created_by) VALUES
  ('Integrated circuits (HS 8542)',
   'HS 8542 — electronic integrated circuits: processors and controllers (854231 — where GPUs, CPUs and MCUs all land when shipped as bare chips), memories (854232), amplifiers (854233), other ICs (854239) and parts (854290). Europe is a net EXPORTER to China at this heading (~EUR 6.8bn out vs ~EUR 4.8bn in, rolling 12mo) — automotive/industrial silicon (Infineon, ST, NXP) against Chinese legacy chips. HS granularity cannot isolate GPUs or AI accelerators: bare accelerator chips are indistinguishable from CPUs inside 854231, and assembled cards classify as computer parts (8473). Do not present any cut of this group as a GPU number.',
   ARRAY['8542%'], 'seed:semiconductors_2026_07'),
  ('Semiconductor devices excl. solar PV (HS 8541)',
   'HS 8541 minus photovoltaics — diodes (854110), transistors (854121/29), thyristors (854130), LEDs (854141), photosensitive devices (854149), legacy other-devices (854150) and its HS2022 successors semiconductor transducers/other (854151/59), piezo crystals (854160), parts (854190). Excludes PV cells 854142/854143 (the "Solar PV cells & modules" group) AND the pre-2022 mixed code 854140, which bundled LEDs and photosensitive devices WITH PV cells — so LEDs/photosensitive enter this series only from 2022-01 (the HS2022 restructure). YoY and rolling-12mo comparisons anchored 2023+ are clean; a from-2019 trend shows a 2022 step. Roughly EUR 2bn each way with China per 12mo (EUR 2.1bn export / EUR 2.3bn import, 12mo to 2026-05).',
   ARRAY['854110%', '854121%', '854129%', '854130%', '854141%', '854149%',
         '854150%', '854151%', '854159%', '854160%', '854190%'],
   'seed:semiconductors_2026_07'),
  ('Doped wafers (HS 3818)',
   'HS 3818 — chemical elements and compounds doped for use in electronics, in discs, wafers or similar forms: the silicon-wafer feedstock stage between polysilicon (280461, tracked under Solar) and chip fabrication. Small EU–China flow (~EUR 0.2bn each way per 12mo) but completes the upstream of the Semiconductors theme. Needs a one-off eurostat_world_aggregates backfill for the China-share metric (no existing group covers chapter 38 at this prefix).',
   ARRAY['3818%'], 'seed:semiconductors_2026_07')
ON CONFLICT (name) DO NOTHING;
