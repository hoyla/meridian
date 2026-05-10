-- Migration: transshipment_hubs + cif_fob_baselines tables.
-- Phase 2.1 + 2.2 of dev_notes/history.md.
--
-- Apply with:
--   psql $DATABASE_URL < migrations/2026-05-09-transshipment-and-cif-fob-tables.sql
--   psql $GACC_TEST_DATABASE_URL < migrations/2026-05-09-transshipment-and-cif-fob-tables.sql
--
-- Idempotent: re-runnable.

BEGIN;

CREATE TABLE IF NOT EXISTS transshipment_hubs (
    iso2          TEXT        PRIMARY KEY,
    notes         TEXT,
    evidence_url  TEXT,
    created_by    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cif_fob_baselines (
    id            BIGSERIAL   PRIMARY KEY,
    partner_iso2  TEXT,
    baseline_pct  NUMERIC     NOT NULL,
    source        TEXT        NOT NULL,
    source_url    TEXT,
    notes         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_cif_fob_baselines_partner
    ON cif_fob_baselines ((COALESCE(partner_iso2, '_GLOBAL_')));

-- Caveat row.
INSERT INTO caveats (code, summary, detail, applies_to) VALUES
  ('transshipment_hub',
   'Partner is a known transshipment hub — gap may reflect routing not direct trade',
   'The partner country in this mirror_gap finding is a known transshipment hub (Rotterdam for NL, Antwerp for BE, port-based re-export economies for HK/SG/AE, Pacific gateway for MX). Goods reported by China as exported to a third country may transit through the hub before being declared by Eurostat as imported from China by a different EU member, and vice versa. Persistent large gaps for hub partners are therefore primarily a routing artefact — distinct from CIF/FOB baseline (cif_fob caveat) and from sudden gap movements (mirror_gap_zscore). Editorial weight should be on movements relative to the hub''s own baseline, not on the absolute level.',
   ARRAY['mirror_gap'])
ON CONFLICT (code) DO NOTHING;

-- Hub seeds.
INSERT INTO transshipment_hubs (iso2, notes, evidence_url, created_by) VALUES
  ('NL', 'Rotterdam — largest container port in Europe; well-documented Chinese-goods routing into the EU. Persistent ~65-70% Eurostat-higher mirror gap is the classic transshipment signature.',
   'https://unctad.org/topic/transport-and-trade-logistics/review-of-maritime-transport',
   'seed:roadmap_phase_2'),
  ('BE', 'Antwerp — second-largest EU container port; secondary hub to Rotterdam.',
   'https://unctad.org/topic/transport-and-trade-logistics/review-of-maritime-transport',
   'seed:roadmap_phase_2'),
  ('HK', 'Hong Kong — major China re-export economy; ~15% of China''s reported exports route through HK, often re-exported to third destinations. Important note: in Eurostat reporting, HK trade is reported under partner=HK (not CN), so the mirror gap to CN under-counts goods Chinese in origin but routed via HK. Phase 2.3 (multi-partner support) addresses this directly.',
   'https://www.censtatd.gov.hk/en/page_8000.html',
   'seed:roadmap_phase_2'),
  ('SG', 'Singapore — pan-Asian port hub; petrochemicals and electronics commonly transit.',
   'https://www.mpa.gov.sg/maritime-singapore/about-mpa',
   'seed:roadmap_phase_2'),
  ('AE', 'United Arab Emirates (Jebel Ali) — Middle East gateway; growing role in China-Europe routing post-2020.',
   'https://www.dpworld.com/jebel-ali',
   'seed:roadmap_phase_2'),
  ('MX', 'Mexico (Manzanillo, Lázaro Cárdenas) — Pacific gateway; secondary China-Latam transshipment route.',
   'https://en.wikipedia.org/wiki/Port_of_Manzanillo_(Mexico)',
   'seed:roadmap_phase_2')
ON CONFLICT (iso2) DO NOTHING;

-- CIF/FOB global default (replaces the CIF_FOB_BASELINE_PCT constant).
INSERT INTO cif_fob_baselines (partner_iso2, baseline_pct, source, source_url, notes) VALUES
  (NULL, 0.075,
   'UNCTAD/WTO global default — Eurostat (CIF) typically reports 5-10% higher than GACC (FOB) for the same flow before any other effects',
   'https://unctad.org/topic/transport-and-trade-logistics/review-of-maritime-transport',
   'Global default. Per-partner rows override; route-specific UNCTAD figures (CN→DE container vs. CN→landlocked-EU) are deferred to Phase 4 of the roadmap when we need the granularity for a specific investigation.');
-- The unique index is on COALESCE(partner_iso2, '_GLOBAL_') — re-runs without
-- ON CONFLICT would duplicate rows, but the index would block them. Use a
-- SELECT-and-skip pattern for idempotency:
DELETE FROM cif_fob_baselines a
 USING cif_fob_baselines b
 WHERE a.partner_iso2 IS NOT DISTINCT FROM b.partner_iso2
   AND a.id < b.id;

COMMIT;
