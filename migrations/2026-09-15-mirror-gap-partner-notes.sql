-- 2026-09-15 — plain-English mirror-gap context for NL, plus a first note for
-- Italy (not a hub).
--
-- Context: Luke found the NL transshipment-hub note unreadable on the portal
-- ("…but weight movements relative to NL's o"). Two problems: the renderers
-- cut it at 200/160 chars (fixed in code), and the text never said WHY NL's gap
-- exists. The mechanism is Eurostat's documented "Rotterdam effect"
-- (quasi-transit): goods bound for other EU countries are released into free
-- circulation in Dutch ports and recorded as Dutch extra-EU imports. China
-- records them against the final destination, so NL reads high and destination
-- countries (DE) read low — which is exactly what the live data shows. The
-- evidence_url moves from a generic UNCTAD port page to the Eurostat page that
-- documents the mechanism.
--
-- Italy has the largest gap on the page but no note, inviting a reader to
-- infer transshipment. Eurostat does not document quasi-transit for Italy, so
-- Italy must NOT go in transshipment_hubs (that auto-attaches the
-- `transshipment_hub` caveat). A new mirror_gap_partner_notes table carries
-- context without a caveat, and the Italy note says plainly that the cause is
-- not established.
--
-- Deliberately no magnitudes in either note: the old NL note's "~65-70%" figure
-- went stale (it was a double-count artefact, fixed 2026-06-17).
--
-- After applying: re-run `scrape.py --analyse mirror-trade`. Both notes are in
-- the analyser's value_fields, so NL and IT findings append-supersede with the
-- new text (history preserved) rather than citing the old note.
--
-- Idempotent: CREATE IF NOT EXISTS; the NL UPDATE only matches the old text;
-- the IT INSERT is ON CONFLICT DO NOTHING.

CREATE TABLE IF NOT EXISTS mirror_gap_partner_notes (
    iso2          TEXT        PRIMARY KEY,
    notes         TEXT        NOT NULL,
    evidence_url  TEXT,
    created_by    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

UPDATE transshipment_hubs
   SET notes = 'Rotterdam effect (quasi-transit): Chinese goods bound for other EU countries are often unloaded and cleared into the EU in Dutch ports, so Eurostat records them as Dutch imports while China records them as exports to the country of final destination. The Netherlands therefore normally shows more imports than China reports sending it, and destination countries such as Germany show fewer. The size of the Dutch gap is not news; a move away from its usual level may be.',
       evidence_url = 'https://ec.europa.eu/eurostat/statistics-explained/index.php?title=International_trade_statistics_-_background'
 WHERE iso2 = 'NL'
   AND notes LIKE 'Rotterdam — largest container port in Europe%';

INSERT INTO mirror_gap_partner_notes (iso2, notes, evidence_url, created_by) VALUES
  ('IT', 'Italy is not a documented transshipment hub, and the cause of its gap is not established. Italy has usually reported importing substantially more from China than China reports exporting to it since 2022, having been close to level in 2019-21, and the gap swings sharply from month to month. Explanations to test before reporting it: goods cleared through Italian ports for other EU destinations (the mechanism Eurostat documents for the Netherlands and Belgium, but not for Italy), differences in how the two sides assign the partner country, and shipping-time lags between the export and import months. Do not read the gap as evidence of transshipment or under-declaration.',
   'https://ec.europa.eu/eurostat/statistics-explained/index.php?title=International_trade_statistics_-_background',
   'editorial:2026-09-15')
ON CONFLICT (iso2) DO NOTHING;
