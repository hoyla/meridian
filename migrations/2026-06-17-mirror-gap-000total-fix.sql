-- 2026-06-17 — correct the Netherlands transshipment-hub note after the
-- mirror_gap double-count fix.
--
-- Context: the mirror_gap Eurostat totals were ~2x because
-- `anomalies._eurostat_aggregate_for_members` summed BOTH the per-CN8
-- detail rows AND the `000TOTAL` all-goods aggregate row (which already
-- sums the detail). The analyser now reads 000TOTAL alone. Re-running
-- `scrape.py --analyse mirror-trade` append-supersedes every affected
-- finding with corrected numbers (history preserved — journalism
-- principle 4), so NO data migration of `findings` is needed here.
--
-- What DOES need correcting is editorial commentary that asserted the
-- doubled gap as fact. The transshipment_hubs.notes row for NL claimed a
-- "Persistent ~65-70% Eurostat-higher mirror gap is the classic
-- transshipment signature" — a figure that only ever existed because of
-- the double-count. Corrected to ~+20% (NL excess over the CIF/FOB
-- baseline is ~+13.5pp on the corrected data, not ~+58pp). Rather than
-- stamp a new magnitude that will drift, the note now drops the number
-- and points at the editorial stance the rest of the system already
-- takes: for hub partners the gap *level* is not the signal — movements
-- relative to the partner's own baseline are (see the transshipment_hub
-- caveat text and anomalies.detect_mirror_gap_trends).
--
-- Idempotent: the UPDATE is a no-op once applied (the WHERE clause only
-- matches the stale text). Safe to re-run.

UPDATE transshipment_hubs
   SET notes = 'Rotterdam — largest container port in Europe; well-documented Chinese-goods routing into the EU. Expect a structural positive mirror gap from re-export routing, but weight movements relative to NL''s own baseline over the absolute level.'
 WHERE iso2 = 'NL'
   AND notes LIKE '%65-70%% Eurostat-higher%';
