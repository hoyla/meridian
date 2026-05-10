# Forward work: EU-27 aggregate absolute scale runs ~2x Eurostat headline

Captured 2026-05-10 during Phase 6.1f validation (HMRC ingest close-out).
Pre-existing methodology concern — affects all eu_27-scope absolute
totals since Phase 1, not a Phase 6.1 regression. YoY ratios are
unaffected (numerator and denominator both subject to the same factor)
so headline editorial comparisons stay valid; the issue surfaces only
when sanity-checking absolute EUR magnitudes against Eurostat's
published headline numbers.

## The discrepancy

Our `eurostat_raw_rows` aggregated per-reporter and summed:

| Cut | Our data 2024 | Eurostat published 2024 | Ratio |
|---|---|---|---|
| EU-27 imports from CN, full year | €998B | ~€517B | 1.93x |
| EU-27 exports to CN, full year | €369B | ~€213B | 1.73x |
| Germany alone, imports from CN, 2024 | €177B | ~€155B | 1.14x |

Per-country numbers are roughly right (Germany 14% high — within plausible
for CIF-vs-FOB or methodology variance). The factor only inflates as we
sum across reporters, suggesting a structural double-count somewhere in
the aggregation.

## What we ruled out

- **`stat_procedure` double-counting**: filter to `stat_procedure='1'`
  (the headline-flow code); 2x discrepancy persists.
- **`trade_type` double-counting**: only one trade_type appears in our
  data ('E'); not the issue.
- **Zero/NULL value rows**: spot-checked Germany Jun 2024 — only
  positive-value rows; not the issue.
- **GB pre-Brexit double-count**: already excluded by Phase 6.0.5
  (`reporter <> ALL(EU27_EXCLUDE_REPORTERS)`).

## Hypotheses to investigate

1. **Reporter-coverage overlap**. Maybe some EU member states
   double-report transit shipments. NL imports a container from China,
   re-exports to DE; DE customs declares its own "import from China"
   even though the goods physically entered EU at Rotterdam. Our sum
   would count both. Eurostat's headline aggregation may dedupe.
2. **Different aggregation key**. Eurostat's published "EU-27 trade
   with China" might be a centrally-computed aggregate (treating EU-27
   as a single reporter) rather than a sum-of-members. Bulk file
   supplies per-member rows; the EU-27 aggregate row may be supplied
   too but under a different `reporter` code we're not picking up.
3. **PRODUCT_NC inclusion of partial codes**. Some HS codes might
   appear at multiple aggregation levels (HS-2, HS-4, HS-6, CN8) in
   the same row set; our LIKE filtering would catch them all.
4. **STAT_PROCEDURE 9 / Other**. Even with `stat_procedure='1'`
   filter we get the 2x — but maybe there are additional records under
   `stat_procedure IS NULL` or other codes representing "all
   procedures" that overlap with the per-procedure rows.

## Why this isn't urgent

- **Editorial YoY framing is unaffected**. Every percentage-change
  finding in the briefing pack is a ratio of two consistent figures;
  whatever the per-reporter scaling factor, it cancels.
- **UK comparison (Phase 6.1) is unaffected**. HMRC ingest is a
  separate source, Feb 2026 sanity check matched HMRC published
  headlines (£5.77B UK imports from CN, within the £4-7B range from
  ONS releases).
- **Briefing-pack absolute amounts** would mislead a journalist if
  they tried to anchor a story on the absolute level (e.g. "EU
  imports €998B"). The brief currently shows absolute EUR but should
  add an explicit "absolute totals are sum-of-reporter and may differ
  from Eurostat's published EU-aggregate headline by ~2x — see
  caveat" warning until this is resolved.

## When to pick this up

Before any editorial use that quotes our absolute EU-wide totals as a
headline. YoY-only stories (which is most of the briefing pack) are
safe in the meantime.

A reasonable next step: download Eurostat's published "EU trade with
China" datasheet for 2024, compare each reporter's published per-
country imports vs ours line-by-line. Identify where the 2x emerges
— is it specific countries, specific product groups, or a structural
aggregation difference. ~half a day of investigation.
