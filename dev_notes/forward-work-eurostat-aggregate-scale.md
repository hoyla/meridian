# Forward work: EU-27 aggregate absolute scale runs ~2x Eurostat headline ✓ RESOLVED

Original captured 2026-05-10 during Phase 6.1f validation. Resolved
2026-05-10 during the autonomous-work session: the 2x was caused
entirely by Eurostat's bulk file shipping a `product_nc='000TOTAL'`
aggregate row alongside the CN8-detail rows. Naïve un-filtered sums
double-counted. **HS-group analysers were never affected** because they
all apply HS-pattern LIKE filters (e.g. `'8507%'`) that don't match
`'000TOTAL'`. Editorial impact: zero. Sanity-check guidance: any
direct `SUM(value_eur)` over `eurostat_raw_rows` MUST filter
`product_nc != '000TOTAL'`.

## The original symptom

Direct sums over `eurostat_raw_rows` for sanity-checking against
Eurostat's published headlines:

| Cut | Our data 2024 | Eurostat published 2024 | Ratio |
|---|---|---|---|
| EU-27 imports from CN, full year | €998B | ~€517B | 1.93x |
| EU-27 exports to CN, full year | €369B | ~€213B | 1.73x |
| Germany alone, imports from CN, 2024 | €177B | ~€155B | 1.14x |

Per-country numbers were roughly right (Germany 14% high — within
plausible for stat_procedure mix); the factor only inflated as we
summed across reporters, pointing at a structural double-count
somewhere in the aggregation.

## What it actually was

Eurostat's bulk file (`full_v2_YYYYMM.7z`) emits, **per
(reporter, period, partner, flow, stat_procedure)**, both:

1. The CN8-detail rows (one per 8-digit code), and
2. A single grand-total row with `product_nc = '000TOTAL'` representing
   all-products trade for that slice.

Naïve `SUM(value_eur)` includes both, so detail + total = roughly 2x
the true value. The 'roughly' is because the `'000TOTAL'` row reflects
ALL trade (including confidentiality-suppressed flows that Eurostat
strips out of the per-CN8 detail), whereas the CN8 detail sum is
slightly lower by the suppression rate.

The bulk file also includes **chapter-level X-suffix codes** like
`'85XXXXXX'` and `'850610XX'`. These look like aggregates but are
actually **confidentiality residuals** — Eurostat aggregates flows that
would otherwise identify a single trader and reports them under an XX
suffix. Sum of `'85XXXXXX'` is much smaller than the sum of `'85NNNNNN'`
(€961k vs €1.93B for DE / Jun 2024). Including them in HS-pattern
LIKE-matched sums is correct; they're distinct flows, not duplicates.

## The reconciliation

```
2024 EU-27 imports from CN:
  CN8 detail, all stat_procedure:           €517.1B  ← matches published
  CN8 detail, stat_procedure=1 (headline):  €491.1B
  '000TOTAL' row, all stat_procedure:       €525.7B  ← per-reporter grand totals
  '000TOTAL' row, stat_procedure=1:         €499.7B
  Eurostat published headline:              ~€517B   ← exact match
```

Per-reporter suppression rate (CN8-detail-sum vs `'000TOTAL'`-row,
stat_procedure=1, 2024 EU-27 imports from CN): mostly 0-2%, a few
outliers — DK -11.4%, ES -8.1%, FI -5.2%. Editorially: when a story
rests on a specific reporter's flows, especially DK/ES/FI, expect the
CN8-detail sum to undershoot the published per-country headline by
that suppression rate.

## What we ruled out (and were right to)

- `stat_procedure` double-counting (eliminated by the 2x persisting
  after the filter — but for the wrong reason; the actual cause is
  the orthogonal `'000TOTAL'` issue).
- `trade_type` double-counting (eliminated correctly).
- Zero/NULL value rows (eliminated correctly).
- GB pre-Brexit double-count (eliminated correctly via Phase 6.0.5).

## Code action

No code change to the analysers — they all apply HS-pattern LIKE
filters that don't match `'000TOTAL'`. Added a smoke test
(`tests/test_eurostat_scale_reconciliation.py`) that asserts the CN8
detail sum matches the `'000TOTAL'` sum within suppression bounds,
to detect any future regression where a bulk-file format change
introduces another aggregate row class.

Convention added in code via comment in `anomalies.py`:

```python
# All direct sums of eurostat_raw_rows.value_eur MUST filter
# product_nc != '000TOTAL' (a per-reporter grand-total row that
# would double-count the per-CN8 detail). HS-pattern LIKE filters
# already exclude it because '000TOTAL' doesn't match any HS
# pattern; ad-hoc sums for sanity checks must add the filter
# explicitly.
```

## Lessons

1. **The "rule out" reasoning was reasonable but missed an axis.**
   Stratifying by `stat_procedure`, `trade_type`, `reporter` looked
   correct because they were the non-X dimension columns we were
   thinking about. The aggregate axis was hiding inside `product_nc`
   itself — a column we didn't think of as carrying aggregates.
2. **Always check distinct values of every dimension column, even
   ones that "look" like detail-level only.** The `'000TOTAL'` row
   in `product_nc` would have surfaced immediately from
   `SELECT product_nc FROM ... GROUP BY product_nc ORDER BY value_eur`.
3. **Per-country reasonableness is not enough.** Each Germany row
   was ~14% high, which we attributed to CIF/FOB methodology — but
   actually it was the per-reporter `'000TOTAL'` row added once
   (within Germany's slice). The single-reporter check looked
   merely "high"; only when we summed across all 27 did the doubling
   become visible as an editorial absurdity.
