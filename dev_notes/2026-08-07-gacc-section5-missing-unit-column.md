# GACC section-5 column shift: the April-2025 USD page has no Quantity Unit column

**Date:** 2026-08-07
**Status:** FIXED (branch `ljh-gacc-s5-missing-unit-column`); release re-ingested,
corrected rows live at `version_seen = 2`.
**Severity:** latent data corruption — 62 observations wrong in the DB, but
**nothing published was affected**: no finding referenced any of them.

## Symptom

One GACC release held column-shifted values:
`source='gacc'`, `period=2025-04-01`, `section_number=5` (exports),
`currency='USD'` — release id 1038,
`http://english.customs.gov.cn/Statics/5f7ac0d0-b47e-4fad-bb4f-b21739f10a7c.html`.

The visible tell was a **number where a unit string belongs**:

```sql
SELECT o.id FROM observations o JOIN releases r ON r.id = o.release_id
 WHERE r.source = 'gacc' AND o.source_row->>'quantity_unit' ~ '^[0-9,.]+$';
```

For `Motor vehicles（including chassis fitted with engines)` the USD row stored
`monthly_quantity = 11164.1` — 111 million cars exported in a single month,
against 56–99 (units of 10,000 autos) in every other month — and
`monthly_value = 215.8`. The CNY sibling for the same month
(`Statics/4d9157fb-…`, release 1018) parsed correctly:
`monthly_value = 801.5`, `monthly_quantity = 62.0`, `quantity_unit = "10,000 Autos"`.

## Cause

The USD page **omits the Quantity Unit `<td>` from every body row** and pads the
end with a blank `<td>`. The row therefore keeps its expected 10-cell width, so
`_parse_section_5_6_commodities`'s cell-count guard — the guard that catches
column drift by rejecting any row that is not exactly 10 cells — passed it
straight through, and every field landed one place to the left.

Cars row, both pages, cell by cell:

```
CNY: [label] [10,000 Autos] [62.0] [801.5]    [215.8] [2,649.8]  [187.4] [2,548.4]  [15.2] [4.0]
USD: [label]                [62.0] [11,164.1] [215.8] [36,897.6] [187.4] [35,883.4] [15.2] [2.8] []
      ^label   ^unit         ^mQty  ^mVal      ^ytdQty ^ytdVal     ^pyQty  ^pyVal     ^yoyQ ^yoyV  ^pad
```

The parser read cell 1 as the unit (`"62.0"`) and cells 2–9 as the eight
numbers, so the month quantity became the month value, and so on down the row.

This is a page-authoring quirk on GACC's side, not a transient glitch: the live
page is still byte-identical to the snapshot first stored 2026-05-09
(sha256 `a8909f6b…703b`, re-fetched 2026-08-07). It has to be **parsed**, not
merely rejected.

## The corruption was twice the size the diagnostic showed

The numeric-`quantity_unit` scan finds 34 observations. The true count is **62** —
the whole release (31 commodities × `monthly` + `ytd`).

The 28 it misses are the **value-only commodities** (`Agricultural products*`,
`Mechanical and electrical products*`, `Hi-tech products*`, `Plastic products`,
…). Those rows carry `-` in the shifted unit slot, so `quantity_unit` came out
NULL — no numeric tell — while the *value* landed in `quantity` and `value_amount`
was left NULL. Release 1038's shape gave it away: 17 rows with a value and 31 with
a quantity, where its CNY sibling had 31 with a value and 17 with a quantity.

**Scope is still one release.** Two independent detectors, run over all 335
stored section-5/6 releases (~20k observations), both return only 1038:

* `quantity_unit ~ '^[0-9,.]+$'` — 34 rows, all in 1038
* `value_amount IS NULL AND quantity IS NOT NULL` — 28 rows, all in 1038

Any page with this layout necessarily produces the first signature (some row has
a real quantity), so release-level detection was sound — only the per-observation
count was understated. No finding referenced any of the 62
(`findings.observation_ids && …` → 0), so nothing published was ever wrong.

## Fix

**1. Parse the variant** (`parse._section56_has_unit_column`). Detected at table
level, on the **last** cell rather than the first: a blank trailing cell is the
padding, and the real final column (GACC's published YoY value %) is never blank
on a healthy page. Required blank on *every* candidate row, so one odd row cannot
flip the table's interpretation. When absent, the numeric slice shifts one place
left — same width either way.

`quantity_unit` stays **NULL** for these rows. The unit is genuinely not in the
document, and borrowing it from the CNY sibling would write an inference into a
source-material field (principle 3). `source_row.unit_column_absent = true`
records why, and is stamped only on the variant so normal rows keep their
existing `source_row` shape on re-scrape.

**2. Two alignment guards** in `parse.section56_floor_check`, so a layout we have
*not* seen fails loud (`scrape_runs` `'failed'`, no release row, next walk
retries) instead of persisting shifted values:

* a `quantity_unit` that is wholly numeric — the quantity column read as the unit;
* a row with a quantity but no value — the mirror signature, and the only one that
  sees the value-only rows. GACC section 5/6 are value tables: every commodity has
  a value, and a quantity only where a unit applies, so quantity-without-value is
  never legitimate.

Both are zero-tolerance, on the empirical basis above (zero legitimate instances
in ~20k observations). The count floor alone cannot catch a shift — ~30 rows of
plausible-looking numbers are still there.

**3. Regression test** — `tests/fixtures/release_section5_major_exports_apr2025_usd.html`
is the real page (the stored snapshot, byte-identical to live).
`tests/test_gacc_section56_parser.py` asserts the corrected alignment against the
CNY sibling's quantities, and reproduces the exact pre-fix parse by patching
`_section56_has_unit_column` to `True` — that reproduction yields precisely the
34 numeric-unit and 28 orphan-quantity rows found in the DB, and each guard
rejects it independently.

## Re-ingest

```bash
python scrape.py --url "http://english.customs.gov.cn/Statics/5f7ac0d0-b47e-4fad-bb4f-b21739f10a7c.html" --force-refetch
# Persisted: {'inserted': 0, 'versioned': 62, 'unchanged': 0}
```

Append-only: the 62 corrupt rows stay at `version_seen = 1` as history, the
corrected rows are `version_seen = 2`. `anomalies.py` reads `DISTINCT ON … ORDER BY
version_seen DESC`, so the live view is the corrected one — verified 0 NULL values,
0 numeric units, 0 orphan quantities.

Independently validated against the CNY sibling, which never went through the
broken path: **all 62 quantities match exactly**, and the implied rate
(13.563–13.971 USD-million per CNY-100-million ⇒ CNY/USD 7.16–7.37) is April 2025's
actual rate.

## Gotcha for next time

The original diagnostic still returns 34 rows, and always will — those are the
retained `version_seen = 1` history rows, which is the point of append-only. Any
future corruption scan must read the **live view**, not the raw table:

```sql
WITH live AS (
  SELECT DISTINCT ON (release_id, commodity_label, period_kind) *
    FROM observations o JOIN releases r ON r.id = o.release_id
   WHERE r.source = 'gacc' AND r.section_number IN (5, 6)
   ORDER BY release_id, commodity_label, period_kind, version_seen DESC)
SELECT * FROM live
 WHERE quantity_unit ~ '^[0-9,.]+$'
    OR (quantity IS NOT NULL AND value_amount IS NULL);
```
