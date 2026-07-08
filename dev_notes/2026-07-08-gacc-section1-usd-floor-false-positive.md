# GACC held-back false alarm: currency/unit floor + the standing `failed` backlog

**Date:** 2026-07-08
**Status:** FIXED (branch `ljh-gacc-section1-usd-floor-fix`); verified live.
**Severity:** low (no data corruption) — but fired a false "ingest problem"
alert on every routine walk, now surfaced to the Meridian chat Space.

## Symptom

The daily routine's GACC probe reported, twice a day:

```
gacc: error × due (candidate 2026-06) — held back 32 release(s) this walk —
GACC page http://english.customs.gov.cn/Statics/d7718500-...html self-inconsistent:
title declares currency 'USD' but the page's Unit: row reads 'USD 100 Million'.
```

`--notify-chat` then posted `held back: GACC` to the Space, and the push
heartbeat led with a (wrong) "June release held back" line.

## First-pass diagnosis (correct, but incomplete)

`d7718500` is `(1) China's Total Export & Import Values, May 2026 (in USD)` —
GACC **section 1** (headline totals). `parse.extract_metadata` applied a single
canonical USD unit (`_CANONICAL_GACC_UNIT["USD"] = "USD1 Million"`) to **every**
section and raised "self-inconsistent" when the page's Unit row differed. But
section-1 totals are legitimately denominated in `USD 100 Million` (亿美元);
only section-4 by-country tables — the only section we ingest — use
`USD1 Million`. So a section-1 USD page (which *agrees with itself* on currency;
only the scale differs) got mis-flagged. Confirmed against Feb 2026 `01692e74`,
same `Unit: USD 100 Million`.

That is real and is fixed (see fix 1 below). **But it is only 1 of the failures
driving the alarm.**

## What the DB actually showed (the correction)

`--source-status` and a `scrape_runs` audit revealed the "held back 32" is a
**standing backlog** of pages that re-`failed` on *every* walk, going back to
**2026-05-09** (≈32/walk, 64 `failed` rows/day). By latest-status-per-URL:

| bucket | count | what they are |
|---|---|---|
| `no table inside .atcl-cnt` | 24 | **2018** section-4 by-country pages — GACC's 2018 English site carries no inline HTML table (zero 2018 section-4 releases are ingested; nothing to recover) |
| `Unrecognised release title` | 7 | **2018** non-section-4 pages (annual totals, by-trade-mode, commodity) — fail at the title regex; never ingested |
| `self-inconsistent` (floor) | 2 | `d7718500` (section-1 USD, the false positive) + `03a39470` (Jun-2025 section-4, the *genuine* release-184 reject) |

Year spread across all 33: 29×2018, 1×2025, 1×2026, 2×undated-2018. Latest live
section-4 period is 2026-05. **None of the 33 is a genuinely missing current
release.**

Two structural facts explain why the alarm is new even though the failures are
two months old:

- **The visibility is new, not the failures.** The `no table` / unrecognised
  pages have failed benignly since 2026-05-09. The 2026-07-07 surfacing commit
  (`2fa41ba`, "surface a held-back release") added `_failed_gacc_runs_since` →
  `error`, which turned the *entire* standing backlog loud — not just
  `d7718500`. Fixing `d7718500` alone takes 32→31; the alarm keeps firing.
- **`03a39470` never contributed to the daily alarm.** It has *prior success*
  rows (it ingested fine before the floor landed 2026-05-19), so
  `gacc_release_url_already_processed` returns `'success'` and the walk **skips**
  it. Its lone `failed` row (2026-05-19) is stale and inert — never re-fetched,
  never counted by `_failed_gacc_runs_since`. The real daily set was
  `d7718500` (1) + 24 no-table + 7 unrecognised = **32**.

Root cause, generalised: **pages we can never turn into section-4 observations
were recorded `failed` (retried + alerted) instead of `no_parser` (terminal +
silent).** `failed` is for a *real current* table that regressed and is worth
retrying/surfacing; these are permanent historical dead-ends.

## Fix (shipped)

Three changes, all keyed on real semantics (no recency heuristics, no schema
change, no probe-side re-parsing):

1. **Scope the currency/unit floor to section 4** (`parse.extract_metadata`,
   `if section == 4:`). Section-1/2/3 pages fall through to the
   `NotImplementedError` → `no_parser` path exactly as before the floor
   (2026-05-19). `d7718500` → `no_parser`. The release-184 guard is untouched
   for section 4.

2. **Reclassify permanently-unparseable pages to `no_parser`.** New
   `parse.UnparseableReleasePage(NotImplementedError)` is raised for an
   unrecognised title and for a section-4 page with no `.atcl-cnt` table.
   `scrape_release`'s existing `except NotImplementedError` retires them
   terminal `no_parser`, so the walk stops retrying and the alert stays quiet.
   A genuine current-table regression (empty/partial parse, column drift) still
   raises `ValueError` → `failed` → retried/surfaced. Relies on GACC's
   documented invariant that a modern section-4 page always carries its table
   once published.

3. **Make the floor superseded-aware.** New
   `parse.CurrencyUnitMismatch(ValueError)` carries the resolved
   `(section, period, currency)`. `scrape_release` checks
   `db.gacc_release_exists(...)`: if a live canonical sibling already covers the
   cell → `no_parser` (superseded duplicate, not re-alerted); if not → `failed`
   (a genuine held-back release, release-184 protection preserved). This is the
   correct guard for a *fresh* release-184 recurrence (a mismatched page with a
   good sibling but no prior success of its own); `03a39470` is already handled
   by the prior-success skip, so it stays an inert stale `failed` row.

Alternatives rejected: per-section canonical units (adds a unit we never
consume); a probe-side period/recency filter on `_failed_gacc_runs_since`
(needs each failed URL's period, which requires re-parsing — the very thing that
failed — and leaves the wasteful twice-daily re-fetch of 31 dead pages in
place). Reclassifying at the two semantic sources stops the re-fetch too.

## Tests

- `tests/test_parse.py`: section-1 USD page → `no_parser`/`NotImplementedError`,
  not a floor error; section-4 CNY-title/USD-unit still raises (release-184);
  `CurrencyUnitMismatch` carries `(section, period, currency)` and stays a
  `ValueError`; unrecognised title and no-table section-4 page raise
  `UnparseableReleasePage`.
- `tests/test_gacc_currency_floor_supersede.py` (DB-backed): floor reject with a
  live sibling → `no_parser`; without → `failed`; `gacc_release_exists` matches
  on the cell regardless of `release_kind`.
- Full suite: 695 passed, 6 skipped.

## Live verification (2026-07-08)

Before: `gacc: error × due — held back … self-inconsistent …`; 33 URLs
latest-status `failed`.
After `--probe-source gacc`: **`gacc: no_change × due (candidate 2026-06) —
walked indexes, no new releases`** — the walk retired all 32 retryable
backlog URLs to `no_parser`; `--source-status` dropped the GACC error line.
latest-status-per-URL: `no_parser 966, success 163, failed 1` (the lone `failed`
is the inert stale `03a39470`). May/Apr section-4 releases (CNY+USD, 4 rows)
untouched.

## Side note — stale notification

The 2026-07-08 07:41 run's push and Meridian-Space post said "GACC June release
held back," wrong on both period and substance. No correction was posted (to
avoid noise); the alarm stops on its own now the backlog is `no_parser`.
