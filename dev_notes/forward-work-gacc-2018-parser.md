# Forward work: 2018 GACC release parser

Captured 2026-05-09 during the clean-state rebuild (Phase 5.4). The Eurostat
backfill went all the way to 2017-01; the GACC backfill stops at 2019-01
because **2018-era GACC release pages use a sufficiently different title
format that the current parser fails on every release that year**. This is
fixable but the work was non-trivial enough that we shipped without it.

## The symptom

When `scrape.py` walks `preliminary2018.html` and tries each section-4
release URL, every page raises:

```
ValueError: Unrecognised release title:
"China's Total Value of Imports and Exports by Major Country (Region), Jan. 2018"
```

The `try/except` in `scrape_release` catches this and logs it as a
`status='failed'` scrape_run; ingestion continues to the next URL. So the
script finishes cleanly but the DB has zero 2018 GACC releases.

## Why the parser misses

The 2018 release-page title has FOUR divergences from the modern
(2019+) format the regex in `parse._RELEASE_TITLE_RE` was tuned against:

| Aspect | 2019+ format | 2018 format |
|---|---|---|
| Wording | `"China's Total Export & Import Values by Country/Region"` | `"China's Total Value of Imports and Exports by Major Country (Region)"` |
| Section prefix | `(4)` (handled by 2026-05-09 fix) | absent (handled by 2026-05-09 fix) |
| Month | `"Mar 2026"` or `"March 2026"` | `"Jan. 2018"` (trailing period) |
| Currency | `"(in CNY)"` suffix on every page | **absent entirely** |

The 2026-05-09 fix made the `(N)` prefix optional and added RMB → CNY
normalisation, both for the 2018 monthly summary section (section 1)
which uses `"China's Total Export & Import Values, July 2018 (in CNY)"`.
Section-4 in 2018 uses an entirely different wording (`"Total Value of
Imports and Exports by Major Country (Region)"` instead of `"Total
Export & Import Values by Country/Region"`) and the month-with-period
form (`"Jan."`), and most critically **omits the `(in CNY|USD)` suffix
on the page title even though the parent index-page bulletin row labels
the link as CNY or USD**.

## What it would take to fix

Three changes layered:

1. **Relax the regex**: accept the alternative description wording;
   accept the month-with-trailing-period form; make the `(in CCY)`
   suffix optional.

2. **Plumb currency through**. `discover_release_urls` in `api_client.py`
   already captures the currency from the bulletin title in the index
   page (section row tagged `(in CNY)` vs `(in USD)`). It returns a
   `DiscoveredRelease` with `currency` set. But that gets discarded —
   `scrape_release` is called with just the URL, and `parse_response`
   re-extracts everything from the page itself. To make currency
   inference robust to titles that omit it, pass the discovered
   currency through:

   - `scrape_release(url, release_kind, *, discovered_currency: str | None = None, ...)`
   - `parse_response(response, *, expected_currency: str | None = None)`
   - `extract_metadata(soup, url, *, expected_currency: str | None = None)`

   Then `extract_metadata` uses `m.group("currency") or expected_currency`
   when assigning `currency`. The discovered currency is the source of
   truth; the title becomes a confirmation when present.

3. **Add coverage**: a 2018-format fixture in `tests/fixtures/` and a
   test that confirms a section-4 release with the divergent title
   parses to `(section=4, period=2018-01-01, currency=CNY)`.

## Why we didn't ship it

Time-box: the Phase 5 rebuild was already a multi-hour pipeline, and
the work to plumb `discovered_currency` through three function call
sites with a covering test is ~30-60 minutes of focused dev. The
editorial value of the missing year is bounded:

- **Mirror-trade for 2018 needs it.** Without 2018 GACC, the
  mirror-trade analyser produces zero comparisons for 2018 even though
  Eurostat has 2018 data. So 2018 mirror-gap findings are absent. The
  Section 232 tariff validation (Phase 5.6) for 2018 has only the
  Eurostat-side YoY/trajectory signal, not the mirror-gap signal.

- **Hs-group analyses are unaffected.** They read from
  `eurostat_raw_rows`, not GACC. The 2017-01 → 2026-02 Eurostat
  backfill is complete; YoY anchors from 2018-12 onwards have full
  24-month windows.

So 2018 mirror-trade is the only editorial loss. Not zero, but not
load-bearing for the current investigations.

## How to pick this up

1. Read this doc.
2. Curl one 2018 section-4 URL to see the actual HTML structure (title +
   pub-date + unit) — paths in the log:
   `http://english.customs.gov.cn/Statics/851cff3d-297f-4cf3-a500-5241199cc957.html`
   was the first one that failed in our 2026-05-09 run.
3. Save that HTML to `tests/fixtures/release_section4_by_country_jan2018_cny.html`.
4. Add a failing test in `tests/test_parse.py` covering the 2018 format.
5. Implement the three changes above. Aim to keep the
   `expected_currency` plumbing optional so 2019+ behaviour is
   unchanged.
6. Re-run `python scrape.py --url http://english.customs.gov.cn/statics/report/preliminary2018.html`
   to backfill 2018 only (idempotent — won't touch existing 2019+ data).
7. Re-run `python scrape.py --analyse mirror-trade` to extend the
   mirror-trade findings into 2018.
