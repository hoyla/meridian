# Forward work: 2018 GACC release parser — IMAGE-ONLY BLOCKER

Status updated 2026-05-10 during Phase 6.7. The original scope of this
doc was "fix the title-parser regex so 2018 section-4 release pages
parse cleanly". That work is **DONE** in Phase 6.7's commit, but it
did NOT recover the underlying data because the **2018 section-4
release pages are image-only** (`<div class="atcl-cnt"><img
src='\Excel\4-RMB.png'/></div>`) — they embed PNG screenshots of an
Excel table rather than parseable HTML. 2019 onwards has proper
`<table>` elements.

## What shipped in Phase 6.7

The parser now handles all three 2018 title-format quirks the
original doc identified, plus a fourth that surfaced during this
phase's investigation:

1. **Alternative wording**: "China's Total Value of Imports and
   Exports by Major Country (Region)" alongside the modern
   "China's Total Export & Import Values by Country/Region".
2. **Trailing period after month abbreviation**: "Jan." accepted
   as well as "Jan".
3. **Missing `(in CCY)` suffix on the page title**: caller passes
   `expected_currency` from the discovery side; parser uses it as
   a fallback when the title doesn't supply one.
4. **(NEW)** **Missing date entirely**: some 2018 section-4 release
   pages reuse the bulletin-row title verbatim with no month/year
   appended at all (Jul 2018: "China's Total Export & Import Values
   by Country/Region (in CNY)"). A fallback regex matches the
   no-date shape; caller passes `expected_period` from the
   discovery side; parser uses it.

Plumbing: `discover_release_urls` already captured both currency and
year/month per `DiscoveredRelease`. `scrape_release` now passes both
through to `parse.parse_response` → `parse_html` → `extract_metadata`
as `expected_currency` and `expected_period`. Pages with the standard
date-bearing title still take the title's values (the discovery-side
fallbacks only fire when the title can't supply them).

Tests cover all four format variants plus a fixture-driven check of
the actual archived Jan 2018 page from `tests/fixtures/`.

## What blocks 2018 mirror-trade ingestion

Title parsing succeeds for all 24 section-4 URLs (12 CNY + 12 USD)
on the 2018 index. But every body parse fails with:

```
ValueError: Section 4 page http://...html has no table inside .atcl-cnt
```

Because the body is a single `<img>` tag pointing to an Excel-rendered
PNG (filenames like `4-RMB.png`, `4-USD.png`). The data exists, but
it's pixels, not numbers.

I checked whether the source Excel files are reachable:

- `http://english.customs.gov.cn/Excel/4-RMB.png` → **200 OK** (the rendered image)
- `http://english.customs.gov.cn/Excel/4-RMB.xlsx` → **404 Not Found**

So the rendered image is the only public artefact.

## Options to recover 2018 GACC mirror-trade

In rough order of cost:

1. **OCR the PNGs**. ~24 files (12 months × 2 currencies). Tabular
   numeric OCR with a quality engine (Textract, Tabula-OCR, or
   open-source equivalents) is plausible but accuracy on Chinese-
   sourced English-rendered tables is variable. Editorial risk:
   one number wrong per page is a story-killer. Would need a
   verification step (cross-totals against published headlines).

2. **Find the underlying Excel via another GACC URL pattern**. The
   image filename `4-RMB.png` suggests a deterministic naming
   scheme — there might be `Excel/2018/Jan/4-RMB.xlsx` somewhere on
   the server. Worth a few exploratory curls if a 2018 mirror-trade
   story becomes editorially load-bearing.

3. **Use a different source for 2018 mirror-trade**.
   - **Eurostat** already covers 2017+ on the EU import side
     (Phase 5 backfill).
   - **HMRC** already covers 2017+ on the UK side (Phase 6.1).
   - The EU/UK side can compute YoY without GACC. What we lose is
     the *mirror gap* — the comparison between what China says it
     exported to the EU and what the EU says it imported from
     China. That comparison is the editorial value of GACC.
   - For 2018 specifically, the Section 232 (US steel/aluminium)
     tariff story is one we'd want to mirror-check, but the gap is
     primarily a CIF/FOB plus transshipment artefact at that scale,
     not a smoking-gun discrepancy. So the editorial cost of
     missing 2018 mirror-gaps is bounded.

4. **Accept the 2018 mirror-trade gap**. The EU-side and UK-side
   data fully cover 2018; only the cross-source comparison is
   missing. For most stories this is acceptable — the 2018
   Section 232 validation in `shock-validation-2026-05-09.md`
   already runs on the Eurostat side without needing GACC.

## Recommendation

Defer until a journalist needs a 2018 mirror-gap specifically.
Option 1 (OCR) is the only path to the data and it's a ~half-day
investment with editorial risk that needs verification. Option 2
(find the Excel) is worth ~30 minutes of exploration if/when this
becomes the bottleneck.

The parser fixes shipped in Phase 6.7 mean we're ready to ingest
the moment the data becomes reachable in machine-readable form.

## Why the original doc misjudged the scope

The original forward-work doc was written in Phase 5.4 based on the
title-format failures only — the body-parse failure surfaced only
when the title fixes were in place. A useful general lesson:
"unrecognised release title" hides downstream parser failures
because the script never reaches the body parser at all.
