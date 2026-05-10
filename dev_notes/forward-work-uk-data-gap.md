# Forward work: UK trade data is missing

Captured 2026-05-09. Surfaced after the user clarified the editorial
target is **the Guardian** — a UK publication. The current ingest
covers GACC + Eurostat. **Eurostat does not include the UK** since
Brexit (the UK left the EU on 31 Jan 2020, fully exited Eurostat
reporting from 2021-01). So our briefing pack systematically misses
UK-China trade — which is the most directly relevant comparison for
a Guardian piece.

This is a structural scope issue, not a bug. Worth fixing before the
tool gets handed to a Guardian journalist for serious use.

## What's missing today

Looking at active findings post-Phase-5 backfill:

- **Mirror-trade**: GACC China-export-to-UK vs ??? — we don't have
  the EU import side equivalent. Eurostat doesn't report UK. The
  finding for `partner=GB` simply doesn't exist.
- **HS-group-yoy**: Eurostat aggregates over EU member reporters
  (DE, NL, FR, IT, etc.). UK is excluded. So when the brief says
  "EV imports from China grew +34%", that's EU+27 imports — not
  UK imports. A Guardian piece quoting the figure would need to
  caveat "in the EU; UK separate" or use UK numbers directly.
- **Editorial-sources.md**: Lisa O'Carroll's article is about
  EU (she's Brussels correspondent), so the EU framing is correct
  for *her* piece. But for any UK-domestic angle (UK industry
  affected by Chinese imports, UK steelworks closures vs
  alternative sourcing, etc.) we have nothing.

## Three options for filling the gap

### Option A: HMRC (UK government) trade statistics — RECOMMENDED

UK government publishes monthly trade-in-goods data through HMRC.

**Two access paths, both publicly accessible** (probed 2026-05-09):

1. **Bulk ZIP files** at `https://www.uktradeinfo.com/trade-data/latest-bulk-data-sets/`
   — naming pattern `bdsimp{YYMM}.zip` (imports), `bdsexp{YYMM}.zip`
   (exports), plus `smka12{YYMM}.zip` and the by-trader files. Mirrors
   the Eurostat bulk-file pattern (`full_v2_YYYYMM.7z`).
2. **OData REST API** at `https://api.uktradeinfo.com` —
   filterable queries via OData syntax, e.g.
   `https://api.uktradeinfo.com/Commodity?$filter=Hs6Code eq '850760'`.
   *This is nicer than Eurostat: we'd avoid the 44 MB per-period
   download by querying just the codes/periods we want.*

- HS classification: UK uses CN8 (same as EU pre-Brexit; UK Global
  Tariff post-Brexit preserves CN8 for goods). Codes match Eurostat
  one-for-one — the existing `hs_groups` patterns work unchanged.
- Coverage: 2022 onwards reliably; pre-2022 may be patchier as the
  post-Brexit reporting infrastructure shifted.

**Editorially**: this is the cleanest source. It's the official UK
trade data. Direct mirror-comparison vs GACC works.

**Implementation effort**: medium. Recommended path: use the OData
API rather than bulk files (cleaner, smaller, supports incremental
queries). Maybe 200-400 LOC for `hmrc.py` + tests + integration into
the analyser pipeline (UK reporter alongside EU reporters in the
per-reporter aggregations). Probably rename the
`eurostat_raw_rows` table to `customs_raw_rows` with a `source`
column distinguishing `eurostat` from `hmrc`, and rename
`eurostat_partners` in the analysers → `comparison_partners`. The
abstraction is "countries we compare GACC against", not specifically
Eurostat partners.

### Option B: UN Comtrade

Has UK + most other countries.
- Source: <https://comtradeplus.un.org/>
- Format: API-driven; rate-limited; needs an API key.
- HS classification: HS6 (less granular than CN8).
- Coverage: deep historical (decades).

**Editorially**: the right source if we want non-EU partner
coverage broadly (US, UK, Brazil, Russia, etc.). Pairs naturally
with the recent `gacc-aggregate-yoy` analyser for non-EU bloc
analysis.

**Implementation effort**: medium-high. API-key setup, rate-limit
handling, less-granular HS data means some current methods need
adaptation.

### Option C: ONS scrape

ONS publishes UK trade stats too, with different granularity.
- Less rich than HMRC but easier to scrape.
- Probably duplicate of A; pick one.

## Recommendation

**Do A first**. UK is the highest-priority gap (Guardian editorial
target); HMRC is the canonical source; CN8 matches Eurostat so the
existing `hs_groups` machinery works unchanged. The new code is
parallel to `eurostat.py` rather than touching it.

Pair with: rename `eurostat_partners` to something like
`comparison_partners` in the analysers — the abstraction is
"countries we compare GACC against", not specifically Eurostat
partners. Then UK rows from HMRC and EU rows from Eurostat live
in the same `observations` table under different `source` values
(`'eurostat'`, `'hmrc'`).

Then **Option B (UN Comtrade)** as a Phase 7 item if the editorial
team wants global coverage beyond EU+UK.

## Until then — explicit caveat in the brief

The briefing pack should say plainly that all EU-side numbers exclude
the UK. Currently the `## Period coverage` section just lists
`eurostat: <range> (<n> releases)` — easy for a journalist to misread
as "the EU including UK". Quick fix: add a note in the
`## Defaults applied to every finding` block.

## Editorial note

The two big political/social lenses Guardian readers care about — UK
domestic industry effects, and the Brexit-era UK-China relationship
specifically — both need UK data. Until we have it, the tool serves
Lisa O'Carroll (Brussels) better than a UK-desk reporter.
