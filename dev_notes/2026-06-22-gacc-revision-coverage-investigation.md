# GACC revisions & series coverage — investigation + decision (2026-06-22)

**Trigger.** While adding GACC overdue detection (PR #50,
`release_calendar._GACC`), the question came up: *do we check for updates /
backfills to **existing** GACC releases?* China is known to tweak trade data
after first publication, and our routine only ever detects **new** releases —
never re-examines ones already ingested.

**Decision (TL;DR): no code change.** The preliminary series we ingest is
immutable at the URL, and the one freely-available revised series (the English
Monthly Bulletin) restates the headline by ~0.001% — immaterial — and does not
expose the by-country / HS detail where our findings actually live. The
existing blanket `cn8_revision` analytical caveat remains the right mitigation
for the revision risk we can't see. Recorded here so it isn't re-investigated.

---

## What the code does

- `scrape.scrape_release` skips any URL that already has a terminal
  `success` / `no_parser` run unless `--force-refetch` is passed
  (`scrape.py`, the `gacc_release_url_already_processed` guard). The daily
  routine probe calls `run_scrape(force_refetch=False)`, and `--force-refetch`
  is **only ever set by hand** — nothing schedules a re-check sweep.
- The *data model* is fully revision-capable, though:
  `db.upsert_observations` has a slow path that re-SELECTs each observation by
  its dimensional key and, on a changed value, **inserts a new row with
  `version_seen` bumped** (append-only, never overwrites);
  `db.find_or_create_gacc_release` is `ON CONFLICT … DO UPDATE` and refreshes
  display fields; snapshots are append-only. So a re-ingested revision *would*
  be captured as versioned history — the machinery just never runs for
  already-ingested URLs under the routine.

## Evidence — preliminary pages are immutable at their URL

Read-only queries against the live working DB (`DATABASE_URL`) + the project
fetcher (`api_client.fetch`, which handles GACC's self-signed HTTPS cert —
note `WebFetch` cannot, it force-upgrades to HTTPS and fails):

- **26,544 GACC observations, every one `version_seen = 1`.** No revision has
  ever been captured.
- **160 release-bearing GACC URLs were fetched more than once; 0 ever returned
  different bytes** (`content_sha256` identical across every re-fetch, back to
  2019). The single URL with 44 snapshots was a page that *consistently failed
  to parse* — byte-identical, never produced data, not a revision.
- All 161 GACC releases are `preliminary` / `preliminary_jan_feb`; period range
  2019-01 → 2026-05.

→ GACC publishes each preliminary release to a UUID page
(`/Statics/<uuid>.html`) whose content is **frozen once published**. The
routine's no-refetch behaviour is therefore *correct*, and a scheduled
`--force-refetch` sweep over known URLs would find nothing. (An earlier
hypothesis that same-URL revisions were silently slipping past us is empirically
**false**.) Real movement only ever arrives as a new period at a new URL — which
the index walk handles, and which the overdue work now flags when late.

## GACC publishes four statistics series — we ingest one

From the site nav (`/statics/report/preliminary.html`), the Statistics menu
lists: **Preliminary Release · Trade Indices · Monthly Bulletin · Quarterly
Release**. `SEED_INDEXES` walks **Preliminary Release** only. The
`_is_index_url` helper already pattern-matches `/statics/report/monthly`, but
nothing seeds it and `parse.py` only knows the preliminary layout.

The **Monthly Bulletin** (`/statics/report/monthly.html`) is GACC's
final/detailed series — same 2018–2026 archive, same immutable UUID pages, but
delivered as structured `.xls` (numbered tables, CNY + USD). It is the natural
home of revised figures.

## Empirical revision check — Monthly Bulletin vs our preliminary

Compared the Monthly Bulletin Table 1 ("Summary of Imports and Exports", USD,
*A: Annually* view) for Dec-2025 against our stored preliminary **cumulative /
YTD** 2025-12 figures (US$ million):

| Metric | Preliminary (our DB) | Monthly Bulletin (final) | Revision |
|---|---|---|---|
| Export | 3,771,873.3 | 3,771,842.4 | **−30.9m** (−0.0008%) |
| Import | 2,582,896.1 | 2,582,896.1 | ≈0 (Δ 0.04) |
| Total  | 6,354,769.4 | 6,354,738.6 | **−30.8m** (−0.0005%) |

→ **GACC does revise** — the preliminary headline is provisional and the
Bulletin restates it (the $30.9m export cut is well above our stored value's 0.1
rounding step, so it's a genuine revision, not an artefact). But at the topline
the revision is **negligible** (~0.001%), nothing that would move a
trade-balance story.

## Why no code change

1. **Topline revisions are immaterial.** Ingesting the Bulletin buys a ~0.001%
   headline correction.
2. **The English Bulletin only exposes Table 1 (summary totals).** The
   by-country and HS-level tables — where our findings actually live, and where
   revisions could be *materially* larger — are **not in the English edition**
   at all (Chinese site / paid product). So we cannot quantify partner- or
   commodity-level revision magnitude from what's freely available, and we
   couldn't ingest revised partner detail even if we wanted to.
3. Actually ingesting the Bulletin is non-trivial regardless: new parser
   (different numbered-table `.xls` layout), new `SEED_INDEXES` entry, new
   `release_kind`.
4. The blanket **`cn8_revision`** caveat (see `docs/methodology.md` §9) already
   flags every cross-period GACC claim as revision-exposed — the right
   defensive posture for the revision risk we can't observe.

## What would change this decision (triggers to revisit)

- A reporter needs partner- or HS-level figures at *final* precision, not
  provisional — then sourcing the Chinese-edition detailed tables (or another
  final source) becomes worth scoping.
- Evidence that headline preliminary→final revisions grow materially (e.g. a
  future month diverging >0.5%) — re-run this Bulletin-vs-preliminary diff to
  recheck.
- We start ingesting the Quarterly Release / Trade Indices for other reasons and
  Bulletin ingestion can ride along.

## How to reproduce

- Preliminary immutability: `SELECT version_seen, COUNT(*)` over `observations`
  joined to `releases WHERE source='gacc'`; and per-URL
  `COUNT(DISTINCT content_sha256)` over `source_snapshots` (all = 1).
- Bulletin diff: `api_client.fetch` the year index
  `/statics/report/monthly2025.html` → Dec table-1 page → its `.xls`
  (backslash-normalise + percent-encode the Chinese path) → read with pandas
  `engine="xlrd"`; compare the 2025 annual row to preliminary YTD 2025-12 USD.
