# GACC commodity highlights on the GACC-only page — build note (2026-07-14)

**Trigger.** FT coverage of the June GACC drop (14 Jul) led with sector facts —
car exports +71% YoY to 1.06mn units, ICs, rare earths −34% — that our
GACC-only page can't show: it is partners-only. Luke: include sector
highlights on the GACC page, without granular HS reporting. Time-sensitive
(Lisa is asking now; June lands on the English site within days).

**Correction to the page's premise.** The on-page line "the GACC country
release has no commodity dimension" is true of **section 4** but not of the
source: the preliminary release also publishes **(5) Major Exports** and
**(6) Major Imports by Quantity and Value** — GACC's own curated catalogue of
~30 headline commodities (no HS codes), each with physical quantity + value,
monthly + cumulative-YTD, in CNY and USD variants. Verified against the live
May-2026 section-5 USD page (14 Jul): cars +50.4% YTD value / +48.7% units,
ICs +90.0% YTD, rare-earths exports +44.9% YTD. This IS "sector highlights
without granular HS" — GACC curates the list for us.

**Fit with the two-track design.** No overlap with the main page's sector
layer: main = China↔EU bilateral at CN8 via journalist-curated `hs_groups`;
this = China↔world at GACC's own headline-commodity grain. The world-commodity
view directly evidences the page's framing question ("general Chinese export
surge, or EU-specific re-routing?") — it is the general-surge half.

## Source-table facts (May 2026 section-5 USD page, inspected live)

- Container/table shape matches section 4 (`.atcl-cnt` → `<table>`), same
  Unit row convention ("Unit: USD1 Million" / "CNY 100 Million").
- Columns (regular monthly): commodity label · quantity-unit · month
  {Quantity, Value} · 1-N cumulative {Q, V} · prior-year 1-N cumulative {Q, V}
  · YoY% {Q, V} → **10 cells**, one wider than section 4's, and the *published*
  YoY refers to the **cumulative**, not the month.
- Quantity-only-missing cells are `-` (value-only commodities: plastics,
  textiles, garments, toys…). `_parse_number` already maps `-` → None.
- Hierarchy: three starred aggregate rows ("Agriculture products*",
  "Mechanical and electrical products*", "Hi-tech products*") whose membership
  is **not adjacency** — the footnote says they "include relevant products
  listed in the table", and Hi-tech overlaps Mech&elec (ICs sit in both).
  Indented rows are members of *some* starred aggregate; nbsp-indent depth
  varies (ICs and LCD modules are double-indented).
  **Ruling: never sum rows; store labels + indent verbatim (principle 3);
  starred rows are excluded from "movers" selection and usable only as
  context.** Same double-count family as the 000TOTAL mirror-gap bug.
- Quantity units vary per row ("10,000 Tons", "Ton", "10,000 Sets", "Ship",
  "100 Million PCS") — stored verbatim in `quantity_unit`, never normalised.
- Jan–Feb combined releases exist for 5/6 exactly as for 4 (narrower layout,
  cumulative-only) — handled in v1 as **parse-and-store** with
  `period_kind='cumulative_jan_feb'`; the analyser skips Jan/Feb anchors in
  v1 (same posture as the section-4 Jan/Feb carve-outs).

## What we can and cannot reproduce from press coverage

- **Can (GACC's own tables, full provenance):** motor vehicles (value +
  units), ICs, rare earths, ships, steel, refined petroleum, mobile phones,
  household appliances, aluminium, footwear… — the whole catalogue.
- **Cannot (not in the tables):** EV-specific splits, lithium batteries, wind
  turbines — those came from NBS press-briefing commentary, not the release.
  Out of scope here; belongs to the V1.1 spokesperson-commentary path in the
  gacc-update design note. The page must not imply we compute these.

## Design

### Ingest (parse.py, scrape.py)

- `_parse_section_5_6_commodities(soup, meta)` mirroring the section-4 parser:
  same container discovery, same cell-count row filter (10 regular /
  narrower Jan-Feb — count confirmed against fixture at build time), same
  nbsp-indent normalisation reused via `_normalise_partner_label` (renamed
  helper reuse — it strips/records nbsp indents; "of which:" handling is
  harmless here).
- Emits ParsedObservations with `commodity_label` (+ `partner_country=None`),
  `flow` = export (section 5) / import (section 6), `period_kind` ∈
  {'monthly','ytd'}, quantity + quantity_unit + value; `source_row` carries
  the full raw row incl. prior-year cumulative and published YoY% (kept as
  provenance, not recomputed-over).
- Prior-year "1-N Total, 2025" columns: **stored in source_row only**, not
  emitted as observations (they duplicate the prior year's own release;
  emitting them would create two provenance paths to one number).
- `parse_html` dispatch: sections 5 and 6 route to the new parser; everything
  else stays `UnparseableReleasePage`/`NotImplementedError` as today.
- Floor check `section56_floor_check`: ≥ N commodity rows (calibrated from
  fixtures across years at build time; catalogue is ~25–30 and stable) —
  same reject ⇒ `failed` ⇒ no release row ⇒ walk-retry contract as F5.
  No magnitude invariant (no Total row on these pages); the starred
  aggregates are NOT used as checksums (their membership is not adjacency,
  so no arithmetic identity is available — by design we don't reconstruct).
- `releases` natural key already covers this: (section_number, currency,
  period, release_kind). **No migration.** `observations` already has
  commodity_label/quantity/quantity_unit. **No migration.**

### Backfill (no re-fetch — principle 5)

Historical section-5/6 pages are already in `source_snapshots` (the walk
fetches + snapshots every discovered release; 5/6 currently die as terminal
`no_parser`, which `gacc_release_url_already_processed` skips forever). New
`scrape.py --gacc-replay-snapshots`: for each GACC URL whose latest run is
`no_parser`, re-run the parser over the **stored bytes** (newest snapshot per
URL); on success, write a fresh scrape_run (+ release + observations) marked
in scrape_runs as a replay. Idempotent: URLs that still raise
UnparseableReleasePage record `no_parser` again and stay terminal; replayed
successes are skipped next time by the already-processed guard. Zero requests
to GACC.

### Analyser (anomalies.py)

`detect_gacc_commodity_yoy(flow)` → subkinds `gacc_commodity_yoy` /
`gacc_commodity_yoy_import`, one finding per (commodity, anchor period),
supersede-on-change like the other GACC families:

- **Single-month YoY** = month value vs prior-year same-month value from the
  prior year's release (both sides from our own observations) — the FT-style
  quotable. Quantity YoY computed alongside when both sides have quantity in
  identical units ("cars +71% by value, +49% by units" class of line).
- **Cumulative-YTD YoY**: our ytd observation vs prior-year ytd observation,
  with **GACC's published YoY% (from source_row) carried in detail as a
  cross-check** — drawer shows ours next to GACC's; a >0.2pp divergence
  gates emission (fact-verification posture: never confidently wrong).
- Canonical **CNY** releases (matching `_gacc_aggregate_per_period_totals`),
  EUR-converted via fx for display, USD variant left as the dual-currency
  supersede family handles it today. Standing FX caveat applies (our YoY ≠
  GACC's published CNY/USD YoY) — reuse the existing caveat code.
- detail carries: commodity_label raw, indent, is_starred_aggregate,
  monthly + ytd windows, quantity windows + unit, provenance obs/release ids.
- Low-base guard: reuse the F2 reporter low-base machinery/thresholds.

### Page block (report_builder.py GACC-only page)

New section **between "Since the last read" and "Europe up close"** (Luke,
2026-07-14 — supersedes this note's original "under the world-context
strip" placement): **"What's moving — GACC's headline commodities"**. The
movers are part of the month's momentum and set up the Europe-specific
read that follows.

- Selection: leaf rows only (starred aggregates excluded), ranked by
  |single-month value YoY|, value floor (low-base guard), top ~5 movers +
  any large decliner (the rare-earths −34% class matters editorially);
  counts tuned at build.
- Each row: label · single-month value YoY (lead) · quantity YoY where
  available · EUR-equiv month value. Provenance drawer (Quotability-gated,
  provenance_payload idiom): source-page trail, our arithmetic, GACC's
  published cumulative YoY as the defensible published figure, FX caveat,
  world-not-EU scope note.
- **Scope labelling is load-bearing**: this block is China↔world on a page
  that also shows China↔EU — the section title + about-box must make the
  scope unmistakable (portal-UI-consistency: reuse brief-sec/about-box
  patterns, no new variants).
- The on-page "no commodity dimension" copy is corrected to "the by-country
  table has no commodity dimension; headline commodities are world-total
  only" — the two-lens explanation moves into the about box.
- "Takes" for the section (Luke, 2026-07-14) split by defensibility:
  - **Computed, never LLM-asserted (in PR C):** round-number milestone
    crossings on the quantity series ("first month above 1mn autos" — the
    June 2026 cars case fires the day the data lands) and linear run-rate
    pace lines ("on pace for ~10mn in 2026 vs 7.1mn in 2025"; month ≥ 3
    only, ±5% dead-band vs prior full-year). Both era-local (same catalogue
    label), both carrying method notes. The pipeline spots; the LLM only
    narrates — an LLM "spotting" a milestone is a hope, code is a
    guarantee.
  - **LLM commodity-take slot (PR D, +1 paid call/month, opt-in per the
    cost discipline):** the causal/contextual layer — rare-earths ↓ in the
    export-controls/tech-race frame, the car-export pivot against the
    domestic EV-subsidy-phaseout backdrop. Fed the movers + computed facts
    + hypothesis catalog; verify-numbers round-trip as everywhere. Pure
    external-context claims (domestic sales) get attributed-context
    framing — and are the strongest argument yet for the V1.1 curated
    policy-events file.

### Sequencing (time-sensitive)

1. **PR A (today): parser + floor + dispatch + replay backfill + tests.**
   After merge: run replay → history through May 2026 lands in observations.
   When the June English pages drop, the Routine ingests sections 4+5+6
   together with no further action.
2. **PR B: analyser + findings + tests** (needs A's data to eyeball).
3. **PR C: page block + provenance payload + copy correction.**
   B and C can compress into one PR if review is smooth; A ships alone so
   the Routine is already capturing June regardless of B/C timing.

### Tests

- Fixtures: real section-5 and section-6 pages (CNY + a USD control, plus a
  Jan-Feb combined page and one older year for format drift) pulled from
  existing DB snapshots — no new fetches. Convention: tests/fixtures/
  release_section5_major_exports_may2026_usd.html etc.
- Parser: row counts, a spot-checked row (cars: quantity+value, both
  period_kinds), `-` quantity → None, starred-aggregate + indent capture,
  Jan-Feb layout, floor trip on truncated fixture.
- Analyser: single-month + ytd YoY arithmetic vs hand-computed fixture
  values; published-YoY cross-check divergence gate; supersede behaviour.
- Page: selection excludes starred rows; scope copy present; drawer payload
  arithmetic reproduces.

**Decision taken (Luke, this session):** highlights lead with computed
single-month YoY; GACC's published cumulative YoY sits alongside in the
provenance drawer. Both are stored regardless.
