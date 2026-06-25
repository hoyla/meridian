# Roadmap — outstanding work

What's still open. For history of what shipped, see
[`history.md`](history.md). For the design rationale that drove
the original Phase 1–6 plan, look at the git log around
`8f18e68`–`5d0e23e` (2026-05-09 to 2026-05-10).

## Portal / data-integrity backlog — remaining small items

Nearly all of the 2026-06-22/23 deploy batch shipped and is live (see history.md:
the Eurostat data correction, Q2 sector expansion, group display-names,
CN+HK+MO scope-labelling, GACC regional charts, Period-covered "Last updated"
column, State-of-play→By-country link, section-naming, `--portal-reuse-takes`,
the `eurostat_raw_rows` natural-key UNIQUE backstop, and the source-freshness
alerting suite). What's left:

- **Minor / optional.** Fetch dates still render raw ISO (left deliberately — they
  are real calendar days, not the `-01` period artefact; could prettify to "15 Jun
  2026"). The month format is the abbreviated "Apr 2026" (full "April 2026" was an
  option).
- **"Top products" long-label tail.** The CN8 product-description labels (PR #87)
  read cleanly for ~90% of codes, but ~10% — mostly chemicals and vehicles, e.g.
  `29183000` "Carboxylic acids with aldehyde or ketone function but without other
  oxygen function" — run long enough to stretch the Top-products line. Optional:
  CSS-clamp the inline `label_short` (truncate with an ellipsis) and let the
  existing hover tooltip carry the full self-explanatory text. Cosmetic only; the
  full text is always reachable on hover.

## Analyser de-duplication / shared windowing primitive (E1) — flagged 2026-06-25

**Full plan:** [`2026-06-25-e1-analyser-dedup-plan.md`](2026-06-25-e1-analyser-dedup-plan.md)
(problem + drift table, the `WindowScope` design, the phased behaviour-locked
migration, risks, and the cheapest partial win). Summary below.

From the adversarial-correctness review
([`2026-06-25-adversarial-correctness-review.md`](2026-06-25-adversarial-correctness-review.md),
finding E1). `anomalies.py` (~240KB) re-implements per-family
windowing/aggregation SQL, so the *same* correctness rule has to be applied
per-site and drifts between copies: the `000TOTAL` exclusion, the GACC
`currency='CNY'` pin, and the EU-27 reporter scope each lived in some analyser
copies and not others. A1 / B2 / A3 were per-site fixes (all shipped — see
history.md); A3 went one better and made the `000TOTAL` backstop un-driftable by
baking it into the shared `_hs_pattern_or_clause` helper. The structural fix
extends that "state it once" move to the window sum itself: a single primitive
taking `(source, scope, partners, patterns)` that bakes in the `000TOTAL` rule,
the currency pin, and the EU-27 inclusion list — so each correctness rule is
stated exactly once. Not urgent (the point fixes hold); this is what makes the
whole bug-class impossible to reintroduce.

### A2 — EU-27 reporter scope: exclusion → inclusion (ready to implement within E1)

The cheap detect-and-alert guard shipped (PR #108: `eurostat.unexpected_reporters`
+ an ingest-time ERROR when a declarant outside the 27 + GB lands). The *correct*
fix is to scope the EU-27 by an **inclusion list** (the 27 member states) rather
than `reporter <> 'GB'` (everything-but-GB), which silently folds any stray
aggregate / territory / newly-acceded reporter into "EU-27". Deferred to E1
because the "exclude GB" rule is **duplicated across 5 surfaces** — a partial
swap would make them disagree:

1. `anomalies.py` — `EU27_EXCLUDE_REPORTERS` + ~8 inline `reporter <> ALL(%s)` SQL sites
2. `report_builder.py` — imports `EU27_EXCLUDE_REPORTERS` (one query)
3. `scrape.py` — passes it into the Eurostat world-aggregate ingest (`exclude_reporters=`)
4. `sheets_export.py` — hardcoded copy `_CHARTS_EU27_EXCLUDE_REPORTERS = ("GB",)`
5. `briefing_pack/docx.py` — hardcoded copy `_EU27_EXCLUDE_REPORTERS = ("GB",)`

Recipe: one canonical `EU27_REPORTER_CODES` (= `eurostat.EU27_PARTNER_CODES`, the
27 — `anomalies.py` adds `import eurostat`, no circular import); flip every
`reporter <> ALL(<exclude>)` → `reporter = ANY(<include>)`; retire the two
hardcoded copies; rename the `reporters_excluded` provenance fields (grep it) to
`reporters_included`. Behaviour-preserving on current data: the live reporter set
is exactly the 27 + GB (Greece as `GR`, not Eurostat's `EL`) — verified
2026-06-25 — so the swap moves no published number today and only differs if a
stray code appears, which the #108 guard now alerts on. Keep the full suite
green; the existing EU-27 tests seed the 27 reporters, so they exercise the swap.

## HMRC-triggered release — headline-only today; design parked (2026-06-23)

An HMRC-triggered briefing currently renders **the standout-movers headline and
nothing beneath it**: only the Eurostat branch in `report_builder`
(`source_trigger == "eurostat"`) builds a section tree, and GACC builds its own;
an HMRC trigger falls through with `sections = []`. Rarely hit in practice — HMRC
only triggers when there is *no fresher Eurostat month* (`_VARIANTS["hmrc"]`) —
and HMRC is **added value, not the tool's critical focus** (China↔Europe is). So
acceptable as-is and **not scheduled**. Captured here so the gap isn't mistaken
for an oversight.

If we ever want a real HMRC release, the open question is what its body holds —
the Eurostat sections don't all transfer:

- **The standing deficit** (the State-of-play / "Europe's deficit" section) —
  transfers directly: the UK-scoped standing deficit (HMRC) is already computed
  and shown as one scope under the Eurostat variant; an HMRC release would just
  foreground the UK line.
- **Sector detail** — transfers: HMRC carries the same HS-group YoY breakdown
  (the UK column already exists in the combined view).
- **Mirror-trade gaps** — does **not** transfer as-is: the gap is specifically
  China↔EU (GACC exports vs Eurostat imports). A UK analogue means China↔UK
  (GACC vs HMRC) — a new analyser, not a reuse.
- **China's trade by partner (GACC)** — transfers unchanged (source-independent
  context).

Smallest viable version: reuse the Eurostat section-builder with the UK scope
foregrounded and the mirror-gap section dropped (or swapped for a China↔UK
variant once that analyser exists).

## Slim GACC-period update — its own self-expiring tab; design parked (2026-06-23)

GACC leads Eurostat by ~5 weeks (China Customs publishes ~8–10 days after the
reference month; Eurostat ~46 days), so a fresh GACC release is an early read on
the China↔EU numbers. Reporters asked whether we should auto-build a briefing for
each new GACC release as well as each Eurostat one. We costed the repetition: a
*full* briefing on a GACC release would be ~60–80% stale — the Eurostat-derived
sections (HS-group movers, trajectories, the standing deficit, partner-share)
don't change when only GACC arrives, so they'd repeat the prior Eurostat cycle
verbatim. Only the GACC-fed sections move.

Decision — a **slim, GACC-only update**, not a briefing clone:

- **Content**: render only the fresh GACC-fed sections — `gacc_bilateral_aggregate_yoy`
  (China↔EU + per-country) and `gacc_aggregate_yoy` (ASEAN / RCEP / Belt & Road /
  Africa / LatAm / world). Drop the Eurostat HS-group / trajectory / trade-balance
  sections — they'd be stale.
- **Trigger on a new GACC *period*, not a new release.** GACC publishes dual-currency
  (CNY + USD) for the same period, so release-triggering fires twice a month;
  period-triggering is ~once.
- **Exclude the mirror gap.** A GACC period lands ~5 weeks before the matching
  Eurostat month, so the Eurostat side of the gap for that month doesn't exist yet —
  it can't be computed. (It refreshes normally on the next Eurostat cycle.)
- **Surface as its own distinct tab**, non-destructive to the main Briefing tab, that
  **self-expires on the next Eurostat release** (the Eurostat cycle supersedes it).
  Keeps the leading-indicator read visibly separate from the confirmed EU read, and
  self-cleans.

Cost note: each brief's LLM takes are paid calls, so the slim shape (few sections)
plus `--portal-reuse-takes` for anything carried is the cost-sensible build, not a
second full pack.

Not scheduled — a thinking-question outcome from the same 2026-06-23 thread as the
HMRC release-timing work (which is what got built: the combined-scope guard, the
HMRC-lag disclosure, the overdue alert, the gap-scan). Pick up when reporters want
the earlier cadence.

## docx → Drive pipeline — legacy; teardown deferred (2026-06-22)

**Decision (Luke, 2026-06-22).** The web portal is the live Lisa-facing
surface; the `.docx` → Google Drive delivery pipeline predates it and is
unlikely to be used again. **The markdown bundle (`02_Findings.md` etc.) is
still wanted**, so this is *not* a teardown of the findings export — only of the
docx/Drive half.

**Done:** `periodic.run_periodic(docx=False)` by default — the daily cycle no
longer renders the per-cycle `.docx` (it still writes the `.md` + `.xlsx` +
portal snapshot). Drive upload was already manual, so nothing auto-pushes to
Google. Reversible (pass `docx=True` or `--briefing-pack --docx`).

**Deferred until confirmed dead** (don't rip out reactively — dormant code is
cheap; "deleted then needed it" is not): remove `briefing_pack/docx.py`,
`briefing_pack/md_to_docx.py`, `drive_export`, the `python-docx` dependency,
`test_briefing_pack_docx.py`, and — notably — the anchor-link contortions in
`briefing_pack/sections/front_page.py` that exist *only* so Google Docs' .docx
importer can reconnect dropped `#slug` links. The `.md` bundle stays: it is
**solely the LLM / NotebookLM ingestion corpus** (Luke, 2026-06-22; already
documented in `architecture.md`, `methodology.md`, `briefing_pack/__init__.py`,
`README.md`), so the Tier 1/2/3 scaffolding earns its keep — the teardown
removes only the docx/Drive half and leaves the markdown and its structure
intact.

## Breadth expansion — ingest more now that the report is navigable (2026-06-21)

**Premise.** The portal restructuring (tabs; "More about" + per-group +
per-partner progressive disclosure; charts; Tables / Sources & coverage /
Methodology / Glossary) changed the cost/benefit of breadth. Extra coverage used
to *overwhelm* the two long docs; the portal now lets a reader dig into what they
want and ignore the rest. So the constraint that kept coverage tight has largely
lifted — time to **ingest more broadly and surface it on demand** (global
principle 1: ingest broadly, analyse second). Luke's call, 2026-06-21.

Discipline still applies: **look at the data before building infra** (principle
6 — hand-pull samples from each new source/code before writing adapters);
append-only + provenance on everything new (principles 3/4/7).

### Concrete pieces (roughly priority order)

1. **All-goods extra-EU world totals → donut + dependency-over-time — DONE 2026-06-24.**
   Shipped (history.md): `000TOTAL` is now aggregated into `eurostat_world_aggregates`
   (extra-EU all-goods denominator), backfilled 2017→2026; the
   `detect_china_all_goods_share` analyser emits China's (CN+HK+MO, + CN-only)
   share of EU-27 extra-EU all-goods trade per anchor with a `share_series`; and
   the portal renders the long-deferred **donut KPI** + the **dependency trend
   line**. Live: 22.5% imports (12mo to 2026-04), 18.3%→23.1%(2021)→22.5% trend.
   **Trend starts 2019-01** — the stored numerator is pre-v2-contaminated for
   2017–2018, so extending the trend back waits on a clean re-ingest of those
   months (the "2017 pre-v2 … duplicate rows" item below). Closes the standalone
   "donut data source" open question.

2. **More HS groups.** The curated `hs_groups` set (~46) is an editorial
   selection; the portal's filter + per-group disclosure now make a much larger
   set navigable. Use the **Trade map's "dark tail"** (SITC divisions with low
   editorial-group coverage) to prioritise — it's already a map of what we're
   missing. Candidates: deeper pharma precursors, semiconductor
   equipment/materials, more critical-minerals lines, agri commodities,
   textiles/apparel, furniture/toys, etc. (Ties to *Sector breadth review
   (round 2)* below.)

   **Let the editorial themes pull group selection, not just the dark tail**
   (Luke, 2026-06-22). Themes (`labels.py`) are cheap to declare but only worth
   showing once they have enough member groups; several are **thin or absent**
   for want of coverage, so when adding groups, deliberately pick ones that make
   a theme viable:
   - **Plastics & petrochemicals** — only Plastic waste + Adipic acid today
     (left undeclared as too thin). Add primary plastics (PET/PE/PVC, ch39) and
     petrochemical feedstocks (ethylene/propylene/styrene, ch29) to earn it.
   - **Textiles & apparel** — only Cotton today (and only as a Xinjiang lens).
     Add apparel, man-made fibres/fabrics, footwear.
   - **Semiconductors & electronics** — beyond the broad ch84-85 group and the
     SME/telecoms ones: chips, components, displays.
   - **Aerospace** — only Civil aircraft + Drones; add parts/engines if a story
     warrants.
   - **Base metals / trade-defence** — beyond Steel + Aluminium + fasteners
     (copper, other base-metal products) — the AD/CVD-case cluster.
   - **Consumer goods** — furniture, toys, appliances (none yet).

   When such groups land, declaring the matching theme in `labels.py` is a
   one-liner and lights up chips on the movers + sector filter automatically.

   **Lisa's Jun-2026 sector questions → material groups + new themes**
   (see [`2026-06-22-lisa-sector-coverage-questions.md`](2026-06-22-lisa-sector-coverage-questions.md)).
   The chemicals / refined-critical-minerals expansion is scoped there as
   material-named groups, each tagged to themes — including two *new* themes
   (**Cosmetics & personal care**, **Paint & coatings**) and rows that fill the
   already-declared **Pharma & fine chemicals** theme. Titanium dioxide
   (`320611`) is the worked multi-theme case (paint + cosmetics + pigment).
   **Seeded + analysed 2026-06-22** (round 1 — critical minerals + pharma APIs +
   engine parts: #59; round 2 — cosmetics + paint + the two new themes: #62; see
   history.md). This sub-item is done; the broader breadth menu below remains.

   **Retrofit the 3 legacy application-bound groups — DONE 2026-06-22**
   (branch `ljh-legacy-group-taxonomy-retrofit`). Added a journalist-editable
   `hs_groups.display_name` column + `db.group_display_names` resolver, plumbed
   through every reader-facing surface (portal, briefing sections, sheets,
   glossary) with heading/slug/link consistency. `EV batteries (Li-ion)` now
   displays as `Lithium-ion accumulators (HS 850760)` — the `name` key, and all
   findings/tests keyed off it, untouched (so no orphan backfill).
   `Wind turbine components` retired (its `850300`/`730820` patterns weren't
   wind-specific); `Wind generating sets only` survives and a new overlapping
   `Wind power` theme gathers the wind-relevant groups. `Solar/grid inverters
   (broad)` left as-is by decision (already hedged; has PV-specific siblings).
   The display-name column is now available for the **group display-names**
   item below (China-mentioning names that read awkwardly in front-page
   sentences).

3. **Richer per-source statistics.**
   - **Eurostat:** unit-price (€/kg) trends as a first-class series (we hold
     value + kg; price = the divergence signal); supplementary units; finer
     partner cuts.
   - **GACC:** more partner countries (beyond the ~24); **commodity-level** GACC
     if obtainable — currently bloc/partner aggregates only, so a China-side HS
     mirror is a big unlock (ties to *Eurostat-side HS-level mirror* below);
     surface the CNY-vs-USD divergence.
   - **HMRC:** finer UK granularity (regions, transport mode) if a UK story
     warrants it.

4. **Derived / cross-source metrics** (journalistically rich; mostly compute
   over existing + new data):
   - China's **share of EU imports per sector over time** (dependency by
     product) — generalises per-group `partner_share` to a headline series.
   - China's **global** export share by product (is the EU's dependence matched
     worldwide?) — needs a world-trade source (UN Comtrade).
   - **Price divergence** (China export vs EU import — the CIF/FOB wedge +
     markup) per group.
   - Broader **mirror-gap** coverage (more partners, more commodities).

5. **New context sources (stretch).** UN Comtrade (China's global trade by
   commodity, for "China-specific vs worldwide"); tariff-change timelines (to
   correlate moves with policy — also feeds the LLM-takes v2 retrieval angle).

## "Biggest mover" KPI — surface what we're *not* watching (A→B); design 2026-06-24

**Motivation.** A fifth KPI card that highlights an out-of-the-ordinary sector
move. The real goal (Luke, 2026-06-24) is to surface something **we're not
already looking for** — to fight fixation on the curated watchlist. With several
reporters now on differing beats (not just Lisa), you can no longer hand-tune the
~46 `hs_groups` to one person's interests; a fixed watchlist quietly encodes last
cycle's priorities. So this card's real job is less "content feature" and more
**instrument for discovering demand**: throw an unexpected product in front of
several people, and whoever's beat it touches says "watch that" — and *that*
signal drives where we spend the (real) ingest budget for breadth expansion. It
is the editorial-instinct check of global principle 1 made into a UI surface
(see **Breadth expansion** above — Option B is an instance of it).

**The asterisk that shapes the design.** We do *not* ingest the whole COMEXT
product universe. The bulk file is filtered at ingest to a prefix set derived
from the watchlist itself (`scrape._world_aggregate_hs_prefixes_from_hs_groups`
→ `eurostat.iter_raw_rows(hs_prefixes=…)`), plus `000TOTAL`. So:
- *Within* watched HS chapters/headings we hold **every** CN8 sub-code (raw rows
  persist in `eurostat_raw_rows`) — finer than the 60-ish displayed groups.
- *Outside* the watched prefixes the data isn't in our DB at all — filtered out
  at ingest. A chapter we don't watch is a blind spot we literally cannot see.

So "biggest mover from everything" splits into two builds:

- **Option A — biggest single-product mover *within* watched sectors (cheap; data
  already here).** New CN8-grain YoY analyser over `eurostat_raw_rows`, deduped
  against what's surfaced at group level, strict floors applied. Surfaces a
  specific code a flat-looking group is masking — and breaks the `top_movers[0]`
  duplication (a naive "biggest mover" card would just restate Standout moves'
  #1). Honest framing: "biggest single-product move *within what we monitor*",
  **not** a blind-spot detector. **This is the stepping stone — build first to
  provoke reporter feedback.**
- **Option B — true blind-spot radar (needs wider ingest).** Periodically (not
  necessarily every cycle) ingest a much broader prefix set, compute YoY at
  HS-heading/chapter grain across *everything*, exclude the watched chapters,
  surface the biggest mover in the remainder. The principle-1-pure version and
  what the motivation actually asks for — but a real piece of work: bigger/slower
  ingest (we filter deliberately for size/speed), a new aggregation grain, and
  serious noise control. **Gated on whether reporter feedback to Option A shows
  the blind-spot mandate is worth the ingest cost.**

**Traps (both options; worse for B).**
- *Low-base explosion.* Across thousands of CN8 codes the biggest YoY mover is
  almost always a tiny/noisy/newly-reported code with a spurious +5000%. The EUR
  value floor + `low_base` gate become load-bearing, not nice-to-have (ties to
  the still-open low-base threshold calibration).
- *CN8 churn.* The Combined Nomenclature is revised annually; split/merge/new
  codes manufacture phantom ∞% moves — needs a code-continuity guard (require N
  prior comparable periods).
- *No curated context = dead-end risk.* An off-watchlist code has a CN8
  description (PR #87) but no theme/drill-down section, so the card must be
  **self-contained** (number + CN8 label + provenance drawer + "outside the
  watchlist — worth a look" framing), not a link into detail that doesn't exist.
- *Granularity.* CN8 is noisy; scan blind spots (Option B) at HS heading/chapter,
  where a move is material and robust, not at 8-digit.
- *"Biggest", not "unusual".* There is no per-sector z-score yet (only
  `mirror_gap_zscore`). Ranking is magnitude-on-a-real-base; don't label a
  magnitude leader "unusual" until a YoY z-score analyser exists.

**Looser noise contract than a normal KPI.** Because this card's job is
provocation, not authority, a slightly weird single-product mover that makes a
reporter go "huh, why?" is doing its job even if it isn't headline-grade — the
opposite of the deficit card, where a glance-wrong number is a failure. Be
deliberate that this card is allowed to be exploratory.

**Feedback loop to design in.** Make the path *card surfaces code Y → reporter
flags interest → editorial promotes it into an `hs_group`* low-friction (even an
informal Chat nudge via the existing `--notify-chat` hook at first). The
journalist-editable `hs_groups` already make the "promote it" step cheap.

**Layout (do regardless).** The KPI strip is a 3-column grid; `sparkline` cards
get `kpi-wide` (span 2). Today EU-deficit (2) + UK-deficit (2) + import-level (1)
+ donut (1) = two rows of three, but the UK card is wide only *by accident* of
being a sparkline, not by importance. Render UK deficit as a 1-col `bignumber`
and a slot frees up: EU-deficit (2) + import-level (1); donut (1) + UK-deficit (1)
+ **new card (1)**. EU-deficit becomes the sole wide card — correct, it's the one
number that earns the emphasis.

## Reference-data freshness — keeping externally-sourced lookups current (2026-06-24)

The portal leans on several committed reference lookups derived from external
authorities, and they go stale **silently**: the source-freshness alert suite
watches incoming *trade data* (Eurostat / HMRC / GACC releases), but nothing
watches the *reference* vintages. Inventory and cadence:

- **CN8 product descriptions** (`reference/cn8_descriptions.csv`, built from
  `reference/cn_descriptions/cn8_2025_en.xlsx` — the "Top products" labels +
  hover text). The EU Combined Nomenclature is **reissued every 1 January**
  (codes added / split / withdrawn); `cn_year` records the edition. Refresh:
  drop the new year's KSH self-explanatory xlsx in and rerun
  `classifications.build_cn8_descriptions()`. Currently CN 2025. See
  [`reference/cn8_descriptions.PROVENANCE.md`](../reference/cn8_descriptions.PROVENANCE.md).
- **CN8 → SITC / BEC crosswalks** (`cn8_sitc.csv`, `cn8_bec.csv`) — keyed on
  **HS editions** (HS2017 / HS2022), which revise ~every 5 years. Next is
  **HS2027**: when it ships, add the HS2027→SITC/BEC correspondence ahead of
  HS2022 in the fallback chain (see `classifications.py` and the *CN8
  concordance table* item below).
- **CIF/FOB baselines** (`cif_fob_baselines`, OECD ITIC) — refresh in a future
  year (see [`2026-05-10-cif-fob-baselines.md`](2026-05-10-cif-fob-baselines.md)).

Two cheap, additive guards worth building (neither scheduled):

1. **Build-time staleness warning.** When `cn_year` (or the active HS edition)
   trails the current calendar year by more than the known cadence, emit a
   `log.warning` on the build path — the same fail-visible instinct as
   `assert_classifications_available`, but a soft nudge: descriptions are reader
   enrichment, not publication-critical, so warn, don't refuse.
2. **Annual reference-refresh reminder** as a once-a-year Routine/cron line,
   separate from the data-release cadence, so the 1-Jan CN reissue and the
   HS-edition changes don't depend on someone remembering.

Defensibility holds across refreshes by construction: derived descriptions are
stored *alongside* the source codes (the codes stay canonical, never
overwritten — global principle 3), and the cross-validation against the EU
primary SKOS/RDF is re-runnable on each refresh via `CN_RDF_PATH`. Ties to the
**CN8 concordance table** item below — a real old→new concordance would let a
refreshed description set remap superseded codes rather than silently dropping
them.

## Observability / logging follow-ups (2026-05-15 evening arc)

Four new audit-log surfaces shipped tonight along with
[`dev_notes/2026-05-15-logging-policy.md`](2026-05-15-logging-policy.md):
`routine_check_log` (per-source Routine telemetry + lifecycle
bookends), `llm_rejection_log`, `periodic_run_log`,
`findings_emit_log`. CLIs: `--source-status`, `--llm-rejections`,
`--periodic-history`, `--emit-history`. What's still open:

### Ad-hoc CLI coverage for `findings_emit_log`

Today's integration is in `periodic.run_periodic`'s analyser
dispatch — so `--periodic-run` cycles write rows, but ad-hoc
`python scrape.py --analyse hs-group-yoy` from the CLI does not.
Closing this means instrumenting each `detect_X()` directly (~9
functions in `anomalies.py` + `llm_framing.detect_llm_framings`).
The cleanest pattern is a context manager that wraps the body of
each function: open it after `analysis_run_id` is created, capture
the returned counts at exit, write the row in `__exit__`. ~half a
day; deferred until ad-hoc runs become a frequent debugging case.

### Supersede-reason classification

`findings_emit_log` records aggregate counts (`new` / `confirmed`
/ `superseded`) per analyser invocation, but doesn't distinguish
*why* a row was superseded — data change vs method-version bump
vs caveat-list change. Today's only way to tell is to inspect the
old and new rows' `detail.method` manually.

Implementation sketch: add `supersede_reason TEXT` and
`prior_value_fields JSONB` to `findings`, populate them in
`findings_io.emit_finding` on the supersede branch by comparing
the new `value_fields` against the prior row's. Reasons:
`method_bump` (only `method` differs), `value_change` (numeric
fields differ), `caveat_change` (caveat list differs), `mixed`.

Editorial payoff: the brief's Tier 1 method-bump-churn
auto-suppression in [briefing_pack/sections/diff.py](../briefing_pack/sections/diff.py)
could use a structured signal instead of inferring from
value-identity. The first-export audit on 2026-05-12 surfaced this
as the kind of inference that ought to be explicit.

### Other silent-decision surfaces flagged but deferred

Lower priority — pick up when one of them breaks visibly:

- **Currency-unit guard rejections.** `db._assert_currency_unit_consistent`
  raises on bad pairs. Adding a log table would capture which
  release pages tripped it.
- **Parser anomalies.** Title-format mismatches, unexpected column
  counts, etc. Most raise today; some `log.warning`. Per-anomaly
  table if frequency rises.

## Journalist-usability arc — paused for feedback (2026-06-12)

Iterations 0–2 shipped 2026-06-11 (PRs #5, #6, #7/#8, #9 — see
[`history.md`](history.md)): the plain-language pass, quotability
verdicts + integrity riders, the "If you read only this page" front
page, and the file renumbering that puts Findings (02) ahead of
Leads (03) so folder order matches reading order.

**Deliberate pause.** The next step is observational, not build: wait
for the next substantive source release, deliver the resulting
briefing pack, then run a five-minute **quote audit** — what did the
journalist actually quote or chase, and did the Quotability verdicts
agree? Disagreements in either direction (quoted a 🔴 figure; ignored
everything 🟢) are the calibration data for everything below.

Then, in order:

### Iteration 3 — self-verifying portal (MVP DONE 2026-06-24)

Rescoped from "self-verifying *bundle*" to "self-verifying *portal*" (the live
surface; the Drive-appendix half is dropped) and the **MVP shipped** — see
history.md. A `finding/N` token on the portal was a dead end for a journalist
without DB access; now the Quotability-gated numbers (KPI standing levels +
headline movers) carry a no-JS `<details>` drawer baked into the snapshot:
source-URL trail (links to every release), arithmetic, caveats, collapsed
replay-SQL. `provenance_payload.py` + `report_model.provenance_payloads`.

Remaining (follow-ups, not blocking):

- **Extend the gated set** to the long tail — sector-detail rows, mirror-gap,
  partner-share, GACC bilateral — once the MVP's been used. Each is just another
  finding id added to the gated collection in `report_builder`; the generic
  source trail already covers them (mirror_gap/partner_share are observation- or
  window-based). Watch snapshot size as the set grows.
- **Richer per-subkind arithmetic.** The drawer arithmetic is best-effort per
  family (trade_balance / china_share / hs_group_yoy) with a graceful fallback
  to "sources + caveats only". `mirror_gap*`, `partner_share*`,
  `gacc_aggregate_yoy*` would benefit from their own arithmetic lines (absorbs
  the old § "Provenance renderers for remaining subkinds" item — but as
  structured payload, not markdown renderers).
- **Markdown bundle** still carries the on-demand `provenance/finding-N.md` docs
  (provenance.py) as the LLM-ingestion corpus; unchanged by this.
- **Eurostat human-friendly companion link — narrative shipped; interactive
  parked.** The drawer's source links point at the raw COMEXT `.7z` bulk files
  (the true source, but not human-friendly). Shipped a per-drawer **narrative**
  link to Eurostat's "EU–China trade in goods" Statistics Explained page
  (auto-current, zero-maintenance). Revisit adding an **interactive** Data
  Browser link. Findings from a live browser+API investigation (2026-06-24):
  - There is **no constructible deep-link** to a filtered Data Browser view —
    selections are client-side; the address bar stays dataset-level, and
    "Share/bookmark" only copies the dataset URL.
  - The reusable `…/view/ds-059331__custom_NNNNNN/…` URLs are **saved server-side
    objects** (resolved via `api/custom-extraction/.../custom_NNNN`, which
    returns the full selection as JSON: reporter/partner/product/flow/time). They
    are reusable but **not constructible from values**; created via the
    interactive "Customise + save" flow (login / "My datasets"); and they **pin
    the time selection** (drift stale). Mass-creating one per finding via the
    internal API would be fragile + abusive.
  - Partner `CN`/`HK` **are** valid in `ds-059331` (the dataset is NOT
    aggregate-partner-only — an earlier wrong read; the search box just wasn't
    matching). The product axis there is SITC; the all-goods row is
    `product=TOTAL`.
  - **Realistic option:** ONE (or a few) hand-curated fixed custom-view links
    (e.g. EU↔China all-goods, both flows, monthly — basically the shape of
    `__custom_21936152`) as an editorial "browse interactively" link, accepting
    the time-staleness. Not per-finding. The JSON API can deep-link the exact
    slice but returns JSON-stat — no better than the `.7z` for a human.

### Iteration 4 — low-base threshold calibration

The shock-replay calibration of `low_base_threshold_eur` — the
§ "Editorial calibration of low_base_threshold_eur" item below,
promoted: the sensitivity sweep showed 49% of `hs_group_yoy*`
findings flip classification across €5M–€500M, and the threshold now
drives the rendered Quotability verdicts, so calibrating it has
direct editorial effect. A recalibration propagates as a method bump
through the supersede chain — cheap to apply once decided.

### Iteration 5 — change-based delivery (Layer 3)

When the delivery vector gets decided (see § "Watch the first 2-3
real cycles + decide delivery vector"), make the unit of delivery the
story-worthy change, not the export — an alert like "EV battery
imports crossed a threshold this morning" linking into the front
page. The verdict layer is the trigger filter. Don't pre-build;
design the alert granularity when the vector is picked.

### Smaller follow-ons surfaced by the arc

- **Group display names — infra shipped (#53); no live target, parked against
  the Q2 chemicals expansion.** The `hs_groups.display_name` column + resolver
  exist (EV batteries → "Lithium-ion accumulators"). The original driver —
  names that mention China reading awkwardly in front-page sentences ("EU-27
  imports of Critical minerals (export-controlled by China) from China") — has
  **no current group to fix**: no live `hs_groups.name` mentions China (that
  example is an aspirational `labels.py` member, not a real group). It folds
  into the **Q2 chemicals expansion**, which would actually create such a group
  — at which point setting its `display_name` is a one-line add where the group
  is defined, not a standalone task.
- **Repo restructure for public readability.** ~21 root-level modules
  → a `meridian/` package with a root `scrape.py` shim preserving the
  Routine's pre-approved commands. Proposal + compatibility notes in
  [`2026-06-12-repo-structure-proposal.md`](2026-06-12-repo-structure-proposal.md).
  Do after the watched cycle, ideally after `--upload-to-drive` is
  CLI-wired (removes the `python -m briefing_pack.drive_export`
  invocation the move would break).
- **Mirror-gap "Period: None" rows.** Some mirror-gap blocks render
  `Period: **None**` — the period lookup via `observation_ids[1]`
  returns NULL for some findings. Diagnose and fix; also the
  "1 releases" grammar in the same block.

## Near-term (likely next session)

### Docx + Drive upload — v1 and v4 shipped 2026-05-16

Lisa-feedback arc on docx + charts is substantially complete:

- **v1** — `--docx` CLI flag + chart-bearing top-N renderer +
  xlsx `Charts` tab. Commits `f2b5c1c` → `0b0d88b`.
- **v4** — full markdown-content parity (the docx now contains
  the same sections as `03_Findings.md` plus charts at top-N
  movers via a mistune-based md→docx translator). Commits
  `713a337` → `f181419`. Design addendum captured in
  [`2026-05-16-docx-production-module-design.md`](2026-05-16-docx-production-module-design.md).

What's still open from this arc:

- **v2 chart recipes** (partial — see design doc § "v2 addendum").
  Per-reporter bar (Option 1) and bilateral summary bar (Option 2)
  shipped 2026-05-16 to give Lisa varied surfaces to react to.
  Three families still deferred: `mirror_gap*`,
  `hs_group_trajectory*`, `partner_share*`. Demand-driven from
  here — wait for Lisa's reaction before committing to more.
- **v3 Drive upload.** GCP access restored 2026-05-21; OAuth
  (`drive.file`) + Drive API + Docs API enabled in the
  `investigations-tools` project. Desktop-app client at
  `~/.config/meridian/client_secret.json`, token at
  `~/.config/meridian/google-token.json` (both outside the repo, chmod 600).
  Spike spec at
  [`2026-05-16-docx-drive-spike.md`](2026-05-16-docx-drive-spike.md)
  — legs 1 and 2 verified manually; **leg 3 (OAuth) + the
  heading-anchor approach verified 2026-05-21** via
  `scripts/drive_heading_anchor_test.py` (see that note's
  "Heading-anchor result" section). Decided flow: generate the `.docx`
  as now → Drive upload-with-conversion → batched Docs-API
  "style-flip" pass (`HEADING_n` → `NORMAL_TEXT` → back) to mint the
  `#heading=` nav anchors that `.docx` import omits; charts ride along
  in the conversion for free.

  **Built 2026-05-21** — `briefing_pack/drive_export.py`. Uploads the
  full bundle: all four `.docx` → native Google Docs and `04_Data.xlsx`
  → a Sheet, running the batched style-flip anchor pass on each Doc, plus
  `fix_internal_heading_links` (repoints in-document links — e.g. the
  Groups "Quick index" — at the real headings via `headingId`). Raw
  `.md`/`.xlsx` copies go to a "Markdown versions for use with LLMs etc"
  subfolder. Idempotent (match-by-name, update in place); parent folder
  via `MERIDIAN_DRIVE_PARENT_ID` (works with a user-created folder by ID
  under `drive.file`). House styling (heading sizes, tinted metadata
  sections) shared across all Docs.

  Remaining:
  - **Cross-*document* links.** Sibling references (e.g. "see
    03_Findings" in the Groups intro) don't link across Docs — Google
    drops the relative `.md` hrefs on import, leaving plain text. To make
    them clickable in Drive, map each sibling artefact to its uploaded
    Doc ID and set the link target as a post-upload step (the export
    already returns every Doc ID, so the mapping is in hand).
  - **`--upload-to-drive` CLI wiring** into the export / periodic path
    (currently invoked via `python -m briefing_pack.drive_export`).
    OAuth-token durability: the consent screen is **Internal** (Guardian
    Workspace), so the refresh token does **not** hit the 7-day
    External/Testing expiry — it lasts until revoked / ~6-months-unused, so
    a weekly cron is safe and re-prompts no one. BUT the unattended path
    must **fail loud, not hang**: give `get_credentials()` a
    non-interactive mode that *raises* on an unusable token instead of
    falling through to `run_local_server` (which would block waiting on a
    browser). On that failure: record it in `periodic_run_log` (error
    field) and notify (see below); recovery is a one-off
    `python -m briefing_pack.drive_export …` by hand to refresh the token.
  - **Per-run outcome notification (Luke wants this).** Have the scheduled
    Routine report the outcome *every* run, not just on failure: whether
    new source data was found and **from where** (GACC / Eurostat / HMRC),
    whether a new briefing was generated (and its data period), whether the
    Drive upload succeeded, and any error. Most of this is already in
    `PeriodicRunResult` (`action_taken`, `reason`, `data_period`,
    `findings_path`, `analyser_counts`) plus `_new_releases_since` /
    `_why_this_export_paragraph`; the Drive-upload result depends on the
    `--upload-to-drive` wiring above, so build the notification together
    with that. A dead-token alert is then just one possible outcome line.
    Delivery, simplest first: (a) the Routine agent summarises + push-
    notifies at the end of each run; (b) a macOS `osascript` desktop
    notification with a one-line summary; (c) the Slack/email digest
    channel once built. (a)+(b) work today without the deferred delivery
    channel; the unattended path must still fail-loud (non-interactive
    `get_credentials` that raises rather than blocking on a browser).

  Sharing needs no work: export folders inherit permissions from the
  `MERIDIAN_DRIVE_PARENT_ID` parent, which is already shared with Lisa and
  colleagues.
- **Promote `--docx` from opt-in to default-on.** Defer until
  Lisa has eyeballed 2-3 real cycles' worth of output.

### Test coverage catch-up (flagged 2026-05-21)

The Drive-export + bundle-restructure arc moved fast and the tests didn't
keep pace — coverage is leaning on manual round-trips and live-Drive
checks rather than the suite. Honest-accounting items, roughly in priority
order:

- **`drive_export.py` has no unit tests.** Its deterministic logic is
  currently only exercised against live Drive. Add mocked-service tests
  (stub the Drive/Docs `build()` services) for: folder find-or-create and
  `_upsert` idempotency (update-in-place vs create); the heading-anchor
  flip request shape (`mint_heading_anchors` builds the two NORMAL_TEXT→
  HEADING_n batches); `fix_internal_heading_links` matching (link text →
  `headingId`, including the `" ("` draft-suffix prefix fallback); and the
  `export_bundle_to_drive` orchestration walk (which files convert vs go
  raw). `get_credentials(interactive=False)` raising `TokenUnusableError`
  is already covered by a quick check but deserves a real test.
- **No `docx=True` bundle-structure test.** The mirror-Drive layout
  (top-level `.docx`/`.xlsx` + the `Markdown versions…` subfolder, xlsx
  duplicated) was verified by hand only. The DB-gated `test_briefing_pack`
  cases exercise the `docx=False` flat layout; add a `docx=True` case
  asserting the subfolder structure + `_bundle_root` round-trip.
- **Periodic run summary.** `PeriodicRunResult.summary()` is unit-tested
  ad hoc; fold those into the suite, and add a DB-backed test that
  `new_data` is populated from real `releases` rows (and is `None`, not
  `""`, on a query failure).
- **DB-gated tests skip silently.** They need `GACC_TEST_DATABASE_URL`
  (→ `gacc_test`); without it the suite reports a pile of skips that reads
  like "no DB tests" (cost an hour today). Consider a CI step / a Make
  target / a conftest warning so a bare `pytest` makes the skip obvious,
  and so these run in CI rather than only when remembered locally.

### Watch the first 2-3 real cycles + decide delivery vector

Periodic-run **pipeline + Routine** shipped 2026-05-11 (Phase 6.9 /
6.10 — see `history.md` and
[`2026-05-11-periodic-runs-design.md`](2026-05-11-periodic-runs-design.md)).
Routine fires daily at 09:01 local time. What remains is observation
and Layer-3 design:

- **Click "Run now" once from the Scheduled sidebar** to pre-approve
  the tools the Routine uses (`psql`, `python scrape.py ...`).
  Otherwise the first real scheduled run will pause on permission
  prompts.
- **Watch the first 2–3 real cycles land** (whenever the next
  Eurostat release publishes — typically 6-8 weeks after period
  close). Tier 1 currently shows same-day method-bump churn
  (everything created today); after the first real Eurostat-release
  cycle, it'll show the actual data diff. Validate that the diff
  reads usefully editorially.
- **Decide on delivery vector** (Layer 3) once we've seen what a
  real cycle looks like in Lisa's hands. Don't pre-pick
  email / Slack / Drive — pick after the first usable export
  has been delivered manually a few times.
- **Migrate Luke's environment** from laptop to desktop. Steps in
  the design doc § "Migration: laptop → desktop". Routines are
  account-bound; the pipeline is portable via `git clone` +
  `pg_dump | pg_restore`.

## Coverage extension (surfaced by the 2026-05-11 Soapbox validation pass)

Items the Soapbox validation surfaced as real gaps but not on
the periodic-runs critical path. Each is small-to-medium and
self-contained. See
[`2026-05-11-soapbox-validation.md`](2026-05-11-soapbox-validation.md)
for the per-claim test that motivates each.

### Eurostat-side HS-level mirror for "China's exports to EU"

Soapbox routinely quotes GACC-side HS-level figures ("China's
EV+hybrid exports to EU +87% in Q1 2026 at $20.6B"). GACC
sections 5/6 in our DB have only ~30 hand-curated commodity
names (no HS codes), so the GACC-side HS-level test is blocked
on parser work for sections 5 and 6 specifically. The cleaner
path is to rely on Eurostat for HS-level and accept the CIF/FOB
caveat — but the editorial register ("China reported $20.6B...")
isn't substitutable.

### 2017 pre-v2 COMEXT format duplicate `000TOTAL` rows

Surfaced by the §5.4 snapshot refresh on 2026-05-11. The pre-v2
bulk-file format produces duplicate `000TOTAL` rows per
(reporter, period, partner, flow, stat_procedure) — 2017 sp=1
has 648 rows vs 2018 sp=1 has 351. Analyser output is unaffected
(HS LIKE filters skip aggregate rows) but any 2017 raw-row
aggregate rollup is 2x inflated. Forward work to dedupe or
re-ingest 2017 with the v2 parser. Independent of the 000TOTAL
filter rule resolution from 2026-05-10.

Now broader than just `000TOTAL`: the 2026-06-24 natural-key UNIQUE backstop
(`uq_eurostat_raw_natural_key`, history.md) measured ~1.96M duplicate raw rows
across 2017–2018 (up to 50 per cell, HS detail included), so it scopes itself to
`period >= 2019-01` and deliberately leaves this era untouched. Deduping /
re-ingesting 2017–2018 is the prerequisite to extending the unique index back
over those periods.

## Methodology depth (pick up if a story warrants it)

### Editorial calibration of `low_base_threshold_eur` via shock-validation backtest

> **Promoted to iteration 4 of the journalist-usability arc** (see top
> of this file) — the threshold now drives the rendered Quotability
> verdicts, so this is no longer "pick up if a story warrants it".

Phase 6.3 sensitivity sweep showed €50M is the single largest
editorial-framing driver — 49% of `hs_group_yoy*` findings would
flip low_base classification across €5M–€500M. The default has
never been calibrated against editorial reality. Approach:

1. Take each shock from `2026-05-09-shock-validation.md` (2018
   Section 232 steel, Q1 2020 COVID lockdown, Feb 2022 Russia
   invasion, Oct 2023 EV probe).
2. For each, identify which HS groups carried the story and what
   their absolute 12mo €-figures were at the surfacing anchor.
3. Replay the sensitivity sweep — would those groups have been
   suppressed under €100M? Surfaced cleanly under €25M?
4. Pick the threshold that minimises both false-positive
   (low_base flag on a real story) and false-negative (no flag
   on a story that genuinely rests on a niche base).

Same exercise plausibly applies to the Soapbox-validation
2026-05-11 doc's per-claim concur table. Decide whether to keep
€50M (the engineered floor), shift it, or move to a per-group
threshold seeded from the group's typical EU-27 12mo magnitude.
Discussed 2026-05-12 with the first-export audit.

### CN8 concordance table (Phase 4 carry-over)

Full mapping of old→new codes across Eurostat's annual revisions.
Currently we apply a blanket `cn8_revision` caveat to any YoY
window spanning a year boundary; a real concordance would let us
strip the caveat where the relevant codes didn't change. The
historical Eurostat backfill (2017–2026) spans 9 CN8 revisions so
the blanket caveat is on most findings. Pick up when a story rests
on a precise YoY for a specific HS-CN8 code.

### Per-(country, commodity) CIF/FOB granularity

Phase 4 carry-over partially addressed in
`2026-05-10-cif-fob-baselines.md`. Per-(EU member state, China)
margins are now in `cif_fob_baselines`. The OECD ITIC SDMX endpoint
also supports HS-4 splits (1224 commodities × 28 EU countries × CN
≈ 34k rows) for per-(country, commodity) precision. Schema-extend
`cif_fob_baselines` and pull when a story needs it.

### Structural-break detection (Chow / CUSUM)

Statistically right but unstable on the 11 periods we had at
Phase 1. With the historical Eurostat backfill we now have 110
periods; this is ready to move from parked to scheduled if a
journalist's question warrants it.

### Sector breadth review (round 2)

The 6.5 promote/drop pass shipped 2026-05-10. A year from now a
similar pass should re-evaluate what's editorially live. Three
groups stayed draft (Honey, Polysilicon, Tropical timber) and
might warrant a second look.

### Derive January from Feb release's `(ytd − monthly)`

The 2026-05-15 Jan+Feb combined-release work closes the prior-year
Jan/Feb gap for 2020-2025 (years where GACC bundled them as a
single cumulative release). 2026 broke the combined pattern by
publishing a separate February release with both Monthly and YTD
columns — meaning January is implicitly available as
`Feb-release YTD − Feb-release Monthly = 1529.1 − 696.6 = 832.5
(100M CNY)` for Germany exports in our case. That's a deterministic
arithmetic identity, not interpolation: the cumulative IS the sum
of Jan + Feb, and Monthly IS Feb alone, so Jan = ytd − monthly by
definition.

Implementation sketch:
- At analyser time (not ingest), in `_gacc_aggregate_per_period_totals`
  (or a sibling helper), for each year where a Feb-only release
  has both monthly and YTD observations AND no separate January
  monthly exists: synthesise a January datapoint with value
  `ytd − monthly`, anchored at Jan 1 of the year. Source the
  derivation from both the YTD obs and the monthly obs (carry
  both obs_ids forward so the finding's provenance file shows the
  arithmetic chain).
- Same honest-accounting principle as the combined-release work:
  no interpolation, no estimation. Just an algebraic identity.
- Likely needs a new caveat code (`jan_derived_from_feb` or
  similar) so journalists can see when a window's January is
  derived rather than directly reported.

Editorial payoff: closes the remaining `partial_window` cases on
the four Lisa-facing bilateral findings; YoYs would shift by
roughly +5pp toward what's probably the true 12mo figure.

Roughly half a day's work. Triggered: any cycle where a journalist
asks why the current-year January is still flagged missing.

### Promote 2020 GACC Jan-Feb release (section=3 → section=4)

The 2020 combined Jan-Feb release was tagged `(3)` rather than
`(4)` by GACC (their own numbering inconsistency that year). Our
parser stored the section_number faithfully so the YoY analysers
skip it. Two options if a story rests on 2020 specifically:
manual override at ingest, or extend `_infer_section_from_description`
to take precedence when the prefix and the description disagree.
~30 minutes of work; deferred until needed.

### Provenance renderers for remaining subkinds

> **Absorbed into iteration 3 of the journalist-usability arc** (see
> top of this file) — no longer demand-deferred; the per-family detail
> below still describes the work.

The 2026-05-14/15 arc added detailed provenance templates for
`gacc_bilateral_aggregate_yoy{,_import}`, `hs_group_yoy*` (six
scope/flow variants), and `hs_group_trajectory*` (six scope/flow
variants). What stays as a stub for now:

- `mirror_gap*` — per-country CIF/FOB gap. Pick up if a story rests
  on a specific mirror-gap finding being challenged.
- `partner_share*` — China's share of EU extra-EU imports. Same
  trigger.
- `gacc_aggregate_yoy*` — non-EU bloc YoY (ASEAN, RCEP, etc.).
- `llm_topline*` — narrative lead scaffold. The verification chain
  is upstream (each verified number traces back to a deterministic
  finding); the LLM-prose audit is less pressing than the
  underlying-finding audit.

Each is ~100 lines of renderer + a focused test, modelled on the
shape of the corresponding subkind in `provenance.py`. The CLI flag
`--finding-provenance N` already returns a stub for any of these
that flags "generator pending"; extend `provenance._RENDERERS` when
a journalist asks for one specifically.

## Data sources (deferred until needed)

### 2018 GACC mirror-trade

See [`2026-05-10-forward-work-gacc-2018-parser.md`](2026-05-10-forward-work-gacc-2018-parser.md).
Title parser handles all 2018 quirks but the section-4 release
pages embed PNG screenshots, not HTML tables. Body parse fails
and the data is in pixels. Options: OCR (~half-day with editorial
risk), hunt for source xlsx, accept gap, lean on Eurostat+HMRC
which already cover 2018.

### Aggregate-label handling for non-EU blocs (ASEAN, RCEP)

Original project requirement. GACC reports trade with these blocs
as labelled aggregates; we don't currently split them into
member-country flows. Pick up if a non-EU-bloc story emerges.

### Chinese-language source URL backfill on `releases`

Most GACC releases have a Chinese-language equivalent at
`www.customs.gov.cn` (vs the English `english.customs.gov.cn`).
The brief constructs the link via `_construct_chinese_source_url`
but we don't store it. Backfill if useful for downstream consumers.

## Future-platform items

### Web UI / hosted deployment

Required for any non-luke-laptop use. AWS-side (Fargate, RDS,
Cognito) per the fuel-finder precedent. Triggers: when a desk
journalist actually wants to use the tool independently.

### Custom Q&A bot (Phase 7+)

See [`2026-05-10-forward-work-qa-bot.md`](2026-05-10-forward-work-qa-bot.md). Two-tier
scope (ask the findings, ask the underlying data). Tier 1 is ~3-5
days of build. Triggers: web UI exists AND a journalist has a
recurrent question pattern the brief can't answer.

### GoogleSheetsWriter implementation

Pending service-account credentials (was due "next week" as of
2026-05-09). Once available, the sheets-export module wires up
trivially.

## Closed forward-work, kept for reference

These are real options that may be picked up later but aren't on
any near-term path:

- [`2026-05-10-forward-work-gacc-2018-parser.md`](2026-05-10-forward-work-gacc-2018-parser.md)
  — 2018 image-only blocker (above).
- [`2026-05-10-forward-work-qa-bot.md`](2026-05-10-forward-work-qa-bot.md) — Phase 7+
  Q&A bot (above).
- [`2026-05-09-shock-validation.md`](2026-05-09-shock-validation.md)
  + companion `.sql` — pre-registered shock validation
  methodology. Living methodology doc, not just a one-off; re-run
  after any major analyser change. §5.4 refreshed 2026-05-11
  using the canonical `product_nc='000TOTAL'` row.
- [`2026-05-11-soapbox-validation.md`](2026-05-11-soapbox-validation.md)
  — peer-comparison validation against Soapbox Trade
  (substack). 50 testable claims pre-registered, ~60% clean concur,
  ~80% directional. The 2026-05-12 A1 re-test (Stage B/C) drove
  the four-step feature pass (Tier 1 hs_groups, briefing-pack
  modularisation, bilateral aggregate analyser, partner_share +
  extra-EU aggregates) recorded in
  [`history.md`](history.md#2026-05-12--soapbox-a1-re-test--four-step-feature-pass).
  Living methodology doc — re-run after any major analyser change.
- [`2026-05-10-cif-fob-baselines.md`](2026-05-10-cif-fob-baselines.md)
  — sourced reference for the OECD ITIC backfill. Reproducibility
  notes for refreshing in a future year.
