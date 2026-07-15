# GACC update page — design (2026-07-05)

**Status: agreed (Luke, 2026-07-05).** Consolidates the design session that
picked up the parked "Slim GACC-period update" (roadmap.md, 2026-06-23). The
parked content decisions stand (slim GACC-only sections; trigger on new
*period* not release; current-month mirror gap excluded; cost-sensible take
handling). **One parked decision is superseded:** the page does NOT
self-expire on the next Eurostat release — supersession is within-track (see
§ Two-track model). Build target: live for the GACC June-data drop expected
**8–10 July 2026**.

## Purpose

> Our differentiation is the EU/UK per-country detail with provenance,
> placed in the world context: is the EU move part of a general Chinese
> export surge, or EU-specific re-routing after US tariffs?

Wires (Reuters/Bloomberg) own the "China's exports rose X%" world headline
within hours of every GACC drop; we don't compete on that. This page gives a
reporter (a) China's own numbers for Europe, per-country, with provenance
drawers, ~5 weeks before Eurostat confirms; (b) the world context that makes
the EU number interpretable. It is GACC-specific and deliberately less
Euro-centric than the main briefing — but EU-anchored, not un-anchored.

## Two-track model (sequencing & supersession)

Publication rhythm (release_calendar.py; real 2026 dates):

| ~Date       | Event                    | Reference month     |
|-------------|--------------------------|---------------------|
| 8–10 Jun    | GACC publishes           | **May** (China-side)|
| 12 Jun      | HMRC publishes           | April               |
| 15 Jun      | Eurostat publishes       | April               |
| 8–10 Jul    | GACC publishes           | **June**            |

GACC is always one reference month ahead, landing days *before* the
Eurostat/HMRC drop for the month prior. So a calendar-arrival supersession
("next Eurostat release expires the GACC page") would retire the freshest
month in the system within ~6 days of it arriving, every month, and leave
the surface absent ~3.5 weeks/month — no reader habit can form, and
notify-chat links die young.

**Rule: supersession follows source-track + reference period, never calendar
arrival.** Two live surfaces at all times — the current main briefing and the
current GACC page — each superseded only by its own successor, cross-linked
("China has already reported June — see the GACC update" / "harmonised
European figures run to May — see the briefing"). Elegant consequence: the
GACC page for month M is retired by its own successor (M+1, ~9th) within
days of Eurostat confirming M (~15th), so a GACC page is always the *only*
current word on its reference month. Note the convergence: the parked
"self-expires on next Eurostat release", corrected for the reference-month
offset ("when Eurostat covers *its* month"), is within-track supersession
give or take a few days.

### brief_runs mechanics (the footgun)

`brief_runs` is a flat latest-wins log and the periodic-run idempotency
check is `MAX(data_period)` — where `data_period` is documented as
**Eurostat** freshness (`briefing_pack/render.py::latest_recorded_data_period`).
A GACC-page row carries a data_period one month ahead; unscoped, the main
track would conclude it had already published a month it hasn't and no-op.

- GACC-page runs get their own `trigger`/stream value; **every** baseline
  and since-last delta query must be track-scoped. The per-trigger filter
  already exists; `latest_recorded_data_period(trigger=None)`-style calls
  are the bug to hunt.
- The page's "since the last read" diffs against the previous GACC period;
  the main briefing diffs against the previous main run. Never across.
- Trigger on first arrival of a new GACC reference month; the second
  dual-currency release for the same period is a quiet within-track refresh
  of the same page (same-month supersede), not a second release.
- notify-chat says which track fired ("GACC June → early-read page updated"
  vs "Eurostat May → briefing rebuilt").

## What the page is / is not

**Is:** China-perspective throughout ("China's exports to the EU" — GACC's
own frame and wire convention; deliberate divergence from the main page's
Europe-perspective, with the mapping stated in the expander). Change-shaped:
growth rates carry the page; levels appear as facts within findings, never
as headline faces. Partners-spine (vs the main page's sectors-spine).

**Is not — stated on-page:**
- **No sector/product detail.** The GACC country/region release has no
  commodity dimension; the page is partners-only by data availability. Say
  so: "product-level detail arrives with the European confirmation in ~5
  weeks" — pre-answers the obvious question, turns the limitation into
  cadence-clarity.
- **No current-month mirror gap** (its Eurostat side doesn't exist yet;
  refreshes normally on the next Eurostat cycle — unchanged from parked
  design).
- **No €/day-family analogue.** No "surplus per day" face, however tempting:
  it would set up a competing level number across the ~20% mirror against
  the main page's Eurostat-based €1bn/day family. Levels de-emphasised
  page-wide.

## Scope ruling: "China" ≠ CN+HK+MO here

The main page's CN+HK+MO bundling (`EUROSTAT_PARTNERS`, anomalies.py — the
editorial-standard envelope on the Eurostat side) **cannot be paralleled**:
GACC is the mainland customs territory's own ledger; Hong Kong and Macao run
separate customs administrations. On this page "China" ≡ mainland by
construction, and HK/MO switch sides of the ledger to appear as *partners*.
A mainland→HK→EU entrepôt shipment appears in GACC as an export to Hong
Kong — one structural driver of the mirror gap, and the very reason the
Eurostat side bundles. Same problem, opposite ends: Eurostat-side you bundle
the partner; GACC-side you can't bundle the reporter.

- Scope note (identity header / expander), roughly: *"On this page 'China'
  means the mainland customs territory, GACC's remit; goods routed via Hong
  Kong appear here as trade with Hong Kong. The main briefing's Eurostat
  figures use the wider China+Hong Kong+Macao envelope, so the two pages'
  'China' differ by construction."*
- **HK-as-partner is a feature:** mainland exports *to* HK are an entrepôt
  leading signal — worth a line in the world table, labelled so it is never
  summed into an EU figure.
- A China-side "greater China → EU" composite would need HK C&SD re-exports
  by origin+destination plus double-count decomposition (mainland→HK exports
  vs HK re-exports of mainland origin). Real candidate source; parked under
  breadth expansion with its own principle-6 pass.
- Macao: negligible volumes; nothing measurable lost.

## Report structure

Reader context: arrives via chat ping within days of the release, asking
"what did China just say?"

1. **Identity header.** "China's customs figures for June 2026 · published
   10 Jul · European confirmation due ~15 Aug" (due date from
   release_calendar.py — teaches the two-track rhythm). Two standing
   caveats, one line each: China's own figures (levels historically ~20%
   above the EU mirror); FX note (values EUR-equivalent at per-period rates,
   so growth rates won't exactly match GACC's published CNY/USD figures —
   matters more on a page that leads with GACC growth rates). Source links
   to **both** the English and Chinese-language releases
   (`_construct_chinese_source_url` exists at brief time).
2. **Standout.** One partner-agnostic take on the most newsworthy move
   anywhere in the release (ASEAN/US/EU member — wherever). Anti-fixation:
   the biggest-mover philosophy applied here.
3. **Context strip (doubles as the KPI row).** Four cards: **EU · US ·
   ASEAN · World**, single-month YoY face, YTD beneath, exports flow. The
   purpose statement made into furniture. **Verified pure rendering:** the
   aggregate findings carry optional `ytd_block` + `sm_block` operators
   (`_insert_gacc_aggregate_yoy_finding`) and bilaterals carry all three
   operators (rolling_12mo / ytd_cumulative / single_month) natively — no
   analyser work. Provenance drawer per card.
4. **Europe up close.** EU bloc, both flows — China's exports to the EU AND
   China's imports from the EU (the European-exporters'-China-demand story).
   Single-month leads; YTD and rolling-12mo behind. Then the per-country
   table: every EU member GACC releases **plus the UK** (China-side UK read,
   present despite HMRC having no role on this page), sorted by single-month
   swing. Takes on the EU-bloc finding + top 2–3 country movers only.
   Trend chart via existing machinery.
5. **China and the world.** ASEAN, RCEP, Belt & Road, Africa, LatAm + world
   total, both flows; table-first; reuse the shipped regional charts. Takes
   only when one of these is the standout. (The findings already embed
   "GACC-side data only; no Eurostat counterpart… cross-reference UN
   Comtrade".)
6. **Since the last read.** Within-track delta: biggest YoY swings vs the
   previous GACC period; revisions via the supersede chain; dual-currency
   second-release refresh note when applicable. Small.
7. **Understanding these figures** (collapsed expander, About-box pattern).
   Flow-direction mapping; the scope note (§ above); mirror explanation +
   methodology link; Jan–Feb combined note when in-window; quotability
   legend. Later add (v1.1): computed track record of recent early reads vs
   subsequent Eurostat confirmations (past months' mirror findings already
   exist); v1 carries a static sentence.

**Cross-page (main-track, later):** the reconciliation note — each main
briefing scores the *previous* GACC read against the just-arrived harmonised
numbers ("GACC's early read for April implied X; Eurostat says Y; gap n% ≈
the structural ~20%"). Uses existing mirror-gap machinery + the pinned
CN+HK+MO comparison conventions; teaches reporters to read the page as a
leading indicator with known bias.

## LLM layer

Budget is not the constraint (total project spend to date: $2.25; everything
below ≈ single-digit dollars/year at monthly cadence). The constraint is the
verify-or-reject discipline — which already exists: `llm_framing`'s
verify_numbers round-trip (every cited number must match a supplied fact or
the whole output is rejected into `llm_rejection_log`), and
`hypothesis_catalog` (13 entries incl. `tariff_preloading`,
`transshipment_reroute`, friend-shoring, trade-defence) so the LLM picks
explanations, never invents them. **Fence: no free-form declarative analysis
outside catalog+verify** — the interrogative takes and the catalog are why
the tool can't be confidently wrong, not cost-saving measures.

**V1 (ship with the page):**
- **Release-synthesis scaffold** — page-level lead-scaffold on llm_framing's
  three-part contract: fact-verified summary connecting the context strip
  ("EU +12% against a world average of +4%, US −18%"); 2–3 catalog
  hypotheses; deterministic corroboration steps. Needs a page-level fact
  set + 1–2 new catalog entries framed China-side (the US↓/ASEAN↑/EU↑
  diversion signature — existing entries are written around Eurostat
  partner-field effects). Failure mode: silence, by design.
- **"Questions this release raises"** — the takes contract (interrogative on
  purpose) once at page level, each question annotated for answerability:
  "our data answers this → [drawer]" vs "needs external source → UN
  Comtrade / Eurostat confirmation ~15 Aug".

**V1.1 (after first live cycle):**
- **Takes v2 debut: curated policy-events retrieval.** The takes design
  reserves v2 for "specific external facts allowed iff retrieved and
  cited"; first retrieval source = a small journalist-editable events file
  (dated tariff rounds / export-control announcements, each cited). Turns
  "consistent with re-routing" into "the US decline began the month after
  the April tariff round". (The roadmap's tariff-timeline item, concrete.)
- **Chinese-language commentary.** Fetch + translate + summarise GACC's
  spokesperson / press-conference framing, clearly attributed as GACC's own
  account. No wire does this systematically; fits the page's China-source
  identity. Principle-6 hands-on look at the Chinese pages first; adapter
  with append-only snapshots.
- **Streak/rank facts.** "Third consecutive month of US decline"; "largest
  single-month EU jump since 2022-10". Finding payloads already store their
  series with obs_ids → clean derived provenance. Code computes; Claude
  narrates; the LLM never counts.

Parked: per-beat routing of chat notifications (needs a beats registry +
telemetry evidence that multiple reporters engage).

**Take gating:** takes on EU bloc + top 2–3 EU movers + the standout +
the two page-level slots. No per-finding takes for all ~24 single-country
partners; non-EU partners render as tables/drilldowns until demand shows.

## Build plan

Sequencing agreed 2026-07-05 (ahead of other roadmap items): the page first,
aimed at the 8–10 Jul drop; then the instruments batch (heartbeat, portal
telemetry, biggest-mover feedback link — IAP request logs accumulate
regardless, so launch-week usage is reconstructable after the fact); then
the iteration-4 calibration sprint (near-orthogonal: this page's aggregate
findings sit far above low-base thresholds, and recalibration propagates
retroactively via method bump). E1 phase 1 stays parked — this build adds
**no analysers**; the tripwire is the next analyser family (China↔UK
mirror, Eurostat-revisions).

Suggested PR slicing (branch-per-change off fresh main):
1. **Trigger + track scoping** — new-GACC-period detection wired to the
   routine; stream/trigger value; track-scoped baseline/delta queries +
   tests (the `trigger=None` hunt); second-currency quiet-refresh handling.
2. **Slim variant + portal tab** — reshape the existing GACC report variant
   into the § Report structure (verify its current section list first);
   context strip; per-country table; identity header; expander; scope note.
3. **LLM V1** — page-level fact set; synthesis scaffold + questions-take on
   the existing verify-or-reject plumbing; China-side catalog entries.

Build-time checks:
- Trend charts' EU line pre-2020: how does GACC's historical "EU" label
  treat the UK (EU-28 boundary)? Worst case start charts at 2021.
- Confirm the GACC bilateral partner list includes US + HK as expected.
- data_period semantics for GACC-track rows (stamp the GACC reference
  month; document the divergence from the Eurostat-freshness convention).
- Verify the current GACC report-variant section list in report_builder
  before reshaping (this doc was written from analyser payloads + roadmap,
  not a report_builder read).
- ~~Tab naming — open question~~ **Decided 2026-07-05 (Luke): tabs are
  period-explicit.** **"Full briefing (Apr 2026)"** and **"GACC-only
  (May 2026)"** — readers know the sources; the visible month offset makes
  the two-track cadence legible in the tab bar itself, and "-only" carries
  the single-source caveat in one word. ("Full" chosen over "Combined" to
  avoid colliding with the internal eu_27_plus_uk "combined scope"
  vocabulary.) Details:
  - Month format: the portal's existing abbreviated convention ("Apr 2026")
    — year always present (cross-year ambiguity around Dec/Jan).
  - Label semantics = the reference month of the page's *lead claim set*
    (the Full briefing's GACC context sections run a month ahead inside
    it; per-source freshness stays disclosed in Sources & coverage).
    One-line definition in the About box.
  - chat-notify pings use the identical label ("GACC-only (Jun 2026) is
    live") — ping→tab consistency teaches the rhythm.
  - Jan–Feb combined GACC release renders "(Jan+Feb 2027)" (existing
    jan_feb_combined handling; 2026's standalone Feb broke the pattern
    once, so derive the label from period_kind, not assumption).
  - Build (PR 2): each track's tab label needs that track's current period
    at render time — if the two tracks live in one snapshot document, a
    one-track rebuild must carry the other track's content+label over
    (reuse-takes discipline applies to content assembly, not just
    brief_runs); if separate documents, the shell reads each track's
    period from its own snapshot. Decide the composition model in PR 2.
- **Masthead & chips (flagged by Luke 2026-07-05): claims descend to the
  level where they're true.** Today's masthead (`report_render_html.py`,
  `mast-meta`) shows the snapshot variant as a chip ("Eurostat") and
  "Data to {period}" — both single-track claims that become false once two
  tracks coexist (and the browser `<title>` repeats the period claim).
  - **Global masthead**: brand + subtitle only, plus the one fact true
    portal-wide: "Updated {generated_at}" (promoted from the footer; later
    joined/replaced by the heartbeat "pipeline last ran" stamp from the
    instruments batch). No source chip, no global "Data to".
  - **Per-tab identity strip** (first element of each content tab) inherits
    the current chips' claim-structure, now true: Full briefing → "Eurostat
    · to Apr 2026", "HMRC · to Apr 2026", "GACC context · to May 2026"
    (the third chip makes the lead-claim-set semantics visible and
    pre-empts "why does the GACC section here show May?"); GACC-only →
    "GACC · May 2026" + "European confirmation due ~{date}". Keep the
    existing "to" convention where windows are rolling; single-month leads
    read plain.
  - Tables tab inherits the Full-briefing strip (its data serves that
    track); Sources & coverage stays the detailed freshness view;
    Methodology/Glossary carry no vintage.
  - `<title>` drops the period ("Meridian — China–Europe trade").
  - `m.variant` semantics: with two tracks, variant effectively becomes
    the track/stream — PR 2 maps or renames it; don't leave both concepts
    live.

## Superseded / source decisions

- roadmap.md § "Slim GACC-period update" (2026-06-23): content decisions
  stand; the self-expiry rule is superseded by § Two-track model above.
- Chat design session 2026-07-05 (this doc is its record): purpose
  statement, two-track rule, structure, scope rulings, LLM set, sequencing.

## As-built addendum (2026-07-05, post-deploy)

Everything above shipped same-day (#119–#125; full narrative in
[`history.md`](history.md) § 2026-07-05). Deltas and decisions made during
the build, recorded so this doc doesn't mislead a future reader:

- **Composition resolved ONE-BLOB**, not the two-document option § Build
  plan left open: tabs are client-side panels of a single static snapshot,
  so the page ships as a first-class tab — zero portal_service /
  portal_publish changes, labels always consistent, and PR 1's track
  scoping already makes cross-track rebuilds correct. The two-blob +
  manifest idea died on inspection of the render model.
- **Tab labels as decided** ("Full briefing (Apr 2026)" / "GACC-only
  (May 2026)") + the masthead descent per § masthead. "Methodology" tab
  relabelled "Method" (nav width; the tab KEY is unchanged so deep-links
  hold).
- **Live-review additions** (same evening): the identity caveats collapsed
  into an "About this page" disclosure placed under the KPI strip (the
  Briefing's About-box slot); the since-last-read swings gained a
  **dumbbell chart** (open dot = previous month, filled = current, zero
  line marked; plots the dominant basis only — mixing single-month and
  12-month readings on one axis would mislead — with exclusions named in
  the caption); the page gained the Briefing-style sticky subnav; the
  world table got th.num alignment + tokens folded under partner names.
- **LLM layer as designed, plus the answerability enum**: the questions
  take tags each question with where its answer lives (six fixed values —
  our sections / drawers / Eurostat-confirmation / UN Comtrade / no-product-
  detail); the render supplies the words and links per tag. Generation on
  the new-period path only (~2 calls/month, `--skip-llm` opts out);
  refreshes and main-track rebuilds carry the slots via
  `portal_takes_reuse.graft_gacc_slots`, gated on the GACC page's OWN
  data_period. Operational lifecycle (the "one command preserves both
  tracks" invariant + the publish-order caveat):
  `portal_service/README.md` § "The second track".
- **Deploy-day lessons**: (1) the reuse graft needs
  `PORTAL_BUCKET=meridian-500111-portal` +
  `GOOGLE_CLOUD_PROJECT=meridian-500111` at build time — both now in
  `.env`; the first Routine fire lacked them, built a bundle with empty
  briefing takes, and publishing it would have wiped the live ones (a
  `--upload-to-portal` takes-count guard is a roadmap residual). (2) The
  verifier's `_TIME_PERIOD_RE` learned the bare "12mo" shorthand after a
  live false positive — the model echoes the notation it is shown, so fact
  lines say "12-month". (3) `notify`'s export enrichment needed the track
  filter (a gacc row masqueraded as "a fresh briefing" otherwise).

---

## Addendum — 2026-07-15: single-period surfaces & the by-country consolidation

**Trigger:** Luke hit the Full briefing's "GACC context · to Jun 2026" chip
cold and read it as a two-track violation. It wasn't — the cycle invariants
all held — but the investigation exposed a real remnant: the briefing's
"China's trade by country (GACC)" section predated this design (f2a97c7,
21 Jun), floated at GACC-latest (after the June drop, TWO months past the
Apr anchor — quarter-end timing makes the doc's "a month ahead" the best
case, not the rule), and this doc ratified rather than reconciled it. If the
designer trips on it, reporters will.

**Ruling (Luke): surfaces are single-period.** Every claim on a tab is
anchored to that tab's reference month; the other track is reached by
cross-link, never embedded at another vintage.

As built (branch `ljh-gacc-by-country-consolidation`):

- **Briefing:** the GACC section is REMOVED. The identity strip carries
  Eurostat/HMRC vintage chips only, plus a SIGNPOST ("China has already
  reported {Mon} — see the GACC-only tab") that claims nothing about this
  page. The state-of-play see-also bridge points cross-tab. And
  `what_changed.new_count` is track-scoped (gacc_* subkinds excluded): a
  GACC calendar arrival must not move the briefing's "since last briefing"
  tally. (The bundle findings.md keeps the whole-cycle count — portal-model
  scoping only.)
- **GACC page:** "Europe up close" + the briefing's former 24-partner
  roster CONSOLIDATE into one grouped **"By country"** section — Europe
  group first (EU bloc default-open, then member states + UK), then Rest
  of the world; biggest-first within groups (the change-shaped reads stay
  in strip / standout / since-last). **"China and the world" stays as-is**
  and gains the three annual region charts (region-tier data beside the
  region-tier table), moving ABOVE By country — compact context before the
  long roster. Each partner row carries a fixed-size ◐ **balance dial**
  (the larger flow fills the row budget, the smaller scales by √ratio) —
  same grammar as the world strip's area-scaled glyphs, which keep the
  single-country majors per the 2026-07-05 proportion ruling.
- **Copy/plumbing:** the publication-calendar effect line no longer claims
  the briefing refreshes on a GACC drop; About-box guide updated; the
  `europe_section` answerability TAG is kept (baked into stored LLM
  output) and retargeted to `#gacc-bycountry-europe`; `GaccPage.europe` →
  `by_country`; schema 0.4.0.
- **Identity note:** this doc's "slim GACC tab" framing is formally
  retired — with the commodity catalogue and the full roster this is the
  dense page, and rightly so: it is the product's freshest asset.

**Refinement (2026-07-15, same day, Luke):** the world section sheds its two
non-region rows. The **UK** goes (it was in neither the region charts nor the
KPI strip; it lives one scroll down in the Europe group) and the **HK
entrepôt line** goes — so the section's cast equals the region charts' cast
exactly (Total + ASEAN / EU / US / Latin America / Africa / Russia). The US
and Russia stay: the section's framing question needs the US, and Russia is
the standing story. The entrepôt treatment moves to the By-country HK row,
upgraded: a structural iso2 flag (HK/MO, never label spellings) drives a
collapsed-row chip + a panel-leading explainer, including the cross-reference
to the Full briefing's Mirror-trade gaps (plain text, not a link — section
anchors don't switch tabs). This also fixed a gap the consolidation would
have shipped: the old warning lived only on the world row, so the By-country
HK row — second-biggest rest-of-world, +189% import month — would have
carried no flag at all.

**Plus (same day):** the world section gained its own "More about" expander —
the cast rationale ("every region-level aggregate GACC publishes, plus the US
and Russia; the cast is GACC's, not an editorial selection") and **why there
is no Middle East line**: GACC's preliminary release publishes no Middle East
aggregate and names no Middle East partner (no Saudi Arabia, no UAE) — that
trade sits unseparated inside Belt & Road and the world total; splitting it
out needs GACC's fuller monthly publications or partner-side data (UN
Comtrade), on the roadmap. Echoed in the By-country "Which countries appear"
note. Rationale: an absence a reporter would otherwise read as our omission
must be named as the source's reporting choice (provenance principle — never
present a gap without saying whose gap it is).
