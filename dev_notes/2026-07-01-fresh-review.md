# Fresh review — decisions, code, tests, and value gaps

**Date:** 2026-07-01
**Scope:** the June 2026 sprint (roughly PRs #43–#112: the portal go-live and
iteration arc, the Q2 sector expansion, the source-freshness suite, the
self-verifying-portal MVP, and the 2026-06-25 adversarial-review fixes), plus a
whole-system look at what's missing that could add value.
**Method:** five independent review passes (decisions/docs audit; ingestion
layer; analysis layer; presentation/delivery layer; tests & CI), each run
fresh against the code, then cross-verified — every high-severity claim below
was re-checked against the source before inclusion, and several
claims that did not survive verification were discarded.

**Relationship to the prior reviews:** `2026-05-12-review-2.txt` was a
simplification critique; `2026-06-25-adversarial-correctness-review.md` was a
correctness/robustness pass whose findings (A1–E1) were point-fixed in
#106–#111. This review is complementary: it verifies those fixes actually
hold, looks where that review didn't (delivery surfaces, tests/CI, the
newest analyser families), audits the June *decisions* against the project's
seven principles, and closes with a product-level gap analysis.

---

## Headline

The June work holds up. The core invariants the previous review certified
re-confirmed independently: the supersede chain is enforced at the database
layer (partial unique index, no resurrect path in `findings_io`), the
provenance drawers format the *stored* finding values rather than recomputing
them, FX is skip-not-guess, HTML escaping in the portal renderer is systematic
(no XSS vector found; no `innerHTML` anywhere in the embedded JS), and
`portal_service` has no path-traversal surface. All six 2026-06-25 fixes
(A1, A2-guard, A3, B1, B2, D1) genuinely landed, at both the analyser and
presentation layers.

The new findings cluster into three themes:

1. **One data-integrity gap that contradicts the project's own principles** —
   the HMRC raw layer has a delete path, and it isn't atomic (F1).
2. **One systemic blind spot** — low-base protection exists at exactly one of
   three journalist-facing layers (F2).
3. **One irony** — the trust surface (provenance drawers) is the least-tested
   code in the repo, and disagrees cosmetically with the cards it explains
   (F3, and the tests section).

---

## Prior-fix verification (all confirmed in code)

- **A1** (GACC empty-parse guard): shipped — `scrape.py` records `failed` and
  creates no release row on a zero-observation parse; tested. *Residual gap:
  see F5 — the guard is `len == 0` only; the plausibility floor the original
  finding proposed was not implemented.*
- **A2** (EU-27 reporter scope): the detect-and-alert guard is live
  (`eurostat.unexpected_reporters`, ingest-time ERROR). The inclusion-list
  swap remains correctly deferred to E1, recipe in `roadmap.md`.
- **A3**: the `hs_patterns` CHECK constraint and the `<> '000TOTAL'`
  query-level backstop are both present (`schema.sql`; the shared
  `_hs_pattern_or_clause` helper).
- **B1**: `excess = (gap_pct - baseline_pct) if gap_pct > 0 else None`
  (`anomalies.py:542`, method `mirror_trade_v7_negative_gap_excess_na`);
  the negative-gap render is an honest "freight cannot explain this" note;
  `sheets_export.py:689` carries the matching guard — no drift between the
  two copies. `docs/methodology.md` §0 now describes the pre-fix +65% figure
  historically (D1 fixed).
- **B2**: `AND r.currency = 'CNY'` present in `_select_gacc_export_rows`;
  regression test exists (`test_mirror_gap_currency_pin.py`).

---

## Decisions audit

The ~12 consequential June decisions were checked against the seven
principles (ingest-broadly, defensibility, never-mutate, append-only,
idempotency, look-at-data-first, provenance). **None is wrong.** Sound and
well-documented: the 000TOTAL fix with method-bump supersede; the CN+HK+MO
envelope with explicit scope labelling; `--portal-reuse-takes` with the
fail-loud-when-publishing split (verified end-to-end: `PriorSnapshotUnreadable`
propagates past the best-effort wrapper exactly as PR #82 claims); docx-off
by default with the .md corpus kept; hidden-groups staging; the 2019+ partial
UNIQUE backstop; the Biggest-mover KPI's honest "within what we monitor"
framing; the E1 deferral (the point fixes hold and the plan is written).

Three residuals deserve a conscious decision rather than drift:

- **R1 — the mirror-gap normalisation mixing survived B1.** `gap_pct` is
  max-normalised (`gap / max(E, G)`, `anomalies.py:525`) while the CIF/FOB
  baseline is a ratio-space markup, so `excess = gap_pct − baseline_pct`
  still subtracts across two different definitions of "percent" — worth
  ~0.4pp at current NL-size gaps. The June fix chose relabel-and-hedge over
  redefine, a legitimate option the review itself offered. Redefining
  `gap_pct` as ratio-space (`gap / GACC`) would make the baseline subtraction
  *exact* — but it renumbers published gaps (NL ~+20% → ~+25%), an editorial
  call. Recommendation: fold into the next natural method bump with a
  changelog note, rather than leaving a permanent known-approximation.
- **R2 — E1 has no date while analyser velocity is high.** June added three
  new analyser families in ten days; each new family is a fresh chance to
  re-drift the rules E1 centralises. Schedule at least E1 Phase 1 (the EU-27
  inclusion swap — recipe already in `roadmap.md`) *before* the next family
  lands.
- **R3 — process note for Q3:** the hidden-groups staging gate (#99) shipped
  two days *after* the Q2 groups went straight into live rankings. It worked
  out; the next expansion should use `hidden:` from the first commit.

---

## Findings (new; all verified before inclusion)

### F1 — HIGH: HMRC re-ingest can silently destroy raw history

`db.py:366-376` / `scrape.py:445-450`. Re-ingest is delete-then-insert, and
each half opens **its own connection and commits independently**
(`db.transaction()` constructs a fresh `psycopg2.connect` per call). A crash
between the two commits deletes the period's raw rows permanently — and
because the release row from the *original* ingest still exists, the next
probe sees the period as present and never re-ingests. The half-state is
silent and persistent until a manual re-run, with `observations` holding
dangling `hmrc_raw_row_ids` provenance pointers throughout.

It is also a principle violation by design: the delete exists because
`hmrc_raw_rows` has no natural-key constraint (the docstring says so) — the
HMRC raw layer trades append-only for dedup-by-deletion, while the same June
sprint gave Eurostat's raw layer the proper answer
(`uq_eurostat_raw_natural_key`). Same problem, two different answers.

**Fix, two steps:** (1) wrap delete + insert in one transaction (~5 lines) —
shipped alongside this review; (2) give HMRC the Eurostat treatment (natural
key + `ON CONFLICT DO NOTHING`) so the delete path can be removed entirely.

### F2 — HIGH: low-base protection exists at exactly one of three layers

The €50M gate + Quotability machinery guard the **group total** only:

- **Per-reporter breakdown** (`_build_per_reporter_breakdown`,
  `anomalies.py:1440-1500`): each member state's YoY is
  `delta / abs(prior)` with **no floor on that reporter's prior value** and
  no low-base marker in the output dict — rendered on four surfaces (portal
  `report_builder.py:805`, briefing `hs_yoy_movers.py:163`, xlsx
  `sheets_export.py:436`, docx). A €150k→€9M single-reporter swing prints
  "+5,900%" verbatim directly beneath a green group-level Quotability
  verdict. Ranking by absolute delta softens the worst cases but does not
  stop material-delta-from-tiny-base ones.
- **Both GACC YoY families** (`detect_gacc_aggregate_yoy`,
  `detect_gacc_bilateral_aggregate_yoy`) emit unconditionally
  (`yoy_threshold_pct=0.0`) and have **no `low_base` field at all** — a
  small partner's +1,088% gets the same rendering weight as a China–ASEAN
  headline. Bounded by the ~24-partner curated set, so slightly less exposed.
- Meanwhile `detect_cn8_biggest_mover` — the newest family — got excellent
  gates (dual floor, persistence-across-anchors, leave-one-out). The team
  knows the trap; the protection wasn't retrofitted sideways.

**This reframes iteration 4**: calibrate once, apply at all three layers.
Cheapest interim: suppress or annotate the *percentage* (keep the € delta)
wherever the prior value is under a modest floor.

### F3 — MED: the provenance drawer disagrees with the card it explains

`provenance_payload.py:36-52` has local `_eur`/`_pct` formatters; the KPI card
uses a third inline format (`report_builder.py:403`); neither uses the shared
`briefing_pack._helpers._fmt_eur`. Concretely: the flagship EU-deficit card
renders "€1,027M/day"-style while its own drawer renders "≈€1.0bn/day" for
the identical stored value (different bn-boundary and precision rules).
Never *wrong* — both read the same `detail` field — but the drawer exists for
the moment a journalist double-checks before quoting, and a mismatch there is
where trust leaks. Unify on one shared formatter; the drawer's bn-boundary
rule is arguably the better editorial choice.

### F4 — MED: a mid-cycle analyser crash is invisible to the tool's own observability

`periodic.py:403`: an exception in any analyser aborts the whole cycle with
**no `periodic_run_log` row** — `--periodic-history` shows nothing and the
Chat notification simply doesn't arrive. Safe for publish integrity (nothing
half-built goes live), silent for operations: a reporter waiting on a
briefing has no visible reason it's missing. Wrap the cycle in a
try/except that persists an error row before re-raising.

### F5 — MED: A1 closes only the catastrophic case

The shipped guard is `if not result.observations` (`scrape.py:112-136`); the
plausibility floor the original A1 finding proposed (~180 obs for a section-4
parse, or "the Total row must be found") was not implemented. Partial column
drift — some `<tr>`s keep the expected cell count, others don't
(`parse.py:394-398` silently skips non-matching rows) — still records a clean
success and reintroduces the partial-window YoY bias A1 named. Implement the
originally-sketched floor; downgrade to `failed` on miss.

### F6 — LOW (bundled)

- **Publish/write atomicity**: the GCS publish (`portal_publish.py:116-127`)
  and the local snapshot write (`periodic.py:247-248`) are each sequential
  two-file operations with no staging/rename. Self-contained `index.html`
  makes real impact small; temp-file-then-`os.replace` is ~10 lines.
- **`--hs-prefix` backfill silently no-ops** on an already-present period:
  the presence guard (`db.eurostat_reporters_present_for_period`) checks
  `(period, partners)` but not `hs_prefixes` — an operator widening HS scope
  gets `noop` with no signal. Topical because the NL/GR incident was exactly
  a manual-backfill scenario. Minimum: a warning log naming the limitation.
- **`notify._new_data_since`** (`notify.py:156-178`): `DISTINCT ON (source)`
  collapses two arrivals between notify cycles into one message (later row's
  notes only). Degrades gracefully; edge case at daily cadence.
- **`partner_share` internal `score`** (`anomalies.py:5253`) sums
  percentage-points with a fraction×100 — unit-mixing in a field nothing
  currently reads. Fix or delete before anything sorts on it.
- **Upsert fast-path design note**: `db.upsert_observations`' freshness
  check is scoped to `partner_country` only — safe for every current caller
  (one row per partner), silently wrong if a future section-5/6 GACC parser
  emits multiple rows per partner through the same path. For whoever builds
  the China-side HS mirror.

---

## Tests & CI

Unusually healthy: hermetic, numeric, regression-anchored (every previously
shipped bug has a named test); CI runs the full DB-backed suite (605 passed /
6 skipped) against real Postgres in ~45s. Two structural points:

1. **Coverage risk concentrates in the June-24 additions** — the newest
   journalist-facing surfaces are trusted on the fewest assertions:
   - `provenance_payload.py`: **zero tests** (256 lines; the defensibility
     surface — pairs badly with F3).
   - `detect_cn8_biggest_mover`: only the pure gates are tested; the SQL
     window/join/insert path is not — a join bug ships a confident wrong %
     with provenance attached.
   - `eurostat.aggregate_to_world_totals` / `scrape_eurostat_world_totals`:
     untested — the denominator of the flagship 22.5% China-share KPI.
   - `report_builder` KPI extraction (`_latest_deficit_per_day` etc.):
     untested numerically; a drifted JSON path makes the KPI *silently
     vanish* (the empty-safe test stays green).
   Each has a cheap test cloning an existing pattern
   (`test_china_all_goods_share.py` is the template). Bundle as one
   test-catch-up PR. Second tier: GACC happy-path e2e (fixture HTML through
   `scrape_release`), `gacc_bilateral_aggregate_yoy` (1 test; its sibling
   selector had B2), `partner_share` (1 test).
2. **CI can't catch schema-vs-migrations drift**: the workflow loads
   `schema.sql` only, and `tests/test_db.py:277` *skips* (not fails) when a
   migration is absent — drift would pass CI while prod runs different DDL.
   Make the lockstep check fail under CI. Also: six live-DB invariant tests
   (000TOTAL reconciliation, orphan findings) never run automatically
   anywhere — a scheduled `pytest -k live` against production, logged via
   the routine machinery, closes that. Deps are fully unpinned; a
   constraints file stops upstream releases breaking CI under unrelated PRs.

---

## What's missing that would add value

Ordered by leverage; excludes what the roadmap already schedules.

1. **A pipeline heartbeat journalists can see.** The pieces fail loud; the
   *system* can fail silent (laptop is a single point of failure; a crashed
   cycle logs nothing — F4; the overdue alert only fires if the routine
   itself runs). A dead-man's-switch — a daily "still alive, data as of X"
   Chat line plus a visible "pipeline last ran" stamp on the portal — turns
   "silence means fine" into "silence means look". Matters more now that the
   team relies on the cadence.
2. **Portal usage telemetry before iterations 4 and 5.** The quote audit,
   the low-base calibration, and change-based delivery all depend on knowing
   what journalists actually open. The portal sits behind IAP — request logs
   already exist; coarse per-tab/per-drawer counts beat asking reporters to
   self-report.
3. **Wire the Biggest-mover feedback loop.** The card's whole rationale is
   demand discovery, but it has no affordance to say "watch this". A
   per-card link posting to the existing Chat webhook is a one-day build
   that completes the instrument.
4. **Surface Eurostat revisions as findings.** Eurostat *does* revise;
   today a revision only shows as supersede churn in "What changed". A small
   analyser emitting "Eurostat revised April NL imports by −8%" is both a
   journalistic signal and a data-quality tripwire — and gives the deferred
   supersede-reason classification a concrete consumer.
5. **Promote the 2017–18 re-ingest.** Already roadmapped, but it now
   unblocks two live things: extending the dependency-trend line back two
   years, and extending the UNIQUE backstop over the era that actually holds
   the 1.96M duplicates.

Hygiene noted in passing: `.env` correctly ignored, nothing secret-shaped
tracked; the stale scratch worktree (`.claude/worktrees/ljh-scratch`, branch
merged) can be `git worktree remove`d.

---

## Suggested order of attack

1. **F1** (HMRC transaction wrap — shipped with this review) and **F4**
   (periodic error-row) — each under an hour; they close the two silent
   failure modes.
2. **F2 / iteration 4 as one calibration sprint** across all three layers —
   the highest-leverage methodology item open.
3. **Provenance-payload tests + formatter unification (F3)** — cheap, and
   they protect the surface the tool's credibility rides on.
4. **E1 Phase 1** (EU-27 inclusion swap) before the next analyser family.
5. Value-adds 1–3 (heartbeat, telemetry, feedback loop) as the next product
   increment after the fixes.
