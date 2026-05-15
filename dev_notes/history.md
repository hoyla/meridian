# History — addressed items

A single chronological record of significant design decisions and
problem resolutions. New entries go at the top. Each entry: what
the issue was, what shipped, where to find the work in git.

The companion `roadmap.md` lists only what's still open. This file
exists so you don't need to read through closed forward-work docs
to understand how the project got here.

---

## 2026-05-15 — Jan+Feb combined-release parser + transparency surfacing

A two-part arc Luke flagged after the morning's batch of work: the
"missing month" problem (GACC publishes Jan + Feb as a single
cumulative release each Chinese New Year, leaving the rolling-12mo
windows missing months and skewing every YoY) and then making the
handling visible to journalists once fixed. Test count 243 → 244.

### Parser + analyser handling ([`908b1f3`](https://github.com/hoyla/meridian/commit/908b1f3))

The combined release page has a narrower 7-column layout (the
"Monthly" and "YTD" columns collapse to one Jan+Feb cumulative value
per flow). New regex `_RELEASE_TITLE_JAN_FEB_RE` recognises the
"January-February YYYY (in CCY)" title shape; `extract_metadata`
returns `is_jan_feb_combined=True` and `period` anchored at Feb 1 of
the year; the body parser emits one observation per partner × flow
with `period_kind='cumulative_jan_feb'`. The release row gets
`release_kind='preliminary_jan_feb'` so the natural key on
`releases` doesn't collide with a hypothetical separate-Feb release.

Both `gacc_aggregate_yoy` and `gacc_bilateral_aggregate_yoy` learned
to treat each cumulative observation as a 2-month chunk filling the
window's Jan + Feb gap. Honest accounting via
`_add_jan_feb_combined_to_window` — the cumulative is added to the
sum once and both calendar months are marked covered, but the value
is NOT split 50/50 across Jan and Feb (interpolation would invent
per-month figures the source never asserted). Backfilled the
combined releases for 2021-2024 + 2025 from the live GACC site
(5 releases, 84-90 observations each). 2020's release filed under
section=3 (GACC numbering inconsistency) so was skipped — separate
follow-up.

**Editorial impact**: Lisa's four bilateral YoYs moved materially
because the previous post-184-fix figures had been comparing
11-month current sums against 10-month prior sums (the prior was
missing Jan + Feb), which made growth look ~15-20pp more positive
than reality:

| Finding | Before fix | After backfill |
|---|---:|---:|
| Germany imports from China (12mo) | +14.79% | -3.09% |
| Germany exports to China (12mo)   |  +2.57% | -12.66% |
| France imports from China (12mo)   | +14.65% | -2.55% |
| France exports to China (12mo)    | +10.99% |  -4.77% |

Current side still has Jan 2026 missing because 2026 broke the
combined-release pattern (separate Feb published); the analyser
doesn't yet derive Jan = `(ytd − monthly)` from that. The
remaining current-window gap means the "after" YoYs are biased
slightly low — true 12mo YoY probably sits ~5pp higher than the
post-fix figure, somewhere between the old and new readings and
closer to flat than to either extreme. Derive enhancement is
queued in roadmap.md.

### Transparency surfacing ([`5ae662c`](https://github.com/hoyla/meridian/commit/5ae662c), [`1cd147a`](https://github.com/hoyla/meridian/commit/1cd147a))

Luke's reminder: transparency is good. Surface the handling
everywhere a journalist might read.

- New `jan_feb_combined` caveat row in the `caveats` table (seeded
  in `schema.sql` + migrated to live via
  `2026-05-15-jan-feb-combined-caveat.sql`).
- Both YoY analysers now accept a `jan_feb_combined_years` list,
  write it into `detail.totals.jan_feb_combined_years`, append
  `'jan_feb_combined'` to `caveat_codes`, and add an `ℹ JAN+FEB
  CUMULATIVE USED` notice to the finding body. Method versions
  bumped (`v1` → `v2` for bilateral, `v4` → `v5` for non-EU-bloc
  aggregate) to flush the new metadata through the supersede chain.
- `briefing_pack/sections/state_of_play_{bilaterals,aggregates}.py`:
  inline annotation on each affected Tier 2 row reads
  "— partial window; includes Jan+Feb 2025 cumulative".
- `provenance.py`: new entry in the caveat glossary; `_sources_table`
  grows a "Coverage" column so the per-month source-URL chain in a
  finding's provenance file flags which rows are 2-month
  cumulatives vs single-month observations.
- `sheets_export.py`: new `gacc_bilateral_yoy` tab (spreadsheet tab
  count 9 → 10) with explicit `jan_feb_combined_years` and
  `visible_caveats` columns. Sortable / filterable.

So a journalist reading any of the bundle's surfaces — the brief
itself, the spreadsheet, or the per-finding provenance — sees what
went into the rolling-12mo total at the right level of detail for
that surface.

---

## 2026-05-14/15 — provenance system, groups glossary, bundle rename

A self-contained arc triggered by sending Lisa O'Carroll her first
real export pack. Lisa wanted full audit-trail provenance for the
findings she was about to quote; tracing the chain by hand surfaced
a data-rigor issue that propagated into a wider hardening pass. Test
count grew from 217 → 241.

### Release-184 unit-field bug + three-layer guard ([`47d1596`](https://github.com/hoyla/meridian/commit/47d1596))

While building the bespoke audit doc for Lisa's bilateral findings,
discovered that release 184 (June 2025 GACC section-4 page) had
`currency='CNY'` but `unit='USD1 Million'`, and 180 spurious v=2
observation rows. The aggregate analysers summed both readings,
inflating every GACC bilateral 12mo headline by ~0.6-0.7%. Headline
rounding wasn't affected (€98.63B → €98.01B both read as "almost
€100bn") but the audit trail wouldn't have survived close scrutiny.

Fix in three layers so this cannot recur silently:

- `parse.py` coerces unit to the canonical form when the page's
  Unit annotation disagrees with the title-derived currency, with a
  warning log.
- `db._assert_currency_unit_consistent` refuses to persist a row
  whose (currency, unit) pair isn't in `_GACC_CURRENCY_UNIT_PAIRS`.
- `schema.sql` CHECK constraint `releases_gacc_unit_consistent` as
  the final SQL-level guarantee.

Migration `2026-05-14-fix-release-184-cny-usd-unit-mismatch.sql`
corrected the data + added the constraint. Re-ran the bilateral
and non-EU-bloc aggregate analysers, producing 240+40 supersedes
per flow direction; everything else was unchanged.

### Per-finding provenance generator ([`d390568`](https://github.com/hoyla/meridian/commit/d390568) + [`cb3b121`](https://github.com/hoyla/meridian/commit/cb3b121) + [`81927ac`](https://github.com/hoyla/meridian/commit/81927ac))

`provenance.py` writes journalist-readable audit-trail files at
`provenance/finding-N.md`. Each file: source URLs (per-month GACC
release pages or Eurostat bulk-file URLs or HMRC OData query URLs),
ECB FX rates used, plain-English glosses of each caveat code,
cross-source corroboration where available, headline arithmetic
decomposition, and a fact-checker replay-SQL appendix.

Two entry points:

- CLI on-demand: `python scrape.py --finding-provenance N`.
- Bundled with `--briefing-pack --with-provenance`, restricted to
  the editorially-fresh subset (Tier 1 changes + Top-N movers +
  Top-N leads). Typically ~5-15 files per export.

Frozen-snapshot semantics: idempotent on existing files; pass
`--force` to regenerate after a methodology refresh. If the finding
is later superseded, the old file is left in place so a journalist
re-reading an earlier export sees what they read.

Detailed templates ship for: `gacc_bilateral_aggregate_yoy{,_import}`
(the first one, modelled on Lisa's hand-built doc); all six
`hs_group_yoy*` scope/flow variants; all six `hs_group_trajectory*`
scope/flow variants. Other subkinds emit a stub noting "generator
pending" with a SQL hint. Extend `provenance._RENDERERS`.

### Bundle filename numeric prefixes ([`17cbde6`](https://github.com/hoyla/meridian/commit/17cbde6))

Lisa was renaming `findings.md`/`leads.md`/`data.xlsx` to
`02_Leads.md`/`03_Findings.md`/`04_Data.xlsx` before forwarding to
the desk. Baked the convention into the generator so the rename
isn't a per-cycle chore. Order reflects Luke's framing in
`01_Read_Me_First.md` ("This is actually where I suggest you start"):

```
01_Read_Me_First.md   (template — already prefixed)
02_Leads.md           (was leads.md)
03_Findings.md        (was findings.md)
04_Data.xlsx          (was data.xlsx)
05_Groups.md          (new — see below)
```

All cross-document references inside the docs updated to match.
Standalone groups glossary keeps the un-prefixed dated form
(`exports/groups-glossary-YYYY-MM-DD.md`) since it leaves a bundle
on its own.

### HS group glossary (`05_Groups.md`) ([`3076559`](https://github.com/hoyla/meridian/commit/3076559) + [`865a8a8`](https://github.com/hoyla/meridian/commit/865a8a8))

A fifth bundle artefact: a journalist-facing reference doc explaining
every named HS group. One section per row in `hs_groups`: description,
HS LIKE patterns, top contributing CN8 codes from the most recent
active `hs_group_yoy*` finding (with a concentration warning if a
single CN8 is >80% of the group's value), and sibling groups
auto-discovered by 4-digit HS prefix overlap. Draft groups
(`created_by` starting with `draft:`) live in their own section so
journalists don't quote unvalidated figures.

Motivated by Luke's 2026-05-13 pack-review note flagging that
journalists hit a mix of obviously-coded ("Plastics — chapter 39"),
implicitly-coded ("EV batteries (Li-ion)" → HS 850760), and
apparently-uncoded ("Honey") groups in `03_Findings.md` with no
upstream reference to orient them.

Also available standalone via `python scrape.py --groups-glossary
[--out PATH]` for one-off generation between bundle runs.

---

## 2026-05-12/13 — polish for first journalist handover

A focused arc from "the supersede chain is clean" (post-v11) through
"ready to send Lisa the first real export pack". One commit per
discrete improvement; all on `main`, all pushed. Test count grew from
203 → 217.

### First-export audit + fixes ([`98363dc`](https://github.com/hoyla/meridian/commit/98363dc))

Six small editorial-readability fixes from the 2026-05-12 first-export
audit pass:

- **Tier 1 method-bump suppression** in `diff.py`: when ≥95% of
  supersede pairs are value-identical AND no material shifts surfaced,
  render a one-line "this cycle is a method-version bump, not
  editorial movement" notice naming the method transitions, instead
  of dumping a 23,207-line list of new findings.
- **Predictability badge gate**: 🟢/🟡/🔴 only render when ≥3 (scope,
  flow) permutations carry T-6 pair data (`PREDICTABILITY_MIN_PAIRS`
  in `_helpers.py`). Below that the signal is too sparse to support
  an editorial confidence cue.
- **LLM prompt fix**: filter universal caveats (`cn8_revision`,
  `cif_fob`, etc.) out of the typed-facts block in `llm_framing.py`.
  Rewrite of rule D so the LLM doesn't default to
  `cn8_reclassification`. Hypothesis-pick distribution rebalanced
  from 33/99 `cn8_reclassification` to 1/138 — `base_effect` now
  leads at 28%, grounded in real per-finding `low_base_effect`
  signal.
- **Setup hygiene**: `.env.example` updated to `OLLAMA_MODEL=qwen3.6:latest`
  (was stale `qwen2.5:14b`); README setup notes the `gacc` DB name
  retained post-rename + the `ollama pull qwen3.6:latest` step.
- **methodology.md §10 "Known editorial-output limitations"**
  documenting trajectory-volatile over-firing, LLM catalog skew,
  predictability sparseness, Tier 1 method-bump suppression,
  single-month YoY on lumpy categories, and the cross-source caveat.
- **roadmap.md**: new "Editorial calibration of
  `low_base_threshold_eur` via shock-validation backtest" section
  (deferred until a real story rests on the threshold).

### Documentation pass ([`d4a3fe0`](https://github.com/hoyla/meridian/commit/d4a3fe0))

Four shipping changes; zero docs deleted or restructured (the bones
were sound):

- **New `docs/glossary.md`** — alphabetical, three sections (economic
  & data terms, sources, system & methodology terms). 42 entries
  covering mirror trade, CIF/FOB, CN8, EU-27 scopes, the supersede
  chain, finding IDs, predictability badges, etc. Cross-linked into
  the other docs on first mention of the most-confusing terms.
- **TL;DR boxes** at the top of architecture.md, methodology.md, and
  editorial-sources.md. methodology.md's is audience-keyed.
- **`docs/README.md` rewritten** from a flat list into a real
  navigation guide with five reading paths by reader goal.
- **`/README.md` opens with output** — a real excerpted Tier 2 EV-
  batteries entry from the post-fix audit export — before
  stack/setup. Names the three design rules (LLM-free brief and
  spreadsheet; versioned findings; journalist-editable HS groups).

### leads.md Provenance block + glossary linkification ([`24454ee`](https://github.com/hoyla/meridian/commit/24454ee))

Two readability fixes after the audit read:

- **Provenance block**: per-lead trailing italic text replaced with
  a labelled `**Provenance:**` block — caveats (clickable to the
  methodology table), underlying finding IDs, trace token, one
  bullet each.
- **Linkification**: caveat codes link to methodology §3 (every
  occurrence); jargon terms (mirror gap, transshipment, CN+HK+MO,
  partner share, single-month YoY, 12mo rolling) link to the
  glossary on first occurrence per lead. Standard journalism-
  register terms (EU-27, imports, percentages) deliberately not
  linked. Links resolve to canonical GitHub URLs so leads.md works
  when shared standalone.

### Top-5 movers editorial digest ([`c4b0bb5`](https://github.com/hoyla/meridian/commit/c4b0bb5) + [`e838f85`](https://github.com/hoyla/meridian/commit/e838f85))

A journalist asked for a "top five" section so they don't have to
scroll 50 pages of leads.md looking for what's interesting. Sits at
the top of both findings.md (above Tier 1) and leads.md (above the
full per-group leads), composite-ranked across the EU-27
hs_group_yoy* family.

**Scoring rule** in `_compute_top_movers`:
- |yoy_pct| ≥ 10pp
- current_12mo_eur ≥ €100M
- low_base = False
- predictability badge ≠ 🔴
- `current_end` = latest anchor across the family (recency filter —
  caught a stale 2022 MPPT-inverters finding that would otherwise
  have ranked #1)
- Score = |yoy_pct| × log10(current_12mo_eur)

**Where it surfaces**:
- `findings.md`: numbered list above Tier 1, anchor links to Tier 2
  detail.
- `leads.md`: same picks intersected with HS groups that have an
  active `narrative_hs_group` finding (so the count may be less
  than 5; on the audit-postfix export it's 4 because EV batteries
  appears as both imports+exports in findings.md's Top 5 but
  dedups to one lead).
- `data.xlsx`: new columns. `hs_yoy_imports` / `hs_yoy_exports`
  carry `top_movers_rank` (1-5 for picks, NULL otherwise) AND
  `top_movers_score` (composite, computed on every row for long-
  tail sorting). `summary` tab carries `top_movers_rank_imp` and
  `top_movers_rank_exp` so the wide view shows at a glance.

The single source of truth (`_compute_top_movers`) drives all three
surfaces. A journalist sorting the spreadsheet by
`top_movers_score` desc gets the same ordering as `findings.md`'s
Top 5.

Companion fix in `e838f85`: new `## Full lead detail by HS group`
heading above the per-group blocks in `leads.md` — symmetric with
findings.md's "Tier 3 — Full detail by HS group", visually delimits
the full body from the Top N digest above it.

### Trajectory volatile suppression ([`3427e30`](https://github.com/hoyla/meridian/commit/3427e30))

The trajectory classifier fires `volatile` on ~68% of HS-group
series at the current anchor — the underlying data really IS noisy
at 6mo horizons (Phase 6.6 backtest), but the resulting label
carries no narrative shape the journalist can lean on. Rendering it
inline trains the reader to skim past the line.

Fix is rendering-side, not methodology: when a Tier 2 state-of-play
row's underlying trajectory shape is `volatile`, the inline
`Trajectory: …` annotation is dropped. Absence signals "no useful
shape — rely on the headline %, the predictability badge, and the
absolute figures." Non-volatile shapes (rising / falling / peak-
and-fall / u-recovery / etc.) still render inline as before.

Scope: Tier 2 ONLY. The Tier 3 trajectory-shape section in
`findings.md` still surfaces the `Volatile` bucket count for
completeness; the spreadsheet's `trajectories` tab still carries
every row (the audit use case wants the full picture even when
display heuristics suppress it).

Impact on audit-postfix export: 166 of 247 trajectory annotations
dropped; 81 useful annotations retained.

### Templates pipeline ([`a7b6953`](https://github.com/hoyla/meridian/commit/a7b6953))

Every file dropped into `briefing_pack/templates/` is copied
verbatim into every export folder by `export()` — filename and all.
The templates dir's own `README.md` is excluded (it's documentation
for the dir itself, not template content).

The intended file is `01_Read_Me_First.md` — a per-cycle
orientation piece. The leading `01_` numeric prefix is a deliberate
sort-first trick: most file viewers list it above `findings.md`,
`leads.md`, and `data.xlsx`, so it's the first thing a journalist
sees when opening the folder.

Workflow: Luke edits the template in place between exports if a
cycle's framing needs to differ. Commit if persistent, leave
uncommitted for one-off tweaks.

Implementation: `_copy_export_templates(dest_dir)` in `render.py`
is idempotent and tolerant — a missing or empty `templates/`
directory is a no-op. The copy runs after findings.md/leads.md/
data.xlsx are written.

### Where this leaves us

Test suite: 217 passing (was 203 before the arc started). The
audit-postfix export at `exports/audit-postfix/` is the regenerated
substrate for Luke's read-as-Lisa pass before handover. Two audit
items remain deliberately deferred (read-as-Lisa pass; editorial
calibration of `low_base_threshold_eur`).

---

## 2026-05-12 (post-v11) — orphan hs_group findings supersede + regression guard

The Phase 6.11 v11 re-emit pass made visible a pre-existing pollution
problem: findings under HS groups that had been deleted or renamed
during Phase 6.5 (2026-05-10) were still active in the live DB and
ranked above real stories in any top-movers query — most painfully the
"Lithium chemicals (carbonate + hydroxide)" findings carrying +2554%
YoY at 2025 anchors.

### Scope of the stale set

Two failure modes, three groups affected:

- **Deleted**: `Pharmaceutical APIs (broad)` was dropped during
  Phase 6.5 in favour of three narrower groups (Paracetamol-class
  amides, Ibuprofen-class monocarboxylic acids, Antibiotics). 526
  active `hs_group_yoy*` + 5 active `hs_group_trajectory*` findings
  remained with `hs_group_ids` pointing at a now-deleted row.

- **Renamed in place** (same `hs_groups.id`, different name): two
  cases.
  - `Lithium chemicals (carbonate + hydroxide)` → `Lithium hydroxide
    (battery-grade)` with the pattern set tightened to HS 282520 only.
    111 yoy + 2 trajectory findings still carrying the old name in
    `detail.group.name`. Critically, these did NOT collide with the
    new findings on natural key — the new narrower patterns produce
    findings for a different set of (anchor) periods, so both streams
    coexisted as "active" until this cleanup.
  - `Plastic waste` → `Plastic waste (post-National-Sword residual)`.
    Yoy findings were already superseded by the v11 pass (same
    anchor periods produce same natural keys); 5 trajectory findings
    lingered.

Total: 651 stale findings across 6 yoy subkinds and 6 trajectory
subkinds.

### Cleanup

Single-shot transactional supersede using a name-equality criterion
that catches both deletion and rename:

```sql
UPDATE findings f
   SET superseded_at = now()
 WHERE f.superseded_at IS NULL
   AND (f.subkind LIKE 'hs_group_yoy%' OR f.subkind LIKE 'hs_group_trajectory%')
   AND array_length(f.hs_group_ids, 1) > 0
   AND NOT EXISTS (
       SELECT 1 FROM hs_groups g
        WHERE g.id = ANY(f.hs_group_ids)
          AND g.name = f.detail->'group'->>'name'
   );
```

No `superseded_by_finding_id` set — these are retirements, not
revisions; there's no successor.

### Editorial impact

Top-movers query for `hs_group_yoy` at anchors ≥ 2025-09 now reads:

| group | yoy_pct |
|---|---:|
| Wind generating sets only | +405% |
| Civil aircraft (HS 8802) | +213% |
| Gadolinium/terbium/dysprosium compounds (CN8 28469060) | +208% |

All three are legitimate editorial stories (Tan article narrow wind
sub-group, Soapbox A1 civil aircraft, Soapbox A1 "blue bucket" heavy
rare earths). The +2554% Lithium chemicals / +330% Pharma APIs noise
is gone.

### Regression guard

New live-DB-gated test
[`tests/test_orphan_findings.py`](../tests/test_orphan_findings.py)
asserts no active hs_group_yoy* / hs_group_trajectory* finding
references an `hs_groups` row whose name no longer matches. Skipped
when `GACC_LIVE_DATABASE_URL` isn't set, same pattern as
`test_eurostat_scale_reconciliation.py`. The test re-prints the
cleanup query in its docstring so a future failure walks the next
developer straight to the fix.

### Forward-work consideration

A more defensive analyser pattern would have `detect_hs_group_yoy`
sweep for orphans of its own subkind family at the start of each
run — "any active finding whose hs_group_id is missing from
hs_groups gets superseded before the new pass begins". Not done in
this commit because:

1. The natural-key supersede chain already handles the common case
   where a group's patterns evolve but its identity persists.
2. The deletion case is rare (Phase 6.5 was a one-off; the project's
   stable state is "groups grow, don't shrink").
3. The regression guard catches recurrence at test time — the
   feedback loop is fast.

If this pattern recurs (more renames or deletions), promote the
sweep into the analyser. Documented here so future sessions don't
need to re-derive the trade-off.

---

## 2026-05-12 (later) — per-reporter hs_group breakdown (Phase 6.11)

`hs_group_yoy*` findings now carry a top-10 per-reporter breakdown
ranked by absolute EUR delta, with current/prior/delta/YoY plus
`share_of_group_delta_pct` (the reporter's contribution to the
group's overall move, in percentage-point form). Closes the
Soapbox A4.5 / A5.6 gaps surfaced by the 2026-05-11 validation
pass:

- **A5.6**: "Germany car-parts -11.1% / to CN -20.1%; Germany alone
  accounts for ~66% of EU-wide drop." `share_of_group_delta_pct`
  surfaces the 66% directly.
- **A4.5**: "Germany dragged Jan 2026 export drop, offset by FR/IT/PL."
  The ranked breakdown shows DE pushing the group direction at a
  large share while FR/IT/PL push back at smaller opposite-sign shares.

### Shape

- New analyser helper `_hs_group_per_reporter_window_totals(patterns,
  start, end, flow, partners, source)` sums `(value_eur, quantity_kg)`
  per reporter for one window; the existing `_hs_group_top_reporters`
  is dropped (the new helper, called twice per anchor, is a strict
  information superset).
- New helper `_build_per_reporter_breakdown(...)` merges current and
  prior window dicts, computes per-reporter `delta_eur`, `yoy_pct`,
  `yoy_pct_kg`, and `share_of_group_delta_pct`, sorts by
  `abs(delta_eur)`, caps at 10.
- In the finding `detail`, `top_reporters_in_current_12mo` is replaced
  with `per_reporter_breakdown`. The shape is a strict superset; no
  downstream consumer was reading the old field (grepped briefing_pack,
  sheets_export, llm_framing, tests — only the analyser test asserted
  on it).
- Method version: `hs_group_yoy_v10_single_month_and_two_month_cumulative`
  → `hs_group_yoy_v11_per_reporter_breakdown`. Supersede chain
  propagates the bump across all 14,800-ish active findings on the
  next periodic-run.

### Brief render

`briefing_pack/sections/hs_yoy_movers.py` queries the new field and
renders a `**Reporter contributions**` sub-bullet under each Tier-3
mover, top 5 reporters per group. Zero-delta entries filtered out
so single-reporter `_uk*` scopes stay quiet. Share is rendered as
`+200% of group's Δ` (DE drove twice the group's drop) or
`-100% of group's Δ` (FR pushed back at the size of the group's
own move). The deliberate "Δ" + percentage rendering is the same
register Soapbox uses.

### Spreadsheet

New 9th tab `hs_yoy_reporter_movers` — long-format, one row per
(group, scope, flow, reporter, period), ranked by `|delta_eur|`.
Each row carries the originating `finding_id` so the spreadsheet
pairs one-to-many with `hs_yoy_imports` / `hs_yoy_exports`. Tab
roster docstring in `sheets_export.assemble_sheets` updated.

### Tests

- `tests/test_hs_groups.py::test_yoy_per_reporter_breakdown_attributes_group_delta`
  — multi-reporter seed (DE drives the drop, FR pushes against),
  asserts per-reporter delta / YoY / share + the cross-check
  invariant that reporter deltas sum to the group delta.
- `tests/test_briefing_pack.py::test_reporter_contributions_block_renders_under_mover`
  — seeds a breakdown into a finding's detail, asserts the
  "Reporter contributions" sub-block + DE/FR YoY% + share strings
  render.
- `tests/test_hs_groups.py::test_yoy_records_per_month_series_and_top_reporters`
  — existing test updated to assert on the new `per_reporter_breakdown`
  shape (current_eur / prior_eur / yoy_pct /
  share_of_group_delta_pct).
- `tests/test_sheets_export.py::test_export_produces_xlsx_with_all_tabs`
  — expected tab set bumped to 9.
- Test suite at this commit: 204 passing (+2 vs the 2026-05-12
  morning state).

### Editorial discipline

Per-reporter sums are queryable from raw rows today; the change is
**surfacing** the per-reporter contribution as a named finding-grade
artefact so a journalist can quote "Germany alone explains 66% of
the drop" with a citable finding ID rather than ad-hoc SQL. The
universal caveats already applied to `hs_group_yoy*` (CIF/FOB,
classification drift, CN8 revisions, etc.) carry through unchanged
— they apply equally to per-reporter sums of the same underlying
rows.

---

## 2026-05-12 — Soapbox A1 re-test → four-step feature pass

A re-test of Soapbox A1 ("China's export surge puts EU trade defence
in the spotlight", 2026-05-11) against the live DB confirmed every
numerical claim the tool *could* check (no contradictions), and
surfaced the **shape** of the gap precisely: Soapbox's reporting is
share-and-bilateral-aggregate-heavy; ours was growth-and-per-HS-
group-heavy. Closed the gap with four sequential commits — cheapest
to most-impactful per hour. Stage B/C results captured in
[`soapbox-validation-2026-05-11.md`](soapbox-validation-2026-05-11.md).

### Step 1 — Thirteen new `hs_groups` ([`5fa011d`](https://github.com/hoyla/meridian/commit/5fa011d), [`b4357ed`](https://github.com/hoyla/meridian/commit/b4357ed))

`seed:soapbox_a1_2026_05_12`. Spot-check of latest hs_group_yoy
findings at `current_end=2026-02`, scope=eu_27, flow=1 (CN→EU
imports), `partial_window=false`:

**Chemicals / feed / pharma-adjacent inputs** (the article's "less
visible inputs" cluster):

- **Amino acids (HS 2922)** — id=36. 12mo to 2026-02: €968M,
  -28.4% YoY value / -22.9% kg.
- **Adipic acid (HS 291712)** — id=37. €74M, -43.4% / -31.9%.
  (Roadmap originally proposed 291713 which is sebacic/azelaic;
  corrected to 291712 during the per-code data verification.)
- **Choline (HS 292310)** — id=38. €8M, -50.8% / -70.8% (low_base).
- **Vanillin and ethylvanillin (HS 29124100 + 29124200)** —
  id=39. €42M, -46.9% / -42.5% (low_base).
- **Feed premixes (HS 230990)** — id=40. €210M, -36.4% / -30.1%.
- **Inorganic acids (HS 2811)** — id=41. €171M, +5.0% / +28.3%.
- **Aldehyde/ketone acids (HS 2918)** — id=42. €663M, +0.4% / +12.1%.

**Rare-earth sub-buckets** (post-2023 EU CN8 split of HS 284690;
element labels per Eurostat 2024 CN8 nomenclature):

- **Lanthanum compounds (CN8 28469040)** — id=43. €5M, +5.2% /
  +7.4% (low_base). Soapbox's "dark-red bucket".
- **Praseodymium/neodymium/samarium compounds (CN8 28469050)** —
  id=44. €2M, +25.6% / +12.8% (low_base). Nd is the magnet element.
- **Gadolinium/terbium/dysprosium compounds (CN8 28469060)** —
  id=45. €11M, **+149.6% value** / -21.9% kg (low_base). The
  article's "blue bucket"; price-per-kg surging.
- **Europium/holmium/erbium/thulium/ytterbium/lutetium/yttrium
  compounds (CN8 28469070)** — id=46. €16M, +85.0% / +67.5%.

**Tier 2 article-exposed gaps**:

- **MPPT solar inverters (CN8 85044084)** — id=47. Skipped with
  `insufficient_history` (code separated by EU from 1 Jan 2026;
  24mo window needs ~mid-2027).
- **Civil aircraft (HS 8802)** — id=48. €1.16B, +57.7% / +26.7%.
- **Photovoltaic inverters (CN8 85044086)** — id=49 (added during
  step 4 when the partner_share validation showed broad HS 850440
  diluting the article's solar-inverter claim).

**Dropped from Tier 1**:

- **Crude oil (HS 2709)** — CN→EU 2025 ≈ €0 (China is not a crude
  exporter to EU). The article's Libya story is China-as-importer,
  outside our `partner ∈ {CN, HK, MO}` ingest.
- **Central Asia `country_aliases` row** — GACC section 4 rolls
  KZ/UZ/KG/TJ/TM into Belt & Road, not individually.

`b4357ed` followed up to tighten the descriptions: stripped Soapbox
share figures (which would prime LLM hallucination as facts not in
the typed-facts block) and clarified broad-chapter codes (HS 2922,
230990, 2811, 2918, 8802) so the model doesn't over-specify
findings. Same hallucination class as the 93%-permanent-magnets
case from Phase 3.

### Step 2 — `briefing_pack.py` modularisation ([`4da7eca`](https://github.com/hoyla/meridian/commit/4da7eca), [`5457e21`](https://github.com/hoyla/meridian/commit/5457e21))

The 2,001-line `briefing_pack.py` became a `briefing_pack/` package:

- `_helpers.py` — DB connection, citation tokens, formatters,
  predictability + threshold-fragility logic, scope labels,
  `_ALL_UNIVERSAL_CAVEATS` (derived from
  `anomalies.UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY`).
- `sections/*.py` — one module per `_section_*` builder. 14 files
  ranging 27 lines (`detail_opener.py`) to 172 lines
  (`hs_yoy_movers.py`).
- `render.py` — `render()` / `render_leads()` / `export()` plus
  `latest_eurostat_period()` / `latest_recorded_data_period()`
  for periodic-run idempotency.
- `__init__.py` — re-export shim so external callers (scrape.py,
  periodic.py, sheets_export.py, tests) see no API change.

Zero behaviour change — test suite passes 200/200 identical to
pre-refactor. Done before steps 3+4 so new analyser sections land
as new files in `sections/` rather than appends to a monolith.

### Step 3 — `gacc_bilateral_aggregate_yoy` analyser ([`c8ea9c3`](https://github.com/hoyla/meridian/commit/c8ea9c3))

`gacc_aggregate_yoy` deliberately excluded `eu_bloc` ("mirror-trade
handles EU"). The A1 re-test confirmed this left a real editorial
gap: Soapbox's USD top-lines ("$201B in Jan-Apr 2026, +19% YoY")
aren't the same finding as a bilateral mirror gap, and Lisa quotes
them directly.

**Shape**: rather than separate subkinds per (period_kind, flow),
one finding per (partner, anchor_period, flow) carries three YoY
operators side-by-side in `detail.totals`. Mirrors the Phase 6.10
design on `hs_group_yoy`:

- `yoy_pct` — 12mo rolling (the primary, drives `score` + supersede)
- `ytd_cumulative.yoy_pct` — Jan-to-anchor of current year vs prior
  year. Null when prior-year YTD is missing.
- `single_month.yoy_pct` — anchor month vs same month prior year.
  Null when prior month is missing.

Coverage: EU bloc + all single-country GACC partners
(`country_aliases.aggregate_kind = 'eu_bloc' OR IS NULL`). The
existing `gacc_aggregate_yoy` keeps its non-EU-multi-country scope
unchanged. Subkinds: `gacc_bilateral_aggregate_yoy[_import]`.
Method version: `gacc_bilateral_aggregate_yoy_v1_eu_and_single_countries`.
Wired into periodic. Brief renders a new "GACC bilateral partners"
Tier-2 block (`state_of_play_bilaterals.py`).

**First run**: 2,664 findings across 22 partners × 2 flows × ~30
valid anchor periods. EU export YTD through 2026-04 = +18.2% to
€175.04B (≈$200B at period FX) vs Soapbox's +19% to $201bn — within
rounding + FX-rate-source noise.

### Step 4 — `partner_share` analyser + extra-EU world aggregates ([`956e9db`](https://github.com/hoyla/meridian/commit/956e9db))

Soapbox's most distinctive analytical move is the "China share of
EU imports by qty vs value" pattern. Every claim of the form
"China supplied X% of EU imports of Y" was unverifiable from our
DB because `eurostat.py` filters at ingest to `partner ∈ {CN, HK,
MO}` — China-side numerator only, no denominator.

**Implementation**. New table `eurostat_world_aggregates` stores
pre-summed extra-EU partner totals per (period, reporter, product_nc,
flow) for the HS prefixes our hs_groups track. The aggregator
(`eurostat.aggregate_to_world_totals`) excludes the 27 EU-27
ISO-2 partner codes (`eurostat.EU27_PARTNER_CODES`) so the
denominator matches the editorial register "share of imports from
*outside* the EU". Including intra-EU would conflate "share of EU
consumption" with "share of non-EU imports" — Soapbox always means
the latter.

The discovery during validation: the bulk files contain only ISO-2
country codes, no pre-computed `EXTRA_EU27_2020` or `WORLD`
aggregate codes. So "rest of world" has to be computed by summing
across 246 partner codes minus the 27 EU-27 codes. Aggregator runs
in a single streaming pass over the bulk file (~11s per period).
Backfill: 26 months (2024-01 → 2026-02), 77k–94k aggregate rows
per period, ~2.2M total rows.

**Analyser** `detect_partner_share` emits one finding per
(hs_group, anchor_period, flow) under subkind
`partner_share[_export]`. `detail.totals` carries `share_value`,
`share_kg`, and `qty_minus_value_pp` — the Soapbox "bigger in
tonnes than in euros" signal. Natural key
`(hs_group_id, current_end_yyyymm)`. Method version
`partner_share_v1_eurostat_world_aggregates`. New universal caveat
`extra_eu_definitional_drift` documents that the 246-partner
denominator sums customs authorities with slight definitional
drift across CN8 revisions.

**Brief section**: `briefing_pack/sections/partner_share.py` —
Tier-3 block listing groups by share descending, with both shares
+ the gap + a brief framing note (>+5pp = unit-price pressure;
>-5pp = premium pricing; between = noise band).

**Validation** vs Soapbox A1. The article cites "China supplied
87% of EU solar inverter imports by quantity in 2025, compared
with 75% by value" — for the narrow PV-specific CN8 sub-code
85044086 (added as a new hs_group during this step) our finding at
12mo-to-2025-12 shows **80% value / 91% qty**, gap +10.7pp. Within
±5pp validation band on the absolute figures; the qty-over-value
gap shape matches exactly.

**Initial bug, then fix**: the first version of the aggregator
included intra-EU trade in the denominator. Solar inverter share
came out at 27% / 38% (vs Soapbox's 75% / 87%) because intra-EU
trade — German imports from the Netherlands etc. — dwarfs extra-EU
trade for most chapters. Fix: filter out the 27 EU-27 ISO codes at
aggregation time. Truncate + re-backfill. The mistake was useful as
a sanity check on the editorial framing (we'd have shipped a
defensible-but-wrong-for-the-question number had Lisa quoted us
without reference to Soapbox).

**Coverage limitation**: EU-27 scope only. UK and combined scopes
would need an HMRC-side world aggregate (HMRC ingest stores
GB+CN/HK/MO only) — forward work.

### Coverage-extension follow-up — `gacc_aggregate_yoy` v4 ([`605bf0d`](https://github.com/hoyla/meridian/commit/605bf0d))

Mirrors step 3's pattern on the existing non-EU aggregate analyser:
ASEAN / RCEP / Belt&Road / Africa / Latin America / world Total
findings now carry `ytd_cumulative` and `single_month` sub-fields
in `detail.totals` alongside the 12mo rolling. Method bumped
`v3_per_alias_natural_key` → `v4_ytd_and_single_month_operators`.
The Tier-2 partner-aggregate brief block surfaces all three
operators at once. Re-using `_gacc_partner_ytd_by_period` from
step 3 (same `observations.partner_country` semantics for both
single-country and multi-country aggregates) made this a clean
shared-code extension, not a copy-paste.

First run on live DB: 464 supersedes (every existing aggregate
finding bumped to v4). Spot-check at 2026-04 anchor:
- ASEAN exports — 12mo +17.5%, YTD Jan-Apr +18.2%, Apr +13.5%
- Africa exports — 12mo +32.3%, YTD Jan-Apr +27.3%, Apr +15.5%

Soapbox A3-style "China-X Jan-N trade +Y%" and single-month
"Feb 2026 -16.2%" claims now have finding-grade citation rather
than raw-row queries.

### Cumulative test-suite impact

200 → 202 passing across the four steps + coverage-follow-up.
Three new tests (one per analyser family added); existing tests
updated for the universal-caveats refactor, the CN+HK+MO hardcode,
the gacc_aggregate_yoy method-version bump, and one CN8 typo
correction (291712 vs 291713).

### Companion refactor: family-universal caveats + CN+HK+MO hardcode ([`434b592`](https://github.com/hoyla/meridian/commit/434b592))

Two cuts from a code review that landed alongside the A1 work:

- Universal caveats are no longer attached to per-finding
  `caveat_codes`. They live once in
  `anomalies.UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY` and render in the
  brief's new Methodology footer (moved from header position).
  Per-finding caveat_codes now carries only what varies between
  findings of the same family. The old `SUPPRESSED_INLINE_CAVEATS`
  filter is gone.
- `--eurostat-partners` CLI knob removed; `EUROSTAT_PARTNERS` is
  now a fixed `(CN, HK, MO)` tuple. `multi_partner_sum` becomes
  universal for the affected families. For a CN-only spot check,
  query `eurostat_raw_rows` directly.

---

## 2026-05-11 (late evening) — bug fixes surfaced by the first periodic-run

The first end-to-end periodic-run cycle surfaced three real bugs.
Pre-release, with no journalist depending on existing data, we went
destructive: code fix + delete affected rows + re-emit cleanly. No
audit chain preserved for the buggy data — it had no editorial value.

### Bug 1 — `nk_gacc_aggregate_yoy` natural-key collision (Africa silently overwritten)

The natural key was `(aggregate_kind, current_end_yyyymm)`. Two
aggregates share `aggregate_kind='region'` — **Africa** and
**Latin America**. So every analyser run, Africa's row would be
superseded by Latin America's (alphabetically later) or vice
versa, depending on iteration order. The supersede chain showed
this as 160+ paired entries with opposite-sign YoYs at the same
period.

Pre-fix DB state: **228 superseded Africa rows, 0 active Africa
rows**. Africa GACC aggregate data was completely invisible in
active findings — overwritten by LatAm on every run.

Fix: added `alias_id` (the stable `country_aliases.id`) to the
natural key in
[`findings_io.nk_gacc_aggregate_yoy`](../findings_io.py).
Updated the call site in
[`anomalies.py`](../anomalies.py). Method version bumped
`gacc_aggregate_yoy_v2_loose_partial_window` →
`gacc_aggregate_yoy_v3_per_alias_natural_key`.

Cleanup: `DELETE FROM findings WHERE subkind LIKE
'gacc_aggregate_yoy%'` (684 rows: 342 active LatAm-stomping +
342 historically-superseded Africa). Re-ran the analyser. New
state: 114 active findings each for Africa, LatAm, ASEAN, Total
(456 total across both flows). Africa is editorially visible
again.

Regression test added (`test_gacc_aggregate_yoy_distinguishes_aggregates_of_same_kind`):
seeds Africa + LatAm with distinct values, asserts both produce
their own active findings post-analyser.

### Bug 2 — Tier 1 diff shows "None" for aggregate findings

`_section_diff_since_last_brief` read `detail.group.name` to
label each superseded finding. `gacc_aggregate_yoy*` findings
store their label under `detail.aggregate.raw_label` (because
they're aggregate-keyed, not hs-group-keyed). So every aggregate
row showed literal "None" as the group name.

Fix: `COALESCE(detail.group.name, detail.aggregate.raw_label)`
in the diff query. One-line change in
[`briefing_pack.py`](../briefing_pack.py).

### Bug 3 — `USD1 Million` unit-string regex mismatch

`_UNIT_RE` required whitespace between the ISO-4217 currency
code and the multiplier digit:
`^([A-Z]{3})(?:\s+(\d+...))?...`. GACC release pages use BOTH
formats:

```
CNY 100 Million   ← has space, matched
USD1 Million      ← no space, REFUSED — silently dropped half the GACC dataset
```

The unit parser's docstring explicitly says it returns
`(None, None)` for unrecognised forms — and the analyser logs
ERROR + skips the row. So USD-side GACC data has been silently
dropped from the gacc_aggregate analyser. (CNY-side was always
preferred when both present; USD was the fallback path.)

Fix: `\s+` → `\s*` in the currency/multiplier separator.
Two test cases added to `test_unit_scale_parses_known_forms`
covering `'USD1 Million'` and `'CNY100 Million'`.

After the regex fix the analyser only logs a separate INFO
message (no FX rate for USD/EUR on 2025-06-01) — a real but
pre-existing data-coverage gap, not a parser bug. Loading USD/EUR
FX rates is forward work.

### Sanity audit

Ran a query over the whole `findings` table looking for any
(subkind, natural_key_hash) tuple with more than one active row.
**No collisions** — the gacc_aggregate fix was the only one
hiding. All other natural keys are correctly per-instance.

### Tests + state

203 passing tests (+ 3 in the new regression cases). Live DB has
456 active gacc_aggregate findings (was 342 before, with Africa
missing). No method-version-bumped findings under broken keys
remain. Findings table is a clean baseline for the first real
periodic-run cycle when the next Eurostat release lands.

### Aggregate state-of-play + single-month YoY + Routine deployed

Three planned follow-ups landed together (commit
[`00bff29`](https://github.com/hoyla/meridian/commit/00bff29) and the
Routine creation via the scheduled-tasks MCP):

- **Tier 2 aggregate state-of-play block.** New
  `_section_state_of_play_aggregates` renders one block per GACC
  partner aggregate (ASEAN / Africa / Latin America / world Total)
  alongside the existing per-HS-group block. Closes the "Africa is
  invisible" surfacing gap from the bug-fix sweep — the data was
  always there (after the natural-key fix) but no Tier 2 block
  read it. Now journalist reads `findings.md` and sees the bloc
  context alongside the EU-CN per-group view.
- **Single-month + 2-month-cumulative YoY** added as sub-fields on
  every `hs_group_yoy` finding (`detail.totals.single_month` +
  `detail.totals.two_month_cumulative`). Method version
  `v9_comparison_scope` → `v10_single_month_and_two_month_cumulative`.
  Editorial register: Soapbox / Lisa routinely quote "Feb 2026 vs
  Feb 2025" (single-month) or "Jan-Feb 2026 vs Jan-Feb 2025"
  (2-month cumulative) rather than 12mo rolling. The Tier 2 render
  now shows both: `+1.4% (kg +7.9%) to €732.4M (12mo to 2026-02-01).
  Latest month: +11.9% (kg +19.2%)` — for the NdFeB sub-group, the
  acceleration is the story. Graceful degradation for findings
  still on older method versions. Currently 3 of 35 hs_groups
  carry v10 (the seed:soapbox_validation sub-CN8 groups, re-run
  today to validate); the rest upgrade automatically on the next
  `--periodic-run`.
- **Claude Code Routine deployed.** `gacc-daily-periodic-run`,
  cron `0 9 * * *` (09:01 local time daily — jitter ~40 sec). The
  Routine fetches the next candidate Eurostat / HMRC period if it
  might be available, runs `python scrape.py --periodic-run`
  (idempotent, no-op on most days), and surfaces a 3-5 line Tier 1
  summary back to chat if a new export was written. Luke handles
  delivery to Lisa manually (Layer 3). Migration to the desktop
  later: pipeline portable via `git clone` + `pg_dump | pg_restore`;
  routine follows the account (per the schedule MCP setup).

Phase 6.9 (periodic-runs pipeline) is now end-to-end live.

### Follow-up sweep: FX coverage + Natural graphite + state-of-play check

After the three bug fixes landed:

- **USD/EUR FX rates fetched** (`scrape.py --fetch-fx USD --fx-since 2017-01`):
  112 monthly rates inserted (2017-01 → 2026-04). Re-ran
  gacc-aggregate-yoy: 232 emitted per flow (up from 228 before
  the FX load), confirming 4 anchors per flow were previously
  being skipped on USD/EUR conversion. Still get an INFO log
  for one 2025-06-01 'Total' row — separate data quality issue
  worth chasing if anyone reaches into the GACC USD-side feed
  again. Live DB now has 464 active gacc_aggregate findings.
- **Natural graphite (HS 250410)** added as a new hs_group
  (id=35, `seed:soapbox_validation`). Closes the third remaining
  Soapbox-validation sub-CN8 group recommendation (MPPT will land
  when 2026-01-onward has enough history; rare-earth narrow needs
  CN8-code research before adding). EU-27 imports 12mo to 2026-02:
  **-45% value, -27.8% kg** (low base €30.7M). Editorial direction
  matches Soapbox A2.3 ("EU graphite imports from CN fell 22%
  Jan-Feb 2026"); precise single-month figure still needs the
  single-month-YoY operator (open forward work).
- **State-of-play render verified post-fix**: regenerated
  findings.md with `--briefing-pack --no-record`. Tier 1 now
  shows only the expected 456 new gacc_aggregate findings (no
  spurious paired supersedes); Tier 2 / Tier 3 unchanged. Tier 1
  diff label rendering ready to surface real names for any future
  aggregate supersedes (the COALESCE was the load-bearing fix).
- **Gap surfaced for forward work**: gacc_aggregate_yoy* findings
  are NOT currently rendered in Tier 2 or Tier 3 sections (only
  Tier 1 picks them up when they change). A dedicated state-of-
  play block for non-EU bloc aggregates (ASEAN, Latin America,
  Africa, Total) belongs in Tier 2 alongside the hs_group blocks.

---

## 2026-05-11 (evening) — periodic-run pipeline (Phase 6.9)

The "what's new since last time" loop that was Phase 6.8 sketched out
plus the deployment-agnostic orchestration the roadmap had as its #1
near-term item. Shipped as a three-layer separation: pipeline (CLI),
scheduler (pluggable: Claude Code Routine today, hosted cron later),
delivery (manual today).

### Three-tier findings document structure

Commit [`abd07ec`](https://github.com/hoyla/meridian/commit/abd07ec) (with
wording follow-up [`c85bfb6`](https://github.com/hoyla/meridian/commit/c85bfb6)).
Background reasoning: a findings export at time T+1 mostly repeats one
at time T — every 12mo-rolling window shifts by a month but most YoY
values barely move. Rendering a full snapshot every cycle is
repetitive. The fix is to structure the document so a regular reader
gets a small "what's new" lead, while a new joiner still gets the
orienting compact summary plus the full detail.

Three explicit tiers in `findings.md`:

- **Tier 1 — What's new this cycle**. The diff against the previous
  export (was "Changes since the previous export"; same content, now
  prominently labelled and at the top under a horizontal-rule
  divider). Includes a "first findings export" baseline message and a
  "nothing material has changed" message so the tier is always
  rendered, even on no-op cycles.
- **Tier 2 — Current state of play** (new). One block per HS group;
  inside, one compact line per (scope, flow) with latest 12mo YoY
  (value + kg), current 12mo EUR, trajectory shape, low-base /
  partial-window flags, and the `finding/N` trace token.
  Predictability badges inline. The persistent picture between cycles.
- **Tier 3 — Full detail by HS group**. The existing per-scope mover
  sections, trajectory shape buckets, mirror gaps, low-base review —
  unchanged in content but with section headings demoted from `##` to
  `###` (and per-group `###` to `####`) so they nest under the Tier 3
  parent.

A reader's-guide section right after the headline names the three
tiers so it's obvious which section serves which mode of use.

### Schema: `brief_runs` gains `data_period` + `trigger`

Live DB ALTERed and `schema.sql` updated. Both columns are required
infrastructure for the idempotency logic:

- `data_period DATE` — the most recent Eurostat `releases.period` at
  the time of the render. Stamped onto every `brief_runs` row.
- `trigger TEXT NOT NULL DEFAULT 'manual'` — distinguishes manual
  ad-hoc renders from periodic-run cycle outputs. Only the latter
  participate in the global subscriber-facing cycle.

The table name `brief_runs` is retained from the pre-rename era (per
the `brief.md → findings.md` decision in commit `73a7f71` — module
and table internals stay, only reader-facing prose changes).

### New `periodic.py` module + `--periodic-run` CLI

`periodic.run_periodic()` is the deployment-agnostic pipeline
entrypoint. It:

1. Idempotency-checks: compares `latest_eurostat_period()` against
   `latest_recorded_data_period(trigger='periodic_run')`. Exits
   cleanly with a no-op if the latter is no older than the former
   (unless `--force`).
2. Re-runs every analyser kind across all scope/flow combos
   (`mirror_trade`, `mirror_gap_trends`, six `hs_group_yoy*`, six
   `hs_group_trajectory*`, two `gacc_aggregate_yoy*`,
   `llm_framing`). Each is per-row idempotent via the supersede
   chain.
3. Generates the bundled findings export with
   `trigger='periodic_run'`.

CLI: `python scrape.py --periodic-run [--force] [--skip-llm]
[--export-dir PATH]`. Prints the absolute path of the new
`findings.md` to stdout (empty string on no-op) so a scheduler
wrapper can branch on it.

The orchestrator is deliberately non-fetching — it operates on
whatever is in the DB. The scheduler is expected to invoke
`python scrape.py --eurostat-period YYYY-MM` separately before
the periodic-run call. Keeps network failure (fetch) and analyser
failure as distinct concerns.

5 unit tests in `tests/test_periodic.py` cover the helper
functions and the two no-op paths (empty DB; already-published).
196 + 5 = 201 tests pass.

### Three-layer deployment design

Captured in [`periodic-runs-design-2026-05-11.md`](periodic-runs-design-2026-05-11.md).
Key idea: the pipeline (Layer 1, this repo), the scheduler (Layer
2, Routine / cron / GHA — pluggable), and the delivery channel
(Layer 3, manual / email / Slack — pluggable) are three independent
concerns. Layer 1 is built; Layers 2 and 3 are wrappers around it.
Migration from "Claude Code Routine on Luke's laptop" to "hosted
cron on AWS" later is a wrapper swap, not a code change.

Routine prompt for v1 (laptop / desktop, manual delivery to Lisa)
is in the design doc.

Commit [`(this commit)`].

---

## 2026-05-11 — Soapbox validation pass + two follow-up hs_groups

A peer-comparison audit modelled on the shock-validation discipline
(predict, then look). Output: [`soapbox-validation-2026-05-11.md`](soapbox-validation-2026-05-11.md).

### Stage A — pre-registration

Picked 10 Soapbox Trade articles spanning 2024-06 → 2026-05 (recent
~10 plus topic-matched back-catalogue), extracted 50 testable claims
with their natural-key tuples (`hs_group`, `current_end`, `flow`,
`scope`, `partners`), and pre-registered Expected magnitudes under
both `CN`-only (Soapbox convention) and `CN+HK+MO` (our default).
17 idea-generation claims (categories not in our hs_groups) flagged
with provisional HS codes for future hs_group proposals.

### Stage B — comparison

Ran the validation against the live DB the same day. Headline:

- **57% clean concur** on testable claims (within ±5pp on YoY, ±10%
  on EUR levels).
- **80% directional concur** if grading on direction + order of
  magnitude.
- Strongest single-paragraph result: A3.1, A3.2, A3.3 — three EXACT
  single-month YoY matches in one article (Soapbox Apr 20: EU
  exports to CN Feb 2026 −16.2%, EU imports +2.2%, Jan −5.1% /
  Jan-Feb −11%; ours: −16.2% / +2.2% / −5.1% / −11.0%).
- Other clean concurs: pork exports to China (−10/−11%),
  finished-car exports (−41.6% vs −43%), motor-vehicle-parts
  exports to CN (€-2.03B vs −€2.01B), 2025 BEV imports
  (−45.6% vs −43%), HK imports +7% vs +9%.
- **Gap surfaced**: every aggregate Soapbox quotes (EU-CN imports,
  exports, deficit at single-month + annual cadence) reproduces
  cleanly from raw rows but **doesn't surface as a named finding** —
  `gacc_aggregate_yoy` excludes `eu_bloc` by design (see
  [`anomalies.py:2435`](../anomalies.py) — "mirror-trade handles EU"),
  and there's no Eurostat-side aggregate analyser at all.

### Stage B follow-up #3 — pork+offal + NdFeB sub-CN8

Acted on the cheapest forward-work items the same afternoon. Two
new hs_groups added in [`schema.sql`](../schema.sql) and the live
DB, commit [`91354b3`](https://github.com/hoyla/meridian/commit/91354b3):

- **id=33 Pork offal (HS 0206 swine)** — patterns
  `020630% / 020641% / 020649%`, `created_by='seed:soapbox_validation'`.
  Soapbox cites pork meat and offal separately (A6.5: "meat -11%,
  offal -3%"); before this commit only meat was a named finding.
  New `hs_group_yoy_export` at 2025-12 reads **−2.9% kg** (Soapbox:
  −3% — within ±0.1pp).
- **id=34 Sintered NdFeB magnets (CN8 85051110)** — patterns
  `85051110%`, same `created_by`. Narrower than the broad HS-8505
  Permanent magnets group. Broad-chapter kg YoY at 2026-02 was
  +1.4%; the NdFeB sub-code alone is **+7.9%** at 12mo rolling
  (single-period Jan-Feb +18% matches Soapbox to the pp, but
  needs a single-month-YoY operator the tool doesn't yet emit).
  **Closes the Phase 3 LLM hallucination loop**: qwen3.6 previously
  invented "China supplies 93% of permanent magnets" from training
  data because no NdFeB-specific typed facts were in the prompt;
  the verifier correctly rejected it. With the new sub-group, the
  next `--analyse llm-framing` run produces a verified narrative
  drawing on typed facts (+7.9% kg, −6.0% unit prices,
  `u_recovery` trajectory) without reaching into training data.
  0 verification rejections on the same-day run.

356 new findings emitted across 6 scope×flow combos
(`hs_group_yoy*`, `hs_group_trajectory*`, `narrative_hs_group`).
No existing findings superseded, no method versions bumped — pure
coverage extension. Tests: 196 passed.

### Stage B follow-up #6 — §5.4 snapshot refresh

Commit [`38940ce`](https://github.com/hoyla/meridian/commit/38940ce).
The annual aggregate table in
[`shock-validation-2026-05-09.md:404-415`](shock-validation-2026-05-09.md)
pre-dated the 2026-05-10 `000TOTAL`-mystery resolution (commit
`50f8dbd`) and double-counted by ~2x. Refreshed using the canonical
`product_nc='000TOTAL'` aggregate row. **Sanity-check vs Lisa
O'Carroll's "€360bn 2025 surplus" cite**: our refreshed CN-only
2025 EU-side deficit reads **€360.0B — to the percentage point**.
The HK/MO inclusion adds ~€20B (multi_partner_sum caveat); the
CIF/FOB inflation explains the rest of the gap to GACC's FOB
USD-equivalent figure. Numbers now concur cleanly with the
methodology choices explicit.

### Pre-existing artefact surfaced (forward-work)

While refreshing §5.4, surfaced that **2017 has duplicate `000TOTAL`
rows** from the pre-v2 COMEXT bulk-file format (per-stat_procedure
`000TOTAL` counts: sp=1 has 648 rows for 2017 vs 351 for 2018).
This is independent of the 000TOTAL filter rule and predates
today's work. Flagged in the refreshed §5.4 table; no analyser
output affected (HS LIKE filters never matched aggregate rows).
Forward work to dedupe or re-ingest 2017.

### What's now closed vs still open

**Closed by this session:** Soapbox-validation forward-work #3
(pork+offal split into two groups), #6 (§5.4 snapshot refresh),
and the partial half of #2 (NdFeB sub-CN8 added; MPPT, graphite,
rare-earth narrow remain).

**Still open** (now in [`roadmap.md`](roadmap.md)): periodic
analyser runs (#1 on the roadmap, single biggest unlock); the
remaining sub-CN8 groups (MPPT 85044084, graphite 250410,
rare-earth narrow); single-month YoY operator; `eu_bloc`
aggregate analyser (design discussion needed — explicit exclusion
in `anomalies.py:2435`); per-reporter rollup; GACC sec 5/6
ingest; 2017 pre-v2-format dedup.

---

## 2026-05-10 (evening) — output-shape refactor and transparency annotations

A focused session on what the journalist actually opens. No new
analysers; no methodology change; the data layer is unchanged. The
work re-shapes the output bundle and surfaces methodology safeguards
where readers can see them.

### Transparency annotations in the findings document and spreadsheet

Three editorial signals that previously sat only in dev_notes
reports now appear inline next to the findings they qualify:

- **Per-group YoY-predictability badge** (🟢 / 🟡 / 🔴) next to
  each HS group heading, computed via the same logic as the Phase
  6.6 backtest (T vs T-6 across all (scope, flow) permutations).
  ≥67% persistent → 🟢; 33–67% → 🟡; <33% → 🔴. Includes a one-line
  rationale for 🔴 ("Lean on trajectory shape; hedge any % quoted
  from this group").
- **Threshold-fragility annotation** (⚖️) for findings whose
  smaller-of-(curr, prior) sits within 1.5× the low_base threshold,
  above OR below. A finding at €48M (low_base) and one at €52M
  (not low_base) are equally fragile to a small threshold move; the
  annotation surfaces that without making editorial claims.
- **Per-finding CIF/FOB baseline display** in the mirror-gap
  section: the per-(partner) OECD ITIC baseline plus the
  excess-over-baseline-pp split. Was sitting in `detail` since the
  ITIC backfill but not surfaced.

Commit [`314962f`](https://github.com/hoyla/meridian/commit/314962f).
Helpers `is_threshold_fragile()` and `_compute_predictability_per_group()`
shared between briefing_pack and sheets_export so both render
paths use the same definition.

### LLM leads split out of the findings document

The "no LLM in the loop" framing on the original brief had become
inaccurate once the Phase 6.4 lead-scaffold layer landed: leads
were rendering inside the brief alongside deterministic findings.
For a NotebookLM-style downstream LLM tool that mixed bundle
created a telephone-game effect (the tool ends up reasoning over
another LLM's interpretation, not over the data).

Split into two paired files: brief stays fully deterministic; LLM
lead scaffolds move into a separate companion document. Both share
the same finding IDs; cross-references explicit.

Commit [`acb8697`](https://github.com/hoyla/meridian/commit/acb8697).
Diff section ("Changes since previous brief" → "Changes since
previous export") now also excludes `narrative_hs_group` since
those don't appear in the brief.

### Per-export folder convention + scope label

Replaced timestamped flat files (`brief-YYYYMMDD-HHMMSS.md`) with
per-export folders containing stable filenames:

```
exports/2026-05-10-1747[-slug]/
  findings.md
  leads.md
  data.xlsx
```

Pairs are self-evident from the folder; consumers find the bundle
by convention. Optional `scope_label` parameter (default None)
slugifies into the folder suffix and surfaces in both docs' headers
as a "*Scope: …*" line, so a doc shared standalone still announces
what slice of the data it covers. Currently metadata only — the
filtering logic is forward work; the naming convention is in place
so scoped exports can land cleanly when needed.

Commit [`4c3da25`](https://github.com/hoyla/meridian/commit/4c3da25). New
CLI flags: `--export-dir PATH` and `--export-scope LABEL`.

### Spreadsheet refresh — three-artefact bundle

The spreadsheet had drifted on multiple axes (UK / combined scopes
absent, no per-country CIF/FOB column, no predictability badge, no
threshold-fragility flag). Refreshed all eight tabs to match the
current methodology and added a NEW `predictability_index` tab.
`briefing_pack.export()` now drops `data.xlsx` into the per-export
folder by default so all three artefacts share a single DB
snapshot.

Commit [`c1ed375`](https://github.com/hoyla/meridian/commit/c1ed375).
Tab roster (8): summary, hs_yoy_imports, hs_yoy_exports,
trajectories, mirror_gaps, mirror_gap_movers, low_base_review,
predictability_index. The narrative_hs_group findings are
intentionally NOT in any tab (same telephone-game argument as the
findings document).

### Endnote on `finding/N` citations

Both docs now end with the same shared endnote explaining what
`finding/N` citations mean — what a finding is, the supersede chain
(so a citation is reproducible even after numbers later move), how
to look one up today (direct DB query), and pointers to
`docs/methodology.md` + `docs/architecture.md` for deeper context.

Commit [`4c3da25`](https://github.com/hoyla/meridian/commit/4c3da25).

### "In this export folder" block

Replaced the prose cross-references between findings.md and
leads.md with a structured block in each, listing all three
artefacts (with the current one marked "(this document)"). The
spreadsheet is now visible from the Markdown side too.

Commit [`49e9c64`](https://github.com/hoyla/meridian/commit/49e9c64).

### `brief.md` → `findings.md` rename

The output filename was misleading: the file is comprehensive, not
brief. Renamed to `findings.md` to match what's actually in it (a
render of the `findings` table) and to pair cleanly with
`data.xlsx`. H1 changed from "GACC × Eurostat trade briefing" to
"GACC × Eurostat trade findings". Module name `briefing_pack.py`
and CLI flag `--briefing-pack` kept (the *bundle* is still a
briefing pack — the rename is just the deterministic document
inside it).

Commit [`73a7f71`](https://github.com/hoyla/meridian/commit/73a7f71).

---

## 2026-05-10 — Phase 6 closeouts and autonomous methodology block

A long working session that closed Phase 6 except for the
infrastructure-track item (periodic analyser runs).

### 6.1 — HMRC ingest + comparison_scope abstraction

UK trade data was structurally missing from the brief because
Eurostat dropped UK reporting after Brexit. Shipped HMRC OTS
ingest via OData REST API at `https://api.uktradeinfo.com`, plus a
`comparison_scope` parameter (eu_27 / uk / eu_27_plus_uk) on the
hs-group analysers. Briefing pack restructured for per-scope
sections. 3.9M HMRC raw rows backfilled 2017–2026; UK numbers
cross-checked against HMRC published headlines.

Commits `9489970` → `0cb91bf`. (Closes the original
`forward-work-uk-data-gap.md`.)

### 6.2 — Universal-caveat suppression in the brief

Eleven caveats fired on essentially every active finding
(`cif_fob`, `classification_drift`, `cn8_revision`,
`currency_timing`, `eurostat_stat_procedure_mix`, `multi_partner_sum`,
`general_vs_special_trade`, `transshipment`, `cross_source_sum`,
`aggregate_composition_drift`, `llm_drafted`). They cluttered
per-finding caveat lists and obscured the *unusual* caveats
(`partial_window`, `low_base_effect`, `low_baseline_n`,
`low_kg_coverage`, `transshipment_hub`). Now suppressed inline and
explained once in a top-of-brief "Universal caveats" section that
reads canonical text from the `caveats` schema table.

Side-effect: surfaced two missing schema definitions
(`aggregate_composition_drift`, `cross_source_sum`) which had been
emitted by analysers but never had `caveats` table entries; both
seeded.

Commit [`6765afa`](https://github.com/hoyla/meridian/commit/6765afa).

### 6.3 — Methodology sensitivity sweep

Pure-compute pass over active findings that replays
classifications under variant thresholds. Three findings:

- **`low_base_threshold_eur` (default €50M)** is HIGHLY sensitive:
  ~7,100 findings (49%) flip classification across the €5M–€500M
  range. The single largest methodology-choice driver of editorial
  framing.
- **`kg_coverage_threshold` (default 0.80)** is INSENSITIVE in
  production: 84% of findings sit at 0.90–1.00 coverage, 15.7% at
  exactly 1.0; only 7 findings in the 0.80–0.90 band. The gate is
  defensive against a failure mode that doesn't currently trigger.
- **`z_threshold` (default 1.5)** is MODERATELY sensitive: 18 of
  74 mirror_gap_zscore findings sit within ±0.3 of the default.

Script: `scripts/sensitivity_sweep.py`.
Report: `dev_notes/sensitivity-sweep-2026-05-10.md` (kept as
dated artefact). Commit [`85d6cf7`](https://github.com/hoyla/meridian/commit/85d6cf7).

### 6.4 — Lead-scaffold restructure of LLM framing

Replaced the v1 narrative-drafting prompt ("write a 2-3 sentence
top-line") with a structured lead-scaffolding shape: per HS group
the LLM produces (a) one-line anomaly summary, (b) 2-3 hypothesis
ids picked from a curated catalog with one-line rationales, (c)
deterministic corroboration steps drawn from the picked catalog
entries.

The catalog (`hypothesis_catalog.py`) seeds 12 standard causes for
China-EU/UK trade movements — tariff_preloading,
capacity_expansion_china, eu_demand_pull, transshipment_reroute,
russia_substitution, currency_effect, friend_shoring_decline,
trade_defence_outcome, cn8_reclassification, base_effect,
energy_transition, post_pandemic_normalisation. Verifier
discipline carries through unchanged. Method: `llm_topline_v2_lead_scaffold`.

Follow-up [`f301342`](https://github.com/hoyla/meridian/commit/f301342) adds an HS-code regex strip
to the verifier so groups whose names embed HS codes (e.g.
"Antibiotics (HS 2941)") don't trigger false-positive failures
when the LLM cites the code in a rationale.

Commits [`f624108`](https://github.com/hoyla/meridian/commit/f624108) + [`f301342`](https://github.com/hoyla/meridian/commit/f301342).

### 6.5 — Sector breadth review

Thirteen draft HS groups were proposed in Phase 5; a review pass
classified each as promote / keep-draft / drop-or-rework. After
user approval:

- **Promoted (7)**: Critical minerals, Drones, PPE, Semicon mfg
  eqpt, Telecoms base stations, Cotton, Tomato paste.
- **Stayed draft (3)**: Honey, Polysilicon, Tropical timber.
- **Lithium chemicals → Lithium hydroxide (battery-grade)**:
  scope shrunk to HS 282520 only (the cell-grade chemical with
  the cleaner EV-supply-chain story).
- **Pharmaceutical APIs (broad) dropped + replaced** by three
  narrower groups: Paracetamol-class amides (HS 2924),
  Ibuprofen-class monocarboxylic acids (HS 2916), Antibiotics (HS
  2941). The broad group's +215.6% YoY at €8.11B base was
  unmistakably an artefact of HS 2942 being a catch-all that
  includes non-APIs.
- **Plastic waste renamed** "Plastic waste (post-National-Sword
  residual)" so the historical-only intent is explicit.

Commits [`1b3cdf8`](https://github.com/hoyla/meridian/commit/1b3cdf8) (proposal) + the user-approved
group revisions in the live DB.

### 6.6 — Out-of-sample backtest of YoY signal stability

Pure-compute script that compares each `hs_group_yoy*` finding at
T (2026-02) against the same (group, subkind) at T-6 (2025-08).
Headlines: 31% of YoY signals sign-flip across 6 months; 43%
shift by ≥5pp same-sign; only 26% are persistent. Per-group
predictability ranges 100% (broad chapter groups like Electrical
84-85) to 0% (Telecoms / Pharma niche groups, Industrial
fasteners, etc.).

Crucial caveat captured prominently in the report: this is NOT a
clean out-of-sample forecast test (rolling windows share data);
it IS a YoY-framing-stability test. The result is that
YoY-on-rolling-windows is genuinely less stable than it looks —
groups with low persistence should rely on the trajectory shape,
not the headline percentage.

Trajectory-shape backtest sketched but flagged as forward work:
all current findings have `created_at = today` from the Phase 5
clean-state rebuild, so the supersede chain isn't a historical
record yet. Becomes measurable once analyser runs are scheduled
periodically — see `roadmap.md`.

Script: `scripts/out_of_sample_backtest.py`.
Report: `dev_notes/out-of-sample-backtest-2026-05-10.md` (kept as
dated artefact). Commit [`5d0e23e`](https://github.com/hoyla/meridian/commit/5d0e23e).

### 6.7 — GACC 2018 parser (partial)

Title parser fixed to handle four 2018-format quirks:
alternative wording ("by Major Country (Region)"), trailing
period after month abbreviation ("Jan." not "Jan"), missing
`(in CCY)` suffix, and missing date entirely. Fix plumbs
`expected_currency` and `expected_period` from the discovery side
through to the parser.

But: 2018 section-4 release pages embed PNG screenshots
(`<img src='Excel/4-RMB.png'>`) instead of HTML tables, so the
body parse still fails. The data is in pixels, not numbers.
Editorial cost is bounded (only 2018 mirror-trade is missing;
hs-group analyses use Eurostat which extends to 2017).

Forward work options (OCR, hunt for source xlsx, accept gap, lean
on Eurostat+HMRC) captured in
`dev_notes/forward-work-gacc-2018-parser.md` — kept open because
this is genuinely deferred, not closed.

Commit [`3f115b4`](https://github.com/hoyla/meridian/commit/3f115b4).

### 6.8 — Brief versioning ("Changes since previous brief")

New `brief_runs` table tracks brief generation timestamps. The
brief now opens with a "Changes since previous brief" section
listing findings with `created_at > prev_at` (new) or
`superseded_at > prev_at` (revised). Material YoY shifts (>5pp)
highlighted; direction flips (sign change) marked 🔄.

Foundation for the journalist workflow piece — they want to know
what's changed since they last looked, not re-read the whole
brief.

Commit [`1267362`](https://github.com/hoyla/meridian/commit/1267362).

### Eurostat aggregate-scale 2x mystery — RESOLVED

Original symptom: direct sums over `eurostat_raw_rows` for
sanity-checking ran ~2x Eurostat's published EU-27 totals (€998B
vs published ~€517B for 2024 imports from CN). Per-country
numbers were roughly right; the factor only inflated as we summed
across reporters.

Cause: Eurostat's bulk file ships, per (reporter, period, partner,
flow, stat_procedure), a `product_nc='000TOTAL'` aggregate row
that sums the per-CN8-detail rows for the same slice. Naïve
`SUM(value_eur)` includes both = ~2x. CN8-only sum across all
EU-27 reporters and all stat_procedures for 2024 = **€517.1B**,
matches Eurostat's published headline exactly.

**HS-group analysers were never affected** because they all apply
HS-pattern LIKE filters (`'8507%'`, `'85%'`, etc.) that don't
match `'000TOTAL'`. Editorial impact: zero. The "X-suffix" codes
(`'85XXXXXX'`, `'850610XX'`) in the bulk file are confidentiality
residuals, not aggregates — including them in HS-pattern LIKE-
matched sums is correct.

Code change: new `EUROSTAT_AGGREGATE_PRODUCT_NC` constant in
`anomalies.py` documenting the convention; new
`tests/test_eurostat_scale_reconciliation.py` (3 tests, opt-in to
live DB) guards against regression.

Commit [`50f8dbd`](https://github.com/hoyla/meridian/commit/50f8dbd).

### Per-country CIF/FOB baselines from OECD ITIC

Replaced the 7.5% global default in `cif_fob_baselines` with 28
per-(EU member state, China) values sourced from OECD's
International Transport and Insurance Costs of merchandise trade
(ITIC) dataset, 2022. Range: 3.15% (SK) → 7.79% (BG); unweighted
mean 6.65%. Northwest-European core (DE 6.50%, NL 6.55%, FR
7.22%, IT 7.00%, BE 7.01%) clusters around 6.5–7.2%. The 7.5%
global default is preserved as fallback for non-EU partners.

Method bumped: `mirror_trade_v4_multi_partner_default` →
`mirror_trade_v5_per_country_cif_fob_baselines`. The mirror_gap
value_signature now includes `cif_fob_baseline_pct` so future
baseline updates propagate via the supersede chain without
needing a method-version bump. 351 mirror_gap findings re-emitted.

Sourced reference kept at
`dev_notes/cif-fob-baselines-2026-05-10.md` for reproducibility.
Commit [`4d4f7cc`](https://github.com/hoyla/meridian/commit/4d4f7cc).

---

## 2026-05-09 — Phase 5: methodology audit + clean-state rebuild

Phase 5 was triggered by a strategic review of whether the tool
was genuinely surfacing newsworthy insights vs converging on a
self-consistent loop with confirmation bias. Six concerns
surfaced; this phase fully addressed three (HK/MO routing,
historical baseline depth, validation methodology) and partially
addressed two (sector breadth, threshold robustness).

### 5.1 — HK/MO partner inclusion

Eurostat reports goods routed via Hong Kong / Macau under
partner=HK / partner=MO rather than partner=CN (~15% of China's
exports to EU). New constant
`EUROSTAT_PARTNERS_DEFAULT = ('CN', 'HK', 'MO')` adopted by all
four analysers as the default. CLI override `--eurostat-partners CN`
available for the narrower direct-China view. The
`multi_partner_sum` caveat fires by default as honest annotation.

### 5.2 — `upsert_observations` partner-scoped fast path

The bulk-insert fast path keyed off "release_id has any rows" —
adding HK/MO observations to existing CN-only releases fell into
the per-row slow path, taking ~6 minutes per period instead of
~1 second. Scoped the freshness check by `partner_country` for a
~280x speedup on partner-additive ingest.

### 5.3 — GACC parser: historical title formats

GACC release titles in 2018 and earlier had divergences from the
2025/2026-tuned regex (no `(N)` prefix, "RMB" synonym for CNY,
"Only August" parenthetical). Fixed for the formats seen in the
2018 monthly summary releases.

### 5.4 — Historical Eurostat + GACC backfill

- **Eurostat**: 2017-01 → 2026-02 (110 periods, partners CN+HK+MO).
- **GACC**: walks 9 yearly indexes (preliminary.html for current
  year + preliminaryYYYY.html for 2018–2025); section 4 parses
  cleanly across all years after the parser fix. (2018 still
  blocked at the body level — see Phase 6.7 above.)

### 5.5 — Clean-state rebuild

Wiped the live DB, re-applied `schema.sql` from scratch, archived
`migrations/` (folded into `schema.sql`; preserved as
`migrations.archived-2026-05-09/` for the dev history), and
re-ingested everything with the new defaults.

### 5.6 — Pre-registered shock validation

Document `dev_notes/shock-validation-2026-05-09.md` —
pre-registered expectations for what the analysers should
surface across four known historical shocks (2018 Section 232
tariffs, Q1 2020 COVID lockdown, Feb 2022 Russia invasion → renewables
substitution, Oct 2023 EU EV anti-subsidy probe). Written **before**
running the analysers; Results sections filled in afterwards.
The discipline is the structural defence against the
confirmation-bias risk. **This document is kept** in dev_notes/
because the methodology has ongoing value.

### 6.0.5 — EU-27 means EU-27 at all times

Pre-Brexit UK reporter rows (2017–Q1 2020) were inflating EU-27
sums. Three SQL helpers in `anomalies.py` now filter
`reporter <> ALL(EU27_EXCLUDE_REPORTERS)` where
`EU27_EXCLUDE_REPORTERS = ('GB',)`. Method bumps:
`hs_group_yoy v7→v8_excludes_gb_reporter_pre_brexit`,
`hs_group_trajectory v5→v6_inherits_eu27_yoy`. 4596 supersedes
triggered; 1144 findings (25%) had YoY shifts > 5pp; **337 had
the YoY direction flip** ("growth" ↔ "decline"). Worst examples
in 2018-2019 aluminium and electrical machinery — old EU-28 sums
showed +25–30% growth, new EU-27 shows -20–30% decline. Commit
[`388be73`](https://github.com/hoyla/meridian/commit/388be73).

### 6.0.6 — ~10000x analyser speedup

A re-run of all hs-group-yoy findings was projected to take ~3
hours because each per-anchor query was doing a Parallel Seq Scan
on the 17.5M-row `eurostat_raw_rows` table. Two changes dropped
the planner's estimated cost from ~476725 to ~41:

1. New covering index `idx_eu_raw_analyser` on
   `(flow, partner, product_nc text_pattern_ops, period)` INCLUDE
   `(value_eur, quantity_kg, reporter)`.
2. New helper `_hs_pattern_or_clause(patterns)` rewrites
   `product_nc LIKE ANY(%s)` (which the planner refuses to push
   down through the text_pattern_ops btree) into separate ORed
   LIKEs that the planner happily turns into a BitmapOr.

Wall time dropped from ~40 minutes (mid-run, ~40%) to ~7 minutes
full chain. Commit [`70d7bc5`](https://github.com/hoyla/meridian/commit/70d7bc5).

### 6.0.7 — Trajectory tolerates gaps

Phase 1.7's all-or-nothing gap rejection was producing only
~5/58 expected trajectory findings on real data. Replaced with
longest-contiguous-run: find the longest unbroken sub-series and
classify on that. The chosen window is recorded in
`features.{effective_first_period, effective_last_period,
original_series_length, effective_series_length,
dropped_periods_due_to_gaps}`. The `TRAJECTORY_MIN_WINDOWS = 6`
safeguard still rejects too-short remnants. Coverage went from 5
to 57 trajectory findings. The EV trajectory now classifies as
`dip_recovery` with trough at 2024-08, exactly when EU duties bit
hardest. Commit [`13f5ea1`](https://github.com/hoyla/meridian/commit/13f5ea1).

---

## 2026-05-09 — Phases 1-3: roadmap delivery

Triggered by an analysis-assumptions review identifying eight
per-pass concerns and two cross-cutting issues across the four
anomaly passes (`mirror-trade`, `mirror-gap-trends`,
`hs-group-yoy`, `hs-group-trajectory`). The review document is
preserved as the planning record (now consolidated below); each
phase shipped its scope.

### Phase 1 — Rigour fixes (7 items)

1. **Idempotent findings with revision history** (cross-cut).
   Append-plus-supersede chain on findings, mirroring how
   observations are versioned. New columns: `superseded_at`,
   `superseded_by_finding_id`, `last_confirmed_at`,
   `natural_key_hash`, `value_signature`. Per-subkind natural keys
   in `findings_io.py`. Default queries filter
   `WHERE superseded_at IS NULL`.
2. **Unit-scale parse failure → hard skip** (mirror-trade). The
   fallback multiplier 1.0 with WARNING was risking 10⁴-off EUR
   values for unrecognised unit strings. Now treated as a skip
   with ERROR log.
3. **Theil-Sen slope replaces OLS** (hs-group-trajectory). Robust
   to endpoint outliers. Live impact: Solar PV cells & modules
   trajectory flipped from `falling` → `falling_decelerating` (the
   decline is slowing, not just continuing).
4. **`min_baseline_n=6` confidence threshold + `low_baseline_n`
   caveat** (mirror-gap-trends). Hard floor stays 3
   (mathematical minimum); confidence threshold 6 triggers the
   caveat. "Make the noise honest" rather than dropping early-
   period signal.
5. **kg-coverage metric → conditional decomposition**
   (hs-group-yoy). `kg_coverage_pct` computed; below 80% the
   volume/price decomposition is suppressed and a
   `low_kg_coverage` caveat fires.
6. **Configurable `--low-base-threshold`** (hs-group-yoy). CLI
   flag accepting EUR, default unchanged at €50M.
7. **Trajectory gap detection** (hs-group-trajectory). Before
   classifying, check the YoY series for period gaps (later
   refined in 6.0.7 to longest-contiguous-run rather than skip).

Commits `8f18e68` → `3dc4c72`. Tests 75 → 101.

### Phase 2 — Editorial framing (8 items)

1. **Transshipment-hub flag** (mirror-trade). New
   `transshipment_hubs` table seeded with NL, BE, HK, SG, AE, MX,
   each with a citable `evidence_url`. The mirror-trade analyser
   auto-attaches a `transshipment_hub` caveat when the partner is
   in the table.
2. **CIF/FOB baselines table — lighter version** (mirror-trade).
   New `cif_fob_baselines` table with global default 7.5% (later
   superseded per-country by the 2026-05-10 OECD ITIC backfill).
3. **Multi-partner Eurostat support** (cross-cut). Default
   unchanged at the time of Phase 2 (`['CN']`); fully promoted to
   default in Phase 5.1.
4. **Configurable trajectory smoothing** (hs-group-trajectory).
   `--smooth-window N` flag, default 3.
5. **Seasonality as a feature, not a shape**
   (hs-group-trajectory). `_autocorrelation_at_lag()` helper
   computes a detrended Pearson correlation; surfaces as
   `features.seasonal_signal_strength`.
6. **Staleness log line** (mirror-gap-trends). Before running,
   warns if the latest mirror_gap finding period is older than
   the latest available Eurostat or GACC release.
7. **Single-missing-month tolerance** (hs-group-yoy). Allow up to
   1 missing month across both 12mo windows; sum what's present;
   never interpolate. `partial_window` caveat attached when
   triggered.
8. **Blanket `cn8_revision` caveat for cross-year-boundary
   windows** (hs-group-yoy). Auto-applied to any YoY window
   spanning a calendar-year boundary.

Commits `c0aa48c` → `26b2c94`. Tests 101 → 114.

### Phase 3 — LLM framing layer v1

`llm_framing.py` v1: per-HS-group narratives with strict numeric
verification. Default backend Ollama, default model
`qwen3.6:latest`. Every number cited had to round-trip to a fact
within tolerance, or the narrative was rejected. v1 shipped 15/16
hs_groups producing verified narratives on first pass; one
hallucination (qwen3.6 cited "93%" for permanent magnets,
recalled from training data) correctly rejected.

**Subsequently restructured** into the lead-scaffold shape in
Phase 6.4 (above) — replaces narrative drafting with structured
hypothesis selection from a curated catalog.

Commit `e3766c7`. Tests 114 → 129.
