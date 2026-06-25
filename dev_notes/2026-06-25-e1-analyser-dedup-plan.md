# E1 — analyser de-duplication: shared windowing/scope primitive (plan)

**Status:** planned, not scheduled. Deferred deliberately — the point fixes
(A1, B2, A3, A2, B1) all hold; this is the structural change that makes that
*class* of bug hard to reintroduce. Pick it up when there's appetite for a
focused multi-session pass (see "When to do it"), not under deadline.

**Source:** finding E1 of
[`2026-06-25-adversarial-correctness-review.md`](2026-06-25-adversarial-correctness-review.md).
Roadmap stub: "Analyser de-duplication / shared windowing primitive" in
[`roadmap.md`](roadmap.md).

---

## 1. Problem

`anomalies.py` is ~5,300 lines with **10 analyser families** and **~18 private
window-sum / aggregation helpers**, each re-implementing the same windowed
`SUM(value_eur)` over a filtered slice of `eurostat_raw_rows` /
`hmrc_raw_rows` / `observations` / `eurostat_world_aggregates`. Because the SQL
is copied per family, every *correctness rule* is restated per site — and the
copies drift. This isn't hypothetical: **every fix in the 2026-06-25 review was
one copy of a rule that had drifted from another.**

| Correctness rule | Lives in… | Drift that bit us |
|---|---|---|
| Exclude the `000TOTAL` aggregate from CN8 sums | the hs-group LIKE queries; `detect_cn8_biggest_mover` had its own | **A3**: the primary hs-group query had no guard; a journalist `00%`/`000%` pattern would sweep ~40k aggregate rows. Fixed by baking it into `_hs_pattern_or_clause` (one home). |
| Pin GACC `currency = 'CNY'` (CNY+USD editions both ingested) | the GACC *aggregate* selectors (3×) | **B2**: `_select_gacc_export_rows` lacked it → every mirror gap computed twice, nondeterministic provenance. |
| EU-27 reporter scope | `EU27_EXCLUDE_REPORTERS` + ~8 inline `reporter <> ALL(%s)` in anomalies, **plus hardcoded copies** in `report_builder.py`, `scrape.py`, `sheets_export.py`, `briefing_pack/docx.py` | **A2**: defined by *exclusion* (`≠ GB`) not inclusion across **5 surfaces**; a stray declarant would fold into "EU-27". Cheap alert shipped; the real fix (inclusion) is deferred *here* precisely because it's 5 copies. |
| CIF/FOB excess only applies to a positive gap | `_compute_one_gap` **and** `sheets_export`'s re-derivation | **B1**: both carried the same `abs(gap_pct) - baseline`, fabricating an "excess" for negative gaps (DE −4.5%). Fixed in both copies. |
| HMRC `suppression_index = 0` | every HMRC branch | not yet bitten — but it's copied per helper, same shape. |
| `version_seen` DISTINCT-ON dedup of re-scrapes | the GACC selectors | copied with a "see X for the full rationale" comment — i.e. already known to be duplicated. |
| CN+HK+MO partner envelope (`EUROSTAT_PARTNERS`) | passed per call | mostly fine, but the default is restated everywhere. |

The pattern is clear: **a correctness rule stated N times is a rule that will be
applied N−1 times after the next edit.** The 000TOTAL bug lived as long as it did
because the fix had to be applied per-site.

## 2. Goal

State each correctness rule **exactly once**, in a place every analyser is forced
to go through, so:
- a new analyser **cannot silently omit** a rule (it composes the shared scope,
  it doesn't hand-roll SQL);
- fixing or changing a rule (e.g. the A2 exclusion→inclusion swap) is a
  **one-line change in one place**, not an N-site sweep;
- `anomalies.py` shrinks materially and the remaining per-family code is the part
  that's *actually* different (the SELECT shape and the finding emit), not the
  boilerplate scope.

Non-goal: changing any finding output. E1 is **behaviour-preserving** by
construction (with one deliberate exception, the A2 swap — which is itself
behaviour-preserving on today's data). No new findings, no supersede churn.

## 3. Design — a `WindowScope`, not a mega-function

The naïve reading ("one function for everything") is wrong: the helpers genuinely
differ in SELECT shape (per-period vs per-reporter vs top-N CN8), source table,
and measures (value + kg + the `eur_with_kg` coverage FILTER). The variation that
*should* be shared is the **WHERE clause and its params** — the scope. So:

**A `WindowScope` spec object owns every correctness predicate; thin
query-shape functions consume it.** This is the generalisation of what A3 already
did in miniature with `_hs_pattern_or_clause` (a fragment-builder with the
000TOTAL rule baked in, reused by all six callers).

```python
# Sketch — the spec owns the rules; .where() composes them once.
@dataclass(frozen=True)
class WindowScope:
    source: str                       # 'eurostat' | 'eurostat_world' | 'hmrc' | 'gacc'
    partners: tuple[str, ...] = EUROSTAT_PARTNERS
    patterns: tuple[str, ...] | None = None   # CN8 LIKE patterns (None = all-goods 000TOTAL)
    reporters: ReporterScope = ReporterScope.EU27   # EU27 | UK | EU27_PLUS_UK
    # ...

    @property
    def table(self) -> str: ...        # picks raw_rows / world_aggregates / hmrc / observations
    @property
    def kg_column(self) -> str: ...    # quantity_kg vs net_mass_kg

    def where(self) -> tuple[str, list]:
        # composes, each rule defined ONCE:
        #   partner = ANY(...)                         (partner envelope)
        #   _hs_pattern_or_clause(...)                 (000TOTAL excluded — A3)
        #   reporter = ANY(EU27_REPORTER_CODES)        (EU-27 INCLUSION — fixes A2 here)
        #   currency = 'CNY'                           (GACC pin — B2)
        #   suppression_index = 0                      (HMRC)
        ...

# Shape functions stay thin — they add only SELECT + GROUP BY:
def window_sum_per_period(scope: WindowScope, flow, *, with_obs_ids=False): ...
def window_sum_per_reporter(scope: WindowScope, flow, start, end): ...
def top_cn8s(scope: WindowScope, flow, start, end, limit): ...
```

The ~18 helpers collapse onto ~3 shape functions over one `WindowScope`. The
`abs()`/negative-gap rule (B1) and the CIF/FOB baseline lookup belong to the
mirror-gap finding builder, not the window sum, so they stay there — but as one
function, not two copies (the `sheets_export` re-derivation should call the same
helper rather than re-deriving).

## 4. Migration strategy — incremental, behaviour-locked

A 5,300-line file feeding a **live pipeline** with ~100 DB-backed tests cannot be
big-banged. The discipline:

**Phase 0 — characterization harness (do first).** Before touching anything,
build a findings-diff: run every analyser against the seeded fixtures (and,
read-only, against a snapshot of live `gacc`), serialise the emitted findings,
and store as a golden master. The refactor's gate is **byte-identical findings**
at every step. The existing suite is the floor; this catches output drift the
unit tests miss. (Re-use `gacc_test`; never write to live.)

**Phase 1 — reporter scope (unlocks A2).** Extract the EU-27 predicate into one
`EU27_REPORTER_CODES` + a `reporter = ANY(...)` builder; switch all anomalies
sites; **keep exclusion semantics first** so it's pure-refactor (suite green,
zero finding diff); commit. *Then* flip that one site exclusion→inclusion (the
A2 fix) and retire the hardcoded copies in `report_builder` / `scrape` /
`sheets_export` / `briefing_pack/docx` so all five read the one definition.
Behaviour-preserving on today's data (live reporters = the 27 + GB, `GR` not
`EL` — verified 2026-06-25).

**Phase 2 — source + partner + product into `WindowScope`.** Introduce the spec,
move table/kg-column selection, partner envelope, and the `_hs_pattern_or_clause`
call behind it. Switch the hs-group helpers (`_hs_group_per_period_totals`,
`_hs_group_top_cn8s`, `_hs_group_per_reporter_window_totals`, the world/partner
variants) to consume it. Green + zero diff per switch.

**Phase 3 — GACC selectors.** Fold the `currency='CNY'` pin and the `version_seen`
DISTINCT-ON dedup into the GACC scope; collapse `_select_gacc_export_rows` and
`_gacc_aggregate_per_period_totals` onto it. (Watch the Jan-Feb combined handling
— `_add_jan_feb_combined_to_window`.)

**Phase 4 — collapse + delete.** Merge the now-near-identical
`_eurostat_aggregate_for*` / `_*_allgoods_totals` helpers; delete the dead ones.
Measure the line drop.

**Phase 5 — single-source the mirror-gap excess.** Make `sheets_export` call the
analyser's excess helper instead of re-deriving (kills the B1-class duplication
permanently).

Each phase is its own PR, full suite green, **zero findings-diff** (except the
intentional Phase-1 A2 flip), commit-by-commit reviewable.

## 5. Risks & constraints (the things that make this non-trivial)

- **Provenance FK arrays are load-bearing.** Some helpers return
  `obs_ids` / `eurostat_raw_row_ids` for the per-number drill-down; some
  deliberately don't (chapter-wide hs-groups skip per-row ids — see the comment
  at the HS-group section header). The `with_obs_ids` flag must preserve this
  exactly; defensibility (principle 2/7) depends on it.
- **Supersede signatures must not move.** `value_fields` and `method` strings
  drive supersede. A pure refactor must leave them byte-identical or it
  mass-re-emits findings. The findings-diff harness is the guard.
- **kg-coverage nuance.** `_hs_group_per_period_totals` computes
  `eur_with_kg` (value summed only where `quantity_kg > 0`) for the unit-price
  decomposition. The shape function must keep that FILTER, and the
  `quantity_kg` vs `net_mass_kg` column differs by source.
- **Query-plan regressions.** `_hs_pattern_or_clause` is ORed LIKEs *on purpose*
  (Bitmap-OR index use; `LIKE ANY` seq-scans — see its docstring). The spec must
  emit the same index-friendly SQL; check `EXPLAIN` on the hot paths.
- **Cross-module reach.** The EU-27 duplication crosses into `report_builder`,
  `sheets_export`, `briefing_pack/docx`. Either they import the one canonical
  constant/predicate, or (cleaner long-term) they stop re-querying and read the
  analyser's output. Decide per surface.
- **HMRC vs Eurostat asymmetry.** Different tables, columns, suppression, and the
  fact HMRC ships no `000TOTAL`. The spec encodes `source`; don't paper over real
  differences.

## 6. Success criteria

- Each correctness rule in the §1 table has **exactly one definition** and **one
  test asserting it** (the test moves with the rule).
- Adding a new analyser family composes a `WindowScope` and **cannot** hand-roll
  a scope that omits a rule.
- `anomalies.py` materially smaller; the ~18 window helpers down to ~3 shapes.
- Full suite green and **zero findings-diff** across the whole migration (bar the
  one deliberate A2 flip).

## 7. When to do it (and when not)

**Do it** when: about to add another analyser family (build it on the primitive
instead of copy #11); or after any future drift scare; or as a dedicated
"correctness-hardening" sprint. There's real appetite-cost: ~100 tests to keep
green, a live pipeline, and **no user-visible feature** at the end — it's
insurance, not a feature.

**Don't** start it under deadline or interleaved with feature work — the value is
entirely in being thorough and behaviour-locked, which a rushed pass destroys.

**Cheapest partial win** if a full pass never gets scheduled: just **Phase 1**
(reporter scope → inclusion list, 5 copies → 1). That alone closes A2 properly
and removes the highest-count duplication, in one focused PR.
