# Adversarial correctness & robustness review

**Date:** 2026-06-25
**Scope:** whole system, with emphasis on modelling, assumptions, regex/structure-
derived parsing, source-data drift over time, architecture and layering.
**Method:** read the four `docs/`, the relevant `dev_notes/`, ran the live GACC
parser against the 2018 / Jan-Feb 2025 / Mar 2026 fixtures, and traced the
analysers, FX, natural-key chain, and Eurostat aggregation paths through the
code. Stored project memory was deliberately not used, to avoid ratifying past
decisions without re-deriving them.

**Relationship to the prior review:** `2026-05-12-review-2.txt` is a
*simplification* critique ("you over-built this"). This document is a different
question — *correctness and robustness* ("where is it actually wrong, or one
upstream change away from being silently wrong"). The two don't overlap much.

> [!NOTE]
> Items A1 & A2 addressed in https://github.com/hoyla/meridian/pull/106


---

## Headline

The data-correctness core is in better shape than the "it grew and grew"
history suggests. I went looking for more instances of the bug classes that have
bitten the project before (the 000TOTAL ~2× double-count; the Africa/LatAm
natural-key collision) and the systematic ones are genuinely closed.

The real exposure is elsewhere: **robustness to the source data changing under
you**, a couple of **modelling/labelling errors in the headline mirror-gap
product**, and a cluster of **"correct today, silently wrong after the next
upstream change" traps**. Findings below are tiered by how likely each is to put
a wrong number in front of a journalist without anyone noticing.

---

## Verified strengths (what holds up)

- **000TOTAL is clean.** All 13 Eurostat aggregation sites correctly read either
  the `000TOTAL` row alone or digit-prefix detail (a pattern like `8507%`
  provably cannot match the string `000TOTAL`). There is a regression test
  (`tests/test_eurostat_scale_reconciliation.py`). The newer families
  (`trade_balance`, `partner_share`, `china_all_goods_share`) were built right
  from the start.
- **No GACC partner double-count.** Every GACC finding is scoped to exactly one
  alias; nothing sums a bloc with its members or a parent with its "of which:"
  subset. `_gacc_aggregate_per_period_totals` hard-filters
  `o.partner_country = %s` (one label) and each finding keys on a single
  `alias_id`.
- **The natural-key / supersede chain is collision-safe.** `subkind` is folded
  into `natural_key_hash` (`findings_io._stable_hash("nk", subkind, *identity)`),
  floats are rounded before they enter the value signature, parts are
  NUL-delimited so field boundaries can't bleed, and the partial unique index
  matches the app-level key.
- **The raw → observations → findings layering with FK arrays back to source
  rows is the genuinely sound part** — the defensibility guarantee is real and
  cheap.
- **FX is skip-not-guess.** A missing ECB rate returns `None` →
  `_compute_one_gap` returns `skipped_no_fx`; no stale/invented rate is used.

For the *already-discovered* failure modes, the system works. The findings below
are about the failure modes it hasn't hit yet.

---

## Tier A — silent miscount / drift-robustness (a wrong number nobody catches)

### A1. GACC ingest has no parse-completeness guard — a format change records "success" with zero data

The single most important finding: GACC is the most fragile source (HTML parsed
structurally) yet has the *weakest* failure detection of the three.

In `scrape.py:85-133`, on a parse that yields `[]` observations the GACC path:

- still calls `find_or_create_gacc_release` (line 122) → **creates the release row**;
- `upsert_observations(run_id, release_id, [])` (line 123) → inserts nothing;
- `finish_run(status="success")` (line 125) → **records success**.

Contrast Eurostat (`scrape.py:245`) and HMRC (`scrape.py:396`), which both return
`IngestOutcome(status="empty")` *without* creating a release row. GACC — the one
that needs it most — lacks the guard.

The parser's row detector is purely structural: it keeps only `<tr>`s with
*exactly* 10 cells (or exactly 7 for Jan-Feb), skipping everything else
(`parse.py:394-398`). Verified against the fixtures: 2026 is 10-col, Jan-Feb 2025
is 7-col — but the layout is chosen *solely from the title regex*
(`is_jan_feb_combined`). So if GACC adds one column (a new YoY variant, a new
flow split), **every data row becomes 11 cells, all are skipped, and you get zero
observations recorded as a clean success.** Because a release row *is* created,
the overdue-release alerting (#77, which keys off release presence) sees "new
data" and never fires. Downstream YoY analysers then see a missing month →
`partial_window` → biased YoY — the exact failure mode the Jan-Feb fix addressed,
but triggered silently by format drift.

**Cheap fix:** assert a plausible floor on a section-4 parse (≈30 partner rows ×
3 flows × 2 kinds ≈ 180 obs; at minimum the "Total" row must be found); on
failure record `status="failed"` (or `empty`, no release row) rather than
success.

### A2. "EU-27" is defined by exclusion (`reporter != 'GB'`), not inclusion of the 27 members

`anomalies.py:186`: `EU27_EXCLUDE_REPORTERS = ("GB",)`, applied everywhere as
`reporter <> ALL(...)`. Meanwhile the *partner* side already has the correct
shape — an explicit 27-code inclusion set, `eurostat.py:258-262`
(`EU27_PARTNER_CODES`).

This is the reporter-side analog of the 000TOTAL risk. At whole-period ingest
`reporters=None`, so **every reporter code Eurostat ships is stored verbatim**
(`eurostat.py:157-165`), and the analysers then trust that the only non-EU-27
reporter ever present is GB. If Eurostat ever ships an aggregate declarant row
(an `EU`/`EU27_2020`-style reporter) you would **double-count** (aggregate +
members); if it ships a new member, candidate country, or special territory, it
is silently folded into "EU-27." This is an upstream-change-over-time hazard of
the same class as bugs that have already hit the project, and the fix is nearly
free — switch the reporter scope to an inclusion list (reuse the 27-code set one
module over), or at least assert the stored reporter set ⊆ {27} ∪ {GB} and alert
on surprises.

### A3. `hs_groups` patterns are journalist-editable with zero validation, and the primary query has no `000TOTAL` backstop

The docs sell "add a row to `hs_groups` and the next run produces findings; no
code change required" and "editable by journalists who don't want to read
Python." But `hs_patterns TEXT[] NOT NULL` has **no CHECK constraint**
(`schema.sql:264`), and the primary YoY query `_hs_group_per_period_totals`
relies *entirely* on the patterns never matching `000TOTAL` — it has no
`product_nc <> '000TOTAL'` guard (only `detect_cn8_biggest_mover` adds one).

So a non-technical editor typing `8%` instead of `85%`, or `850%`, or a stray
`%`, silently produces an over-broad or 2×-inflated group with a clean "success"
and a finding that looks quotable. Untrusted input reaches the one query path
with no backstop.

**Cheap fix:** a CHECK constraint (e.g. each pattern matches `^[0-9]{2,8}%$`) plus
an explicit `product_nc <> '000TOTAL'` in the primary query as defence-in-depth.

---

## Tier B — modelling / interpretation errors in the headline mirror-gap product

### B1. The mirror-gap metric mixes two different definitions of "percent" in one subtraction

`anomalies.py:498-511`:

```python
gap_pct = gap_eur / max(abs(gacc_value_eur), abs(eurostat_total))
excess  = abs(gap_pct) - baseline_pct
```

`gap_pct = (E − G) / max(E, G)` is **max-normalised**: a true ratio gap of +20%
(E = 1.2·G) renders as **+16.7%**, and a true doubling renders as **+50%**, not
+100%. The metric systematically *compresses* large gaps toward ±100%.

But `baseline_pct` (the OECD ITIC CIF/FOB markup, e.g. NL 6.55%) is a
**ratio-space markup** (CIF ≈ FOB × 1.0655). Subtracting a ratio-space percentage
from a max-normalised percentage is a category error. Even at the baseline
itself, the metric-consistent value is 6.55/1.0655 = **6.15%**, not 6.55%, and
the mismatch grows with gap size. `excess_over_baseline_pct` is surfaced to
journalists verbatim (methodology §8: "excess over baseline = +52.8 pp", plus
spreadsheet columns), so the error rides into copy.

Two compounding problems in the same three lines:

- **`abs(gap_pct)`** means the baseline is subtracted even when E < G (gap
  negative). CIF/FOB markup only explains E > G (freight makes imports *higher*).
  For negative gaps the "excess over baseline" is meaningless yet still computed
  and rendered.
- **Only CIF/FOB is subtracted, not transshipment** — yet the methodology says
  transshipment is the *dominant* structural term for exactly the hub partners
  (NL, BE, HK…) that generate the biggest gaps. So "excess over baseline =
  +52.8pp" reads as "anomaly beyond what's expected" when most of it is *expected*
  transshipment that hasn't been netted out. The `transshipment_hub` caveat flags
  the partner, but the headline number's label ("excess over baseline")
  over-claims.

The number is mislabelled rather than miscomputed — but for a defensibility-first
tool that is the same problem. (The `mirror_gap_zscore` variant is more
defensible — looking at *changes* differences out the structural level — though a
persistent CNY trend would still inject a spurious trend into it.)

### B2. The mirror-gap GACC selector lacks the `currency='CNY'` filter the aggregate selector has

`discover_release_urls` matches **both** `(in CNY)` and `(in USD)` bulletins
(`api_client.py:32-33`), and the seed walk ingests both editions of section 4 →
two `releases` rows per period (different `release_id`, both section 4). The GACC
*aggregate* analysers defensively pin `r.currency = 'CNY'`. But
`_select_gacc_export_rows` (`anomalies.py:406-434`) has **no currency filter**,
and its `DISTINCT ON` includes `release_id`, so it returns *both* the CNY and USD
row for every partner → each mirror gap is computed twice along two
slightly-different FX paths. Same natural key `(iso2, period)` → the active
finding's value and provenance `obs_id` become **nondeterministic** between
editions, with possible supersede thrash when the two EUR conversions round
differently. Not catastrophic (values ≈ equal) but a real consistency bug and a
provenance-integrity problem (the audit trail points at an arbitrarily chosen
edition).

Quick check: `SELECT period, currency, count(*) FROM releases WHERE source='gacc'
AND section_number=4 GROUP BY 1,2`.

---

## Tier C — "correct today, silently wrong after the next change"

- **C1. `cn8_yoy_mover` has no `_export` subkind suffix** (`anomalies.py:2183`,
  key `(product_nc, anchor)`). Import-only today, enforced by a `raise`. Whoever
  adds exports without adding the suffix recreates the exact Africa/LatAm
  collision (import and export movers of the same code at the same anchor
  supersede each other). The safety lives in a runtime `raise`, not in the key.
- **C2. `narrative_hs_group` uses a hand-rolled key** `(group_id,)`
  (`llm_framing.py:1035`) that bypasses the `nk_*` helpers. Safe only because
  clusters fold all scopes/flows into one lead; add per-scope leads and it
  silently supersedes.
- **C3. `partner_is_subset` / `partner_indent` are write-only** — computed,
  stored, never read. The no-double-count invariant is "nobody sums across
  partner rows," not an enforced guard. Any future "China total" rollup that
  forgets this re-creates a GACC-side 000TOTAL bug; the flags to prevent it exist
  but aren't wired.
- **C4. Calendar staleness is graceful but silent.** `release_calendar.py`
  `exact` dicts run out after ref-month 2026-10 (Eurostat) / 2026-06 (HMRC), then
  fall back to a formula. Good design — but nothing alerts that the precise
  calendar is stale, and the GACC January carve-out (`release_calendar.py:186-187`)
  assumes the Jan-Feb-combined pattern persists, which 2026 already broke once.

---

## Tier D — documentation / defensibility (principle #2)

- **D1. The methodology's flagship worked example is pre-000TOTAL-fix stale.**
  `docs/methodology.md:185-186` still says "a mirror gap of +65% for NL is mostly
  the transshipment effect (≈65%)…", while `docs/methodology.md:239-240` says the
  *corrected* NL gap is ~+20%. §0 is the "why sources don't agree" foundation an
  auditing journalist reads first — and the "transshipment ≈ 65%" decomposition
  was derived from the **doubled** number the fix invalidated. If the +65% was 2×
  inflated, transshipment is *not* ~65%, and the whole worked decomposition needs
  redoing, not just the headline figure.
- **D2. HS-group overlaps (22 broad⊃narrow pairs) are under-disclosed as a
  partition trap.** By design they're overlapping lenses (the `84%/85%` group
  alone contains 13 others), which is fine — *until* something sums them or
  renders them as a partition. Confirm the portal trade-map/treemap
  (`report_builder._trade_map_section`, ~line 1187) doesn't assign a CN8 to
  multiple groups and present it as a whole. Related: broad chapter groups absorb
  `NNXXXXXX` confidentiality residuals while their narrow children can't, so
  "Sintered NdFeB +18%" and "Permanent magnets +1.4%" sit on different coverage
  bases — an honest provenance footnote not surfaced at the quote point.

---

## Tier E — the root cause (architecture)

**E1. `anomalies.py` at 240KB with per-family duplicated windowing/aggregation
SQL is not just an aesthetic problem — it has already produced divergent handling
of the same rule.** The 000TOTAL guard, the `currency='CNY'` pin (B2), and the
`<> '000TOTAL'` backstop (A3) each exist in *some* analyser copies and not
others. The 000TOTAL bug itself lived as long as it did because the fix had to be
applied per-site rather than once. Every new analyser re-implements the window
sum, and each re-implementation is a chance to drift. The highest-leverage
structural change is a single shared "sum this window for this (source, scope,
partners, patterns)" primitive that bakes in the 000TOTAL rule, the currency pin,
and the EU-27 inclusion list — so a correctness rule is stated once.

---

## Suggested order of attack

1. **A1** (GACC empty-parse guard) and **A2** (EU-27 inclusion list) — both
   cheap, both close a *silent* drift-failure on live data.
2. **B1** (relabel/redefine `excess_over_baseline`) — it's in front of
   journalists now and mislabels expected transshipment as anomaly.
3. **D1** (fix the stale §0 example) — pure defensibility, near-zero effort.
4. **A3 / B2** — guardrails on the journalist-editable surface and the currency
   duplication.
5. **E1** — the structural fix that makes A3/B2-class drift impossible to
   reintroduce.

None of this contradicts "it works" — the core invariants hold. These are the
places where the system is *trusting* the source data to keep behaving, or where
a headline number's label outruns what it computes.
