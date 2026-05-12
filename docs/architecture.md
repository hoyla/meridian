# Architecture

How the tool works, end-to-end. For the journalism it serves see
[editorial-sources.md](editorial-sources.md); for what each finding
means and how to interpret it see [methodology.md](methodology.md);
for unfamiliar terms see [glossary.md](glossary.md).

> **TL;DR.** Three customs sources (GACC / Eurostat / HMRC), three
> data layers (`raw_rows` → `observations` → `findings`), an
> append-plus-supersede chain on findings, a per-export folder
> bundle of three artefacts (`findings.md` + `leads.md` +
> `data.xlsx`). A daily periodic-run pipeline re-emits the bundle
> when a new Eurostat release lands; idempotent on no-op days.
>
> Reading paths:
> - **Adding a new analyser kind?** Start with
>   [Three-layer data flow](#three-layer-data-flow) →
>   [Append-plus-supersede chain](#append-plus-supersede-chain).
> - **Wiring a new data source?** Same two sections, then
>   [Storage layout](#storage-layout-key-tables) and
>   [External dependencies](#external-dependencies).
> - **Setting up a fresh deployment?** [CLI surface](#cli-surface)
>   → [Configuration](#configuration) → the README's setup block.

## Two-source-by-design (now three)

The original brief was a cross-source comparison: how does what
China's customs (GACC) says it exported to a country differ from
what that country's customs says it imported from China? The
*divergence* is the editorial story; building a tool that ingests
both sides under a shared schema is the technical prerequisite.

Today the tool ingests three sources:

| Source | What it reports | Native form | Frequency |
|---|---|---|---|
| **GACC** (China) | Chinese customs declarations of trade with named countries / regions | HTML release pages, CNY/USD, FOB | Monthly preliminary, then revised |
| **Eurostat** | EU member states' customs declarations of trade with non-EU partners | Bulk `.7z` files, EUR-native, CIF | Monthly, ~6-8 week lag |
| **HMRC OTS** | UK customs declarations of trade with all partners | OData REST API, GBP-native, CIF | Monthly |

The schema anticipated the second source from the start; the third
([HMRC](glossary.md#hmrc-ots-overseas-trade-statistics), Phase 6.1)
slotted in cleanly behind the same interface. The analysers all
support a `--comparison-scope` flag that picks
[EU-27 / UK / EU-27 + UK combined](glossary.md#eu-27-vs-eu-27--uk-comparison-scopes).

## Three-layer data flow

```
                ┌──────────────────────────────────────────┐
                │  external sources                        │
                │  english.customs.gov.cn (GACC HTML)      │
                │  ec.europa.eu/eurostat (bulk 7z)         │
                │  api.uktradeinfo.com (HMRC OData)        │
                │  data-api.ecb.europa.eu (FX rates)       │
                └──────────────────────────────────────────┘
                                  │
                          [scrape.py --url … / --eurostat-period …]
                                  │
                                  ▼
                ┌──────────────────────────────────────────┐
                │  raw_rows (preserved verbatim)           │
                │  • eurostat_raw_rows                     │
                │  • hmrc_raw_rows                         │
                │  • source_snapshots (GACC HTML bytes)    │
                │  • releases (per source × period)        │
                └──────────────────────────────────────────┘
                                  │
                          [parsers + aggregators]
                                  │
                                  ▼
                ┌──────────────────────────────────────────┐
                │  observations (per cell, normalised)     │
                │  flow × reporter × partner × period      │
                │  × HS-code, in EUR                       │
                │  • observations.eurostat_raw_row_ids[]   │
                │  • observations.hmrc_raw_row_ids[]       │
                └──────────────────────────────────────────┘
                                  │
                          [scrape.py --analyse …]
                                  │
                                  ▼
                ┌──────────────────────────────────────────┐
                │  findings (anomaly subkinds + LLM leads) │
                │  • mirror_gap / mirror_gap_zscore        │
                │  • hs_group_yoy{,_export}                │
                │  • hs_group_trajectory{,_export}         │
                │  • narrative_hs_group (LLM scaffold)     │
                │  • append-plus-supersede chain           │
                └──────────────────────────────────────────┘
                                  │
                ┌─────────────┬─────────────┬─────────────────┐
                ▼             ▼             ▼                 ▼
          findings.md      leads.md     data.xlsx        LLM framing
          (markdown,       (markdown,   (xlsx, 8 tabs;   (Ollama →
           deterministic    LLM-drafted  Google Sheets    structured
           — no LLM in the  leads,       writer stubbed)  JSON, then
           loop;            companion    Same DB          another find-
           NotebookLM-      to findings) snapshot;        ing in the
           ready)                        also LLM-free.   same chain)
```

Three layers because each has a distinct concern:

- **`raw_rows`** preserves every CSV line / HTML page byte exactly
  as the source published it. So provenance is verifiable down to
  the specific upstream row, and re-parsing is always possible
  without re-fetching.
- **`observations`** is the normalised, cross-source view: the same
  flow expressed as a single EUR figure regardless of which source
  it came from, with an `*_raw_row_ids` array linking back. This
  is what cross-source queries (mirror-gap) join on.
- **`findings`** is editorial output: one row per anomaly the
  analyser detected, with a `detail` JSONB blob carrying enough
  context (window dates, totals, caveat codes, score) for the findings document
  to render without re-querying the underlying observations.

## Append-plus-supersede chain

[Findings](glossary.md#finding) are versioned, not over-written.
When the analyser re-runs and concludes the same
[*natural key*](glossary.md#natural-key) (e.g. `(group_id,
period_end)`) should produce a different value, the prior row gets
`superseded_at = now()` + `superseded_by_finding_id` set; a new row
is inserted with the same natural-key.

Three things this gives us:

1. **Idempotency.** Re-running an analyser on unchanged data is a
   no-op at the row level (`last_confirmed_at` ticks; nothing
   inserts).
2. **Revision history.** "EU imports of EV batteries +34% YoY"
   moving to "+18% YoY" because Eurostat revised Feb 2026 leaves a
   trace; the brief-versioning section ("Changes since previous
   export") reads it directly.
3. **Method-version propagation.** Every finding's `value_fields`
   includes the analyser's method tag (e.g.
   `mirror_trade_v5_per_country_cif_fob_baselines`). Bumping the
   method version causes a clean supersede pass on next run, so
   improvements ripple through the findings document without manual cleanup.

`findings_io.emit_finding` is the canonical write path. Each call
site declares:

- a **natural key** (`nk_mirror_gap(iso2, period)`,
  `nk_hs_group_yoy(group_id, period_end)`, etc. — one per subkind);
- a **value-fields dict** (the values that, if they move, mean the
  finding has revised);
- the **detail JSONB** (everything else — window dates, totals,
  observation IDs, caveat codes).

The helper computes `natural_key_hash` and `value_signature`, looks
up the un-superseded row with the same hash, and decides
insert / confirm / supersede. A partial unique index on
`(natural_key_hash) WHERE superseded_at IS NULL` enforces "at most
one active finding per natural key" at the DB level.

## CLI surface

`scrape.py` is the single entry point, multi-modal. Grouped by
purpose:

### Ingest

```bash
# GACC (Chinese customs)
scrape.py --url http://english.customs.gov.cn/statics/report/preliminary.html
scrape.py --url http://english.customs.gov.cn/statics/report/preliminary2024.html

# Eurostat (bulk 7z file per month)
scrape.py --eurostat-period 2026-02

# HMRC (OData REST)
scrape.py --hmrc-period 2026-02

# ECB FX rates (for GBP→EUR conversion)
scrape.py --fetch-fx CNY --fetch-fx GBP
```

### Analyse

```bash
# Deterministic anomaly passes
scrape.py --analyse mirror-trade
scrape.py --analyse mirror-gap-trends
scrape.py --analyse hs-group-yoy [--flow 1|2] [--comparison-scope SCOPE]
scrape.py --analyse hs-group-trajectory [--flow 1|2] [--comparison-scope SCOPE]
scrape.py --analyse gacc-aggregate-yoy

# LLM lead-scaffold (consumes existing findings)
scrape.py --analyse llm-framing [--llm-model NAME]
```

The four hs-group passes are scope-aware: re-run them with
`--comparison-scope eu_27 / uk / eu_27_plus_uk` to fill the findings
document's three per-scope sections.

### Periodic-run orchestrator (Phase 6.9)

```bash
scrape.py --periodic-run [--force] [--skip-llm] [--export-dir PATH]
```

Deployment-agnostic Layer-1 pipeline. Chains the ingest+analyse+render
steps with idempotency: if the latest Eurostat `releases.period` in
the DB is no fresher than what the last `trigger='periodic_run'` row
in `brief_runs` already published, exits cleanly as a no-op. Otherwise
runs every analyser kind across all scope/flow combos (idempotent
per-row via the supersede chain), optionally runs `llm-framing`
(`--skip-llm` to omit), and writes the bundled findings export. Prints
the new `findings.md` path to stdout (empty string on no-op) so the
calling wrapper (Routine, GHA cron, etc.) can branch on it.

The orchestrator deliberately does NOT fetch new Eurostat / HMRC
periods — fetch is the scheduler layer's responsibility. Keeping
network and analyser as separate concerns means a network failure
during fetch doesn't leave the pipeline in flight.

Layer 2 (scheduler) is currently a Claude Code Routine
(`gacc-daily-periodic-run`, cron `0 9 * * *`). Layer 3 (delivery to
the journalist) is currently manual. See
`dev_notes/periodic-runs-design-2026-05-11.md`.

### Export

`briefing_pack.export()` writes a three-artefact bundle per call,
into a per-export folder so all three share a single DB snapshot:

```
exports/
  2026-05-10-1747/
    findings.md     ← deterministic; NotebookLM-ready, no LLM in the loop
    leads.md     ← LLM lead scaffold (anomaly summary + picked
                    hypotheses + corroboration steps); cross-
                    references finding IDs the findings document also surfaces
    data.xlsx    ← 8-tab spreadsheet for data journalists; LLM-free,
                    same DB snapshot as findings.md
  2026-05-10-1830-ev-batteries-li-ion/   ← future scoped export
    findings.md
    leads.md
    data.xlsx
```

Folder name pattern: `YYYY-MM-DD-HHMM[-slug]/`. The optional slug comes
from `scope_label` (slugified to kebab-case) when set; full-brief
exports have no suffix. Each doc surfaces the scope in its header so
a doc shared standalone still announces what slice of the data it
covers.

Each export records a row in `brief_runs` so the next export can
compute its "Changes since previous brief" section. The leads file
isn't versioned in `brief_runs` (it doesn't need a diff yet — add
one if a journalist asks for "what leads changed?"). `brief_runs`
also stamps the Eurostat `data_period` the export reflects, plus a
`trigger` column distinguishing `'periodic_run'` (canonical
subscriber-facing cycle) from `'manual'` (ad-hoc / test / preview).
The `--no-record` flag (or `record=False` kwarg) produces an
"unsequenced" export that doesn't insert a row — useful for test or
preview renders that shouldn't pollute the cycle baseline.

The findings document is structured in **three explicit tiers**
separated by `---` and named in their `## Tier N — ...` headings:

- **Tier 1 — What's new this cycle**: the diff against the previous
  `trigger='periodic_run'` row.
- **Tier 2 — Current state of play**: compact summary, one block
  per HS group + one per GACC partner aggregate (ASEAN / Africa /
  Latin America / world Total). Each row shows 12mo rolling YoY AND
  single-month "Latest month" YoY inline (Phase 6.10).
- **Tier 3 — Full detail by HS group**: per-finding mover sections.

A reader's-guide section right after the headline names the tiers so
journalists know where to dive in (regular subscriber: Tier 1; new
joiner: Tier 2 → Tier 3).

Note: `scope_label` is currently metadata only — the findings document
and leads still render the full finding set. Scoped *filtering* (only emit
findings for one HS group, only one comparison scope) is forward
work; the naming convention is in place so scoped exports can land
cleanly when needed.

Sheets export ships local `.xlsx`; Google Sheets writer is stubbed
pending service-account credentials.

## Configuration

### Environment variables

| Var | Purpose |
|---|---|
| `DATABASE_URL` | Postgres connection (live `gacc` DB) |
| `GACC_TEST_DATABASE_URL` | Test DB for pytest |
| `GACC_LIVE_DATABASE_URL` | Optional: lets opt-in tests check the live DB |
| `LLM_BACKEND` | `ollama` (default) or future alternatives |
| `GACC_PERMALINK_BASE` | If set, findings.md renders trace tokens as Markdown links to a hosted finding viewer |

### Schema-table seeds

Several lookup tables exist to keep policy out of code, and editable
by journalists who don't want to read Python:

| Table | What it holds | Seeded in `schema.sql`? |
|---|---|---|
| `hs_groups` | Editorial HS-code clusters (HS patterns + name + description) | Yes (~17 seed groups + 13 added Phase 5) |
| `caveats` | Canonical summary + detail text per caveat code | Yes |
| `transshipment_hubs` | iso2 + evidence_url for known transshipment partners | Yes (NL, BE, HK, SG, AE, MX) |
| `cif_fob_baselines` | Per-(partner, baseline_pct) CIF/FOB margin overrides | Yes (28 EU-27+GB rows from OECD ITIC + 1 global default) |
| `country_aliases` | Maps GACC partner labels → ISO-2 codes | Yes |

Add a row to `hs_groups` and the next analyser run will produce
findings for it; no code change required.

## Storage layout (key tables)

```
releases               (per source × period; one row per ingestable file/page)
   │
   ├──── eurostat_raw_rows ──────┐
   ├──── hmrc_raw_rows ──────────┤
   └──── source_snapshots        │   (GACC HTML bytes, sha256-deduped)
                                 │
                                 ▼
                      observations (normalised cells)
                                 │
                                 ▼
                      findings (versioned editorial output)
                                 │
                                 └──── brief_runs (per export timestamp)

lookup tables (read-side only):
  hs_groups, caveats, transshipment_hubs, cif_fob_baselines,
  country_aliases, fx_rates
```

Re-derivability is the discipline: any aggregated number can be
walked back to the specific raw rows it summed (via
`observations.*_raw_row_ids[]`), and any finding can be walked back
to the observations it cited (via `findings.observation_ids[]`).
The brief's "Sources" appendix lists every third-party URL the
brief rests on with fetch timestamps.

## External dependencies

What fails (and how the tool degrades) if each is unreachable:

| Dependency | What breaks | Degradation |
|---|---|---|
| `english.customs.gov.cn` | GACC ingest | Existing data still queryable; brief's mirror-trade section ages |
| Eurostat bulk file server | New-period Eurostat ingest | Existing periods unaffected; trajectories shrink at the leading edge |
| `api.uktradeinfo.com` | HMRC OData ingest | UK scope's findings age; eu_27 + eu_27_plus_uk unaffected |
| ECB SDMX | FX rate refresh | New CNY/GBP→EUR conversions can't run; cached rates work |
| Ollama daemon (`localhost:11434`) | LLM lead-scaffold pass | Leads file empty / stale; brief is unaffected (no LLM in the loop) |
| OECD SDMX | (One-off) refreshing CIF/FOB baselines | Existing per-country baselines stay; new-year refresh waits |

The CLI handles each failure as a normal logged error rather than
crashing — a `scrape_runs` row is left with `status='failed'` and
`error_message` populated, and the next pass picks up where it left
off.

## What this tool deliberately doesn't do

- **Predict.** Findings describe the past, not the future.
- **Compute live.** Every analyser pass writes findings to disk;
  the findings document and spreadsheet read findings, not raw data. So
  "what does the findings document currently say" is always answered without
  the analyser running.
- **Auto-publish.** The journalist pulls; the tool doesn't push.
  No webhooks, no Slack postings, no auto-emails — by design,
  because a finding misclassified once should never have already
  hit a desk.
- **Free-form LLM narrative.** The LLM lead-scaffold layer picks
  hypotheses from a curated catalog and writes one-line rationales
  for each, both numerically verified. It does NOT draft top-line
  prose. (See [methodology.md](methodology.md) §5.)
