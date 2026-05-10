# Architecture

How the tool works, end-to-end. For the journalism it serves see
[editorial-sources.md](editorial-sources.md); for what each finding
means and how to interpret it see [methodology.md](methodology.md).

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
(HMRC, Phase 6.1) slotted in cleanly behind the same interface. The
analysers all support a `--comparison-scope` flag that picks
EU-27 (Eurostat) / UK (HMRC) / EU-27 + UK combined.

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
          briefing pack    leads doc    sheets export    LLM framing
          (markdown,       (markdown,   (.xlsx /         (Ollama →
           deterministic    LLM-drafted  Google Sheets)   structured
           only — no LLM    leads,                        JSON, then
           in the loop)     companion                     another find-
                            to brief)                     ing in the
                                                          same chain)
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
  context (window dates, totals, caveat codes, score) for the brief
  to render without re-querying the underlying observations.

## Append-plus-supersede chain

Findings are versioned, not over-written. When the analyser re-runs
and concludes the same natural-key (e.g. `(group_id, period_end)`)
should produce a different value, the prior row gets
`superseded_at = now()` + `superseded_by_finding_id` set; a new row
is inserted with the same natural-key.

Three things this gives us:

1. **Idempotency.** Re-running an analyser on unchanged data is a
   no-op at the row level (`last_confirmed_at` ticks; nothing
   inserts).
2. **Revision history.** "EU imports of EV batteries +34% YoY"
   moving to "+18% YoY" because Eurostat revised Feb 2026 leaves a
   trace; the brief versioning section ("Changes since previous
   brief") reads it directly.
3. **Method-version propagation.** Every finding's `value_fields`
   includes the analyser's method tag (e.g.
   `mirror_trade_v5_per_country_cif_fob_baselines`). Bumping the
   method version causes a clean supersede pass on next run, so
   improvements ripple through the brief without manual cleanup.

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
`--comparison-scope eu_27 / uk / eu_27_plus_uk` to fill the brief's
three per-scope sections.

### Export

`briefing_pack.export()` writes a three-artefact bundle per call,
into a per-export folder so all three share a single DB snapshot:

```
exports/
  2026-05-10-1747/
    brief.md     ← deterministic; NotebookLM-ready, no LLM in the loop
    leads.md     ← LLM lead scaffold (anomaly summary + picked
                    hypotheses + corroboration steps); cross-
                    references finding IDs the brief also surfaces
    data.xlsx    ← 8-tab spreadsheet for data journalists; LLM-free,
                    same DB snapshot as the brief
  2026-05-10-1830-ev-batteries-li-ion/   ← future scoped export
    brief.md
    leads.md
    data.xlsx
```

Folder name pattern: `YYYY-MM-DD-HHMM[-slug]/`. The optional slug comes
from `scope_label` (slugified to kebab-case) when set; full-brief
exports have no suffix. Each doc surfaces the scope in its header so
a doc shared standalone still announces what slice of the data it
covers.

Each export records a row in `brief_runs` so the next brief can
compute its "Changes since previous brief" section. The leads file
isn't versioned in `brief_runs` (it doesn't need a diff yet — add
one if a journalist asks for "what leads changed?").

Note: `scope_label` is currently metadata only — the brief and leads
still render the full finding set. Scoped *filtering* (only emit
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
| `GACC_PERMALINK_BASE` | If set, brief renders trace tokens as Markdown links to a hosted finding viewer |

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
  the brief and sheets export read findings, not raw data. So
  "what does the brief currently say" is always answered without
  the analyser running.
- **Auto-publish.** The journalist pulls; the tool doesn't push.
  No webhooks, no Slack postings, no auto-emails — by design,
  because a finding misclassified once should never have already
  hit a desk.
- **Free-form LLM narrative.** The LLM lead-scaffold layer picks
  hypotheses from a curated catalog and writes one-line rationales
  for each, both numerically verified. It does NOT draft top-line
  prose. (See [methodology.md](methodology.md) §5.)
