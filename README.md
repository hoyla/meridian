# Meridian

A trade-statistics monitor for Guardian journalists working the
China–EU/UK beat. Ingests both sides of the customs fence — GACC
(China), Eurostat Comext (EU-27), HMRC OTS (UK post-Brexit) — into a
shared schema, cross-compares them, and writes a per-export bundle —
a deterministic Markdown findings doc, a companion LLM-leads file, a
10-tab spreadsheet, an HS-group reference and a read-me — optionally
rendered as styled Word docs and published to Google Drive as native
Docs/Sheet. The journalism it
should inform is in part inspired by the excellent work of 
[Soapbox Trade](https://soapboxtrade.substack.com);
the model journalist is Lisa O'Carroll.

## What the brief looks like

Every export bundle's `02_Findings.md` opens with a one-page front
page — **"If you read only this page"** — the cycle's most quotable
shifts as publishable, hedged sentences (no LLM involved; these are
template-rendered from the deterministic findings):

> **EU-27 exports of Finished cars (broad) to China** fell 41.1% by
> value in the 12 months to Mar 2026, to €7.94B; volume down 37.7% —
> though the signal has been mixed over the past six months, so
> double-check before headlining. `finding/65865`

followed by three tiers (what's new since last time → current state
of play → full detail). A typical Tier 2 entry for one HS group:

> ### EV batteries (Li-ion) 🟡
>
>   - **EU-27 imports from China**: value +35.6%, volume +71.3%,
>     12mo total €27.63B (12 months to Mar 2026). Latest month:
>     value +19.0%, volume +47.9%. Trend: dip-and-recovery (was
>     rising, dipped, now rising again). `finding/65765`
>   - **UK imports from China**: value +13.8%, volume +35.3%, 12mo
>     total €1.66B (12 months to Feb 2026). `finding/45367`
>   - **EU-27 exports to China**: value -37.4%, volume -1.9%, 12mo
>     total €422.6M (12 months to Mar 2026). `finding/65853`

🟢 / 🟡 / 🔴 are stability badges (would the headline % from
6 months ago still hold today?); `finding/{id}` is a stable citation
token you can paste into a story and follow back to source rows.
Tier 3 ("Full detail") opens every block with a plain-English
**Quotability** verdict that applies the methodology's quote/hedge
rules at the point of quotation, then adds the top contributing
reporter countries (*"Germany alone explains 66% of the EU-wide
drop"*), top contributing HS-CN8 codes, mirror-trade gaps, and the
per-finding caveats. Every finding traces back to a Eurostat / HMRC
raw row or a GACC release page.

## Three rules the design follows

- **Brief and spreadsheet are LLM-free.** The LLM lead scaffolds live
  in a separate `03_Leads.md` — so a downstream LLM tool (NotebookLM,
  Claude, etc.) reasoning over the brief sees raw findings, not
  another LLM's interpretation.
- **Findings are versioned, not over-written.** When data revises or
  methodology evolves, the old finding is marked superseded with a
  back-pointer to the new one. Citation tokens stay stable; the
  supersede chain is the audit trail.
- **HS groups are journalist-editable.** They live in an
  `hs_groups` DB table — to investigate a new commodity or sector,
  add a row, no code change required.

## Where to read more

- **[docs/README.md](docs/README.md)** — guided reading paths by
  what you're trying to do.
- **[docs/glossary.md](docs/glossary.md)** — every unfamiliar term
  in one place.
- **[docs/architecture.md](docs/architecture.md)** — how the pipeline
  works.
- **[docs/methodology.md](docs/methodology.md)** — what each finding
  means and when to quote it.
- **[docs/editorial-sources.md](docs/editorial-sources.md)** — the
  journalism the tool exists to support.

## Stack

- Python 3.12+, native venv (no Docker for the dev loop)
- Postgres (local) — schema in `schema.sql`
- Optional `docker-compose.yml` for a Postgres container if you'd rather not install it
- LLM: Ollama (local default — `qwen3.6:latest` works well for this task), behind a pluggable `LLMBackend` in `llm_framing.py`. Tests use a `FakeBackend` so CI never calls Ollama.

## Setup

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt  # for tests

cp .env.example .env  # edit DATABASE_URL etc.

# The Postgres DB is named `gacc` (pre-rename); the project renamed to
# Meridian in May 2026 but the local DB and the `DATABASE_URL` default in
# .env.example kept the old name. Rename if you prefer — nothing in the
# code depends on the literal string.
createdb gacc
psql gacc < schema.sql

# Ollama for the LLM lead-scaffold pass (optional — `--skip-llm` skips it):
ollama pull qwen3.6:latest
```

## Usage

```sh
# GACC scraping
python scrape.py                          # walk all configured GACC index URLs
python scrape.py --url <url>              # one-shot (index OR release URL)
python scrape.py --dry-run                # fetch + parse without DB writes

# Eurostat bulk ingest (one month at a time; bulk file is ~44MB and hosts all partners)
python scrape.py --eurostat-period 2026-03                                       # one month, default partner CN (the scheduled run uses CN+HK+MO)
python scrape.py --eurostat-period 2026-03 --partner CN --partner HK --partner MO   # what the periodic run ingests
python scrape.py --eurostat-period 2026-03 --partner US                          # a different partner into the same month (additive)
python scrape.py --eurostat-period 2026-03 --hs-prefix 87038                     # filter by HS prefix
# Re-ingest is additive-only: a period/partner already stored is skipped (status=noop), never duplicated.
python scrape.py --eurostat-period 2026-03 --eurostat-reporter NL                # surgically backfill ONE missing member state
python scrape.py --eurostat-coverage 2026-01 2026-04                             # report member-state months missing from the 000TOTAL set

# HMRC OTS ingest (UK side, post-Brexit canonical source for UK-China trade)
# Pre-requires GBP/EUR FX loaded (see below). Default partners CN+HK+MO.
python scrape.py --hmrc-period 2026-02

# FX rates
python scrape.py --fetch-fx CNY                       # full ECB CNY/EUR history
python scrape.py --fetch-fx GBP --fx-since 2017-01     # GBP/EUR history (pre-req for HMRC ingest)
python scrape.py --fetch-fx CNY --fx-since 2024-01     # from a given month

# Anomaly detection (over already-ingested data)
# All analysers sum Eurostat across CN+HK+MO — the editorially-correct "Chinese trade" envelope
# including the two Special Administrative Regions. multi_partner_sum is a family-universal caveat
# (rendered once in the brief's Methodology footer, not per finding). For a CN-only spot check,
# query eurostat_raw_rows directly.
python scrape.py --analyse mirror-trade                       # CN-export vs EU-import per partner
python scrape.py --analyse mirror-gap-trends --trend-window 6 --z-threshold 1.5
python scrape.py --analyse hs-group-yoy --flow 1              # imports (CN→EU); --flow 2 for exports
python scrape.py --analyse hs-group-yoy --hs-group "EV batteries (Li-ion)" --yoy-threshold 0.1
python scrape.py --analyse hs-group-yoy --low-base-threshold 10000000   # lower €50M low-base floor for niche-commodity work
python scrape.py --analyse hs-group-yoy --comparison-scope uk           # UK side only (HMRC; Phase 6.1)
python scrape.py --analyse hs-group-yoy --comparison-scope eu_27_plus_uk  # combined view (cross_source_sum caveat)
python scrape.py --analyse hs-group-trajectory --flow 1       # rolling YoY shape classifier (inherits from yoy)
python scrape.py --analyse hs-group-trajectory --smooth-window 1   # disable smoothing for short-term policy effects
python scrape.py --analyse hs-group-trajectory --comparison-scope uk    # UK trajectories
python scrape.py --analyse gacc-aggregate-yoy --flow 1         # GACC-only YoY for non-EU partner aggregates (ASEAN, RCEP, Belt&Road, Africa, LatAm, world Total)
python scrape.py --analyse gacc-aggregate-yoy --flow 2         # same, China imports from each bloc
python scrape.py --analyse gacc-bilateral-aggregate-yoy --flow 1   # bilateral counterpart: EU bloc + every single-country GACC partner; each finding carries 12mo rolling, YTD cumulative, and single-month YoY operators side-by-side
python scrape.py --analyse gacc-bilateral-aggregate-yoy --flow 2   # same, China imports from each partner
python scrape.py --eurostat-world-aggregates-period 2026-02        # pre-populate the denominator (extra-EU totals) for the partner-share metric — backfill before running partner-share
python scrape.py --analyse partner-share --flow 1                  # China's share of EU-27 extra-EU imports per HS group, by value AND by tonnes (Soapbox "bigger in tonnes than euros" register)
python scrape.py --analyse partner-share --flow 2                  # same, exports

# LLM-drafted editorial top-lines (consumes the deterministic findings above)
python scrape.py --analyse llm-framing                        # one narrative per HS group (default qwen3.6:latest)
python scrape.py --analyse llm-framing --hs-group "EV batteries (Li-ion)"
python scrape.py --analyse llm-framing --llm-model qwen3.5:14b      # alternative model

# Re-running an --analyse pass is idempotent. Findings whose values match the
# existing row bump last_confirmed_at; revised values insert a new row and
# mark the old one superseded with a back-pointer. The supersede chain
# captures revisions as queryable history (see Design notes below).

# Spreadsheet only (standalone — for spreadsheet-only invocations; the
# bundled briefing pack already includes 04_Data.xlsx in its folder)
python scrape.py --export-sheet                               # local .xlsx, 10 tabs
python scrape.py --export-sheet --out-path exports/custom.xlsx
python scrape.py --export-sheet --out-format sheets --spreadsheet-id <ID>   # Google Sheets (pending creds)

# Export bundle: read-me + LLM leads + findings + data + HS-group reference
python scrape.py --briefing-pack                              # ./exports/YYYY-MM-DD-HHMM/{01_Read_Me_First.md, 03_Leads.md, 02_Findings.md, 04_Data.xlsx, 05_Groups.md}
python scrape.py --briefing-pack --docx                       # also render styled .docx; restructures to mirror the Drive upload (docs at top, .md in a "Markdown versions for use with LLMs etc" subfolder); required before --upload-to-drive
python scrape.py --briefing-pack --briefing-top-n 20          # 20 movers per flow direction
python scrape.py --briefing-pack --export-dir exports/today   # explicit output folder
python scrape.py --briefing-pack --export-scope "EV batteries (Li-ion)"  # adds slug to folder + scope line in docs
python scrape.py --briefing-pack --with-provenance            # also bundle per-finding provenance for the editorially-fresh subset (~5-15 files in <export>/provenance/)

# Per-finding provenance — journalist-readable audit trail for a single finding
# (source URLs, FX rates, plain-English caveats, cross-source check). Writes
# `provenance/finding-{N}.md`. Idempotent: skips if the file already exists; pass
# --force to regenerate (e.g. after a methodology refresh).
#
# Use this when a journalist asks "where exactly did finding/N come from?" — the
# output is a self-contained Markdown file that can be forwarded directly.
# Detailed templates currently cover the GACC bilateral aggregate, hs_group_yoy*,
# and hs_group_trajectory* families; other subkinds emit a stub noting
# "generator pending".
python scrape.py --finding-provenance 57378                   # writes provenance/finding-57378.md, prints the path
python scrape.py --finding-provenance 57378 --force           # regenerate even if the file exists

# HS group glossary, standalone — same content as the `05_Groups.md` in a bundle
# but generated as a one-off dated file, useful for forwarding the glossary by
# itself between briefing-pack runs.
python scrape.py --groups-glossary                            # → exports/groups-glossary-YYYY-MM-DD.md
python scrape.py --groups-glossary --out path/to/groups.md    # explicit output path

# Periodic cycle — the full pipeline in one idempotent command (ingest-aware → analyse → render bundle)
python scrape.py --periodic-run                               # no-op unless a fresher data period has landed
python scrape.py --periodic-run --force                       # re-run against the same data
python scrape.py --periodic-run --skip-llm                    # skip the LLM lead-scaffold pass
# Prints a per-run summary (which sources brought new data; the exact --upload-to-drive command when a
# briefing was generated) then the 02_Findings.md path on its own final line, for a wrapper to branch on.

# Source freshness monitoring — always-probe + publication-calendar expectation (none_expected/due/overdue)
python scrape.py --probe-source eurostat                      # probe one source (also: hmrc, gacc); records result + expectation
python scrape.py --source-status                              # rolled-up per-source view of routine_check_log

# Publish a generated bundle to Google Drive — a deliberate hand-run step (never auto-published)
python scrape.py --upload-to-drive exports/2026-05-15-1430    # .docx → Google Docs, .xlsx → Sheet; needs a --docx bundle
```

The two export surfaces share the same underlying data layer: switching between them — or adding a new one — is a thin render shim, not a re-ingest.

## Layout

| File               | Responsibility                                          |
|--------------------|---------------------------------------------------------|
| `scrape.py`        | CLI entry point + run orchestration                     |
| `api_client.py`    | HTTP fetch, hashing, link discovery                     |
| `parse.py`         | HTML / PDF → structured observations                    |
| `db.py`            | Postgres access (psycopg2-binary, no ORM)               |
| `eurostat.py`      | Eurostat Comext bulk-file fetcher (7z download, stream-decompress, filter, aggregate) |
| `hmrc.py`          | HMRC OTS fetcher via OData REST API (Phase 6.1; UK-side counterpart to eurostat.py; converts GBP→EUR at ingest) |
| `fx.py`            | ECB monthly-average FX rate fetcher → `fx_rates`        |
| `lookups.py`       | Country-alias resolution, caveat metadata, FX rate lookups |
| `anomalies.py`     | Deterministic anomaly detection: 6 anomaly subkinds — `mirror_gap`, `mirror_gap_zscore`, `hs_group_yoy` (+ `_export`), `hs_group_trajectory` (+ `_export`) |
| `findings_io.py`   | Idempotent `emit_finding()` helper — append-plus-supersede chain. The canonical write path: every analyser call site declares a natural key + value-fields dict, and the helper handles insert / re-confirm / supersede |
| `llm_framing.py`   | LLM lead-scaffold layer over the deterministic findings. v2 produces, per HS group, an anomaly summary + 2-3 hypothesis ids picked from `hypothesis_catalog.py` with one-line rationales + deterministic corroboration steps. Numeric-verification gate rejects any number not present in the underlying facts; hypothesis ids must exist in the catalog. Default backend: Ollama / `qwen3.6:latest`. |
| `hypothesis_catalog.py` | 12 standard causal hypotheses for China-EU/UK trade movements. Each entry carries a description (in the LLM prompt) and corroboration steps (attached deterministically post-pick). |
| `scripts/`         | One-off analysis scripts (sensitivity sweep, OOS backtest) — not part of the CLI; run directly. |
| `briefing_pack/` | Five-artefact export bundle into `./exports/YYYY-MM-DD-HHMM[-slug]/`. `02_Findings.md` is the deterministic NotebookLM-ready findings document (no LLM in the loop). `03_Leads.md` is the LLM lead-scaffold companion (anomaly summaries + picked hypotheses + corroboration steps), kept separate so downstream LLM tools reasoning over them see raw findings, not another LLM's interpretation. `04_Data.xlsx` is the 10-tab spreadsheet for data journalists. `05_Groups.md` is the HS group reference (auto-generated from the `hs_groups` table). `01_Read_Me_First.md` is the journalist-facing orientation page copied from the templates directory. Optionally with a `provenance/` subdir when `--with-provenance` is set. All artefacts share a single DB snapshot. With `docx=True` (the default in scheduled runs) the documents are also rendered as styled `.docx` and the folder is restructured to mirror the Google Drive upload. |
| `briefing_pack/drive_export.py` | Publishes a bundle to Google Drive (OAuth `drive.file`) via the manual `--upload-to-drive` CLI — never auto-published: top-level `.docx` → native Google Docs and `04_Data.xlsx` → a Sheet; mints the heading navigation anchors Google's `.docx` import omits (a batched style-flip pass); repoints in-document links via `headingId`; and mirrors the `Markdown versions for use with LLMs etc` subfolder of raw `.md`/`.xlsx`. Idempotent (update-in-place). Fail-loud token: an unattended caller raises `TokenUnusableError` rather than blocking on a browser consent prompt. Target folder via the `MERIDIAN_DRIVE_PARENT_ID` env var; export folders inherit the parent's sharing. |
| `periodic.py`     | Periodic-cycle orchestrator behind `--periodic-run`: idempotency-checks the latest data period against `brief_runs`, runs every analyser across scope/flow combos, renders the bundle, and returns a `PeriodicRunResult` whose `summary()` names the new-data sources and the manual `--upload-to-drive` command. Deliberately does not auto-publish. |
| `release_calendar.py` | Pure publication-calendar engine (no DB, no network) for the source-freshness *expectation axis*: given (source, period, today) it classifies `none_expected` / `due` / `overdue` from hand-entered Eurostat/HMRC calendar constants. Replaced the old "5 weeks past period close" fetch-gate (2026-06-02). Surfaced via `--probe-source` / `--source-status`. |
| `routine_log.py`  | Routine-check telemetry (no journalist surface): records each probe to `routine_check_log` (result × expectation) and computes the `--source-status` view. |
| `provenance.py`    | Per-finding provenance file generator. Each call writes `provenance/finding-{N}.md` — a journalist-readable audit trail (source URLs, FX rates, plain-English caveats, cross-source check, replay queries). CLI: `--finding-provenance N`. Frozen-snapshot semantics: idempotent on existing files; pass `--force` to regenerate. The `--briefing-pack --with-provenance` flag opt-in copies the editorially-fresh subset (Tier 1 changes + Top-N movers + Top-N leads, typically ~5-15 files) into the export bundle's `provenance/` subdir. Detailed templates cover `gacc_bilateral_aggregate_yoy*`, `hs_group_yoy*` (six scope/flow variants), and `hs_group_trajectory*` (six variants); other subkinds emit a stub. |
| `sheets_export.py` | 10-tab spreadsheet exporter (xlsx local; the in-place Google Sheets writer is stubbed pending creds — Drive delivery instead converts the xlsx to a Sheet via `drive_export.py`). Tabs: summary (wide, all scopes), hs_yoy_imports/exports (long with scope column), trajectories, mirror_gaps (with per-country CIF/FOB baseline + excess-pp), mirror_gap_movers, low_base_review, predictability_index. Intentionally LLM-free for the same telephone-game reason as the findings document. |
| `schema.sql`       | Canonical schema (includes lookup-table seeds: hs_groups, country_aliases, caveats, transshipment_hubs, cif_fob_baselines). A fresh setup is `createdb gacc && psql gacc < schema.sql` — no migration replay needed. |
| `migrations.archived-2026-05-09/` | Historical record of the dev migrations that built up to the current schema. Folded into `schema.sql` on the 2026-05-09 clean-state rebuild; kept for reference but no longer applied. |
| `dev_notes/`       | In-repo planning artefacts. `roadmap.md` (outstanding work), `history.md` (chronological record of addressed items), open `forward-work` design docs (deferred options), dated analysis artefacts (sensitivity sweep, OOS backtest, CIF/FOB sourcing), and the pre-registered `2026-05-09-shock-validation.md` methodology doc. All dated notes are named `YYYY-MM-DD-<slug>` so they sort chronologically. |
| `docs/`            | Repo-level documentation: `architecture.md` (system overview), `methodology.md` (analysis-methodology reference), `editorial-sources.md` (the journalism the tool serves). |
| `exports/`         | Default output directory for generated `.xlsx` and `.md` exports (gitignored) |
| `tests/`           | pytest, live local Postgres. FakeBackend keeps Ollama out of the suite. |

See docs folder for architecture and details about methodology.

## Licence, attribution, citation

- **Code**: Apache License 2.0 — © Guardian News & Media Limited.
  See [LICENSE](LICENSE) and [NOTICE](NOTICE).
- **Data**: Meridian re-uses public trade statistics from Eurostat,
  HMRC, GACC and the ECB. Each source has its own attribution
  requirements (HMRC's OGL v3.0 is the strictest — it requires the
  exact licence acknowledgement when its data is re-published).
  [NOTICE](NOTICE) lists each source and the wording to carry into
  derived journalism; every export bundle also carries source URLs in
  its `02_Findings.md` Sources appendix.
- **Citing the tool**: see [CITATION.cff](CITATION.cff), or use the
  "Cite this repository" button on GitHub.

## Design notes

- **Two-source by design.** GACC and Eurostat ingest into a shared `observations` table with a per-cell view, so any cross-source query (mirror-gap, agg-vs-agg) is just a join. The schema anticipated the second source from the start.
- **Provenance discipline.** Raw response bytes for every GACC fetch are stored in `source_snapshots`; raw Eurostat CSV rows are preserved verbatim in `eurostat_raw_rows` (the aggregated `observations` row carries an FK array back to its raw rows, so any aggregation can be audited or re-derived). Findings reference observation_ids so a journalist clicking through any number can land on the underlying row. The findings document's Sources appendix lists every third-party URL it rests on.
- **Observations are versioned.** When the same (release, dimension) reappears with a different value (preliminary → monthly → revised), `version_seen` is bumped rather than overwritten. The revisions are sometimes the story.
- **Findings are versioned the same way.** Re-running an `--analyse` pass on unchanged data is a no-op at the row level (only `last_confirmed_at` ticks forward). When the underlying data revises, the helper inserts a new row and marks the prior row superseded with a back-pointer. Default queries filter `WHERE superseded_at IS NULL`; the chain itself is the queryable revision history. Each analyser declares its natural key per subkind (e.g. `(hs_group_id, current_end)` for `hs_group_yoy`) and a value-signature drawn from the editorially-meaningful fields plus the analyser's `method` version, so a method bump propagates as a supersede even when numbers don't move.
- **Permalink scheme.** Every finding has a stable `finding/{id}` handle. Spreadsheet outputs include a `link` column that emits a Sheets `HYPERLINK` formula resolved at view-time against `GACC_PERMALINK_BASE`; the findings document renders the same handle as a Markdown link. When a web UI later exists, set the env var and existing exports light up automatically — no backfill.
- **The LLM never computes numbers.** `anomalies.py` does the maths; `llm_framing.py` only narrates the deterministic findings. Every number extracted from LLM output is matched against the typed facts within tolerance — sign-aware first, magnitude-only fallback for cross-clause prose ambiguity. Calendar years, time periods and HS codes are pre-stripped (editorial scaffolding, not facts). On verification failure: REJECT the narrative, log a WARNING, never store. Editorial cost: silence on that group when the LLM hallucinates. Editorial benefit: never confidently wrong. (Real example: qwen3.6 cited "China supplies 93% of permanent magnets" recalled from a Lisa O'Carroll article in training data; not in our facts; correctly rejected.)
- **"Chinese trade" is CN+HK+MO.** Eurostat reports trade routed via Hong Kong and Macao under separate partner codes (HK, MO) because those are independent trade jurisdictions. Editorially they are still Chinese trade. All analysers sum across CN+HK+MO (see `anomalies.EUROSTAT_PARTNERS`); HMRC ingest mirrors the same partner set. For a CN-only spot check against a Soapbox / Merics figure, query `eurostat_raw_rows` directly with `partner = 'CN'`. The `multi_partner_sum` caveat is family-universal — documented once in the brief's Methodology footer, not attached to each finding.
- **Three comparison scopes** (Phase 6.1). `eu_27` (default): EU-27 from Eurostat, excluding UK at all times. `uk`: UK-only from HMRC OTS (Brexit-canonical UK source). `eu_27_plus_uk`: combined sum across both, with a `cross_source_sum` caveat acknowledging the methodological imperfection (different threshold rules, suppression policies, revision cycles between Eurostat and HMRC). Pass `--comparison-scope {eu_27|uk|eu_27_plus_uk}` to `--analyse hs-group-yoy` or `--analyse hs-group-trajectory`. The findings document renders one section per scope present in the active findings.
- **Editorial caveats are first-class data, split two ways.** *Family-universal* caveats (the ones that fire on every finding of a given analyser family — `cif_fob`, `currency_timing`, `classification_drift`, `eurostat_stat_procedure_mix`, `multi_partner_sum`, `general_vs_special_trade`, `transshipment`, `cn8_revision`, `aggregate_composition_drift`, `llm_drafted`) live in the brief's Methodology footer, defined once in `anomalies.UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY`. *Per-finding-variable* caveats (`transshipment_hub`, `low_base_effect`, `low_baseline_n`, `low_kg_coverage`, `partial_window`, `cross_source_sum`, `aggregate_composition`) ride on each finding's `caveat_codes` array. The LLM framing layer hedges its prose using the variable set. Each caveat has a row in the `caveats` table with full editorial guidance.
- **Low-base flagging.** YoY findings whose prior or current 12mo total is below €50M get auto-flagged. The findings document and the spreadsheet both surface a dedicated review section so percentages aren't quoted from tiny denominators without a verifier glance. Threshold is configurable per call (`--low-base-threshold`).
