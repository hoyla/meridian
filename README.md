# gacc

Ingest China–EU/UK trade statistics from all three sides of the customs fence — GACC (China), Eurostat Comext (EU-27), and HMRC OTS (UK, post-Brexit) — into a shared schema, cross-compare them to surface mirror-trade gaps and HS-group trends, and present the most journalistically interesting findings as: a spreadsheet for editorial scanning, a Markdown briefing pack (NotebookLM-ready) opening with LLM-drafted top-lines and unfolding into citation-traceable structured detail. ECB FX rates are pulled automatically (CNY/EUR for GACC, GBP/EUR for HMRC) so all values are comparable in EUR.

For Guardian journalists. Domain-agnostic by design: HS-group definitions live in a journalist-editable `hs_groups` table, so the same machinery investigates EVs, solar PV, rare earths, pork, or whatever the next desk asks about.

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

createdb gacc
psql gacc < schema.sql
```

## Usage

```sh
# GACC scraping
python scrape.py                          # walk all configured GACC index URLs
python scrape.py --url <url>              # one-shot (index OR release URL)
python scrape.py --dry-run                # fetch + parse without DB writes

# Eurostat bulk ingest (one month at a time; bulk file is ~44MB and hosts all partners)
python scrape.py --eurostat-period 2026-03                                       # one month, default partners (CN, HK, MO)
python scrape.py --eurostat-period 2026-03 --partner CN --partner HK --partner MO   # explicit equivalent
python scrape.py --eurostat-period 2026-03 --partner US                          # different partner entirely
python scrape.py --eurostat-period 2026-03 --hs-prefix 87038                     # filter by HS prefix

# HMRC OTS ingest (UK side, post-Brexit canonical source for UK-China trade)
# Pre-requires GBP/EUR FX loaded (see below). Default partners CN+HK+MO.
python scrape.py --hmrc-period 2026-02

# FX rates
python scrape.py --fetch-fx CNY                       # full ECB CNY/EUR history
python scrape.py --fetch-fx GBP --fx-since 2017-01     # GBP/EUR history (pre-req for HMRC ingest)
python scrape.py --fetch-fx CNY --fx-since 2024-01     # from a given month

# Anomaly detection (over already-ingested data)
# Default eurostat_partners is (CN, HK, MO) — the editorially-correct "Chinese trade" envelope including
# the two Special Administrative Regions. Findings carry a multi_partner_sum caveat by default.
# Override with --eurostat-partners CN for the narrower direct-China-only view (matches Soapbox/Merics figures).
python scrape.py --analyse mirror-trade                       # CN-export vs EU-import per partner (default CN+HK+MO)
python scrape.py --analyse mirror-trade --eurostat-partners CN   # narrower CN-only view
python scrape.py --analyse mirror-gap-trends --trend-window 6 --z-threshold 1.5
python scrape.py --analyse hs-group-yoy --flow 1              # imports (CN→EU); --flow 2 for exports
python scrape.py --analyse hs-group-yoy --eurostat-partners CN  # CN-only override (default sums CN+HK+MO)
python scrape.py --analyse hs-group-yoy --hs-group "EV batteries (Li-ion)" --yoy-threshold 0.1
python scrape.py --analyse hs-group-yoy --low-base-threshold 10000000   # lower €50M low-base floor for niche-commodity work
python scrape.py --analyse hs-group-yoy --comparison-scope uk           # UK side only (HMRC; Phase 6.1)
python scrape.py --analyse hs-group-yoy --comparison-scope eu_27_plus_uk  # combined view (cross_source_sum caveat)
python scrape.py --analyse hs-group-trajectory --flow 1       # rolling YoY shape classifier (inherits from yoy)
python scrape.py --analyse hs-group-trajectory --smooth-window 1   # disable smoothing for short-term policy effects
python scrape.py --analyse hs-group-trajectory --comparison-scope uk    # UK trajectories
python scrape.py --analyse gacc-aggregate-yoy --flow 1         # GACC-only YoY for non-EU partner aggregates (ASEAN, RCEP, Belt&Road, Africa, LatAm, world Total)
python scrape.py --analyse gacc-aggregate-yoy --flow 2         # same, China imports from each bloc

# LLM-drafted editorial top-lines (consumes the deterministic findings above)
python scrape.py --analyse llm-framing                        # one narrative per HS group (default qwen3.6:latest)
python scrape.py --analyse llm-framing --hs-group "EV batteries (Li-ion)"
python scrape.py --analyse llm-framing --llm-model qwen3.5:14b      # alternative model

# Re-running an --analyse pass is idempotent. Findings whose values match the
# existing row bump last_confirmed_at; revised values insert a new row and
# mark the old one superseded with a back-pointer. The supersede chain
# captures revisions as queryable history (see Design notes below).

# Spreadsheet export (editorial scanning)
python scrape.py --export-sheet                               # local .xlsx, 7 sheets
python scrape.py --export-sheet --out-path exports/custom.xlsx
python scrape.py --export-sheet --out-format sheets --spreadsheet-id <ID>   # Google Sheets (pending creds)

# Markdown briefing pack (narrative reading; NotebookLM-ready)
python scrape.py --briefing-pack                              # ./exports/briefing-{timestamp}.md
python scrape.py --briefing-pack --briefing-top-n 20          # 20 movers per flow direction
python scrape.py --briefing-pack --briefing-out exports/today.md
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
| `llm_framing.py`   | LLM narrative layer over the deterministic findings. v1 generates one `narrative_hs_group` finding per HS group; numeric-verification gate rejects any number not present in the underlying facts. Default backend: Ollama / `qwen3.6:latest`. |
| `sheets_export.py` | Export findings to local `.xlsx` (shipped) or Google Sheets (stub, pending service-account creds) |
| `briefing_pack.py` | Markdown briefing-pack export — NotebookLM-ready. Opens with LLM-drafted top-lines, unfolds into structured deterministic sections, ends with a Sources appendix tracing every finding back to a third-party URL. |
| `schema.sql`       | Canonical schema (includes lookup-table seeds: hs_groups, country_aliases, caveats, transshipment_hubs, cif_fob_baselines). A fresh setup is `createdb gacc && psql gacc < schema.sql` — no migration replay needed. |
| `migrations.archived-2026-05-09/` | Historical record of the dev migrations that built up to the current schema. Folded into `schema.sql` on the 2026-05-09 clean-state rebuild; kept for reference but no longer applied. |
| `dev_notes/`       | In-repo planning artefacts: review documents and the multi-phase roadmap (`roadmap-2026-05-09.md`) — Phases 1, 2 and 3 done; Phase 4 lists parked statistical-depth items. |
| `exports/`         | Default output directory for generated `.xlsx` and `.md` exports (gitignored) |
| `tests/`           | pytest, live local Postgres. FakeBackend keeps Ollama out of the suite. |

## Design notes

- **Two-source by design.** GACC and Eurostat ingest into a shared `observations` table with a per-cell view, so any cross-source query (mirror-gap, agg-vs-agg) is just a join. The schema anticipated the second source from the start.
- **Provenance discipline.** Raw response bytes for every GACC fetch are stored in `source_snapshots`; raw Eurostat CSV rows are preserved verbatim in `eurostat_raw_rows` (the aggregated `observations` row carries an FK array back to its raw rows, so any aggregation can be audited or re-derived). Findings reference observation_ids so a journalist clicking through any number can land on the underlying row. The briefing pack's Sources appendix lists every third-party URL the brief rests on.
- **Observations are versioned.** When the same (release, dimension) reappears with a different value (preliminary → monthly → revised), `version_seen` is bumped rather than overwritten. The revisions are sometimes the story.
- **Findings are versioned the same way.** Re-running an `--analyse` pass on unchanged data is a no-op at the row level (only `last_confirmed_at` ticks forward). When the underlying data revises, the helper inserts a new row and marks the prior row superseded with a back-pointer. Default queries filter `WHERE superseded_at IS NULL`; the chain itself is the queryable revision history. Each analyser declares its natural key per subkind (e.g. `(hs_group_id, current_end)` for `hs_group_yoy`) and a value-signature drawn from the editorially-meaningful fields plus the analyser's `method` version, so a method bump propagates as a supersede even when numbers don't move.
- **Permalink scheme.** Every finding has a stable `finding/{id}` handle. Spreadsheet outputs include a `link` column that emits a Sheets `HYPERLINK` formula resolved at view-time against `GACC_PERMALINK_BASE`; the briefing pack renders the same handle as a Markdown link. When a web UI later exists, set the env var and existing exports light up automatically — no backfill.
- **The LLM never computes numbers.** `anomalies.py` does the maths; `llm_framing.py` only narrates the deterministic findings. Every number extracted from LLM output is matched against the typed facts within tolerance — sign-aware first, magnitude-only fallback for cross-clause prose ambiguity. Calendar years, time periods and HS codes are pre-stripped (editorial scaffolding, not facts). On verification failure: REJECT the narrative, log a WARNING, never store. Editorial cost: silence on that group when the LLM hallucinates. Editorial benefit: never confidently wrong. (Real example: qwen3.6 cited "China supplies 93% of permanent magnets" recalled from a Lisa O'Carroll article in training data; not in our facts; correctly rejected.)
- **"Chinese trade" is CN+HK+MO by default.** Eurostat reports trade routed via Hong Kong and Macau under separate partner codes (HK, MO) because those are independent trade jurisdictions. Editorially they are still Chinese trade. All four analysers (mirror-trade, hs-group-yoy, hs-group-trajectory, llm-framing) sum across CN+HK+MO by default; pass `--eurostat-partners CN` to get the narrower direct-China-only view (matches Soapbox/Merics single-partner figures). Default-partner findings carry a `multi_partner_sum` caveat as honest annotation.
- **Three comparison scopes** (Phase 6.1). `eu_27` (default): EU-27 from Eurostat, excluding UK at all times. `uk`: UK-only from HMRC OTS (Brexit-canonical UK source). `eu_27_plus_uk`: combined sum across both, with a `cross_source_sum` caveat acknowledging the methodological imperfection (different threshold rules, suppression policies, revision cycles between Eurostat and HMRC). Pass `--comparison-scope {eu_27|uk|eu_27_plus_uk}` to `--analyse hs-group-yoy` or `--analyse hs-group-trajectory`. The briefing pack renders one section per scope present in the active findings.
- **Editorial caveats are first-class data.** Findings carry `caveat_codes` lists — `cif_fob`, `transshipment_hub` (Rotterdam, Antwerp, HK, SG, AE, MX), `multi_partner_sum`, `low_base_effect`, `low_baseline_n`, `low_kg_coverage`, `partial_window`, `cn8_revision`, `llm_drafted`. The briefing pack surfaces them inline; the LLM framing layer hedges its prose accordingly. Each caveat has a row in the `caveats` table with full editorial guidance.
- **Low-base flagging.** YoY findings whose prior or current 12mo total is below €50M get auto-flagged. The briefing pack and Sheets export both surface a dedicated review section so percentages aren't quoted from tiny denominators without a verifier glance. Threshold is configurable per call (`--low-base-threshold`).
