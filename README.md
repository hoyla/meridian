# gacc

Ingest China–EU trade statistics from both sides of the customs fence — GACC (China) and Eurostat Comext (EU) — into a shared schema, cross-compare them to surface mirror-trade gaps and HS-group trends, and surface the most journalistically interesting findings to a spreadsheet, a Markdown briefing pack (NotebookLM-ready), or — eventually — an LLM-framed narrative. ECB FX rates are pulled automatically so all values are comparable in EUR.

For Guardian journalists. Domain-agnostic by design: HS-group definitions live in a journalist-editable `hs_groups` table, so the same machinery investigates EVs, solar PV, rare earths, pork, or whatever the next desk asks about.

## Stack

- Python 3.12+, native venv (no Docker for the dev loop)
- Postgres (local) — schema in `schema.sql`
- Optional `docker-compose.yml` for a Postgres container if you'd rather not install it
- LLM: Ollama (local default) or Gemini, behind a pluggable interface in `llm_framing.py`

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

# Eurostat bulk ingest
python scrape.py --eurostat-period 2026-03                        # one month, partner=CN
python scrape.py --eurostat-period 2026-03 --partner CN --partner US
python scrape.py --eurostat-period 2026-03 --hs-prefix 87038      # filter by HS prefix

# FX rates
python scrape.py --fetch-fx CNY                       # full ECB history
python scrape.py --fetch-fx CNY --fx-since 2024-01     # from a given month

# Anomaly detection (over already-ingested data)
python scrape.py --analyse mirror-trade                       # CN-export vs EU-import per partner
python scrape.py --analyse mirror-gap-trends --trend-window 6 --z-threshold 1.5
python scrape.py --analyse hs-group-yoy --flow 1              # imports (CN→EU); --flow 2 for exports
python scrape.py --analyse hs-group-yoy --hs-group "EV batteries (Li-ion)" --yoy-threshold 0.1
python scrape.py --analyse hs-group-yoy --low-base-threshold 10000000   # lower the €50M low-base floor for niche-commodity work
python scrape.py --analyse hs-group-trajectory --flow 1       # rolling YoY shape classifier

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
| `fx.py`            | ECB monthly-average FX rate fetcher → `fx_rates`        |
| `lookups.py`       | Country-alias resolution, caveat metadata, FX rate lookups |
| `anomalies.py`     | Deterministic anomaly detection: 6 finding subkinds — `mirror_gap`, `mirror_gap_zscore`, `hs_group_yoy` (+ `_export`), `hs_group_trajectory` (+ `_export`) |
| `findings_io.py`   | Idempotent `emit_finding()` helper — append-plus-supersede chain. The canonical write path: every analyser call site declares a natural key + value-fields dict, and the helper handles insert / re-confirm / supersede |
| `llm_framing.py`   | LLM narrative layer over `anomalies` findings (planned) |
| `sheets_export.py` | Export findings to local `.xlsx` (shipped) or Google Sheets (stub, pending service-account creds) |
| `briefing_pack.py` | Markdown briefing-pack export — NotebookLM-ready, with a Sources appendix tracing every finding back to a third-party URL |
| `schema.sql`       | Canonical schema. Live DBs evolve via dated `migrations/*.sql` (idempotent) and `migrations/*.py` (data backfills). |
| `migrations/`      | Dated schema + data migrations. Each one is idempotent and re-runnable. |
| `dev_notes/`       | In-repo planning artefacts: review documents and the multi-phase roadmap (`roadmap-2026-05-09.md`) currently driving Phase 2 work. |
| `exports/`         | Default output directory for generated `.xlsx` and `.md` exports (gitignored) |
| `tests/`           | pytest, live local Postgres                             |

## Design notes

- **Two-source by design.** GACC and Eurostat ingest into a shared `observations` table with a per-cell view, so any cross-source query (mirror-gap, agg-vs-agg) is just a join. The schema anticipated the second source from the start.
- **Provenance discipline.** Raw response bytes for every GACC fetch are stored in `source_snapshots`; raw Eurostat CSV rows are preserved verbatim in `eurostat_raw_rows` (the aggregated `observations` row carries an FK array back to its raw rows, so any aggregation can be audited or re-derived). Findings reference observation_ids so a journalist clicking through any number can land on the underlying row. The briefing pack's Sources appendix lists every third-party URL the brief rests on.
- **Observations are versioned.** When the same (release, dimension) reappears with a different value (preliminary → monthly → revised), `version_seen` is bumped rather than overwritten. The revisions are sometimes the story.
- **Findings are versioned the same way.** Re-running an `--analyse` pass on unchanged data is a no-op at the row level (only `last_confirmed_at` ticks forward). When the underlying data revises, the helper inserts a new row and marks the prior row superseded with a back-pointer. Default queries filter `WHERE superseded_at IS NULL`; the chain itself is the queryable revision history. Each analyser declares its natural key per subkind (e.g. `(hs_group_id, current_end)` for `hs_group_yoy`) and a value-signature drawn from the editorially-meaningful fields plus the analyser's `method` version, so a method bump propagates as a supersede even when numbers don't move.
- **Permalink scheme.** Every finding has a stable `finding/{id}` handle. Spreadsheet outputs include a `link` column that emits a Sheets `HYPERLINK` formula resolved at view-time against `GACC_PERMALINK_BASE`; the briefing pack renders the same handle as a Markdown link. When a web UI later exists, set the env var and existing exports light up automatically — no backfill.
- **The LLM never computes numbers.** `anomalies.py` does the maths; `llm_framing.py` (planned) will only cluster and narrate the deterministic findings, with every numeric claim validated back to a source row before storage.
- **Low-base flagging.** YoY findings whose prior or current 12mo total is below €50M get auto-flagged. The briefing pack and Sheets export both surface a dedicated review section so percentages aren't quoted from tiny denominators without a verifier glance.
