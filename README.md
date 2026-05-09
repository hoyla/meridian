# gacc

Ingest China–EU trade statistics from both sides of the customs fence — GACC (China) and Eurostat Comext (EU) — into a shared schema, cross-compare them to surface mirror-trade gaps and HS-group trends, and have an LLM frame the most journalistically interesting findings. ECB FX rates are pulled automatically so all values are comparable in EUR.

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
python scrape.py --analyse mirror-trade
python scrape.py --analyse mirror-gap-trends --trend-window 6 --z-threshold 1.5
python scrape.py --analyse hs-group-yoy --hs-group "Electric vehicles" --yoy-threshold 0.1
python scrape.py --analyse hs-group-trajectory --analyse-period 2026-03 --flow 1

# Spreadsheet export
python scrape.py --export-sheet                               # local .xlsx (default)
python scrape.py --export-sheet --out-path exports/custom.xlsx
python scrape.py --export-sheet --out-format sheets --spreadsheet-id <ID>
```

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
| `anomalies.py`     | Deterministic anomaly detection (mirror-gap implemented; YoY/MoM/rank-shift planned) |
| `llm_framing.py`   | LLM narrative layer over `anomalies` findings           |
| `sheets_export.py` | Export findings to local `.xlsx` (primary) or Google Sheets (stub) |
| `schema.sql`       | Canonical initial schema (move to Alembic on 1st change)|
| `exports/`         | Default output directory for `.xlsx` exports            |
| `tests/`           | pytest, live local Postgres                             |

## Design notes

- Raw response bytes are stored in `source_snapshots` for every fetch — full audit trail.
- Observations are versioned: when the same (release, dimension) reappears with a different value (preliminary → monthly → revised), `version_seen` is bumped rather than overwritten. The revisions are sometimes the story.
- The LLM never computes numbers. `anomalies.py` does the maths; `llm_framing.py` only clusters and narrates the deterministic findings, with every numeric claim validated back to a source row before storage.
