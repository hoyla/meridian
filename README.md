# gacc

Monitor the China General Administration of Customs (GACC) statistical releases, detect updates, parse them into structured observations, surface deterministic anomalies, and have an LLM frame the most journalistically interesting top-lines.

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
python scrape.py                  # walk the seed URL list
python scrape.py --url <one-url>  # one-shot
python scrape.py --dry-run        # fetch + parse without DB writes
```

## Layout

| File               | Responsibility                                          |
|--------------------|---------------------------------------------------------|
| `scrape.py`        | CLI entry point + run orchestration                     |
| `api_client.py`    | HTTP fetch, hashing, link discovery                     |
| `parse.py`         | HTML / PDF → structured observations                    |
| `db.py`            | Postgres access (psycopg2, no ORM)                      |
| `anomalies.py`     | Deterministic stats: MoM / YoY / z-score / rank-shift   |
| `llm_framing.py`   | LLM narrative layer over `anomalies` findings           |
| `sheets_export.py` | Push observations + findings to Google Sheets           |
| `schema.sql`       | Canonical initial schema (move to Alembic on 1st change)|
| `tests/`           | pytest, live local Postgres                             |

## Design notes

- Raw response bytes are stored in `source_snapshots` for every fetch — full audit trail.
- Observations are versioned: when the same (release, dimension) reappears with a different value (preliminary → monthly → revised), `version_seen` is bumped rather than overwritten. The revisions are sometimes the story.
- The LLM never computes numbers. `anomalies.py` does the maths; `llm_framing.py` only clusters and narrates the deterministic findings, with every numeric claim validated back to a source row before storage.
