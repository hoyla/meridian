# Periodic findings-export runs — design (2026-05-11)

The "what's new since last time" loop the project has always intended,
implemented in a deployment-agnostic way so it can run as a Claude Code
Routine today and migrate to a hosted scheduler later without code
changes.

## Three-layer separation of concerns

The single load-bearing idea: keep the pipeline, the scheduler, and the
delivery channel as three independent layers.

### Layer 1 — the pipeline (this repo, `python scrape.py --periodic-run`)

Pure compute. Reads the DB, runs all analysers, writes a findings-export
bundle to a folder, exits. Knows nothing about scheduling or delivery.

Today this is `periodic.run_periodic()` invoked via the `--periodic-run`
CLI flag. The function:

1. **Idempotency check.** Queries `latest_eurostat_period()` (the most
   recent `releases.period` for `source='eurostat'`) and
   `latest_recorded_data_period(trigger='periodic_run')` (the most
   recent `brief_runs.data_period` from a periodic-run cycle). If the
   former is no fresher than the latter, exits cleanly with a no-op
   message. `--force` overrides.
2. **Re-runs every analyser kind across all scope/flow combos.** Each
   analyser is per-row idempotent via the supersede chain (unchanged
   rows are confirmed; shifted rows produce new rows + supersedes).
   The supersede chain becomes the diff against the previous cycle.
3. **Re-runs `--analyse llm-framing`.** Each `narrative_hs_group`
   finding is regenerated against the latest typed facts. The verifier
   rejects hallucinations as before. `--skip-llm` omits this step
   (useful when Ollama is unavailable).
4. **Writes the findings-export bundle** (`findings.md` + `leads.md` +
   `data.xlsx`) into `exports/YYYY-MM-DD-HHMM/` with
   `trigger='periodic_run'` stamped on the `brief_runs` row. The
   bundle's findings.md uses the three-tier structure shipped in
   commit `abd07ec` — Tier 1 (what's new) leads, Tier 2 (state of play)
   sits in the middle, Tier 3 (full detail) at the bottom.

Output to stdout: the absolute path of the new `findings.md`, or an
empty string if the call was a no-op. A scheduler wrapper can branch
on that.

The pipeline does **not** fetch data. It works against whatever's in
the DB. Fetching is the scheduler's responsibility (typically a
`python scrape.py --eurostat-period YYYY-MM` call before
`--periodic-run`). This keeps "did the network fail?" and "did the
analyser fail?" as distinct concerns.

### Layer 2 — the scheduler

What invokes Layer 1 on a schedule. Pluggable.

**Today (pre-release, Luke's laptop / desktop)**: a Claude Code Routine
created via the `/schedule` skill. The routine fires daily; the prompt
tells Claude to run the periodic cycle. See "Routine setup" below.

**Tomorrow (hosted)**: a GHA workflow, AWS EventBridge, a hosted cron
job — any of them invoking the same CLI. Migration is configuration,
not code.

**Cadence**: Eurostat releases month M's data ~6-8 weeks after M-end.
A daily fire is wasteful but harmless (idempotent no-op on most days,
real cycle on the day Eurostat ships). A weekly fire (e.g. Mondays
07:00) is more economical and still catches the release within a week.
A monthly fire targeted at the typical Eurostat release date works but
is brittle if Eurostat slips its publication.

Recommended for v1: **daily, accepting the idempotent no-op cost**.
Simpler to reason about, never misses a release by more than 24 hours.

### Layer 3 — delivery

How the export bundle reaches the journalist. Pluggable.

**Today**: manual. Luke checks the export folder; emails / shares /
copies the bundle to Lisa O'Carroll as needed. The routine prints the
path of the new export to its log; Luke reads it.

**Tomorrow (still under his account)**: the routine itself can attach
delivery — e.g., the prompt could be "run the periodic cycle, and if a
new export was written, email a summary to lisa.ocarroll@theguardian.com
with the findings.md attached". The mechanism stays inside the same
account-bound Routine machinery.

**Later (hosted, multiple consumers)**: a notification service in the
deployment env (email/Slack/webhook) reads the new export path from
the pipeline's stdout and dispatches.

This layer is **not built yet** — Luke is doing it manually for now.
That's an explicit design decision, not technical debt: don't decide
"email vs Slack vs Drive" before the first few cycles have actually
landed in a journalist's inbox.

## Idempotency semantics

The cycle's identity is `(trigger, data_period)`:

- **`trigger='periodic_run'`** — produced by Layer 1's `--periodic-run`
  path. These are the canonical subscriber-facing exports.
- **`trigger='manual'`** — any ad-hoc render (`python scrape.py
  --briefing-pack`, an on-demand bundle for a new journalist, a test).
  These do NOT advance the global cycle.
- **`data_period`** — the most recent Eurostat `releases.period` at the
  time of the render. Stamped onto every `brief_runs` row.

The idempotency rule: run `--periodic-run` again with the same
`latest_eurostat_period()` and no new export is written.

This solves the three subscriber-experience scenarios discussed
2026-05-11:

1. **Scheduled subscriber** — gets one export per Eurostat release
   (a `periodic_run` row).
2. **New mid-cycle joiner** — can ask for an on-demand `manual`
   render at any time; gets the current snapshot without altering
   the global cycle for anyone else. Their next "regular" export
   is the next `periodic_run`.
3. **Original subscriber unaffected by new joiner** — sequence stays
   `periodic_run` #N → #N+1 → #N+2; no `manual` renders interleaved.

## Routine setup (the current deployment, Luke's machine)

The Routine is created via the `/schedule` skill (or the
`scheduled-tasks` MCP tool). The body is roughly:

```text
Run the gacc periodic findings-export cycle.

Working directory: /Users/luke_hoyland/Code/Other_GitHub/gacc

Steps:
1. cd into the working directory and activate the venv:
   source .venv/bin/activate
2. Fetch the latest available Eurostat period if it's newer than what
   we already have. Compute the candidate as max(releases.period for
   source='eurostat') + 1 month. If it's <= today - 6 weeks, try to
   fetch it:
     python scrape.py --eurostat-period YYYY-MM --partner CN --partner HK --partner MO
   (If Eurostat returns 404 / empty, that's fine — exit cleanly.)
3. Run the periodic-cycle orchestrator:
     python scrape.py --periodic-run
4. If a new export folder was created, write a brief summary message
   to the chat noting the new export path and any material Tier 1
   changes. Don't email anyone — Luke is handling delivery manually.

Treat any subprocess failure as a soft alert: log and continue, don't
retry aggressively. Network errors against Eurostat are common when
the period hasn't published yet and should not be treated as failures.
```

Schedule: daily at 07:00 UTC. Eurostat publishes during the European
working day; firing in the early morning maximises the chance of
catching same-day releases before Luke sits down.

## Migration: laptop → desktop (under the same account)

The current expectation (Luke, 2026-05-11): the project moves from
this laptop to a desktop computer soon, both under the same account.
The Routine is account-bound; the local environment is the only
device-specific piece.

Steps to migrate:

1. **On the desktop**: `git clone https://github.com/hoyla/meridian.git`,
   `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`.
   Install Postgres (brew on macOS) and Ollama (`brew install ollama`).
2. **Dump the live DB on the laptop**:
   `pg_dump postgresql://localhost:5432/gacc > /tmp/gacc-dump.sql`.
   Copy to the desktop.
3. **Restore on the desktop**:
   `createdb gacc && psql gacc < /tmp/gacc-dump.sql`.
   (Same for `gacc_test` if you plan to run tests there.)
4. **Copy `.env`** (or recreate it from `.env.example`). `DATABASE_URL`
   should point at the desktop's Postgres; other endpoints (Ollama,
   Eurostat) are the same.
5. **Verify**: `python scrape.py --periodic-run --force --skip-llm`
   on the desktop. Should produce a new export with the latest
   `data_period` from the dumped DB.
6. **Routine sanity-check**: trigger the existing Routine manually
   (the `/schedule` skill allows this) and confirm it fires against
   the desktop env. If the Routine was created with a hardcoded
   working directory pointing at the laptop, update it.
7. **Cut over**: stop the laptop Routine cycle (or let both run for
   one cycle to confirm the desktop produces the same output — they
   would write to different DBs but with identical analyser outputs
   if the DB dump was complete).

The reason this is straightforward: nothing in the pipeline knows
about the host. Database connection is via env var; output directory
is a parameter; LLM endpoint is configurable.

## What's deferred

- **Automated delivery.** Email/Slack/Drive can come once we know what
  the first few cycles feel like. Don't pick a vector before the
  problem is fully shaped.
- **Hosted deployment.** AWS Fargate, RDS, etc. Following the
  fuel-finder precedent (per `roadmap.md`). Trigger: a desk journalist
  wants the tool independently of Luke's machine.
- **Auto-fetch in the periodic-run.** The current Routine prompt does
  the fetch and the pipeline does the analysis separately. A combined
  `--periodic-run --auto-fetch` flag is possible but introduces a
  network dependency into the analyser pipeline; the current split is
  cleaner.
- **Per-subscriber state.** Multiple subscribers with personal "last
  seen" tracking is a future hosted-service concern. The current
  global cycle works for Lisa today and for any second journalist
  who joins via the same delivery channel.

## Code references

- [`periodic.py`](../periodic.py) — `run_periodic()` and
  `PeriodicRunResult`.
- [`briefing_pack.py`](../briefing_pack.py) —
  `latest_eurostat_period()`, `latest_recorded_data_period()`,
  `_record_brief_run()` (stamps `data_period` + `trigger`),
  `export(trigger=...)`.
- [`schema.sql`](../schema.sql) — `brief_runs` table with the new
  `data_period DATE` + `trigger TEXT DEFAULT 'manual'` columns.
- [`scrape.py`](../scrape.py) — `--periodic-run`, `--force`,
  `--skip-llm` flags.
