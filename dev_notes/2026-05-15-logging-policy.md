# Logging policy

Where to put "what just happened" information so future-you / a
journalist / a hosted-deployment log shipper can find it cleanly. Agreed
2026-05-15 after the LLM verifier rejection investigation showed how
much editorial signal was being lost to terminal scrollback.

## Two layers, clean boundary

### Layer 1 — editorial / audit events → focused DB tables

Each silent system decision that matters for editorial integrity or
post-hoc debugging gets its own small, focused DB table. Not one big
generic events table — different concerns have different shapes,
retention horizons, and consumers, and a single table would collapse the
schema into something nobody can query against.

Already in the project:

- **`routine_check_log`** — per-source telemetry from the daily Routine
  (Eurostat / HMRC / GACC checks), plus `_routine` lifecycle bookends.
  Surface: `python scrape.py --source-status`.
- **`brief_runs`** — per-export-bundle row with `trigger`, `data_period`,
  `output_path`. Drives the periodic-run idempotency check.
- **`findings`** — append-plus-supersede chain. The chain *is* the
  audit trail; default queries filter `WHERE superseded_at IS NULL`.

Added 2026-05-15:

- **`llm_rejection_log`** — every LLM-framing output that fails parse /
  numeric verification. Captures raw output + reason + detail + model.
  Surface: `python scrape.py --llm-rejections [--limit N]`.
- **`periodic_run_log`** — one row per `--periodic-run` invocation
  (whether action_taken or no-op). Captures `data_period`, `reason`,
  `analyser_counts`, `duration_ms`. Surface: `python scrape.py
  --periodic-history [--limit N]`.
- **`findings_emit_log`** — one row per `detect_X()` analyser invocation,
  capturing the analyser method version, comparison scope, flow, and the
  emit-counts dict (`new` / `confirmed` / `superseded` / `skipped_*`).
  Surface: `python scrape.py --emit-history [--limit N]`.

### Layer 2 — operational diagnostics → Python `logging` to stderr

What's already in the codebase. `logging.basicConfig(level=INFO, ...)`
in [scrape.py](../scrape.py). Module-level `log = logging.getLogger(__name__)`
in each file. Free-form `log.info` / `log.warning` / `log.error` /
`log.exception` for "what happened during this run."

Captured by:

- Terminal scrollback for interactive invocations.
- The Routine's chat reply for scheduled runs (the Mac app preserves
  the conversation).

**Not** captured anywhere permanently. That's the trade — Layer 2 is
ephemeral by design.

## Heuristic for new logging decisions

> Would a journalist, you in six months, or future-Claude ever ask
> *"what was rejected / decided / changed in cycle N?"*

- **Yes** → Layer 1. New focused DB table, helper module, CLI surface,
  tests. Follow the [routine_log.py](../routine_log.py) pattern.
- **No, this is a diagnostic for *this* run only** → Layer 2. Just
  `log.info` / `log.warning`. No DB write.

Borderline cases:

- **Sustained per-row chatter** (e.g. `emit_finding: superseded 57302
  -> 65112`): log line, NOT a DB row per emit. The aggregate goes in
  `findings_emit_log` per analyser invocation.
- **An exception that the code recovers from** (e.g. `--eurostat-period`
  404 when the period hasn't published yet): log line + a
  `routine_check_log` row with result='no_change'. The exception
  itself is operational; the source-check outcome is editorial.
- **A migration / schema event**: log line. Migrations are version-
  controlled in `migrations/`; no DB-side audit needed.

## What we deliberately don't have

- **Log files.** Terminal scrollback + Layer-1 DB tables cover us
  pre-deployment. Adding files would force rotation, retention, disk-
  management concerns we don't need yet.
- **A unified "events" table.** Tried in many codebases, always ends as
  a schemaless JSONB column nobody trusts. Focused tables stay
  queryable.
- **Per-emit_finding rows.** ~158K emits per --periodic-run cycle.
  Aggregated counts in `findings_emit_log` give the same editorial
  signal without exploding the DB.

## Future: hosted deployment

When the project moves off Luke's laptop:

1. Swap `logging.basicConfig`'s default formatter for a JSON formatter
   (one-line change in [scrape.py](../scrape.py)). Layer 2 becomes
   structured, ELK/CloudWatch-shippable.
2. The Layer 1 tables stay as-is. They're the editorial-audit layer;
   they should never be ephemeral, regardless of deployment model.
3. The deployment's log aggregator subsumes terminal scrollback as the
   Layer-2 sink.

No code change to the Layer 1 / Layer 2 boundary itself. The boundary
just survives the move.

## Naming convention

For Layer 1 tables: `{concern}_log` if the rows are append-only
events; `{concern}_runs` if rows represent invocations of a long-running
process. We currently have one of each (`routine_check_log`, `brief_runs`,
`scrape_runs`). Future additions follow the same shape.
