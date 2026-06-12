# Repo structure proposal — 2026-06-12

Status: **proposed, not scheduled**. Written at the iteration-2 pause
(see `roadmap.md` § "Journalist-usability arc"). Trigger to pick up:
after the next real briefing pack has been delivered and the quote
audit done — the restructure is mechanical but touches every import,
and the unattended daily Routine should not be the first thing to
exercise it.

## The problem

The repo root holds ~21 Python modules side by side. For the two of
us that's fine — the README's Layout table maps every file — but the
repo is public, and a newcomer landing on the root sees `parse.py`,
`api_client.py`, `lookups.py`, `routine_log.py`, `periodic_run_log.py`
… with no visual grouping of what belongs to which concern. The names
also under-describe: `api_client.py` and `parse.py` are GACC-specific
(the other two sources have their own named modules), which nothing
at root level signals.

There is also no `pyproject.toml` — imports work because everything
runs from the repo root. That's a packaging smell for a public repo
and makes the module sprawl worse (no package name to anchor a mental
model on).

## Proposed layout

One `meridian/` package, subpackaged by concern; a thin `scrape.py`
shim stays at root so every documented command — and the daily
Routine's pre-approved `python scrape.py …` invocations — keep
working unchanged.

```
meridian/
  __init__.py
  cli.py                 ← today's scrape.py (argparse + orchestration)
  periodic.py            ← periodic-run pipeline
  sources/               ← one module per upstream source
    gacc_api.py          ← was api_client.py (HTTP, hashing, discovery)
    gacc_parse.py        ← was parse.py (HTML/PDF → observations)
    eurostat.py
    hmrc.py
    fx.py
    release_calendar.py
  store/                 ← Postgres access + write paths
    db.py
    lookups.py
    findings_io.py
  analysis/
    anomalies.py
    llm_framing.py
    hypothesis_catalog.py
  audit/                 ← the four append-only log surfaces
    routine_log.py
    periodic_run_log.py
    findings_emit_log.py
    llm_rejection_log.py
  export/
    briefing_pack/       ← moves wholesale (sections/, templates/ intact)
    sheets_export.py
    provenance.py
scrape.py                ← shim: from meridian.cli import main; main()
schema.sql               ← stays at root (setup docs reference it)
pyproject.toml           ← new; enables pip install -e ., pins deps
migrations/  docs/  dev_notes/  scripts/  tests/  exports/
```

Renames beyond moves: only `api_client.py → sources/gacc_api.py` and
`parse.py → sources/gacc_parse.py` — the single most clarifying
change for outside readers. Everything else keeps its name.

## Compatibility notes (the reasons this is half a day, not an hour)

- **The daily Routine** invokes `python scrape.py …` with pre-approved
  permissions. The root shim keeps those commands byte-identical — no
  re-approval, no broken unattended runs.
- **`python -m briefing_pack.drive_export`** (the current manual Drive
  upload entry) becomes `python -m meridian.export.briefing_pack.drive_export`
  — OR lands behind the `--upload-to-drive` CLI flag first (already on
  the roadmap), which removes the `-m` invocation entirely. Prefer the
  latter ordering: wire the CLI flag, then restructure.
- **`git mv`** for every move so `git log --follow` keeps history.
- **Tests**: imports update mechanically; `tests/` itself stays put.
- **Docs**: README Layout table and `docs/architecture.md` re-point to
  package paths (the table gets *shorter* — subpackages summarise).

## What was considered and rejected

- **Documentation-only** (keep flat, rely on the README table): zero
  churn but doesn't fix the first-impression problem or the packaging
  gap; the root listing IS the first thing a visitor reads.
- **Partial grouping** (move only the four `*_log.py` audit modules):
  touches almost as many imports as the full move for a fraction of
  the clarity.

## Estimate

Half a day: mechanical `git mv` + import rewrites + pyproject + docs,
then the full suite and one `docx=True` preview export. Schedule on a
day with headroom to watch the next morning's Routine run.
