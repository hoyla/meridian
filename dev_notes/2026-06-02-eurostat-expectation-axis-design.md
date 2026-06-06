# Eurostat polling: replace the `not_yet_eligible` fetch-gate with an expectation axis

**Status:** ✅ implemented 2026-06-02 (branch `eurostat-expectation-axis`, off
`main`). All three
forks taken at their fuller option: Eurostat **and** HMRC; formula **+**
hand-entered exact 2026 dates; a new `--probe-source` command (logic moved out
of SKILL.md prose into the pipeline). Additive migration applied to both
`gacc_test` and live `gacc`. Pairs with `2026-05-11-periodic-runs-design.md`.
**Date:** 2026-06-02

---

## Implementation status (2026-06-02)

Landed:

- **`release_calendar.py`** (new) — pure expectation engine. Per-source
  publication calendars with hand-entered exact 2026 dates + a
  `period_close + lag_days` formula fallback + a grace window;
  `classify_expectation(source, period, today) → none_expected | due | overdue`
  (None for gacc). The exact dates were lifted from the **authoritative G.3
  PDF** and cross-checked: the PDF's `19 May 2026 → March 2026 ref` matches our
  own DB row (`first_seen 2026-05-19`) exactly. HMRC dates from the uktradeinfo
  release calendar (HMRC publishes ~3 days before Eurostat for the same ref
  month). Eurostat table covers ref months 2025-11..2026-10; HMRC 2026-04..06
  authoritative, formula beyond.
- **`eurostat.bulk_file_exists(period)`** — HEAD probe keyed on
  `Content-Disposition` / `application/octet-stream` of the *final*
  (post-redirect) response. Re-confirmed live 2026-06-02: 202603 present,
  202604/202605 absent. Wired into `scrape_eurostat`, so an unpublished month
  is now a clean no-op instead of the old failed-scrape_run + py7zr traceback.
  The `~10 weeks` comment (line 26) is fixed.
- **`routine_check_log.expectation`** column (additive migration
  `migrations/2026-06-02-routine-check-log-expectation.sql` + `schema.sql`).
  The result CHECK still permits `not_yet_eligible` so historical rows stay
  valid (principle 4 — append-only), but the app no longer writes it.
- **`scrape.py --probe-source <eurostat|hmrc|gacc>`** — one command does
  candidate → probe → ingest → classify → log. `scrape_eurostat`/`scrape_hmrc`
  now return an `IngestOutcome` (success/absent/empty/skipped/failed); the
  empty-period guard stops an unpublished HMRC month from creating a release
  row that would falsely advance `max(period)`.
- **Period-closed floor** (added on Luke's point, same session): the probe
  skips the network call entirely when `today < period_close(candidate)` —
  data for a month that hasn't ended cannot exist, so it's a guaranteed no-op
  (logged `no_change × none_expected`). This is a hard logical floor, *not* a
  return of the 5-week heuristic: we still probe through the
  `[period_close → expected_publish]` window, so early arrivals are caught and
  the lag stays un-censored. Mostly relevant to HMRC, whose OData API can be
  current enough that `max+1` lands on the not-yet-closed month; Eurostat's
  ~46-day lag already guarantees a closed candidate.
- **`routine_log`** carries the expectation through `log_check`,
  `SourceStatus`, the rollup, and `--source-status` (new `expectation` column
  + an `OVERDUE:` alert line answerable independently of "did anything land").
- **Tests**: `test_release_calendar.py` (pure), `test_probe_source.py`
  (orchestration, network mocked), updated `test_routine_log.py`. Full suite
  **372 passed, 5 skipped** against `gacc_test`.
- **Routine SKILL.md** rewritten: steps 2-4 collapse to three `--probe-source`
  calls; the 5-week gate / `not_yet_eligible` prose is gone.

**Migration applied** to live `gacc` 2026-06-02 (additive `ADD COLUMN
expectation TEXT` + CHECK, idempotent). `--source-status` confirmed clean
against live; historical `not_yet_eligible` rows were left untouched (which is
why the result CHECK still permits that value).

**Deferred (not done):** populating `releases.publication_date` from the
calendar (the brief's "consider"), and multi-period catch-up in a single run
(the probe advances one candidate per fire; an overdue flag surfaces a
backlog, and it self-heals over days).
**Origin:** fell out of a scheduled `meridian-daily-periodic-run` while
debugging why Eurostat logged `not_yet_eligible`. The investigation below was
done live against the real source; the numbers are verified, not assumed.

---

## TL;DR of the change

Today the daily routine decides *whether to fetch* Eurostat/HMRC based on a
hardcoded "5 weeks past period close" gate, logging `not_yet_eligible` when it
skips. That gate conflates two independent things:

1. **Whether to bother fetching** — which is pointless, because fetching is
   harmless (idempotent, see below), and
2. **Whether data is expected yet** — which is real, but is currently
   expressed as a guessed threshold instead of the source's actual schedule.

Proposed model: **always probe after a period closes**, and split the signal
into two orthogonal axes on `routine_check_log`:

- `result` — the objective outcome: `new_data` | `no_change` | `error`
- `expectation` — derived from the publication calendar:
  `none_expected` | `due` | `overdue`

So a quiet expected gap is `no_change` × `none_expected` (ignore), a missing
release past its scheduled date is `no_change` × `overdue` (**the one row a
human should look at**), and a release that shows up late is `new_data` ×
`overdue` (also interesting — it arrived, but late). A single sub-typed enum
(`no_change (overdue)`) would flatten the outcome/expectation distinction and
lose that; keep them as separate columns.

`not_yet_eligible` goes away as a result value — we always look now.

---

## Why this is safe: fetching is harmless

The pipeline is idempotent (CLAUDE.md journalism principle 5). A premature
Eurostat fetch that finds no file is a no-op: it logs and continues, corrupts
nothing. So there is no operational reason to gate the fetch — the only thing
the gate ever bought was *signal legibility*, and the calendar gives us that
more precisely. Removing the gate also **un-censors our lag measurement**: with
the gate we never look before week 5, so we can never observe whether Eurostat
publishes earlier. Always-checking lets the real distribution accumulate.

## The evidence (verified 2026-06-02)

- **Our partners (CN, HK, MO) are all extra-EU.** Eurostat publishes
  **extra-EU** detailed trade data **46 days after the reference month ends**,
  at the same time as the short-term indicators. Intra-EU lags ~1 month more
  (~2.5 months). Source: Eurostat "International trade in goods – information
  on data" and the SIMS metadata `ext_go_detail_sims.htm`.
- **The `~10 weeks` comment at `eurostat.py:26` is wrong** for our use case —
  that's the intra-EU / annual figure. It should say ~46 days (~6.6 weeks) for
  extra-EU. Fix this regardless of whether the rest of the redesign lands.
- **There is an authoritative calendar.** The "G.3 Trade in goods Publication
  Calendar" (annual PDF:
  https://ec.europa.eu/eurostat/documents/6842948/10520689/Release+Calendar )
  marks, per month, the exact date of *"Publication of the monthly news release
  & update of Comext data/Bulk download files (at 11:00 AM)"* (purple) and the
  *"most recent reference month for which data are published"* (green). The
  purple date is **literally our fetch target** — when `full_v2_YYYYMM.7z`
  refreshes. The 2026 PDF shows e.g. **March 2026 → published ~15 May 2026**.
- **Corroboration from our own data:** the only non-backfill Eurostat row is
  2026-03, `first_seen_at = 2026-05-19` — 4 days after the calendar's ~May 15
  publication, and 49 days after 31 March (≈ the 46-day rule). Everything older
  shares `first_seen_at = 2026-05-10`, a single backfill, so it carries no lag
  information. `releases.publication_date` is NULL for every Eurostat row — the
  column exists but has never been populated.

## Eurostat existence-probe gotcha (important for implementation)

The bulk endpoint **never returns 404**. A request for a not-yet-published
period (`full_v2_202604.7z`, `..._202605.7z`) returns **HTTP 200 with an HTML
error body**. The real discriminator between "file exists" and "doesn't exist
yet" is the **`Content-Disposition: attachment` / `Content-Type:
application/octet-stream`** response headers, which only the real `.7z` carries.

This means:
- A cheap **HEAD-style existence probe** is viable (don't download ~25 MB just
  to discover the file isn't there) — but it must key on the headers, **not**
  the status code.
- The task file's current assumption ("404 / empty response → `no_change`") is
  technically wrong and should be corrected wherever it drives logic.

---

## Where the calendar dates come from

Two options; the recommendation is to do both — formula as the engine, PDF as
the annual sanity-check, since the PDF is human-readable and published once a
year (not a clean API):

1. **Formula (engine):** `expected_publish = period_close + 46 days`, where
   `period_close = (period + 1 month) - 1 day`. Drives `none_expected` vs
   `due`/`overdue` with a small grace window (suggest ~3–5 days to absorb
   weekend/holiday shifts; the 2026 dates land mid-month, shifted off
   weekends).
2. **Calendar (authority for exact dates):** the ~12 purple dates per year from
   the G.3 PDF. Either hand-entered annually into a small table/constant, or
   parsed. Lets `overdue` fire on the *real* scheduled date rather than a
   formula approximation. Treat as refinement, not a blocker.

Do **not** try to recover historical publication dates from Eurostat — the
bulk files are rebuilt on every refresh cycle, so their listing dates all read
"≈ now" and carry no first-publication signal. The real lag sample can only be
built going forward (or read from the calendar).

---

## Change surface (verified file references)

- `routine_log.py` — `result` CHECK list at ~line 45 (currently
  `new_data`/`no_change`/`not_yet_eligible`/`error`); docstring at ~line 11
  describes the "5 weeks" gate. Add the `expectation` column + its CHECK.
- `scrape.py` — second copy of the same enum at ~line 565; the `--log-check`
  CLI surface; ideally **move the expectation classification out of the routine
  SKILL.md prose and into the pipeline here**, so the routine just calls a
  command and the code decides `none_expected`/`due`/`overdue`. Add a
  `--log-expectation` arg (or compute it server-side from `period` + source).
- `eurostat.py` — fix the `~10 weeks` comment (line 26); add/confirm a
  header-based existence probe (HEAD on the bulk URL, key on
  `Content-Disposition`) used before the full download.
- DB schema — `routine_check_log` gets an `expectation` column (+ CHECK). This
  is a migration; follow the project's append-only/idempotent conventions.
  Consider finally populating `releases.publication_date` (currently always
  NULL for Eurostat) from the calendar so lag is queryable later.
- `--source-status` renderer — surface the expectation axis; make "anything
  overdue?" answerable independently of "did anything land?". This is the
  header-line debug view Luke hits after a run.
- The routine task file
  (`~/.claude/scheduled-tasks/meridian-daily-periodic-run/SKILL.md`) — rewrite
  the Eurostat/HMRC steps: drop the `not_yet_eligible` skip, always probe,
  let the pipeline classify. Keep GACC as-is.

## Scope notes

- **HMRC**: same shape applies (UK trade-stats has its own release calendar).
  Worth doing in the same change for symmetry, but Eurostat is the one that
  actually advances the export cycle, so it's the priority.
- **GACC**: unchanged. No candidate-period concept (it's an index walk), so it
  has no expectation axis — `no_change` stays as-is.
- **Journalism principles (CLAUDE.md):** this is telemetry/orchestration, not
  source data, so principles 3/4 (never mutate, append-only) aren't directly at
  stake — but the migration should be additive (new column, no rewrite of
  existing `routine_check_log` rows) and the probe must stay idempotent.

## Verification

- Unit: a not-yet-published period probe classifies `none_expected` (today <
  scheduled); a past-due missing period classifies `overdue`; a present file
  drives a full ingest and `new_data`.
- Header-probe test against live endpoint: `full_v2_202603.7z` returns
  `Content-Disposition`; `full_v2_202605.7z` (unpublished) returns 200 **without**
  it. (Confirmed by hand 2026-06-02 — re-confirm, periods will have advanced.)
- `--source-status` shows the new axis and flags overdue independently.
- Existing tests: `tests/test_routine_log.py` covers the enum — update for the
  new column.
