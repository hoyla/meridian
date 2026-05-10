# Roadmap — outstanding work

What's still open. For history of what shipped, see
[`history.md`](history.md). For the design rationale that drove
the original Phase 1–6 plan, look at the git log around
`8f18e68`–`5d0e23e` (2026-05-09 to 2026-05-10).

## Near-term (likely next session)

### Periodic analyser runs (infrastructure)

The single most-requested follow-on. Today's findings all have
`created_at = today` because of the Phase 5 clean-state rebuild
plus iterative method-version bumps. The supersede chain isn't
yet a historical record of "what we said last month".

A monthly cron (or GitHub Action) that re-runs the analyser
pipeline after each Eurostat release would unlock:

- The trajectory-shape backtest (currently sketched but flagged
  forward in `out-of-sample-backtest-2026-05-10.md`).
- Honest "Changes since previous brief" diffs (Phase 6.8 ships
  the section but it currently reflects same-day method-bump
  churn rather than data revisions).
- Editorial framing of which findings are stable across
  Eurostat's actual revision cycle vs which mean-revert.

Sketch: GitHub Action triggered monthly on the 1st, runs
`scrape.py --eurostat-period $(date)`, then chained `--analyse`
calls for each (scope, flow), then `--analyse llm-framing`, then
brief regeneration. Output committed to a branch; PR opened for
review.

## Methodology depth (pick up if a story warrants it)

### CN8 concordance table (Phase 4 carry-over)

Full mapping of old→new codes across Eurostat's annual revisions.
Currently we apply a blanket `cn8_revision` caveat to any YoY
window spanning a year boundary; a real concordance would let us
strip the caveat where the relevant codes didn't change. The
historical Eurostat backfill (2017–2026) spans 9 CN8 revisions so
the blanket caveat is on most findings. Pick up when a story rests
on a precise YoY for a specific HS-CN8 code.

### Per-(country, commodity) CIF/FOB granularity

Phase 4 carry-over partially addressed in
`cif-fob-baselines-2026-05-10.md`. Per-(EU member state, China)
margins are now in `cif_fob_baselines`. The OECD ITIC SDMX endpoint
also supports HS-4 splits (1224 commodities × 28 EU countries × CN
≈ 34k rows) for per-(country, commodity) precision. Schema-extend
`cif_fob_baselines` and pull when a story needs it.

### Structural-break detection (Chow / CUSUM)

Statistically right but unstable on the 11 periods we had at
Phase 1. With the historical Eurostat backfill we now have 110
periods; this is ready to move from parked to scheduled if a
journalist's question warrants it.

### Sector breadth review (round 2)

The 6.5 promote/drop pass shipped 2026-05-10. A year from now a
similar pass should re-evaluate what's editorially live. Three
groups stayed draft (Honey, Polysilicon, Tropical timber) and
might warrant a second look.

## Data sources (deferred until needed)

### 2018 GACC mirror-trade

See [`forward-work-gacc-2018-parser.md`](forward-work-gacc-2018-parser.md).
Title parser handles all 2018 quirks but the section-4 release
pages embed PNG screenshots, not HTML tables. Body parse fails
and the data is in pixels. Options: OCR (~half-day with editorial
risk), hunt for source xlsx, accept gap, lean on Eurostat+HMRC
which already cover 2018.

### Aggregate-label handling for non-EU blocs (ASEAN, RCEP)

Original project requirement. GACC reports trade with these blocs
as labelled aggregates; we don't currently split them into
member-country flows. Pick up if a non-EU-bloc story emerges.

### Chinese-language source URL backfill on `releases`

Most GACC releases have a Chinese-language equivalent at
`www.customs.gov.cn` (vs the English `english.customs.gov.cn`).
The brief constructs the link via `_construct_chinese_source_url`
but we don't store it. Backfill if useful for downstream consumers.

## Future-platform items

### Web UI / hosted deployment

Required for any non-luke-laptop use. AWS-side (Fargate, RDS,
Cognito) per the fuel-finder precedent. Triggers: when a desk
journalist actually wants to use the tool independently.

### Custom Q&A bot (Phase 7+)

See [`forward-work-qa-bot.md`](forward-work-qa-bot.md). Two-tier
scope (ask the findings, ask the underlying data). Tier 1 is ~3-5
days of build. Triggers: web UI exists AND a journalist has a
recurrent question pattern the brief can't answer.

### GoogleSheetsWriter implementation

Pending service-account credentials (was due "next week" as of
2026-05-09). Once available, the sheets-export module wires up
trivially.

## Closed forward-work, kept for reference

These are real options that may be picked up later but aren't on
any near-term path:

- [`forward-work-gacc-2018-parser.md`](forward-work-gacc-2018-parser.md)
  — 2018 image-only blocker (above).
- [`forward-work-qa-bot.md`](forward-work-qa-bot.md) — Phase 7+
  Q&A bot (above).
- [`shock-validation-2026-05-09.md`](shock-validation-2026-05-09.md)
  + companion `.sql` — pre-registered shock validation
  methodology. Living methodology doc, not just a one-off; re-run
  after any major analyser change.
- [`cif-fob-baselines-2026-05-10.md`](cif-fob-baselines-2026-05-10.md)
  — sourced reference for the OECD ITIC backfill. Reproducibility
  notes for refreshing in a future year.
