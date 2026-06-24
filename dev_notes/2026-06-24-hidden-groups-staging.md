# "Analyse but hold back" — staging a group out of the published rankings

**Date:** 2026-06-24

## Why

The portal is a snapshot rebuilt fresh from the DB on every publish (even a
layout-only `--portal-reuse-takes` rebuild — that flag only carries the prior
LLM *takes* forward, not the rankings). So the moment a new HS group is
analysed, the *next* rebuild — for any reason — pulls it into the headline
rankings: the **Standout movers** list and the **Biggest-mover KPI**. A
journalist who adds, say, refined petroleum (a big-value group) and then ships
an unrelated copy fix would find the front page reordered by it.

This gives a way to **analyse a group, eyeball it, and promote it deliberately**
— rather than have it gatecrash the headlines on the next rebuild.

## How it works

A group is *held back* when its `hs_groups.created_by` carries a hold prefix:

| prefix    | meaning                                              |
|-----------|------------------------------------------------------|
| `hidden:` | valid, but deliberately staged out of the rankings   |
| `draft:`  | methodology not yet validated (also kept out)        |

`db.is_held_created_by()` / `db.held_group_names()` are the single source of
truth. A held group is:

- **still analysed** — findings are computed exactly as before;
- **excluded** from the two ranking surfaces — `_compute_top_movers`
  (briefing_pack/_helpers.py) and `_biggest_mover_indicator` (report_builder.py).
  A CN8 that is "watched" *only* because a held group widened the prefixes is
  dropped from the Biggest-mover card too;
- **still listed, flagged**, in Sector detail (`metrics.held` + a
  "(held back — not yet in rankings)" title marker) and in the briefing-pack
  group glossary, so you can see its numbers;
- **kept out of the paid LLM takes** as a side effect (takes run on top-movers
  only) — you don't spend money narrating an unpromoted group.

No schema change — the signal is the existing `created_by` text column.

## Lifecycle

1. **Add** the group with a `hidden:` prefix, e.g.
   `('Crude oil (HS 2709)', …, ARRAY['2709%'], 'hidden:reporter_request_2026_06')`.
2. **Analyse** — the periodic `--analyse` run produces its findings.
3. **Preview** with `--portal-no-publish` and inspect Sector detail (or the
   briefing-pack glossary). Check the values are sane and won't distort the
   rankings.
4. **Promote** by dropping the prefix —
   `UPDATE hs_groups SET created_by = 'seed:reporter_request_2026_06' WHERE name = 'Crude oil (HS 2709)';`
   — and rebuild. It now enters the rankings.

   (To stage a group that already shipped live, go the other way: prepend
   `hidden:` to its `created_by`.)

## Applies to the 2026-06 energy groups

Crude (2709), refined (2710) and gases (2711) shipped in PR #98 as `seed:`
(live). They were added to test the petrochemicals-reselling hypothesis and are
big-value, so they're prime candidates to stage with this tool: re-tag them
`hidden:` before applying #98's migration to the live DB, eyeball them in a
preview, then promote once their contribution to the rankings is understood.
