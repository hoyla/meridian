# Handover — web portal + taxonomy build (start here)

**Date:** 2026-06-21 (work done 2026-06-20→21)
**Branch:** `ljh-content-schema-spine` — **PR [#18](https://github.com/hoyla/meridian/pull/18)**, now **MERGED** into `main` (see the update box below).
**Status:** A complete *deterministic* replacement for the Findings doc exists as a
rendering-agnostic content model + web portal. Two things remain: the **pipeline
wiring** (Luke) and the **LLM layer** (deferred). Full test suite green (433 passed,
5 skipped).

Read alongside: `dev_notes/2026-06-20-web-portal-and-content-schema-design.md`
(the decision) and `dev_notes/2026-06-20-taxonomy-sitc-spine-and-labels.md` (the
taxonomy design). Memory: `project_web_portal_direction`.

> ### Update — 2026-06-21 (later the same day): merged + hardened
>
> PR #18 (this work) and the docx-link fix ([#19](https://github.com/hoyla/meridian/pull/19))
> are now **merged into `main`** — everything described below is live in `main`,
> not a parallel branch. A fresh-eyes `/code-review` of the merged code then found
> a material bug and several cleanups, fixed in
> **PR [#20](https://github.com/hoyla/meridian/pull/20)**:
>
> - **Trade-map double-count (~8×).** `_structural_section` summed `observations`
>   across all periods / partners / release snapshots — €4,455bn vs the real
>   €553bn. Rebuilt on the canonical `eurostat_raw_rows`, rolling 12 months, with
>   the CN+HK+MO / EU-27 / flow discipline (mirrors
>   `anomalies._hs_group_top_cn8s`), bucketed by SITC division. Shares now sum to
>   1.0 with an explicit "unclassified" remainder, and the section carries its own
>   `Section.provenance` (source + as-of) so the numbers stay attributable
>   (principle 7).
> - GACC macro prose no longer prints "fell" for a null YoY — it states the level.
> - The four divergent EUR formatters are consolidated onto `_helpers._fmt_eur`
>   (+ a €T tier); the markdown surface emits explicit `<a id>` heading anchors.
> - The vestigial `headlines.py` prototype (item 5 in STILL TO DO) is **deleted**;
>   `_VARIANTS` is single-sourced in `report_builder`.
>
> Net effect on the "STILL TO DO" list below: **item 5 is done**; items 1–4
> (pipeline wiring, LLM layer, deployment, editorial label sign-off) still stand.
> Suite after the fixes: 432 passed / 6 skipped.

---

## The arc of this session

1. Started from a journalistic question (do we ever surface GACC's lead month?) →
   widened into restructuring the briefing output.
2. **Decision:** move the briefing surface from Google Docs/.docx to a **web portal**,
   rendered from a **rendering-agnostic content model**. Driven by real Lisa-behaviour
   evidence (she doesn't notice Drive additions; used comments only to flag; copies only
   numbers) and by the Google-Workspace rendering friction (we were rebuilding a website
   inside Docs). Delivery target: **Google Cloud Run + IAP** (auth tractable); serve
   **published JSON snapshots**, not the live laptop DB.
3. Built the spine, two renderers, a Guardian skin, then the **taxonomy** (SITC spine +
   editorial label overlay + BEC end-use), then **full Findings parity**, then **tests**,
   then the **PR**.

## Architecture (what's where)

- `report_model.py` — the schema. Carries data+semantics, NOT presentation. Per-leaf
  `Provenance` (finding ids), `Facets` (sector/theme/partner/commodity/end_use), charts
  carry `series` not images, `LLMSlot` is a first-class node, `Section.metrics` lets a
  node be an aggregate summary. JSON serialisation = the published-snapshot format.
- `report_builder.py` — DB → `Report`. Reuses existing analytical/prose helpers
  (`_compute_top_movers`, `_compute_diff`, `front_page._mover_sentence`). One section
  builder per content type (sector detail, mirror gaps, state of play, structural trade
  map, reference, GACC bilaterals). Variant-shaped by trigger (Q1: eurostat/gacc/hmrc).
- `report_render_markdown.py` / `report_render_html.py` — two renderers over the model.
  HTML is self-contained (inline CSS, inline-SVG sparklines, a small embedded filter JS).
- `classifications.py` — CN8→SITC division + BEC end-use, from the UNSD correspondence.
  `build()` / `build_bec()` regenerate `reference/cn8_sitc.csv` / `cn8_bec.csv` (+ PROVENANCE).
- `labels.py` — the editorial THEME registry (5 cross-cutting seed labels).
- `reference/cn8_sitc.csv`, `reference/cn8_bec.csv` — committed derived lookups (provenance sidecars).
- **`~/Code/un-classifications/`** — the UNSD source workbooks (HS2022/HS2017→SITC4,
  HS2022→BEC5, combined). Shared local, NOT in-repo. Needed to *regenerate* the lookups.
- `briefing_pack/sections/headlines.py` + `render.render_headlines()` — the early
  markdown-direct prototype, now **superseded** by the schema path. Vestigial.
- `tests/test_portal.py` — 18 tests for the new code.

## The portal's content (Findings parity: 26/27 finding-types, all cited)

Key indicators (deficit sparkline) · headline movers (drill-downs resolve) · what-changed
diff · **state of play** (deficit ×3 scopes + China-reported counterpart) · **mirror-trade
gaps** (two-sided, CIF/FOB excess, transshipment hub, z-score) · **sector detail** (46 EU
groups; per group: description, themes, SITC + end-use, EU-27/UK/EU-27+UK flows with
sparklines, top CN8 products, member-state drivers, trajectory shapes, China import & export
share) · **trade map** (65 SITC divisions, value-weighted, the dark tail flagged) ·
**methodology/sources/caveats** endmatter · GACC variant: bloc macro lead + **24 bilateral
partners**. Filter box spans name/SITC/theme/end-use; clickable theme chips.

## STILL TO DO

1. **Pipeline wiring (Luke's job, in progress).** Integrate `build_report` into
   `--periodic-run` (`periodic.py`) so a cycle produces the portal snapshot; decide whether
   it joins or replaces the docx bundle. Until done, the portal is a **parallel surface**,
   not the live export. The model is wiring-agnostic — `build_report(source_trigger=…)`
   returns a `Report`; serialise with `report_model.to_json`.
2. **The LLM layer (deferred, needs design).** `narrative_hs_group` (the Leads doc) is the
   one finding-type NOT surfaced; the `LLMSlot`s render as labelled placeholders. Design
   ratified earlier: two block types (specific = on a finding; general = across the release),
   *interpret-not-introduce* (cite only existing findings, no new facts), self-attribute
   in-sentence (the hedge must survive copy-paste). Build after the LLM design is settled.
3. **Deployment** — Cloud Run + IAP (allow-list Guardian accounts; IAP, NOT the default
   IAM-invoker). Confirm whether IAP-for-Cloud-Run still needs a load balancer. Repoint the
   existing daily push notification at the report URL. Flagging feature ("my flagged
   findings") needs its own small store (multi-user).
4. **Editorial follow-ups (need a journalist's eye, not code):** the cross-cutting **label
   code-sets in `labels.py` are illustrative seeds** — confirm membership before they're
   authoritative. The Key-indicators set, and whether the trade map shows all codes or a
   value-cut tail, are product calls.
5. **Minor:** `headlines.py` could be deleted once the schema path is confirmed as the only
   one. A few low-value finding variants are surfaced compactly; revisit if a reporter wants
   more depth.

## Gotchas / decisions a new session must know

- **Data hygiene:** `observations` mixes Comext aggregate pseudo-codes (`000TOTAL`, `…XX`)
  with real CN8. ANY whole-dataset aggregation MUST filter to real 8-digit numeric codes or
  it double-counts catastrophically (the `000TOTAL` row is the grand total). The per-pattern
  analyses are unaffected.
- **Multi-HS-edition data:** the dataset spans HS2017 + HS2022 (smartphones 851712→851713,
  solar 854140→854142). The SITC lookup maps HS2022 then HS2017 fallback. Swap in HS2027
  when it ships.
- **Mirror-gap double-count is FIXED** (PR #13, 2026-06-17) — the portal numbers are the
  corrected ones (NL +20%, DE −4.5%), safe to surface.
- **BEC Rev 4, not Rev 5** for the end-use axis — Rev 4 is the documented capital/
  intermediate/consumption standard; Rev 5 restructured and needs its own legend.
- **Invariant:** SITC divisions PARTITION the data (sum to total — safe); editorial labels
  OVERLAP (a code can carry several) — NEVER sum labels to a total.
- **DesignSync/Claude-Design auth:** `/design-login` isn't available on this surface or the
  CLI; the Guardian design system was pulled by running DesignSync through a separate Claude
  CLI session and exporting to `~/Code/guardian-source/` (the design tokens; `CONVENTIONS.md`
  has the palette/type/spacing). Portal fonts are Source Serif / Source Sans / Noto Serif
  (Google Fonts substitutes; the licensed Guardian faces are next in the CSS stack).
- **Conventions:** no AI-attribution trailers in commits/PRs; don't auto-apply labels;
  work branch-per-change; commit/push only when asked. Personal repo (`hoyla`), no Core4 gate.
- **DB tests** need `GACC_TEST_DATABASE_URL` (the `gacc_test` DB exists locally); sourcing
  `.env` alone isn't enough. Call `.venv/bin/python` directly (activation doesn't persist).
- **Pre-existing uncommitted files** in the working tree — `briefing_pack/drive_export.py`,
  `briefing_pack/sections/front_page.py`, `tests/test_briefing_pack.py`,
  `tests/test_drive_export.py` — are Luke's SEPARATE in-progress work. They were deliberately
  kept OUT of this branch. Do not commit them as part of this work.

## How to see it

`build_report(source_trigger="eurostat")` → `render_html(report)` → write to a file → open in
a browser (or headless-screenshot via Chrome). The GACC variant: `source_trigger="gacc"`.
