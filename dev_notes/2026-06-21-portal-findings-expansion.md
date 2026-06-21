# Portal: porting the Findings-doc content into the web portal

**Date:** 2026-06-21  **Branch:** `ljh-portal-findings-expansion`
**Status:** Built; full suite green (470 passed / 6 skipped). Not yet deployed.

> **Update — progressive disclosure.** Added **GACC bilateral partners to the
> eurostat portal** (was GACC-variant-only; the Findings doc's state-of-play
> includes it). Rendered with **progressive disclosure**: one collapsed
> `<details>` button per partner (name + a China's-exports headline figure),
> expanding to its flows + sparklines + citations on click — ~24 partners stay
> compact, full per-country granularity on demand. `_gacc_bilateral_html`
> redesigned; `_ABOUT['gacc-bilateral']` + `_more_about` added. This is the
> pattern for the remaining not-yet-included content (see the menu at the end).

The portal's first cut surfaced the top-5 findings + sector ("tier 2") reports +
LLM takes. This pass ports much more of the **02_Findings** document into the
portal and restructures it around **tabs**, so the very large briefing is
browsable rather than two long documents.

Read alongside: `dev_notes/2026-06-20-web-portal-and-content-schema-design.md`
(the schema/portal decision) and the `tabs-brief.md` Guardian-tokens spec.

## What shipped

All of it flows through the rendering-agnostic content model
(`report_model.py`) → both renderers (`report_render_html.py`,
`report_render_markdown.py`), so the LLM-facing markdown surface carries the
new content too. Tabs/charts are HTML-only presentation.

1. **Tabs** (Guardian Source spec). Client-side, single document: panels
   show/hide, deep-links (`#tab-x`) and in-page drill-down anchors both resolve,
   degrades to plain anchored sections with no JS. **No `app.py` change** — the
   snapshot stays one static blob. Tabs: **Briefing · Tables · Sources &
   coverage · Methodology · Glossary** (the "minimal" split Luke chose — the
   main page stays whole).
2. **Methodology & caveats** moved off the main page into its own tab, with
   curated guides (the three comparison scopes; predictability badges; what to
   quote vs hedge) + caveats. (Sources moved out — see the Sources tab below.)
2b. **Sources & coverage** tab (added after Luke's nudge): data **sources** +
   **period coverage** (date range & release count per source) + a humanised
   **findings manifest** (counts by family, not raw subkinds) + the **Trade
   Map**, moved off Briefing — together "what the briefing rests on and how
   completely it covers the ground" (principle 7 gets a home; Briefing
   declutters). `kind="sources"`, built by `_sources_section`.
3. **Glossary** tab, parsed from `docs/glossary.md` at build time (a bespoke
   parser for that one regular file — no markdown dependency), baked into the
   snapshot. 49 terms, with a live filter.
4. **"More about this section"** collapsed disclosure at the head of each
   main-page section (`Section.about`, new model field), carrying the
   explanatory matter from the long Findings preamble (deficit basis / CIF-FOB /
   CN+HK+MO; mirror-trade & transshipment; reading-the-numbers; SITC partition).
5. **Restored graphs** as inline SVG (the docx charts), then redesigned after
   review: a **chart card** with the number / title / key in a meta column to
   the **left** of the plot (so the plot isn't stretched), and the plot carries
   real **x and y axes** (3 €-gridlines + start/end month labels). Two-tone line
   (earlier grey / latest-12-months red, auto-scaled) for trajectories; a
   **bar chart** type (zero-based) for relative-scale comparisons. Sector groups
   show a **line + bar side by side** on wide viewports (EU-27 import trajectory
   + imports-vs-exports bar — the flow imbalance a line can't show), stacking on
   narrow. Helpers: `_chart_card`, `_line_chart_svg`, `_bar_chart_svg`. Series
   already lived in the model — no new queries. Line charts carry intermediate
   date ticks + vertical gridlines on a 'nice' month step (`_x_tick_indices`) so
   the span is legible (a 9-year deficit isn't two bare end labels); each sector
   chart is titled by its group ("EV batteries (Li-ion): EU-27 imports from
   China"), not a generic repeated headline.
6. **Tables** tab from the existing `assemble_sheets()`: the digestible tabs
   embedded (summary, trade_balance, mirror_gaps, gacc_bilateral, predictability)
   with a per-table **Copy as TSV** (pastes into Sheets/Excel) and a
   **Download Excel workbook** (`/data.xlsx`); the heavy tabs (hs_yoy*, etc.)
   are listed download-only with row counts (no silent truncation).
7. **Key indicators** — added **EU-27 imports (12-month level, €561B)** and the
   **UK deficit/day**, beside the existing EU-27 deficit/day. (Donut deferred —
   see below.)
8. **Latest-month register** beside the 12-month figure in every sector row
   (the acceleration signal the Findings doc shows; muted, because it swings on
   lumpy categories).

Infra: `portal_service/app.py` gains a `/data.xlsx` route + `GZipMiddleware`
(the index.html/report.json are large; gzip is ~5-6×). `portal_publish.py`
uploads the bundle's `04_Data.xlsx` to `latest/data.xlsx` (+ per-period
archive). An `--portal-snapshot`-only refresh has no bundle workbook, so the
download 404s until the next full periodic run (logged, graceful).

## The donut data-gap (the one ask I did NOT ship as asked)

Luke picked a **"China's share of EU goods imports" donut**. It is **not
honestly computable** from current data: `eurostat_world_aggregates` (the
extra-EU world denominator) is populated **only for the tracked HS prefixes**
(`aggregate_to_world_totals(hs_prefixes=...)`), sum ≈ €737.6B — not all goods
(~€2.8T/yr) — and has no `000TOTAL` row. Dividing the all-goods China figure
(€561B, from `000TOTAL`) by it gives a nonsensical ~76%. Mixing an all-goods
numerator with a covered-codes denominator violates the data-rigor rule, so I
held it back. The donut **renderer** is built and tested (`_donut_svg`,
`chart="donut"`), so wiring a defensible source is a small change. Options for
Luke:
- **(a) proper fix:** ingest an all-goods extra-EU world total (a new Eurostat
  fetch / a `000TOTAL` world aggregate) — then the donut is a clean lookup.
- **(b) clean stand-in now:** a bilateral **imports vs exports split** donut
  (€561B imports / €219B exports of EU–China goods trade — both on the same
  `trade_balance` finding, cited) — a genuine part-of-whole showing the
  lopsidedness, but a *different* meaning than "share of EU imports".
- **(c)** "share across Meridian's tracked categories" — defensible only if
  labelled precisely; weak as a headline vital sign.

## 02_Findings → portal: gap review (item 5)

Now covered: scope/how-to-read/reading-the-numbers/predictability (More-about +
Methodology); top-5 movers; standing deficit (KPIs + State of play); per-group
12-month **and latest-month**; top products / drivers / trajectory shapes +
charts; universal caveats; glossary.

Now also covered, all via **progressive disclosure** (expand-on-demand — the
agreed principle for adding depth without bloat):
- **GACC bilateral partners** — per-partner expand buttons (above).
- **Per-group sector depth** — each group's charts + top-products + drivers +
  trajectory collapse behind a "Show detail & charts" expander (`details.gdetail`);
  flow rows stay visible. Adds Tier-3 granularity *and* fixes the Briefing
  chart-density concern in one move.
- **Tier-1 new-findings breakdown** — `WhatChanged.new_by_subkind` (from
  `diff.new_by_subkind`, labelled via `_subkind_plain_label`), behind an
  expander under What changed (renders only when there are new findings).
- **Full Sources appendix** — per-source recent releases with URL + fetch date
  (`_sources_section` appendix), behind a per-source expander in the Sources tab.
- **Per-row caveat flags** — `_visible_caveats` threads the per-finding-variable
  caveats (partial window, low kg coverage, jan-feb combined) as row chips;
  universal + `low_base_effect` + structural `cross_source_sum` filtered out so
  the chips mark only what's *unusual* about a row.

## Open product calls for Luke

- The donut source (a/b/c above).
- Per-group charts render for **all** ~46 sectors on the Briefing page
  (+~210KB HTML). Faithful to "restore the graphs", but if it reads heavy we can
  collapse them behind a toggle or limit to headline movers.
- Key-indicators set is now 3; design doc wanted ~4–5. The donut (or a 4th
  clean number) fills the last slot.
- Vital signs sit at the top of the **Briefing** panel, not above the tabs
  (i.e. not visible from other tabs). Easy to promote if "always-on" matters.
