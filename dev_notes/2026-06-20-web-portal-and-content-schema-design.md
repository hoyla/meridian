# Web portal + rendering-agnostic content schema — design decision

**Date:** 2026-06-20
**Status:** Direction agreed (Luke + Claude). No portal/infra code yet. The
*first* build step is the content schema; everything else is downstream.

## Why this note exists

A long design conversation (2026-06-20) started from a journalistic
question — "do we ever surface GACC's lead month, or only when Eurostat
triggers a report?" — and widened into how the briefing is *structured*
and *delivered*. It ended with a directional pivot: stop fighting Google
Workspace's rendering limits and move toward a **web portal**, rendered
from a **rendering-agnostic content model**. This records the reasoning
so it isn't lost, and so future sessions don't re-litigate it.

## The chain of reasoning

1. **Overload + timeliness.** The current monolithic Findings + Leads
   docs are overwhelming, and report generation is Eurostat-triggered, so
   GACC's lead month (it runs ~1 month ahead) is computed but not
   surfaced promptly. Restructuring around a short entry surface + deeper
   companions addresses both.

2. **Information architecture (ratified this session).**
   - **Headlines** doc = per-release entry point; **companions** =
     uniform reference. The Headlines surface is mostly a *promotion* of
     the existing deterministic front page (`front_page`) + Tier 1 diff,
     not a new invention.
   - **Q1 — trigger variants + precedence.** Every source has a headline
     *variant* and a *fold-in* form. Precedence **Eurostat > GACC > HMRC**
     picks the lead; lower-ranked sources that also advanced fold in as
     **dated** subsections. HMRC isn't special-cased — its variant is the
     UK cut, which leads only when HMRC is the sole trigger (rare).
   - **Q2 — restate the few, link the rest.** Headline items are restated
     inline with full numbers (copy-paste-ready) + citation token + a
     drill-down link. Everything below the cap is a link, not a
     restatement.
   - **Q3 (amended) — two registers.** Headlines = *vital signs*
     (standing levels, shown every release) **+** *what changed* (the
     delta). State-of-play still owns exhaustive standing detail.
   - **Governing rule:** the entry surface is bounded by **attention, not
     completeness**. When in doubt an item goes *down* into a companion.

3. **Key indicators.** The amended Q3 came from Luke's "Key indicators"
   idea: an always-on vital-signs panel (big number / sparkline / donut)
   that gives levels — notably the **€1bn/day EU–China deficit** — a home
   the delta-driven sections can't provide. Constraints agreed: small
   fixed set (~4–5); every indicator carries figure + citation + "as of"
   date (provenance survives the glyph); robust series only (hold the
   aggregate mirror-gap back — see the [double-count caveat]); donut only
   for the genuine part-of-whole (China's share of EU imports); default
   treatment is big-number + delta + sparkline, not pies.

4. **Charts already exist.** Correction logged mid-session: the pipeline
   is **not** pure text. `briefing_pack/docx.py` already generates
   matplotlib PNGs (24-month trajectory lines, bilateral bars,
   per-reporter bars) and embeds them in the Lisa-facing `.docx`. So Key
   indicators reuses existing plumbing — the differences are *placement*
   (fixed top panel vs per-finding), *cadence* (always-on vs
   change-gated), and *style* (sparkline small-multiples vs full
   trajectory charts), plus a couple of new data series (deficit,
   import-share). Not a new toolchain.

5. **The Google Workspace complications were the tell.** Tabs API,
   cross-tab links, image-by-URL — all bespoke plumbing to make a
   *document* behave like a *navigable site*. That's reimplementing a
   website, badly, inside Docs. HTML does structure, charting,
   cross-referencing, and **granularity** (dig as deep as you want)
   natively. Granularity is the one thing a linear document
   *fundamentally cannot do*.

## Why the portal won (the Lisa evidence)

The objection to a portal was never technical — it was distribution /
workflow. Luke's actual observations of Lisa (the current primary user)
knocked the objections down:

- **"Meets her where she is" was wrong — and inverts.** She doesn't
  notice Drive additions; she has to be *nudged*. So the real
  job-to-be-done is **notification**, which is surface-independent and
  *easier* pointing at a link. Mildly favours the portal.
- **Commenting = flagging, not discussion/editing.** She used comments
  only to flag findings for attention (hers or colleagues'); never
  discussed, never edited. Replaceable — arguably improved — by a
  "flag this finding / my flagged findings" feature. One debt: some flags
  are *for colleagues*, so portal flagging must be multi-user/shareable.
- **Copy = numbers only.** Trivial from a webpage.
- **Mobile.** She likely doesn't read mobile, but responsive is easy, and
  a Guardian-style design system is available in Claude Design — turning
  "look good" from a cost into an accelerant.

**What still stands:** (a) **auth** for pre-publication Guardian material
— but Luke considers it tractable via Cloud Run + IAP (below), so it's no
longer a blocker; (b) **n=1** — this is all Lisa; mitigated by keeping the
content layer portable (the whole point of the schema-first approach).

## Delivery architecture (late-binding, but agreed direction)

- **Host:** Google **Cloud Run** (containerised Python app; same language
  and content model as the pipeline; scales to zero → near-free for
  rare-change/low-traffic reports). Justified over static hosting by a
  genuine *dynamic* requirement: multi-user flagging.
- **Auth:** **Identity-Aware Proxy (IAP)**, *not* the default IAM-invoker
  (which is service-to-service and gives browser users a 403, no login).
  IAP allow-lists Guardian Google accounts. Confirm at build time whether
  current IAP-for-Cloud-Run still needs an external load balancer in
  front (this has changed over time).
- **Notification:** repoint the **existing daily push** (today a
  heartbeat) at the report URL — "new report ready → link". The
  notification gap Luke identified is ~80% already built.
- **Content serving:** the portal serves **published snapshots**
  (structured JSON generated by the laptop pipeline), **not** the live
  analytical Postgres (which lives on the laptop and must not be a cloud
  dependency). Fits the append-only / timestamped-snapshot principle.
- **Flagging:** its own small cloud store (Firestore or a tiny Cloud SQL
  table), separate from report content.
- **Governance flag:** Guardian IP in a GCP project — personal vs
  Guardian-owned. IAP allow-listing works either way; fine for a
  prototype in Luke's project, but be deliberate about ownership before
  it carries real pre-pub material at scale. Same personal/work boundary
  as the GitHub hosting.

## Build order

1. **Rendering-agnostic content schema** ← the spine; build first.
2. HTML renderer over the schema.
3. Cloud Run + IAP wrapper.
4. Flagging (its own store; multi-user).

The markdown/LLM surface stays. The `.docx` may persist as a lightweight
"lands in your workflow" fallback until portal usage is proven (n=1
hedge), or be retired — TBD by behaviour.

## Status of this session's prototype

`briefing_pack/sections/headlines.py` + `render.render_headlines()` were
built and generate a real April (Eurostat-variant) Headlines doc from
live findings; 68 briefing-pack tests pass. **But** it emits *markdown
strings* — i.e. it's already a *rendering*, not a content model. Under the
schema-first plan it is **demoted to one renderer over the schema**. It
was valuable for nailing the IA (Q1–Q3, the variant shapes); it is not
the content layer.

## Still-open editorial items (not blockers)

- The **Key indicators** final set (the ~4–5) and their chart treatments.
- Wiring the **GACC macro lead** (partner/bloc `gacc_aggregate_yoy`) so a
  GACC-triggered headline is real, not a scaffold stub.
- Whether the `.docx` fallback survives.

[double-count caveat]: the aggregate mirror-gap Eurostat totals are ~2×
inflated (known bug, flagged, not yet fixed) — hence holding it out of the
always-on indicators.
