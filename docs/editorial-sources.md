# Editorial sources

The articles, journalists, and analytical sources that shaped this
tool's design. Read this to understand *what kind of journalism* it
serves — the technical architecture (in `dev_notes/history.md` and
`README.md`) makes more sense once you know which sentences a desk
expects to write at the end of it.

Append new pieces under "Other articles tracked" as they appear; the
register is open-ended.

## Three modes of journalist use

Agreed during the design conversations on 2026-05-09. Every shipped
surface (Sheets export, briefing pack, LLM framing layer) maps to one
or more of these:

1. **Direct query / fact-finding** — a journalist has a question
   ("how have EU imports of Chinese permanent magnets moved this
   year?") and wants the numbers. Surface: SQL behind the Sheets
   export. No LLM.
2. **Ongoing trend monitoring** — a journalist has known interests
   and wants to be told when something shifts. Surface: the briefing
   pack as a recurring file drop, eventually plus a digest channel
   (Slack/email — not yet built).
3. **Lead surfacing** — a journalist wants to be shown patterns they
   didn't know to look for. Surface: the LLM framing layer's
   `narrative_hs_group` findings, plus optionally NotebookLM as a
   one-shot exploration tool over a briefing pack.

This ordering is also the *trust* ordering. Mode 1 is purely
deterministic and citation-traceable. Mode 2 adds the temporal
dimension (revisions become part of the story). Mode 3 introduces
LLM-drafted prose, which is why the verification discipline in
`llm_framing.py` is strict — anything quoted must round-trip to a
source finding.

## Primary editorial target

### Lisa O'Carroll (Guardian Brussels correspondent)

The intended primary user. Her piece "EU faces 'China shock' as EV
imports drive Beijing's record surplus with bloc" (Guardian,
2026-04-27) was the editorial brief that shaped the tool's first
year of work.

- <https://www.theguardian.com/world/2026/apr/27/eu-faces-china-shock-as-ev-imports-drive-beijings-record-surplus-with-bloc>

**Load-bearing data points she cited** (each maps to a finding
shape this tool now produces):

- China-EU trade surplus: $83bn for Q1 2026 (China's exports $148bn,
  imports $65bn). 2025 full-year surplus: €360bn.
- Chinese electric + hybrid car sales to EU: $11bn Q1 2025 → $20.6bn
  Q1 2026 (almost doubled). *This is exactly the kind of HS-group YoY
  finding our analyser generates — the EV+hybrid passenger cars HS
  group (HS 870380, 870370, 870360) was seeded specifically to
  reproduce this data point on demand.*
- Europe + UK + Norway + Switzerland accounts for 42% of Chinese EV
  sales. (Currently we cover EU; UK/Norway/Switzerland support is
  forward work — see `feedback_journalist_secondary.md` notes.)
- EU→China exports fell 16.2% in February 2026, pork shipments
  notably down. *The Pork (HS 0203) HS group was seeded to track this
  story shape; flow=2 export support shipped in Phase 1.*
- China still accounts for 93% of permanent magnet imports, volumes
  up 18% YoY. *The Permanent magnets (HS 8505) group was seeded for
  this specific claim. Notable: when the LLM framing layer was first
  run, qwen3.6 cited "93%" recalled from this article in training
  data — the verifier correctly rejected it because that figure is
  not in our current data.*

**Her own cited sources:**

- **Merics** (Mercator Institute for China Studies) — quantitative
  analysis of GACC customs data. <https://merics.org/>
- **Soapbox Trade** (the substack, see below) — she relies on them
  for headline figures.
- **Bruegel** (think-tank, qualitative). <https://www.bruegel.org/>

**The implication for tool design**: she's a *consumer* of trade-
data analyses, not a producer of them. The bottleneck for her is
*getting current* — she can't scrape Eurostat or GACC herself, so
she waits for a Merics or Soapbox piece to publish, then writes
around it. What this tool gives her is the same kinds of figures
Merics/Soapbox produce, **on her schedule, not theirs**, with
editorial-grade caveats inline (CIF/FOB, transhipment, classification
drift) so she can decide how confidently to phrase a claim.

## Secondary editorial references

Articles by other journalists/analysts that show story shapes the
tool should support beyond Lisa's brief.

### Chee Meng Tan, "Europe's dilemma — to use China's turbines to meet its renewable targets or not"

The Conversation, 2026-05-05.

- <https://theconversation.com/europes-dilemma-to-use-chinas-turbines-to-meet-its-renewable-targets-or-not-281475>

**Load-bearing data points:**
- EU wind = 17% of EU electricity (was 13% in 2019); target 42.5%
  renewables by 2030.
- China makes 6 of top-10 turbines, 70%+ of new turbines globally in
  2024. Chinese turbines 30-40% cheaper than western equivalents.
- Chinese wind turbine exports surged 50% in 2025; cumulative >28 GW
  by end-2025 (13× 2015).
- Top 2024 Chinese-turbine buyers: Saudi Arabia, Uzbekistan, Brazil,
  Egypt, Kazakhstan (all Belt & Road).

**What our tool surfaces against this**: Wind turbine components
(HS 850231 + 850300 + 730820), CN→EU imports as a 5-year rolling
12mo series. Trajectory shape: peaked ~€2.3B late 2022, bottomed
€1.66B Jan 2024 (-27% YoY), recovered sharply (+48% peak Jul 2025),
decelerating to +17% by Feb 2026 — i.e. classified as `dip_recovery`
by the trajectory analyser. The narrower "Wind generating sets only"
group (HS 850231) was added (`seed:tan_article` in schema.sql) to
isolate the finished-turbine question from generator parts and steel
towers.

**Note**: the global numbers in Tan's piece (70% of world supply,
exports to Saudi/Brazil/Egypt) need additional sources — UN Comtrade
for any reporter+partner pair, or scraping `customs.gov.cn` for
Chinese-language partner detail. Forward work.

## Standing analytical sources

Reference these for editorial register, methodology cross-checks, and
to know what's already published before drafting our own narratives.

### Soapbox Trade

<https://soapboxtrade.substack.com/>

The model substack for this kind of work — Lisa relies on them for
headline figures; their methodology is what we're trying to reproduce
on a Guardian-internal schedule. Range covers EVs, pharma APIs,
honey, gold, photovoltaics, wine, semiconductors, etc. The
domain-agnostic ambition of this tool (HS groups journalist-editable
per investigation) was deliberately modelled on Soapbox's range. Luke
can share an auth cookie for full articles if needed.

### Merics — Mercator Institute for China Studies

<https://merics.org/>

Quantitative think-tank work on Chinese trade and policy. Lisa cites
them frequently. Useful for cross-checking findings before
publication and for context the raw data can't supply (policy
direction, leadership signals).

### Bruegel

<https://www.bruegel.org/>

Brussels economic think-tank. Mostly qualitative — useful for the
"what does this mean for EU policy" framing journalists need to
sandwich around the numbers.

## How HS groups in `schema.sql` map to articles

The `created_by` column on `hs_groups` tags which editorial input
prompted each addition:

- `seed` — initial generic set (EV batteries, solar PV, motor-vehicle
  parts, machine tools, etc.).
- `seed:lisa_article` — added after Lisa's April 2026 piece, to make
  her data points reproducible: Permanent magnets (HS 8505), Finished
  cars (HS 8703), EV + hybrid passenger cars (HS 870380/870370/870360),
  Pork (HS 0203).
- `seed:tan_article` — Wind generating sets only (HS 850231) added
  after Tan's May 2026 piece.

When a new article prompts a new group, follow this convention: add
the row with a `seed:<short_descriptor>` `created_by` value, and
record the article URL in this file under "Other articles tracked".

## Editorial register the LLM framing layer should match

The LLM-drafted top-lines should sound like Lisa or Soapbox, not
breaking-news wire copy. Indicative shape:

> "[Component group] imports from China to the EU rose +X% to €YB in
> the rolling 12 months ending [period], from €ZB the year before.
> [Top importer iso2] accounted for €AB ([share]% of the increase).
> Caveat: CIF/FOB inflates EU-side imports vs China's reported exports
> by ~5-10% baseline."

Tone: factual, analytical, names sources where applicable, hedges
with caveats inline. Never breaking-news energy. The system prompt
in `llm_framing.SYSTEM_PROMPT` enforces some of this; the rest is
the verifier's job (rejecting any number not in the source data).

## Other articles tracked

Append below as new pieces appear that prompt new HS groups, new
caveat codes, or new story shapes. Format suggestion:

> ### {Author}, "{Title}"
> {Outlet}, {date}.
> URL: {link}
> Why noted: {one-line — what data point or story shape it supplies}
> Action taken: {hs_group added? caveat added? forward work logged?
> nothing yet?}

(None yet beyond the two above — this is the place to add them.)
