# Docs

Reader-facing documentation for **Meridian**. For developer-internal
notes (current roadmap, historical record, dated analysis artefacts)
see [`../dev_notes/`](../dev_notes/).

## The four docs

- **[glossary.md](glossary.md)** — alphabetical reference for every
  unfamiliar term. Three sections: economic & data terms,
  sources, system & methodology terms. Cross-linked from the other
  three docs on first mention of each term.
- **[architecture.md](architecture.md)** — system overview. Three
  sources, three layers, append-plus-supersede findings, the
  per-export bundle, the CLI surface, configuration, external
  dependencies.
- **[methodology.md](methodology.md)** — what each finding means
  and when to quote it. Why the three sources don't agree, the
  anomaly subkinds, caveats, trajectory shapes, the hypothesis
  catalog, numeric verification, known fragility, and a
  quote / hedge / don't-quote rubric.
- **[editorial-sources.md](editorial-sources.md)** — the journalism
  the tool serves. Articles, journalists, analytical sources that
  shaped its design.

## Reading paths by goal

**"I want to quote a number from the brief tomorrow."**
[methodology.md §9 (quote / hedge / don't quote)](methodology.md#9-what-to-quote-vs-hedge-vs-not-quote)
→ [§3 (caveats reference)](methodology.md#3-caveats-reference) to
decode any caveat codes on the finding → [glossary.md](glossary.md)
for any unfamiliar term in either.

**"I'm new — what does this tool actually do?"**
[editorial-sources.md](editorial-sources.md) (why it exists) →
[methodology.md §0 (why sources don't agree)](methodology.md#0-sources-and-why-they-dont-agree)
→ [methodology.md §1 (anomaly subkinds)](methodology.md#1-anomaly-subkinds-catalogue)
→ a real export bundle (e.g. `exports/audit-postfix/`).

**"I'm auditing the methodology before quoting widely."**
[methodology.md §0 (why sources don't agree)](methodology.md#0-sources-and-why-they-dont-agree)
+ [§7 (known fragility)](methodology.md#7-known-fragility)
+ [§10 (known editorial-output limitations)](methodology.md#10-known-editorial-output-limitations).

**"I'm adding a new HS group / analyser / data source."**
[architecture.md TL;DR](architecture.md#architecture) for the layout,
then the section for your goal: `Three-layer data flow` for adding an
analyser, `Storage layout` for schema extensions, `External
dependencies` for new sources. Then look at the closest existing
analyser in `anomalies.py` for the implementation pattern.

**"I hit a term I don't recognise."**
[glossary.md](glossary.md).

## What's *not* in /docs/

Working notes — roadmap, history, dated validation artefacts,
forward-work decisions — live in [`../dev_notes/`](../dev_notes/).
That's where you go to understand "how did the project get here" or
"what's open right now"; this folder is for "what is this thing and
how do I use it."
