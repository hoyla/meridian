# Docs

Reader-facing documentation for **Meridian**. For developer-internal
notes (current roadmap, historical record, dated analysis artefacts)
see [`../dev_notes/`](../dev_notes/).

- **[architecture.md](architecture.md)** — system overview. How the
  three sources (GACC / Eurostat / HMRC) flow through three layers
  (raw_rows / observations / findings), the append-plus-supersede
  chain, the CLI surface, configuration, dependencies.
- **[methodology.md](methodology.md)** — what each finding means
  and when to quote it. Opens with why the three sources don't
  agree (CIF/FOB, transshipment, HK/MO routing, classification
  drift, etc.), then anomaly subkinds, comparison scopes, caveats,
  trajectory shapes, the hypothesis catalog, numeric verification,
  known fragility, and a quote / hedge / don't-quote rubric.
- **[editorial-sources.md](editorial-sources.md)** — the journalism
  the tool serves. Articles, journalists, and analytical sources
  that shaped its design.

If you're new to the project: start with `editorial-sources.md`
(why), then `methodology.md` (what), then `architecture.md` (how).
