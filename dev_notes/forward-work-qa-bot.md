# Forward work: journalist Q&A bot (Phase 7+)

Captured 2026-05-10 alongside Phases 6.4 and 6.8. The brief versioning
(6.8) and lead-scaffold restructure (6.4) cover the *push* side of
journalist workflow — "tell me what changed" and "give me a starting
point". A Q&A bot covers the *pull* side: "let me ask my own questions
of the data". This doc captures scope, constraints and a phased
approach so we can pick it up cleanly when the editorial value
justifies the build effort.

## Why this isn't shipping in Phase 6

- **Cost of building right is high.** A naive RAG-over-the-database
  bot is easy and bad. Doing it usefully means designing tools the LLM
  calls (parameterised SQL, finding-id lookups, source URLs) and
  shaping prompts so the bot stays in evidence-cited mode rather than
  free-recalling from training data. ~1-2 weeks of focused work plus
  ongoing prompt iteration as journalists hit unexpected questions.
- **Editorial demand is unproven.** No journalist has actually used
  the deterministic findings or the lead-scaffold output yet (as of
  2026-05-10 the tool runs only on luke's laptop). We don't yet know
  whether journalists ask for "more depth on what's already in the
  brief" (= scaffold extension, Phase 6.4) or "totally different
  questions the brief doesn't anticipate" (= Q&A bot territory).
- **Deployment story for "always-available bot" is non-trivial.** A
  Q&A bot needs to be reachable when the journalist is at their desk,
  not when luke happens to have his laptop on. That implies AWS-side
  hosting (fargate / ECS / RDS), Cognito auth, and a chat surface (Web
  UI or Google Spaces integration) — none of which exist today.

Defer until at least one of:
- A journalist has a specific question the brief can't answer that
  feels recurrent (i.e. the *pattern* of questions justifies the build).
- The web UI / hosted deployment is on the roadmap for other reasons,
  reducing the marginal cost of adding the bot.

## What the bot should do

Two-tier scope, smallest-useful-thing first:

### Tier 1 — "Ask the findings"

The bot answers questions that can be resolved by running parameterised
SQL over `findings` and `observations` and returning a fact + finding
trace. Examples a journalist would actually ask:

- "Which HS groups had the biggest YoY shifts in EU imports of Chinese
  goods in the year to February 2026?"
- "What does the trajectory look like for Solar PV cells over the
  last 24 months?"
- "What was the UK's import value from China for permanent magnets
  in the year to March 2026, and how does it compare with two years
  earlier?"
- "Show me all findings that fired the `cn8_revision` caveat for
  electrical machinery."

These are well-defined queries against the existing schema. The bot
needs:

1. A small library of named SQL templates (one per question class).
   The LLM picks the template + parameters; the bot executes the SQL
   and returns the rows. The LLM does NOT compose SQL freehand.
2. A finding-trace footer on every answer ("Sources: finding 12345
   (link), finding 12349 (link)") so the journalist can verify.
3. The same numeric verification pipeline from `llm_framing.py` —
   any number the bot speaks must round-trip to the rows it pulled.

Tier 1 is achievable in ~3-5 days of build, maybe 2-3 more for
robust prompt engineering and a thin chat UI.

### Tier 2 — "Ask the underlying data"

The bot answers questions that need to compute new aggregates from
`eurostat_raw_rows` / `hmrc_raw_rows` directly, or that need to
combine multiple findings in non-templated ways.

- "What's the unit-price trend for HS code 8507.60 specifically?"
- "Which Chinese provinces (or which CN8 codes within a group) drove
  the surge?"
- "Is the trajectory of EV imports correlated with the trajectory of
  battery cell imports?"

This needs either (a) a wider library of SQL templates covering more
analytic shapes, or (b) the LLM composing parameterised SQL with a
schema-aware safety harness. Both have substantially more failure
surface than Tier 1.

Tier 2 should not be attempted until Tier 1 is in production AND the
journalist usage pattern shows specific questions the templates can't
answer.

## Architecture sketch

When we do build it:

```
Journalist (chat surface) → Cognito auth → Fargate task running
    bot.py → tool dispatcher → SQL template library → RDS (gacc DB)
                            ↓
                        LLM (Anthropic API,
                        not local Ollama —
                        production reliability)
```

- **Backend**: FastAPI on Fargate, RDS for the DB (existing
  fuel-finder shape), Cognito for Guardian-only auth.
- **Chat surface**: web UI first (own login, session history). Google
  Spaces integration is a nice-to-have but adds another auth/integration
  layer; postpone unless desk uptake demands it.
- **LLM**: Claude (Anthropic API) with tool-use, NOT local Ollama.
  The latency, reliability and tool-use quality of qwen3.6 don't meet
  a "journalist-facing tool" bar. Cost is bounded — at maybe 50
  questions/day across the desk, this is a few dollars per day.
- **Tool catalog**: starts as ~10 SQL templates (Tier 1), grows
  empirically.
- **Verification**: same approach as `llm_framing.py` — extract numbers
  from the bot's response, round-trip against the result-set rows.
  Reject responses with hallucinated numbers; show the journalist a
  "sorry, I had to discard a draft answer that didn't pass fact
  verification" message and fall back to "here are the raw rows".

## What we'll need from the existing codebase

Most of it already exists:

- **`findings` table** — the bot's primary read source for Tier 1.
  Already there; supersede chain handled.
- **Numeric verification** — `llm_framing.verify_numbers` ports
  directly. Lift into a shared module if/when we add a second consumer.
- **Caveat propagation** — bot answers should surface the caveat codes
  on the findings they cite, same shape as the briefing pack.

What's new:

- **SQL template registry** with named query + parameter schema.
- **Tool dispatcher** that maps tool-use calls to template + args.
- **Auth + hosting** (fully greenfield).

## Adjacent ideas that bot might subsume

- **Brief-on-demand**. A journalist could ask "give me a brief for
  permanent magnets focused on the UK". This is bot territory rather
  than CLI. The current `briefing_pack.py` is well-structured for
  reuse here (per-section render functions take a cur + filters).
- **Hypothesis tracking**. If the lead-scaffold layer (Phase 6.4)
  picks `tariff_preloading` for an HS group, the bot could be asked
  "show me which other groups have that hypothesis selected this
  month" — turning the catalog from output structure into a queryable
  tag.

## Open questions for when this becomes Phase 7+

1. **Audit / accountability layer**. Should bot conversations be
   logged for editorial review? (Probably yes — Guardian tools tend
   to need a paper trail.)
2. **Read-only or read-write?** Tier 1 is pure read. Should
   journalists be able to ask the bot to add an HS group, flag a
   finding, or trigger an ad-hoc analyser run? (Probably read-only
   v1; write actions require a separate authorised path.)
3. **Embedding history vs. statelessness?** Should the bot remember
   the prior turns in a session for follow-up questions ("and what
   about exports of the same group?")? Adds complexity but is what
   journalists will expect.
4. **Confidence display.** When the bot picks an SQL template that
   isn't a great fit, should it say "I don't have a clean template
   for this question — here's a partial answer"? Yes; brittle bot
   silence is worse than honest hedging.

## Trigger to revisit

When at least two of the following are true:
- A journalist (or editor) asks for a way to query the data
  themselves rather than wait for the brief.
- Web UI / hosted deployment is being scoped for another reason.
- The lead-scaffold output (Phase 6.4) generates enough engagement
  that follow-up questions are clearly the bottleneck.
