# LLM layer: the per-finding "take" (specific blocks)

**Date:** 2026-06-21  **Status:** Design agreed in discussion (Luke + Claude);
not yet built. Covers the *specific* (per-finding) take only — the *general*
(across-release) take is parked (see end).

Read alongside: `dev_notes/2026-06-20-web-portal-and-content-schema-design.md`
(where `LLMSlot` and the trust boundary were ratified). Memory:
`project_web_portal_direction`.

## Why this note exists

The LLM layer is the one deferred piece of the portal — `LLMSlot` renders as a
labelled placeholder today. A design conversation settled how the *specific*
takes should work: what they say, how they stay safe, which model, and a v1→v2
build path. Recorded so a future session doesn't re-litigate it.

## The problem

The existing Leads pipeline (`llm_framing.py`) is factually bulletproof — every
number it emits is checked against the data within tolerance, and the whole
scaffold is rejected and logged on any mismatch (`llm_rejection_log.py`). But
strict verify-or-reject produces takes that mostly *restate* the finding ("the
35% rise is notable") — low insight; a take that only echoes the deterministic
number is noise. Separately, reporters found the old **Findings doc + Leads
doc** split (two different structures) confusing.

So: we want takes that reach beyond the verified numbers into *context*,
attached to the finding in place, **without spending the trust the rest of the
tool is built on**.

## What we build on (already exists)

- `llm_framing.py` — the generate → fact-verify → reject engine; a pluggable
  `LLMBackend` Protocol; today an Ollama local backend (`qwen3.6`). The numeric
  guard is the proven part and stays.
- `llm_rejection_log.py` — audit table for rejected output.
- `report_model.LLMSlot` — first-class node (`slot_type`, `grounded_in`,
  `status`, `content`); already renders as a placeholder, so the layer is
  strictly additive.

## Decisions

1. **In place, under the finding.** "Here's the finding; here's the take on it."
   Co-location kills the Findings/Leads structural split that confused
   reporters. (Co-location is great for a take we trust and dangerous for one we
   don't — hence the controls below.)

2. **Fewer, richer.** A take only on the ~5 **most quotable shifts** this cycle
   (the headline movers), not every finding. Concentrating the budget lets each
   take reach further.

3. **Takes are leading questions, not assertions.** The interrogative form *is*
   the safety mechanism — a question can't be lifted into copy as a fact.
   "Worth checking: does this 35% rise track the EU anti-subsidy tariff
   timeline?" is a lead by grammar; "this rise reflects the tariffs" is a
   liftable claim. This replaces a self-attribution *disclaimer* as the primary
   control — the hedge lives in the sentence, not a footnote people ignore. (It
   also matches the existing hypothesis mode in `llm_framing`; we're allowing
   *open* hypotheses rather than catalogue-bounded ones.)

4. **Facts cited, connections questioned.** Any concrete external fact a question
   invokes must be *retrieved and cited* (a real source); only the
   **connection** — "does X explain Y?" — is the open part. This keeps the LLM
   layer provenance-compliant (principle 7), not the one exception to it. The
   failure mode it prevents: a fabricated premise smuggled inside a question
   ("does BYD's new March plant explain this?" when there was no such plant).

5. **Frontier (cloud) model, not local `qwen3.6`.**
   - **Secrecy is a non-issue here.** The source data is public, and in this
     domain there's little scoop-protection value — specialists already know
     what each other are working on; the tool's moat is *sifting hard material*,
     not exclusivity. So the privacy argument for staying local is moot.
   - **The value is world-knowledge + question quality** — exactly where small
     local models are weakest and most prone to inventing world-facts. The
     numeric guard protects *numbers*; it does nothing for a *question*, so model
     quality matters more here, not less.
   - Cost at ~5 takes/cycle is negligible.
   - *Correction logged in discussion:* the personal-vs-Guardian Anthropic
     account boundary applies to **Claude Code** (the dev tool), not to the
     deployed meridian app's own LLM credential — so it is not an argument
     against a cloud model for the app. The only real item is which API account
     the deployed app bills to.

6. **Strictly additive; never blocks.** If generation fails or is rejected, the
   slot stays a placeholder. The deterministic report is complete without it.

7. **Trust stance, enforced structurally.** Takes are "ideas to explore," not
   reportable material — but enforced by *form* (questions), *grounding* (cited
   facts), and *hard visual segregation* (a distinct "machine hypothesis,
   unverified" treatment), **not** by a one-line disclaimer sitting next to a
   rigorously-sourced finding.

## Build path

**v1 — no retrieval.** Frontier model; in-place leading questions on the top ~5
movers; **axes-framing only** — name the categories to check ("volume or price?
which member states? timing vs any tariff change or new capacity coming
online?"), asserting no specific external facts. A prompt change on existing
infra; shippable fast. Serves the *broadening* goal (orients a less-experienced
reporter even where a specialist would find it obvious).

**v2 — retrieval.** Add a search/read tool to the *same* generation call so
questions can name *specific real events*, cited. **Same `LLMSlot`, same UX,
same facts-cited / connections-questioned contract** — only the generation
function grows. This is where the "insights we're not getting from the strict
focus" actually live.

The surface and contract are stable across v1→v2; the rework is confined to the
generation function, so v1 is not throwaway scaffolding.

**Evaluation guard.** Judge v1 by *"is the in-place-question UX right, and does
it surface a worthwhile axis?"* — **not** by a specialist's *"did it tell me
something I didn't know?"* That bar belongs to v2. Showing generic v1 questions
to a specialist (Lisa) risks a false-negative that kills a good idea on the
wrong evidence.

## Consequence: the old `narrative_hs_group` leads

The new in-place leading-questions take is a *fresh* generation, not a
repackaging of the old per-group narrative leads. So `narrative_hs_group` (the
old Leads doc) is **superseded** for the portal. Open: retire it outright, or
keep it for the legacy docx bundle until the portal is proven?

## Parked: the "general" (across-release) take

Deferred deliberately. It is the hallucination danger zone: a cross-release
synthesis is where a model invents *causation* ("the battery surge is driven by
the solar collapse") — a relationship no single finding asserts and the numeric
verifier won't catch (both numbers are real; the link is fabricated). It needs a
stricter rule than the specific take — *juxtapose findings, don't assert
relationships absent from the data* — and probably its own check. Design it
separately once v1 of the specific take is proven.

## Open / next

- The frontier model choice + how the deployed app authenticates to it
  (ops/credential decision, independent of Claude Code).
- v2 retrieval design: which search/news source; the tool-use loop; validating
  that cited links are real and relevant (extend the rejection-log discipline to
  the question layer).
- Visual treatment for the segregated "machine hypothesis" block (design-system
  work).
- Confirm the fate of `narrative_hs_group` (retire vs keep for docx).
