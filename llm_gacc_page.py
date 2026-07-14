"""LLM slots for the GACC-only page — the release synthesis + the page-level
questions-take (V1 of dev_notes/2026-07-05-gacc-update-page-design.md § LLM
layer).

Both slots run on the house verify-or-reject contract (`llm_framing`):

**Synthesis** — the page's purpose question answered each month: *is the EU
move part of a general Chinese export surge, or EU-specific re-routing?* A
short declarative paragraph connecting the context strip's readings, plus at
most TWO hypotheses picked from the curated catalog with a one-line rationale
each. Declarative is allowed here precisely because nothing free-floats:
every number must round-trip to the supplied facts (verify_numbers),
hypothesis ids must come from the catalog, and the corroboration steps are
attached deterministically from the catalog — never model-written. This is
the lead-scaffold shape (`llm_framing`) at page level.

**Questions** — the takes contract (interrogative on purpose: a question
can't be lifted into copy as a fact), once at page level, with an
ANSWERABILITY tag per question chosen from a fixed enum — "our data answers
this (and where)" vs "needs an external source (and which)". Selection from
an enum, not generation, so the tag can't hallucinate a capability the tool
doesn't have.

Rejection = silence (the deterministic page stands alone), with the rejected
output preserved in llm_rejection_log for inspection — same editorial cost /
benefit as every other LLM surface here: never confidently wrong.
"""
from __future__ import annotations

import json
import logging

import hypothesis_catalog
import llm_rejection_log
from llm_framing import make_backend, matched_fact_paths, verify_numbers

log = logging.getLogger(__name__)

SYNTHESIS_WORD_CAP = 70        # soft target in the prompt
SYNTHESIS_WORD_HARD = 90       # reject above this
MAX_HYPOTHESES = 2
MAX_QUESTIONS = 3

# The catalog subset offered for the page synthesis — macro/flow-level
# hypotheses that can be argued from partner-level GACC readings. The
# HS-group-shaped entries (cn8_reclassification etc.) are deliberately
# excluded: this page has no product dimension to ground them in.
SYNTHESIS_CATALOG_IDS = (
    "us_tariff_diversion",
    "capacity_expansion_china",
    "eu_demand_pull",
    "tariff_preloading",
    "transshipment_reroute",
    "base_effect",
    "post_pandemic_normalisation",
)

# The catalog subset for the COMMODITY take — product-shaped hypotheses the
# sections-5/6 tables can actually ground (the commodity block gave this
# page a product dimension, so the exclusion rationale above no longer
# applies there). export_controls_china and domestic_demand_pivot were added
# for this take (dev_notes/2026-07-14-gacc-commodity-highlights.md § takes):
# the rare-earths and car-export registers respectively — their catalog
# descriptions carry the causal vocabulary so the model never asserts
# policy events itself.
COMMODITY_CATALOG_IDS = (
    "export_controls_china",
    "domestic_demand_pivot",
    "energy_transition",
    "trade_defence_outcome",
    "capacity_expansion_china",
    "us_tariff_diversion",
    "currency_effect",
    "base_effect",
)

# Answerability enum for the questions-take: the model TAGS each question
# with where its answer lives, choosing from this fixed vocabulary. The
# render supplies the reader-facing copy (and links) per tag — the model
# never writes capability claims.
ANSWERABLE_TAGS = frozenset({
    "world_table",          # the page's China-and-the-world section
    "europe_section",       # the page's Europe-up-close section
    "drawers",              # a figure's provenance drawer
    "eurostat_confirmation",  # answerable when Eurostat covers this month
    "un_comtrade",          # needs third-country / world data we don't hold
    "gacc_no_product_detail",  # needs product detail GACC doesn't publish
})


SYNTHESIS_SYSTEM_PROMPT = """You are a trade-desk research assistant for Guardian journalists.

You are given this month's readings from China's own customs figures (GACC):
China's exports to the EU, the US, ASEAN and the world, plus the sharpest
single move and the biggest swings since last month. Write the ONE paragraph a
desk editor needs (aim ~55 words, max 70): does the EU reading sit above or
below China's general trend this month, and what pattern do the US/ASEAN
readings make with it? Then pick AT MOST TWO hypotheses from the CATALOG that
are consistent with these readings, each with a one-line rationale citing a
number shown.

If the readings make no coherent pattern worth flagging, return
{"summary": null}. Abstaining is correct on an unremarkable month.

Output JSON only:
  {"summary": "<the paragraph>",
   "hypotheses": [{"id": "<catalog id>", "rationale": "<one line>"}]}
  or {"summary": null}

Hard rules — violating any one silently rejects the whole output:
1. Every number you mention MUST appear in the facts shown, unchanged
   (round +20.5% -> +20% or +21%, never -> +22%).
2. hypotheses[].id MUST come from the CATALOG list shown. At most two.
3. Use ONLY the facts shown. No external events, dates, companies or
   policies by name — the catalog descriptions carry the causal vocabulary.
4. State comparisons, never causes: "X rose while Y fell" is yours;
   "because of tariffs" belongs in a hypothesis rationale, hedged.
5. Name the parties ("China's exports to the EU"), never bare "exports".
6. <= 70 words for the summary; one sentence per rationale.

This is a scaffold for a reporter to investigate, never a publishable claim."""


COMMODITY_SYSTEM_PROMPT = """You are a trade-desk research assistant for Guardian journalists.

You are given this month's product-level readings from China's own customs
figures (GACC): every line of its headline commodity catalogue — exports and
imports, value growth (in CNY terms) and, where published, volume growth and
computed milestone / pace facts. Write the ONE paragraph a desk editor needs
(aim ~60 words, max 75): which product moves define this month's release, and
what pattern do they make together (volume vs value, exports vs imports)?
Prefer lines that combine a sharp rate with real scale; a computed milestone
or pace fact is usually the lead. Then pick AT MOST TWO hypotheses from the
CATALOG consistent with those readings, each with a one-line rationale citing
a number shown.

If the readings make no coherent pattern worth flagging, return
{"summary": null}. Abstaining is correct on an unremarkable month.

Output JSON only:
  {"summary": "<the paragraph>",
   "hypotheses": [{"id": "<catalog id>", "rationale": "<one line>"}]}
  or {"summary": null}

Hard rules — violating any one silently rejects the whole output:
1. Every number you mention MUST appear in the facts shown, unchanged
   (round +100.7% -> +100% or +101%, never -> +102%).
2. hypotheses[].id MUST come from the CATALOG list shown. At most two.
3. Use ONLY the facts shown. No external events, dates, companies or
   policies by name — the catalog descriptions carry the causal vocabulary.
4. State comparisons, never causes: "volume fell while value rose" is
   yours; "because of export controls" belongs in a hypothesis rationale,
   hedged.
5. Name the product and direction ("China's exports of rare earths"),
   never bare "exports".
6. These are world totals with no country split — never imply a
   destination.
7. <= 75 words for the summary; one sentence per rationale.

This is a scaffold for a reporter to investigate, never a publishable claim."""


QUESTIONS_SYSTEM_PROMPT = """You are a trade-desk research assistant for Guardian journalists.

You are given this month's readings from China's own customs figures (GACC).
Write AT MOST THREE leading questions a reporter should chase — threads, not
conclusions. Every question MUST be answerable-in-principle, and you must TAG
each with where its answer lives, using EXACTLY one tag from:

  world_table            — the page's China-and-the-world table answers it
  europe_section         — the page's per-country Europe section answers it
  drawers                — a shown figure's provenance drawer answers it
  eurostat_confirmation  — answerable when Eurostat publishes this month (~5 weeks)
  un_comtrade            — needs third-country/world data (UN Comtrade)
  gacc_no_product_detail — needs product detail GACC does not publish

Output JSON only:
  {"questions": [{"q": "<the question>", "axis": "<2-4 word investigative angle>",
                  "answerable": "<tag>"}]}
  or {"questions": null}

Hard rules — violating any one silently rejects the whole output:
1. Each q MUST end with a question mark. Never a flat assertion.
2. Every number you mention MUST appear in the facts shown, unchanged.
3. `answerable` MUST be one of the six tags, verbatim.
4. No external events, dates, companies or policies by name; pointing at a
   CATEGORY is fine ("does the timing fit any tariff round?").
5. If a question implies its own answer, append exactly:  (NB: hypothesis, not a finding)
6. Name the parties and direction, never bare "trade" or "imports".

These are leads for a reporter to investigate, never publishable claims."""


# --------------------------------------------------------------------------
# Prompt assembly
# --------------------------------------------------------------------------

def _facts_lines(facts: dict) -> str:
    return "\n".join(f"- {line}" for line in facts.get("lines", []))


def assemble_synthesis_prompt(facts: dict) -> tuple[str, str]:
    catalog = [h for h in hypothesis_catalog.get_catalog_for_prompt()
               if h["id"] in SYNTHESIS_CATALOG_IDS]
    cat_lines = "\n".join(
        f"- {h['id']}: {h['label']} — {h['description']}" for h in catalog)
    user = (
        "THIS MONTH'S GACC READINGS (China's own customs figures, "
        "EUR-equivalent):\n"
        + _facts_lines(facts)
        + "\n\nCATALOG (pick hypothesis ids from here only):\n"
        + cat_lines
        + '\n\nOutput JSON only: {"summary": "<= 70-word paragraph", '
          '"hypotheses": [{"id", "rationale"}]} or {"summary": null}'
    )
    return SYNTHESIS_SYSTEM_PROMPT, user


def assemble_commodity_prompt(facts: dict) -> tuple[str, str]:
    catalog = [h for h in hypothesis_catalog.get_catalog_for_prompt()
               if h["id"] in COMMODITY_CATALOG_IDS]
    cat_lines = "\n".join(
        f"- {h['id']}: {h['label']} — {h['description']}" for h in catalog)
    user = (
        "THIS MONTH'S GACC COMMODITY READINGS (China's own customs figures; "
        "value growth in CNY terms, volume growth in physical units, EUR "
        "levels display-only; world totals, no country split):\n"
        + _facts_lines(facts)
        + "\n\nCATALOG (pick hypothesis ids from here only):\n"
        + cat_lines
        + '\n\nOutput JSON only: {"summary": "<= 75-word paragraph", '
          '"hypotheses": [{"id", "rationale"}]} or {"summary": null}'
    )
    return COMMODITY_SYSTEM_PROMPT, user


def assemble_questions_prompt(facts: dict) -> tuple[str, str]:
    user = (
        "THIS MONTH'S GACC READINGS (China's own customs figures, "
        "EUR-equivalent):\n"
        + _facts_lines(facts)
        + "\n\nRemember: at most three questions, each tagged with exactly "
          "one answerability tag from the list."
        + '\nOutput JSON only: {"questions": [{"q", "axis", "answerable"}]} '
          'or {"questions": null}'
    )
    return QUESTIONS_SYSTEM_PROMPT, user


# --------------------------------------------------------------------------
# Parse + validate (verify-or-reject; rejection = logged silence)
# --------------------------------------------------------------------------

def _parse_json(raw: str) -> dict | None:
    s = (raw or "").strip()
    if "{" not in s:
        return None
    try:
        return json.loads(s[s.find("{"):s.rfind("}") + 1])
    except Exception:
        return None


def _log_reject(cluster: str, stage: str, reason: str, raw: str,
                failures=None) -> None:
    """Preserve the rejected output for inspection — same audit posture as
    llm_framing's verifier (best-effort; a logging failure never escalates)."""
    try:
        closest_path = closest_val = None
        if failures:
            closest_path = failures[0].closest_fact_path
            closest_val = failures[0].closest_fact_value
        llm_rejection_log.log_rejection(
            scrape_run_id=None, cluster_name=cluster, model=None,
            stage=stage, reason=reason, detail=None, raw_output=raw,
            closest_fact_path=closest_path, closest_fact_value=closest_val,
        )
    except Exception:
        log.exception("gacc-page LLM: failed to log rejection (%s)", cluster)


def _cited_finding_ids(text: str, facts: dict) -> list[int]:
    """The distinct source findings behind the numbers actually cited —
    resolved by the verifier's OWN matcher (llm_framing.matched_fact_paths:
    same extraction, same tolerances), so a finding is cited iff one of its
    numbers is genuinely the one the text used. The previous substring
    heuristic over-cited catastrophically on the commodity take's ~200-fact
    input (65 citations for a four-number summary — 2026-07-14 debut run).
    Falls back to every strip finding when nothing matches (the summary
    always leans on the strip)."""
    fids: list[int] = []
    prov = facts.get("prov") or {}
    for path in matched_fact_paths(text, facts.get("numbers") or {}):
        fid = prov.get(path)
        if fid is not None and fid not in fids:
            fids.append(fid)
    if not fids:
        fids = [f for f in (facts.get("strip_fids") or []) if f is not None]
    return fids


def _generate_scaffold(facts: dict, backend, *, assemble, offered_ids,
                       cluster: str) -> dict | None:
    """Shared verify-or-reject body for the summary-plus-hypotheses takes
    (page synthesis + commodity take). Facts -> verified dict or None
    (abstain / reject / error): {summary, citations,
    hypotheses: [{id, label, rationale, steps}]}."""
    system, user = assemble(facts)
    backend = backend or make_backend(role="takes")
    raw = backend.generate(system, user)
    obj = _parse_json(raw)
    if obj is None:
        _log_reject(cluster, "parse", "unparseable JSON", raw)
        return None
    if obj.get("summary") in (None, "null", ""):
        return None  # first-class abstention
    summary = str(obj["summary"]).strip()
    hyps = obj.get("hypotheses") or []
    if len(summary.split()) > SYNTHESIS_WORD_HARD:
        _log_reject(cluster, "validate",
                    f"too long ({len(summary.split())} words)", raw)
        return None
    if len(hyps) > MAX_HYPOTHESES:
        _log_reject(cluster, "validate",
                    f"{len(hyps)} hypotheses (max {MAX_HYPOTHESES})", raw)
        return None
    offered = set(offered_ids)
    picked: list[dict] = []
    for h in hyps:
        hid = (h or {}).get("id")
        if hid not in offered:
            _log_reject(cluster, "validate",
                        f"hypothesis id {hid!r} not in the offered catalog", raw)
            return None
        entry = hypothesis_catalog.CATALOG_BY_ID[hid]
        picked.append({
            "id": hid,
            "label": entry["label"],
            "rationale": str((h or {}).get("rationale") or "").strip(),
            # Deterministic: the model picks, the catalog supplies the steps.
            "steps": hypothesis_catalog.get_corroboration_steps([hid]),
        })
    check_text = " ".join([summary] + [p["rationale"] for p in picked])
    ok, failures = verify_numbers(check_text, facts.get("numbers") or {})
    if not ok:
        _log_reject(cluster, "validate",
                    f"unverified number(s): {failures}", raw, failures)
        return None
    return {
        "summary": summary,
        "citations": _cited_finding_ids(check_text, facts),
        "hypotheses": picked,
    }


def generate_synthesis(facts: dict, backend=None) -> dict | None:
    """Facts -> verified synthesis dict or None (abstain / reject / error)."""
    return _generate_scaffold(
        facts, backend, assemble=assemble_synthesis_prompt,
        offered_ids=SYNTHESIS_CATALOG_IDS, cluster="gacc_page_synthesis")


def generate_commodity_take(facts: dict, backend=None) -> dict | None:
    """The commodity take (dev_notes/2026-07-14-gacc-commodity-highlights.md
    § takes): the causal/contextual layer over the FULL sections-5/6 fact
    set — the input set equals the displayed set, so every citable number
    has a drawer on the page. Same verify-or-reject contract as the
    synthesis; the commodity-shaped catalog subset carries the causal
    vocabulary (export controls, domestic-demand pivot, …)."""
    return _generate_scaffold(
        facts, backend, assemble=assemble_commodity_prompt,
        offered_ids=COMMODITY_CATALOG_IDS, cluster="gacc_page_commodity")


def generate_questions(facts: dict, backend=None) -> list[dict] | None:
    """Facts -> verified question dicts [{q, axis, answerable}] or None."""
    system, user = assemble_questions_prompt(facts)
    backend = backend or make_backend(role="takes")
    raw = backend.generate(system, user)
    obj = _parse_json(raw)
    if obj is None:
        _log_reject("gacc_page_questions", "parse", "unparseable JSON", raw)
        return None
    qs = obj.get("questions")
    if qs in (None, "null") or not isinstance(qs, list) or not qs:
        return None  # abstention
    if len(qs) > MAX_QUESTIONS:
        _log_reject("gacc_page_questions", "validate",
                    f"{len(qs)} questions (max {MAX_QUESTIONS})", raw)
        return None
    cleaned: list[dict] = []
    for q in qs:
        text = str((q or {}).get("q") or "").strip()
        tag = (q or {}).get("answerable")
        if "?" not in text:
            _log_reject("gacc_page_questions", "validate",
                        "no question mark (interrogative anchor missing)", raw)
            return None
        if tag not in ANSWERABLE_TAGS:
            _log_reject("gacc_page_questions", "validate",
                        f"answerable tag {tag!r} not in the enum", raw)
            return None
        cleaned.append({"q": text,
                        "axis": str((q or {}).get("axis") or "").strip(),
                        "answerable": tag})
    ok, failures = verify_numbers(" ".join(c["q"] for c in cleaned),
                                  facts.get("numbers") or {})
    if not ok:
        _log_reject("gacc_page_questions", "validate",
                    f"unverified number(s): {failures}", raw, failures)
        return None
    return cleaned
