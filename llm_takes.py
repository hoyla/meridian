"""LLM per-finding *take* — the portal's specific `LLMSlot`, v1.

A "take" is 1–3 **leading questions** attached under a top-mover finding —
threads a reporter might pull, not analysis or conclusions. The interrogative
form is the safety mechanism: a question can't be lifted into copy as a fact.

Design: `dev_notes/2026-06-21-llm-takes-design.md`. This module is the
backend-agnostic **prompt-assembly** piece (the first buildable step, and the
part whose wording wants real iteration). It deliberately reuses the proven
machinery in `llm_framing`:

- `_load_hs_group_clusters` / `_build_facts` — the typed, DB-loaded fact set for
  a group (the move, volume-vs-value, scopes, trajectory, caveats).
- `_format_facts_for_prompt` — the %/€ formatting the model should cite verbatim.
- `verify_numbers` — the numeric guard reused later for verify-or-reject.

v1 vs v2 (see the design note): v1 names *axes* to investigate and asserts NO
specific external facts. v2 adds a retrieval tool and relaxes rule 3 to "specific
external facts allowed iff retrieved and cited" — same prompt, same contract.

Not yet wired here (separate steps): the Claude backend, generation +
verify-or-reject, and populating the `LLMSlot` in `report_builder`.
"""

from __future__ import annotations

import json
import logging
import os

from llm_framing import (
    ClaudeCLIBackend,
    _build_facts,
    _conn,
    _format_facts_for_prompt,
    _load_hs_group_clusters,
    make_backend,
    verify_numbers,
)
from llm_rejection_log import log_rejection

log = logging.getLogger(__name__)

# Top movers are the v1 targets (the most quotable shifts); their findings are
# the EU-27 hs_group_yoy* family.
TAKE_SUBKINDS = ("hs_group_yoy", "hs_group_yoy_export")


TAKE_SYSTEM_PROMPT = """You are a trade-desk research assistant for Guardian journalists.

For ONE finding about China–Europe trade, you propose investigative ANGLES as
LEADING QUESTIONS — threads a reporter might pull. You are NOT writing analysis,
and you draw NO conclusions. You point at what is worth checking.

Output a JSON object, nothing else:
  {"questions": [{"q": "<one-sentence question>", "axis": "<short tag>"}, ...]}

Hard rules — violating any one gets your whole output silently rejected:

1. EVERY item is a QUESTION — a single interrogative sentence containing "?".
   Never a statement or conclusion: write "Is this rise volume- or
   price-driven?", never "This rise is volume-driven."
   Leading questions are GOOD — a question may point at its likely answer when
   the facts support it; that is often the most useful kind. But WHENEVER a
   question implies or embeds its own answer, append this EXACT marker right
   after the "?":  (NB: hypothesis, not a finding)
   so the hedge travels with the sentence if a reporter copies it. A genuinely
   open question (one that implies no answer) does not need the marker.
2. EVERY number you mention MUST appear in the FACTS block, unchanged. You may
   round +34.2% to "34%" but not to "35%". Prefer to phrase questions
   qualitatively; cite a figure only when it sharpens the question.
3. DO NOT name a specific external event, date, company, policy, or place that
   is not in the FACTS — do not assert that any such thing exists. You MAY point
   at CATEGORIES to check ("does the timing coincide with any tariff or
   anti-subsidy measure?", "is one member state driving this?"). Naming specific
   real events is a later, retrieval-backed capability; you do not have it.
4. Ground every question in what the FACTS make salient: the direction and size
   of the move, the volume-vs-value (kg vs EUR) split, member-state
   concentration, trajectory shape, China's import/export share, any caveat.
5. DEFAULT TO ONE OR TWO questions; three is the rare exception, not the norm —
   allowed only for an unusually rich finding where a third question is genuinely
   non-obvious. Do NOT add a question just because a generic axis is available:
   volume-vs-price, member-state concentration, and policy/timing apply to
   almost EVERY finding, so raise one of those only when THIS finding's facts
   make it specifically pointed. One sharp question beats three generic ones; a
   flat or featureless finding may warrant just one.
6. Always name the scope and parties — "EU-27 imports from China", "UK exports
   to China" — never bare "imports" or "exports".
7. Output VALID JSON ONLY. No markdown, no preamble, no code fences.

These are leads for a reporter to investigate, never publishable claims."""


_SCOPE_LEGEND = (
    "Scopes (all China-trade): `eu_27` = Eurostat, EU-27 reporters, partners "
    "CN+HK+MO (\"EU-27 imports from China\" / \"EU-27 exports to China\"); "
    "`uk` = HMRC, UK only; `eu_27_plus_uk` = cross-source sum. Within a scope, "
    "`imports` = goods INTO the reporter from China, `exports` = the reporter's "
    "goods TO China. Lead with eu_27; mention uk/combined only on real "
    "divergence. An absent scope means no data, NOT zero."
)


def group_name_for_finding(finding_id: int) -> str | None:
    """Resolve a finding id to its hs_group name (the take is per-group)."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail->'group'->>'name' FROM findings WHERE id = %s",
            (finding_id,),
        )
        row = cur.fetchone()
    return row[0] if row and row[0] else None


def _assemble(group_name: str) -> tuple[dict, str, str] | None:
    """Load a group's facts and assemble (raw_facts, system, user).

    `raw_facts` is the typed fact set the verifier checks output numbers
    against — the same set the old leads pipeline verifies against, so the
    guard transfers unchanged; the prompt shows the %/€-formatted form. Returns
    None if the group has no loadable findings."""
    clusters = _load_hs_group_clusters([group_name])
    if not clusters:
        return None
    cluster = clusters[0]
    facts = _build_facts(cluster)
    formatted = _format_facts_for_prompt(facts)
    user = (
        f"Finding (HS group): {cluster.group_name}\n"
        f"Definition: {cluster.group_description or '—'}\n\n"
        f"{_SCOPE_LEGEND}\n\n"
        f"FACTS — the only numbers you may cite:\n"
        f"{json.dumps(formatted, indent=2, default=str)}\n\n"
        f"Propose 1–3 leading questions per the rules. Output JSON only:\n"
        f'{{"questions": [{{"q": "...", "axis": "..."}}]}}'
    )
    return facts, TAKE_SYSTEM_PROMPT, user


def build_take_prompt(group_name: str) -> tuple[str, str] | None:
    """The (system, user) prompt for a group's leading-question take — the
    backend-agnostic dev artifact. Returns None if the group has no findings."""
    assembled = _assemble(group_name)
    if assembled is None:
        return None
    _facts, system, user = assembled
    return system, user


def build_take_prompt_for_finding(finding_id: int) -> tuple[str, str] | None:
    """Convenience: finding id → assembled prompt (via its group)."""
    group = group_name_for_finding(finding_id)
    if not group:
        return None
    return build_take_prompt(group)


# ---------------------------------------------------------------------------
# Generation + verify-or-reject guard
# ---------------------------------------------------------------------------

def _parse_questions(raw: str) -> list[dict] | None:
    """Parse the model's JSON, tolerating a ```-fence. Returns the questions
    list ([{q, axis}, …]) or None on any structural problem."""
    s = raw.strip()
    if s.startswith("```"):
        nl = s.find("\n")
        if nl != -1:
            s = s[nl + 1:]
        if s.endswith("```"):
            s = s[:-3]
        s = s.strip()
    try:
        obj = json.loads(s)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict):
        return None
    qs = obj.get("questions")
    if not isinstance(qs, list) or not qs:
        return None
    out = [
        {"q": str(it["q"]).strip(), "axis": str(it.get("axis", "")).strip()}
        for it in qs
        if isinstance(it, dict) and it.get("q")
    ]
    return out or None


def _default_backend():
    """Dev default is the CLI (Max subscription, no API key). If LLM_BACKEND is
    set (e.g. claude_api in production) honour it via make_backend()."""
    if os.environ.get("LLM_BACKEND"):
        return make_backend()
    return ClaudeCLIBackend()


def _validate_questions(questions: list[dict], facts: dict) -> dict | None:
    """Verify-or-reject. Returns a rejection dict on the first failure, else
    None. Each question must be interrogative (contain '?') and cite no number
    absent from the facts (reusing llm_framing.verify_numbers). Pure — no DB,
    no LLM — so it's unit-testable in isolation."""
    for item in questions:
        q = item["q"]
        if "?" not in q:  # interrogative form is the safety contract
            return {"reason": "not_interrogative", "detail": q[:300]}
        ok, failures = verify_numbers(q, facts)
        if not ok:
            f0 = failures[0]
            return {
                "reason": "number_not_in_facts",
                "detail": f"{f0.raw_text} ({f0.kind})",
                "closest_fact_path": f0.closest_fact_path,
                "closest_fact_value": f0.closest_fact_value,
            }
    return None


def generate_take(
    group_name: str, backend=None, *, scrape_run_id: int | None = None,
) -> list[dict] | None:
    """Generate and verify a take for one group. Returns the validated
    questions, or None if the group has no findings, generation fails, or the
    output is rejected — in which case the LLMSlot stays a placeholder and the
    report is unaffected (the take never blocks).

    verify-or-reject (reusing the leads pipeline's guard): reject any output
    that fails to parse, isn't interrogative, or cites a number absent from the
    facts. Rejections are logged to llm_rejection_log for later inspection."""
    assembled = _assemble(group_name)
    if assembled is None:
        return None
    facts, system, user = assembled
    backend = backend or _default_backend()
    model_name = getattr(backend, "model", None) or backend.__class__.__name__

    try:
        raw = backend.generate(system, user)
    except Exception as e:  # transport/backend failure → placeholder, don't block
        log.warning("take generation failed for %r: %s", group_name, e)
        return None

    questions = _parse_questions(raw)
    if questions is None:
        log.warning("take rejected (parse) for %r", group_name)
        log_rejection(scrape_run_id=scrape_run_id, cluster_name=group_name,
                      model=model_name, stage="parse",
                      reason="json_parse_error", raw_output=raw[:4000])
        return None

    rejection = _validate_questions(questions, facts)
    if rejection is not None:
        log.warning("take rejected (%s) for %r: %s",
                    rejection["reason"], group_name, rejection.get("detail", ""))
        log_rejection(scrape_run_id=scrape_run_id, cluster_name=group_name,
                      model=model_name, stage="validate",
                      reason=rejection["reason"], detail=rejection.get("detail"),
                      raw_output=raw[:4000],
                      closest_fact_path=rejection.get("closest_fact_path"),
                      closest_fact_value=rejection.get("closest_fact_value"))
        return None

    return questions


def generate_take_for_finding(
    finding_id: int, backend=None, *, scrape_run_id: int | None = None,
) -> list[dict] | None:
    """Convenience: finding id → verified take (via its group)."""
    group = group_name_for_finding(finding_id)
    if not group:
        return None
    return generate_take(group, backend, scrape_run_id=scrape_run_id)


def _main(argv: list[str]) -> int:
    """CLI. Emit the assembled prompt (default, pipeable to `claude -p`):
        python -m llm_takes <finding_id>
        python -m llm_takes --group "<name>"
    Or generate + verify a take end-to-end (uses the configured backend;
    defaults to the `claude -p` CLI, i.e. the Max subscription):
        python -m llm_takes --generate <finding_id>
        python -m llm_takes --generate --group "<name>"
    """
    import sys
    usage = ("usage: python -m llm_takes [--generate] "
             "<finding_id> | --group <name>")
    do_generate = bool(argv) and argv[0] == "--generate"
    rest = argv[1:] if do_generate else argv
    if not rest:
        print(usage, file=sys.stderr)
        return 2

    if rest[0] == "--group":
        group = " ".join(rest[1:]).strip()
    else:
        group = group_name_for_finding(int(rest[0]))
    if not group:
        print(f"no group for {rest!r}", file=sys.stderr)
        return 1

    if do_generate:
        questions = generate_take(group)
        if questions is None:
            print(f"take rejected or unavailable for {group!r}", file=sys.stderr)
            return 1
        print(json.dumps({"questions": questions}, indent=2, ensure_ascii=False))
        return 0

    result = build_take_prompt(group)
    if result is None:
        print(f"no loadable findings for {group!r}", file=sys.stderr)
        return 1
    system, user = result
    # Combined and pipeable to `claude -p` (one coherent prompt is enough for
    # the dev loop; the system/user split is the API backend's concern).
    print(f"{system}\n\n{user}")
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(_main(sys.argv[1:]))
