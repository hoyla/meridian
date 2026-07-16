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
import re
from dataclasses import dataclass, field

from llm_framing import (
    _SCOPE_PARTIES,
    ClaudeCLIBackend,
    _build_facts,
    _conn,
    _format_facts_for_prompt,
    _load_hs_group_clusters,
    finding_ids_for_paths,
    make_backend,
    matched_fact_paths,
    verify_numbers,
)
from llm_rejection_log import log_rejection

log = logging.getLogger(__name__)

# Top movers are the v1 targets (the most quotable shifts); their findings are
# the EU-27 hs_group_yoy* family.
TAKE_SUBKINDS = ("hs_group_yoy", "hs_group_yoy_export")

# Attempts allowed at getting parseable JSON out of the backend. A parse
# failure is a backend flake, not a judgement — the same bundle produced a
# clean take on the preceding run — so one retry is worth it: each blanked
# slot costs a reporter a take on a top mover. Bounded at 2 and applied to
# the parse path ONLY; a `_validate_questions` rejection is a correctness
# verdict on the content and is never retried. Backend-agnostic by design:
# the default is the CLI (Max subscription, free), and even on claude_api one
# extra call per flake is negligible against losing the slot.
TAKE_PARSE_ATTEMPTS = 2

# ---------------------------------------------------------------------------
# Flow anchoring
#
# A take is generated per-GROUP (the facts span both flows and all scopes so
# cross-flow contrast stays possible), but each headline slot is one specific
# FINDING — one flow, one scope. Without an anchor the model leads with the
# group's dominant story regardless of which slot it sits under: on the
# 2026-07-16 live run the "Finished cars" EXPORT slot (-41.8%) opened with a
# question about IMPORTS (+34.1%) — a different subject — and the two
# same-group slots got near-duplicate takes. The anchor pins question 1 to
# the slot's own flow; the opposite flow stays legal as later contrast.
# ---------------------------------------------------------------------------

_SUBKIND_ANCHOR_RE = re.compile(
    r"^hs_group_(?:yoy|trajectory)(_uk|_combined)?(_export)?$"
)
_SUBKIND_SCOPE = {None: "eu_27", "_uk": "uk", "_combined": "eu_27_plus_uk"}

# Q1 must talk about the anchored flow. Phrasing varies ("EU-27 imports of
# finished cars from China", "UK car exports to China"), so match the flow
# word and its direction preposition with anything in between (within the
# one-sentence question).
_FLOW_ANCHOR_RES = {
    "imports": re.compile(r"\bimports?\b.*?\bfrom\s+China\b", re.IGNORECASE | re.DOTALL),
    "exports": re.compile(r"\bexports?\b.*?\bto\s+China\b", re.IGNORECASE | re.DOTALL),
}


@dataclass
class TakeAnchor:
    """The specific (scope, flow) a take's slot is about."""
    finding_id: int
    scope: str   # eu_27 | uk | eu_27_plus_uk
    flow: str    # imports | exports

    @property
    def party_phrase(self) -> str:
        """Human phrasing for the prompt, e.g. 'EU-27 exports to China'."""
        imports_phrase, exports_phrase = _SCOPE_PARTIES[self.scope]
        return imports_phrase if self.flow == "imports" else exports_phrase


def _anchor_from_subkind(finding_id: int, subkind: str | None) -> TakeAnchor | None:
    """Parse a finding's subkind into its anchor. The subkind encodes both
    scope and flow (hs_group_yoy[_uk|_combined][_export]); an unrecognised
    subkind anchors nothing (the take falls back to unanchored, as before)."""
    m = _SUBKIND_ANCHOR_RE.match(subkind or "")
    if not m:
        return None
    return TakeAnchor(
        finding_id=finding_id,
        scope=_SUBKIND_SCOPE[m.group(1)],
        flow="exports" if m.group(2) else "imports",
    )


@dataclass
class TakeResult:
    """A verified take plus its honest provenance: `grounded_in` opens with
    the slot's anchor finding and then lists every OTHER finding whose
    numbers the questions actually cite (via matched_fact_paths — the same
    matcher the verifier uses). Before this, the slot asserted grounded_in=
    [anchor] while freely citing numbers from sibling findings — a
    provenance trail that didn't contain the numbers shown (2026-07-16:
    the EV import slot's Q3 cited the export finding's -40.9%)."""
    questions: list[dict]
    grounded_in: list[int] = field(default_factory=list)


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
   flat or featureless finding may warrant just one. Before returning a third
   question, delete the weakest of the three and keep it only if all three
   stand independently — three questions on a routine finding is itself a miss.
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
    group, _anchor = _group_and_anchor_for_finding(finding_id)
    return group


def _group_and_anchor_for_finding(
    finding_id: int,
) -> tuple[str | None, TakeAnchor | None]:
    """Resolve a finding id to (group name, flow/scope anchor) in one query.
    The facts stay per-group; the anchor pins the take to the finding's own
    flow and scope."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail->'group'->>'name', subkind FROM findings WHERE id = %s",
            (finding_id,),
        )
        row = cur.fetchone()
    if not row or not row[0]:
        return None, None
    return row[0], _anchor_from_subkind(finding_id, row[1])


def _assemble(
    group_name: str, anchor: TakeAnchor | None = None,
) -> tuple[dict, str, str, dict[str, int]] | None:
    """Load a group's facts and assemble (raw_facts, system, user, id_map).

    `raw_facts` is the typed fact set the verifier checks output numbers
    against — the same set the old leads pipeline verifies against, so the
    guard transfers unchanged; the prompt shows the %/€-formatted form.
    `id_map` is the cluster's scope/flow → finding-id map, for resolving
    which findings the output's numbers actually came from. An `anchor`
    adds the slot-subject instruction (question 1 must address that flow).
    Returns None if the group has no loadable findings."""
    clusters = _load_hs_group_clusters([group_name])
    if not clusters:
        return None
    cluster = clusters[0]
    facts = _build_facts(cluster)
    formatted = _format_facts_for_prompt(facts)
    anchor_block = ""
    if anchor is not None:
        anchor_block = (
            f"ANCHOR — this take sits under ONE specific headline: "
            f"{anchor.party_phrase} ({anchor.flow}). Hard rule, same rejection "
            f"contract as the numbered rules: your FIRST question must be about "
            f"{anchor.party_phrase} — the anchored flow. The opposite flow or "
            f"another scope may appear only in a later question, and only as "
            f"explicit contrast with the anchored flow.\n\n"
        )
    user = (
        f"Finding (HS group): {cluster.group_name}\n"
        f"Definition: {cluster.group_description or '—'}\n\n"
        f"{_SCOPE_LEGEND}\n\n"
        f"{anchor_block}"
        f"FACTS — the only numbers you may cite:\n"
        f"{json.dumps(formatted, indent=2, default=str)}\n\n"
        f"Propose the questions this finding genuinely warrants — usually one, "
        f"sometimes two; a third only when it opens a distinctly different, "
        f"non-obvious thread (rare). Do not pad to three. Output JSON only:\n"
        f'{{"questions": [{{"q": "...", "axis": "..."}}]}}'
    )
    return facts, TAKE_SYSTEM_PROMPT, user, dict(cluster.finding_id_by_scope_attr)


def build_take_prompt(
    group_name: str, anchor: TakeAnchor | None = None,
) -> tuple[str, str] | None:
    """The (system, user) prompt for a group's leading-question take — the
    backend-agnostic dev artifact. Returns None if the group has no findings."""
    assembled = _assemble(group_name, anchor)
    if assembled is None:
        return None
    _facts, system, user, _id_map = assembled
    return system, user


def build_take_prompt_for_finding(finding_id: int) -> tuple[str, str] | None:
    """Convenience: finding id → assembled prompt (via its group), anchored
    to the finding's own flow and scope."""
    group, anchor = _group_and_anchor_for_finding(finding_id)
    if not group:
        return None
    return build_take_prompt(group, anchor)


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
    """Dev default is the CLI (Max subscription, no API key). If a backend is
    configured for takes — LLM_TAKES_BACKEND, or the global LLM_BACKEND (e.g.
    claude_api in production) — honour it via make_backend(role='takes')."""
    if os.environ.get("LLM_TAKES_BACKEND") or os.environ.get("LLM_BACKEND"):
        return make_backend(role="takes")
    return ClaudeCLIBackend()


def _validate_questions(
    questions: list[dict], facts: dict, anchor: TakeAnchor | None = None,
) -> dict | None:
    """Verify-or-reject. Returns a rejection dict on the first failure, else
    None. Each question must be interrogative (contain '?') and cite no number
    absent from the facts (reusing llm_framing.verify_numbers). With an
    `anchor`, the FIRST question must address the anchored flow — the backstop
    for the prompt's ANCHOR rule (2026-07-16: an export slot's take led with
    the import story). Pure — no DB, no LLM — so it's unit-testable in
    isolation."""
    if anchor is not None and questions:
        q1 = questions[0]["q"]
        if not _FLOW_ANCHOR_RES[anchor.flow].search(q1):
            return {
                "reason": "first_question_off_anchor",
                "detail": f"anchor={anchor.party_phrase}; q1={q1[:240]}",
            }
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


def _grounded_in(
    questions: list[dict], facts: dict, id_map: dict[str, int],
    anchor: TakeAnchor | None,
) -> list[int]:
    """Honest provenance for a verified take: the anchor finding first (it is
    the slot's subject even when the questions are qualitative), then every
    other finding whose numbers the questions actually cite — resolved with
    the verifier's own matcher (matched_fact_paths), so a finding is listed
    iff one of its numbers is genuinely the one a question used. Pure."""
    text = " ".join(item["q"] for item in questions)
    cited = finding_ids_for_paths(id_map, matched_fact_paths(text, facts))
    out: list[int] = [anchor.finding_id] if anchor is not None else []
    for fid in cited:
        if fid not in out:
            out.append(fid)
    return out


def generate_take(
    group_name: str, backend=None, *, scrape_run_id: int | None = None,
    anchor: TakeAnchor | None = None,
    parse_attempts: int = TAKE_PARSE_ATTEMPTS,
) -> TakeResult | None:
    """Generate and verify a take for one group. Returns a TakeResult
    (validated questions + the finding ids they actually cite), or None if
    the group has no findings, generation fails, or the output is rejected —
    in which case the LLMSlot stays a placeholder and the report is
    unaffected (the take never blocks).

    verify-or-reject (reusing the leads pipeline's guard): reject any output
    that fails to parse, isn't interrogative, cites a number absent from the
    facts, or (when anchored) opens off the slot's own flow. Rejections are
    logged to llm_rejection_log for later inspection.

    Malformed JSON is re-asked up to `parse_attempts` times (see
    TAKE_PARSE_ATTEMPTS) because it's a non-deterministic backend flake rather
    than anything wrong with the finding. A validation rejection is NOT retried
    — neither an unverified number nor an off-anchor opener: both are
    correctness verdicts on the content and stay rejected first time. Every
    attempt is logged, so the flake rate stays measurable even when the retry
    saves the slot."""
    assembled = _assemble(group_name, anchor)
    if assembled is None:
        return None
    facts, system, user, id_map = assembled
    backend = backend or _default_backend()
    model_name = getattr(backend, "model", None) or backend.__class__.__name__

    questions = None
    for attempt in range(1, max(1, parse_attempts) + 1):
        try:
            raw = backend.generate(system, user)
        except Exception as e:  # transport/backend failure → placeholder, don't block
            log.warning("take generation failed for %r: %s", group_name, e)
            return None

        questions = _parse_questions(raw)
        if questions is not None:
            break
        log.warning("take rejected (parse) for %r [attempt %d/%d]",
                    group_name, attempt, parse_attempts)
        log_rejection(scrape_run_id=scrape_run_id, cluster_name=group_name,
                      model=model_name, stage="parse",
                      reason="json_parse_error",
                      detail=f"attempt {attempt}/{parse_attempts}",
                      raw_output=raw[:4000])
    if questions is None:
        return None

    rejection = _validate_questions(questions, facts, anchor)
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

    return TakeResult(
        questions=questions,
        grounded_in=_grounded_in(questions, facts, id_map, anchor),
    )


def generate_take_for_finding(
    finding_id: int, backend=None, *, scrape_run_id: int | None = None,
) -> TakeResult | None:
    """Convenience: finding id → verified take (via its group), anchored to
    the finding's own flow and scope."""
    group, anchor = _group_and_anchor_for_finding(finding_id)
    if not group:
        return None
    return generate_take(group, backend, scrape_run_id=scrape_run_id,
                         anchor=anchor)


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

    anchor = None
    if rest[0] == "--group":
        group = " ".join(rest[1:]).strip()
    else:
        group, anchor = _group_and_anchor_for_finding(int(rest[0]))
    if not group:
        print(f"no group for {rest!r}", file=sys.stderr)
        return 1

    if do_generate:
        take = generate_take(group, anchor=anchor)
        if take is None:
            print(f"take rejected or unavailable for {group!r}", file=sys.stderr)
            return 1
        print(json.dumps(
            {"questions": take.questions, "grounded_in": take.grounded_in},
            indent=2, ensure_ascii=False))
        return 0

    result = build_take_prompt(group, anchor)
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
