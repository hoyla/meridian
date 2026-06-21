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

from llm_framing import (
    _build_facts,
    _conn,
    _format_facts_for_prompt,
    _load_hs_group_clusters,
)

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


def build_take_prompt(group_name: str) -> tuple[str, str] | None:
    """Assemble the (system, user) prompt for a group's leading-question take.

    Returns None if the group has no loadable findings. The user prompt carries
    only the facts the model is allowed to cite — the same typed set the old
    leads pipeline verifies against, so the verify-or-reject guard transfers
    unchanged."""
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
    return TAKE_SYSTEM_PROMPT, user


def build_take_prompt_for_finding(finding_id: int) -> tuple[str, str] | None:
    """Convenience: finding id → assembled prompt (via its group)."""
    group = group_name_for_finding(finding_id)
    if not group:
        return None
    return build_take_prompt(group)


def _main(argv: list[str]) -> int:
    """CLI: emit the assembled prompt for the dev loop, e.g.
        python -m llm_takes <finding_id>            | claude -p
        python -m llm_takes --group "EV batteries"  | claude -p
    Prints the system prompt then the user prompt (separated), so the whole
    thing is pipeable while the system/user split stays visible."""
    if not argv:
        print("usage: python -m llm_takes <finding_id> | --group <name>",
              file=__import__("sys").stderr)
        return 2
    if argv[0] == "--group":
        result = build_take_prompt(" ".join(argv[1:]))
        label = " ".join(argv[1:])
    else:
        fid = int(argv[0])
        label = group_name_for_finding(fid)
        result = build_take_prompt_for_finding(fid)
    if result is None:
        print(f"no loadable findings for {label!r}", file=__import__("sys").stderr)
        return 1
    system, user = result
    # Combined and pipeable to `claude -p`; the system/user split becomes a
    # proper backend concern later (one coherent prompt is enough for the dev loop).
    print(f"{system}\n\n{user}")
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(_main(sys.argv[1:]))
