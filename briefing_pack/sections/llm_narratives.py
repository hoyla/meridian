"""Lead scaffolds from llm_framing — rendered into the leads.md companion."""

from __future__ import annotations

import re

from briefing_pack._helpers import _ALL_UNIVERSAL_CAVEATS, _Section, _trace_token


# Leads are intended to be shared standalone (not relative to a checkout),
# so doc cross-links resolve to canonical GitHub URLs rather than relative
# paths. If the repo URL changes again, update here.
_DOCS_BASE = "https://github.com/hoyla/meridian/blob/main/docs/"
_METHODOLOGY_CAVEATS = _DOCS_BASE + "methodology.md#3-caveats-reference"
_GLOSSARY = _DOCS_BASE + "glossary.md"

# Caveat codes — every occurrence links to the methodology caveats table
# (rather than the glossary's "Caveat" meta-entry). Includes both family-
# universal and per-finding-variable codes, because the LLM occasionally
# mentions universal codes by name in rationales.
_CAVEAT_CODES = (
    "cross_source_sum",
    "low_base_effect",
    "partial_window",
    "transshipment_hub",
    "low_kg_coverage",
    "low_baseline_n",
    "aggregate_composition",
    "aggregate_composition_drift",
    "cn8_revision",
    "multi_partner_sum",
    "cif_fob",
    "currency_timing",
    "classification_drift",
    "eurostat_stat_procedure_mix",
    "general_vs_special_trade",
    "extra_eu_definitional_drift",
    "llm_drafted",
)

# Glossary terms worth linking on first occurrence in lead prose. Order
# matters: longer / more-specific phrases first so they win the regex
# match before a shorter substring (e.g. "mirror gap" before bare
# "transshipment"). Standard journalism-register terms (EU-27, imports,
# percentages, etc.) are deliberately omitted — they'd flood the prose.
_GLOSSARY_TERMS: tuple[tuple[str, str], ...] = (
    ("CN+HK+MO", _GLOSSARY + "#cnhkmo"),
    ("mirror-trade", _GLOSSARY + "#mirror-trade--mirror-gap"),
    ("mirror trade", _GLOSSARY + "#mirror-trade--mirror-gap"),
    ("mirror gap", _GLOSSARY + "#mirror-trade--mirror-gap"),
    ("partner share", _GLOSSARY + "#partner-share"),
    ("transshipment", _GLOSSARY + "#transshipment--transshipment-hub"),
    ("single-month YoY", _GLOSSARY + "#single-month--2-month--12mo-rolling--ytd-yoy"),
    ("12mo rolling", _GLOSSARY + "#single-month--2-month--12mo-rolling--ytd-yoy"),
)


def _linkify_first_occurrence(
    text: str,
    seen: set[str],
    terms: tuple[tuple[str, str], ...],
) -> str:
    """Wrap the first occurrence of each unseen term in `text` with a
    Markdown link. `seen` accumulates lowercased terms across calls so
    consecutive paragraphs within one lead don't all get linked.

    Word-boundary matching is custom because some terms contain
    underscores or punctuation that the standard \\b doesn't recognise.
    The lookarounds reject matches inside larger identifiers
    (e.g. `transshipment` inside `transshipment_hub`).
    """
    for term, url in terms:
        key = term.lower()
        if key in seen:
            continue
        pattern = re.compile(
            rf'(?<![a-zA-Z0-9_])({re.escape(term)})(?![a-zA-Z0-9_+])',
            re.IGNORECASE,
        )
        new_text, n = pattern.subn(rf'[\1]({url})', text, count=1)
        if n > 0:
            text = new_text
            seen.add(key)
    return text


def _linkify_caveat_codes(text: str, seen: set[str]) -> str:
    """Every appearance of a caveat code in lead prose gets linked
    (not just first occurrence) — codes are jargon that the reader is
    likely to stop on each time. Tracked in `seen` for consistency with
    the glossary linker but not gated on it."""
    for code in _CAVEAT_CODES:
        pattern = re.compile(
            rf'(?<![a-zA-Z0-9_])({re.escape(code)})(?![a-zA-Z0-9_])',
        )
        text = pattern.sub(rf'[`\1`]({_METHODOLOGY_CAVEATS})', text)
        seen.add(code.lower())
    return text


def _linkify_lead_prose(text: str, seen: set[str]) -> str:
    """Convenience: caveat codes always; glossary terms on first occurrence."""
    text = _linkify_caveat_codes(text, seen)
    text = _linkify_first_occurrence(text, seen, _GLOSSARY_TERMS)
    return text


def _section_llm_narratives(cur) -> _Section:
    """Lead scaffolds from llm_framing — for each HS group with a current
    `narrative_hs_group` finding, render the anomaly summary, picked
    hypotheses, and corroboration steps. Suppressed entirely when there
    are no leads (a journalist who hasn't run the framing pass still gets
    a clean deterministic-only brief).

    Each lead carries an `llm_drafted` caveat plus the union of caveats on
    its underlying findings. They're surfaced inline (caveat codes get
    methodology-table links) and in a per-lead **Provenance** bullet
    block at the end so the editorial framing is honest about its own
    origin.

    For backward compatibility this still reads any v1 prose-narrative
    findings via the body field — they render as a single paragraph
    block. New v2 lead-scaffold findings render with the structured
    breakdown sourced from `detail.lead_scaffold`.
    """
    cur.execute(
        """
        SELECT f.id, f.body, f.detail, f.last_confirmed_at
          FROM findings f
         WHERE f.subkind = 'narrative_hs_group'
           AND f.superseded_at IS NULL
      ORDER BY f.detail->'group'->>'name'
        """
    )
    rows = cur.fetchall()

    lines: list[str] = []
    if not rows:
        return _Section(markdown="")

    lines.append("## Investigation leads")
    lines.append("")
    lines.append(
        "LLM-scaffolded investigation starts for each HS group, ordered by "
        "group name. Each lead has three parts: a one-line anomaly summary "
        "(numerically verified against the underlying findings), 2–3 "
        "candidate hypotheses picked from a curated catalog of standard "
        "causes for China-EU/UK trade movements, and a list of concrete "
        "corroboration steps a journalist can run to test the hypotheses. "
        f"Caveat codes link to the [methodology caveats reference]"
        f"({_METHODOLOGY_CAVEATS}); other jargon links to the "
        f"[glossary]({_GLOSSARY}) on first occurrence per lead."
    )
    lines.append("")
    for r in rows:
        detail = r["detail"]
        group_name = detail.get("group", {}).get("name", "—")
        caveats = detail.get("caveat_codes") or []
        visible_caveats = [c for c in caveats if c not in _ALL_UNIVERSAL_CAVEATS]
        lines.append(f"### {group_name}")
        lines.append("")

        # First-occurrence linking is per-lead: each lead starts with a
        # fresh `seen` set so terms get linked once per lead, not once
        # per file.
        seen: set[str] = set()
        scaffold = detail.get("lead_scaffold")
        if isinstance(scaffold, dict) and scaffold.get("anomaly_summary"):
            summary = _linkify_lead_prose(scaffold["anomaly_summary"], seen)
            lines.append(f"**Anomaly:** {summary}")
            lines.append("")
            hyps = scaffold.get("hypotheses") or []
            if hyps:
                lines.append("**Possible causes:**")
                lines.append("")
                for h in hyps:
                    label = h.get("label") or h.get("id", "—")
                    rationale = _linkify_lead_prose(h.get("rationale", ""), seen)
                    lines.append(f"- *{label}* — {rationale}")
                lines.append("")
            steps = scaffold.get("corroboration_steps") or []
            if steps:
                lines.append("**Corroboration steps:**")
                lines.append("")
                for s in steps:
                    # Corroboration steps come deterministically from the
                    # catalog (not LLM-drafted), but they can mention
                    # caveat codes — still apply linkification for
                    # consistency.
                    lines.append(f"- {_linkify_lead_prose(s, seen)}")
                lines.append("")
        else:
            # v1 (or any other shape) — fall back to the body field.
            lines.append(_linkify_lead_prose(r["body"], seen))
            lines.append("")

        # Provenance block: each fact on its own bullet, caveat codes
        # linked to the methodology reference, trace token preserved.
        lines.append("**Provenance:**")
        lines.append("")
        if visible_caveats:
            linked = ", ".join(
                f"[`{c}`]({_METHODOLOGY_CAVEATS})" for c in visible_caveats
            )
            lines.append(f"- *Caveats from underlying findings*: {linked}")
        underlying_ids = detail.get("underlying_finding_ids") or []
        if underlying_ids:
            ids_str = ", ".join(str(i) for i in underlying_ids)
            lines.append(f"- *Underlying findings*: {ids_str}")
        lines.append(f"- *Trace*: {_trace_token(r['id'])}")
        lines.append("")

    return _Section(markdown="\n".join(lines))
