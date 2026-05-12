"""Lead scaffolds from llm_framing — rendered into the leads.md companion."""

from __future__ import annotations

from briefing_pack._helpers import _ALL_UNIVERSAL_CAVEATS, _Section, _trace_token


def _section_llm_narratives(cur) -> _Section:
    """Lead scaffolds from llm_framing — for each HS group with a current
    `narrative_hs_group` finding, render the anomaly summary, picked
    hypotheses, and corroboration steps. Suppressed entirely when there
    are no leads (a journalist who hasn't run the framing pass still gets
    a clean deterministic-only brief).

    Each lead carries an `llm_drafted` caveat plus the union of caveats on
    its underlying findings. We surface those inline so the editorial
    framing is honest about its own provenance.

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
        # Nothing to render — caller treats empty markdown as "skip section".
        return _Section(markdown="")

    lines.append("## Investigation leads")
    lines.append("")
    lines.append(
        f"LLM-scaffolded investigation starts for each HS group, ordered by "
        f"group name. Each lead has three parts: a one-line anomaly "
        f"summary (numerically verified against the underlying findings), "
        f"2-3 candidate hypotheses picked from a curated catalog of "
        f"standard causes for China-EU/UK trade movements, and a list of "
        f"concrete corroboration steps a journalist can run to test the "
        f"hypotheses. The `llm_drafted` caveat tags every block below as "
        f"editorial origin; underlying caveats (low_base, partial_window, "
        f"transshipment_hub, cn8_revision, low_kg_coverage, etc.) "
        f"propagate from the source findings. Trace ids point to the lead "
        f"finding, not the underlying — query "
        f"`findings.detail->>'underlying_finding_ids'` to walk the chain."
    )
    lines.append("")
    for r in rows:
        detail = r["detail"]
        group_name = detail.get("group", {}).get("name", "—")
        caveats = detail.get("caveat_codes") or []
        visible_caveats = [c for c in caveats if c not in _ALL_UNIVERSAL_CAVEATS]
        lines.append(f"### {group_name}")
        lines.append("")
        scaffold = detail.get("lead_scaffold")
        if isinstance(scaffold, dict) and scaffold.get("anomaly_summary"):
            lines.append(f"**Anomaly:** {scaffold['anomaly_summary']}")
            lines.append("")
            hyps = scaffold.get("hypotheses") or []
            if hyps:
                lines.append("**Possible causes:**")
                lines.append("")
                for h in hyps:
                    label = h.get("label") or h.get("id", "—")
                    rationale = h.get("rationale", "")
                    lines.append(f"- *{label}* — {rationale}")
                lines.append("")
            steps = scaffold.get("corroboration_steps") or []
            if steps:
                lines.append("**Corroboration steps:**")
                lines.append("")
                for s in steps:
                    lines.append(f"- {s}")
                lines.append("")
        else:
            # v1 (or any other shape) — fall back to the body field
            lines.append(r["body"])
            lines.append("")
        if visible_caveats:
            lines.append(f"*Caveats from underlying findings: {', '.join(visible_caveats)}*")
        lines.append(
            f"*Underlying findings: "
            f"{', '.join(str(i) for i in detail.get('underlying_finding_ids', []))} "
            f"— Trace: {_trace_token(r['id'])}*"
        )
        lines.append("")

    return _Section(markdown="\n".join(lines))
