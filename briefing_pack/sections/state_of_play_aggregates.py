"""Tier 2 sibling — GACC non-EU-bloc aggregate YoY summary."""

from __future__ import annotations

from typing import Any

from briefing_pack._helpers import _Section, _fmt_eur, _trace_token


def _section_state_of_play_aggregates(cur) -> _Section:
    """Tier 2 sibling — compact summary of GACC non-EU-bloc aggregate
    YoY findings (ASEAN, RCEP, Belt & Road, Latin America, Africa,
    world Total). Parallel structure to `_section_state_of_play` but
    one block per aggregate label rather than per HS group.

    Editorially this is the "China's trade with the rest of the world"
    view that contextualises the EU-CN deep-dive. Soapbox routinely
    cites these aggregates alongside EU-CN figures; without this
    section a journalist reading the export sees only EU-CN coverage
    and has to query the DB directly for the bloc context.

    Findings come from gacc_aggregate_yoy (China-side exports) and
    gacc_aggregate_yoy_import (China-side imports). The aggregate
    label sits under detail.aggregate.raw_label (NOT detail.group.name
    — gacc_aggregate is keyed by aggregate, not HS group)."""
    cur.execute(
        """
        WITH latest AS (
          SELECT DISTINCT ON (detail->'aggregate'->>'raw_label', subkind)
                 id,
                 detail->'aggregate'->>'raw_label' AS agg_label,
                 detail->'aggregate'->>'kind' AS agg_kind,
                 subkind,
                 (detail->'windows'->>'current_end')::date AS current_end,
                 (detail->'totals'->>'yoy_pct')::numeric AS yoy_pct,
                 (detail->'totals'->>'current_12mo_eur')::numeric AS cur_eur,
                 (detail->'totals'->>'partial_window')::boolean AS partial_window
            FROM findings
           WHERE subkind LIKE 'gacc_aggregate_yoy%%' AND superseded_at IS NULL
        ORDER BY detail->'aggregate'->>'raw_label', subkind,
                 (detail->'windows'->>'current_end')::date DESC, id DESC
        )
        SELECT * FROM latest ORDER BY agg_label, subkind
        """
    )
    rows = list(cur.fetchall())

    lines: list[str] = []
    lines.append("## Tier 2 — Current state of play: GACC partner aggregates")
    lines.append("")
    lines.append(
        "China-side YoY for the non-EU-bloc partner aggregates GACC "
        "publishes — ASEAN, RCEP, Belt & Road, Africa, Latin America, "
        "world Total. The flow direction is from China's perspective: "
        "**Exports** = China sells; **Imports** = China buys. Context "
        "for the EU-CN per-HS-group view below — Soapbox routinely "
        "quotes these aggregates alongside EU-CN figures."
    )
    lines.append("")

    if not rows:
        lines.append("*No active gacc_aggregate_yoy findings to render.*")
        lines.append("")
        return _Section(markdown="\n".join(lines))

    # Index by aggregate label → {subkind: row}. Each aggregate gets
    # one heading; under it, one bullet per flow direction.
    by_agg: dict[str, dict[str, Any]] = {}
    for r in rows:
        by_agg.setdefault(r["agg_label"], {})[r["subkind"]] = r

    SCOPE_ORDER = [
        ("gacc_aggregate_yoy", "Exports (China → aggregate)"),
        ("gacc_aggregate_yoy_import", "Imports (aggregate → China)"),
    ]

    for agg_label in sorted(by_agg.keys()):
        by_sk = by_agg[agg_label]
        # Show the kind alongside the label so a reader knows whether they're
        # looking at ASEAN (asean), Latin America (region), etc. Helpful when
        # two aggregates share the kind label.
        any_row = next(iter(by_sk.values()))
        kind = any_row["agg_kind"] or ""
        kind_str = f" *({kind})*" if kind else ""
        lines.append(f"### {agg_label}{kind_str}")
        lines.append("")
        for sk, label in SCOPE_ORDER:
            r = by_sk.get(sk)
            if not r:
                continue
            yoy_v = float(r["yoy_pct"]) * 100 if r["yoy_pct"] is not None else None
            yoy_str = f"{yoy_v:+.1f}%" if yoy_v is not None else "n/a"
            partial = " — partial window" if r["partial_window"] else ""
            lines.append(
                f"  - **{label}**: {yoy_str} YoY to {_fmt_eur(r['cur_eur'])} "
                f"(12mo to {r['current_end']}).{partial} {_trace_token(r['id'])}"
            )
        lines.append("")

    return _Section(markdown="\n".join(lines))
