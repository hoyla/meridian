"""Tier 3 partner-share findings — China's share of EU-27 imports/exports.

The Soapbox A1 analytical register: "China supplied X% of EU import
quantity but only Y% of value." Surfaced as findings under subkind
`partner_share[_export]`. Each block shows the latest 12mo rolling
shares per group + the qty-vs-value gap, ordered by absolute share
descending so the high-China-share lines lead.
"""

from __future__ import annotations

from briefing_pack._helpers import _Section, _fmt_eur, _trace_token


def _section_partner_share(cur, flow: int = 1) -> _Section:
    """Latest partner_share finding per HS group, top to bottom by share.

    Scope is EU-27 only (the world-aggregate denominator is Eurostat-side;
    HMRC doesn't have an equivalent all-partner table yet). Renders one
    block per HS group with: share by value, share by tonnes, the gap, and
    a brief framing note when the qty-vs-value gap exceeds 5 pp (the
    threshold below which the gap is editorially noise-band)."""
    subkind = "partner_share" if flow == 1 else "partner_share_export"
    direction_label = (
        "China's share of EU-27 extra-EU imports" if flow == 1
        else "China's share of EU-27 extra-EU exports"
    )
    cur.execute(
        """
        WITH latest AS (
          SELECT DISTINCT ON (detail->'group'->>'name')
                 id,
                 detail->'group'->>'name' AS group_name,
                 (detail->'windows'->>'current_end')::date AS current_end,
                 (detail->'totals'->>'cn_12mo_eur')::numeric AS cn_eur,
                 (detail->'totals'->>'world_12mo_eur')::numeric AS world_eur,
                 (detail->'totals'->>'cn_12mo_kg')::numeric AS cn_kg,
                 (detail->'totals'->>'world_12mo_kg')::numeric AS world_kg,
                 (detail->'totals'->>'share_value')::numeric AS share_value,
                 (detail->'totals'->>'share_kg')::numeric AS share_kg,
                 (detail->'totals'->>'qty_minus_value_pp')::numeric AS gap_pp
            FROM findings
           WHERE subkind = %s AND superseded_at IS NULL
        ORDER BY detail->'group'->>'name',
                 (detail->'windows'->>'current_end')::date DESC, id DESC
        )
        SELECT * FROM latest ORDER BY share_value DESC NULLS LAST
        """,
        (subkind,),
    )
    rows = list(cur.fetchall())

    lines: list[str] = []
    if not rows:
        return _Section(markdown="")

    lines.append(f"### {direction_label} — partner-share snapshot")
    lines.append("")
    lines.append(
        "China's share of EU-27 import flow from outside the EU, per HS "
        "group, both by value and by tonnes — with the gap between them "
        "(positive gap = tonnage share exceeds value share, consistent "
        "with downward unit-price pressure). Denominator is **extra-EU "
        "imports only** (intra-EU trade is excluded) so the share matches "
        "the Soapbox editorial register: \"China supplied X% of EU "
        "imports of Y\". The 12-month window ends at the latest period "
        "with matching numerator + denominator data."
    )
    lines.append("")
    lines.append(
        "*Methodology footer covers the universal caveats — "
        "`extra_eu_definitional_drift` in particular hedges the absolute "
        "figure while leaving the direction of the qty-vs-value gap intact.*"
    )
    lines.append("")

    for r in rows:
        share_v = float(r["share_value"]) * 100 if r["share_value"] is not None else None
        share_k = float(r["share_kg"]) * 100 if r["share_kg"] is not None else None
        gap = float(r["gap_pp"]) if r["gap_pp"] is not None else None

        lines.append(f"#### {r['group_name']}")
        lines.append(
            f"- **Period (12mo ending)**: {r['current_end'].strftime('%Y-%m')}"
        )
        lines.append(
            f"- **Share by value**: "
            f"{(f'{share_v:.1f}%') if share_v is not None else 'n/a'} "
            f"({_fmt_eur(r['cn_eur'])} of {_fmt_eur(r['world_eur'])})"
        )
        if share_k is not None and r["world_kg"]:
            tonnes_cn = float(r["cn_kg"]) / 1e3
            tonnes_world = float(r["world_kg"]) / 1e3
            lines.append(
                f"- **Share by tonnes**: {share_k:.1f}% "
                f"({tonnes_cn:,.1f}k of {tonnes_world:,.1f}k t)"
            )
        if gap is not None:
            lines.append(
                f"- **Gap (qty − value)**: **{gap:+.1f} pp**"
                + (
                    " — *qty share exceeds value share; consistent with downward unit-price pressure*"
                    if gap > 5
                    else " — *value share exceeds qty share; consistent with premium pricing*"
                    if gap < -5
                    else " — *gap within ±5 pp noise band; treat both shares as roughly equal*"
                )
            )
        lines.append(f"- *Trace*: {_trace_token(r['id'])}")
        lines.append("")

    return _Section(markdown="\n".join(lines))
