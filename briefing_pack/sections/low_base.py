"""Editorial review queue: hs_group_yoy* findings flagged low_base."""

from __future__ import annotations

from briefing_pack._helpers import _Section, _fmt_eur, _fmt_pct, _trace_token


def _section_low_base(cur) -> _Section:
    """Editorial review queue: every hs_group_yoy*-flavoured finding flagged low_base."""
    cur.execute(
        """
        SELECT id, subkind,
               detail->'group'->>'name' AS group_name,
               to_char((detail->'windows'->>'current_end')::date, 'YYYY-MM') AS period,
               (detail->'totals'->>'current_12mo_eur')::numeric AS current_eur,
               (detail->'totals'->>'prior_12mo_eur')::numeric AS prior_eur,
               (detail->'totals'->>'yoy_pct')::numeric AS yoy_pct,
               (detail->'totals'->>'low_base_threshold_eur')::numeric AS threshold
          FROM findings
         WHERE subkind IN ('hs_group_yoy', 'hs_group_yoy_export')
           AND (detail->'totals'->>'low_base')::boolean = true
           AND superseded_at IS NULL
      ORDER BY abs((detail->'totals'->>'yoy_pct')::numeric) DESC NULLS LAST
        """
    )
    rows = cur.fetchall()

    lines: list[str] = []
    if not rows:
        # Suppress the section entirely when there's nothing to review.
        return _Section(markdown="")

    lines.append("### Low-base review queue")
    lines.append("")
    lines.append(
        f"{len(rows)} findings rest on a denominator below the low-base threshold "
        f"(€50M for either current or prior 12mo total). Verify the absolute figures "
        f"before quoting any percentage from these — small bases can exaggerate."
    )
    lines.append("")
    for r in rows:
        flow = "imports" if r['subkind'] == 'hs_group_yoy' else "exports"
        lines.append(
            f"- **{r['group_name']}** ({flow}, {r['period']}): "
            f"{_fmt_pct(r['yoy_pct'])}, "
            f"prior {_fmt_eur(r['prior_eur'])} → current {_fmt_eur(r['current_eur'])} — "
            f"{_trace_token(r['id'])}"
        )
    lines.append("")
    return _Section(markdown="\n".join(lines))
