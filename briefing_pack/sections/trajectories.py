"""Trajectory findings grouped by shape — narrative-rich pattern bucket."""

from __future__ import annotations

from datetime import timedelta

import psycopg2.extras

from briefing_pack._helpers import (
    _Section,
    _SCOPE_LABEL,
    _SCOPE_SUBKIND_SUFFIX,
    _fmt_pct,
    _release_ids_for_window,
    _trace_token,
)


def _section_trajectories(cur, comparison_scope: str = "eu_27") -> _Section:
    """Trajectory findings grouped by shape — narrative-rich pattern bucket.
    Phase 6.1e: scoped to one of EU-27 / UK / combined."""
    scope_suffix = _SCOPE_SUBKIND_SUFFIX[comparison_scope]
    scope_label = _SCOPE_LABEL[comparison_scope]
    subkind_imp = f"hs_group_trajectory{scope_suffix}"
    subkind_exp = f"hs_group_trajectory{scope_suffix}_export"
    cur.execute(
        """
        SELECT id, subkind,
               detail->'group'->>'name' AS group_name,
               detail->>'shape' AS shape,
               detail->>'shape_label' AS shape_label,
               (detail->'features'->>'last_yoy')::numeric AS last_yoy,
               (detail->'features'->>'max_yoy')::numeric AS peak,
               (detail->'features'->>'min_yoy')::numeric AS trough,
               (detail->'features'->>'first_period')::date AS first_period,
               (detail->'features'->>'last_period')::date AS last_period,
               (detail->'features'->>'low_base_majority')::boolean AS low_base_majority
          FROM findings
         WHERE subkind IN (%s, %s)
           AND superseded_at IS NULL
      ORDER BY detail->>'shape', subkind, detail->'group'->>'name'
        """,
        (subkind_imp, subkind_exp),
    )
    rows = cur.fetchall()

    release_ids: set[int] = set()
    lines: list[str] = []
    if not rows:
        return _Section(markdown="")
    lines.append(f"### {scope_label} trajectory shapes")
    lines.append("")
    lines.append(
        "Each HS group's rolling-12mo YoY series classified by shape. "
        "Editorially the shape vocabulary matters: `dip_recovery` and `inverse_u_peak` "
        "are narrative-rich (a comeback or a peak-and-fall); `falling`/`rising` are "
        "directional; `volatile` flags series the classifier didn't fit confidently."
    )
    lines.append("")
    if not rows:
        lines.append("*No trajectory findings yet.*")
        lines.append("")
        return _Section(markdown="\n".join(lines))

    by_shape: dict[str, list[psycopg2.extras.DictRow]] = {}
    for r in rows:
        by_shape.setdefault(r['shape'], []).append(r)

    # Order shapes editorially: narrative-rich first, then directional, then volatile/flat.
    shape_order = [
        "dip_recovery", "failed_recovery", "inverse_u_peak", "u_recovery",
        "rising_accelerating", "rising_decelerating", "rising",
        "falling_decelerating", "falling_accelerating", "falling",
        "volatile", "flat",
    ]
    seen_shapes = set()
    for shape in shape_order + sorted(by_shape.keys()):
        if shape in seen_shapes or shape not in by_shape:
            continue
        seen_shapes.add(shape)
        shape_label = by_shape[shape][0]['shape_label'] or shape
        lines.append(f"#### {shape} — *{shape_label}*")
        for r in by_shape[shape]:
            flow = "imports" if r['subkind'] == 'hs_group_trajectory' else "exports"
            low_base_marker = " ⚠️ low-base" if r['low_base_majority'] else ""
            lines.append(
                f"- **{r['group_name']}** ({flow}): "
                f"latest YoY {_fmt_pct(r['last_yoy'])}, "
                f"peak {_fmt_pct(r['peak'])}, trough {_fmt_pct(r['trough'])}"
                f"{low_base_marker} — {_trace_token(r['id'])}"
            )
            # Window: features.first_period (first 12mo-window end) — 12mo back covers
            # the earliest observation period that fed this trajectory.
            if r['first_period'] and r['last_period']:
                window_start = (r['first_period'].replace(day=1) - timedelta(days=1)).replace(day=1)
                # Step back 11 more months to cover the full 12mo prior window for the first point.
                ws = r['first_period']
                for _ in range(12):
                    ws = (ws.replace(day=1) - timedelta(days=1)).replace(day=1)
                ids = _release_ids_for_window(cur, ws, r['last_period'])
                release_ids |= ids
        lines.append("")

    return _Section(markdown="\n".join(lines), release_ids=release_ids)
