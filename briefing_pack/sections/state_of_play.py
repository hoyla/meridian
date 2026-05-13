"""Tier 2 — compact one-block-per-HS-group state-of-play summary."""

from __future__ import annotations

from typing import Any

from briefing_pack._helpers import _Section, _fmt_eur, _trace_token


def _section_state_of_play(
    cur, predictability: dict[str, tuple[str, float, int]] | None = None,
) -> _Section:
    """Tier 2 — compact one-block-per-HS-group summary of where every
    active finding stands today. Intentionally short: imports headline,
    exports headline, trajectory shape (if any), per group. The detail
    sections below in Tier 3 are the source for the same numbers; this
    section is for orientation, not citation."""
    predictability = predictability or {}

    # Latest hs_group_yoy* finding per (group, subkind). DISTINCT ON pulls
    # the newest current_end per (group, subkind) pair — same pattern as
    # _section_hs_yoy_movers. Also pulls the Phase 6.10 single-month YoY
    # from the new detail.totals.single_month block; that's null for any
    # finding still on method v9 or earlier (older row), in which case the
    # render simply omits the single-month figure rather than failing.
    cur.execute(
        """
        WITH latest AS (
          SELECT DISTINCT ON (detail->'group'->>'name', subkind)
                 id,
                 detail->'group'->>'name' AS group_name,
                 subkind,
                 (detail->'windows'->>'current_end')::date AS current_end,
                 (detail->'totals'->>'yoy_pct')::numeric AS yoy_pct,
                 (detail->'totals'->>'yoy_pct_kg')::numeric AS yoy_pct_kg,
                 (detail->'totals'->>'current_12mo_eur')::numeric AS cur_eur,
                 (detail->'totals'->>'low_base')::boolean AS low_base,
                 (detail->'totals'->>'partial_window')::boolean AS partial_window,
                 (detail->'totals'->'single_month'->>'yoy_pct')::numeric AS sm_yoy_pct,
                 (detail->'totals'->'single_month'->>'yoy_pct_kg')::numeric AS sm_yoy_pct_kg
            FROM findings
           WHERE subkind LIKE 'hs_group_yoy%%' AND superseded_at IS NULL
        ORDER BY detail->'group'->>'name', subkind,
                 (detail->'windows'->>'current_end')::date DESC, id DESC
        )
        SELECT * FROM latest ORDER BY group_name, subkind
        """
    )
    yoy_rows = list(cur.fetchall())

    # Latest trajectory per (group, subkind).
    cur.execute(
        """
        SELECT DISTINCT ON (detail->'group'->>'name', subkind)
               detail->'group'->>'name' AS group_name,
               subkind,
               detail->>'shape' AS shape,
               detail->>'shape_label' AS shape_label
          FROM findings
         WHERE subkind LIKE 'hs_group_trajectory%%' AND superseded_at IS NULL
      ORDER BY detail->'group'->>'name', subkind, id DESC
        """
    )
    traj_rows = list(cur.fetchall())
    # Index trajectories: traj_by_group[group][subkind] -> (shape, label)
    # Carrying both lets us filter on the underlying shape (e.g. drop
    # `volatile` inline) without losing the human-readable label for the
    # cases we do render. Methodology.md §10 explains the editorial
    # rationale for suppressing volatile inline — at HS-group granularity
    # the classifier fires `volatile` on ~68% of series, which carries no
    # narrative information; absence here signals "no useful shape to
    # lean on, rely on the headline %."
    traj_by_group: dict[str, dict[str, tuple[str, str]]] = {}
    for r in traj_rows:
        shape = r["shape"] or ""
        label = r["shape_label"] or shape or "—"
        traj_by_group.setdefault(r["group_name"], {})[r["subkind"]] = (shape, label)

    # Index yoy: yoy_by_group[group][subkind] -> row
    yoy_by_group: dict[str, dict[str, Any]] = {}
    for r in yoy_rows:
        yoy_by_group.setdefault(r["group_name"], {})[r["subkind"]] = r

    def _fmt_yoy_line(label: str, row: Any, traj_label: str | None) -> str:
        """One-line per (scope, flow) summary inside a group block."""
        yoy_v = float(row["yoy_pct"]) * 100 if row["yoy_pct"] is not None else None
        yoy_k = float(row["yoy_pct_kg"]) * 100 if row["yoy_pct_kg"] is not None else None
        cur_eur = row["cur_eur"]
        flags: list[str] = []
        if row["low_base"]:
            flags.append("⚠ low base")
        if row["partial_window"]:
            flags.append("partial window")
        flags_str = (" — " + ", ".join(flags)) if flags else ""
        yoy_v_str = f"{yoy_v:+.1f}%" if yoy_v is not None else "n/a"
        yoy_k_str = f" (kg {yoy_k:+.1f}%)" if yoy_k is not None else ""
        eur_str = _fmt_eur(cur_eur)
        traj_str = f" Trajectory: `{traj_label}`." if traj_label else ""
        # Phase 6.10: surface the single-month YoY (latest period vs same
        # month a year earlier) next to the 12mo rolling figure when the
        # finding carries it. The Soapbox / Lisa register often quotes the
        # single-month figure directly; the journalist seeing both at a
        # glance can pick the cadence that matches the story they're
        # writing.
        sm_yoy_pct = row["sm_yoy_pct"]
        sm_str = ""
        if sm_yoy_pct is not None:
            sm_v = float(sm_yoy_pct) * 100
            sm_k_raw = row["sm_yoy_pct_kg"]
            sm_k_str = f" (kg {float(sm_k_raw)*100:+.1f}%)" if sm_k_raw is not None else ""
            sm_str = f" Latest month: {sm_v:+.1f}%{sm_k_str}."
        return (
            f"  - **{label}**: {yoy_v_str}{yoy_k_str} to {eur_str} (12mo to "
            f"{row['current_end']}).{sm_str}{traj_str}{flags_str} "
            f"{_trace_token(row['id'])}"
        )

    # Render order: each group section, then within it group lines by
    # (flow, scope). Imports first (the more common editorial direction),
    # then exports.
    SCOPE_ORDER = [
        # (subkind_yoy, subkind_trajectory, display_label, flow_direction)
        ("hs_group_yoy", "hs_group_trajectory", "EU-27 imports (CN→reporter)", 1),
        ("hs_group_yoy_uk", "hs_group_trajectory_uk", "UK imports (CN→reporter)", 1),
        ("hs_group_yoy_combined", "hs_group_trajectory_combined", "EU-27+UK imports (combined)", 1),
        ("hs_group_yoy_export", "hs_group_trajectory_export", "EU-27 exports (reporter→CN)", 2),
        ("hs_group_yoy_uk_export", "hs_group_trajectory_uk_export", "UK exports (reporter→CN)", 2),
        ("hs_group_yoy_combined_export", "hs_group_trajectory_combined_export", "EU-27+UK exports (combined)", 2),
    ]

    lines: list[str] = []
    lines.append("---")
    lines.append("")
    lines.append("## Tier 2 — Current state of play")
    lines.append("")
    lines.append(
        "One block per HS group. Each row inside is a (scope, flow) "
        "compact summary: latest 12mo YoY (value, and kg in parens), current "
        "12mo total in EUR, trajectory shape if classified, and the "
        "`finding/N` token you can use to find the same row in the spreadsheet "
        "or the detail tier below. Predictability badges (🟢/🟡/🔴) sit next "
        "to the group name where the historical pair exists."
    )
    lines.append("")
    lines.append(
        "*This section is the picture between cycles. Items that have moved "
        "materially since the previous findings export appear in Tier 1 "
        "above; this tier shows where every active finding stands right now.*"
    )
    lines.append("")

    if not yoy_by_group:
        lines.append("*No active hs_group YoY findings to render.*")
        lines.append("")
        return _Section(markdown="\n".join(lines))

    for group_name in sorted(yoy_by_group.keys()):
        by_subkind = yoy_by_group[group_name]
        traj = traj_by_group.get(group_name, {})

        pred = predictability.get(group_name)
        badge_str = f" {pred[0]}" if pred is not None else ""
        lines.append(f"### {group_name}{badge_str}")
        lines.append("")

        any_emitted = False
        for sk_yoy, sk_traj, label, _flow in SCOPE_ORDER:
            r = by_subkind.get(sk_yoy)
            if not r:
                continue
            # Resolve the trajectory label, but only render it when the
            # underlying shape is editorially informative. `volatile`
            # fires on the majority of HS-group series at this
            # granularity (Phase 6.6 backtest established the underlying
            # noise rate); rendering it inline trains the reader to skim
            # past the line. Drop it instead — absence signals "no
            # narrative shape; lean on the headline %." Methodology §10.
            traj_entry = traj.get(sk_traj)
            if traj_entry is not None and traj_entry[0] != "volatile":
                traj_label = traj_entry[1]
            else:
                traj_label = None
            lines.append(_fmt_yoy_line(label, r, traj_label))
            any_emitted = True
        if not any_emitted:
            lines.append("  - *(no active findings)*")
        lines.append("")

    return _Section(markdown="\n".join(lines))
