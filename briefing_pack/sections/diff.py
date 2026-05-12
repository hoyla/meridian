"""Tier 1 — what's new since the previous brief export."""

from __future__ import annotations

from briefing_pack._helpers import _Section, _trace_token


def _section_diff_since_last_brief(cur) -> _Section:
    """Phase 6.8: render 'what changed since the previous brief'.
    Reads brief_runs to find the most recent prior generated_at;
    queries findings created or superseded since then. Returns empty
    markdown if there's no previous brief (first-ever run on a fresh
    DB) or if nothing materially changed.

    Editorial threshold: a YoY shift of > 5pp is "material"; a
    direction flip is highlighted separately. New findings are listed
    by subkind count rather than per-row to keep the section terse —
    the journalist can drill into the per-finding sections below
    once they know what's new."""
    cur.execute("SELECT MAX(generated_at) FROM brief_runs")
    row = cur.fetchone()
    prev_at = row[0] if row else None
    if prev_at is None:
        # First-ever findings export: nothing to compare against. Still emit
        # the tier header so the document structure is consistent across
        # cycles, and tell the reader explicitly that this is the baseline.
        first_export_lines = [
            "---",
            "",
            "## Tier 1 — What's new this cycle",
            "",
            "*This is the **first findings export** generated against this DB "
            "— there is no previous export to diff against. The picture below "
            "in **Tier 2 — Current state of play** is your baseline; "
            "subsequent exports will surface here what changed since this "
            "one.*",
            "",
        ]
        return _Section(markdown="\n".join(first_export_lines))

    # New active findings since previous brief. Excludes narrative_hs_group
    # — LLM lead-scaffold findings live in the companion leads file, not the
    # brief, so the brief's diff should reflect deterministic changes only.
    cur.execute(
        """
        SELECT subkind, COUNT(*) AS n
          FROM findings
         WHERE created_at > %s AND superseded_at IS NULL
           AND subkind <> 'narrative_hs_group'
      GROUP BY subkind ORDER BY subkind
        """,
        (prev_at,),
    )
    new_by_subkind = list(cur.fetchall())

    # Findings superseded since previous brief — pair the old (superseded)
    # row with its new replacement so we can compute the YoY shift.
    # hs_group_* findings store the label in detail.group.name; gacc_aggregate_*
    # findings store it under detail.aggregate.raw_label (the aggregate label
    # like "Africa" / "ASEAN" / "Total"). COALESCE so the diff section
    # renders a real name for either family rather than the literal "None"
    # that older versions of this query produced for aggregate subkinds.
    cur.execute(
        """
        SELECT
            old.id AS old_id, new.id AS new_id, old.subkind,
            COALESCE(
                old.detail->'group'->>'name',
                old.detail->'aggregate'->>'raw_label'
            ) AS group_name,
            old.detail->'windows'->>'current_end' AS window_end,
            (old.detail->'totals'->>'yoy_pct')::numeric AS old_yoy,
            (new.detail->'totals'->>'yoy_pct')::numeric AS new_yoy
          FROM findings old
          JOIN findings new ON old.superseded_by_finding_id = new.id
         WHERE old.superseded_at > %s
           AND old.subkind <> 'narrative_hs_group'
           AND old.detail->'totals'->>'yoy_pct' IS NOT NULL
           AND new.detail->'totals'->>'yoy_pct' IS NOT NULL
        """,
        (prev_at,),
    )
    significant = []
    for r in cur.fetchall():
        old_yoy = float(r["old_yoy"])
        new_yoy = float(r["new_yoy"])
        if abs(new_yoy - old_yoy) > 0.05:  # > 5pp shift = material
            significant.append({
                "subkind": r["subkind"],
                "group_name": r["group_name"],
                "window_end": r["window_end"],
                "old_yoy": old_yoy,
                "new_yoy": new_yoy,
                "direction_flipped": (old_yoy * new_yoy < 0),
                "shift_pp": (new_yoy - old_yoy) * 100,
                "new_finding_id": r["new_id"],
            })

    lines: list[str] = []
    lines.append("---")
    lines.append("")
    lines.append("## Tier 1 — What's new this cycle")
    lines.append("")
    if not new_by_subkind and not significant:
        lines.append(
            f"*Previous findings export generated {prev_at:%Y-%m-%d %H:%M %Z}. "
            f"**Nothing material has changed since then** — no new findings, "
            f"no YoY shifts > 5pp, no direction flips. The Tier 2 summary "
            f"below is still the current picture.*"
        )
        lines.append("")
        return _Section(markdown="\n".join(lines))

    lines.append(
        f"*Previous findings export generated {prev_at:%Y-%m-%d %H:%M %Z}. "
        f"The lists below reflect findings that have been added or whose "
        f"value has materially shifted since then. New findings without a "
        f"comparable predecessor — e.g. a new HS group, a new period anchor — "
        f"appear under \"New findings\".*"
    )
    lines.append("")

    if significant:
        # Direction flips first, then size of shift.
        significant.sort(key=lambda s: (-int(s["direction_flipped"]), -abs(s["shift_pp"])))
        lines.append(f"### Material YoY shifts ({len(significant)})")
        lines.append("")
        lines.append(
            "*A shift > 5 percentage points between the previous export's value "
            "and the current value. Direction flips (growth ↔ decline) are "
            "highlighted with 🔄.*"
        )
        lines.append("")
        for s in significant[:30]:  # cap to top 30 — supersede chain is queryable
            flip = " 🔄 **direction flip**" if s["direction_flipped"] else ""
            lines.append(
                f"- **{s['group_name']}** — `{s['subkind']}`, "
                f"window ending {s['window_end']}: "
                f"{s['old_yoy']*100:+.1f}% → {s['new_yoy']*100:+.1f}% "
                f"({s['shift_pp']:+.1f}pp){flip}. "
                f"Trace: `{_trace_token(s['new_finding_id'])}`"
            )
        if len(significant) > 30:
            lines.append("")
            lines.append(
                f"*…and {len(significant) - 30} more material shifts; "
                f"query the supersede chain for the full set.*"
            )
        lines.append("")

    if new_by_subkind:
        total_new = sum(n for _, n in new_by_subkind)
        lines.append(f"### New findings ({total_new})")
        lines.append("")
        for subkind, n in new_by_subkind:
            lines.append(f"- {n} new `{subkind}`")
        lines.append("")

    return _Section(markdown="\n".join(lines))
