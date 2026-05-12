"""Tier 1 — what's new since the previous brief export."""

from __future__ import annotations

from briefing_pack._helpers import _Section, _trace_token


# A method-version bump (e.g. v10 → v11) causes every active hs_group_yoy*
# finding to supersede its predecessor — but the editorially-relevant
# numbers usually don't change (only the JSON shape evolves). When the
# vast majority of supersede pairs are "value-identical" (YoY moved by
# less than this threshold), Tier 1 should suppress the noise and tell
# the reader explicitly that what they're seeing is plumbing, not news.
_METHOD_BUMP_YOY_TOLERANCE = 1e-4   # 0.01 pp; same scale as `value_signature` rounding
_METHOD_BUMP_RATIO_THRESHOLD = 0.95  # >=95% of supersede pairs unchanged → call it a bump


def _section_diff_since_last_brief(cur) -> _Section:
    """Phase 6.8 (+ 2026-05-12 method-bump detection): render 'what
    changed since the previous brief'.

    Three regimes the section handles distinctly:

    - **No previous brief**: emit the tier header with a baseline note.
    - **Method-version bump churn dominates**: the previous run was
      "yesterday" and a method-version bump (e.g. Phase 6.11 v10 → v11
      per-reporter breakdown) caused thousands of supersedes whose
      headline YoYs are unchanged. Without this branch, Tier 1 reads as
      "23,207 new findings" — pure noise. Detect via the ratio of
      value-identical supersedes, suppress the full list, surface only
      the affected method versions and any genuinely-new analyser kinds.
    - **Real editorial movement**: behaviour unchanged — list material
      YoY shifts (>5pp) and new findings by subkind.

    Editorial threshold: a YoY shift > 5pp is "material"; a direction
    flip is highlighted separately. New findings are listed by subkind
    count rather than per-row to keep the section terse — the journalist
    can drill into the per-finding sections below once they know what's
    new."""
    cur.execute("SELECT MAX(generated_at) FROM brief_runs")
    row = cur.fetchone()
    prev_at = row[0] if row else None
    if prev_at is None:
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

    # Supersede pairs since last brief. Includes the YoYs and the new
    # method tag — the method tag distinguishes a routine data revision
    # from a methodology version bump.
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
            (new.detail->'totals'->>'yoy_pct')::numeric AS new_yoy,
            old.detail->>'method' AS old_method,
            new.detail->>'method' AS new_method
          FROM findings old
          JOIN findings new ON old.superseded_by_finding_id = new.id
         WHERE old.superseded_at > %s
           AND old.subkind <> 'narrative_hs_group'
           AND old.detail->'totals'->>'yoy_pct' IS NOT NULL
           AND new.detail->'totals'->>'yoy_pct' IS NOT NULL
        """,
        (prev_at,),
    )
    pairs = cur.fetchall()

    significant: list[dict] = []
    n_value_identical = 0
    method_transitions: set[tuple[str, str]] = set()
    for r in pairs:
        old_yoy = float(r["old_yoy"])
        new_yoy = float(r["new_yoy"])
        if abs(new_yoy - old_yoy) < _METHOD_BUMP_YOY_TOLERANCE:
            n_value_identical += 1
        if r["old_method"] != r["new_method"]:
            method_transitions.add((r["old_method"], r["new_method"]))
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

    total_new = sum(n for _, n in new_by_subkind)
    n_pairs = len(pairs)

    lines: list[str] = []
    lines.append("---")
    lines.append("")
    lines.append("## Tier 1 — What's new this cycle")
    lines.append("")

    # Method-bump detection: most supersede pairs are value-identical AND
    # no material shifts surfaced. The reader is staring at plumbing —
    # tell them that explicitly.
    is_method_bump_churn = (
        n_pairs > 0
        and not significant
        and (n_value_identical / n_pairs) >= _METHOD_BUMP_RATIO_THRESHOLD
        and bool(method_transitions)
    )

    if is_method_bump_churn:
        lines.append(
            f"*Previous findings export generated {prev_at:%Y-%m-%d %H:%M %Z}. "
            f"**This cycle is a method-version bump, not editorial movement.** "
            f"{n_pairs:,} findings superseded their predecessors with no "
            f"material YoY change (all shifts under "
            f"{_METHOD_BUMP_YOY_TOLERANCE*100:.2f}pp). The Tier 2 summary "
            f"below remains the current editorial picture.*"
        )
        lines.append("")
        # Surface which methods changed so the supersede chain stays auditable.
        if method_transitions:
            lines.append("### Method versions changed")
            lines.append("")
            for old_m, new_m in sorted(method_transitions):
                lines.append(f"- `{old_m}` → `{new_m}`")
            lines.append("")
        return _Section(markdown="\n".join(lines))

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
        significant.sort(key=lambda s: (-int(s["direction_flipped"]), -abs(s["shift_pp"])))
        lines.append(f"### Material YoY shifts ({len(significant)})")
        lines.append("")
        lines.append(
            "*A shift > 5 percentage points between the previous export's value "
            "and the current value. Direction flips (growth ↔ decline) are "
            "highlighted with 🔄.*"
        )
        lines.append("")
        for s in significant[:30]:
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
        lines.append(f"### New findings ({total_new})")
        lines.append("")
        for subkind, n in new_by_subkind:
            lines.append(f"- {n} new `{subkind}`")
        lines.append("")

    return _Section(markdown="\n".join(lines))
