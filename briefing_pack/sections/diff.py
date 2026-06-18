"""Tier 1 — what's new since the previous brief export.

Split compute/render: `_compute_diff(cur)` builds a `_DiffData` snapshot
of everything that changed since the previous export (regime detection,
material shifts, new-finding counts), and `_section_diff_since_last_brief`
renders Tier 1 from it. The front page's "Since the last pack" digest
consumes the same `_DiffData`, so the two surfaces can never disagree
about what this cycle is.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from briefing_pack._helpers import (
    _Section,
    _fmt_month,
    _format_new_releases_phrase,
    _new_releases_since,
    _prev_export_folder,
    _prev_export_ref,
    _source_data_sentence,
    _subkind_flow_scope_phrase,
    _subkind_plain_label,
    _trace_token,
)


# A method-version bump (e.g. v10 → v11) causes every active hs_group_yoy*
# finding to supersede its predecessor — but the editorially-relevant
# numbers usually don't change (only the JSON shape evolves). When the
# vast majority of supersede pairs are "value-identical" (YoY moved by
# less than this threshold), Tier 1 should suppress the noise and tell
# the reader explicitly that what they're seeing is plumbing, not news.
_METHOD_BUMP_YOY_TOLERANCE = 1e-4   # 0.01 pp; same scale as `value_signature` rounding
_METHOD_BUMP_RATIO_THRESHOLD = 0.95  # >=95% of supersede pairs unchanged → call it a bump


@dataclass
class _DiffData:
    """What changed since the previous export, regime-classified.

    regime: "first_export" (no previous brief), "method_bump"
    (supersede churn with value-identical numbers), "no_change"
    (previous brief exists, nothing moved), or "movement" (material
    shifts and/or new findings). `significant` is pre-sorted: direction
    flips first, then by |shift| descending."""
    regime: str
    prev_ref: str = ""
    source_sentence: str = ""
    significant: list[dict] = field(default_factory=list)
    new_by_subkind: list = field(default_factory=list)
    total_new: int = 0
    n_pairs: int = 0
    n_value_identical: int = 0
    method_transitions: set = field(default_factory=set)


def _compute_diff(cur, baseline_brief_run_id: int | None = None) -> _DiffData:
    """Phase 6.8 (+ 2026-05-12 method-bump detection): compute 'what
    changed since the previous brief'.

    Editorial threshold: a YoY shift > 5pp is "material"; a direction
    flip is highlighted separately. New findings are counted by subkind
    rather than per-row — the journalist drills into the per-finding
    sections once they know what's new.

    `baseline_brief_run_id`: when set, diff against that specific
    `brief_runs` row rather than the most recent one. Used for a corrected
    *re-issue* — regenerating a withdrawn pack so its Tier 1 reads against
    the pack *before* the withdrawn one (e.g. re-issuing the 16 Jun pack
    against the 21 May baseline), without deleting the withdrawn pack's
    audit row from `brief_runs`."""
    if baseline_brief_run_id is not None:
        cur.execute(
            "SELECT generated_at, output_path FROM brief_runs WHERE id = %s",
            (baseline_brief_run_id,),
        )
    else:
        cur.execute(
            "SELECT generated_at, output_path FROM brief_runs "
            "ORDER BY generated_at DESC LIMIT 1"
        )
    row = cur.fetchone()
    prev_at = row[0] if row else None
    prev_output_path = row[1] if row else None
    if prev_at is None:
        return _DiffData(regime="first_export")

    # New active findings since previous brief. Excludes narrative_hs_group
    # — LLM lead-scaffold findings live in the companion leads file, not the
    # brief, so the brief's diff should reflect deterministic changes only.
    cur.execute(
        """
        SELECT subkind, COUNT(*) AS n
          FROM findings f
         WHERE created_at > %s AND superseded_at IS NULL
           AND subkind <> 'narrative_hs_group'
           -- Genuinely new = the natural key first appeared after the
           -- baseline. A finding that supersedes an ancestor predating the
           -- baseline is a *revision*, not a new finding — it belongs to the
           -- "changed" path (where it has a predecessor to diff against), not
           -- the "new" list. Without this, a methodology re-run that
           -- supersedes many existing findings at once (e.g. the 2026-06-17
           -- mirror_gap double-count fix) floods Tier 1 with phantom "new"
           -- findings that are really corrections of existing ones.
           AND NOT EXISTS (
               SELECT 1 FROM findings p
                WHERE p.natural_key_hash = f.natural_key_hash
                  AND p.created_at <= %s
           )
      GROUP BY subkind ORDER BY subkind
        """,
        (prev_at, prev_at),
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
                old.detail->'aggregate'->>'raw_label',
                old.detail->'partner'->>'raw_label'
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
    significant.sort(
        key=lambda s: (-int(s["direction_flipped"]), -abs(s["shift_pp"])),
    )

    n_pairs = len(pairs)
    is_method_bump_churn = (
        n_pairs > 0
        and not significant
        and (n_value_identical / n_pairs) >= _METHOD_BUMP_RATIO_THRESHOLD
        and bool(method_transitions)
    )
    if is_method_bump_churn:
        regime = "method_bump"
    elif not new_by_subkind and not significant:
        regime = "no_change"
    else:
        regime = "movement"

    # Why-this-export framing: the source releases (if any) that arrived
    # since the previous brief, and the folder name of that previous
    # export — woven into each branch's lead-in so the reader sees the
    # editorial *trigger* alongside the *delta*.
    new_releases = _new_releases_since(cur, prev_at)
    new_releases_phrase = _format_new_releases_phrase(new_releases)
    prev_folder = _prev_export_folder(prev_output_path)

    return _DiffData(
        regime=regime,
        prev_ref=_prev_export_ref(prev_at, prev_folder),
        source_sentence=_source_data_sentence(new_releases_phrase),
        significant=significant,
        new_by_subkind=new_by_subkind,
        total_new=sum(n for _, n in new_by_subkind),
        n_pairs=n_pairs,
        n_value_identical=n_value_identical,
        method_transitions=method_transitions,
    )


def _shift_flow_phrase(subkind: str) -> str:
    """The 'which flow is this' clause for a shift line — plain scope/
    flow phrase where one exists, the family label otherwise."""
    return _subkind_flow_scope_phrase(subkind) or _subkind_plain_label(subkind)


def _fmt_window_end(window_end) -> str:
    """detail.windows.current_end arrives as an ISO string; render it
    as 'Mar 2026'. Falls back to the raw value on anything unparseable
    (older rows, exotic subkinds)."""
    try:
        return _fmt_month(date.fromisoformat(str(window_end)))
    except (TypeError, ValueError):
        return str(window_end)


def _section_diff_since_last_brief(diff: _DiffData) -> _Section:
    """Render Tier 1 from a computed `_DiffData`.

    Three non-baseline regimes render distinctly: method-bump churn is
    suppressed down to the affected method versions; no-change states
    that explicitly; movement lists material shifts (>5pp, flips first)
    and new-finding counts."""
    lines: list[str] = []
    lines.append("---")
    lines.append("")
    lines.append("## Tier 1 — What's new this cycle")
    lines.append("")

    if diff.regime == "first_export":
        lines.append(
            "*This is the **first findings export** generated against this DB "
            "— there is no previous export to diff against. The picture below "
            "in **Tier 2 — Current state of play** is your baseline; "
            "subsequent exports will surface here what changed since this "
            "one.*"
        )
        lines.append("")
        return _Section(markdown="\n".join(lines))

    if diff.regime == "method_bump":
        lines.append(
            f"*{diff.prev_ref}. {diff.source_sentence} "
            f"**This cycle is a method-version bump, not editorial movement.** "
            f"{diff.n_pairs:,} findings superseded their predecessors with no "
            f"material YoY change (all shifts under "
            f"{_METHOD_BUMP_YOY_TOLERANCE*100:.2f}pp). The Tier 2 summary "
            f"below remains the current editorial picture.*"
        )
        lines.append("")
        # Surface which methods changed so the supersede chain stays auditable.
        if diff.method_transitions:
            lines.append("### Method versions changed")
            lines.append("")
            for old_m, new_m in sorted(diff.method_transitions):
                lines.append(f"- `{old_m}` → `{new_m}`")
            lines.append("")
        return _Section(markdown="\n".join(lines))

    if diff.regime == "no_change":
        lines.append(
            f"*{diff.prev_ref}. {diff.source_sentence} "
            f"**Nothing material has changed since then** — no new findings, "
            f"no YoY shifts > 5pp, no direction flips. The Tier 2 summary "
            f"below is still the current picture.*"
        )
        lines.append("")
        return _Section(markdown="\n".join(lines))

    lines.append(
        f"*{diff.prev_ref}. {diff.source_sentence} "
        f"The lists below reflect findings that have been added or whose "
        f"value has materially shifted since then. New findings without a "
        f"comparable predecessor — e.g. a new HS group, a new period anchor — "
        f"appear under \"New findings\".*"
    )
    lines.append("")

    if diff.significant:
        lines.append(f"### Material YoY shifts ({len(diff.significant)})")
        lines.append("")
        lines.append(
            "*A shift > 5 percentage points between the previous export's value "
            "and the current value. Direction flips (growth ↔ decline) are "
            "highlighted with 🔄.*"
        )
        lines.append("")
        for s in diff.significant[:30]:
            flip = " 🔄 **direction flip**" if s["direction_flipped"] else ""
            # _trace_token already formats the token (backticks, or a
            # link when GACC_PERMALINK_BASE is set).
            lines.append(
                f"- **{s['group_name']}** — {_shift_flow_phrase(s['subkind'])} "
                f"(`{s['subkind']}`), 12 months to "
                f"{_fmt_window_end(s['window_end'])}: "
                f"{s['old_yoy']*100:+.1f}% → {s['new_yoy']*100:+.1f}% "
                f"({s['shift_pp']:+.1f}pp){flip}. "
                f"Trace: {_trace_token(s['new_finding_id'])}"
            )
        if len(diff.significant) > 30:
            lines.append("")
            lines.append(
                f"*…and {len(diff.significant) - 30} more material shifts; "
                f"query the supersede chain for the full set.*"
            )
        lines.append("")

    if diff.new_by_subkind:
        lines.append(f"### New findings ({diff.total_new})")
        lines.append("")
        for subkind, n in diff.new_by_subkind:
            lines.append(
                f"- {n} new — {_subkind_plain_label(subkind)} (`{subkind}`)"
            )
        lines.append("")

    return _Section(markdown="\n".join(lines))
