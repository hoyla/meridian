"""Render the per-export HS group reference (`groups.md`).

A journalist-facing glossary of every group in the `hs_groups` table:
what each one contains, what HS codes feed it, what its top CN8
contributions look like in the most recent 12-month window, and which
other groups sit next to it in HS-code space.

Why this exists: Luke's 2026-05-13 pack-review note flagged that
journalists reading `findings.md` hit a mix of obviously-coded groups
("Plastics — chapter 39"), implicitly-coded groups ("EV batteries
(Li-ion)" → HS 850760), and apparently-uncoded groups ("Honey"). The
provenance file for any individual finding contains the group
definition for that one group; this document is the upstream cousin —
read it once before clicking into any individual finding to get the
full landscape of what the system tracks and how it carves up the trade
data.

Standalone document — sits alongside `findings.md` / `leads.md` /
`data.xlsx` / `01_Read_Me_First.md` in the per-export folder. Same DB
snapshot as the rest of the bundle, but doesn't depend on any other
document being present.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

import psycopg2
import psycopg2.extras

from briefing_pack._helpers import _slugify_heading

log = logging.getLogger(__name__)


def _conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _slug(name: str) -> str:
    """Use the canonical GitHub-style heading-anchor slugifier so a
    cross-reference from `findings.md` lands on the same anchor."""
    return _slugify_heading(name)


def _fmt_eur(v: Any) -> str:
    if v is None:
        return "—"
    n = float(v)
    if abs(n) >= 1e9:
        return f"€{n/1e9:.2f}B"
    if abs(n) >= 1e6:
        return f"€{n/1e6:.1f}M"
    return f"€{n:,.0f}"


def _fmt_kg(v: Any) -> str:
    if v is None or float(v) == 0:
        return "—"
    n = float(v)
    if abs(n) >= 1e9:
        return f"{n/1e9:.2f} bn kg"
    if abs(n) >= 1e6:
        return f"{n/1e6:.1f}M kg"
    if abs(n) >= 1e3:
        return f"{n/1e3:.0f}k kg"
    return f"{n:,.0f} kg"


def _pattern_to_prefix4(pattern: str) -> str | None:
    """Take an HS LIKE pattern like '850760%' or '85044086%' and return
    the leading 4-digit chapter+heading. Returns None if the pattern is
    shorter than 4 digits or doesn't start with digits — which excludes
    chapter-wide wildcards like '7%' from the sibling computation
    (everything in HS 7 would be a sibling, which is unhelpful)."""
    stripped = pattern.rstrip("%").strip()
    if len(stripped) >= 4 and stripped[:4].isdigit():
        return stripped[:4]
    return None


def _sibling_groups(group: dict, all_groups: list[dict]) -> list[dict]:
    """Find groups whose HS patterns share a 4-digit HS prefix with this
    group's patterns. Same group is excluded. Result is deduped and
    capped — journalists want a pointer, not an exhaustive list."""
    my_prefixes = {
        p for p in (_pattern_to_prefix4(pat) for pat in group["hs_patterns"])
        if p is not None
    }
    if not my_prefixes:
        return []
    siblings: list[dict] = []
    seen_ids: set[int] = {group["id"]}
    for other in all_groups:
        if other["id"] in seen_ids:
            continue
        other_prefixes = {
            p for p in (_pattern_to_prefix4(pat) for pat in other["hs_patterns"])
            if p is not None
        }
        if my_prefixes & other_prefixes:
            siblings.append(other)
            seen_ids.add(other["id"])
        if len(siblings) >= 6:
            break
    return siblings


def _latest_hs_group_yoy(cur, group_id: int) -> dict | None:
    """The most recent active hs_group_yoy* finding for this group.
    Prefers eu_27 scope, flow=1 (imports CN→EU) since that's the canonical
    'China-side of the China shock' framing; falls back to any active
    finding for the group ordered by current_end DESC."""
    cur.execute(
        """
        SELECT id, subkind, detail
          FROM findings
         WHERE superseded_at IS NULL
           AND subkind LIKE 'hs_group_yoy%%'
           AND hs_group_ids @> ARRAY[%s]::bigint[]
         ORDER BY
           CASE WHEN subkind = 'hs_group_yoy' THEN 0 ELSE 1 END,
           (detail->'windows'->>'current_end')::date DESC NULLS LAST,
           id DESC
         LIMIT 1
        """,
        (group_id,),
    )
    return cur.fetchone()


def _is_draft(group: dict) -> bool:
    return (group.get("created_by") or "").startswith("draft:")


def _section_for_group(
    group: dict, all_groups: list[dict], finding: dict | None,
) -> str:
    name = group["name"]
    desc = (group.get("description") or "").strip()
    patterns = group["hs_patterns"] or []
    created_by = group.get("created_by") or "(unknown)"
    created_at = group.get("created_at")
    draft = _is_draft(group)

    out: list[str] = []
    suffix = " *(draft — methodology not yet validated)*" if draft else ""
    out.append(f"### {name}{suffix}")
    out.append("")
    if desc:
        out.append(desc)
        out.append("")
    out.append(
        f"**HS patterns**: "
        f"{', '.join(f'`{p}`' for p in patterns) or '*(none)*'}  "
    )
    out.append(
        f"**Group id**: `{group['id']}`  "
        f"**Added**: {created_at.strftime('%Y-%m-%d') if created_at else '?'} "
        f"(`{created_by}`)"
    )
    out.append("")

    # ---- Top contributing CN8 codes ---------------------------------------
    if finding is not None:
        detail = finding["detail"]
        top = detail.get("top_cn8_codes_in_current_12mo") or []
        totals = detail.get("totals") or {}
        if top:
            anchor_period = (detail.get("windows") or {}).get("current_end")
            group_eur = totals.get("current_12mo_eur") or 0
            out.append(
                f"**Top contributing CN8 codes** "
                f"(rolling 12 months to {anchor_period}, from "
                f"[`finding/{finding['id']}`](provenance/finding-{finding['id']}.md)):"
            )
            out.append("")
            out.append("| CN8 code | 12mo value | 12mo quantity | Share of group |")
            out.append("|---|---:|---:|---:|")
            for r in top[:5]:
                eur = r.get("total_eur") or 0
                kg = r.get("total_kg") or 0
                share = (eur / group_eur) if group_eur else None
                share_cell = f"{share*100:.0f}%" if share is not None else "—"
                cn8 = r.get("hs_code") or r.get("product_nc") or r.get("cn8") or "?"
                out.append(
                    f"| `{cn8}` "
                    f"| {_fmt_eur(eur)} "
                    f"| {_fmt_kg(kg)} "
                    f"| {share_cell} |"
                )
            out.append("")
            # Concentration warning: if a single CN8 is >80% of the group,
            # the group is editorially a wrapper around that one code and
            # any future code-rename/split is a continuity risk.
            if top and group_eur:
                lead_share = (top[0].get("total_eur") or 0) / group_eur
                if lead_share > 0.8:
                    lead_cn8 = (
                        top[0].get("hs_code")
                        or top[0].get("product_nc")
                        or top[0].get("cn8")
                        or "?"
                    )
                    out.append(
                        f"⚠ **Concentration note**: a single CN8 "
                        f"(`{lead_cn8}`) accounts for "
                        f"{lead_share*100:.0f}% of this group's 12-month value. "
                        f"This group is effectively a wrapper around that "
                        f"one code; a future CN8 rename or split would "
                        f"break its trend continuity. See the brief's "
                        f"`cn8_revision` caveat."
                    )
                    out.append("")
    else:
        out.append(
            "*No active `hs_group_yoy*` finding for this group yet — "
            "re-run `python scrape.py --analyse hs-group-yoy` after "
            "ingesting fresh data to populate the contributing-CN8 table.*"
        )
        out.append("")

    # ---- Related groups ---------------------------------------------------
    siblings = _sibling_groups(group, all_groups)
    if siblings:
        out.append("**Related groups** (overlapping HS chapter+heading):")
        out.append("")
        for s in siblings:
            label = f" *(draft)*" if _is_draft(s) else ""
            out.append(f"- [{s['name']}](#{_slug(s['name'])}){label}")
        out.append("")
    return "\n".join(out)


def render_groups(companion_filename: str = "findings.md") -> str:
    """Build the standalone `groups.md` reference for an export bundle.

    Pulls every row from `hs_groups`, fetches the latest active
    `hs_group_yoy*` finding per group, and renders one section per
    group — alphabetised, with a quick index at the top and a separate
    section at the bottom for draft groups (so journalists don't quote
    a draft figure thinking it's been validated)."""

    lines: list[str] = []
    lines.append("# HS group reference")
    lines.append("")
    lines.append(
        f"*Auto-generated "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')} from the "
        f"`hs_groups` table. Read this once before quoting any "
        f"category figure in [`{companion_filename}`]({companion_filename}) "
        f"or `leads.md` — it explains what each named group does and "
        f"does not contain, and points you at sibling groups for "
        f"adjacent material.*"
    )
    lines.append("")
    lines.append(
        "Each group is a named collection of HS-CN8 codes that the "
        "analysers sum together. The named labels are journalist-"
        "configurable: edit a row in the `hs_groups` table (or ask "
        "Luke) to add, rename, refine, or drop a group. Changes take "
        "effect on the next analyser run; existing findings carry the "
        "name they were generated under."
    )
    lines.append("")

    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, name, description, hs_patterns, created_by, created_at
              FROM hs_groups
             ORDER BY lower(name)
            """
        )
        groups = [dict(r) for r in cur.fetchall()]
        # Pre-fetch the latest finding per group (one query per group is
        # fine — 48 groups × cheap PK-ordered LIMIT 1).
        latest_by_group: dict[int, dict] = {}
        for g in groups:
            f = _latest_hs_group_yoy(cur, g["id"])
            if f is not None:
                latest_by_group[g["id"]] = dict(f)

    active = [g for g in groups if not _is_draft(g)]
    drafts = [g for g in groups if _is_draft(g)]

    # ---- Quick index ----------------------------------------------------
    lines.append("## Quick index")
    lines.append("")
    if active:
        lines.append("**Active groups:**")
        lines.append("")
        for g in active:
            lines.append(f"- [{g['name']}](#{_slug(g['name'])})")
        lines.append("")
    if drafts:
        lines.append(
            "**Draft groups** (methodology not yet validated — figures "
            "are surfaced in the brief but should not be quoted without "
            "a verification pass):"
        )
        lines.append("")
        for g in drafts:
            lines.append(f"- [{g['name']}](#{_slug(g['name'])})")
        lines.append("")

    # ---- Active groups ---------------------------------------------------
    if active:
        lines.append("## Active groups")
        lines.append("")
        for g in active:
            lines.append(
                _section_for_group(g, groups, latest_by_group.get(g["id"]))
            )

    # ---- Draft groups ----------------------------------------------------
    if drafts:
        lines.append("## Draft groups")
        lines.append("")
        lines.append(
            "These groups are seeded but their HS code selection has "
            "not yet been editorially validated against a real story. "
            "Figures appear in `findings.md` for transparency but "
            "should not be quoted without verification. Each will "
            "either be promoted (the `draft:` prefix on `created_by` "
            "removed) or dropped once a journalist tests it in anger."
        )
        lines.append("")
        for g in drafts:
            lines.append(
                _section_for_group(g, groups, latest_by_group.get(g["id"]))
            )

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(
        "*Suggesting a new group, a code refinement, or a sibling "
        "split? Pull request or ask Luke. Group definitions live in "
        "the `hs_groups` DB table (see `schema.sql` for the seed "
        "rows). The provenance file for any individual finding (see "
        "the `provenance/` subdir if your export bundle includes "
        "one) reproduces this same group definition alongside the "
        "finding's source URLs, FX rates, and caveats — a more "
        "concentrated audit trail when you're checking one specific "
        "number.*"
    )

    return "\n".join(lines).rstrip() + "\n"
