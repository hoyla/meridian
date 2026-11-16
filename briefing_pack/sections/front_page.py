"""Front page — "If you read only this page".

The deterministic one-page brief at the top of findings.md, replacing
the old "Top N movers this cycle" data lines with publishable, hedged
sentences. Two parts:

1. The cycle's most quotable shifts, one sentence each, written ready
   to lift into copy — verb by direction, value and volume both named,
   the stability hedge baked into the sentence, the citation token at
   the end. Candidates come from `_compute_top_movers`, which already
   applies the quotability filters (≥10pp move, ≥€100M 12-month total,
   not low-base, no 🔴 stability badge).
2. A "Since the last pack" digest derived from the same `_DiffData`
   that drives Tier 1, so the front page and the diff section can never
   disagree about what kind of cycle this is.

No LLM in the loop: every sentence is template-rendered from the
deterministic finding fields, same trust class as the rest of the
findings document.
"""

from __future__ import annotations

from briefing_pack._helpers import (
    MOVER_PAIR_SIZE_GAP,
    _Section,
    _fmt_eur,
    _fmt_month,
    _slugify_heading,
    _trace_token,
)
from briefing_pack.sections.diff import _DiffData, _fmt_window_end, _shift_flow_phrase

FRONT_PAGE_HEADING = "If you read only this page"


def _mover_sentence(m: dict, disp: dict[str, str] | None = None) -> str:
    """One publishable sentence for a top mover. The hedge is graded by
    the stability badge; 🔴 never reaches here (filtered upstream).

    `disp` maps the stable internal group key → reader-facing display name
    (db.group_display_names). The cross-reference link text and its slug both
    use the display name so they match the target heading's displayed text and
    anchor. Defaults to identity (no rename) so pure-render unit tests can call
    this without a DB."""
    disp = disp or {}
    group = disp.get(m["group_name"], m["group_name"])
    is_export = m["subkind"].endswith("_export")
    # The cross-reference link wraps just the group name — not the whole
    # subject — so its visible text equals the target heading's group name.
    # That is what lets the Drive uploader reconnect it after import: it
    # matches a dangling internal link to a heading by text/slug, because
    # Google's .docx importer drops the `#slug` itself. The leads digest
    # links resolve for exactly this reason; the front page used to wrap the
    # whole subject and silently failed to. See briefing_pack.drive_export.
    linked_group = f"[{group}](#{_slugify_heading(group)})"
    subject = (
        f"EU-27 exports of {linked_group} to China" if is_export
        else f"EU-27 imports of {linked_group} from China"
    )
    yoy = float(m["yoy_pct"])
    verb = "rose" if yoy > 0 else "fell"
    pct = f"{abs(yoy) * 100:.1f}%"

    vol = ""
    yoy_kg = m.get("yoy_pct_kg")
    if yoy_kg is not None:
        kgf = float(yoy_kg)
        vol = f"; volume {'up' if kgf > 0 else 'down'} {abs(kgf) * 100:.1f}%"

    pred = m.get("predictability")
    badge = pred[0] if pred is not None else None
    if badge == "🟢":
        tail = " — a trend that has held over the past six months"
    elif badge == "🟡":
        tail = (
            " — though the signal has been mixed over the past six "
            "months, so double-check before headlining"
        )
    else:
        tail = (
            " — trend stability not yet scored, so verify before "
            "headlining"
        )

    cp = m.get("counterpart")
    if cp is None:
        return (
            f"**{subject}** {verb} {pct} by value in the "
            f"12 months to {_fmt_month(m['current_end'])}, to "
            f"{_fmt_eur(m['current_eur'])}{vol}{tail}. {_trace_token(m['id'])}"
        )

    # Both directions of this commodity qualified: one item, three sentences
    # (lead flow, the other direction, the net position), then the stability
    # hedge as its own sentence so it reads as covering the whole item.
    if badge == "🟢":
        hedge = "The trend has held over the past six months."
    elif badge == "🟡":
        hedge = ("The signal has been mixed over the past six months, so "
                 "double-check before headlining.")
    else:
        hedge = "Trend stability not yet scored, so verify before headlining."
    parts = [
        f"**{subject}** {verb} {pct} by value in the "
        f"12 months to {_fmt_month(m['current_end'])}, to "
        f"{_fmt_eur(m['current_eur'])}{vol}.",
        _counterpart_sentence(m, cp),
    ]
    net = _net_sentence(m.get("net"))
    if net:
        parts.append(net)
    parts.append(hedge)
    return (" ".join(parts)
            + f" {_trace_token(m['id'])} {_trace_token(cp['id'])}")


def _counterpart_sentence(lead: dict, cp: dict) -> str:
    """The other direction of a paired mover. States the size gap in words
    when one flow dwarfs the other, so "up X, down Y" is never read as two
    comparable halves (Sep 2026 batteries: €33B in, €354M out)."""
    cp_export = cp["subkind"].endswith("_export")
    yoy = float(cp["yoy_pct"])
    verb = "rose" if yoy > 0 else "fell"
    if (yoy > 0) == (float(lead["yoy_pct"]) > 0):
        verb = f"also {verb}"
    pct = f"{abs(yoy) * 100:.1f}%"
    vol = ""
    if cp.get("yoy_pct_kg") is not None:
        kgf = float(cp["yoy_pct_kg"])
        vol = f"; volume {'up' if kgf > 0 else 'down'} {abs(kgf) * 100:.1f}%"
    flow = "EU-27 exports to China" if cp_export else "EU-27 imports from China"
    eur = _fmt_eur(cp["current_eur"])
    lead_eur = float(lead["current_eur"]) or 1.0
    if float(cp["current_eur"]) / lead_eur < MOVER_PAIR_SIZE_GAP:
        return (f"In the other direction, the much smaller flow of {flow} "
                f"({eur}) {verb} {pct}{vol}.")
    return f"In the other direction, {flow} {verb} {pct}, to {eur}{vol}."


def _net_sentence(net: dict | None) -> str:
    """The commodity's net position in words — see _helpers._net_position."""
    if not net:
        return ""
    cur, prior, chg = (_fmt_eur(net["current_eur"]), _fmt_eur(net["prior_eur"]),
                       _fmt_eur(net["change_eur"]))
    if net["kind"] in ("deficit", "surplus"):
        return (f"Net, the EU-27’s {net['kind']} with China in this category "
                f"{net['direction']} by {chg}, to {cur}.")
    was, now = (("surplus", "deficit") if net["kind"] == "swing_to_deficit"
                else ("deficit", "surplus"))
    return (f"Net, the EU-27’s position with China in this category swung "
            f"from a {prior} {was} to a {cur} {now}.")


def _since_last_pack_lines(
    diff: _DiffData, disp: dict[str, str] | None = None,
) -> list[str]:
    """The cycle-digest paragraph — one regime, one plain statement.

    `disp` (db.group_display_names) maps the stable internal group key →
    reader-facing display name; the highlighted group names use it. Defaults to
    identity so pure-render unit tests can call this without a DB."""
    disp = disp or {}
    if diff.regime == "first_export":
        return [
            "**Since the last pack:** this is the first export from this "
            "database — everything below is baseline, not change.",
        ]
    if diff.regime == "method_bump":
        return [
            f"**Since the last pack:** a methodology version bump "
            f"re-stamped {diff.n_pairs:,} findings without changing their "
            "numbers — nothing editorial moved. Details in Tier 1.",
        ]
    if diff.regime == "no_change":
        return [
            "**Since the last pack:** nothing material — no new findings, "
            "no shifts over 5 percentage points, no direction flips.",
        ]
    # movement
    flips = [s for s in diff.significant if s["direction_flipped"]]
    bits: list[str] = []
    if diff.significant:
        clause = (
            f"{len(diff.significant)} findings shifted materially "
            "(more than 5 percentage points)"
        )
        if flips:
            clause += f", {len(flips)} of them flipping direction"
        bits.append(clause + ".")
        # Up to three highlights — the list is pre-sorted flips-first,
        # sharpest-first, so the head is the editorial lead.
        highlights = []
        for s in diff.significant[:3]:
            highlights.append(
                f"**{disp.get(s['group_name'], s['group_name'])}** "
                f"({_shift_flow_phrase(s['subkind'])}, "
                f"12 months to {_fmt_window_end(s['window_end'])}) went from "
                f"{s['old_yoy']*100:+.1f}% to {s['new_yoy']*100:+.1f}%"
            )
        bits.append("Sharpest: " + "; ".join(highlights) + ".")
    if diff.restated_count:
        bits.append(
            f"{diff.restated_count:,} older-window findings "
            f"({diff.restated_range}) were re-stated by a source "
            "correction — historic values, not this cycle's news."
        )
    if diff.total_new:
        bits.append(f"{diff.total_new:,} findings are new this cycle.")
    bits.append("The full list is in Tier 1 below.")
    return ["**Since the last pack:** " + " ".join(bits)]


def _section_front_page(
    top_movers: list[dict], diff: _DiffData | None,
    disp: dict[str, str] | None = None,
) -> _Section:
    """Render the front page. Dropped entirely on a fresh DB with
    nothing to say (no qualifying movers AND no previous export) —
    same convention as the other gated sections.

    `disp` (db.group_display_names) supplies reader-facing group labels for the
    mover sentences and the digest; defaults to identity so pure-render unit
    tests can call this without a DB."""
    has_digest = diff is not None and diff.regime != "first_export"
    if not top_movers and not has_digest:
        return _Section(markdown="")
    disp = disp or {}

    lines: list[str] = []
    lines.append(f"## {FRONT_PAGE_HEADING}")
    lines.append("")
    if top_movers:
        lines.append(
            "*The most quotable shifts this cycle, written ready to "
            "check — every figure is a 12-month total compared with the "
            "previous 12 months, and every sentence ends with its "
            "`finding/N` citation token. To qualify, a move must be at "
            "least 10 points, on a 12-month total of at least €100M, "
            "with no low-base warning and no 🔴 stability badge. Where "
            "both directions of one commodity qualify they are reported "
            "together, with the change in the EU-27’s net position in "
            "that commodity (exports minus imports; imports are valued "
            "CIF and exports FOB, which slightly overstates a deficit). "
            "The full picture, including the smaller and shakier moves, "
            "is in the tiers below.*"
        )
        lines.append("")
        for i, m in enumerate(top_movers, start=1):
            lines.append(f"{i}. {_mover_sentence(m, disp)}")
        lines.append("")
    if diff is not None:
        lines.extend(_since_last_pack_lines(diff, disp))
        lines.append("")

    return _Section(markdown="\n".join(lines))
