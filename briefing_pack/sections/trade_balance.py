"""Front-of-pack — the EU's goods-trade deficit with China, framed per day.

Renders the `trade_balance` / `trade_balance_cn_only` findings emitted by
`anomalies.detect_eu_china_trade_balance`. This block exists because the
single biggest Eurostat story of a typical cycle — the EU running a
goods-trade deficit with China of roughly €1bn a DAY — is a standing
*level*, not a year-on-year *change*, and so falls through every
change-threshold the rest of the pack is built on. A reader flagged that
the tool never surfaced it; this is the fix.

It sits at the very top of the document (right after "If you read only
this page") because the level is the headline a generalist desk wants
first. Two scopes are shown side by side:

- **CN+HK+MO** (subkind `trade_balance`): our editorial standard, the
  same China definition every other family in the pack uses.
- **CN-only** (subkind `trade_balance_cn_only`): the slice Eurostat uses
  in its own published EU–China headline, so the figure reconciles
  directly against the press number.

Deterministic, no LLM — same trust class as the Findings tiers below.
"""

from __future__ import annotations

from typing import Any

from briefing_pack._helpers import (
    _Section,
    _fmt_month,
    _trace_token,
)

_HEADING = "The standing picture: the EU's goods-trade deficit with China"


def _fmt_bn(v: Any) -> str:
    """Deficit figures are always multi-billion; render to one decimal B."""
    if v is None:
        return "—"
    return f"€{float(v) / 1e9:,.1f}B"


def _fmt_per_day(v: Any) -> str:
    """€/day, rendered in millions (the register the press quotes)."""
    if v is None:
        return "—"
    return f"€{abs(float(v)) / 1e6:,.0f}M a day"


def _latest_by_scope(cur, subkind: str) -> dict | None:
    """Latest (most recent anchor) active finding for a trade_balance
    subkind, with its detail JSON parsed out. None when none exist."""
    cur.execute(
        """
        SELECT id,
               (detail->'windows'->>'anchor_period')::date AS anchor,
               (detail->'windows'->>'rolling_current_end')::date AS roll_end,
               detail->'totals' AS totals
          FROM findings
         WHERE subkind = %s AND superseded_at IS NULL
      ORDER BY (detail->'windows'->>'anchor_period')::date DESC, id DESC
         LIMIT 1
        """,
        (subkind,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {"id": row["id"], "anchor": row["anchor"], "roll_end": row["roll_end"],
            "totals": row["totals"]}


def _direction_word(deficit_eur: float | None) -> str:
    if deficit_eur is None:
        return "balance"
    return "deficit" if float(deficit_eur) >= 0 else "surplus"


def _section_trade_balance(cur) -> _Section:
    """Render the standing-deficit headline block. Empty markdown when no
    trade_balance findings exist yet (so the document degrades cleanly on
    a DB that predates the analyser) — same convention as the other gated
    sections."""
    std = _latest_by_scope(cur, "trade_balance")
    cn = _latest_by_scope(cur, "trade_balance_cn_only")
    if std is None and cn is None:
        return _Section(markdown="")

    lead = std or cn  # prefer the editorial-standard scope for the headline
    sm = lead["totals"]["single_month"]
    roll = lead["totals"]["rolling_12mo"]
    ytd = lead["totals"].get("ytd")

    word_sm = _direction_word(sm["deficit_eur"])
    word_roll = _direction_word(roll["deficit_eur"])
    scope_label = "CN+HK+MO" if (std is not None) else "CN only"

    lines: list[str] = []
    lines.append(f"## {_HEADING}")
    lines.append("")
    lines.append(
        "Unlike everything below, this is a **standing level, not a "
        "year-on-year change** — the EU-27's all-goods trade balance with "
        "China (imports minus exports), straight from Eurostat. It barely "
        "moves cycle to cycle, so it never trips the \"what's new\" "
        "thresholds — but the *size* of it, expressed per day, is usually "
        "the most quotable single number in the pack."
    )
    lines.append("")

    # Headline sentence — editorial-standard scope, single month + per day.
    lines.append(
        f"- In **{_fmt_month(lead['anchor'])}** the EU-27 ran a goods-trade "
        f"{word_sm} with China of **{_fmt_bn(sm['deficit_eur'])}** — about "
        f"**{_fmt_per_day(sm['deficit_per_day_eur'])}** ({scope_label}). "
        f"{_trace_token(lead['id'])}"
    )
    # Rolling 12-month context.
    roll_line = (
        f"- Over the **12 months to {_fmt_month(lead['roll_end'])}** the "
        f"rolling {word_roll} was **{_fmt_bn(roll['deficit_eur'])}** "
        f"({_fmt_per_day(roll['deficit_per_day_eur'])})"
    )
    if roll.get("yoy_pct") is not None:
        roll_line += f", {float(roll['yoy_pct'])*100:+.1f}% vs the prior 12 months"
    roll_line += "."
    lines.append(roll_line)
    # Year-to-date, if present.
    if ytd is not None and ytd.get("current_deficit_eur") is not None:
        ytd_line = (
            f"- Year-to-date (Jan–{_fmt_month(lead['anchor'])}, "
            f"{ytd['months_in_ytd']} months): {_direction_word(ytd['current_deficit_eur'])} "
            f"**{_fmt_bn(ytd['current_deficit_eur'])}**"
        )
        if ytd.get("yoy_pct") is not None:
            ytd_line += f" ({float(ytd['yoy_pct'])*100:+.1f}% YoY)"
        ytd_line += "."
        lines.append(ytd_line)
    lines.append("")

    # CN-only reconciliation line, when both scopes exist.
    if std is not None and cn is not None:
        cn_sm = cn["totals"]["single_month"]
        lines.append(
            f"**Reconciling with the press figure:** Eurostat's own published "
            f"EU–China headline counts mainland China only. On that **CN-only** "
            f"basis the {_fmt_month(cn['anchor'])} {_direction_word(cn_sm['deficit_eur'])} "
            f"was **{_fmt_bn(cn_sm['deficit_eur'])}** "
            f"({_fmt_per_day(cn_sm['deficit_per_day_eur'])}) — the number you'll "
            f"see quoted. Our headline above adds Hong Kong + Macau, consistent "
            f"with the rest of this pack. {_trace_token(cn['id'])}"
        )
        lines.append("")

    # Quote-safety caveat.
    lines.append(
        "*Before quoting: Eurostat values imports CIF (freight + insurance "
        "included) and exports FOB, which widens the deficit relative to a "
        "like-for-like basis — but it is the basis Eurostat publishes on, so "
        "the figure is directly comparable to its press releases. This is an "
        "all-goods total across every product, not the curated HS categories "
        "the tiers below break down.*"
    )
    lines.append("")

    return _Section(markdown="\n".join(lines))
