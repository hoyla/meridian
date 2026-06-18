"""Front-of-pack — Europe's goods-trade deficit with China, framed per day.

Renders the `trade_balance*` findings emitted by
`anomalies.detect_eu_china_trade_balance`. This block exists because the
single biggest Eurostat story of a typical cycle — Europe running a
goods-trade deficit with China of roughly €1bn a DAY — is a standing
*level*, not a year-on-year *change*, and so falls through every
change-threshold the rest of the pack is built on. A reader flagged that
the tool never surfaced it; this is the fix.

It sits at the very top of the document (right after "If you read only
this page") because the level is the headline a generalist desk wants
first. Deliberately **very compact**: one line per reporter scope, each
led by the figure a journalist would quote. If the top lines don't land
in a few seconds, a reader never gets to the detail tiers.

Three reporter scopes, each on the CN+HK+MO standard envelope:
- **EU-27** (Eurostat) — the headline; the CN-only press figure rides
  alongside in parentheses so it reconciles against what's published.
- **UK** (HMRC) — led by the 12-month figure only; the UK single month
  is too lumpy (aircraft, gold) to frame per-day.
- **EU-27 + UK** (the two summed) — "Europe" in the widest sense; a
  cross-source figure, flagged as such.

Deterministic, no LLM — same trust class as the Findings tiers below.
"""

from __future__ import annotations

from briefing_pack._helpers import (
    _Section,
    _fmt_month,
    _trace_token,
)

_HEADING = "The standing picture: Europe's goods-trade deficit with China"


def _fmt_bn(v) -> str:
    """Deficit totals are multi-billion; render to one decimal B."""
    if v is None:
        return "—"
    return f"€{float(v) / 1e9:,.1f}B"


def _fmt_per_day_bn(v) -> str:
    """€/day rendered in billions to two decimals, so the three scopes are
    comparable at a glance (the editorial point is 'EU ~€0.9bn, Europe-wide
    just over €1bn a day')."""
    if v is None:
        return "—"
    return f"€{abs(float(v)) / 1e9:,.2f}bn a day"


def _direction_word(deficit_eur) -> str:
    if deficit_eur is None:
        return "balance"
    return "deficit" if float(deficit_eur) >= 0 else "surplus"


def _latest(cur, subkind: str) -> dict | None:
    """Latest (most recent anchor) active finding for a trade_balance
    subkind, detail parsed out. None when none exist."""
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
    return {"id": row["id"], "anchor": row["anchor"],
            "roll_end": row["roll_end"], "totals": row["totals"]}


def _scope_line(label: str, std: dict | None, *, tail: str = "") -> str | None:
    """One compact line for a reporter scope, led by the 12-month deficit +
    €/day (the stable figure to quote). `tail` appends scope-specific
    colour (e.g. the EU CN-only press reconciliation). None when the scope
    has no finding."""
    if std is None:
        return None
    roll = std["totals"]["rolling_12mo"]
    word = _direction_word(roll["deficit_eur"])
    line = (
        f"- **{label}:** {_fmt_bn(roll['deficit_eur'])} {word} over the 12 "
        f"months to {_fmt_month(std['roll_end'])} — about "
        f"**{_fmt_per_day_bn(roll['deficit_per_day_eur'])}**"
    )
    if tail:
        line += tail
    return line + f". {_trace_token(std['id'])}"


def _section_trade_balance(cur) -> _Section:
    """Render the standing-deficit headline block. Empty markdown when no
    trade_balance findings exist yet (so the document degrades cleanly on a
    DB that predates the analyser) — same convention as the other gated
    sections."""
    eu = _latest(cur, "trade_balance")
    eu_cn = _latest(cur, "trade_balance_cn_only")
    uk = _latest(cur, "trade_balance_uk")
    combined = _latest(cur, "trade_balance_combined")
    if eu is None and uk is None and combined is None:
        return _Section(markdown="")

    lines: list[str] = []
    lines.append(f"## {_HEADING}")
    lines.append("")
    lines.append(
        "Unlike everything below, this is a **standing level, not a "
        "year-on-year change** — Europe's all-goods trade balance with China "
        "(imports minus exports), straight from Eurostat and HMRC. It barely "
        "moves cycle to cycle, so it never trips the \"what's new\" "
        "thresholds — but the *size* of it, per day, is usually the most "
        "quotable single number in the pack. CN+HK+MO basis."
    )
    lines.append("")

    # EU-27 headline, with the CN-only press figure riding alongside.
    eu_tail = ""
    if eu_cn is not None:
        cn_sm = eu_cn["totals"]["single_month"]
        eu_tail = (
            f" (€{abs(float(cn_sm['deficit_eur']))/1e9:,.1f}B in "
            f"{_fmt_month(eu_cn['anchor'])} alone on the mainland-China basis "
            f"Eurostat headlines — `finding/{eu_cn['id']}`)"
        )
    for line in [
        _scope_line("EU-27", eu, tail=eu_tail),
        _scope_line("UK", uk),
        _scope_line("EU-27 + UK", combined,
                    tail=" (two statistical agencies summed — see note)"),
    ]:
        if line:
            lines.append(line)
    lines.append("")

    # One-line caveat covering both sources + the cross-source sum.
    lines.append(
        "*Before quoting: Eurostat values imports CIF (freight + insurance) "
        "and exports FOB, which widens the deficit — but it is the basis "
        "Eurostat publishes on. The UK side is HMRC OTS (a sum of commodity "
        "detail, which can slightly undercount HMRC's published total). The "
        "EU-27 + UK line adds two agencies' figures: a close approximation, "
        "not a single-source number. All figures are all-goods totals, not "
        "the curated HS categories the tiers below break down.*"
    )
    lines.append("")

    return _Section(markdown="\n".join(lines))
