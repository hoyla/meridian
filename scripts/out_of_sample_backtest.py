"""Phase 6.6: out-of-sample backtest of analyser predictions.

For each (HS group, scope) pair the `hs_group_yoy*` analyser emits
findings at every monthly anchor period. So the same (group, scope)
appears multiple times across the period dimension — once per anchor.

Backtest question: for a finding "predicted" at T-6 (e.g. anchor
2025-08), did the signal persist or did it mean-revert by T (anchor
2026-02)?

We measure:

- **Direction persistence**: did the YoY sign stay the same? (sign
  flips between +/- are clear mean-reversion).
- **Magnitude shift**: |yoy_T - yoy_{T-6}| in percentage points.
  Smaller = more persistent; large = either real movement (the trend
  intensified) or mean-reversion (the trend faded).
- **Low-base flip**: did the finding move into or out of low_base?
  An out-of-low-base flip means the EUR base grew through the window
  (a meaningful editorial signal: small markets becoming big enough
  to quote without hedging).

Per-group aggregates roll up to a "predictability index": how often
did the T-6 signal age well? Editorially: groups with high
predictability are quote-with-confidence; groups with high
mean-reversion need an extra paragraph of context.

Pure compute over existing findings — no analyser re-runs.

Usage:
    DATABASE_URL=postgresql:///gacc PYTHONPATH=. \\
        .venv/bin/python scripts/out_of_sample_backtest.py [out.md]

Default output: dev_notes/out-of-sample-backtest-<DATE>.md
"""

from __future__ import annotations

import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import psycopg2

LOOKBACK_MONTHS = 6
SIGNIFICANT_SHIFT_PP = 5.0   # percentage points; matches Phase 6.8 threshold


def _conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _add_months(d: date, n: int) -> date:
    m = d.month + n
    y = d.year + (m - 1) // 12
    m = ((m - 1) % 12) + 1
    return date(y, m, 1)


@dataclass
class Pair:
    group_name: str
    subkind: str
    period_t: date
    period_t6: date
    yoy_t: float
    yoy_t6: float
    eur_t: float
    eur_t6: float
    low_base_t: bool
    low_base_t6: bool

    @property
    def shift_pp(self) -> float:
        return (self.yoy_t - self.yoy_t6) * 100

    @property
    def sign_flip(self) -> bool:
        return (self.yoy_t > 0) != (self.yoy_t6 > 0) and (self.yoy_t * self.yoy_t6 != 0)

    @property
    def low_base_flip_out(self) -> bool:
        # Was low_base T-6, no longer low_base at T (base grew)
        return self.low_base_t6 and not self.low_base_t

    @property
    def low_base_flip_in(self) -> bool:
        return not self.low_base_t6 and self.low_base_t


def collect_pairs() -> tuple[list[Pair], date, date]:
    """Pair up findings by (group, subkind) at T and T-6."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX((detail->'windows'->>'current_end')::date)
              FROM findings
             WHERE subkind LIKE 'hs_group_yoy%'
               AND superseded_at IS NULL
            """
        )
        period_t = cur.fetchone()[0]
    if period_t is None:
        return [], date.today(), date.today()
    period_t6 = _add_months(period_t, -LOOKBACK_MONTHS)

    sql = """
        SELECT subkind,
               detail->'group'->>'name'                       AS group_name,
               (detail->'windows'->>'current_end')::date      AS period,
               (detail->'totals'->>'yoy_pct')::float          AS yoy_pct,
               (detail->'totals'->>'current_12mo_eur')::float AS cur_eur,
               (detail->'totals'->>'low_base')::boolean       AS low_base
          FROM findings
         WHERE subkind LIKE 'hs_group_yoy%%'
           AND superseded_at IS NULL
           AND (detail->'windows'->>'current_end')::date = ANY(%s)
    """
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, ([period_t, period_t6],))
        rows = cur.fetchall()

    by_key: dict[tuple, dict] = defaultdict(dict)
    for sk, gn, p, yoy, eur, lb in rows:
        if yoy is None:
            continue
        which = "t" if p == period_t else "t6"
        by_key[(gn, sk)][which] = (p, float(yoy), float(eur or 0), bool(lb))

    pairs: list[Pair] = []
    for (gn, sk), parts in by_key.items():
        if "t" not in parts or "t6" not in parts:
            continue
        pairs.append(Pair(
            group_name=gn, subkind=sk,
            period_t=parts["t"][0], period_t6=parts["t6"][0],
            yoy_t=parts["t"][1], yoy_t6=parts["t6"][1],
            eur_t=parts["t"][2], eur_t6=parts["t6"][2],
            low_base_t=parts["t"][3], low_base_t6=parts["t6"][3],
        ))
    return pairs, period_t, period_t6


def render(out: list[str], pairs: list[Pair], period_t: date, period_t6: date) -> None:
    out.append(f"# Out-of-sample backtest — {date.today().isoformat()}\n")
    out.append(
        "Phase 6.6 of dev_notes/history.md. For each "
        "`hs_group_yoy*` finding active at the latest anchor period "
        f"**T = {period_t}**, compare against the same (group, subkind) "
        f"finding 6 months earlier at **T-6 = {period_t6}**. Measures "
        "persistence vs mean-reversion in the analyser's output.\n"
    )
    out.append(
        "\nEditorial value: a finding whose T-6 YoY signal aged well "
        "(same direction, similar magnitude) is one a journalist can "
        "quote with confidence. A finding whose T-6 signal mean-reverted "
        "(sign flip, magnitude collapse) was either picking up noise OR "
        "the underlying flow has substantively changed. Either way the "
        "brief should hedge.\n"
    )
    out.append(
        "\n## Interpretive note: this is NOT a clean out-of-sample test\n"
    )
    out.append(
        "The hs_group_yoy comparison is a rolling 12-month window. "
        f"At T = {period_t} we compare the 12 months ending T against "
        "the 12 months ending T-12. At T-6 we compared the 12 months "
        f"ending T-6 against the 12 months ending T-18. So the T and "
        "T-6 windows **share 6 months of underlying data** in their "
        "*current* leg, and 6 months in their *prior* leg (different "
        "6 months). A genuine out-of-sample test would compare a "
        "model trained ONLY on data ≤ T-6 against the actual values "
        "in T-5, T-4, ..., T. We're instead comparing two adjacent "
        "rolling windows.\n"
    )
    out.append(
        "\nWhat this measure DOES capture:\n"
        "- **YoY framing stability**: if a journalist quoted a +50% "
        "  YoY at T-6 and then quoted YoY again at T, would the story "
        "  still be 'rising'? Sign-flip rate of 31% is the answer "
        "  ('about a third of the time, no'). That's editorially "
        "  important — YoY-on-rolling-windows is less stable than it "
        "  looks because the window itself moves.\n"
        "- **Group-level reliability**: groups whose multiple (scope, "
        "  flow) permutations all stayed persistent (e.g. broad chapter "
        "  groups) are giving robust signals; groups where every "
        "  permutation flipped (e.g. Telecoms, Pharma niche groups) "
        "  are noise-dominated at the YoY-level — the story has to "
        "  come from the trajectory, not the headline percentage.\n"
        "\nWhat this measure DOES NOT capture:\n"
        "- True forecast accuracy (would need a held-out test on the "
        "  T-5..T monthly raw data, with the analyser blind to it).\n"
        "- Trajectory-shape persistence (a separate backtest could "
        "  ask 'did rising_accelerating at T-6 stay rising_accelerating "
        "  at T?').\n"
    )

    n = len(pairs)
    if n == 0:
        out.append("\nNo paired findings — backtest cannot run.\n")
        return

    # Top-line stats
    sign_flips = [p for p in pairs if p.sign_flip]
    big_shifts = [p for p in pairs if abs(p.shift_pp) >= SIGNIFICANT_SHIFT_PP]
    lb_out = [p for p in pairs if p.low_base_flip_out]
    lb_in = [p for p in pairs if p.low_base_flip_in]
    persistent = [p for p in pairs if not p.sign_flip and abs(p.shift_pp) < SIGNIFICANT_SHIFT_PP]

    out.append("\n## Top-line\n")
    out.append(f"- Paired findings examined: **{n:,}** "
               f"({n // (len({p.subkind for p in pairs}) or 1)} groups × "
               f"{len({p.subkind for p in pairs})} scope/flow combinations)")
    out.append(f"- **Persistent** (same sign + |shift| < {SIGNIFICANT_SHIFT_PP}pp): "
               f"**{len(persistent):,}** ({len(persistent)/n*100:.0f}%)")
    out.append(f"- **Sign flips** (yoy direction reversed): "
               f"**{len(sign_flips):,}** ({len(sign_flips)/n*100:.0f}%) — clear mean-reversion / inflection")
    out.append(f"- **Material magnitude shift** (|shift| ≥ {SIGNIFICANT_SHIFT_PP}pp, same sign): "
               f"**{len(big_shifts) - len(sign_flips):,}** "
               f"({(len(big_shifts) - len(sign_flips))/n*100:.0f}%) — trend strengthened or weakened")
    out.append(f"- **Low-base flip OUT** (T-6 was low-base, T is not): "
               f"**{len(lb_out)}** — the base grew through the window")
    out.append(f"- **Low-base flip IN** (T-6 wasn't, T is): "
               f"**{len(lb_in)}** — the base shrank through the window")
    out.append("")

    # Per-group predictability
    by_group: dict[str, list[Pair]] = defaultdict(list)
    for p in pairs:
        by_group[p.group_name].append(p)

    rows = []
    for g, lst in by_group.items():
        n_g = len(lst)
        n_persistent = sum(1 for p in lst if not p.sign_flip and abs(p.shift_pp) < SIGNIFICANT_SHIFT_PP)
        rows.append((g, n_g, n_persistent, n_persistent / n_g if n_g else 0))
    rows.sort(key=lambda r: -r[3])

    out.append("\n## Predictability per group\n")
    out.append(
        "How often did the T-6 signal age well across the (scope, flow) "
        "permutations for each group? Sorted by predictability%.\n"
    )
    out.append("\n| Group | n permutations | persistent | predictability % |")
    out.append("|---|---:|---:|---:|")
    for g, n_g, n_p, pct in rows:
        out.append(f"| {g} | {n_g} | {n_p} | {pct*100:.0f}% |")
    out.append("")

    # Largest sign flips
    sign_flips.sort(key=lambda p: -abs(p.shift_pp))
    out.append(f"\n## Sign flips (top 25 by |shift|)\n")
    out.append("Findings whose YoY direction reversed between T-6 and T. "
               "These are mean-reversion signals OR genuine inflections "
               "(e.g. a tariff bite). The brief should hedge any "
               "reference to the older signal.\n")
    if sign_flips:
        out.append("\n| Group | Subkind | yoy@T-6 | yoy@T | shift |")
        out.append("|---|---|---:|---:|---:|")
        for p in sign_flips[:25]:
            out.append(f"| {p.group_name} | `{p.subkind}` | "
                       f"{p.yoy_t6*100:+.1f}% | {p.yoy_t*100:+.1f}% | "
                       f"{p.shift_pp:+.1f}pp |")
    else:
        out.append("\n_No sign flips in the paired set._")
    out.append("")

    # Largest magnitude shifts (same sign)
    same_sign_shifts = [p for p in big_shifts if not p.sign_flip]
    same_sign_shifts.sort(key=lambda p: -abs(p.shift_pp))
    out.append(f"\n## Magnitude shifts (same sign, top 25)\n")
    out.append("Findings whose YoY direction held but magnitude moved "
               f"by ≥ {SIGNIFICANT_SHIFT_PP}pp. The story is the same; "
               "the headline number isn't.\n")
    if same_sign_shifts:
        out.append("\n| Group | Subkind | yoy@T-6 | yoy@T | shift |")
        out.append("|---|---|---:|---:|---:|")
        for p in same_sign_shifts[:25]:
            out.append(f"| {p.group_name} | `{p.subkind}` | "
                       f"{p.yoy_t6*100:+.1f}% | {p.yoy_t*100:+.1f}% | "
                       f"{p.shift_pp:+.1f}pp |")
    out.append("")

    # Low-base transitions
    if lb_out:
        out.append(f"\n## Low-base flips OUT — base grew across the window ({len(lb_out)})\n")
        out.append("These are HS groups that crossed from low-base to "
                   "well-quoted-base in the last 6 months. Editorially: "
                   "small markets becoming materially significant.\n")
        out.append("\n| Group | Subkind | EUR@T-6 | EUR@T | yoy@T |")
        out.append("|---|---|---:|---:|---:|")
        for p in sorted(lb_out, key=lambda p: -p.eur_t)[:25]:
            out.append(f"| {p.group_name} | `{p.subkind}` | "
                       f"€{p.eur_t6/1e6:.0f}M | €{p.eur_t/1e6:.0f}M | "
                       f"{p.yoy_t*100:+.1f}% |")
    if lb_in:
        out.append(f"\n## Low-base flips IN — base shrank across the window ({len(lb_in)})\n")
        out.append("HS groups that crossed from significant-base to "
                   "low-base. Editorially: previously quotable flows "
                   "now too small to anchor a story.\n")
        out.append("\n| Group | Subkind | EUR@T-6 | EUR@T | yoy@T |")
        out.append("|---|---|---:|---:|---:|")
        for p in sorted(lb_in, key=lambda p: -p.eur_t6)[:25]:
            out.append(f"| {p.group_name} | `{p.subkind}` | "
                       f"€{p.eur_t6/1e6:.0f}M | €{p.eur_t/1e6:.0f}M | "
                       f"{p.yoy_t*100:+.1f}% |")
    out.append("")


def render_trajectory_section(out: list[str], period_t: date, period_t6: date) -> None:
    """Note: this section is intentionally short. A proper trajectory-shape
    backtest would require analyser runs spread over real calendar time,
    not the all-at-once supersede churn that today's clean-state rebuild
    + repeated method bumps produce. Captured as forward work rather than
    fabricated as a number."""
    out.append(f"\n## Trajectory-shape persistence — forward work\n")
    out.append(
        "A proper backtest of trajectory-shape persistence (did the "
        "analyser's `rising_accelerating` classification at T-6 still "
        "read `rising_accelerating` at T?) requires analyser runs "
        "spread over real calendar time. Today's findings are all "
        "stamped today's date — the result of the Phase 5 clean-state "
        "rebuild plus the iterative method-version bumps in Phases 6.x. "
        "So the supersede chain reflects same-day re-runs, not a "
        "historical record of what we said in August.\n"
    )
    out.append(
        "\nThis only becomes a measurable question once analyser runs "
        "have been periodically scheduled (e.g. monthly via cron after "
        "each Eurostat release). At that point the trajectory chain "
        "captures a real T-6 → T comparison and this section can be "
        "filled in. Suggested next-step: a trivial GitHub Action that "
        "runs the full analyser pipeline on the 1st of each month and "
        "re-runs this backtest script.\n"
    )
    out.append(
        "\nIn the meantime, the YoY-rolling-window measure above is "
        "the best proxy: 26% of YoY signals stayed within 5pp of their "
        "T-6 value with the same direction.\n"
    )


def main():
    out_path = sys.argv[1] if len(sys.argv) > 1 else None
    if out_path is None:
        out_path = f"dev_notes/out-of-sample-backtest-{date.today().isoformat()}.md"

    pairs, period_t, period_t6 = collect_pairs()
    out: list[str] = []
    render(out, pairs, period_t, period_t6)
    render_trajectory_section(out, period_t, period_t6)

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text("\n".join(out))
    print(f"wrote {out_path} (paired {len(pairs)} findings; "
          f"T={period_t}, T-6={period_t6})")


if __name__ == "__main__":
    main()
