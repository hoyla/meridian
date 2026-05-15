"""Tier 2 sibling — GACC bilateral aggregate YoY summary.

Renders the China-X bilateral aggregate findings emitted by
`anomalies.detect_gacc_bilateral_aggregate_yoy`: the EU bloc plus every
single-country GACC partner. Companion to `state_of_play_aggregates.py`,
which covers the multi-country non-EU aggregates (ASEAN, RCEP, Belt &
Road, Africa, Latin America, world Total).

Each block surfaces all three YoY operators side-by-side: 12mo rolling
(the analyser's primary), YTD cumulative (the Soapbox A1 register —
"Jan-Apr exports +19% YoY"), and single-month (the Soapbox A3 register
— "Feb 2026 -16.2% YoY"). A journalist quoting the bilateral can pick
whichever cadence matches the story they're writing.
"""

from __future__ import annotations

from typing import Any

from briefing_pack._helpers import _Section, _fmt_eur, _trace_token


def _section_state_of_play_bilaterals(cur) -> _Section:
    """Tier 2 sibling — China-X bilateral aggregate state-of-play.

    Pulls the latest gacc_bilateral_aggregate_yoy finding per (partner,
    flow), renders one block per partner with both flow directions and
    all three YoY operators (12mo rolling, YTD, single-month) inside.

    Sort: EU bloc first (the headline editorial bilateral), then single
    countries by raw_label. Empty findings render the section header
    only — same convention as the other state-of-play sections."""
    cur.execute(
        """
        WITH latest AS (
          SELECT DISTINCT ON (detail->'partner'->>'raw_label', subkind)
                 id,
                 detail->'partner'->>'raw_label' AS partner_label,
                 detail->'partner'->>'kind' AS partner_kind,
                 subkind,
                 (detail->'windows'->>'current_end')::date AS current_end,
                 (detail->'totals'->>'yoy_pct')::numeric AS rolling_yoy_pct,
                 (detail->'totals'->>'current_12mo_eur')::numeric AS rolling_curr_eur,
                 (detail->'totals'->'ytd_cumulative'->>'yoy_pct')::numeric AS ytd_yoy_pct,
                 (detail->'totals'->'ytd_cumulative'->>'current_eur')::numeric AS ytd_curr_eur,
                 (detail->'totals'->'ytd_cumulative'->>'months_in_ytd')::int AS ytd_months,
                 (detail->'totals'->'single_month'->>'yoy_pct')::numeric AS sm_yoy_pct,
                 (detail->'totals'->'single_month'->>'current_eur')::numeric AS sm_curr_eur,
                 (detail->'totals'->>'partial_window')::boolean AS partial_window,
                 detail->'totals'->'jan_feb_combined_years' AS jan_feb_combined_years
            FROM findings
           WHERE subkind LIKE 'gacc_bilateral_aggregate_yoy%%' AND superseded_at IS NULL
        ORDER BY detail->'partner'->>'raw_label', subkind,
                 (detail->'windows'->>'current_end')::date DESC, id DESC
        )
        SELECT * FROM latest ORDER BY partner_label, subkind
        """
    )
    rows = list(cur.fetchall())

    lines: list[str] = []
    lines.append("## Tier 2 — Current state of play: GACC bilateral partners")
    lines.append("")
    lines.append(
        "China-side YoY for the EU bloc and every single-country GACC "
        "partner. The 12mo rolling figure is the stable comparator; the "
        "**YTD cumulative** figure is the Soapbox / Merics register "
        "(\"China-EU exports +19% Jan-Apr YoY\"); the **single-month** "
        "figure is the latest-month acceleration signal. All three "
        "operators share the same anchor period — pick whichever cadence "
        "fits the story."
    )
    lines.append("")

    if not rows:
        lines.append("*No active gacc_bilateral_aggregate_yoy findings to render — "
                     "run `scrape.py --analyse gacc-bilateral-aggregate-yoy`.*")
        lines.append("")
        return _Section(markdown="\n".join(lines))

    # Index by partner label → {subkind: row}.
    by_partner: dict[str, dict[str, Any]] = {}
    for r in rows:
        by_partner.setdefault(r["partner_label"], {})[r["subkind"]] = r

    SUBKIND_ORDER = [
        ("gacc_bilateral_aggregate_yoy", "Exports (China → partner)"),
        ("gacc_bilateral_aggregate_yoy_import", "Imports (partner → China)"),
    ]

    # EU bloc first, then single countries alphabetically. Matches the
    # editorial bias: the EU bilateral is the lead claim of most Soapbox
    # articles; single-country partners are secondary colour.
    def _sort_key(label: str) -> tuple[int, str]:
        kind = next(iter(by_partner[label].values()))["partner_kind"]
        return (0 if kind == "eu_bloc" else 1, label)

    for partner_label in sorted(by_partner.keys(), key=_sort_key):
        by_sk = by_partner[partner_label]
        any_row = next(iter(by_sk.values()))
        kind = any_row["partner_kind"] or "single_country"
        kind_str = "" if kind == "single_country" else f" *({kind})*"
        lines.append(f"### {partner_label}{kind_str}")
        lines.append("")
        for sk, label in SUBKIND_ORDER:
            r = by_sk.get(sk)
            if not r:
                continue
            rolling_v = float(r["rolling_yoy_pct"]) * 100 if r["rolling_yoy_pct"] is not None else None
            rolling_str = f"{rolling_v:+.1f}%" if rolling_v is not None else "n/a"
            rolling_eur = _fmt_eur(r["rolling_curr_eur"])

            ytd_block = ""
            if r["ytd_yoy_pct"] is not None and r["ytd_curr_eur"] is not None:
                ytd_v = float(r["ytd_yoy_pct"]) * 100
                ytd_eur = _fmt_eur(r["ytd_curr_eur"])
                months = r["ytd_months"]
                ytd_block = f" YTD ({months}mo): {ytd_v:+.1f}% to {ytd_eur}."

            sm_block = ""
            if r["sm_yoy_pct"] is not None and r["sm_curr_eur"] is not None:
                sm_v = float(r["sm_yoy_pct"]) * 100
                sm_eur = _fmt_eur(r["sm_curr_eur"])
                sm_block = f" Latest month: {sm_v:+.1f}% to {sm_eur}."

            # Inline annotations for the per-finding caveats most relevant
            # to a journalist scanning the line. `partial_window` flags that
            # months are missing; `jan_feb_combined` flags that part of the
            # window came in as a 2-month cumulative (Chinese-New-Year
            # combined release) rather than separate monthly figures. The
            # full caveat text is in the per-finding provenance file.
            annotations: list[str] = []
            if r["partial_window"]:
                annotations.append("partial window")
            jfc_years = r["jan_feb_combined_years"]
            if jfc_years:
                annotations.append(f"includes Jan+Feb {','.join(str(y) for y in jfc_years)} cumulative")
            suffix = (" — " + "; ".join(annotations)) if annotations else ""
            lines.append(
                f"  - **{label}**: 12mo rolling {rolling_str} to {rolling_eur} "
                f"(12mo to {r['current_end']}).{ytd_block}{sm_block}{suffix} "
                f"{_trace_token(r['id'])}"
            )
        lines.append("")

    return _Section(markdown="\n".join(lines))
