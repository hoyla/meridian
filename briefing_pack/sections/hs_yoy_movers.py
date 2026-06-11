"""Top-N hs_group_yoy movers per scope × flow."""

from __future__ import annotations

from typing import Any

from briefing_pack._helpers import (
    PREDICTABILITY_SHIFT_PP,
    _Section,
    _SCOPE_LABEL,
    _SCOPE_SUBKIND_SUFFIX,
    _flow_phrase,
    _fmt_eur,
    _fmt_kg,
    _fmt_pct,
    _hmrc_suppressed_counts,
    _quotability_verdict,
    _release_ids_for_window,
    _threshold_fragility_annotation,
    _trace_token,
)


def _decomposition_label(yoy_eur: Any, yoy_kg: Any) -> str:
    """Mirrors the volume-vs-price decomposition in anomalies.py."""
    if yoy_eur is None or yoy_kg is None or float(yoy_eur) == 0:
        return ""
    share = float(yoy_kg) / float(yoy_eur)
    return "volume-driven" if abs(share) > 0.5 else "price-driven"


def _section_hs_yoy_movers(
    cur, flow: int, top_n: int, comparison_scope: str = "eu_27",
    predictability: dict[str, tuple[str, float, int]] | None = None,
) -> _Section:
    """Top-N movers by |yoy_pct| for the latest period per group, scoped to
    one of EU-27 / UK / combined. Each scope renders its own section so a
    journalist scanning the brief sees the three views distinctly.

    `predictability` (when provided): per-group YoY-stability badge from
    `_compute_predictability_per_group`. Surfaced inline next to the group
    name so a journalist reading the brief sees which group's headline
    YoY is robust vs noise-dominated.
    """
    predictability = predictability or {}
    scope_suffix = _SCOPE_SUBKIND_SUFFIX[comparison_scope]
    flow_suffix = "" if flow == 1 else "_export"
    subkind = f"hs_group_yoy{scope_suffix}{flow_suffix}"
    scope_label = _SCOPE_LABEL[comparison_scope]
    flow_label = f"{scope_label} {_flow_phrase(flow)}"
    flow_short = "imports" if flow == 1 else "exports"
    cur.execute(
        """
        WITH latest AS (
          SELECT DISTINCT ON (detail->'group'->>'name')
                 id,
                 detail->'group'->>'name' AS group_name,
                 (detail->'windows'->>'current_start')::date AS current_start,
                 (detail->'windows'->>'current_end')::date AS current_end,
                 (detail->'windows'->>'prior_start')::date AS prior_start,
                 (detail->'windows'->>'prior_end')::date AS prior_end,
                 detail->'totals'->'missing_months_current' AS missing_curr,
                 detail->'totals'->'missing_months_prior' AS missing_prior,
                 (detail->'totals'->>'current_12mo_eur')::numeric AS current_eur,
                 (detail->'totals'->>'prior_12mo_eur')::numeric AS prior_eur,
                 (detail->'totals'->>'yoy_pct')::numeric AS yoy_pct,
                 (detail->'totals'->>'current_12mo_kg')::numeric AS current_kg,
                 (detail->'totals'->>'yoy_pct_kg')::numeric AS yoy_pct_kg,
                 (detail->'totals'->>'unit_price_pct_change')::numeric AS unit_price_pct,
                 (detail->'totals'->>'low_base')::boolean AS low_base,
                 (detail->'totals'->>'low_base_threshold_eur')::numeric AS low_base_threshold,
                 detail->'method_query'->'hs_patterns' AS hs_patterns,
                 detail->'method_query'->'partners' AS partners_used,
                 detail->'per_reporter_breakdown' AS per_reporter_breakdown
            FROM findings
           WHERE subkind = %s AND superseded_at IS NULL
        ORDER BY detail->'group'->>'name', (detail->'windows'->>'current_end')::date DESC, id DESC
        )
        SELECT * FROM latest ORDER BY abs(yoy_pct) DESC NULLS LAST LIMIT %s
        """,
        (subkind, top_n),
    )
    rows = cur.fetchall()

    release_ids: set[int] = set()
    lines: list[str] = []
    if not rows:
        # Empty scope — return blank markdown so render() drops the section
        # rather than printing N empty headers per scope. The default scope
        # (eu_27) still surfaces a "no findings" header below if needed.
        return _Section(markdown="")
    lines.append(f"### {flow_label} — top {len(rows)} movers (12-month year-on-year)")
    lines.append("")

    for r in rows:
        # Phase: per-group YoY-predictability badge from the historical
        # supersede chain. Suppressed if no T-6 pair exists for this group
        # (fresh groups + edge cases).
        pred = predictability.get(r['group_name'])
        badge_str = ""
        if pred is not None:
            badge, _pct, _n = pred
            badge_str = f" {badge}"
        lines.append(f"#### {r['group_name']}{badge_str}")
        # The render-time quotability verdict leads the block — the
        # plain-English instruction that applies methodology §9/§10 at
        # the point of quotation. The bullets after it are the
        # supporting evidence.
        lines.append(
            "- **Quotability**: "
            + _quotability_verdict(
                badge=pred[0] if pred is not None else None,
                low_base=r["low_base"],
                current_eur=r["current_eur"],
                prior_eur=r["prior_eur"],
                threshold_eur=r["low_base_threshold"],
                missing_current=r["missing_curr"],
                missing_prior=r["missing_prior"],
            )
        )
        if pred is not None:
            badge, pct, n = pred
            label = (
                "persistent" if badge == "🟢"
                else "noisy" if badge == "🟡"
                else "volatile"
            )
            lines.append(
                f"- *Signal stability* ({badge} {label}): "
                f"{int(pct*100)}% of {n} views of this group stayed on "
                f"the same direction with a shift under "
                f"{int(PREDICTABILITY_SHIFT_PP)}pp vs 6 months ago."
            )
        # Surface the period the finding actually refers to. For groups where
        # the analyser has stopped emitting findings (e.g. low-base failure),
        # this prevents the brief from claiming a stale period is "latest".
        lines.append(
            f"- **Period (12mo ending)**: {r['current_end'].strftime('%Y-%m')}"
        )
        lines.append(
            f"- **Value**: {_fmt_pct(r['yoy_pct'])} "
            f"({_fmt_eur(r['prior_eur'])} → {_fmt_eur(r['current_eur'])})"
        )
        if r['yoy_pct_kg'] is not None:
            lines.append(
                f"- **Volume**: {_fmt_pct(r['yoy_pct_kg'])} "
                f"(12mo total: {_fmt_kg(r['current_kg'])})"
            )
        if r['unit_price_pct'] is not None:
            decomp = _decomposition_label(r['yoy_pct'], r['yoy_pct_kg'])
            lines.append(
                f"- **Unit price (€/kg)**: {_fmt_pct(r['unit_price_pct'])}"
                + (f" — *{decomp}*" if decomp else "")
            )
        # Phase 6.11: reporter contributions. Surfaces which EU member states
        # drove the group's headline YoY — answers Soapbox-style "Germany
        # alone accounts for 66% of the drop" framing. Cap at 5 in the
        # brief; the full top-10 is in detail.per_reporter_breakdown and
        # the spreadsheet's hs_yoy_reporter_movers tab.
        per_rep = r.get("per_reporter_breakdown") or []
        # Filter to entries that actually moved the needle: skip pure
        # zero deltas and trivially-tiny ones so the brief stays readable
        # for UK-scope findings (where the only reporter is GB).
        per_rep = [pr for pr in per_rep if pr.get("delta_eur") not in (None, 0)]
        if per_rep:
            lines.append("- **Reporter contributions**:")
            for pr in per_rep[:5]:
                rep = pr["reporter"]
                yoy = pr.get("yoy_pct")
                yoy_str = (
                    f"{float(yoy)*100:+.1f}%" if yoy is not None else "n/a"
                )
                share = pr.get("share_of_group_delta_pct")
                share_str = (
                    f", {float(share)*100:+.0f}% of group's Δ"
                    if share is not None else ""
                )
                delta = float(pr.get("delta_eur") or 0)
                lines.append(
                    f"  - {rep}: {yoy_str} ({_fmt_eur(pr['prior_eur'])} → "
                    f"{_fmt_eur(pr['current_eur'])}, Δ {_fmt_eur(delta)}"
                    f"{share_str})"
                )
        # Low-base instruction now lives in the Quotability verdict above
        # (with the actual amounts + threshold), not a separate bullet.
        # Threshold-fragility annotation stays as supporting evidence
        # (orthogonal to the low_base flag — a finding can be flagged AND
        # fragile, or fragile-but-not-flagged because it's just above the
        # threshold).
        fragility = _threshold_fragility_annotation(
            r['current_eur'], r['prior_eur'], r['low_base_threshold'],
        )
        if fragility:
            lines.append(fragility)
        # UK-scope integrity rider: how many HMRC source rows were
        # suppressed for confidentiality (and so excluded from the
        # totals above), split by window because differing suppression
        # between windows skews the YoY itself. Render-time count
        # mirroring the analyser's aggregation predicates.
        if comparison_scope == "uk" and r["prior_end"] is not None:
            n_curr, n_prior = _hmrc_suppressed_counts(
                cur,
                patterns=list(r["hs_patterns"] or []),
                partners=list(r.get("partners_used") or ["CN", "HK", "MO"]),
                flow=flow,
                current_start=r["current_start"],
                current_end=r["current_end"],
                prior_start=r["prior_start"],
                prior_end=r["prior_end"],
            )
            if n_curr or n_prior:
                lines.append(
                    f"- **HMRC suppression**: {n_curr} source rows in the "
                    f"current window and {n_prior} in the prior window were "
                    "suppressed by HMRC for confidentiality and are excluded "
                    "from these totals — the UK figures are a slight "
                    "undercount."
                )
        # Pull partner list from the finding's method_query (default new
        # behaviour: CN+HK+MO; legacy CN-only findings are superseded after
        # the v7 method bump but rendering here stays defensive).
        partners_used = (
            r.get("partners_used") or ["CN", "HK", "MO"]
        )
        lines.append(
            f"- *Method*: 12mo rolling, partners={','.join(partners_used)}, "
            f"flow={flow_short}, hs_patterns=`{r['hs_patterns']}`"
        )
        # Window-traced source span
        period_start = r['prior_start']
        period_end = r['current_end']
        ids = _release_ids_for_window(cur, period_start, period_end)
        release_ids |= ids
        lines.append(
            f"- *Sources*: {len(ids)} Eurostat monthly bulk files, "
            f"{period_start.strftime('%Y-%m')} → {period_end.strftime('%Y-%m')}"
        )
        lines.append(f"- *Trace*: {_trace_token(r['id'])}")
        lines.append("")

    return _Section(markdown="\n".join(lines), release_ids=release_ids)
