"""Latest mirror_gap finding per partner, plus z-score movers."""

from __future__ import annotations

from briefing_pack._helpers import (
    _ALL_UNIVERSAL_CAVEATS,
    _Section,
    _fmt_eur,
    _fmt_pct,
    _release_ids_for_observations,
    _trace_token,
)


def _section_mirror_gaps(cur) -> _Section:
    """Latest mirror_gap finding per partner, plus z-score movers."""
    cur.execute(
        """
        SELECT DISTINCT ON (detail->>'iso2')
            f.id, f.observation_ids,
            detail->>'iso2' AS iso2,
            detail->'gacc'->>'partner_label_raw' AS gacc_label,
            (detail->'gacc'->>'value_eur_converted')::numeric AS gacc_eur,
            (detail->'eurostat'->>'total_eur')::numeric AS eurostat_eur,
            (detail->>'gap_eur')::numeric AS gap_eur,
            (detail->>'gap_pct')::numeric AS gap_pct,
            (detail->>'is_aggregate')::boolean AS is_aggregate,
            detail->'caveat_codes' AS caveat_codes,
            detail->'transshipment_hub'->>'iso2' AS hub_iso2,
            detail->'transshipment_hub'->>'notes' AS hub_notes,
            (detail->'cif_fob_baseline'->>'baseline_pct')::numeric AS baseline_pct,
            detail->'cif_fob_baseline'->>'scope' AS baseline_scope,
            detail->'cif_fob_baseline'->>'source' AS baseline_source,
            detail->'cif_fob_baseline'->>'source_url' AS baseline_source_url,
            (SELECT to_char(r.period, 'YYYY-MM')
               FROM observations o JOIN releases r ON r.id = o.release_id
              WHERE o.id = f.observation_ids[1]) AS period
          FROM findings f
         WHERE subkind = 'mirror_gap' AND superseded_at IS NULL
      ORDER BY detail->>'iso2',
               (SELECT r.period FROM observations o JOIN releases r ON r.id = o.release_id
                 WHERE o.id = f.observation_ids[1]) DESC,
               f.id DESC
        """
    )
    gap_rows = cur.fetchall()

    release_ids: set[int] = set()
    lines: list[str] = []
    lines.append("### Mirror-trade gaps (latest per partner)")
    lines.append("")
    lines.append(
        "Mirror-gap = (Eurostat — GACC_EUR_converted) / Eurostat. The *expected* "
        "baseline is +5–10% (CIF vs FOB pricing — caveat `cif_fob`). Persistent gaps "
        "well above that — Netherlands and Italy notably — sit in the structural "
        "transshipment territory; sudden movements are flagged separately as movers."
    )
    lines.append("")
    if not gap_rows:
        lines.append("*No mirror-gap findings yet.*")
        lines.append("")
    else:
        # Sort: real countries first (iso2 not null), then aggregates.
        gap_rows_sorted = sorted(
            gap_rows,
            key=lambda r: (r['is_aggregate'] or False, r['iso2'] or '~'),
        )
        for r in gap_rows_sorted:
            label = r['gacc_label'] or r['iso2']
            agg = " *(aggregate)*" if r['is_aggregate'] else ""
            lines.append(f"#### {r['iso2']} — {label}{agg}")
            lines.append(
                f"- Period: **{r['period']}** | GACC (EUR-converted): {_fmt_eur(r['gacc_eur'])} "
                f"| Eurostat: {_fmt_eur(r['eurostat_eur'])} | Gap: **{_fmt_pct(r['gap_pct'])}**"
            )
            # Phase: per-finding CIF/FOB baseline display. The expected
            # gap is structural (CIF imports vs FOB exports + freight + insurance);
            # showing the per-country baseline from OECD ITIC plus the excess
            # over it makes the editorial framing transparent. Falls back
            # quietly when an older finding doesn't carry the field.
            if r['baseline_pct'] is not None and r['gap_pct'] is not None:
                baseline_pct_f = float(r['baseline_pct'])
                gap_pct_f = float(r['gap_pct'])
                excess_pp = (abs(gap_pct_f) - baseline_pct_f) * 100
                scope_label = r['baseline_scope'] or "global"
                lines.append(
                    f"- **CIF/FOB baseline**: {baseline_pct_f*100:.2f}% "
                    f"({scope_label}); excess over baseline = "
                    f"**{excess_pp:+.1f} pp**"
                )
                if r['baseline_source']:
                    lines.append(
                        f"  - *Baseline source*: {r['baseline_source'][:120]}"
                    )
            # Caveats now read from the finding's actual caveat_codes list,
            # so editorial-framing caveats added in Phase 2 (e.g.
            # `transshipment_hub`) surface correctly. Caveats that apply to
            # essentially every finding by default (multi_partner_sum) are
            # suppressed inline; the top-of-brief note covers them.
            caveats = [c for c in (r['caveat_codes'] or []) if c not in _ALL_UNIVERSAL_CAVEATS]
            lines.append(f"- *Caveats*: {', '.join(caveats) if caveats else '—'}")
            if r['hub_iso2']:
                # One-line transshipment-hub annotation when the partner is in
                # the table — the finding body has the longer version.
                lines.append(
                    f"- ⚓ **Transshipment hub** ({r['hub_iso2']}): "
                    f"{r['hub_notes'][:200] if r['hub_notes'] else '—'}"
                )
            ids = _release_ids_for_observations(cur, list(r['observation_ids'] or []))
            release_ids |= ids
            lines.append(
                f"- *Sources*: {len(ids)} releases (one GACC + one Eurostat per period)"
            )
            lines.append(f"- *Trace*: {_trace_token(r['id'])}")
            lines.append("")

    # z-score movers
    cur.execute(
        """
        SELECT id, detail->>'iso2' AS iso2,
               to_char((detail->>'period')::date, 'YYYY-MM') AS period,
               (detail->>'gap_pct')::numeric AS gap_pct,
               (detail->'baseline'->>'mean')::numeric AS baseline_mean,
               (detail->>'z_score')::numeric AS z
          FROM findings
         WHERE subkind = 'mirror_gap_zscore' AND superseded_at IS NULL
      ORDER BY abs((detail->>'z_score')::numeric) DESC NULLS LAST
         LIMIT 10
        """
    )
    movers = cur.fetchall()
    lines.append("#### Mirror-gap movers (top 10 by |z|)")
    lines.append("")
    lines.append(
        "Each row: a partner whose gap shifted notably vs that partner's own rolling "
        "baseline. High |z| = the gap moved unusually for *this* country, regardless "
        "of where the gap level sits structurally."
    )
    lines.append("")
    if not movers:
        lines.append("*No mover findings yet.*")
        lines.append("")
    else:
        for m in movers:
            lines.append(
                f"- **{m['iso2']} {m['period']}**: gap {_fmt_pct(m['gap_pct'])} vs "
                f"baseline mean {_fmt_pct(m['baseline_mean'])} — "
                f"z = **{float(m['z']):+.2f}** — {_trace_token(m['id'])}"
            )
        lines.append("")

    return _Section(markdown="\n".join(lines), release_ids=release_ids)
