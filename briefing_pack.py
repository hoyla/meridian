"""Markdown briefing-pack export for findings.

Companion to sheets_export.py. Where the spreadsheet is for editorial
scanning, the briefing pack is for narrative reading — and, by design,
for upload to NotebookLM as a one-shot exploration corpus.

Design principles:

1. Deterministic. No LLM. The pack is a structured render of what's in
   the `findings` table, grouped and sorted but otherwise untransformed.
   The LLM framing layer is a separate later step that operates over the
   same finding set.
2. Provenance-first. Every finding line ends with a canonical
   `[finding/{id}]` token (NotebookLM citation handle, future web-UI
   permalink) and a one-line method tag. A `## Sources` appendix at the
   end of the pack lists every release URL underlying the brief, grouped
   by source, with fetch timestamps. A journalist clicking through has
   third-party links one tap away.
3. Same data layer as the Sheets exporter. We re-read findings from
   Postgres, not the rendered XLSX — so the two surfaces are independent
   and any one of them can be wrong without contaminating the other.

CLI: see scrape.py `--briefing-pack`.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

import eurostat

log = logging.getLogger(__name__)

PERMALINK_BASE_ENV = "GACC_PERMALINK_BASE"
DEFAULT_TOP_N = 10

# Caveats that apply universally to every active finding within their relevant
# subkind family (because of analyser defaults / scope choice / inherent
# methodology, rather than something unusual about a specific finding).
# Suppressing them inline keeps the per-finding caveat list focused on signal
# — what's *unusual* about THIS finding — while the top-of-brief
# "Universal caveats" section covers them once with full definitions.
#
# Membership verified empirically (Phase 6.2): each code below fires on 100%
# of the active findings in its applicable subkinds (queried 2026-05-10
# against the live DB). If a code stops being universal — e.g. because we
# add a scope where it no longer applies — drop it from this set so the per-
# finding display surfaces the variation again.
#
# Per-finding-informative caveats (kept inline): partial_window, low_base_effect,
# low_baseline_n, low_kg_coverage, transshipment_hub. These signal something
# specific about the individual finding and matter at glance.
SUPPRESSED_INLINE_CAVEATS = frozenset({
    "cif_fob",
    "classification_drift",
    "cn8_revision",
    "currency_timing",
    "eurostat_stat_procedure_mix",
    "multi_partner_sum",
    "general_vs_special_trade",
    "transshipment",
    "cross_source_sum",
    "aggregate_composition_drift",
    "llm_drafted",
})


def _construct_chinese_source_url(english_url: str | None) -> str | None:
    """Construct the Chinese-language equivalent of a GACC English release URL.
    GACC keeps the same Statics/<UUID>.html path on both hosts; only the
    subdomain changes. Returns None if the URL doesn't match the expected
    GACC English pattern (so callers can skip the link cleanly)."""
    if not english_url:
        return None
    en_host = "english.customs.gov.cn"
    cn_host = "www.customs.gov.cn"
    if en_host not in english_url:
        return None
    return english_url.replace(en_host, cn_host)


@dataclass
class _Section:
    """Rendered section + the set of release_ids it touched (for the appendix)."""
    markdown: str
    release_ids: set[int] = field(default_factory=set)


# =============================================================================
# DB helpers
# =============================================================================


def _conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _trace_token(finding_id: int) -> str:
    """Stable citation token for NotebookLM. If GACC_PERMALINK_BASE is
    set, render as a Markdown link; otherwise emit the bare token. The
    bare token still works as a citation handle — NotebookLM picks up
    `finding/123` strings as searchable references."""
    base = os.environ.get(PERMALINK_BASE_ENV, "").rstrip("/")
    if base:
        return f"[finding/{finding_id}]({base}/finding/{finding_id})"
    return f"`finding/{finding_id}`"


def _fmt_eur(v: Any) -> str:
    if v is None:
        return "—"
    n = float(v)
    if abs(n) >= 1e9:
        return f"€{n / 1e9:.2f}B"
    if abs(n) >= 1e6:
        return f"€{n / 1e6:.1f}M"
    if abs(n) >= 1e3:
        return f"€{n / 1e3:.1f}k"
    return f"€{n:.0f}"


def _fmt_pct(v: Any) -> str:
    if v is None:
        return "—"
    return f"{float(v) * 100:+.1f}%"


def _fmt_kg(v: Any) -> str:
    if v is None:
        return "—"
    n = float(v)
    if abs(n) >= 1e9:
        return f"{n / 1e9:.2f}B kg"
    if abs(n) >= 1e6:
        return f"{n / 1e6:.1f}M kg"
    if abs(n) >= 1e3:
        return f"{n / 1e3:.1f}k kg"
    return f"{n:.0f} kg"


def _release_ids_for_window(cur, start: date, end: date) -> set[int]:
    """Eurostat releases whose period falls in [start, end]. Used to
    populate the sources appendix for window-traced findings (hs_group_yoy
    and trajectories) — these don't have observation_ids[] so we go via
    the period range that fed them."""
    cur.execute(
        "SELECT id FROM releases WHERE source = 'eurostat' "
        "AND period BETWEEN %s AND %s",
        (start, end),
    )
    return {r[0] for r in cur.fetchall()}


def _release_ids_for_observations(cur, obs_ids: list[int]) -> set[int]:
    if not obs_ids:
        return set()
    cur.execute(
        "SELECT DISTINCT release_id FROM observations WHERE id = ANY(%s)",
        (obs_ids,),
    )
    return {r[0] for r in cur.fetchall()}


# =============================================================================
# Section builders
# =============================================================================


def _section_headline(
    cur, companion_filename: str | None = None, scope_label: str | None = None,
) -> _Section:
    """Top-of-pack scene-setting: schema version, period coverage, finding counts."""
    cur.execute(
        "SELECT source, MIN(period) AS lo, MAX(period) AS hi, COUNT(*) AS n "
        "FROM releases GROUP BY source ORDER BY source"
    )
    sources = cur.fetchall()
    # Active (un-superseded) findings only — superseded rows are revision
    # history, queryable but not part of the current picture.
    cur.execute(
        "SELECT subkind, COUNT(*) FROM findings "
        "WHERE kind = 'anomaly' AND superseded_at IS NULL "
        "GROUP BY subkind ORDER BY subkind"
    )
    counts = cur.fetchall()

    lines: list[str] = []
    lines.append(f"# GACC × Eurostat trade findings")
    lines.append(f"*Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} from the `findings` table.*")
    if scope_label:
        lines.append(f"*Scope: **{scope_label}**.*")
    lines.append("")
    lines.append("This document is a deterministic render of the underlying findings — no LLM in the loop. ")
    lines.append("Each finding line ends with a citation token (e.g. `finding/123`) which is a stable handle ")
    lines.append("into the project's database. A **Sources** appendix at the end lists every third-party ")
    lines.append("URL the findings rest on, with fetch timestamps.")
    lines.append("")
    leads_ref = f"`{companion_filename}`" if companion_filename else "`leads.md`"
    lines.append("## In this export folder")
    lines.append("")
    lines.append(
        "This is one of three artefacts generated together from the same DB "
        "snapshot. All three share the same finding IDs; switch between them "
        "depending on what you need."
    )
    lines.append("")
    lines.append("- **`findings.md`** — deterministic Markdown findings (this document). NotebookLM-ready.")
    lines.append(
        f"- **{leads_ref}** — LLM-scaffolded investigation leads. One per HS group: "
        "anomaly summary, 2-3 picked hypotheses from a curated catalog, "
        "corroboration steps. Kept separate so a downstream LLM tool reasoning "
        "over this findings document sees raw data, not another LLM's "
        "interpretation."
    )
    lines.append(
        "- **`data.xlsx`** — 8-tab spreadsheet for data journalists. Same "
        "findings, long-format with filterable scope/flow columns, "
        "predictability badges, CIF/FOB baseline expansion. Also LLM-free."
    )
    lines.append("")
    lines.append("## Scope notes")
    lines.append("")
    lines.append("- **Eurostat partners summed**: CN + HK + MO (the editorially-correct \"Chinese trade\" ")
    lines.append("  envelope including the two Special Administrative Regions). Pass `--eurostat-partners CN` ")
    lines.append("  to get a narrower direct-China-only view (matches Soapbox / Merics single-partner figures).")
    lines.append("- **EU-27 = EU-27.** Eurostat reporter rows from GB (pre-2021) are excluded at all times so ")
    lines.append("  EU-27 totals are consistent through the Brexit transition. UK trade is captured ")
    lines.append("  separately via HMRC ingest (Phase 6.1) and surfaced under the **UK** comparison scope.")
    lines.append("- **Comparison scopes**: each hs-group section renders three views — EU-27 (Eurostat), UK ")
    lines.append("  (HMRC), and EU-27 + UK combined. The combined view carries a `cross_source_sum` caveat ")
    lines.append("  reflecting the methodological non-comparability of summing across two statistical agencies.")
    lines.append("")
    lines.append("Standard methodological caveats that apply to every finding in their respective sections ")
    lines.append("are explained once in the **Universal caveats** block below and suppressed from per-finding ")
    lines.append("caveat lists, so those lists highlight only what's *unusual* about each finding.")
    lines.append("")
    lines.append("## Period coverage")
    for s in sources:
        lines.append(f"- **{s['source']}**: {s['lo']} → {s['hi']} ({s['n']} releases)")
    lines.append("")
    lines.append("## Findings included")
    for k, n in counts:
        lines.append(f"- {k}: {n}")
    lines.append("")
    return _Section(markdown="\n".join(lines))


def _section_universal_caveats(cur) -> _Section:
    """Top-of-brief explainer for caveats that fire on essentially every active
    finding (within their applicable subkind family). Reads the canonical
    summary text from the `caveats` table so the explainer stays in sync with
    the schema. Each entry shows where the caveat applies (per subkind)
    based on a live count of how many active findings carry it; if a code
    in SUPPRESSED_INLINE_CAVEATS turns out not to be universal anymore,
    the breakdown will show the gap and you'll know to reconsider."""
    codes = sorted(SUPPRESSED_INLINE_CAVEATS - {"llm_drafted"})
    cur.execute(
        "SELECT code, summary, detail FROM caveats WHERE code = ANY(%s) ORDER BY code",
        (codes,),
    )
    rows = cur.fetchall()
    found_codes = {r["code"] for r in rows}

    lines: list[str] = []
    lines.append("## Universal caveats")
    lines.append("")
    lines.append(
        "These methodological caveats apply by default to every finding in "
        "the sections they cover. They are real limitations on the underlying "
        "data, but they don't differentiate one finding from another — so the "
        "per-finding caveat lists below suppress them to keep the focus on "
        "what's *unusual* about each finding. The full definitions live here."
    )
    lines.append("")
    for r in rows:
        lines.append(f"**`{r['code']}` — {r['summary']}**")
        lines.append("")
        if r["detail"]:
            lines.append(r["detail"])
            lines.append("")
    # llm_drafted is suppressed inline because the section header itself
    # already says "LLM-scaffolded". Mention it briefly so a reader who sees
    # the SUPPRESSED set in code knows where it is documented.
    lines.append(
        "**`llm_drafted`** — applied to every finding in the *Investigation "
        "leads* section. Suppressed inline because the section header already "
        "communicates editorial origin."
    )
    lines.append("")
    # Surface any code whose schema row is missing — usually means a typo or
    # a code added to the analysers without a matching caveats-table entry.
    missing = sorted(c for c in codes if c not in found_codes)
    if missing:
        lines.append(f"*Note: missing `caveats` table definition for: {', '.join(missing)}.*")
        lines.append("")
    return _Section(markdown="\n".join(lines))


def _section_llm_narratives(cur) -> _Section:
    """Lead scaffolds from llm_framing — for each HS group with a current
    `narrative_hs_group` finding, render the anomaly summary, picked
    hypotheses, and corroboration steps. Suppressed entirely when there
    are no leads (a journalist who hasn't run the framing pass still gets
    a clean deterministic-only brief).

    Each lead carries an `llm_drafted` caveat plus the union of caveats on
    its underlying findings. We surface those inline so the editorial
    framing is honest about its own provenance.

    For backward compatibility this still reads any v1 prose-narrative
    findings via the body field — they render as a single paragraph
    block. New v2 lead-scaffold findings render with the structured
    breakdown sourced from `detail.lead_scaffold`.
    """
    cur.execute(
        """
        SELECT f.id, f.body, f.detail, f.last_confirmed_at
          FROM findings f
         WHERE f.subkind = 'narrative_hs_group'
           AND f.superseded_at IS NULL
      ORDER BY f.detail->'group'->>'name'
        """
    )
    rows = cur.fetchall()

    lines: list[str] = []
    if not rows:
        # Nothing to render — caller treats empty markdown as "skip section".
        return _Section(markdown="")

    lines.append("## Investigation leads")
    lines.append("")
    lines.append(
        f"LLM-scaffolded investigation starts for each HS group, ordered by "
        f"group name. Each lead has three parts: a one-line anomaly "
        f"summary (numerically verified against the underlying findings), "
        f"2-3 candidate hypotheses picked from a curated catalog of "
        f"standard causes for China-EU/UK trade movements, and a list of "
        f"concrete corroboration steps a journalist can run to test the "
        f"hypotheses. The `llm_drafted` caveat tags every block below as "
        f"editorial origin; underlying caveats (low_base, partial_window, "
        f"transshipment_hub, cn8_revision, low_kg_coverage, etc.) "
        f"propagate from the source findings. Trace ids point to the lead "
        f"finding, not the underlying — query "
        f"`findings.detail->>'underlying_finding_ids'` to walk the chain."
    )
    lines.append("")
    for r in rows:
        detail = r["detail"]
        group_name = detail.get("group", {}).get("name", "—")
        caveats = detail.get("caveat_codes") or []
        visible_caveats = [c for c in caveats if c not in SUPPRESSED_INLINE_CAVEATS]
        lines.append(f"### {group_name}")
        lines.append("")
        scaffold = detail.get("lead_scaffold")
        if isinstance(scaffold, dict) and scaffold.get("anomaly_summary"):
            lines.append(f"**Anomaly:** {scaffold['anomaly_summary']}")
            lines.append("")
            hyps = scaffold.get("hypotheses") or []
            if hyps:
                lines.append("**Possible causes:**")
                lines.append("")
                for h in hyps:
                    label = h.get("label") or h.get("id", "—")
                    rationale = h.get("rationale", "")
                    lines.append(f"- *{label}* — {rationale}")
                lines.append("")
            steps = scaffold.get("corroboration_steps") or []
            if steps:
                lines.append("**Corroboration steps:**")
                lines.append("")
                for s in steps:
                    lines.append(f"- {s}")
                lines.append("")
        else:
            # v1 (or any other shape) — fall back to the body field
            lines.append(r["body"])
            lines.append("")
        if visible_caveats:
            lines.append(f"*Caveats from underlying findings: {', '.join(visible_caveats)}*")
        lines.append(
            f"*Underlying findings: "
            f"{', '.join(str(i) for i in detail.get('underlying_finding_ids', []))} "
            f"— Trace: {_trace_token(r['id'])}*"
        )
        lines.append("")

    return _Section(markdown="\n".join(lines))


_SCOPE_LABEL = {
    "eu_27": "EU-27",
    "uk": "UK",
    "eu_27_plus_uk": "EU-27 + UK (combined)",
}
_SCOPE_SUBKIND_SUFFIX = {"eu_27": "", "uk": "_uk", "eu_27_plus_uk": "_combined"}

# Phase 6 transparency annotations.
#
# Predictability: for each HS group, pair its `hs_group_yoy*` finding at
# the latest anchor period (T) against the same (group, subkind) finding
# 6 months earlier (T-6) and ask: did the YoY signal age well? A group
# whose multiple (scope, flow) permutations all stayed persistent is
# giving robust signals; a group where every permutation flipped is
# noise-dominated at the YoY level. Mirrors the methodology in
# scripts/out_of_sample_backtest.py.
#
# Threshold fragility: a finding whose smaller-of-(curr,prior) sits
# within 1.5× the low_base threshold is in the "flip zone" identified
# by scripts/sensitivity_sweep.py — small threshold movements would
# change its low_base classification. Surface that fragility per-finding.
PREDICTABILITY_LOOKBACK_MONTHS = 6
PREDICTABILITY_SHIFT_PP = 5.0    # |yoy_T - yoy_{T-6}| pp threshold
PREDICTABILITY_GREEN_PCT = 0.67  # ≥67% persistent → 🟢
PREDICTABILITY_YELLOW_PCT = 0.33  # 33-67% → 🟡; <33% → 🔴
THRESHOLD_FRAGILITY_RATIO = 1.5  # within 1.5× of threshold → flag


def _compute_predictability_per_group(cur) -> dict[str, tuple[str, float, int]]:
    """Returns {group_name: (badge, persistence_pct, n_pairs)}.
    Empty if no T-6 pairs available (e.g. fresh DB)."""
    cur.execute(
        """
        SELECT MAX((detail->'windows'->>'current_end')::date)
          FROM findings
         WHERE subkind LIKE 'hs_group_yoy%%'
           AND superseded_at IS NULL
        """
    )
    period_t = cur.fetchone()[0]
    if period_t is None:
        return {}
    # Compute T-6
    m = period_t.month - PREDICTABILITY_LOOKBACK_MONTHS
    y = period_t.year + (m - 1) // 12
    m = ((m - 1) % 12) + 1
    period_t6 = date(y, m, 1)

    cur.execute(
        """
        SELECT subkind,
               detail->'group'->>'name'                       AS group_name,
               (detail->'windows'->>'current_end')::date      AS period,
               (detail->'totals'->>'yoy_pct')::float          AS yoy_pct
          FROM findings
         WHERE subkind LIKE 'hs_group_yoy%%'
           AND superseded_at IS NULL
           AND (detail->'windows'->>'current_end')::date = ANY(%s)
        """,
        ([period_t, period_t6],),
    )
    rows = cur.fetchall()
    # Pair (group, subkind) → {t: yoy, t6: yoy}
    pairs: dict[tuple[str, str], dict[str, float]] = {}
    for sk, gn, p, yoy in rows:
        if yoy is None:
            continue
        which = "t" if p == period_t else "t6"
        pairs.setdefault((gn, sk), {})[which] = float(yoy)
    # Per-group: count permutations + persistent permutations
    by_group: dict[str, list[bool]] = {}
    for (gn, _sk), parts in pairs.items():
        if "t" not in parts or "t6" not in parts:
            continue
        yoy_t = parts["t"]
        yoy_t6 = parts["t6"]
        sign_flip = (yoy_t > 0) != (yoy_t6 > 0) and (yoy_t * yoy_t6 != 0)
        big_shift = abs((yoy_t - yoy_t6) * 100) >= PREDICTABILITY_SHIFT_PP
        persistent = not sign_flip and not big_shift
        by_group.setdefault(gn, []).append(persistent)

    out: dict[str, tuple[str, float, int]] = {}
    for gn, persists in by_group.items():
        n = len(persists)
        pct = sum(persists) / n if n else 0.0
        if pct >= PREDICTABILITY_GREEN_PCT:
            badge = "🟢"
        elif pct >= PREDICTABILITY_YELLOW_PCT:
            badge = "🟡"
        else:
            badge = "🔴"
        out[gn] = (badge, pct, n)
    return out


def is_threshold_fragile(curr_eur: Any, prior_eur: Any, threshold_eur: Any) -> bool:
    """Return True if smaller-of-(curr, prior) sits within
    THRESHOLD_FRAGILITY_RATIO of `threshold_eur` (above OR below).
    Shared with sheets_export so both the brief and the spreadsheet use
    the same definition of "near the low_base threshold"."""
    if curr_eur is None or prior_eur is None or threshold_eur is None:
        return False
    smaller = min(float(curr_eur), float(prior_eur))
    thr = float(threshold_eur)
    if thr <= 0:
        return False
    return smaller < thr * THRESHOLD_FRAGILITY_RATIO and smaller > thr / THRESHOLD_FRAGILITY_RATIO


def _threshold_fragility_annotation(curr_eur: Any, prior_eur: Any, threshold_eur: Any) -> str | None:
    """If the smaller-of-(curr, prior) is within 1.5× the threshold (above
    OR below it), return a markdown annotation; else None.

    A finding at €48M (just below €50M threshold) is low_base; a finding
    at €52M (just above) is not. Both are fragile to a small threshold
    move. The annotation surfaces that without making editorial claims
    about which way the classification "should" go.
    """
    if not is_threshold_fragile(curr_eur, prior_eur, threshold_eur):
        return None
    smaller = min(float(curr_eur), float(prior_eur))
    thr = float(threshold_eur)
    return (
        f"- ⚖️ **Near low-base threshold** ({_fmt_eur(smaller)} vs "
        f"€{thr/1e6:.0f}M threshold) — classification is fragile to "
        "small threshold changes; see "
        "`dev_notes/sensitivity-sweep-2026-05-10.md`."
    )


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
    direction = "Imports (CN→reporter)" if flow == 1 else "Exports (reporter→CN)"
    flow_label = f"{scope_label} {direction}"
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
                 (detail->'totals'->>'current_12mo_eur')::numeric AS current_eur,
                 (detail->'totals'->>'prior_12mo_eur')::numeric AS prior_eur,
                 (detail->'totals'->>'yoy_pct')::numeric AS yoy_pct,
                 (detail->'totals'->>'current_12mo_kg')::numeric AS current_kg,
                 (detail->'totals'->>'yoy_pct_kg')::numeric AS yoy_pct_kg,
                 (detail->'totals'->>'unit_price_pct_change')::numeric AS unit_price_pct,
                 (detail->'totals'->>'low_base')::boolean AS low_base,
                 (detail->'totals'->>'low_base_threshold_eur')::numeric AS low_base_threshold,
                 detail->'method_query'->'hs_patterns' AS hs_patterns,
                 detail->'method_query'->'partners' AS partners_used
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
    lines.append(f"## {flow_label} — top {len(rows)} movers (latest 12mo YoY)")
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
        lines.append(f"### {r['group_name']}{badge_str}")
        if pred is not None:
            badge, pct, n = pred
            label = (
                "persistent" if badge == "🟢"
                else "noisy" if badge == "🟡"
                else "volatile"
            )
            lines.append(
                f"- *YoY predictability* ({badge} {label}): "
                f"{int(pct*100)}% of {n} (scope, flow) permutations stayed "
                f"on the same direction with shift <{int(PREDICTABILITY_SHIFT_PP)}pp "
                f"vs 6 months ago. "
                + ("Headline % is robust." if badge == "🟢"
                   else "Lean on trajectory shape; hedge any % quoted from this group."
                   if badge == "🔴"
                   else "Treat the headline % with caution.")
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
        if r['low_base']:
            lines.append(
                "- ⚠️ **Low-base flag**: prior or current 12mo total below the €50M "
                "threshold. Verify absolute figures before quoting the percentage."
            )
        # Phase: threshold-fragility annotation (orthogonal to the low_base
        # flag — a finding can be flagged AND fragile, or fragile-but-not-
        # flagged because it's just above the threshold).
        fragility = _threshold_fragility_annotation(
            r['current_eur'], r['prior_eur'], r['low_base_threshold'],
        )
        if fragility:
            lines.append(fragility)
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


def _decomposition_label(yoy_eur: Any, yoy_kg: Any) -> str:
    """Mirrors the volume-vs-price decomposition in anomalies.py."""
    if yoy_eur is None or yoy_kg is None or float(yoy_eur) == 0:
        return ""
    share = float(yoy_kg) / float(yoy_eur)
    return "volume-driven" if abs(share) > 0.5 else "price-driven"


def _section_trajectories(cur, comparison_scope: str = "eu_27") -> _Section:
    """Trajectory findings grouped by shape — narrative-rich pattern bucket.
    Phase 6.1e: scoped to one of EU-27 / UK / combined."""
    scope_suffix = _SCOPE_SUBKIND_SUFFIX[comparison_scope]
    scope_label = _SCOPE_LABEL[comparison_scope]
    subkind_imp = f"hs_group_trajectory{scope_suffix}"
    subkind_exp = f"hs_group_trajectory{scope_suffix}_export"
    cur.execute(
        """
        SELECT id, subkind,
               detail->'group'->>'name' AS group_name,
               detail->>'shape' AS shape,
               detail->>'shape_label' AS shape_label,
               (detail->'features'->>'last_yoy')::numeric AS last_yoy,
               (detail->'features'->>'max_yoy')::numeric AS peak,
               (detail->'features'->>'min_yoy')::numeric AS trough,
               (detail->'features'->>'first_period')::date AS first_period,
               (detail->'features'->>'last_period')::date AS last_period,
               (detail->'features'->>'low_base_majority')::boolean AS low_base_majority
          FROM findings
         WHERE subkind IN (%s, %s)
           AND superseded_at IS NULL
      ORDER BY detail->>'shape', subkind, detail->'group'->>'name'
        """,
        (subkind_imp, subkind_exp),
    )
    rows = cur.fetchall()

    release_ids: set[int] = set()
    lines: list[str] = []
    if not rows:
        return _Section(markdown="")
    lines.append(f"## {scope_label} trajectory shapes")
    lines.append("")
    lines.append(
        "Each HS group's rolling-12mo YoY series classified by shape. "
        "Editorially the shape vocabulary matters: `dip_recovery` and `inverse_u_peak` "
        "are narrative-rich (a comeback or a peak-and-fall); `falling`/`rising` are "
        "directional; `volatile` flags series the classifier didn't fit confidently."
    )
    lines.append("")
    if not rows:
        lines.append("*No trajectory findings yet.*")
        lines.append("")
        return _Section(markdown="\n".join(lines))

    by_shape: dict[str, list[psycopg2.extras.DictRow]] = {}
    for r in rows:
        by_shape.setdefault(r['shape'], []).append(r)

    # Order shapes editorially: narrative-rich first, then directional, then volatile/flat.
    shape_order = [
        "dip_recovery", "failed_recovery", "inverse_u_peak", "u_recovery",
        "rising_accelerating", "rising_decelerating", "rising",
        "falling_decelerating", "falling_accelerating", "falling",
        "volatile", "flat",
    ]
    seen_shapes = set()
    for shape in shape_order + sorted(by_shape.keys()):
        if shape in seen_shapes or shape not in by_shape:
            continue
        seen_shapes.add(shape)
        shape_label = by_shape[shape][0]['shape_label'] or shape
        lines.append(f"### {shape} — *{shape_label}*")
        for r in by_shape[shape]:
            flow = "imports" if r['subkind'] == 'hs_group_trajectory' else "exports"
            low_base_marker = " ⚠️ low-base" if r['low_base_majority'] else ""
            lines.append(
                f"- **{r['group_name']}** ({flow}): "
                f"latest YoY {_fmt_pct(r['last_yoy'])}, "
                f"peak {_fmt_pct(r['peak'])}, trough {_fmt_pct(r['trough'])}"
                f"{low_base_marker} — {_trace_token(r['id'])}"
            )
            # Window: features.first_period (first 12mo-window end) — 12mo back covers
            # the earliest observation period that fed this trajectory.
            if r['first_period'] and r['last_period']:
                window_start = (r['first_period'].replace(day=1) - timedelta(days=1)).replace(day=1)
                # Step back 11 more months to cover the full 12mo prior window for the first point.
                ws = r['first_period']
                for _ in range(12):
                    ws = (ws.replace(day=1) - timedelta(days=1)).replace(day=1)
                ids = _release_ids_for_window(cur, ws, r['last_period'])
                release_ids |= ids
        lines.append("")

    return _Section(markdown="\n".join(lines), release_ids=release_ids)


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
    lines.append("## Mirror-trade gaps (latest per partner)")
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
            lines.append(f"### {r['iso2']} — {label}{agg}")
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
            caveats = [c for c in (r['caveat_codes'] or []) if c not in SUPPRESSED_INLINE_CAVEATS]
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
    lines.append("### Mirror-gap movers (top 10 by |z|)")
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


def _section_low_base(cur) -> _Section:
    """Editorial review queue: every hs_group_yoy*-flavoured finding flagged low_base."""
    cur.execute(
        """
        SELECT id, subkind,
               detail->'group'->>'name' AS group_name,
               to_char((detail->'windows'->>'current_end')::date, 'YYYY-MM') AS period,
               (detail->'totals'->>'current_12mo_eur')::numeric AS current_eur,
               (detail->'totals'->>'prior_12mo_eur')::numeric AS prior_eur,
               (detail->'totals'->>'yoy_pct')::numeric AS yoy_pct,
               (detail->'totals'->>'low_base_threshold_eur')::numeric AS threshold
          FROM findings
         WHERE subkind IN ('hs_group_yoy', 'hs_group_yoy_export')
           AND (detail->'totals'->>'low_base')::boolean = true
           AND superseded_at IS NULL
      ORDER BY abs((detail->'totals'->>'yoy_pct')::numeric) DESC NULLS LAST
        """
    )
    rows = cur.fetchall()

    lines: list[str] = []
    if not rows:
        # Suppress the section entirely when there's nothing to review.
        return _Section(markdown="")

    lines.append("## Low-base review queue")
    lines.append("")
    lines.append(
        f"{len(rows)} findings rest on a denominator below the low-base threshold "
        f"(€50M for either current or prior 12mo total). Verify the absolute figures "
        f"before quoting any percentage from these — small bases can exaggerate."
    )
    lines.append("")
    for r in rows:
        flow = "imports" if r['subkind'] == 'hs_group_yoy' else "exports"
        lines.append(
            f"- **{r['group_name']}** ({flow}, {r['period']}): "
            f"{_fmt_pct(r['yoy_pct'])}, "
            f"prior {_fmt_eur(r['prior_eur'])} → current {_fmt_eur(r['current_eur'])} — "
            f"{_trace_token(r['id'])}"
        )
    lines.append("")
    return _Section(markdown="\n".join(lines))


def _section_sources_appendix(cur, release_ids: set[int]) -> _Section:
    """Final appendix listing every release URL underlying the brief.

    Eurostat: synthesises the bulk-file URL via eurostat.bulk_file_url, since
    the canonical URL is deterministic per period (and we deliberately don't
    store the 44 MB 7z bytes). GACC: the actual source_url from the release
    row, plus the fetched_at from source_snapshots so a journalist knows
    the page state we read."""
    lines: list[str] = []
    lines.append("## Sources")
    lines.append("")
    lines.append(
        "Every release whose data fed any finding above. Eurostat URLs are "
        "the deterministic monthly bulk-file URLs; the raw CSV rows we extracted "
        "from each are preserved verbatim in the project DB (`eurostat_raw_rows`). "
        "GACC URLs are the actual customs.gov.cn pages we scraped — the page "
        "bytes are stored in `source_snapshots` so the read is reproducible "
        "even if the page is later revised or removed."
    )
    lines.append("")
    if not release_ids:
        lines.append("*No releases referenced.*")
        lines.append("")
        return _Section(markdown="\n".join(lines))

    cur.execute(
        """
        SELECT r.id, r.source, r.source_url, r.period, r.first_seen_at, r.last_seen_at,
               r.section_number, r.currency, r.release_kind,
               (SELECT MAX(s.fetched_at) FROM source_snapshots s
                  JOIN scrape_runs sr ON sr.id = s.scrape_run_id
                 WHERE s.url = r.source_url) AS snapshot_fetched_at
          FROM releases r
         WHERE r.id = ANY(%s)
      ORDER BY r.source, r.period DESC, r.id
        """,
        (sorted(release_ids),),
    )
    rels = cur.fetchall()

    by_source: dict[str, list[psycopg2.extras.DictRow]] = {}
    for r in rels:
        by_source.setdefault(r['source'], []).append(r)

    if 'eurostat' in by_source:
        lines.append("### Eurostat monthly bulk files")
        lines.append("")
        lines.append(
            "*Eurostat occasionally re-publishes corrected files at the same URL. "
            "The `as_of` timestamp is when we fetched and parsed the file into "
            "`eurostat_raw_rows` — that is the ground truth we used.*"
        )
        lines.append("")
        for r in by_source['eurostat']:
            url = eurostat.bulk_file_url(r['period'])
            as_of = r['first_seen_at'].strftime('%Y-%m-%d') if r['first_seen_at'] else '—'
            lines.append(
                f"- **{r['period'].strftime('%Y-%m')}** — as_of {as_of} — <{url}>"
            )
        lines.append("")

    if 'gacc' in by_source:
        lines.append("### GACC release pages")
        lines.append("")
        lines.append(
            "*Page bytes preserved in `source_snapshots`. The `fetched_at` "
            "timestamp is when we last successfully read the page; the EN "
            "link below points to the live page. The CN link is the "
            "constructed Chinese-language equivalent (see note below).*"
        )
        lines.append("")
        for r in by_source['gacc']:
            ts = r['snapshot_fetched_at'] or r['last_seen_at']
            ts_str = ts.strftime('%Y-%m-%d') if ts else '—'
            kind_bits = " ".join(filter(None, [
                f"section {r['section_number']}" if r['section_number'] else None,
                r['currency'],
                r['release_kind'],
            ]))
            chinese_url = _construct_chinese_source_url(r['source_url'])
            cn_link = f" / CN: <{chinese_url}>" if chinese_url else ""
            lines.append(
                f"- **{r['period'].strftime('%Y-%m')}** "
                f"({kind_bits}) — fetched {ts_str} — EN: <{r['source_url']}>{cn_link}"
            )
        lines.append("")

    lines.append("### Known gaps in source coverage")
    lines.append("")
    lines.append(
        "- The `CN:` Chinese-language URLs above are *constructed* from the "
        "English URL by host substitution (`english.customs.gov.cn` → "
        "`www.customs.gov.cn`); GACC keeps the same `Statics/<UUID>.html` "
        "path on both. We don't verify these links automatically — the "
        "Chinese site fronts a JavaScript anti-bot challenge that blocks "
        "headless `curl` — but a journalist clicking through in a real "
        "browser will land on the Chinese-language version of the same "
        "release. Useful for in-language verification or when the English "
        "translation drops a nuance."
    )
    lines.append(
        "- Caveat codes referenced inline (e.g. `cif_fob`, `low_base_effect`) "
        "have full definitions in the project's `caveats` table."
    )
    lines.append("")
    return _Section(markdown="\n".join(lines))


# =============================================================================
# Top-level orchestrator
# =============================================================================


def _section_diff_since_last_brief(cur) -> _Section:
    """Phase 6.8: render 'what changed since the previous brief'.
    Reads brief_runs to find the most recent prior generated_at;
    queries findings created or superseded since then. Returns empty
    markdown if there's no previous brief (first-ever run on a fresh
    DB) or if nothing materially changed.

    Editorial threshold: a YoY shift of > 5pp is "material"; a
    direction flip is highlighted separately. New findings are listed
    by subkind count rather than per-row to keep the section terse —
    the journalist can drill into the per-finding sections below
    once they know what's new."""
    cur.execute("SELECT MAX(generated_at) FROM brief_runs")
    row = cur.fetchone()
    prev_at = row[0] if row else None
    if prev_at is None:
        return _Section(markdown="")

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

    # Findings superseded since previous brief — pair the old (superseded)
    # row with its new replacement so we can compute the YoY shift.
    cur.execute(
        """
        SELECT
            old.id AS old_id, new.id AS new_id, old.subkind,
            old.detail->'group'->>'name' AS group_name,
            old.detail->'windows'->>'current_end' AS window_end,
            (old.detail->'totals'->>'yoy_pct')::numeric AS old_yoy,
            (new.detail->'totals'->>'yoy_pct')::numeric AS new_yoy
          FROM findings old
          JOIN findings new ON old.superseded_by_finding_id = new.id
         WHERE old.superseded_at > %s
           AND old.subkind <> 'narrative_hs_group'
           AND old.detail->'totals'->>'yoy_pct' IS NOT NULL
           AND new.detail->'totals'->>'yoy_pct' IS NOT NULL
        """,
        (prev_at,),
    )
    significant = []
    for r in cur.fetchall():
        old_yoy = float(r["old_yoy"])
        new_yoy = float(r["new_yoy"])
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

    if not new_by_subkind and not significant:
        return _Section(markdown="")

    lines: list[str] = []
    lines.append(f"## Changes since the previous export")
    lines.append("")
    lines.append(
        f"*Previous findings export generated {prev_at:%Y-%m-%d %H:%M %Z}. The "
        f"lists below reflect findings that have been added or whose value has "
        f"materially shifted since then. New findings without a comparable "
        f"predecessor — e.g. a new HS group, a new period anchor — appear "
        f"under \"New findings\".*"
    )
    lines.append("")

    if significant:
        # Direction flips first, then size of shift.
        significant.sort(key=lambda s: (-int(s["direction_flipped"]), -abs(s["shift_pp"])))
        lines.append(f"### Material YoY shifts ({len(significant)})")
        lines.append("")
        lines.append(
            "*A shift > 5 percentage points between the previous export's value "
            "and the current value. Direction flips (growth ↔ decline) are "
            "highlighted with 🔄.*"
        )
        lines.append("")
        for s in significant[:30]:  # cap to top 30 — supersede chain is queryable
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
        total_new = sum(n for _, n in new_by_subkind)
        lines.append(f"### New findings ({total_new})")
        lines.append("")
        for subkind, n in new_by_subkind:
            lines.append(f"- {n} new `{subkind}`")
        lines.append("")

    return _Section(markdown="\n".join(lines))


def _section_about_findings() -> _Section:
    """Endnote explaining what `finding/N` citation tokens mean and how
    to look one up. Identical text in both the brief and the leads doc
    (called from both `render()` and `render_leads()`).

    Kept terse here because the deeper data-model + per-subkind detail
    lives in `docs/architecture.md` and `docs/methodology.md`. This
    endnote is just the bridge from a cited number to those docs.
    """
    lines: list[str] = []
    lines.append("---")
    lines.append("")
    lines.append("## About the `finding/N` citations")
    lines.append("")
    lines.append(
        "Every claim in this document ends with a citation token like "
        "`finding/12345`. Each refers to a row in the project's `findings` "
        "table — one per detected anomaly, carrying a JSONB `detail` "
        "blob with the totals, window dates, observation IDs, caveat "
        "codes, and method version that produced the claim."
    )
    lines.append("")
    lines.append("**Subkinds you'll see cited:**")
    lines.append("")
    lines.append(
        "- `mirror_gap`, `mirror_gap_zscore` — China-vs-EU/UK customs "
        "comparison and z-score movers."
    )
    lines.append(
        "- `hs_group_yoy*` — rolling 12-month YoY for an HS group, "
        "scoped to one of `eu_27` / `uk` / `eu_27_plus_uk`. Suffixes "
        "encode flow + scope (e.g. `_uk_export`)."
    )
    lines.append(
        "- `hs_group_trajectory*` — 24-month shape classification for "
        "the same series (12-shape vocabulary)."
    )
    lines.append(
        "- `narrative_hs_group` — LLM-scaffolded leads (companion "
        "doc only). Catalogued in `docs/methodology.md` §1."
    )
    lines.append("")
    lines.append(
        "**Stability across revisions.** A citation always points at "
        "a *specific* row. When the analyser re-runs and concludes a "
        "different value, it inserts a new row and stamps the old one "
        "with `superseded_at` + `superseded_by_finding_id`. So "
        "`finding/12345` remains a reproducible reference to the exact "
        "claim made in this document, even after the underlying numbers "
        "later move."
    )
    lines.append("")
    lines.append(
        "**Looking one up today.** Direct DB query: "
        "`SELECT * FROM findings WHERE id = 12345;` against the project's "
        "Postgres instance. A hosted finding viewer is on the roadmap "
        "(set `GACC_PERMALINK_BASE` to render citations as Markdown "
        "links instead of bare tokens once it exists)."
    )
    lines.append("")
    lines.append(
        "**For deeper context**, see "
        "[`docs/methodology.md`](../docs/methodology.md) (what each "
        "subkind measures and when to quote it) and "
        "[`docs/architecture.md`](../docs/architecture.md) (the full "
        "raw_rows → observations → findings data flow)."
    )
    lines.append("")
    return _Section(markdown="\n".join(lines))


def _record_brief_run(out_path: str | None, top_n: int) -> None:
    """Insert a row into brief_runs after a successful brief generation.
    Called by export() — render() doesn't write since callers may render
    for non-archival purposes (preview, test)."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO brief_runs (output_path, top_n) VALUES (%s, %s)",
            (out_path, top_n),
        )


def _slugify_scope(label: str) -> str:
    """Convert a human scope label into a kebab-case folder suffix.
    Idempotent on already-slug strings; collapses any non-alphanumeric
    runs to a single dash; strips leading/trailing dashes."""
    s = label.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def render(
    top_n: int = DEFAULT_TOP_N,
    companion_filename: str | None = None,
    scope_label: str | None = None,
) -> str:
    """Render the full briefing pack as a single Markdown string.

    `companion_filename` (when provided): the basename of the paired
    leads document. The headline paragraph cites it directly so a reader
    can find the LLM-scaffolded leads alongside. Set automatically by
    `export()`; pass None for ad-hoc renders that have no paired file.

    `scope_label` (when provided): a human-readable scope description
    surfaced in the headline so a brief shared standalone still
    announces what slice of the data it covers (None = full brief).
    """
    sections: list[_Section] = []
    release_ids: set[int] = set()
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sections.append(_section_headline(
            cur, companion_filename=companion_filename, scope_label=scope_label,
        ))

        # Phase 6.2: universal-caveat explainer right after the headline so
        # a reader knows up-front which methodological caveats apply to
        # every finding (and are therefore suppressed from per-finding
        # caveat lists below).
        sections.append(_section_universal_caveats(cur))

        # Phase 6.8: 'what changed since the previous brief' section sits
        # immediately after the header so a journalist scanning the brief
        # sees the deltas first. Returns empty on first-ever brief.
        # Excludes narrative_hs_group findings — those live in the
        # companion leads file, not the brief.
        sections.append(_section_diff_since_last_brief(cur))

        # LLM lead-scaffold findings render to a separate leads file
        # (render_leads / export_leads). The brief itself is LLM-free
        # so a downstream LLM tool (NotebookLM, etc.) is reasoning over
        # the raw findings, not over another LLM's interpretation.

        # Phase: per-group YoY-predictability badges. Computed once and
        # passed into each per-scope mover section. Empty dict on a fresh
        # DB with no T-6 history; the section renderer falls back to no
        # badge in that case.
        predictability = _compute_predictability_per_group(cur)

        # Per-scope sections (Phase 6.1e). Each scope renders its own
        # YoY top-movers + trajectory sections so a journalist scanning
        # the brief sees the EU-27 / UK / combined views as distinct
        # blocks. Scopes with no findings return empty markdown and are
        # dropped by the join filter at the bottom.
        for scope in ("eu_27", "uk", "eu_27_plus_uk"):
            for flow in (1, 2):
                sec = _section_hs_yoy_movers(
                    cur, flow=flow, top_n=top_n, comparison_scope=scope,
                    predictability=predictability,
                )
                sections.append(sec)
                release_ids |= sec.release_ids
            sec = _section_trajectories(cur, comparison_scope=scope)
            sections.append(sec)
            release_ids |= sec.release_ids

        sec = _section_mirror_gaps(cur)
        sections.append(sec)
        release_ids |= sec.release_ids

        sec = _section_low_base(cur)
        sections.append(sec)
        release_ids |= sec.release_ids

        sections.append(_section_sources_appendix(cur, release_ids))
        sections.append(_section_about_findings())

    return "\n".join(s.markdown for s in sections if s.markdown).rstrip() + "\n"


def render_leads(
    companion_filename: str | None = None,
    scope_label: str | None = None,
) -> str:
    """Render the LLM lead-scaffold companion document. Standalone — does
    not depend on the brief — but cross-references finding IDs that the
    brief also surfaces. Lives in its own document so a downstream LLM
    tool (NotebookLM, etc.) can choose to consume the deterministic
    brief, the leads, both, or neither, without one being baked into the
    other.

    `companion_filename` (when provided): the basename of the paired
    brief document, cited near the top so a reader can find the
    deterministic context. Set automatically by `export()`; pass None
    for ad-hoc renders.

    `scope_label` (when provided): a human-readable scope description
    surfaced in the header so a leads doc shared standalone still
    announces what slice of the data it covers.
    """
    lines: list[str] = []
    lines.append("# GACC × Eurostat trade — investigation leads")
    lines.append(
        f"*Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} from "
        "active `narrative_hs_group` findings.*"
    )
    if scope_label:
        lines.append(f"*Scope: **{scope_label}**.*")
    lines.append("")
    lines.append(
        "Each lead below is an LLM-scaffolded starting position for one HS "
        "group: a one-sentence anomaly summary, 2–3 hypotheses picked from "
        "a curated catalog of standard causes for China-EU/UK trade "
        "movements, and concrete corroboration steps to test them. The LLM "
        "does NOT compute, draft prose, or invent hypotheses outside the "
        "catalog. Every number cited is verified against the underlying "
        "findings before storage; failures are silently rejected rather "
        "than published. Use the leads as starting positions for "
        "investigation; verify against the deterministic findings or the "
        "underlying database."
    )
    lines.append("")
    findings_ref = f"`{companion_filename}`" if companion_filename else "`findings.md`"
    lines.append("## In this export folder")
    lines.append("")
    lines.append(
        "This is one of three artefacts generated together from the same DB "
        "snapshot. All three share the same finding IDs; switch between them "
        "depending on what you need."
    )
    lines.append("")
    lines.append(
        f"- **{findings_ref}** — deterministic Markdown findings. "
        "NotebookLM-ready, no LLM in the loop. Cite this for the "
        "underlying numbers any lead below references."
    )
    lines.append(
        "- **`leads.md`** — LLM-scaffolded investigation leads (this "
        "document). Kept separate from the findings so a downstream LLM "
        "tool reasoning over them sees raw data, not another LLM's "
        "interpretation."
    )
    lines.append(
        "- **`data.xlsx`** — 8-tab spreadsheet for data journalists. Same "
        "findings, long-format with filterable scope/flow columns, "
        "predictability badges, CIF/FOB baseline expansion. Also LLM-free."
    )
    lines.append("")
    lines.append(
        "Each lead carries an `llm_drafted` caveat plus the union of "
        "caveats on its underlying findings; underlying caveats "
        "(low_base, partial_window, transshipment_hub, cn8_revision, "
        "low_kg_coverage, etc.) propagate from the source findings. "
        "Trace ids point to the lead finding itself; the underlying "
        "deterministic findings are listed alongside so you can walk the "
        "chain."
    )
    lines.append("")

    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        section = _section_llm_narratives(cur)

    if not section.markdown:
        lines.append(
            "_No active `narrative_hs_group` findings — run "
            "`scrape.py --analyse llm-framing` to generate leads._"
        )
        lines.append("")
    else:
        # _section_llm_narratives starts with its own "## Investigation
        # leads" header + intro paragraph; strip those (we provide the
        # framing here) and keep the per-group blocks.
        body = section.markdown
        marker = "### "
        idx = body.find(marker)
        if idx > 0:
            body = body[idx:]
        lines.append(body)

    # Endnote on what `finding/N` citations mean — same text as the brief's
    # endnote so a journalist coming to either doc gets the same orientation.
    lines.append("")
    lines.append(_section_about_findings().markdown)

    return "\n".join(lines).rstrip() + "\n"


def export(
    out_dir: str | None = None,
    scope_label: str | None = None,
    top_n: int = DEFAULT_TOP_N,
    out_path: str | None = None,
    leads_path: str | None = None,
    spreadsheet: bool | None = None,
) -> tuple[str, str]:
    """Write the findings document AND the companion leads file to disk.
    Returns (findings_path, leads_path).

    Default behaviour: create `./exports/YYYY-MM-DD-HHMM[-slug]/` and
    write `findings.md` + `leads.md` inside it. Pairs are self-evident
    from the folder; consumers find the pair by convention.

    `scope_label` (optional, human-readable): when set, slugified into a
    folder suffix (e.g. "EV batteries (Li-ion)" → `-ev-batteries-li-ion`)
    AND surfaced inside both docs' headers so a brief shared standalone
    still announces its scope. Note: the scope_label is currently
    metadata only; the brief/leads still render the full finding set.
    Scoped *filtering* (only emit findings for one HS group, only one
    comparison scope) is a separate future change — having the naming
    convention in place now means scoped exports can land cleanly.

    `out_dir` (optional): override the default folder path.

    `out_path` / `leads_path` (legacy escape hatch): explicit per-file
    paths, both required if either is given. Skips folder creation.
    Use the folder approach by default — these are kept only for
    callers (e.g. tests) that want explicit control.

    `spreadsheet`: also write `data.xlsx` into the export folder so
    all three artefacts (findings / leads / spreadsheet) share a
    single DB snapshot. A data journalist opens data.xlsx; an editorial
    journalist opens findings.md; everyone is working from the same
    point in time. Default depends on mode: folder mode → True
    (spreadsheet is part of the user-facing bundle); legacy explicit-
    paths mode → False (callers using explicit paths are typically
    tests / preview / programmatic use that don't need the bundle).
    Pass explicitly to override either default.

    Records the brief run in `brief_runs` so the next brief can compute
    "what changed since" (Phase 6.8). render() is called for the
    markdown but doesn't record — record only on disk-writing exports
    so test/preview renders don't pollute the run log.
    """
    if out_path is not None or leads_path is not None:
        # Legacy explicit-paths mode. Both must be given.
        if out_path is None or leads_path is None:
            raise ValueError(
                "If using explicit out_path / leads_path, pass both."
            )
        p = Path(out_path)
        lp = Path(leads_path)
        if spreadsheet is None:
            spreadsheet = False  # legacy callers opt in if they want it
    else:
        if out_dir is None:
            ts = datetime.now().strftime("%Y-%m-%d-%H%M")
            slug = f"-{_slugify_scope(scope_label)}" if scope_label else ""
            out_dir = f"./exports/{ts}{slug}"
        d = Path(out_dir)
        p = d / "findings.md"
        lp = d / "leads.md"
        if spreadsheet is None:
            spreadsheet = True  # bundle is the default user-facing mode

    # Each render gets the OTHER doc's basename as its companion citation.
    brief_basename = p.name
    leads_basename = lp.name

    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(render(
        top_n=top_n, companion_filename=leads_basename,
        scope_label=scope_label,
    ))
    _record_brief_run(out_path=str(p), top_n=top_n)
    log.info("Wrote briefing pack to %s", p)

    lp.parent.mkdir(parents=True, exist_ok=True)
    lp.write_text(render_leads(
        companion_filename=brief_basename, scope_label=scope_label,
    ))
    log.info("Wrote investigation leads to %s", lp)

    if spreadsheet:
        # Lazy import — sheets_export imports from this module, so a
        # top-level import would create a cycle. The sheet always lives
        # next to the brief in the same folder; filename is `data.xlsx`.
        import sheets_export
        xlsx_path = p.parent / "data.xlsx"
        sheets_export.XlsxWriter().write(
            sheets_export.assemble_sheets(), str(xlsx_path),
        )
        log.info("Wrote spreadsheet to %s", xlsx_path)

    return str(p), str(lp)
