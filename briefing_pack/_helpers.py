"""Cross-section utilities for the briefing-pack package.

DB connection, citation tokens, formatters, predictability + threshold-
fragility helpers, scope labels, and the constant frozenset of family-
universal caveats (computed once from anomalies.UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY).
Anything used by two or more `briefing_pack.sections.*` modules lives here;
section-local helpers stay in the section module.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2

import anomalies

PERMALINK_BASE_ENV = "GACC_PERMALINK_BASE"
DEFAULT_TOP_N = 10

# Defensive filter for per-finding caveat-line rendering. Analysers no longer
# attach universal caveats to `findings.caveat_codes` (those live in the
# Methodology footer once, sourced from
# `anomalies.UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY`). This frozenset re-derives
# the universal codes so per-finding lines remain clean even for findings
# inserted before the refactor — without this, an older DB row could surface
# `cif_fob` etc. inline.
_ALL_UNIVERSAL_CAVEATS: frozenset[str] = frozenset(
    c for codes in anomalies.UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY.values() for c in codes
)


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


# Export-trigger framing — shared by the findings Tier 1 lead-in and the
# leads doc's "Why this export" paragraph. Both surfaces need to name the
# previous export (folder + timestamp) and either cite the new source
# releases that triggered this cycle or call out a rerun-without-new-data
# explicitly. Centralised here so the two docs stay phrased consistently.
_SOURCE_LABELS = {"gacc": "GACC", "eurostat": "Eurostat", "hmrc": "HMRC"}


def _prev_export_folder(output_path: str | None) -> str | None:
    """Derive the parent folder name from a recorded brief_runs.output_path
    (which points at `…/03_Findings.md`). Returns None for legacy / test
    paths that don't follow the standard layout — the caller falls back to
    citing the timestamp alone."""
    if not output_path:
        return None
    name = Path(output_path).parent.name
    if not name or name in {".", "..", "tmp", "exports"}:
        return None
    return name


def _new_releases_since(cur, prev_at) -> list[dict]:
    """Source releases first seen between the previous brief and now.

    Drives the export-trigger framing — a new GACC monthly, a new
    Eurostat period, etc. Used both to (a) cite the substantive trigger
    when there is one and (b) call out explicitly when a rerun has no
    new source data behind it (so the reader doesn't assume movement
    they're seeing reflects fresh figures)."""
    cur.execute(
        """
        SELECT source, period, release_kind
          FROM releases
         WHERE first_seen_at > %s
         ORDER BY source, period DESC, release_kind
        """,
        (prev_at,),
    )
    return list(cur.fetchall())


def _format_new_releases_phrase(rows: list[dict]) -> str:
    """Render the rowset from `_new_releases_since` as a one-line phrase
    suitable for embedding in the lead-in. Returns '' if the rowset is
    empty.

    Example: ``GACC March 2026 (preliminary), February 2026 (monthly);
    Eurostat March 2026``."""
    if not rows:
        return ""
    by_source: dict[str, list[dict]] = {}
    for r in rows:
        by_source.setdefault(r["source"], []).append(r)
    parts: list[str] = []
    for src in ("gacc", "eurostat", "hmrc"):
        if src not in by_source:
            continue
        bits: list[str] = []
        for r in by_source[src]:
            period_str = r["period"].strftime("%B %Y")
            if r["release_kind"]:
                bits.append(f"{period_str} ({r['release_kind']})")
            else:
                bits.append(period_str)
        parts.append(f"{_SOURCE_LABELS[src]} {', '.join(bits)}")
    return "; ".join(parts)


def _prev_export_ref(prev_at, prev_folder: str | None) -> str:
    """Lead-in reference to the previous export — includes the folder
    name when available so the reader can navigate straight to it. The
    timestamp is always present; the folder is only added when the
    recorded output_path follows the standard `exports/<ts>/` layout."""
    ts = f"{prev_at:%Y-%m-%d %H:%M %Z}"
    if prev_folder:
        return f"Previous findings export `{prev_folder}` generated {ts}"
    return f"Previous findings export generated {ts}"


def _source_data_sentence(new_releases_phrase: str) -> str:
    """One-sentence framing of *why* this export exists. Either names
    the new source releases that triggered it, or — when nothing new has
    arrived — calls out explicitly that this is a rerun against the same
    DB snapshot, so the reader doesn't misread mechanical churn as fresh
    figures. Assumes the caller has already cited the previous export
    (folder + timestamp) immediately before, so no further folder-ref is
    needed here."""
    if new_releases_phrase:
        return f"New source data since then: {new_releases_phrase}."
    return (
        "**No new GACC / Eurostat / HMRC release has been published "
        "since then** — this is a rerun against the same source snapshot."
    )


def _why_this_export_paragraph(cur) -> str:
    """Standalone one-paragraph "why this export" framing for surfaces
    that don't have a Tier 1 diff section (notably `02_Leads.md`). Returns
    an italicised markdown paragraph, or '' if there is no previous brief
    to compare against. Phrasing is kept consistent with the findings
    doc's Tier 1 lead-in so the two surfaces don't disagree about what
    triggered the cycle."""
    cur.execute(
        "SELECT generated_at, output_path FROM brief_runs "
        "ORDER BY generated_at DESC LIMIT 1"
    )
    row = cur.fetchone()
    if row is None or row[0] is None:
        return ""
    prev_at, prev_output_path = row[0], row[1]
    prev_folder = _prev_export_folder(prev_output_path)
    prev_ref = _prev_export_ref(prev_at, prev_folder)
    new_releases = _new_releases_since(cur, prev_at)
    source_sentence = _source_data_sentence(
        _format_new_releases_phrase(new_releases),
    )
    return f"*{prev_ref}. {source_sentence}*"


_SCOPE_LABEL = {
    "eu_27": "EU-27",
    "uk": "UK",
    "eu_27_plus_uk": "EU-27 + UK (combined)",
}
_SCOPE_SUBKIND_SUFFIX = {"eu_27": "", "uk": "_uk", "eu_27_plus_uk": "_combined"}

# ---------------------------------------------------------------------------
# Shared "In this export folder" listing
# ---------------------------------------------------------------------------
# One canonical artefact listing, used by BOTH the findings and the leads
# documents so the two never drift. Names carry no file extension: in the
# delivered Drive folder each is a native Google Doc / Sheet for human
# readers, and the plain markdown / spreadsheet copies live in a subfolder.
_EXPORT_ARTEFACTS = [
    ("02_Leads",
     "LLM-scaffolded investigation leads, one per HS group: a one-sentence "
     "anomaly summary, 2–3 hypotheses from a curated catalog, and concrete "
     "corroboration steps. Kept separate from the findings so a downstream "
     "LLM tool reasoning over them sees raw data, not another LLM's "
     "interpretation."),
    ("03_Findings",
     "the deterministic findings — no LLM in the loop. Cite this for the "
     "underlying numbers behind any lead."),
    ("04_Data",
     "a multi-tab spreadsheet for data journalists: the same findings in "
     "long, filterable form with scope/flow columns, predictability badges, "
     "and CIF/FOB baseline expansion."),
    ("05_Groups",
     "the HS group reference — what each named group contains, its top "
     "contributing CN8 codes, and sibling groups. Read once before quoting "
     "any category figure."),
]


def _in_this_export_folder_md(current: str | None = None) -> str:
    """The shared "In this export folder" section. `current` (e.g.
    "03_Findings") marks which artefact is the document being read."""
    lines = ["## In this export folder", ""]
    lines.append(
        "This is one of four documents generated together from the same "
        "database snapshot. All four share the same finding IDs, so you can "
        "move freely between them."
    )
    lines.append("")
    for name, desc in _EXPORT_ARTEFACTS:
        marker = " *(this document)*" if name == current else ""
        lines.append(f"- **{name}**{marker} — {desc}")
    lines.append("")
    return "\n".join(lines)


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
PREDICTABILITY_MIN_PAIRS = 3     # need at least N (scope, flow) T-6 pairs
                                 # to render a badge; below this the
                                 # signal is too sparse to support an
                                 # editorial confidence cue. Phase 6.6
                                 # backtest had 5–6 per established group.
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
        # Below the minimum-pairs threshold, the badge would rest on too
        # few permutations to be a confident editorial cue. Suppress
        # rather than display a misleadingly-strong signal — the journalist
        # reads the trajectory line and the headline % without a badge,
        # which correctly reflects "we don't yet know."
        if n < PREDICTABILITY_MIN_PAIRS:
            continue
        pct = sum(persists) / n
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


def _slugify_scope(label: str) -> str:
    """Convert a human scope label into a kebab-case folder suffix.
    Idempotent on already-slug strings; collapses any non-alphanumeric
    runs to a single dash; strips leading/trailing dashes."""
    s = label.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def _slugify_heading(label: str) -> str:
    """Approximate GitHub's Markdown heading-anchor slug rule for
    cross-reference links inside the brief.

    GitHub rules (paraphrased): lowercase, strip punctuation (most
    symbols → empty), replace spaces with dashes, collapse runs of
    dashes. Some punctuation (slash, plus, parentheses) leaves
    double-dashes where the spaces around them collapse together —
    we mimic this to match observed slugs like
    `mirror-trade--mirror-gap` and `cif--fob`.
    """
    s = label.lower()
    # Strip punctuation that GitHub drops outright. Keeps letters,
    # digits, hyphen, and spaces; the space → dash step happens next.
    s = re.sub(r"[^a-z0-9\s\-]", "", s)
    # Spaces → dashes. Existing dashes preserved.
    s = re.sub(r"\s", "-", s)
    return s.strip("-")


# =============================================================================
# Top-5 movers — composite-ranked editorial digest
# =============================================================================
#
# Scoring constants used by `_compute_top_movers`. The composite rule
# behind it: surface findings that are likely to be quotable in copy.
# That means a meaningful move (≥10pp) on a meaningful base (≥€100M
# current 12mo), filtered against base-effect and predictability
# concerns. Tuned to land 5–10 candidate rows per cycle on the current
# DB; if it ever returns 0, the brief just omits the section.

TOP_MOVERS_MIN_YOY_ABS = 0.10           # 10pp absolute move
TOP_MOVERS_MIN_CURRENT_EUR = 100_000_000  # €100M current 12mo total
TOP_MOVERS_LIMIT = 5                     # editorial cap


def _compute_top_movers(
    cur, predictability: dict[str, tuple[str, float, int]] | None = None,
    *, limit: int = TOP_MOVERS_LIMIT,
) -> list[dict]:
    """Composite-ranked top movers across the EU-27 hs_group_yoy* family.

    Filters applied in order:
    1. |yoy_pct| ≥ TOP_MOVERS_MIN_YOY_ABS
    2. low_base = False
    3. current_12mo_eur ≥ TOP_MOVERS_MIN_CURRENT_EUR
    4. predictability badge ≠ 🔴 (groups without enough T-6 data for any
       badge ARE included — absence of evidence is not evidence of
       volatility; we just don't have a confidence cue yet)

    Score = |yoy_pct| × log10(current_12mo_eur). Rewards moderate moves
    on big bases as well as big moves on moderate bases; a 30% move on
    €27B (EV batteries) and a 40% move on €1B (Drones) both land near
    score 3.5, which feels editorially right.

    Returns up to `limit` rows as dicts with the headline fields plus
    `score` (the composite) and `subkind` (the originating subkind so
    the renderer can label imports vs exports correctly).

    Scope: eu_27 only (`hs_group_yoy` + `hs_group_yoy_export`). UK and
    combined are deliberately omitted to avoid surfacing the same group
    three times — UK-specific stories are still visible in Tier 2.

    Recency filter: only findings at the **latest anchor period across
    the entire family** are considered. A group with findings only at
    older anchors (e.g. an HS code that became reportable but lacks
    enough months for the rolling window to reach the current cycle) is
    silently skipped. Without this filter, stale 2018–2022 anchors on
    fringe codes could outrank the genuine top movers — observed on
    MPPT solar inverters (CN8 85044084) which has findings to 2022-12
    but nothing recent.
    """
    import math
    predictability = predictability or {}

    cur.execute(
        """
        SELECT MAX((detail->'windows'->>'current_end')::date)
          FROM findings
         WHERE subkind IN ('hs_group_yoy', 'hs_group_yoy_export')
           AND superseded_at IS NULL
        """
    )
    latest_anchor = cur.fetchone()[0]
    if latest_anchor is None:
        return []

    cur.execute(
        """
        SELECT id,
               subkind,
               detail->'group'->>'name'                          AS group_name,
               (detail->'totals'->>'yoy_pct')::numeric           AS yoy_pct,
               (detail->'totals'->>'yoy_pct_kg')::numeric        AS yoy_pct_kg,
               (detail->'totals'->>'current_12mo_eur')::numeric  AS current_eur,
               (detail->'totals'->>'prior_12mo_eur')::numeric    AS prior_eur,
               (detail->'totals'->>'current_12mo_kg')::numeric   AS current_kg,
               (detail->'totals'->>'low_base')::boolean          AS low_base,
               (detail->'windows'->>'current_end')::date         AS current_end
          FROM findings
         WHERE subkind IN ('hs_group_yoy', 'hs_group_yoy_export')
           AND superseded_at IS NULL
           AND (detail->'windows'->>'current_end')::date = %s
           AND (detail->'totals'->>'low_base')::boolean = false
           AND (detail->'totals'->>'current_12mo_eur')::numeric >= %s
           AND abs((detail->'totals'->>'yoy_pct')::numeric) >= %s
        """,
        (latest_anchor, TOP_MOVERS_MIN_CURRENT_EUR, TOP_MOVERS_MIN_YOY_ABS),
    )
    rows = cur.fetchall()

    scored: list[dict] = []
    for r in rows:
        d = dict(r) if not isinstance(r, dict) else dict(r)
        pred = predictability.get(d["group_name"])
        if pred is not None and pred[0] == "🔴":
            continue
        yoy = abs(float(d["yoy_pct"]))
        eur = max(float(d["current_eur"]), 1.0)
        d["score"] = yoy * math.log10(eur)
        d["predictability"] = pred  # (badge, pct, n) or None
        scored.append(d)
    scored.sort(key=lambda x: -x["score"])
    return scored[:limit]
