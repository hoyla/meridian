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
import psycopg2.extras

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
    if abs(n) >= 1e12:
        return f"€{n / 1e12:.2f}T"
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


def _fmt_month(d: date | None) -> str:
    """Render a period date as "Mar 2026" — every journalist-facing
    surface uses this instead of the raw ISO date, whose day component
    (always -01) is an artefact of period storage, not information."""
    if d is None:
        return "—"
    return d.strftime("%b %Y")


def _flow_phrase(flow: int) -> str:
    """Plain-English direction phrase. The customs-register arrow form
    ("CN→reporter") read as jargon to cold readers in the 2026-05-13
    pack review; every surface now phrases flow as seen from Europe."""
    return "imports from China" if flow == 1 else "exports to China"


def _caveat_summary_map(cur) -> dict[str, str]:
    """code → one-line plain-English summary, from the `caveats` table.
    Sections that render per-finding caveat lines fetch this once and
    render the summary with the code in parens — the code stays for
    cross-referencing the methodology footer and the spreadsheet, but
    the words carry the meaning."""
    cur.execute("SELECT code, summary FROM caveats")
    return {r[0]: r[1] for r in cur.fetchall()}


def _fmt_caveats_inline(codes: list[str], summaries: dict[str, str]) -> str:
    """Render a caveat-code list as plain English: "summary (`code`)";
    falls back to the bare code for any code without a `caveats` row."""
    parts = []
    for c in codes:
        s = summaries.get(c)
        parts.append(f"{s} (`{c}`)" if s else f"`{c}`")
    return "; ".join(parts)


# Single-month YoY sanity bound. Above this (fractional, 3.0 = 300%) a
# single-month percentage almost always reflects a tiny or empty base
# month — e.g. civil aircraft printed "+686380.9%" in the 2026-05-21
# export — and methodology.md §10 says such figures are not quotable.
# The number still renders (never hide data) but carries an explicit
# not-quotable warning so it can't be lifted into copy unhedged.
SINGLE_MONTH_EXTREME = 3.0

_SM_EXTREME_NOTE = (
    " ⚠ *extreme swing — single-month percentages this size usually "
    "mean a tiny or empty base month; quote the 12-month figure, not "
    "this one.*"
)


def _single_month_warning(sm_pct: Any) -> str:
    """'' or the not-quotable note, given a fractional single-month YoY."""
    if sm_pct is not None and abs(float(sm_pct)) >= SINGLE_MONTH_EXTREME:
        return _SM_EXTREME_NOTE
    return ""


# Warning line rendered directly under a 🔴-badged group heading in Tier 2.
# Additive (a line, not a heading change) so heading anchors stay stable.
_VOLATILE_GROUP_NOTE = (
    "*🔴 Volatile — this group's year-on-year signal has not held over "
    "the past 6 months. Verify any figure here independently before "
    "quoting it.*"
)


def _fmt_missing_months(
    missing_current: list[str] | None,
    missing_prior: list[str] | None,
) -> str:
    """Render detail.totals.missing_months_current/_prior (ISO date
    strings) as a window-attributed phrase: which month is absent, and
    whether it falls in the current window (understates the new total)
    or the prior comparison window (skews the YoY denominator). ''
    when nothing is missing."""
    def _months(vals: list[str]) -> str:
        return ", ".join(_fmt_month(date.fromisoformat(v)) for v in vals)

    bits: list[str] = []
    if missing_current:
        bits.append(
            f"missing {_months(missing_current)} from the current "
            "12-month window"
        )
    if missing_prior:
        bits.append(
            f"missing {_months(missing_prior)} from the prior "
            "(comparison) window"
        )
    return " and ".join(bits)


def _quotability_verdict(
    *,
    badge: str | None,
    low_base: Any,
    current_eur: Any,
    prior_eur: Any,
    threshold_eur: Any,
    missing_current: list[str] | None = None,
    missing_prior: list[str] | None = None,
) -> str:
    """One-sentence plain-English quotability instruction for a YoY
    finding — the render-time verdict that applies methodology §9/§10
    at the point of quotation instead of trusting the reader to have
    read them. Composed entirely from facts already on the finding row
    plus the render-time stability badge; nothing is stored, so verdict
    wording can evolve without superseding findings.

    Priority: low base (percentages unusable) → 🔴 (signal didn't hold)
    → threshold fragility → badge-graded go-ahead. A missing-month
    qualifier is appended whatever the lead clause."""
    thr_m = f"€{float(threshold_eur) / 1e6:.0f}M" if threshold_eur else "low-base"
    smaller = None
    if current_eur is not None and prior_eur is not None:
        smaller = min(float(current_eur), float(prior_eur))

    if low_base:
        lead = (
            "percentages here are not quotable — the smaller 12-month "
            f"total is only {_fmt_eur(smaller)}, below the {thr_m} "
            "low-base line; quote the absolute € amounts instead"
        )
    elif badge == "🔴":
        lead = (
            "verify before quoting — the year-on-year signal for this "
            "group has flipped or shifted over the past 6 months (🔴)"
        )
    elif is_threshold_fragile(current_eur, prior_eur, threshold_eur):
        lead = (
            f"quote with care — the smaller 12-month total "
            f"({_fmt_eur(smaller)}) sits close to the {thr_m} low-base "
            "line, so check the absolute € amounts alongside the "
            "percentage"
        )
    elif badge == "🟢":
        lead = (
            "quotable as a 12-month trend — the signal has held over "
            "the past 6 months and the base is meaningful"
        )
    elif badge == "🟡":
        lead = (
            "quotable with a double-check — the signal has been mixed "
            "over the past 6 months"
        )
    else:
        lead = (
            "quotable on the numbers, but stability is unscored — not "
            "enough history yet to know whether this trend holds"
        )

    parts = [lead]
    miss = _fmt_missing_months(missing_current, missing_prior)
    if miss:
        parts.append(
            f"note: the window is {miss} — re-check once that month "
            "has been ingested"
        )
    sentence = "; ".join(parts)
    return sentence[0].upper() + sentence[1:] + "."


def _hmrc_suppressed_counts(
    cur,
    *,
    patterns: list[str],
    partners: list[str],
    flow: int,
    current_start: date,
    current_end: date,
    prior_start: date,
    prior_end: date,
) -> tuple[int, int]:
    """(current-window, prior-window) counts of HMRC raw rows excluded
    from the totals because HMRC suppressed their value for
    confidentiality (small-trader flows). Mirrors the analyser's
    aggregation predicates (anomalies hmrc branch: flow + partner +
    hs-pattern) with `suppression_index <> 0` — so the count describes
    exactly the rows missing from the rendered totals. Both windows are
    counted because suppression that differs between them skews the YoY
    itself, not just the level."""
    like_clause, like_params = anomalies._hs_pattern_or_clause(patterns)
    cur.execute(
        f"""
        SELECT
          COUNT(*) FILTER (WHERE period >= %s AND period <= %s) AS n_curr,
          COUNT(*) FILTER (WHERE period >= %s AND period <= %s) AS n_prior
          FROM hmrc_raw_rows
         WHERE flow = %s
           AND partner = ANY(%s)
           AND {like_clause}
           AND suppression_index <> 0
        """,
        (current_start, current_end, prior_start, prior_end,
         flow, list(partners), *like_params),
    )
    row = cur.fetchone()
    return int(row[0] or 0), int(row[1] or 0)


# Plain-English labels for finding subkind families — used wherever a
# subkind code would otherwise face a journalist (Tier 1 diff counts,
# material-shift lines). Keyed by family prefix; suffixed variants
# (_uk, _combined, _export) resolve via _subkind_plain_label.
_SUBKIND_PLAIN_LABELS: dict[str, str] = {
    "mirror_gap_zscore": "mirror-trade gap mover (unusual shift vs the partner's own baseline)",
    "mirror_gap": "mirror-trade gap (China-reported vs Europe-reported totals)",
    "hs_group_yoy": "year-on-year change for an HS group",
    "hs_group_trajectory": "trend shape for an HS group",
    "gacc_bilateral_aggregate_yoy": "China-side bilateral year-on-year",
    "gacc_aggregate_yoy": "China-side bloc aggregate year-on-year",
    "partner_share": "China's share of EU imports/exports",
    "trade_balance_combined_cn_only": "Europe (EU-27 + UK)–China trade deficit (all-goods, CN-only)",
    "trade_balance_combined": "Europe (EU-27 + UK)–China trade deficit (all-goods, CN+HK+MO)",
    "trade_balance_uk_cn_only": "UK–China trade deficit (all-goods, CN-only — matches the published figure)",
    "trade_balance_uk": "UK–China trade deficit (all-goods, CN+HK+MO)",
    "trade_balance_cn_only": "EU–China trade deficit (all-goods, CN-only — matches Eurostat's published figure)",
    "trade_balance": "EU–China trade deficit (all-goods, CN+HK+MO)",
    "narrative_hs_group": "LLM-scaffolded lead",
}


def _subkind_plain_label(subkind: str) -> str:
    """Best plain-English label for a subkind, tolerant of scope/flow
    suffixes. Falls back to the raw subkind string."""
    for prefix in sorted(_SUBKIND_PLAIN_LABELS, key=len, reverse=True):
        if subkind.startswith(prefix):
            return _SUBKIND_PLAIN_LABELS[prefix]
    return subkind


# Scope + flow phrase per YoY subkind — the "which trade flow is this"
# clause for sentence contexts (Tier 1 shift lines, the front page).
# Longest-prefix wins, so suffixed variants must precede their parents.
_SUBKIND_FLOW_SCOPE_PHRASES: list[tuple[str, str]] = [
    ("hs_group_yoy_uk_export", "UK exports to China"),
    ("hs_group_yoy_uk", "UK imports from China"),
    ("hs_group_yoy_combined_export", "EU-27 + UK exports to China (combined)"),
    ("hs_group_yoy_combined", "EU-27 + UK imports from China (combined)"),
    ("hs_group_yoy_export", "EU-27 exports to China"),
    ("hs_group_yoy", "EU-27 imports from China"),
    ("gacc_bilateral_aggregate_yoy_import", "China's imports (GACC-reported)"),
    ("gacc_bilateral_aggregate_yoy", "China's exports (GACC-reported)"),
    ("gacc_aggregate_yoy_import", "China's imports (GACC-reported)"),
    ("gacc_aggregate_yoy", "China's exports (GACC-reported)"),
    ("partner_share_export", "China's share of EU-27 extra-EU exports"),
    ("partner_share", "China's share of EU-27 extra-EU imports"),
]


def _subkind_flow_scope_phrase(subkind: str) -> str | None:
    """Scope/flow phrase for a YoY subkind ("EU-27 imports from China"),
    or None when the subkind has no flow framing (trajectories etc.)."""
    for prefix, phrase in sorted(
        _SUBKIND_FLOW_SCOPE_PHRASES, key=lambda t: -len(t[0]),
    ):
        if subkind.startswith(prefix):
            return phrase
    return None


def _reading_the_numbers_md() -> str:
    """The shared "Reading the numbers" key — the six conventions a
    cold reader needs before quoting anything. Rendered near the top of
    both the findings and the leads documents (and kept here so the two
    never drift)."""
    lines = [
        "## Reading the numbers",
        "",
        "- **Value vs volume** — *value* is what the goods cost (€); "
        "*volume* is their weight (kg). When the two diverge, the price "
        "per kg moved: value falling faster than volume means the same "
        "goods are getting cheaper.",
        "- **12-month figure vs latest month** — the 12-month rolling "
        "figure compares the last 12 months with the 12 before; it "
        "smooths seasonal swings and is the right number to quote. "
        "*Latest month* compares one month with the same month a year "
        "earlier — a useful direction hint, but it swings wildly on "
        "lumpy categories (aircraft, ships), so don't make it the "
        "headline number.",
        "- **% vs pp** — % is change relative to a year earlier; *pp* "
        "(percentage points) is the difference between two percentages "
        "(a share moving from 10% to 15% is +5 pp, not +50%).",
        "- **🟢 🟡 🔴** — whether this group's year-on-year signal has "
        "held up over the past 6 months: 🟢 held / 🟡 mixed / 🔴 didn't "
        "hold (verify before quoting). No badge just means not enough "
        "history yet.",
        "- **⚠ low base** — the percentage rests on a small total "
        "(under €50M), so it can look dramatic without being "
        "significant. Quote the absolute € amounts instead.",
        "- **`finding/N`** — the citation token ending every claim. It "
        "is a permanent reference to the exact database row behind the "
        "number; include it when asking for verification and the full "
        "audit trail (source URLs, FX rates, the arithmetic) can be "
        "produced.",
        "",
    ]
    return "\n".join(lines)


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
_SOURCE_LABELS = {"gacc": "GACC", "eurostat": "Eurostat", "hmrc": "HMRC",
                  "cross_source": "Eurostat + HMRC"}


def _source_label(src: str | None) -> str:
    """Display name for a provenance source code ('eurostat' → 'Eurostat'),
    so a figure's origin is legible on the surface rather than only in the
    drill-down. Unknown/empty codes pass through unchanged."""
    return _SOURCE_LABELS.get(src or "", src or "")


# Subfolder holding the plain markdown / spreadsheet copies of the bundle,
# alongside the top-level .docx/.xlsx (which become Google Docs/Sheet once
# uploaded). Keep in sync with `briefing_pack.drive_export.MARKDOWN_SUBFOLDER`.
_MARKDOWN_SUBFOLDER = "Markdown versions for use with LLMs etc"


def _bundle_root(file_path: str) -> Path:
    """The top-level export folder for a bundle file (the dir to hand to
    `--upload-to-drive`). When docx is generated the findings markdown sits
    in the `_MARKDOWN_SUBFOLDER` subfolder; flat / older bundles have it at
    the root. Either way, step up out of the subfolder if present."""
    parent = Path(file_path).parent
    return parent.parent if parent.name == _MARKDOWN_SUBFOLDER else parent


def _prev_export_folder(output_path: str | None) -> str | None:
    """Derive the export-folder name from a recorded brief_runs.output_path.
    Returns None for legacy / test paths that don't follow the standard
    layout — the caller falls back to citing the timestamp alone."""
    if not output_path:
        return None
    name = _bundle_root(output_path).name
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


def _new_data_phrase_since_last_brief() -> str:
    """One-line phrase naming the source releases (GACC / Eurostat / HMRC)
    first seen since the most recent brief_runs row — e.g. 'GACC March 2026
    (preliminary); Eurostat March 2026'. '' when nothing new arrived (or no
    prior brief exists). Opens its own connection; call it BEFORE recording
    the current run's brief_runs row, so the latest row is still the
    previous cycle. Mirrors the release detection in
    `_why_this_export_paragraph`."""
    with _conn() as conn, conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor,
    ) as cur:
        cur.execute(
            "SELECT generated_at FROM brief_runs "
            "ORDER BY generated_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return ""
        return _format_new_releases_phrase(_new_releases_since(cur, row[0]))


def _why_this_export_paragraph(cur) -> str:
    """Standalone one-paragraph "why this export" framing for surfaces
    that don't have a Tier 1 diff section (notably `03_Leads.md`). Returns
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
    # Numeric order IS the recommended reading order (renumbered
    # 2026-06-11: Findings ahead of Leads, now that the deterministic
    # front page — not the LLM tip-sheet — is the entry point).
    ("02_Findings",
     "the deterministic findings — no LLM in the loop. Starts with "
     "\"If you read only this page\". Cite this for the underlying "
     "numbers behind any lead."),
    ("03_Leads",
     "LLM-scaffolded investigation leads, one per HS group: a one-sentence "
     "anomaly summary, 2–3 hypotheses from a curated catalog, and concrete "
     "corroboration steps. Kept separate from the findings so a downstream "
     "LLM tool reasoning over them sees raw data, not another LLM's "
     "interpretation."),
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
    "02_Findings") marks which artefact is the document being read."""
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
        f"- ⚖️ **Near the low-base line** ({_fmt_eur(smaller)} vs the "
        f"€{thr/1e6:.0f}M threshold) — this finding sits close enough "
        "to the cut-off that a small change in the threshold would flip "
        "whether it carries the low-base warning. Treat it with the "
        "same care as a flagged one: check the absolute € amounts "
        "before quoting the percentage."
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
