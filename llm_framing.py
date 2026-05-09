"""LLM narrative layer over the deterministic findings.

Phase 3 of dev_notes/roadmap-2026-05-09.md. The deterministic findings
(hs_group_yoy, mirror_gap, etc.) are already structured and citable;
what this module adds is editorial *framing*: a 2-3 sentence top-line
per cluster that a desk could quote, drawing on the underlying
findings + their attached caveats.

Strict discipline:

1. **The LLM never computes.** It receives a frozen list of typed
   facts (yoy_pct=+0.342, current_eur=2.69e10, shape='dip_recovery',
   caveats=['cn8_revision', 'partial_window']). Its job is to narrate
   them in journalistic English, not to derive new numbers.

2. **Every number in the output must appear in the facts.** A
   `verify_numbers` pass extracts numbers from the LLM output and
   checks each against the facts list within rounding tolerance. If
   any extracted number doesn't match, the narrative is REJECTED —
   not stored, not surfaced. We'd rather be silent than confidently
   wrong.

3. **Caveats propagate.** The narrative finding's `caveat_codes` is
   the union of caveats on its underlying findings, plus a
   `llm_drafted` caveat flagging editorial origin.

4. **Idempotent + revision-aware.** Narratives use the same
   append-plus-supersede chain as deterministic findings (subkind
   `narrative_hs_group`, natural_key per hs_group_id). Re-running on
   unchanged underlying data is a no-op; when an underlying finding
   revises, the narrative regenerates and supersedes the prior one.

v1 scope: HS-group narratives only. Per-partner (mirror_gap clusters)
and per-period clusters are forward work.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Protocol

import psycopg2
import psycopg2.extras

import findings_io

log = logging.getLogger(__name__)

DEFAULT_OLLAMA_MODEL = "qwen3.6:latest"
LLM_FRAMING_SOURCE_URL = "analysis://llm_framing/v1"

NARRATIVE_METHOD = "llm_topline_v1_hs_group"

# Tolerance bands for numeric verification. Percentages get a 0.5pp
# absolute tolerance because LLMs naturally round (+34.2% → "34%").
# Currency values get an order-of-magnitude check (the LLM might say "€27B"
# for €26.9B; we accept ±5% relative). Pure integer counts must match exactly.
PCT_TOLERANCE_ABS = 0.005     # 0.5 percentage points (the value is in fraction form, 0.342 not 34.2)
CURRENCY_TOLERANCE_REL = 0.05  # 5% relative


# =============================================================================
# Backend interface
# =============================================================================


class LLMBackend(Protocol):
    def generate(self, system: str, prompt: str) -> str:
        ...


def make_backend(model: str | None = None) -> LLMBackend:
    """Pick the configured backend. Default Ollama. Override via LLM_BACKEND env."""
    backend_name = os.environ.get("LLM_BACKEND", "ollama").lower()
    if backend_name == "ollama":
        return OllamaBackend(model=model or DEFAULT_OLLAMA_MODEL)
    raise ValueError(f"Unknown LLM_BACKEND: {backend_name}")


class OllamaBackend:
    """Calls a local Ollama daemon's /api/chat endpoint. The Python `ollama`
    package is in requirements.txt. We wrap it to give a stable, testable
    interface.

    Handles thinking models (Qwen 3.x family, DeepSeek-R1, o1-style): we
    pass `think=False` so the model skips its reasoning channel and writes
    directly to content. Thinking would otherwise burn the num_predict
    budget before any content is emitted (qwen3.6 emits ~10k chars of
    reasoning trace for a single trade-narrative prompt — enough to
    exhaust most reasonable caps). For non-thinking models the param is
    a no-op."""

    def __init__(self, model: str = DEFAULT_OLLAMA_MODEL):
        self.model = model

    def generate(self, system: str, prompt: str) -> str:
        # Imported lazily so unit tests with a FakeBackend don't need ollama
        # available at import time.
        import ollama
        log.debug("OllamaBackend.generate model=%s prompt_chars=%d", self.model, len(prompt))
        kwargs = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "options": {
                "temperature": 0.2,    # low, but not zero — narrative phrasing
                "num_predict": 800,    # 2-3 sentences fit easily; headroom for any thinking that slips through
            },
        }
        # `think=False` is supported by recent ollama python lib versions for
        # thinking models. Older versions / non-thinking models may not
        # accept it; we degrade gracefully.
        try:
            response = ollama.chat(think=False, **kwargs)
        except (TypeError, ValueError):
            response = ollama.chat(**kwargs)
        # The ollama package returns a pydantic Message; access via attr.
        return response.message.content or ""


# =============================================================================
# Cluster + prompt construction
# =============================================================================


@dataclass
class HsGroupCluster:
    """Findings clustered for one HS group across both flows. Either side
    may be missing (group has yoy_imports but no yoy_exports yet, etc.) —
    the prompt builder handles that."""
    group_id: int
    group_name: str
    group_description: str | None
    hs_patterns: list[str]
    # Latest active findings:
    yoy_import: dict | None = None
    yoy_export: dict | None = None
    trajectory_import: dict | None = None
    trajectory_export: dict | None = None
    # Union of underlying finding ids for provenance.
    underlying_finding_ids: list[int] = field(default_factory=list)
    # Union of caveat codes across the underlying findings.
    caveat_codes: set[str] = field(default_factory=set)


def _conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _load_hs_group_clusters(group_names: list[str] | None = None) -> list[HsGroupCluster]:
    """Build one HsGroupCluster per hs_group from the latest active findings.
    Skips groups with no underlying findings at all (they have nothing to
    narrate)."""
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        if group_names:
            cur.execute(
                "SELECT id, name, description, hs_patterns FROM hs_groups "
                "WHERE name = ANY(%s) ORDER BY id",
                (list(group_names),),
            )
        else:
            cur.execute(
                "SELECT id, name, description, hs_patterns FROM hs_groups ORDER BY id"
            )
        groups = cur.fetchall()

        clusters: list[HsGroupCluster] = []
        for g in groups:
            cluster = HsGroupCluster(
                group_id=g["id"], group_name=g["name"],
                group_description=g["description"], hs_patterns=list(g["hs_patterns"]),
            )
            for subkind, attr in [
                ("hs_group_yoy", "yoy_import"),
                ("hs_group_yoy_export", "yoy_export"),
                ("hs_group_trajectory", "trajectory_import"),
                ("hs_group_trajectory_export", "trajectory_export"),
            ]:
                cur.execute(
                    """
                    SELECT id, detail FROM findings
                     WHERE subkind = %s AND %s = ANY(hs_group_ids)
                       AND superseded_at IS NULL
                  ORDER BY (detail->'windows'->>'current_end')::date DESC NULLS LAST,
                           created_at DESC
                     LIMIT 1
                    """,
                    (subkind, g["id"]),
                )
                row = cur.fetchone()
                if row is not None:
                    setattr(cluster, attr, dict(row["detail"]))
                    cluster.underlying_finding_ids.append(row["id"])
                    cluster.caveat_codes.update(row["detail"].get("caveat_codes") or [])
            if cluster.underlying_finding_ids:
                clusters.append(cluster)
        return clusters


def _build_facts(cluster: HsGroupCluster) -> dict[str, Any]:
    """Extract the typed facts the LLM is allowed to use. Each is a (label,
    value, kind) tuple internally; the returned dict is what we serialise
    into the prompt and what the verifier checks against.

    Editorial intent: the LLM is told these are the ONLY numbers it may
    cite. Any deviation gets the narrative rejected at verification time.
    """
    facts: dict[str, Any] = {
        "group_name": cluster.group_name,
        "hs_patterns": cluster.hs_patterns,
        "caveats": sorted(cluster.caveat_codes),
    }
    if cluster.yoy_import:
        t = cluster.yoy_import.get("totals", {})
        w = cluster.yoy_import.get("windows", {})
        facts["imports"] = {
            "period_end": w.get("current_end"),
            "yoy_pct": t.get("yoy_pct"),
            "yoy_pct_kg": t.get("yoy_pct_kg"),
            "current_12mo_eur": t.get("current_12mo_eur"),
            "prior_12mo_eur": t.get("prior_12mo_eur"),
            "unit_price_pct_change": t.get("unit_price_pct_change"),
            "low_base": t.get("low_base"),
            "decomposition_suppressed": t.get("decomposition_suppressed"),
            "partial_window": t.get("partial_window"),
        }
    if cluster.yoy_export:
        t = cluster.yoy_export.get("totals", {})
        w = cluster.yoy_export.get("windows", {})
        facts["exports"] = {
            "period_end": w.get("current_end"),
            "yoy_pct": t.get("yoy_pct"),
            "yoy_pct_kg": t.get("yoy_pct_kg"),
            "current_12mo_eur": t.get("current_12mo_eur"),
            "prior_12mo_eur": t.get("prior_12mo_eur"),
            "unit_price_pct_change": t.get("unit_price_pct_change"),
            "low_base": t.get("low_base"),
            "decomposition_suppressed": t.get("decomposition_suppressed"),
            "partial_window": t.get("partial_window"),
        }
    if cluster.trajectory_import:
        f = cluster.trajectory_import.get("features", {})
        facts["trajectory_imports"] = {
            "shape": cluster.trajectory_import.get("shape"),
            "shape_label": cluster.trajectory_import.get("shape_label"),
            "last_yoy": f.get("last_yoy"),
            "max_yoy": f.get("max_yoy"),
            "min_yoy": f.get("min_yoy"),
            "n_windows": f.get("n"),
            "has_strong_seasonal_signal": f.get("has_strong_seasonal_signal"),
        }
    if cluster.trajectory_export:
        f = cluster.trajectory_export.get("features", {})
        facts["trajectory_exports"] = {
            "shape": cluster.trajectory_export.get("shape"),
            "shape_label": cluster.trajectory_export.get("shape_label"),
            "last_yoy": f.get("last_yoy"),
            "max_yoy": f.get("max_yoy"),
            "min_yoy": f.get("min_yoy"),
            "n_windows": f.get("n"),
            "has_strong_seasonal_signal": f.get("has_strong_seasonal_signal"),
        }
    return facts


SYSTEM_PROMPT = """You are an editorial assistant for Guardian trade journalists.
You convert structured trade-statistics findings into 2-3 sentence top-lines
suitable for a copy desk. You are NOT a researcher and you do NOT compute.

Strict rules — violating any of these gets your output silently rejected:

1. EVERY NUMBER YOU CITE MUST APPEAR IN THE FACTS BLOCK. Do not estimate,
   round to a different value, or interpolate. If the fact is 0.342, you
   may write "34%" or "34.2%" — but NOT "35%" or "33%" or "around 30%".
   If a fact is in fraction form (e.g. 0.342 means 34.2%), convert
   directly without arithmetic.
2. PREFER QUALITATIVE PHRASING WHEN A SINGLE NUMBER WOULD BE MISLEADING.
   "Sharp double-digit growth" is allowed; "+37% YoY" when the fact is
   +34.2% is not.
3. If a caveat applies (low_base, partial_window, transshipment_hub,
   low_kg_coverage, cn8_revision, multi_partner_sum), hedge in your
   prose. Never quote a percentage from a low_base finding without
   flagging the small base.
4. NEVER claim "volume-driven" or "price-driven" when
   decomposition_suppressed is true.
5. Output prose only — no bullet lists, no preamble ("Here is the
   top-line:"), no markdown headings, no HS codes. Just 2-3 sentences.
6. "Imports" / "exports" are from the EU's perspective: imports = goods
   coming INTO the EU from China; exports = EU goods going TO China."""


_PCT_KEYS = {"yoy_pct", "yoy_pct_kg", "unit_price_pct_change", "last_yoy",
             "max_yoy", "min_yoy"}
_EUR_KEYS = {"current_12mo_eur", "prior_12mo_eur"}


def _format_facts_for_prompt(facts: dict[str, Any]) -> dict[str, Any]:
    """Convert raw numeric facts into the human-readable forms the LLM
    should cite directly. Percentages get their % suffix; EUR gets its €
    symbol and B/M/k unit. The verifier still uses the underlying raw
    numbers — this layer just removes the fraction→percentage mental
    conversion that LLMs reliably get wrong."""
    formatted: dict[str, Any] = {}
    for k, v in facts.items():
        if k == "hs_patterns":
            # Don't surface HS codes — they look like numbers and trigger
            # false-positive verification failures. Group name is the
            # editorial handle.
            continue
        if isinstance(v, dict):
            formatted[k] = _format_facts_for_prompt(v)
        elif isinstance(v, list):
            formatted[k] = v
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            if k in _PCT_KEYS:
                formatted[k] = f"{v * 100:+.1f}%"
            elif k in _EUR_KEYS:
                formatted[k] = _format_eur_for_prompt(v)
            else:
                formatted[k] = v
        else:
            formatted[k] = v
    return formatted


def _format_eur_for_prompt(v: float) -> str:
    """Mirror the briefing-pack _fmt_eur formatting so the LLM sees the
    same shape the rest of the pipeline emits."""
    n = float(v)
    if abs(n) >= 1e9:
        return f"€{n / 1e9:.2f}B"
    if abs(n) >= 1e6:
        return f"€{n / 1e6:.1f}M"
    if abs(n) >= 1e3:
        return f"€{n / 1e3:.1f}k"
    return f"€{n:.0f}"


def _build_user_prompt(cluster: HsGroupCluster, facts: dict[str, Any]) -> str:
    formatted = _format_facts_for_prompt(facts)
    return (
        f"Group: {cluster.group_name}\n"
        f"Definition: {cluster.group_description or '—'}\n\n"
        f"FACTS (these are the only numbers you may cite — quote them directly,\n"
        f"do not perform arithmetic on them):\n"
        f"{json.dumps(formatted, indent=2, default=str)}\n\n"
        f"Write a 2-3 sentence top-line for the desk. Do NOT cite HS codes."
    )


# =============================================================================
# Numeric verification — the discipline that makes this safe to ship
# =============================================================================


_NUMBER_RE = re.compile(
    # Captures: optional sign, digits (with optional comma-thousands grouping
    # OR plain digits — both forms valid), optional decimal, optional unit.
    # Leading boundary: (?<![A-Za-z0-9]) prevents "CN8" / "HS850760" / etc.
    # parsing as "8" / "850760". Unit alternation has two arms: '%' alone
    # (no \b because % isn't a word char), and alpha units with trailing \b
    # so "39 months" doesn't parse as "39M". Long-form unit words listed
    # before short forms so longest-first rule wins ("billion" > "b").
    r"(?<![A-Za-z0-9])(?:€\s*)?([+-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)"
    r"\s*(%|(?:billion|million|thousand|kg|bn|B|M|k|m)\b)?",
    re.IGNORECASE,
)
_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
# Time-period phrasings ("12 months", "24-month", "in 6 weeks") are
# editorial scaffolding, not facts to verify. Strip them before extraction
# so a "rolling 12 months" reference doesn't fail verification.
_TIME_PERIOD_RE = re.compile(
    r"\b\d+(?:[-\s]+)(?:month|year|day|week|quarter|period|window)s?\b",
    re.IGNORECASE,
)

# Words that signal a *decrease* — used for sign-inference around unsigned
# numbers in LLM prose. Matches verb forms ("decreased", "fell"), noun forms
# ("decrease", "decline", "drop"), and short adverbs ("down"). We allow the
# keyword to appear within a 40-char window EITHER side of the number, so
# all of these phrasings round-trip:
#   "decreased by 36.8%"  /  "decline of 36.8%"  /  "36.8% drop"  /
#   "down 36.8%"  /  "36.8% reduction"  /  "fell 36.8%"
_DECREASE_KEYWORDS = re.compile(
    r"\b(decreas\w*|declin\w*|fell|fall\w*|dropp?\w*|drop\b|reduc\w*|"
    r"cut\w*|down\b|lost|loss\w*)\b",
    re.IGNORECASE,
)
_CONTEXT_WINDOW_CHARS = 40


def _has_decrease_context(text: str, match_start: int, match_end: int) -> bool:
    """True if a decrease-keyword appears within _CONTEXT_WINDOW_CHARS of
    the matched-number span."""
    lo = max(0, match_start - _CONTEXT_WINDOW_CHARS)
    hi = min(len(text), match_end + _CONTEXT_WINDOW_CHARS)
    return _DECREASE_KEYWORDS.search(text[lo:hi]) is not None


def _collect_facts_numeric(facts: dict, into: list[tuple[str, float]] | None = None,
                           prefix: str = "") -> list[tuple[str, float]]:
    """Walk the facts dict and collect every numeric value with a path label.
    Returns [(path, value), ...] e.g. ('imports.yoy_pct', 0.342)."""
    if into is None:
        into = []
    if isinstance(facts, dict):
        for k, v in facts.items():
            path = f"{prefix}.{k}" if prefix else k
            _collect_facts_numeric(v, into, path)
    elif isinstance(facts, list):
        for i, v in enumerate(facts):
            _collect_facts_numeric(v, into, f"{prefix}[{i}]")
    elif isinstance(facts, (int, float)) and not isinstance(facts, bool):
        into.append((prefix, float(facts)))
    return into


def _extract_numbers_from_text(text: str) -> list[tuple[str, float, str]]:
    """Extract (raw_match, parsed_value, kind) triples from the LLM output.
    `kind` ∈ {'pct', 'currency', 'count'}. Currency parsed to EUR-base.

    Sign inference: when a number is unsigned in the prose but has a
    decrease-keyword in its 40-char context window ("a 36.8% drop", "a
    decrease of 36.8%", "fell 36.8%"), we treat its parsed value as
    negative so it can match a fact stored as a negative fraction
    (e.g. yoy_pct=-0.368). Already-signed values pass through unchanged.

    Calendar years (19xx / 20xx) are pre-stripped from the text so they
    don't accidentally enter the verification pipeline (a year 'matched'
    against a fact would always fail, generating false positives)."""
    text_for_extraction = _YEAR_RE.sub("YEAR", text)
    text_for_extraction = _TIME_PERIOD_RE.sub("PERIOD", text_for_extraction)
    out: list[tuple[str, float, str]] = []
    for m in _NUMBER_RE.finditer(text_for_extraction):
        raw = m.group(0).strip()
        num_str = m.group(1).replace(",", "")
        unit = (m.group(2) or "").lower()
        try:
            n = float(num_str)
        except ValueError:
            continue
        # Apply sign inference ONLY to percentages, not to currency values.
        # A currency value is a stock — "exports fell to €441M" leaves the
        # €441M positive; the negative is in the percentage of decline that
        # produced it. Sign-flipping a stock by directional verbs in the
        # surrounding prose is the wrong semantic.
        is_pct = unit == "%"
        if is_pct and not (num_str.startswith("+") or num_str.startswith("-")):
            if _has_decrease_context(text_for_extraction, m.start(), m.end()):
                n = -n
        if unit == "%":
            # Convert "+34%" or "34%" to fraction 0.34 to match facts representation.
            out.append((raw, n / 100.0, "pct"))
        elif unit in ("b", "bn", "billion"):
            # "€27B" or "€27 billion" → 27e9 EUR
            out.append((raw, n * 1e9, "currency"))
        elif unit in ("m", "million"):
            out.append((raw, n * 1e6, "currency"))
        elif unit in ("k", "thousand"):
            out.append((raw, n * 1e3, "currency"))
        elif unit == "kg":
            # kg figures are facts-side numbers too (yoy_pct_kg etc.).
            # Treat as raw count; verifier will compare directly to facts.
            out.append((raw, n, "count"))
        else:
            # No unit — count. Calendar years were already substituted
            # to "YEAR" in the pre-processed text, so they don't reach
            # this branch.
            out.append((raw, n, "count"))
    return out


@dataclass
class VerificationFailure:
    raw_text: str
    parsed_value: float
    kind: str
    closest_fact_path: str | None
    closest_fact_value: float | None


def verify_numbers(
    text: str, facts: dict[str, Any],
) -> tuple[bool, list[VerificationFailure]]:
    """Walk every number in `text` and assert each matches a fact within
    tolerance. Returns (ok, failures). An empty text or text with no
    numbers returns (True, []) — abstaining from numbers is fine."""
    fact_numbers = _collect_facts_numeric(facts)
    extracted = _extract_numbers_from_text(text)
    failures: list[VerificationFailure] = []
    for raw, val, kind in extracted:
        match, closest_path, closest_val = _find_closest_fact(val, kind, fact_numbers)
        if not match:
            failures.append(VerificationFailure(
                raw_text=raw, parsed_value=val, kind=kind,
                closest_fact_path=closest_path, closest_fact_value=closest_val,
            ))
    return (not failures, failures)


def _find_closest_fact(
    value: float, kind: str, facts: list[tuple[str, float]],
) -> tuple[bool, str | None, float | None]:
    """Return (match, path, fact_value) for the closest fact within tolerance.
    Match=False if no fact is within tolerance for this kind.

    For percentages we try sign-aware match first (the cleaner signal), then
    fall back to a magnitude-only match. The fallback exists because LLM
    prose ambiguity (cross-clause confusion like "+69.2% increase ... 20.7%
    drop in unit prices" makes context-window sign inference flip "+69.2%"
    incorrectly) produces false-negatives that aren't editorial errors. If
    the LLM is fundamentally direction-wrong (says "+37%" when fact is
    "-36.8%"), the magnitude-only fallback still catches the hallucination
    because nothing matches."""
    if not facts:
        return False, None, None
    best_path: str | None = None
    best_val: float | None = None
    best_dist: float = float("inf")
    for path, fact_val in facts:
        if kind == "pct":
            # Sign-aware first.
            dist = abs(value - fact_val)
            within = dist <= PCT_TOLERANCE_ABS
        elif kind == "currency":
            # ±5% relative
            if fact_val == 0:
                within = value == 0
                dist = abs(value - fact_val)
            else:
                dist = abs(value - fact_val) / max(abs(fact_val), 1.0)
                within = dist <= CURRENCY_TOLERANCE_REL
        else:  # count
            # Exact-match for integers; tolerate ±0.5 for floats.
            dist = abs(value - fact_val)
            within = dist <= 0.5
        if within and dist < best_dist:
            best_dist = dist
            best_path = path
            best_val = fact_val
    if best_path is not None:
        return True, best_path, best_val
    # Percentage magnitude-only fallback.
    if kind == "pct":
        for path, fact_val in facts:
            dist = abs(abs(value) - abs(fact_val))
            if dist <= PCT_TOLERANCE_ABS and dist < best_dist:
                best_dist = dist
                best_path = path
                best_val = fact_val
        if best_path is not None:
            return True, best_path, best_val
    # No match — record the absolute closest for the failure report.
    closest_path: str | None = None
    closest_val: float | None = None
    closest_dist: float = float("inf")
    for path, fact_val in facts:
        d = abs(value - fact_val)
        if d < closest_dist:
            closest_dist = d
            closest_path = path
            closest_val = fact_val
    return False, closest_path, closest_val


# =============================================================================
# Persistence
# =============================================================================


def _persist_narrative(
    cur, *, scrape_run_id: int, cluster: HsGroupCluster,
    narrative_text: str, facts: dict[str, Any], model_used: str,
) -> tuple[int, findings_io.EmitAction]:
    detail = {
        "method": NARRATIVE_METHOD,
        "model": model_used,
        "cluster_kind": "hs_group",
        "group": {"id": cluster.group_id, "name": cluster.group_name,
                  "hs_patterns": cluster.hs_patterns},
        "narrative_text": narrative_text,
        "facts_used": facts,
        "underlying_finding_ids": sorted(cluster.underlying_finding_ids),
        "caveat_codes": sorted(cluster.caveat_codes | {"llm_drafted"}),
    }
    return findings_io.emit_finding(
        cur,
        scrape_run_id=scrape_run_id,
        kind="llm_topline",
        subkind="narrative_hs_group",
        natural_key=(cluster.group_id,),
        # Including the narrative text in value_fields means the supersede
        # chain captures any prose change, not just numeric/source change.
        # That's intentional: a regenerated narrative IS a revision.
        value_fields={
            "method": NARRATIVE_METHOD,
            "model": model_used,
            "narrative_text": narrative_text,
            "underlying_finding_ids": sorted(cluster.underlying_finding_ids),
        },
        hs_group_ids=[cluster.group_id],
        score=None,  # narratives don't sort by magnitude
        title=f"Top-line: {cluster.group_name}",
        body=narrative_text,
        detail=detail,
    )


# =============================================================================
# Top-level orchestrator
# =============================================================================


def detect_llm_framings(
    group_names: list[str] | None = None,
    backend: LLMBackend | None = None,
    model: str | None = None,
) -> dict[str, int]:
    """Generate one narrative per HS group (per cluster), persist as
    `narrative_hs_group` findings.

    Returns counts: {emitted, inserted_new, confirmed_existing, superseded,
                     skipped_no_findings, skipped_unverified,
                     skipped_llm_error}.
    """
    if backend is None:
        backend = make_backend(model=model)
    counts = {
        "emitted": 0,
        "inserted_new": 0, "confirmed_existing": 0, "superseded": 0,
        "skipped_no_findings": 0,
        "skipped_unverified": 0,
        "skipped_llm_error": 0,
    }

    clusters = _load_hs_group_clusters(group_names)
    if not clusters:
        log.info("No clusters with active findings; nothing to narrate.")
        return counts

    model_used = getattr(backend, "model", "unknown")

    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'running') RETURNING id",
            (LLM_FRAMING_SOURCE_URL,),
        )
        analysis_run_id = cur.fetchone()[0]
    try:
        for cluster in clusters:
            facts = _build_facts(cluster)
            user_prompt = _build_user_prompt(cluster, facts)
            try:
                narrative = backend.generate(SYSTEM_PROMPT, user_prompt).strip()
            except Exception as e:
                log.exception("LLM error for group %r", cluster.group_name)
                counts["skipped_llm_error"] += 1
                continue

            ok, failures = verify_numbers(narrative, facts)
            if not ok:
                log.warning(
                    "Numeric verification FAILED for %r: %d unmatched numbers. "
                    "Sample: %r → closest fact %s=%s (parsed %.4f). Skipping.",
                    cluster.group_name, len(failures),
                    failures[0].raw_text, failures[0].closest_fact_path,
                    failures[0].closest_fact_value, failures[0].parsed_value,
                )
                counts["skipped_unverified"] += 1
                continue

            with _conn() as conn2, conn2.cursor() as cur2:
                _, action = _persist_narrative(
                    cur2, scrape_run_id=analysis_run_id, cluster=cluster,
                    narrative_text=narrative, facts=facts, model_used=model_used,
                )
                conn2.commit()
            _tally_action(counts, action)

        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='success', ended_at=now() WHERE id=%s",
                (analysis_run_id,),
            )
    except Exception as e:
        log.exception("LLM framing run failed")
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_runs SET status='failed', error_message=%s, ended_at=now() WHERE id=%s",
                (str(e), analysis_run_id),
            )
        raise

    return counts


def _tally_action(counts: dict, action: findings_io.EmitAction) -> None:
    counts[action] = counts.get(action, 0) + 1
    counts["emitted"] = counts.get("emitted", 0) + 1
