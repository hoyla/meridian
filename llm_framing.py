"""LLM lead-scaffold layer over the deterministic findings.

Phase 6.4 of dev_notes/history.md restructures the original
v1 narrative-drafting layer (Phase 3) into a lead-scaffolding layer.
The earlier shape — "draft a 2-3 sentence top-line per HS group" —
was journalistically useful but invited the LLM to do editorial work
the data alone couldn't justify. The new shape:

For each HS group with active findings:

1. **Anomaly summary** — one short factual line (LLM-drafted, fact-verified).
2. **Selected hypotheses** — 2-3 items picked from a curated catalog of
   standard causal explanations for China-EU/UK trade movements, each
   with a one-line LLM-written rationale that cites a specific fact.
3. **Corroboration steps** — concrete checks a journalist can run to
   test the hypotheses. Pulled deterministically from the catalog
   entries the LLM picked. The LLM does NOT invent these.

The fact-verification discipline carries through: every number cited
in the anomaly summary or any rationale must round-trip to a fact
within tolerance, or the whole lead-scaffold is rejected. Hypothesis
IDs that aren't in the catalog also cause rejection. Editorial cost:
silence on that group. Editorial benefit: never confidently wrong;
journalist gets a starting position, not a finished story.

Method: `llm_topline_v2_lead_scaffold`. The supersede chain handles
the cutover from v1 — re-running on a DB that has v1 narratives will
produce v2 leads that supersede them via the same natural key
(group_id,).
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
from hypothesis_catalog import (
    CATALOG_BY_ID,
    get_catalog_for_prompt,
    get_catalog_ids,
    get_corroboration_steps,
)

log = logging.getLogger(__name__)

DEFAULT_OLLAMA_MODEL = "qwen3.6:latest"
LLM_FRAMING_SOURCE_URL = "analysis://llm_framing/v2"

LEAD_METHOD = "llm_topline_v2_lead_scaffold"

# Tolerance bands for numeric verification. Percentages get a 0.5pp
# absolute tolerance because LLMs naturally round (+34.2% → "34%").
# Currency values get an order-of-magnitude check (the LLM might say "€27B"
# for €26.9B; we accept ±5% relative). Pure integer counts must match exactly.
PCT_TOLERANCE_ABS = 0.005     # 0.5 percentage points (the value is in fraction form, 0.342 not 34.2)
CURRENCY_TOLERANCE_REL = 0.05  # 5% relative

# Hard cap on hypotheses per scaffold. The LLM is asked for 2-3; if it
# returns more we truncate (rather than reject) — the editorial harm of
# truncation is bounded.
MAX_HYPOTHESES_PER_LEAD = 3


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
    """Calls a local Ollama daemon's /api/chat endpoint with format='json'.

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
            "format": "json",      # ollama enforces JSON output for structured response
            "options": {
                "temperature": 0.2,    # low — narrative phrasing in rationales but no creative leaps
                "num_predict": 1200,   # JSON envelope + 3 hypotheses with rationales fits comfortably
            },
        }
        # `think=False` is supported by recent ollama python lib versions for
        # thinking models. Older versions / non-thinking models may not
        # accept it; we degrade gracefully.
        try:
            response = ollama.chat(think=False, **kwargs)
        except (TypeError, ValueError):
            response = ollama.chat(**kwargs)
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
    scaffold)."""
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
    """Extract the typed facts the LLM is allowed to use. Same shape as v1 —
    the verifier uses these as the universe of allowed numbers."""
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


SYSTEM_PROMPT = """You are an editorial research assistant for Guardian trade journalists.
Your job is to scaffold an investigation, NOT to write a finished story.

For each HS group you receive, you produce a JSON object with three fields:

1. `anomaly_summary`: ONE short sentence stating the unusual movement in plain
   English. Cite specific numbers from the FACTS block only.
2. `hypotheses`: a list of 2-3 items, each `{id, rationale}`, where `id` is
   one of the catalog ids supplied in the prompt (HYPOTHESIS CATALOG section)
   and `rationale` is a SINGLE sentence (max ~30 words) explaining why this
   hypothesis fits the facts. Cite at least one specific fact per rationale.
3. (Corroboration steps are NOT in your output — they are attached
   deterministically from the catalog after you submit.)

Strict rules — violating any of these gets your output silently rejected:

A. EVERY NUMBER YOU CITE (in the anomaly_summary OR any rationale) MUST APPEAR
   IN THE FACTS BLOCK. Do not estimate, round to a different value, or
   interpolate. If the fact is "+34.2%", you may write "34%" or "34.2%" — but
   NOT "35%" or "around 30%".
B. EVERY HYPOTHESIS ID YOU PICK MUST APPEAR IN THE CATALOG. Do not invent
   new ids. Do not modify existing ids.
C. PICK 2-3 HYPOTHESES, NOT MORE. Pick the ones most clearly supported by
   the facts. If only one fits, pick one. If none fit (rare, but possible
   for very flat / featureless groups), return an empty hypotheses list.
D. If a caveat applies (low_base, partial_window, transshipment_hub,
   low_kg_coverage, cn8_revision, multi_partner_sum), let it shape your
   hypothesis selection (e.g. cn8_revision strongly suggests cn8_reclassification).
   Mention the caveat by name in the relevant rationale when it directly
   motivates the pick.
E. NEVER claim "volume-driven" or "price-driven" when decomposition_suppressed
   is true.
F. "Imports" / "exports" in the FACTS block are from the EU-27 reporter's
   perspective: `imports` = goods coming INTO the EU-27 from China;
   `exports` = EU-27 goods going TO China.
G. ALWAYS NAME THE PARTIES EXPLICITLY in any direction reference. Don't
   write "imports rose" — write "EU-27 imports from China rose". Don't
   write "exports collapsed" — write "EU-27 exports to China collapsed".
   This applies to the anomaly_summary AND every rationale. A journalist
   reading the lead doesn't share your context; spell it out.
H. Output VALID JSON ONLY. No markdown, no preamble, no code fences."""


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
    catalog = get_catalog_for_prompt()
    return (
        f"Group: {cluster.group_name}\n"
        f"Definition: {cluster.group_description or '—'}\n\n"
        f"PERSPECTIVE — all numbers below are EU-27 trade with China "
        f"(Eurostat-side, partners CN+HK+MO summed). When you cite a "
        f"figure in the anomaly_summary or any rationale, ALWAYS name "
        f"the parties explicitly:\n"
        f"  - `imports` → write \"EU-27 imports from China\"\n"
        f"  - `exports` → write \"EU-27 exports to China\"\n"
        f"  - `trajectory_imports` / `trajectory_exports` → same convention\n"
        f"Never write bare \"imports rose\" or \"exports fell\" — a "
        f"journalist reading the lead doesn't share your context.\n\n"
        f"FACTS (the only numbers you may cite):\n"
        f"{json.dumps(formatted, indent=2, default=str)}\n\n"
        f"HYPOTHESIS CATALOG (pick 2-3 ids that best fit the facts):\n"
        f"{json.dumps(catalog, indent=2)}\n\n"
        f"Output a JSON object with exactly these keys: anomaly_summary "
        f"(string), hypotheses (list of {{id, rationale}}). No other keys, "
        f"no preamble, no code fences."
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
# HS-code references: groups whose name embeds an HS code (e.g.
# "Antibiotics (HS 2941)") prompt the LLM to write that code into the
# anomaly summary or rationale ("imports under HS 2941..."). The code is
# editorial scaffolding, not a fact to verify. Strip patterns of the form
# "HS NNNN" (4-8 digits) before extraction so the verifier doesn't pick
# up a 4-digit HS code as an unverifiable count.
_HS_CODE_RE = re.compile(r"\bHS\s*\d{4,8}\b", re.IGNORECASE)
# Geo-economic labels: when the prompt requires the LLM to write
# "EU-27 imports from China" (rule G), the bare "27" in "EU-27" reaches
# number extraction as an unverifiable count. Strip "EU-NN" / "G7" /
# "G20" / "G-7" style labels before extraction. Required after Phase 6.4
# evening rule-G addition; without it, every other lead would falsely
# fail verification.
_GEO_LABEL_RE = re.compile(r"\b(?:EU-?\d{1,3}|G-?\d{1,3})\b")

# Words that signal a *decrease* — used for sign-inference around unsigned
# numbers in LLM prose. Matches verb forms ("decreased", "fell"), noun forms
# ("decrease", "decline", "drop"), and short adverbs ("down").
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
    negative so it can match a fact stored as a negative fraction.

    Calendar years (19xx / 20xx) are pre-stripped so they don't enter the
    verification pipeline as false-positive failures."""
    text_for_extraction = _YEAR_RE.sub("YEAR", text)
    text_for_extraction = _TIME_PERIOD_RE.sub("PERIOD", text_for_extraction)
    text_for_extraction = _HS_CODE_RE.sub("HSCODE", text_for_extraction)
    text_for_extraction = _GEO_LABEL_RE.sub("GEOLABEL", text_for_extraction)
    out: list[tuple[str, float, str]] = []
    for m in _NUMBER_RE.finditer(text_for_extraction):
        raw = m.group(0).strip()
        num_str = m.group(1).replace(",", "")
        unit = (m.group(2) or "").lower()
        try:
            n = float(num_str)
        except ValueError:
            continue
        # Sign inference applies to percentages only — currency stocks
        # ("fell to €441M") leave the magnitude positive.
        is_pct = unit == "%"
        if is_pct and not (num_str.startswith("+") or num_str.startswith("-")):
            if _has_decrease_context(text_for_extraction, m.start(), m.end()):
                n = -n
        if unit == "%":
            out.append((raw, n / 100.0, "pct"))
        elif unit in ("b", "bn", "billion"):
            out.append((raw, n * 1e9, "currency"))
        elif unit in ("m", "million"):
            out.append((raw, n * 1e6, "currency"))
        elif unit in ("k", "thousand"):
            out.append((raw, n * 1e3, "currency"))
        elif unit == "kg":
            out.append((raw, n, "count"))
        else:
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

    For percentages we try sign-aware match first, then fall back to a
    magnitude-only match. The fallback rescues cross-clause prose ambiguity
    where context-window sign inference flips wrongly. A fundamentally
    direction-wrong claim (says "+37%" when fact is "-36.8%") still fails
    because nothing matches at any sign within tolerance."""
    if not facts:
        return False, None, None
    best_path: str | None = None
    best_val: float | None = None
    best_dist: float = float("inf")
    for path, fact_val in facts:
        if kind == "pct":
            dist = abs(value - fact_val)
            within = dist <= PCT_TOLERANCE_ABS
        elif kind == "currency":
            if fact_val == 0:
                within = value == 0
                dist = abs(value - fact_val)
            else:
                dist = abs(value - fact_val) / max(abs(fact_val), 1.0)
                within = dist <= CURRENCY_TOLERANCE_REL
        else:  # count
            dist = abs(value - fact_val)
            within = dist <= 0.5
        if within and dist < best_dist:
            best_dist = dist
            best_path = path
            best_val = fact_val
    if best_path is not None:
        return True, best_path, best_val
    if kind == "pct":
        for path, fact_val in facts:
            dist = abs(abs(value) - abs(fact_val))
            if dist <= PCT_TOLERANCE_ABS and dist < best_dist:
                best_dist = dist
                best_path = path
                best_val = fact_val
        if best_path is not None:
            return True, best_path, best_val
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
# JSON parsing + lead-scaffold validation
# =============================================================================


@dataclass
class LeadScaffoldRejection:
    reason: str
    detail: str = ""


@dataclass
class LeadScaffold:
    anomaly_summary: str
    hypotheses: list[dict]               # [{id, label, rationale}]
    corroboration_steps: list[str]       # derived deterministically from hypothesis ids


def _parse_lead_scaffold_json(raw: str) -> dict | LeadScaffoldRejection:
    """Parse the LLM's JSON output, tolerating a leading/trailing code fence."""
    s = raw.strip()
    # Strip ```json ... ``` or ``` ... ``` fences if the model added them
    # despite the prompt asking it not to.
    if s.startswith("```"):
        # Remove first fence line
        first_newline = s.find("\n")
        if first_newline != -1:
            s = s[first_newline + 1 :]
        if s.endswith("```"):
            s = s[: -3]
        s = s.strip()
    try:
        obj = json.loads(s)
    except json.JSONDecodeError as e:
        return LeadScaffoldRejection(reason="json_parse_error", detail=str(e))
    if not isinstance(obj, dict):
        return LeadScaffoldRejection(reason="json_not_object", detail=type(obj).__name__)
    return obj


def _validate_lead_scaffold(
    raw_obj: dict, facts: dict[str, Any],
) -> LeadScaffold | LeadScaffoldRejection:
    """Validate the parsed LLM output, returning a structured LeadScaffold or
    a typed rejection. Validation steps:

    1. Required keys present (anomaly_summary str, hypotheses list).
    2. Hypothesis ids all in catalog.
    3. Anomaly summary numerically verified.
    4. Each rationale numerically verified.
    5. Cap hypotheses at MAX_HYPOTHESES_PER_LEAD (truncate, not reject).

    The verifier accepts an empty hypotheses list (rare but valid for
    featureless groups). It does NOT accept a missing or non-string
    anomaly_summary."""
    summary = raw_obj.get("anomaly_summary")
    if not isinstance(summary, str) or not summary.strip():
        return LeadScaffoldRejection(reason="missing_anomaly_summary")
    hyps_in = raw_obj.get("hypotheses")
    if not isinstance(hyps_in, list):
        return LeadScaffoldRejection(reason="hypotheses_not_list")

    catalog_ids = set(get_catalog_ids())
    validated: list[dict] = []
    for item in hyps_in[:MAX_HYPOTHESES_PER_LEAD]:
        if not isinstance(item, dict):
            return LeadScaffoldRejection(reason="hypothesis_item_not_object")
        hid = item.get("id")
        rationale = item.get("rationale", "")
        if not isinstance(hid, str) or hid not in catalog_ids:
            return LeadScaffoldRejection(
                reason="unknown_hypothesis_id", detail=str(hid),
            )
        if not isinstance(rationale, str):
            return LeadScaffoldRejection(reason="rationale_not_string", detail=hid)
        # Verify any numbers in the rationale
        ok, failures = verify_numbers(rationale, facts)
        if not ok:
            return LeadScaffoldRejection(
                reason="rationale_failed_verification",
                detail=f"{hid}: {failures[0].raw_text} (parsed {failures[0].parsed_value:.4f})",
            )
        validated.append({
            "id": hid,
            "label": CATALOG_BY_ID[hid]["label"],
            "rationale": rationale.strip(),
        })

    # Verify numbers in the anomaly summary
    ok, failures = verify_numbers(summary, facts)
    if not ok:
        return LeadScaffoldRejection(
            reason="anomaly_summary_failed_verification",
            detail=f"{failures[0].raw_text} (parsed {failures[0].parsed_value:.4f})",
        )

    return LeadScaffold(
        anomaly_summary=summary.strip(),
        hypotheses=validated,
        corroboration_steps=get_corroboration_steps([h["id"] for h in validated]),
    )


def render_lead_scaffold_as_body(scaffold: LeadScaffold) -> str:
    """Render the structured scaffold to the markdown body that gets stored
    in `findings.body`. Briefing pack consumes detail.lead_scaffold directly
    for richer rendering; this body is the audit-friendly plain-text form."""
    lines: list[str] = []
    lines.append(f"**Anomaly:** {scaffold.anomaly_summary}")
    lines.append("")
    if scaffold.hypotheses:
        lines.append("**Possible causes:**")
        for h in scaffold.hypotheses:
            lines.append(f"- *{h['label']}* — {h['rationale']}")
        lines.append("")
    if scaffold.corroboration_steps:
        lines.append("**Corroboration steps:**")
        for s in scaffold.corroboration_steps:
            lines.append(f"- {s}")
    return "\n".join(lines).strip()


# =============================================================================
# Persistence
# =============================================================================


def _persist_lead(
    cur, *, scrape_run_id: int, cluster: HsGroupCluster,
    scaffold: LeadScaffold, facts: dict[str, Any], model_used: str,
) -> tuple[int, findings_io.EmitAction]:
    body = render_lead_scaffold_as_body(scaffold)
    detail = {
        "method": LEAD_METHOD,
        "model": model_used,
        "cluster_kind": "hs_group",
        "group": {"id": cluster.group_id, "name": cluster.group_name,
                  "hs_patterns": cluster.hs_patterns},
        "lead_scaffold": {
            "anomaly_summary": scaffold.anomaly_summary,
            "hypotheses": scaffold.hypotheses,
            "corroboration_steps": scaffold.corroboration_steps,
        },
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
        # Including the scaffold contents in value_fields means the supersede
        # chain captures any change to the picked hypotheses or rationales,
        # not just the anomaly summary. A regenerated scaffold IS a revision.
        value_fields={
            "method": LEAD_METHOD,
            "model": model_used,
            "anomaly_summary": scaffold.anomaly_summary,
            "hypothesis_ids": [h["id"] for h in scaffold.hypotheses],
            "rationales": [h["rationale"] for h in scaffold.hypotheses],
            "underlying_finding_ids": sorted(cluster.underlying_finding_ids),
        },
        hs_group_ids=[cluster.group_id],
        score=None,
        title=f"Lead: {cluster.group_name}",
        body=body,
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
    """Generate one lead-scaffold per HS group, persist as `narrative_hs_group`
    findings (subkind unchanged so the supersede chain catches v1→v2 cleanly).

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
        log.info("No clusters with active findings; nothing to scaffold.")
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
                raw_output = backend.generate(SYSTEM_PROMPT, user_prompt).strip()
            except Exception:
                log.exception("LLM error for group %r", cluster.group_name)
                counts["skipped_llm_error"] += 1
                continue

            parsed = _parse_lead_scaffold_json(raw_output)
            if isinstance(parsed, LeadScaffoldRejection):
                log.warning(
                    "Lead-scaffold parse rejected for %r: %s (%s)",
                    cluster.group_name, parsed.reason, parsed.detail,
                )
                counts["skipped_unverified"] += 1
                continue

            scaffold = _validate_lead_scaffold(parsed, facts)
            if isinstance(scaffold, LeadScaffoldRejection):
                log.warning(
                    "Lead-scaffold validation rejected for %r: %s (%s)",
                    cluster.group_name, scaffold.reason, scaffold.detail,
                )
                counts["skipped_unverified"] += 1
                continue

            with _conn() as conn2, conn2.cursor() as cur2:
                _, action = _persist_lead(
                    cur2, scrape_run_id=analysis_run_id, cluster=cluster,
                    scaffold=scaffold, facts=facts, model_used=model_used,
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
