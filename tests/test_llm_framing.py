"""Tests for the LLM lead-scaffold layer.

Phase 6.4 of dev_notes/history.md restructured the original
v1 narrative drafter into a v2 lead scaffolder. The discipline being
defended:

1. The LLM never computes — facts are pre-extracted; numbers are verified.
2. Every number cited (in anomaly_summary OR any rationale) must round-trip
   to a fact within tolerance, or the lead is rejected.
3. Every hypothesis id picked must exist in the curated catalog, or the
   lead is rejected.
4. Corroboration steps are sourced deterministically from the catalog —
   the LLM doesn't invent them.
5. Leads use the same append-plus-supersede chain as deterministic
   findings, idempotent on re-run with unchanged sources.

Tests use a FakeBackend that returns canned JSON so the suite never calls
Ollama. Live integration is exercised separately via the CLI smoke.
"""

import json
from datetime import date

import psycopg2
import pytest

import hypothesis_catalog
import llm_framing


# ---------------------------------------------------------------------------
# Fake backend
# ---------------------------------------------------------------------------


class FakeBackend:
    """Returns a fixed string regardless of the prompt. Tests can also
    construct one with `responses` keyed by group name to vary output per
    cluster."""

    def __init__(self, response: str = "", responses: dict[str, str] | None = None):
        self.response = response
        self.responses = responses or {}
        self.model = "fake-model"
        self.calls: list[tuple[str, str]] = []

    def generate(self, system: str, prompt: str) -> str:
        self.calls.append((system, prompt))
        for name, resp in self.responses.items():
            if name in prompt:
                return resp
        return self.response


def _scaffold_json(anomaly_summary: str, hypotheses: list[dict]) -> str:
    """Convenience: build the JSON payload the LLM is supposed to emit."""
    return json.dumps({
        "anomaly_summary": anomaly_summary,
        "hypotheses": hypotheses,
    })


# ---------------------------------------------------------------------------
# Pure-function tests: numeric verification (unchanged from v1)
# ---------------------------------------------------------------------------


def test_verify_numbers_accepts_text_with_no_numbers():
    """Abstaining from quoting numbers is fine — the LLM can write
    qualitative text and pass verification."""
    facts = {"imports": {"yoy_pct": 0.34}}
    ok, failures = llm_framing.verify_numbers(
        "EU demand for Chinese components remains strong, with a clear post-COVID rebound.",
        facts,
    )
    assert ok is True
    assert failures == []


def test_verify_numbers_accepts_rounded_percentage_within_tolerance():
    """LLM rounding +34.2% → '34%' is acceptable (PCT_TOLERANCE_ABS = 0.5pp)."""
    facts = {"imports": {"yoy_pct": 0.342}}
    ok, _ = llm_framing.verify_numbers("EU imports rose 34% in the year.", facts)
    assert ok is True


def test_verify_numbers_rejects_invented_percentage():
    """If the LLM cites a percentage not in the facts, verification fails."""
    facts = {"imports": {"yoy_pct": 0.342}}
    ok, failures = llm_framing.verify_numbers(
        "EU imports rose 60% in the year.", facts,
    )
    assert ok is False
    assert len(failures) == 1
    assert failures[0].kind == "pct"
    assert abs(failures[0].parsed_value - 0.6) < 1e-9


def test_verify_numbers_accepts_currency_within_tolerance():
    """€26.9B → '€27B' is within ±5% relative."""
    facts = {"imports": {"current_12mo_eur": 26_900_000_000.0}}
    ok, _ = llm_framing.verify_numbers("Imports totalled €27B.", facts)
    assert ok is True


def test_verify_numbers_rejects_invented_currency():
    """€26.9B claimed as €40B is well outside tolerance."""
    facts = {"imports": {"current_12mo_eur": 26_900_000_000.0}}
    ok, failures = llm_framing.verify_numbers("Imports totalled €40B.", facts)
    assert ok is False
    assert any(f.kind == "currency" for f in failures)


def test_verify_numbers_ignores_calendar_years():
    """A year like 2026 isn't treated as a fact-check candidate."""
    facts = {"imports": {"yoy_pct": 0.34}}
    ok, failures = llm_framing.verify_numbers(
        "In the year to February 2026, imports rose 34%.", facts,
    )
    assert ok is True
    assert failures == []


def test_verify_numbers_pct_magnitude_fallback_for_cross_clause_ambiguity():
    """Cross-clause ambiguity ('a 69.2% increase that offset a 20.7% drop')
    rescued by the magnitude-only fallback."""
    facts = {
        "imports": {"yoy_pct_kg": 0.692, "unit_price_pct_change": -0.207},
    }
    text = "a 69.2% increase in volume that offset a 20.7% drop in unit prices"
    ok, _ = llm_framing.verify_numbers(text, facts)
    assert ok is True


def test_verify_numbers_currency_not_sign_flipped_by_decrease_context():
    """Currency stocks aren't sign-flipped by movement verbs."""
    facts = {"exports": {"current_12mo_eur": 441_600_000.0}}
    text = "Exports fell to €441.6 million."
    ok, _ = llm_framing.verify_numbers(text, facts)
    assert ok is True


def test_verify_numbers_ignores_hs_chapter_references():
    """Groups whose name embeds an HS chapter number (e.g. 'Electrical
    equipment & machinery (chapters 84-85, broad)') prompt the LLM to
    write any of: 'chapter 84', 'chapters 84-85', 'HS-84/85', 'HS 84'.
    The bare 2-digit numbers should NOT reach number extraction."""
    facts = {"imports": {"yoy_pct": 0.342}}
    for phrasing in [
        "EU-27 imports rose 34% in chapters 84-85.",
        "EU-27 imports rose 34% in chapter 85, the main driver.",
        "EU-27 imports rose 34%; the broad HS-84/85 group is up.",
        "EU-27 imports rose 34%; HS 84 components led.",
    ]:
        ok, _ = llm_framing.verify_numbers(phrasing, facts)
        assert ok is True, f"failed on: {phrasing!r}"


def test_verify_numbers_ignores_geo_labels_like_eu27():
    """When rule G requires the LLM to write 'EU-27 imports from China',
    the bare '27' in 'EU-27' should NOT reach number extraction. Same
    for G7 / G20 / EU-15. Editorial scaffolding, not a fact to verify."""
    facts = {"imports": {"yoy_pct": 0.342}}
    ok, _ = llm_framing.verify_numbers(
        "EU-27 imports from China rose 34% while G7 demand stayed flat. "
        "EU-15 saw similar gains.",
        facts,
    )
    assert ok is True


def test_verify_numbers_ignores_hs_code_references():
    """Groups whose name embeds an HS code (e.g. 'Antibiotics (HS 2941)')
    prompt the LLM to write 'HS 2941' or 'HS 292429' into rationales.
    These are editorial scaffolding, not facts. The verifier strips them
    before extraction so they don't trigger false-positive failures."""
    facts = {"imports": {"yoy_pct": 0.342}}
    ok, _ = llm_framing.verify_numbers(
        "Imports under HS 2941 rose 34% in the year. The HS 2924 sub-bracket "
        "performed similarly to HS 292429 historically.",
        facts,
    )
    assert ok is True


def test_verify_numbers_walks_nested_facts():
    """Numbers at any depth count as 'available facts'."""
    facts = {
        "imports": {"yoy_pct": 0.342},
        "trajectory_imports": {
            "shape": "dip_recovery",
            "last_yoy": 0.342,
            "max_yoy": 0.48,
        },
    }
    ok, _ = llm_framing.verify_numbers(
        "Imports peaked at 48% YoY before easing to 34%.", facts,
    )
    assert ok is True


# ---------------------------------------------------------------------------
# Catalog tests
# ---------------------------------------------------------------------------


def test_catalog_ids_are_unique():
    """Catalog entries must have distinct ids; lookup by id assumes uniqueness."""
    ids = hypothesis_catalog.get_catalog_ids()
    assert len(ids) == len(set(ids))


def test_catalog_for_prompt_excludes_corroboration_steps():
    """Corroboration steps are attached deterministically post-hoc, not picked
    by the LLM. Excluding them from the prompt prevents the LLM from
    rephrasing them as creative output."""
    catalog = hypothesis_catalog.get_catalog_for_prompt()
    for entry in catalog:
        assert "corroboration_steps" not in entry
        assert set(entry.keys()) == {"id", "label", "description"}


def test_get_corroboration_steps_unions_picks_in_catalog_order():
    """Steps from all picked hypotheses, deduplicated, in the order they
    appear in the catalog."""
    picked = ["base_effect", "tariff_preloading"]
    steps = hypothesis_catalog.get_corroboration_steps(picked)
    # Catalog order: tariff_preloading appears before base_effect, so
    # tariff_preloading's steps come first regardless of pick order.
    expected_first = hypothesis_catalog.CATALOG_BY_ID["tariff_preloading"]["corroboration_steps"][0]
    assert steps[0] == expected_first
    # All steps from both hypotheses are present
    expected_total = (
        len(hypothesis_catalog.CATALOG_BY_ID["tariff_preloading"]["corroboration_steps"]) +
        len(hypothesis_catalog.CATALOG_BY_ID["base_effect"]["corroboration_steps"])
    )
    assert len(steps) == expected_total


def test_get_corroboration_steps_ignores_unknown_ids():
    """Unknown ids are skipped (validation should have rejected them earlier)."""
    steps = hypothesis_catalog.get_corroboration_steps(["totally_made_up_id"])
    assert steps == []


# ---------------------------------------------------------------------------
# Lead-scaffold parsing + validation
# ---------------------------------------------------------------------------


def test_parse_lead_scaffold_strips_code_fence():
    """LLMs sometimes wrap JSON in a markdown code fence despite being told
    not to. The parser tolerates it."""
    raw = '```json\n{"anomaly_summary": "x", "hypotheses": []}\n```'
    obj = llm_framing._parse_lead_scaffold_json(raw)
    assert isinstance(obj, dict)
    assert obj["anomaly_summary"] == "x"


def test_parse_lead_scaffold_rejects_invalid_json():
    raw = "not actually json {"
    result = llm_framing._parse_lead_scaffold_json(raw)
    assert isinstance(result, llm_framing.LeadScaffoldRejection)
    assert result.reason == "json_parse_error"


def test_validate_lead_scaffold_accepts_valid_payload():
    facts = {"imports": {"yoy_pct": 0.342, "current_12mo_eur": 2.69e10}}
    obj = {
        "anomaly_summary": "Imports rose 34% to €27B.",
        "hypotheses": [
            {"id": "tariff_preloading",
             "rationale": "The 34% YoY surge is consistent with importers pulling forward shipments."},
            {"id": "capacity_expansion_china",
             "rationale": "Sustained 34% growth suggests structural rather than one-off demand."},
        ],
    }
    result = llm_framing._validate_lead_scaffold(obj, facts)
    assert isinstance(result, llm_framing.LeadScaffold)
    assert result.anomaly_summary == "Imports rose 34% to €27B."
    assert len(result.hypotheses) == 2
    assert result.hypotheses[0]["label"] == "Tariff pre-loading"  # attached from catalog
    assert result.corroboration_steps  # deterministically derived


def test_validate_lead_scaffold_rejects_unknown_hypothesis_id():
    facts = {"imports": {"yoy_pct": 0.342}}
    obj = {
        "anomaly_summary": "Imports rose 34%.",
        "hypotheses": [
            {"id": "tariff_preloading", "rationale": "Imports rose 34%."},
            {"id": "wholly_invented_id", "rationale": "Imports rose 34%."},
        ],
    }
    result = llm_framing._validate_lead_scaffold(obj, facts)
    assert isinstance(result, llm_framing.LeadScaffoldRejection)
    assert result.reason == "unknown_hypothesis_id"


def test_validate_lead_scaffold_rejects_invented_number_in_rationale():
    """A hallucinated number in a rationale fails verification, same as in the
    anomaly_summary. Editorial discipline must be uniform."""
    facts = {"imports": {"yoy_pct": 0.342}}
    obj = {
        "anomaly_summary": "Imports rose 34%.",
        "hypotheses": [
            {"id": "tariff_preloading",
             "rationale": "An 87% surge is consistent with pre-loading."},  # 87% not in facts
        ],
    }
    result = llm_framing._validate_lead_scaffold(obj, facts)
    assert isinstance(result, llm_framing.LeadScaffoldRejection)
    assert result.reason == "rationale_failed_verification"


def test_validate_lead_scaffold_rejects_invented_number_in_anomaly_summary():
    facts = {"imports": {"yoy_pct": 0.342}}
    obj = {
        "anomaly_summary": "Imports rose 87%.",  # not in facts
        "hypotheses": [],
    }
    result = llm_framing._validate_lead_scaffold(obj, facts)
    assert isinstance(result, llm_framing.LeadScaffoldRejection)
    assert result.reason == "anomaly_summary_failed_verification"


def test_validate_lead_scaffold_caps_hypotheses_at_three():
    """If the LLM ignores the 'pick 2-3' instruction and returns 5, we
    truncate to 3 rather than reject — editorial harm is bounded."""
    facts = {"imports": {"yoy_pct": 0.342}}
    obj = {
        "anomaly_summary": "Imports rose 34%.",
        "hypotheses": [
            {"id": "tariff_preloading", "rationale": "Imports rose 34%."},
            {"id": "capacity_expansion_china", "rationale": "Imports rose 34%."},
            {"id": "eu_demand_pull", "rationale": "Imports rose 34%."},
            {"id": "energy_transition", "rationale": "Imports rose 34%."},
            {"id": "currency_effect", "rationale": "Imports rose 34%."},
        ],
    }
    result = llm_framing._validate_lead_scaffold(obj, facts)
    assert isinstance(result, llm_framing.LeadScaffold)
    assert len(result.hypotheses) == 3
    assert [h["id"] for h in result.hypotheses] == [
        "tariff_preloading", "capacity_expansion_china", "eu_demand_pull",
    ]


def test_validate_lead_scaffold_accepts_empty_hypotheses_list():
    """Featureless groups can legitimately have nothing to scaffold beyond a
    summary. Empty hypotheses list is allowed."""
    facts = {"imports": {"yoy_pct": 0.001}}  # essentially flat
    obj = {
        "anomaly_summary": "Imports broadly flat year-on-year.",
        "hypotheses": [],
    }
    result = llm_framing._validate_lead_scaffold(obj, facts)
    assert isinstance(result, llm_framing.LeadScaffold)
    assert result.hypotheses == []
    assert result.corroboration_steps == []


def test_user_prompt_carries_multi_scope_perspective_preamble():
    """The user prompt must name all three comparison scopes (EU-27 / UK /
    combined) and require explicit scope-and-parties naming in any
    direction reference. A journalist reading the anomaly summary should
    never see bare 'imports rose' — always 'EU-27 imports from China
    rose' or 'UK imports from China rose'."""
    cluster = llm_framing.HsGroupCluster(
        group_id=1, group_name="Test group", group_description="x",
        hs_patterns=["8507%"],
    )
    facts = {"scopes": {"eu_27": {"imports": {"yoy_pct": 0.30}}}}
    prompt = llm_framing._build_user_prompt(cluster, facts)
    # Perspective preamble names all three scopes explicitly
    assert "EU-27 imports from China" in prompt
    assert "EU-27 exports to China" in prompt
    assert "UK imports from China" in prompt
    assert "UK exports to China" in prompt
    assert "EU-27 + UK combined imports from China" in prompt
    # Cross-scope coverage rule present so LLM knows when to mention UK
    assert "CROSS-SCOPE COVERAGE" in prompt


def test_system_prompt_requires_explicit_scope_and_parties():
    """Rule G in SYSTEM_PROMPT enforces explicit scope+parties naming.
    Rule H requires cross-scope coverage discipline. Catches accidental
    rule-removal in future edits.

    Asserts on collapsed-whitespace text to be robust against line-wrap
    changes in the prompt source."""
    collapsed = " ".join(llm_framing.SYSTEM_PROMPT.split())
    assert "ALWAYS NAME BOTH THE SCOPE AND THE PARTIES" in collapsed
    assert "EU-27 imports from China" in collapsed
    assert "UK imports from China" in collapsed
    assert "CROSS-SCOPE COVERAGE" in collapsed


def test_render_lead_scaffold_as_body_includes_all_three_parts():
    scaffold = llm_framing.LeadScaffold(
        anomaly_summary="Imports rose 34%.",
        hypotheses=[
            {"id": "tariff_preloading", "label": "Tariff pre-loading",
             "rationale": "Surge consistent with pull-forward."},
        ],
        corroboration_steps=["Check DG TRADE register."],
    )
    body = llm_framing.render_lead_scaffold_as_body(scaffold)
    assert "**Anomaly:**" in body
    assert "Imports rose 34%." in body
    assert "**Possible causes:**" in body
    assert "Tariff pre-loading" in body
    assert "**Corroboration steps:**" in body
    assert "Check DG TRADE register." in body


# ---------------------------------------------------------------------------
# Cluster building (unchanged from v1)
# ---------------------------------------------------------------------------


@pytest.fixture
def empty_op(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE findings, observations, source_snapshots, eurostat_raw_rows, "
            "scrape_runs, releases RESTART IDENTITY CASCADE"
        )
    yield


def _seed_yoy(conn, *, group_id: int, subkind: str, period: date,
              yoy_pct: float, current_eur: float = 1e9, prior_eur: float = 0.7e9,
              caveats: list[str] | None = None):
    """Insert a minimal hs_group_yoy* finding so the cluster builder can pick
    it up."""
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES "
        "('seed', 'success') RETURNING id"
    )
    run = cur.fetchone()[0]
    detail = {
        "method": "test",
        "group": {"id": group_id, "name": "test"},
        "windows": {"current_end": period.isoformat()},
        "totals": {
            "yoy_pct": yoy_pct,
            "current_12mo_eur": current_eur,
            "prior_12mo_eur": prior_eur,
            "yoy_pct_kg": yoy_pct * 1.5,
            "low_base": False,
            "partial_window": False,
            "decomposition_suppressed": False,
        },
        "caveat_codes": caveats or ["cif_fob"],
    }
    cur.execute(
        "INSERT INTO findings (scrape_run_id, kind, subkind, hs_group_ids, "
        "                       natural_key_hash, value_signature, detail) "
        "VALUES (%s, 'anomaly', %s, %s, %s, 'sig', %s::jsonb)",
        (run, subkind, [group_id], f"test-{group_id}-{subkind}-{period}",
         json.dumps(detail)),
    )
    conn.commit()


def test_load_clusters_skips_groups_with_no_findings(empty_op, test_db_url):
    """A group with no underlying findings is not scaffolded — there's
    nothing to say."""
    clusters = llm_framing._load_hs_group_clusters()
    assert clusters == []


def test_load_clusters_unions_caveats_across_underlying(empty_op, test_db_url):
    """Cluster.caveat_codes should be the union across all (scope, flow)
    permutations that have findings."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy",
                  period=date(2026, 2, 1), yoy_pct=0.342,
                  caveats=["cif_fob", "cn8_revision"])
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy_export",
                  period=date(2026, 2, 1), yoy_pct=-0.45,
                  caveats=["cif_fob", "low_base_effect"])

    clusters = llm_framing._load_hs_group_clusters(group_names=["EV batteries (Li-ion)"])
    assert len(clusters) == 1
    c = clusters[0]
    assert c.group_id == 1
    assert "cn8_revision" in c.caveat_codes
    assert "low_base_effect" in c.caveat_codes
    assert "cif_fob" in c.caveat_codes
    assert len(c.underlying_finding_ids) == 2
    # Findings landed in the eu_27 scope (their subkinds have no scope suffix)
    assert c.scopes["eu_27"].yoy_import is not None
    assert c.scopes["eu_27"].yoy_export is not None
    # Other scopes empty
    assert c.scopes["uk"].has_any() is False
    assert c.scopes["eu_27_plus_uk"].has_any() is False


def test_load_clusters_collects_findings_across_all_three_scopes(
    empty_op, test_db_url,
):
    """Phase: multi-scope cluster loading. Seed findings in all three
    scopes for the same group; cluster should hold them under the
    correct scope keys."""
    with psycopg2.connect(test_db_url) as conn:
        # eu_27 imports
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy",
                  period=date(2026, 2, 1), yoy_pct=0.342)
        # UK imports
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy_uk",
                  period=date(2026, 2, 1), yoy_pct=0.182)
        # combined imports
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy_combined",
                  period=date(2026, 2, 1), yoy_pct=0.331)

    clusters = llm_framing._load_hs_group_clusters(
        group_names=["EV batteries (Li-ion)"]
    )
    assert len(clusters) == 1
    c = clusters[0]
    assert c.scopes["eu_27"].yoy_import is not None
    assert c.scopes["uk"].yoy_import is not None
    assert c.scopes["eu_27_plus_uk"].yoy_import is not None
    # All three contributed to underlying ids
    assert len(c.underlying_finding_ids) == 3


def test_build_facts_drops_low_base_imports_for_non_eu27_scopes(
    empty_op, test_db_url,
):
    """Defensive: low_base imports/exports for UK / combined scopes are
    filtered out of the prompt. Telling the LLM 'skip low_base UK'
    via a rule was unreliable in practice (qwen3.6 cited them anyway in
    half of test cases). Removing them from the facts means the LLM
    literally can't see them. EU-27 low_base data is preserved because
    it's the always-lead."""
    with psycopg2.connect(test_db_url) as conn:
        # EU-27 imports: low_base, but kept (it's the lead)
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy",
                  period=date(2026, 2, 1), yoy_pct=0.20,
                  caveats=["low_base_effect"])
        cur = conn.cursor()
        cur.execute(
            "UPDATE findings SET detail = jsonb_set(detail, '{totals,low_base}', 'true') "
            "WHERE subkind = 'hs_group_yoy'"
        )
        # UK imports: low_base, should be dropped
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy_uk",
                  period=date(2026, 2, 1), yoy_pct=0.50)
        cur.execute(
            "UPDATE findings SET detail = jsonb_set(detail, '{totals,low_base}', 'true') "
            "WHERE subkind = 'hs_group_yoy_uk'"
        )
        # Combined imports: NOT low_base, should be kept
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy_combined",
                  period=date(2026, 2, 1), yoy_pct=0.25)
        conn.commit()

    clusters = llm_framing._load_hs_group_clusters(
        group_names=["EV batteries (Li-ion)"]
    )
    facts = llm_framing._build_facts(clusters[0])
    # EU-27 imports kept even though low_base
    assert "imports" in facts["scopes"]["eu_27"]
    assert facts["scopes"]["eu_27"]["imports"]["low_base"] is True
    # UK imports dropped (low_base AND non-eu_27)
    assert "uk" not in facts["scopes"] or "imports" not in facts["scopes"]["uk"]
    # Combined imports kept (not low_base)
    assert facts["scopes"]["eu_27_plus_uk"]["imports"]["yoy_pct"] == 0.25


def test_build_facts_nests_by_scope(empty_op, test_db_url):
    """Phase: facts dict is structured as {scopes: {eu_27: {...}, uk:
    {...}, ...}} so the LLM sees the multi-scope picture explicitly.
    Empty scopes are omitted to keep the prompt terse."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy",
                  period=date(2026, 2, 1), yoy_pct=0.342,
                  current_eur=2.69e10)
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy_uk",
                  period=date(2026, 2, 1), yoy_pct=0.182,
                  current_eur=1.87e9)
    clusters = llm_framing._load_hs_group_clusters(
        group_names=["EV batteries (Li-ion)"]
    )
    facts = llm_framing._build_facts(clusters[0])
    assert "scopes" in facts
    assert "eu_27" in facts["scopes"]
    assert "uk" in facts["scopes"]
    # combined scope had no findings → omitted entirely
    assert "eu_27_plus_uk" not in facts["scopes"]
    # Per-scope shape preserved
    assert facts["scopes"]["eu_27"]["imports"]["yoy_pct"] == 0.342
    assert facts["scopes"]["uk"]["imports"]["yoy_pct"] == 0.182


# ---------------------------------------------------------------------------
# End-to-end with FakeBackend
# ---------------------------------------------------------------------------


def test_end_to_end_persists_verified_lead_scaffold(empty_op, test_db_url):
    """A FakeBackend returning a numerically-verifiable scaffold produces
    a kind='llm_topline' finding with the v2 lead_scaffold detail shape."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy",
                  period=date(2026, 2, 1), yoy_pct=0.342,
                  current_eur=2.69e10, prior_eur=2.0e10)

    backend = FakeBackend(response=_scaffold_json(
        "Imports of Chinese EV batteries rose 34% to €27B in the year to February 2026.",
        [
            {"id": "energy_transition",
             "rationale": "EU battery demand has surged with the auto-sector EV ramp."},
            {"id": "capacity_expansion_china",
             "rationale": "The 34% increase is consistent with Chinese cell-capacity expansion."},
        ],
    ))
    counts = llm_framing.detect_llm_framings(
        group_names=["EV batteries (Li-ion)"], backend=backend,
    )
    assert counts["inserted_new"] == 1
    assert counts["skipped_unverified"] == 0

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT kind, subkind, body, detail FROM findings "
            "WHERE subkind = 'narrative_hs_group'"
        )
        kind, subkind, body, detail = cur.fetchone()
    assert kind == "llm_topline"
    assert subkind == "narrative_hs_group"
    assert "**Anomaly:**" in body
    assert "Tariff pre-loading" not in body  # we picked energy_transition + capacity_expansion
    assert "Energy-transition demand surge" in body or "Chinese capacity expansion" in body
    assert "**Corroboration steps:**" in body
    assert detail["method"] == llm_framing.LEAD_METHOD
    scaffold = detail["lead_scaffold"]
    assert len(scaffold["hypotheses"]) == 2
    assert {h["id"] for h in scaffold["hypotheses"]} == {
        "energy_transition", "capacity_expansion_china",
    }
    assert scaffold["corroboration_steps"]
    assert "llm_drafted" in detail["caveat_codes"]


def test_end_to_end_rejects_invented_number(empty_op, test_db_url):
    """If the LLM hallucinates a figure in the anomaly summary, the lead is
    rejected and tallied under skipped_unverified."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy",
                  period=date(2026, 2, 1), yoy_pct=0.342,
                  current_eur=2.69e10, prior_eur=2.0e10)

    backend = FakeBackend(response=_scaffold_json(
        "Imports rose 60% in the year.",  # 60% not in facts
        [{"id": "tariff_preloading", "rationale": "Surge fits pre-loading."}],
    ))
    counts = llm_framing.detect_llm_framings(
        group_names=["EV batteries (Li-ion)"], backend=backend,
    )
    assert counts["skipped_unverified"] == 1
    assert counts["inserted_new"] == 0
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM findings WHERE subkind='narrative_hs_group'")
        assert cur.fetchone()[0] == 0


def test_end_to_end_rejects_unknown_hypothesis_id(empty_op, test_db_url):
    """A hypothesis id outside the catalog gets the lead rejected."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy",
                  period=date(2026, 2, 1), yoy_pct=0.342,
                  current_eur=2.69e10, prior_eur=2.0e10)

    backend = FakeBackend(response=_scaffold_json(
        "Imports rose 34%.",
        [{"id": "made_up_hypothesis_not_in_catalog",
          "rationale": "Imports rose 34%."}],
    ))
    counts = llm_framing.detect_llm_framings(
        group_names=["EV batteries (Li-ion)"], backend=backend,
    )
    assert counts["skipped_unverified"] == 1
    assert counts["inserted_new"] == 0


def test_end_to_end_idempotent_on_rerun(empty_op, test_db_url):
    """Re-running with an unchanged underlying finding set is a no-op at
    the row level (last_confirmed_at bumps, no new rows)."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy",
                  period=date(2026, 2, 1), yoy_pct=0.342,
                  current_eur=2.69e10, prior_eur=2.0e10)

    backend = FakeBackend(response=_scaffold_json(
        "Imports rose 34%.",
        [{"id": "tariff_preloading", "rationale": "Imports rose 34%."}],
    ))
    first = llm_framing.detect_llm_framings(
        group_names=["EV batteries (Li-ion)"], backend=backend,
    )
    second = llm_framing.detect_llm_framings(
        group_names=["EV batteries (Li-ion)"], backend=backend,
    )
    assert first["inserted_new"] == 1
    assert second["inserted_new"] == 0
    assert second["confirmed_existing"] == 1
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM findings WHERE subkind='narrative_hs_group'")
        assert cur.fetchone()[0] == 1


def test_end_to_end_supersedes_when_picked_hypotheses_change(empty_op, test_db_url):
    """If the LLM picks different hypotheses on re-run (different scaffold,
    same underlying data), the new row supersedes the old."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy",
                  period=date(2026, 2, 1), yoy_pct=0.342,
                  current_eur=2.69e10, prior_eur=2.0e10)

    backend1 = FakeBackend(response=_scaffold_json(
        "Imports rose 34%.",
        [{"id": "tariff_preloading", "rationale": "Imports rose 34%."}],
    ))
    llm_framing.detect_llm_framings(
        group_names=["EV batteries (Li-ion)"], backend=backend1,
    )
    backend2 = FakeBackend(response=_scaffold_json(
        "Imports rose 34%.",
        [{"id": "energy_transition", "rationale": "EV-driven demand surge."}],
    ))
    counts = llm_framing.detect_llm_framings(
        group_names=["EV batteries (Li-ion)"], backend=backend2,
    )
    assert counts["superseded"] == 1
    assert counts["inserted_new"] == 0
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FILTER (WHERE superseded_at IS NULL) AS active, "
            "       COUNT(*) FILTER (WHERE superseded_at IS NOT NULL) AS superseded "
            "  FROM findings WHERE subkind='narrative_hs_group'"
        )
        active, sup = cur.fetchone()
    assert active == 1
    assert sup == 1
