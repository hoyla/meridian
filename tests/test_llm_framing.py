"""Tests for the LLM framing layer.

Phase 3 of dev_notes/roadmap-2026-05-09.md. The discipline being defended
across these tests:

1. The LLM never computes — facts are pre-extracted and verified.
2. Every number in the output must round-trip to a fact within tolerance,
   or the narrative is rejected (skipped_unverified).
3. Narratives use the same append-plus-supersede chain as deterministic
   findings, idempotent on re-run with unchanged sources.
4. The briefing pack surfaces narratives ABOVE the deterministic mover
   sections, with a section that's suppressed when no narratives exist
   (so journalists who haven't run framing still get a clean brief).

Tests use a FakeBackend that returns canned output so the suite never
calls Ollama. Live integration is exercised separately via the CLI smoke.
"""

import json
from datetime import date

import psycopg2
import pytest

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


# ---------------------------------------------------------------------------
# Pure-function tests: numeric verification
# ---------------------------------------------------------------------------


def test_verify_numbers_accepts_text_with_no_numbers():
    """Abstaining from quoting numbers is fine — the LLM can write
    qualitative narrative and pass verification."""
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
    """Phase 3: 'a 69.2% increase in volume that offset a 20.7% drop in
    unit prices' — the verifier's context-window sign inference might mis-
    flip 69.2% because 'drop' is in window. The magnitude-only fallback
    rescues this case, accepting +69.2% even when prose context suggests
    negative. The fallback only fires after sign-aware match has failed,
    so a fundamentally direction-wrong claim ('+37%' when fact is -36.8%')
    still fails (nothing matches at any sign)."""
    facts = {
        "imports": {"yoy_pct_kg": 0.692, "unit_price_pct_change": -0.207},
    }
    text = "a 69.2% increase in volume that offset a 20.7% drop in unit prices"
    ok, _ = llm_framing.verify_numbers(text, facts)
    assert ok is True


def test_verify_numbers_currency_not_sign_flipped_by_decrease_context():
    """Currency values are stocks; 'fell to €441.6M' leaves €441.6M positive.
    Only percentages take signs from movement verbs."""
    facts = {"exports": {"current_12mo_eur": 441_600_000.0}}
    text = "Exports fell to €441.6 million."
    ok, _ = llm_framing.verify_numbers(text, facts)
    assert ok is True


def test_verify_numbers_walks_nested_facts():
    """The fact-collection helper recurses through nested dicts/lists.
    Numbers at any depth count as 'available facts'."""
    facts = {
        "imports": {"yoy_pct": 0.342},
        "trajectory_imports": {
            "shape": "dip_recovery",
            "last_yoy": 0.342,
            "max_yoy": 0.48,
        },
    }
    # 48% is in trajectory_imports.max_yoy — should verify.
    ok, _ = llm_framing.verify_numbers(
        "Imports peaked at 48% YoY before easing to 34%.", facts,
    )
    assert ok is True


# ---------------------------------------------------------------------------
# Cluster building
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
    it up. Bypasses findings_io because the test is exercising the cluster
    side, not the emit side."""
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
    """A group with no underlying findings is not narrated — there's
    nothing to say."""
    clusters = llm_framing._load_hs_group_clusters()
    # No findings exist; clusters list should be empty.
    assert clusters == []


def test_load_clusters_unions_caveats_across_underlying(empty_op, test_db_url):
    """Cluster.caveat_codes should be the union across both flows + both
    trajectory + yoy."""
    with psycopg2.connect(test_db_url) as conn:
        # Group 1 (EV batteries): seed yoy import with cn8_revision; yoy
        # export with low_base.
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


# ---------------------------------------------------------------------------
# End-to-end with FakeBackend
# ---------------------------------------------------------------------------


def test_end_to_end_persists_verified_narrative(empty_op, test_db_url):
    """A FakeBackend returning a numerically-verifiable narrative produces
    a kind='llm_topline' finding with the right shape."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy",
                  period=date(2026, 2, 1), yoy_pct=0.342,
                  current_eur=2.69e10, prior_eur=2.0e10)

    backend = FakeBackend(
        response="EU imports of Chinese EV batteries rose 34% in the year to "
                 "February 2026, totalling €27B."
    )
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
    assert "34%" in body
    assert "llm_drafted" in detail["caveat_codes"]
    assert detail["model"] == "fake-model"
    assert detail["underlying_finding_ids"]


def test_end_to_end_rejects_invented_number(empty_op, test_db_url):
    """If the LLM hallucinates a figure, the narrative is REJECTED — not
    persisted, tallied under skipped_unverified. Editorial cost: silence
    on that group. Editorial benefit: never confidently wrong."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy",
                  period=date(2026, 2, 1), yoy_pct=0.342,
                  current_eur=2.69e10, prior_eur=2.0e10)

    # The LLM claims +60% which is NOT in the facts (yoy_pct=0.342).
    backend = FakeBackend(
        response="EU imports of Chinese EV batteries rose 60% in the year."
    )
    counts = llm_framing.detect_llm_framings(
        group_names=["EV batteries (Li-ion)"], backend=backend,
    )
    assert counts["skipped_unverified"] == 1
    assert counts["inserted_new"] == 0
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM findings WHERE subkind='narrative_hs_group'")
        assert cur.fetchone()[0] == 0


def test_end_to_end_idempotent_on_rerun(empty_op, test_db_url):
    """Re-running with an unchanged underlying finding set is a no-op at
    the row level (last_confirmed_at bumps, no new rows). Same as for
    deterministic findings — narratives use the same supersede chain."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy",
                  period=date(2026, 2, 1), yoy_pct=0.342,
                  current_eur=2.69e10, prior_eur=2.0e10)

    backend = FakeBackend(response="EU imports rose 34% in 2026.")
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


def test_end_to_end_supersedes_when_narrative_text_changes(empty_op, test_db_url):
    """If the LLM produces a new narrative on re-run (different prose, same
    underlying data), the new row is inserted and the old is superseded."""
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy(conn, group_id=1, subkind="hs_group_yoy",
                  period=date(2026, 2, 1), yoy_pct=0.342,
                  current_eur=2.69e10, prior_eur=2.0e10)

    backend1 = FakeBackend(response="EU imports rose 34% in 2026.")
    llm_framing.detect_llm_framings(
        group_names=["EV batteries (Li-ion)"], backend=backend1,
    )
    backend2 = FakeBackend(response="The 34% jump in EU imports continued through February 2026.")
    counts = llm_framing.detect_llm_framings(
        group_names=["EV batteries (Li-ion)"], backend=backend2,
    )
    assert counts["superseded"] == 1
    assert counts["inserted_new"] == 0  # the new row is the supersede
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FILTER (WHERE superseded_at IS NULL) AS active, "
            "       COUNT(*) FILTER (WHERE superseded_at IS NOT NULL) AS superseded "
            "  FROM findings WHERE subkind='narrative_hs_group'"
        )
        active, sup = cur.fetchone()
    assert active == 1
    assert sup == 1
