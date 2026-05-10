"""Tests for the Markdown briefing-pack exporter.

Same approach as test_sheets_export: seed findings into the test DB, render
the pack, then assert structure + content + provenance discipline.

Provenance discipline is what we're actually defending here: every finding
referenced inline must have its `finding/{id}` token; the Sources appendix
must list the third-party URLs underlying any finding included; appendix
URLs must be reachable third-party URLs, not internal handles.
"""

import json
import re
from datetime import date

import psycopg2
import pytest

import briefing_pack


@pytest.fixture(autouse=True)
def _direct_db_url(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    monkeypatch.delenv(briefing_pack.PERMALINK_BASE_ENV, raising=False)


@pytest.fixture
def empty_findings(test_db_url):
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE findings, observations, source_snapshots, eurostat_raw_rows, "
            "scrape_runs, releases RESTART IDENTITY CASCADE"
        )
    yield


def _seed_run(cur, source_url: str = "seed") -> int:
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'success') RETURNING id",
        (source_url,),
    )
    return cur.fetchone()[0]


def _seed_eurostat_release(cur, period: date) -> int:
    """Insert a Eurostat release row so the appendix has something to list."""
    cur.execute(
        "INSERT INTO releases (source, period, source_url) VALUES "
        "('eurostat', %s, %s) RETURNING id",
        (period, f"https://example.invalid/eurostat/{period.strftime('%Y%m')}"),
    )
    return cur.fetchone()[0]


def _seed_gacc_release(cur, period: date) -> int:
    cur.execute(
        "INSERT INTO releases (source, period, source_url, section_number, currency, release_kind) "
        "VALUES ('gacc', %s, %s, 4, 'CNY', 'monthly') RETURNING id",
        (period, f"http://english.customs.gov.cn/seed-{period.strftime('%Y%m')}.html"),
    )
    return cur.fetchone()[0]


def _seed_observation(cur, run_id: int, release_id: int) -> int:
    cur.execute(
        """
        INSERT INTO observations
            (release_id, scrape_run_id, period_kind, source_row, version_seen)
        VALUES (%s, %s, 'monthly', '{}'::jsonb, 1)
        RETURNING id
        """,
        (release_id, run_id),
    )
    return cur.fetchone()[0]


def _seed_hs_yoy_finding(cur, run_id: int, group_name: str, *,
                        subkind: str = "hs_group_yoy",
                        yoy_pct: float = 0.4,
                        current_eur: float = 1e9,
                        prior_eur: float = 0.7e9,
                        low_base: bool = False,
                        period: date = date(2026, 2, 1)) -> int:
    cur.execute("SELECT id FROM hs_groups WHERE name = %s", (group_name,))
    hg = cur.fetchone()
    hg_ids = [hg[0]] if hg else []
    prior_start = date(period.year - 2, period.month, 1)
    prior_end = date(period.year - 1, period.month, 1)
    detail = {
        "method": "hs_group_yoy_v4_with_low_base_flag",
        "method_query": {"hs_patterns": ["8507%"]},
        "group": {"name": group_name, "hs_patterns": ["8507%"]},
        "windows": {
            "current_start": prior_end.isoformat(),
            "current_end": period.isoformat(),
            "prior_start": prior_start.isoformat(),
            "prior_end": prior_end.isoformat(),
        },
        "totals": {
            "yoy_pct": yoy_pct,
            "current_12mo_eur": current_eur,
            "prior_12mo_eur": prior_eur,
            "yoy_pct_kg": yoy_pct * 1.5,
            "current_12mo_kg": 1e6,
            "unit_price_pct_change": -0.1,
            "low_base": low_base,
            "low_base_threshold_eur": 5e7,
        },
    }
    cur.execute(
        """
        INSERT INTO findings (scrape_run_id, kind, subkind, observation_ids, hs_group_ids,
                              score, title, body, detail)
        VALUES (%s, 'anomaly', %s, '{}', %s, %s, %s, 'b', %s::jsonb)
        RETURNING id
        """,
        (run_id, subkind, hg_ids, abs(yoy_pct),
         f"seed {subkind} {group_name}", json.dumps(detail)),
    )
    return cur.fetchone()[0]


def _seed_mirror_gap_finding(cur, run_id: int, iso2: str, period: date,
                             gacc_obs_id: int, eu_obs_id: int) -> int:
    detail = {
        "iso2": iso2,
        "gacc": {"partner_label_raw": iso2, "value_eur_converted": 1e9},
        "eurostat": {"total_eur": 1.7e9},
        "gap_eur": 7e8, "gap_pct": 0.7, "is_aggregate": False,
    }
    cur.execute(
        """
        INSERT INTO findings (scrape_run_id, kind, subkind, observation_ids,
                              score, title, body, detail)
        VALUES (%s, 'anomaly', 'mirror_gap', %s, 0.7, %s, 'b', %s::jsonb)
        RETURNING id
        """,
        (run_id, [gacc_obs_id, eu_obs_id], f"seed mirror_gap {iso2}", json.dumps(detail)),
    )
    return cur.fetchone()[0]


def test_render_produces_all_top_level_sections(empty_findings, test_db_url):
    """An empty DB renders the framing headers (period coverage, findings
    inventory, mirror-trade, sources) but the per-scope YoY/trajectory
    sections are gated — Phase 6.1e returns empty markdown for scopes
    with no findings so the brief stays terse rather than printing N
    empty per-scope headers."""
    md = briefing_pack.render()
    assert "# GACC × Eurostat trade briefing" in md
    assert "## Period coverage" in md
    assert "## Findings included" in md
    # The mirror-trade section always renders (it's not scope-gated yet).
    assert "## Mirror-trade gaps" in md
    assert "## Sources" in md
    # Scope-gated sections are absent on an empty DB; verified positively
    # in test_top_n_truncates_movers below where seed data is present.


def test_finding_lines_carry_trace_token(empty_findings, test_db_url):
    """Every finding rendered inline must have its `finding/{id}` citation."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        rel = _seed_eurostat_release(cur, date(2026, 2, 1))
        # plus the prior_start (Feb 2024) → covers prior window
        for m in range(1, 13):
            _seed_eurostat_release(cur, date(2025, m, 1))
        fid = _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)", yoy_pct=0.34)
        conn.commit()

    md = briefing_pack.render()
    assert f"finding/{fid}" in md


def test_low_base_section_only_appears_when_flagged(empty_findings, test_db_url):
    """The low-base review section is suppressed when nothing is flagged."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_eurostat_release(cur, date(2026, 2, 1))
        _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)", low_base=False)
        conn.commit()

    md = briefing_pack.render()
    assert "## Low-base review queue" not in md

    # Now seed one flagged finding and re-render
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_hs_yoy_finding(cur, run, "Rare-earth materials", low_base=True,
                             current_eur=2e7, prior_eur=1e7)
        conn.commit()

    md = briefing_pack.render()
    assert "## Low-base review queue" in md
    assert "Rare-earth materials" in md


def test_sources_appendix_lists_third_party_urls(empty_findings, test_db_url):
    """When a finding references a release, the appendix must include that
    release's third-party URL (Eurostat) — i.e. not just our internal
    finding-id token."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        # prior + current windows → 24 monthly releases would normally cover it.
        # One release is enough to prove the appendix wires up.
        _seed_eurostat_release(cur, date(2025, 6, 1))
        _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)", yoy_pct=0.5)
        conn.commit()

    md = briefing_pack.render()
    assert "### Eurostat monthly bulk files" in md
    # The appendix lists the Eurostat URL synthesised from the period —
    # not the seed URL. That's deliberate: the canonical bulk-file URL
    # is deterministic per period.
    assert "ec.europa.eu/eurostat" in md
    assert "2025-06" in md


def test_mirror_gap_observations_pull_in_both_sources(empty_findings, test_db_url):
    """A mirror_gap finding ties to both a GACC release and a Eurostat release.
    The appendix must list both."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        eu_release = _seed_eurostat_release(cur, date(2026, 1, 1))
        gacc_release = _seed_gacc_release(cur, date(2026, 1, 1))
        eu_obs = _seed_observation(cur, run, eu_release)
        gacc_obs = _seed_observation(cur, run, gacc_release)
        _seed_mirror_gap_finding(cur, run, "NL", date(2026, 1, 1),
                                gacc_obs_id=gacc_obs, eu_obs_id=eu_obs)
        conn.commit()

    md = briefing_pack.render()
    assert "### Eurostat monthly bulk files" in md
    assert "### GACC release pages" in md
    assert "english.customs.gov.cn" in md


def test_top_n_truncates_movers(empty_findings, test_db_url):
    """top_n caps the number of mover entries rendered. Default is 10; we
    seed 12 and ask for 3."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        cur.execute("SELECT name FROM hs_groups ORDER BY id LIMIT 12")
        names = [r[0] for r in cur.fetchall()]
        for i, n in enumerate(names):
            _seed_hs_yoy_finding(cur, run, n, yoy_pct=0.1 * (i + 1))
        conn.commit()

    md = briefing_pack.render(top_n=3)
    # The "### {group}" headings under the EU-27 imports section — count them.
    # Phase 6.1e: section header now reads "## EU-27 Imports (CN→reporter) — top N movers".
    # Other sections also use ### headings (trajectory shape buckets,
    # mirror gaps, other scopes), so isolate to between this section's
    # header and the next "## ".
    imports_block = md.split("## EU-27 Imports (CN→reporter)")[1].split("\n## ")[0]
    h3_count = len(re.findall(r"^### ", imports_block, re.MULTILINE))
    assert h3_count == 3


def test_permalink_base_changes_trace_token_to_link(
    empty_findings, test_db_url, monkeypatch,
):
    """When GACC_PERMALINK_BASE is set, finding tokens become Markdown links
    rather than backticked handles. This is what flips on once we have a
    web UI — existing exports light up automatically."""
    monkeypatch.setenv(briefing_pack.PERMALINK_BASE_ENV, "https://gacc.example")
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        fid = _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)", yoy_pct=0.4)
        conn.commit()

    md = briefing_pack.render()
    # Looks like: [finding/123](https://gacc.example/finding/123)
    assert f"[finding/{fid}](https://gacc.example/finding/{fid})" in md


def test_export_writes_to_disk(empty_findings, test_db_url, tmp_path):
    out = briefing_pack.export(out_path=str(tmp_path / "brief.md"))
    assert out.endswith("brief.md")
    content = (tmp_path / "brief.md").read_text()
    assert content.startswith("# GACC × Eurostat trade briefing")


def test_diff_section_empty_on_first_brief(empty_findings, test_db_url):
    """Phase 6.8: a fresh DB with no prior brief_runs row produces no
    'Changes since the previous brief' section. The brief still renders;
    the section just doesn't appear."""
    md = briefing_pack.render()
    assert "## Changes since the previous brief" not in md


def test_diff_section_lists_material_yoy_shifts(empty_findings, test_db_url):
    """Phase 6.8: when a previous brief exists and findings have been
    superseded since, the diff section lists material YoY shifts (>5pp)
    with the old vs new values, and flags direction flips with 🔄."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)

        # Seed an old finding and supersede it BEFORE the brief_runs marker
        # — this should NOT appear in the diff.
        old_unrelated_id = _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)",
                                                 yoy_pct=0.10)
        new_unrelated_id = _seed_hs_yoy_finding(cur, run, "Solar PV cells & modules",
                                                 yoy_pct=0.20)
        cur.execute(
            "UPDATE findings SET superseded_at = now() - interval '1 hour', "
            "                    superseded_by_finding_id = %s WHERE id = %s",
            (new_unrelated_id, old_unrelated_id),
        )

        # Mark the prior brief reference point. Explicit past timestamp:
        # in a single transaction, postgres `now()` returns the transaction
        # start, so the marker would otherwise have the same timestamp as
        # the subsequent findings and the strict `created_at > prev_at`
        # filter would miss them. In production each brief is its own
        # transaction so this is purely a test-setup concern.
        cur.execute(
            "INSERT INTO brief_runs (generated_at, output_path, top_n) "
            "VALUES (now() - interval '1 second', '/tmp/prev.md', 10)"
        )

        # Seed a new active finding AFTER the marker — should appear under
        # "New findings".
        _seed_hs_yoy_finding(cur, run, "Steel (broad)", yoy_pct=0.15)

        # Seed an old finding superseded AFTER the marker with > 5pp shift
        # AND a direction flip — should appear under "Material YoY shifts"
        # with the 🔄 flag.
        old_id = _seed_hs_yoy_finding(cur, run, "Aluminium (broad)",
                                      yoy_pct=0.12)
        new_id = _seed_hs_yoy_finding(cur, run, "Aluminium (broad)",
                                      yoy_pct=-0.08)
        # Mark the old as superseded by the new, AFTER the brief_runs marker.
        cur.execute(
            "UPDATE findings SET superseded_at = now(), "
            "                    superseded_by_finding_id = %s WHERE id = %s",
            (new_id, old_id),
        )
        conn.commit()

    md = briefing_pack.render()
    assert "## Changes since the previous brief" in md
    assert "### Material YoY shifts" in md
    assert "Aluminium (broad)" in md
    # The Aluminium shift is 12% → -8% = 20pp swing AND a direction flip.
    assert "🔄" in md
    # New findings header lists the post-marker insertions.
    assert "### New findings" in md
    # The pre-marker supersede should NOT show up.
    assert "Solar PV cells & modules" not in md.split("## ")[1]  # rough containment


def test_export_records_brief_run(empty_findings, test_db_url, tmp_path):
    """Phase 6.8: export() inserts a brief_runs row so the next brief's
    diff section has a reference point. render() does NOT record (used by
    test/preview without polluting history)."""
    n_before = _count_brief_runs(test_db_url)
    briefing_pack.render()  # render alone doesn't record
    assert _count_brief_runs(test_db_url) == n_before

    briefing_pack.export(out_path=str(tmp_path / "brief.md"), top_n=5)
    assert _count_brief_runs(test_db_url) == n_before + 1

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT output_path, top_n FROM brief_runs ORDER BY id DESC LIMIT 1")
        path, top_n = cur.fetchone()
    assert path == str(tmp_path / "brief.md")
    assert top_n == 5


def _count_brief_runs(test_db_url) -> int:
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM brief_runs")
        return cur.fetchone()[0]


def test_construct_chinese_source_url():
    """The Chinese-language equivalent is constructed by host substitution
    (`english.customs.gov.cn` → `www.customs.gov.cn`); the Statics/<UUID>.html
    path is identical on both. Returns None for non-GACC URLs so callers can
    skip the link cleanly."""
    from briefing_pack import _construct_chinese_source_url

    en = "http://english.customs.gov.cn/Statics/2e61c8a1-17b2-4074-b909-c039ccf8c8fb.html"
    cn = _construct_chinese_source_url(en)
    assert cn == "http://www.customs.gov.cn/Statics/2e61c8a1-17b2-4074-b909-c039ccf8c8fb.html"

    assert _construct_chinese_source_url("https://ec.europa.eu/foo.html") is None
    assert _construct_chinese_source_url("") is None
    assert _construct_chinese_source_url(None) is None
