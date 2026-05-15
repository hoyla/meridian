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
from pathlib import Path

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
                        period: date = date(2026, 2, 1),
                        per_reporter_breakdown: list[dict] | None = None) -> int:
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
        "per_reporter_breakdown": per_reporter_breakdown or [],
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


def _seed_trajectory_finding(
    cur, run_id: int, group_name: str, *,
    shape: str,
    subkind: str = "hs_group_trajectory",
    shape_label: str | None = None,
) -> int:
    cur.execute("SELECT id FROM hs_groups WHERE name = %s", (group_name,))
    hg = cur.fetchone()
    hg_ids = [hg[0]] if hg else []
    detail = {
        "method": "hs_group_trajectory_v8_comparison_scope",
        "group": {"name": group_name, "hs_patterns": ["8507%"]},
        "shape": shape,
        "shape_label": shape_label or shape.replace("_", " "),
        "features": {},
    }
    cur.execute(
        """
        INSERT INTO findings (scrape_run_id, kind, subkind, observation_ids, hs_group_ids,
                              score, title, body, detail)
        VALUES (%s, 'anomaly', %s, '{}', %s, %s, %s, 'b', %s::jsonb)
        RETURNING id
        """,
        (run_id, subkind, hg_ids, 0.5,
         f"seed traj {shape} {group_name}", json.dumps(detail)),
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
    assert "# GACC × Eurostat trade findings" in md
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
    # The "#### {group}" headings under the EU-27 imports section — count them.
    # Tiered restructure (2026-05-11): the section heading itself was demoted
    # from "## EU-27 Imports..." to "### EU-27 Imports..." so it sits under
    # the Tier 3 "## Full detail" parent; per-group sub-headings were
    # correspondingly demoted from "### {group}" to "#### {group}".
    imports_block = md.split("### EU-27 Imports (CN→reporter)")[1].split("\n### ")[0]
    h4_count = len(re.findall(r"^#### ", imports_block, re.MULTILINE))
    assert h4_count == 3


def test_reporter_contributions_block_renders_under_mover(
    empty_findings, test_db_url,
):
    """Phase 6.11: each hs_group_yoy mover renders a per-reporter
    contributions sub-list when `detail.per_reporter_breakdown` is
    populated. Top-5 of the breakdown is surfaced inline; zero-delta
    entries are filtered out so single-reporter UK findings stay quiet."""
    breakdown = [
        {
            "reporter": "DE", "current_eur": 6e8, "prior_eur": 1.2e9,
            "delta_eur": -6e8, "yoy_pct": -0.5,
            "current_kg": 6e7, "prior_kg": 1.2e8, "yoy_pct_kg": -0.5,
            "share_of_group_delta_pct": 2.0,
        },
        {
            "reporter": "FR", "current_eur": 9e8, "prior_eur": 6e8,
            "delta_eur": 3e8, "yoy_pct": 0.5,
            "current_kg": 9e7, "prior_kg": 6e7, "yoy_pct_kg": 0.5,
            "share_of_group_delta_pct": -1.0,
        },
    ]
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_hs_yoy_finding(
            cur, run, "EV batteries (Li-ion)", yoy_pct=-0.1667,
            current_eur=1.5e9, prior_eur=1.8e9,
            per_reporter_breakdown=breakdown,
        )
        conn.commit()

    md = briefing_pack.render()
    # Sub-block heading present
    assert "**Reporter contributions**" in md
    # DE entry surfaces its YoY% and share-of-group-delta. Use the prefix
    # so a future _fmt_eur tweak doesn't break the test.
    assert "DE: -50.0%" in md
    assert "+200% of group's Δ" in md
    # FR also appears with the opposite-sign share.
    assert "FR: +50.0%" in md
    assert "-100% of group's Δ" in md


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


def test_export_writes_brief_and_leads_into_folder(
    empty_findings, test_db_url, tmp_path,
):
    """Default export creates a per-export folder with the numbered
    bundle files inside. The numeric prefix on each filename is
    deliberate — most file viewers sort lexically, so the prefixes
    drive a default reading order (read-me → leads → findings → data
    → groups)."""
    brief_path, leads_path = briefing_pack.export(
        out_dir=str(tmp_path / "20260510-1200"),
    )
    assert Path(brief_path).name == "03_Findings.md"
    assert Path(leads_path).name == "02_Leads.md"
    assert Path(brief_path).parent == Path(leads_path).parent
    brief_content = Path(brief_path).read_text()
    leads_content = Path(leads_path).read_text()
    assert brief_content.startswith("# GACC × Eurostat trade findings")
    assert leads_content.startswith("# GACC × Eurostat trade — investigation leads")
    # Empty DB: leads file gracefully announces absence of findings.
    assert "No active `narrative_hs_group` findings" in leads_content


def test_export_copies_templates_into_folder(
    empty_findings, test_db_url, tmp_path, monkeypatch,
):
    """Any file dropped into the repo's `templates/` directory (other
    than its own README.md) is copied verbatim into every export
    folder. Filenames are preserved — so a leading `01_` prefix sorts
    above findings.md / leads.md / data.xlsx in most file viewers."""
    # Re-point the templates dir at a per-test temp folder so the test
    # is isolated from whatever is actually in the repo's templates/.
    fake_templates = tmp_path / "fake_templates"
    fake_templates.mkdir()
    (fake_templates / "01_Read_Me_First.md").write_text(
        "# Read me first\n\nThe intro pack.\n"
    )
    (fake_templates / "README.md").write_text(
        "# This is documentation, NOT a template\n"
    )
    # `briefing_pack.render` resolves to the imported function (shadowed
    # by __init__.py); grab the actual module via sys.modules to patch
    # its module-level constant.
    import sys
    render_mod = sys.modules["briefing_pack.render"]
    monkeypatch.setattr(render_mod, "_TEMPLATES_DIR", fake_templates)

    out_dir = tmp_path / "20260513-1500"
    briefing_pack.export(out_dir=str(out_dir))

    # The user-facing template was copied with its original filename.
    assert (out_dir / "01_Read_Me_First.md").exists()
    assert (out_dir / "01_Read_Me_First.md").read_text() == (
        "# Read me first\n\nThe intro pack.\n"
    )
    # The templates dir's own README.md is documentation, not a
    # template; it must NOT propagate into the export.
    assert not (out_dir / "README.md").exists()


def test_export_handles_missing_templates_dir(
    empty_findings, test_db_url, tmp_path, monkeypatch,
):
    """If the templates/ dir is absent, export() still succeeds — the
    copy step is a no-op rather than a failure. Same for an empty dir."""
    missing_dir = tmp_path / "does_not_exist"
    import sys
    render_mod = sys.modules["briefing_pack.render"]
    monkeypatch.setattr(render_mod, "_TEMPLATES_DIR", missing_dir)
    # Should not raise.
    brief_path, leads_path = briefing_pack.export(
        out_dir=str(tmp_path / "20260513-1500"),
    )
    assert Path(brief_path).exists()
    assert Path(leads_path).exists()


def test_export_legacy_explicit_paths_still_work(
    empty_findings, test_db_url, tmp_path,
):
    """Power users (e.g. tests, ad-hoc one-off renders) can still pass
    explicit per-file paths via the legacy out_path/leads_path kwargs."""
    brief_path, leads_path = briefing_pack.export(
        out_path=str(tmp_path / "findings.md"),
        leads_path=str(tmp_path / "leads.md"),
    )
    assert brief_path.endswith("findings.md")
    assert leads_path.endswith("leads.md")


def test_export_legacy_paths_require_both(
    empty_findings, test_db_url, tmp_path,
):
    """Passing only one of out_path/leads_path is ambiguous; refuse it."""
    with pytest.raises(ValueError, match="both"):
        briefing_pack.export(out_path=str(tmp_path / "findings.md"))


def test_export_default_folder_uses_minute_timestamp(
    empty_findings, test_db_url, tmp_path, monkeypatch,
):
    """No out_dir given → default to ./exports/YYYY-MM-DD-HHMM/. Verify
    by pointing the cwd at tmp_path and checking the folder shape."""
    monkeypatch.chdir(tmp_path)
    brief_path, leads_path = briefing_pack.export()
    folder = Path(brief_path).parent
    # Folder lives under ./exports/ relative to cwd
    assert folder.parent.name == "exports"
    # Folder name matches YYYY-MM-DD-HHMM (no scope suffix on default)
    assert re.match(r"^\d{4}-\d{2}-\d{2}-\d{4}$", folder.name), folder.name


def test_export_scope_label_adds_slug_suffix_and_header_line(
    empty_findings, test_db_url, tmp_path, monkeypatch,
):
    """A scope_label is slugified into the folder suffix AND surfaced
    in both docs' headers."""
    monkeypatch.chdir(tmp_path)
    brief_path, leads_path = briefing_pack.export(
        scope_label="EV batteries (Li-ion)",
    )
    folder = Path(brief_path).parent
    assert folder.name.endswith("-ev-batteries-li-ion")
    assert "*Scope: **EV batteries (Li-ion)**.*" in Path(brief_path).read_text()
    assert "*Scope: **EV batteries (Li-ion)**.*" in Path(leads_path).read_text()


def test_slugify_scope_helper():
    from briefing_pack import _slugify_scope as fn
    assert fn("EV batteries (Li-ion)") == "ev-batteries-li-ion"
    assert fn("UK only") == "uk-only"
    assert fn("  Mixed  Case  ") == "mixed-case"
    assert fn("HS 8507 — sub-bracket") == "hs-8507-sub-bracket"
    assert fn("already-slug") == "already-slug"


def test_paired_export_cross_references_each_other(
    empty_findings, test_db_url, tmp_path,
):
    """When generated by the same export() call, each doc cites the
    other's basename. With the folder convention these are stable
    (`findings.md`, `leads.md`) — pairing is by folder, not by filename
    timestamp."""
    brief_path, leads_path = briefing_pack.export(
        out_dir=str(tmp_path / "20260510-1200"),
    )
    brief = Path(brief_path).read_text()
    leads = Path(leads_path).read_text()
    # Both docs carry an "In this export folder" block listing all four
    # artefacts. The block names every artefact by filename (with the
    # numeric reading-order prefix on each) and marks the current doc
    # as "(this document)".
    for doc in (brief, leads):
        assert "## In this export folder" in doc
        assert "`03_Findings.md`" in doc
        assert "`02_Leads.md`" in doc
        assert "`04_Data.xlsx`" in doc
        assert "`05_Groups.md`" in doc
    assert "**`03_Findings.md`** — deterministic Markdown findings (this document)" in brief
    assert "**`02_Leads.md`** — LLM-scaffolded investigation leads (this " in leads


def test_about_findings_endnote_appears_in_both_docs(
    empty_findings, test_db_url, tmp_path,
):
    """Both the brief and the leads doc end with the same endnote
    explaining what `finding/N` citations mean."""
    brief_path, leads_path = briefing_pack.export(
        out_dir=str(tmp_path / "20260510-1200"),
    )
    brief = Path(brief_path).read_text()
    leads = Path(leads_path).read_text()
    for doc in (brief, leads):
        assert "## About the `finding/N` citations" in doc
        assert "superseded_at" in doc
        assert "docs/methodology.md" in doc
        assert "docs/architecture.md" in doc


def test_diff_section_empty_on_first_brief(empty_findings, test_db_url):
    """Phase 6.8: a fresh DB with no prior brief_runs row produces no
    'Changes since the previous export' section. The brief still renders;
    the section just doesn't appear."""
    md = briefing_pack.render()
    assert "## Changes since the previous export" not in md


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
    # Tiered restructure (2026-05-11): the diff section is Tier 1.
    assert "## Tier 1 — What's new this cycle" in md
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

    briefing_pack.export(
        out_dir=str(tmp_path / "20260510-1200"),
        top_n=5,
    )
    assert _count_brief_runs(test_db_url) == n_before + 1

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT output_path, top_n FROM brief_runs ORDER BY id DESC LIMIT 1")
        path, top_n = cur.fetchone()
    assert path == str(tmp_path / "20260510-1200" / "03_Findings.md")
    assert top_n == 5


def test_export_with_record_false_skips_brief_runs(empty_findings, test_db_url, tmp_path):
    """The unsequenced-export path: `record=False` produces the bundle
    without inserting a brief_runs row. Useful for test/preview/on-demand
    renders that shouldn't pollute the cycle history or become the
    baseline for the next export's Tier 1 "what's new" section. Exposed
    via the CLI as `--no-record`."""
    n_before = _count_brief_runs(test_db_url)
    briefing_pack.export(
        out_dir=str(tmp_path / "20260510-1200"),
        top_n=5,
        record=False,
    )
    assert _count_brief_runs(test_db_url) == n_before


def _count_brief_runs(test_db_url) -> int:
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM brief_runs")
        return cur.fetchone()[0]


def test_methodology_footer_renders_with_definitions(empty_findings, test_db_url):
    """The Methodology footer reads canonical summary + detail text from the
    `caveats` schema table for each code in
    `anomalies.UNIVERSAL_CAVEATS_BY_SUBKIND_FAMILY`, grouped by analyser
    family. It appears even on an empty DB (the caveats table is seeded
    by schema.sql)."""
    md = briefing_pack.render()
    assert "## Methodology — universal caveats" in md
    # Spot-check codes seeded in schema.sql — they should all appear under
    # at least one family heading.
    assert "`cif_fob`" in md
    assert "`cn8_revision`" in md
    assert "`multi_partner_sum`" in md
    assert "`llm_drafted`" in md
    # The detail text from the caveats table propagates through:
    assert "CIF" in md and "FOB" in md


def test_universal_caveats_suppressed_inline_in_finding_lines(empty_findings, test_db_url):
    """A finding with universal caveats in its detail.caveat_codes should
    not show those caveats inline in the rendered output (they're explained
    once at the top instead). This test seeds a narrative_hs_group finding
    with a mix of universal + non-universal caveats and asserts the
    universal ones don't reach the per-finding caveat display in the
    rendered leads doc (which is now separate from the brief)."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        # Seed an llm_topline finding so the LLM-leads section actually
        # renders inline caveats.
        cur.execute("SELECT id FROM hs_groups WHERE name = %s", ("EV batteries (Li-ion)",))
        hg_id = cur.fetchone()[0]
        detail = {
            "method": "llm_topline_v2_lead_scaffold",
            "model": "fake",
            "group": {"id": hg_id, "name": "EV batteries (Li-ion)",
                      "hs_patterns": ["8507%"]},
            "lead_scaffold": {
                "anomaly_summary": "Test anomaly.",
                "hypotheses": [],
                "corroboration_steps": [],
            },
            "underlying_finding_ids": [],
            # Mix: cif_fob + currency_timing are universal; low_kg_coverage
            # is per-finding-informative (kept inline).
            "caveat_codes": ["cif_fob", "currency_timing", "low_kg_coverage"],
        }
        cur.execute(
            """
            INSERT INTO findings (scrape_run_id, kind, subkind, hs_group_ids,
                                  natural_key_hash, value_signature, title, body, detail)
            VALUES (%s, 'llm_topline', 'narrative_hs_group', %s,
                    'nk-test', 'sig-test', 'Lead: test', 'b', %s::jsonb)
            """,
            (run, [hg_id], json.dumps(detail)),
        )
        conn.commit()

    leads = briefing_pack.render_leads()
    # The Investigation leads document's per-finding Provenance bullet
    # for caveats shows surviving (per-finding-variable) caveats only;
    # universal caveats are suppressed.
    assert "Caveats from underlying findings" in leads
    inline_caveat_line = next(
        line for line in leads.splitlines()
        if line.startswith("- *Caveats from underlying findings*:")
    )
    assert "low_kg_coverage" in inline_caveat_line
    assert "cif_fob" not in inline_caveat_line
    assert "currency_timing" not in inline_caveat_line
    # Caveat codes link to the methodology caveats table.
    assert "methodology.md#3-caveats-reference" in inline_caveat_line
    # Leads no longer appear in the brief itself.
    md = briefing_pack.render()
    assert "Caveats from underlying findings" not in md


def test_leads_provenance_bullets_and_linkification(empty_findings, test_db_url):
    """The per-lead Provenance block renders as three labelled bullets
    (caveats / underlying findings / trace), caveat codes link to the
    methodology table, and lead-body prose gets first-occurrence
    glossary links for heavyweight terms like 'mirror gap'."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        cur.execute("SELECT id FROM hs_groups WHERE name = %s", ("EV batteries (Li-ion)",))
        hg_id = cur.fetchone()[0]
        detail = {
            "method": "llm_topline_v2_lead_scaffold",
            "model": "fake",
            "group": {"id": hg_id, "name": "EV batteries (Li-ion)",
                      "hs_patterns": ["8507%"]},
            "lead_scaffold": {
                # Body mentions both a glossary term (mirror gap) and a
                # caveat code (low_base_effect) — both should be linked.
                # Second sentence repeats "mirror gap" — should NOT be
                # linked (first occurrence already taken).
                "anomaly_summary": (
                    "The mirror gap on EV batteries widened sharply; "
                    "the low_base_effect caveat applies. A second mirror "
                    "gap reference here should remain plain."
                ),
                "hypotheses": [],
                "corroboration_steps": [],
            },
            "underlying_finding_ids": [101, 102, 103],
            "caveat_codes": ["low_base_effect", "partial_window"],
        }
        cur.execute(
            """
            INSERT INTO findings (scrape_run_id, kind, subkind, hs_group_ids,
                                  natural_key_hash, value_signature, title, body, detail)
            VALUES (%s, 'llm_topline', 'narrative_hs_group', %s,
                    'nk-leads-prov', 'sig-leads-prov', 'Lead: test', 'b', %s::jsonb)
            RETURNING id
            """,
            (run, [hg_id], json.dumps(detail)),
        )
        lead_id = cur.fetchone()[0]
        conn.commit()

    leads = briefing_pack.render_leads()

    # Provenance shape: three bullets under a **Provenance:** header.
    assert "**Provenance:**" in leads
    assert "- *Caveats from underlying findings*:" in leads
    assert "- *Underlying findings*: 101, 102, 103" in leads
    assert f"- *Trace*: `finding/{lead_id}`" in leads

    # Caveat codes in the Provenance block are clickable.
    caveat_line = next(
        line for line in leads.splitlines()
        if line.startswith("- *Caveats from underlying findings*:")
    )
    assert "[`low_base_effect`]" in caveat_line
    assert "[`partial_window`]" in caveat_line
    assert "methodology.md#3-caveats-reference" in caveat_line

    # Body-level linkification: first occurrence of "mirror gap" linked,
    # second occurrence plain. low_base_effect in body also linked.
    anomaly_line = next(
        line for line in leads.splitlines()
        if line.startswith("**Anomaly:**")
    )
    assert "[mirror gap](" in anomaly_line  # first occurrence linked
    # Caveat code in body also linked (always, not just first occurrence).
    assert "[`low_base_effect`]" in anomaly_line
    # Second "mirror gap" stays plain — count `[mirror gap]` occurrences.
    assert anomaly_line.count("[mirror gap]") == 1


def test_top_movers_filters_and_composite_ranking(empty_findings, test_db_url):
    """`_compute_top_movers` applies all four filter rules and ranks
    survivors by |yoy_pct| × log10(current_eur). Seed five candidates
    crossing each filter and assert the right one(s) survive."""
    from briefing_pack._helpers import _compute_top_movers

    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_eurostat_release(cur, date(2026, 2, 1))
        # 1. Eligible big mover: +35% on €27B (target rank 1).
        _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)",
                             yoy_pct=0.35, current_eur=27e9, prior_eur=20e9,
                             low_base=False)
        # 2. Eligible smaller-base bigger-move: +40% on €1B (target rank 2).
        _seed_hs_yoy_finding(cur, run, "Drones and unmanned aircraft",
                             yoy_pct=0.40, current_eur=1.0e9, prior_eur=0.7e9,
                             low_base=False)
        # 3. Excluded: |yoy| < 10pp threshold (+5% on €100B).
        _seed_hs_yoy_finding(
            cur, run, "Electrical equipment & machinery (chapters 84-85, broad)",
            yoy_pct=0.05, current_eur=100e9, prior_eur=95e9, low_base=False,
        )
        # 4. Excluded: current < €100M (+30% on €50M).
        _seed_hs_yoy_finding(cur, run, "Honey",
                             yoy_pct=0.30, current_eur=50e6, prior_eur=38e6,
                             low_base=False)
        # 5. Excluded: low_base = True (+50% on €200M with low_base flag).
        _seed_hs_yoy_finding(cur, run, "Cotton (raw + woven fabrics)",
                             yoy_pct=0.50, current_eur=200e6, prior_eur=133e6,
                             low_base=True)
        conn.commit()

    with psycopg2.connect(test_db_url) as conn, conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor,
    ) as cur:
        # No predictability data seeded → all groups are badge-less,
        # which is eligible (the filter excludes only 🔴).
        movers = _compute_top_movers(cur, predictability={})

    names = [m["group_name"] for m in movers]
    assert "EV batteries (Li-ion)" in names
    assert "Drones and unmanned aircraft" in names
    # The three excluded groups must NOT appear.
    assert "Electrical equipment & machinery (chapters 84-85, broad)" not in names
    assert "Honey" not in names
    assert "Cotton (raw + woven fabrics)" not in names
    # Ranking: EV batteries (0.35 × log10(27e9) ≈ 3.65) beats Drones
    # (0.40 × log10(1e9) = 3.60). Tight but deterministic.
    assert names[0] == "EV batteries (Li-ion)"
    assert names[1] == "Drones and unmanned aircraft"


def test_top_movers_excludes_stale_anchors(empty_findings, test_db_url):
    """Findings whose `current_end` isn't the latest anchor in the
    hs_group_yoy* family are skipped. Without this filter, a stale 2022
    finding on a fringe HS code can outrank legitimate 2026 movers.
    Observed on MPPT solar inverters (CN8 85044084) on the live DB."""
    from briefing_pack._helpers import _compute_top_movers
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_eurostat_release(cur, date(2022, 12, 1))
        _seed_eurostat_release(cur, date(2026, 2, 1))
        # Stale: large % move on a niche group, 4 years old.
        _seed_hs_yoy_finding(cur, run, "MPPT solar inverters (CN8 85044084)",
                             yoy_pct=1.20, current_eur=1.5e9, prior_eur=0.7e9,
                             low_base=False, period=date(2022, 12, 1))
        # Current: legitimate mover at the latest anchor.
        _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)",
                             yoy_pct=0.35, current_eur=27e9, prior_eur=20e9,
                             low_base=False, period=date(2026, 2, 1))
        conn.commit()

    with psycopg2.connect(test_db_url) as conn, conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor,
    ) as cur:
        movers = _compute_top_movers(cur, predictability={})

    names = [m["group_name"] for m in movers]
    assert "EV batteries (Li-ion)" in names
    assert "MPPT solar inverters (CN8 85044084)" not in names


def test_top_movers_excludes_red_predictability(empty_findings, test_db_url):
    """A 🔴 predictability badge excludes the group from top movers even
    if it would otherwise clear the size/move filters."""
    from briefing_pack._helpers import _compute_top_movers

    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_eurostat_release(cur, date(2026, 2, 1))
        _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)",
                             yoy_pct=0.35, current_eur=27e9, prior_eur=20e9,
                             low_base=False)
        conn.commit()

    with psycopg2.connect(test_db_url) as conn, conn.cursor(
        cursor_factory=psycopg2.extras.DictCursor,
    ) as cur:
        # Inject a synthetic 🔴 badge — production code computes this
        # from T-6 pairs but the filter takes the dict as input.
        movers = _compute_top_movers(
            cur, predictability={"EV batteries (Li-ion)": ("🔴", 0.0, 6)},
        )
    assert movers == []


def test_top_movers_section_renders_above_tier_1(empty_findings, test_db_url):
    """The Top-N section sits between the reader's guide and Tier 1
    in the rendered findings.md, and surfaces eligible movers with
    anchor links."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_eurostat_release(cur, date(2026, 2, 1))
        _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)",
                             yoy_pct=0.35, current_eur=27e9, prior_eur=20e9,
                             low_base=False)
        conn.commit()

    md = briefing_pack.render()
    assert "## Top 1 movers this cycle" in md
    assert "[EV batteries (Li-ion)](#ev-batteries-li-ion)" in md
    # Order: Top movers comes before Tier 1.
    assert md.find("## Top 1 movers this cycle") < md.find("## Tier 1")


def test_top_movers_section_absent_when_no_candidates(empty_findings, test_db_url):
    """An empty findings table → no top movers → the section drops out
    entirely; Tier 1 follows the reader's guide directly."""
    md = briefing_pack.render()
    assert "## Top" not in md.split("## Tier 1")[0]


def test_state_of_play_suppresses_volatile_trajectory_inline(
    empty_findings, test_db_url,
):
    """In Tier 2's per-group block, the inline `Trajectory: …`
    annotation is dropped when the underlying shape is `volatile`
    (~68% of HS-group series are classified volatile and the label
    carries no narrative information — methodology §10).

    A non-volatile shape (e.g. `dip_recovery`) still renders inline."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_eurostat_release(cur, date(2026, 2, 1))
        # Volatile group: trajectory line should be SUPPRESSED inline.
        _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)", yoy_pct=0.20)
        _seed_trajectory_finding(
            cur, run, "EV batteries (Li-ion)",
            shape="volatile", shape_label="volatile (multiple direction changes)",
        )
        # Non-volatile group: trajectory line should appear.
        _seed_hs_yoy_finding(cur, run, "Drones and unmanned aircraft", yoy_pct=0.30)
        _seed_trajectory_finding(
            cur, run, "Drones and unmanned aircraft",
            shape="dip_recovery",
            shape_label="dip-and-recovery (was rising, dipped, now rising again)",
        )
        conn.commit()

    md = briefing_pack.render()
    # Carve out each group's Tier-2 state-of-play block by its `### `
    # heading. The blocks are separated by `### ` lines under
    # `## Tier 2 — Current state of play`.
    tier_2 = md.split("## Tier 2 — Current state of play")[1].split("\n## ")[0]
    ev_block = tier_2.split("### EV batteries (Li-ion)")[1].split("\n### ")[0]
    drones_block = tier_2.split("### Drones and unmanned aircraft")[1].split("\n### ")[0]
    # Volatile suppressed inline: no Trajectory annotation anywhere in
    # the EV batteries block.
    assert "Trajectory:" not in ev_block
    assert "volatile" not in ev_block.lower()
    # The dip_recovery shape for the other group keeps its label.
    assert "Trajectory:" in drones_block
    assert "dip-and-recovery" in drones_block


def test_leads_has_full_detail_by_hs_group_heading(empty_findings, test_db_url):
    """leads.md surfaces a `## Full lead detail by HS group` heading
    above the per-group blocks so the structure is visually delimited
    from the Top N digest section above it (mirrors findings.md's
    `## Tier 3 — Full detail by HS group` shape)."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        cur.execute("SELECT id FROM hs_groups WHERE name = %s",
                    ("EV batteries (Li-ion)",))
        hg_id = cur.fetchone()[0]
        detail = {
            "method": "llm_topline_v2_lead_scaffold",
            "model": "fake",
            "group": {"id": hg_id, "name": "EV batteries (Li-ion)",
                      "hs_patterns": ["8507%"]},
            "lead_scaffold": {
                "anomaly_summary": "Test anomaly.",
                "hypotheses": [],
                "corroboration_steps": [],
            },
            "underlying_finding_ids": [],
            "caveat_codes": [],
        }
        cur.execute(
            """
            INSERT INTO findings (scrape_run_id, kind, subkind, hs_group_ids,
                                  natural_key_hash, value_signature, title, body, detail)
            VALUES (%s, 'llm_topline', 'narrative_hs_group', %s,
                    'nk-heading', 'sig-heading', 'Lead: test', 'b', %s::jsonb)
            """,
            (run, [hg_id], json.dumps(detail)),
        )
        conn.commit()

    leads = briefing_pack.render_leads()
    assert "## Full lead detail by HS group" in leads
    # The heading sits above the per-group `### {group}` block.
    full_detail_idx = leads.find("## Full lead detail by HS group")
    group_block_idx = leads.find("### EV batteries (Li-ion)")
    assert full_detail_idx < group_block_idx


def test_threshold_fragility_annotation_helper():
    """Pure-function test: a finding within 1.5x of the threshold (above
    OR below it) gets an annotation; outside that band returns None."""
    from briefing_pack import _threshold_fragility_annotation as fn
    threshold = 5e7  # €50M
    # Just below the threshold
    assert fn(4.8e7, 4.9e7, threshold) is not None
    # Just above the threshold
    assert fn(5.5e7, 4.5e7, threshold) is not None
    # Well below the band (curr=€10M, threshold=€50M → ratio 0.2)
    assert fn(1e7, 1e7, threshold) is None
    # Well above the band (curr=€200M, threshold=€50M → ratio 4)
    assert fn(2e8, 2e8, threshold) is None
    # Edge: NULL inputs return None
    assert fn(None, 1e9, threshold) is None
    assert fn(1e9, None, threshold) is None
    assert fn(1e9, 1e9, None) is None


def test_predictability_badge_appears_when_t_minus_6_pair_exists(
    empty_findings, test_db_url,
):
    """Phase: per-group YoY-predictability badge. Seed the same group at
    T (2026-02) and T-6 (2025-08) with the same yoy_pct → 100% persistent
    → 🟢 badge. Seed another with sign-flipped yoy → 🔴 badge.

    Need at least PREDICTABILITY_MIN_PAIRS = 3 (scope, flow) permutations
    with both-anchors data for the badge to render at all — seed three
    subkinds per group so the gate passes."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        for y, m in [(2024, 2), (2024, 8), (2025, 2), (2025, 8), (2026, 2)]:
            _seed_eurostat_release(cur, date(y, m, 1))
        # Persistent group: same yoy at T and T-6, across 3 subkinds.
        for sk in ("hs_group_yoy", "hs_group_yoy_export", "hs_group_yoy_combined"):
            _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)", subkind=sk,
                                 yoy_pct=0.30, period=date(2025, 8, 1))
            _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)", subkind=sk,
                                 yoy_pct=0.32, period=date(2026, 2, 1))
        # Volatile group: sign flip between T-6 and T, across 3 subkinds.
        for sk in ("hs_group_yoy", "hs_group_yoy_export", "hs_group_yoy_combined"):
            _seed_hs_yoy_finding(cur, run, "Rare-earth materials", subkind=sk,
                                 yoy_pct=0.40, period=date(2025, 8, 1))
            _seed_hs_yoy_finding(cur, run, "Rare-earth materials", subkind=sk,
                                 yoy_pct=-0.30, period=date(2026, 2, 1))
        conn.commit()

    md = briefing_pack.render(top_n=20)
    # The persistent group gets 🟢; the volatile group gets 🔴.
    assert "🟢" in md
    assert "🔴" in md
    # Anchor the assertion to the specific group lines so we know each
    # got the right badge.
    ev_line = next(line for line in md.splitlines()
                   if line.startswith("### EV batteries (Li-ion)"))
    assert "🟢" in ev_line
    re_line = next(line for line in md.splitlines()
                   if line.startswith("### Rare-earth materials"))
    assert "🔴" in re_line
    # Brief explains the badge.
    assert "YoY predictability" in md


def test_predictability_badge_suppressed_below_min_pairs(
    empty_findings, test_db_url,
):
    """Predictability badges require PREDICTABILITY_MIN_PAIRS=3 (scope, flow)
    permutations with T-6 pair data. Below that, the brief should NOT show a
    badge — the signal is too sparse to be a confident editorial cue.

    Seed only one subkind with T and T-6 data → one permutation → badge
    suppressed. The group still renders in the brief; just without a badge."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        for y, m in [(2024, 2), (2024, 8), (2025, 2), (2025, 8), (2026, 2)]:
            _seed_eurostat_release(cur, date(y, m, 1))
        _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)",
                             subkind="hs_group_yoy",
                             yoy_pct=0.30, period=date(2025, 8, 1))
        _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)",
                             subkind="hs_group_yoy",
                             yoy_pct=0.32, period=date(2026, 2, 1))
        conn.commit()

    md = briefing_pack.render(top_n=20)
    ev_line = next(line for line in md.splitlines()
                   if line.startswith("### EV batteries (Li-ion)"))
    # No badge emoji should appear on the heading line.
    assert "🟢" not in ev_line
    assert "🟡" not in ev_line
    assert "🔴" not in ev_line


def test_threshold_fragility_appears_in_brief(empty_findings, test_db_url):
    """A finding whose 12mo EUR sits just above the €50M threshold gets a
    fragility annotation in the brief — even though `low_base = False`."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_eurostat_release(cur, date(2026, 2, 1))
        # current=€55M, prior=€60M — both above €50M threshold (so
        # low_base=False), but smaller-of (€55M) is within 1.5x band.
        _seed_hs_yoy_finding(cur, run, "EV batteries (Li-ion)",
                             yoy_pct=0.10, current_eur=5.5e7,
                             prior_eur=6.0e7, low_base=False)
        conn.commit()

    md = briefing_pack.render(top_n=20)
    assert "Near low-base threshold" in md
    assert "⚖️" in md


def test_cif_fob_baseline_per_finding_renders_in_mirror_gap(
    empty_findings, test_db_url,
):
    """The mirror-gap section now displays the per-finding CIF/FOB baseline
    (from detail.cif_fob_baseline) and the excess over it."""
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        rel_eu = _seed_eurostat_release(cur, date(2026, 2, 1))
        rel_gacc = _seed_gacc_release(cur, date(2026, 2, 1))
        gacc_obs = _seed_observation(cur, run, rel_gacc)
        eu_obs = _seed_observation(cur, run, rel_eu)
        # Seed a mirror_gap finding with the new cif_fob_baseline detail
        # the v5 method emits.
        detail = {
            "iso2": "NL",
            "gacc": {"partner_label_raw": "Netherlands", "value_eur_converted": 1e10},
            "eurostat": {"total_eur": 1.7e10},
            "gap_eur": 7e9, "gap_pct": 0.65, "is_aggregate": False,
            "cif_fob_baseline": {
                "baseline_pct": 0.0655,
                "scope": "per-partner",
                "partner_iso2": "NL",
                "source": "OECD ITIC dataset 2022 (NL)",
                "source_url": "https://www.oecd.org/...",
            },
        }
        cur.execute(
            "INSERT INTO findings (scrape_run_id, kind, subkind, observation_ids, "
            "                       score, title, body, detail) "
            "VALUES (%s, 'anomaly', 'mirror_gap', %s, 0.65, 'NL gap', 'b', %s::jsonb)",
            (run, [gacc_obs, eu_obs], json.dumps(detail)),
        )
        conn.commit()

    md = briefing_pack.render()
    assert "**CIF/FOB baseline**: 6.55%" in md
    assert "(per-partner)" in md
    assert "OECD ITIC" in md
    # Excess: |0.65| - 0.0655 = 0.5845 → +58.5pp
    assert "+58.5 pp" in md


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
