"""Tests for the Sheets/XLSX exporter.

Approach: seed a few findings, run the exporter to a temp .xlsx, then load
it back with openpyxl and assert structure + values. We don't mock the SQL
layer — the queries are part of what we want to test.
"""

import json
from datetime import date
from pathlib import Path

import psycopg2
import pytest
from openpyxl import load_workbook

import sheets_export


@pytest.fixture(autouse=True)
def _direct_db_url(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    # Ensure permalink base is unset so the `link` column stays empty for tests
    # that don't explicitly set it.
    monkeypatch.delenv(sheets_export.PERMALINK_BASE_ENV, raising=False)


@pytest.fixture
def empty_findings(test_db_url):
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE findings, observations, source_snapshots, eurostat_raw_rows, "
            "scrape_runs, releases RESTART IDENTITY CASCADE"
        )
    yield


def _seed_one_finding(conn, subkind: str, group_name: str, detail: dict) -> int:
    cur = conn.cursor()
    cur.execute("INSERT INTO scrape_runs (source_url, status) VALUES ('seed', 'success') RETURNING id")
    run = cur.fetchone()[0]
    cur.execute(
        "SELECT id FROM hs_groups WHERE name = %s", (group_name,),
    )
    hg = cur.fetchone()
    hg_ids = [hg[0]] if hg else []
    cur.execute(
        """
        INSERT INTO findings (scrape_run_id, kind, subkind, observation_ids, hs_group_ids,
                              score, title, body, detail)
        VALUES (%s, 'anomaly', %s, '{}', %s, %s, %s, 'b', %s::jsonb)
        RETURNING id
        """,
        (run, subkind, hg_ids, abs(detail.get("totals", {}).get("yoy_pct", 0.0)),
         f"seed {subkind} {group_name}", json.dumps(detail)),
    )
    fid = cur.fetchone()[0]
    conn.commit()
    return fid


def test_export_produces_xlsx_with_all_sheets(empty_findings, test_db_url, tmp_path):
    """An empty findings table still produces all 7 sheets — just with no data rows."""
    out = sheets_export.export(out_format="xlsx", out_path=str(tmp_path / "out.xlsx"))
    wb = load_workbook(out)
    expected_sheets = {
        "summary", "hs_yoy_imports_latest", "hs_yoy_exports_latest",
        "trajectories", "mirror_gaps_latest", "mirror_gap_movers", "low_base_review",
    }
    assert set(wb.sheetnames) == expected_sheets


def test_summary_sheet_picks_latest_per_group(empty_findings, test_db_url, tmp_path):
    """If two periods of findings exist for the same group, the summary sheet
    shows the *latest* — not duplicates, and not the older."""
    period_old = date(2025, 12, 1)
    period_new = date(2026, 2, 1)
    with psycopg2.connect(test_db_url) as conn:
        for period, yoy in [(period_old, 0.10), (period_new, 0.40)]:
            _seed_one_finding(conn, "hs_group_yoy", "EV batteries (Li-ion)", {
                "windows": {"current_end": period.isoformat(), "current_start": period.isoformat()},
                "totals": {
                    "yoy_pct": yoy, "current_12mo_eur": 1e9, "prior_12mo_eur": 0.9e9,
                    "yoy_pct_kg": yoy * 1.5, "current_12mo_kg": 1e6,
                    "low_base": False,
                },
                "group": {"name": "EV batteries (Li-ion)"},
            })

    out = sheets_export.export(out_format="xlsx", out_path=str(tmp_path / "out.xlsx"))
    wb = load_workbook(out)
    ws = wb["summary"]
    # Row 1 is the description, row 3 is headers, row 4+ is data.
    headers = [c.value for c in ws[3]]
    rows = [[c.value for c in row] for row in ws.iter_rows(min_row=4, values_only=False)]
    # Find the EV batteries row
    ev_rows = [r for r in rows if r[headers.index("group")] == "EV batteries (Li-ion)"]
    assert len(ev_rows) == 1
    assert ev_rows[0][headers.index("import_yoy_pct")] == 0.40  # latest, not 0.10


def test_link_column_empty_when_permalink_base_unset(empty_findings, test_db_url, tmp_path):
    with psycopg2.connect(test_db_url) as conn:
        _seed_one_finding(conn, "hs_group_yoy", "EV batteries (Li-ion)", {
            "windows": {"current_end": "2026-02-01", "current_start": "2026-02-01"},
            "totals": {"yoy_pct": 0.4, "current_12mo_eur": 1e9, "prior_12mo_eur": 0.9e9,
                       "low_base": False},
            "group": {"name": "EV batteries (Li-ion)"},
        })

    out = sheets_export.export(out_format="xlsx", out_path=str(tmp_path / "out.xlsx"))
    wb = load_workbook(out)
    ws = wb["hs_yoy_imports_latest"]
    headers = [c.value for c in ws[3]]
    rows = [[c.value for c in r] for r in ws.iter_rows(min_row=4)]
    # link column stays empty because GACC_PERMALINK_BASE is unset
    assert all(r[headers.index("link")] in (None, "") for r in rows)


def test_link_column_emits_hyperlink_formula_when_permalink_base_set(
    empty_findings, test_db_url, tmp_path, monkeypatch,
):
    monkeypatch.setenv(sheets_export.PERMALINK_BASE_ENV, "https://gacc.example")
    with psycopg2.connect(test_db_url) as conn:
        fid = _seed_one_finding(conn, "hs_group_yoy", "EV batteries (Li-ion)", {
            "windows": {"current_end": "2026-02-01", "current_start": "2026-02-01"},
            "totals": {"yoy_pct": 0.4, "current_12mo_eur": 1e9, "prior_12mo_eur": 0.9e9,
                       "low_base": False},
            "group": {"name": "EV batteries (Li-ion)"},
        })

    out = sheets_export.export(out_format="xlsx", out_path=str(tmp_path / "out.xlsx"))
    wb = load_workbook(out)
    ws = wb["hs_yoy_imports_latest"]
    headers = [c.value for c in ws[3]]
    rows = [[c.value for c in r] for r in ws.iter_rows(min_row=4)]
    link = rows[0][headers.index("link")]
    assert link is not None
    assert link.startswith("=HYPERLINK(")
    assert f"finding/{fid}" in link


def test_low_base_review_sheet_only_includes_flagged(empty_findings, test_db_url, tmp_path):
    """The low_base_review sheet must only include findings with low_base=true."""
    with psycopg2.connect(test_db_url) as conn:
        # One flagged, one not
        _seed_one_finding(conn, "hs_group_yoy", "Rare-earth materials", {
            "windows": {"current_end": "2026-02-01", "current_start": "2026-02-01"},
            "totals": {"yoy_pct": 0.5, "current_12mo_eur": 1e7, "prior_12mo_eur": 0.5e7,
                       "low_base": True, "low_base_threshold_eur": 5e7},
            "group": {"name": "Rare-earth materials"},
        })
        _seed_one_finding(conn, "hs_group_yoy", "EV batteries (Li-ion)", {
            "windows": {"current_end": "2026-02-01", "current_start": "2026-02-01"},
            "totals": {"yoy_pct": 0.3, "current_12mo_eur": 2e10, "prior_12mo_eur": 1.5e10,
                       "low_base": False},
            "group": {"name": "EV batteries (Li-ion)"},
        })

    out = sheets_export.export(out_format="xlsx", out_path=str(tmp_path / "out.xlsx"))
    wb = load_workbook(out)
    ws = wb["low_base_review"]
    headers = [c.value for c in ws[3]]
    rows = [[c.value for c in r] for r in ws.iter_rows(min_row=4)]
    groups = [r[headers.index("group")] for r in rows]
    assert "Rare-earth materials" in groups
    assert "EV batteries (Li-ion)" not in groups


def test_google_sheets_writer_raises_until_wired_up():
    with pytest.raises(NotImplementedError, match="not yet wired up"):
        sheets_export.GoogleSheetsWriter().write([], "any-spreadsheet-id")
