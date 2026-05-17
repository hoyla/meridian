"""Tests for the docx output module (`briefing_pack.docx`).

Pure-function tests live first (no DB), followed by integration tests
that seed the test DB and run the full renderer end-to-end.

Determinism testing notes (design doc step 5):
- docx-internal timestamps and openpyxl metadata are NOT byte-stable
  across runs. Don't SHA256-hash the entire .docx — the test will be
  flaky.
- matplotlib Agg with explicit color overrides and explicit DPI IS
  byte-stable on a given host. PNG-bytes tests are safe there.
- Cross-host PNG byte stability is not guaranteed (font fallback,
  freetype version drift). For CI we'd need to pin a font; out of
  scope for v1.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import psycopg2
import pytest

from docx import Document

import briefing_pack
import briefing_pack.docx as bp_docx
from tests.test_briefing_pack import (
    _seed_eurostat_release,
    _seed_hs_yoy_finding,
    _seed_run,
)


@pytest.fixture(autouse=True)
def _direct_db_url(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    monkeypatch.delenv(briefing_pack.PERMALINK_BASE_ENV, raising=False)


@pytest.fixture
def empty_findings(test_db_url):
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE findings, observations, source_snapshots, "
            "eurostat_raw_rows, scrape_runs, releases "
            "RESTART IDENTITY CASCADE"
        )
    yield


# ---------------------------------------------------------------------------
# Pure-function tests
# ---------------------------------------------------------------------------

class TestPickEurScale:
    def test_billions(self):
        scale, label = bp_docx._pick_eur_scale(2.5e9)
        assert scale == 1e9
        assert label == "€ billions"

    def test_millions(self):
        scale, label = bp_docx._pick_eur_scale(450e6)
        assert scale == 1e6
        assert label == "€ millions"

    def test_thousands(self):
        scale, label = bp_docx._pick_eur_scale(5e3)
        assert scale == 1e3
        assert label == "€ thousands"

    def test_units(self):
        scale, label = bp_docx._pick_eur_scale(500)
        assert scale == 1.0
        assert label == "€"

    def test_billions_boundary_exactly_1e9(self):
        # 1e9 should be billions (≥, not >)
        scale, _ = bp_docx._pick_eur_scale(1e9)
        assert scale == 1e9


class TestMonthsBack:
    def test_23_months_back_from_feb_2026(self):
        # Real-world case: current_end = 2026-02-01, 24-month window
        # starts at March 2024 (23 months earlier).
        assert bp_docx._months_back(date(2026, 2, 1), 23) == date(2024, 3, 1)

    def test_1_month_back_across_year_boundary(self):
        assert bp_docx._months_back(date(2026, 1, 1), 1) == date(2025, 12, 1)

    def test_12_months_back_lands_on_same_month_prior_year(self):
        assert bp_docx._months_back(date(2026, 5, 1), 12) == date(2025, 5, 1)

    def test_zero_months_back_is_identity(self):
        assert bp_docx._months_back(date(2026, 5, 1), 0) == date(2026, 5, 1)


class TestMonthIter:
    def test_yields_12_months_inclusive(self):
        months = list(bp_docx._month_iter(date(2025, 3, 1), date(2026, 2, 1)))
        assert len(months) == 12
        assert months[0] == date(2025, 3, 1)
        assert months[-1] == date(2026, 2, 1)

    def test_single_month(self):
        months = list(bp_docx._month_iter(date(2026, 5, 1), date(2026, 5, 1)))
        assert months == [date(2026, 5, 1)]

    def test_spans_year_boundary(self):
        months = list(bp_docx._month_iter(date(2025, 11, 1), date(2026, 2, 1)))
        assert months == [
            date(2025, 11, 1), date(2025, 12, 1),
            date(2026, 1, 1), date(2026, 2, 1),
        ]


class TestFlowLabel:
    def test_export_subkind(self):
        assert bp_docx._flow_label_for_subkind("hs_group_yoy_export") == \
            "EU-27 exports (reporter→CN)"

    def test_import_subkind(self):
        assert bp_docx._flow_label_for_subkind("hs_group_yoy") == \
            "EU-27 imports (CN→reporter)"


class TestBuildChartPng:
    def _series(self) -> dict[date, float]:
        return {
            date(2024, 3 + i, 1) if (3 + i) <= 12 else date(2024 + (3 + i - 1) // 12, (3 + i - 1) % 12 + 1, 1): float(100e6 + i * 5e6)
            for i in range(24)
        }

    def test_returns_png_bytes(self):
        series = self._series()
        png = bp_docx._build_chart_png(
            current_end=date(2026, 2, 1),
            monthly_eur=series,
            group_name="Test group",
            flow_label="EU-27 imports (CN→reporter)",
        )
        assert isinstance(png, bytes)
        # PNG magic bytes
        assert png.startswith(b"\x89PNG\r\n\x1a\n")
        # Reasonable size — not a one-pixel placeholder
        assert len(png) > 5_000

    def test_deterministic_same_input_same_output(self):
        """Same series → same PNG bytes. matplotlib Agg with our color +
        DPI pins is deterministic on a given host. This test catches
        regressions where a future contributor adds non-determinism
        (e.g. random palette, default ax title with timestamp)."""
        series = self._series()
        kwargs = dict(
            current_end=date(2026, 2, 1),
            monthly_eur=series,
            group_name="Test group",
            flow_label="EU-27 imports (CN→reporter)",
        )
        a = bp_docx._build_chart_png(**kwargs)
        b = bp_docx._build_chart_png(**kwargs)
        assert hashlib.sha256(a).hexdigest() == hashlib.sha256(b).hexdigest()

    def test_handles_empty_series(self):
        """Empty data dict — chart still renders (no exception) and
        produces a valid PNG showing an empty plot. Real-world case:
        a finding whose eurostat_raw_rows are missing in the test DB."""
        png = bp_docx._build_chart_png(
            current_end=date(2026, 2, 1),
            monthly_eur={},
            group_name="No data",
            flow_label="EU-27 imports (CN→reporter)",
        )
        assert png.startswith(b"\x89PNG\r\n\x1a\n")

    def test_handles_gaps_in_series(self):
        """Some months missing — matplotlib NaN handling skips them
        rather than raising. Important for real findings where Eurostat
        partial-window caveats apply."""
        series = {
            date(2024, 3, 1): 100e6,
            date(2024, 5, 1): 110e6,  # April missing
            date(2024, 9, 1): 120e6,  # May–Aug missing
        }
        png = bp_docx._build_chart_png(
            current_end=date(2026, 2, 1),
            monthly_eur=series,
            group_name="Sparse data",
            flow_label="EU-27 imports (CN→reporter)",
        )
        assert png.startswith(b"\x89PNG\r\n\x1a\n")


# ---------------------------------------------------------------------------
# Integration tests — full renderer against a seeded DB
# ---------------------------------------------------------------------------

def _seed_chart_capable_finding(
    cur,
    run_id: int,
    group_name: str,
    *,
    subkind: str = "hs_group_yoy",
    hs_pattern: str = "8507%",
    **kwargs,
) -> int:
    """Wrap `_seed_hs_yoy_finding` and patch the detail JSONB to include
    the `method_query.flow` / `method_query.partners` /
    `method_query.hs_patterns` fields the chart fetcher needs.

    The shared `_seed_hs_yoy_finding` helper omits these because the
    older test surface (markdown rendering) doesn't need them; the
    docx renderer's chart-fetch path does. Production findings always
    have all three (set by `anomalies.detect_hs_group_yoy`)."""
    fid = _seed_hs_yoy_finding(
        cur, run_id, group_name, subkind=subkind, **kwargs,
    )
    cur.execute("SELECT detail FROM findings WHERE id = %s", (fid,))
    detail = cur.fetchone()[0]
    detail.setdefault("method_query", {})
    detail["method_query"]["flow"] = (
        2 if subkind.endswith("_export") else 1
    )
    detail["method_query"]["partners"] = ["CN", "HK", "MO"]
    detail["method_query"]["hs_patterns"] = [hs_pattern]
    cur.execute(
        "UPDATE findings SET detail = %s::jsonb WHERE id = %s",
        (json.dumps(detail), fid),
    )
    return fid


def _seed_eurostat_raw_rows_for_finding(
    cur,
    run_id: int,
    *,
    hs_pattern: str,
    reporter: str = "DE",
    partner: str = "CN",
    flow: int = 1,
    anchor: date = date(2026, 2, 1),
    months: int = 24,
    monthly_eur: float = 100e6,
) -> None:
    """Seed `months` of eurostat_raw_rows ending at `anchor` so the chart
    fetch in `_fetch_monthly_eur_series` returns non-empty data."""
    code = hs_pattern.rstrip("%")
    for i in range(months):
        period = bp_docx._months_back(anchor, months - 1 - i)
        cur.execute(
            """
            INSERT INTO eurostat_raw_rows
                (scrape_run_id, period, reporter, partner, product_nc,
                 flow, value_eur, quantity_kg)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (run_id, period, reporter, partner, code, flow,
             monthly_eur * (1.0 + 0.02 * i),  # gentle upward trend
             monthly_eur * 0.5),
        )


def _count_images(doc: Document) -> int:
    return sum(
        1 for p in doc.paragraphs
        for r in p.runs
        if r.element.xpath(".//w:drawing")
    )


def test_render_findings_docx_smoke(empty_findings, test_db_url, tmp_path):
    """Seed one eligible mover + its raw rows, render, and assert the
    docx file is produced and opens as a valid Document with the full
    findings.md content rendered into it."""
    out_path = tmp_path / "smoke.docx"
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_eurostat_release(cur, date(2026, 2, 1))
        _seed_chart_capable_finding(
            cur, run, "EV batteries (Li-ion)",
            yoy_pct=0.35, current_eur=27e9, prior_eur=20e9, low_base=False,
        )
        _seed_eurostat_raw_rows_for_finding(cur, run, hs_pattern="8507%")
        conn.commit()

    result = bp_docx.render_findings_docx(out_path)
    assert result == out_path
    assert out_path.exists()

    doc = Document(str(out_path))
    # v4: full markdown content → many paragraphs (period coverage,
    # scope notes, findings inventory, top movers, methodology footer,
    # etc.) plus at least one chart for the seeded mover.
    assert len(doc.paragraphs) > 20


def test_render_findings_docx_full_content_parity(
    empty_findings, test_db_url, tmp_path,
):
    """v4 contract: the docx contains the same content as findings.md.
    Verify by checking that several known sections from the markdown's
    structural skeleton appear as headings in the docx."""
    out_path = tmp_path / "parity.docx"
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_eurostat_release(cur, date(2026, 2, 1))
        _seed_chart_capable_finding(
            cur, run, "EV batteries (Li-ion)",
            yoy_pct=0.345, current_eur=27.25e9, prior_eur=20.27e9,
        )
        _seed_eurostat_raw_rows_for_finding(cur, run, hs_pattern="8507%")
        conn.commit()

    bp_docx.render_findings_docx(out_path)
    doc = Document(str(out_path))

    headings = [
        p.text.strip()
        for p in doc.paragraphs
        if p.style and p.style.name.startswith("Heading")
    ]
    # The renderer always emits these structural sections regardless
    # of content. Catches regressions where the translator stops
    # walking blocks midway.
    assert any("Scope notes" in h for h in headings), \
        f"expected 'Scope notes' heading, got: {headings[:20]}"
    assert any("Findings included" in h for h in headings), \
        f"expected 'Findings included' heading, got: {headings[:20]}"
    assert any("Methodology" in h for h in headings), \
        f"expected a Methodology heading, got: {headings[:20]}"


def test_render_findings_docx_injects_chart_for_top_mover(
    empty_findings, test_db_url, tmp_path,
):
    """A seeded top mover with underlying raw rows → exactly one chart
    image in the docx (the one in the Top-N section; first-occurrence-
    only guard prevents duplicates in Tier 2 state-of-play)."""
    out_path = tmp_path / "chart.docx"
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_eurostat_release(cur, date(2026, 2, 1))
        _seed_chart_capable_finding(
            cur, run, "EV batteries (Li-ion)",
            yoy_pct=0.345, current_eur=27.25e9, prior_eur=20.27e9,
        )
        _seed_eurostat_raw_rows_for_finding(cur, run, hs_pattern="8507%")
        conn.commit()

    bp_docx.render_findings_docx(out_path)
    doc = Document(str(out_path))

    assert _count_images(doc) == 1


def test_render_findings_docx_skips_chart_for_finding_without_raw_rows(
    empty_findings, test_db_url, tmp_path,
):
    """A finding seeded without underlying eurostat_raw_rows → no
    chart injected (chart-data fetch returns None; translator's
    chart_for_finding lookup returns None; no picture added).
    The rest of the markdown still renders normally."""
    out_path = tmp_path / "no-chart.docx"
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_eurostat_release(cur, date(2026, 2, 1))
        _seed_hs_yoy_finding(
            cur, run, "EV batteries (Li-ion)",
            yoy_pct=0.35, current_eur=27e9, prior_eur=20e9, low_base=False,
        )
        # Deliberately no raw rows + no chart_capable_finding patching,
        # so method_query lacks flow/partners — chart fetch returns None.
        conn.commit()

    bp_docx.render_findings_docx(out_path)
    doc = Document(str(out_path))

    assert _count_images(doc) == 0
    # But the document still has substantive content
    assert len(doc.paragraphs) > 10


def test_render_findings_docx_respects_top_n_for_charts(
    empty_findings, test_db_url, tmp_path,
):
    """`top_n=2` caps the number of charts (not headings — headings
    come from the markdown's structural skeleton). The .md still
    renders all eligible movers in its Top-N section; only the chart
    insertion is bounded by `top_n`."""
    out_path = tmp_path / "topn.docx"
    with psycopg2.connect(test_db_url) as conn:
        cur = conn.cursor()
        run = _seed_run(cur)
        _seed_eurostat_release(cur, date(2026, 2, 1))
        _seed_chart_capable_finding(
            cur, run, "EV batteries (Li-ion)",
            yoy_pct=0.35, current_eur=27e9, prior_eur=20e9,
        )
        _seed_chart_capable_finding(
            cur, run, "Drones and unmanned aircraft",
            yoy_pct=0.40, current_eur=1.0e9, prior_eur=0.7e9,
        )
        _seed_chart_capable_finding(
            cur, run, "Wind generating sets only",
            yoy_pct=0.345, current_eur=375e6, prior_eur=279e6,
        )
        _seed_eurostat_raw_rows_for_finding(cur, run, hs_pattern="8507%")
        conn.commit()

    bp_docx.render_findings_docx(out_path, top_n=2)
    doc = Document(str(out_path))

    assert _count_images(doc) == 2


def test_render_findings_docx_renders_with_no_findings(
    empty_findings, test_db_url, tmp_path,
):
    """Empty DB — markdown render still emits its framing sections
    (period coverage, scope notes, etc.) so the docx is non-empty
    even when no top movers exist. No charts in this case."""
    out_path = tmp_path / "empty.docx"
    # empty_findings has truncated tables; no seeds.

    bp_docx.render_findings_docx(out_path)
    doc = Document(str(out_path))

    # Framing content present (markdown always emits these)
    assert len(doc.paragraphs) > 5
    # No charts because no top movers
    assert _count_images(doc) == 0


def test_render_findings_docx_applies_page_setup(
    empty_findings, test_db_url, tmp_path,
):
    """A4 + 10mm margins must reach the saved .docx — these were the
    fix that prompted the spike, and a regression here would put us
    back to US Letter / 1-inch margins for Lisa."""
    out_path = tmp_path / "pagesetup.docx"
    bp_docx.render_findings_docx(out_path)
    doc = Document(str(out_path))

    section = doc.sections[0]
    # python-docx stores Length objects with `.mm` property
    assert round(section.page_width.mm) == 210
    assert round(section.page_height.mm) == 297
    assert round(section.top_margin.mm) == 10
    assert round(section.bottom_margin.mm) == 10
    assert round(section.left_margin.mm) == 10
    assert round(section.right_margin.mm) == 10
