"""Tests for the section-5/6 commodity parser (GACC commodity highlights,
dev_notes/2026-07-14-gacc-commodity-highlights.md).

Sections 5/6 — "China's Major Exports/Imports by Quantity and Value" — are
GACC's curated ~30-commodity headline catalogue (no HS codes), China↔world.
Fixtures are real pages pulled from stored source_snapshots (no fetches):
May 2026 CNY+USD exports, May 2026 CNY imports, May 2025 CNY exports (label
drift vs 2026: "Agricultural products" → "Agriculture products"), May 2019
CNY exports (format-drift control — layout unchanged since 2019), and the
January-February 2025 combined release (8-cell cumulative layout).

Covered here: row extraction (both layouts), the aggregate-star and
nbsp-indent conventions, '-' quantity cells → None, the source_row provenance
payload (prior-year cumulative + GACC's published YoY% travel there, NOT as
observations), the count floor, and the currency-unit floor extension to
sections 5/6.
"""
from __future__ import annotations

import hashlib

import pytest

import api_client
import parse

_FIX = "tests/fixtures"
_MAY26_CNY = "release_section5_major_exports_may2026_cny.html"
_MAY26_USD = "release_section5_major_exports_may2026_usd.html"
_MAY26_IMP = "release_section6_major_imports_may2026_cny.html"
_MAY25_CNY = "release_section5_major_exports_may2025_cny.html"
_MAY19_CNY = "release_section5_major_exports_may2019_cny.html"
_JANFEB25 = "release_section5_major_exports_janfeb2025_cny.html"


def _parse_fixture(name: str) -> parse.ParseResult:
    content = open(f"{_FIX}/{name}", "rb").read()
    fr = api_client.FetchResult(
        url=f"file://{name}", status_code=200, content_type="text/html",
        content=content, sha256=hashlib.sha256(content).hexdigest(),
    )
    return parse.parse_response(fr)


# --- metadata ----------------------------------------------------------------

def test_section5_metadata():
    meta = _parse_fixture(_MAY26_CNY).metadata
    assert meta.section_number == 5
    assert meta.currency == "CNY"
    assert meta.unit == "CNY 100 Million"
    assert meta.period.isoformat() == "2026-05-01"
    assert not meta.is_jan_feb_combined


def test_section6_metadata():
    meta = _parse_fixture(_MAY26_IMP).metadata
    assert meta.section_number == 6
    assert meta.unit == "CNY 100 Million"


def test_janfeb_combined_detected():
    meta = _parse_fixture(_JANFEB25).metadata
    assert meta.section_number == 5
    assert meta.is_jan_feb_combined
    assert meta.period.isoformat() == "2025-02-01"


def test_section_inferred_from_description_without_prefix():
    # 2018-era pages can omit the "(N)" title prefix; the description prose
    # must be enough to route to the commodity parser.
    assert parse._infer_section_from_description(
        "China's Major Exports by Quantity and Value") == 5
    assert parse._infer_section_from_description(
        "China's Major Imports by Quantity and Value") == 6


# --- regular monthly layout (10 cells) ----------------------------------------

def test_monthly_release_emits_monthly_and_ytd_pairs():
    res = _parse_fixture(_MAY26_CNY)
    kinds = {o["period_kind"] for o in res.observations}
    assert kinds == {"monthly", "ytd"}
    labels = {o["commodity_label"] for o in res.observations}
    assert len(labels) == 31  # the May-2026 export catalogue
    # every observation carries the export flow and the page unit
    assert {o["flow"] for o in res.observations} == {"export"}
    assert {o["unit"] for o in res.observations} == {"CNY 100 Million"}


def test_cars_row_quantity_value_and_published_yoy():
    # Spot-check against the live page read 2026-07-14: May exports of motor
    # vehicles = 98.8 (10,000 Autos), USD value 16,706.5 M, GACC's published
    # cumulative YoY +50.4% (value) / +48.7% (quantity).
    res = _parse_fixture(_MAY26_USD)
    cars = {
        o["period_kind"]: o for o in res.observations
        if o["commodity_label"].startswith("Motor vehicles")
    }
    m, ytd = cars["monthly"], cars["ytd"]
    assert m["quantity"] == 98.8
    assert m["quantity_unit"] == "10,000 Autos"
    assert m["value"] == 16706.5
    assert ytd["quantity"] == 423.8
    assert ytd["value"] == 73571.6
    src = m["source_row"]
    assert src["published_yoy_value_pct"] == 50.4
    assert src["published_yoy_quantity_pct"] == 48.7
    assert src["prior_year_ytd_value"] == 48907.7
    assert not src["is_aggregate"]


def test_value_only_commodity_has_none_quantity():
    # Plastic articles publishes no quantity — unit cell '-', quantity cells '-'.
    res = _parse_fixture(_MAY26_CNY)
    rows = [o for o in res.observations
            if o["commodity_label"] == "Plastic articles"]
    assert rows, "Plastic articles row missing"
    for o in rows:
        assert o["quantity"] is None
        assert o["quantity_unit"] is None
        assert o["value"] is not None


def test_starred_aggregates_flagged_not_summed():
    # The three catalogue aggregates carry a trailing '*' on the page; the
    # parser strips it into source_row.is_aggregate so downstream selection
    # can exclude them (their membership is NOT adjacency — see build note).
    res = _parse_fixture(_MAY26_CNY)
    aggs = {o["commodity_label"] for o in res.observations
            if o["source_row"]["is_aggregate"]}
    assert aggs == {"Agriculture products", "Mechanical and electrical products",
                    "Hi-tech products"}
    # and the star is stripped from the stored label
    assert all("*" not in a for a in aggs)


def test_indent_captured_for_member_rows():
    res = _parse_fixture(_MAY26_CNY)
    by_label = {o["commodity_label"]: o for o in res.observations
                if o["period_kind"] == "monthly"}
    # Aquatic products is an indented member; the aggregates sit at indent 0.
    assert by_label["Aquatic products"]["partner_indent"] > 0
    assert by_label["Agriculture products"]["partner_indent"] == 0
    # Double-indented rows (ICs) sit deeper than single-indented ones.
    ics = by_label["Electronic integrated circuits"]
    assert ics["partner_indent"] > by_label["Aquatic products"]["partner_indent"]


def test_prior_year_columns_stay_in_source_row_only():
    # Prior-year cumulative + published YoY are provenance, not observations —
    # emitting them would duplicate the prior year's own release. So the
    # period_kind set must never grow beyond monthly/ytd (or the jan-feb kind).
    for name in (_MAY26_CNY, _MAY26_IMP, _MAY19_CNY):
        res = _parse_fixture(name)
        assert {o["period_kind"] for o in res.observations} <= {"monthly", "ytd"}


def test_2019_format_parses_identically():
    # Layout is stable back to 2019 (same 10-cell rows); the catalogue and
    # wording differ ("Aquatic/Marine products", no starred aggregates).
    res = _parse_fixture(_MAY19_CNY)
    labels = {o["commodity_label"] for o in res.observations}
    assert len(labels) == 32
    assert parse.section56_floor_check(res.observations, res.metadata) is None


def test_import_side_flow():
    res = _parse_fixture(_MAY26_IMP)
    assert {o["flow"] for o in res.observations} == {"import"}
    assert len({o["commodity_label"] for o in res.observations}) == 34


# --- combined Jan-Feb layout (8 cells) ----------------------------------------

def test_janfeb_combined_layout():
    res = _parse_fixture(_JANFEB25)
    kinds = {o["period_kind"] for o in res.observations}
    assert kinds == {"cumulative_jan_feb"}
    labels = {o["commodity_label"] for o in res.observations}
    assert len(labels) == 31
    aqua = next(o for o in res.observations
                if o["commodity_label"] == "Aquatic products")
    assert aqua["quantity"] == 65.8
    assert aqua["value"] == 212.9
    src = aqua["source_row"]
    assert src["prior_year_cumulative_value"] == 205.5
    assert src["published_yoy_value_pct"] == 3.6


# --- floor -------------------------------------------------------------------

def test_healthy_releases_clear_the_floor():
    for name in (_MAY26_CNY, _MAY26_IMP, _JANFEB25):
        res = _parse_fixture(name)
        assert parse.section56_floor_check(res.observations, res.metadata) is None


def test_truncated_catalogue_is_rejected():
    res = _parse_fixture(_MAY26_CNY)
    keep = {"Motor vehicles（including chassis fitted with engines)",
            "Electronic integrated circuits", "Footwear"}
    thin = [o for o in res.observations
            if any(o["commodity_label"].startswith(k[:20]) for k in keep)]
    assert thin  # the stub still has observations — that's the point
    reason = parse.section56_floor_check(thin, res.metadata)
    assert reason and "commodities" in reason


def test_floor_ignores_other_sections():
    res = _parse_fixture(_MAY26_CNY)
    import dataclasses
    meta4 = dataclasses.replace(res.metadata, section_number=4)
    assert parse.section56_floor_check(res.observations, meta4) is None


# --- currency-unit floor extension --------------------------------------------

def test_unit_mismatch_raises_for_section5():
    # The release-184 defence now covers sections 5/6 (their cell values reach
    # observations too, and they share section 4's canonical units).
    content = open(f"{_FIX}/{_MAY26_CNY}", "rb").read()
    tampered = content.replace(b"CNY 100 Million", b"USD1 Million")
    with pytest.raises(parse.CurrencyUnitMismatch):
        parse.parse_html(tampered, "file://tampered")
