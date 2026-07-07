"""Tests for the section-4 parse *floor* (finding F5, 2026-07-07).

The A1 empty-parse guard (test_gacc_empty_parse_guard.py) rejects a parse that
yields ZERO observations. F5 closes the adjacent gap: a *partial* parse — some
rows silently dropped by column-layout drift, or a truncated preliminary table —
yields >0 observations yet an incomplete/garbled partner set, which the empty
guard waves through and the YoY analysers read as a complete month.

parse.section4_floor_check applies two layout-independent invariants — the count
of top-level partners, and the 'Total' grand-total row carrying the max value —
verified here against the real mar2026 (monthly) and janfeb2025 (cumulative)
fixtures, plus the failure shapes each invariant is meant to catch.
"""
from __future__ import annotations

import copy
import dataclasses
import hashlib
from datetime import date

import psycopg2
import pytest

import api_client
import db
import parse
import scrape

_FIX = "tests/fixtures"
_MONTHLY = "release_section4_by_country_mar2026_cny.html"
_JANFEB = "release_section4_by_country_janfeb2025_cny.html"


def _parse_fixture(name: str) -> parse.ParseResult:
    content = open(f"{_FIX}/{name}", "rb").read()
    fr = api_client.FetchResult(
        url=f"file://{name}", status_code=200, content_type="text/html",
        content=content, sha256=hashlib.sha256(content).hexdigest(),
    )
    return parse.parse_response(fr, expected_currency="CNY")


# --- pure unit tests (no DB) ------------------------------------------------

@pytest.mark.parametrize("name", [_MONTHLY, _JANFEB])
def test_healthy_release_clears_the_floor(name):
    res = _parse_fixture(name)
    assert parse.section4_floor_check(res.observations, res.metadata) is None


def test_truncated_partner_set_is_rejected():
    # A stub/truncated table with only a handful of partners must be caught by
    # the count floor.
    res = _parse_fixture(_MONTHLY)
    keep = {"Total", "European Union", "United States", "ASEAN"}
    thin = [o for o in res.observations if o.get("partner_country") in keep]
    reason = parse.section4_floor_check(thin, res.metadata)
    assert reason and "partners" in reason


def test_misread_value_columns_break_total_dominance():
    # Simulate a same-width value-column shift: inflate one non-Total partner
    # above the grand total. The count floor still passes; the magnitude check
    # fires because 'Total' no longer carries the max.
    res = _parse_fixture(_MONTHLY)
    obs = copy.deepcopy(res.observations)
    grand_max = max(o["value"] for o in obs)
    for o in obs:
        if o.get("partner_country") == "France":
            o["value"] = grand_max * 2
            break
    reason = parse.section4_floor_check(obs, res.metadata)
    assert reason and "Total" in reason


def test_absent_total_row_alone_does_not_fail_a_full_partner_set():
    # Deliberate design choice: the 'Total' row is not consumed by the
    # per-partner analysers, so losing only its label must NOT block an
    # otherwise-complete release. The magnitude check is conditional on the
    # Total row being present.
    res = _parse_fixture(_MONTHLY)
    no_total = [o for o in res.observations if o.get("partner_country") != "Total"]
    assert parse.section4_floor_check(no_total, res.metadata) is None


def test_non_section4_meta_is_not_floored():
    res = _parse_fixture(_MONTHLY)
    other = dataclasses.replace(res.metadata, section_number=2)
    assert parse.section4_floor_check([], other) is None


# --- integration: the floor drives scrape_release's failed/no-release path ---

_URL = "http://example/gacc/section4-202606"


def _fake_fetch(url: str, *a, **k) -> api_client.FetchResult:
    return api_client.FetchResult(
        url=url, status_code=200, content_type="text/html",
        content=b"<html><body>partial layout</body></html>", sha256="0" * 64,
    )


def _meta() -> parse.ReleaseMetadata:
    return parse.ReleaseMetadata(
        section_number=4, description="Imports and Exports by Country",
        period=date(2026, 6, 1), currency="CNY", publication_date=date(2026, 7, 8),
        unit="CNY 100 Million", excel_url=None, source_url=_URL,
        title="Imports and Exports by Country (in CNY)",
    )


def test_partial_parse_creates_no_release_and_marks_failed(
    clean_db, test_db_url, monkeypatch,
):
    # Three partners — below the floor — so the guard trips and takes the same
    # failed/no-release path as the empty guard.
    thin = [
        parse.ParsedObservation(
            section_number=4, period="2026-06-01", period_kind="monthly",
            currency="CNY", flow="export", partner_country=c,
            partner_is_subset=False, value=100.0,
        )
        for c in ("United States", "European Union", "Japan")
    ]
    monkeypatch.setattr(api_client, "fetch", _fake_fetch)
    monkeypatch.setattr(
        parse, "parse_response",
        lambda *a, **k: parse.ParseResult(metadata=_meta(), observations=thin),
    )

    scrape.scrape_release(_URL, force_refetch=True)

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM releases WHERE source = 'gacc'")
        n_releases = cur.fetchone()[0]
        cur.execute(
            "SELECT status, error_message FROM scrape_runs ORDER BY id DESC LIMIT 1"
        )
        status, error_message = cur.fetchone()

    assert n_releases == 0, "a sub-floor parse must not create a phantom release"
    assert status == "failed"
    assert error_message and "plausibility floor" in error_message
