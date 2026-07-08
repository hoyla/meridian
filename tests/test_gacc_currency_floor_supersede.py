"""The currency/unit floor (release-184 shape: title '(in CNY)' + Unit
'USD1 Million') is superseded-aware (2026-07-08).

When a section-4 page trips the floor, scrape_release checks whether a live
release already covers that (section, period, currency) cell:

  - live sibling exists → the bad page is a duplicate; retire it 'no_parser'
    (terminal, not re-alerted every walk).
  - no live sibling → a genuinely held-back release; record 'failed' so the
    probe surfaces it (release-184 protection preserved for the real case).

This is what silences the standing GACC held-back false alarm without weakening
the guard: 03a39470 (Jun-2025 CNY page, already ingested via its canonical
sibling) stops re-alerting, while a first-seen mismatch still fires.
"""
from __future__ import annotations

from datetime import date

import psycopg2
import pytest

import api_client
import db
import parse
import scrape

_URL = "http://english.customs.gov.cn/Statics/floor-reject.html"


def _fake_fetch(url: str, *a, **k) -> api_client.FetchResult:
    return api_client.FetchResult(
        url=url, status_code=200, content_type="text/html",
        content=b"<html><body>irrelevant</body></html>", sha256="0" * 64,
    )


def _mismatch(*a, **k):
    raise parse.CurrencyUnitMismatch(
        f"GACC page {_URL} self-inconsistent: title declares currency 'CNY' "
        f"but the page's Unit: row reads 'USD1 Million'.",
        section=4, period=date(2025, 6, 1), currency="CNY",
    )


def _live_release() -> parse.ReleaseMetadata:
    # The canonical sibling — title and Unit agree (CNY / 'CNY 100 Million').
    return parse.ReleaseMetadata(
        section_number=4, description="by Country/Region", period=date(2025, 6, 1),
        currency="CNY", publication_date=date(2025, 7, 14), unit="CNY 100 Million",
        excel_url=None, source_url="http://english.customs.gov.cn/Statics/sibling.html",
        title="(4) ... by Country/Region, June 2025 (in CNY)",
    )


def _latest_status(cur) -> tuple[str, str]:
    cur.execute("SELECT status, error_message FROM scrape_runs ORDER BY id DESC LIMIT 1")
    return cur.fetchone()


def test_floor_reject_with_live_sibling_is_retired_no_parser(
    clean_db, test_db_url, monkeypatch,
):
    # A live Jun-2025 CNY release already exists (the canonical sibling).
    db.find_or_create_gacc_release(_live_release(), release_kind="preliminary")

    monkeypatch.setattr(api_client, "fetch", _fake_fetch)
    monkeypatch.setattr(parse, "parse_response", _mismatch)
    scrape.scrape_release(_URL, force_refetch=True)

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        status, msg = _latest_status(cur)
    # Superseded, not held back → terminal no_parser (walk stops retrying, no
    # held-back alert).
    assert status == "no_parser", f"expected no_parser, got {status}: {msg}"


def test_floor_reject_without_live_sibling_is_failed(
    clean_db, test_db_url, monkeypatch,
):
    # No release for the cell → the mismatch is a genuinely held-back release.
    monkeypatch.setattr(api_client, "fetch", _fake_fetch)
    monkeypatch.setattr(parse, "parse_response", _mismatch)
    scrape.scrape_release(_URL, force_refetch=True)

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        status, msg = _latest_status(cur)
    assert status == "failed", f"expected failed, got {status}: {msg}"
    assert "self-inconsistent" in (msg or "")


def test_gacc_release_exists_matches_on_cell_not_release_kind(clean_db, test_db_url):
    assert db.gacc_release_exists(4, date(2025, 6, 1), "CNY") is False
    db.find_or_create_gacc_release(_live_release(), release_kind="preliminary")
    assert db.gacc_release_exists(4, date(2025, 6, 1), "CNY") is True
    # Different cell → not covered.
    assert db.gacc_release_exists(4, date(2025, 6, 1), "USD") is False
    assert db.gacc_release_exists(4, date(2025, 5, 1), "CNY") is False
