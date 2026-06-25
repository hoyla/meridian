"""Regression test for finding A1 (2026-06-25 adversarial-correctness review).

GACC is the most fragile source (HTML parsed structurally) yet had the weakest
failure detection: on a parse that yielded zero observations, scrape_release
still created a `releases` row and recorded the run as `success`. A single
added/removed column upstream zeroes the structural row detector, so format
drift would land as a clean success with no data — a phantom release the
overdue-release alert reads as "new data" and a silently-missing month for the
YoY analysers.

The guard: zero observations → record the run `failed` and create NO release
row, matching the Eurostat/HMRC contract. The happy path (>=1 observation) is
unchanged.
"""
from __future__ import annotations

from datetime import date

import psycopg2
import pytest

import api_client
import db
import parse
import scrape

_URL = "http://example/gacc/section4-202603"


def _fake_fetch(url: str, *a, **k) -> api_client.FetchResult:
    return api_client.FetchResult(
        url=url,
        status_code=200,
        content_type="text/html",
        content=b"<html><body>drifted layout, no parseable rows</body></html>",
        sha256="0" * 64,
    )


def _meta() -> parse.ReleaseMetadata:
    return parse.ReleaseMetadata(
        section_number=4,
        description="Imports and Exports by Country",
        period=date(2026, 3, 1),
        currency="CNY",
        publication_date=date(2026, 4, 18),
        unit="CNY 100 Million",
        excel_url=None,
        source_url=_URL,
        title="Imports and Exports by Country (in CNY)",
    )


def test_empty_parse_creates_no_release_and_marks_failed(
    clean_db, test_db_url, monkeypatch,
):
    monkeypatch.setattr(api_client, "fetch", _fake_fetch)
    monkeypatch.setattr(
        parse, "parse_response",
        lambda *a, **k: parse.ParseResult(metadata=_meta(), observations=[]),
    )

    scrape.scrape_release(_URL, force_refetch=True)

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM releases WHERE source = 'gacc'")
        n_releases = cur.fetchone()[0]
        cur.execute(
            "SELECT status, error_message FROM scrape_runs ORDER BY id DESC LIMIT 1"
        )
        status, error_message = cur.fetchone()

    assert n_releases == 0, "an empty parse must not create a phantom release row"
    assert status == "failed"
    assert error_message and "0 observations" in error_message


def test_nonempty_parse_still_takes_the_success_path(
    clean_db, test_db_url, monkeypatch,
):
    # Control: the guard is scoped to the empty case only. With at least one
    # observation, scrape_release must still create the release and finish
    # 'success'. Persistence is spied so the assertion targets the guard's
    # branch logic, not upsert_observations' obs-dict contract (covered
    # elsewhere).
    calls: dict[str, object] = {}
    monkeypatch.setattr(api_client, "fetch", _fake_fetch)
    monkeypatch.setattr(
        parse, "parse_response",
        lambda *a, **k: parse.ParseResult(
            metadata=_meta(),
            observations=[{"partner_country": "United States", "flow": "export"}],
        ),
    )
    monkeypatch.setattr(
        db, "find_or_create_gacc_release",
        lambda meta, release_kind: calls.setdefault("release_kind", release_kind) or 999,
    )
    monkeypatch.setattr(
        db, "upsert_observations",
        lambda run_id, release_id, obs: calls.__setitem__("upserted", len(obs))
        or {"inserted": len(obs), "versioned": 0, "unchanged": 0},
    )
    monkeypatch.setattr(
        db, "finish_run",
        lambda run_id, status, **k: calls.__setitem__("status", status),
    )

    scrape.scrape_release(_URL, force_refetch=True)

    assert calls.get("upserted") == 1, "the non-empty path must reach upsert"
    assert calls.get("status") == "success"
