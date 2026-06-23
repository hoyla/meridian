"""Tests for notify — Google Chat new-data alerts.

The transport (post_to_chat) is exercised with httpx mocked; the trigger /
watermark / idempotency logic runs against the live test DB (same pattern as
test_routine_log). The two subtle guarantees under test:

  * the trigger reads routine_check_log, so a GACC/HMRC-only run (no Eurostat
    export) still notifies — the whole reason we don't hang off the export; and
  * a successful post advances a high-water mark, so re-running is a no-op,
    while a *failed* post leaves the mark put so the next run retries.
"""
from __future__ import annotations

from datetime import date

import pytest

import notify
import routine_log


# --- transport (no DB) -------------------------------------------------------


class _FakeResponse:
    def __init__(self, raises: Exception | None = None):
        self._raises = raises

    def raise_for_status(self) -> None:
        if self._raises is not None:
            raise self._raises


def test_post_to_chat_noop_without_webhook(monkeypatch):
    monkeypatch.delenv(notify.WEBHOOK_ENV, raising=False)
    # No env var and no explicit url → harmless no-op, returns False.
    assert notify.post_to_chat("hi") is False


def test_post_to_chat_posts_and_returns_true(monkeypatch):
    captured = {}

    def fake_post(url, json, timeout):
        captured["url"] = url
        captured["json"] = json
        return _FakeResponse()

    monkeypatch.setattr(notify.httpx, "post", fake_post)
    ok = notify.post_to_chat("*hello*", webhook_url="https://chat.example/x")
    assert ok is True
    assert captured["url"] == "https://chat.example/x"
    assert captured["json"] == {"text": "*hello*"}


def test_post_to_chat_swallows_transport_error(monkeypatch):
    def fake_post(url, json, timeout):
        return _FakeResponse(raises=RuntimeError("503"))

    monkeypatch.setattr(notify.httpx, "post", fake_post)
    # raise_for_status blows up inside → caught → False, never propagates.
    assert notify.post_to_chat("x", webhook_url="https://chat.example/x") is False


# --- trigger / watermark / idempotency (live test DB) ------------------------


@pytest.fixture
def stub_post(monkeypatch):
    """Replace post_to_chat with a recorder; default success. Set
    `stub_post.ok = False` to simulate a failed send."""
    calls: list[str] = []

    def _stub(text, *, webhook_url=None, timeout=notify.POST_TIMEOUT_SECONDS):
        calls.append(text)
        return _stub.ok

    _stub.ok = True
    _stub.calls = calls
    monkeypatch.setattr(notify, "post_to_chat", _stub)
    return _stub


def test_no_post_when_nothing_new(clean_db, stub_post):
    res = notify.notify_new_data()
    assert res.posted is False
    assert "no new data" in res.reason
    assert stub_post.calls == []


def test_posts_on_new_data_and_writes_watermark(clean_db, test_db_url, stub_post):
    routine_log.log_check(
        "eurostat", "new_data", expectation="due",
        candidate_period=date(2026, 4, 1), notes="ingested 159260 raw rows for 2026-04",
    )
    res = notify.notify_new_data()

    assert res.posted is True
    assert len(stub_post.calls) == 1
    assert "Eurostat" in stub_post.calls[0]
    # A _notify completed row (the watermark + audit trail) was written.
    import psycopg2
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT result, notes FROM routine_check_log WHERE source = %s",
            (notify.NOTIFY_SOURCE,),
        )
        rows = cur.fetchall()
    assert rows == [("completed", "posted: Eurostat")]


def test_gacc_only_run_still_notifies(clean_db, stub_post):
    """The core requirement: GACC brings new data with no Eurostat advance and
    no export — the alert must still fire (and carry no briefing line)."""
    routine_log.log_check("gacc", "new_data", notes="fetched 2 new releases")
    res = notify.notify_new_data()

    assert res.posted is True
    msg = stub_post.calls[0]
    assert "GACC" in msg
    assert "fetched 2 new releases" in msg
    assert "fresh briefing" not in msg  # no periodic_run_log export → no briefing line


def test_idempotent_second_call_is_noop(clean_db, stub_post):
    routine_log.log_check("hmrc", "new_data", notes="ingested 38236 raw rows for 2026-04")

    first = notify.notify_new_data()
    assert first.posted is True

    # Same data, no new rows since the watermark → nothing to post.
    second = notify.notify_new_data()
    assert second.posted is False
    assert "no new data" in second.reason
    assert len(stub_post.calls) == 1  # posted exactly once across both calls


def test_failed_post_does_not_advance_watermark(clean_db, test_db_url, stub_post):
    routine_log.log_check("gacc", "new_data", notes="fetched 1 new release")
    stub_post.ok = False  # simulate Google Chat being unreachable

    failed = notify.notify_new_data()
    assert failed.posted is False
    assert "retry" in failed.reason

    # An _notify *error* row was written, but no *completed* — so the mark
    # didn't advance.
    import psycopg2
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT result FROM routine_check_log WHERE source = %s",
            (notify.NOTIFY_SOURCE,),
        )
        assert cur.fetchall() == [("error",)]

    # Next run (chat back up) retries the same data and posts.
    stub_post.ok = True
    retry = notify.notify_new_data()
    assert retry.posted is True
    assert len(stub_post.calls) == 2


def test_dry_run_builds_message_without_posting(clean_db, test_db_url, stub_post):
    routine_log.log_check("eurostat", "new_data", candidate_period=date(2026, 4, 1),
                          notes="ingested rows")
    res = notify.notify_new_data(dry_run=True)

    assert res.posted is False
    assert "dry-run" in res.reason
    assert res.message and "Eurostat" in res.message
    assert stub_post.calls == []  # nothing sent

    # No watermark written on a dry run.
    import psycopg2
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM routine_check_log WHERE source = %s",
            (notify.NOTIFY_SOURCE,),
        )
        assert cur.fetchone()[0] == 0


def test_message_includes_export_line_when_briefing_written(clean_db, stub_post):
    """When a periodic-run wrote an export this cycle, the alert carries the
    briefing period + bundle path."""
    routine_log.log_check("eurostat", "new_data", candidate_period=date(2026, 4, 1),
                          notes="ingested rows")
    import periodic_run_log
    periodic_run_log.log_run(
        action_taken=True,
        reason="new export written for data_period 2026-04",
        data_period=date(2026, 4, 1),
        findings_path="exports/2026-06-16-0841/02_Findings.md",
        analyser_counts=None,
        duration_ms=1000,
        forced=False,
        skip_llm=False,
    )
    notify.notify_new_data()
    msg = stub_post.calls[0]
    assert "fresh briefing" in msg
    assert "2026-04" in msg
    assert "exports/2026-06-16-0841/02_Findings.md" in msg
