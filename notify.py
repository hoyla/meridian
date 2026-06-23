"""Outbound notification to a Google Chat (Spaces) incoming webhook.

The daily Routine (`.claude/scheduled-tasks/meridian-daily-periodic-run/`)
probes Eurostat / HMRC / GACC, writing one `routine_check_log` row per source
with `result ∈ {new_data, no_change, error}`. After the probes + the
`--periodic-run` orchestrator, the Routine calls `scrape.py --notify-chat`,
which posts to the Space *only when a source ingested new data this run* —
the faithful "new data acquired from any source" signal Luke asked for.

Why anchor on `routine_check_log` and not `PeriodicRunResult.new_data`:
only Eurostat advances the export cycle, so the export-side signal is silent
on the days GACC/HMRC bring fresh rows but Eurostat doesn't. The per-source
probe rows fire regardless, so they're the correct trigger.

High-water mark + audit trail (no migration): each successful post writes a
`source='_notify', result='completed'` row to `routine_check_log` (the `notes`
column carries the one-line summary that was posted). The next run only
considers `new_data` rows newer than that mark, so:
  * re-running `--notify-chat` in the same fire is a no-op (idempotent), and
  * the twice-daily Routine only re-posts when something genuinely newer
    landed since the previous post.
A failed post writes `result='error'` (mark does NOT advance, so the next run
retries the same data). `source` has no CHECK constraint and `result` already
permits completed/error (schema.sql:860), so this needs no schema change.

Transport is a single HTTPS POST — no OAuth, no refresh token to expire — so
it runs unattended from cron indefinitely. Best-effort throughout: a missing
webhook or a transport failure never raises into the pipeline.
"""
from __future__ import annotations

import dataclasses
import logging
import os
from datetime import date, datetime, timedelta, timezone

import httpx

import db
import routine_log

log = logging.getLogger(__name__)

WEBHOOK_ENV = "MERIDIAN_CHAT_WEBHOOK"

# Reserved routine_check_log.source for the notifier's own high-water mark /
# audit rows (mirrors routine_log.ROUTINE_LIFECYCLE_SOURCE = '_routine').
NOTIFY_SOURCE = "_notify"

# Upstream sources whose `new_data` rows trigger a notification.
TRIGGER_SOURCES: tuple[str, ...] = ("eurostat", "hmrc", "gacc")

# First-run bootstrap: if there is neither a prior `_notify` mark nor a
# `_routine started` row to anchor to, only consider new_data from this far
# back — so a fresh/manual invocation can't replay the entire history.
BOOTSTRAP_LOOKBACK = timedelta(hours=24)

POST_TIMEOUT_SECONDS = 10.0


@dataclasses.dataclass(frozen=True)
class NewDataRow:
    source: str
    notes: str | None
    candidate_period: date | None
    checked_at: datetime


@dataclasses.dataclass(frozen=True)
class NotifyResult:
    posted: bool
    reason: str
    message: str | None = None  # the text built (whether or not it was sent)

    def summary(self) -> str:
        if self.posted:
            return f"notify-chat: posted to Space ({self.reason})"
        return f"notify-chat: no post ({self.reason})"


def post_to_chat(
    text: str,
    *,
    webhook_url: str | None = None,
    timeout: float = POST_TIMEOUT_SECONDS,
) -> bool:
    """POST a plain-text message to the Google Chat webhook. Best-effort:
    returns True on a 2xx, False on a missing webhook or any failure (never
    raises). Google Chat accepts simple `*bold*`, `_italic_`, `<url|label>`
    links, and newlines in the `text` field."""
    url = webhook_url or os.environ.get(WEBHOOK_ENV)
    if not url:
        log.warning(
            "notify: %s not set — skipping chat post (this is a no-op, not "
            "an error)", WEBHOOK_ENV,
        )
        return False
    try:
        resp = httpx.post(url, json={"text": text}, timeout=timeout)
        resp.raise_for_status()
        return True
    except Exception:
        log.exception("notify: failed to POST to Google Chat webhook")
        return False


def _last_notify_mark() -> datetime | None:
    """checked_at of the most recent successful post (the high-water mark)."""
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(checked_at) FROM routine_check_log
            WHERE source = %s AND result = 'completed'
            """,
            (NOTIFY_SOURCE,),
        )
        return cur.fetchone()[0]


def _last_routine_start() -> datetime | None:
    """checked_at of the most recent `_routine started` bookend — the
    fallback anchor on the first-ever notify run."""
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(checked_at) FROM routine_check_log
            WHERE source = %s AND result = 'started'
            """,
            (routine_log.ROUTINE_LIFECYCLE_SOURCE,),
        )
        return cur.fetchone()[0]


def _resolve_mark() -> datetime:
    """The lower bound for 'new data since the last notification'. Prefer the
    last successful post; fall back to this fire's start; else a bounded
    lookback so a first/manual run can't replay all history."""
    mark = _last_notify_mark()
    if mark is not None:
        return mark
    started = _last_routine_start()
    if started is not None:
        return started
    return datetime.now(timezone.utc) - BOOTSTRAP_LOOKBACK


def _new_data_since(mark: datetime) -> list[NewDataRow]:
    """The latest `new_data` row per trigger source newer than `mark`."""
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (source)
                source, notes, candidate_period, checked_at
            FROM routine_check_log
            WHERE source = ANY(%s)
              AND result = 'new_data'
              AND checked_at > %s
            ORDER BY source, checked_at DESC
            """,
            (list(TRIGGER_SOURCES), mark),
        )
        rows = cur.fetchall()
    # Stable, human-sensible order: Eurostat, HMRC, GACC.
    order = {s: i for i, s in enumerate(TRIGGER_SOURCES)}
    return sorted(
        (NewDataRow(source=r[0], notes=r[1], candidate_period=r[2], checked_at=r[3])
         for r in rows),
        key=lambda r: order.get(r.source, 99),
    )


def _latest_export_since(mark: datetime) -> tuple[date | None, str | None] | None:
    """If a `--periodic-run` cycle wrote a fresh export after `mark`, return
    (data_period, findings_path); else None. Best-effort — a missing/empty
    periodic_run_log just means no export enrichment."""
    try:
        with db.transaction() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT data_period, findings_path
                FROM periodic_run_log
                WHERE action_taken = TRUE AND invoked_at > %s
                ORDER BY invoked_at DESC
                LIMIT 1
                """,
                (mark,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return row[0], row[1]
    except Exception:
        log.exception("notify: failed to read periodic_run_log for export enrichment")
        return None


def _source_label(source: str) -> str:
    return {"eurostat": "Eurostat", "hmrc": "HMRC", "gacc": "GACC"}.get(
        source, source
    )


def build_message(
    new_rows: list[NewDataRow],
    export: tuple[date | None, str | None] | None,
) -> str:
    """Render the Google Chat text payload. One bullet per source that brought
    new data (notes verbatim — they're already one-line human summaries), plus
    an export line when a briefing was written this cycle."""
    lines = ["*Meridian — new trade data ingested*"]
    for row in new_rows:
        label = _source_label(row.source)
        period = (
            f" ({row.candidate_period:%Y-%m})" if row.candidate_period else ""
        )
        detail = f" — {row.notes}" if row.notes else ""
        lines.append(f"• {label}{period}{detail}")

    if export is not None:
        data_period, findings_path = export
        period_str = data_period.strftime("%Y-%m") if data_period else "?"
        lines.append("")
        lines.append(
            f"A fresh briefing was written for *{period_str}* — review & "
            "publish (the portal is not auto-published)."
        )
        if findings_path:
            lines.append(f"Bundle: {findings_path}")

    return "\n".join(lines)


def notify_new_data(
    *,
    webhook_url: str | None = None,
    dry_run: bool = False,
) -> NotifyResult:
    """Post to the Space iff a trigger source ingested new data since the last
    successful post. Writes the high-water-mark / audit row on a real send.

    `dry_run` builds and returns the message without POSTing or moving the
    watermark — for previewing what a run would say."""
    mark = _resolve_mark()
    new_rows = _new_data_since(mark)
    if not new_rows:
        return NotifyResult(
            posted=False,
            reason=f"no new data from any source since {mark:%Y-%m-%d %H:%M}",
        )

    export = _latest_export_since(mark)
    text = build_message(new_rows, export)
    posted_sources = ", ".join(_source_label(r.source) for r in new_rows)

    if dry_run:
        return NotifyResult(
            posted=False,
            reason=f"dry-run — would post (sources: {posted_sources})",
            message=text,
        )

    ok = post_to_chat(text, webhook_url=webhook_url)
    # Record the outcome. On success this advances the watermark and serves as
    # the send audit trail; on failure the watermark stays put so the next run
    # retries the same data.
    try:
        routine_log.log_check(
            NOTIFY_SOURCE,
            "completed" if ok else "error",
            notes=(f"posted: {posted_sources}" if ok else None),
            error=(None if ok else "Google Chat POST failed (see logs)"),
        )
    except Exception:
        log.exception("notify: failed to write _notify audit row")

    reason = (
        f"posted (sources: {posted_sources})" if ok
        else "post failed — watermark not advanced, will retry next run"
    )
    return NotifyResult(posted=ok, reason=reason, message=text)
