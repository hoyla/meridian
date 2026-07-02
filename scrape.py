"""GACC + Eurostat trade-statistics scraper.

GACC path: walks customs.gov.cn index pages, snapshots each release page,
parses tables into structured observations, and persists them with versioning
so successive scrapes surface revisions rather than silently overwriting.

Eurostat path: downloads the monthly bulk 7z, stream-decompresses + filters,
aggregates by (reporter, partner, hs, flow), and persists the same way.

Usage:
    python scrape.py                                    # walk all configured GACC index URLs
    python scrape.py --url <url>                        # one-shot GACC fetch (index OR release URL)
    python scrape.py --eurostat-period YYYY-MM          # one-shot Eurostat month (default: partner=CN)
    python scrape.py --eurostat-period YYYY-MM --partner XX [--partner YY]
    python scrape.py --dry-run                          # fetch + parse but don't write to DB
"""

import argparse
import dataclasses
import logging
import os
from datetime import date

from dotenv import load_dotenv

import anomalies
import api_client
import briefing_pack
import db
import eurostat
import fx
import hmrc
import llm_framing
import lookups
import parse
import periodic
import release_calendar
import sheets_export

load_dotenv()
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


# Index URLs and the release_kind their links represent.
# `preliminary.html` serves the current year (the year picker reflects 2026
# at time of writing); `preliminaryYYYY.html` serves the historical year-
# specific archive. GACC publishes year archives back to 2018.
SEED_INDEXES: list[tuple[str, str]] = [
    ("http://english.customs.gov.cn/statics/report/preliminary.html", "preliminary"),
    *(
        (f"http://english.customs.gov.cn/statics/report/preliminary{y}.html", "preliminary")
        for y in range(2018, 2026)
    ),
]


def _is_index_url(url: str) -> bool:
    return "/statics/report/preliminary" in url.lower() or "/statics/report/monthly" in url.lower()


def scrape_index(
    url: str, release_kind: str, dry_run: bool = False,
    *, force_refetch: bool = False,
) -> None:
    log.info("Fetching index %s", url)
    response = api_client.fetch(url)
    discovered = api_client.discover_release_urls(response.content, url)
    log.info("Discovered %d release links from %s", len(discovered), url)
    for rel in discovered:
        expected_period = None
        if rel.get("year") and rel.get("month"):
            expected_period = date(rel["year"], rel["month"], 1)
        scrape_release(
            rel["url"], release_kind=release_kind, dry_run=dry_run,
            expected_currency=rel.get("currency"),
            expected_period=expected_period,
            force_refetch=force_refetch,
        )


def scrape_release(
    url: str, release_kind: str = "preliminary", dry_run: bool = False,
    *, expected_currency: str | None = None,
    expected_period: date | None = None,
    force_refetch: bool = False,
) -> None:
    if not force_refetch and not dry_run:
        prior = db.gacc_release_url_already_processed(url)
        if prior is not None:
            log.info("Skipping %s (already %s; --force-refetch to retry)", url, prior)
            return
    log.info("Fetching release %s", url)
    run_id = db.start_run(url) if not dry_run else None
    try:
        response = api_client.fetch(url)
        if not dry_run:
            db.save_snapshot(run_id, response)
        result = parse.parse_response(
            response,
            expected_currency=expected_currency,
            expected_period=expected_period,
        )
        meta = result.metadata
        log.info(
            "Parsed %d observations from section %d (%s, %s)",
            len(result.observations), meta.section_number, meta.currency, meta.period.isoformat(),
        )
        if not result.observations:
            # A GACC page that fetched (HTTP 200) and parsed without raising
            # but yielded ZERO observations is not a benign "no data yet"
            # state: GACC publishes a release only once the table exists, and
            # section 4 is the only section we parse. An empty parse almost
            # always means the structural row detector skipped every row
            # because the column layout drifted — _parse_section_4_by_country
            # keeps only <tr>s with exactly the expected cell count, so one
            # added or removed column zeroes the whole parse. Record a FAILURE
            # and create NO release row, matching the Eurostat/HMRC contract of
            # never writing a release on an empty parse. A phantom release here
            # reads as "new data" to the overdue-release alert and feeds a
            # silently-missing month to the YoY analysers (the partial_window
            # bias the Jan/Feb fix addressed) — all under a green "success".
            # status='failed' (not 'no_parser'/'success') also lets the next
            # walk retry, since gacc_release_url_already_processed skips failed.
            msg = (
                f"GACC parse yielded 0 observations for section {meta.section_number} "
                f"({meta.currency}, {meta.period.isoformat()}) — likely an upstream "
                f"column-layout change; recording failed, no release row created"
            )
            log.error(msg)
            if not dry_run:
                db.finish_run(run_id, status="failed", error_message=msg)
            return
        if not dry_run:
            # Combined Jan+Feb cumulative releases get their own
            # release_kind so the natural-key on `releases`
            # (section, currency, period, release_kind) doesn't collide
            # with a hypothetical separate-February release for the same
            # year. The period anchor for both is 1 Feb.
            effective_kind = (
                "preliminary_jan_feb" if meta.is_jan_feb_combined
                else release_kind
            )
            release_id = db.find_or_create_gacc_release(meta, release_kind=effective_kind)
            counts = db.upsert_observations(run_id, release_id, result.observations)
            log.info("Persisted: %s", counts)
            db.finish_run(run_id, status="success", http_status=response.status_code)
    except NotImplementedError as e:
        log.warning("No parser yet for %s: %s", url, e)
        if run_id is not None:
            db.finish_run(run_id, status="no_parser", error_message=str(e))
    except Exception as e:
        log.exception("Scrape failed for %s", url)
        if run_id is not None:
            db.finish_run(run_id, status="failed", error_message=str(e))


@dataclasses.dataclass(frozen=True)
class IngestOutcome:
    """What a single-period ingest attempt did, so callers (notably
    `probe_source`) can map it to a routine_check_log result.

    status:
      success — rows ingested for the period
      absent  — the source hasn't published this period yet (no write)
      empty   — the file/response is present but held no rows for our filters
                (no release row created)
      noop    — already ingested; an idempotent no-op, not an error (the guard
                refused a duplicate re-ingest)
      skipped — a precondition was missing (e.g. HMRC FX rate absent)
      failed  — an exception during fetch/parse/persist
    """

    status: str
    rows: int = 0
    error: str | None = None


def scrape_eurostat(
    period: date,
    partners: set[str] | None = None,
    hs_prefixes: tuple[str, ...] | None = None,
    reporters: set[str] | None = None,
    dry_run: bool = False,
) -> IngestOutcome:
    """Fetch one Eurostat monthly bulk file, persist raw rows, aggregate, persist observations.

    The raw CSV rows are stored verbatim in `eurostat_raw_rows`; the aggregated
    per-cell observations in `observations` carry an FK array back to the raw
    rows so any aggregation can be audited or re-derived.

    Gated by a cheap existence probe: the bulk endpoint returns HTTP 200 with an
    HTML body for not-yet-published periods, so a blind download would feed
    garbage to py7zr. `bulk_file_exists` keys on the response headers instead;
    an unpublished period returns IngestOutcome("absent") without touching the DB.

    NB: we don't write the 44 MB raw 7z to source_snapshots — Eurostat bulk files
    are immutable per period (re-fetchable by URL) and storing them would inflate
    the DB. The release row's source_url is the audit trail.
    """
    url = eurostat.bulk_file_url(period)
    if not dry_run and not eurostat.bulk_file_exists(period):
        log.info("Eurostat bulk file for %s not published yet — skipping",
                 period.strftime("%Y-%m"))
        return IngestOutcome(status="absent")
    # Idempotency guard. The raw-row insert is append-only (no ON CONFLICT), so
    # re-ingesting a period/reporter already stored would duplicate it. Keep
    # re-ingest additive: in surgical mode (reporters given) drop those already
    # present; in whole-period mode refuse outright if anything is stored. A
    # genuine *revision* of stored values is separate, deeper work (delete +
    # observation FK re-derivation) — this guard only protects against dupes.
    if not dry_run:
        # Presence is scoped to the partner set being ingested — a new partner
        # (e.g. --partner US) into a period that already holds CN rows is NOT a
        # duplicate and must proceed.
        existing = db.eurostat_reporters_present_for_period(period, partners=partners)
        if reporters is None:
            if existing:
                log.info(
                    "Eurostat %s (partners=%s) already ingested (%d reporters "
                    "present) — skipping whole-period re-ingest to avoid "
                    "duplicate raw rows. Backfill a missing reporter with "
                    "--eurostat-reporter.",
                    period.strftime("%Y-%m"),
                    ",".join(sorted(partners)) if partners else "ANY",
                    len(existing))
                return IngestOutcome(status="noop")
        else:
            already = reporters & existing
            reporters = reporters - existing
            if already:
                log.info("Eurostat %s: skipping reporters already present: %s",
                         period.strftime("%Y-%m"), ",".join(sorted(already)))
            if not reporters:
                log.info("Eurostat %s: all requested reporters already present "
                         "— nothing to backfill", period.strftime("%Y-%m"))
                return IngestOutcome(status="noop")
    log.info("Fetching Eurostat bulk file for %s%s", period.strftime("%Y-%m"),
             f" (reporters={','.join(sorted(reporters))})" if reporters else "")
    run_id = db.start_run(url) if not dry_run else None
    try:
        response = eurostat.fetch_bulk_file(period)
        raw_rows = list(
            eurostat.iter_raw_rows(
                response.content, period, partners=partners,
                reporters=reporters, hs_prefixes=hs_prefixes
            )
        )
        log.info(
            "Fetched %d raw rows for %s (partners=%s, hs_prefixes=%s)",
            len(raw_rows), period.strftime("%Y-%m"),
            sorted(partners) if partners else "ANY",
            hs_prefixes or "ANY",
        )

        # A2 guard: the EU-27 analysers scope by EXCLUDING GB, trusting that the
        # only non-EU-27 declarant Eurostat ever ships is GB. Alert loudly if an
        # unknown reporter code appears, before it folds into EU-27 and inflates
        # a published number. Alert-only — we don't drop the rows (ingest broadly,
        # judge downstream); a human then adds a genuine new member to
        # eurostat.EU27_PARTNER_CODES or confirms the code stays out of scope.
        # Runs before the persist/dry-run branches so it surfaces in --dry-run too.
        surprise = eurostat.unexpected_reporters(r["reporter"] for r in raw_rows)
        if surprise:
            log.error(
                "Eurostat %s: unexpected reporter code(s) ingested: %s. The EU-27 "
                "analysers scope by excluding GB, so an unknown declarant (an "
                "EU/EU27_2020 aggregate row, a new member, a candidate country, or "
                "a special territory) is silently folded into EU-27 and would "
                "double-count or inflate it. Verify before trusting EU-27 figures "
                "for this period.",
                period.strftime("%Y-%m"), ", ".join(sorted(surprise)),
            )

        if dry_run:
            obs = list(eurostat.aggregate_to_observations(period, [(None, r) for r in raw_rows]))
            log.info("Dry run: would aggregate to %d observations", len(obs))
            return IngestOutcome(status="success", rows=len(raw_rows))

        if not raw_rows:
            # Published file but nothing matched our partner/HS filters. Don't
            # create an empty release — that would falsely advance max(period).
            log.info("Eurostat %s: 0 rows after filters — no release created",
                     period.strftime("%Y-%m"))
            db.finish_run(run_id, status="success", http_status=response.status_code)
            return IngestOutcome(status="empty")

        raw_ids = db.bulk_insert_eurostat_raw_rows(run_id, raw_rows)
        log.info("Inserted %d eurostat_raw_rows", len(raw_ids))
        observations = list(eurostat.aggregate_to_observations(period, list(zip(raw_ids, raw_rows))))
        log.info("Aggregated to %d observations", len(observations))
        release_id = db.find_or_create_eurostat_release(period, url)
        counts = db.upsert_observations(run_id, release_id, observations)
        log.info("Persisted: %s", counts)
        db.finish_run(run_id, status="success", http_status=response.status_code)
        return IngestOutcome(status="success", rows=len(raw_ids))
    except Exception as e:
        log.exception("Eurostat scrape failed for %s", period)
        if run_id is not None:
            db.finish_run(run_id, status="failed", error_message=str(e))
        return IngestOutcome(status="failed", error=str(e))


def _world_aggregate_hs_prefixes_from_hs_groups() -> tuple[str, ...]:
    """Read the active `hs_groups.hs_patterns` and convert them to the prefix
    set the bulk-file streamer can use, PLUS the all-goods `000TOTAL` aggregate.
    Each pattern ends with '%' (SQL LIKE convention); we strip the '%' to get a
    literal startswith prefix.

    The resulting tuple is passed to `iter_raw_rows(hs_prefixes=...)` which
    filters via `str.startswith` — so '2922%' becomes '2922' and '85044084%'
    becomes '85044084'. Eurostat product_nc is zero-padded to 8 chars, so a
    short prefix like '2922' matches every CN8 sub-code beneath HS chapter
    2922 as you'd expect.

    `000TOTAL` (the per-partner all-products aggregate row) is always included:
    its extra-EU sum is the denominator for the China all-goods-share metric
    (the dependency donut + trend, anomalies.detect_china_all_goods_share). It's
    cheap — one aggregate row per (reporter, flow) — and harmless to the
    per-group partner_share analyser, which filters on its own HS prefixes.
    """
    import psycopg2
    with psycopg2.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute("SELECT hs_patterns FROM hs_groups")
        rows = cur.fetchall()
    prefixes: set[str] = {"000TOTAL"}
    for (patterns,) in rows:
        for p in (patterns or []):
            if p.endswith("%"):
                prefixes.add(p[:-1])
            else:
                prefixes.add(p)
    return tuple(sorted(prefixes))


def scrape_eurostat_world_totals(
    period: date,
    hs_prefixes: tuple[str, ...] | None = None,
    dry_run: bool = False,
) -> None:
    """Fetch one Eurostat monthly bulk file and populate
    `eurostat_world_aggregates` with all-partner sums per
    (reporter, product_nc, flow). Used as the denominator for the
    partner_share analyser (anomalies.detect_partner_share).

    Separate orchestrator from `scrape_eurostat` because the two have
    different filters: the per-partner ingest stores rows for CN/HK/MO
    only (storage-bounded); this one aggregates across all 246 partner
    codes Eurostat publishes for a focused HS-prefix subset.

    `hs_prefixes`: required in production usage (an unfiltered pass over
    all CN8 codes would balloon the aggregates table and is rarely the
    editorial need). Caller should pass the patterns from the active
    hs_groups set — see `db.list_hs_group_patterns_for_aggregates`.
    """
    url = eurostat.bulk_file_url(period)
    log.info(
        "Fetching Eurostat bulk file for world-aggregates pass (period=%s, hs_prefixes=%s)",
        period.strftime("%Y-%m"), hs_prefixes or "ANY",
    )
    run_id = db.start_run(url) if not dry_run else None
    try:
        response = eurostat.fetch_bulk_file(period)
        agg_rows = list(
            eurostat.aggregate_to_world_totals(
                response.content, period, hs_prefixes=hs_prefixes,
            )
        )
        log.info(
            "Aggregated to %d world-total rows for %s",
            len(agg_rows), period.strftime("%Y-%m"),
        )
        if dry_run:
            log.info("Dry run: would upsert %d world-aggregate rows", len(agg_rows))
            return
        n = db.bulk_upsert_eurostat_world_aggregates(run_id, agg_rows)
        log.info("Upserted %d eurostat_world_aggregates rows", n)
        db.finish_run(run_id, status="success", http_status=response.status_code)
    except Exception as e:
        log.exception("Eurostat world-aggregates scrape failed for %s", period)
        if run_id is not None:
            db.finish_run(run_id, status="failed", error_message=str(e))


def scrape_hmrc(
    period: date,
    country_ids: tuple[int, ...] | None = None,
    dry_run: bool = False,
) -> IngestOutcome:
    """Fetch one HMRC OTS monthly slice (period × China-and-SARs by default),
    persist raw rows, aggregate, persist observations.

    Pre-requisite: the period's GBP/EUR FX rate must be in `fx_rates`.
    Run `python scrape.py --fetch-fx GBP --fx-since 2017-01` once to
    populate the full ECB history. Without it the conversion to EUR is
    skipped (value_eur left NULL on raw rows; observations would sum to 0).

    HMRC has no 404 / header trick for "not published yet": the OData query
    simply returns zero rows. An empty period therefore returns
    IngestOutcome("empty") WITHOUT creating a release row (which would falsely
    advance max(period) and break the always-probe candidate computation).
    """
    if country_ids is None:
        country_ids = hmrc.DEFAULT_COUNTRY_IDS

    fx = lookups.lookup_fx("GBP", "EUR", period)
    if fx is None:
        msg = (
            f"no GBP/EUR FX rate in fx_rates for {period.strftime('%Y-%m')}; "
            "run --fetch-fx GBP first"
        )
        log.error("HMRC scrape for %s skipped — %s", period.strftime("%Y-%m"), msg)
        return IngestOutcome(status="skipped", error=msg)
    fx_rate = fx.rate

    initial_url = hmrc.ots_query_url(period, country_ids, hmrc.DEFAULT_PAGE_SIZE)
    log.info("Fetching HMRC OTS for %s (country_ids=%s)", period.strftime("%Y-%m"), country_ids)
    run_id = db.start_run(initial_url) if not dry_run else None
    try:
        response = hmrc.fetch_ots_for_period(period, country_ids=country_ids)
        raw_rows = list(hmrc.iter_raw_rows(
            response.content, period, fx_rate_gbp_eur=fx_rate, country_ids=country_ids,
        ))
        log.info("Parsed %d HMRC raw rows for %s", len(raw_rows), period.strftime("%Y-%m"))

        if dry_run:
            obs = list(hmrc.aggregate_to_observations(period, [(None, r) for r in raw_rows]))
            log.info("Dry run: would aggregate to %d observations", len(obs))
            return IngestOutcome(status="success", rows=len(raw_rows))

        if not raw_rows:
            # OData returned nothing → period not published yet. Don't create
            # an empty release (it would falsely advance max(period)).
            log.info("HMRC %s: 0 rows — not published yet, no release created",
                     period.strftime("%Y-%m"))
            db.finish_run(run_id, status="success", http_status=response.status_code)
            return IngestOutcome(status="empty")

        # Idempotent re-ingest: atomically swap any prior raw rows for this
        # period for the fresh fetch — delete and insert share one
        # transaction, so a failed insert cannot leave the period cleared.
        # Observations are handled by upsert_observations' supersede chain.
        deleted, raw_ids = db.replace_hmrc_raw_rows_for_period(run_id, period, raw_rows)
        if deleted:
            log.info("Cleared %d stale hmrc_raw_rows for %s before re-insert",
                     deleted, period.strftime("%Y-%m"))
        log.info("Inserted %d hmrc_raw_rows", len(raw_ids))
        observations = list(hmrc.aggregate_to_observations(period, list(zip(raw_ids, raw_rows))))
        log.info("Aggregated to %d observations", len(observations))
        release_id = db.find_or_create_hmrc_release(period, initial_url)
        counts = db.upsert_observations(run_id, release_id, observations)
        log.info("Persisted: %s", counts)
        db.finish_run(run_id, status="success", http_status=response.status_code)
        return IngestOutcome(status="success", rows=len(raw_ids))
    except Exception as e:
        log.exception("HMRC scrape failed for %s", period)
        if run_id is not None:
            db.finish_run(run_id, status="failed", error_message=str(e))
        return IngestOutcome(status="failed", error=str(e))


def _latest_release_period(source: str) -> date | None:
    """The most recent releases.period for `source`, or None if it has none."""
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute("SELECT MAX(period) FROM releases WHERE source = %s", (source,))
        row = cur.fetchone()
    return row[0] if row else None


def _count_gacc_releases() -> int:
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM releases WHERE source = 'gacc'")
        return cur.fetchone()[0]


def _outcome_to_result(
    candidate: date, outcome: "IngestOutcome",
) -> tuple[str, str | None, str | None]:
    """Map an IngestOutcome to a (result, notes, error) routine_check_log triple."""
    if outcome.status == "success":
        return "new_data", f"ingested {outcome.rows} raw rows for {candidate:%Y-%m}", None
    if outcome.status == "absent":
        return "no_change", "not published yet", None
    if outcome.status == "empty":
        return "no_change", "response present but no rows for our filters", None
    if outcome.status == "noop":
        return "no_change", "already ingested", None
    # skipped (missing precondition) or failed (exception) — both are errors
    # worth surfacing rather than silently swallowing.
    return "error", None, outcome.error


def probe_source(source: str, today: date | None = None) -> str:
    """Always-probe one upstream source and record the outcome.

    For eurostat / hmrc: compute the next candidate period (max published + 1
    month), attempt to fetch it (cheap existence probe gates the download),
    classify the publication-calendar expectation, and write one
    routine_check_log row carrying both the objective result and the
    expectation. For gacc: walk the indexes and compare release counts, and
    classify the same expectation axis against GACC's formula-only calendar
    (candidate = next month after the latest preliminary release in the DB).

    Replaces the old SKILL.md prose that computed candidates via psql and
    guessed a 5-week eligibility gate. Returns a one-line human-readable
    outcome (also printed to stdout for the Routine to surface).
    """
    import time
    import routine_log

    today = today or date.today()
    started = time.monotonic()

    if source == "gacc":
        # GACC is an index walk, not a single-period fetch, so the walk always
        # runs (it's idempotent/cached and needs no candidate to discover new
        # releases). But it now carries the same expectation axis as the other
        # sources: candidate = the next reference month after the latest
        # preliminary release in the DB (all gacc SEED_INDEXES are 'preliminary',
        # so MAX(period) is that release's month). Classify it against GACC's
        # formula-only calendar (scheduled ≈ 8th of the following month, due-by
        # ~12th) so a slipped release reads `overdue` on --source-status instead
        # of a blank cell. Computed before the walk — mirroring the eurostat/hmrc
        # branch — so a release that finally lands after slipping logs the
        # informative new_data × overdue ("arrived, but late") for that one run,
        # then clears next run as the latest period advances. Empty DB → no
        # anchor → expectation stays None (the walk still runs).
        latest = _latest_release_period("gacc")
        candidate = release_calendar.next_period(latest) if latest else None
        expectation = (
            release_calendar.classify_expectation("gacc", candidate, today)
            if candidate else None
        )
        before = _count_gacc_releases()
        try:
            run_scrape(urls=None, dry_run=False, force_refetch=False)
            added = _count_gacc_releases() - before
            result = "new_data" if added > 0 else "no_change"
            notes = (f"fetched {added} new releases" if added > 0
                     else "walked indexes, no new releases")
            error = None
        except Exception as e:
            log.exception("GACC walk failed")
            result, notes, error = "error", None, str(e)
        duration_ms = int((time.monotonic() - started) * 1000)
        routine_log.log_check("gacc", result, expectation=expectation,
                              candidate_period=candidate, notes=notes,
                              error=error, duration_ms=duration_ms)
        line = (f"gacc: {result} × {expectation or '—'}"
                + (f" (candidate {candidate:%Y-%m})" if candidate else "")
                + (f" — {notes}" if notes else "")
                + (f" — {error}" if error else ""))
        print(line)
        return line

    if source not in ("eurostat", "hmrc"):
        raise ValueError(f"unknown probe source {source!r}")

    latest = _latest_release_period(source)
    if latest is None:
        routine_log.log_check(
            source, "no_change",
            notes="no prior releases in DB; cannot compute candidate period",
        )
        line = (f"{source}: no_change — no prior releases in DB to anchor the "
                "candidate period")
        print(line)
        return line

    candidate = release_calendar.next_period(latest)
    expectation = release_calendar.classify_expectation(source, candidate, today)

    if today < release_calendar.period_close(candidate):
        # Hard floor: the candidate's reference month hasn't ended, so data for
        # it cannot exist yet — skip the network probe (a guaranteed no-op).
        # This is NOT the retired 5-week heuristic: we still probe through the
        # [period_close → expected_publish] window, so early arrivals are
        # caught and the publication lag stays un-censored. We only skip probes
        # before the month has even closed, where there is nothing to find.
        result, notes, error = "no_change", "reference month not closed yet", None
    else:
        if source == "eurostat":
            outcome = scrape_eurostat(candidate, partners={"CN", "HK", "MO"})
        else:  # hmrc
            outcome = scrape_hmrc(candidate)
        result, notes, error = _outcome_to_result(candidate, outcome)
        # Completeness guard: a late/unreported member state would silently
        # understate any aggregate spanning the gap (the NL-March-2026 case).
        # Surface it loudly rather than letting it slip into the briefing.
        if source == "eurostat" and outcome.status == "success":
            # Check the full CN+HK+MO envelope the ingest stores, not just CN —
            # a missing HK/MO reporter-month understates a spanning aggregate
            # too. CN gaps are near-certainly missing data; HK/MO can be genuine
            # no-trade (thin flows), so they're advisory — hence partner-tagged.
            gaps = db.eurostat_coverage_gaps_multi(
                anomalies._months_back(candidate, 12), candidate,
                exclude_reporters=anomalies.EU27_EXCLUDE_REPORTERS)
            if gaps:
                glist = ", ".join(f"{p:%Y-%m}/{r}/{ptnr}" for p, r, ptnr in gaps)
                log.warning("Eurostat coverage gaps in trailing 12mo (%d): %s "
                            "— CN = near-certainly missing data; HK/MO may be "
                            "genuine no-trade. Backfill real gaps with "
                            "--eurostat-period P --eurostat-reporter R",
                            len(gaps), glist)

    duration_ms = int((time.monotonic() - started) * 1000)
    routine_log.log_check(
        source, result, expectation=expectation, candidate_period=candidate,
        notes=notes, error=error, duration_ms=duration_ms,
    )
    line = (
        f"{source}: {result} × {expectation or '—'} "
        f"(candidate {candidate:%Y-%m})"
        + (f" — {notes}" if notes else "")
        + (f" — {error}" if error else "")
    )
    print(line)
    return line


def _parse_period(s: str) -> date:
    """Accept YYYY-MM or YYYYMM; returns the first-of-month anchor date."""
    s = s.strip().replace("-", "")
    if len(s) != 6 or not s.isdigit():
        raise argparse.ArgumentTypeError(f"--eurostat-period must be YYYY-MM, got {s!r}")
    return date(int(s[:4]), int(s[4:]), 1)


def run_scrape(
    urls: list[str] | None = None, dry_run: bool = False,
    *, force_refetch: bool = False,
) -> None:
    if urls:
        for url in urls:
            if _is_index_url(url):
                scrape_index(
                    url, release_kind="preliminary", dry_run=dry_run,
                    force_refetch=force_refetch,
                )
            else:
                scrape_release(url, dry_run=dry_run, force_refetch=force_refetch)
        return
    for index_url, release_kind in SEED_INDEXES:
        scrape_index(
            index_url, release_kind=release_kind, dry_run=dry_run,
            force_refetch=force_refetch,
        )


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", help="Scrape a single GACC URL (index OR release)")
    p.add_argument("--eurostat-period", type=_parse_period, metavar="YYYY-MM",
                   help="Fetch one Eurostat monthly bulk file for the given period")
    p.add_argument("--eurostat-world-aggregates-period", type=_parse_period, metavar="YYYY-MM",
                   help="Fetch one Eurostat monthly bulk file and aggregate "
                        "across all 246 partner codes for the HS prefixes "
                        "tracked by active hs_groups (or pass --hs-prefix to "
                        "override). Populates eurostat_world_aggregates — the "
                        "denominator for the partner_share analyser.")
    p.add_argument("--hmrc-period", type=_parse_period, metavar="YYYY-MM",
                   help="Fetch one HMRC OTS slice for the given period (UK trade with "
                        "non-EU partners CN+HK+MO by default; pre-requires GBP/EUR FX "
                        "loaded via --fetch-fx GBP)")
    p.add_argument("--partner", action="append", metavar="CC",
                   help="ISO-2 partner country code(s) to filter Eurostat to. "
                        "Default: CN. Repeat for multiple, e.g. --partner CN --partner US")
    p.add_argument("--eurostat-reporter", action="append", metavar="CC",
                   help="Restrict an Eurostat ingest to these member-state "
                        "reporters (ISO-2). Use to surgically backfill a missing "
                        "(period, reporter); reporters already stored are skipped "
                        "so the additive re-ingest can't duplicate. Repeat for "
                        "multiple.")
    p.add_argument("--eurostat-coverage", nargs=2, metavar=("START", "END"),
                   type=_parse_period,
                   help="Report member-state months missing from the Eurostat "
                        "000TOTAL set across [START, END] (YYYY-MM each) and exit. "
                        "A coverage gap = a member state present in some months "
                        "but absent in another (silently understating aggregates).")
    p.add_argument("--hs-prefix", action="append", metavar="HS",
                   help="HS-CN8 prefix(es) to filter Eurostat to (e.g. 87038). "
                        "Default: no HS filter. Repeat for multiple.")
    p.add_argument("--fetch-fx", metavar="CCY", action="append",
                   help="Fetch ECB monthly average rates for CCY/EUR and populate fx_rates. "
                        "Repeatable, e.g. --fetch-fx CNY --fetch-fx USD")
    p.add_argument("--fx-since", type=_parse_period, metavar="YYYY-MM",
                   help="Only fetch FX rates from this period onwards (default: full history)")
    p.add_argument("--analyse",
                   choices=["mirror-trade", "mirror-gap-trends", "hs-group-yoy",
                            "hs-group-trajectory", "cn8-biggest-mover",
                            "gacc-aggregate-yoy",
                            "gacc-bilateral-aggregate-yoy", "partner-share",
                            "trade-balance", "china-all-goods-share", "llm-framing"],
                   help="Run a deterministic anomaly pass over already-ingested data, "
                        "or 'llm-framing' to generate per-hs-group lead scaffolds "
                        "(anomaly summary + 2-3 picked hypotheses + corroboration steps; "
                        "consumes existing deterministic findings)")
    p.add_argument("--llm-model", metavar="NAME", default=None,
                   help=f"Ollama model name for --analyse llm-framing. Default: "
                        f"{llm_framing.DEFAULT_OLLAMA_MODEL}")
    p.add_argument("--comparison-scope",
                   choices=anomalies.VALID_COMPARISON_SCOPES,
                   default=anomalies.COMPARISON_SCOPE_DEFAULT,
                   help=f"Which reporter side(s) to sum on the China-trade comparison. "
                        f"'eu_27' (default): EU-27 from Eurostat (excludes UK at all times). "
                        f"'uk': UK-only from HMRC (Phase 6.1). "
                        f"'eu_27_plus_uk': EU-27 + UK summed (carries cross_source_sum caveat). "
                        f"Applies to hs-group-yoy and hs-group-trajectory. "
                        f"Pre-requires HMRC ingest for 'uk' and 'eu_27_plus_uk' — see README.")
    p.add_argument("--export-sheet", action="store_true",
                   help="Export findings to a spreadsheet (default: local .xlsx)")
    p.add_argument("--out-format", choices=["xlsx", "sheets"], default="xlsx",
                   help="Spreadsheet output format (default: xlsx)")
    p.add_argument("--out-path", metavar="PATH",
                   help="Output file path for xlsx export (default: ./exports/findings-{timestamp}.xlsx)")
    p.add_argument("--spreadsheet-id", metavar="ID",
                   help="Google Sheets spreadsheet ID (for --out-format sheets)")
    p.add_argument("--briefing-pack", action="store_true",
                   help="Export findings as a paired briefing pack + leads "
                        "doc (NotebookLM-ready). Default output: "
                        "./exports/YYYY-MM-DD-HHMM[-slug]/{findings.md, leads.md}")
    p.add_argument("--export-dir", metavar="PATH",
                   help="Output folder for the briefing pack + leads "
                        "(default: ./exports/YYYY-MM-DD-HHMM[-slug]/)")
    p.add_argument("--export-scope", metavar="LABEL",
                   help="Optional human-readable scope label (e.g. "
                        "'EV batteries (Li-ion)' or 'UK only'). Slugified "
                        "into the folder suffix and surfaced in the "
                        "headers of both the brief and the leads doc.")
    p.add_argument("--briefing-top-n", type=int, default=briefing_pack.DEFAULT_TOP_N, metavar="N",
                   help=f"Top N hs_group_yoy movers per flow direction "
                        f"(default: {briefing_pack.DEFAULT_TOP_N})")
    p.add_argument("--hs-group", action="append", metavar="NAME",
                   help="Restrict --analyse hs-group-yoy to specific group name(s); repeat for multiple")
    p.add_argument("--yoy-threshold", type=float, default=0.0, metavar="PCT",
                   help="Minimum |YoY %% as fraction| to emit hs-group-yoy findings (default 0.0 = always)")
    p.add_argument("--flow", type=int, choices=[1, 2], default=1, metavar="N",
                   help="Eurostat flow direction for hs-group-yoy: 1=EU imports from CN (default), 2=EU exports to CN")
    p.add_argument("--low-base-threshold", type=float, metavar="EUR",
                   default=anomalies.LOW_BASE_THRESHOLD_EUR,
                   help=(f"Per-12mo-window EUR threshold below which an hs-group-yoy or "
                         f"hs-group-trajectory window is flagged low-base. Default: "
                         f"€{anomalies.LOW_BASE_THRESHOLD_EUR:,.0f}. Lower for niche-commodity "
                         f"investigations; raise for macro-only analyses."))
    p.add_argument("--smooth-window", type=int, default=None, metavar="N",
                   help=f"Centered moving-average window for trajectory shape detection. "
                        f"Default {anomalies.TRAJECTORY_SMOOTH_WINDOW} (the historic behaviour). "
                        f"Pass 1 to disable smoothing — useful for analyses focused on short-"
                        f"term policy effects (tariff pre-loading spikes that 3-window "
                        f"smoothing would absorb).")
    p.add_argument("--analyse-period", type=_parse_period, metavar="YYYY-MM",
                   help="Restrict --analyse to a single period (default: all)")
    p.add_argument("--trend-window", type=int, default=6, metavar="N",
                   help="Rolling baseline window in months for trend analyses (default: 6)")
    p.add_argument("--z-threshold", type=float, default=1.5, metavar="Z",
                   help="Minimum |z| to emit a trend finding (default: 1.5)")
    p.add_argument("--dry-run", action="store_true", help="Fetch + parse but don't write to DB")
    p.add_argument(
        "--force-refetch", action="store_true",
        help=(
            "GACC walker: bypass the dedup guard and re-fetch every release "
            "URL the index pages enumerate, even ones already terminally "
            "processed (success / no_parser). Use after shipping a new HTML "
            "parser to convert prior 'no_parser' URLs into 'success'."
        ),
    )
    p.add_argument(
        "--periodic-run", action="store_true",
        help=(
            "Run the full periodic-cycle pipeline: re-run all analysers "
            "across scope/flow combos, re-run llm-framing, and write a new "
            "findings export bundle (trigger='periodic_run'). Idempotent: "
            "exits cleanly with a no-op message if the latest Eurostat "
            "period in the DB has already been published by an earlier "
            "periodic-run cycle. Pass --force to override."
        ),
    )
    p.add_argument(
        "--force", action="store_true",
        help="With --periodic-run: skip the idempotency check and re-run "
             "regardless of whether the current Eurostat period has "
             "already been published.",
    )
    p.add_argument(
        "--skip-llm", action="store_true",
        help="With --periodic-run: skip the llm-framing step. Useful "
             "when Ollama is unavailable or for fast iterations.",
    )
    p.add_argument(
        "--portal-takes", action="store_true",
        help="With --periodic-run / --portal-snapshot: generate the LLM "
             "per-finding 'takes' for the portal snapshot. Off by default — "
             "needs an LLM backend (LLM_BACKEND=claude_api unattended, or the "
             "claude CLI in an attended dev run) and costs API spend. This is "
             "the right choice whenever the underlying content has changed.",
    )
    p.add_argument(
        "--portal-reuse-takes", action="store_true",
        help="With --portal-snapshot: carry the PREVIOUS LLM takes forward "
             "instead of regenerating them — no LLM spend. For amending an "
             "existing release (cosmetic/layout fixes, low-impact data "
             "corrections) where the prior takes still hold. Reads the live "
             "snapshot from the portal bucket (needs --portal-bucket / "
             "PORTAL_BUCKET) and only carries a take over when the data_period "
             "is unchanged AND its finding still matches; anything else is left "
             "blank. Mutually exclusive with --portal-takes.",
    )
    p.add_argument(
        "--no-record", action="store_true",
        help=(
            "With --briefing-pack: produce the export bundle without "
            "inserting a brief_runs row. The export is 'unsequenced' — it "
            "does not advance any cycle and does not become the baseline "
            "for the next export's Tier 1 'what's new' section. Use for "
            "test, preview, or on-demand renders. Has no effect on "
            "--periodic-run (which always records)."
        ),
    )
    p.add_argument(
        "--upload-to-drive", metavar="BUNDLE_DIR",
        help=(
            "Upload an already-generated local bundle directory to Google "
            "Drive: top-level .docx convert to native Google Docs and "
            "04_Data.xlsx to a Sheet, heading navigation anchors are minted, "
            "in-document links repaired, and the markdown subfolder mirrored. "
            "Expects a docx=True bundle (run `--briefing-pack --docx` or "
            "`--periodic-run` first). Run by hand — opens a browser to "
            "re-authorise if the saved OAuth token has lapsed. Target parent "
            "folder via the MERIDIAN_DRIVE_PARENT_ID env var."
        ),
    )
    p.add_argument(
        "--upload-to-portal", metavar="BUNDLE_DIR",
        help=(
            "Upload a bundle's 04_Portal snapshot (report.json + index.html) to "
            "the portal's GCS bucket: gs://<bucket>/latest/ (what the Cloud Run "
            "service serves) plus a per-period archive. Bucket from "
            "--portal-bucket or the PORTAL_BUCKET env var. Run by hand after "
            "`--periodic-run`; needs gcloud Application Default Credentials."
        ),
    )
    p.add_argument(
        "--portal-bucket", metavar="NAME", default=None,
        help="GCS bucket for --upload-to-portal (default: PORTAL_BUCKET env).",
    )
    p.add_argument(
        "--portal-warm", action="store_true",
        help="With --upload-to-portal: warm the Cloud Run service "
             "(--min-instances=1) so the freshly published report has no "
             "cold-start delay. Service/region from PORTAL_SERVICE "
             "(default meridian-portal) / PORTAL_REGION. Flip back to 0 by hand.",
    )
    p.add_argument(
        "--portal-no-publish", action="store_true",
        help="With --portal-snapshot: build the snapshot locally but DON'T "
             "publish it, even when a bucket is configured — a preview. The "
             "bucket is still read (so --portal-reuse-takes can graft the prior "
             "takes) and the grafted/generated takes are baked into the local "
             "index.html, so you can open it to check the result before going "
             "live. Publish the previewed bundle as-is with `--upload-to-portal "
             "<DIR>` (same bytes — no rebuild, no extra LLM spend).",
    )
    p.add_argument(
        "--portal-snapshot", nargs="?", const="exports/portal-snapshot",
        default=None, metavar="DIR",
        help=(
            "Build a standalone portal snapshot (report.json + index.html) "
            "into DIR/04_Portal/ WITHOUT recording a brief_runs row — an "
            "on-demand render that does NOT advance the subscriber cycle or "
            "move the 'since last brief' baseline (unlike --periodic-run, "
            "which always records). DIR defaults to exports/portal-snapshot. "
            "Honours --portal-takes (pay for fresh takes) or "
            "--portal-reuse-takes (carry the prior takes forward, free). If "
            "--portal-bucket (or PORTAL_BUCKET) is set it also publishes to GCS "
            "(and --portal-warm warms the service); otherwise — or with "
            "--portal-no-publish — it writes locally and prints the path for a "
            "later --upload-to-portal."
        ),
    )
    p.add_argument(
        "--with-provenance", action="store_true",
        help=(
            "With --briefing-pack: also bundle per-finding provenance files "
            "into the export folder's `provenance/` subdir. Only the "
            "editorially-fresh subset (Tier 1 changes, top-N movers, top-N "
            "leads) is bundled — typically ~40-60 files — to keep the "
            "export browsable. The long tail of state-of-play findings "
            "stays on-demand via --finding-provenance. Default off."
        ),
    )
    p.add_argument(
        "--docx", action="store_true",
        help=(
            "With --briefing-pack: also write a parallel `02_Findings.docx` "
            "to the export folder — the Lisa-facing surface that carries "
            "charts on top of the editorial top-N movers. The .md remains "
            "canonical (NotebookLM feed); the .docx is additive. Default off "
            "pending Lisa's review of the first cycles."
        ),
    )
    p.add_argument(
        "--groups-glossary", action="store_true",
        help=(
            "Write the HS group reference (`05_Groups.md` in a normal "
            "bundle) as a standalone, dated file at "
            "`exports/groups-glossary-YYYY-MM-DD.md`. Use `--out PATH` "
            "to override. Convenient for forwarding the glossary by "
            "itself between briefing-pack runs without regenerating "
            "the full bundle."
        ),
    )
    p.add_argument(
        "--out", metavar="PATH",
        help=(
            "With --groups-glossary: override the default output path. "
            "Ignored by other commands."
        ),
    )
    p.add_argument(
        "--finding-provenance", type=int, metavar="ID",
        help=(
            "Generate a per-finding provenance file at "
            "`provenance/finding-{ID}.md` — source URLs, methodology, "
            "caveats in plain English, cross-source check. Idempotent: "
            "skips if the file already exists. Use --force to regenerate. "
            "Currently the detailed template covers GACC bilateral "
            "aggregate findings; other subkinds get a stub."
        ),
    )
    p.add_argument(
        "--log-check", metavar="SOURCE",
        help=(
            "Write one row to routine_check_log. SOURCE is typically "
            "'eurostat', 'hmrc', or 'gacc'. Pair with --log-result; "
            "optional --log-period / --log-notes / --log-error / "
            "--log-duration-ms. Used by the daily Routine prompt to "
            "record each per-source check (debug-only telemetry; no "
            "journalist-facing surface reads from this table)."
        ),
    )
    p.add_argument(
        "--log-result",
        choices=[
            "new_data", "no_change", "error",
            "started", "completed",
        ],
        help=(
            "With --log-check: outcome of the check. The 'started' / "
            "'completed' values are reserved for the whole-Routine "
            "lifecycle bookends — paired with --log-check _routine. "
            "(The old 'not_yet_eligible' was retired 2026-06-02 — we always "
            "probe now; see --probe-source.)"
        ),
    )
    p.add_argument(
        "--log-expectation",
        choices=["none_expected", "due", "overdue"],
        help=(
            "With --log-check: the publication-calendar expectation for the "
            "candidate period (none_expected / due / overdue). Normally set "
            "automatically by --probe-source; this is for manual / test use. "
            "Omit for the _routine bookends and any check with no candidate "
            "period (e.g. an empty-DB gacc walk)."
        ),
    )
    p.add_argument(
        "--log-period", type=_parse_period, metavar="YYYY-MM",
        help=(
            "With --log-check: the candidate reference month the Routine "
            "checked (for gacc, the next month after the latest preliminary "
            "release). Omit only when there is no candidate (e.g. an empty-DB "
            "gacc walk, or the _routine bookends)."
        ),
    )
    p.add_argument(
        "--log-notes", metavar="TEXT",
        help=(
            "With --log-check: short human-readable note, e.g. "
            "'walked 9 indexes, no new releases'."
        ),
    )
    p.add_argument(
        "--log-error", metavar="TEXT",
        help="With --log-check --log-result error: the error message.",
    )
    p.add_argument(
        "--log-duration-ms", type=int, metavar="N",
        help="With --log-check: wall-clock duration of the check, milliseconds.",
    )
    p.add_argument(
        "--probe-source", metavar="SOURCE",
        choices=["eurostat", "hmrc", "gacc"],
        help=(
            "Always-probe one source and record the outcome to "
            "routine_check_log in one step: compute the next candidate period, "
            "fetch it if available (a cheap header probe gates the Eurostat "
            "download), ingest, classify the publication-calendar expectation "
            "(none_expected / due / overdue), and log result + expectation. "
            "The daily Routine calls this once per source — it replaces the "
            "old psql-candidate + 5-week-gate prose. Prints a one-line outcome."
        ),
    )
    p.add_argument(
        "--source-status", action="store_true",
        help=(
            "Print a rolled-up debug view of routine_check_log: per "
            "expected source (eurostat / hmrc / gacc), when last checked, "
            "the latest result × expectation, when new data last arrived, "
            "what's the latest period in the DB, and a flag for anything "
            "overdue. No writes."
        ),
    )
    p.add_argument(
        "--notify-chat", action="store_true",
        help=(
            "Post a Google Chat (Spaces) message iff a source (Eurostat / "
            "HMRC / GACC) ingested new data since the last successful post. "
            "Reads routine_check_log for the trigger (so GACC/HMRC arrivals "
            "between Eurostat releases still notify) and the webhook URL from "
            "MERIDIAN_CHAT_WEBHOOK (unset → harmless no-op). The daily Routine "
            "calls this after the probes + --periodic-run. Idempotent: a "
            "second call in the same fire finds nothing new. Combine with "
            "--dry-run to preview the message without posting."
        ),
    )
    p.add_argument(
        "--llm-rejections", action="store_true",
        help=(
            "Print recent rows from llm_rejection_log — LLM-framing "
            "outputs the verifier rejected (parse failure or numeric "
            "verification failure). Each row shows cluster name, stage, "
            "reason, detail, and a preview of the rejected prose. Use "
            "--limit N to control how many rows to show (default 20). "
            "No writes."
        ),
    )
    p.add_argument(
        "--periodic-history", action="store_true",
        help=(
            "Print recent rows from periodic_run_log — one row per "
            "`--periodic-run` invocation, including no-ops. Pairs with "
            "brief_runs (which only has rows for cycles that wrote an "
            "export). Use --limit N. No writes."
        ),
    )
    p.add_argument(
        "--emit-history", action="store_true",
        help=(
            "Print recent rows from findings_emit_log — one row per "
            "`detect_X()` analyser invocation, with the emit counts "
            "(new / confirmed / superseded) the analyser produced. Use "
            "--limit N. No writes."
        ),
    )
    p.add_argument(
        "--limit", type=int, default=20, metavar="N",
        help=(
            "With --llm-rejections / --periodic-history / --emit-history: "
            "how many rows to show. Default 20."
        ),
    )
    args = p.parse_args()

    if args.probe_source:
        probe_source(args.probe_source)
        return

    if args.log_check:
        if not args.log_result:
            p.error("--log-check requires --log-result")
        import routine_log
        rid = routine_log.log_check(
            args.log_check,
            args.log_result,
            expectation=args.log_expectation,
            candidate_period=args.log_period,
            notes=args.log_notes,
            error=args.log_error,
            duration_ms=args.log_duration_ms,
        )
        log.info(
            "routine_check_log id=%d (source=%s result=%s expectation=%s)",
            rid, args.log_check, args.log_result, args.log_expectation,
        )
        return

    if args.source_status:
        import routine_log
        statuses = routine_log.compute_status()
        lifecycle = routine_log.compute_lifecycle()
        print(routine_log.render_status_table(statuses, lifecycle), end="")
        return

    if args.notify_chat:
        import notify
        result = notify.notify_new_data(dry_run=args.dry_run)
        print(result.summary())
        if result.message and (args.dry_run or not result.posted):
            print("---")
            print(result.message)
        return

    if args.llm_rejections:
        import llm_rejection_log
        rows = llm_rejection_log.recent_rejections(limit=args.limit)
        print(llm_rejection_log.render_rejections(rows), end="")
        return

    if args.periodic_history:
        import periodic_run_log
        rows = periodic_run_log.recent_cycles(limit=args.limit)
        print(periodic_run_log.render_cycles(rows), end="")
        return

    if args.emit_history:
        import findings_emit_log
        rows = findings_emit_log.recent_runs(limit=args.limit)
        print(findings_emit_log.render_runs(rows), end="")
        return

    if args.finding_provenance is not None:
        import provenance
        path = provenance.generate_for_finding(
            args.finding_provenance, force=args.force,
        )
        print(path)
        return

    if args.groups_glossary:
        from datetime import date as _date
        from pathlib import Path as _Path
        import briefing_pack.render_groups as _rg
        out = _Path(args.out) if args.out else (
            _Path(f"exports/groups-glossary-{_date.today().isoformat()}.md")
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(_rg.render_groups())
        log.info("Wrote groups glossary to %s (%d bytes)", out, out.stat().st_size)
        print(out)
        return

    if args.portal_snapshot is not None:
        # Standalone, on-demand portal snapshot. Reuses the exact read-only
        # path periodic-run uses for 04_Portal/, but records NO brief_runs row
        # — so refreshing the live portal never advances the subscriber cycle
        # or moves the 'since last brief' baseline. Publishes too when a bucket
        # is configured (the common "refresh the portal now" case).
        # NB: briefing_pack and periodic are module-level imports — do NOT
        # re-import locally here, or Python makes them function-local and the
        # earlier briefing_pack.DEFAULT_TOP_N at parser-build time breaks.
        if args.portal_takes and args.portal_reuse_takes:
            print("--portal-takes and --portal-reuse-takes are mutually "
                  "exclusive: the first regenerates the takes (pays for fresh "
                  "LLM output — use it when the content changed); the second "
                  "carries the prior takes forward (free — for amending a "
                  "release). Pick one.")
            return
        out_dir = args.portal_snapshot
        period = briefing_pack.latest_eurostat_period()
        if period is None:
            print("No Eurostat data ingested yet — nothing to snapshot "
                  "(ingest a period first).")
            return
        # Resolve the bucket up front: --portal-reuse-takes reads the live
        # snapshot from it at BUILD time (the served index.html is pre-rendered,
        # so prior takes must be grafted before render — not at publish).
        bucket = args.portal_bucket or os.environ.get("PORTAL_BUCKET")
        if args.portal_reuse_takes and not bucket:
            print("--portal-reuse-takes needs a portal bucket to read the prior "
                  "takes from — pass --portal-bucket or set PORTAL_BUCKET.")
            return
        # Will this snapshot actually go live? If so, reuse-takes reads the prior
        # snapshot strictly — a read error refuses the publish rather than
        # shipping takes-less while reporting success.
        publishing = bool(bucket) and not args.portal_no_publish
        import portal_publish
        try:
            portal_dir = periodic.write_portal_snapshot(
                out_dir, period, generate_takes=args.portal_takes,
                write_workbook=True,  # so the Tables-tab /data.xlsx download resolves (no briefing-pack run on this path)
                reuse_takes=args.portal_reuse_takes, portal_bucket=bucket,
                publishing=publishing,
            )
        except portal_publish.PriorSnapshotUnreadable as e:
            print(f"Refusing to publish: --portal-reuse-takes could not read the "
                  f"prior snapshot to carry takes forward.\n  {e}\n"
                  f"  Retry with the project + --portal-bucket/PORTAL_BUCKET "
                  f"reachable, or drop --portal-reuse-takes to rebuild with "
                  f"fresh/empty takes (use --portal-no-publish to preview first).")
            return
        if portal_dir is None:
            print("Portal snapshot failed — see logs.")
            return
        print(f"Portal snapshot written to {portal_dir} "
              f"(data_period {period}; no brief_runs row — cycle unaffected).")
        if bucket and not args.portal_no_publish:
            import portal_publish
            written = portal_publish.publish_snapshot(out_dir, bucket=bucket)
            print("Published:")
            for dest in written:
                print(f"  {dest}")
            if args.portal_warm:
                service = os.environ.get("PORTAL_SERVICE", "meridian-portal")
                region = os.environ.get("PORTAL_REGION")
                if region:
                    ok = portal_publish.warm_service(service, region)
                    print(f"  warmed {service} (min-instances=1): "
                          f"{'ok' if ok else 'failed — see logs'}")
                else:
                    print("  --portal-warm skipped: set PORTAL_REGION")
        else:
            if args.portal_no_publish and bucket:
                # Preview: built (with prior takes grafted in, if --portal-reuse-
                # takes) but deliberately not published. The local index.html is
                # the exact artefact --upload-to-portal would publish.
                print(f"  --portal-no-publish: built locally, NOT published.")
                print(f"  Preview:  open {out_dir}/04_Portal/index.html")
            pub_bucket = bucket or "<NAME>"
            print(f"  To publish: python scrape.py --upload-to-portal "
                  f"{out_dir} --portal-bucket {pub_bucket}")
        return

    if args.upload_to_portal:
        import portal_publish
        written = portal_publish.publish_snapshot(
            args.upload_to_portal, bucket=args.portal_bucket,
        )
        print("Portal snapshot published:")
        for dest in written:
            print(f"  {dest}")
        if args.portal_warm:
            service = os.environ.get("PORTAL_SERVICE", "meridian-portal")
            region = os.environ.get("PORTAL_REGION")
            if region:
                ok = portal_publish.warm_service(service, region)
                print(f"  warmed {service} (min-instances=1): "
                      f"{'ok' if ok else 'failed — see logs'}")
            else:
                print("  --portal-warm skipped: set PORTAL_REGION")
        return

    if args.upload_to_drive:
        import briefing_pack.drive_export as drive_export
        res = drive_export.export_bundle_to_drive(args.upload_to_drive)
        print(f"\nDrive folder: {res['folder_name']} ({res['folder_id']})")
        for name, d in res["docs"].items():
            extra = (
                f"  ({d['anchors_minted']} anchors, "
                f"{d.get('links_fixed', 0)} links fixed)"
                if "anchors_minted" in d else "  (Sheet)"
            )
            print(f"  {name}: {d['link']}{extra}")
        print(f"  markdown subfolder: {len(res['raw'])} raw files")
        return

    if args.periodic_run:
        result = periodic.run_periodic(
            force=args.force,
            out_dir=args.export_dir,
            top_n=args.briefing_top_n,
            llm_model=args.llm_model,
            skip_llm=args.skip_llm,
            generate_takes=args.portal_takes,
        )
        log.info("periodic-run: %s", result.reason)
        # Human-readable per-run report for the scheduling layer (the
        # Routine agent) to surface — and, when a briefing was generated,
        # the exact manual `--upload-to-drive` command (we don't auto-publish).
        print(result.summary())
        # Then the findings path on its own final line, so a brittle wrapper
        # can still capture it (empty string on a no-op).
        print(result.findings_path or "")
        return

    if args.analyse == "mirror-trade":
        counts = anomalies.detect_mirror_trade_gaps(
            period=args.analyse_period,
        )
        log.info("Mirror-trade analysis: %s", counts)
        return

    if args.analyse == "mirror-gap-trends":
        counts = anomalies.detect_mirror_gap_trends(
            window_months=args.trend_window,
            z_threshold=args.z_threshold,
            period=args.analyse_period,
        )
        log.info("Mirror-gap trend analysis: %s", counts)
        return

    if args.analyse == "hs-group-yoy":
        counts = anomalies.detect_hs_group_yoy(
            group_names=args.hs_group,
            yoy_threshold_pct=args.yoy_threshold,
            flow=args.flow,
            low_base_threshold_eur=args.low_base_threshold,
            comparison_scope=args.comparison_scope,
        )
        log.info("HS-group YoY analysis (flow=%d, scope=%s): %s",
                 args.flow, args.comparison_scope, counts)
        return

    if args.analyse == "hs-group-trajectory":
        # Reads from active hs_group_yoy findings matching the scope. Re-run
        # hs-group-yoy with the matching --comparison-scope first if needed.
        counts = anomalies.detect_hs_group_trajectories(
            group_names=args.hs_group, flow=args.flow,
            low_base_threshold_eur=args.low_base_threshold,
            smooth_window=args.smooth_window,
            comparison_scope=args.comparison_scope,
        )
        log.info("HS-group trajectory analysis (flow=%d): %s", args.flow, counts)
        return

    if args.analyse == "cn8-biggest-mover":
        # Biggest single-product (CN8) mover within the watched HS prefixes —
        # finer than hs-group-yoy (roadmap "Biggest mover KPI", Option A).
        # Imports only (flow=1). Reads eurostat_raw_rows.
        counts = anomalies.detect_cn8_biggest_mover()
        log.info("CN8 biggest-mover analysis: %s", counts)
        return

    if args.analyse == "gacc-aggregate-yoy":
        # GACC-only YoY for non-EU partner aggregates (ASEAN, RCEP, Belt&Road,
        # Africa, Latin America, world Total). No mirror-comparison; the
        # editorial story is "is China-bloc trade growing or shrinking" rather
        # than "do the two sides agree".
        flow_str = "export" if args.flow == 1 else "import"
        counts = anomalies.detect_gacc_aggregate_yoy(
            flow=flow_str, yoy_threshold_pct=args.yoy_threshold,
        )
        log.info("GACC-aggregate YoY analysis (flow=%s): %s", flow_str, counts)
        return

    if args.analyse == "gacc-bilateral-aggregate-yoy":
        # Bilateral counterpart to gacc-aggregate-yoy: EU bloc + single-country
        # GACC partners. Each finding carries three YoY operators side-by-side
        # (12mo rolling, YTD cumulative, single-month) so the brief can quote
        # whichever cadence matches the story being written. Surfaces the
        # Soapbox A1 lead claim ($201bn EU exports Jan-Apr 2026, +19% YoY).
        flow_str = "export" if args.flow == 1 else "import"
        counts = anomalies.detect_gacc_bilateral_aggregate_yoy(
            flow=flow_str, yoy_threshold_pct=args.yoy_threshold,
        )
        log.info("GACC bilateral-aggregate YoY analysis (flow=%s): %s", flow_str, counts)
        return

    if args.analyse == "partner-share":
        # China's share of EU-27 imports/exports per HS group, by value
        # AND by quantity_kg. Numerator: eurostat_raw_rows (CN+HK+MO).
        # Denominator: eurostat_world_aggregates (all 246 partners) —
        # pre-populate via `--eurostat-world-aggregates-period YYYY-MM`
        # before running this analyser. Surfaces the Soapbox A1
        # "China supplied X% of EU imports of Y, bigger in tonnes than
        # in euros" register.
        counts = anomalies.detect_partner_share(
            group_names=args.hs_group, flow=args.flow,
        )
        log.info("partner-share analysis (flow=%d): %s", args.flow, counts)
        return

    if args.analyse == "trade-balance":
        # EU–China all-goods trade deficit (imports minus exports), framed
        # per day. No flow axis — emits both partner scopes (CN+HK+MO and
        # CN-only) in one pass. Eurostat-only; reads the 000TOTAL aggregate
        # rows. Surfaces the "€1bn a day" register the press quotes.
        counts = anomalies.detect_eu_china_trade_balance()
        log.info("EU–China trade-balance analysis: %s", counts)
        return

    if args.analyse == "china-all-goods-share":
        # China's share of EU-27 extra-EU all-goods trade — the dependency
        # donut + trend line. Numerator: eurostat_raw_rows 000TOTAL (CN+HK+MO);
        # denominator: eurostat_world_aggregates 000TOTAL (extra-EU) — populate
        # via `--eurostat-world-aggregates-period YYYY-MM` (default prefixes now
        # include 000TOTAL) before running this. From 2019-01 (pre-v2 numerator
        # dupes make 2017-18 unreliable).
        counts = anomalies.detect_china_all_goods_share()
        log.info("China all-goods share analysis: %s", counts)
        return

    if args.analyse == "llm-framing":
        counts = llm_framing.detect_llm_framings(
            group_names=args.hs_group,
            model=args.llm_model,
        )
        log.info("LLM framing run: %s", counts)
        return

    if args.export_sheet:
        out = sheets_export.export(
            out_format=args.out_format,
            out_path=args.out_path,
            spreadsheet_id=args.spreadsheet_id,
        )
        log.info("Exported to %s", out)
        return

    if args.briefing_pack:
        brief_path, leads_path = briefing_pack.export(
            out_dir=args.export_dir,
            scope_label=args.export_scope,
            top_n=args.briefing_top_n,
            record=not args.no_record,
            with_provenance=args.with_provenance,
            docx=args.docx,
        )
        log.info("Wrote briefing pack to %s", brief_path)
        log.info("Wrote investigation leads to %s", leads_path)
        return

    if args.fetch_fx:
        for ccy in args.fetch_fx:
            counts = fx.populate_fx_rates_from_ecb(ccy.upper(), since=args.fx_since)
            log.info("FX %s/EUR: %s", ccy.upper(), counts)
        return

    if args.eurostat_coverage:
        start, end = args.eurostat_coverage
        partner = (args.partner[0] if args.partner else "CN")
        gaps = db.eurostat_coverage_gaps(
            start, end, partner=partner,
            exclude_reporters=anomalies.EU27_EXCLUDE_REPORTERS)
        if not gaps:
            print(f"No Eurostat 000TOTAL coverage gaps in "
                  f"{start:%Y-%m}..{end:%Y-%m}")
        else:
            print(f"Eurostat 000TOTAL coverage gaps ({len(gaps)}):")
            for period, rep in gaps:
                print(f"  {period:%Y-%m}  {rep}")
        return

    if args.eurostat_period:
        partners = set(args.partner) if args.partner else {"CN"}
        reporters = set(args.eurostat_reporter) if args.eurostat_reporter else None
        hs_prefixes = tuple(args.hs_prefix) if args.hs_prefix else None
        scrape_eurostat(args.eurostat_period, partners=partners,
                        hs_prefixes=hs_prefixes, reporters=reporters,
                        dry_run=args.dry_run)
        return

    if args.eurostat_world_aggregates_period:
        # If the user didn't pass --hs-prefix, derive from active hs_groups
        # so we only aggregate what the analysers care about.
        hs_prefixes = (
            tuple(args.hs_prefix)
            if args.hs_prefix
            else _world_aggregate_hs_prefixes_from_hs_groups()
        )
        scrape_eurostat_world_totals(
            args.eurostat_world_aggregates_period,
            hs_prefixes=hs_prefixes,
            dry_run=args.dry_run,
        )
        return

    if args.hmrc_period:
        scrape_hmrc(args.hmrc_period, dry_run=args.dry_run)
        return

    run_scrape(
        urls=[args.url] if args.url else None,
        dry_run=args.dry_run,
        force_refetch=args.force_refetch,
    )


if __name__ == "__main__":
    main()
