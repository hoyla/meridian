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


def scrape_index(url: str, release_kind: str, dry_run: bool = False) -> None:
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
        )


def scrape_release(
    url: str, release_kind: str = "preliminary", dry_run: bool = False,
    *, expected_currency: str | None = None,
    expected_period: date | None = None,
) -> None:
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


def scrape_eurostat(
    period: date,
    partners: set[str] | None = None,
    hs_prefixes: tuple[str, ...] | None = None,
    dry_run: bool = False,
) -> None:
    """Fetch one Eurostat monthly bulk file, persist raw rows, aggregate, persist observations.

    The raw CSV rows are stored verbatim in `eurostat_raw_rows`; the aggregated
    per-cell observations in `observations` carry an FK array back to the raw
    rows so any aggregation can be audited or re-derived.

    NB: we don't write the 44 MB raw 7z to source_snapshots — Eurostat bulk files
    are immutable per period (re-fetchable by URL) and storing them would inflate
    the DB. The release row's source_url is the audit trail.
    """
    url = eurostat.bulk_file_url(period)
    log.info("Fetching Eurostat bulk file for %s", period.strftime("%Y-%m"))
    run_id = db.start_run(url) if not dry_run else None
    try:
        response = eurostat.fetch_bulk_file(period)
        raw_rows = list(
            eurostat.iter_raw_rows(
                response.content, period, partners=partners, hs_prefixes=hs_prefixes
            )
        )
        log.info(
            "Fetched %d raw rows for %s (partners=%s, hs_prefixes=%s)",
            len(raw_rows), period.strftime("%Y-%m"),
            sorted(partners) if partners else "ANY",
            hs_prefixes or "ANY",
        )

        if dry_run:
            obs = list(eurostat.aggregate_to_observations(period, [(None, r) for r in raw_rows]))
            log.info("Dry run: would aggregate to %d observations", len(obs))
            return

        raw_ids = db.bulk_insert_eurostat_raw_rows(run_id, raw_rows)
        log.info("Inserted %d eurostat_raw_rows", len(raw_ids))
        observations = list(eurostat.aggregate_to_observations(period, list(zip(raw_ids, raw_rows))))
        log.info("Aggregated to %d observations", len(observations))
        release_id = db.find_or_create_eurostat_release(period, url)
        counts = db.upsert_observations(run_id, release_id, observations)
        log.info("Persisted: %s", counts)
        db.finish_run(run_id, status="success", http_status=response.status_code)
    except Exception as e:
        log.exception("Eurostat scrape failed for %s", period)
        if run_id is not None:
            db.finish_run(run_id, status="failed", error_message=str(e))


def _world_aggregate_hs_prefixes_from_hs_groups() -> tuple[str, ...]:
    """Read the active `hs_groups.hs_patterns` and convert them to the prefix
    set the bulk-file streamer can use. Each pattern ends with '%' (SQL LIKE
    convention); we strip the '%' to get a literal startswith prefix.

    The resulting tuple is passed to `iter_raw_rows(hs_prefixes=...)` which
    filters via `str.startswith` — so '2922%' becomes '2922' and '85044084%'
    becomes '85044084'. Eurostat product_nc is zero-padded to 8 chars, so a
    short prefix like '2922' matches every CN8 sub-code beneath HS chapter
    2922 as you'd expect.
    """
    import psycopg2
    with psycopg2.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute("SELECT hs_patterns FROM hs_groups")
        rows = cur.fetchall()
    prefixes: set[str] = set()
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
) -> None:
    """Fetch one HMRC OTS monthly slice (period × China-and-SARs by default),
    persist raw rows, aggregate, persist observations.

    Pre-requisite: the period's GBP/EUR FX rate must be in `fx_rates`.
    Run `python scrape.py --fetch-fx GBP --fx-since 2017-01` once to
    populate the full ECB history. Without it the conversion to EUR is
    skipped (value_eur left NULL on raw rows; observations would sum to 0).
    """
    if country_ids is None:
        country_ids = hmrc.DEFAULT_COUNTRY_IDS

    fx = lookups.lookup_fx("GBP", "EUR", period)
    if fx is None:
        log.error(
            "HMRC scrape for %s skipped — no GBP/EUR FX rate in fx_rates "
            "for that period. Run --fetch-fx GBP first.",
            period.strftime("%Y-%m"),
        )
        return
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
            return

        # Idempotent re-ingest: clear any prior raw rows for this period
        # before inserting the fresh fetch. Observations are handled by
        # upsert_observations' supersede chain.
        deleted = db.delete_hmrc_raw_rows_for_period(period)
        if deleted:
            log.info("Cleared %d stale hmrc_raw_rows for %s before re-insert",
                     deleted, period.strftime("%Y-%m"))
        raw_ids = db.bulk_insert_hmrc_raw_rows(run_id, raw_rows)
        log.info("Inserted %d hmrc_raw_rows", len(raw_ids))
        observations = list(hmrc.aggregate_to_observations(period, list(zip(raw_ids, raw_rows))))
        log.info("Aggregated to %d observations", len(observations))
        release_id = db.find_or_create_hmrc_release(period, initial_url)
        counts = db.upsert_observations(run_id, release_id, observations)
        log.info("Persisted: %s", counts)
        db.finish_run(run_id, status="success", http_status=response.status_code)
    except Exception as e:
        log.exception("HMRC scrape failed for %s", period)
        if run_id is not None:
            db.finish_run(run_id, status="failed", error_message=str(e))


def _parse_period(s: str) -> date:
    """Accept YYYY-MM or YYYYMM; returns the first-of-month anchor date."""
    s = s.strip().replace("-", "")
    if len(s) != 6 or not s.isdigit():
        raise argparse.ArgumentTypeError(f"--eurostat-period must be YYYY-MM, got {s!r}")
    return date(int(s[:4]), int(s[4:]), 1)


def run_scrape(urls: list[str] | None = None, dry_run: bool = False) -> None:
    if urls:
        for url in urls:
            if _is_index_url(url):
                scrape_index(url, release_kind="preliminary", dry_run=dry_run)
            else:
                scrape_release(url, dry_run=dry_run)
        return
    for index_url, release_kind in SEED_INDEXES:
        scrape_index(index_url, release_kind=release_kind, dry_run=dry_run)


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
                            "hs-group-trajectory", "gacc-aggregate-yoy",
                            "gacc-bilateral-aggregate-yoy", "partner-share",
                            "llm-framing"],
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
            "new_data", "no_change", "not_yet_eligible", "error",
            "started", "completed",
        ],
        help=(
            "With --log-check: outcome of the check. The 'started' / "
            "'completed' values are reserved for the whole-Routine "
            "lifecycle bookends — paired with --log-check _routine."
        ),
    )
    p.add_argument(
        "--log-period", type=_parse_period, metavar="YYYY-MM",
        help=(
            "With --log-check: the period the Routine attempted to fetch "
            "(Eurostat / HMRC). Omit for GACC index walks where the "
            "concept doesn't apply."
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
        "--source-status", action="store_true",
        help=(
            "Print a rolled-up debug view of routine_check_log: per "
            "expected source (eurostat / hmrc / gacc), when last checked, "
            "when new data last arrived, what's the latest period in the "
            "DB. No writes."
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

    if args.log_check:
        if not args.log_result:
            p.error("--log-check requires --log-result")
        import routine_log
        rid = routine_log.log_check(
            args.log_check,
            args.log_result,
            candidate_period=args.log_period,
            notes=args.log_notes,
            error=args.log_error,
            duration_ms=args.log_duration_ms,
        )
        log.info(
            "routine_check_log id=%d (source=%s result=%s)",
            rid, args.log_check, args.log_result,
        )
        return

    if args.source_status:
        import routine_log
        statuses = routine_log.compute_status()
        lifecycle = routine_log.compute_lifecycle()
        print(routine_log.render_status_table(statuses, lifecycle), end="")
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

    if args.periodic_run:
        result = periodic.run_periodic(
            force=args.force,
            out_dir=args.export_dir,
            top_n=args.briefing_top_n,
            llm_model=args.llm_model,
            skip_llm=args.skip_llm,
        )
        log.info("periodic-run: %s", result.reason)
        # Print the findings path to stdout (separate from log) so a wrapper
        # (Claude Code routine, GHA, cron) can capture it for delivery.
        # Empty string on no-op so the wrapper can branch on `if path:`.
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
        )
        log.info("Wrote briefing pack to %s", brief_path)
        log.info("Wrote investigation leads to %s", leads_path)
        return

    if args.fetch_fx:
        for ccy in args.fetch_fx:
            counts = fx.populate_fx_rates_from_ecb(ccy.upper(), since=args.fx_since)
            log.info("FX %s/EUR: %s", ccy.upper(), counts)
        return

    if args.eurostat_period:
        partners = set(args.partner) if args.partner else {"CN"}
        hs_prefixes = tuple(args.hs_prefix) if args.hs_prefix else None
        scrape_eurostat(args.eurostat_period, partners=partners,
                        hs_prefixes=hs_prefixes, dry_run=args.dry_run)
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

    run_scrape(urls=[args.url] if args.url else None, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
