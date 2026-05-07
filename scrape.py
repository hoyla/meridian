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

import api_client
import db
import eurostat
import parse

load_dotenv()
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


# Index URLs and the release_kind their links represent.
SEED_INDEXES: list[tuple[str, str]] = [
    ("http://english.customs.gov.cn/statics/report/preliminary.html", "preliminary"),
]


def _is_index_url(url: str) -> bool:
    return "/statics/report/preliminary" in url.lower() or "/statics/report/monthly" in url.lower()


def scrape_index(url: str, release_kind: str, dry_run: bool = False) -> None:
    log.info("Fetching index %s", url)
    response = api_client.fetch(url)
    discovered = api_client.discover_release_urls(response.content, url)
    log.info("Discovered %d release links from %s", len(discovered), url)
    for rel in discovered:
        scrape_release(rel["url"], release_kind=release_kind, dry_run=dry_run)


def scrape_release(url: str, release_kind: str = "preliminary", dry_run: bool = False) -> None:
    log.info("Fetching release %s", url)
    run_id = db.start_run(url) if not dry_run else None
    try:
        response = api_client.fetch(url)
        if not dry_run:
            db.save_snapshot(run_id, response)
        result = parse.parse_response(response)
        meta = result.metadata
        log.info(
            "Parsed %d observations from section %d (%s, %s)",
            len(result.observations), meta.section_number, meta.currency, meta.period.isoformat(),
        )
        if not dry_run:
            release_id = db.find_or_create_gacc_release(meta, release_kind=release_kind)
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
    p.add_argument("--partner", action="append", metavar="CC",
                   help="ISO-2 partner country code(s) to filter Eurostat to. "
                        "Default: CN. Repeat for multiple, e.g. --partner CN --partner US")
    p.add_argument("--hs-prefix", action="append", metavar="HS",
                   help="HS-CN8 prefix(es) to filter Eurostat to (e.g. 87038). "
                        "Default: no HS filter. Repeat for multiple.")
    p.add_argument("--dry-run", action="store_true", help="Fetch + parse but don't write to DB")
    args = p.parse_args()

    if args.eurostat_period:
        partners = set(args.partner) if args.partner else {"CN"}
        hs_prefixes = tuple(args.hs_prefix) if args.hs_prefix else None
        scrape_eurostat(args.eurostat_period, partners=partners,
                        hs_prefixes=hs_prefixes, dry_run=args.dry_run)
        return

    run_scrape(urls=[args.url] if args.url else None, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
