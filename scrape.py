"""GACC (China Customs) trade-statistics scraper.

Fetches the configured customs.gov.cn release URLs, snapshots the raw response,
parses tables into structured observations, and triggers downstream analysis.

Usage:
    python scrape.py                     # check all configured URLs
    python scrape.py --url <single-url>  # one-shot fetch of a single page
    python scrape.py --dry-run           # fetch + parse but don't write to DB
"""

import argparse
import logging
import os

from dotenv import load_dotenv

import api_client
import db
import parse

load_dotenv()
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


# Seed URL list — extend as we identify more pages worth tracking.
# api_client.discover_release_urls() walks from these to individual release pages.
SEED_URLS = [
    "http://english.customs.gov.cn/statics/report/preliminary.html",
]


def run_scrape(urls: list[str], dry_run: bool = False) -> None:
    for url in urls:
        log.info("Fetching %s", url)
        run_id = db.start_run(url) if not dry_run else None
        try:
            response = api_client.fetch(url)
            if not dry_run:
                db.save_snapshot(run_id, response)
            parsed = parse.parse_response(response)
            log.info("Parsed %d observation rows from %s", len(parsed), url)
            if not dry_run:
                db.upsert_observations(run_id, parsed)
                db.finish_run(run_id, status="success", http_status=response.status_code)
        except Exception as e:
            log.exception("Scrape failed for %s", url)
            if run_id is not None:
                db.finish_run(run_id, status="failed", error_message=str(e))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", help="Scrape a single URL instead of the seed list")
    p.add_argument("--dry-run", action="store_true", help="Fetch + parse but don't write to DB")
    args = p.parse_args()

    urls = [args.url] if args.url else SEED_URLS
    run_scrape(urls, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
