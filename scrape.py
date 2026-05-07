"""GACC (China Customs) trade-statistics scraper.

Walks the configured customs.gov.cn index pages, snapshots each release page,
parses tables into structured observations, and persists them with versioning
so successive scrapes of the same page surface real revisions rather than
silently overwriting them.

Usage:
    python scrape.py                     # walk all configured index URLs
    python scrape.py --url <url>         # one-shot fetch (index OR release URL)
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
            release_id = db.find_or_create_release(meta, release_kind=release_kind)
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
    p.add_argument("--url", help="Scrape a single URL (index OR release) instead of the seed list")
    p.add_argument("--dry-run", action="store_true", help="Fetch + parse but don't write to DB")
    args = p.parse_args()
    run_scrape(urls=[args.url] if args.url else None, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
