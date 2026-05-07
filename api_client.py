"""HTTP client for fetching GACC release pages.

Centralises timeouts, content hashing, and link discovery so scrape.py stays
focused on orchestration.
"""

import hashlib
import logging
import os
import re
from dataclasses import dataclass
from typing import TypedDict
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30.0

PRELIMINARY_INDEX_URL = "http://english.customs.gov.cn/statics/report/preliminary.html"
PRELIMINARY_YEAR_URL_TEMPLATE = "http://english.customs.gov.cn/statics/report/preliminary{year}.html"

_MONTH_ABBREVS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

# Bulletin titles in the index table look like:
#   "(4) China's Total Export & Import Values by Country/Region (in CNY)"
_BULLETIN_TITLE_RE = re.compile(
    r"^\((?P<section>\d+)\)\s*(?P<description>.+?)\s*\(in\s*(?P<currency>CNY|USD)\)\s*$"
)


def _user_agent() -> str:
    return os.environ.get("USER_AGENT", "gacc-monitor/0.1")


@dataclass
class FetchResult:
    url: str
    status_code: int
    content_type: str | None
    content: bytes
    sha256: str


class DiscoveredRelease(TypedDict):
    url: str
    bulletin_title: str
    section_number: int
    description: str
    currency: str       # 'CNY' | 'USD'
    year: int
    month: int


def fetch(url: str, timeout: float = DEFAULT_TIMEOUT) -> FetchResult:
    """Fetch a URL once with sensible defaults. Caller decides about retries."""
    with httpx.Client(
        headers={"User-Agent": _user_agent()},
        timeout=timeout,
        follow_redirects=True,
    ) as client:
        r = client.get(url)
    r.raise_for_status()
    return FetchResult(
        url=str(r.url),
        status_code=r.status_code,
        content_type=r.headers.get("content-type"),
        content=r.content,
        sha256=hashlib.sha256(r.content).hexdigest(),
    )


def _year_from_index_url(url: str) -> int:
    """preliminary.html → current year (read from the year selector); preliminaryNNNN.html → NNNN."""
    m = re.search(r"preliminary(\d{4})\.html", url)
    if m:
        return int(m.group(1))
    raise ValueError(f"Cannot infer year from index URL {url!r}; expected preliminary{{year}}.html")


def discover_release_urls(seed_html: bytes, base_url: str) -> list[DiscoveredRelease]:
    """Parse a year-index page (preliminary[YYYY].html) and return release links.

    For the current year the URL is preliminary.html and the year is read from the
    selected option in the year `<select>`; for other years it's parsed from the URL.
    """
    soup = BeautifulSoup(seed_html, "lxml")

    # Year: from URL pattern or from the selected <option> in the year picker.
    year: int | None = None
    try:
        year = _year_from_index_url(base_url)
    except ValueError:
        sel = soup.find("select", id="preliminarysel")
        if sel:
            for opt in sel.find_all("option"):
                if opt.get("selected") is not None:
                    year = int(opt["value"])
                    break
            if year is None:
                # Default to the first option (the page renders the most recent year).
                first = sel.find("option")
                if first:
                    year = int(first["value"])
    if year is None:
        raise ValueError(f"Could not determine year for index page {base_url}")

    table = soup.find("div", class_="nr-n-content-detail")
    if table is None:
        raise ValueError("Index page is missing the expected .nr-n-content-detail container")

    releases: list[DiscoveredRelease] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 2:
            continue
        bulletin_title = cells[0].get_text(strip=True)
        m = _BULLETIN_TITLE_RE.match(bulletin_title)
        if not m:
            # Header rows or rows without a CNY/USD currency tag (e.g. bulletins 7 & 8).
            continue
        section = int(m.group("section"))
        description = m.group("description").strip()
        currency = m.group("currency")

        for a in cells[1].find_all("a", href=True):
            label = a.get_text(strip=True).rstrip(".")
            month = _MONTH_ABBREVS.get(label[:3])
            if month is None:
                log.warning("Unrecognised month label %r in %s", label, bulletin_title)
                continue
            releases.append(
                DiscoveredRelease(
                    url=urljoin(base_url, a["href"]),
                    bulletin_title=bulletin_title,
                    section_number=section,
                    description=description,
                    currency=currency,
                    year=year,
                    month=month,
                )
            )
    return releases
