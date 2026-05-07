"""HTTP client for fetching GACC release pages.

Centralises timeouts, content hashing, and link discovery so scrape.py stays
focused on orchestration.
"""

import hashlib
import logging
import os
from dataclasses import dataclass

import httpx

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30.0


def _user_agent() -> str:
    return os.environ.get("USER_AGENT", "gacc-monitor/0.1")


@dataclass
class FetchResult:
    url: str
    status_code: int
    content_type: str | None
    content: bytes
    sha256: str


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


def discover_release_urls(seed_html: bytes, base_url: str) -> list[str]:
    """Parse a seed/index page and return URLs of individual release pages."""
    raise NotImplementedError("Implement once we've inspected the seed page")
