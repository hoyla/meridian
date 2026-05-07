"""HTML / PDF parsing for GACC releases.

Returns a list of ParsedObservation dicts that scrape.py hands to
db.upsert_observations. Each dict carries enough provenance (source_row, dims)
to be audited back to the raw table cell.
"""

import logging
from typing import Any, TypedDict

from api_client import FetchResult

log = logging.getLogger(__name__)


class ParsedObservation(TypedDict, total=False):
    flow: str | None
    partner_country: str | None
    hs_code: str | None
    commodity_label: str | None
    value_usd: float | None
    quantity: float | None
    quantity_unit: str | None
    source_row: dict[str, Any]
    period: str            # ISO date for the release period
    release_kind: str      # 'preliminary' | 'monthly' | 'revised'


def parse_response(response: FetchResult) -> list[ParsedObservation]:
    ct = (response.content_type or "").lower()
    if "pdf" in ct or response.url.lower().endswith(".pdf"):
        return parse_pdf(response.content)
    return parse_html(response.content, response.url)


def parse_html(html: bytes, url: str) -> list[ParsedObservation]:
    raise NotImplementedError("Implement once we've inspected a real GACC page")


def parse_pdf(pdf_bytes: bytes) -> list[ParsedObservation]:
    raise NotImplementedError("Implement once we've inspected a real GACC PDF")
