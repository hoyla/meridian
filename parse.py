"""HTML / PDF parsing for GACC releases.

Returns a list of ParsedObservation dicts that scrape.py hands to
db.upsert_observations. Each dict carries enough provenance (source_row, dims)
to be audited back to the raw table cell.
"""

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, TypedDict

from bs4 import BeautifulSoup

from api_client import FetchResult

log = logging.getLogger(__name__)


_MONTH_ABBREVS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

# Release page <title> / .atcl-ttl format. Format varies by year:
#   2026: "(4) China's Total Export & Import Values by Country/Region, Mar 2026 (in CNY)"
#   2018: "China's Total Export & Import Values by Country/Region, March 2018 (in CNY)"
#         (no leading section number — must infer from description)
#         "(2) China's Total Export & Import Values by Trade Mode, August 2018 (Only August, in CNY)"
#         (parenthetical includes "Only August" prefix on some 2018 monthly releases)
#   2018: also uses "in RMB" variant (treat as synonym for CNY)
#   2018 (early-year section 4): "China's Total Value of Imports and Exports by
#         Major Country (Region), Jan. 2018"
#         — entirely different wording, month-with-trailing-period, AND no
#         "(in CCY)" suffix at all. The parent index page's bulletin row
#         carries the currency; callers pass it as `expected_currency`.
# Months: GACC inconsistently uses 3-letter abbreviation or full name in the
# title; 2018 also uses the abbreviation followed by a period ("Jan.").
_RELEASE_TITLE_RE = re.compile(
    r"^\s*(?:\((?P<section>\d+)\)\s*)?(?P<description>.+?),\s*"
    r"(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|"
    r"Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|"
    r"Nov(?:ember)?|Dec(?:ember)?)\.?\s*"
    r"(?P<year>\d{4})\s*"
    r"(?:\((?:Only\s+\w+\.?,\s*)?in\s*(?P<currency>CNY|USD|RMB)\)\s*)?$"
)

# Some 2018 section-4 release pages reuse the bulletin-row title verbatim
# with no date in the page title at all (e.g.
# "China's Total Export & Import Values by Country/Region (in CNY)" for the
# Jul 2018 release). The discovery side captures the period; we accept it
# as `expected_period` and use this fallback regex to confirm the title is
# the bulletin-row shape (description + currency suffix only) rather than
# something genuinely unrecognised.
_RELEASE_TITLE_NODATE_RE = re.compile(
    r"^\s*(?:\((?P<section>\d+)\)\s*)?(?P<description>.+?)\s*"
    r"\(in\s*(?P<currency>CNY|USD|RMB)\)\s*$"
)


def _infer_section_from_description(description: str) -> int:
    """Used when the title lacks a leading "(N)" prefix (2018 historical format).
    Returns the section number to assign based on the description prose.
    Section 1 is the catch-all default since it has no descriptive suffix.

    Section 4 has two seen wordings:
    - 2019+: "...by Country/Region"
    - 2018:  "...by Major Country (Region)"
    Both contain "country" or "region" so the existing match catches them.
    """
    d = description.lower()
    if "by country" in d or "by region" in d or "by major country" in d:
        return 4
    if "by trade mode" in d:
        return 2
    return 1


class ParsedObservation(TypedDict, total=False):
    section_number: int
    period: str             # ISO date for the period anchor (first of month)
    period_kind: str        # 'monthly' | 'ytd'
    currency: str           # 'CNY' | 'USD'
    unit: str | None        # e.g. 'CNY 100 Million'
    flow: str               # 'export' | 'import' | 'total'
    partner_country: str | None
    partner_label_raw: str | None
    partner_indent: int | None
    partner_is_subset: bool | None
    hs_code: str | None
    commodity_label: str | None
    value: float | None
    quantity: float | None
    quantity_unit: str | None
    source_row: dict[str, Any]


@dataclass
class ReleaseMetadata:
    section_number: int
    description: str
    period: date            # first of month for monthly releases
    currency: str           # 'CNY' | 'USD'
    publication_date: date | None
    unit: str | None
    excel_url: str | None
    source_url: str
    title: str


@dataclass
class ParseResult:
    metadata: ReleaseMetadata
    observations: list[ParsedObservation]


def parse_response(
    response: FetchResult, *, expected_currency: str | None = None,
    expected_period: date | None = None,
) -> ParseResult:
    ct = (response.content_type or "").lower()
    if "pdf" in ct or response.url.lower().endswith(".pdf"):
        return parse_pdf(response.content)
    return parse_html(
        response.content, response.url,
        expected_currency=expected_currency, expected_period=expected_period,
    )


def parse_html(
    html: bytes, url: str, *, expected_currency: str | None = None,
    expected_period: date | None = None,
) -> ParseResult:
    soup = BeautifulSoup(html, "lxml")
    meta = extract_metadata(
        soup, url,
        expected_currency=expected_currency, expected_period=expected_period,
    )
    if meta.section_number == 4:
        return ParseResult(metadata=meta, observations=_parse_section_4_by_country(soup, meta))
    raise NotImplementedError(
        f"HTML parser for section {meta.section_number} ({meta.description!r}) not implemented yet"
    )


def parse_pdf(pdf_bytes: bytes) -> ParseResult:
    raise NotImplementedError("Implement once we've inspected a real GACC PDF")


def extract_metadata(
    soup: BeautifulSoup, url: str, *, expected_currency: str | None = None,
    expected_period: date | None = None,
) -> ReleaseMetadata:
    """Extract release metadata from the page HTML.

    `expected_currency` and `expected_period` are values captured at discovery
    time from the parent index page (the bulletin row's `(in CCY)` tag and
    the link's month-label respectively). They are used as fallbacks for
    early-2018 release pages whose own title omits the currency suffix
    and/or the date entirely. Page-title values, when present, take
    precedence — the discovery-side fallbacks are only consulted when the
    title can't supply them.
    """
    title_el = soup.find("div", class_="atcl-ttl")
    if title_el is None:
        raise ValueError(f"Release page {url} missing .atcl-ttl")
    title = title_el.get_text(strip=True)
    m = _RELEASE_TITLE_RE.match(title)
    period: date | None = None
    if m:
        period = date(int(m.group("year")), _MONTH_ABBREVS[m.group("month")[:3]], 1)
    else:
        # Fall back to the no-date bulletin-row format (some 2018 pages
        # reuse the bulletin title verbatim with no date appended).
        m = _RELEASE_TITLE_NODATE_RE.match(title)
        if not m:
            raise ValueError(f"Unrecognised release title: {title!r}")
        if expected_period is None:
            raise ValueError(
                f"Release title {title!r} omits date and caller "
                f"supplied no expected_period"
            )
        period = expected_period

    pub_date: date | None = None
    pub_date_el = soup.find("div", class_="atcl-date")
    if pub_date_el:
        raw = pub_date_el.get_text(strip=True)
        # Modern releases use YYYY/MM/DD; 2018-era historical releases use
        # DD/MM/YYYY. Try both before warning.
        for fmt in ("%Y/%m/%d", "%d/%m/%Y"):
            try:
                pub_date = datetime.strptime(raw, fmt).date()
                break
            except ValueError:
                continue
        else:
            log.warning("Unparseable publication date: %r", raw)

    # Unit annotation appears in either a <span> wrapper (most pages) or directly
    # inside a <td> (Aug + Sep 2025 in our backfill, possibly others). Search both.
    unit: str | None = None
    for el in soup.find_all(["span", "td"]):
        text = el.get_text(strip=True)
        if text.startswith("Unit:"):
            unit = text[len("Unit:"):].strip()
            break

    excel_url: str | None = None
    rct = soup.find("div", class_="atcl-rct")
    if rct:
        a = rct.find("a", href=True)
        if a:
            # Source uses Windows-style backslashes in the href.
            excel_url = a["href"].replace("\\", "/")

    section_str = m.group("section")
    description = m.group("description").strip()
    section = int(section_str) if section_str else _infer_section_from_description(description)
    # GACC uses RMB and CNY interchangeably in titles (RMB appears in some 2018
    # releases). They're the same currency — normalise to CNY so the dimensional
    # key in releases matches across years. Early-2018 section-4 release pages
    # omit the currency tag entirely; in that case we fall back to the
    # `expected_currency` captured at discovery time from the parent index
    # page's bulletin row, raising if neither source supplies one.
    currency = m.group("currency")
    if currency is None:
        if expected_currency is None:
            raise ValueError(
                f"Release title {title!r} omits currency tag and caller "
                f"supplied no expected_currency"
            )
        currency = expected_currency
    if currency == "RMB":
        currency = "CNY"

    return ReleaseMetadata(
        section_number=section,
        description=description,
        period=period,
        currency=currency,
        publication_date=pub_date,
        unit=unit,
        excel_url=excel_url,
        source_url=url,
        title=title,
    )


def _normalise_partner_label(raw: str) -> tuple[str, int, bool]:
    """Returns (label, indent_level, is_subset). The hierarchy in the source HTML
    is encoded with non-breaking spaces, so we strip only ASCII whitespace before
    counting the indent — Python's default str.strip() would eat nbsps too.
    Interior whitespace (including embedded newlines from multi-line cells) is
    collapsed to a single space so labels join the country_aliases lookup cleanly."""
    stripped = raw.strip(" \t\n\r\f\v")
    indent = len(stripped) - len(stripped.lstrip("\xa0"))
    label = stripped.replace("\xa0", " ")
    label = re.sub(r"\s+", " ", label).strip()
    is_subset = label.startswith("of which:")
    if is_subset:
        label = label[len("of which:"):].strip()
    return label, indent, is_subset


def _parse_number(raw: str) -> float | None:
    """Strip nbsp/comma/whitespace and parse as float. Returns None for empty cells."""
    s = raw.replace("\xa0", " ").replace(",", "").strip()
    if not s or s == "-":
        return None
    return float(s)  # raises ValueError on non-numeric — used to detect header rows


def _parse_section_4_by_country(soup: BeautifulSoup, meta: ReleaseMetadata) -> list[ParsedObservation]:
    """Section 4: 'China's Total Export & Import Values by Country/Region'.

    Column layout (10 cells per data row):
      0: partner country/region label (indented)
      1,2: Export & Import — month, YTD-1-to-N
      3,4: Export         — month, YTD-1-to-N
      5,6: Import         — month, YTD-1-to-N
      7,8,9: YoY% for E&I, Export, Import (computed downstream from history)
    """
    container = soup.find("div", class_="atcl-cnt")
    if container is None:
        raise ValueError(f"Section 4 page {meta.source_url} missing .atcl-cnt")
    table = container.find("table")
    if table is None:
        raise ValueError(f"Section 4 page {meta.source_url} has no table inside .atcl-cnt")

    period_iso = meta.period.isoformat()  # e.g. '2026-03-01' for both monthly & YTD anchor
    out: list[ParsedObservation] = []

    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) != 10:
            continue
        if any(c.get("colspan") for c in cells):
            continue

        raw_label = cells[0].get_text()
        label, indent, is_subset = _normalise_partner_label(raw_label)
        if not label:
            continue

        try:
            values = [_parse_number(cells[i].get_text()) for i in range(1, 7)]
        except ValueError:
            # Header row whose value cells aren't numeric — skip.
            continue

        source_row = {
            "raw_label": raw_label,
            "monthly_total": values[0],
            "ytd_total": values[1],
            "monthly_export": values[2],
            "ytd_export": values[3],
            "monthly_import": values[4],
            "ytd_import": values[5],
        }

        for flow, monthly_idx, ytd_idx in [
            ("total", 0, 1),
            ("export", 2, 3),
            ("import", 4, 5),
        ]:
            for kind, idx in [("monthly", monthly_idx), ("ytd", ytd_idx)]:
                v = values[idx]
                if v is None:
                    continue
                out.append(
                    ParsedObservation(
                        section_number=meta.section_number,
                        period=period_iso,
                        period_kind=kind,
                        currency=meta.currency,
                        unit=meta.unit,
                        flow=flow,
                        partner_country=label,
                        partner_label_raw=raw_label,
                        partner_indent=indent,
                        partner_is_subset=is_subset,
                        value=v,
                        source_row=source_row,
                    )
                )
    return out
