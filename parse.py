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


class UnparseableReleasePage(NotImplementedError):
    """The page is not an ingestable section-4 release table in a format we
    parse: an unrecognised title (a non-section-4 / historical page shape we
    never ingest) or a section-4 page carrying no HTML table at all.

    Subclasses NotImplementedError so scrape_release records it as terminal
    'no_parser' — the same disposition as any section we deliberately don't
    ingest — rather than a retried-and-alerted 'failed'. This is distinct from
    a genuine ingest failure on a real current table (column-layout drift, an
    empty/partial parse), which stays a ValueError → 'failed' → retried, so a
    live release that regresses is still surfaced. Relies on GACC's invariant
    (see the empty-parse guard in scrape.scrape_release) that a modern
    section-4 release page always carries its table once published; the pages
    that trip this are historical 2018 shapes with no inline table."""


class CurrencyUnitMismatch(ValueError):
    """A section-4 page whose title currency disagrees with its Unit-row scale
    — the release-184 shape (title '(in CNY)' + Unit 'USD1 Million'). Carries
    the resolved (section, period, currency) so scrape_release can distinguish
    a genuine held-back release (no live sibling yet → 'failed', surfaced) from
    a page already superseded by its canonical sibling (a live release for the
    same cell already exists → terminal 'no_parser', not re-alerted every
    walk). Kept a ValueError subclass so callers matching on ValueError /
    'self-inconsistent' still catch it."""

    def __init__(self, message: str, *, section: int, period: date, currency: str):
        super().__init__(message)
        self.section = section
        self.period = period
        self.currency = currency


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

# Combined "January-February" cumulative release pattern (Chinese New Year
# release shape). Some years (2025 confirmed) publish only ONE release
# covering Jan + Feb together, with both the "Monthly" and "YTD" columns
# in the body table holding the same Jan+Feb cumulative value. Title shape
# observed: "(4) China's Total Export & Import Values by Country/Region,
# January-February 2025 (in CNY)". Hyphen variants seen in the wild: ASCII
# `-`, en-dash `–`, em-dash `—`. Whitespace around the separator varies.
# When matched, the parser sets ReleaseMetadata.is_jan_feb_combined and
# the body parser emits observations under period_kind='cumulative_jan_feb'
# instead of the usual 'monthly' + 'ytd' pair — see
# briefing_pack/templates/README.md and `dev_notes/history.md` for the
# editorial motivation (the rolling-12mo windows previously had to skip
# Jan / Feb for these years and the YoY caveat was load-bearing).
_RELEASE_TITLE_JAN_FEB_RE = re.compile(
    r"^\s*(?:\((?P<section>\d+)\)\s*)?(?P<description>.+?),\s*"
    r"Jan(?:uary)?\s*[-–—]\s*Feb(?:ruary)?\.?\s*"
    r"(?P<year>\d{4})\s*"
    r"(?:\(in\s*(?P<currency>CNY|USD|RMB)\)\s*)?$"
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
    if "major exports" in d:
        return 5
    if "major imports" in d:
        return 6
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
    # True when the title matched `_RELEASE_TITLE_JAN_FEB_RE` — a release
    # that publishes a single Jan+Feb cumulative value rather than the
    # usual single-month figure. The body parser branches on this to emit
    # observations with period_kind='cumulative_jan_feb'. `period` itself
    # is set to Feb 1 of the year (the latest month the release covers);
    # `release_kind` is set to 'preliminary_jan_feb' by the caller so the
    # natural-key on `releases` doesn't collide with a hypothetical
    # separate-Feb release for the same year.
    is_jan_feb_combined: bool = False


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
    if meta.section_number in (5, 6):
        return ParseResult(
            metadata=meta, observations=_parse_section_5_6_commodities(soup, meta)
        )
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
    is_jan_feb_combined = False
    if m:
        period = date(int(m.group("year")), _MONTH_ABBREVS[m.group("month")[:3]], 1)
    elif (mjf := _RELEASE_TITLE_JAN_FEB_RE.match(title)) is not None:
        # Combined Jan+Feb release (Chinese New Year shape). Period is
        # anchored at February — the latest month the release covers.
        m = mjf
        period = date(int(m.group("year")), 2, 1)
        is_jan_feb_combined = True
    else:
        # Fall back to the no-date bulletin-row format (some 2018 pages
        # reuse the bulletin title verbatim with no date appended).
        m = _RELEASE_TITLE_NODATE_RE.match(title)
        if not m:
            # A title none of the recognised shapes match is a page we have no
            # parser for (historical annual-totals / by-trade-mode / commodity
            # pages, and 2018 wordings we don't ingest). Record it terminal
            # 'no_parser', not a retried-and-alerted 'failed' — see
            # UnparseableReleasePage and
            # dev_notes/2026-07-08-gacc-section1-usd-floor-false-positive.md.
            raise UnparseableReleasePage(f"Unrecognised release title: {title!r}")
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
    inferred_section = _infer_section_from_description(description)
    if section_str is None:
        section = inferred_section
    else:
        section = int(section_str)
        # GACC occasionally mis-numbers the leading "(N)" prefix — the 2020
        # combined Jan-Feb "by Country/Region" release was tagged (3), not (4).
        # When the description unambiguously identifies a section (anything but
        # the section-1 catch-all), trust it over the glitchy prefix; otherwise
        # the by-country data is dropped (section 4 is the only one we ingest).
        if inferred_section != 1 and inferred_section != section:
            log.warning(
                "GACC title section prefix (%d) disagrees with description "
                "%r; trusting the description -> section %d",
                section, description, inferred_section,
            )
            section = inferred_section
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

    # Defend against GACC pages where the title's currency disagrees with
    # the page's "Unit:" annotation — release 184 (June 2025) is the
    # archetypal incident: title "(in CNY)" + Unit: "USD1 Million" + USD-
    # edition excel link, with the table cells carrying USD-million values
    # the parser would otherwise file as CNY-100-million. Coercing only
    # the metadata (as we did between 2026-05-14 and 2026-05-19) keeps the
    # release row's CHECK constraint happy but lets the bad cell values
    # propagate into observations and downstream findings — see
    # migrations/2026-05-14-fix-release-184-cny-usd-unit-mismatch.sql and
    # migrations/2026-05-19-reject-mismatched-gacc-currency-unit-pages.sql
    # for the recurrence. Raise here so the scrape lands as
    # status='failed' and no observations get inserted for the bad URL;
    # the canonical sibling URL (whose title and Unit row agree) supplies
    # the period. Older releases that omit the Unit row entirely fall
    # through unchanged.
    #
    # Scope the floor to the sections whose cell values reach `observations`:
    # 4 (by country), and — since the commodity-highlights build
    # (dev_notes/2026-07-14-gacc-commodity-highlights.md) — 5/6 (major
    # exports/imports by quantity and value), which share section 4's
    # canonical units exactly ("CNY 100 Million" / "USD1 Million", verified
    # across the 2019–2026 fixtures). Section-1 "Total Values" pages
    # legitimately denominate USD headline totals in "USD 100 Million"
    # (亿美元), not "USD1 Million"; applying the canonical unit to them
    # mis-fired a "self-inconsistent" currency conflict (the title and Unit
    # row actually AGREE on currency — only the scale differs) on a page we
    # never ingest. Sections 1/2/3 fall through to the NotImplementedError
    # no_parser path in parse_html, so a scale mislabel on an un-ingested
    # section cannot corrupt anything.
    # See dev_notes/2026-07-08-gacc-section1-usd-floor-false-positive.md.
    if section in (4, 5, 6):
        _CANONICAL_GACC_UNIT = {"CNY": "CNY 100 Million", "USD": "USD1 Million"}
        canonical_unit = _CANONICAL_GACC_UNIT.get(currency)
        if canonical_unit is not None:
            if unit is not None and unit != canonical_unit:
                raise CurrencyUnitMismatch(
                    f"GACC page {url} self-inconsistent: title declares "
                    f"currency {currency!r} but the page's Unit: row reads "
                    f"{unit!r}. Refusing to ingest cell values that don't "
                    f"match the title's currency; the table values are likely "
                    f"in {unit!r}, not in the canonical {canonical_unit!r} "
                    f"for {currency}. See migrations/2026-05-14- and "
                    f"migrations/2026-05-19- for the incident history.",
                    section=section, period=period, currency=currency,
                )
            unit = canonical_unit

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
        is_jan_feb_combined=is_jan_feb_combined,
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
        # No content container / no table = not an ingestable section-4 data
        # table (historical 2018 by-country pages carry no inline table).
        # Terminal 'no_parser', not retried 'failed' — see UnparseableReleasePage.
        raise UnparseableReleasePage(f"Section 4 page {meta.source_url} missing .atcl-cnt")
    table = container.find("table")
    if table is None:
        raise UnparseableReleasePage(f"Section 4 page {meta.source_url} has no table inside .atcl-cnt")

    period_iso = meta.period.isoformat()  # e.g. '2026-03-01' for both monthly & YTD anchor
    out: list[ParsedObservation] = []

    # Combined Jan+Feb releases use a narrower 7-column layout (no separate
    # "monthly" view — both columns would carry the same Jan+Feb sum, so
    # the page just publishes one). Layout observed on the 2025 release:
    #   0: partner label
    #   1: Export & Import (Jan+Feb cumulative total)
    #   2: Export          (Jan+Feb cumulative)
    #   3: Import          (Jan+Feb cumulative)
    #   4,5,6: YoY% for the three flows (derived downstream, not stored)
    # Regular monthly releases keep the existing 10-column layout below.
    if meta.is_jan_feb_combined:
        expected_cells = 7
        flow_value_idx = [("total", 1), ("export", 2), ("import", 3)]
    else:
        expected_cells = 10
        flow_value_idx = None  # the regular loop uses the monthly/ytd pair below

    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) != expected_cells:
            continue
        if any(c.get("colspan") for c in cells):
            continue

        raw_label = cells[0].get_text()
        label, indent, is_subset = _normalise_partner_label(raw_label)
        if not label:
            continue

        if meta.is_jan_feb_combined:
            try:
                vals = [_parse_number(cells[i].get_text()) for i in (1, 2, 3)]
            except ValueError:
                continue
            source_row = {
                "raw_label": raw_label,
                "cumulative_total": vals[0],
                "cumulative_export": vals[1],
                "cumulative_import": vals[2],
            }
            for (flow, _idx), v in zip(flow_value_idx, vals):
                if v is None:
                    continue
                out.append(
                    ParsedObservation(
                        section_number=meta.section_number,
                        period=period_iso,
                        period_kind="cumulative_jan_feb",
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


# Reject a section-4 parse below this many top-level partners. A healthy
# 'by Country/Region' release lists ~26 top-level partner/bloc rows — the major
# economies plus regional aggregates (EU, ASEAN, US, Japan, Belt & Road, …) —
# a set stable month to month (mar2026 & janfeb2025 fixtures: 26 each). Floor
# well below that so a legitimate release never trips it, yet well above a
# broken/partial parse.
_SECTION4_MIN_PARTNERS = 20


def section4_floor_check(
    observations: list[ParsedObservation], meta: ReleaseMetadata
) -> str | None:
    """Plausibility floor for a section-4 parse — the count/magnitude guard the
    A1 empty-parse guard left open (finding F5).

    The empty-parse guard in scrape.py rejects a parse that yields ZERO
    observations (a whole-table layout drift zeroes the structural row detector).
    But a *partial* parse — some rows dropped by drift over part of the table, or
    a truncated preliminary listing — yields >0 observations and an
    incomplete/garbled partner set, which the empty check waves through and the
    YoY analysers then read as a complete month under a green 'success'. Two
    cheap, layout-independent invariants catch it (both hold on the mar2026
    monthly and janfeb2025 cumulative fixtures):

      count     — far fewer than the ~26 stable top-level partners means rows
                  were silently dropped.
      magnitude — where the 'Total' grand-total row parsed, it must carry the
                  largest value (the grand total dominates any single
                  partner/bloc); a partner out-valuing it means the value columns
                  were misread by a same-width column shift the row detector
                  cannot see. Conditional on the Total row being present: it is
                  not consumed by the per-partner analysers, so a mere label
                  change must not block an otherwise-complete release.

    Returns a human-readable reason to reject, or None if the parse is plausible.
    Scoped to section 4 — the only section that yields observations today.
    """
    if meta.section_number != 4:
        return None

    partners = {
        o.get("partner_country")
        for o in observations
        if o.get("partner_country") and not o.get("partner_is_subset")
    }
    if len(partners) < _SECTION4_MIN_PARTNERS:
        return (
            f"only {len(partners)} top-level partners parsed (floor "
            f"{_SECTION4_MIN_PARTNERS}; a healthy section-4 release lists ~26) — "
            f"likely a partial parse from column-layout drift or a truncated table"
        )

    total_values = [
        o["value"]
        for o in observations
        if (o.get("partner_country") or "").strip().lower() == "total"
        and o.get("value") is not None
    ]
    if total_values:
        all_values = [o["value"] for o in observations if o.get("value") is not None]
        if max(total_values) < max(all_values):
            return (
                "a partner out-values the 'Total' grand-total row — the value "
                "columns were likely misread (column-layout drift)"
            )
    return None


def _normalise_commodity_label(raw: str) -> tuple[str, int, bool]:
    """Returns (label, indent_level, is_aggregate) for a section-5/6 commodity
    cell. Same nbsp-indent convention as the section-4 partner column (reuses
    the indent-counting approach of `_normalise_partner_label`), plus the
    commodity pages' own aggregate marker: a trailing '*' flags the three
    catalogue aggregates ("Agriculture products*", "Mechanical and electrical
    products*", "Hi-tech products*") whose membership is NOT adjacency — the
    page footnote says they "include relevant products listed in the table",
    and Hi-tech overlaps Mech&elec (ICs sit in both). Downstream must never
    sum rows; the flag exists so selection logic can exclude aggregates."""
    stripped = raw.strip(" \t\n\r\f\v")
    indent = len(stripped) - len(stripped.lstrip("\xa0"))
    label = stripped.replace("\xa0", " ")
    label = re.sub(r"\s+", " ", label).strip()
    is_aggregate = label.endswith("*")
    if is_aggregate:
        label = label[:-1].strip()
    return label, indent, is_aggregate


def _cell_text(cell) -> str:
    """Normalised text of a table cell: nbsp → space, collapsed, stripped."""
    return re.sub(r"\s+", " ", cell.get_text().replace("\xa0", " ")).strip()


def _section56_has_unit_column(rows: list[list]) -> bool:
    """Does this section-5/6 body carry the Quantity Unit column?

    Normally yes — cell 1 is the unit and the final cell is GACC's published
    YoY value %, always a number. But the April-2025 USD exports page
    (english.customs.gov.cn/Statics/5f7ac0d0-…html, still live and unchanged
    as of 2026-08-07) drops the unit <td> from every body row and appends a
    blank <td> instead, so the row keeps its 10-cell width, clears the
    cell-count guard, and every field lands one place to the left. That page
    stored 111 million cars exported in a month (the value column read as
    quantity) while its CNY sibling parsed correctly. See
    dev_notes/2026-08-07-gacc-section5-missing-unit-column.md.

    Discriminator is the LAST cell, not the first: a blank trailing cell is
    the padding, and the real final column (published YoY value %) is never
    blank on a healthy page. Require it blank on *every* candidate row so a
    single odd row can't flip the whole table's interpretation.

    `rows` is the list of candidate data rows, each a list of <td> elements.
    """
    if not rows:
        return True
    return not all(_cell_text(cells[-1]) == "" for cells in rows)


def _parse_section_5_6_commodities(
    soup: BeautifulSoup, meta: ReleaseMetadata
) -> list[ParsedObservation]:
    """Sections 5/6: 'China's Major Exports/Imports by Quantity and Value' —
    GACC's curated ~30-commodity headline catalogue (no HS codes), China↔world.
    See dev_notes/2026-07-14-gacc-commodity-highlights.md.

    Column layout (10 cells per regular data row, stable 2019–2026 fixtures):
      0: commodity label (nbsp-indented; '*' suffix on the three aggregates)
      1: quantity unit ('10,000 Tons', 'Ton', 'Ship', …; '—'/'-' for
         value-only commodities)
      2,3:  month        — Quantity, Value
      4,5:  1-N YTD      — Quantity, Value
      6,7:  prior-year 1-N YTD — Quantity, Value (GACC's own comparison basis)
      8,9:  published YoY% (cumulative) — Quantity, Value

    Combined Jan+Feb releases drop the month pair → 8 cells:
      0: label  1: unit  2,3: Jan+Feb cumulative Q,V
      4,5: prior-year Jan+Feb Q,V  6,7: published YoY% Q,V

    Some pages ship a variant that OMITS the Quantity Unit <td> from every
    body row and pads the end with a blank <td>, keeping the expected width
    (see `_section56_has_unit_column`). Handled by shifting the numeric slice
    one place left; `quantity_unit` is then genuinely absent from the document
    and stored as NULL, with `unit_column_absent` recorded in source_row so
    the provenance trail says why. Never borrow the unit from the currency
    sibling — that would be an inference written into a source-material field.

    Emits one observation per (row, period_kind): 'monthly' + 'ytd' for
    regular releases, 'cumulative_jan_feb' for combined ones. The prior-year
    cumulative columns and GACC's published YoY% are stored in source_row
    ONLY, not as observations — emitting them would create a second
    provenance path to numbers the prior year's own release already carries.
    (They matter downstream: the analyser derives single-month prior-year
    values as adjacent-page prior-YTD differences, label-consistent within an
    era, and cross-checks our cumulative YoY against GACC's published one.)
    Quantity units vary per row and are stored verbatim, never normalised."""
    container = soup.find("div", class_="atcl-cnt")
    if container is None:
        raise UnparseableReleasePage(
            f"Section {meta.section_number} page {meta.source_url} missing .atcl-cnt"
        )
    table = container.find("table")
    if table is None:
        raise UnparseableReleasePage(
            f"Section {meta.section_number} page {meta.source_url} has no table "
            f"inside .atcl-cnt"
        )

    period_iso = meta.period.isoformat()
    flow = "export" if meta.section_number == 5 else "import"
    out: list[ParsedObservation] = []

    if meta.is_jan_feb_combined:
        expected_cells = 8
    else:
        expected_cells = 10

    candidates = [
        cells
        for tr in table.find_all("tr")
        if (cells := tr.find_all("td"))
        and len(cells) == expected_cells
        and not any(c.get("colspan") for c in cells)
    ]
    has_unit_column = _section56_has_unit_column(candidates)
    # Numeric block start: after label+unit normally, after label alone when the
    # unit column is absent (the trailing blank pad takes the last slot). Either
    # way the slice is the same width — 8 numbers, or 6 for Jan+Feb combined.
    first_num = 2 if has_unit_column else 1
    last_num = expected_cells if has_unit_column else expected_cells - 1

    for cells in candidates:
        raw_label = cells[0].get_text()
        label, indent, is_aggregate = _normalise_commodity_label(raw_label)
        if not label:
            continue

        if has_unit_column:
            unit_raw = _cell_text(cells[1])
            quantity_unit = None if unit_raw in ("", "-", "—") else unit_raw
        else:
            quantity_unit = None

        try:
            nums = [_parse_number(cells[i].get_text()) for i in range(first_num, last_num)]
        except ValueError:
            # Header row whose numeric cells aren't numeric — skip.
            continue

        if meta.is_jan_feb_combined:
            cum_qty, cum_val, prior_qty, prior_val, yoy_qty, yoy_val = nums
            source_row = {
                "raw_label": raw_label,
                "is_aggregate": is_aggregate,
                "quantity_unit": quantity_unit,
                "cumulative_quantity": cum_qty,
                "cumulative_value": cum_val,
                "prior_year_cumulative_quantity": prior_qty,
                "prior_year_cumulative_value": prior_val,
                "published_yoy_quantity_pct": yoy_qty,
                "published_yoy_value_pct": yoy_val,
            }
            kinds = [("cumulative_jan_feb", cum_qty, cum_val)]
        else:
            (m_qty, m_val, ytd_qty, ytd_val,
             prior_qty, prior_val, yoy_qty, yoy_val) = nums
            source_row = {
                "raw_label": raw_label,
                "is_aggregate": is_aggregate,
                "quantity_unit": quantity_unit,
                "monthly_quantity": m_qty,
                "monthly_value": m_val,
                "ytd_quantity": ytd_qty,
                "ytd_value": ytd_val,
                "prior_year_ytd_quantity": prior_qty,
                "prior_year_ytd_value": prior_val,
                "published_yoy_quantity_pct": yoy_qty,
                "published_yoy_value_pct": yoy_val,
            }
            kinds = [("monthly", m_qty, m_val), ("ytd", ytd_qty, ytd_val)]

        if not has_unit_column:
            # Only stamped on the variant so normal rows keep their existing
            # source_row shape byte-for-byte on re-scrape.
            source_row["unit_column_absent"] = True

        for kind, qty, val in kinds:
            if val is None and qty is None:
                continue
            out.append(
                ParsedObservation(
                    section_number=meta.section_number,
                    period=period_iso,
                    period_kind=kind,
                    currency=meta.currency,
                    unit=meta.unit,
                    flow=flow,
                    commodity_label=label,
                    partner_label_raw=raw_label,
                    partner_indent=indent,
                    value=val,
                    quantity=qty,
                    quantity_unit=quantity_unit,
                    source_row=source_row,
                )
            )
    return out


# Reject a section-5/6 parse below this many distinct commodities. The
# catalogue is ~30 rows and stable across 2019–2026 fixtures (may2019: 32,
# janfeb2025: 31, may2026 exports: 31, may2026 imports: 34). Floor well below
# that so a legitimate release never trips, well above a broken/partial parse.
_SECTION56_MIN_COMMODITIES = 20

# A quantity_unit that is wholly numeric — digits, thousands separators and a
# decimal point. Matches the diagnostic used to find the April-2025 corruption
# (source_row->>'quantity_unit' ~ '^[0-9,.]+$').
_NUMERIC_QUANTITY_UNIT_RE = re.compile(r"^[0-9,.]+$")


def section56_floor_check(
    observations: list[ParsedObservation], meta: ReleaseMetadata
) -> str | None:
    """Plausibility floor for a section-5/6 parse — the commodity-catalogue
    analogue of `section4_floor_check` (same reject ⇒ 'failed' ⇒ no release
    row ⇒ walk-retry contract in scrape.py).

    Count invariant only: these pages have no 'Total' grand-total row, and the
    starred aggregates' membership is not adjacency (Hi-tech overlaps
    Mech&elec), so no arithmetic identity is available as a magnitude check —
    by design we never reconstruct their sums.

    Plus two column-alignment invariants. The count floor alone cannot see a
    shift: a page whose columns all move one place still yields ~30 rows of
    plausible-looking numbers. Both signatures below are what the April-2025
    USD exports page produced before `_section56_has_unit_column` handled its
    layout, and both are zero-tolerance — a scan of all 335 stored section-5/6
    releases (~20k observations) found not one legitimate instance of either.
    They stay as the backstop for a drift we have not seen yet, so a future
    variant fails loud instead of persisting shifted values. See
    dev_notes/2026-08-07-gacc-section5-missing-unit-column.md.

    Returns a human-readable reason to reject, or None if plausible.
    Scoped to sections 5/6; returns None for anything else."""
    if meta.section_number not in (5, 6):
        return None

    commodities = {
        o.get("commodity_label") for o in observations if o.get("commodity_label")
    }
    if len(commodities) < _SECTION56_MIN_COMMODITIES:
        return (
            f"only {len(commodities)} commodities parsed (floor "
            f"{_SECTION56_MIN_COMMODITIES}; a healthy section-"
            f"{meta.section_number} release lists ~30) — likely a partial "
            f"parse from column-layout drift or a truncated table"
        )

    # (1) A unit cell holding a number means the label/unit boundary moved: the
    # quantity column has been read as the unit. Units are always prose
    # ('10,000 Tons', 'Ship', '100 Million PCS') or absent.
    numeric_units = sorted({
        u for o in observations
        if (u := o.get("quantity_unit")) and _NUMERIC_QUANTITY_UNIT_RE.match(u)
    })
    if numeric_units:
        return (
            f"quantity_unit holds numbers, not units ({', '.join(numeric_units[:3])}"
            f"{', …' if len(numeric_units) > 3 else ''}) — the quantity column "
            f"was read as the unit column, so every field on those rows is "
            f"shifted (column-layout drift)"
        )

    # (2) The mirror-image signature, and the one that catches the rows guard
    # (1) cannot see: on a value-only commodity the shifted page leaves the unit
    # slot as '-' (→ NULL, no numeric tell) while the value lands in `quantity`
    # and the value itself goes missing. These are value tables — GACC publishes
    # a value for every commodity and quantity only where a unit applies, so
    # quantity-without-value is never legitimate.
    orphan_quantities = sorted({
        o.get("commodity_label") for o in observations
        if o.get("quantity") is not None and o.get("value") is None
    }, key=lambda s: s or "")
    if orphan_quantities:
        return (
            f"{len(orphan_quantities)} commodities carry a quantity but no value "
            f"({', '.join(str(c) for c in orphan_quantities[:3])}"
            f"{', …' if len(orphan_quantities) > 3 else ''}) — the value column "
            f"was read as the quantity column (column-layout drift)"
        )
    return None
