"""Tests for parse.parse_html and metadata extraction against a real release fixture."""

from datetime import date
from pathlib import Path

from bs4 import BeautifulSoup

from parse import extract_metadata, parse_html

FIXTURES = Path(__file__).parent / "fixtures"
SECTION4_FIXTURE = FIXTURES / "release_section4_by_country_mar2026_cny.html"
SECTION4_URL = "http://english.customs.gov.cn/Statics/2e61c8a1-17b2-4074-b909-c039ccf8c8fb.html"


def _load_section4() -> bytes:
    return SECTION4_FIXTURE.read_bytes()


def test_partner_label_collapses_embedded_newlines():
    """Some GACC release pages wrap aggregate names across lines (e.g. 'Regional\\n
    Comprehensive Economic Partnership'). The normaliser must collapse the
    embedded whitespace so the label joins the country_aliases seed cleanly."""
    from parse import _normalise_partner_label

    # Embedded newline + extra spaces between words
    raw = "\xa0\xa0\xa0Regional\n  Comprehensive Economic Partnership"
    label, indent, is_subset = _normalise_partner_label(raw)
    assert label == "Regional Comprehensive Economic Partnership"
    assert indent == 3
    assert is_subset is False


def test_metadata_extracts_unit_from_td_or_span():
    """The Unit annotation lives in either a <span> wrapper or directly in a <td>
    (Aug + Sep 2025 release pages use the latter format)."""
    from bs4 import BeautifulSoup
    from parse import extract_metadata

    span_html = """
    <html><body>
      <div class="atcl-ttl">(4) China's Total Export & Import Values by Country/Region, Mar 2026 (in CNY)</div>
      <div class="atcl-date">2026/04/08</div>
      <table><tr><td><span><b>Unit: CNY 100 Million</b></span></td></tr></table>
    </body></html>
    """
    td_html = """
    <html><body>
      <div class="atcl-ttl">(4) China's Total Export & Import Values by Country/Region, Aug 2025 (in CNY)</div>
      <div class="atcl-date">2025/09/08</div>
      <table><tr><td>Unit: CNY 100 Million</td></tr></table>
    </body></html>
    """
    for html in (span_html, td_html):
        meta = extract_metadata(BeautifulSoup(html, "lxml"), "http://example/x.html")
        assert meta.unit == "CNY 100 Million"


def test_metadata_handles_full_month_name():
    """GACC inconsistently writes 'Mar' or 'March' in release titles. Verify
    extract_metadata accepts both — Mar 2025 and Apr 2025 use full names while
    Feb/Mar 2026 use abbreviations."""
    from parse import _RELEASE_TITLE_RE
    for title in [
        "(4) China's Total Export & Import Values by Country/Region, Mar 2026 (in CNY)",
        "(4) China's Total Export & Import Values by Country/Region, March 2025 (in CNY)",
        "(4) China's Total Export & Import Values by Country/Region, April 2025 (in CNY)",
        "(4) China's Total Export & Import Values by Country/Region, May 2025 (in USD)",
        "(4) China's Total Export & Import Values by Country/Region, September 2024 (in CNY)",
    ]:
        m = _RELEASE_TITLE_RE.match(title)
        assert m, f"failed to match: {title!r}"


def test_metadata_accepts_dd_mm_yyyy_publication_date():
    """2018-era release pages render the publication date as DD/MM/YYYY rather
    than the modern YYYY/MM/DD; both formats must parse so the historical
    backfill captures the correct pub_date instead of dropping it."""
    from bs4 import BeautifulSoup
    from parse import extract_metadata

    html = """
    <html><body>
      <div class="atcl-ttl">China's Total Export & Import Values by Country/Region, July 2018 (in CNY)</div>
      <div class="atcl-date">08/08/2018</div>
    </body></html>
    """
    meta = extract_metadata(BeautifulSoup(html, "lxml"), "http://example/x.html")
    assert meta.publication_date == date(2018, 8, 8)


def test_metadata_handles_2018_historical_formats():
    """Historical (2018) release titles diverge from the current format in two ways:
    (1) the leading "(N)" section prefix is omitted — section must be inferred from
    the description; (2) currency may appear as "RMB" instead of "CNY"; (3) some
    monthly releases use "(Only August, in CNY)" parenthetical instead of the
    bare "(in CNY)". The parser must accept all three so 2018 section-4 releases
    backfill cleanly."""
    from bs4 import BeautifulSoup
    from parse import extract_metadata

    cases = [
        # No (N) prefix → section inferred as 4 from "by Country/Region"
        (
            "China's Total Export & Import Values by Country/Region, July 2018 (in CNY)",
            4, "CNY", date(2018, 7, 1),
        ),
        # RMB normalised to CNY
        (
            "China's Total Export & Import Values by Country/Region, June 2018 (in RMB)",
            4, "CNY", date(2018, 6, 1),
        ),
        # (N) prefix present + "Only August" parenthetical
        (
            "(2) China's Total Export & Import Values by Trade Mode, August 2018 (Only August, in CNY)",
            2, "CNY", date(2018, 8, 1),
        ),
    ]
    for title, expected_section, expected_ccy, expected_period in cases:
        html = f"""
        <html><body>
          <div class="atcl-ttl">{title}</div>
          <div class="atcl-date">2018/09/08</div>
        </body></html>
        """
        meta = extract_metadata(BeautifulSoup(html, "lxml"), "http://example/x.html")
        assert meta.section_number == expected_section, f"{title!r}: section"
        assert meta.currency == expected_ccy, f"{title!r}: currency"
        assert meta.period == expected_period, f"{title!r}: period"


def test_metadata_extraction():
    soup = BeautifulSoup(_load_section4(), "lxml")
    meta = extract_metadata(soup, SECTION4_URL)

    assert meta.section_number == 4
    assert meta.description.startswith("China's Total Export & Import Values by Country/Region")
    assert meta.period == date(2026, 3, 1)
    assert meta.currency == "CNY"
    assert meta.publication_date == date(2026, 4, 8)
    assert meta.unit == "CNY 100 Million"
    assert meta.excel_url is not None
    assert meta.excel_url.endswith(".xls")
    assert "/" in meta.excel_url and "\\" not in meta.excel_url  # backslashes normalised


def test_parses_total_row():
    obs = parse_html(_load_section4(), SECTION4_URL).observations
    totals = [o for o in obs if o["partner_country"] == "Total"]
    # Expect 6 observations: 3 flows × (monthly + ytd).
    assert len(totals) == 6

    monthly_total = next(o for o in totals if o["flow"] == "total" and o["period_kind"] == "monthly")
    assert monthly_total["value"] == 41046.4
    assert monthly_total["currency"] == "CNY"
    assert monthly_total["unit"] == "CNY 100 Million"
    assert monthly_total["period"] == "2026-03-01"

    ytd_export = next(o for o in totals if o["flow"] == "export" and o["period_kind"] == "ytd")
    assert ytd_export["value"] == 68466.7


def test_parses_united_states_row():
    obs = parse_html(_load_section4(), SECTION4_URL).observations
    us_rows = [o for o in obs if o["partner_country"] == "United States (US)"]
    assert len(us_rows) == 6

    us_monthly_export = next(o for o in us_rows if o["flow"] == "export" and o["period_kind"] == "monthly")
    assert us_monthly_export["value"] == 2045.0


def test_subset_marker_is_extracted():
    obs = parse_html(_load_section4(), SECTION4_URL).observations
    germany = [o for o in obs if o["partner_country"] == "Germany"]
    assert germany, "Germany row should be parsed"
    assert all(o["partner_is_subset"] is True for o in germany)
    # Indent should be deeper than the EU parent.
    eu = [o for o in obs if o["partner_country"] == "European Union"]
    assert eu
    assert germany[0]["partner_indent"] > eu[0]["partner_indent"]


def test_observation_count_is_consistent():
    obs = parse_html(_load_section4(), SECTION4_URL).observations
    # Each data row produces 6 observations. The fixture has 25 partner rows
    # (Total + EU + Germany + Netherlands + France + Italy + US + ASEAN +
    #  Vietnam + Malaysia + Thailand + Singapore + Indonesia + Philippines +
    #  Japan + Hong Kong + ROK + Taiwan + Australia + Russia + India + UK +
    #  Canada + NZ + Latin America + Brazil + Africa + South Africa + RCEP +
    #  Belt and Road) — 30 actually.
    assert len(obs) == 30 * 6
