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
    obs = parse_html(_load_section4(), SECTION4_URL)
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
    obs = parse_html(_load_section4(), SECTION4_URL)
    us_rows = [o for o in obs if o["partner_country"] == "United States (US)"]
    assert len(us_rows) == 6

    us_monthly_export = next(o for o in us_rows if o["flow"] == "export" and o["period_kind"] == "monthly")
    assert us_monthly_export["value"] == 2045.0


def test_subset_marker_is_extracted():
    obs = parse_html(_load_section4(), SECTION4_URL)
    germany = [o for o in obs if o["partner_country"] == "Germany"]
    assert germany, "Germany row should be parsed"
    assert all(o["partner_is_subset"] is True for o in germany)
    # Indent should be deeper than the EU parent.
    eu = [o for o in obs if o["partner_country"] == "European Union"]
    assert eu
    assert germany[0]["partner_indent"] > eu[0]["partner_indent"]


def test_observation_count_is_consistent():
    obs = parse_html(_load_section4(), SECTION4_URL)
    # Each data row produces 6 observations. The fixture has 25 partner rows
    # (Total + EU + Germany + Netherlands + France + Italy + US + ASEAN +
    #  Vietnam + Malaysia + Thailand + Singapore + Indonesia + Philippines +
    #  Japan + Hong Kong + ROK + Taiwan + Australia + Russia + India + UK +
    #  Canada + NZ + Latin America + Brazil + Africa + South Africa + RCEP +
    #  Belt and Road) — 30 actually.
    assert len(obs) == 30 * 6
