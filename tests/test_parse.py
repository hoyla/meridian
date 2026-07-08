"""Tests for parse.parse_html and metadata extraction against a real release fixture."""

from datetime import date
from pathlib import Path

import pytest
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


def test_metadata_trusts_description_over_glitchy_section_prefix():
    """GACC mis-numbered the 2020 combined Jan-Feb 'by Country/Region' release as
    (3) instead of (4). The description is unambiguous, so it must win over the
    glitchy prefix — otherwise the by-country data is skipped (section 4 is the
    only section the parser ingests)."""
    from bs4 import BeautifulSoup
    from parse import extract_metadata

    title = ("(3) China's Total Export & Import Values by Country/Region, "
             "January-February 2020 (in CNY)")
    html = (f'<html><body><div class="atcl-ttl">{title}</div>'
            f'<div class="atcl-date">2020/03/07</div></body></html>')
    meta = extract_metadata(BeautifulSoup(html, "lxml"), "http://example/x.html")
    assert meta.section_number == 4            # description wins over the (3) prefix
    assert meta.is_jan_feb_combined is True
    assert meta.period == date(2020, 2, 1)

    # A non-section-4 prefix that AGREES with its description is left untouched.
    t2 = "(2) China's Total Export & Import Values by Trade Mode, March 2020 (in CNY)"
    h2 = (f'<html><body><div class="atcl-ttl">{t2}</div>'
          f'<div class="atcl-date">2020/04/07</div></body></html>')
    m2 = extract_metadata(BeautifulSoup(h2, "lxml"), "http://example/y.html")
    assert m2.section_number == 2


def test_metadata_handles_2018_section4_alternate_wording():
    """Phase 6.7: a deeper class of 2018 historical format. Section-4 release
    pages from early 2018 use:
    (a) entirely different description wording: 'Total Value of Imports and
        Exports by Major Country (Region)' vs the modern
        'Total Export & Import Values by Country/Region';
    (b) month abbreviation with a trailing period ('Jan.' not 'Jan');
    (c) NO '(in CCY)' suffix on the page title at all (the currency tag
        lives only on the parent index page's bulletin row).

    The parser must (1) match on the alternative wording, (2) accept the
    trailing period after the month, (3) accept the absent currency suffix
    when the caller has supplied an `expected_currency` from the
    DiscoveredRelease metadata. Section is inferred as 4 from the
    'by Major Country' wording.
    """
    from bs4 import BeautifulSoup
    from parse import extract_metadata

    title = "China's Total Value of Imports and Exports by Major Country (Region), Jan. 2018"
    html = f"""
    <html><body>
      <div class="atcl-ttl">{title}</div>
      <div class="atcl-date">03/08/2018</div>
    </body></html>
    """
    meta = extract_metadata(
        BeautifulSoup(html, "lxml"), "http://example/x.html",
        expected_currency="CNY",
    )
    assert meta.section_number == 4
    assert meta.currency == "CNY"
    assert meta.period == date(2018, 1, 1)


def test_metadata_handles_2018_section4_no_date_in_title():
    """Phase 6.7 (extension): some 2018 section-4 release pages reuse the
    bulletin-row title verbatim with no date in the page title at all
    (Jul 2018 CNY: 'China's Total Export & Import Values by Country/Region
    (in CNY)'). The parser falls back to the discovery-supplied
    `expected_period` in that case."""
    from bs4 import BeautifulSoup
    from parse import extract_metadata

    title = "China's Total Export & Import Values by Country/Region (in CNY)"
    html = f"""
    <html><body>
      <div class="atcl-ttl">{title}</div>
      <div class="atcl-date">08/08/2018</div>
    </body></html>
    """
    meta = extract_metadata(
        BeautifulSoup(html, "lxml"), "http://example/x.html",
        expected_currency="CNY",
        expected_period=date(2018, 7, 1),
    )
    assert meta.section_number == 4
    assert meta.currency == "CNY"
    assert meta.period == date(2018, 7, 1)


def test_metadata_no_date_title_fails_without_expected_period():
    """Without `expected_period` the no-date title can't be resolved — the
    parser raises a clear error rather than silently making up a date."""
    from bs4 import BeautifulSoup
    from parse import extract_metadata

    title = "China's Total Export & Import Values by Country/Region (in CNY)"
    html = f'<html><body><div class="atcl-ttl">{title}</div></body></html>'
    try:
        extract_metadata(
            BeautifulSoup(html, "lxml"), "http://example/x.html",
            expected_currency="CNY",
        )
    except ValueError as e:
        assert "omits date" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_metadata_2018_section4_against_real_fixture():
    """Round-trip the actual archived 2018 release page (saved as a fixture
    under tests/fixtures/) — proves the parser handles the real HTML, not
    just a hand-crafted title string."""
    from bs4 import BeautifulSoup
    from parse import extract_metadata

    fixture = FIXTURES / "release_section4_by_country_jan2018_cny.html"
    soup = BeautifulSoup(fixture.read_bytes(), "lxml")
    meta = extract_metadata(
        soup, "http://english.customs.gov.cn/Statics/851cff3d-297f-4cf3-a500-5241199cc957.html",
        expected_currency="CNY",
    )
    assert meta.section_number == 4
    assert meta.currency == "CNY"
    assert meta.period == date(2018, 1, 1)
    assert meta.publication_date == date(2018, 8, 3)


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
    assert meta.is_jan_feb_combined is False


JANFEB_FIXTURE = FIXTURES / "release_section4_by_country_janfeb2025_cny.html"
JANFEB_URL = "http://english.customs.gov.cn/Statics/2ab97956-c57d-4387-8d60-a141b5a5b48b.html"


def test_metadata_recognises_jan_feb_combined_release():
    """2025-Feb's release on the GACC English site is a combined
    January-February cumulative release (Chinese New Year shape). The
    title reads 'China's Total Export & Import Values by Country/Region,
    January-February 2025 (in CNY)' — parser must:
      - match the title via `_RELEASE_TITLE_JAN_FEB_RE`
      - set `is_jan_feb_combined=True`
      - anchor `period` at Feb 1 of the year (latest month covered)."""
    soup = BeautifulSoup(JANFEB_FIXTURE.read_bytes(), "lxml")
    meta = extract_metadata(soup, JANFEB_URL)
    assert meta.section_number == 4
    assert meta.currency == "CNY"
    assert meta.period == date(2025, 2, 1)
    assert meta.is_jan_feb_combined is True


def test_jan_feb_body_emits_cumulative_period_kind():
    """For the combined Jan-Feb release, the body parser must emit ONE
    observation per partner × flow with period_kind='cumulative_jan_feb'
    rather than the usual two ('monthly' + 'ytd'). The combined release
    publishes Jan + Feb's cumulative in both columns; emitting both as
    monthly + ytd would double-count it when the windowed analyser sums
    per-period totals.

    Editorial guarantee: the cumulative value is NOT split 50/50 across
    January and February — interpolation would invent per-month figures
    the source never asserted."""
    obs = parse_html(JANFEB_FIXTURE.read_bytes(), JANFEB_URL).observations
    de_obs = [o for o in obs if o["partner_country"] == "Germany"]
    # 3 flows × 1 kind = 3 observations per partner.
    assert len(de_obs) == 3, f"expected 3 Germany rows, got {len(de_obs)}"
    kinds = {o["period_kind"] for o in de_obs}
    assert kinds == {"cumulative_jan_feb"}
    flows = {o["flow"] for o in de_obs}
    assert flows == {"total", "export", "import"}
    # Every cell is the Jan+Feb cumulative — sanity-check the export cell.
    exp = next(o for o in de_obs if o["flow"] == "export")
    assert exp["value"] is not None and exp["value"] > 0


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


# --- currency/unit floor scoping (2026-07-08 false-positive fix) ------------
# The `_CANONICAL_GACC_UNIT` floor in extract_metadata guards section 4 — the
# only section we ingest. Section-1 "Total Values" pages legitimately carry
# `Unit: USD 100 Million` (亿美元), which agrees with the title on currency but
# differs on scale. Before the fix they tripped the floor's "self-inconsistent"
# ValueError; now they fall through to the no_parser path unchanged. See
# dev_notes/2026-07-08-gacc-section1-usd-floor-false-positive.md.

_SECTION1_USD_HTML = """
<html><body>
  <div class="atcl-ttl">(1) China's Total Export &amp; Import Values, May 2026 (in USD)</div>
  <div class="atcl-date">2026/06/11</div>
  <table><tr><td><span>Unit: USD 100 Million</span></td></tr></table>
</body></html>
"""


def test_section1_usd_100_million_metadata_is_not_floored():
    """The real d7718500 shape: a section-1 USD headline-totals page whose Unit
    row reads 'USD 100 Million'. The floor must NOT fire — title and Unit agree
    on currency (only the scale differs), and section 1 is never ingested. The
    raw unit passes through untouched (no canonical coercion off section 4)."""
    meta = extract_metadata(
        BeautifulSoup(_SECTION1_USD_HTML, "lxml"),
        "http://english.customs.gov.cn/Statics/d7718500-x.html",
    )
    assert meta.section_number == 1
    assert meta.currency == "USD"
    assert meta.unit == "USD 100 Million"  # left as-is, not coerced to 'USD1 Million'


def test_section1_usd_page_lands_no_parser_not_floor_error():
    """End-to-end: the page must reach the section-dispatch NotImplementedError
    (recorded as status='no_parser', terminal) rather than a floor ValueError
    (status='failed', retried every walk — the twice-daily false alarm)."""
    with pytest.raises(NotImplementedError):
        parse_html(
            _SECTION1_USD_HTML.encode(),
            "http://english.customs.gov.cn/Statics/d7718500-x.html",
        )


def test_section4_currency_unit_mismatch_still_raises():
    """Release-184 protection preserved: a section-4 by-country page whose title
    declares CNY but whose Unit row reads 'USD1 Million' is a genuine
    currency/scale disagreement on data we DO ingest — the floor must still
    raise so the URL lands status='failed' with no observations inserted."""
    html = """
    <html><body>
      <div class="atcl-ttl">(4) China's Total Export &amp; Import Values by Country/Region, May 2026 (in CNY)</div>
      <div class="atcl-date">2026/06/11</div>
      <table><tr><td><span>Unit: USD1 Million</span></td></tr></table>
    </body></html>
    """
    with pytest.raises(ValueError, match="self-inconsistent"):
        extract_metadata(
            BeautifulSoup(html, "lxml"),
            "http://english.customs.gov.cn/Statics/synthetic-184.html",
        )


def test_section4_currency_unit_mismatch_carries_period_and_currency():
    """CurrencyUnitMismatch must carry the resolved (section, period, currency)
    so scrape_release can check for a superseding live sibling. It stays a
    ValueError subclass for callers matching on that."""
    from parse import CurrencyUnitMismatch

    html = """
    <html><body>
      <div class="atcl-ttl">(4) China's Total Export &amp; Import Values by Country/Region, June 2025 (in CNY)</div>
      <div class="atcl-date">2025/07/14</div>
      <table><tr><td><span>Unit: USD1 Million</span></td></tr></table>
    </body></html>
    """
    with pytest.raises(CurrencyUnitMismatch) as ei:
        extract_metadata(BeautifulSoup(html, "lxml"), "http://x/184.html")
    assert isinstance(ei.value, ValueError)
    assert ei.value.section == 4
    assert ei.value.period == date(2025, 6, 1)
    assert ei.value.currency == "CNY"


# --- unparseable pages land no_parser, not retried 'failed' (2026-07-08) -----
# The held-back GACC alarm fired on a standing backlog of ~32 historical pages
# that re-failed every walk. Pages we can never ingest — a title none of our
# shapes match, or a section-4 page with no HTML table (the 2018 by-country
# shape) — must raise UnparseableReleasePage (a NotImplementedError) so
# scrape_release retires them terminal 'no_parser' rather than 'failed'.


def test_unrecognised_title_raises_unparseable_not_plain_valueerror():
    from parse import UnparseableReleasePage

    # A 2018 annual-totals title (not by-country) matched by none of the shapes.
    html = (
        '<html><body><div class="atcl-ttl">'
        "China's Total Value of Imports and Exports, 2018 (RMB)"
        "</div></body></html>"
    )
    with pytest.raises(UnparseableReleasePage) as ei:
        # expected_period supplied so we get past the no-date guard to the
        # genuine title-unrecognised branch.
        extract_metadata(
            BeautifulSoup(html, "lxml"), "http://x/annual2018.html",
            expected_period=date(2018, 12, 1), expected_currency="CNY",
        )
    assert isinstance(ei.value, NotImplementedError)


def test_section4_page_with_no_table_raises_unparseable():
    from parse import UnparseableReleasePage

    # A section-4 by-country title whose content div carries no table (the
    # 2018 English pages) must be no_parser, not a retried failure.
    html = """
    <html><body>
      <div class="atcl-ttl">(4) China's Total Export &amp; Import Values by Country/Region, June 2018 (in USD)</div>
      <div class="atcl-cnt"><p>No table here.</p></div>
    </body></html>
    """
    with pytest.raises(UnparseableReleasePage):
        parse_html(html.encode(), "http://english.customs.gov.cn/Statics/notable2018.html")
