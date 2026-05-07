"""Tests for api_client.discover_release_urls against the saved seed fixture."""

from pathlib import Path

from api_client import discover_release_urls

FIXTURES = Path(__file__).parent / "fixtures"


def _load_seed() -> bytes:
    return (FIXTURES / "seed_2026.html").read_bytes()


SEED_URL = "http://english.customs.gov.cn/statics/report/preliminary.html"


def test_discovers_known_releases():
    releases = discover_release_urls(_load_seed(), SEED_URL)

    # The 2026 fixture has Feb + Mar published across bulletins (1)..(6) in CNY and USD
    # — that's 6 sections × 2 currencies × 2 months = 24 release links.
    assert len(releases) == 24

    # Spot-check section 4 March CNY.
    s4_mar_cny = [r for r in releases if r["section_number"] == 4 and r["currency"] == "CNY" and r["month"] == 3]
    assert len(s4_mar_cny) == 1
    rel = s4_mar_cny[0]
    assert rel["year"] == 2026
    assert rel["description"].startswith("China's Total Export & Import Values by Country/Region")
    assert rel["url"].startswith("http://english.customs.gov.cn/Statics/")


def test_section_currency_split():
    releases = discover_release_urls(_load_seed(), SEED_URL)
    # Each (section, month) pair appears once per currency.
    pairs = {(r["section_number"], r["currency"], r["month"]) for r in releases}
    # 6 sections × 2 currencies × 2 months
    assert len(pairs) == 24


def test_year_inferred_for_archived_page():
    # Even though the fixture *content* is the current-year preliminary.html, the
    # discovery routine should pick up the year from preliminary{YYYY}.html when
    # called with that URL.
    archived_url = "http://english.customs.gov.cn/statics/report/preliminary2024.html"
    releases = discover_release_urls(_load_seed(), archived_url)
    assert all(r["year"] == 2024 for r in releases)
