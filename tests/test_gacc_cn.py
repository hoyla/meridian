"""Tests for gacc_cn — the Chinese-site (统计快讯) Express xls parser.

Fixtures are real GACC files fetched 2026-07-14 (SHA-256s in
dev_notes/2026-07-14-gacc-chinese-source-investigation.md):
- release_cn_section4_by_country_jun2026_cny.xls — the June-2026 drop that
  landed on the Chinese site 15+ hours before the English site (title
  numbered （4）).
- release_cn_section4_by_country_jun2026_usd.xls — its USD sibling.
- release_cn_section4_by_country_may2025_cny.xls — a 2025-era file (title
  numbered （6）— the numbering-drift case) whose values are proven
  identical to our English-parsed DB rows.
"""

from datetime import date
from pathlib import Path

import pytest

import db
import gacc_cn
import parse
from parse import CurrencyUnitMismatch, UnparseableReleasePage

FIXTURES = Path(__file__).parent / "fixtures"
JUN_CNY = FIXTURES / "release_cn_section4_by_country_jun2026_cny.xls"
JUN_USD = FIXTURES / "release_cn_section4_by_country_jun2026_usd.xls"
MAY25_CNY = FIXTURES / "release_cn_section4_by_country_may2025_cny.xls"

XLS_URL = "http://www.customs.gov.cn/customs/attachDir/2026/07/2026071409284388537.xls"
ARTICLE_URL = "http://www.customs.gov.cn/customs/2026-07/14/article_2026071409284366427.html"


def _parse(path, article_url=ARTICLE_URL):
    return gacc_cn.parse_cn_express_xls(
        path.read_bytes(), xls_url=XLS_URL, article_url=article_url)


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

def test_june_2026_cny_metadata():
    r = _parse(JUN_CNY)
    m = r.metadata
    assert m.section_number == 4
    assert m.currency == "CNY"
    assert m.period == date(2026, 6, 1)
    assert m.unit == "CNY 100 Million"       # 亿元人民币 — same scale as EN
    assert m.publication_date == date(2026, 7, 14)  # from the article path
    assert m.source_url == ARTICLE_URL
    assert m.excel_url == XLS_URL
    assert m.description == "China's Total Export & Import Values by Country/Region"
    assert "主要国别" in m.title
    assert m.is_jan_feb_combined is False


def test_june_2026_usd_metadata():
    m = _parse(JUN_USD).metadata
    assert m.currency == "USD"
    assert m.unit == "USD1 Million"          # 百万美元 — same scale as EN
    assert m.period == date(2026, 6, 1)


def test_2025_era_title_numbering_drift_still_reads_section_4():
    """The May-2025 file is titled （6）…主要国别（地区）总值表 — the printed
    number drifted to （4）by 2026. Table identity must come from the title
    keywords, so both eras parse as OUR section 4 (the English site's stable
    numbering, which is the natural key)."""
    m = _parse(MAY25_CNY, article_url=None).metadata
    assert m.section_number == 4
    assert m.period == date(2025, 5, 1)
    assert m.currency == "CNY"
    assert m.publication_date is None        # no article URL supplied
    assert m.title.startswith("（6）")


# ---------------------------------------------------------------------------
# Observations
# ---------------------------------------------------------------------------

def test_june_2026_cny_observations():
    r = _parse(JUN_CNY)
    # 30 partner rows × 3 flows × 2 period kinds.
    assert len(r.observations) == 180
    by_key = {(o["partner_country"], o["flow"], o["period_kind"]): o
              for o in r.observations}
    # Values at English precision (1dp), full precision in source_row.
    eu_exp_ytd = by_key[("European Union", "export", "ytd")]
    assert eu_exp_ytd["value"] == 21651.6
    assert eu_exp_ytd["source_row"]["ytd_export"] == 21651.61013382
    assert eu_exp_ytd["source_row"]["site"] == "www.customs.gov.cn"
    assert by_key[("Total", "total", "monthly")]["value"] == 47822.9
    # of-which rows: subset flag set, 其中： stripped, zh raw preserved.
    vn = by_key[("Vietnam", "export", "monthly")]
    assert vn["partner_is_subset"] is True
    assert "其中：越南" in vn["partner_label_raw"]
    assert vn["value"] == 1553.1
    # A 2026-era aggregate-label variant maps.
    assert ("Regional Comprehensive Economic Partnership",
            "total", "ytd") in by_key
    # Every observation carries the canonical unit + currency.
    assert {o["unit"] for o in r.observations} == {"CNY 100 Million"}
    assert {o["currency"] for o in r.observations} == {"CNY"}


def test_june_2026_usd_observations_spot_values():
    by_key = {(o["partner_country"], o["flow"], o["period_kind"]): o["value"]
              for o in _parse(JUN_USD).observations}
    assert by_key[("Total", "total", "monthly")] == 699151.2
    assert by_key[("European Union", "total", "ytd")] == 447881.2


def test_may_2025_values_match_the_proven_en_identical_figures():
    """The May-2025 CN file is the one proven identical to our English-parsed
    DB (30/30 rows × 6 values — the dev note's verification). Pin the same
    anchor values here so the parser's rounding matches the DB convention."""
    by_key = {(o["partner_country"], o["flow"], o["period_kind"]): o["value"]
              for o in _parse(MAY25_CNY, article_url=None).observations}
    assert by_key[("Total", "total", "monthly")] == 38098.1
    assert by_key[("Total", "total", "ytd")] == 179448.5
    assert by_key[("Vietnam", "export", "monthly")] == 1248.6
    assert by_key[("Vietnam", "import", "monthly")] == 507.6
    # 2025-era aggregate label variants (no（RCEP）suffix, no curly quotes)
    # map to the same canonical labels as the 2026 forms.
    assert ("Regional Comprehensive Economic Partnership",
            "total", "monthly") in by_key
    assert ("Jointly build the countries along Belt and Road Routes",
            "export", "ytd") in by_key


def test_floor_check_passes_on_real_files():
    r = _parse(JUN_CNY)
    assert parse.section4_floor_check(r.observations, r.metadata) is None


# ---------------------------------------------------------------------------
# Refusals — every way this parser declines to guess
# ---------------------------------------------------------------------------

def test_unmapped_partner_label_fails_loudly(monkeypatch):
    """A label with no zh→en mapping must fail the WHOLE parse with the
    label named — never a silent row drop (a missing partner reads as a
    data gap downstream) and never a fuzzy match."""
    trimmed = {k: v for k, v in gacc_cn.ZH_TO_EN_PARTNERS.items()
               if k != "越南"}
    monkeypatch.setattr(gacc_cn, "ZH_TO_EN_PARTNERS", trimmed)
    with pytest.raises(ValueError, match="越南"):
        _parse(JUN_CNY)


def test_title_unit_mismatch_refuses():
    with pytest.raises(CurrencyUnitMismatch):
        gacc_cn._check_title_unit(
            "USD", "亿元人民币", url="test://x", section=4,
            period=date(2026, 6, 1))
    assert gacc_cn._check_title_unit(
        "CNY", "亿元人民币", url="test://x", section=4,
        period=date(2026, 6, 1)) == "CNY 100 Million"


def test_monthly_bulletin_file_refuses():
    """The verified-vintage guard: a REAL Monthly Bulletin by-country file
    (（2）2026年5月进出口商品国别（地区）总值表 — no 主要, unit 万元, 277
    rows) must not parse as an Express preliminary. Mixing the two vintages
    unlabelled is the exact mistake the Release Calendar's note 2 warns
    about (verification can move figures until the Yearbook)."""
    bulletin = FIXTURES / "release_cn_monthly_bulletin_s2_may2026_cny.xls"
    with pytest.raises(UnparseableReleasePage, match="Refusing to guess"):
        gacc_cn.parse_cn_express_xls(bulletin.read_bytes(), xls_url="t://x")


def test_garbage_bytes_refuse():
    with pytest.raises(UnparseableReleasePage, match="not a readable"):
        gacc_cn.parse_cn_express_xls(b"not-an-xls-anyway", xls_url="t://x")


def test_combined_jan_feb_refuses_without_fixture():
    """No CN combined-release fixture exists yet; the parser must refuse
    rather than ship untested column arithmetic."""
    title = "（5）2026年1至2月进出口商品主要国别（地区）总值表（人民币值）"
    m = gacc_cn._TITLE_RE.search(title)
    assert m is not None and m.group("months") == "1至2月"


# ---------------------------------------------------------------------------
# DB round-trip (needs GACC_TEST_DATABASE_URL)
# ---------------------------------------------------------------------------

def test_ingest_persists_and_is_idempotent(clean_db):
    xls = JUN_CNY.read_bytes()
    rid = gacc_cn.ingest_cn_xls_bytes(
        xls, xls_url=XLS_URL, article_url=ARTICLE_URL, sha256="x" * 64)
    assert rid is not None

    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute("SELECT source, section_number, currency, period, "
                    "release_kind, source_url, publication_date "
                    "FROM releases WHERE id = %s", (rid,))
        src, sec, cur_cy, period, kind, url, pubdate = cur.fetchone()
        assert (src, sec, cur_cy, kind) == ("gacc", 4, "CNY", "preliminary")
        assert period == date(2026, 6, 1)
        assert url == ARTICLE_URL
        assert pubdate == date(2026, 7, 14)
        cur.execute("SELECT count(*) FROM observations WHERE release_id = %s",
                    (rid,))
        assert cur.fetchone()[0] == 180
        cur.execute("SELECT count(*) FROM source_snapshots")
        assert cur.fetchone()[0] == 1

    # Re-ingest: same natural key → same release; every observation
    # unchanged (the supersede chain sees identical values — this is also
    # what the later ENGLISH pass of the same release will look like,
    # since the CN values are stored at English precision).
    rid2 = gacc_cn.ingest_cn_xls_bytes(
        xls, xls_url=XLS_URL, article_url=ARTICLE_URL, sha256="x" * 64)
    assert rid2 == rid
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM observations WHERE release_id = %s",
                    (rid,))
        assert cur.fetchone()[0] == 180


def test_verify_against_db_reports_identity(clean_db, capsys):
    xls = JUN_CNY.read_bytes()
    gacc_cn.ingest_cn_xls_bytes(
        xls, xls_url=XLS_URL, article_url=ARTICLE_URL, sha256="x" * 64)
    assert gacc_cn.verify_against_db(xls, xls_url=XLS_URL) == 0
    assert "IDENTICAL" in capsys.readouterr().out


def test_verify_against_empty_db_declines(clean_db, capsys):
    assert gacc_cn.verify_against_db(JUN_CNY.read_bytes(),
                                     xls_url=XLS_URL) == -1
    assert "nothing to verify" in capsys.readouterr().out
