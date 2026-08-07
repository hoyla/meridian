"""Tests for gacc_cn's CN-side discovery — the 统计快讯 index walk.

Fixtures are real pages from the Statistics Department subdomain
(tjs.customs.gov.cn), captured live on 2026-08-07 — drop day for the July
2026 Express release, ~4h after it published (10:30 Beijing) and while the
English site still topped out at June:

- cn_express_index_tjs_aug2026.html — the Express index
  (/tjs/sjgb/tjkx/index.html): ten July tables + pagination + the site nav,
  including right-rail www article links (微博/微信) that match the article
  URL pattern but must be rejected by title classification.
- cn_express_article_tjs_jul2026_s4_cny.html — the redesigned article page
  for (4) July CNY: no attachDir xls link, table embedded in a WAF-gated WPS
  web-office iframe (the published-awaiting-bytes shape).

See dev_notes/2026-08-07-gacc-cn-discovery-wiring.md for the recon these
shapes come from.
"""
from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path

import psycopg2
import pytest

import api_client
import db
import gacc_cn

FIXTURES = Path(__file__).parent / "fixtures"
INDEX_HTML = (FIXTURES / "cn_express_index_tjs_aug2026.html").read_bytes()
ARTICLE_HTML = (FIXTURES / "cn_express_article_tjs_jul2026_s4_cny.html").read_bytes()
JUN_CNY_XLS = (FIXTURES / "release_cn_section4_by_country_jun2026_cny.xls").read_bytes()

INDEX_URL = gacc_cn.CN_EXPRESS_INDEX_URL


def _fetch_result(url: str, content: bytes,
                  content_type: str = "text/html") -> api_client.FetchResult:
    return api_client.FetchResult(
        url=url, status_code=200, content_type=content_type, content=content,
        sha256=hashlib.sha256(content).hexdigest())


# ---------------------------------------------------------------------------
# Index parsing
# ---------------------------------------------------------------------------

def test_index_recognises_the_six_ingestable_tables():
    articles = gacc_cn.discover_express_articles(INDEX_HTML, INDEX_URL)
    assert len(articles) == 6
    cells = {(a.section, a.currency) for a in articles}
    assert cells == {(4, "CNY"), (4, "USD"), (5, "CNY"), (5, "USD"),
                     (6, "CNY"), (6, "USD")}
    for a in articles:
        assert a.period == date(2026, 7, 1)
        assert a.published == date(2026, 8, 7)
        assert a.is_jan_feb_combined is False
        assert a.url.startswith("http://tjs.customs.gov.cn/tjs/2026-08/07/article_")


def test_index_skips_trade_mode_tables_and_nav_article_links():
    # The July drop also lists (2)/(3) trade-mode tables (including the
    # cumulative 1至7月 variants) and the page carries www article links in
    # the right rail (微博/微信) — none of these are tables we ingest, and the
    # cumulative months form must not confuse the period parser.
    articles = gacc_cn.discover_express_articles(INDEX_HTML, INDEX_URL)
    assert all(a.section in (4, 5, 6) for a in articles)
    assert all("2025-12" not in a.url for a in articles)


def test_index_title_jan_feb_combined_flagged_and_anchored_on_february():
    html = ('<ul class="news_list"><li>'
            '<a href="/tjs/2027-03/10/article_2027031010000000001.html" '
            'title="（4）2027年1至2月进出口商品主要国别（地区）总值表（人民币值）">'
            '（4）2027年1至2月进出口商品主要国别（地区）总值表（人民币值）</a>'
            '<span>2027-03-10</span></li></ul>').encode()
    (a,) = gacc_cn.discover_express_articles(html, INDEX_URL)
    assert a.is_jan_feb_combined is True
    assert a.period == date(2027, 2, 1)   # same anchor as the xls parser


def test_index_title_2025_era_without_currency_suffix_classifies_with_none():
    html = ('<ul class="news_list"><li>'
            '<a href="/customs/2025-06/09/article_2026012219104957632.html" '
            'title="（6）2025年5月进出口商品主要国别（地区）总值表">'
            '（6）2025年5月进出口商品主要国别（地区）总值表</a></li></ul>').encode()
    (a,) = gacc_cn.discover_express_articles(html, "http://www.customs.gov.cn/")
    assert a.section == 4        # keyword-classified, never the printed （6）
    assert a.currency is None
    assert a.period == date(2025, 5, 1)
    assert a.published == date(2025, 6, 9)   # from the URL path date


# ---------------------------------------------------------------------------
# Article attachment discovery
# ---------------------------------------------------------------------------

def test_redesigned_article_has_wps_viewer_and_no_xls():
    xls_urls, uses_wps = gacc_cn.discover_article_attachments(
        ARTICLE_HTML,
        "http://tjs.customs.gov.cn/tjs/2026-08/07/article_2026080710320733327.html")
    assert xls_urls == []
    assert uses_wps is True


def test_old_style_article_yields_direct_xls_url():
    html = ('<div class="atcl"><a href="/customs/attachDir/2026/07/'
            '2026071409284388537.xls">下载</a></div>').encode()
    xls_urls, uses_wps = gacc_cn.discover_article_attachments(
        html,
        "http://www.customs.gov.cn/customs/2026-07/14/article_2026071409284366427.html")
    assert xls_urls == [
        "http://www.customs.gov.cn/customs/attachDir/2026/07/2026071409284388537.xls"]
    assert uses_wps is False


# ---------------------------------------------------------------------------
# probe_cn_express orchestration
# ---------------------------------------------------------------------------

def test_probe_unavailable_when_index_unreachable(monkeypatch):
    def _boom(url, **kwargs):
        raise ConnectionError("no route to host")
    monkeypatch.setattr(api_client, "fetch", _boom)
    outcome = gacc_cn.probe_cn_express()
    assert outcome.status == "unavailable"
    assert "no route" in outcome.error


def test_probe_reports_published_awaiting_bytes(clean_db, monkeypatch):
    # Drop morning: all six July tables are new (empty DB) but every article
    # is the redesigned WPS shape — the probe must say the release is out
    # upstream rather than reading as a quiet day.
    def _fetch(url, **kwargs):
        if url == INDEX_URL:
            return _fetch_result(url, INDEX_HTML)
        return _fetch_result(url, ARTICLE_HTML)
    monkeypatch.setattr(api_client, "fetch", _fetch)

    outcome = gacc_cn.probe_cn_express()
    assert outcome.status == "published_awaiting_bytes"
    assert outcome.ingested == 0
    assert len(outcome.pending) == 6
    assert {a.period for a in outcome.pending} == {date(2026, 7, 1)}


def test_probe_no_new_fetches_only_the_index(clean_db, test_db_url, monkeypatch):
    # Every table cell already has a release row (the English walk got there
    # first): the probe must stop at one index fetch — no article churn.
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        for section in (4, 5, 6):
            for currency in ("CNY", "USD"):
                cur.execute(
                    "INSERT INTO releases (source, source_url, period, "
                    "section_number, currency, release_kind) "
                    "VALUES ('gacc', %s, %s, %s, %s, 'preliminary')",
                    (f"http://english.example/{section}{currency}",
                     date(2026, 7, 1), section, currency))
        conn.commit()

    fetched = []

    def _fetch(url, **kwargs):
        fetched.append(url)
        return _fetch_result(url, INDEX_HTML)
    monkeypatch.setattr(api_client, "fetch", _fetch)

    outcome = gacc_cn.probe_cn_express()
    assert outcome.status == "no_new"
    assert fetched == [INDEX_URL]


def test_probe_ingests_direct_xls_end_to_end(clean_db, test_db_url, monkeypatch):
    # An article that still carries a direct attachDir xls link (the
    # pre-redesign shape) must flow all the way through the existing CN
    # ingest: snapshot, parse, floor check, release + observations.
    article_url = ("http://www.customs.gov.cn/customs/2026-07/14/"
                   "article_2026071409284366427.html")
    xls_url = ("http://www.customs.gov.cn/customs/attachDir/2026/07/"
               "2026071409284388537.xls")
    index_html = (
        '<ul class="news_list"><li>'
        f'<a href="{article_url}" '
        'title="（4）2026年6月进出口商品主要国别（地区）总值表（人民币值）">'
        '（4）2026年6月进出口商品主要国别（地区）总值表（人民币值）</a>'
        '<span>2026-07-14</span></li></ul>').encode()
    article_html = f'<a href="{xls_url}">下载</a>'.encode()

    def _fetch(url, **kwargs):
        if url == INDEX_URL:
            return _fetch_result(url, index_html)
        if url == article_url:
            return _fetch_result(url, article_html)
        assert url == xls_url
        return _fetch_result(url, JUN_CNY_XLS, "application/vnd.ms-excel")
    monkeypatch.setattr(api_client, "fetch", _fetch)

    outcome = gacc_cn.probe_cn_express()
    assert outcome.status == "ingested"
    assert outcome.ingested == 1
    assert outcome.pending == []

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT source_url FROM releases WHERE source = 'gacc' "
            "AND section_number = 4 AND currency = 'CNY' AND period = %s",
            (date(2026, 6, 1),))
        (source_url,) = cur.fetchone()
        assert source_url == article_url
        cur.execute("SELECT COUNT(*) FROM observations")
        (n_obs,) = cur.fetchone()
        assert n_obs > 0

    # Idempotency: the next walk sees the release row and stops at the index.
    fetched = []

    def _fetch_again(url, **kwargs):
        fetched.append(url)
        return _fetch_result(url, index_html)
    monkeypatch.setattr(api_client, "fetch", _fetch_again)
    outcome2 = gacc_cn.probe_cn_express()
    assert outcome2.status == "no_new"
    assert fetched == [INDEX_URL]
