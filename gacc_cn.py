"""GACC Chinese-site (统计快讯 / Statistics Express) ingest — Track 1 of the
CN-source build (dev_notes/2026-07-14-gacc-chinese-source-investigation.md).

The Chinese site publishes the SAME preliminary release as the English site
we scrape, hours-to-a-day earlier (observed 2026-07-14: Chinese tables up
02:28 UK, English still absent 15+ hours later). Values are identical —
verified exactly for May-2025 (30/30 partner rows × 6 values) — so a CN
ingest and the later EN ingest are two reads of one release, not two
releases.

This module parses a CN Express **xls attachment** (the CN article pages
carry no HTML table — one Excel-97 file each, GBK code page, fetchable with
plain HTTP: the WAF guards only the HTML) into the existing
release/observation model. Design decisions, each load-bearing:

- **`partner_country` carries the English canonical label** (mapped via
  `ZH_TO_EN_PARTNERS`, derived by exact value-tuple matching against our
  English-parsed data — not guessed). Every analyser, floor check and
  portal surface keys on those labels, so a CN-ingested release is
  indistinguishable downstream. The raw Chinese cell text is preserved in
  `partner_label_raw` and `source_row` (principle 3: store alongside,
  never overwrite).
- **Unmapped labels fail the whole parse loudly.** The partner catalogue is
  stable; a new label is a real event a human should look at, not
  something to fuzzy-match at 2am (never guess — data-rigor rule 1).
- **Values are rounded to 1 decimal** — the English pages' printed
  precision, which is what the DB holds. The CN files carry full decimals
  (kept in `source_row`); rounding at parse time means the later English
  pass reads every observation as `unchanged`, so a `versioned` row keeps
  meaning "GACC actually revised a figure", not "the two sites print
  different precision".
- **Same natural key** (section, currency, period, release_kind
  ='preliminary'): the CN ingest creates the release row; the English walk
  later lands on the same row (find_or_create) and re-verifies the
  observations. `releases.source_url` then flips to the English page — the
  eyeballable one — while the CN provenance stays in `scrape_runs` and
  `source_snapshots` (the xls bytes are snapshotted append-only).
- **Table identity comes from title keywords, never the printed（N）** —
  CN title numbering drifts ((6) in 2025 → (4) in 2026 for the same
  by-country table). 主要国别（地区）总值表 = the Express by-country table;
  the 月报's all-country cousin lacks 主要 and must NOT match (different
  vintage — the verified Monthly Bulletin, revisable until the Yearbook).
- Section 4 (by-country) only for now. The commodity tables (5/6) follow
  once their zh↔en label dictionary is derived the same value-matched way.
  Combined Jan–Feb releases raise until we hold a fixture (no live layout
  to code against — fixtureless parsers are how silent corruption ships).

CLI:
    python gacc_cn.py ingest --xls-url URL --article-url URL [--dry-run]
    python gacc_cn.py verify --xls PATH_OR_URL

`verify` re-parses an xls and diffs it against whatever the DB already
holds for the same (period, currency) — the institutionalised form of the
pre-registered June-2026 diff in the dev note.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import date
from typing import Any

import xlrd

import api_client
import db
import parse
from parse import (
    CurrencyUnitMismatch,
    ParsedObservation,
    ParseResult,
    ReleaseMetadata,
    UnparseableReleasePage,
)

log = logging.getLogger(__name__)

# zh partner label → English canonical `partner_country` (the exact strings
# our English-parsed observations carry). Derived 2026-07-14 by exact
# 6-value-tuple matching of the CN May-2025 Express by-country xls against
# the DB (30/30 rows) — see the dev note's derivation section. 其中：("of
# which:") prefixes are stripped before lookup, mirroring the English
# parser's handling of "of which:".
#
# Label variants: GACC's own zh labels drift across years (the RCEP and
# Belt-and-Road aggregate rows gained/lost suffixes and quote marks between
# 2025 and 2026) — every observed variant maps to the same canonical label.
ZH_TO_EN_PARTNERS: dict[str, str] = {
    "总值": "Total",
    "欧洲联盟": "European Union",
    "德国": "Germany",
    "荷兰": "Netherlands",
    "法国": "France",
    "意大利": "Italy",
    "美国": "United States (US)",
    "东南亚国家联盟": "ASEAN",
    "越南": "Vietnam",
    "马来西亚": "Malaysia",
    "泰国": "Thailand",
    "新加坡": "Singapore",
    "印度尼西亚": "Indonesia",
    "菲律宾": "Philippines",
    "日本": "Japan",
    "中国香港": "Hong Kong, China",
    "韩国": "R. O. Korea",
    "中国台湾": "Taiwan, China",
    "澳大利亚": "Australia",
    "俄罗斯": "Russian Federation",
    "俄罗斯联邦": "Russian Federation",
    "印度": "India",
    "英国": "United Kingdom (UK)",
    "加拿大": "Canada",
    "新西兰": "New Zealand",
    "拉丁美洲": "Latin America",
    "巴西": "Brazil",
    "非洲": "Africa",
    "南非": "South Africa",
    # 2026 form / 2025 form of the two treaty/initiative aggregates:
    "区域全面经济伙伴关系协定（RCEP）成员国": "Regional Comprehensive Economic Partnership",
    "区域全面经济伙伴关系协定": "Regional Comprehensive Economic Partnership",
    "共建“一带一路”国家和地区": "Jointly build the countries along Belt and Road Routes",
    "共建一带一路国家": "Jointly build the countries along Belt and Road Routes",
}

# English-site descriptions per canonical section, so a CN-created release
# row reads identically on every surface until the English pass refreshes it.
_EN_DESCRIPTION = {
    4: "China's Total Export & Import Values by Country/Region",
}

# The Express by-country table, either title era. 主要 is load-bearing: the
# Monthly Bulletin's all-country table (（2）…进出口商品国别（地区）总值表)
# lacks it and is a DIFFERENT product (the verified vintage) — it must not
# parse as a preliminary. The currency suffix is a 2026-era addition
# (（人民币值）); 2025-era titles carry none, so it is optional here and the
# 单位 row is the currency authority when absent.
_TITLE_RE = re.compile(
    r"（(?P<printed_no>\d+)）\s*"
    r"(?P<year>\d{4})年(?P<months>\d{1,2}月|1至2月)"
    r"进出口商品主要国别（地区）总值表\s*"
    r"(?:（(?P<currency>人民币值?|美元值?)）)?"
)

_UNIT_RE = re.compile(r"单位\s*[∶:：]\s*(?P<unit>亿元人民币|百万美元)")

_CANONICAL_UNIT = {"CNY": "CNY 100 Million", "USD": "USD1 Million"}
_ZH_UNIT_CURRENCY = {"亿元人民币": "CNY", "百万美元": "USD"}

# /customs/2026-07/14/article_… — the path date is the publish date.
_ARTICLE_DATE_RE = re.compile(r"/customs/(\d{4})-(\d{2})/(\d{2})/article_")


def _check_title_unit(title_currency: str, unit_text: str, *,
                      url: str, section: int, period: date) -> str:
    """The CN analogue of the English currency/unit floor: the title's
    currency (人民币值/美元值) and the sheet's 单位 row must agree. A
    disagreement means the cell values are not in the scale the title
    implies — refuse to ingest rather than mis-scale (the release-184
    incident class). Returns the canonical unit string on agreement."""
    unit_currency = _ZH_UNIT_CURRENCY.get(unit_text)
    if unit_currency != title_currency:
        raise CurrencyUnitMismatch(
            f"CN Express page {url} self-inconsistent: title declares "
            f"currency {title_currency!r} but the sheet's 单位 row reads "
            f"{unit_text!r}. Refusing to ingest cell values that don't match "
            f"the title's currency.",
            section=section, period=period, currency=title_currency,
        )
    return _CANONICAL_UNIT[title_currency]


def _normalise_zh_partner_label(raw: str) -> tuple[str, int, bool]:
    """(zh label, indent, is_subset) from a raw CN label cell. Mirrors
    parse._normalise_partner_label: leading whitespace is the hierarchy
    indent; 其中： marks an of-which subset row. Interior whitespace and
    embedded newlines collapse so the dictionary lookup is exact."""
    stripped = raw.strip(" \t\n\r\f\v")
    indent = len(raw) - len(raw.lstrip(" \xa0　"))
    label = re.sub(r"\s+", "", stripped.replace("\xa0", ""))
    is_subset = label.startswith("其中：") or label.startswith("其中:")
    if is_subset:
        label = re.sub(r"^其中[：:]", "", label)
    return label, indent, is_subset


def parse_cn_express_xls(
    xls_bytes: bytes,
    *,
    xls_url: str,
    article_url: str | None = None,
) -> ParseResult:
    """Parse a CN Express by-country xls into the same ParseResult the
    English HTML parser produces — English-canonical `partner_country`,
    values at English precision, zh originals in `partner_label_raw` /
    `source_row`, full-precision values in `source_row`."""
    try:
        wb = xlrd.open_workbook(file_contents=xls_bytes)
    except xlrd.XLRDError as e:
        raise UnparseableReleasePage(
            f"CN xls {xls_url}: not a readable Excel-97 file ({e}). "
            "Refusing to guess."
        ) from e
    sh = wb.sheets()[0]

    # Title + unit live in the first few rows, but their COLUMN drifts
    # across eras (June-2026 puts 单位 in column 1; May-2025 right-aligns it
    # in column 8) — scan every column of the header rows.
    title, unit_text = None, None
    for i in range(min(5, sh.nrows)):
        for j in range(sh.ncols):
            cell = str(sh.cell_value(i, j))
            if title is None:
                m = _TITLE_RE.search(cell.replace("\n", ""))
                if m:
                    title = cell.strip()
                    tm = m
            if unit_text is None:
                um = _UNIT_RE.search(cell)
                if um:
                    unit_text = um.group("unit")
    if title is None:
        raise UnparseableReleasePage(
            f"CN xls {xls_url}: no Express by-country title "
            "(主要国别（地区）总值表) in the header rows — either a different "
            "table (commodities? the Monthly Bulletin's all-country table?) "
            "or a layout change. Refusing to guess."
        )
    if unit_text is None:
        raise UnparseableReleasePage(
            f"CN xls {xls_url}: no 单位 row found — cannot establish the "
            "value scale, refusing to ingest."
        )

    if tm.group("months") == "1至2月":
        raise UnparseableReleasePage(
            f"CN xls {xls_url}: combined Jan–Feb release — the CN combined "
            "layout has no fixture yet, so this parser refuses it rather "
            "than ship untested column arithmetic. Ingest the English "
            "release (due the same day, 公告 240号) or add a fixture."
        )

    # 2026-era titles declare the currency (（人民币值）); 2025-era titles
    # don't, and there the 单位 row is the sole authority. When both exist
    # they must agree (_check_title_unit).
    if tm.group("currency"):
        currency = "CNY" if tm.group("currency").startswith("人民币") else "USD"
    else:
        currency = _ZH_UNIT_CURRENCY[unit_text]
    period = date(int(tm.group("year")), int(tm.group("months").rstrip("月")), 1)
    unit = _check_title_unit(currency, unit_text,
                             url=xls_url, section=4, period=period)

    publication_date = None
    if article_url:
        am = _ARTICLE_DATE_RE.search(article_url)
        if am:
            publication_date = date(*map(int, am.groups()))

    meta = ReleaseMetadata(
        section_number=4,
        description=_EN_DESCRIPTION[4],
        period=period,
        currency=currency,
        publication_date=publication_date,
        unit=unit,
        excel_url=xls_url,
        source_url=article_url or xls_url,
        title=title,
        is_jan_feb_combined=False,
    )

    observations: list[ParsedObservation] = []
    unmapped: list[str] = []
    period_iso = period.isoformat()
    for i in range(sh.nrows):
        raw_label = str(sh.cell_value(i, 1))
        vals = [sh.cell_value(i, j) for j in range(2, 8)]
        if not raw_label.strip() or not all(isinstance(v, float) for v in vals):
            continue
        label_zh, indent, is_subset = _normalise_zh_partner_label(raw_label)
        if not label_zh:
            continue
        partner_en = ZH_TO_EN_PARTNERS.get(label_zh)
        if partner_en is None:
            unmapped.append(label_zh)
            continue
        # Column layout mirrors the English table: 进出口/出口/进口 ×
        # (month, 1-to-N cumulative). YoY columns (8–10) are ignored —
        # computed downstream from history, exactly as the English parser
        # does.
        source_row: dict[str, Any] = {
            "raw_label": raw_label,
            "label_zh": label_zh,
            "monthly_total": vals[0],
            "ytd_total": vals[1],
            "monthly_export": vals[2],
            "ytd_export": vals[3],
            "monthly_import": vals[4],
            "ytd_import": vals[5],
            "site": "www.customs.gov.cn",
        }
        for flow, monthly_idx, ytd_idx in [
            ("total", 0, 1), ("export", 2, 3), ("import", 4, 5),
        ]:
            for kind, idx in [("monthly", monthly_idx), ("ytd", ytd_idx)]:
                observations.append(
                    ParsedObservation(
                        section_number=4,
                        period=period_iso,
                        period_kind=kind,
                        currency=currency,
                        unit=unit,
                        flow=flow,
                        partner_country=partner_en,
                        partner_label_raw=raw_label,
                        partner_indent=indent,
                        partner_is_subset=is_subset,
                        value=round(vals[idx], 1),
                        source_row=source_row,
                    )
                )
    if unmapped:
        raise ValueError(
            f"CN xls {xls_url}: {len(unmapped)} partner label(s) with no "
            f"zh→en mapping: {sorted(set(unmapped))}. A new partner row is a "
            "real event — extend ZH_TO_EN_PARTNERS after checking what GACC "
            "changed (do not fuzzy-match)."
        )
    return ParseResult(metadata=meta, observations=observations)


def ingest_cn_xls_bytes(
    xls_bytes: bytes,
    *,
    xls_url: str,
    article_url: str | None,
    sha256: str,
    content_type: str | None = None,
    dry_run: bool = False,
) -> int | None:
    """Persist one CN Express xls: snapshot → parse → floor check → release
    + observations. Mirrors scrape.scrape_release's error contract: floor
    failures record status='failed' and create no release row; an
    unparseable file records 'no_parser'. Returns the release id (None on
    dry-run or failure)."""
    run_url = article_url or xls_url
    run_id = db.start_run(run_url) if not dry_run else None
    try:
        if not dry_run:
            db.save_snapshot(run_id, api_client.FetchResult(
                url=xls_url, status_code=200, content_type=content_type,
                content=xls_bytes, sha256=sha256))
        result = parse_cn_express_xls(
            xls_bytes, xls_url=xls_url, article_url=article_url)
        meta = result.metadata
        log.info("Parsed %d observations from CN xls (section %d, %s, %s)",
                 len(result.observations), meta.section_number,
                 meta.currency, meta.period.isoformat())
        floor_reason = parse.section4_floor_check(result.observations, meta)
        if floor_reason:
            msg = (f"CN Express parse failed the plausibility floor "
                   f"({meta.currency}, {meta.period.isoformat()}): "
                   f"{floor_reason}. Recording failed, no release row.")
            log.error(msg)
            if not dry_run:
                db.finish_run(run_id, status="failed", error_message=msg)
            return None
        if dry_run:
            print(f"DRY RUN: would ingest section {meta.section_number} "
                  f"{meta.currency} {meta.period} "
                  f"({len(result.observations)} observations) from {xls_url}")
            return None
        release_id = db.find_or_create_gacc_release(
            meta, release_kind="preliminary")
        counts = db.upsert_observations(run_id, release_id,
                                        result.observations)
        log.info("Persisted: %s", counts)
        db.finish_run(run_id, status="success", http_status=200)
        return release_id
    except (UnparseableReleasePage, NotImplementedError) as e:
        log.warning("No parser for %s: %s", xls_url, e)
        if run_id is not None:
            db.finish_run(run_id, status="no_parser", error_message=str(e))
        return None
    except Exception as e:
        log.exception("CN ingest failed for %s", xls_url)
        if run_id is not None:
            db.finish_run(run_id, status="failed", error_message=str(e))
        return None


def ingest_cn_release(xls_url: str, article_url: str | None,
                      dry_run: bool = False) -> int | None:
    """Fetch a CN Express xls attachment (WAF-free — verified) and ingest."""
    log.info("Fetching CN xls %s", xls_url)
    response = api_client.fetch(xls_url)
    return ingest_cn_xls_bytes(
        response.content, xls_url=xls_url, article_url=article_url,
        sha256=response.sha256, content_type=response.content_type,
        dry_run=dry_run)


def verify_against_db(xls_bytes: bytes, *, xls_url: str) -> int:
    """Diff a CN Express xls against what the DB already holds for the same
    (period, currency, section 4) — the institutionalised pre-registered
    diff. Prints one line per mismatch; returns the mismatch count (0 =
    the two sites published identical figures, at English precision)."""
    result = parse_cn_express_xls(xls_bytes, xls_url=xls_url)
    meta = result.metadata
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT o.partner_country, o.flow, o.period_kind, o.value_amount
                 FROM observations o JOIN releases r ON r.id = o.release_id
                WHERE r.source = 'gacc' AND r.period = %s AND r.currency = %s
                  AND r.section_number = 4""",
            (meta.period, meta.currency),
        )
        db_vals = {(p, f, k): float(v) for p, f, k, v in cur.fetchall()}
    if not db_vals:
        print(f"DB holds nothing for {meta.period} {meta.currency} section 4 "
              "— nothing to verify against (ingest the English release "
              "first, or this IS the first read).")
        return -1
    mismatches = 0
    for obs in result.observations:
        key = (obs["partner_country"], obs["flow"], obs["period_kind"])
        got = db_vals.pop(key, None)
        if got is None:
            print(f"MISSING in DB: {key} = {obs['value']}")
            mismatches += 1
        elif abs(got - obs["value"]) > 0.05:
            print(f"MISMATCH {key}: CN {obs['value']} vs DB {got}")
            mismatches += 1
    for key, v in db_vals.items():
        print(f"MISSING in CN xls: {key} = {v}")
        mismatches += 1
    label = "IDENTICAL" if mismatches == 0 else f"{mismatches} mismatches"
    print(f"verify {meta.period} {meta.currency}: "
          f"{len(result.observations)} CN observations vs DB → {label}")
    return mismatches


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    ing = sub.add_parser("ingest", help="Fetch + persist one CN Express xls")
    ing.add_argument("--xls-url", required=True,
                     help="The attachDir/fileDir xls attachment URL")
    ing.add_argument("--article-url", default=None,
                     help="The CN article page URL (provenance + publish "
                          "date from its path; the page itself is WAF-gated "
                          "and never fetched here)")
    ing.add_argument("--dry-run", action="store_true")
    ver = sub.add_parser("verify", help="Diff a CN xls against the DB")
    ver.add_argument("--xls", required=True,
                     help="Path or URL of the CN Express by-country xls")
    args = p.parse_args()
    if args.cmd == "ingest":
        release_id = ingest_cn_release(args.xls_url, args.article_url,
                                       dry_run=args.dry_run)
        if release_id is not None:
            print(f"release id {release_id}")
    elif args.cmd == "verify":
        if re.match(r"^https?://", args.xls):
            xls_bytes = api_client.fetch(args.xls).content
        else:
            with open(args.xls, "rb") as f:
                xls_bytes = f.read()
        mismatches = verify_against_db(xls_bytes, xls_url=args.xls)
        sys.exit(0 if mismatches == 0 else 1)


if __name__ == "__main__":
    main()
