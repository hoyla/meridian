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
import dataclasses
import logging
import os
import re
import sys
from datetime import date
from typing import Any
from urllib.parse import urljoin

import xlrd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

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

# zh commodity label → (English canonical `commodity_label`, expected zh
# quantity-unit cell, English quantity_unit string as the DB stores it).
# Derived 2026-07-15 by exact value-matching of the Monthly-Bulletin May-2026
# commodity files (whose values are proven identical to the May preliminary)
# against the DB's English-parsed May-2026 section-5/6 rows, then pairing
# each line's zh unit from the June-2026 Express files. Keys are normalised:
# whitespace stripped, fullwidth（）→ halfwidth (GACC mixes widths WITHIN one
# file — the import file prints 汽车(包括底盘）), trailing aggregate '*'
# removed (the flag is carried separately, mirroring the English parser).
#
# The expected zh unit is a guard, not decoration: a row whose unit cell
# differs from the derivation-time unit refuses its QUANTITY (values still
# ingest) — the Machine-tools-2021 label-collision class, where a label
# survives an era change but its basis doesn't.
#
# Unit note, recorded for honesty: Footwear's English pages print
# "10,000 Pairs" against numbers that are plainly 亿双 (100M pairs — the CN
# unit cell says so and the quantities match at ratio 1.0). That is GACC's
# own English-edition mislabel; we store the DB-established English string
# for label continuity and keep the truthful zh unit in source_row.
ZH_TO_EN_COMMODITIES_EXPORT: dict[str, tuple[str, str | None, str | None]] = {
    "农产品": ("Agriculture products", None, None),
    "医疗仪器及器械": ("Medical or surgical instruments and apparatuses", None, None),
    "医药品": ("Medical and pharmaceutical products", "万吨", "10,000 Tons"),
    "塑料制品": ("Plastic articles", None, None),
    "家具及其零件": ("Furniture and parts thereof", None, None),
    "家用电器": ("Electric appliances of household type", "万台", "10,000 Sets"),
    "成品油": ("Refined petroleum products", "万吨", "10,000 Tons"),
    "手机": ("Mobile phones", "万台", "10,000 Sets"),
    "服装及衣着附件": ("Garments and clothing accessories", None, None),
    "未锻轧铝及铝材": ("Unwrought aluminium and aluminium products", "万吨", "10,000 Tons"),
    "机电产品": ("Mechanical and electrical products", None, None),
    "水产品": ("Aquatic products", "万吨", "10,000 Tons"),
    "汽车(包括底盘)": ("Motor vehicles（including chassis fitted with engines)", "万辆", "10,000 Autos"),
    "汽车零配件": ("Parts and accessories of vehicle", None, None),
    "液晶平板显示模组": ("Flat panel display modules of liquid crystals", "万个", "10,000 PCS"),
    "灯具、照明装置及其零件": ("Lamps and lighting fittings and parts thereof", None, None),
    "玩具": ("Toys", None, None),
    "稀土": ("Rare-earth ore, metals, compounds", "吨", "Ton"),
    "箱包及类似容器": ("Suit-cases, hand bags and similar containers", "万吨", "10,000 Tons"),
    "粮食": ("Grain food", "万吨", "10,000 Tons"),
    "纺织纱线、织物及其制品": ("Textile yarn, fabrics and articles thereof", None, None),
    "肥料": ("Fertilizers", "万吨", "10,000 Tons"),
    "自动数据处理设备及其零部件": ("Automatic data processing machines and parts thereof", None, None),
    "船舶": ("Ships", "艘", "Ship"),
    "通用机械设备": ("General machines", None, None),
    "钢材": ("Products, of steel or iron", "万吨", "10,000 Tons"),
    "陶瓷产品": ("Ceramic products", "万吨", "10,000 Tons"),
    "集成电路": ("Electronic integrated circuits", "亿个", "100 Million PCS"),
    "鞋靴": ("Footwear", "亿双", "10,000 Pairs"),
    "音视频设备及其零件": ("Audio or video devices and parts thereof", None, None),
    "高新技术产品": ("Hi-tech products", None, None),
}

ZH_TO_EN_COMMODITIES_IMPORT: dict[str, tuple[str, str | None, str | None]] = {
    "二极管及类似半导体器件": ("Diodes and similar semiconductors", "亿个", "100 Million PCS"),
    "农产品": ("Agriculture products", None, None),
    "初级形状的塑料": ("Plastics in primary forms", "万吨", "10,000 Tons"),
    "医疗仪器及器械": ("Medical or surgical instruments and apparatuses", None, None),
    "医药材及药品": ("Medicinal materials and pharmaceutical products", "吨", "Ton"),
    # Hand-mapped: the Bulletin anchor splits logs/lumber into sub-lines so
    # value-matching can't see the Express's combined row; the pairing is
    # unambiguous (the catalogue's only wood line, unit 万立方米 == the DB's
    # 10,000 CBM).
    "原木及锯材": ("Wood Logs and Lumber", "万立方米", "10,000 CBM"),
    "原油": ("Crude petroleum oils", "万吨", "10,000 Tons"),
    "大豆": ("Soya beans", "万吨", "10,000 Tons"),
    "天然及合成橡胶(包括胶乳)": ("Natural and synthetic rubber(including Latex)", "万吨", "10,000 Tons"),
    "天然气": ("Natural gases", "万吨", "10,000 Tons"),
    "干鲜瓜果及坚果": ("Fresh or dried fruit and nuts", "万吨", "10,000 Tons"),
    "成品油": ("Refined petroleum products", "万吨", "10,000 Tons"),
    "未锻轧铜及铜材": ("Unwrought copper and copper products", "万吨", "10,000 Tons"),
    "机床": ("Machine tools", "台", "Set"),
    "机电产品": ("Mechanical and electrical products", None, None),
    "汽车(包括底盘)": ("Motor vehicles（including chassis fitted with engines)", "万辆", "10,000 Cars"),
    "汽车零配件": ("Parts and accessories of vehicle", None, None),
    "液晶平板显示模组": ("Flat panel display modules of liquid crystals", "万个", "10,000 PCS"),
    "煤及褐煤": ("Coal and lignite", "万吨", "10,000 Tons"),
    # Hand-mapped: two Bulletin rows round to the same 0.1亿 value so the
    # derivation dropped it as ambiguous; the zh label is identical to the
    # export side's (which derived cleanly) and the DB import row's unit is
    # Ton, matching the file's 吨.
    "稀土": ("Rare-earth ore, metals, compounds", "吨", "Ton"),
    "空载重量超过2吨的飞机": ("Aircraft of an unladen weight exceeding 2T", "架", "Craft"),
    "粮食": ("Grain food", "万吨", "10,000 Tons"),
    "纸浆": ("Paper pulp", "万吨", "10,000 Tons"),
    "纺织纱线、织物及其制品": ("Textile yarn, fabrics and articles thereof", None, None),
    "美容化妆品及洗护用品": ("Make-ups or personal care and toiletries", "吨", "Ton"),
    "肉类(包括杂碎)": ("Meat(including meat offal)", "万吨", "10,000 Tons"),
    "肥料": ("Fertilizers", "万吨", "10,000 Tons"),
    "自动数据处理设备及其零部件": ("Automatic data processing machines and parts thereof", None, None),
    "钢材": ("Products, of steel or iron", "万吨", "10,000 Tons"),
    "铁矿砂及其精矿": ("Iron ores and concentrates", "万吨", "10,000 Tons"),
    "铜矿砂及其精矿": ("Copper ores and concentrates", "万吨", "10,000 Tons"),
    "集成电路": ("Electronic integrated circuits", "亿个", "100 Million PCS"),
    "食用植物油": ("Edible vegetable oil", "万吨", "10,000 Tons"),
    "高新技术产品": ("Hi-tech products", None, None),
}

# English-site descriptions per canonical section, so a CN-created release
# row reads identically on every surface until the English pass refreshes it.
_EN_DESCRIPTION = {
    4: "China's Total Export & Import Values by Country/Region",
    5: "China's Major Exports by Quantity and Value",
    6: "China's Major Imports by Quantity and Value",
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

# The Express commodity tables — 全国出口/进口重点商品量值表. 重点 is
# load-bearing exactly as 主要 is above: the Monthly Bulletin's commodity
# tables (（13）/（14）…主要商品量值表) use 主要 and are the verified vintage;
# they must never parse as a preliminary.
_COMMODITY_TITLE_RE = re.compile(
    r"（(?P<printed_no>\d+)）\s*"
    r"(?P<year>\d{4})年(?P<months>\d{1,2}月|1至2月)"
    r"全国(?P<flow>出口|进口)重点商品量值表\s*"
    r"(?:（(?P<currency>人民币值?|美元值?)）)?"
)

_UNIT_RE = re.compile(r"单位\s*[∶:：]\s*(?P<unit>亿元人民币|百万美元)")

_CANONICAL_UNIT = {"CNY": "CNY 100 Million", "USD": "USD1 Million"}
_ZH_UNIT_CURRENCY = {"亿元人民币": "CNY", "百万美元": "USD"}

# /customs/2026-07/14/article_… — the path date is the publish date. The
# 2025-redesign Statistics Department subdomain uses /tjs/ for the same
# pattern (tjs.customs.gov.cn/tjs/2026-08/07/article_…).
_ARTICLE_DATE_RE = re.compile(r"/(?:customs|tjs)/(\d{4})-(\d{2})/(\d{2})/article_")


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


def _numify(x: Any) -> float | None:
    """Cell → float. The CN files mix real floats with text numerics — the
    published-YoY columns arrive as strings ('3.7'), value-only rows print
    '-' — so both forms must parse and both empties must read None."""
    if isinstance(x, float):
        return x
    s = str(x).replace(",", "").replace("\xa0", "").strip()
    if not s or s in ("-", "—"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _norm_commodity_zh(raw: str) -> tuple[str, int, bool]:
    """(normalised zh label, indent, is_aggregate) for a commodity cell.
    Normalisation: whitespace stripped, fullwidth（）→ halfwidth (GACC mixes
    widths within one file), trailing '*' removed with the aggregate flag
    carried — mirroring parse._normalise_commodity_label's contract."""
    stripped = raw.strip(" \t\n\r\f\v")
    indent = len(raw) - len(raw.lstrip(" \xa0　"))
    label = re.sub(r"\s+", "", stripped.replace("\xa0", ""))
    label = label.replace("（", "(").replace("）", ")")
    is_aggregate = label.endswith("*")
    if is_aggregate:
        label = label.rstrip("*")
    return label, indent, is_aggregate


def parse_cn_express_xls(
    xls_bytes: bytes,
    *,
    xls_url: str,
    article_url: str | None = None,
) -> ParseResult:
    """Parse a CN Express xls — by-country (section 4) or commodity
    (sections 5/6, routed by title keywords) — into the same ParseResult
    the English HTML parser produces: English-canonical labels, values at
    English precision, zh originals in `partner_label_raw` / `source_row`,
    full precision preserved in `source_row`."""
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
    title, unit_text, tm, kind_re = None, None, None, None
    for i in range(min(5, sh.nrows)):
        for j in range(sh.ncols):
            cell = str(sh.cell_value(i, j))
            if title is None:
                for regex in (_TITLE_RE, _COMMODITY_TITLE_RE):
                    m = regex.search(cell.replace("\n", ""))
                    if m:
                        title, tm, kind_re = cell.strip(), m, regex
                        break
            if unit_text is None:
                um = _UNIT_RE.search(cell)
                if um:
                    unit_text = um.group("unit")
    if title is None:
        raise UnparseableReleasePage(
            f"CN xls {xls_url}: no Express title (主要国别（地区）总值表 / "
            "全国出口·进口重点商品量值表) in the header rows — either a "
            "different table (the Monthly Bulletin's 主要商品/国别 cousins — "
            "the VERIFIED vintage — or a layout change). Refusing to guess."
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

    if kind_re is _COMMODITY_TITLE_RE:
        section = 5 if tm.group("flow") == "出口" else 6
    else:
        section = 4

    # 2026-era titles declare the currency (（人民币值）); 2025-era titles
    # don't, and there the 单位 row is the sole authority. When both exist
    # they must agree (_check_title_unit).
    if tm.group("currency"):
        currency = "CNY" if tm.group("currency").startswith("人民币") else "USD"
    else:
        currency = _ZH_UNIT_CURRENCY[unit_text]
    period = date(int(tm.group("year")), int(tm.group("months").rstrip("月")), 1)
    unit = _check_title_unit(currency, unit_text,
                             url=xls_url, section=section, period=period)

    publication_date = None
    if article_url:
        am = _ARTICLE_DATE_RE.search(article_url)
        if am:
            publication_date = date(*map(int, am.groups()))

    meta = ReleaseMetadata(
        section_number=section,
        description=_EN_DESCRIPTION[section],
        period=period,
        currency=currency,
        publication_date=publication_date,
        unit=unit,
        excel_url=xls_url,
        source_url=article_url or xls_url,
        title=title,
        is_jan_feb_combined=False,
    )
    if section == 4:
        observations = _parse_by_country_rows(sh, meta, xls_url)
    else:
        observations = _parse_commodity_rows(sh, meta, xls_url)
    return ParseResult(metadata=meta, observations=observations)


def _parse_by_country_rows(sh, meta: ReleaseMetadata,
                           xls_url: str) -> list[ParsedObservation]:
    observations: list[ParsedObservation] = []
    unmapped: list[str] = []
    period_iso = meta.period.isoformat()
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
                        section_number=meta.section_number,
                        period=period_iso,
                        period_kind=kind,
                        currency=meta.currency,
                        unit=meta.unit,
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
    return observations


def _parse_commodity_rows(sh, meta: ReleaseMetadata,
                          xls_url: str) -> list[ParsedObservation]:
    """Sections 5/6 body. Column layout (payload columns 1–10, matching the
    English pages' 10-cell rows exactly):
      1: commodity label  2: quantity unit (zh; '-' for value-only rows)
      3,4: month Q,V   5,6: cumulative Q,V   7,8: prior-year cumulative Q,V
      9,10: GACC's published cumulative YoY% Q,V (text cells)
    Emits monthly + ytd observations mirroring parse._parse_section_5_6_
    commodities; prior-year and published-% stay in source_row only (same
    rationale). Numbers rounded to the English printed precision (1dp) with
    the full-precision originals kept under source_row['full_precision'] —
    the analyser derives adjacent-month priors from these fields, so they
    must be era-consistent with the English rows they sit beside."""
    observations: list[ParsedObservation] = []
    unmapped: list[str] = []
    flow = "export" if meta.section_number == 5 else "import"
    mapping = (ZH_TO_EN_COMMODITIES_EXPORT if flow == "export"
               else ZH_TO_EN_COMMODITIES_IMPORT)
    period_iso = meta.period.isoformat()
    r1 = lambda v: None if v is None else round(v, 1)
    for i in range(sh.nrows):
        raw_label = str(sh.cell_value(i, 1))
        if not raw_label.strip():
            continue
        cells = [_numify(sh.cell_value(i, j)) for j in range(3, 11)]
        (m_qty, m_val, ytd_qty, ytd_val,
         prior_qty, prior_val, yoy_qty_pct, yoy_val_pct) = cells
        if ytd_val is None and m_val is None:
            continue  # header/footnote rows
        label_zh, indent, is_aggregate = _norm_commodity_zh(raw_label)
        if not label_zh:
            continue
        entry = mapping.get(label_zh)
        if entry is None:
            unmapped.append(label_zh)
            continue
        en_label, zh_unit_expected, en_unit = entry
        unit_raw = str(sh.cell_value(i, 2)).replace("\xa0", " ").strip()
        zh_unit = None if unit_raw in ("", "-", "—") else unit_raw
        # The unit guard: a changed unit cell means the line's basis moved
        # (the Machine-tools-2021 label-collision class). Values are still
        # trustworthy (the 单位 row governs them); quantities are not —
        # drop them loudly rather than mis-scale the quotable half.
        quantity_unit = en_unit
        if zh_unit != zh_unit_expected:
            log.warning(
                "CN commodity %r (%s): unit cell %r differs from the "
                "derivation-time %r — ingesting values but REFUSING "
                "quantities for this line until the mapping is re-verified.",
                label_zh, en_label, zh_unit, zh_unit_expected,
            )
            m_qty = ytd_qty = prior_qty = None
            quantity_unit = None
        source_row: dict[str, Any] = {
            "raw_label": raw_label,
            "label_zh": label_zh,
            "is_aggregate": is_aggregate,
            "quantity_unit": quantity_unit,
            "quantity_unit_zh": zh_unit,
            "monthly_quantity": r1(m_qty),
            "monthly_value": r1(m_val),
            "ytd_quantity": r1(ytd_qty),
            "ytd_value": r1(ytd_val),
            "prior_year_ytd_quantity": r1(prior_qty),
            "prior_year_ytd_value": r1(prior_val),
            "published_yoy_quantity_pct": yoy_qty_pct,
            "published_yoy_value_pct": yoy_val_pct,
            "full_precision": {
                "monthly_quantity": m_qty, "monthly_value": m_val,
                "ytd_quantity": ytd_qty, "ytd_value": ytd_val,
                "prior_year_ytd_quantity": prior_qty,
                "prior_year_ytd_value": prior_val,
            },
            "site": "www.customs.gov.cn",
        }
        for kind, qty, val in [("monthly", m_qty, m_val),
                               ("ytd", ytd_qty, ytd_val)]:
            if val is None and qty is None:
                continue
            observations.append(
                ParsedObservation(
                    section_number=meta.section_number,
                    period=period_iso,
                    period_kind=kind,
                    currency=meta.currency,
                    unit=meta.unit,
                    flow=flow,
                    commodity_label=en_label,
                    partner_label_raw=raw_label,
                    partner_indent=indent,
                    value=r1(val),
                    quantity=r1(qty),
                    quantity_unit=quantity_unit,
                    source_row=source_row,
                )
            )
    if unmapped:
        raise ValueError(
            f"CN xls {xls_url}: {len(unmapped)} commodity label(s) with no "
            f"zh→en mapping: {sorted(set(unmapped))}. GACC revises this "
            "catalogue (the official 目录 changes yearly) — extend "
            "ZH_TO_EN_COMMODITIES_* after checking the new line against the "
            "official catalogue and the English release (do not fuzzy-match)."
        )
    return observations


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
        floor_reason = (
            parse.section4_floor_check(result.observations, meta)
            if meta.section_number == 4
            else parse.section56_floor_check(result.observations, meta))
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


# ---------------------------------------------------------------------------
# CN-side discovery (the 统计快讯 index walk — the piece the July work left
# open; see dev_notes/2026-08-07-gacc-cn-discovery-wiring.md).
#
# The Statistics Department subdomain tjs.customs.gov.cn (part of GACC's
# late-2025 site redesign) serves the Express index AND its article pages as
# static HTML **outside** the JS-challenge WAF that gates www.customs.gov.cn —
# verified live 2026-08-07, drop day for the July release: plain GETs returned
# the full index (all ten July tables, published 10:30 Beijing, plus a
# 239-page archive the old "current drop only" index never had) while the
# same-day www article still served the challenge shell. So DISCOVERY is
# plain-HTTP work now. The remaining gap is the table BYTES: redesigned tjs
# articles embed their spreadsheet in a WPS web-office viewer (whose /wps/
# path IS WAF-gated) instead of the old attachDir xls link. `probe_cn_express`
# therefore ingests whenever an article carries a direct xls link (the
# pre-redesign shape; also what a hand-supplied www article uses) and
# otherwise reports the release as published-awaiting-bytes — which is
# exactly what the routine needs on drop morning: "the Chinese release is
# out, the English translation is the laggard", instead of a quiet
# `no_change` drifting toward a false `overdue`.
# ---------------------------------------------------------------------------

CN_EXPRESS_INDEX_URL = "http://tjs.customs.gov.cn/tjs/sjgb/tjkx/index.html"

# The Chinese-site surfaces serve honest content to browser-looking clients;
# the July recon (and the 2026-08-07 verification) used a desktop UA
# throughout, so the discovery fetches do too rather than gambling that
# `gacc-monitor/0.1` stays unfiltered on a WAF-fronted host.
CN_USER_AGENT = os.environ.get(
    "GACC_CN_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
)

# Article URLs are CMS-dated on both hosts: /customs/2026-07/14/article_<id>
# (www, pre-redesign) and /tjs/2026-08/07/article_<id> (tjs, 2025 redesign).
_CN_ARTICLE_HREF_RE = re.compile(
    r"/(?:customs|tjs)/\d{4}-\d{2}/\d{2}/article_\d+\.html$")

# Direct spreadsheet attachments (the pre-redesign article shape). These live
# under attachDir/fileDir on www.customs.gov.cn and are NOT WAF-gated —
# verified ×4 in the July recon.
_CN_XLS_HREF_RE = re.compile(r"(?:attachDir|fileDir)/\S*?\.xlsx?$",
                             re.IGNORECASE)

_LIST_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# The 瑞数-style JS challenge fronting customs.gov.cn answers 412 (sometimes
# 200) with a shell whose bootstrap always defines the `$_ts` global. Verified
# 2026-08-07: tjs.customs.gov.cn served this to every plain-HTTP shape tried
# (bare UA, full browser header set, https) despite the same host answering
# 200 to the cloud run hours earlier — so WAF-free tjs is a vantage-point
# accident, not a property of the host.
_WAF_CHALLENGE_MARKER = b"$_ts"


def _looks_like_waf_challenge(body: bytes | None,
                              status_code: int | None) -> bool:
    """True when a response is the JS-challenge shell rather than content.

    Keyed on the `$_ts` bootstrap global (definitive) or a bare 412 (the
    status this WAF uses and the site otherwise has no reason to return)."""
    if body and _WAF_CHALLENGE_MARKER in body[:8192]:
        return True
    return status_code == 412


# ---------------------------------------------------------------------------
# The WPS → attachDir byte route (found 2026-08-07, July drop).
#
# The 2026-08-07 dev note recorded the bytes as unreachable on redesigned tjs
# articles: they embed a WAF-gated WPS web-office viewer instead of the old
# direct xls link. That is true of the article's RAW HTML — but not of the
# rendered viewer. Once the WPS iframe renders (real Chrome passes the
# challenge; headless and the in-app browser do not), its document carries a
# single 19-digit attachment id, and THAT id addresses an ordinary attachment:
#
#     http://www.customs.gov.cn/customs/attachDir/YYYY/MM/<19-digit-id>.xls
#
# which is NOT WAF-gated — plain httpx gets 200 application/vnd.ms-excel
# (verified for all six July tables). The id is self-describing: its first six
# digits are the YYYYMM of the attachment directory, so no extra input is
# needed to build the URL.
#
# So the drop-day cost is ONE browser-assisted step (harvest six ids), and
# everything downstream is ordinary HTTP through the unchanged ingest
# contract — that is what the `bridge` subcommand automates. Full automation
# still waits on a WAF-passing fetch; this narrows the manual part to copying
# ids rather than hunting spreadsheets.
# ---------------------------------------------------------------------------

CN_ATTACHMENT_URL_TEMPLATE = ("http://www.customs.gov.cn/customs/attachDir/"
                              "{year:04d}/{month:02d}/{attachment_id}.xls")

# 2026080710320733553 → 2026-08 → /attachDir/2026/08/<id>.xls
_CN_ATTACHMENT_ID_RE = re.compile(r"^(?P<year>\d{4})(?P<month>\d{2})\d{13}$")

_WPS_FILE_ID_RE = re.compile(r"/wps/weboffice/\S*?/s/(?P<file_id>[A-Za-z0-9]+)")


def attachment_url_from_id(attachment_id: str) -> str:
    """Build the WAF-free attachDir xls URL for a 19-digit attachment id
    harvested from a rendered WPS viewer. Fails loud on anything that isn't
    one — a mistyped id must not become a silent 404 mid-ingest."""
    cleaned = attachment_id.strip()
    m = _CN_ATTACHMENT_ID_RE.match(cleaned)
    if not m:
        raise ValueError(
            f"{attachment_id!r} is not a 19-digit GACC attachment id "
            "(expected YYYYMMDDhhmmss + 5 digits, e.g. 2026080710320733553)")
    month = int(m.group("month"))
    if not 1 <= month <= 12:
        raise ValueError(
            f"{attachment_id!r} has an impossible month {month:02d} — the "
            "first six digits must be the YYYYMM of the attachment directory")
    return CN_ATTACHMENT_URL_TEMPLATE.format(
        year=int(m.group("year")), month=month, attachment_id=cleaned)


def find_wps_file_id(article_html: bytes) -> str | None:
    """The WPS viewer file id embedded in a redesigned tjs article, if any.

    Not the attachment id (that only exists in the RENDERED viewer) — this is
    the handle an operator needs to open the right viewer and read the
    attachment id off it, so the probe logs it rather than making them hunt."""
    m = _WPS_FILE_ID_RE.search(article_html.decode("utf-8", "ignore"))
    return m.group("file_id") if m else None


@dataclasses.dataclass(frozen=True)
class CnExpressArticle:
    """One Express-index entry whose title identifies a table we ingest."""

    url: str
    title: str
    section: int                 # 4 (by-country) / 5 (exports) / 6 (imports)
    currency: str | None         # 'CNY' | 'USD' | None (2025-era title, no suffix)
    period: date                 # first-of-month anchor (Feb for Jan–Feb combined)
    is_jan_feb_combined: bool
    published: date | None       # list-page date span, else the URL path date


@dataclasses.dataclass
class CnDiscoveryOutcome:
    """What one CN Express discovery walk found / did.

    status:
      ingested                — ≥1 release ingested from a discovered xls
      published_awaiting_bytes — new release articles are up on the Chinese
                                 site but none carried a direct xls link (the
                                 redesigned WPS-viewer shape) — the release
                                 EXISTS upstream, we just can't pull bytes yet
      no_new                  — index walked; every recognised table already
                                 has a release row in the DB
      challenged              — the index answered, but with the JS-challenge
                                 WAF shell instead of content (412 + `$_ts`,
                                 or a 200 shell with no articles). We are
                                 BLIND to the Chinese site, not looking at a
                                 quiet one — a distinction that matters on
                                 drop morning, when `unavailable` would read
                                 as "the index is broken" rather than "we
                                 can't see through the challenge from here".
      unavailable             — the index itself couldn't be fetched/parsed
                                 (never fatal to the caller: the English walk
                                 remains authoritative)
    """

    status: str
    ingested: int = 0
    pending: list[CnExpressArticle] = dataclasses.field(default_factory=list)
    error: str | None = None


def _classify_cn_title(title: str) -> tuple[int, str | None, date, bool] | None:
    """(section, currency, period, is_jan_feb) from an Express article title,
    or None for tables we don't ingest (总值表 (1), trade-mode (2)/(3), the
    cumulative 1至N月 variants, and anything unrecognised). Reuses the same
    title regexes the xls parser trusts, so discovery and parse can't drift
    apart on what counts as an ingestable table."""
    cleaned = title.replace("\n", "").strip()
    for regex in (_TITLE_RE, _COMMODITY_TITLE_RE):
        m = regex.search(cleaned)
        if not m:
            continue
        if regex is _COMMODITY_TITLE_RE:
            section = 5 if m.group("flow") == "出口" else 6
        else:
            section = 4
        currency = None
        if m.group("currency"):
            currency = "CNY" if m.group("currency").startswith("人民币") else "USD"
        is_jan_feb = m.group("months") == "1至2月"
        month = 2 if is_jan_feb else int(m.group("months").rstrip("月"))
        return section, currency, date(int(m.group("year")), month, 1), is_jan_feb
    return None


def discover_express_articles(index_html: bytes,
                              base_url: str) -> list[CnExpressArticle]:
    """Parse an Express index page into the articles whose titles identify
    tables we ingest (sections 4/5/6, either currency). Keys on the CMS-dated
    article URL pattern plus the title keywords — never on DOM specifics —
    so a template reskin degrades to an empty list (loud downstream), not a
    wrong parse."""
    soup = BeautifulSoup(index_html, "lxml")
    articles: list[CnExpressArticle] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].split("#")[0].split("?")[0]
        if not _CN_ARTICLE_HREF_RE.search(href):
            continue
        url = urljoin(base_url, href)
        if url in seen:
            continue
        title = (a.get("title") or a.get_text(strip=True) or "").strip()
        classified = _classify_cn_title(title)
        if classified is None:
            log.debug("CN index: skipping non-ingested table %r", title or href)
            continue
        section, currency, period, is_jan_feb = classified
        published = None
        sib = a.find_next_sibling("span")
        if sib and _LIST_DATE_RE.match(sib.get_text(strip=True)):
            published = date.fromisoformat(sib.get_text(strip=True))
        if published is None:
            dm = _ARTICLE_DATE_RE.search(url)
            if dm:
                published = date(*map(int, dm.groups()))
        seen.add(url)
        articles.append(CnExpressArticle(
            url=url, title=title, section=section, currency=currency,
            period=period, is_jan_feb_combined=is_jan_feb,
            published=published))
    return articles


def discover_article_attachments(article_html: bytes,
                                 base_url: str) -> tuple[list[str], bool]:
    """(xls attachment URLs, uses_wps_viewer) from an Express article page.

    Pre-redesign articles carry one direct attachDir/fileDir xls link;
    redesigned tjs articles instead embed a WAF-gated WPS web-office iframe
    (no fetchable bytes). Both signals are returned so the caller can tell
    "no attachment because WPS-era page" from "no attachment at all"."""
    soup = BeautifulSoup(article_html, "lxml")
    refs = [a["href"] for a in soup.find_all("a", href=True)]
    refs += [ifr["src"] for ifr in soup.find_all("iframe", src=True)]
    xls_urls: list[str] = []
    for ref in refs:
        if _CN_XLS_HREF_RE.search(ref.split("?")[0]):
            url = urljoin(base_url, ref)
            if url not in xls_urls:
                xls_urls.append(url)
    uses_wps = any("/wps/weboffice/" in (ifr.get("src") or "")
                   for ifr in soup.find_all("iframe"))
    return xls_urls, uses_wps


def _cn_release_missing(article: CnExpressArticle) -> bool:
    """True when the DB holds no release row for this article's table cell —
    the dedup that keeps the daily walk from re-fetching articles for
    releases we already hold (from either site: same natural key). A
    currency-less 2025-era title is 'missing' until BOTH currencies exist."""
    if article.currency is not None:
        return not db.gacc_release_exists(article.section, article.period,
                                          article.currency)
    return not (db.gacc_release_exists(article.section, article.period, "CNY")
                and db.gacc_release_exists(article.section, article.period,
                                           "USD"))


def probe_cn_express(dry_run: bool = False,
                     index_url: str = CN_EXPRESS_INDEX_URL) -> CnDiscoveryOutcome:
    """Walk the CN Express index once: fetch, classify titles, and for each
    recognised table missing from the DB fetch its article page and ingest
    any direct xls attachment. Never raises for network/parse trouble — the
    caller (scrape.probe_source) treats CN discovery as additive on top of
    the authoritative English walk."""
    try:
        response = api_client.fetch(index_url, user_agent=CN_USER_AGENT)
    except Exception as e:
        resp = getattr(e, "response", None)
        if _looks_like_waf_challenge(getattr(resp, "content", None),
                                     getattr(resp, "status_code", None)):
            log.warning("CN Express index is behind the JS-challenge WAF "
                        "(%s): %s — CN discovery is blind from here",
                        index_url, e)
            return CnDiscoveryOutcome(status="challenged", error=str(e))
        log.warning("CN Express index unavailable (%s): %s", index_url, e)
        return CnDiscoveryOutcome(status="unavailable", error=str(e))

    try:
        articles = discover_express_articles(response.content, index_url)
    except Exception as e:
        log.warning("CN Express index unparseable (%s): %s", index_url, e)
        return CnDiscoveryOutcome(status="unavailable", error=str(e))

    # A challenge can also arrive as a 200 whose body is the shell. Left
    # undetected it parses to zero articles and reads as a quiet no_new —
    # the exact false-negative this status exists to prevent.
    if not articles and _looks_like_waf_challenge(response.content, None):
        log.warning("CN Express index returned the JS-challenge shell (200) "
                    "— CN discovery is blind from here")
        return CnDiscoveryOutcome(
            status="challenged",
            error="index returned the JS-challenge shell instead of content")

    missing = [a for a in articles if _cn_release_missing(a)]
    if not missing:
        log.info("CN Express index: %d recognised tables, none new", len(articles))
        return CnDiscoveryOutcome(status="no_new")

    ingested = 0
    pending: list[CnExpressArticle] = []
    errors: list[str] = []
    for art in missing:
        if art.is_jan_feb_combined:
            # The xls parser refuses combined Jan–Feb layouts (no fixture) —
            # skip pre-fetch; the English release covers it.
            log.info("CN discovery: skipping combined Jan–Feb article %s", art.url)
            continue
        try:
            page = api_client.fetch(art.url, user_agent=CN_USER_AGENT)
        except Exception as e:
            log.warning("CN article fetch failed for %s: %s", art.url, e)
            errors.append(f"{art.url}: {e}")
            pending.append(art)
            continue
        xls_urls, uses_wps = discover_article_attachments(page.content, art.url)
        if not xls_urls:
            file_id = find_wps_file_id(page.content) if uses_wps else None
            log.info(
                "CN Express published %s (%s) but the article carries no "
                "direct xls link%s — bytes unavailable to plain HTTP; open "
                "the viewer in a real browser and pass the 19-digit "
                "attachment id to `gacc_cn.py bridge`. Will recheck next "
                "walk until the release lands via any route",
                art.title, art.url,
                f" (WPS web-office viewer, file id {file_id})" if file_id
                else " (WPS web-office viewer)" if uses_wps else "")
            pending.append(art)
            continue
        for xls_url in xls_urls:
            release_id = ingest_cn_release(xls_url, art.url, dry_run=dry_run)
            if release_id is not None:
                ingested += 1

    if ingested:
        status = "ingested"
    elif pending:
        status = "published_awaiting_bytes"
    else:
        status = "no_new"
    return CnDiscoveryOutcome(status=status, ingested=ingested,
                              pending=pending,
                              error=" | ".join(errors) or None)


def verify_against_db(xls_bytes: bytes, *, xls_url: str) -> int:
    """Diff a CN Express xls against what the DB already holds for the same
    (period, currency, section) — the institutionalised pre-registered
    diff, for the by-country AND commodity tables. Prints one line per
    mismatch; returns the mismatch count (0 = the two sites published
    identical figures, at English precision)."""
    result = parse_cn_express_xls(xls_bytes, xls_url=xls_url)
    meta = result.metadata
    with db.transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT o.partner_country, o.commodity_label, o.flow,
                      o.period_kind, o.value_amount, o.quantity
                 FROM observations o JOIN releases r ON r.id = o.release_id
                WHERE r.source = 'gacc' AND r.period = %s AND r.currency = %s
                  AND r.section_number = %s""",
            (meta.period, meta.currency, meta.section_number),
        )
        db_vals = {(p, c, f, k): (float(v), float(q) if q is not None else None)
                   for p, c, f, k, v, q in cur.fetchall()}
    if not db_vals:
        print(f"DB holds nothing for {meta.period} {meta.currency} section "
              f"{meta.section_number} — nothing to verify against (ingest "
              "the English release first, or this IS the first read).")
        return -1
    def _close(a, b):
        if a is None and b is None:
            return True
        if a is None or b is None:
            return False
        return abs(a - b) <= 0.05
    mismatches = 0
    for obs in result.observations:
        key = (obs.get("partner_country"), obs.get("commodity_label"),
               obs["flow"], obs["period_kind"])
        got = db_vals.pop(key, None)
        if got is None:
            print(f"MISSING in DB: {key} = {obs['value']}")
            mismatches += 1
            continue
        got_v, got_q = got
        if not _close(got_v, obs.get("value")):
            print(f"VALUE MISMATCH {key}: CN {obs.get('value')} vs DB {got_v}")
            mismatches += 1
        if not _close(got_q, obs.get("quantity")):
            print(f"QUANTITY MISMATCH {key}: CN {obs.get('quantity')} "
                  f"vs DB {got_q}")
            mismatches += 1
    for key, (v, _q) in db_vals.items():
        print(f"MISSING in CN xls: {key} = {v}")
        mismatches += 1
    label = "IDENTICAL" if mismatches == 0 else f"{mismatches} mismatches"
    print(f"verify {meta.period} {meta.currency} s{meta.section_number}: "
          f"{len(result.observations)} CN observations vs DB → {label}")
    return mismatches


def main() -> None:
    # This module is a first-class CLI (the documented drop-day manual
    # bridge), not just a library imported by scrape.py — which is where the
    # process usually picks up DATABASE_URL. Without this, every standalone
    # `gacc_cn.py ingest/discover/bridge` dies on KeyError: 'DATABASE_URL'.
    load_dotenv()
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
    dis = sub.add_parser(
        "discover",
        help="Walk the CN Express index (tjs.customs.gov.cn — WAF-free) and "
             "ingest any new release whose article carries a direct xls link; "
             "report releases published upstream but not yet fetchable")
    dis.add_argument("--index-url", default=CN_EXPRESS_INDEX_URL)
    dis.add_argument("--dry-run", action="store_true")
    brg = sub.add_parser(
        "bridge",
        help="Ingest CN tables from 19-digit attachment ids harvested from "
             "rendered WPS viewers (the drop-day manual bridge). Builds the "
             "WAF-free attachDir URL for each id and runs the normal ingest.")
    brg.add_argument("--attachment-id", action="append", required=True,
                     dest="attachment_ids", metavar="ID",
                     help="Repeatable. 19-digit id read off a rendered WPS "
                          "viewer, e.g. 2026080710320733553")
    brg.add_argument("--article-url", action="append", default=None,
                     dest="article_urls", metavar="URL",
                     help="Repeatable, paired with --attachment-id by "
                          "position. Optional provenance; omit and the xls "
                          "URL alone is recorded.")
    brg.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    if args.cmd == "discover":
        outcome = probe_cn_express(dry_run=args.dry_run,
                                   index_url=args.index_url)
        print(f"cn-express discovery: {outcome.status}"
              + (f" — ingested {outcome.ingested} release(s)"
                 if outcome.ingested else ""))
        for art in outcome.pending:
            print(f"  published upstream, bytes unavailable: {art.title} "
                  f"({art.published}) {art.url}")
        if outcome.error:
            print(f"  errors: {outcome.error}")
        sys.exit(0 if outcome.status not in ("unavailable", "challenged") else 1)
    elif args.cmd == "bridge":
        article_urls = args.article_urls or []
        failed = 0
        for i, attachment_id in enumerate(args.attachment_ids):
            article_url = article_urls[i] if i < len(article_urls) else None
            try:
                xls_url = attachment_url_from_id(attachment_id)
                release_id = ingest_cn_release(xls_url, article_url,
                                               dry_run=args.dry_run)
            except Exception as e:
                # One bad id must not abort the remaining tables: a partial
                # bridge is recoverable (re-run the failures), an aborted one
                # leaves the drop half-ingested with no summary of what's left.
                failed += 1
                log.exception("bridge failed for attachment id %s", attachment_id)
                print(f"  {attachment_id}: FAILED — {e}")
                continue
            print(f"  {attachment_id}: "
                  + (f"release id {release_id}" if release_id is not None
                     else "dry run — parsed, not persisted"))
        print(f"bridge: {len(args.attachment_ids) - failed}/"
              f"{len(args.attachment_ids)} "
              + ("parsed (dry run)" if args.dry_run else "ingested"))
        sys.exit(1 if failed else 0)
    elif args.cmd == "ingest":
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
