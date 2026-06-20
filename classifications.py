"""Derive a CN8 → SITC-division lookup for the codes in our dataset.

This is the structural-spine half of the taxonomy direction (see
`dev_notes/2026-06-20-taxonomy-sitc-spine-and-labels.md`): every CN8 code
in the Eurostat data is mapped to its SITC Rev 4 division — a defensible,
authoritative, reporter-legible bucket — so the whole 9,500-code universe
becomes navigable, not just the ~25% in editorial groups.

Provenance is first-class (the journalism defensibility principle): the
derived lookup records exactly which UNSD source editions produced each
mapping, and a sidecar PROVENANCE.md records the source files, vintages,
and methodology.

Source files live OUTSIDE the repo (cross-project reference, not duplicated
per repo) at SOURCE_DIR. The small derived CSV is the only thing the repo
carries.

Two correctness rules learned the hard way (2026-06-20):
1. **Real codes only.** Eurostat Comext mixes aggregate pseudo-codes into
   the data (`000TOTAL`, and `…XX` confidential/aggregated codes). They are
   NOT real CN8 and must be excluded — `000TOTAL` alone is the grand total
   and double-counts the entire denominator if left in.
2. **Multi-edition data.** The dataset spans HS 2017 and HS 2022 (e.g.
   smartphones are 851712 pre-2022, 851713/14 after; solar cells 854140 →
   854142/43). Map HS2022 first, then fall back to HS2017, or ~half the
   value (the big electronics) fails to map.
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

import openpyxl

import db

SOURCE_DIR = Path(os.path.expanduser("~/Code/un-classifications"))
HS2022_SITC4 = SOURCE_DIR / "hs2022_sitc4.xlsx"
HS2017_SITC4 = SOURCE_DIR / "hs2017_sitc4.xlsx"
BEC_SOURCE = SOURCE_DIR / "HS-SITC-BEC_Correlations_2022.xlsx"  # has HS22→BEC4
OUT_DIR = Path(__file__).resolve().parent / "reference"

# BEC Rev 4 basic category → SNA broad end-use (the documented standard).
# (Rev 4, not Rev 5: Rev 4 is purpose-built for the capital/intermediate/
# consumption split; Rev 5 restructured in ways that need its own legend.)
_BEC4_ENDUSE = {
    "41": "Capital", "521": "Capital",
    "111": "Intermediate", "121": "Intermediate", "21": "Intermediate",
    "22": "Intermediate", "42": "Intermediate", "53": "Intermediate",
    "322": "Intermediate",
    "112": "Consumption", "122": "Consumption", "51": "Consumption",
    "522": "Consumption", "61": "Consumption", "62": "Consumption",
    "63": "Consumption",
    "31": "Fuel", "32": "Fuel", "321": "Fuel",
    "7": "Other",
}
_ENDUSE_ORDER = {"Capital": 0, "Intermediate": 1, "Consumption": 2,
                 "Fuel": 3, "Other": 4}

# SITC Rev 4 division titles (2-digit), authoritative published list.
SITC_DIVISION = {
    "00": "Live animals", "01": "Meat & preparations", "02": "Dairy & eggs",
    "03": "Fish & seafood", "04": "Cereals & preparations", "05": "Vegetables & fruit",
    "06": "Sugar & honey", "07": "Coffee, tea, cocoa, spices", "08": "Animal feed",
    "09": "Misc edible products", "11": "Beverages", "12": "Tobacco",
    "21": "Hides & skins, raw", "22": "Oil-seeds", "23": "Crude rubber",
    "24": "Cork & wood", "25": "Pulp & waste paper", "26": "Textile fibres",
    "27": "Crude fertilizers & minerals", "28": "Metalliferous ores & scrap",
    "29": "Crude animal/veg materials", "32": "Coal & coke", "33": "Petroleum & products",
    "34": "Gas", "35": "Electric current", "41": "Animal oils & fats",
    "42": "Vegetable fats & oils", "43": "Processed fats/oils, waxes",
    "51": "Organic chemicals", "52": "Inorganic chemicals", "53": "Dyeing/tanning/colouring",
    "54": "Medicinal & pharmaceutical", "55": "Essential oils, cosmetics, cleaning",
    "56": "Fertilizers (manufactured)", "57": "Plastics, primary forms",
    "58": "Plastics, non-primary", "59": "Chemical materials nes",
    "61": "Leather & manufactures", "62": "Rubber manufactures",
    "63": "Cork & wood manufactures", "64": "Paper & paperboard",
    "65": "Textile yarn & fabrics", "66": "Non-metallic mineral manufactures",
    "67": "Iron & steel", "68": "Non-ferrous metals", "69": "Metal manufactures nes",
    "71": "Power-generating machinery", "72": "Specialized industrial machinery",
    "73": "Metalworking machinery", "74": "General industrial machinery",
    "75": "Office & data-processing machines", "76": "Telecom & sound equipment",
    "77": "Electrical machinery & apparatus", "78": "Road vehicles",
    "79": "Other transport equipment", "81": "Prefab buildings; fixtures",
    "82": "Furniture & bedding", "83": "Travel goods & handbags",
    "84": "Apparel & clothing", "85": "Footwear",
    "87": "Scientific & precision instruments", "88": "Photographic & optical goods",
    "89": "Misc manufactured articles", "91": "Postal packages",
    "93": "Special transactions", "96": "Coin", "97": "Gold, non-monetary",
}
SITC_SECTION = {
    "0": "Food & live animals", "1": "Beverages & tobacco", "2": "Crude materials",
    "3": "Mineral fuels", "4": "Oils & fats", "5": "Chemicals",
    "6": "Manufactured goods by material", "7": "Machinery & transport",
    "8": "Misc manufactured", "9": "Other / unclassified",
}


def _load_conversion(path: Path) -> dict[str, str]:
    """HS6 -> SITC4 from a UNSD 'Conversion' sheet."""
    wb = openpyxl.load_workbook(path, read_only=True)
    sheet = next(s for s in wb.sheetnames if s.lower().startswith("conversion"))
    ws = wb[sheet]
    m: dict[str, str] = {}
    for i, (hs, sitc) in enumerate(ws.iter_rows(values_only=True)):
        if i and hs and sitc:
            m[str(hs).strip()] = str(sitc).strip()
    return m


def _real_cn8(code: str) -> bool:
    """A genuine 8-digit CN code — excludes 000TOTAL and …XX aggregates."""
    return code.isdigit() and len(code) == 8


_CONVERSION: dict[str, str] | None = None


def _conversion() -> dict[str, str]:
    """HS6 -> SITC4, HS2022 over HS2017 (cached). Used to assign the SITC
    `sector` facet to an editorial group from its HS patterns."""
    global _CONVERSION
    if _CONVERSION is None:
        try:
            m = _load_conversion(HS2017_SITC4)
            m.update(_load_conversion(HS2022_SITC4))  # HS2022 wins
        except FileNotFoundError:
            # Shared classification files absent (e.g. a clean checkout
            # without ~/Code/un-classifications/). The SITC sector facet is
            # optional enrichment — degrade to empty, never break a render.
            m = {}
        _CONVERSION = m
    return _CONVERSION


def sitc_divisions_for_patterns(patterns) -> list[str]:
    """The SITC division code(s) an editorial group spans, from its HS
    wildcard patterns (2/4/6/8-digit). A group may span several — it's a
    label, not a partition (e.g. 'Electrical equipment, broad' → 10)."""
    conv = _conversion()
    divs: set[str] = set()
    for p in patterns or []:
        d = p.replace("%", "")
        if len(d) >= 6:
            s = conv.get(d[:6])
            if s:
                divs.add(s[:2])
        else:
            for k, s in conv.items():
                if k.startswith(d):
                    divs.add(s[:2])
    return sorted(divs)


def division_title(code: str) -> str:
    return SITC_DIVISION.get(code, f"div {code}")


_HS_BEC4: dict[str, str] | None = None


def _hs6_bec4() -> dict[str, str]:
    """HS6 -> BEC Rev 4 (cached) from the combined correlation workbook.
    Empty if absent — the end-use facet is optional enrichment."""
    global _HS_BEC4
    if _HS_BEC4 is None:
        m: dict[str, str] = {}
        try:
            ws = openpyxl.load_workbook(BEC_SOURCE, read_only=True)["HS SITC BEC"]
            hi = bi = None
            for i, row in enumerate(ws.iter_rows(values_only=True)):
                if i == 0:
                    hdr = [str(c) for c in row]
                    hi, bi = hdr.index("HS22"), hdr.index("BEC4")
                    continue
                hs, b = row[hi], row[bi]
                if hs and b and str(b).strip() != "NULL":
                    m[str(hs).strip()] = str(b).strip()
        except (FileNotFoundError, KeyError, ValueError):
            m = {}
        _HS_BEC4 = m
    return _HS_BEC4


def bec4_enduse(bec4: str) -> str:
    return _BEC4_ENDUSE.get(str(bec4).strip(), "Other")


def enduse_for_patterns(patterns) -> list[str]:
    """The SNA end-use category/categories an editorial group spans
    (Capital / Intermediate / Consumption / Fuel), from its HS patterns."""
    hsbec = _hs6_bec4()
    cats: set[str] = set()
    for p in patterns or []:
        d = p.replace("%", "")
        if len(d) >= 6:
            b = hsbec.get(d[:6])
            if b:
                cats.add(bec4_enduse(b))
        else:
            for k, b in hsbec.items():
                if k.startswith(d):
                    cats.add(bec4_enduse(b))
    return sorted(cats, key=lambda c: _ENDUSE_ORDER.get(c, 9))


_CN8_DIV: dict[str, str] | None = None


def cn8_division_map() -> dict[str, str]:
    """CN8 -> SITC division, from the committed derived lookup
    (reference/cn8_sitc.csv). Empty dict if absent (never breaks a render)."""
    global _CN8_DIV
    if _CN8_DIV is None:
        path = OUT_DIR / "cn8_sitc.csv"
        m: dict[str, str] = {}
        try:
            with open(path) as f:
                for row in csv.DictReader(f):
                    m[row["cn8"]] = row["sitc_division"]
        except FileNotFoundError:
            pass
        _CN8_DIV = m
    return _CN8_DIV


def build(write: bool = True) -> dict:
    m22 = _load_conversion(HS2022_SITC4)
    m17 = _load_conversion(HS2017_SITC4)

    with db._conn() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT DISTINCT o.hs_code
                 FROM observations o JOIN releases r ON r.id = o.release_id
                WHERE r.source = 'eurostat' AND o.hs_code IS NOT NULL"""
        )
        codes = [r[0] for r in cur.fetchall()]

    rows = []
    stats = {"raw": len(codes), "real": 0, "mapped": 0, "unmapped": 0,
             "via_hs2022": 0, "via_hs2017": 0}
    for code in sorted(c for c in codes if _real_cn8(c)):
        stats["real"] += 1
        hs6 = code[:6]
        sitc = m22.get(hs6)
        via = "hs2022"
        if sitc is None:
            sitc = m17.get(hs6)
            via = "hs2017"
        if sitc is None:
            stats["unmapped"] += 1
            continue
        stats["mapped"] += 1
        stats[f"via_{via}"] += 1
        div = sitc[:2]
        rows.append({
            "cn8": code, "hs6": hs6, "sitc4": sitc,
            "sitc_division": div, "sitc_division_title": SITC_DIVISION.get(div, ""),
            "sitc_section": sitc[0], "sitc_section_title": SITC_SECTION.get(sitc[0], ""),
            "mapped_via": via,
        })

    if write:
        OUT_DIR.mkdir(exist_ok=True)
        with open(OUT_DIR / "cn8_sitc.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        _write_provenance(stats)
    return {"stats": stats, "rows": rows}


def _write_provenance(stats: dict) -> None:
    from datetime import datetime
    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / "cn8_sitc.PROVENANCE.md").write_text(f"""# cn8_sitc.csv — provenance

Generated by `classifications.build()` on {datetime.now():%Y-%m-%d %H:%M}.

## What it is
A CN8 → SITC Rev 4 division lookup for every real 8-digit code present in
the Eurostat China-trade observations. The SITC division is the structural
navigation spine for the portal (see the taxonomy design note).

## Source classifications (UNSD)
- HS 2022 → SITC Rev 4: `hs2022_sitc4.xlsx`
- HS 2017 → SITC Rev 4 (fallback): `hs2017_sitc4.xlsx`
- Source: <https://unstats.un.org/unsd/classifications/Econ/tables/>, downloaded 2026-06-20.
- Files live at `~/Code/un-classifications/` (shared local ref, not in-repo).

## Method
- CN8 mapped via its 6-digit HS stem.
- **HS2022 first, then HS2017 fallback** — the dataset spans both editions.
- Aggregate pseudo-codes (`000TOTAL`, `…XX`) excluded: real 8-digit numeric only.

## Vintages to refresh on
HS 2022 / SITC Rev 4. When HS 2027 ships and the EU CN rebases, add the
HS2027→SITC correspondence ahead of HS2022 in the fallback chain.

## Counts at generation
- raw distinct codes in data: {stats['raw']:,}
- real 8-digit CN8: {stats['real']:,}  (pseudo/aggregate dropped: {stats['raw'] - stats['real']:,})
- mapped: {stats['mapped']:,}  (via HS2022 {stats['via_hs2022']:,}, via HS2017 {stats['via_hs2017']:,})
- unmapped: {stats['unmapped']:,}
""")


def build_bec(write: bool = True) -> dict:
    """Derive CN8 → BEC Rev 4 → SNA end-use for the dataset codes."""
    from collections import Counter
    hsbec = _hs6_bec4()
    with db._conn() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT DISTINCT o.hs_code
                 FROM observations o JOIN releases r ON r.id = o.release_id
                WHERE r.source = 'eurostat' AND o.hs_code IS NOT NULL"""
        )
        codes = [r[0] for r in cur.fetchall()]
    rows = []
    stats = {"raw": len(codes), "real": 0, "mapped": 0, "unmapped": 0}
    eu_count: Counter = Counter()
    for code in sorted(c for c in codes if _real_cn8(c)):
        stats["real"] += 1
        b = hsbec.get(code[:6])
        if not b:
            stats["unmapped"] += 1
            continue
        eu = bec4_enduse(b)
        stats["mapped"] += 1
        eu_count[eu] += 1
        rows.append({"cn8": code, "hs6": code[:6], "bec4": b, "end_use": eu})
    if write:
        OUT_DIR.mkdir(exist_ok=True)
        with open(OUT_DIR / "cn8_bec.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["cn8", "hs6", "bec4", "end_use"])
            w.writeheader()
            w.writerows(rows)
        from datetime import datetime
        (OUT_DIR / "cn8_bec.PROVENANCE.md").write_text(f"""# cn8_bec.csv — provenance

Generated by `classifications.build_bec()` on {datetime.now():%Y-%m-%d %H:%M}.

CN8 → BEC Rev 4 → SNA broad end-use (Capital / Intermediate / Consumption /
Fuel) for every real 8-digit code in the Eurostat data.

- Source: `HS-SITC-BEC_Correlations_2022.xlsx` (UNSD, HS2022→BEC4 column),
  at `~/Code/un-classifications/`.
- **BEC Rev 4, not Rev 5** — Rev 4 is the documented standard for the
  capital/intermediate/consumption end-use split; Rev 5 restructured in ways
  that need its own legend to read end-use.
- Mapped via the 6-digit HS stem; real 8-digit codes only.

## Counts
- real CN8: {stats['real']:,}  mapped: {stats['mapped']:,}  unmapped: {stats['unmapped']:,}
- by end-use: {dict(eu_count)}
""")
    return {"stats": stats, "end_use": dict(eu_count)}


if __name__ == "__main__":
    print("cn8_sitc lookup built:", build()["stats"])
    print("cn8_bec lookup built:", build_bec()["stats"])
