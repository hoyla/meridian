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
OUT_DIR = Path(__file__).resolve().parent / "reference"

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


if __name__ == "__main__":
    out = build()
    print("cn8_sitc lookup built:", out["stats"])
