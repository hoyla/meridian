"""Eurostat Comext bulk-file fetcher.

Downloads the monthly bulk 7z, stream-decompresses the CSV, filters to the
configured partner/reporter/HS prefixes, and yields ParsedObservation-shaped
dicts that scrape.py can hand to db.upsert_observations.

Notes from the recon (see project memory `project_gacc_datasources.md`):
- The 'main' file `full_v2_YYYYMM.7z` is hidden from the directory listing —
  only the UK-specific `full_partxixu_v2_*` shows up. Fetch by direct URL.
- PRODUCT_NC is HS-CN8, stored without leading zeros — must zero-pad on load.
- FLOW: 1 = import, 2 = export. PARTNER/REPORTER are ISO-2 codes.
- Latest available lags real time by ~10 weeks.
"""

import csv
import io
import logging
import os
import tempfile
from collections.abc import Iterator
from datetime import date

import httpx
import py7zr

from api_client import FetchResult

log = logging.getLogger(__name__)

BULK_BASE = (
    "https://ec.europa.eu/eurostat/api/dissemination/files"
    "?downfile=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_{period}.7z"
)
DEFAULT_TIMEOUT = 300.0  # bulk files are 40-60 MB; allow time on slow links


def bulk_file_url(period: date) -> str:
    """The Eurostat URL for the monthly bulk file at the given period anchor."""
    return BULK_BASE.format(period=f"{period.year}{period.month:02d}")


def fetch_bulk_file(period: date, timeout: float = DEFAULT_TIMEOUT) -> FetchResult:
    """Download one monthly bulk file. Returns a FetchResult with the raw 7z bytes."""
    url = bulk_file_url(period)
    log.info("Fetching Eurostat bulk file %s", url)
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        r = client.get(url)
    r.raise_for_status()
    import hashlib
    return FetchResult(
        url=url,
        status_code=r.status_code,
        content_type=r.headers.get("content-type"),
        content=r.content,
        sha256=hashlib.sha256(r.content).hexdigest(),
    )


def _flow_label(raw: str | int) -> str:
    """Eurostat FLOW: 1 = import, 2 = export."""
    return {1: "import", 2: "export", "1": "import", "2": "export"}.get(raw, str(raw))


def iter_observations(
    archive_bytes: bytes,
    period: date,
    partners: set[str] | None = None,
    reporters: set[str] | None = None,
    hs_prefixes: tuple[str, ...] | None = None,
) -> Iterator[dict]:
    """Stream rows from a Eurostat bulk archive, yield filtered observations.

    A row passes the filter if (partners is None or PARTNER in partners) AND
    (reporters is None or REPORTER in reporters) AND (hs_prefixes is None or
    PRODUCT_NC starts with one of them). PRODUCT_NC is zero-padded to 8 chars
    before matching.
    """
    period_iso = period.replace(day=1).isoformat()

    # py7zr.extract() writes to disk; extracted CSV is ~500 MB. Use a temp dir
    # so we don't hold the decompressed text in memory all at once.
    with tempfile.TemporaryDirectory(prefix="gacc-eurostat-") as tmpdir:
        with py7zr.SevenZipFile(io.BytesIO(archive_bytes), "r") as archive:
            archive.extractall(path=tmpdir)
        files = sorted(os.listdir(tmpdir))
        if not files:
            raise ValueError("Eurostat archive is empty")
        for filename in files:
            path = os.path.join(tmpdir, filename)
            with open(path, "r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                yield from _iter_filtered(reader, period_iso, partners, reporters, hs_prefixes)


def _iter_filtered(reader, period_iso, partners, reporters, hs_prefixes) -> Iterator[dict]:
    """Aggregate rows sharing the same (reporter, partner, hs, flow) key.

    Eurostat splits monthly trade by STAT_PROCEDURE (tariff regime) and SUPPL_UNIT,
    so a single (reporter, partner, hs, flow) cell can appear across several rows.
    For mirror-trade analysis we want one row per logical observation, so we sum
    VALUE_EUR/QUANTITY_KG within the dim key and emit once per group.
    """
    agg: dict[tuple, dict] = {}
    for row in reader:
        partner = row.get("PARTNER", "")
        reporter = row.get("REPORTER", "")
        hs = (row.get("PRODUCT_NC") or "").zfill(8)

        if partners is not None and partner not in partners:
            continue
        if reporters is not None and reporter not in reporters:
            continue
        if hs_prefixes is not None and not hs.startswith(hs_prefixes):
            continue

        flow = _flow_label(row.get("FLOW", ""))
        key = (reporter, partner, hs, flow)

        value_eur = _to_float(row.get("VALUE_EUR")) or 0.0
        qty_kg = _to_float(row.get("QUANTITY_KG")) or 0.0
        qty_supp = _to_float(row.get("QUANTITY_SUPPL_UNIT")) or 0.0

        bucket = agg.setdefault(key, {
            "period": period_iso,
            "period_kind": "monthly",
            "flow": flow,
            "reporter_country": reporter,
            "partner_country": partner,
            "hs_code": hs,
            "value": 0.0,
            "currency": "EUR",
            "quantity_kg": 0.0,
            "quantity_supp": 0.0,
            "quantity_unit": row.get("SUPPL_UNIT") or "kg",
            "source_row": {"_aggregated_rows": []},
        })
        bucket["value"] += value_eur
        bucket["quantity_kg"] += qty_kg
        bucket["quantity_supp"] += qty_supp
        bucket["source_row"]["_aggregated_rows"].append(dict(row))

    for bucket in agg.values():
        bucket["quantity"] = bucket["quantity_supp"] if bucket["quantity_supp"] else bucket["quantity_kg"]
        # Clean up the intermediate accumulators before yielding.
        del bucket["quantity_kg"], bucket["quantity_supp"]
        yield bucket


def _to_float(s: str | None) -> float | None:
    if s is None or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None
