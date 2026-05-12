"""Eurostat Comext bulk-file fetcher.

Pipeline (raw → comparable):

    fetch_bulk_file(period)
        ↓ httpx.get -> 7z bytes
    iter_raw_rows(archive_bytes, period, ...filters)
        ↓ stream-decompress + filter, no aggregation
        yields one dict per CSV row (keys lower-cased to match DB columns)
    db.bulk_insert_eurostat_raw_rows(run_id, raws) -> list[int] of inserted ids
    aggregate_to_observations(period, [(raw_id, raw_dict), ...])
        ↓ group by (reporter, partner, product_nc, flow), sum value/quantity
        yields aggregated observation dicts carrying eurostat_raw_row_ids
    db.upsert_observations(run_id, release_id, observations)

Two-stage layering keeps the source data immutable: the raw CSV rows are
queryable in `eurostat_raw_rows`, the comparable per-cell view in
`observations`, and the aggregation method is a single named function rather
than implicit in every cross-source query.

Notes from the recon (see project memory `project_gacc_datasources.md`):
- The 'main' file `full_v2_YYYYMM.7z` is hidden from the directory listing —
  only the UK-specific `full_partxixu_v2_*` shows up. Fetch by direct URL.
- PRODUCT_NC is HS-CN8, stored without leading zeros — must zero-pad on load.
- FLOW: 1 = import, 2 = export. PARTNER/REPORTER are ISO-2 codes.
- Latest available lags real time by ~10 weeks.
"""

import csv
import hashlib
import io
import logging
import os
import tempfile
from collections.abc import Iterable, Iterator
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

# CSV column → DB column name mapping for the raw row.
_CSV_TO_DB_COLS = {
    "REPORTER": "reporter",
    "PARTNER": "partner",
    "TRADE_TYPE": "trade_type",
    "PRODUCT_NC": "product_nc",
    "PRODUCT_SITC": "product_sitc",
    "PRODUCT_CPA21": "product_cpa21",
    "PRODUCT_CPA22": "product_cpa22",
    "PRODUCT_BEC": "product_bec",
    "PRODUCT_BEC5": "product_bec5",
    "PRODUCT_SECTION": "product_section",
    "FLOW": "flow",
    "STAT_PROCEDURE": "stat_procedure",
    "SUPPL_UNIT": "suppl_unit",
    "VALUE_EUR": "value_eur",
    "VALUE_NAC": "value_nac",
    "QUANTITY_KG": "quantity_kg",
    "QUANTITY_SUPPL_UNIT": "quantity_suppl_unit",
}
_NUMERIC_DB_COLS = {"value_eur", "value_nac", "quantity_kg", "quantity_suppl_unit"}


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
    return FetchResult(
        url=url,
        status_code=r.status_code,
        content_type=r.headers.get("content-type"),
        content=r.content,
        sha256=hashlib.sha256(r.content).hexdigest(),
    )


def iter_raw_rows(
    archive_bytes: bytes,
    period: date,
    partners: set[str] | None = None,
    reporters: set[str] | None = None,
    hs_prefixes: tuple[str, ...] | None = None,
) -> Iterator[dict]:
    """Yield one dict per raw CSV row passing the filters. No aggregation.

    Keys are lower-cased to match the eurostat_raw_rows DB column names.
    PRODUCT_NC is zero-padded to 8 chars. FLOW is converted to int.
    Numeric columns are converted to float (or None for empty).
    """
    period = period.replace(day=1)

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
                yield from _iter_filtered_raw(reader, period, partners, reporters, hs_prefixes)


def _iter_filtered_raw(reader, period: date, partners, reporters, hs_prefixes) -> Iterator[dict]:
    for row in reader:
        partner = row.get("PARTNER", "")
        reporter = row.get("REPORTER", "")
        product_nc = (row.get("PRODUCT_NC") or "").zfill(8)

        if partners is not None and partner not in partners:
            continue
        if reporters is not None and reporter not in reporters:
            continue
        if hs_prefixes is not None and not product_nc.startswith(hs_prefixes):
            continue

        out = {db_col: row.get(csv_col) or None for csv_col, db_col in _CSV_TO_DB_COLS.items()}
        out["product_nc"] = product_nc
        out["period"] = period
        # Native types
        try:
            out["flow"] = int(out["flow"]) if out["flow"] is not None else None
        except (TypeError, ValueError):
            log.warning("Unparseable FLOW=%r in row %s/%s/%s", out["flow"], reporter, partner, product_nc)
            continue
        for col in _NUMERIC_DB_COLS:
            out[col] = _to_float(out[col])
        yield out


def aggregate_to_observations(
    period: date,
    indexed_rows: Iterable[tuple[int | None, dict]],
) -> Iterator[dict]:
    """Aggregate raw rows by (reporter, partner, product_nc, flow), sum measures.

    `indexed_rows` is `(raw_row_id, raw_row_dict)` pairs. The id may be None for
    pre-DB usage (e.g. tests). Output observation dicts carry
    `eurostat_raw_row_ids`: the list of raw row ids that aggregated into the cell.
    """
    period_iso = period.replace(day=1).isoformat()

    agg: dict[tuple, dict] = {}
    for raw_id, raw in indexed_rows:
        key = (raw["reporter"], raw["partner"], raw["product_nc"], raw["flow"])
        bucket = agg.setdefault(key, {
            "period": period_iso,
            "period_kind": "monthly",
            "flow": _flow_label(raw["flow"]),
            "reporter_country": raw["reporter"],
            "partner_country": raw["partner"],
            "hs_code": raw["product_nc"],
            "value": 0.0,
            "currency": "EUR",
            "_quantity_kg": 0.0,
            "_quantity_supp": 0.0,
            "quantity_unit": raw.get("suppl_unit") or "kg",
            "eurostat_raw_row_ids": [],
            "source_row": {
                "_method": "aggregated by (reporter, partner, product_nc, flow); summed VALUE_EUR / QUANTITY_KG / QUANTITY_SUPPL_UNIT across STAT_PROCEDURE",
                "_n_raw_rows": 0,
            },
        })
        bucket["value"] += raw.get("value_eur") or 0.0
        bucket["_quantity_kg"] += raw.get("quantity_kg") or 0.0
        bucket["_quantity_supp"] += raw.get("quantity_suppl_unit") or 0.0
        bucket["source_row"]["_n_raw_rows"] += 1
        if raw_id is not None:
            bucket["eurostat_raw_row_ids"].append(raw_id)

    for bucket in agg.values():
        # Use supplementary unit when present, else fall back to kg.
        bucket["quantity"] = bucket["_quantity_supp"] if bucket["_quantity_supp"] else bucket["_quantity_kg"]
        del bucket["_quantity_kg"], bucket["_quantity_supp"]
        if not bucket["eurostat_raw_row_ids"]:
            # In test/no-DB mode, leave the array empty so the downstream still inserts cleanly.
            bucket["eurostat_raw_row_ids"] = None
        yield bucket


def iter_observations(
    archive_bytes: bytes,
    period: date,
    partners: set[str] | None = None,
    reporters: set[str] | None = None,
    hs_prefixes: tuple[str, ...] | None = None,
) -> Iterator[dict]:
    """Convenience: raw → aggregated, no DB ids attached. Useful for tests and
    one-off inspections; the DB pipeline goes through iter_raw_rows + insert +
    aggregate_to_observations so it can capture raw-row ids on the observations."""
    raws = list(iter_raw_rows(archive_bytes, period, partners, reporters, hs_prefixes))
    yield from aggregate_to_observations(period, [(None, r) for r in raws])


# EU-27 ISO-2 partner codes. When these appear as PARTNER in a Eurostat
# bulk file row, the row represents intra-EU trade (one EU member importing
# from another EU member). For the editorial register Soapbox uses —
# "China supplied X% of EU imports of Y" — the implicit denominator is
# **extra-EU imports** (imports from non-EU partners). Including intra-EU
# in the denominator would conflate "what fraction of EU consumption is
# Chinese" (much smaller) with "what fraction of non-EU imports is Chinese"
# (the editorial point). aggregate_to_world_totals filters these out by
# default so the resulting eurostat_world_aggregates rows are extra-EU
# totals.
EU27_PARTNER_CODES: frozenset[str] = frozenset({
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR",
    "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK",
    "SI", "ES", "SE",
})


def aggregate_to_world_totals(
    archive_bytes: bytes,
    period: date,
    hs_prefixes: tuple[str, ...] | None = None,
    reporters: set[str] | None = None,
    exclude_partners: frozenset[str] | None = None,
) -> Iterator[dict]:
    """Stream the bulk file once and emit one aggregate row per
    (reporter, product_nc, flow) summing across all non-excluded partner
    codes. The output rows populate `eurostat_world_aggregates` and act
    as the denominator for the partner_share metric.

    Default `exclude_partners = EU27_PARTNER_CODES` — i.e. the
    aggregator filters intra-EU trade so the denominator is
    extra-EU imports (the Soapbox-style "X% of EU imports from outside
    the EU"). Pass `exclude_partners=frozenset()` to retain intra-EU
    and get a "X% of EU consumption" denominator instead — a different
    editorial question, rarely the one journalists ask.

    Memory-conscious: streams via `iter_raw_rows` (which already streams
    the CSV) and aggregates in a single pass dict-keyed by
    (reporter, product_nc, flow). For our typical hs_prefixes set (the
    HS patterns covered by `hs_groups`), the resulting dict is small
    (low millions of distinct keys at most) and fits in memory.

    `hs_prefixes`: restrict the aggregation to the HS prefixes we care
    about. None = aggregate ALL HS codes (large memory + storage).
    `reporters`: restrict the aggregation to specific EU reporters.
    None = aggregate over every reporter the bulk file contains.

    Each emitted dict has the columns of `eurostat_world_aggregates`
    (minus the auto-populated id / scrape_run_id / computed_at). Caller
    inserts via `db.bulk_upsert_eurostat_world_aggregates`.
    """
    if exclude_partners is None:
        exclude_partners = EU27_PARTNER_CODES
    agg: dict[tuple[str, str, int], dict] = {}
    partners_seen: dict[tuple[str, str, int], set[str]] = {}
    for raw in iter_raw_rows(
        archive_bytes, period, partners=None, reporters=reporters, hs_prefixes=hs_prefixes,
    ):
        if raw["partner"] in exclude_partners:
            continue
        key = (raw["reporter"], raw["product_nc"], raw["flow"])
        bucket = agg.setdefault(key, {
            "period": period.replace(day=1),
            "reporter": raw["reporter"],
            "product_nc": raw["product_nc"],
            "flow": raw["flow"],
            "value_eur": 0.0,
            "quantity_kg": 0.0,
            "quantity_suppl_unit": 0.0,
            "n_raw_rows": 0,
        })
        bucket["value_eur"] += raw.get("value_eur") or 0.0
        bucket["quantity_kg"] += raw.get("quantity_kg") or 0.0
        bucket["quantity_suppl_unit"] += raw.get("quantity_suppl_unit") or 0.0
        bucket["n_raw_rows"] += 1
        partners_seen.setdefault(key, set()).add(raw["partner"])

    for key, bucket in agg.items():
        bucket["n_partners_summed"] = len(partners_seen[key])
        yield bucket


def _flow_label(raw: int | str | None) -> str:
    """Eurostat FLOW: 1 = import, 2 = export."""
    return {1: "import", 2: "export"}.get(raw, f"flow_{raw}")


def _to_float(s) -> float | None:
    if s is None or s == "":
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None
