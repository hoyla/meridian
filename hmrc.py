"""HMRC OTS (Overseas Trade Statistics) ingest via the OData REST API.

Phase 6.1 — UK-side counterpart to eurostat.py. The Guardian publishes from
London, and Eurostat dropped the UK after Brexit (full exit 2021-01); this
module fetches UK-side trade with non-EU partners (CN + HK + MO, matching
`anomalies.EUROSTAT_PARTNERS`) so the analyser can build a UK-only or
EU-27-plus-UK comparison alongside the existing EU-27 view.

API: https://api.uktradeinfo.com/OTS — public, unauthenticated, OData v4.
The OTS entity is the per-(commodity, country, port, flow, period) fact
table. Currency is GBP; we convert to EUR at ingest using the period's
GBP/EUR FX rate from `fx_rates` so analyser queries can sum across
sources without per-row FX lookups.

Pipeline:

    fetch_ots_for_period(period, country_ids=...)
        ↓ OData GET (paginated via @odata.nextLink)
    iter_raw_rows(api_rows, period, fx_rate)
        ↓ map CountryId → ISO-2, normalise flow, convert GBP → EUR
        yields one dict per row (keys lower-cased to match DB columns)
    db.bulk_insert_hmrc_raw_rows(scrape_run_id, raws) → list[int] of inserted ids
    aggregate_to_observations(period, [(raw_id, raw_dict), ...])
        ↓ group by (partner, product_nc, flow), sum across ports
        yields aggregated observation dicts carrying hmrc_raw_row_ids in source_row
    db.upsert_observations(run_id, release_id, observations)

Two-stage layering keeps source data immutable + queryable in `hmrc_raw_rows`,
the comparable per-cell view in `observations`. Same pattern as eurostat.py.

HMRC-specific notes:
- FlowTypeId is HMRC's native code: 1=EU Imports, 2=EU Exports, 3=Non-EU
  Imports, 4=Non-EU Exports. For China, HK, MO (all non-EU) only 3 and 4
  appear. Normalised `flow` collapses imports (1+3) and exports (2+4) so
  the analyser can use the same predicate as for Eurostat.
- SuppressionIndex flags rows where Value is suppressed for confidentiality
  (small numbers from few traders). We preserve it in raw rows; the
  aggregator excludes suppressed rows from the sum.
- Cn8Code can be 2-8 digits in the API response (HS-chapter-level entries
  have shorter codes). We zero-pad to 8 chars to match Eurostat convention.
- $count endpoint is rate-limited / disallowed; pagination uses
  @odata.nextLink instead of pre-counting.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterable, Iterator
from datetime import date

import httpx

from api_client import FetchResult

log = logging.getLogger(__name__)

ODATA_BASE = "https://api.uktradeinfo.com"
DEFAULT_TIMEOUT = 120.0
DEFAULT_PAGE_SIZE = 10000

# Partner CountryIds for "Chinese trade" — CN plus the two SARs. Matches
# `anomalies.EUROSTAT_PARTNERS` (CN+HK+MO) so EU-27 + UK combined-scope
# findings sum the same partner envelope on both sides.
DEFAULT_COUNTRY_IDS: tuple[int, ...] = (720, 740, 743)  # CN, HK, MO

# CountryId → ISO-2 lookup. Extended as new partners are added; the
# uktradeinfo `Country` endpoint is the source of truth (use it to discover
# new IDs). Hardcoded here to avoid an extra round-trip per ingest.
COUNTRY_ID_TO_ISO: dict[int, str] = {
    720: "CN",   # China
    740: "HK",   # Hong Kong
    743: "MO",   # Macao
}


def ots_query_url(
    period: date,
    country_ids: tuple[int, ...],
    page_size: int,
    skip: int = 0,
) -> str:
    """Build an OData query URL for one period × country set, optionally
    skipping the first `skip` rows for pagination.

    Two HMRC-API quirks worth knowing:
    - The `in (a,b,c)` operator is rejected with HTTP 403; we use chained
      `or` predicates. Single-country collapses to a bare `eq`.
    - The API does NOT emit `@odata.nextLink` — pagination needs explicit
      `$skip` increments by `page_size` until a short page comes back."""
    month_id = period.year * 100 + period.month
    if len(country_ids) == 1:
        country_filter = f"CountryId eq {country_ids[0]}"
    else:
        ored = " or ".join(f"CountryId eq {c}" for c in country_ids)
        country_filter = f"({ored})"
    query = (
        f"$filter=MonthId eq {month_id} and {country_filter}"
        f"&$expand=Commodity($select=CommodityId,Cn8Code,Hs2Code,Hs4Code,Hs6Code)"
        f"&$top={page_size}"
    )
    if skip > 0:
        query += f"&$skip={skip}"
    return f"{ODATA_BASE}/OTS?{query}"


def fetch_ots_for_period(
    period: date,
    country_ids: tuple[int, ...] = DEFAULT_COUNTRY_IDS,
    page_size: int = DEFAULT_PAGE_SIZE,
    timeout: float = DEFAULT_TIMEOUT,
) -> FetchResult:
    """Fetch all OTS rows for one (period, country_ids) combination.
    Paginates via explicit `$skip` (HMRC API does not emit
    @odata.nextLink). Returns a FetchResult whose `content` is JSON
    bytes — the union of all page responses concatenated as
    `{"value": [...]}` so downstream parsers don't have to know about
    pagination."""
    initial_url = ots_query_url(period, country_ids, page_size)
    all_rows: list[dict] = []
    page = 0
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        while True:
            url = ots_query_url(period, country_ids, page_size, skip=page * page_size)
            log.info(
                "HMRC OTS page %d (skip=%d): fetching",
                page + 1, page * page_size,
            )
            r = client.get(url)
            r.raise_for_status()
            data = r.json()
            page_rows = data.get("value", [])
            all_rows.extend(page_rows)
            page += 1
            # Short page = last page.
            if len(page_rows) < page_size:
                break
    log.info(
        "HMRC OTS for %s: fetched %d rows across %d page(s)",
        period.strftime("%Y-%m"), len(all_rows), page,
    )
    import json
    body = json.dumps({"value": all_rows}).encode("utf-8")
    return FetchResult(
        url=initial_url,
        status_code=200,
        content_type="application/json",
        content=body,
        sha256=hashlib.sha256(body).hexdigest(),
    )


def iter_raw_rows(
    api_response_bytes: bytes,
    period: date,
    fx_rate_gbp_eur: float,
    country_ids: tuple[int, ...] | None = None,
) -> Iterator[dict]:
    """Yield one dict per OTS row. Keys match hmrc_raw_rows columns.
    `fx_rate_gbp_eur` is the EUR-per-GBP rate for the period (looked up by
    the caller from `fx_rates`); we apply it to compute value_eur at ingest
    so analyser queries don't have to do per-row FX lookups."""
    import json
    period = period.replace(day=1)
    data = json.loads(api_response_bytes)
    rows = data.get("value", [])
    skipped_unknown_country = 0
    for row in rows:
        country_id = row.get("CountryId")
        partner = COUNTRY_ID_TO_ISO.get(country_id)
        if partner is None:
            # Defensive — a country we didn't pre-register. Skip with a log.
            skipped_unknown_country += 1
            continue
        if country_ids is not None and country_id not in country_ids:
            continue
        commodity = row.get("Commodity") or {}
        cn8 = (commodity.get("Cn8Code") or "").zfill(8)
        flow_type_id = row.get("FlowTypeId")
        # Normalise: HMRC 1=EU Imp, 2=EU Exp, 3=NonEU Imp, 4=NonEU Exp.
        # Collapse to our convention: 1=any import, 2=any export.
        flow = 1 if flow_type_id in (1, 3) else 2
        value_gbp = row.get("Value")
        value_eur = (
            float(value_gbp) * fx_rate_gbp_eur if value_gbp is not None else None
        )
        yield {
            "period": period,
            "reporter": "GB",
            "partner": partner,
            "product_nc": cn8,
            "product_hs6": commodity.get("Hs6Code"),
            "product_hs4": commodity.get("Hs4Code"),
            "product_hs2": commodity.get("Hs2Code"),
            "flow_type_id": flow_type_id,
            "flow": flow,
            "suppression_index": row.get("SuppressionIndex") or 0,
            "port_id": row.get("PortId"),
            "value_gbp": value_gbp,
            "value_eur": value_eur,
            "net_mass_kg": row.get("NetMass"),
            "suppl_unit": row.get("SuppUnit"),
        }
    if skipped_unknown_country:
        log.warning(
            "Skipped %d HMRC rows with unknown CountryId; extend "
            "COUNTRY_ID_TO_ISO if these are partners we want to ingest.",
            skipped_unknown_country,
        )


def aggregate_to_observations(
    period: date,
    indexed_rows: Iterable[tuple[int | None, dict]],
) -> Iterator[dict]:
    """Aggregate raw rows by (reporter, partner, product_nc, flow) summing
    value_eur, value_gbp, net_mass_kg across UK ports. Mirrors
    eurostat.aggregate_to_observations in shape so the same observations
    table consumes both sources cleanly.

    Suppressed rows (suppression_index != 0) are excluded from the sum but
    their ids are retained in source_row.suppressed_raw_row_ids so the
    audit trail captures them. Editorially: HMRC suppresses small-trader
    flows for confidentiality; surfacing how much was suppressed (count +
    sum if available) is forward work but the audit trail is here.
    """
    period_iso = period.replace(day=1).isoformat()

    agg: dict[tuple, dict] = {}
    for raw_id, raw in indexed_rows:
        key = (raw["reporter"], raw["partner"], raw["product_nc"], raw["flow"])
        bucket = agg.setdefault(key, {
            "period": period_iso,
            "period_kind": "monthly",
            "flow": "import" if raw["flow"] == 1 else "export",
            "reporter_country": raw["reporter"],
            "partner_country": raw["partner"],
            "hs_code": raw["product_nc"],
            "value": 0.0,
            "currency": "EUR",
            "_value_gbp": 0.0,
            "_quantity_kg": 0.0,
            "quantity_unit": "kg",
            "hmrc_raw_row_ids": [],
            "_suppressed_raw_row_ids": [],
            "source_row": {
                "_method": "aggregated by (reporter, partner, product_nc, flow); "
                           "summed Value (GBP→EUR via period FX) and NetMass across "
                           "UK ports; SuppressionIndex != 0 rows excluded from sum",
                "_n_raw_rows": 0,
                "_source": "hmrc_ots",
            },
        })
        if raw.get("suppression_index", 0) != 0:
            if raw_id is not None:
                bucket["_suppressed_raw_row_ids"].append(raw_id)
            bucket["source_row"]["_n_raw_rows"] += 1
            continue
        bucket["value"] += raw.get("value_eur") or 0.0
        bucket["_value_gbp"] += raw.get("value_gbp") or 0.0
        bucket["_quantity_kg"] += raw.get("net_mass_kg") or 0.0
        bucket["source_row"]["_n_raw_rows"] += 1
        if raw_id is not None:
            bucket["hmrc_raw_row_ids"].append(raw_id)

    for bucket in agg.values():
        bucket["quantity"] = bucket["_quantity_kg"]
        bucket["source_row"]["_value_gbp_total"] = bucket["_value_gbp"]
        if bucket["_suppressed_raw_row_ids"]:
            bucket["source_row"]["_suppressed_raw_row_ids"] = bucket["_suppressed_raw_row_ids"]
        del bucket["_quantity_kg"], bucket["_value_gbp"], bucket["_suppressed_raw_row_ids"]
        if not bucket["hmrc_raw_row_ids"]:
            bucket["hmrc_raw_row_ids"] = None
        yield bucket


def iter_observations(
    api_response_bytes: bytes,
    period: date,
    fx_rate_gbp_eur: float,
    country_ids: tuple[int, ...] | None = None,
) -> Iterator[dict]:
    """Convenience: raw → aggregated, no DB ids attached. Useful for tests
    and one-off inspections; the DB pipeline goes through iter_raw_rows +
    insert + aggregate_to_observations so it can capture raw-row ids on
    the observations."""
    raws = list(iter_raw_rows(api_response_bytes, period, fx_rate_gbp_eur, country_ids))
    yield from aggregate_to_observations(period, [(None, r) for r in raws])
