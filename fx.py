"""ECB foreign-exchange rate fetcher.

Pulls monthly-average reference rates from the ECB Statistical Data Warehouse
and persists them into `fx_rates`. The conversion direction is normalised so
that `amount_in_to = amount_in_from * rate` holds — ECB publishes
{currency_from}-per-EUR, we store the inverse (EUR-per-{currency_from}) so the
DB shape and our `lookup_fx` helper stay consistent.

Usage:
    fx.populate_fx_rates_from_ecb('CNY', since=date(2024, 1, 1))
    fx.populate_fx_rates_from_ecb('USD')

Or via the CLI: `python scrape.py --fetch-fx CNY [--fx-since 2024-01]`

ECB API is open, no key required. SDMX-JSON format. Endpoint:
    https://data-api.ecb.europa.eu/service/data/EXR/M.{CCY}.EUR.SP00.A?format=jsondata
where M = monthly, SP00.A = foreign exchange reference rate / average.
"""

import json
import logging
from dataclasses import dataclass
from datetime import date

import httpx

import db

log = logging.getLogger(__name__)

ECB_BASE = (
    "https://data-api.ecb.europa.eu/service/data/EXR"
    "/M.{currency}.EUR.SP00.A?format=jsondata"
)
DEFAULT_TIMEOUT = 30.0
RATE_SOURCE_LABEL = "ECB monthly average"


@dataclass
class FxRateRecord:
    """One ready-to-insert FX rate, in our DB shape (rate is EUR per `currency_from`)."""
    currency_from: str
    currency_to: str
    rate_date: date
    rate: float
    rate_source: str
    rate_source_url: str
    notes: str


def fetch_ecb_monthly_rates(
    currency_from: str,
    since: date | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> list[FxRateRecord]:
    """Fetch monthly average exchange rates for {currency_from}/EUR from ECB.

    Returns records in our DB shape: rate_date is the first of the period month,
    rate is EUR per `currency_from` (inverse of ECB's reported value), notes
    record what ECB published so the inversion is auditable.
    """
    url = ECB_BASE.format(currency=currency_from.upper())
    if since:
        url += f"&startPeriod={since.year}-{since.month:02d}"

    log.info("Fetching ECB rates: %s", url)
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        r = client.get(url)
    r.raise_for_status()
    return parse_ecb_response(r.text, currency_from=currency_from, source_url=url)


def parse_ecb_response(json_text: str, currency_from: str, source_url: str) -> list[FxRateRecord]:
    """Parse an SDMX-JSON response into FxRateRecord rows. Pure function — no IO."""
    data = json.loads(json_text)
    obs_dim_values = data["structure"]["dimensions"]["observation"][0]["values"]
    series_dict = data.get("dataSets", [{}])[0].get("series", {})
    if not series_dict:
        log.warning("ECB response has no series data for %s", currency_from)
        return []
    # Our query (M.{ccy}.EUR.SP00.A) yields a single series.
    series = next(iter(series_dict.values()))
    observations = series.get("observations", {})

    out: list[FxRateRecord] = []
    for idx_str, vals in observations.items():
        idx = int(idx_str)
        period_id = obs_dim_values[idx]["id"]   # 'YYYY-MM'
        ecb_value = vals[0]
        if ecb_value is None or ecb_value == 0:
            log.warning("Skipping empty/zero ECB value at %s for %s", period_id, currency_from)
            continue
        year, month = period_id.split("-")
        rate_date = date(int(year), int(month), 1)
        rate_eur_per_unit = 1.0 / float(ecb_value)
        out.append(FxRateRecord(
            currency_from=currency_from.upper(),
            currency_to="EUR",
            rate_date=rate_date,
            rate=rate_eur_per_unit,
            rate_source=RATE_SOURCE_LABEL,
            rate_source_url=source_url,
            notes=(
                f"ECB published {currency_from.upper()}/EUR = {ecb_value} "
                f"(monthly average for {period_id}); "
                f"we store the inverse (EUR per {currency_from.upper()} = {rate_eur_per_unit:.8f}) "
                f"so amount_in_eur = amount_in_{currency_from.lower()} * rate."
            ),
        ))
    return out


def populate_fx_rates_from_ecb(
    currency_from: str,
    since: date | None = None,
) -> dict[str, int]:
    """Fetch ECB rates and INSERT ON CONFLICT DO NOTHING. Returns counts."""
    rates = fetch_ecb_monthly_rates(currency_from, since=since)
    counts = {"inserted": 0, "skipped_existing": 0, "total_fetched": len(rates)}
    if not rates:
        return counts
    with db.transaction() as conn, conn.cursor() as cur:
        for r in rates:
            cur.execute(
                """
                INSERT INTO fx_rates (currency_from, currency_to, rate_date, rate, rate_source, rate_source_url, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (currency_from, currency_to, rate_date, rate_source) DO NOTHING
                """,
                (r.currency_from, r.currency_to, r.rate_date, r.rate,
                 r.rate_source, r.rate_source_url, r.notes),
            )
            if cur.rowcount > 0:
                counts["inserted"] += 1
            else:
                counts["skipped_existing"] += 1
    return counts
