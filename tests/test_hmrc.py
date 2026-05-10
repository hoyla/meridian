"""Tests for hmrc.py — the OTS OData ingest pipeline.

No live API calls — fixtures are synthetic JSON in the shape the HMRC
OData endpoint returns. Covers the three editorially load-bearing
behaviours: ISO mapping (CountryId → partner code), flow normalisation
(HMRC's 1/2/3/4 → our 1/2), GBP→EUR conversion at ingest, and
suppression handling (SuppressionIndex != 0 rows excluded from sums
but tracked in source_row for audit)."""

from __future__ import annotations

import json
from datetime import date

import hmrc as hmrc_mod


def _api_response(rows: list[dict]) -> bytes:
    """Wrap rows in the OData-ish envelope the API returns."""
    return json.dumps({"value": rows}).encode("utf-8")


def test_iter_raw_rows_maps_country_id_to_iso():
    """CountryId 720 → partner 'CN'; rows for unknown CountryIds are
    skipped silently (with a warning log)."""
    body = _api_response([
        {
            "MonthId": 202602, "FlowTypeId": 3, "SuppressionIndex": 0,
            "CommodityId": 5, "CountryId": 720, "PortId": 131,
            "Value": 100000.0, "NetMass": 5000.0, "SuppUnit": 0.0,
            "Commodity": {"Cn8Code": "85076010", "Hs2Code": "85", "Hs4Code": "8507", "Hs6Code": "850760"},
        },
        {
            "MonthId": 202602, "FlowTypeId": 3, "SuppressionIndex": 0,
            "CommodityId": 5, "CountryId": 999999, "PortId": 131,
            "Value": 50000.0, "NetMass": 1000.0, "SuppUnit": 0.0,
            "Commodity": {"Cn8Code": "85076010", "Hs2Code": "85", "Hs4Code": "8507", "Hs6Code": "850760"},
        },
    ])
    rows = list(hmrc_mod.iter_raw_rows(body, date(2026, 2, 1), fx_rate_gbp_eur=1.18))
    assert len(rows) == 1, "unknown CountryId should be silently skipped"
    assert rows[0]["partner"] == "CN"
    assert rows[0]["reporter"] == "GB"


def test_iter_raw_rows_normalises_flow_and_converts_currency():
    """FlowTypeId 1 (EU Imports) and 3 (Non-EU Imports) both → flow=1;
    FlowTypeId 2 and 4 → flow=2. value_eur = value_gbp * fx_rate at
    ingest, computed once per period."""
    body = _api_response([
        {"MonthId": 202602, "FlowTypeId": 3, "CountryId": 720, "PortId": 1,
         "Value": 1000.0, "NetMass": 100.0, "SuppUnit": 0.0, "SuppressionIndex": 0,
         "Commodity": {"Cn8Code": "85076010"}},
        {"MonthId": 202602, "FlowTypeId": 4, "CountryId": 720, "PortId": 1,
         "Value": 2000.0, "NetMass": 200.0, "SuppUnit": 0.0, "SuppressionIndex": 0,
         "Commodity": {"Cn8Code": "85076010"}},
    ])
    rows = list(hmrc_mod.iter_raw_rows(body, date(2026, 2, 1), fx_rate_gbp_eur=1.20))
    assert rows[0]["flow"] == 1 and rows[0]["flow_type_id"] == 3
    assert rows[1]["flow"] == 2 and rows[1]["flow_type_id"] == 4
    assert rows[0]["value_gbp"] == 1000.0
    assert rows[0]["value_eur"] == 1200.0
    assert rows[1]["value_eur"] == 2400.0


def test_iter_raw_rows_zero_pads_short_cn8():
    """HMRC's Cn8Code can be 2-8 digits (HS-chapter-level rows like '05').
    We zero-pad to 8 chars so the analyser's prefix-LIKE patterns match
    the same shape as Eurostat-side product_nc."""
    body = _api_response([
        {"MonthId": 202602, "FlowTypeId": 3, "CountryId": 720, "PortId": 1,
         "Value": 100.0, "NetMass": 10.0, "SuppUnit": 0.0, "SuppressionIndex": 0,
         "Commodity": {"Cn8Code": "05"}},
    ])
    rows = list(hmrc_mod.iter_raw_rows(body, date(2026, 2, 1), fx_rate_gbp_eur=1.0))
    assert rows[0]["product_nc"] == "00000005"


def test_aggregate_sums_across_ports_and_excludes_suppressed():
    """Two ports, three rows for the same (partner, product_nc, flow):
    one suppressed (SuppressionIndex=1) should be excluded from the
    sum but tracked in source_row.suppressed_raw_row_ids."""
    body = _api_response([
        {"MonthId": 202602, "FlowTypeId": 3, "CountryId": 720, "PortId": 1,
         "Value": 1000.0, "NetMass": 100.0, "SuppUnit": 0.0, "SuppressionIndex": 0,
         "Commodity": {"Cn8Code": "85076010"}},
        {"MonthId": 202602, "FlowTypeId": 3, "CountryId": 720, "PortId": 2,
         "Value": 500.0, "NetMass": 50.0, "SuppUnit": 0.0, "SuppressionIndex": 0,
         "Commodity": {"Cn8Code": "85076010"}},
        {"MonthId": 202602, "FlowTypeId": 3, "CountryId": 720, "PortId": 3,
         "Value": 99999.0, "NetMass": 9999.0, "SuppUnit": 0.0, "SuppressionIndex": 1,
         "Commodity": {"Cn8Code": "85076010"}},
    ])
    raws = list(hmrc_mod.iter_raw_rows(body, date(2026, 2, 1), fx_rate_gbp_eur=1.20))
    indexed = [(i + 1, r) for i, r in enumerate(raws)]
    obs = list(hmrc_mod.aggregate_to_observations(date(2026, 2, 1), indexed))

    assert len(obs) == 1
    o = obs[0]
    # Sum of unsuppressed: 1000 + 500 = 1500 GBP → 1800 EUR
    assert o["value"] == 1800.0
    assert o["source_row"]["_value_gbp_total"] == 1500.0
    assert o["quantity"] == 150.0  # 100 + 50
    # Three raw rows seen, but only the two unsuppressed contributed to the FK array.
    assert o["source_row"]["_n_raw_rows"] == 3
    assert sorted(o["hmrc_raw_row_ids"]) == [1, 2]
    assert o["source_row"]["_suppressed_raw_row_ids"] == [3]
    # Standard observation fields populated.
    assert o["partner_country"] == "CN"
    assert o["reporter_country"] == "GB"
    assert o["hs_code"] == "85076010"
    assert o["flow"] == "import"
    assert o["currency"] == "EUR"


def test_aggregate_separates_imports_from_exports():
    """Same partner + commodity but different flows → separate observations."""
    body = _api_response([
        {"MonthId": 202602, "FlowTypeId": 3, "CountryId": 720, "PortId": 1,
         "Value": 1000.0, "NetMass": 100.0, "SuppUnit": 0.0, "SuppressionIndex": 0,
         "Commodity": {"Cn8Code": "85076010"}},
        {"MonthId": 202602, "FlowTypeId": 4, "CountryId": 720, "PortId": 1,
         "Value": 200.0, "NetMass": 20.0, "SuppUnit": 0.0, "SuppressionIndex": 0,
         "Commodity": {"Cn8Code": "85076010"}},
    ])
    obs = list(hmrc_mod.iter_observations(body, date(2026, 2, 1), fx_rate_gbp_eur=1.0))
    assert len(obs) == 2
    flows = sorted(o["flow"] for o in obs)
    assert flows == ["export", "import"]


def test_ots_query_url_format():
    """The OData query URL uses HMRC's expected $filter / $expand / $top syntax.
    Multi-country uses chained `or` (the `in (a,b,c)` operator is rejected by
    the API with HTTP 403). Single-country collapses to a bare `eq`."""
    multi = hmrc_mod.ots_query_url(date(2026, 2, 1), (720, 740, 743), 5000)
    assert multi.startswith("https://api.uktradeinfo.com/OTS?")
    assert "MonthId eq 202602" in multi
    assert "(CountryId eq 720 or CountryId eq 740 or CountryId eq 743)" in multi
    assert "$expand=Commodity" in multi
    assert "$top=5000" in multi
    # Single country: no parens, plain eq
    single = hmrc_mod.ots_query_url(date(2026, 2, 1), (720,), 100)
    assert "and CountryId eq 720" in single
    assert " or " not in single
