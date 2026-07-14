"""Tests for the GACC commodity-highlights page block (PR C of
dev_notes/2026-07-14-gacc-commodity-highlights.md): unit humanisation, the
milestone/run-rate computed facts (the deterministic 'takes' layer), section
selection (headline floor / watchlist tier / aggregate exclusion), the
provenance-drawer arithmetic, and the render placement between "Since the
last read" and "Europe up close" (Luke, 2026-07-14)."""

from __future__ import annotations

import json
from datetime import date

import psycopg2
import pytest

import findings_io
import provenance_payload
import report_builder as rb
import report_model as rm
from report_render_html import render_html


# ---------------------------------------------------------------------------
# Pure helpers.
# ---------------------------------------------------------------------------

def test_qty_units_absolute_parses_scaled_units():
    assert rb._qty_units_absolute(98.8, "10,000 Autos") == (988000.0, "Autos")
    assert rb._qty_units_absolute(307.3, "100 Million PCS") == (307.3e8, "PCS")
    assert rb._qty_units_absolute(17.0, "Craft") == (17.0, "Craft")
    assert rb._qty_units_absolute(5.0, None) == (5.0, "")


def test_fmt_units_tiers():
    assert rb._fmt_units(988000.0) == "988k"
    assert rb._fmt_units(3.073e10) == "30.7bn"
    assert rb._fmt_units(5490.0) == "5k"
    assert rb._fmt_units(17.0) == "17"


def test_milestone_rungs_ladder():
    # 1/2/5 × 10^k rungs at or below the value.
    assert rb._milestone_rungs(1.06e6)[-1] == 1e6
    assert 5e5 in rb._milestone_rungs(1.06e6)
    assert rb._milestone_rungs(0.5) == []


# ---------------------------------------------------------------------------
# Render placement + surface (no DB).
# ---------------------------------------------------------------------------

def _commodities_section(**over) -> rm.Section:
    sec = rm.Section(
        id="gacc-commodities",
        title="What’s moving — GACC’s headline commodities",
        kind="gacc_commodities",
        intro="China’s trade by product.",
        about="**What this is.** Test copy.",
    )
    sec.metrics = {
        "period": "2026-05-01",
        "rows": [{
            "label": "Motor vehicles", "flow": "export", "finding_id": 91,
            "sm_value_yoy": 0.331, "sm_quantity_yoy": 0.426,
            "eur_month": 1.45e10, "quantity_unit": "10,000 Autos",
            "quantity_display": "0.99mn autos",
            "ytd_value_yoy": 0.455, "published_ytd_yoy_pct": 45.5,
            "caveats": [],
            "facts": {"milestone": {
                "text": "First month above 1.0mn autos in our records "
                        "(catalogue line tracked from 2019)",
                "threshold_units": 1e6, "month_units": 1.06e6,
                "method": "round-number crossing"}},
        }],
        "watchlist": [{
            "label": "Rare-earth ore, metals, compounds", "flow": "export",
            "finding_id": 92, "sm_value_yoy": -0.34, "sm_quantity_yoy": None,
            "eur_month": 5.4e7, "quantity_unit": "Ton",
            "ytd_value_yoy": -0.064, "published_ytd_yoy_pct": -6.4,
            "caveats": ["low_base_effect"],
        }],
    }
    for k, v in over.items():
        setattr(sec, k, v)
    return sec


def _minimal_gacc_page(commodities=None) -> rm.GaccPage:
    return rm.GaccPage(
        data_period=date(2026, 5, 1),
        tab_label="GACC-only (May 2026)",
        identity={"published": "2026-06-10", "confirmation_due": None,
                  "source_url": "https://english.customs.gov.cn/x.html",
                  "source_url_zh": None},
        since_last={"prev_period": "2026-04-01", "rows": []},
        europe=rm.Section(id="gacc-europe", title="Europe up close",
                          kind="gacc_bilateral", intro="China’s own numbers.",
                          metrics={}),
        commodities=commodities,
        understanding="**An early read.** Test copy.",
    )


def _render(page: rm.GaccPage) -> str:
    report = rm.Report(
        meta=rm.ReportMeta(data_period=date(2026, 4, 1), variant="eurostat",
                           snapshot_id="t", generated_at=None),
        headline=rm.Headline(variant="eurostat", lead_title="t", note="",
                             items=[]),
        gacc_page=page,
    )
    return render_html(report)


def test_commodities_renders_between_since_last_and_europe():
    html = _render(_minimal_gacc_page(commodities=_commodities_section()))
    i_since = html.index('id="gacc-sincelast"')
    i_com = html.index('id="gacc-commodities"')
    i_eur = html.index('id="gacc-europe"')
    assert i_since < i_com < i_eur
    # subnav entry, in the same order
    assert html.index('data-spy="gacc-sincelast"') \
        < html.index('data-spy="gacc-commodities"') \
        < html.index('data-spy="gacc-europe"')


def test_commodities_surface_carries_basis_facts_and_tiers():
    html = _render(_minimal_gacc_page(commodities=_commodities_section()))
    assert "CNY&nbsp;terms" in html           # basis named in the header
    assert "First month above 1.0mn autos" in html   # milestone chip
    assert "Also notable — smaller lines, big swings" in html  # watchlist tier
    assert "+42.6%" in html                    # volume rate beside value rate
    assert "finding/91" in html and "finding/92" in html


def test_no_commodities_section_no_render():
    html = _render(_minimal_gacc_page(commodities=None))
    assert 'id="gacc-commodities"' not in html
    assert 'data-spy="gacc-commodities"' not in html


# ---------------------------------------------------------------------------
# Provenance-drawer arithmetic (pure).
# ---------------------------------------------------------------------------

def test_drawer_arithmetic_for_commodity_finding():
    detail = {
        "commodity": {"label": "Motor vehicles", "is_aggregate": False,
                      "quantity_unit": "10,000 Autos"},
        "totals": {
            "single_month": {
                "current_value_cny": 1149.2, "prior_value_cny": 863.3,
                "value_yoy_pct": 0.331, "current_quantity": 98.8,
                "prior_quantity": 69.3, "quantity_yoy_pct": 0.426,
                "prior_derivation": "prior_ytd_adjacent_page_difference",
            },
            "ytd_cumulative": {"value_yoy_pct": 0.455,
                               "published_yoy_value_pct": 45.5},
            "eur_month": 1.45e10,
        },
    }
    lines = provenance_payload._arithmetic("gacc_commodity_yoy", detail)
    text = " ".join(lines)
    assert "CNY 1,149.2" in text and "derived CNY 863.3" in text
    assert "difference of two prior-year cumulative columns" in text
    assert "By volume" in text and "98.8" in text
    assert "GACC's own published figure is 45.5%" in text
    assert "no country split" in text
    assert "CNY-denominated" in text


# ---------------------------------------------------------------------------
# DB-backed: selection + computed facts.
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _direct_db_url(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)


@pytest.fixture
def empty_op_tables(test_db_url):
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE findings, observations, source_snapshots, eurostat_raw_rows, "
            "scrape_runs, releases, fx_rates RESTART IDENTITY CASCADE"
        )
    yield


def _emit_commodity_finding(cur, label, *, is_aggregate=False, flow="export",
                            sm_yoy=0.30, eur_month=2e9, qty=None, qty_unit=None,
                            period="2026-05-01"):
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES ('analysis://t', 'success') "
        "RETURNING id")
    run_id = cur.fetchone()[0]
    sm = {"current_value_cny": 100.0, "prior_value_cny": 100.0 / (1 + sm_yoy),
          "value_yoy_pct": sm_yoy, "current_quantity": qty,
          "prior_quantity": None, "quantity_yoy_pct": None,
          "prior_derivation": "prior_ytd_adjacent_page_difference"}
    detail = {
        "commodity": {"label": label, "is_aggregate": is_aggregate,
                      "quantity_unit": qty_unit},
        "windows": {"current_end": period},
        "totals": {"single_month": sm, "ytd_cumulative": None,
                   "eur_month": eur_month, "eur_ytd": None},
        "caveat_codes": ["cny_denominated"],
    }
    subkind = "gacc_commodity_yoy" + ("" if flow == "export" else "_import")
    findings_io.emit_finding(
        cur, scrape_run_id=run_id, kind="anomaly", subkind=subkind,
        natural_key=findings_io.nk_gacc_commodity_yoy(
            label, is_aggregate, period[:7]),
        value_fields={"sm_value_yoy_pct": sm_yoy},
        observation_ids=[], score=abs(sm_yoy),
        title=f"t {label}", body="t", detail=detail,
    )


def test_selection_floors_tiers_and_aggregate_exclusion(
        empty_op_tables, test_db_url):
    period = date(2026, 5, 1)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        # Headline candidates (≥ €1bn month):
        _emit_commodity_finding(cur, "Big mover", sm_yoy=0.80, eur_month=5e9)
        _emit_commodity_finding(cur, "Big steady", sm_yoy=0.02, eur_month=8e9)
        # Aggregate — must never appear however big it moves:
        _emit_commodity_finding(cur, "Mech & elec", is_aggregate=True,
                                sm_yoy=2.0, eur_month=9e10)
        # Watchlist: small line, big swing (the rare-earths register):
        _emit_commodity_finding(cur, "Rare earths", sm_yoy=-0.45, eur_month=6e7)
        # Below even the watchlist floor:
        _emit_commodity_finding(cur, "Tiny line", sm_yoy=0.90, eur_month=1e7)
        conn.commit()
        sec = rb._gacc_commodities_section(cur, period)

    assert sec is not None
    labels = [r["label"] for r in sec.metrics["rows"]]
    watch_labels = [r["label"] for r in sec.metrics["watchlist"]]
    assert "Big mover" in labels and "Big steady" in labels
    assert "Mech & elec" not in labels + watch_labels
    assert watch_labels == ["Rare earths"]
    assert "Tiny line" not in labels + watch_labels
    # sharpest first
    assert labels[0] == "Big mover"
    # the family-universal caveat is stripped from row chips (header carries it)
    assert all("cny_denominated" not in r["caveats"]
               for r in sec.metrics["rows"] + sec.metrics["watchlist"])


def _seed_quantity_history(cur, label, months, *, flow="export",
                           unit="10,000 Autos", start=date(2024, 1, 1),
                           quantities=None):
    """`months` monthly observations for one catalogue line, quantity 50.0
    (in unit terms) unless `quantities` supplies per-month values."""
    section = 5 if flow == "export" else 6
    for i in range(months):
        y, m = start.year + (start.month - 1 + i) // 12, (start.month - 1 + i) % 12 + 1
        p = date(y, m, 1)
        cur.execute(
            "INSERT INTO releases (source, section_number, currency, period, release_kind, "
            "source_url, unit, title, description) "
            "VALUES ('gacc', %s, 'CNY', %s, 'preliminary', %s, 'CNY 100 Million', 't', 'd') "
            "ON CONFLICT DO NOTHING RETURNING id",
            (section, p, f"http://example/s{section}-{p.isoformat()}.html"))
        row = cur.fetchone()
        if row is None:
            cur.execute("SELECT id FROM releases WHERE source='gacc' AND section_number=%s "
                        "AND currency='CNY' AND period=%s AND release_kind='preliminary'",
                        (section, p))
            row = cur.fetchone()
        rel_id = row[0]
        cur.execute("INSERT INTO scrape_runs (source_url, status) "
                    "VALUES ('http://example/t', 'success') RETURNING id")
        run_id = cur.fetchone()[0]
        q = quantities[i] if quantities else 50.0
        cur.execute(
            "INSERT INTO observations (release_id, scrape_run_id, period_kind, flow, "
            "commodity_label, quantity, quantity_unit, value_amount, value_currency, source_row) "
            "VALUES (%s, %s, 'monthly', %s, %s, %s, %s, 100.0, 'CNY', %s)",
            (rel_id, run_id, flow, label, q, unit,
             json.dumps({"is_aggregate": False})))


def _facts_row(label, qty, unit, ytd_qty=None) -> rb._GaccCommodityRow:
    detail = {
        "commodity": {"label": label, "is_aggregate": False,
                      "quantity_unit": unit},
        "windows": {"current_end": "2026-05-01"},
        "totals": {
            "single_month": {"current_quantity": qty, "value_yoy_pct": 0.3},
            "ytd_cumulative": ({"current_quantity": ytd_qty}
                               if ytd_qty is not None else None),
            "eur_month": 2e9,
        },
        "caveat_codes": [],
    }
    return rb._GaccCommodityRow(1, "gacc_commodity_yoy", detail)


def test_milestone_fires_on_round_number_crossing_with_deep_history(
        empty_op_tables, test_db_url):
    period = date(2026, 5, 1)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        # 28 prior months at ≤99.0 (i.e. below 1mn autos), then May at 106.
        _seed_quantity_history(cur, "Cars", 28, quantities=[80.0 + i * 0.5
                                                            for i in range(28)])
        conn.commit()
        facts = rb._gacc_commodity_facts(
            cur, _facts_row("Cars", 106.0, "10,000 Autos"), period)
    assert "milestone" in facts, facts
    assert facts["milestone"]["threshold_units"] == 1e6
    assert "First month above 1.00mn autos" in facts["milestone"]["text"]


def test_milestone_suppressed_on_shallow_history(empty_op_tables, test_db_url):
    period = date(2026, 5, 1)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        # Only 4 prior months — "first above N" would be meaningless.
        _seed_quantity_history(cur, "New line", 4, start=date(2026, 1, 1),
                               quantities=[5.0, 6.0, 7.0, 8.0])
        conn.commit()
        facts = rb._gacc_commodity_facts(
            cur, _facts_row("New line", 17.0, "Craft"), period)
    assert "milestone" not in facts


def test_run_rate_projects_against_prior_full_year(empty_op_tables, test_db_url):
    period = date(2026, 5, 1)
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        # Prior-year December cumulative = 710 (10,000 Autos) = 7.1mn.
        cur.execute(
            "INSERT INTO releases (source, section_number, currency, period, release_kind, "
            "source_url, unit, title, description) "
            "VALUES ('gacc', 5, 'CNY', '2025-12-01', 'preliminary', "
            "'http://example/dec.html', 'CNY 100 Million', 't', 'd') RETURNING id")
        rel_id = cur.fetchone()[0]
        cur.execute("INSERT INTO scrape_runs (source_url, status) "
                    "VALUES ('http://example/t', 'success') RETURNING id")
        run_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO observations (release_id, scrape_run_id, period_kind, flow, "
            "commodity_label, quantity, quantity_unit, value_amount, value_currency, source_row) "
            "VALUES (%s, %s, 'ytd', 'export', 'Cars', 710.0, '10,000 Autos', "
            "5000.0, 'CNY', '{}')", (rel_id, run_id))
        conn.commit()
        # YTD through May = 423.8 → pace 423.8 × 12/5 = 1017.1 ≈ 10.17mn.
        facts = rb._gacc_commodity_facts(
            cur, _facts_row("Cars", 98.8, "10,000 Autos", ytd_qty=423.8), period)
    assert "run_rate" in facts, facts
    assert facts["run_rate"]["prior_full_year_units"] == pytest.approx(7.1e6)
    assert facts["run_rate"]["pace_units"] == pytest.approx(423.8e4 * 12 / 5)
    assert "On pace for ~10.17mn autos in 2026 vs 7.10mn in 2025" \
        in facts["run_rate"]["text"]
