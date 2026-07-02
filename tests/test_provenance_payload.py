"""Tests for provenance_payload — the data behind the portal's per-number
provenance drawers (journalist-usability iteration 3, the self-verifying
portal).

Until now this module had ZERO tests despite being the defensibility surface
itself (2026-07-01 fresh review, F3 + the tests section): if the drawer's
source trail or arithmetic is wrong, the tool actively misleads the
journalist who opened it to double-check a number. Covered here:

- the source-URL trail resolves to the seeded releases, for both
  observation-based findings (trade_balance) and window-based ones
  (hs_group_yoy, which carry no observation_ids);
- the arithmetic lines restate the finding's stored values;
- drawer ↔ card format consistency: every €/day KPI's drawer restates the
  card's `formatted` string verbatim — the F3 regression lock;
- payload building is best-effort per finding: a malformed detail is
  skipped, never fatal to the snapshot.
"""
from __future__ import annotations

import json
from datetime import date

import psycopg2
import psycopg2.extras
import pytest

import anomalies
import provenance_payload


@pytest.fixture(autouse=True)
def _direct_db_url(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)


@pytest.fixture
def empty_op_tables(test_db_url):
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE findings, observations, source_snapshots, eurostat_raw_rows, "
            "scrape_runs, releases RESTART IDENTITY CASCADE"
        )
    yield


def _eu_release(cur, period: date) -> tuple[int, int]:
    cur.execute(
        "INSERT INTO releases (source, period, source_url) VALUES "
        "('eurostat', %s, %s) RETURNING id",
        (period, f"http://example/eurostat-{period}.7z"),
    )
    rel = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES (%s, 'success') RETURNING id",
        (f"http://example/eurostat-{period}.7z",),
    )
    return rel, cur.fetchone()[0]


def _obs(cur, rel, run, flow, reporter, partner, hs, val):
    cur.execute(
        "INSERT INTO observations (release_id, scrape_run_id, period_kind, flow, "
        "                          reporter_country, partner_country, hs_code, "
        "                          value_amount, value_currency, source_row) "
        "VALUES (%s, %s, 'monthly', %s, %s, %s, %s, %s, 'EUR', '{}')",
        (rel, run, flow, reporter, partner, hs, val),
    )


# 12 consecutive months ending Apr 2026 = a complete current rolling window.
_MONTHS = [date(2025, 5, 1)]
while _MONTHS[-1] < date(2026, 4, 1):
    m = _MONTHS[-1]
    _MONTHS.append(date(m.year + (m.month // 12), (m.month % 12) + 1, 1))


def _seed_full_year(conn):
    """Mirror of tests/test_trade_balance._seed_full_year: per month DE+FR
    import 000TOTAL 3bn / export 1.5bn → rolling-12mo deficit 18bn."""
    with conn, conn.cursor() as cur:
        for period in _MONTHS:
            rel, run = _eu_release(cur, period)
            _obs(cur, rel, run, "import", "DE", "CN", "000TOTAL", 2_000_000_000)
            _obs(cur, rel, run, "import", "FR", "CN", "000TOTAL", 1_000_000_000)
            _obs(cur, rel, run, "export", "DE", "CN", "000TOTAL", 1_000_000_000)
            _obs(cur, rel, run, "export", "FR", "CN", "000TOTAL", 500_000_000)


def _latest_finding(cur, subkind):
    cur.execute(
        "SELECT id, observation_ids FROM findings "
        "WHERE subkind = %s AND superseded_at IS NULL ORDER BY id DESC LIMIT 1",
        (subkind,),
    )
    return cur.fetchone()


def test_trade_balance_payload_sources_arithmetic_and_sql(
    empty_op_tables, test_db_url,
):
    """The observation-based trail: one source entry per seeded release, the
    arithmetic restates the stored rolling totals in the shared formats, and
    the replay SQL names the finding's own observation ids."""
    conn = psycopg2.connect(test_db_url)
    _seed_full_year(conn)
    assert anomalies.detect_eu_china_trade_balance()["inserted_new"] > 0

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        row = _latest_finding(cur, "trade_balance")
        assert row is not None
        payloads = provenance_payload.build_payloads_for(cur, [row["id"]])
    conn.close()

    payload = payloads[str(row["id"])]

    # Source trail: exactly the 12 seeded releases, oldest first, all Eurostat.
    sources = payload["sources"]
    assert len(sources) == 12
    assert [s["period"] for s in sources] == [m.isoformat() for m in _MONTHS]
    assert all(s["source"] == "Eurostat" for s in sources)
    assert sources[0]["url"] == f"http://example/eurostat-{_MONTHS[0]}.7z"

    # Arithmetic: 36bn imports − 18bn exports = 18bn deficit; 18bn/365d ≈
    # €49M/day in the card's own face format. No YoY line (no prior window).
    arith = payload["arithmetic"]
    assert any("€36.00B" in a and "€18.00B" in a and "deficit" in a for a in arith)
    assert any("€49M/day" in a for a in arith)
    assert not any("Year on year" in a for a in arith)

    # Replay SQL pulls the exact observation rows the figure summed.
    assert "FROM observations o JOIN releases r" in payload["replay_sql"]
    assert str(row["observation_ids"][0]) in payload["replay_sql"]


def test_deficit_kpi_drawer_restates_card_value_verbatim(
    empty_op_tables, test_db_url,
):
    """F3 regression lock: for every €/day KPI card in a built report, the
    card's `formatted` string appears verbatim in its own provenance drawer's
    arithmetic. A future format tweak to either side that reopens the
    card/drawer divergence ("€1,027M/day" vs "≈€1.0bn/day") fails here."""
    conn = psycopg2.connect(test_db_url)
    _seed_full_year(conn)
    anomalies.detect_eu_china_trade_balance()
    conn.close()

    from report_builder import build_report
    r = build_report(source_trigger="eurostat")

    per_day_cards = [i for i in r.key_indicators if i.unit == "eur_per_day"]
    assert per_day_cards, "expected at least the EU deficit/day KPI"
    for ind in per_day_cards:
        assert ind.provenance.finding_ids, f"{ind.key} carries no finding id"
        payload = r.provenance_payloads.get(str(ind.provenance.finding_ids[0]))
        assert payload is not None, f"{ind.key} has no provenance payload"
        assert any(ind.formatted in line for line in payload["arithmetic"]), (
            f"{ind.key}: card face {ind.formatted!r} not restated in drawer "
            f"arithmetic {payload['arithmetic']!r}"
        )


def test_hs_group_window_sources_scope_and_window_bounds(
    empty_op_tables, test_db_url,
):
    """Window-based findings (no observation_ids) cite the releases inside
    their current 12-month window only, filtered to the scope's sources:
    an eu_27 finding cites Eurostat releases, not HMRC, and nothing from
    before the window."""
    conn = psycopg2.connect(test_db_url)
    with conn, conn.cursor() as cur:
        for period in _MONTHS:
            _eu_release(cur, period)
        # Out-of-window Eurostat release + in-window HMRC release: both must
        # be excluded from an eu_27-scope trail.
        _eu_release(cur, date(2024, 12, 1))
        cur.execute(
            "INSERT INTO releases (source, period, source_url) VALUES "
            "('hmrc', %s, 'http://example/hmrc-in-window')",
            (date(2026, 1, 1),),
        )
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) VALUES ('seed', 'success') RETURNING id"
        )
        run_id = cur.fetchone()[0]
        detail = {
            "windows": {
                "current_start": _MONTHS[0].isoformat(),
                "current_end": _MONTHS[-1].isoformat(),
            },
            "totals": {
                "current_12mo_eur": 9_000_000, "prior_12mo_eur": 150_000,
                "yoy_pct": 59.0, "low_base": True,
            },
            "caveat_codes": ["low_base_effect"],
        }
        cur.execute(
            """
            INSERT INTO findings (scrape_run_id, kind, subkind, observation_ids,
                                  hs_group_ids, score, title, body, detail)
            VALUES (%s, 'anomaly', 'hs_group_yoy', '{}', '{}', 1.0, 't', 'b', %s::jsonb)
            RETURNING id
            """,
            (run_id, json.dumps(detail)),
        )
        fid = cur.fetchone()[0]

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        payloads = provenance_payload.build_payloads_for(cur, [fid])
    conn.close()

    payload = payloads[str(fid)]
    sources = payload["sources"]
    assert len(sources) == 12
    assert all(s["source"] == "Eurostat" for s in sources)
    assert all(s["period"] >= _MONTHS[0].isoformat() for s in sources)

    # Arithmetic restates the stored totals and carries the low-base warning.
    arith = payload["arithmetic"]
    assert any("€9.0M" in a and "€150.0k" in a for a in arith)
    assert any("Low base" in a for a in arith)
    # Caveat gloss resolved from the shared glossary.
    assert payload["caveats"][0]["code"] == "low_base_effect"
    assert payload["caveats"][0]["gloss"]
    # No observation ids → no replay SQL (window findings replay differently).
    assert payload["replay_sql"] is None


def test_malformed_detail_is_skipped_not_fatal(empty_op_tables, test_db_url):
    """build_payloads_for is best-effort per finding: one finding whose
    detail breaks the arithmetic builder is logged and skipped; the healthy
    finding's payload still ships. A bad row must never sink the snapshot."""
    conn = psycopg2.connect(test_db_url)
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (source_url, status) VALUES ('seed', 'success') RETURNING id"
        )
        run_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO findings (scrape_run_id, kind, subkind, observation_ids,
                                  hs_group_ids, score, title, body, detail)
            VALUES (%s, 'anomaly', 'trade_balance', '{}', '{}', 1.0, 'bad', 'b',
                    '{"totals": "not-a-dict"}'::jsonb)
            RETURNING id
            """,
            (run_id,),
        )
        bad_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO findings (scrape_run_id, kind, subkind, observation_ids,
                                  hs_group_ids, score, title, body, detail)
            VALUES (%s, 'anomaly', 'hs_group_yoy', '{}', '{}', 1.0, 'ok', 'b',
                    '{"totals": {"current_12mo_eur": 1000000, "prior_12mo_eur": 500000}}'::jsonb)
            RETURNING id
            """,
            (run_id,),
        )
        good_id = cur.fetchone()[0]

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        payloads = provenance_payload.build_payloads_for(cur, [bad_id, good_id])
    conn.close()

    assert str(bad_id) not in payloads
    assert str(good_id) in payloads
