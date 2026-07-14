"""DB-backed tests for report_builder._publication_calendar — the payload
behind the Sources & coverage publication-calendar table (Luke, 2026-07-14).

The pure date arithmetic (official 2026 schedule, formula fallback, the
January→February remap, bulletin cadence) is covered in
test_release_calendar.py; here we test the payload assembly: the window,
landed-vs-awaited resolution against `releases`, the informational
gacc_bulletin column never claiming a landing, and the folded/combined
flags the renderer keys on.
"""

from datetime import date

import psycopg2

import report_builder


def _seed_release(cur, source, period, pubdate, section=4, currency="CNY",
                  kind="preliminary"):
    unit = {"CNY": "CNY 100 Million", "USD": "USD1 Million"}[currency]
    cur.execute(
        """INSERT INTO releases (source, section_number, currency, period,
                                 release_kind, description, title, source_url,
                                 publication_date, unit)
           VALUES (%s, %s, %s, %s, %s, 'd', 't', %s, %s, %s)""",
        (source, section, currency, period, kind,
         f"test://{source}/{period}/{currency}", pubdate, unit),
    )


def _calendar(test_db_url, today):
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        return report_builder._publication_calendar(cur, today=today)


def _row(pc, iso):
    return next(r for r in pc["rows"] if r["period"] == iso)


def test_window_landed_awaited_and_scheduled(db_conn, test_db_url):
    with db_conn, db_conn.cursor() as cur:
        # June GACC landed today (the 2026-07-14 drop, on its official date);
        # April Eurostat landed on its calendar date; May Eurostat not yet
        # (due 16 Jul); April GACC deliberately NOT seeded → awaited.
        _seed_release(cur, "gacc", date(2026, 6, 1), date(2026, 7, 14))
        _seed_release(cur, "gacc", date(2026, 6, 1), date(2026, 7, 14),
                      currency="USD")
        _seed_release(cur, "eurostat", date(2026, 4, 1), date(2026, 6, 15))
        _seed_release(cur, "hmrc", date(2026, 4, 1), date(2026, 6, 12))

    pc = _calendar(test_db_url, today=date(2026, 7, 14))
    assert pc["as_of"] == "2026-07-14"
    # Window: 3 back .. 2 forward of July → Apr..Sep 2026.
    assert [r["period"] for r in pc["rows"]] == [
        "2026-04-01", "2026-05-01", "2026-06-01",
        "2026-07-01", "2026-08-01", "2026-09-01"]

    jun = _row(pc, "2026-06-01")["cells"]
    assert jun["gacc"] == {
        "expected": "2026-07-14", "official": True,
        "landed": "2026-07-14", "status": "landed", "combined": False}
    # The bulletin is informational: future date, official, never landed.
    assert jun["gacc_bulletin"]["status"] == "scheduled"
    assert jun["gacc_bulletin"]["expected"] == "2026-07-18"
    assert jun["gacc_bulletin"]["landed"] is None
    assert jun["eurostat"] == {
        "expected": "2026-08-14", "official": True,
        "landed": None, "status": "scheduled", "combined": False}

    apr = _row(pc, "2026-04-01")["cells"]
    assert apr["eurostat"]["status"] == "landed"
    assert apr["eurostat"]["landed"] == "2026-06-15"
    assert apr["hmrc"]["status"] == "landed"
    # April GACC was due 9 May and never seeded → awaited, date kept.
    assert apr["gacc"] == {
        "expected": "2026-05-09", "official": True,
        "landed": None, "status": "awaited", "combined": False}
    # The bulletin's April date has passed but we don't track it → it stays a
    # plain scheduled date, never 'awaited'.
    assert apr["gacc_bulletin"]["status"] == "scheduled"
    assert apr["gacc_bulletin"]["expected"] == "2026-05-18"

    may = _row(pc, "2026-05-01")["cells"]
    assert may["eurostat"] == {
        "expected": "2026-07-16", "official": True,
        "landed": None, "status": "scheduled", "combined": False}


def test_january_folds_and_february_carries_the_combined_release(
        db_conn, test_db_url):
    with db_conn, db_conn.cursor() as cur:
        # The Jan–Feb combined release sits on the February anchor.
        _seed_release(cur, "gacc", date(2026, 2, 1), date(2026, 3, 10),
                      kind="preliminary_jan_feb")

    pc = _calendar(test_db_url, today=date(2026, 3, 20))
    jan = _row(pc, "2026-01-01")["cells"]
    assert jan["gacc"]["status"] == "folded"
    assert jan["gacc_bulletin"]["status"] == "folded"
    # Eurostat/HMRC have real standalone January data — no folding.
    assert jan["eurostat"]["status"] != "folded"

    feb = _row(pc, "2026-02-01")["cells"]
    assert feb["gacc"]["status"] == "landed"
    assert feb["gacc"]["landed"] == "2026-03-10"
    assert feb["gacc"]["combined"] is True
    assert feb["gacc_bulletin"]["combined"] is True


def test_formula_years_read_estimated(db_conn, test_db_url):
    # 2027 has no entered schedule (each year's announcement lands Jan–Mar of
    # that year), so future cells carry the formula date flagged unofficial.
    pc = _calendar(test_db_url, today=date(2027, 5, 10))
    jun27 = _row(pc, "2027-06-01")["cells"]
    assert jun27["gacc"] == {
        "expected": "2027-07-13", "official": False,
        "landed": None, "status": "estimated", "combined": False}
    # The bulletin formula reproduces the official 18th rule, still estimated.
    assert jun27["gacc_bulletin"]["expected"] == "2027-07-18"
    assert jun27["gacc_bulletin"]["status"] == "estimated"
