"""Regression test for finding A2 (2026-06-25 adversarial-correctness review).

The EU-27 analysers scope by EXCLUDING GB (anomalies.EU27_EXCLUDE_REPORTERS),
trusting that the only non-EU-27 declarant Eurostat ever ships is GB. The cheap
detect-and-alert guard: eurostat.unexpected_reporters() flags any reporter
outside the 27 members + GB, and scrape_eurostat logs it at ingest before the
stray code can fold into EU-27 and double-count. (The full inclusion-list swap
across the 5 EU-27 query surfaces is deferred to E1.)

These are pure / dry-run tests — no DB, so they run in CI too.
"""
import logging
from datetime import date

import eurostat
import scrape


# --- pure helper / constant --------------------------------------------------

def test_known_reporters_are_the_27_plus_gb():
    assert len(eurostat.EU27_KNOWN_REPORTERS) == 28
    assert "GB" in eurostat.EU27_KNOWN_REPORTERS
    assert eurostat.EU27_PARTNER_CODES <= eurostat.EU27_KNOWN_REPORTERS
    # Greece is 'GR' (not Eurostat's 'EL') and the UK is 'GB' — the exact
    # spellings the live reporter column uses (verified 2026-06-25).
    assert "GR" in eurostat.EU27_KNOWN_REPORTERS
    assert "EL" not in eurostat.EU27_KNOWN_REPORTERS


def test_unexpected_reporters_flags_strangers_only():
    # An aggregate declarant + a candidate country are surprises; 27 + GB aren't.
    assert eurostat.unexpected_reporters(
        ["DE", "FR", "GB", "GR", "EU27_2020", "RS"]
    ) == {"EU27_2020", "RS"}


def test_unexpected_reporters_clean_set_and_falsy_codes():
    assert eurostat.unexpected_reporters(sorted(eurostat.EU27_KNOWN_REPORTERS)) == set()
    assert eurostat.unexpected_reporters(["", None, "DE"]) == set()


# --- wiring: scrape_eurostat alerts at ingest (dry-run path, no DB) -----------

def _fake_raw(reporter: str) -> dict:
    return {"reporter": reporter, "partner": "CN", "product_nc": "85076000",
            "flow": 1, "value_eur": 1.0}


def _patch_ingest(monkeypatch, rows: list[dict]) -> None:
    monkeypatch.setattr(eurostat, "bulk_file_exists", lambda *a, **k: True)
    monkeypatch.setattr(
        eurostat, "fetch_bulk_file",
        lambda *a, **k: type("R", (), {"content": b"", "status_code": 200})(),
    )
    monkeypatch.setattr(eurostat, "iter_raw_rows", lambda *a, **k: rows)
    monkeypatch.setattr(eurostat, "aggregate_to_observations", lambda *a, **k: [])


def test_scrape_eurostat_logs_on_unexpected_reporter(monkeypatch, caplog):
    _patch_ingest(monkeypatch, [_fake_raw("DE"), _fake_raw("EU27_2020")])

    with caplog.at_level(logging.ERROR):
        scrape.scrape_eurostat(date(2026, 3, 1), dry_run=True)

    errors = " ".join(r.message for r in caplog.records if r.levelno >= logging.ERROR)
    assert "EU27_2020" in errors
    assert "unexpected reporter" in errors.lower()


def test_scrape_eurostat_quiet_when_reporters_all_known(monkeypatch, caplog):
    _patch_ingest(monkeypatch, [_fake_raw("DE"), _fake_raw("GB"), _fake_raw("GR")])

    with caplog.at_level(logging.ERROR):
        scrape.scrape_eurostat(date(2026, 3, 1), dry_run=True)

    assert not [r for r in caplog.records if "unexpected reporter" in r.message.lower()]
