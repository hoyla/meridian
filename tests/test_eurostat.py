"""Tests for the Eurostat fetcher.

We synthesise a tiny 7z archive in a temp dir for each test rather than
committing a multi-MB binary fixture, since the source format (a CSV inside a
7z) is small to construct from a list of dicts.
"""

import csv
import io
import os
import tempfile
from datetime import date

import py7zr
import pytest

import eurostat


# Minimum columns iter_observations actually reads.
_BASE_COLS = [
    "REPORTER", "PARTNER", "TRADE_TYPE", "PRODUCT_NC", "PRODUCT_SITC",
    "PRODUCT_CPA21", "PRODUCT_CPA22", "PRODUCT_BEC", "PRODUCT_BEC5",
    "PRODUCT_SECTION", "FLOW", "STAT_PROCEDURE", "SUPPL_UNIT", "PERIOD",
    "VALUE_EUR", "VALUE_NAC", "QUANTITY_KG", "QUANTITY_SUPPL_UNIT",
]


def _row(**overrides) -> dict:
    base = {c: "" for c in _BASE_COLS}
    base.update(overrides)
    return base


def _build_archive(rows: list[dict]) -> bytes:
    """Write rows as a CSV inside a fresh 7z archive and return the bytes."""
    with tempfile.TemporaryDirectory(prefix="gacc-test-") as tmpdir:
        csv_path = os.path.join(tmpdir, "fixture.dat")
        with open(csv_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=_BASE_COLS)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        archive_path = os.path.join(tmpdir, "fixture.7z")
        with py7zr.SevenZipFile(archive_path, "w") as archive:
            archive.write(csv_path, arcname="fixture.dat")
        with open(archive_path, "rb") as fh:
            return fh.read()


@pytest.fixture
def archive_two_partners_two_hs() -> bytes:
    return _build_archive([
        # (DE, CN, 87038010, import) — split across 2 STAT_PROCEDURE rows; should aggregate
        _row(REPORTER="DE", PARTNER="CN", PRODUCT_NC="87038010", FLOW="1",
             STAT_PROCEDURE="1", VALUE_EUR="1000", QUANTITY_KG="100", QUANTITY_SUPPL_UNIT="5"),
        _row(REPORTER="DE", PARTNER="CN", PRODUCT_NC="87038010", FLOW="1",
             STAT_PROCEDURE="2", VALUE_EUR="500",  QUANTITY_KG="50",  QUANTITY_SUPPL_UNIT="3"),
        # (DE, CN, 87038010, export) — different flow, separate observation
        _row(REPORTER="DE", PARTNER="CN", PRODUCT_NC="87038010", FLOW="2",
             STAT_PROCEDURE="1", VALUE_EUR="2000", QUANTITY_KG="200", QUANTITY_SUPPL_UNIT="10"),
        # (FR, CN, 8703210,   import) — leading zero needed (HS-CN8 should pad to 08703210? no, 7-digit means HS-CN7 — invalid in real life but tests zero-padding)
        _row(REPORTER="FR", PARTNER="CN", PRODUCT_NC="8703210", FLOW="1",
             STAT_PROCEDURE="1", VALUE_EUR="800", QUANTITY_KG="80", QUANTITY_SUPPL_UNIT="0"),
        # (DE, US, 87038010, import) — non-CN, should be filtered out by partner=CN
        _row(REPORTER="DE", PARTNER="US", PRODUCT_NC="87038010", FLOW="1",
             STAT_PROCEDURE="1", VALUE_EUR="9999", QUANTITY_KG="9999", QUANTITY_SUPPL_UNIT="0"),
    ])


def test_aggregates_within_dim_key(archive_two_partners_two_hs):
    obs = list(eurostat.iter_observations(
        archive_two_partners_two_hs, date(2026, 1, 1), partners={"CN"},
    ))
    de_imports = [o for o in obs
                  if o["reporter_country"] == "DE"
                  and o["flow"] == "import"
                  and o["hs_code"] == "87038010"]
    assert len(de_imports) == 1, "two source rows should collapse into one observation"
    assert de_imports[0]["value"] == 1500
    assert de_imports[0]["quantity"] == 8  # 5 + 3 supplementary units
    assert de_imports[0]["currency"] == "EUR"
    assert de_imports[0]["period"] == "2026-01-01"
    assert de_imports[0]["period_kind"] == "monthly"


def test_partner_filter_excludes_others(archive_two_partners_two_hs):
    obs = list(eurostat.iter_observations(
        archive_two_partners_two_hs, date(2026, 1, 1), partners={"CN"},
    ))
    assert all(o["partner_country"] == "CN" for o in obs)
    # The US row should not appear.
    assert not any(o["reporter_country"] == "DE" and o["partner_country"] == "US" for o in obs)


def test_hs_prefix_filter(archive_two_partners_two_hs):
    obs = list(eurostat.iter_observations(
        archive_two_partners_two_hs, date(2026, 1, 1), partners={"CN"},
        hs_prefixes=("87038",),
    ))
    assert all(o["hs_code"].startswith("87038") for o in obs)


def test_zero_pads_hs_code(archive_two_partners_two_hs):
    obs = list(eurostat.iter_observations(
        archive_two_partners_two_hs, date(2026, 1, 1), partners={"CN"},
    ))
    fr = next(o for o in obs if o["reporter_country"] == "FR")
    # Source value was '8703210' (7 chars); should be padded to 8 chars.
    assert fr["hs_code"] == "08703210"


def test_flow_label_translates_numeric(archive_two_partners_two_hs):
    obs = list(eurostat.iter_observations(
        archive_two_partners_two_hs, date(2026, 1, 1), partners={"CN"},
    ))
    flows = {o["flow"] for o in obs}
    assert flows == {"import", "export"}


def test_empty_archive_filter_returns_nothing():
    archive = _build_archive([
        _row(REPORTER="DE", PARTNER="US", PRODUCT_NC="87038010", FLOW="1",
             VALUE_EUR="100", QUANTITY_KG="10"),
    ])
    obs = list(eurostat.iter_observations(archive, date(2026, 1, 1), partners={"CN"}))
    assert obs == []
