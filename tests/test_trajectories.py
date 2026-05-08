"""Tests for the HS-group trajectory classifier.

Two layers of test:
1. _classify_trajectory is a pure function — exercise it directly with
   synthetic YoY series for each shape.
2. detect_hs_group_trajectories is integration — seed hs_group_yoy findings
   in the test DB and verify the resulting trajectory finding.
"""

import json
from datetime import date

import psycopg2
import pytest

import anomalies


# ---------------------------------------------------------------------------
# Pure-function tests
# ---------------------------------------------------------------------------

def test_classify_flat():
    yoys = [0.005, -0.01, 0.012, -0.008, 0.005, 0.0, 0.01, -0.005]
    shape, _ = anomalies._classify_trajectory(yoys)
    assert shape == "flat"


def test_classify_rising():
    yoys = [0.10, 0.12, 0.11, 0.13, 0.12, 0.14, 0.13, 0.15]
    shape, _ = anomalies._classify_trajectory(yoys)
    assert shape in ("rising", "rising_decelerating")  # could read either way; allow either


def test_classify_rising_accelerating():
    # YoY accelerating: gentle rise then sharp rise
    yoys = [0.05, 0.06, 0.07, 0.08, 0.20, 0.30, 0.45, 0.60]
    shape, features = anomalies._classify_trajectory(yoys)
    assert shape == "rising_accelerating", f"got {shape}, features={features}"


def test_classify_rising_decelerating():
    # YoY decelerating: sharp rise then gentle rise
    yoys = [0.40, 0.45, 0.42, 0.38, 0.20, 0.18, 0.17, 0.16]
    shape, features = anomalies._classify_trajectory(yoys)
    assert shape == "rising_decelerating", f"got {shape}, features={features}"


def test_classify_falling():
    yoys = [-0.10, -0.12, -0.11, -0.13, -0.12, -0.14, -0.13, -0.15]
    shape, _ = anomalies._classify_trajectory(yoys)
    assert shape in ("falling", "falling_decelerating", "falling_accelerating")


def test_classify_u_recovery():
    # Was negative, now positive — single sign change
    yoys = [-0.20, -0.18, -0.15, -0.10, -0.05, 0.05, 0.15, 0.30]
    shape, features = anomalies._classify_trajectory(yoys)
    assert shape == "u_recovery", f"got {shape}, features={features}"
    assert features["sign_changes"] == 1


def test_classify_inverse_u_peak():
    # Was positive, now negative — single sign change
    yoys = [0.20, 0.25, 0.18, 0.12, 0.08, -0.02, -0.10, -0.18]
    shape, _ = anomalies._classify_trajectory(yoys)
    assert shape == "inverse_u_peak"


def test_classify_dip_recovery():
    """Wind-turbines-style: positive → negative → positive."""
    yoys = [0.22, 0.18, 0.10, -0.05, -0.20, -0.27, -0.10, 0.10, 0.30, 0.48, 0.30, 0.17]
    shape, features = anomalies._classify_trajectory(yoys)
    assert shape == "dip_recovery", f"got {shape}, features={features}"
    assert features["smoothed_sign_changes"] == 2


def test_classify_failed_recovery():
    """Negative → positive → negative."""
    yoys = [-0.20, -0.18, -0.10, 0.05, 0.20, 0.30, 0.10, -0.05, -0.20, -0.30, -0.40, -0.45]
    shape, features = anomalies._classify_trajectory(yoys)
    assert shape == "failed_recovery", f"got {shape}, features={features}"
    assert features["smoothed_sign_changes"] == 2


def test_classify_volatile():
    # Multiple sign changes — no clear direction
    yoys = [0.10, -0.08, 0.05, -0.12, 0.08, -0.06, 0.04, -0.09]
    shape, features = anomalies._classify_trajectory(yoys)
    assert shape == "volatile"
    assert features["sign_changes"] >= 2


def test_classify_insufficient_data():
    yoys = [0.10, 0.20, 0.15]
    shape, features = anomalies._classify_trajectory(yoys)
    assert shape == "insufficient_data"
    assert features["n"] == 3


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _direct_db_url(test_db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", test_db_url)


@pytest.fixture
def empty_op(test_db_url):
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE findings, observations, source_snapshots, eurostat_raw_rows, "
            "scrape_runs, releases RESTART IDENTITY CASCADE"
        )
    yield


def _seed_yoy_findings(conn, group_id: int, periods_and_yoys: list[tuple[date, float]]) -> None:
    """Insert one hs_group_yoy finding per (period, yoy)."""
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES ('seed', 'success') RETURNING id",
    )
    run = cur.fetchone()[0]
    for period, yoy in periods_and_yoys:
        detail = {
            "windows": {"current_end": period.isoformat(), "current_start": period.isoformat()},
            "totals": {
                "yoy_pct": yoy,
                "current_12mo_eur": 1000.0,
                "yoy_pct_kg": yoy * 0.8,
            },
        }
        cur.execute(
            """
            INSERT INTO findings (scrape_run_id, kind, subkind, observation_ids,
                                  hs_group_ids, score, title, body, detail)
            VALUES (%s, 'anomaly', 'hs_group_yoy', '{}', %s, %s, 't', 'b', %s::jsonb)
            """,
            (run, [group_id], abs(yoy), json.dumps(detail)),
        )
    conn.commit()


def test_trajectory_emits_finding_with_shape_and_underlying_ids(empty_op, test_db_url):
    """Seed 8 ascending YoYs → expect a 'rising' trajectory finding linking back to all 8."""
    # Use group id 1 (EV batteries) which is in the seed data
    series = [
        (date(2025, 7, 1), 0.10),
        (date(2025, 8, 1), 0.12),
        (date(2025, 9, 1), 0.11),
        (date(2025, 10, 1), 0.13),
        (date(2025, 11, 1), 0.12),
        (date(2025, 12, 1), 0.14),
        (date(2026, 1, 1), 0.13),
        (date(2026, 2, 1), 0.15),
    ]
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy_findings(conn, group_id=1, periods_and_yoys=series)

    counts = anomalies.detect_hs_group_trajectories(group_names=["EV batteries (Li-ion)"])
    assert counts["emitted"] == 1, f"counts={counts}"

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT subkind, title, detail FROM findings WHERE subkind='hs_group_trajectory'"
        )
        subkind, title, detail = cur.fetchone()

    assert subkind == "hs_group_trajectory"
    assert "EV batteries" in title
    assert detail["shape"] in ("rising", "rising_decelerating", "rising_accelerating")
    assert detail["features"]["n"] == 8
    assert len(detail["underlying_yoy_finding_ids"]) == 8
    # Series is preserved chronologically
    assert detail["series"][0]["period"] == "2025-07-01"
    assert detail["series"][-1]["period"] == "2026-02-01"


def test_trajectory_skips_when_no_findings(empty_op):
    counts = anomalies.detect_hs_group_trajectories(group_names=["EV batteries (Li-ion)"])
    assert counts["emitted"] == 0
    assert counts["skipped_no_findings"] >= 1


def test_trajectory_u_recovery_integration(empty_op, test_db_url):
    """Wind-turbine-style series: declining then recovering."""
    series = [
        (date(2024, 1, 1), -0.27),
        (date(2024, 4, 1), -0.20),
        (date(2024, 7, 1), -0.18),
        (date(2024, 10, 1), 0.04),
        (date(2025, 1, 1), 0.25),
        (date(2025, 4, 1), 0.35),
        (date(2025, 7, 1), 0.48),
        (date(2026, 2, 1), 0.17),
    ]
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy_findings(conn, group_id=4, periods_and_yoys=series)  # Wind turbine components

    anomalies.detect_hs_group_trajectories(group_names=["Wind turbine components"])
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail FROM findings WHERE subkind='hs_group_trajectory'"
        )
        detail = cur.fetchone()[0]

    assert detail["shape"] == "u_recovery"
    # Should cite the cross-zero point in body+detail
    assert "cross_zero_idx" in detail["features"]
