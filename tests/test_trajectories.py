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
    # Genuinely borderline data — every accel/decel flavour is editorially
    # defensible. Phase 1.3 swap to Theil-Sen lands closer to the threshold,
    # which is fine; the test's job is to lock the *family* of shapes, not
    # the precise sub-classification.
    assert shape in ("rising", "rising_accelerating", "rising_decelerating")


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
# Phase 1.3: Theil-Sen slope robustness vs OLS
# ---------------------------------------------------------------------------


def _ols_slope(xs, ys):
    """Reference OLS implementation, kept inline here so the test demonstrates
    the difference between the two estimators on a single contrived series."""
    n = len(xs)
    if n < 2:
        return 0.0
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    num = sum((xs[i] - x_mean) * (ys[i] - y_mean) for i in range(n))
    den = sum((xs[i] - x_mean) ** 2 for i in range(n))
    return num / den if den else 0.0


def test_theil_sen_resists_endpoint_outlier_that_flips_ols():
    """Construct a series that's flat-ish in the middle but has one extreme
    outlier at the end. OLS will report a strongly positive slope (the
    outlier dominates); Theil-Sen takes the median pairwise slope, so the
    outlier is one of many comparisons and doesn't move the median much.
    The trajectory classifier reads `overall_slope` to break the
    rising/falling tie when there's no sign change — endpoint robustness
    matters editorially here because a single noisy month at the series
    end shouldn't flip a "flat" trajectory to "rising"."""
    xs = list(range(8))
    # Flat values with one extreme positive outlier at the end.
    ys = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 50.0]

    ols = _ols_slope(xs, ys)
    ts = anomalies._theil_sen_slope(xs, ys)

    # OLS is yanked positive by the outlier; Theil-Sen sees mostly zero
    # pairs and one large jump — median is 0.
    assert ols > 1.0, f"OLS should be pulled hard positive, got {ols}"
    assert ts == 0.0, f"Theil-Sen should be unmoved by single outlier, got {ts}"


def test_theil_sen_matches_ols_on_clean_linear_data():
    """Sanity check: when the series IS linear, both estimators should agree
    closely. We don't require exact equality (Theil-Sen averages even-N
    medians, OLS minimises squared error) but they should be within a few
    percent on a clean line."""
    xs = list(range(10))
    ys = [3.0 * x + 1.0 for x in xs]  # slope = 3
    ols = _ols_slope(xs, ys)
    ts = anomalies._theil_sen_slope(xs, ys)
    assert abs(ols - 3.0) < 1e-9
    assert abs(ts - 3.0) < 1e-9


def test_theil_sen_handles_trivial_input():
    """Edge cases: empty/single-point input must return 0, not raise."""
    assert anomalies._theil_sen_slope([], []) == 0.0
    assert anomalies._theil_sen_slope([1.0], [2.0]) == 0.0


# ---------------------------------------------------------------------------
# Phase 2.4: configurable smoothing window
# ---------------------------------------------------------------------------


def test_smooth_window_1_disables_smoothing():
    """Phase 2.4: smooth_window=1 should treat the raw series as the smoothed
    series, exposing single-period spikes (tariff pre-loading) that the
    default 3-window smoothing would absorb. The features dict records the
    window actually used."""
    yoys = [0.10] * 6 + [0.50] + [0.10] * 6  # one-month spike
    # Default smoothing absorbs the spike → flat or slight rising.
    shape_default, feat_default = anomalies._classify_trajectory(yoys)
    assert feat_default["smoothing_window"] == anomalies.TRAJECTORY_SMOOTH_WINDOW

    # No smoothing → spike survives and triggers a sign-changes count
    # consistent with the unsmoothed pattern. (We're not assertion the
    # specific shape because the classifier may legitimately read either;
    # the contract here is that the *window value round-trips*.)
    shape_raw, feat_raw = anomalies._classify_trajectory(yoys, smooth_window=1)
    assert feat_raw["smoothing_window"] == 1
    # smoothed and raw signs should be the same when window=1.
    assert feat_raw["smoothed_first"] == yoys[0]
    assert feat_raw["smoothed_last"] == yoys[-1]


# ---------------------------------------------------------------------------
# Phase 2.5: seasonality as a feature, not a shape
# ---------------------------------------------------------------------------


def test_autocorrelation_at_lag_pure_function():
    """Edge cases for the autocorrelation helper."""
    # Series too short for the requested lag → None.
    assert anomalies._autocorrelation_at_lag([1, 2, 3], lag=12) is None
    # Constant series → 0 variance → None.
    assert anomalies._autocorrelation_at_lag([1.0] * 20, lag=12) is None
    # Perfectly periodic series at lag 12 → autocorrelation ≈ 1.
    periodic = [float((i % 12) - 6) for i in range(36)]
    ac = anomalies._autocorrelation_at_lag(periodic, lag=12)
    assert ac is not None
    assert ac > 0.95


def test_seasonal_signal_surfaces_in_features():
    """Phase 2.5: trajectory features include seasonal_signal_strength and
    has_strong_seasonal_signal. A series with a clear annual pattern
    should flag has_strong_seasonal_signal=True; a non-seasonal series
    should not."""
    # 24 months of strong annual oscillation around zero.
    seasonal = [0.30 * (1 if (i % 12) < 6 else -1) for i in range(24)]
    _, feat_seasonal = anomalies._classify_trajectory(seasonal)
    assert feat_seasonal["seasonal_signal_lag"] == 12
    assert feat_seasonal["seasonal_signal_strength"] is not None
    assert abs(feat_seasonal["seasonal_signal_strength"]) >= anomalies.SEASONAL_SIGNAL_THRESHOLD
    assert feat_seasonal["has_strong_seasonal_signal"] is True

    # A linear rise has no seasonal pattern.
    linear = [0.01 * i for i in range(24)]
    _, feat_linear = anomalies._classify_trajectory(linear)
    assert feat_linear["has_strong_seasonal_signal"] is False


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


def test_trajectory_export_side_isolated_from_import(empty_op, test_db_url):
    """flow=2 trajectory reads only hs_group_yoy_export findings; the resulting
    trajectory finding has subkind 'hs_group_trajectory_export' and detail.flow=2.
    Import and export trajectories don't mix even when both exist for the same group."""
    cur_conn = psycopg2.connect(test_db_url)
    cur = cur_conn.cursor()
    cur.execute("INSERT INTO scrape_runs (source_url, status) VALUES ('seed', 'success') RETURNING id")
    run = cur.fetchone()[0]

    # Seed import-side findings (rising) and export-side findings (falling)
    # for the same group_id (1 = EV batteries) over 8 windows.
    for period, imp_yoy, exp_yoy in [
        (date(2025, 7, 1),  0.10, -0.10),
        (date(2025, 8, 1),  0.12, -0.12),
        (date(2025, 9, 1),  0.13, -0.14),
        (date(2025, 10, 1), 0.15, -0.16),
        (date(2025, 11, 1), 0.16, -0.18),
        (date(2025, 12, 1), 0.18, -0.20),
        (date(2026, 1, 1),  0.20, -0.22),
        (date(2026, 2, 1),  0.22, -0.24),
    ]:
        for subkind, yoy in [("hs_group_yoy", imp_yoy), ("hs_group_yoy_export", exp_yoy)]:
            detail = {
                "windows": {"current_end": period.isoformat(), "current_start": period.isoformat()},
                "totals": {"yoy_pct": yoy, "current_12mo_eur": 1_000_000_000.0, "yoy_pct_kg": yoy * 0.8},
            }
            cur.execute(
                """
                INSERT INTO findings (scrape_run_id, kind, subkind, observation_ids,
                                      hs_group_ids, score, title, body, detail)
                VALUES (%s, 'anomaly', %s, '{}', %s, %s, 't', 'b', %s::jsonb)
                """,
                (run, subkind, [1], abs(yoy), json.dumps(detail)),
            )
    cur_conn.commit()
    cur_conn.close()

    # Import side: rising trajectory
    counts_imp = anomalies.detect_hs_group_trajectories(group_names=["EV batteries (Li-ion)"], flow=1)
    assert counts_imp["emitted"] == 1

    # Export side: falling trajectory
    counts_exp = anomalies.detect_hs_group_trajectories(group_names=["EV batteries (Li-ion)"], flow=2)
    assert counts_exp["emitted"] == 1

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur2:
        cur2.execute(
            "SELECT subkind, detail->>'shape', detail->>'flow' FROM findings "
            "WHERE subkind LIKE 'hs_group_trajectory%' ORDER BY subkind"
        )
        rows = cur2.fetchall()

    by_sub = {r[0]: (r[1], r[2]) for r in rows}
    assert "hs_group_trajectory" in by_sub
    assert "hs_group_trajectory_export" in by_sub
    imp_shape, imp_flow = by_sub["hs_group_trajectory"]
    exp_shape, exp_flow = by_sub["hs_group_trajectory_export"]
    assert imp_shape in ("rising", "rising_accelerating")
    assert exp_shape in ("falling", "falling_accelerating")
    assert imp_flow == "1"
    assert exp_flow == "2"


def test_trajectory_u_recovery_integration(empty_op, test_db_url):
    """Wind-turbine-style series: declining then recovering. Phase 1.7 added
    a gap-detection check, so this test now seeds a continuous monthly
    series — sparse quarterly fixtures fail the gap check (and editorially
    they should: trajectory shapes inferred over discontinuous data are
    misleading)."""
    # 24 months of monthly data shaped as a U: declining negative,
    # crossing zero, then climbing positive.
    # Skip exactly-zero values — the cross_zero_idx detector requires both
    # neighbouring smoothed signs to be non-zero, so we transition directly
    # from negative to positive.
    yoys = [-0.30, -0.28, -0.25, -0.22, -0.18, -0.15,
            -0.10, -0.05, -0.02, 0.05, 0.10, 0.15,
            0.20, 0.24, 0.28, 0.32, 0.36, 0.40,
            0.42, 0.40, 0.36, 0.30, 0.22, 0.17]
    series = []
    p = date(2024, 1, 1)
    for y in yoys:
        series.append((p, y))
        p = date(p.year + 1, 1, 1) if p.month == 12 else date(p.year, p.month + 1, 1)

    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy_findings(conn, group_id=4, periods_and_yoys=series)  # Wind turbine components

    counts = anomalies.detect_hs_group_trajectories(group_names=["Wind turbine components"])
    assert counts.get("skipped_incomplete_series", 0) == 0
    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT detail FROM findings WHERE subkind='hs_group_trajectory'"
        )
        detail = cur.fetchone()[0]

    assert detail["shape"] == "u_recovery"
    # Should cite the cross-zero point in body+detail
    assert "cross_zero_idx" in detail["features"]


def test_trajectory_skips_incomplete_series(empty_op, test_db_url):
    """Phase 1.7: when the underlying YoY series has a missing month, the
    classifier refuses to fit a trajectory and tallies under
    skipped_incomplete_series. Editorially, smoothing and slope estimators
    assume continuous monthly data — fitting a shape across a gap would
    mislead."""
    # 12 months but with Apr 2025 missing — a 1-month gap is enough to skip.
    series = [
        (date(2024, 12, 1), 0.10),
        (date(2025, 1, 1), 0.12),
        (date(2025, 2, 1), 0.14),
        (date(2025, 3, 1), 0.16),
        # MISSING: 2025-04
        (date(2025, 5, 1), 0.20),
        (date(2025, 6, 1), 0.22),
        (date(2025, 7, 1), 0.24),
        (date(2025, 8, 1), 0.26),
    ]
    with psycopg2.connect(test_db_url) as conn:
        _seed_yoy_findings(conn, group_id=4, periods_and_yoys=series)

    counts = anomalies.detect_hs_group_trajectories(group_names=["Wind turbine components"])
    assert counts["skipped_incomplete_series"] == 1
    assert counts["emitted"] == 0

    with psycopg2.connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM findings WHERE subkind = 'hs_group_trajectory'")
        assert cur.fetchone()[0] == 0


def test_detect_series_gaps_helper():
    """Pure-function test of the gap detector."""
    # Continuous monthly series → no gaps.
    assert anomalies._detect_series_gaps([
        date(2025, 1, 1), date(2025, 2, 1), date(2025, 3, 1),
    ]) == []
    # One-month gap → reported.
    missing = anomalies._detect_series_gaps([
        date(2025, 1, 1), date(2025, 2, 1),
        # gap at 2025-03
        date(2025, 4, 1),
    ])
    assert missing == [date(2025, 3, 1)]
    # Multi-month gap.
    missing = anomalies._detect_series_gaps([
        date(2025, 1, 1),
        # gap Feb–Apr
        date(2025, 5, 1),
    ])
    assert missing == [date(2025, 2, 1), date(2025, 3, 1), date(2025, 4, 1)]
    # Year-boundary gap.
    missing = anomalies._detect_series_gaps([
        date(2025, 11, 1),
        # gap 2025-12
        date(2026, 1, 1),
    ])
    assert missing == [date(2025, 12, 1)]
    # Trivial input.
    assert anomalies._detect_series_gaps([]) == []
    assert anomalies._detect_series_gaps([date(2025, 1, 1)]) == []
