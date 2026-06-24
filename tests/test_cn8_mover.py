"""Pure unit tests for the CN8 biggest-mover gating (no DB).

`anomalies._cn8_mover_gates` applies the five gates to one product's monthly
value series — both-sides value floor, magnitude, month-continuity, persistence
across the last 3 rolling anchors, and leave-one-out. See the roadmap entry
"Biggest mover KPI" for the rationale and the data look that motivated each gate.
"""
from datetime import date

import anomalies

_LATEST = date(2026, 4, 1)
_FLOOR = anomalies.CN8_MOVER_MIN_WINDOW_EUR
_YOY = anomalies.CN8_MOVER_MIN_YOY_ABS


def _series(month_values):
    """{period: value} for the most-recent N months; month_values[0] is the
    latest month (_LATEST), [1] one month back, etc."""
    return {anomalies._months_back(_LATEST, i): v for i, v in enumerate(month_values)}


def _gate(series):
    return anomalies._cn8_mover_gates(series, _LATEST, _FLOOR, _YOY)


def test_pass_for_clean_persistent_riser():
    # 12 recent months at €5M (current €60M) over 14 prior at €2.5M (€30M):
    # +100% YoY, persistent, survives leave-one-out.
    res = _gate(_series([5e6] * 12 + [2.5e6] * 14))
    assert res is not None
    assert res["yoy_pct"] > 0.9
    assert len(res["anchor_yoys"]) == anomalies.CN8_MOVER_PERSIST_ANCHORS
    assert res["current_eur"] >= _FLOOR and res["prior_eur"] >= _FLOOR


def test_reject_low_base_prior():
    # Prior window below the €25M floor → low-base, rejected despite a huge %
    # (the +39,926%-from-€0.1M failure mode the data look surfaced).
    assert _gate(_series([5e6] * 12 + [0.2e6] * 14)) is None


def test_reject_below_threshold():
    # Both windows material but the move is only ~+4% — under the 25% floor.
    assert _gate(_series([5.2e6] * 12 + [5.0e6] * 14)) is None


def test_reject_single_month_spike_via_leave_one_out():
    # Passes floor, magnitude and persistence, but one €30M month carries the
    # rise: current = 30 + 11×3 = €63M vs €30M prior (+110%), yet dropping the
    # spike collapses it to +10% (< the 15% leave-one-out floor).
    assert _gate(_series([30e6] + [3e6] * 11 + [2.5e6] * 14)) is None


def test_reject_non_persistent_latest_only_jump():
    # The latest month alone jumps; the move doesn't hold across the last 3
    # anchors. Rejected by persistence (and/or leave-one-out).
    assert _gate(_series([30e6] + [2.6e6] * 11 + [2.6e6] * 14)) is None


def test_reject_insufficient_months():
    # Only 8 months of current data (< CN8_MOVER_MIN_MONTHS_PER_WINDOW).
    assert _gate(_series([6e6] * 8)) is None
