"""Tests for ``scripts/walk_forward_profitability.py``.

Focused on the partial-aware hit_rate fix and the direction-asymmetry
narrative helper. The full DB query path is exercised by the live
audit; these tests cover the pure helpers.
"""

from __future__ import annotations

from scripts import walk_forward_profitability as wfp


def _row(*, confidence: float, verdict: str, pnl_pct: float, direction: str = "CALL") -> dict:
    return {
        "confidence": confidence,
        "verdict": verdict,
        "pnl_pct": pnl_pct,
        "direction": direction,
        "ticker": "TEST",
        "created_at": None,
        "prediction_type": "direction",
    }


def test_bucket_stats_counts_hit_and_hit_or_partial_separately():
    rows = [
        _row(confidence=0.8, verdict="hit", pnl_pct=2.0),
        _row(confidence=0.8, verdict="partial", pnl_pct=1.0),
        _row(confidence=0.8, verdict="partial", pnl_pct=0.5),
        _row(confidence=0.8, verdict="miss", pnl_pct=-1.5),
    ]
    stats = wfp._bucket_stats("HIGH", rows)
    assert stats.n == 4
    # 1 hit out of 4
    assert stats.hit_rate == 0.25
    # 3 (hit + 2 partials) out of 4
    assert stats.hit_or_partial_rate == 0.75
    # mean_pnl unaffected by the verdict-counting change
    assert abs(stats.mean_pnl - 0.5) < 1e-9


def test_verdict_call_uses_hit_or_partial_rate():
    # HIGH has lower verdict='hit' rate but higher hit-or-partial rate
    # than MEDIUM. The verdict should be CALIBRATED based on the
    # partial-aware metric + positive pnl lift.
    high = wfp._bucket_stats("HIGH", [
        _row(confidence=0.8, verdict="hit", pnl_pct=3.0),
        _row(confidence=0.8, verdict="partial", pnl_pct=2.0),
        _row(confidence=0.8, verdict="partial", pnl_pct=2.0),
        _row(confidence=0.8, verdict="miss", pnl_pct=-0.5),
    ])
    medium = wfp._bucket_stats("MEDIUM", [
        _row(confidence=0.6, verdict="hit", pnl_pct=1.0),
        _row(confidence=0.6, verdict="hit", pnl_pct=1.0),
        _row(confidence=0.6, verdict="miss", pnl_pct=-1.0),
        _row(confidence=0.6, verdict="miss", pnl_pct=-1.5),
    ])
    verdict = wfp._verdict_call({"HIGH": high, "MEDIUM": medium})
    # HIGH hit-or-partial = 0.75, MEDIUM = 0.5 → +25pp lift; pnl also positive
    assert verdict.startswith("CALIBRATED")
    assert "hit-or-partial" in verdict


def test_direction_asymmetry_note_fires_for_one_sided_market():
    by_direction = {
        "CALL_LOW": {"n": 200, "mean_pnl": 2.5},
        "CALL_MEDIUM": {"n": 150, "mean_pnl": 3.0},
        "CALL_HIGH": {"n": 100, "mean_pnl": 2.1},
        "PUT_LOW": {"n": 200, "mean_pnl": -2.3},
        "PUT_MEDIUM": {"n": 150, "mean_pnl": -0.5},
        "PUT_HIGH": {"n": 100, "mean_pnl": -5.8},
    }
    note = wfp._direction_asymmetry_note(by_direction)
    assert "DIRECTION ASYMMETRY" in note
    assert "CALL wins every bucket" in note
    assert "PUT loses every bucket" in note


def test_direction_asymmetry_note_silent_when_mixed():
    # Mixed signs in each direction → no asymmetry call
    by_direction = {
        "CALL_LOW": {"n": 200, "mean_pnl": 2.5},
        "CALL_MEDIUM": {"n": 150, "mean_pnl": -1.0},
        "PUT_LOW": {"n": 200, "mean_pnl": -2.3},
        "PUT_MEDIUM": {"n": 150, "mean_pnl": 0.5},
    }
    assert wfp._direction_asymmetry_note(by_direction) == ""


def test_direction_asymmetry_note_silent_below_sample_threshold():
    # n < 30 in each cell → no asymmetry call
    by_direction = {
        "CALL_LOW": {"n": 20, "mean_pnl": 2.5},
        "PUT_LOW": {"n": 20, "mean_pnl": -2.5},
    }
    assert wfp._direction_asymmetry_note(by_direction) == ""
