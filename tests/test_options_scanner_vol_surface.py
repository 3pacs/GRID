"""ALPHA-1 — pure-function tests for OptionsScanner._score_vol_surface.

The heavy DB-touching paths (build_surface, compute_skew, detect_arbitrage)
are mocked here. We're testing the score arithmetic + direction inference,
not the SVI fit itself (that lives in tests/test_vol_surface.py if it exists,
otherwise in analysis/vol_surface.py's docstring examples).
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from discovery.options_scanner import OptionsScanner


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture
def scanner():
    """Scanner with a stub engine — we never touch the DB in these tests."""
    return OptionsScanner(db_engine=MagicMock(), lookback_days=252)


def _patch_engine(surface, violations, skew_rows):
    """Helper: build a MagicMock VolSurfaceEngine that returns the given values."""
    eng = MagicMock()
    eng.build_surface.return_value = surface
    eng.detect_arbitrage.return_value = violations
    eng.compute_skew.return_value = skew_rows
    return eng


# ── Tests ─────────────────────────────────────────────────────────────────


class TestScoreVolSurface:
    def test_clean_surface_returns_zero(self, scanner):
        with patch("analysis.vol_surface.VolSurfaceEngine") as VS:
            VS.return_value = _patch_engine(
                surface={"ticker": "AAPL", "raw_points": [{}]},
                violations=[],
                skew_rows=[],
            )
            score, direction, meta = scanner._score_vol_surface("AAPL", date(2026, 4, 13))
        assert score == 0.0
        assert direction == ""
        assert meta["butterfly_violations"] == 0

    def test_butterfly_violations_score(self, scanner):
        with patch("analysis.vol_surface.VolSurfaceEngine") as VS:
            VS.return_value = _patch_engine(
                surface={"ticker": "AAPL", "raw_points": [{}]},
                violations=[
                    {"type": "butterfly", "strike": 100, "expiry1": "2026-05-15"},
                    {"type": "butterfly", "strike": 105, "expiry1": "2026-05-15"},
                ],
                skew_rows=[],
            )
            score, _, meta = scanner._score_vol_surface("AAPL", date(2026, 4, 13))
        assert meta["butterfly_violations"] == 2
        assert score == 4.0  # 2 * 2.0

    def test_butterfly_capped_at_six(self, scanner):
        with patch("analysis.vol_surface.VolSurfaceEngine") as VS:
            VS.return_value = _patch_engine(
                surface={"ticker": "AAPL", "raw_points": [{}]},
                violations=[{"type": "butterfly"}] * 10,
                skew_rows=[],
            )
            score, _, _ = scanner._score_vol_surface("AAPL", date(2026, 4, 13))
        assert score == 6.0

    def test_calendar_capped_at_three(self, scanner):
        with patch("analysis.vol_surface.VolSurfaceEngine") as VS:
            VS.return_value = _patch_engine(
                surface={"ticker": "AAPL", "raw_points": [{}]},
                violations=[{"type": "calendar"}] * 10,
                skew_rows=[],
            )
            score, _, _ = scanner._score_vol_surface("AAPL", date(2026, 4, 13))
        assert score == 3.0

    def test_steep_skew_calls(self, scanner):
        with patch("analysis.vol_surface.VolSurfaceEngine") as VS:
            VS.return_value = _patch_engine(
                surface={"ticker": "AAPL", "raw_points": [{}]},
                violations=[],
                skew_rows=[{"skew": 0.15, "butterfly": 0.01, "atm_iv": 0.30}],
            )
            score, direction, meta = scanner._score_vol_surface("AAPL", date(2026, 4, 13))
        assert direction == "CALL"
        assert score == 2.0
        assert meta["front_skew"] == 0.15

    def test_flat_skew_puts(self, scanner):
        with patch("analysis.vol_surface.VolSurfaceEngine") as VS:
            VS.return_value = _patch_engine(
                surface={"ticker": "AAPL", "raw_points": [{}]},
                violations=[],
                skew_rows=[{"skew": 0.01, "butterfly": 0.01, "atm_iv": 0.20}],
            )
            score, direction, _ = scanner._score_vol_surface("AAPL", date(2026, 4, 13))
        assert direction == "PUT"
        assert score == 2.0

    def test_extreme_butterfly_adds_one(self, scanner):
        with patch("analysis.vol_surface.VolSurfaceEngine") as VS:
            VS.return_value = _patch_engine(
                surface={"ticker": "AAPL", "raw_points": [{}]},
                violations=[],
                skew_rows=[{"skew": 0.05, "butterfly": 0.08, "atm_iv": 0.25}],
            )
            score, direction, _ = scanner._score_vol_surface("AAPL", date(2026, 4, 13))
        # skew 0.05 is in mid-range (no skew score), butterfly > 0.05 → +1.0
        assert score == 1.0
        # direction inferred from butterfly sign
        assert direction == "CALL"

    def test_negative_butterfly_puts(self, scanner):
        with patch("analysis.vol_surface.VolSurfaceEngine") as VS:
            VS.return_value = _patch_engine(
                surface={"ticker": "AAPL", "raw_points": [{}]},
                violations=[],
                skew_rows=[{"skew": 0.05, "butterfly": -0.08, "atm_iv": 0.25}],
            )
            _, direction, _ = scanner._score_vol_surface("AAPL", date(2026, 4, 13))
        assert direction == "PUT"

    def test_combined_max_score(self, scanner):
        """Lots of violations + steep skew + extreme butterfly → cap at 10."""
        with patch("analysis.vol_surface.VolSurfaceEngine") as VS:
            VS.return_value = _patch_engine(
                surface={"ticker": "AAPL", "raw_points": [{}]},
                violations=[{"type": "butterfly"}] * 5 + [{"type": "calendar"}] * 5,
                skew_rows=[{"skew": 0.20, "butterfly": 0.10, "atm_iv": 0.40}],
            )
            score, direction, _ = scanner._score_vol_surface("AAPL", date(2026, 4, 13))
        assert score == 10.0
        assert direction == "CALL"

    def test_build_surface_error_returns_zero(self, scanner):
        with patch("analysis.vol_surface.VolSurfaceEngine") as VS:
            eng = MagicMock()
            eng.build_surface.return_value = {"error": "no data", "ticker": "ZZZ"}
            VS.return_value = eng
            score, direction, _ = scanner._score_vol_surface("ZZZ", date(2026, 4, 13))
        assert score == 0.0
        assert direction == ""

    def test_exception_returns_zero_non_fatal(self, scanner):
        with patch("analysis.vol_surface.VolSurfaceEngine") as VS:
            eng = MagicMock()
            eng.build_surface.side_effect = RuntimeError("boom")
            VS.return_value = eng
            score, direction, _ = scanner._score_vol_surface("AAPL", date(2026, 4, 13))
        assert score == 0.0
        assert direction == ""

    def test_meta_exposes_front_values(self, scanner):
        with patch("analysis.vol_surface.VolSurfaceEngine") as VS:
            VS.return_value = _patch_engine(
                surface={"ticker": "AAPL", "raw_points": [{}]},
                violations=[],
                skew_rows=[{"skew": 0.12, "butterfly": 0.03, "atm_iv": 0.28}],
            )
            _, _, meta = scanner._score_vol_surface("AAPL", date(2026, 4, 13))
        assert meta["front_skew"] == 0.12
        assert meta["front_butterfly"] == 0.03
        assert meta["front_atm_iv"] == 0.28
