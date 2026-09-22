"""ALPHA-2 — pure-function tests for OptionsScanner._score_dealer_gamma_extras.

Mocks the DealerGammaEngine so we test the scoring arithmetic + direction
inference + threshold behavior, not the GEX engine itself.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from discovery.options_scanner import OptionsScanner


@pytest.fixture
def scanner():
    return OptionsScanner(db_engine=MagicMock(), lookback_days=252)


def _patch_dg(profile):
    """Helper: build a MagicMock DealerGammaEngine that returns the given profile."""
    eng = MagicMock()
    eng.compute_gex_profile.return_value = profile
    return eng


# ── Tests ─────────────────────────────────────────────────────────────────


class TestDealerGammaExtras:
    def test_neutral_profile_returns_zero(self, scanner):
        with patch("physics.dealer_gamma.DealerGammaEngine") as DG:
            DG.return_value = _patch_dg({
                "ticker": "AAPL", "spot": 200.0,
                "vanna_exposure": 0.0, "charm_exposure": 0.0,
                "regime": "NEUTRAL",
            })
            v_s, v_d, c_s, c_d, meta = scanner._score_dealer_gamma_extras(
                "AAPL", date(2026, 4, 13)
            )
        assert v_s == 0.0 and v_d == ""
        assert c_s == 0.0 and c_d == ""
        assert meta["spot"] == 200.0

    def test_negative_vanna_calls(self, scanner):
        # Spot 100 → denom 1e8. v_norm = -2e7/1e8 = -0.20 → score 10, dir CALL
        with patch("physics.dealer_gamma.DealerGammaEngine") as DG:
            DG.return_value = _patch_dg({
                "ticker": "AAPL", "spot": 100.0,
                "vanna_exposure": -2.0e7,
                "charm_exposure": 0.0,
                "regime": "NEUTRAL",
            })
            v_s, v_d, _, _, _ = scanner._score_dealer_gamma_extras(
                "AAPL", date(2026, 4, 13)
            )
        assert v_d == "CALL"
        assert v_s == 10.0

    def test_positive_vanna_puts(self, scanner):
        # spot 100, denom 1e8, v_norm = 0.10 → score 5, dir PUT
        with patch("physics.dealer_gamma.DealerGammaEngine") as DG:
            DG.return_value = _patch_dg({
                "ticker": "AAPL", "spot": 100.0,
                "vanna_exposure": 1.0e7,
                "charm_exposure": 0.0,
                "regime": "NEUTRAL",
            })
            v_s, v_d, _, _, _ = scanner._score_dealer_gamma_extras(
                "AAPL", date(2026, 4, 13)
            )
        assert v_d == "PUT"
        assert v_s == 5.0

    def test_below_threshold_no_score(self, scanner):
        # v_norm = 0.05 < 0.10 threshold
        with patch("physics.dealer_gamma.DealerGammaEngine") as DG:
            DG.return_value = _patch_dg({
                "ticker": "AAPL", "spot": 100.0,
                "vanna_exposure": 5.0e6,  # 0.05 normalized
                "charm_exposure": 0.0,
                "regime": "NEUTRAL",
            })
            v_s, v_d, _, _, _ = scanner._score_dealer_gamma_extras(
                "AAPL", date(2026, 4, 13)
            )
        assert v_s == 0.0 and v_d == ""

    def test_positive_charm_puts(self, scanner):
        with patch("physics.dealer_gamma.DealerGammaEngine") as DG:
            DG.return_value = _patch_dg({
                "ticker": "AAPL", "spot": 100.0,
                "vanna_exposure": 0.0,
                "charm_exposure": 1.5e7,  # 0.15 normalized → score 7.5, PUT
                "regime": "NEUTRAL",
            })
            _, _, c_s, c_d, _ = scanner._score_dealer_gamma_extras(
                "AAPL", date(2026, 4, 13)
            )
        assert c_d == "PUT"
        assert c_s == 7.5

    def test_negative_charm_calls(self, scanner):
        with patch("physics.dealer_gamma.DealerGammaEngine") as DG:
            DG.return_value = _patch_dg({
                "ticker": "AAPL", "spot": 100.0,
                "vanna_exposure": 0.0,
                "charm_exposure": -1.5e7,
                "regime": "NEUTRAL",
            })
            _, _, c_s, c_d, _ = scanner._score_dealer_gamma_extras(
                "AAPL", date(2026, 4, 13)
            )
        assert c_d == "CALL"
        assert c_s == 7.5

    def test_score_capped_at_ten(self, scanner):
        with patch("physics.dealer_gamma.DealerGammaEngine") as DG:
            DG.return_value = _patch_dg({
                "ticker": "AAPL", "spot": 100.0,
                "vanna_exposure": -1.0e10,
                "charm_exposure": 1.0e10,
                "regime": "NEUTRAL",
            })
            v_s, _, c_s, _, _ = scanner._score_dealer_gamma_extras(
                "AAPL", date(2026, 4, 13)
            )
        assert v_s == 10.0
        assert c_s == 10.0

    def test_meta_exposes_aggregates(self, scanner):
        with patch("physics.dealer_gamma.DealerGammaEngine") as DG:
            DG.return_value = _patch_dg({
                "ticker": "AAPL", "spot": 250.5,
                "vanna_exposure": 1234567.8,
                "charm_exposure": -987654.3,
                "regime": "SHORT_GAMMA",
            })
            _, _, _, _, meta = scanner._score_dealer_gamma_extras(
                "AAPL", date(2026, 4, 13)
            )
        assert meta["vanna_exposure"] == 1234567.8
        assert meta["charm_exposure"] == -987654.3
        assert meta["regime"] == "SHORT_GAMMA"
        assert meta["spot"] == 250.5

    def test_error_profile_returns_zero(self, scanner):
        with patch("physics.dealer_gamma.DealerGammaEngine") as DG:
            DG.return_value = _patch_dg({"error": "no chain", "ticker": "ZZZ"})
            v_s, v_d, c_s, c_d, _ = scanner._score_dealer_gamma_extras(
                "ZZZ", date(2026, 4, 13)
            )
        assert v_s == 0.0 and v_d == ""
        assert c_s == 0.0 and c_d == ""

    def test_exception_returns_zero_non_fatal(self, scanner):
        with patch("physics.dealer_gamma.DealerGammaEngine") as DG:
            eng = MagicMock()
            eng.compute_gex_profile.side_effect = RuntimeError("boom")
            DG.return_value = eng
            v_s, v_d, c_s, c_d, _ = scanner._score_dealer_gamma_extras(
                "AAPL", date(2026, 4, 13)
            )
        assert v_s == 0.0
        assert c_s == 0.0
