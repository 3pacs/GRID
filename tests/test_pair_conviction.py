"""Unit tests for ``intelligence.pair_conviction``.

The real ``intelligence.decision_gateway.should_i_trade`` pulls in the
oracle engine and half the feature stack. For unit-level tests we swap
it out via ``sys.modules`` with a lightweight fake module and hand-crafted
``_FakeResponse`` instances that carry the exact attributes
``_extract_leg_fields`` inspects.

No live network, no real DB. ``pytest -x -q`` must be green.
"""

from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from intelligence.pair_conviction import (
    DEFAULT_PAIR_CANDIDATES,
    MIN_LEG_CONVICTION,
    MIN_SPREAD_SHARPNESS,
    PAIR_KELLY_CAP,
    PairCandidate,
    PairLeg,
    PairTradeTicket,
    _run_leg,
    compose_pair_thesis,
    compute_pair_conviction_score,
    compute_pair_invalidation,
    compute_spread_sharpness,
    generate_pair_ticket,
    is_correlated_risk_trap,
    scan_candidate_pairs,
    size_pair_legs,
    verdict_from_pair_conviction,
)


# ── Fakes ─────────────────────────────────────────────────────────────────


@dataclass
class _FakeProvenance:
    aggregate_conviction: float = 0.0
    direction: str = "neutral"
    verdict: str = "medium"


@dataclass
class _FakeStress:
    robustness_label: str = "robust"
    robustness_score: float = 0.9


@dataclass
class _FakeTicket:
    entry_price: float = 100.0
    stop_price: float = 95.0
    target_price: float = 110.0
    kelly_size_pct: float = 0.03
    kelly_size_dollars: float = 3000.0
    thesis: str = "fundamentals-driven accumulation into cycle bottom"
    lever: str = "semi capex expansion by anchor foundry"


@dataclass
class _FakeResponse:
    ticker: str = "XYZ"
    unified_verdict: str = "high"
    provenance_report: Any = None
    stress_report: Any = None
    trade_ticket: Any = None
    prediction: Any = None


def _make_response(
    ticker: str,
    *,
    conviction: float,
    signal_direction: str = "bullish",
    robust_label: str = "robust",
    robust_score: float = 0.9,
    entry: float = 100.0,
) -> _FakeResponse:
    prov = _FakeProvenance(
        aggregate_conviction=conviction,
        direction=signal_direction,
        verdict="high",
    )
    stress = _FakeStress(robustness_label=robust_label, robustness_score=robust_score)
    ticket = _FakeTicket(
        entry_price=entry,
        stop_price=entry * 0.95,
        target_price=entry * 1.10,
    )
    return _FakeResponse(
        ticker=ticker.upper(),
        unified_verdict="high",
        provenance_report=prov,
        stress_report=stress,
        trade_ticket=ticket,
        prediction=None,
    )


def _install_fake_gateway(should_i_trade_fn) -> None:
    """Install a fake ``intelligence.decision_gateway`` module so
    ``_run_leg``'s late-bound import picks up our mock.
    """
    mod_name = "intelligence.decision_gateway"
    mod = types.ModuleType(mod_name)
    mod.should_i_trade = should_i_trade_fn  # type: ignore[attr-defined]
    sys.modules[mod_name] = mod


def _remove_fake_gateway() -> None:
    sys.modules.pop("intelligence.decision_gateway", None)


@pytest.fixture(autouse=True)
def _reset_gateway():
    yield
    _remove_fake_gateway()


# ── Pure helper tests ─────────────────────────────────────────────────────


def test_compute_pair_conviction_score_happy() -> None:
    assert compute_pair_conviction_score(0.9, 0.85, 0.5) == pytest.approx(0.85 * 0.5, abs=1e-6)


def test_compute_pair_conviction_score_weak_leg_drags_min() -> None:
    # 0.9 long, 0.2 short — should clamp to 0.2, not average to 0.55
    got = compute_pair_conviction_score(0.9, 0.2, 0.4)
    assert got == pytest.approx(0.2 * 0.4, abs=1e-6)


def test_compute_pair_conviction_score_clamps_to_unit_interval() -> None:
    assert compute_pair_conviction_score(1.5, -0.2, 2.0) == pytest.approx(0.0, abs=1e-6)
    assert compute_pair_conviction_score(0.9, 0.9, 1.1) == pytest.approx(0.9 * 1.0, abs=1e-6)


def test_compute_spread_sharpness_opposite_directions_is_average() -> None:
    got = compute_spread_sharpness(0.8, 0.7, "bullish", "bearish")
    assert got == pytest.approx((0.8 + 0.7) / 2, abs=1e-6)


def test_compute_spread_sharpness_same_direction_is_gap() -> None:
    got = compute_spread_sharpness(0.9, 0.4, "bullish", "bullish")
    assert got == pytest.approx(abs(0.9 - 0.4), abs=1e-6)


def test_compute_spread_sharpness_same_direction_tiny_gap() -> None:
    # Both legs at 0.8 bullish → gap 0 → NOT a spread
    got = compute_spread_sharpness(0.8, 0.8, "bullish", "bullish")
    assert got == pytest.approx(0.0, abs=1e-6)


def test_is_correlated_risk_trap_same_sector_same_dir() -> None:
    assert is_correlated_risk_trap("tech", "tech", "bullish", "bullish") is True


def test_is_correlated_risk_trap_same_sector_opposite_dir() -> None:
    assert is_correlated_risk_trap("tech", "tech", "bullish", "bearish") is False


def test_is_correlated_risk_trap_different_sectors() -> None:
    assert is_correlated_risk_trap("tech", "energy", "bullish", "bullish") is False


def test_is_correlated_risk_trap_none_sector_passes_through() -> None:
    assert is_correlated_risk_trap(None, "tech", "bullish", "bullish") is False
    assert is_correlated_risk_trap("tech", None, "bullish", "bullish") is False


def test_compose_pair_thesis_contains_both_tickers_and_chains() -> None:
    long_leg = PairLeg(
        ticker="TSM",
        direction="long",
        kelly_size_pct=0.05,
        kelly_size_dollars=5000.0,
        entry_price=150.0,
        stop_price=142.5,
        target_price=165.0,
        conviction=0.88,
        robustness_label="robust",
        robustness_score=0.9,
        signal_summary="TSM bullish conv=0.88 | capex cycle up | anchor foundry bid",
    )
    short_leg = PairLeg(
        ticker="NVDA",
        direction="short",
        kelly_size_pct=0.05,
        kelly_size_dollars=5000.0,
        entry_price=900.0,
        stop_price=945.0,
        target_price=810.0,
        conviction=0.78,
        robustness_label="robust",
        robustness_score=0.8,
        signal_summary="NVDA bearish conv=0.78 | retail momentum exhaustion | whale exit",
    )
    thesis = compose_pair_thesis("TSM", "NVDA", long_leg, short_leg)
    assert "TSM" in thesis
    assert "NVDA" in thesis
    assert "capex cycle" in thesis
    assert "retail momentum" in thesis


def test_compute_pair_invalidation_contains_both_stops() -> None:
    long_leg = PairLeg(
        ticker="AAA",
        direction="long",
        kelly_size_pct=0.04,
        kelly_size_dollars=4000.0,
        entry_price=50.0,
        stop_price=47.5,
        target_price=55.0,
        conviction=0.8,
        robustness_label="robust",
        robustness_score=0.9,
        signal_summary="",
    )
    short_leg = PairLeg(
        ticker="BBB",
        direction="short",
        kelly_size_pct=0.04,
        kelly_size_dollars=4000.0,
        entry_price=75.0,
        stop_price=78.75,
        target_price=67.5,
        conviction=0.75,
        robustness_label="robust",
        robustness_score=0.85,
        signal_summary="",
    )
    text = compute_pair_invalidation(long_leg, short_leg)
    assert "47.50" in text
    assert "78.75" in text
    assert "AAA" in text
    assert "BBB" in text


def test_verdict_from_pair_conviction_thresholds() -> None:
    assert verdict_from_pair_conviction(0.9, 0.9) == "high"
    assert verdict_from_pair_conviction(0.6, 0.9) == "medium"
    assert verdict_from_pair_conviction(0.4, 0.9) == "low"
    # Below floor → no_trade
    assert verdict_from_pair_conviction(0.1, 0.9) == "no_trade"


def test_verdict_from_pair_conviction_fragile_worst_leg_downgrades() -> None:
    # 0.9 would be 'high' but worst-leg robustness 0.3 downgrades one level
    assert verdict_from_pair_conviction(0.9, 0.3) == "medium"


def test_size_pair_legs_dollar_neutral_within_5pct() -> None:
    long_d, short_d = size_pair_legs(
        account_size_usd=100_000.0,
        pair_conviction=0.6,
        long_price=100.0,
        short_price=200.0,
    )
    assert long_d > 0 and short_d > 0
    # At medium pair_conv (0.6), no long bias — dollar-neutral
    diff_pct = abs(long_d - short_d) / max(long_d, short_d)
    assert diff_pct < 0.05


def test_size_pair_legs_respects_kelly_cap() -> None:
    long_d, short_d = size_pair_legs(
        account_size_usd=100_000.0,
        pair_conviction=0.95,
        long_price=50.0,
        short_price=50.0,
    )
    # Per-leg cap is PAIR_KELLY_CAP of equity = $8,000
    cap_usd = 100_000.0 * PAIR_KELLY_CAP + 1e-6
    assert long_d <= cap_usd
    assert short_d <= cap_usd


def test_size_pair_legs_high_conviction_has_slight_long_bias() -> None:
    long_d, short_d = size_pair_legs(
        account_size_usd=100_000.0,
        pair_conviction=0.85,
        long_price=100.0,
        short_price=100.0,
    )
    # Long should be >= short at high conviction (bias)
    assert long_d >= short_d


def test_pair_leg_frozen_dataclass_roundtrip() -> None:
    leg = PairLeg(
        ticker="ABC",
        direction="long",
        kelly_size_pct=0.05,
        kelly_size_dollars=5000.0,
        entry_price=100.0,
        stop_price=95.0,
        target_price=110.0,
        conviction=0.8,
        robustness_label="robust",
        robustness_score=0.9,
        signal_summary="ABC bullish",
    )
    d = leg.to_dict()
    assert d["ticker"] == "ABC"
    assert d["kelly_size_pct"] == 0.05
    with pytest.raises((AttributeError, Exception)):
        leg.ticker = "XYZ"  # type: ignore[misc]


def test_pair_trade_ticket_roundtrip() -> None:
    long_leg = PairLeg(
        ticker="LONG",
        direction="long",
        kelly_size_pct=0.04,
        kelly_size_dollars=4000.0,
        entry_price=100.0,
        stop_price=95.0,
        target_price=110.0,
        conviction=0.85,
        robustness_label="robust",
        robustness_score=0.9,
        signal_summary="",
    )
    short_leg = PairLeg(
        ticker="SHRT",
        direction="short",
        kelly_size_pct=0.04,
        kelly_size_dollars=4000.0,
        entry_price=200.0,
        stop_price=210.0,
        target_price=180.0,
        conviction=0.75,
        robustness_label="robust",
        robustness_score=0.85,
        signal_summary="",
    )
    ticket = PairTradeTicket(
        pair_name="LONG LONG / SHORT SHRT",
        long_leg=long_leg,
        short_leg=short_leg,
        pair_conviction_score=0.6,
        spread_sharpness=0.8,
        net_exposure_usd=0.0,
        gross_exposure_usd=8000.0,
        thesis="test",
        invalidation="test",
        causation_chain="test",
        generated_at="2026-04-13T00:00:00+00:00",
        verdict="medium",
    )
    d = ticket.to_dict()
    assert d["pair_name"] == "LONG LONG / SHORT SHRT"
    assert d["long_leg"]["ticker"] == "LONG"
    assert d["short_leg"]["ticker"] == "SHRT"


def test_pair_candidate_roundtrip() -> None:
    c = PairCandidate(
        long_ticker="A",
        short_ticker="B",
        expected_relationship="rel",
        rationale="why",
    )
    d = c.to_dict()
    assert d == {
        "long_ticker": "A",
        "short_ticker": "B",
        "expected_relationship": "rel",
        "rationale": "why",
    }


# ── Engine-touching tests ─────────────────────────────────────────────────


def test_run_leg_happy_path() -> None:
    def _fake_should_i_trade(engine, ticker, *, account_size_usd, **kwargs):
        return _make_response(ticker, conviction=0.82, signal_direction="bullish")

    _install_fake_gateway(_fake_should_i_trade)
    result = _run_leg(MagicMock(), "TSM", "long", account_size_usd=100_000.0)
    assert result["error"] is None
    assert result["ticker"] == "TSM"
    assert result["direction"] == "long"
    assert result["conviction"] == pytest.approx(0.82, abs=1e-6)
    assert result["signal_direction"] == "bullish"
    assert result["robustness_label"] == "robust"
    assert result["entry_price"] == 100.0


def test_run_leg_swallows_exception() -> None:
    def _boom(engine, ticker, *, account_size_usd, **kwargs):
        raise RuntimeError("oracle offline")

    _install_fake_gateway(_boom)
    result = _run_leg(MagicMock(), "XYZ", "long", account_size_usd=100_000.0)
    assert result["error"] is not None
    assert "oracle offline" in result["error"]
    # Defaults are populated so callers don't KeyError
    assert result["ticker"] == "XYZ"
    assert result["conviction"] == 0.0


def test_run_leg_handles_missing_gateway_module() -> None:
    """When decision_gateway cannot be imported at all, _run_leg returns
    an error dict rather than raising."""
    _remove_fake_gateway()
    # Install a broken module that raises on attribute access
    broken = types.ModuleType("intelligence.decision_gateway")
    # Intentionally no should_i_trade attribute → AttributeError on import path
    sys.modules["intelligence.decision_gateway"] = broken
    result = _run_leg(MagicMock(), "ZZZ", "long", account_size_usd=100_000.0)
    # _run_leg should either return an error (AttributeError at call) or swallow it
    assert result["ticker"] == "ZZZ"
    assert result["direction"] == "long"


def test_generate_pair_ticket_happy_path() -> None:
    def _fake(engine, ticker, *, account_size_usd, **kwargs):
        if ticker == "TSM":
            return _make_response(
                "TSM",
                conviction=0.88,
                signal_direction="bullish",
                entry=150.0,
            )
        if ticker == "NVDA":
            return _make_response(
                "NVDA",
                conviction=0.82,
                signal_direction="bearish",
                entry=900.0,
            )
        raise AssertionError(f"unexpected ticker {ticker}")

    _install_fake_gateway(_fake)
    ticket = generate_pair_ticket(
        MagicMock(),
        "TSM",
        "NVDA",
        account_size_usd=100_000.0,
    )
    assert ticket is not None
    assert ticket.pair_name == "LONG TSM / SHORT NVDA"
    assert ticket.long_leg.ticker == "TSM"
    assert ticket.short_leg.ticker == "NVDA"
    assert ticket.pair_conviction_score > 0.0
    # Sharpness here is opposite-direction avg (0.88 + 0.82) / 2 = 0.85
    assert ticket.spread_sharpness == pytest.approx(0.85, abs=1e-6)
    assert ticket.verdict in {"high", "medium", "low"}
    # Gross exposure sanity: 2x a per-leg size
    assert ticket.gross_exposure_usd > 0
    # Thesis references both names
    assert "TSM" in ticket.thesis
    assert "NVDA" in ticket.thesis


def test_generate_pair_ticket_weak_long_leg_returns_none() -> None:
    def _fake(engine, ticker, *, account_size_usd, **kwargs):
        if ticker == "TSM":
            return _make_response("TSM", conviction=0.4, signal_direction="bullish")
        return _make_response("NVDA", conviction=0.85, signal_direction="bearish")

    _install_fake_gateway(_fake)
    assert (
        generate_pair_ticket(MagicMock(), "TSM", "NVDA", account_size_usd=100_000.0)
        is None
    )


def test_generate_pair_ticket_correlated_risk_trap_returns_none(monkeypatch) -> None:
    """When both tickers resolve to the same sector and same direction."""
    from intelligence import pair_conviction

    monkeypatch.setattr(
        pair_conviction,
        "_sector_for_ticker",
        lambda t: "tech" if t in {"AAA", "BBB"} else None,
    )

    def _fake(engine, ticker, *, account_size_usd, **kwargs):
        # Both strong bullish same sector → trap (same direction)
        return _make_response(
            ticker,
            conviction=0.82,
            signal_direction="bullish",
        )

    _install_fake_gateway(_fake)
    result = generate_pair_ticket(
        MagicMock(),
        "AAA",
        "BBB",
        account_size_usd=100_000.0,
    )
    assert result is None


def test_generate_pair_ticket_short_leg_fragile_returns_none() -> None:
    def _fake(engine, ticker, *, account_size_usd, **kwargs):
        if ticker == "TSM":
            return _make_response("TSM", conviction=0.88, signal_direction="bullish")
        # NVDA passes conviction floor but fragile
        return _make_response(
            "NVDA",
            conviction=0.82,
            signal_direction="bearish",
            robust_label="fragile",
            robust_score=0.3,
        )

    _install_fake_gateway(_fake)
    assert (
        generate_pair_ticket(MagicMock(), "TSM", "NVDA", account_size_usd=100_000.0)
        is None
    )


def test_generate_pair_ticket_missing_gateway_returns_none() -> None:
    """Engine path raises at import — generate_pair_ticket must return None."""
    def _raising(engine, ticker, *, account_size_usd, **kwargs):
        raise RuntimeError("gateway down")

    _install_fake_gateway(_raising)
    assert (
        generate_pair_ticket(MagicMock(), "TSM", "NVDA", account_size_usd=100_000.0)
        is None
    )


def test_scan_candidate_pairs_filters_invalid(monkeypatch) -> None:
    from intelligence import pair_conviction

    # Neutralize sector lookup so correlated-risk trap never fires in this test
    monkeypatch.setattr(pair_conviction, "_sector_for_ticker", lambda t: None)

    def _fake(engine, ticker, *, account_size_usd, **kwargs):
        mapping = {
            "GOOD_L": _make_response("GOOD_L", conviction=0.88, signal_direction="bullish"),
            "GOOD_S": _make_response("GOOD_S", conviction=0.82, signal_direction="bearish"),
            "WEAK_L": _make_response("WEAK_L", conviction=0.2, signal_direction="bullish"),
            "WEAK_S": _make_response("WEAK_S", conviction=0.2, signal_direction="bearish"),
            "OK_L": _make_response("OK_L", conviction=0.75, signal_direction="bullish"),
            "OK_S": _make_response("OK_S", conviction=0.74, signal_direction="bullish"),
            # Both bullish, tiny gap → fails MIN_SPREAD_SHARPNESS
        }
        return mapping[ticker]

    _install_fake_gateway(_fake)
    candidates = [
        PairCandidate("GOOD_L", "GOOD_S", "rel", "valid opposite-direction pair"),
        PairCandidate("WEAK_L", "WEAK_S", "rel", "both legs below conviction floor"),
        PairCandidate("OK_L", "OK_S", "rel", "both bullish with tiny gap"),
    ]
    tickets = scan_candidate_pairs(
        MagicMock(),
        candidates,
        account_size_usd=100_000.0,
    )
    assert len(tickets) == 1
    assert tickets[0].long_leg.ticker == "GOOD_L"
    assert tickets[0].short_leg.ticker == "GOOD_S"


def test_default_pair_candidates_non_empty() -> None:
    assert len(DEFAULT_PAIR_CANDIDATES) >= 5
    for cand in DEFAULT_PAIR_CANDIDATES:
        assert isinstance(cand, PairCandidate)
        assert cand.long_ticker
        assert cand.short_ticker
        assert cand.rationale.strip() != ""
        assert cand.expected_relationship.strip() != ""
