"""Unit tests for oracle/sanity_checker.py.

Covers the deterministic checks that gate `oracle.firewall.verify_output`
and `oracle.publisher_gate.gate_decision` — price-range, percentage math,
direction consistency, date sanity, unit sanity, and cross-claim
consistency.  Pure functions; no DB or LLM dependency.

TIER 4 punch-list item — docs/PUNCH-LIST-2026-05-13.md [P1] item 8.
"""
from __future__ import annotations

from datetime import date

from oracle.claim_extractor import Claim
from oracle.claim_verifier import VerifiedClaim
from oracle.sanity_checker import (
    CheckedClaim,
    SanityResult,
    _check_cross_claim_consistency,
    _check_date_sanity,
    _check_direction_consistency,
    _check_pct_math,
    _check_price_range,
    _check_unit_sanity,
    run_sanity_checks,
)


# ── helpers ──────────────────────────────────────────────────────────────


def _vc(
    text: str,
    claim_type: str = "price",
    ticker: str | None = None,
    value: float | None = None,
    verdict: str = "supported",
    reason: str = "",
) -> VerifiedClaim:
    """Build a VerifiedClaim with sensible defaults for tests."""
    return VerifiedClaim(
        claim=Claim(
            text=text,
            claim_type=claim_type,  # type: ignore[arg-type]
            ticker=ticker,
            value=value,
        ),
        verdict=verdict,  # type: ignore[arg-type]
        reason=reason,
    )


# ── _check_price_range ───────────────────────────────────────────────────


def test_price_range_within_known_ticker_passes() -> None:
    result = _check_price_range(_vc("BTC at $50,000", ticker="BTC", value=50_000.0))
    assert result is not None
    assert result.flag == "pass"
    assert "BTC" in result.message


def test_price_range_outside_known_ticker_fails() -> None:
    # SPY range is (100, 1500); 2_000 is above the ceiling.
    result = _check_price_range(_vc("SPY at $2,000", ticker="SPY", value=2_000.0))
    assert result is not None
    assert result.flag == "fail"
    assert "outside plausible range" in result.message


def test_price_range_below_floor_fails() -> None:
    # VIX range is (5, 90); 1.0 is below the floor.
    result = _check_price_range(_vc("VIX at 1", ticker="VIX", value=1.0))
    assert result is not None
    assert result.flag == "fail"


def test_price_range_ticker_normalised_to_uppercase() -> None:
    # ticker arrives lowercase from extractor; should still hit table.
    result = _check_price_range(_vc("btc at $50k", ticker="btc", value=50_000.0))
    assert result is not None
    assert result.flag == "pass"


def test_price_range_unknown_ticker_passes_with_note() -> None:
    result = _check_price_range(_vc("XYZ at $1.50", ticker="XYZ", value=1.50))
    assert result is not None
    assert result.flag == "pass"
    assert "No range data" in result.message


def test_price_range_returns_none_for_non_price_claim() -> None:
    assert _check_price_range(_vc("up 5%", claim_type="percentage")) is None


def test_price_range_returns_none_when_value_missing() -> None:
    assert _check_price_range(_vc("SPY rallied", ticker="SPY", value=None)) is None


def test_price_range_returns_none_when_ticker_missing() -> None:
    assert _check_price_range(_vc("the price hit $500", ticker=None, value=500.0)) is None


# ── _check_pct_math ──────────────────────────────────────────────────────


def test_pct_math_correct_math_passes() -> None:
    result = _check_pct_math(_vc("from $100 to $110, a 10% gain", claim_type="percentage"))
    assert result is not None
    assert result.flag == "pass"


def test_pct_math_incorrect_math_fails() -> None:
    result = _check_pct_math(_vc("from $100 to $110, a 50% gain", claim_type="percentage"))
    assert result is not None
    assert result.flag == "fail"
    assert "actually" in result.message


def test_pct_math_tolerates_within_three_points() -> None:
    # Actual = 10.0%, claimed = 12.0% → diff 2.0 ≤ 3.0 tolerance.
    result = _check_pct_math(_vc("from $100 to $110, a 12% gain", claim_type="percentage"))
    assert result is not None
    assert result.flag == "pass"


def test_pct_math_negative_direction_correct() -> None:
    result = _check_pct_math(_vc("from $100 to $90, a -10% drop", claim_type="percentage"))
    assert result is not None
    assert result.flag == "pass"


def test_pct_math_zero_base_returns_none() -> None:
    # val_from == 0 → cannot compute percent change; skip silently.
    assert _check_pct_math(_vc("from $0 to $10, 100% gain", claim_type="percentage")) is None


def test_pct_math_no_pattern_returns_none() -> None:
    # No "from X to Y, Z%" structure → no math to verify.
    assert _check_pct_math(_vc("up 10% this week", claim_type="percentage")) is None


def test_pct_math_returns_none_for_non_percentage_claim() -> None:
    assert _check_pct_math(_vc("from $100 to $110, a 10% gain", claim_type="price")) is None


# ── _check_direction_consistency ─────────────────────────────────────────


def test_direction_contradicted_fails() -> None:
    result = _check_direction_consistency(
        _vc("market rallied", claim_type="direction", verdict="contradicted", reason="opp move")
    )
    assert result is not None
    assert result.flag == "fail"
    assert "opp move" in result.message


def test_direction_supported_passes() -> None:
    result = _check_direction_consistency(
        _vc("market rallied", claim_type="direction", verdict="supported")
    )
    assert result is not None
    assert result.flag == "pass"


def test_direction_insufficient_passes() -> None:
    # Anything other than "contradicted" is treated as a pass at this stage.
    result = _check_direction_consistency(
        _vc("market rallied", claim_type="direction", verdict="insufficient")
    )
    assert result is not None
    assert result.flag == "pass"


def test_direction_returns_none_for_non_direction_claim() -> None:
    assert (
        _check_direction_consistency(_vc("SPY at $500", claim_type="price", value=500.0))
        is None
    )


# ── _check_date_sanity ───────────────────────────────────────────────────


def test_date_sanity_past_tense_with_future_year_fails() -> None:
    future_year = date.today().year + 5
    result = _check_date_sanity(
        _vc(f"VIX rose to 30 in {future_year}", claim_type="date")
    )
    assert result is not None
    assert result.flag == "fail"
    assert str(future_year) in result.message


def test_date_sanity_past_tense_with_present_year_passes() -> None:
    result = _check_date_sanity(
        _vc(f"VIX rose to 30 in {date.today().year}", claim_type="date")
    )
    assert result is not None
    assert result.flag == "pass"


def test_date_sanity_past_tense_with_past_year_passes() -> None:
    past_year = date.today().year - 3
    result = _check_date_sanity(_vc(f"SPY fell in {past_year}", claim_type="date"))
    assert result is not None
    assert result.flag == "pass"


def test_date_sanity_no_past_tense_returns_none() -> None:
    future_year = date.today().year + 5
    # Forecast language; no past-tense markers → no judgment.
    assert (
        _check_date_sanity(_vc(f"SPY may reach $700 by {future_year}", claim_type="date"))
        is None
    )


def test_date_sanity_no_year_in_text_returns_none() -> None:
    assert _check_date_sanity(_vc("SPY rose last quarter", claim_type="date")) is None


def test_date_sanity_returns_none_for_non_date_claim() -> None:
    assert _check_date_sanity(_vc("SPY at $500", claim_type="price", value=500.0)) is None


# ── _check_unit_sanity ───────────────────────────────────────────────────


def test_unit_sanity_quadrillion_flagged() -> None:
    result = _check_unit_sanity(
        _vc("market cap reached $5Q today", claim_type="price", value=5e15)
    )
    assert result is not None
    assert result.flag == "fail"
    assert "Q" in result.message or "quadrillion" in result.message.lower()


def test_unit_sanity_quadrillion_case_insensitive() -> None:
    result = _check_unit_sanity(_vc("$2.5q valuation", claim_type="price", value=2.5e15))
    assert result is not None
    assert result.flag == "fail"


def test_unit_sanity_trillion_passes() -> None:
    result = _check_unit_sanity(_vc("$3T market cap", claim_type="price", value=3e12))
    assert result is not None
    assert result.flag == "pass"


def test_unit_sanity_billion_passes() -> None:
    result = _check_unit_sanity(_vc("$1.5B revenue", claim_type="price", value=1.5e9))
    assert result is not None
    assert result.flag == "pass"


def test_unit_sanity_returns_none_for_non_price_claim() -> None:
    assert _check_unit_sanity(_vc("$5Q valuation", claim_type="narrative")) is None


# ── _check_cross_claim_consistency ───────────────────────────────────────


def test_cross_claim_bullish_and_bearish_same_ticker_warns() -> None:
    claims = [
        _vc("SPY rallied hard today", claim_type="direction", ticker="SPY"),
        _vc("SPY tanked into the close", claim_type="direction", ticker="SPY"),
    ]
    results = _check_cross_claim_consistency(claims)
    assert len(results) == 2
    indices = sorted(idx for idx, _ in results)
    assert indices == [0, 1]
    for _, sr in results:
        assert sr.flag == "warn"
        assert sr.check_name == "cross_claim"


def test_cross_claim_bullish_and_bearish_no_ticker_warns() -> None:
    # `None` ticker is treated as the broad market — still flag.
    claims = [
        _vc("market rallied", claim_type="direction", ticker=None),
        _vc("market dropped", claim_type="direction", ticker=None),
    ]
    results = _check_cross_claim_consistency(claims)
    assert len(results) == 2


def test_cross_claim_opposing_tickers_not_flagged() -> None:
    # SPY up + QQQ down is normal market behaviour, not a contradiction.
    claims = [
        _vc("SPY rallied", claim_type="direction", ticker="SPY"),
        _vc("QQQ dropped", claim_type="direction", ticker="QQQ"),
    ]
    assert _check_cross_claim_consistency(claims) == []


def test_cross_claim_only_bullish_not_flagged() -> None:
    claims = [
        _vc("SPY rallied", claim_type="direction", ticker="SPY"),
        _vc("QQQ surged", claim_type="direction", ticker="QQQ"),
    ]
    assert _check_cross_claim_consistency(claims) == []


def test_cross_claim_empty_list_returns_empty() -> None:
    assert _check_cross_claim_consistency([]) == []


# ── run_sanity_checks (integration) ──────────────────────────────────────


def test_run_sanity_checks_empty_list() -> None:
    assert run_sanity_checks([]) == []


def test_run_sanity_checks_single_pass_no_critical() -> None:
    [checked] = run_sanity_checks([_vc("BTC at $50k", ticker="BTC", value=50_000.0)])
    assert isinstance(checked, CheckedClaim)
    assert checked.critical_fail is False
    assert all(isinstance(c, SanityResult) for c in checked.checks)
    assert all(c.flag == "pass" for c in checked.checks)


def test_run_sanity_checks_single_fail_marks_critical() -> None:
    # Out-of-range price triggers price_range fail.
    [checked] = run_sanity_checks([_vc("SPY at $9,999", ticker="SPY", value=9_999.0)])
    assert checked.critical_fail is True
    assert any(c.flag == "fail" and c.check_name == "price_range" for c in checked.checks)


def test_run_sanity_checks_warn_does_not_mark_critical() -> None:
    # Two direction claims with conflicting language on the same ticker
    # produce only a `warn` from cross_claim — critical_fail must stay False.
    claims = [
        _vc("SPY rallied", claim_type="direction", ticker="SPY"),
        _vc("SPY tanked", claim_type="direction", ticker="SPY"),
    ]
    results = run_sanity_checks(claims)
    assert len(results) == 2
    for r in results:
        assert r.critical_fail is False
        assert any(c.flag == "warn" and c.check_name == "cross_claim" for c in r.checks)


def test_run_sanity_checks_preserves_input_order() -> None:
    claims = [
        _vc("BTC at $50k", ticker="BTC", value=50_000.0),
        _vc("SPY at $400", ticker="SPY", value=400.0),
    ]
    results = run_sanity_checks(claims)
    assert results[0].verified.claim.ticker == "BTC"
    assert results[1].verified.claim.ticker == "SPY"


def test_run_sanity_checks_attaches_cross_claim_warnings() -> None:
    claims = [
        _vc("market rallied hard", claim_type="direction"),
        _vc("market tanked", claim_type="direction"),
        _vc("BTC at $50k", ticker="BTC", value=50_000.0),  # unrelated price claim
    ]
    results = run_sanity_checks(claims)
    # The two direction claims should carry a cross_claim warn.
    assert any(c.check_name == "cross_claim" for c in results[0].checks)
    assert any(c.check_name == "cross_claim" for c in results[1].checks)
    # The unrelated price claim should not.
    assert all(c.check_name != "cross_claim" for c in results[2].checks)
