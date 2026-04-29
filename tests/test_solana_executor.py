"""
Tests for trading/solana/executor.py.

The executor gate is (decision → safety → limits → paper|live). These
tests inject mocks for each stage so we can walk every branch without a
real DB, RPC, or LLM.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from trading.solana.executor import (
    ExecutionResult,
    PaperSolanaExecutor,
    SOLANA_STRATEGY_ID,
)
from trading.solana.jupiter_client import SOL_MINT, USDC_MINT
from trading.solana.limits import LimitDecision
from trading.solana.pipeline import PipelineDecision
from trading.solana.safety import SafetyCheck, TokenSafetyReport


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _decision(
    action: str = "BUY",
    size_fraction: float = 0.5,
    risk_veto: bool = False,
) -> PipelineDecision:
    return PipelineDecision(
        generated_at=datetime.now(timezone.utc).isoformat(),
        task="t",
        symbol="SOL",
        mint=SOL_MINT,
        thesis="test thesis",
        action=action,
        size_fraction=size_fraction,
        stop_loss_pct=0.08,
        take_profit_pct=0.15,
        risk_score=0.7,
        risk_veto=risk_veto,
    )


def _passing_safety_report() -> TokenSafetyReport:
    return TokenSafetyReport(
        mint=SOL_MINT,
        checks=(
            SafetyCheck(name="mint_authority", passed=True, severity="block", detail="ok"),
        ),
        passed=True,
    )


def _blocked_safety_report(reason: str = "mint authority active") -> TokenSafetyReport:
    return TokenSafetyReport(
        mint=SOL_MINT,
        checks=(
            SafetyCheck(
                name="mint_authority", passed=False, severity="block", detail=reason
            ),
        ),
        passed=False,
    )


def _passing_limits() -> LimitDecision:
    return LimitDecision(
        passed=True,
        reasons=(),
        daily_usd_used=0.0,
        daily_trades_used=0,
        mint_usd_used=0.0,
    )


def _blocked_limits(reason: str = "daily USD cap") -> LimitDecision:
    return LimitDecision(
        passed=False,
        reasons=(reason,),
        daily_usd_used=200.0,
        daily_trades_used=5,
        mint_usd_used=0.0,
    )


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------
@pytest.fixture()
def mock_jupiter():
    j = MagicMock()
    j.get_token_price.return_value = {SOL_MINT: {"usdPrice": 125.0}}
    return j


@pytest.fixture()
def mock_safety():
    s = MagicMock()
    s.check_token.return_value = _passing_safety_report()
    return s


@pytest.fixture()
def mock_limits():
    l = MagicMock()
    l.check.return_value = _passing_limits()
    l.config.capital_per_trade_usd = 50.0
    return l


@pytest.fixture()
def paper_executor(mock_engine, mock_jupiter, mock_safety, mock_limits):
    executor = PaperSolanaExecutor(
        engine=mock_engine,
        jupiter=mock_jupiter,
        live=False,
        safety=mock_safety,
        limits=mock_limits,
    )
    executor.paper = MagicMock()
    executor.paper.open_trade.return_value = 42
    return executor


# ----------------------------------------------------------------------
# Early skips (before gates)
# ----------------------------------------------------------------------
def test_executor_skips_hold(paper_executor, mock_safety):
    result = paper_executor.execute(_decision(action="HOLD"))
    assert result.mode == "skipped"
    assert result.reason == "action=HOLD"
    assert result.trade_id is None
    paper_executor.paper.open_trade.assert_not_called()
    mock_safety.check_token.assert_not_called()


def test_executor_skips_risk_veto(paper_executor, mock_safety):
    result = paper_executor.execute(_decision(risk_veto=True))
    assert result.mode == "skipped"
    assert result.reason == "risk veto"
    mock_safety.check_token.assert_not_called()


def test_executor_skips_on_missing_price(paper_executor, mock_jupiter, mock_safety):
    mock_jupiter.get_token_price.return_value = {SOL_MINT: {}}
    result = paper_executor.execute(_decision())
    assert result.mode == "skipped"
    assert result.reason == "no price data"
    mock_safety.check_token.assert_not_called()


def test_executor_skips_on_jupiter_error(paper_executor, mock_jupiter):
    mock_jupiter.get_token_price.side_effect = RuntimeError("no network")
    result = paper_executor.execute(_decision())
    assert result.mode == "skipped"


# ----------------------------------------------------------------------
# Safety gate
# ----------------------------------------------------------------------
def test_safety_blocker_skips_trade(paper_executor, mock_safety, mock_limits):
    mock_safety.check_token.return_value = _blocked_safety_report("freeze authority active")
    result = paper_executor.execute(_decision())
    assert result.mode == "skipped"
    assert "safety blocked" in result.reason
    assert result.safety_report is not None
    assert result.safety_report.passed is False
    paper_executor.paper.open_trade.assert_not_called()
    mock_limits.check.assert_not_called()


def test_safety_gate_receives_computed_atoms(paper_executor, mock_safety):
    paper_executor.execute(_decision(size_fraction=0.5))
    call = mock_safety.check_token.call_args.kwargs
    # capital_per_trade_usd=50, fraction=0.5 → $25 notional;
    # at $125/SOL → 0.2 SOL → 2e8 atoms (decimals=9)
    assert call["mint"] == SOL_MINT
    assert call["trade_size_atoms"] > 0
    assert call["quote_mint"] == USDC_MINT


# ----------------------------------------------------------------------
# Limits gate
# ----------------------------------------------------------------------
def test_limit_blocker_skips_trade(paper_executor, mock_limits):
    mock_limits.check.return_value = _blocked_limits("daily USD cap")
    result = paper_executor.execute(_decision())
    assert result.mode == "skipped"
    assert "limit blocked" in result.reason
    assert result.limit_decision is not None
    assert result.limit_decision.passed is False
    paper_executor.paper.open_trade.assert_not_called()


def test_limits_receive_usd_notional(paper_executor, mock_limits):
    paper_executor.execute(_decision(size_fraction=0.4))
    call = mock_limits.check.call_args.kwargs
    # 0.4 * capital_per_trade_usd(50) = 20
    assert call["trade_usd"] == pytest.approx(20.0)
    assert call["mint"] == SOL_MINT


# ----------------------------------------------------------------------
# Paper trade path
# ----------------------------------------------------------------------
def test_executor_paper_buy_opens_long(paper_executor):
    result = paper_executor.execute(_decision(action="BUY"))
    assert result.mode == "paper"
    assert result.trade_id == 42
    assert result.entry_price == 125.0
    kwargs = paper_executor.paper.open_trade.call_args.kwargs
    assert kwargs["direction"] == "LONG"
    assert kwargs["strategy_id"] == SOLANA_STRATEGY_ID
    assert kwargs["ticker"] == "SOL"
    assert kwargs["entry_price"] == 125.0
    # Safety + limits threaded through to the result
    assert result.safety_report is not None
    assert result.limit_decision is not None


def test_executor_paper_sell_opens_short(paper_executor):
    result = paper_executor.execute(_decision(action="SELL"))
    assert result.mode == "paper"
    kwargs = paper_executor.paper.open_trade.call_args.kwargs
    assert kwargs["direction"] == "SHORT"


def test_executor_paper_refused_returns_skipped(paper_executor):
    paper_executor.paper.open_trade.return_value = -1
    result = paper_executor.execute(_decision())
    assert result.mode == "skipped"
    assert result.trade_id is None


# ----------------------------------------------------------------------
# Live path
# ----------------------------------------------------------------------
def test_executor_live_requires_wallet(mock_engine, mock_jupiter, mock_safety, mock_limits):
    executor = PaperSolanaExecutor(
        engine=mock_engine,
        jupiter=mock_jupiter,
        live=True,
        wallet=None,
        safety=mock_safety,
        limits=mock_limits,
    )
    with pytest.raises(RuntimeError, match="no SolanaWallet"):
        executor.execute(_decision())


def test_executor_live_happy_path(
    mock_engine, mock_jupiter, mock_safety, mock_limits
):
    wallet = MagicMock()
    wallet.address = "WALLET123"
    mock_jupiter.get_order.return_value = MagicMock(request_id="req-99")
    mock_jupiter.execute_swap.return_value = {"signature": "sig-1"}

    executor = PaperSolanaExecutor(
        engine=mock_engine,
        jupiter=mock_jupiter,
        live=True,
        wallet=wallet,
        safety=mock_safety,
        limits=mock_limits,
    )
    result = executor.execute(_decision(size_fraction=0.5))

    assert result.mode == "live"
    assert "req-99" in result.reason
    call = mock_jupiter.get_order.call_args.kwargs
    assert call["input_mint"] == USDC_MINT
    assert call["output_mint"] == SOL_MINT
    # Computed atoms from USD notional — NOT the raw fraction
    assert call["amount"] > 0
    mock_jupiter.execute_swap.assert_called_once()
    assert result.safety_report is not None
    assert result.limit_decision is not None


def test_executor_live_raises_when_computed_atoms_zero(
    mock_engine, mock_jupiter, mock_safety, mock_limits
):
    wallet = MagicMock()
    wallet.address = "WALLET123"
    executor = PaperSolanaExecutor(
        engine=mock_engine,
        jupiter=mock_jupiter,
        live=True,
        wallet=wallet,
        safety=mock_safety,
        limits=mock_limits,
    )
    # size_fraction=0 means no capital allocated → atoms=0 → safety still
    # runs but the live path refuses because there's nothing to trade.
    # The safety check will compute 0 atoms and price_impact will be
    # skipped; the ultimate error surfaces as a limit OR as this guard.
    with pytest.raises((ValueError, RuntimeError)):
        executor.execute(_decision(size_fraction=0.0))
