"""
Tests for trading/solana/fast_entry.py.

Wires CrossReferencer + PaperSolanaExecutor through mocks and
verifies gating, decision synthesis, and error isolation.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from trading.solana.cross_ref import (
    CrossRefReport,
    DEFAULT_CROSS_REF_WEIGHTS,
    LaunchEvent,
)
from trading.solana.executor import ExecutionResult
from trading.solana.fast_entry import (
    FastEntryConfig,
    FastEntryPath,
)
from trading.solana.smart_money import SmartMoneyMatchSet


def _report(
    mint: str = "MINT1",
    composite: float = 0.8,
    deployer: float = 0.7,
    smart_money_hits: int = 2,
    smart_money_trust: float = 0.9,
    narrative: float = 0.5,
    convergence: float = 0.4,
) -> CrossRefReport:
    return CrossRefReport(
        mint=mint,
        composite_score=composite,
        deployer_score=deployer,
        smart_money_hits=smart_money_hits,
        smart_money_trust=smart_money_trust,
        narrative_weight=narrative,
        convergence_score=convergence,
        deployer_result=None,
        smart_money_matches=SmartMoneyMatchSet(matches=()),
        narrative_hits=(),
        reasons=("test",),
        weights=DEFAULT_CROSS_REF_WEIGHTS,
    )


def _exec_result(mode: str = "paper") -> ExecutionResult:
    return ExecutionResult(
        action="BUY",
        mode=mode,
        trade_id=42 if mode == "paper" else None,
        symbol="SYM",
        mint="MINT1",
        entry_price=100.0,
        size_fraction=0.6,
        reason="ok",
    )


@pytest.fixture()
def mock_cross_ref():
    cr = MagicMock()
    cr.evaluate.return_value = _report()
    return cr


@pytest.fixture()
def mock_executor():
    ex = MagicMock()
    ex.execute.return_value = _exec_result()
    return ex


@pytest.fixture()
def path(mock_cross_ref, mock_executor):
    return FastEntryPath(
        executor=mock_executor,
        cross_referencer=mock_cross_ref,
    )


# ----------------------------------------------------------------------
# Gating
# ----------------------------------------------------------------------
def test_handle_dispatches_on_high_score(path, mock_cross_ref, mock_executor):
    result = path.handle(LaunchEvent(mint="MINT1"))
    assert result.skipped is False
    assert result.executor_result is not None
    mock_cross_ref.evaluate.assert_called_once()
    mock_executor.execute.assert_called_once()


def test_handle_skips_below_threshold(path, mock_cross_ref, mock_executor):
    mock_cross_ref.evaluate.return_value = _report(composite=0.1)
    result = path.handle(LaunchEvent(mint="MINT1"))
    assert result.skipped is True
    assert "composite" in result.reason
    mock_executor.execute.assert_not_called()


def test_handle_skips_empty_mint(path, mock_cross_ref):
    result = path.handle(LaunchEvent(mint=""))
    assert result.skipped is True
    assert result.reason == "empty mint"
    mock_cross_ref.evaluate.assert_not_called()


def test_handle_require_deployer(mock_cross_ref, mock_executor):
    mock_cross_ref.evaluate.return_value = _report(deployer=0.0, composite=0.9)
    path = FastEntryPath(
        executor=mock_executor,
        cross_referencer=mock_cross_ref,
        config=FastEntryConfig(require_deployer=True, min_composite_score=0.4),
    )
    result = path.handle(LaunchEvent(mint="MINT1"))
    assert result.skipped is True
    assert "deployer" in result.reason
    mock_executor.execute.assert_not_called()


# ----------------------------------------------------------------------
# Decision synthesis
# ----------------------------------------------------------------------
def test_handle_scales_size_by_composite(path, mock_cross_ref, mock_executor):
    mock_cross_ref.evaluate.return_value = _report(composite=0.8)
    path.handle(LaunchEvent(mint="MINT1"))
    decision = mock_executor.execute.call_args.args[0]
    # base_size_fraction=0.6 * composite=0.8 = 0.48
    assert decision.size_fraction == pytest.approx(0.48)
    assert decision.action == "BUY"
    assert decision.task.startswith("fast_entry:MINT1")
    assert decision.quant["path"] == "fast_entry"


def test_handle_sets_stop_loss_from_config(mock_cross_ref, mock_executor):
    config = FastEntryConfig(stop_loss_pct=0.25, min_composite_score=0.1)
    path = FastEntryPath(
        executor=mock_executor, cross_referencer=mock_cross_ref, config=config
    )
    path.handle(LaunchEvent(mint="MINT1"))
    decision = mock_executor.execute.call_args.args[0]
    assert decision.stop_loss_pct == 0.25


def test_handle_thesis_contains_reasons(path, mock_cross_ref, mock_executor):
    mock_cross_ref.evaluate.return_value = _report(
        composite=0.7,
    )
    # Replace reasons with something recognisable
    report = _report(composite=0.7)
    report_with_reasons = CrossRefReport(
        mint=report.mint,
        composite_score=report.composite_score,
        deployer_score=report.deployer_score,
        smart_money_hits=report.smart_money_hits,
        smart_money_trust=report.smart_money_trust,
        narrative_weight=report.narrative_weight,
        convergence_score=report.convergence_score,
        deployer_result=report.deployer_result,
        smart_money_matches=report.smart_money_matches,
        narrative_hits=report.narrative_hits,
        reasons=("deployer W1 score=0.7", "2 smart-money wallets"),
        weights=report.weights,
    )
    mock_cross_ref.evaluate.return_value = report_with_reasons
    path.handle(LaunchEvent(mint="MINT1", symbol="SYM"))
    decision = mock_executor.execute.call_args.args[0]
    assert "deployer" in decision.thesis
    assert "smart-money" in decision.thesis


def test_handle_symbol_defaults_to_mint_prefix(path, mock_executor):
    path.handle(LaunchEvent(mint="ABCDEFGHIJKL"))
    decision = mock_executor.execute.call_args.args[0]
    assert decision.symbol.startswith("ABCDEFGH")


def test_handle_symbol_from_launch_preferred(path, mock_executor):
    path.handle(LaunchEvent(mint="M1", symbol="WIF"))
    decision = mock_executor.execute.call_args.args[0]
    assert decision.symbol == "WIF"


# ----------------------------------------------------------------------
# Error isolation
# ----------------------------------------------------------------------
def test_handle_cross_ref_error_is_skip(mock_cross_ref, mock_executor):
    mock_cross_ref.evaluate.side_effect = RuntimeError("db down")
    path = FastEntryPath(
        executor=mock_executor, cross_referencer=mock_cross_ref
    )
    result = path.handle(LaunchEvent(mint="M1"))
    assert result.skipped is True
    assert "cross_ref error" in result.reason
    mock_executor.execute.assert_not_called()


def test_handle_executor_error_is_captured(mock_cross_ref, mock_executor):
    mock_executor.execute.side_effect = RuntimeError("exec boom")
    path = FastEntryPath(
        executor=mock_executor, cross_referencer=mock_cross_ref
    )
    result = path.handle(LaunchEvent(mint="M1"))
    assert result.skipped is True
    assert "executor error" in result.reason
    assert result.decision is not None  # We synthesised before executing
