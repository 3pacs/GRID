"""
Tests for trading/solana/safety.py.

The Solana RPC and Jupiter clients are mocked — we assert on which checks
fire for a given mint state, not on real HTTP.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from trading.solana.jupiter_client import JupiterError, USDC_MINT
from trading.solana.safety import (
    SEVERITY_BLOCK,
    SEVERITY_WARN,
    SafetyConfig,
    SolanaSafetyChecker,
    parse_mint_blocklist,
)
from trading.solana.solana_rpc import MintInfo, SolanaRPCError, TokenHolder

MINT = "So11111111111111111111111111111111111111112"


def _mint_info(
    mint_renounced: bool = True,
    freeze_renounced: bool = True,
    supply: int = 1_000_000_000_000,
    decimals: int = 9,
    initialized: bool = True,
) -> MintInfo:
    return MintInfo(
        mint=MINT,
        supply=supply,
        decimals=decimals,
        is_initialized=initialized,
        mint_authority_renounced=mint_renounced,
        freeze_authority_renounced=freeze_renounced,
    )


@pytest.fixture()
def mock_rpc():
    rpc = MagicMock()
    rpc.get_mint_info.return_value = _mint_info()
    # Three small holders, together 3% of supply → passes concentration.
    rpc.get_token_largest_accounts.return_value = [
        TokenHolder(address="A1", amount=10_000_000_000, ui_amount=10.0),
        TokenHolder(address="A2", amount=10_000_000_000, ui_amount=10.0),
        TokenHolder(address="A3", amount=10_000_000_000, ui_amount=10.0),
    ]
    return rpc


@pytest.fixture()
def mock_jupiter():
    j = MagicMock()
    j.get_token_price.return_value = {
        MINT: {"usdPrice": 100.0, "decimals": 9},
        USDC_MINT: {"usdPrice": 1.0, "decimals": 6},
    }
    # Return an order whose out_amount equals expected — zero slippage
    order = MagicMock()
    order.out_amount = 100 * 10**6  # 100 USDC atoms
    j.get_order.return_value = order
    return j


# ----------------------------------------------------------------------
# Happy path
# ----------------------------------------------------------------------
def test_check_token_all_pass(mock_rpc, mock_jupiter):
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    # Selling 1 SOL (1e9 atoms) at 100 USD — expected 100 USDC, quote 100 USDC
    report = checker.check_token(
        mint=MINT,
        trade_size_atoms=10**9,
        quote_mint=USDC_MINT,
        taker="TAKER1",
    )

    assert report.passed is True
    assert report.blockers == ()
    names = {c.name for c in report.checks}
    assert "mint_authority" in names
    assert "freeze_authority" in names
    assert "holder_concentration" in names
    assert "price_impact" in names


# ----------------------------------------------------------------------
# Mint info blockers
# ----------------------------------------------------------------------
def test_mint_authority_active_blocks(mock_rpc, mock_jupiter):
    mock_rpc.get_mint_info.return_value = _mint_info(mint_renounced=False)
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    report = checker.check_token(MINT)
    assert report.passed is False
    names = {c.name for c in report.blockers}
    assert "mint_authority" in names


def test_freeze_authority_active_blocks(mock_rpc, mock_jupiter):
    mock_rpc.get_mint_info.return_value = _mint_info(freeze_renounced=False)
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    report = checker.check_token(MINT)
    assert report.passed is False
    names = {c.name for c in report.blockers}
    assert "freeze_authority" in names


def test_mint_rpc_error_blocks(mock_rpc, mock_jupiter):
    mock_rpc.get_mint_info.side_effect = SolanaRPCError("boom")
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    report = checker.check_token(MINT)
    assert report.passed is False
    assert any(c.name == "mint_info" for c in report.blockers)


def test_uninitialized_mint_blocks(mock_rpc, mock_jupiter):
    mock_rpc.get_mint_info.return_value = _mint_info(initialized=False)
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    report = checker.check_token(MINT)
    assert report.passed is False
    assert any(c.name == "mint_initialized" for c in report.blockers)


def test_config_disables_mint_authority_requirement(mock_rpc, mock_jupiter):
    mock_rpc.get_mint_info.return_value = _mint_info(mint_renounced=False)
    checker = SolanaSafetyChecker(
        rpc=mock_rpc,
        jupiter=mock_jupiter,
        config=SafetyConfig(require_mint_renounced=False),
    )
    report = checker.check_token(MINT)
    # Still reported as failed, but as a warning, not a blocker.
    assert report.passed is True
    names = {c.name for c in report.warnings}
    assert "mint_authority" in names


# ----------------------------------------------------------------------
# Holder concentration
# ----------------------------------------------------------------------
def test_top10_over_threshold_blocks(mock_rpc, mock_jupiter):
    # Single holder owns 50% of supply → fails concentration cap
    mock_rpc.get_token_largest_accounts.return_value = [
        TokenHolder(address="WHALE", amount=500_000_000_000, ui_amount=500.0),
    ]
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    report = checker.check_token(MINT)
    assert report.passed is False
    assert any(c.name == "holder_concentration" for c in report.blockers)


def test_burn_addresses_excluded_from_concentration(mock_rpc, mock_jupiter):
    # System program holds 90%, real holder 1% — the burn address should
    # be excluded, so concentration stays under the cap.
    mock_rpc.get_token_largest_accounts.return_value = [
        TokenHolder(
            address="11111111111111111111111111111111",
            amount=900_000_000_000,
            ui_amount=900.0,
        ),
        TokenHolder(address="REAL", amount=10_000_000_000, ui_amount=10.0),
    ]
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    report = checker.check_token(MINT)
    concentration = next(
        c for c in report.checks if c.name == "holder_concentration"
    )
    assert concentration.passed is True
    assert concentration.metric is not None
    assert concentration.metric < 25.0


def test_holder_rpc_error_is_warning_not_block(mock_rpc, mock_jupiter):
    mock_rpc.get_token_largest_accounts.side_effect = SolanaRPCError("nope")
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    report = checker.check_token(MINT)
    # Other checks still pass → overall pass
    assert report.passed is True
    assert any(
        c.name == "holder_concentration" and c.severity == SEVERITY_WARN
        for c in report.warnings
    )


# ----------------------------------------------------------------------
# Price impact
# ----------------------------------------------------------------------
def test_price_impact_under_limit_passes(mock_rpc, mock_jupiter):
    # Expected 100 USDC from 1 SOL * $100; simulated order returns 99 USDC
    mock_jupiter.get_order.return_value.out_amount = 99 * 10**6
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    report = checker.check_token(
        mint=MINT, trade_size_atoms=10**9, taker="TAKER1"
    )
    impact = next(c for c in report.checks if c.name == "price_impact")
    assert impact.passed is True
    assert impact.metric is not None and 0 < impact.metric < 5


def test_price_impact_over_limit_blocks(mock_rpc, mock_jupiter):
    # Simulated order returns 90 USDC vs 100 expected → 10% impact
    mock_jupiter.get_order.return_value.out_amount = 90 * 10**6
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    report = checker.check_token(
        mint=MINT, trade_size_atoms=10**9, taker="TAKER1"
    )
    assert report.passed is False
    assert any(c.name == "price_impact" for c in report.blockers)


def test_price_impact_jupiter_refuses_blocks(mock_rpc, mock_jupiter):
    mock_jupiter.get_order.side_effect = JupiterError("no route")
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    report = checker.check_token(
        mint=MINT, trade_size_atoms=10**9, taker="TAKER1"
    )
    assert report.passed is False
    blocker_names = {c.name for c in report.blockers}
    assert "price_impact" in blocker_names


def test_price_impact_skipped_when_no_taker_or_size(mock_rpc, mock_jupiter):
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    # No trade_size_atoms → no price impact check at all.
    report = checker.check_token(mint=MINT)
    assert all(c.name != "price_impact" for c in report.checks)


def test_price_impact_no_spot_price_warns(mock_rpc, mock_jupiter):
    mock_jupiter.get_token_price.return_value = {USDC_MINT: {"usdPrice": 1.0}}
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    report = checker.check_token(
        mint=MINT, trade_size_atoms=10**9, taker="TAKER1"
    )
    # Other checks still pass → overall pass with warning
    assert report.passed is True
    assert any(
        c.name == "price_impact" and c.severity == SEVERITY_WARN
        for c in report.warnings
    )


def test_check_token_requires_mint():
    checker = SolanaSafetyChecker(rpc=MagicMock(), jupiter=MagicMock())
    with pytest.raises(ValueError):
        checker.check_token("")


# ----------------------------------------------------------------------
# Report helpers
# ----------------------------------------------------------------------
def test_report_summary_ok(mock_rpc, mock_jupiter):
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    report = checker.check_token(MINT)
    assert "OK" in report.summary()


def test_report_summary_blocked(mock_rpc, mock_jupiter):
    mock_rpc.get_mint_info.return_value = _mint_info(mint_renounced=False)
    checker = SolanaSafetyChecker(rpc=mock_rpc, jupiter=mock_jupiter)
    report = checker.check_token(MINT)
    assert "BLOCKED" in report.summary()
    assert "mint_authority" in report.summary()


# ----------------------------------------------------------------------
# Operator conflict-of-interest blocklist
# ----------------------------------------------------------------------
BLOCKED_MINT = "80085BAGBAGBAGBAGBAGBAGBAGBAGBAGBAGBAGBAGBA"


def test_blocklist_short_circuits_before_rpc(mock_rpc, mock_jupiter):
    checker = SolanaSafetyChecker(
        rpc=mock_rpc,
        jupiter=mock_jupiter,
        config=SafetyConfig(blocked_mints=frozenset({BLOCKED_MINT})),
    )
    report = checker.check_token(BLOCKED_MINT)

    assert report.passed is False
    assert len(report.checks) == 1
    block = report.checks[0]
    assert block.name == "operator_conflict_of_interest"
    assert block.severity == SEVERITY_BLOCK
    # The whole point — neither RPC nor Jupiter is ever consulted.
    mock_rpc.get_mint_info.assert_not_called()
    mock_rpc.get_token_largest_accounts.assert_not_called()
    mock_jupiter.get_token_price.assert_not_called()
    mock_jupiter.get_order.assert_not_called()


def test_blocklist_does_not_affect_other_mints(mock_rpc, mock_jupiter):
    checker = SolanaSafetyChecker(
        rpc=mock_rpc,
        jupiter=mock_jupiter,
        config=SafetyConfig(blocked_mints=frozenset({BLOCKED_MINT})),
    )
    # MINT is not blocked → the full pipeline runs as usual.
    report = checker.check_token(MINT)
    assert report.passed is True
    mock_rpc.get_mint_info.assert_called_once()


def test_blocklist_cannot_be_disabled_by_other_config(mock_rpc, mock_jupiter):
    # Even with every other requirement turned off, a blocklisted mint
    # still fails — the block is unconditional.
    checker = SolanaSafetyChecker(
        rpc=mock_rpc,
        jupiter=mock_jupiter,
        config=SafetyConfig(
            blocked_mints=frozenset({BLOCKED_MINT}),
            require_mint_renounced=False,
            require_freeze_renounced=False,
            max_top10_holder_pct=100.0,
            max_price_impact_pct=100.0,
        ),
    )
    report = checker.check_token(BLOCKED_MINT)
    assert report.passed is False
    assert report.blockers[0].name == "operator_conflict_of_interest"


def test_blocklist_summary_mentions_blocker(mock_rpc, mock_jupiter):
    checker = SolanaSafetyChecker(
        rpc=mock_rpc,
        jupiter=mock_jupiter,
        config=SafetyConfig(blocked_mints=frozenset({BLOCKED_MINT})),
    )
    report = checker.check_token(BLOCKED_MINT)
    summary = report.summary()
    assert "BLOCKED" in summary
    assert "operator_conflict_of_interest" in summary


def test_empty_blocklist_is_noop(mock_rpc, mock_jupiter):
    # No blocked_mints set — baseline behaviour, full pipeline runs.
    checker = SolanaSafetyChecker(
        rpc=mock_rpc, jupiter=mock_jupiter, config=SafetyConfig()
    )
    report = checker.check_token(MINT)
    assert report.passed is True
    mock_rpc.get_mint_info.assert_called_once()


# ----------------------------------------------------------------------
# parse_mint_blocklist
# ----------------------------------------------------------------------
def test_parse_mint_blocklist_happy_path():
    result = parse_mint_blocklist("MINT_A, MINT_B ,MINT_C")
    assert result == frozenset({"MINT_A", "MINT_B", "MINT_C"})


def test_parse_mint_blocklist_empty_string():
    assert parse_mint_blocklist("") == frozenset()


def test_parse_mint_blocklist_none():
    assert parse_mint_blocklist(None) == frozenset()


def test_parse_mint_blocklist_preserves_case():
    # Solana addresses are case-sensitive base58.
    result = parse_mint_blocklist("AbCdEf,XyZ")
    assert result == frozenset({"AbCdEf", "XyZ"})


def test_parse_mint_blocklist_strips_empty_entries():
    result = parse_mint_blocklist(",,MINT_A,,,MINT_B,")
    assert result == frozenset({"MINT_A", "MINT_B"})
